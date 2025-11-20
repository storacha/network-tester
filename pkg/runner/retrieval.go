package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
)

var log = logging.Logger("runner")

type RetrievalTestRunner struct {
	region  string
	indexer *client.Client
	uploads eventlog.Iterable[model.Upload]
	results eventlog.Appender[model.Retrieval]
}

func (r *RetrievalTestRunner) Run(ctx context.Context) error {
	log.Info("Region")
	log.Infof("  %s", r.region)

loop:
	for u, err := range r.uploads.Iterator() {
		if err != nil {
			return err
		}
		if u.Error.Error() != "" {
			log.Infof("Skipping failed upload: %s", u.ID)
			continue
		}
		log.Info("Upload")
		log.Infof("  %s", u.ID)
		log.Infof("    root: %s", u.Root.String())
		log.Infof("    source: %s", u.Source)
		log.Infof("    index: %s", u.Index.String())
		log.Infof("    shards: %v", u.Shards)
		log.Infof("    started: %s", u.Started.Format(time.DateTime))
		log.Infof("    ended: %s", u.Ended.Format(time.DateTime))

		index, err := findIndex(ctx, r.indexer, u.Root, u.Index, []delegation.Delegation{})
		if err != nil {
			err = r.results.Append(model.Retrieval{
				ID:     uuid.New(),
				Region: r.region,
				Source: u.Source,
				Upload: u.ID,
				Error:  model.Error{Message: err.Error()},
			})
			if err != nil {
				return err
			}
			continue
		}

		log.Info("Index")
		log.Infof("  %s", u.Index)

		for shardDigest, slices := range index.Shards().Iterator() {
			shardURL, shardLocationCommitment, err := findLocation(ctx, r.indexer, shardDigest)
			if err != nil {
				err = r.results.Append(model.Retrieval{
					ID:     uuid.New(),
					Region: r.region,
					Source: u.Source,
					Upload: u.ID,
					Shard:  model.Multihash{Multihash: shardDigest},
					Error:  model.Error{Message: err.Error()},
				})
				if err != nil {
					return err
				}
				continue loop
			}

			log.Info("Shard")
			log.Infof("  z%s", shardDigest.B58String())
			log.Infof("    url: %s", shardURL.String())
			log.Info("    slices:")

			for sliceDigest, position := range slices.Iterator() {
				retrieval := testRetrieveSlice(sliceDigest, shardURL, position)
				log.Infof("      z%s @ %d-%d", sliceDigest.B58String(), position.Offset, position.Offset+position.Length-1)

				nodeDid, err := did.Parse(shardLocationCommitment.With())
				if err != nil {
					return fmt.Errorf("parsing node DID from location commitment: %w", err)
				}

				err = r.results.Append(model.Retrieval{
					ID:        uuid.New(),
					Region:    r.region,
					Source:    u.Source,
					Upload:    u.ID,
					Node:      model.DID{DID: nodeDid},
					Shard:     model.Multihash{Multihash: shardDigest},
					Slice:     model.Multihash{Multihash: sliceDigest},
					Size:      int(position.Length),
					Started:   retrieval.Started,
					Responded: retrieval.Responded,
					Ended:     retrieval.Ended,
					Status:    retrieval.Status,
					Error:     model.Error{Message: retrieval.Error},
				})
				if err != nil {
					return err
				}
			}
		}

		log.Infof("%s passed", u.ID)
	}

	return nil
}

type sliceRetrieval struct {
	Started   time.Time
	Responded time.Time
	Ended     time.Time
	Status    int
	Error     string
}

func testRetrieveSlice(slice mh.Multihash, url url.URL, position blobindex.Position) sliceRetrieval {
	started := time.Now()
	req, err := http.NewRequest(http.MethodGet, url.String(), nil)
	if err != nil {
		return sliceRetrieval{
			Started: started,
			Ended:   time.Now(),
			Error:   fmt.Errorf("creating slice request to: %s for slice: z%s: %w", url.String(), slice.B58String(), err).Error(),
		}
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", position.Offset, position.Offset+position.Length-1))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return sliceRetrieval{
			Started: started,
			Ended:   time.Now(),
			Status:  res.StatusCode,
			Error:   fmt.Errorf("sending slice request to: %s Range: bytes=%d-%d for slice: z%s: %w", url.String(), position.Offset, position.Offset+position.Length-1, slice.B58String(), err).Error(),
		}
	}
	responded := time.Now()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return sliceRetrieval{
			Started:   started,
			Responded: responded,
			Ended:     time.Now(),
			Status:    res.StatusCode,
			Error:     fmt.Errorf("reading slice response: %s Range: bytes=%d-%d for slice: z%s: %w", url.String(), position.Offset, position.Offset+position.Length-1, slice.B58String(), err).Error(),
		}
	}
	dataDigest, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		return sliceRetrieval{
			Started:   started,
			Responded: responded,
			Ended:     time.Now(),
			Status:    res.StatusCode,
			Error:     fmt.Errorf("hashing slice response: %s Range: bytes=%d-%d for slice: z%s: %w", url.String(), position.Offset, position.Offset+position.Length-1, slice.B58String(), err).Error(),
		}
	}
	if !bytes.Equal(slice, dataDigest) {
		return sliceRetrieval{
			Started:   started,
			Responded: responded,
			Ended:     time.Now(),
			Status:    res.StatusCode,
			Error:     fmt.Errorf("hash integrity failure: %s Range: bytes=%d-%d for slice: z%s: %w", url.String(), position.Offset, position.Offset+position.Length-1, slice.B58String(), err).Error(),
		}
	}
	return sliceRetrieval{
		Started:   started,
		Responded: responded,
		Ended:     time.Now(),
		Status:    res.StatusCode,
	}
}

func findIndex(ctx context.Context, indexer *client.Client, root model.Link, index model.Link, delegations []delegation.Delegation) (blobindex.ShardedDagIndexView, error) {
	fmt.Printf(">>> # of Delegations: %d\n", len(delegations))

	result, err := indexer.QueryClaims(ctx, types.Query{
		Hashes:      []mh.Multihash{root.Hash()},
		Delegations: delegations,
	})
	if err != nil {
		return nil, fmt.Errorf("querying claims for: %s: %w", root.String(), err)
	}
	if len(result.Indexes()) == 0 {
		return nil, fmt.Errorf("no results for root CID: %s", root)
	}
	if !slices.ContainsFunc(result.Indexes(), func(l ipld.Link) bool {
		return l.String() == index.String()
	}) {
		return nil, fmt.Errorf("index not found in query results: %s", index)
	}
	indexURL, _, err := extractLocation(index.Hash(), result)
	if err != nil {
		return nil, fmt.Errorf("extracting location URL for: z%s from result for root: %s: %w", index.Hash().B58String(), root, err)
	}
	res, err := http.Get(indexURL.String())
	if err != nil {
		return nil, fmt.Errorf("getting index: z%s from URL: %s: %w", index.Hash().B58String(), indexURL.String(), err)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading index: z%s: %w", index.Hash().B58String(), err)
	}
	digest, err := mh.Sum(body, mh.SHA2_256, -1)
	if err != nil {
		return nil, fmt.Errorf("hashing index body: z%s: %w", index.Hash().B58String(), err)
	}
	fmt.Printf(">>> Index body: %s\n", body)
	if !bytes.Equal(digest, index.Hash()) {
		return nil, fmt.Errorf("hash integrity failure: z%s: %w", index.Hash().B58String(), err)
	}
	dagIndex, err := blobindex.Extract(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("extracting index: z%s: %w", index.Hash().B58String(), err)
	}
	return dagIndex, nil
}

func findLocation(ctx context.Context, indexer *client.Client, shard mh.Multihash) (url.URL, ucan.Capability[assert.LocationCaveats], error) {
	shardResult, err := indexer.QueryClaims(ctx, types.Query{
		Hashes: []mh.Multihash{shard},
	})
	if err != nil {
		return url.URL{}, nil, fmt.Errorf("querying claims for: %s: %w", shard.B58String(), err)
	}
	return extractLocation(shard, shardResult)
}

type source struct {
	capability ucan.Capability[any]
	delegation delegation.Delegation
}

func (s source) Capability() ucan.Capability[any] {
	return s.capability
}

func (s source) Delegation() delegation.Delegation {
	return s.delegation
}

// extractLocation inspects indexing service query results to find a location
// commitment for the passed digest. The URL is extracted and returned.
func extractLocation(digest mh.Multihash, result types.QueryResult) (url.URL, ucan.Capability[assert.LocationCaveats], error) {
	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(result.Blocks()))
	if err != nil {
		return url.URL{}, nil, err
	}

	for _, link := range result.Claims() {
		d, err := delegation.NewDelegationView(link, bs)
		if err != nil {
			return url.URL{}, nil, err
		}

		match, err := assert.Location.Match(source{
			capability: d.Capabilities()[0],
			delegation: d,
		})
		if err != nil {
			continue
		}

		cap := match.Value()
		if bytes.Equal(cap.Nb().Content.Hash(), digest) {
			return cap.Nb().Location[0], cap, nil
		}
	}

	return url.URL{}, nil, fmt.Errorf("no location commitment in results for: z%s", digest.B58String())
}

func NewRetrievalTestRunner(region string, indexer *client.Client, uploads eventlog.Iterable[model.Upload], results eventlog.Appender[model.Retrieval]) *RetrievalTestRunner {
	return &RetrievalTestRunner{region, indexer, uploads, results}
}
