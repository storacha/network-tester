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

		result, index, err := func() (types.QueryResult, blobindex.ShardedDagIndexView, error) {
			result, err := r.indexer.QueryClaims(ctx, types.Query{
				Hashes: []mh.Multihash{u.Root.Hash()},
			})
			if err != nil {
				return nil, nil, fmt.Errorf("querying claims for: %s: %w", u.Root.String(), err)
			}
			if len(result.Indexes()) == 0 {
				return nil, nil, fmt.Errorf("no results for root CID: %s", u.Root)
			}
			if !slices.ContainsFunc(result.Indexes(), func(l ipld.Link) bool {
				return l.String() == u.Index.String()
			}) {
				return nil, nil, fmt.Errorf("index not found in query results: %s", u.Index)
			}
			indexURL, _, err := extractLocation(u.Index.Hash(), result)
			if err != nil {
				return nil, nil, fmt.Errorf("extracting location URL for: z%s from result for root: %s: %w", u.Index.Hash().B58String(), u.Root, err)
			}
			res, err := http.Get(indexURL.String())
			if err != nil {
				return nil, nil, fmt.Errorf("getting index: z%s from URL: %s: %w", u.Index.Hash().B58String(), indexURL.String(), err)
			}
			body, err := io.ReadAll(res.Body)
			if err != nil {
				return nil, nil, fmt.Errorf("reading index: z%s: %w", u.Index.Hash().B58String(), err)
			}
			digest, err := mh.Sum(body, mh.SHA2_256, -1)
			if err != nil {
				return nil, nil, fmt.Errorf("hashing index body: z%s: %w", u.Index.Hash().B58String(), err)
			}
			if !bytes.Equal(digest, u.Index.Hash()) {
				return nil, nil, fmt.Errorf("hash integrity failure: z%s: %w", u.Index.Hash().B58String(), err)
			}
			index, err := blobindex.Extract(bytes.NewReader(body))
			if err != nil {
				return nil, nil, fmt.Errorf("extracting index: z%s: %w", u.Index.Hash().B58String(), err)
			}
			return result, index, nil
		}()
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
			shardURL, shardLocationCommitment, err := func() (url.URL, delegation.Delegation, error) {
				var surl url.URL
				var lcomm delegation.Delegation
				// only the shard that contains the root will get its location commitment included in the result,
				// for other shards we will need to query the indexing-service again for that shard
				if slices.Has(u.Root.Hash()) {
					surl, lcomm, err = extractLocation(shardDigest, result)
					if err != nil {
						return url.URL{}, nil, fmt.Errorf("extracting location URL for: z%s from result for root: z%s: %w", shardDigest.B58String(), u.Root, err)
					}
				} else {
					shardResult, err := r.indexer.QueryClaims(ctx, types.Query{
						Hashes: []mh.Multihash{shardDigest},
					})
					if err != nil {
						return url.URL{}, nil, fmt.Errorf("querying claims for: %s: %w", shardDigest.B58String(), err)
					}

					surl, lcomm, err = extractLocation(shardDigest, shardResult)
					if err != nil {
						return url.URL{}, nil, fmt.Errorf("extracting location URL from result for shard: z%s", shardDigest.B58String())
					}
				}
				return surl, lcomm, nil
			}()
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

				err = r.results.Append(model.Retrieval{
					ID:      uuid.New(),
					Region:  r.region,
					Source:  u.Source,
					Upload:  u.ID,
					Node:    model.DID{DID: shardLocationCommitment.Issuer().DID()},
					Shard:   model.Multihash{Multihash: shardDigest},
					Slice:   model.Multihash{Multihash: sliceDigest},
					Size:    int(position.Length),
					Started: retrieval.Started,
					Ended:   retrieval.Ended,
					Status:  retrieval.Status,
					Error:   model.Error{Message: retrieval.Error},
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
	Started time.Time
	Ended   time.Time
	Status  int
	Error   string
}

func testRetrieveSlice(slice mh.Multihash, url url.URL, position blobindex.Position) sliceRetrieval {
	start := time.Now()
	req, err := http.NewRequest(http.MethodGet, url.String(), nil)
	if err != nil {
		return sliceRetrieval{
			Started: start,
			Ended:   time.Now(),
			Error:   fmt.Errorf("creating slice request to: %s for slice: z%s: %w", url.String(), slice.B58String(), err).Error(),
		}
	}
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", position.Offset, position.Offset+position.Length-1))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return sliceRetrieval{
			Started: start,
			Ended:   time.Now(),
			Status:  res.StatusCode,
			Error:   fmt.Errorf("sending slice request to: %s Range: bytes=%d-%d for slice: z%s: %w", url.String(), position.Offset, position.Offset+position.Length-1, slice.B58String(), err).Error(),
		}
	}
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return sliceRetrieval{
			Started: start,
			Ended:   time.Now(),
			Status:  res.StatusCode,
			Error:   fmt.Errorf("reading slice response: %s Range: bytes=%d-%d for slice: z%s: %w", url.String(), position.Offset, position.Offset+position.Length-1, slice.B58String(), err).Error(),
		}
	}
	dataDigest, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		return sliceRetrieval{
			Started: start,
			Ended:   time.Now(),
			Status:  res.StatusCode,
			Error:   fmt.Errorf("hashing slice response: %s Range: bytes=%d-%d for slice: z%s: %w", url.String(), position.Offset, position.Offset+position.Length-1, slice.B58String(), err).Error(),
		}
	}
	if !bytes.Equal(slice, dataDigest) {
		return sliceRetrieval{
			Started: start,
			Ended:   time.Now(),
			Status:  res.StatusCode,
			Error:   fmt.Errorf("hash integrity failure: %s Range: bytes=%d-%d for slice: z%s: %w", url.String(), position.Offset, position.Offset+position.Length-1, slice.B58String(), err).Error(),
		}
	}
	return sliceRetrieval{
		Started: start,
		Ended:   time.Now(),
		Status:  res.StatusCode,
	}
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
func extractLocation(digest mh.Multihash, result types.QueryResult) (url.URL, delegation.Delegation, error) {
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
			return cap.Nb().Location[0], d, nil
		}
	}

	return url.URL{}, nil, fmt.Errorf("no location commitment in results for: z%s", digest.B58String())
}

func NewRetrievalTestRunner(region string, indexer *client.Client, uploads eventlog.Iterable[model.Upload], results eventlog.Appender[model.Retrieval]) *RetrievalTestRunner {
	return &RetrievalTestRunner{region, indexer, uploads, results}
}
