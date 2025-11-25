package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
	guppyclient "github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/locator"
	grc "github.com/storacha/guppy/pkg/receipt"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
)

var retrievalLog = logging.Logger("retrieval-runner")

type sliceRetrieval struct {
	Started   time.Time
	Responded time.Time
	Ended     time.Time
	Status    int
	Error     string
}

type RetrievalTestRunner struct {
	region                   string
	id                       principal.Signer
	indexingServicePrincipal ucan.Principal
	indexer                  *client.Client
	receipts                 *grc.Client
	guppy                    *guppyclient.Client
	space                    did.DID
	proofs                   delegation.Proofs
	dataDir                  string
}

func (r *RetrievalTestRunner) Run(ctx context.Context) error {
	uploadsFile, err := os.Open(path.Join(r.dataDir, "uploads.csv"))
	if err != nil {
		return err
	}
	uploads := eventlog.NewCSVReader[model.Upload](uploadsFile)
	defer uploadsFile.Close()

	retrievalsFile, err := os.Create(path.Join(r.dataDir, "retrievals.csv"))
	if err != nil {
		return err
	}
	results := eventlog.NewCSVWriter[model.Retrieval](retrievalsFile)
	defer results.Flush()
	defer retrievalsFile.Close()

	retrievalLog.Info("Region")
	retrievalLog.Infof("  %s", r.region)

loop:
	for u, err := range uploads.Iterator() {
		if err != nil {
			return err
		}
		if u.Error.Error() != "" {
			retrievalLog.Infof("Skipping failed upload: %s", u.ID)
			continue
		}
		retrievalLog.Info("Upload")
		retrievalLog.Infof("  %s", u.ID)
		retrievalLog.Infof("    root: %s", u.Root.String())
		retrievalLog.Infof("    source: %s", u.Source)
		retrievalLog.Infof("    index: %s", u.Index.String())
		retrievalLog.Infof("    shards:")
		for _, s := range u.Shards {
			retrievalLog.Infof("      %s", s)
		}
		retrievalLog.Infof("    started: %s", u.Started.Format(time.DateTime))
		retrievalLog.Infof("    ended: %s", u.Ended.Format(time.DateTime))

		index, err := findIndexWithAuth(ctx, r.id, r.indexingServicePrincipal, r.space, r.indexer, u.Root, u.Index, r.proofs)
		if err != nil {
			retrievalLog.Infof("    error: %s", err.Error())
			err = results.Append(model.Retrieval{
				ID:     uuid.New(),
				Region: r.region,
				Source: u.Source,
				Upload: u.ID,
				Error:  model.Error{Message: err.Error()},
			})
			if err != nil {
				return err
			}
			if err := results.Flush(); err != nil {
				return fmt.Errorf("flushing CSV data: %w", err)
			}
			continue
		}

		retrievalLog.Info("Index")
		retrievalLog.Infof("  %s", u.Index)

		for shardDigest, slices := range index.Shards().Iterator() {
			shardLocationCommitment, err := findLocationWithAuth(ctx, r.id, r.indexingServicePrincipal, r.space, r.indexer, shardDigest, r.proofs)
			if err != nil {
				retrievalLog.Infof("    error: %s", err.Error())
				err = results.Append(model.Retrieval{
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
				if err := results.Flush(); err != nil {
					return fmt.Errorf("flushing CSV data: %w", err)
				}
				continue loop
			}

			var urls []string
			for _, url := range shardLocationCommitment.Nb().Location {
				urls = append(urls, url.String())
			}

			retrievalLog.Info("Shard")
			retrievalLog.Infof("  %s", digestutil.Format(shardDigest))
			retrievalLog.Infof("    urls: %s", strings.Join(urls, ", "))
			retrievalLog.Info("    slices:")

			nodeID, err := did.Parse(shardLocationCommitment.With())
			if err != nil {
				retrievalLog.Infof("    error: %s", err.Error())
				err = fmt.Errorf("parsing node DID from location commitment: %w", err)
				err = results.Append(model.Retrieval{
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
				if err := results.Flush(); err != nil {
					return fmt.Errorf("flushing CSV data: %w", err)
				}
				continue loop
			}

			for sliceDigest, position := range slices.Iterator() {
				retrieval := sliceRetrieval{}
				retrieval.Started = time.Now()

				_, err := r.guppy.Retrieve(ctx, r.space, locator.Location{
					Commitment: shardLocationCommitment,
					Position:   position,
					Digest:     sliceDigest,
				})
				if err != nil {
					retrievalLog.Infof("    error: %s", err.Error())
					errDesc := fmt.Sprintf("node: %s, urls: %s, range: bytes=%d-%d, slice: %s", nodeID.DID().String(), strings.Join(urls, ", "), position.Offset, position.Offset+position.Length-1, digestutil.Format(sliceDigest))
					retrieval.Error = fmt.Errorf("executing authorized retrieval: %s: %w", errDesc, err).Error()
				}

				retrieval.Ended = time.Now()

				retrievalLog.Infof("      %s @ %d-%d", digestutil.Format(sliceDigest), position.Offset, position.Offset+position.Length-1)

				err = results.Append(model.Retrieval{
					ID:      uuid.New(),
					Region:  r.region,
					Source:  u.Source,
					Upload:  u.ID,
					Node:    model.DID{DID: nodeID},
					Shard:   model.Multihash{Multihash: shardDigest},
					Slice:   model.Multihash{Multihash: sliceDigest},
					Size:    int(position.Length),
					Started: retrieval.Started,
					// These are currently not visible from outside the Guppy client
					// Responded: retrieval.Responded,
					// Status:    retrieval.Status,
					Ended: retrieval.Ended,
					Error: model.Error{Message: retrieval.Error},
				})
				if err != nil {
					return err
				}
				if err := results.Flush(); err != nil {
					return fmt.Errorf("flushing CSV data: %w", err)
				}
			}
		}

		retrievalLog.Infof("%s complete", u.ID)
	}

	return nil
}

func findIndexWithAuth(
	ctx context.Context,
	id ucan.Signer,
	indexingServicePrincipal ucan.Principal,
	space did.DID,
	indexer *client.Client,
	root model.Link,
	index model.Link,
	proofs delegation.Proofs,
) (blobindex.ShardedDagIndexView, error) {
	dlg, err := delegation.Delegate(
		id,
		indexingServicePrincipal,
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability(content.Retrieve.Can(), space.DID().String(), ucan.NoCaveats{}),
		},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating retrieval delegation to indexing service: %s: %w", root.String(), err)
	}
	result, err := indexer.QueryClaims(ctx, types.Query{
		Hashes: []mh.Multihash{root.Hash()},
		Match: types.Match{
			Subject: []did.DID{space},
		},
		Delegations: []delegation.Delegation{dlg},
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
	for b, err := range result.Blocks() {
		if err != nil {
			return nil, err
		}
		if b.Link().String() == index.String() {
			return blobindex.Extract(bytes.NewReader(b.Bytes()))
		}
	}
	return nil, fmt.Errorf("index not found")
}

func findLocationWithAuth(
	ctx context.Context,
	id ucan.Signer,
	indexingServicePrincipal ucan.Principal,
	space did.DID,
	indexer *client.Client,
	shard mh.Multihash,
	proofs delegation.Proofs,
) (ucan.Capability[assert.LocationCaveats], error) {
	dlg, err := content.Retrieve.Delegate(
		id,
		indexingServicePrincipal,
		space.DID().String(),
		content.RetrieveCaveats{},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating retrieval delegation to indexing service: %w", err)
	}

	shardResult, err := indexer.QueryClaims(ctx, types.Query{
		Hashes: []mh.Multihash{shard},
		Match: types.Match{
			Subject: []did.DID{space},
		},
		Delegations: []delegation.Delegation{dlg},
	})
	if err != nil {
		return nil, fmt.Errorf("querying claims for: %s: %w", shard.B58String(), err)
	}

	_, shardLocationCommitment, err := extractLocation(shard, shardResult)
	if err != nil {
		return nil, fmt.Errorf("extracting location for shard: z%s: %w", shard.B58String(), err)
	}
	return shardLocationCommitment, nil
}

func NewRetrievalTestRunner(
	region string,
	id principal.Signer,
	indexingServicePrincipal ucan.Principal,
	indexer *client.Client,
	guppy *guppyclient.Client,
	receipts *grc.Client,
	proof delegation.Delegation,
	dataDir string,
) (*RetrievalTestRunner, error) {
	space, err := ResourceFromDelegation(proof)
	if err != nil {
		return nil, err
	}
	proofs := []delegation.Proof{delegation.FromDelegation(proof)}
	return &RetrievalTestRunner{region, id, indexingServicePrincipal, indexer, receipts, guppy, space, proofs, dataDir}, nil
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

		match, err := assert.Location.Match(validator.NewSource(d.Capabilities()[0], d))
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

// ResourceFromDelegation extracts the capability resource (with) from a
// delegation and parses it as a DID. It requires the delegation's capabilities
// to all target the same resource and that the resource URI is a DID.
func ResourceFromDelegation(d delegation.Delegation) (did.DID, error) {
	if len(d.Capabilities()) == 0 {
		return did.Undef, errors.New("delegation has no capabilities")
	}
	var resource string
	for _, cap := range d.Capabilities() {
		if resource == "" {
			resource = cap.With()
			continue
		}
		if resource != cap.With() {
			return did.Undef, errors.New("delegation targets multiple resources")
		}
	}
	return did.Parse(resource)
}
