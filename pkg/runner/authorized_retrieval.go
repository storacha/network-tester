package runner

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	guppyclient "github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/locator"
	grc "github.com/storacha/guppy/pkg/receipt"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
)

type AuthorizedRetrievalTestRunner struct {
	region                   string
	id                       principal.Signer
	indexingServicePrincipal ucan.Principal
	indexer                  *client.Client
	receipts                 *grc.Client
	space                    did.DID
	proofs                   delegation.Proofs
	uploads                  eventlog.Iterable[model.Upload]
	results                  eventlog.Appender[model.Retrieval]
}

func (r *AuthorizedRetrievalTestRunner) Run(ctx context.Context) error {
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

		index, err := findIndexWithAuth(ctx, r.id, r.indexingServicePrincipal, r.space, r.indexer, u.Root, u.Index, r.proofs)
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
			shardLocationCommitment, err := findLocationWithAuth(ctx, r.id, r.indexingServicePrincipal, r.space, r.indexer, shardDigest, r.proofs)
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

			var urls []string
			for _, url := range shardLocationCommitment.Nb().Location {
				urls = append(urls, url.String())
			}

			log.Info("Shard")
			log.Infof("  z%s", shardDigest.B58String())
			log.Infof("    urls: %s", strings.Join(urls, ", "))
			log.Info("    slices:")

			nodeID, err := did.Parse(shardLocationCommitment.With())
			if err != nil {
				return fmt.Errorf("parsing node DID from location commitment: %w", err)
			}

			// Create Guppy client
			guppyClient, err := guppyclient.NewClient(
				guppyclient.WithConnection(config.UploadServiceConnection),
				guppyclient.WithPrincipal(r.id),
				guppyclient.WithReceiptsClient(r.receipts),
			)
			if err != nil {
				return fmt.Errorf("creating guppy client: %w", err)
			}

			proofDels := make([]delegation.Delegation, 0, len(r.proofs))
			for _, p := range r.proofs {
				d, ok := p.Delegation()
				if ok {
					proofDels = append(proofDels, d)
				}
			}
			guppyClient.AddProofs(proofDels...)

			for sliceDigest, position := range slices.Iterator() {
				retrieval := sliceRetrieval{}
				retrieval.Started = time.Now()
				defer func() {
					retrieval.Ended = time.Now()
				}()

				_, err := guppyClient.Retrieve(ctx, r.space, locator.Location{
					Commitment: shardLocationCommitment,
					Position:   position,
					Digest:     sliceDigest,
				})
				if err != nil {
					errDesc := fmt.Sprintf("node: %s, urls: %s, range: bytes=%d-%d, slice: z%s", nodeID.DID().String(), strings.Join(urls, ", "), position.Offset, position.Offset+position.Length-1, sliceDigest.B58String())
					retrieval.Error = fmt.Errorf("executing authorized retrieval: %s: %w", errDesc, err).Error()
				}

				log.Infof("      z%s @ %d-%d", sliceDigest.B58String(), position.Offset, position.Offset+position.Length-1)
				if retrieval.Error != "" {
					log.Error(retrieval.Error)
				}

				err = r.results.Append(model.Retrieval{
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
					// Ended:     retrieval.Ended,
					// Status:    retrieval.Status,
					Error: model.Error{Message: retrieval.Error},
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
		delegation.WithExpiration(int(time.Now().Add(30*time.Second).Unix())),
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

func NewAuthorizedRetrievalTestRunner(region string, id principal.Signer, indexingServicePrincipal ucan.Principal, indexer *client.Client, receipts *grc.Client, proof delegation.Delegation, uploads eventlog.Iterable[model.Upload], results eventlog.Appender[model.Retrieval]) (*AuthorizedRetrievalTestRunner, error) {
	space, err := ResourceFromDelegation(proof)
	if err != nil {
		return nil, err
	}
	proofs := []delegation.Proof{delegation.FromDelegation(proof)}
	return &AuthorizedRetrievalTestRunner{region, id, indexingServicePrincipal, indexer, receipts, space, proofs, uploads, results}, nil
}
