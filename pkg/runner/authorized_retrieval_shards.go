package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/ucan"
	guppyclient "github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/client/locator"
	grc "github.com/storacha/guppy/pkg/receipt"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
)

type AuthorizedRetrievalShardsTestRunner struct {
	region                   string
	id                       principal.Signer
	indexingServicePrincipal ucan.Principal
	indexer                  *client.Client
	receipts                 *grc.Client
	space                    did.DID
	proofs                   delegation.Proofs
	shards                   eventlog.Iterable[model.Shard]
	results                  eventlog.Appender[model.Retrieval]
}

func (r *AuthorizedRetrievalShardsTestRunner) Run(ctx context.Context) error {
	log.Info("Region")
	log.Infof("  %s", r.region)

	for s, err := range r.shards.Iterator() {
		if err != nil {
			return fmt.Errorf("reading shard from input: %w", err)
		}
		if s.Error.Error() != "" {
			log.Infof("Skipping failed shard: %s", s.ID)
			continue
		}
		log.Info("Shard")
		log.Infof("  %s (%s)", s.ID, digestutil.Format(s.ID.Hash()))
		log.Infof("    url: %s", s.URL.String())

		shardDigest := s.ID.Hash()
		shardLocationCommitment, err := findLocationWithAuth(ctx, r.id, r.indexingServicePrincipal, r.space, r.indexer, shardDigest, r.proofs)

		if err != nil {
			err = r.results.Append(model.Retrieval{
				ID:     uuid.New(),
				Region: r.region,
				Source: s.Source,
				Upload: s.Upload,
				Shard:  model.Multihash{Multihash: shardDigest},
				Error:  model.Error{Message: err.Error()},
			})
			if err != nil {
				return fmt.Errorf("appending retrieval result for failed shard location find: %w", err)
			}
			continue
		}

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

		retrieval := sliceRetrieval{}
		retrieval.Started = time.Now()

		_, err = guppyClient.Retrieve(ctx, r.space, locator.Location{
			Commitment: shardLocationCommitment,
			Position: blobindex.Position{
				Offset: 0,
				Length: uint64(s.Size),
			},
			Digest: shardLocationCommitment.Nb().Content.Hash(),
		})
		if err != nil {
			retrieval.Error = fmt.Errorf("executing authorized retrieval: %w", err).Error()
		}

		retrieval.Ended = time.Now()

		log.Infof("      %s @ 0-%d", digestutil.Format(shardDigest), s.Size-1)

		err = r.results.Append(model.Retrieval{
			ID:        uuid.New(),
			Region:    r.region,
			Source:    s.Source,
			Upload:    s.Upload,
			Node:      model.DID{DID: nodeID},
			Shard:     model.Multihash{Multihash: shardDigest},
			Slice:     model.Multihash{Multihash: shardDigest},
			Size:      s.Size,
			Started:   retrieval.Started,
			Responded: retrieval.Responded,
			Ended:     retrieval.Ended,
			Status:    retrieval.Status,
			Error:     model.Error{Message: retrieval.Error},
		})
		if err != nil {
			return err
		}

		log.Infof("%s passed", digestutil.Format(shardDigest))
	}

	return nil
}

func NewAuthorizedRetrievalShardsTestRunner(
	region string,
	id principal.Signer,
	indexingServicePrincipal ucan.Principal,
	indexer *client.Client,
	receipts *grc.Client,
	proof delegation.Delegation,
	shards eventlog.Iterable[model.Shard],
	results eventlog.Appender[model.Retrieval],
) (*AuthorizedRetrievalShardsTestRunner, error) {
	space, err := ResourceFromDelegation(proof)
	if err != nil {
		return nil, err
	}
	proofs := []delegation.Proof{delegation.FromDelegation(proof)}
	return &AuthorizedRetrievalShardsTestRunner{region, id, indexingServicePrincipal, indexer, receipts, space, proofs, shards, results}, nil
}
