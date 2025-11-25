package runner

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
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

var retrievalShardsLog = logging.Logger("retrieval-shards-runner")

type RetrievalShardsTestRunner struct {
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

func (r *RetrievalShardsTestRunner) Run(ctx context.Context) error {
	shardsFile, err := os.Open(path.Join(r.dataDir, "shards.csv"))
	if err != nil {
		return err
	}
	shards := eventlog.NewCSVReader[model.Shard](shardsFile)
	defer shardsFile.Close()

	retrievalsFile, err := os.Create(path.Join(r.dataDir, "retrievals.csv"))
	if err != nil {
		return err
	}
	results := eventlog.NewCSVWriter[model.Retrieval](retrievalsFile)
	defer results.Flush()
	defer retrievalsFile.Close()

	retrievalShardsLog.Info("Region")
	retrievalShardsLog.Infof("  %s", r.region)

	// Create Guppy client
	guppyClient, err := guppyclient.NewClient(
		guppyclient.WithConnection(config.UploadServiceConnection),
		guppyclient.WithPrincipal(r.id),
		guppyclient.WithReceiptsClient(r.receipts),
	)
	if err != nil {
		return fmt.Errorf("creating guppy client: %w", err)
	}

	for s, err := range shards.Iterator() {
		if err != nil {
			return fmt.Errorf("reading shard from input: %w", err)
		}
		if s.Error.Error() != "" {
			retrievalShardsLog.Infof("Skipping failed shard: %s", s.ID)
			continue
		}
		retrievalShardsLog.Info("Shard")
		retrievalShardsLog.Infof("  %s (%s)", s.ID, digestutil.Format(s.ID.Hash()))
		retrievalShardsLog.Infof("    url: %s", s.URL.String())

		shardDigest := s.ID.Hash()
		shardLocationCommitment, err := findLocationWithAuth(ctx, r.id, r.indexingServicePrincipal, r.space, r.indexer, shardDigest, r.proofs)

		if err != nil {
			err = results.Append(model.Retrieval{
				ID:     uuid.New(),
				Region: r.region,
				Source: s.Source,
				Upload: s.Upload,
				Shard:  model.Multihash{Multihash: shardDigest},
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

		nodeID, err := did.Parse(shardLocationCommitment.With())
		if err != nil {
			err = fmt.Errorf("parsing node DID from location commitment: %w", err)
			err = results.Append(model.Retrieval{
				ID:     uuid.New(),
				Region: r.region,
				Source: s.Source,
				Upload: s.Upload,
				Shard:  model.Multihash{Multihash: shardDigest},
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

		retrievalShardsLog.Infof("      %s @ 0-%d", digestutil.Format(shardDigest), s.Size-1)

		err = results.Append(model.Retrieval{
			ID:      uuid.New(),
			Region:  r.region,
			Source:  s.Source,
			Upload:  s.Upload,
			Node:    model.DID{DID: nodeID},
			Shard:   model.Multihash{Multihash: shardDigest},
			Slice:   model.Multihash{Multihash: shardDigest},
			Size:    s.Size,
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

		retrievalShardsLog.Infof("%s passed", digestutil.Format(shardDigest))
	}

	return nil
}

func NewRetrievalShardsTestRunner(
	region string,
	id principal.Signer,
	indexingServicePrincipal ucan.Principal,
	indexer *client.Client,
	guppy *guppyclient.Client,
	receipts *grc.Client,
	proof delegation.Delegation,
	dataDir string,
) (*RetrievalShardsTestRunner, error) {
	space, err := ResourceFromDelegation(proof)
	if err != nil {
		return nil, err
	}
	proofs := []delegation.Proof{delegation.FromDelegation(proof)}
	return &RetrievalShardsTestRunner{region, id, indexingServicePrincipal, indexer, receipts, guppy, space, proofs, dataDir}, nil
}
