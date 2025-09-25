package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/google/uuid"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/space/content"
	"github.com/storacha/go-libstoracha/digestutil"
	rclient "github.com/storacha/go-ucanto/client/retrieval"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
	"github.com/storacha/network-tester/pkg/util"
)

type AuthorizedRetrievalShardsTestRunner struct {
	region  string
	id      ucan.Signer
	indexer *client.Client
	space   did.DID
	proofs  delegation.Proofs
	shards  eventlog.Iterable[model.Shard]
	results eventlog.Appender[model.Retrieval]
}

func (r *AuthorizedRetrievalShardsTestRunner) Run(ctx context.Context) error {
	log.Info("Region")
	log.Infof("  %s", r.region)

	for s, err := range r.shards.Iterator() {
		if err != nil {
			return err
		}
		if s.Error.Error() != "" {
			log.Infof("Skipping failed shard: %s", s.ID)
			continue
		}
		log.Info("Shard")
		log.Infof("  %s (%s)", s.ID, digestutil.Format(s.ID.Hash()))
		log.Infof("    url: %s", s.URL.String())

		shardDigest := s.ID.Hash()
		shardURL, shardLocationCommitment, err := findLocation(ctx, r.indexer, shardDigest)
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
				return err
			}
			continue
		}

		nodeID := shardLocationCommitment.Issuer()
		retrieval := testAuthorizedRetrieveShard(ctx, r.id, nodeID, r.space, r.proofs, shardDigest, shardURL, uint64(s.Size))
		log.Infof("      %s @ 0-%d", digestutil.Format(shardDigest), s.Size-1)

		err = r.results.Append(model.Retrieval{
			ID:        uuid.New(),
			Region:    r.region,
			Source:    s.Source,
			Upload:    s.Upload,
			Node:      model.DID{DID: shardLocationCommitment.Issuer().DID()},
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

func testAuthorizedRetrieveShard(
	ctx context.Context,
	id ucan.Signer,
	nodeID ucan.Principal,
	space did.DID,
	proofs delegation.Proofs,
	shard mh.Multihash,
	url url.URL,
	size uint64,
) sliceRetrieval {
	errDesc := fmt.Sprintf("node: %s, url: %s, shard: z%s", nodeID.DID().String(), url.String(), shard.B58String())
	ret := sliceRetrieval{}
	ret.Started = time.Now()

	inv, err := invocation.Invoke(
		id,
		nodeID,
		content.Retrieve.New(
			space.String(),
			content.RetrieveCaveats{
				Blob: content.BlobDigest{
					Digest: shard,
				},
				Range: content.Range{
					Start: 0,
					End:   size - 1,
				},
			},
		),
		delegation.WithProof(proofs...),
	)
	if err != nil {
		ret.Error = fmt.Errorf("creating invocation: %s: %w", errDesc, err).Error()
		ret.Ended = time.Now()
		return ret
	}
	conn, err := rclient.NewConnection(nodeID, &url)
	if err != nil {
		ret.Error = fmt.Errorf("creating connection: %s, %w", errDesc, err).Error()
		ret.Ended = time.Now()
		return ret
	}
	xres, hres, err := rclient.Execute(ctx, inv, conn)
	ret.Responded = time.Now()
	if err != nil {
		ret.Error = fmt.Errorf("executing space/content/retrieve invocation: %s: %w", errDesc, err).Error()
		ret.Ended = time.Now()
		return ret
	}
	ret.Status = hres.Status()

	rcptLink, ok := xres.Get(inv.Link())
	if !ok {
		ret.Error = fmt.Errorf("execution response did not contain receipt for invocation: %s", errDesc).Error()
		ret.Ended = time.Now()
		return ret
	}

	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(xres.Blocks()))
	if err != nil {
		ret.Error = fmt.Errorf("adding blocks to reader: %s: %w", errDesc, err).Error()
		ret.Ended = time.Now()
		return ret
	}

	rcpt, err := receipt.NewAnyReceipt(rcptLink, bs)
	if err != nil {
		ret.Error = fmt.Errorf("adding blocks to reader: %s: %w", errDesc, err).Error()
		ret.Ended = time.Now()
		return ret
	}

	o, x := result.Unwrap(rcpt.Out())
	if x != nil {
		fail, err := util.BindFailure(x)
		if err != nil {
			ret.Error = fmt.Errorf("binding retrieve failure: %s: %w", errDesc, err).Error()
			ret.Ended = time.Now()
			return ret
		}
		name := "Unnamed"
		if fail.Name != nil {
			name = *fail.Name
		}
		ret.Error = fmt.Errorf("execution failure: %s: %s: %s", errDesc, name, fail.Message).Error()
		ret.Ended = time.Now()
		return ret
	}

	_, err = ipld.Rebind[content.RetrieveOk](o, content.RetrieveOkType())
	if err != nil {
		ret.Error = fmt.Errorf("rebinding retrieve ok: %s: %w", errDesc, err).Error()
		ret.Ended = time.Now()
		return ret
	}

	body := hres.Body()
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		ret.Error = fmt.Errorf("reading response body: %s: %w", errDesc, err).Error()
		ret.Ended = time.Now()
		return ret
	}
	dataDigest, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		ret.Error = fmt.Errorf("hashing data: %s: %w", errDesc, err).Error()
		ret.Ended = time.Now()
		return ret
	}
	if !bytes.Equal(shard, dataDigest) {
		ret.Error = fmt.Errorf("hash integirty failure: %s: %w", errDesc, err).Error()
		ret.Ended = time.Now()
		return ret
	}
	return ret
}

type AuthorizedRetrievalShardsTestConfig struct {
	Region  string
	ID      ucan.Signer
	Proof   delegation.Delegation
	Indexer *client.Client
}

func NewAuthorizedRetrievalShardsTestRunner(
	config AuthorizedRetrievalShardsTestConfig,
	shards eventlog.Iterable[model.Shard],
	results eventlog.Appender[model.Retrieval],
) (*AuthorizedRetrievalShardsTestRunner, error) {
	space, err := ResourceFromDelegation(config.Proof)
	if err != nil {
		return nil, err
	}
	proofs := []delegation.Proof{delegation.FromDelegation(config.Proof)}
	return &AuthorizedRetrievalShardsTestRunner{config.Region, config.ID, config.Indexer, space, proofs, shards, results}, nil
}
