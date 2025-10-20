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
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-libstoracha/capabilities/space/content"
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

type AuthorizedRetrievalTestRunner struct {
	region  string
	id      ucan.Signer
	indexer *client.Client
	space   did.DID
	proofs  delegation.Proofs
	uploads eventlog.Iterable[model.Upload]
	results eventlog.Appender[model.Retrieval]
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

		index, err := findIndex(ctx, r.indexer, u.Root, u.Index)
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

			nodeID := shardLocationCommitment.Issuer()
			for sliceDigest, position := range slices.Iterator() {
				retrieval := testAuthorizedRetrieveSlice(ctx, r.id, nodeID, r.space, r.proofs, shardDigest, sliceDigest, shardURL, position)
				log.Infof("      z%s @ %d-%d", sliceDigest.B58String(), position.Offset, position.Offset+position.Length-1)

				err = r.results.Append(model.Retrieval{
					ID:        uuid.New(),
					Region:    r.region,
					Source:    u.Source,
					Upload:    u.ID,
					Node:      model.DID{DID: shardLocationCommitment.Issuer().DID()},
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

func testAuthorizedRetrieveSlice(
	ctx context.Context,
	id ucan.Signer,
	nodeID ucan.Principal,
	space did.DID,
	proofs delegation.Proofs,
	shard mh.Multihash,
	slice mh.Multihash,
	url url.URL,
	position blobindex.Position,
) sliceRetrieval {
	errDesc := fmt.Sprintf("node: %s, url: %s, range: bytes=%d-%d, slice: z%s", nodeID.DID().String(), url.String(), position.Offset, position.Offset+position.Length-1, slice.B58String())
	ret := sliceRetrieval{}
	ret.Started = time.Now()
	defer func() {
		ret.Ended = time.Now()
	}()

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
					Start: position.Offset,
					End:   position.Offset + position.Length - 1,
				},
			},
		),
	)
	if err != nil {
		ret.Error = fmt.Errorf("creating invocation: %s: %w", errDesc, err).Error()
		return ret
	}
	conn, err := rclient.NewConnection(nodeID, &url)
	if err != nil {
		ret.Error = fmt.Errorf("creating connection: %s, %w", errDesc, err).Error()
		return ret
	}
	xres, hres, err := rclient.Execute(ctx, inv, conn)
	ret.Responded = time.Now()
	if err != nil {
		ret.Error = fmt.Errorf("executing space/content/retrieve invocation: %s: %w", errDesc, err).Error()
		return ret
	}
	ret.Status = hres.Status()

	rcptLink, ok := xres.Get(inv.Link())
	if !ok {
		ret.Error = fmt.Errorf("execution response did not contain receipt for invocation: %s", errDesc).Error()
		return ret
	}

	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(xres.Blocks()))
	if err != nil {
		ret.Error = fmt.Errorf("adding blocks to reader: %s: %w", errDesc, err).Error()
		return ret
	}

	rcpt, err := receipt.NewAnyReceipt(rcptLink, bs)
	if err != nil {
		ret.Error = fmt.Errorf("adding blocks to reader: %s: %w", errDesc, err).Error()
		return ret
	}

	o, x := result.Unwrap(rcpt.Out())
	if x != nil {
		fail, err := util.BindFailure(x)
		if err != nil {
			ret.Error = fmt.Errorf("binding retrieve failure: %s: %w", errDesc, err).Error()
			return ret
		}
		name := "Unnamed"
		if fail.Name != nil {
			name = *fail.Name
		}
		ret.Error = fmt.Errorf("execution failure: %s: %s: %s", errDesc, name, fail.Message).Error()
		return ret
	}

	_, err = ipld.Rebind[content.RetrieveOk](o, content.RetrieveOkType())
	if err != nil {
		ret.Error = fmt.Errorf("rebinding retrieve ok: %s: %w", errDesc, err).Error()
		return ret
	}

	body := hres.Body()
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		ret.Error = fmt.Errorf("reading response body: %s: %w", errDesc, err).Error()
		return ret
	}
	dataDigest, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		ret.Error = fmt.Errorf("hashing data: %s: %w", errDesc, err).Error()
		return ret
	}
	if !bytes.Equal(slice, dataDigest) {
		ret.Error = fmt.Errorf("hash integirty failure: %s: %w", errDesc, err).Error()
		return ret
	}
	return ret
}

func NewAuthorizedRetrievalTestRunner(region string, id ucan.Signer, indexer *client.Client, proof delegation.Delegation, uploads eventlog.Iterable[model.Upload], results eventlog.Appender[model.Retrieval]) (*AuthorizedRetrievalTestRunner, error) {
	space, err := ResourceFromDelegation(proof)
	if err != nil {
		return nil, err
	}
	proofs := []delegation.Proof{delegation.FromDelegation(proof)}
	return &AuthorizedRetrievalTestRunner{region, id, indexer, space, proofs, uploads, results}, nil
}
