package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"slices"
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
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
	"github.com/storacha/network-tester/pkg/util"
)

type AuthorizedRetrievalTestRunner struct {
	region                   string
	id                       ucan.Signer
	indexingServicePrincipal ucan.Principal
	indexer                  *client.Client
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
			shardURL, shardLocationCommitment, err := findLocationWithAuth(ctx, r.id, r.indexingServicePrincipal, r.space, r.indexer, shardDigest, r.proofs)
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
				if retrieval.Error != "" {
					log.Error(retrieval.Error)
				}

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
	blocks := map[ipld.Link]ipld.Block{}
	for b, err := range result.Blocks() {
		if err != nil {
			return nil, err
		}
		blocks[b.Link()] = b
	}
	return blobindex.View(index.Link, blocks)
	// indexURL, _, err := extractLocation(index.Hash(), result)
	// if err != nil {
	// 	return nil, fmt.Errorf("extracting location URL for: z%s from result for root: %s: %w", index.Hash().B58String(), root, err)
	// }
	// res, err := http.Get(indexURL.String())
	// if err != nil {
	// 	return nil, fmt.Errorf("getting index: z%s from URL: %s: %w", index.Hash().B58String(), indexURL.String(), err)
	// }
	// body, err := io.ReadAll(res.Body)
	// if err != nil {
	// 	return nil, fmt.Errorf("reading index: z%s: %w", index.Hash().B58String(), err)
	// }
	// digest, err := mh.Sum(body, mh.SHA2_256, -1)
	// if err != nil {
	// 	return nil, fmt.Errorf("hashing index body: z%s: %w", index.Hash().B58String(), err)
	// }
	// if !bytes.Equal(digest, index.Hash()) {
	// 	return nil, fmt.Errorf("hash integrity failure: z%s: z%s", index.Hash().B58String(), digest)
	// }
	// dagIndex, err := blobindex.Extract(bytes.NewReader(body))
	// if err != nil {
	// 	return nil, fmt.Errorf("extracting index: z%s: %w", index.Hash().B58String(), err)
	// }
	// return dagIndex, nil
}

func findLocationWithAuth(
	ctx context.Context,
	id ucan.Signer,
	indexingServicePrincipal ucan.Principal,
	space did.DID,
	indexer *client.Client,
	shard mh.Multihash,
	proofs delegation.Proofs,
) (url.URL, delegation.Delegation, error) {
	dlg, err := delegation.Delegate(
		id,
		indexingServicePrincipal,
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability(content.Retrieve.Can(), space.DID().String(), ucan.NoCaveats{}),
		},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return url.URL{}, nil, fmt.Errorf("creating retrieval delegation to indexing service: %w", err)
	}
	shardResult, err := indexer.QueryClaims(ctx, types.Query{
		Hashes: []mh.Multihash{shard},
		Match: types.Match{
			Subject: []did.DID{space},
		},
		Delegations: []delegation.Delegation{dlg},
	})
	if err != nil {
		return url.URL{}, nil, fmt.Errorf("querying claims for: %s: %w", shard.B58String(), err)
	}
	return extractLocation(shard, shardResult)
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
		delegation.WithProof(proofs...),
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
		ret.Error = fmt.Errorf("hash integrity failure: %s: z%s", errDesc, dataDigest.B58String()).Error()
		return ret
	}
	return ret
}

func NewAuthorizedRetrievalTestRunner(region string, id ucan.Signer, indexingServicePrincipal ucan.Principal, indexer *client.Client, proof delegation.Delegation, uploads eventlog.Iterable[model.Upload], results eventlog.Appender[model.Retrieval]) (*AuthorizedRetrievalTestRunner, error) {
	space, err := ResourceFromDelegation(proof)
	if err != nil {
		return nil, err
	}
	proofs := []delegation.Proof{delegation.FromDelegation(proof)}
	return &AuthorizedRetrievalTestRunner{region, id, indexingServicePrincipal, indexer, space, proofs, uploads, results}, nil
}
