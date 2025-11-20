package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-libstoracha/capabilities/blob/replica"
	spaceblob "github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/validator"
	guppy "github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation/storacha"
)

var log = logging.Logger("client")

type ShardInfo struct {
	Space              did.DID
	Link               ipld.Link
	Digest             mh.Multihash
	Size               uint64
	LocationCommitment delegation.Delegation
	NodeID             did.DID // extracted from location commitment
	URL                url.URL // extracted from location commitment
	Error              error
	Started            time.Time
	Ended              time.Time
}

type ReplicationInfo struct {
	Space              did.DID
	Digest             mh.Multihash
	Size               uint64
	Replicas           uint
	LocationCommitment delegation.Delegation
	Transfers          []ipld.Link
	Error              error
	Requested          time.Time
}

// Client wraps a Guppy client and tracks uploaded shards and replications
type Client struct {
	*guppy.Client
	Shards       map[string]*ShardInfo       // key is shard CID
	Replications map[string]*ReplicationInfo // key is blob digest
	Indexes      []cid.Cid
}

var _ storacha.Client = (*Client)(nil)

func New(client *guppy.Client) *Client {
	return &Client{
		Client:       client,
		Shards:       map[string]*ShardInfo{},
		Replications: map[string]*ReplicationInfo{},
	}
}

func (c *Client) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...guppy.SpaceBlobAddOption) (guppy.AddedBlob, error) {
	// Read content to get size for tracking
	data, err := io.ReadAll(content)
	if err != nil {
		return guppy.AddedBlob{}, fmt.Errorf("reading content: %w", err)
	}

	cidV1, err := cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.Car),
		MhType:   uint64(multicodec.Sha2_256),
		MhLength: -1,
	}.Sum(data)
	if err != nil {
		return guppy.AddedBlob{}, fmt.Errorf("hashing data: %w", err)
	}

	shardInfo := ShardInfo{
		Space:   space,
		Link:    cidlink.Link{Cid: cidV1},
		Digest:  cidV1.Hash(),
		Size:    uint64(len(data)),
		Started: time.Now(),
	}
	c.Shards[cidV1.String()] = &shardInfo

	// Use the Guppy client to upload the shard
	addedBlob, err := c.Client.SpaceBlobAdd(ctx, bytes.NewReader(data), space, options...)
	shardInfo.Ended = time.Now()
	if err != nil {
		shardInfo.Error = fmt.Errorf("uploading shard: %w", err)
		return guppy.AddedBlob{}, shardInfo.Error
	}

	shardInfo.LocationCommitment = addedBlob.Location

	match, err := assert.Location.Match(validator.NewSource(
		addedBlob.Location.Capabilities()[0],
		addedBlob.Location,
	))
	if err != nil {
		shardInfo.Error = fmt.Errorf("matching location commitment: %w", err)
		return guppy.AddedBlob{}, shardInfo.Error
	}

	loc := match.Value()
	did, err := did.Parse(loc.With())
	if err != nil {
		shardInfo.Error = fmt.Errorf("parsing node DID: %w", err)
		return guppy.AddedBlob{}, shardInfo.Error
	}
	shardInfo.NodeID = did

	if len(loc.Nb().Location) == 0 {
		shardInfo.Error = errors.New("missing URLs in location commitment")
		return guppy.AddedBlob{}, shardInfo.Error
	}
	if len(loc.Nb().Location) > 1 {
		log.Warnf("multiple locations in location caveats, using the first one")
	}
	shardInfo.URL = loc.Nb().Location[0]

	return addedBlob, nil
}

func (c *Client) SpaceIndexAdd(ctx context.Context, indexCID cid.Cid, indexSize uint64, rootCID cid.Cid, space did.DID) error {
	c.Indexes = append(c.Indexes, indexCID)
	return c.Client.SpaceIndexAdd(ctx, indexCID, indexSize, rootCID, space)
}

func (c *Client) SpaceBlobReplicate(ctx context.Context, space did.DID, blob types.Blob, replicaCount uint, locationCommitment delegation.Delegation) (spaceblob.ReplicateOk, fx.Effects, error) {
	replInfo := ReplicationInfo{
		Space:              space,
		Digest:             blob.Digest,
		Size:               blob.Size,
		Replicas:           replicaCount,
		LocationCommitment: locationCommitment,
		Requested:          time.Now(),
	}
	c.Replications[digestutil.Format(blob.Digest)] = &replInfo

	res, fx, err := c.Client.SpaceBlobReplicate(ctx, space, blob, replicaCount, locationCommitment)
	if err != nil {
		replInfo.Error = err
		return spaceblob.ReplicateOk{}, nil, err
	}

	for _, f := range fx.Fork() {
		inv, ok := f.Invocation()
		if !ok {
			continue
		}
		if len(inv.Capabilities()) == 0 {
			continue
		}
		cap := inv.Capabilities()[0]
		if cap.Can() == replica.TransferAbility {
			replInfo.Transfers = append(replInfo.Transfers, inv.Link())
		}
	}

	return res, fx, nil
}

func (c *Client) ResetTrackedData() {
	c.Shards = map[string]*ShardInfo{}
	c.Replications = map[string]*ReplicationInfo{}
}
