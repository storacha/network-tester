package runner

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-libstoracha/capabilities/blob/replica"
	"github.com/storacha/go-libstoracha/capabilities/space/blob"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	grc "github.com/storacha/guppy/pkg/receipt"
	isc "github.com/storacha/indexing-service/pkg/client"
	ist "github.com/storacha/indexing-service/pkg/types"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
	"github.com/storacha/network-tester/pkg/util"
)

type ReplicationTestRunner struct {
	region            string
	space             did.DID
	id                principal.Signer
	proof             delegation.Delegation
	indexer           *isc.Client
	serviceConnection client.Connection
	serviceReceipts   *grc.Client
	replicas          int
	shards            eventlog.Iterable[model.Shard]
	replications      eventlog.Appender[model.Replication]
	transfers         eventlog.Appender[model.ReplicaTransfer]
}

func (r *ReplicationTestRunner) Run(ctx context.Context) error {
	log.Info("Region")
	log.Infof("  %s", r.region)
	log.Info("Agent")
	log.Infof("  %s", r.id.DID())
	log.Info("Space")
	log.Infof("  %s", r.space)
	log.Info("Replicas")
	log.Infof("  %d", r.replicas)

	for s, err := range r.shards.Iterator() {
		if err != nil {
			return err
		}
		if s.Error.Error() != "" {
			log.Infof("Skipping failed shard: %s", s.ID)
			continue
		}
		log.Info("Shard")
		log.Infof("  %s", s.ID)
		log.Infof("    node: %s", s.Node.String())
		log.Infof("    size: %v", s.Size)

		if r.replicas < 2 {
			log.Infof("Already satisfied replica count: %s", s.ID)
			continue
		}

		repl, err := r.requestReplicate(ctx, s)

		log.Info("Replication")
		log.Infof("  %s", repl.id)
		log.Infof("    shard: %s", repl.shard.String())
		if repl.cause != nil {
			log.Infof("    cause: %s", repl.cause.String())
		}
		log.Infof("    replicas: %d", repl.replicas)
		if len(repl.transfers) > 0 {
			log.Info("    transfers:")
			for _, s := range repl.transfers {
				log.Infof("      %s", s.String())
			}
		}
		log.Infof("    requested: %s", repl.requested.Format(time.DateTime))

		if err != nil {
			log.Infof("    error: %s", err.Error())
			err = r.replications.Append(repl.ToModel(err))
			if err != nil {
				return err
			}
			continue
		}
		err = r.replications.Append(repl.ToModel(nil))
		if err != nil {
			return err
		}

		var wg sync.WaitGroup
		for _, task := range repl.transfers {
			wg.Add(1)
			go func() {
				transfer, err := r.waitForTransfer(ctx, repl.id, task)

				log.Info("Transfer")
				log.Infof("  %s", task.String())
				if transfer.node != did.Undef {
					log.Infof("    node: %s", transfer.node.String())
				}
				if transfer.url != nil {
					log.Infof("    url: %s", transfer.url.String())
				}
				log.Infof("    elapsed: %s", transfer.ended.Sub(transfer.started).String())
				if err != nil {
					log.Infof("    error: %s", err.Error())
				}
				r.transfers.Append(transfer.ToModel(err))
				wg.Done()
			}()
		}
		wg.Wait()

		log.Infof("%s replicated", s.ID)
	}

	return nil
}

type replication struct {
	id        uuid.UUID
	region    string
	shard     ipld.Link
	cause     ipld.Link
	replicas  int
	transfers []ipld.Link
	requested time.Time
}

func (r replication) ToModel(err error) model.Replication {
	m := model.Replication{
		ID:        r.id,
		Region:    r.region,
		Shard:     model.ToLink(r.shard),
		Cause:     model.ToLink(r.cause),
		Replicas:  r.replicas,
		Transfers: model.ToLinkList(r.transfers),
		Requested: r.requested,
	}
	if err != nil {
		m.Error = model.Error{Message: err.Error()}
	}
	return m
}

func (r *ReplicationTestRunner) requestReplicate(ctx context.Context, shard model.Shard) (replication, error) {
	repl := replication{id: uuid.New(), region: r.region, shard: shard.ID.Link}

	// First, obtain the location commitment we need in the replicate invocation.
	//
	// We have it's CID already so we know what we're looking for. It was provided
	// to the client when the upload test occurred but we don't save it anywhere.
	//
	// So, we need to retrieve it from the storage node that issued it. The
	// indexing service should be able to provide it, either because it has it
	// cached, or by retrieving it again from the storage node.
	lcomms, err := FindLocations(ctx, r.indexer, r.space, shard.ID.Cid.Hash())
	if err != nil {
		return repl, err
	}
	var lcomm delegation.Delegation
	for _, lc := range lcomms {
		if lc.Link().String() == shard.LocationCommitment.Link.String() {
			lcomm = lc
			break
		}
	}
	if lcomm == nil {
		return repl, fmt.Errorf("missing location commitment %s from node: %s", shard.LocationCommitment.Link.String(), shard.Node.String())
	}

	inv, err := blob.Replicate.Invoke(
		r.id,
		r.serviceConnection.ID(),
		r.space.String(),
		blob.ReplicateCaveats{
			Blob: types.Blob{
				Digest: shard.ID.Cid.Hash(),
				Size:   uint64(shard.Size),
			},
			Replicas: uint(r.replicas),
			Site:     lcomm.Link(),
		},
		delegation.WithProof(delegation.FromDelegation(r.proof)),
	)
	if err != nil {
		return repl, err
	}
	for b, err := range lcomm.Export() {
		if err != nil {
			return repl, err
		}
		if err = inv.Attach(b); err != nil {
			return repl, fmt.Errorf("attaching location commitment to replcate invocation: %w", err)
		}
	}

	repl.cause = inv.Link()
	repl.replicas = r.replicas
	repl.requested = time.Now()

	res, err := client.Execute(ctx, []invocation.Invocation{inv}, r.serviceConnection)
	if err != nil {
		return repl, err
	}
	rcptLink, ok := res.Get(inv.Link())
	if !ok {
		return repl, fmt.Errorf("missing receipt for invocation: %s", inv.Link())
	}
	rcptReader, err := receipt.NewReceiptReaderFromTypes[blob.ReplicateOk, ipld.Node](blob.ReplicateOkType(), blob.ReplicateOkType().TypeSystem().TypeByName("Any"), types.Converters...)
	if err != nil {
		return repl, err
	}
	rcpt, err := rcptReader.Read(rcptLink, res.Blocks())
	if err != nil {
		return repl, fmt.Errorf("reading receipt: %w", err)
	}
	_, x := result.Unwrap(rcpt.Out())
	if x != nil {
		f, err := util.BindFailure(x)
		if err != nil {
			return repl, err
		}
		return repl, fmt.Errorf("invocation failure: %+v", f)
	}

	var transfers []ipld.Link
	for _, f := range rcpt.Fx().Fork() {
		inv, ok := f.Invocation()
		if !ok {
			continue
		}
		if len(inv.Capabilities()) == 0 {
			continue
		}
		cap := inv.Capabilities()[0]
		if cap.Can() == replica.TransferAbility {
			transfers = append(transfers, inv.Link())
		}
	}
	repl.transfers = transfers

	return repl, nil
}

type replicaTransfer struct {
	id                 ipld.Link
	replication        uuid.UUID
	locationCommitment ipld.Link
	node               did.DID
	url                *url.URL
	started            time.Time
	ended              time.Time
}

func (t replicaTransfer) ToModel(err error) model.ReplicaTransfer {
	m := model.ReplicaTransfer{
		ID:                 model.ToLink(t.id),
		Replication:        t.replication,
		LocationCommitment: model.ToLink(t.locationCommitment),
		Node:               model.DID{DID: t.node},
		URL:                model.URL(*t.url),
		Started:            t.started,
		Ended:              t.ended,
	}
	if t.url != nil {
		m.URL = model.URL(*t.url)
	}
	if err != nil {
		m.Error = model.Error{Message: err.Error()}
	}
	return m
}

func (r *ReplicationTestRunner) waitForTransfer(ctx context.Context, replID uuid.UUID, task ipld.Link) (replicaTransfer, error) {
	transfer := replicaTransfer{
		id:          task,
		replication: replID,
		started:     time.Now(),
	}

	// spend around 5 mins waiting for the receipt
	// the largest shard is 256mb so it should not really take that long
	rcpt, err := r.serviceReceipts.Poll(ctx, task, grc.WithInterval(5*time.Second), grc.WithRetries(60))
	transfer.ended = time.Now()
	if err != nil {
		return transfer, err
	}

	o, x := result.Unwrap(rcpt.Out())
	if x != nil {
		f, err := util.BindFailure(x)
		if err != nil {
			return transfer, err
		}
		return transfer, fmt.Errorf("invocation failure: %+v", f)
	}

	transferOk, err := ipld.Rebind[replica.TransferOk](o, replica.TransferOkType(), types.Converters...)
	if err != nil {
		return transfer, fmt.Errorf("rebinding receipt for transfer: %w", err)
	}
	transfer.locationCommitment = transferOk.Site

	br, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(rcpt.Blocks()))
	if err != nil {
		return transfer, fmt.Errorf("iterating receipt blocks: %w", err)
	}

	lcomm, err := delegation.NewDelegationView(transferOk.Site, br)
	if err != nil {
		return transfer, fmt.Errorf("creating location commitment: %w", err)
	}
	transfer.node = lcomm.Issuer().DID()

	if len(lcomm.Capabilities()) == 0 {
		return transfer, errors.New("missing capabilities in location commitment")
	}

	nb, err := assert.LocationCaveatsReader.Read(lcomm.Capabilities()[0].Nb())
	if err != nil {
		return transfer, fmt.Errorf("reading location commitment caveats: %w", err)
	}

	if len(nb.Location) == 0 {
		return transfer, errors.New("missing location URI in location commitment")
	}
	transfer.url = &nb.Location[0]

	return transfer, nil
}

type ReplicationTestConfig struct {
	Region            string
	ID                principal.Signer
	Proof             delegation.Delegation
	Indexer           *isc.Client
	ServiceConnection client.Connection
	ServiceReceipts   *grc.Client
	Replicas          int
}

func NewReplicationTestRunner(config ReplicationTestConfig, shards eventlog.Iterable[model.Shard], replications eventlog.Appender[model.Replication], transfers eventlog.Appender[model.ReplicaTransfer]) (*ReplicationTestRunner, error) {
	space, err := ResourceFromDelegation(config.Proof)
	if err != nil {
		return nil, err
	}
	return &ReplicationTestRunner{
		region:            config.Region,
		space:             space,
		id:                config.ID,
		proof:             config.Proof,
		indexer:           config.Indexer,
		serviceConnection: config.ServiceConnection,
		serviceReceipts:   config.ServiceReceipts,
		replicas:          config.Replicas,
		shards:            shards,
		replications:      replications,
		transfers:         transfers,
	}, nil
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

// FindLocations retrieves location commitments for a given digest in a given
// space from the indexing service. Returns an error if no results are found.
func FindLocations(ctx context.Context, indexer *isc.Client, space did.DID, digest multihash.Multihash) ([]delegation.Delegation, error) {
	r, err := indexer.QueryClaims(ctx, ist.Query{
		Type:   ist.QueryTypeLocation,
		Hashes: []multihash.Multihash{digest},
		Match:  ist.Match{Subject: []did.DID{space}},
	})
	if err != nil {
		return nil, err
	}
	if len(r.Claims()) == 0 {
		return nil, fmt.Errorf("no location commitment results for digest: z%s", digest.B58String())
	}
	blocks, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(r.Blocks()))
	if err != nil {
		return nil, err
	}
	var locations []delegation.Delegation
	for _, root := range r.Claims() {
		d, err := delegation.NewDelegationView(root, blocks)
		if err != nil {
			return nil, fmt.Errorf("failed to decode location commitment for digest: z%s", digest.B58String())
		}
		locations = append(locations, d)
	}
	return locations, nil
}
