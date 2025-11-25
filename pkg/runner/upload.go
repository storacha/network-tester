package runner

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/spf13/afero"
	"github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-libstoracha/capabilities/blob/replica"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-libstoracha/digestutil"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	guppyclient "github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	grc "github.com/storacha/guppy/pkg/receipt"
	"github.com/storacha/network-tester/pkg/client"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
	"github.com/storacha/network-tester/pkg/util"
)

var uploadLog = logging.Logger("upload-runner")

const (
	minFileSize       = 128
	maxBytes          = 20 * 1024 * 1024 * 1024 // 20 GB
	maxPerUploadBytes = 1 * 1024 * 1024 * 1024  // 1 GB
	maxShardSize      = 133_169_152             // Default SHARD_SIZE
)

type UploadTestRunner struct {
	region   string
	id       principal.Signer
	receipts *grc.Client
	guppy    *guppyclient.Client
	space    did.DID
	proofs   delegation.Proofs
	dataDir  string
}

func (r *UploadTestRunner) Run(ctx context.Context) error {
	// Create data directory
	if err := os.MkdirAll(r.dataDir, 0755); err != nil {
		return fmt.Errorf("creating data directory: %w", err)
	}

	// Create event logs
	sourceLogFile, err := os.Create(filepath.Join(r.dataDir, "sources.csv"))
	if err != nil {
		return fmt.Errorf("creating source log file: %w", err)
	}
	sourceCSV := eventlog.NewCSVWriter[model.Source](sourceLogFile)
	defer sourceCSV.Flush()
	defer sourceLogFile.Close()

	shardLogFile, err := os.Create(filepath.Join(r.dataDir, "shards.csv"))
	if err != nil {
		return fmt.Errorf("creating shard log file: %w", err)
	}
	shardCSV := eventlog.NewCSVWriter[model.Shard](shardLogFile)
	defer shardCSV.Flush()
	defer shardLogFile.Close()

	uploadLogFile, err := os.Create(filepath.Join(r.dataDir, "uploads.csv"))
	if err != nil {
		return fmt.Errorf("creating upload log file: %w", err)
	}
	uploadCSV := eventlog.NewCSVWriter[model.Upload](uploadLogFile)
	defer uploadCSV.Flush()
	defer uploadLogFile.Close()

	replicationLogFile, err := os.Create(filepath.Join(r.dataDir, "replications.csv"))
	if err != nil {
		return fmt.Errorf("creating replication log file: %w", err)
	}
	replicationCSV := eventlog.NewCSVWriter[model.Replication](replicationLogFile)
	defer replicationCSV.Flush()
	defer replicationLogFile.Close()

	transferLogFile, err := os.Create(filepath.Join(r.dataDir, "transfers.csv"))
	if err != nil {
		return fmt.Errorf("creating replica transfer log file: %w", err)
	}
	transferCSV := eventlog.NewCSVWriter[model.ReplicaTransfer](transferLogFile)
	defer transferCSV.Flush()
	defer transferLogFile.Close()

	uploadLog.Infof("Region: %s", r.region)
	uploadLog.Infof("Agent: %s", r.id.DID())
	uploadLog.Infof("Space: %s", r.space)

	dbPath := filepath.Join(r.dataDir, "upload.db")
	var repo *sqlrepo.Repo
	defer func() {
		if repo != nil {
			repo.Close()
		}
	}()

	uploadClient := client.New(r.guppy)

	totalSize := int64(0)
	totalSources := 0

	for totalSize < maxBytes {
		// Create preparation database
		err = os.RemoveAll(dbPath)
		if err != nil {
			return fmt.Errorf("removing existing database: %w", err)
		}
		if repo != nil {
			repo.Close()
		}
		repo, err = preparation.OpenRepo(ctx, dbPath)
		if err != nil {
			return fmt.Errorf("opening repository: %w", err)
		}

		maxSize := maxPerUploadBytes
		if maxBytes-totalSize < int64(maxSize) {
			maxSize = int(maxBytes - totalSize)
		}
		if maxSize < minFileSize {
			break
		}

		// This is a UUID used for this test, not the DB ID the client tracks.
		sourceID := uuid.New()
		sourceType, sourceData, err := generateSource(maxSize)
		if err != nil {
			return fmt.Errorf("generating source: %w", err)
		}

		startTime := time.Now()

		uploadLog.Infof("Source: %s", sourceID)
		uploadLog.Infof("  type: %s", sourceType)
		uploadLog.Infof("  count: %d", len(sourceData))
		uploadLog.Infof("  size: %d", calculateTotalSize(sourceData))

		err = sourceCSV.Append(model.Source{
			ID:      sourceID,
			Region:  config.Region,
			Type:    sourceType,
			Count:   len(sourceData),
			Size:    calculateTotalSize(sourceData),
			Created: startTime,
		})
		if err != nil {
			return fmt.Errorf("appending to source log: %w", err)
		}
		if err := sourceCSV.Flush(); err != nil {
			return fmt.Errorf("flushing CSV log: %w", err)
		}

		// Create in-memory filesystem with the source data
		memFS := afero.NewMemMapFs()
		for path, data := range sourceData {
			if len(data) > 0 {
				// It's a file
				dir := filepath.Dir(path)
				if dir != "." {
					if err := memFS.MkdirAll(dir, 0755); err != nil {
						return fmt.Errorf("creating directory %s: %w", dir, err)
					}
				}
				if err := afero.WriteFile(memFS, path, data, 0644); err != nil {
					return fmt.Errorf("writing file %s: %w", path, err)
				}
				// Set modified time
				if err := memFS.Chtimes(path, time.Now(), time.Now()); err != nil {
					return fmt.Errorf("setting file time for %s: %w", path, err)
				}
			}
		}

		// Use the preparation API with custom FS provider
		api := preparation.NewAPI(
			repo,
			uploadClient,
			preparation.WithGetLocalFSForPathFn(func(path string) (fs.FS, error) {
				return afero.NewIOFS(memFS), nil
			}),
		)

		// Create space and source
		_, err = api.FindOrCreateSpace(ctx, r.space, sourceID.String(), spacesmodel.WithShardSize(maxShardSize))
		if err != nil {
			return fmt.Errorf("creating space: %w", err)
		}

		source, err := api.CreateSource(ctx, sourceID.String(), ".")
		if err != nil {
			return fmt.Errorf("creating source: %w", err)
		}

		err = api.AddSourceToSpace(ctx, r.space, source.ID())
		if err != nil {
			return fmt.Errorf("adding source to space: %w", err)
		}

		// Create uploads
		uploads, err := api.FindOrCreateUploads(ctx, r.space)
		if err != nil {
			return fmt.Errorf("creating uploads: %w", err)
		}

		if len(uploads) != 1 {
			return fmt.Errorf("expected 1 upload, got %d", len(uploads))
		}

		// This is a UUID used for this test, not the DB ID the client tracks.
		uploadID := uuid.New()
		upload := uploads[0]

		// Clear shard tracker for this upload
		uploadClient.ResetTrackedData()

		uploadLog.Infof("Upload: %s", uploadID)

		// Execute upload
		rootCID, uploadErr := api.ExecuteUpload(ctx, upload)
		if uploadErr != nil {
			uploadLog.Infof("    error: %s", uploadErr.Error())
		}
		endTime := time.Now()

		// Log shards that were tracked during upload
		shardLinks := make([]model.Link, 0, len(uploadClient.Shards))
		for _, info := range uploadClient.Shards {
			uploadLog.Info("Shard")
			uploadLog.Infof("  %s", info.Link)
			uploadLog.Infof("    digest: %s", digestutil.Format(info.Digest))
			uploadLog.Infof("    size: %d", info.Size)
			uploadLog.Infof("    node: %s", info.NodeID.DID())
			uploadLog.Infof("    url: %s", info.URL)
			shardRecord := model.Shard{
				ID:      model.ToLink(info.Link),
				Source:  sourceID,
				Upload:  uploadID,
				Node:    model.DID{DID: info.NodeID},
				URL:     model.URL(info.URL),
				Size:    int(info.Size),
				Started: info.Started,
				Ended:   info.Ended,
			}
			if info.LocationCommitment != nil {
				shardRecord.LocationCommitment = model.ToLink(info.LocationCommitment.Link())
			}
			if info.Error != nil {
				shardRecord.Error = model.Error{Message: info.Error.Error()}
			}
			err = shardCSV.Append(shardRecord)
			if err != nil {
				return fmt.Errorf("appending to shard log: %w", err)
			}
			if err := shardCSV.Flush(); err != nil {
				return fmt.Errorf("flushing CSV log: %w", err)
			}
			shardLinks = append(shardLinks, model.ToLink(info.Link))
		}

		for _, info := range uploadClient.Replications {
			shardLink := cidlink.Link{Cid: cid.NewCidV1(uint64(multicodec.Car), info.Digest)}
			replRecord := model.Replication{
				ID:        uuid.New(),
				Region:    config.Region,
				Shard:     model.Link{Link: shardLink},
				Replicas:  int(info.Replicas),
				Transfers: model.ToLinkList(info.Transfers),
				Requested: info.Requested,
			}
			if info.Error != nil {
				replRecord.Error = model.Error{Message: info.Error.Error()}
			}
			err = replicationCSV.Append(replRecord)
			if err != nil {
				return fmt.Errorf("appending to replication log: %w", err)
			}
			if err := replicationCSV.Flush(); err != nil {
				return fmt.Errorf("flushing CSV log: %w", err)
			}

			uploadLog.Info("Replication")
			uploadLog.Infof("  %s", replRecord.ID)
			uploadLog.Infof("    shard: %s", shardLink)
			uploadLog.Infof("    replicas: %d", info.Replicas)
			if len(info.Transfers) > 0 {
				uploadLog.Info("    transfers:")
				for _, s := range info.Transfers {
					uploadLog.Infof("      %s", s.String())
				}
			}
			uploadLog.Infof("    requested: %s", info.Requested.Format(time.DateTime))

			if info.Error != nil {
				uploadLog.Infof("    error: %s", info.Error.Error())
				continue
			}

			var wg sync.WaitGroup
			var mutex sync.Mutex
			for _, task := range info.Transfers {
				wg.Add(1)
				go func() {
					uploadLog.Info("Waiting for transfer...")
					uploadLog.Infof("  %s", task.String())

					transfer, err := waitForTransfer(ctx, r.receipts, replRecord.ID, task)

					uploadLog.Info("Transfer")
					uploadLog.Infof("  %s", task.String())
					if transfer.node != did.Undef {
						uploadLog.Infof("    node: %s", transfer.node.String())
					}
					if transfer.url != nil {
						uploadLog.Infof("    url: %s", transfer.url.String())
					}
					uploadLog.Infof("    elapsed: %s", transfer.ended.Sub(transfer.started).String())
					if err != nil {
						uploadLog.Infof("    error: %s", err.Error())
					}
					mutex.Lock()
					transferCSV.Append(transfer.ToModel(err))
					transferCSV.Flush()
					mutex.Unlock()
					wg.Done()
				}()
			}
			wg.Wait()

			uploadLog.Infof("%s replicated", shardLink)
		}

		uploadRecord := model.Upload{
			ID:      uploadID,
			Source:  sourceID,
			Shards:  model.LinkList(shardLinks),
			Started: startTime,
			Ended:   endTime,
		}
		if uploadErr == nil {
			uploadRecord.Root = model.Link{Link: cidlink.Link{Cid: rootCID}}
			if len(uploadClient.Indexes) > 0 {
				uploadRecord.Index = model.Link{Link: cidlink.Link{Cid: uploadClient.Indexes[0]}}
			}
		} else {
			uploadRecord.Error = model.Error{Message: uploadErr.Error()}
		}

		// Log upload
		err = uploadCSV.Append(uploadRecord)
		if err != nil {
			return fmt.Errorf("appending to upload log: %w", err)
		}
		if err := uploadCSV.Flush(); err != nil {
			return fmt.Errorf("flushing CSV log: %w", err)
		}

		if uploadErr == nil {
			totalSources++
			totalSize += int64(calculateTotalSize(sourceData))
		}

		uploadLog.Infof("Summary: sources=%d size=%d", totalSources, totalSize)
	}

	return nil
}

func generateSource(maxSize int) (string, map[string][]byte, error) {
	// Simple file source for now
	size := randomInt(minFileSize, maxSize)
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return "", nil, fmt.Errorf("generating random data: %w", err)
	}

	return "file", map[string][]byte{
		"data": data,
	}, nil
}

func randomInt(min, max int) int {
	if min >= max {
		return min
	}
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	return min + int(n.Int64())
}

func calculateTotalSize(files map[string][]byte) int {
	total := 0
	for _, data := range files {
		total += len(data)
	}
	return total
}

func NewUploadTestRunner(
	region string,
	id principal.Signer,
	guppy *guppyclient.Client,
	receipts *grc.Client,
	proof delegation.Delegation,
	dataDir string,
) (*UploadTestRunner, error) {
	space, err := ResourceFromDelegation(proof)
	if err != nil {
		return nil, err
	}
	proofs := []delegation.Proof{delegation.FromDelegation(proof)}
	return &UploadTestRunner{
		id:       id,
		guppy:    guppy,
		region:   region,
		dataDir:  dataDir,
		receipts: receipts,
		proofs:   proofs,
		space:    space,
	}, nil
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

func waitForTransfer(ctx context.Context, receipts *grc.Client, replID uuid.UUID, task ipld.Link) (replicaTransfer, error) {
	transfer := replicaTransfer{
		id:          task,
		replication: replID,
		started:     time.Now(),
	}

	// spend around 5 mins waiting for the receipt
	// the largest shard is 256mb so it should not really take that long
	rcpt, err := receipts.Poll(ctx, task, grc.WithInterval(5*time.Second), grc.WithRetries(60))
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
