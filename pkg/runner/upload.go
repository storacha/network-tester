package runner

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/fs"
	"math/big"
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
	"github.com/storacha/go-ucanto/did"
	guppy "github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	grc "github.com/storacha/guppy/pkg/receipt"
	"github.com/storacha/network-tester/pkg/client"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
)

var uploadLog = logging.Logger("upload-runner")

const (
	minFileSize       = 128
	maxBytes          = 100 * 1024 * 1024 * 1024 // 100 GB
	maxPerUploadBytes = 1 * 1024 * 1024          // * 1024 * 1024   // 1 GB
	maxShardSize      = 133_169_152              // Default SHARD_SIZE
)

type UploadTestRunner struct {
	dataDir  string
	receipts *grc.Client
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

	// Get config
	id := config.ID()
	proof := config.Proof()
	spaceDID, err := did.Parse(proof.Capabilities()[0].With())
	if err != nil {
		return fmt.Errorf("parsing space DID: %w", err)
	}

	uploadLog.Infof("Region: %s", config.Region)
	uploadLog.Infof("Agent: %s", id.DID())
	uploadLog.Infof("Space: %s", spaceDID)

	// Create Guppy client
	guppyClient, err := guppy.NewClient(
		guppy.WithConnection(config.UploadServiceConnection),
		guppy.WithPrincipal(id),
		guppy.WithReceiptsClient(r.receipts),
	)
	if err != nil {
		return fmt.Errorf("creating guppy client: %w", err)
	}

	// Add the proof delegation to the client
	err = guppyClient.AddProofs(proof)
	if err != nil {
		return fmt.Errorf("adding proofs to client: %w", err)
	}

	dbPath := filepath.Join(r.dataDir, "upload.db")
	var repo *sqlrepo.Repo
	defer func() {
		if repo != nil {
			repo.Close()
		}
	}()

	uploadClient := client.New(guppyClient)

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
		_, err = api.FindOrCreateSpace(ctx, spaceDID, sourceID.String(), spacesmodel.WithShardSize(maxShardSize))
		if err != nil {
			return fmt.Errorf("creating space: %w", err)
		}

		source, err := api.CreateSource(ctx, sourceID.String(), ".")
		if err != nil {
			return fmt.Errorf("creating source: %w", err)
		}

		err = api.AddSourceToSpace(ctx, spaceDID, source.ID())
		if err != nil {
			return fmt.Errorf("adding source to space: %w", err)
		}

		// Create uploads
		uploads, err := api.FindOrCreateUploads(ctx, spaceDID)
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

func NewUploadTestRunner(dataDir string, receipts *grc.Client) *UploadTestRunner {
	return &UploadTestRunner{dataDir: dataDir, receipts: receipts}
}
