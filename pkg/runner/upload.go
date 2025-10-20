package runner

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/spf13/afero"
	assertcap "github.com/storacha/go-libstoracha/capabilities/assert"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	guppyclient "github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/preparation"
	spacesmodel "github.com/storacha/guppy/pkg/preparation/spaces/model"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
	_ "modernc.org/sqlite"
)

var uploadLog = logging.Logger("upload-runner")

const (
	minFileSize       = 128
	maxBytes          = 100 * 1024 * 1024 * 1024 // 100 GB
	maxPerUploadBytes = 1 * 1024 * 1024 * 1024   // 1 GB
	maxShardSize      = 133_169_152              // Default SHARD_SIZE
)

type UploadTestRunner struct {
	dataDir string
}

type shardInfo struct {
	cid      cid.Cid
	size     int64
	digest   mh.Multihash
	location delegation.Delegation
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
	defer sourceLogFile.Close()
	sourceCSV := eventlog.NewCSVWriter[model.Source](sourceLogFile)
	defer sourceCSV.Flush()

	shardLogFile, err := os.Create(filepath.Join(r.dataDir, "shards.csv"))
	if err != nil {
		return fmt.Errorf("creating shard log file: %w", err)
	}
	defer shardLogFile.Close()
	shardCSV := eventlog.NewCSVWriter[model.Shard](shardLogFile)
	defer shardCSV.Flush()

	uploadLogFile, err := os.Create(filepath.Join(r.dataDir, "uploads.csv"))
	if err != nil {
		return fmt.Errorf("creating upload log file: %w", err)
	}
	defer uploadLogFile.Close()
	uploadCSV := eventlog.NewCSVWriter[model.Upload](uploadLogFile)
	defer uploadCSV.Flush()

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
	guppyClient, err := guppyclient.NewClient(
		guppyclient.WithConnection(config.UploadServiceConnection),
		guppyclient.WithPrincipal(id),
	)
	if err != nil {
		return fmt.Errorf("creating guppy client: %w", err)
	}

	// Add the proof delegation to the client
	err = guppyClient.AddProofs(proof)
	if err != nil {
		return fmt.Errorf("adding proofs to client: %w", err)
	}

	// Create preparation database
	dbPath := filepath.Join(r.dataDir, "upload.db")
	err = os.RemoveAll(dbPath)
	if err != nil {
		return fmt.Errorf("removing existing database: %w", err)
	}
	repo, err := preparation.OpenRepo(ctx, dbPath)
	if err != nil {
		return fmt.Errorf("opening repository: %w", err)
	}
	defer repo.Close()

	// Track shards as they're uploaded
	shardTracker := &shardTrackerClient{
		Client: guppyClient,
		space:  spaceDID,
		shards: make(map[string]*shardInfo),
	}

	totalSize := int64(0)
	totalSources := 0

	for totalSize < maxBytes {
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
			shardTracker,
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
		shardTracker.shards = make(map[string]*shardInfo)

		// Execute upload
		rootCID, err := api.ExecuteUpload(ctx, upload)
		var uploadErr model.Error
		if err != nil {
			uploadErr = model.Error{Message: err.Error()}
			uploadLog.Errorf("Upload failed: %v", err)
		}

		endTime := time.Now()

		// Log shards that were tracked during upload
		shardLinks := make([]model.Link, 0, len(shardTracker.shards))
		for _, info := range shardTracker.shards {
			var nodeID model.DID
			var locationURL model.URL
			if info.location != nil {
				nodeID = model.DID{DID: info.location.Issuer().DID()}
				if len(info.location.Capabilities()) != 1 {
					return fmt.Errorf("expected 1 capability in location assertion, got %d", len(info.location.Capabilities()))
				}
				locationCaveats, ok := info.location.Capabilities()[0].Nb().(assertcap.LocationCaveats)
				if !ok {
					return fmt.Errorf("expected LocationCaveats in location assertion, got %T", info.location.Capabilities()[0].Nb())
				}
				if len(locationCaveats.Location) == 0 {
					return fmt.Errorf("no locations in location caveats")
				}
				if len(locationCaveats.Location) > 1 {
					uploadLog.Warnf("multiple locations in location caveats, using the first one")
				}
				locationURL = model.URL(locationCaveats.Location[0])
			}

			err = shardCSV.Append(model.Shard{
				ID:                 model.Link{Link: cidlink.Link{Cid: info.cid}},
				Source:             sourceID,
				Upload:             uploadID,
				Node:               nodeID,
				LocationCommitment: model.Link{Link: cidlink.Link{Cid: info.cid}},
				URL:                locationURL,
				Size:               int(info.size),
				Started:            startTime,
				Ended:              endTime,
			})
			if err != nil {
				return fmt.Errorf("appending to shard log: %w", err)
			}

			shardLinks = append(shardLinks, model.Link{Link: cidlink.Link{Cid: info.cid}})
		}

		// Get index link - we don't have this from guppy yet
		// The JS code creates an index separately
		var indexLink model.Link

		// Log upload
		err = uploadCSV.Append(model.Upload{
			ID:      uploadID,
			Root:    model.Link{Link: cidlink.Link{Cid: rootCID}},
			Source:  sourceID,
			Index:   indexLink,
			Shards:  model.LinkList(shardLinks),
			Error:   uploadErr,
			Started: startTime,
			Ended:   endTime,
		})
		if err != nil {
			return fmt.Errorf("appending to upload log: %w", err)
		}

		if uploadErr.Message == "" {
			totalSources++
			totalSize += int64(calculateTotalSize(sourceData))
		}

		uploadLog.Infof("Summary: sources=%d size=%d", totalSources, totalSize)
	}

	return nil
}

// shardTrackerClient wraps a Guppy client and tracks uploaded shards
type shardTrackerClient struct {
	*guppyclient.Client
	space  did.DID
	shards map[string]*shardInfo
}

func (c *shardTrackerClient) SpaceBlobAdd(ctx context.Context, content io.Reader, space did.DID, options ...guppyclient.SpaceBlobAddOption) (mh.Multihash, delegation.Delegation, error) {
	// Read content to get size for tracking
	data, err := io.ReadAll(content)
	if err != nil {
		return nil, nil, fmt.Errorf("reading content: %w", err)
	}

	uploadLog.Infof("Uploading shard: %d bytes", len(data))

	// Use the Guppy client to upload the shard
	digest, location, err := c.Client.SpaceBlobAdd(ctx, bytes.NewReader(data), space, options...)
	if err != nil {
		return nil, nil, fmt.Errorf("uploading shard: %w", err)
	}

	// Create CID from the returned digest
	cidV1 := cid.NewCidV1(cid.Raw, digest)
	uploadLog.Infof("Shard uploaded: %s", cidV1)

	// Track the shard
	c.shards[cidV1.String()] = &shardInfo{
		cid:      cidV1,
		size:     int64(len(data)),
		digest:   digest,
		location: location,
	}

	return digest, location, nil
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

func NewUploadTestRunner(dataDir string) *UploadTestRunner {
	return &UploadTestRunner{dataDir: dataDir}
}
