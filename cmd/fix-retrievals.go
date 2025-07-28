package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/multiformats/go-multihash"
	"github.com/spf13/cobra"
	"github.com/storacha/indexing-service/pkg/blobindex"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
)

var fixRetrievalsCmd = &cobra.Command{
	Use:   "fix-retrievals <path to retrievals csv>",
	Short: "",
	Long:  "",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// logging.SetLogLevel("*", "info")

		retrievalsData, err := os.Open(args[0])
		cobra.CheckErr(err)

		indexer, err := client.New(config.IndexingServicePrincipal, *config.IndexingServiceURL)
		cobra.CheckErr(err)

		retrievals := eventlog.NewCSVReader[model.Retrieval](retrievalsData)
		results := eventlog.NewCSVWriter[model.Retrieval](os.Stdout)

		err = Run(cmd.Context(), indexer, retrievals, results)
		cobra.CheckErr(err)

		err = results.Flush()
		cobra.CheckErr(err)
	},
}

func init() {
	rootCmd.AddCommand(fixRetrievalsCmd)
}

func Run(ctx context.Context, indexer *client.Client, retrievals eventlog.Iterable[model.Retrieval], results eventlog.Appender[model.Retrieval]) error {
	indexCache := blobindex.NewMultihashMap[blobindex.ShardedDagIndex](0)
	for r, err := range retrievals.Iterator() {
		if err != nil {
			return err
		}

		if r.Error.Message != "" {
			err := results.Append(r)
			if err != nil {
				return err
			}
			continue
		}

		index := indexCache.Get(r.Slice.Multihash)
		if index == nil {
			queryRes, err := indexer.QueryClaims(ctx, types.Query{
				Hashes: []multihash.Multihash{r.Slice.Multihash},
			})
			if err != nil {
				return err
			}

			if len(queryRes.Indexes()) != 1 {
				return errors.New("too few (or many) indexes")
			}

			var indexBytes []byte
			for b, err := range queryRes.Blocks() {
				if err != nil {
					return err
				}
				if b.Link() == queryRes.Indexes()[0] {
					indexBytes = b.Bytes()
					break
				}
			}

			index, err = blobindex.Extract(bytes.NewReader(indexBytes))
			if err != nil {
				return fmt.Errorf("decoding index: %w", err)
			}

			for _, slices := range index.Shards().Iterator() {
				for slice := range slices.Iterator() {
					indexCache.Set(slice, index)
				}
			}
		}

		size, err := sliceSize(r.Slice.Multihash, index)
		if err != nil {
			return err
		}

		r.Size = size
		err = results.Append(r)
		if err != nil {
			return err
		}
	}
	return nil
}

func sliceSize(digest multihash.Multihash, index blobindex.ShardedDagIndex) (int, error) {
	for _, slices := range index.Shards().Iterator() {
		for slice, position := range slices.Iterator() {
			if bytes.Equal(slice, digest) {
				return int(position.Length), nil
			}
		}
	}
	return 0, fmt.Errorf("failed to find size of slice: z%s in index for content: %s", digest.B58String(), index.Content().String())
}
