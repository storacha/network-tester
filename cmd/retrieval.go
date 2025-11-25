package cmd

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	guppyclient "github.com/storacha/guppy/pkg/client"
	grc "github.com/storacha/guppy/pkg/receipt"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/runner"
)

var retrievalCmd = &cobra.Command{
	Use:   "retrieval [data directory]",
	Short: "Run UCAN authorized retrieval tests",
	Long:  "Runs UCAN authorized retrieval tests. Reads from <datadir>/uploads.csv and writes to <datadir>/retrievals.csv",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logging.SetLogLevel("*", "info")

		dataDir := "data"
		if len(args) > 0 {
			dataDir = args[0]
		}

		indexer, err := client.New(config.IndexingServicePrincipal, *config.IndexingServiceURL)
		cobra.CheckErr(err)

		receipts := grc.New(config.UploadServiceURL.JoinPath("receipt"))

		guppy, err := guppyclient.NewClient(
			guppyclient.WithConnection(config.UploadServiceConnection),
			guppyclient.WithPrincipal(config.ID()),
			guppyclient.WithReceiptsClient(receipts),
		)
		cobra.CheckErr(err)

		err = guppy.AddProofs(config.Proof())
		cobra.CheckErr(err)

		runner, err := runner.NewRetrievalTestRunner(config.Region, config.ID(), config.IndexingServicePrincipal, indexer, guppy, receipts, config.Proof(), dataDir)
		cobra.CheckErr(err)

		err = runner.Run(cmd.Context())
		cobra.CheckErr(err)
	},
}

func init() {
	rootCmd.AddCommand(retrievalCmd)
}
