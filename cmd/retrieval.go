package cmd

import (
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	"github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
	"github.com/storacha/network-tester/pkg/runner"
)

var retrievalCmd = &cobra.Command{
	Use:   "retrieval <path to upload csv>",
	Short: "Run retrieval tests",
	Long:  "Runs retrieval tests using the passed CSV upload data.",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logging.SetLogLevel("*", "info")

		uploadsData, err := os.Open(args[0])
		cobra.CheckErr(err)

		indexer, err := client.New(config.IndexingServicePrincipal, *config.IndexingServiceURL)
		cobra.CheckErr(err)

		uploads := eventlog.NewCSVReader[model.Upload](uploadsData)
		results := eventlog.NewCSVWriter[model.Retrieval](os.Stdout)

		runner := runner.NewRetrievalTestRunner(config.Region, indexer, uploads, results)
		err = runner.Run(cmd.Context())
		cobra.CheckErr(err)

		err = results.Flush()
		cobra.CheckErr(err)
	},
}

func init() {
	rootCmd.AddCommand(retrievalCmd)
}
