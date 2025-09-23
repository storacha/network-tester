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

var authorizedRetrievalCmd = &cobra.Command{
	Use:   "authorized_retrieval <path to upload csv>",
	Short: "Run UCAN authorized retrieval tests",
	Long:  "Runs UCAN authorized retrieval tests using the passed CSV upload data.",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logging.SetLogLevel("*", "info")

		uploadsData, err := os.Open(args[0])
		cobra.CheckErr(err)

		indexer, err := client.New(config.IndexingServicePrincipal, *config.IndexingServiceURL)
		cobra.CheckErr(err)

		uploads := eventlog.NewCSVReader[model.Upload](uploadsData)
		results := eventlog.NewCSVWriter[model.Retrieval](os.Stdout)

		runner, err := runner.NewAuthorizedRetrievalTestRunner(config.Region, indexer, config.Proof(), uploads, results)
		cobra.CheckErr(err)

		err = runner.Run(cmd.Context())
		cobra.CheckErr(err)

		err = results.Flush()
		cobra.CheckErr(err)
	},
}

func init() {
	rootCmd.AddCommand(authorizedRetrievalCmd)
}
