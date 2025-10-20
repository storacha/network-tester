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

var authorizedRetrievalShardsCmd = &cobra.Command{
	Use:   "authorized_retrieval_shards <path to shards csv>",
	Short: "Run UCAN authorized retrieval tests (just full shard retrievals)",
	Long:  "Runs UCAN authorized retrieval tests using the passed CSV shards data.",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logging.SetLogLevel("*", "info")

		shardsData, err := os.Open(args[0])
		cobra.CheckErr(err)

		indexer, err := client.New(config.IndexingServicePrincipal, *config.IndexingServiceURL)
		cobra.CheckErr(err)

		shards := eventlog.NewCSVReader[model.Shard](shardsData)
		results := eventlog.NewCSVWriter[model.Retrieval](os.Stdout)

		runner, err := runner.NewAuthorizedRetrievalShardsTestRunner(
			runner.AuthorizedRetrievalShardsTestConfig{
				Region:  config.Region,
				ID:      config.ID(),
				Proof:   config.Proof(),
				Indexer: indexer,
			},
			shards,
			results,
		)
		cobra.CheckErr(err)

		err = runner.Run(cmd.Context())
		cobra.CheckErr(err)

		err = results.Flush()
		cobra.CheckErr(err)
	},
}

func init() {
	rootCmd.AddCommand(authorizedRetrievalShardsCmd)
}
