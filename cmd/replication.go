package cmd

import (
	"os"

	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	grc "github.com/storacha/guppy/pkg/receipt"
	isc "github.com/storacha/indexing-service/pkg/client"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/eventlog"
	"github.com/storacha/network-tester/pkg/model"
	"github.com/storacha/network-tester/pkg/runner"
)

var replicationCmd = &cobra.Command{
	Use:   "replication <path to shards input csv> <path to replications output csv> <path to transfers output csv>",
	Short: "Run replication tests",
	Long:  "Runs replication tests using the passed CSV upload data.",
	Args:  cobra.MinimumNArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		logging.SetLogLevel("*", "info")

		shardsData, err := os.Open(args[0])
		cobra.CheckErr(err)
		shards := eventlog.NewCSVReader[model.Shard](shardsData)

		replicationsData, err := os.Create(args[1])
		cobra.CheckErr(err)
		replications := eventlog.NewCSVWriter[model.Replication](replicationsData)
		defer replications.Flush()

		transfersData, err := os.Create(args[2])
		cobra.CheckErr(err)
		transfers := eventlog.NewCSVWriter[model.ReplicaTransfer](transfersData)
		defer transfers.Flush()

		indexer, err := isc.New(config.IndexingServicePrincipal, *config.IndexingServiceURL)
		cobra.CheckErr(err)

		receipts := grc.New(config.UploadServiceURL.JoinPath("receipt"))

		runner, err := runner.NewReplicationTestRunner(runner.ReplicationTestConfig{
			Region:            config.Region,
			ID:                config.ID(),
			Proof:             config.Proof(),
			Indexer:           indexer,
			ServiceConnection: config.UploadServiceConnection,
			ServiceReceipts:   receipts,
		}, shards, replications, transfers)
		cobra.CheckErr(err)

		err = runner.Run(cmd.Context())
		cobra.CheckErr(err)
	},
}

func init() {
	rootCmd.AddCommand(replicationCmd)
}
