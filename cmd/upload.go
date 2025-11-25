package cmd

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	guppyclient "github.com/storacha/guppy/pkg/client"
	grc "github.com/storacha/guppy/pkg/receipt"
	"github.com/storacha/network-tester/pkg/config"
	"github.com/storacha/network-tester/pkg/runner"
)

var uploadCmd = &cobra.Command{
	Use:   "upload [data directory]",
	Short: "Run upload tests",
	Long:  "Runs upload tests and writes CSV logs to the data directory.",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logging.SetLogLevel("*", "info")

		dataDir := "data"
		if len(args) > 0 {
			dataDir = args[0]
		}

		receipts := grc.New(config.UploadServiceURL.JoinPath("receipt"))

		guppy, err := guppyclient.NewClient(
			guppyclient.WithConnection(config.UploadServiceConnection),
			guppyclient.WithPrincipal(config.ID()),
			guppyclient.WithReceiptsClient(receipts),
		)
		cobra.CheckErr(err)

		err = guppy.AddProofs(config.Proof())
		cobra.CheckErr(err)

		runner, err := runner.NewUploadTestRunner(
			config.Region,
			config.ID(),
			guppy,
			receipts,
			config.Proof(),
			dataDir,
		)
		cobra.CheckErr(err)

		err = runner.Run(cmd.Context())
		cobra.CheckErr(err)
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)
}
