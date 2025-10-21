package cmd

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
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

		runner := runner.NewUploadTestRunner(dataDir)
		err := runner.Run(cmd.Context())
		cobra.CheckErr(err)
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)
}
