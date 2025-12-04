package cmd

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/spf13/cobra"
	"github.com/storacha/go-ucanto/core/delegation"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
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

		skipReplication, err := cmd.Flags().GetBool("skip-replication")
		cobra.CheckErr(err)

		parallel, err := cmd.Flags().GetInt("parallel")
		cobra.CheckErr(err)

		receipts := grc.New(config.UploadServiceURL.JoinPath("receipt"))

		agent, err := ed25519.Generate()
		cobra.CheckErr(err)

		// delegation from space to mailto account
		adminDlg, err := delegation.Delegate(
			config.ID(),
			agent.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("*", "ucan:*", ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(config.Proof())),
		)

		guppy, err := guppyclient.NewClient(
			guppyclient.WithConnection(config.UploadServiceConnection),
			guppyclient.WithPrincipal(agent),
			guppyclient.WithReceiptsClient(receipts),
		)
		cobra.CheckErr(err)

		err = guppy.AddProofs(adminDlg)
		cobra.CheckErr(err)

		runner, err := runner.NewUploadTestRunner(
			config.Region,
			config.ID(),
			guppy,
			receipts,
			config.Proof(),
			dataDir,
			skipReplication,
			parallel,
		)
		cobra.CheckErr(err)

		err = runner.Run(cmd.Context())
		cobra.CheckErr(err)
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)
	uploadCmd.Flags().Bool("skip-replication", false, "Skip replication and only test the upload path")
	uploadCmd.Flags().Int("parallel", 1, "Number of concurrent upload workers for load testing")
}
