//go:build unix

package backup

import (
	"context"
	"fmt"
	"strings"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/gda"
	"github.com/rclone/rclone/fs/config/flags"
	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/spf13/cobra"
)

var partitions = 1

func init() {
	addFlags(PlanCommand.Flags())
	flags.IntVarP(PlanCommand.Flags(), &partitions, "partitions", "", partitions, "Number of workers to plan for", "")
	gda.Command.AddCommand(PlanCommand)
	FinishCommand.Flags().AddFlag(Command.Flags().Lookup("run"))
	gda.Command.AddCommand(FinishCommand)
}

// PlanCommand is 'rclone gda plan'.
var PlanCommand = &cobra.Command{
	Use:   "plan <source directory> <destination>",
	Short: `Plan a GDA backup split over several workers.`,
	Long: strings.ReplaceAll(`
Scans the source once, takes the destination lock for the whole run,
splits the tree into partitions for !--partitions! workers and prints
the run ID. Run !rclone gda backup --run ID --partition N! for each
worker N from 0, and !rclone gda finish --run ID! once they are done.

The options which shape the backup, such as !--pack-size! and
!--compression!, are fixed by the plan; the workers' own values of
those are ignored.

The workers refresh the lock while they run, but nothing does between
the plan and the first worker starting. Other runs take the lock over
once it is older than the plan's !--lock-timeout! or their own,
whichever is longer, so raise it if the workers may wait in a queue for
longer than that.
`, "!", "`"),
	Annotations: map[string]string{
		"versionIntroduced": "v1.76",
	},
	RunE: func(command *cobra.Command, args []string) error {
		cmd.CheckArgs(2, 2, command, args)
		dst := cmd.NewFsDir(args[1:2])
		setSizes()
		cmd.Run(false, false, command, func() error {
			id, err := libgda.Plan(context.Background(), args[0], dst, opt, partitions)
			if err != nil {
				return err
			}
			fmt.Println(id)
			return nil
		})
		return nil
	},
}

// FinishCommand is 'rclone gda finish'.
var FinishCommand = &cobra.Command{
	Use:   "finish <destination> --run ID",
	Short: `Finish a GDA backup planned with rclone gda plan.`,
	Long: `Merges the ledgers of the run's workers into the run's ledger and
releases the destination lock. It fails, keeping the lock, while a
partition of the plan hasn't finished, and after releasing the lock if
any worker reported errors. Run partitions which reported errors again
before finishing, as they can't be run once the run is finished.
`,
	Annotations: map[string]string{
		"versionIntroduced": "v1.76",
	},
	RunE: func(command *cobra.Command, args []string) error {
		cmd.CheckArgs(1, 1, command, args)
		if runID == "" {
			return fmt.Errorf("finish needs --run")
		}
		dst := cmd.NewFsDir(args[0:1])
		cmd.Run(false, false, command, func() error {
			ledger, err := libgda.FinishRun(context.Background(), dst, runID, opt)
			logLedger(ledger)
			return err
		})
		return nil
	},
}
