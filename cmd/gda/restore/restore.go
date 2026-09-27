//go:build unix

// Package restore implements 'rclone gda restore'.
package restore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/gda"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/spf13/cobra"
)

var (
	opt      = libgda.DefaultRestoreOptions()
	resumeID = ""
	jsonOut  = false
	yes      = false
)

func init() {
	flagSet := Command.Flags()
	flags.StringVarP(flagSet, &opt.Tier, "tier", "", opt.Tier, "Retrieval tier: Bulk, Standard or Expedited", "")
	flags.IntVarP(flagSet, &opt.Lifetime, "lifetime", "", opt.Lifetime, "Days the restored copies stay available", "")
	flags.StringVarP(flagSet, &opt.At, "at", "", opt.At, "Restore as of this run ID or RFC 3339 time", "")
	flags.BoolVarP(flagSet, &opt.Overwrite, "overwrite", "", opt.Overwrite, "Replace local files which differ from the backup", "")
	flags.StringVarP(flagSet, &resumeID, "resume", "", resumeID, "Fetch what is ready of an earlier restore with this ID", "")
	flags.BoolVarP(flagSet, &jsonOut, "json", "", jsonOut, "Print the progress as JSON", "")
	flags.BoolVarP(flagSet, &yes, "yes", "", yes, "Don't ask for confirmation before requesting restores", "")
	gda.Command.AddCommand(Command)
}

// Command is 'rclone gda restore'.
var Command = &cobra.Command{
	Use:   "restore <gda root> <target directory> [path ...]",
	Short: `Restore files from a GDA destination.`,
	Long: strings.ReplaceAll(`
Restores the given paths, or the whole tree if none are given, from the
GDA destination at gda root into the local target directory. Paths are
relative to the GDA root and keep that relative path below the target.

Data in an archive storage class must be restored before it can be
read, which takes up to 48 hours for Deep Archive with the default
!--tier Bulk!. So a restore runs in two steps:

    rclone gda restore s3:bucket/lab /restore results/plots
    rclone gda restore --resume <id> s3:bucket/lab

The first command requests the restores, saves the plan in the GDA root
under !_gda/restores/<id>! and fetches whatever is already readable.
Running it with !--resume <id>! fetches whatever has become readable
since and reports what is still being restored, so it is safe to run
from cron until it reports !done!.

Local files which are already identical to the backup are skipped. If
any local files differ, the restore stops before requesting anything
unless !--overwrite! is given.

Permissions and modification times are always restored. Owner, group,
setuid and setgid are restored only when running as root, by name where
the name exists and by numeric ID otherwise.

With !--json! the progress is printed as JSON for other programs, such
as Motuz.
`, "!", "`"),
	Annotations: map[string]string{
		"versionIntroduced": "v1.76",
	},
	RunE: func(command *cobra.Command, args []string) error {
		var target string
		if resumeID != "" {
			cmd.CheckArgs(1, 2, command, args)
			if len(args) == 2 {
				target = args[1]
			}
		} else {
			cmd.CheckArgs(2, 1<<30, command, args)
			target = args[1]
			opt.Paths = args[2:]
		}
		dst := cmd.NewFsDir(args[0:1])
		cmd.Run(false, false, command, func() error {
			ctx := context.Background()
			var st *libgda.RestoreStatus
			var err error
			if resumeID != "" {
				st, err = libgda.ResumeRestore(ctx, dst, resumeID, target)
			} else {
				if !yes && !jsonOut && !fs.GetConfig(ctx).DryRun {
					if err := confirm(opt); err != nil {
						return err
					}
				}
				st, err = libgda.StartRestore(ctx, dst, target, opt)
			}
			if err != nil {
				return err
			}
			return report(st)
		})
		return nil
	},
}

// confirm asks before requesting restores, which cost money.
func confirm(opt libgda.RestoreOptions) error {
	fmt.Fprintf(os.Stderr, "Request %s restores of the objects needed? This is charged by AWS. [y/N] ", opt.Tier)
	var answer string
	_, _ = fmt.Scanln(&answer)
	if !strings.EqualFold(answer, "y") && !strings.EqualFold(answer, "yes") {
		return errors.New("restore cancelled")
	}
	return nil
}

func report(st *libgda.RestoreStatus) error {
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(st)
	}
	fmt.Printf("Restore %s (%s): %s\n", st.RestoreID, st.Tier, st.State)
	fmt.Printf("Objects: %d needed, %d fetched, %d still being restored\n", st.Objects.Requested, st.Objects.Fetched, st.Objects.Restoring)
	fmt.Printf("Files: %d total, %d in place (%d were already), %d failed\n", st.Files.Total, st.Files.Fetched, st.Files.SkippedIdentical, st.Files.Failed)
	if st.State == libgda.StateRestoring {
		fmt.Printf("Ready by %s. Fetch the rest with: rclone gda restore --resume %s <gda root>\n", st.ReadyBy.Local().Format("2006-01-02 15:04 MST"), st.RestoreID)
	}
	if st.State == libgda.StateFailed {
		return fmt.Errorf("%d files failed to restore", st.Files.Failed)
	}
	return nil
}
