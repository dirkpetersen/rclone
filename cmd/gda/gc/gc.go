//go:build unix

// Package gc implements 'rclone gda gc'.
package gc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/gda"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/spf13/cobra"
)

var (
	opt = libgda.GCOptions{
		MinAge:      7 * 24 * time.Hour,
		LockTimeout: libgda.DefaultOptions().LockTimeout,
	}
	jsonOut = false
)

// maxListed is how many objects of each kind the text report lists.
const maxListed = 20

func init() {
	flagSet := Command.Flags()
	flags.BoolVarP(flagSet, &opt.DeleteOrphans, "delete-orphans", "", opt.DeleteOrphans, "Remove orphan data objects", "")
	flags.DurationVarP(flagSet, &opt.MinAge, "min-age", "", opt.MinAge, "Keep orphans younger than this", "")
	flags.DurationVarP(flagSet, &opt.LockTimeout, "lock-timeout", "", opt.LockTimeout, "Take over a destination lock older than this", "")
	flags.BoolVarP(flagSet, &jsonOut, "json", "", jsonOut, "Print the report as JSON", "")
	gda.Command.AddCommand(Command)
}

// Command is 'rclone gda gc'.
var Command = &cobra.Command{
	Use:   "gc <gda root>",
	Short: `Report on the data of a GDA destination and remove orphans.`,
	Long: strings.ReplaceAll(`
Reads every changeset and index of the GDA tree and reports:

- how much data is stored, and how much of the packed data is still in
  the current indexes rather than only in history;
- orphans: packs and standalone objects no changeset refers to, left by
  runs that stopped before committing them;
- packs worth compacting: at least 180 days old, less than half live,
  with at least 1 GiB of dead data;
- objects without GDA names which no changeset refers to, which may
  not be GDA's.

With !--delete-orphans! it also removes the orphans older than
!--min-age!, holding the destination lock so that no backup adds data
meanwhile. Nothing else is ever removed, and nothing is removed if any
directory couldn't be read. Superseded and deleted files stay in their
packs, as history is kept.
`, "!", "`"),
	Annotations: map[string]string{
		"versionIntroduced": "v1.76",
	},
	RunE: func(command *cobra.Command, args []string) error {
		cmd.CheckArgs(1, 1, command, args)
		f := cmd.NewFsDir(args)
		cmd.Run(false, false, command, func() error {
			r, err := libgda.GC(context.Background(), f, opt)
			if r == nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				if jsonErr := enc.Encode(r); jsonErr != nil {
					return jsonErr
				}
				return err
			}
			printReport(r)
			return err
		})
		return nil
	},
}

// printReport prints r as text.
func printReport(r *libgda.GCReport) {
	fmt.Printf("Directories:   %d\n", r.Directories)
	fmt.Printf("Data objects:  %d (%s)\n", r.DataObjects, fs.SizeSuffix(r.DataBytes))
	fmt.Printf("Packs:         %d (%s), members %s live, %s only in history\n", r.Packs, fs.SizeSuffix(r.PackBytes), fs.SizeSuffix(r.LivePackData), fs.SizeSuffix(r.DeadPackData))
	fmt.Printf("Orphans:       %d (%s), %d removed\n", len(r.Orphans), fs.SizeSuffix(r.OrphanBytes), r.Deleted)
	for i, o := range r.Orphans {
		if i == maxListed {
			fmt.Printf("  ... and %d more\n", len(r.Orphans)-maxListed)
			break
		}
		fmt.Printf("  %s (%s, %s)\n", o.Key, fs.SizeSuffix(o.Size), o.ModTime.Format(time.RFC3339))
	}
	fmt.Printf("Compactable:   %d packs\n", len(r.Compactable))
	for i, u := range r.Compactable {
		if i == maxListed {
			fmt.Printf("  ... and %d more\n", len(r.Compactable)-maxListed)
			break
		}
		shared := ""
		if u.Shared {
			shared = ", shared"
		}
		fmt.Printf("  %s (%s, %s of %s live%s)\n", u.Key, fs.SizeSuffix(u.Size), fs.SizeSuffix(u.Live), fs.SizeSuffix(u.Members), shared)
	}
	fmt.Printf("Not GDA's:     %d objects no changeset refers to\n", len(r.Unknown))
	for i, o := range r.Unknown {
		if i == maxListed {
			fmt.Printf("  ... and %d more\n", len(r.Unknown)-maxListed)
			break
		}
		fmt.Printf("  %s\n", o.Key)
	}
}
