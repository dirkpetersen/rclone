// Package find implements 'rclone gda find'.
package find

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/gda"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/rclone/rclone/fs/filter"
	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/spf13/cobra"
)

var (
	at      = ""
	jsonOut = false
)

func init() {
	flagSet := Command.Flags()
	flags.StringVarP(flagSet, &at, "at", "", at, "Search the tree as of this run ID or RFC 3339 time", "")
	flags.BoolVarP(flagSet, &jsonOut, "json", "", jsonOut, "Print the entries as JSON lines", "")
	gda.Command.AddCommand(Command)
}

// Command is 'rclone gda find'.
var Command = &cobra.Command{
	Use:   "find <gda root> [path]",
	Short: `Find files in a GDA destination from its indexes.`,
	Long: strings.ReplaceAll(`
Prints the files below path, relative to the GDA root, which pass
rclone's filters, reading only the indexes, so nothing needs to be
restored. With !--at! it searches the tree as it was at the end of that
run. For example, to find BAM files over 1 GiB changed in the last year:

    rclone gda find s3:bucket/lab --include "*.bam" --min-size 1G --max-age 1y

Each line shows the size, modification time and path. !--json! prints
each entry as a line of JSON, with where its data is stored.

To search many runs or the whole history, query the catalog under
!_gda/catalog! with a tool such as DuckDB instead.
`, "!", "`"),
	Annotations: map[string]string{
		"versionIntroduced": "v1.76",
	},
	RunE: func(command *cobra.Command, args []string) error {
		cmd.CheckArgs(1, 2, command, args)
		dst := cmd.NewFsDir(args[0:1])
		p := ""
		if len(args) == 2 {
			p = args[1]
		}
		cmd.Run(false, false, command, func() error {
			ctx := context.Background()
			runID, err := libgda.ParseAt(at)
			if err != nil {
				return err
			}
			fi := filter.GetConfig(ctx)
			enc := json.NewEncoder(os.Stdout)
			return libgda.Walk(ctx, dst, p, runID, func(l *libgda.Located) error {
				if l.IsDir() || !fi.Include(l.LocalPath, max(l.Size, 0), l.ModTime, nil) {
					return nil
				}
				if jsonOut {
					return enc.Encode(l)
				}
				_, err := fmt.Printf("%12d %s %s\n", max(l.Size, 0), l.ModTime.Local().Format("2006-01-02 15:04:05"), l.LocalPath)
				return err
			})
		})
		return nil
	},
}
