// Package ls implements 'rclone gda ls'.
package ls

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/gda"
	"github.com/rclone/rclone/fs/config/flags"
	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/spf13/cobra"
)

var (
	at      = ""
	jsonOut = false
)

func init() {
	flagSet := Command.Flags()
	flags.StringVarP(flagSet, &at, "at", "", at, "List as of this run ID or RFC 3339 time", "")
	flags.BoolVarP(flagSet, &jsonOut, "json", "", jsonOut, "Print the entries as JSON", "")
	gda.Command.AddCommand(Command)
}

// Command is 'rclone gda ls'.
var Command = &cobra.Command{
	Use:   "ls <gda root> [path]",
	Short: `List a directory of a GDA destination from its indexes.`,
	Long: strings.ReplaceAll(`
Lists the directory at path, relative to the GDA root, as it was backed
up, reading only the indexes, so nothing needs to be restored. With
!--at! it lists the directory as it was at the end of that run.
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
			runID, err := libgda.ParseAt(at)
			if err != nil {
				return err
			}
			entries, err := libgda.List(context.Background(), dst, p, runID)
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(entries)
			}
			for _, e := range entries {
				size := e.Size
				if e.IsDir() {
					size = e.TreeSize
				}
				fmt.Printf("%-8s %12d %s %s\n", e.Type, max(size, 0), e.ModTime.Local().Format("2006-01-02 15:04:05"), e.Path)
			}
			return nil
		})
		return nil
	},
}
