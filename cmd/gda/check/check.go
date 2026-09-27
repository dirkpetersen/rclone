// Package check implements 'rclone gda check'.
package check

import (
	"context"
	"encoding/json"
	"errors"
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
	opt     = libgda.CheckOptions{}
	jsonOut = false
)

func init() {
	flagSet := Command.Flags()
	flags.StringVarP(flagSet, &at, "at", "", at, "Check the tree as of this run ID or RFC 3339 time", "")
	flags.BoolVarP(flagSet, &opt.Download, "download", "", opt.Download, "Read the data which can be read now and check its MD5", "")
	flags.BoolVarP(flagSet, &jsonOut, "json", "", jsonOut, "Print the report as JSON", "")
	gda.Command.AddCommand(Command)
}

// Command is 'rclone gda check'.
var Command = &cobra.Command{
	Use:   "check <gda root> [path]",
	Short: `Check that the data of a GDA backup is all there.`,
	Long: strings.ReplaceAll(`
Checks that the data of every file below path, relative to the GDA root,
is stored: that the object holding it exists and, for a file stored as
its own object, has the recorded size. This needs one listing per
directory and reads no data, so it works on data in archive storage
classes. With !--at! it checks the tree as it was at the end of that
run.

With !--download! it also reads the data of every file which can be
read now and checks its MD5. Data in an archive storage class which
hasn't been restored is counted as unreadable rather than as a problem.
Downloading is charged as for any other read.

It fails if any data is missing, the wrong size or corrupt.
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
			r, err := libgda.Check(context.Background(), dst, p, runID, opt)
			if err != nil {
				return err
			}
			if jsonOut {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				if err := enc.Encode(r); err != nil {
					return err
				}
			} else {
				printReport(r)
			}
			if r.Failed() {
				return errors.New("check found problems")
			}
			return nil
		})
		return nil
	},
}

// printReport prints r as text.
func printReport(r *libgda.CheckReport) {
	fmt.Printf("Files:       %d in %d data objects\n", r.Files, r.Objects)
	for _, list := range []struct {
		title string
		paths []string
	}{{"Missing", r.Missing}, {"Wrong size", r.WrongSize}, {"Bad MD5", r.BadMD5}, {"Errors", r.Errors}} {
		fmt.Printf("%-12s %d\n", list.title+":", len(list.paths))
		for _, p := range list.paths {
			fmt.Printf("  %s\n", p)
		}
	}
	if r.Downloaded > 0 || r.Unreadable > 0 {
		fmt.Printf("Downloaded:  %d, %d not readable now\n", r.Downloaded, r.Unreadable)
	}
}
