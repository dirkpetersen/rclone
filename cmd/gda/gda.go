// Package gda implements 'rclone gda'.
package gda

import (
	"errors"

	"github.com/rclone/rclone/cmd"
	"github.com/spf13/cobra"
)

func init() {
	cmd.Root.AddCommand(Command)
}

// Command is the parent of the gda subcommands.
var Command = &cobra.Command{
	Use:   "gda <action> [opts] <source> <destination>",
	Short: `Back up and archive directory trees to S3 Glacier Deep Archive at low cost.`,
	Long: `GDA stores directory trees in S3 Glacier Deep Archive, or any other
remote, at low cost. Small files are packed into tar files per directory,
large files are stored as their own objects, and every directory gets a
CSV index in a hot storage class so it can be browsed without restoring
anything.

Use a subcommand, for example:

    rclone gda backup /data/lab s3:lab-bucket/lab
`,
	Annotations: map[string]string{
		"versionIntroduced": "v1.76",
	},
	RunE: func(command *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("gda requires an action, e.g. 'rclone gda backup /path remote:'")
		}
		return errors.New("unknown action")
	},
}
