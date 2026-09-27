//go:build unix

// Package backup implements 'rclone gda backup'.
package backup

import (
	"context"
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
	opt           = libgda.DefaultOptions()
	packSize      = fs.SizeSuffix(opt.PackSize)
	standaloneMin = fs.SizeSuffix(opt.StandaloneMin)
	rollupMax     = fs.SizeSuffix(opt.RollupMax)
)

func init() {
	flagSet := Command.Flags()
	flags.FVarP(flagSet, &packSize, "pack-size", "", "Maximum size of a pack of small files", "")
	flags.FVarP(flagSet, &standaloneMin, "standalone-min", "", "Store files at least this big as their own objects", "")
	flags.FVarP(flagSet, &rollupMax, "rollup-max", "", "Pack whole subtrees smaller than this as one unit (0 to disable)", "")
	flags.StringVarP(flagSet, &opt.DataTier, "data-tier", "", opt.DataTier, "Storage class for packs and standalone files", "")
	flags.StringVarP(flagSet, &opt.MetaTier, "meta-tier", "", opt.MetaTier, "Storage class for changesets, indexes and run files", "")
	flags.StringVarP(flagSet, &opt.Worker, "worker", "", opt.Worker, "Worker ID used in pack names", "")
	flags.StringVarP(flagSet, &opt.TempDir, "temp-dir", "", opt.TempDir, "Directory for pack spool files (default system temp directory)", "")
	flags.StringVarP(flagSet, &opt.RootLabel, "root-label", "", opt.RootLabel, "Name used for the top directory in pack names (default last element of the destination)", "")
	flags.DurationVarP(flagSet, &opt.LockTimeout, "lock-timeout", "", opt.LockTimeout, "Take over a destination lock older than this", "")
	flags.IntVarP(flagSet, &opt.Retries, "upload-retries", "", opt.Retries, "Upload attempts per object", "")
	gda.Command.AddCommand(Command)
}

// Command is 'rclone gda backup'.
var Command = &cobra.Command{
	Use:   "backup <source directory> <destination>",
	Short: `Back up a local directory tree to a GDA destination.`,
	Long: strings.ReplaceAll(`
Backs up the directory tree at the local path source to dest:path in the
GDA format.

Each directory gets a !gda-index.csv! describing its current contents,
and each run that changes a directory adds a changeset CSV next to it.
Files smaller than !--standalone-min! are packed into tar files of at
most !--pack-size!, one set of packs per directory; bigger files are
stored as their own objects under their own names. A subtree smaller
than !--rollup-max! in total is packed as one unit with its
subdirectories.

Packs and standalone files are uploaded with the storage class
!--data-tier! (default DEEP_ARCHIVE), CSV files with !--meta-tier!
(default STANDARD). Don't set !storage_class! on the destination remote,
as it would override these; the run stops if it finds objects stored
with the wrong class.

For S3, set !--s3-upload-cutoff! above !--pack-size! so that each pack
is uploaded in one request checked against its MD5, and consider
!--s3-no-check-bucket! when the bucket exists.

Runs are incremental: only new and changed files are uploaded, and
nothing already uploaded is overwritten or deleted. Files whose
modification time changed but whose content didn't are recorded without
uploading them again.

The source must be a local path, as it is read directly with POSIX
calls to keep owners, permissions, symlinks and special files.
`, "!", "`"),
	Annotations: map[string]string{
		"versionIntroduced": "v1.76",
	},
	RunE: func(command *cobra.Command, args []string) error {
		cmd.CheckArgs(2, 2, command, args)
		src := args[0]
		if info, err := os.Stat(src); err != nil || !info.IsDir() {
			return fmt.Errorf("source %q must be a local directory", src)
		}
		dst := cmd.NewFsDir(args[1:2])
		opt.PackSize = int64(packSize)
		opt.StandaloneMin = int64(standaloneMin)
		opt.RollupMax = int64(rollupMax)
		cmd.Run(false, true, command, func() error {
			ledger, err := libgda.Backup(context.Background(), src, dst, opt)
			if ledger != nil {
				s := ledger.Stats
				fs.Logf(nil, "gda: run %s: %d dirs indexed, %d added, %d modified, %d metadata only, %d deleted, %d unchanged; %d packs (%s), %d standalone (%s); %d skipped, %d deferred, %d errors",
					ledger.RunID, s.IndexedDirs, s.Added, s.Modified, s.MetaOnly, s.Deleted, s.Unchanged,
					s.Packs, fs.SizeSuffix(s.PackBytes), s.Standalone, fs.SizeSuffix(s.StandaloneBytes),
					s.Skipped, s.Deferred, s.Errors)
			}
			return err
		})
		return nil
	},
}
