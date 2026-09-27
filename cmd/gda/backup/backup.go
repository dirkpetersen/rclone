//go:build unix

// Package backup implements 'rclone gda backup'.
package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/gda"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/flags"
	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	opt           = libgda.DefaultOptions()
	packSize      = fs.SizeSuffix(opt.PackSize)
	standaloneMin = fs.SizeSuffix(opt.StandaloneMin)
	rollupMax     = fs.SizeSuffix(opt.RollupMax)
	dedupMin      = fs.SizeSuffix(opt.DedupMin)
	compressMax   = fs.SizeSuffix(opt.CompressMax)
	pricesFile    = ""
	indexCache    = ""
)

func init() {
	opt.Workers = min(runtime.NumCPU(), 15)
	addFlags(Command.Flags())
	flagSet := Command.Flags()
	flags.StringVarP(flagSet, &runID, "run", "", runID, "Back up a partition of this planned run (see rclone gda plan)", "")
	flags.IntVarP(flagSet, &partitionIndex, "partition", "", partitionIndex, "Index of the worker whose partitions to back up with --run", "")
	flags.StringVarP(flagSet, &changesFrom, "changes-from", "", changesFrom, "Back up only the directories of the paths in this file (- for stdin)", "")
	flags.StringVarP(flagSet, &changesFormat, "changes-format", "", changesFormat, "Format of --changes-from: lines, or zfs for zfs diff -H output", "")
	gda.Command.AddCommand(Command)
}

var (
	runID          = ""
	partitionIndex = -1
	changesFrom    = ""
	changesFormat  = libgda.ChangesLines
)

// readChanges reads the change list given with --changes-from.
func readChanges(src string) ([]string, error) {
	in := os.Stdin
	if changesFrom != "-" {
		f, err := os.Open(changesFrom)
		if err != nil {
			return nil, err
		}
		defer func() { _ = f.Close() }()
		in = f
	}
	changes, err := libgda.ParseChanges(in, changesFormat, src)
	if err != nil {
		return nil, err
	}
	if len(changes) == 0 {
		return nil, errors.New("the change list has no paths below the source")
	}
	return changes, nil
}

// addFlags adds the options which shape a backup to flagSet.
func addFlags(flagSet *pflag.FlagSet) {
	flags.FVarP(flagSet, &packSize, "pack-size", "", "Maximum size of a pack of small files", "")
	flags.FVarP(flagSet, &standaloneMin, "standalone-min", "", "Store files at least this big as their own objects", "")
	flags.FVarP(flagSet, &rollupMax, "rollup-max", "", "Pack whole subtrees smaller than this as one unit (0 to disable)", "")
	flags.FVarP(flagSet, &dedupMin, "dedup-min", "", "Store identical files at least this big once (off to disable)", "")
	flags.StringVarP(flagSet, &opt.DataTier, "data-tier", "", opt.DataTier, "Storage class for packs and standalone files", "")
	flags.StringVarP(flagSet, &opt.MetaTier, "meta-tier", "", opt.MetaTier, "Storage class for changesets, indexes and run files", "")
	flags.StringVarP(flagSet, &opt.Worker, "worker", "", opt.Worker, "Worker ID used in pack names (letters, digits and _)", "")
	flags.StringVarP(flagSet, &opt.TempDir, "temp-dir", "", opt.TempDir, "Directory for pack spool files (default system temp directory)", "")
	flags.StringVarP(flagSet, &opt.RootLabel, "root-label", "", opt.RootLabel, "Name used for the top directory in pack names (default last element of the destination)", "")
	flags.DurationVarP(flagSet, &opt.LockTimeout, "lock-timeout", "", opt.LockTimeout, "Take over a destination lock older than this", "")
	flags.IntVarP(flagSet, &opt.Retries, "upload-retries", "", opt.Retries, "Upload attempts per object", "")
	flags.StringVarP(flagSet, &opt.Compression, "compression", "", opt.Compression, "Compress data where it helps with zstd, or none", "")
	flags.IntVarP(flagSet, &opt.Level, "compression-level", "", opt.Level, "zstd compression level, 1 to 22", "")
	flags.FVarP(flagSet, &compressMax, "compress-max", "", "Store standalone files bigger than this uncompressed", "")
	flags.BoolVarP(flagSet, &opt.Checksum, "checksum", "", opt.Checksum, "Read files whose size and time are unchanged to compare their MD5 too", "")
	flags.BoolVarP(flagSet, &opt.AllowEmpty, "allow-empty", "", opt.AllowEmpty, "Back up an empty source over a backup which isn't empty", "")
	flags.BoolVarP(flagSet, &opt.Xattrs, "xattrs", "", opt.Xattrs, "Keep extended attributes, including ACLs (Linux only)", "")
	flags.StringVarP(flagSet, &pricesFile, "prices", "", pricesFile, "JSON file with the prices to use for the cost estimate (default built in)", "")
	flags.StringVarP(flagSet, &indexCache, "index-cache-dir", "", indexCache, "Directory for local copies of the destination's indexes, or off (default gda in rclone's cache directory)", "")
	flags.IntVarP(flagSet, &opt.Workers, "workers", "", opt.Workers, "Directories to back up in parallel (default one per CPU, up to 15)", "")
}

// setSizes copies the size flags into opt.
func setSizes() {
	opt.PackSize = int64(packSize)
	opt.StandaloneMin = int64(standaloneMin)
	opt.RollupMax = int64(rollupMax)
	opt.DedupMin = int64(dedupMin)
	opt.CompressMax = int64(compressMax)
	switch indexCache {
	case "off":
		opt.IndexCache = ""
	case "":
		opt.IndexCache = filepath.Join(config.GetCacheDir(), "gda")
	default:
		opt.IndexCache = indexCache
	}
}

// logLedger logs what a run did.
func logLedger(ledger *libgda.Ledger) {
	if ledger == nil {
		return
	}
	s := ledger.Stats
	fs.Logf(nil, "gda: run %s: %d dirs indexed, %d added, %d modified, %d metadata only, %d deleted, %d unchanged, %d rebased; %d packs (%s), %d standalone (%s), %s compressed, %d deduplicated (%s); %d skipped, %d deferred, %d errors",
		ledger.RunID, s.IndexedDirs, s.Added, s.Modified, s.MetaOnly, s.Deleted, s.Unchanged, s.Rebased,
		s.Packs, fs.SizeSuffix(s.PackBytes), s.Standalone, fs.SizeSuffix(s.StandaloneBytes), fs.SizeSuffix(s.CompressedFrom),
		s.Deduplicated, fs.SizeSuffix(s.DeduplicatedBytes), s.Skipped, s.Deferred, s.Errors)
	prices := libgda.DefaultPrices()
	if pricesFile != "" {
		var err error
		if prices, err = libgda.LoadPrices(pricesFile); err != nil {
			fs.Errorf(nil, "gda: %v", err)
			return
		}
	}
	cost, ok := prices.EstimateBackup(s.Packs+s.Standalone, s.PackBytes+s.StandaloneBytes, s.MetaObjects, ledger.Options.DataTier, ledger.Options.MetaTier)
	if !ok {
		fs.Debugf(nil, "gda: no prices for storage class %s or %s, so no cost estimate", ledger.Options.DataTier, ledger.Options.MetaTier)
		return
	}
	minimum := ""
	if cost.MinDays > 0 {
		minimum = fmt.Sprintf(", for at least %d days", cost.MinDays)
	}
	fs.Logf(nil, "gda: run %s: estimated cost %.2f %s for uploads, and %.2f %s per month to store the data it added%s (prices of %s)",
		ledger.RunID, float64(cost.Requests), cost.Currency, float64(cost.Monthly), cost.Currency, minimum, prices.Date)
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
stored as their own objects under their own names. When such a file
changes, the new copy is stored as !<name>.gda.<run>.<worker>!, or, in
an S3 bucket with versioning enabled, under its own name as a new
version, whose ID its row records. A subtree smaller
than !--rollup-max! in total is packed as one unit with its
subdirectories.

Packs and standalone files are uploaded with the storage class
!--data-tier! (default DEEP_ARCHIVE), CSV files with !--meta-tier!
(default STANDARD). Don't set !storage_class! on the destination remote,
as it would override these; the run stops if it finds objects stored
with the wrong class.

For S3, unless they are set, !--s3-upload-cutoff! is set above
!--pack-size!, so that each pack is uploaded in one request checked
against its MD5, and !--s3-chunk-size! to 64 MiB, so that large files
are uploaded in few parts; each worker may then hold four parts, 256
MiB, in memory. Consider !--s3-no-check-bucket! when the bucket exists.
This isn't done for S3 behind another remote, such as crypt, so set
them yourself there. Workers of a split run should be given the plan's
!--pack-size!, as the upload cutoff is set before the plan is read.

Data is compressed with zstd where it helps: a pack is compressed when
a trial compression of its files' data saves at least 10%, and a
standalone file when its name doesn't show a compressed format (such as
.gz, .bam or .jpg) and its first MiB compresses by 10%. Compressed data
is written as independent frames, so a single file can still be read
without the rest of its pack, and a compressed pack is a normal
!.tar.zst! file. !--compression none! turns this off.

A compressed standalone file is uploaded as a stream whose size isn't
known in advance, which S3 limits to 10,000 parts of !--s3-chunk-size!,
625 GiB with the 64 MiB a backup uses unless it is set. Files bigger
than !--compress-max! (default 32 GiB) are stored uncompressed; raise it
to compress bigger ones, keeping it below that limit.

!--workers! directories are scanned and backed up in parallel. Each
directory is handled by one worker, whose ID is part of the names of
the packs it writes, so workers never write the same object. Each
worker may have a pack of up to !--pack-size! in !--temp-dir! at once,
and a compressed copy of it, so allow twice !--pack-size! per worker.

To split a run over several hosts, plan it with !rclone gda plan!,
which prints its run ID, run one !rclone gda backup --run ID --partition
N! per worker, each with its own !--worker! ID of at most 16 bytes, and
end it with !rclone gda finish!, which fails until every partition has
finished. A partition which failed or didn't finish can be run again
with the same !--worker! ID; the objects it writes then get a suffix
such as !-r2!. For example with a Slurm job array:

    RUN=$(rclone gda plan /data s3:bucket/lab --partitions 20)
    sbatch --array=0-19 --wrap "rclone gda backup /data s3:bucket/lab \
        --run $RUN --partition \$SLURM_ARRAY_TASK_ID --worker p\$SLURM_ARRAY_TASK_ID"
    rclone gda finish s3:bucket/lab --run $RUN   # once all tasks are done

With !--changes-from! a run looks only at the directories holding the
listed paths, and their parents, reading the rest from the previous
indexes, so it doesn't have to scan the whole source. The list can come
from any change feed: one path per line, absolute or relative to the
source, or the output of !zfs diff -H! with !--changes-format zfs!:

    zfs diff -H pool/data@yesterday pool/data@today | \
        rclone gda backup /pool/data/.zfs/snapshot/today s3:bucket/lab \
        --changes-from - --changes-format zfs

Backing up the snapshot rather than the live file system means the
files don't change while they are read. zfs diff names paths where the
file system is mounted, which a source in a snapshot is taken to be.

GPFS policy lists and Lustre changelogs can be turned into a list of
paths. A change run needs a full run first, and full runs should still
be made now and then to catch anything a change feed missed.

Identical files of at least !--dedup-min! (default 1 MiB) are stored
once per destination: a copy of content already stored, for example in
a renamed or copied directory, is recorded in the index as referring to
the stored copy instead of being uploaded again. Only files of a size
some stored copy has are read to check.

A run keeps a copy of every index it reads or writes in
!--index-cache-dir! (!off! to disable). The next run uses the copies
instead of reading the indexes again if no other run has written to the
destination since, which saves a request per directory; otherwise it
starts the cache afresh. Runs split over several hosts don't use it.
Don't edit indexes by hand while a cache holds them.

When a directory's unchanged files are spread over more than 20 packs,
and over more than twice the packs they would fill, as happens after
many small changes, the run packs them again from the source, so
restoring the directory needs fewer objects. The old packs are kept for
the history.

Each run logs an estimate of what it cost in upload requests and adds
to the monthly storage bill, from the built in prices or !--prices!
(see !rclone gda prices!); with !--dry-run! this estimates a backup
before making it, counting data before compression and deduplication,
so as an upper bound. Uploads are counted as one request per object,
which they are for packs when !--s3-upload-cutoff! is above
!--pack-size!; each part of a multipart upload is charged too. Metadata
storage and the requests of reading indexes are small and not counted.

Only one run writes to a destination at a time: a run holds a lock,
which it refreshes every hour. A run which is killed leaves its lock
behind, and the next run takes it over once it is older than
!--lock-timeout! (default 24 hours); for nightly runs, 2 hours is
enough, as a running run refreshes its lock.

A run refuses to back up an empty source over a backup which isn't
empty, as that is usually a file system which isn't mounted; use
!--allow-empty! if the source really was emptied (with !rclone gda
plan! for a split run).

Runs are incremental: only new and changed files are uploaded, and
nothing already uploaded is overwritten or deleted. Files whose
modification time changed but whose content didn't are recorded without
uploading them again. A file whose content changed while its size and
time didn't is only found with !--checksum!, which reads every file.

The source must be a local path, as it is read directly with POSIX
calls to keep owners, permissions, symlinks, hard links and special
files. With !--xattrs! it keeps extended attributes too, which on Linux
include POSIX and NFSv4 ACLs; this costs a request or two per file on
network file systems. Restores set them where the target file system
and the user's rights allow.
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
		setSizes()
		dst := cmd.NewFsDir([]string{tuneS3(args[1], opt.PackSize)})
		if (runID == "") != (partitionIndex < 0) {
			return errors.New("--run and --partition go together")
		}
		if changesFrom != "" {
			if runID != "" {
				return errors.New("--changes-from can't be used with --run")
			}
			changes, err := readChanges(src)
			if err != nil {
				return err
			}
			opt.Changes = changes
		}
		cmd.Run(false, true, command, func() error {
			var ledger *libgda.Ledger
			var err error
			if runID != "" {
				ledger, err = libgda.BackupPartition(context.Background(), src, dst, opt, runID, partitionIndex)
			} else {
				ledger, err = libgda.Backup(context.Background(), src, dst, opt)
			}
			logLedger(ledger)
			return err
		})
		return nil
	},
}
