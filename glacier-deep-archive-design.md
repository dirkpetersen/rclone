# Design: low-cost backup and archiving to S3 Glacier Deep Archive

Status: draft for discussion, 2026-09-26.

This design builds on
[glacier-deep-archive-analysis.md](glacier-deep-archive-analysis.md), which
describes what rclone does and doesn't do for Deep Archive today. It borrows
two ideas from [Froster](https://github.com/dirkpetersen/froster):

- archive **one directory level at a time**, never including subfolders;
- keep a **CSV manifest** of every file next to the archived data.

It fixes Froster's main shortcoming: only files under 1 MiB are packed, into
a single `Froster.smallfiles.tar` of unbounded size. Here every file belongs
either to a bounded-size archive or is stored as its own object, and the
format supports repeated backup runs as well as one-shot archiving.

Working name for the tool and format: **GDA**.

**Target scale:** many petabytes and about 10 billion files, written by many
parallel workers (10 to 15 per host, on several hosts). Every part of the
design has to work at that scale from the start; see
[Scale and parallel workers](#scale-and-parallel-workers).

## Goals

- **Lowest total cost** for data that is written once and rarely read:
  requests, storage, per-object overhead, early deletion and restores.
- **Browsable without special tools.** A person with the AWS console, S3
  Browser, Cyberduck or `aws s3 ls` can see what is archived and where,
  without restoring anything.
- **Restorable without GDA.** Plain `tar`, a CSV and the AWS CLI are enough
  to get any file back.
- **Small restore units.** Restoring one file or one directory restores only
  a few bounded-size objects.
- **Incremental backup.** Repeated runs upload only what changed and never
  overwrite or delete objects that are already in Deep Archive.
- **Scale.** Many petabytes, about 10 billion files, many parallel workers.
- **Store identical large files once** (file-level deduplication, see
  [Deduplication](#deduplication)).

Non-goals: block-level deduplication inside files (use restic, Kopia or Borg
for that; see [Alternatives considered](#alternatives-considered)), and
workloads that need restores in less than 12 hours.

## Cost principles

These come from the analysis and drive every decision below.

1. **Requests, not storage, dominate for small files.** A Deep Archive PUT
   costs $0.00005, the same as 180 days of storage for about 8 MB.
2. **Never overwrite or delete a Deep Archive object before 180 days.**
   Early deletion is billed for the remainder. The design is append-only.
3. **Restore cost is per object plus per GB.** A Bulk restore is
   $0.025 per 1,000 objects plus $0.0025 per GB, so fewer, larger objects
   restore more cheaply.
4. **Small, frequently rewritten metadata belongs in S3 Standard.** Glacier
   Instant Retrieval and Standard-IA bill at least 128 KB per object and
   have 90- and 30-day minimums. Intelligent-Tiering never tiers objects
   under 128 KB. Rewriting a small index in Standard costs a $0.000005 PUT
   and nothing else.
5. **Decide from metadata, never from Deep Archive.** Change detection,
   browsing and restore planning read only the hot manifests.

## What research file systems look like

The defaults below come from real research-computing data rather than
guesses. The best data available is the census of the OSU CGRB NFS filers
(September 2026, in `oregonstate-ai/cgrb-drpro-linux/docs/rclone-plan.md`
section 10.3.1):

| Statistic | Value |
|---|---|
| Total | 3.2 PiB in 3.69 billion files |
| Mean file size | about 933 KB |
| **Median file size** | **610 B** (per filer: 400 B, 755 B, 819 B) |
| Files of 64 KiB or less | **96% of files, 0.31% of bytes** (about 10.2 TiB) |
| Files over 4 GiB | 114,914 files, **54% of bytes** (about 1.73 PiB) |

What this means for the design:

1. **The mean is misleading.** It is about 1,500 times the median and sits
   where almost no files are. Every default here is based on the
   distribution.
2. **The number of directories, not the number of bytes, sets the cost.**
   With a median of 610 B, the small files in a typical directory add up to
   kilobytes. Most packs will therefore be far below `pack-size`, and each
   still costs one PUT plus 40 KB of overhead, which is more than its
   content. How many directories contain small files is the most important
   number the census doesn't have yet (see [Profiling a file
   system](#profiling-a-file-system)).
3. **The bytes sit in a few huge files.** Files over 4 GiB hold more than
   half the data in about 115,000 objects. For them, the number of multipart
   parts matters more than packing: GDA uploads standalone files with large
   parts (at least 512 MiB) instead of rclone's 5 MiB default (analysis,
   gap 4).
4. **Tiny directory trees need rolling up.** When a whole subtree holds only
   a few KB, one pack per directory level still costs one request per
   directory. See [Rolling up small subtrees](#rolling-up-small-subtrees).

The drpro cloud tier targets about 10% of this estate: roughly 320 TiB in
369 million files. Pack requests for that subset, depending on how many
small files a directory holds on average:

| Small files per directory | Directories | Pack PUTs (Deep Archive) | Changeset and index PUTs (Standard) |
|---|---|---|---|
| 20 | 18.5 million | $923 | $185 |
| 100 | 3.7 million | $185 | $37 |
| 1,000 | 369,000 | $18 | $4 |
| For comparison: one PUT per file | | **$18,450** | |

### Defaults

| Setting | Default | Reason |
|---|---|---|
| `standalone-min` | 64 MiB | Above it, a file's own PUT and overhead are at most about 2% of its 3-year storage cost, so packing gains little |
| `pack-size` | 256 MiB | Rarely reached at these file sizes. It bounds temp space and the restore unit for directories with many mid-size files |
| `rollup-max` | 16 MiB | Twice the 8 MB break-even where one PUT costs as much as 180 days of storage |
| Standalone part size | at least 512 MiB | Cuts a 100 GiB upload from about 9,300 requests to about 200 |

All four are configurable. They should be revisited once a profile of the
target file system exists.

## Overview

For each source directory, one run produces up to four kinds of object:

| Object | Content | Storage class | Mutable? |
|---|---|---|---|
| **Pack** `<dir>.gda.<run>.<worker>.<part>.tar[.zst]` | Small files from this directory only, at most `pack-size` | DEEP_ARCHIVE | No, never rewritten |
| **Standalone file** `<original name>` | One large file, stored under its own name | DEEP_ARCHIVE | Only via bucket versioning (see below) |
| **Changeset** `<dir>.gda.<run>.csv` | What this run changed in this directory: added, modified and deleted entries, and where each lives | STANDARD | No, immutable |
| **Index** `gda-index.csv` | Current state of the directory: every live entry and where it lives | STANDARD | Yes, regenerated each run that changes the directory |

Plus, once per run and once globally:

| Object | Content | Storage class |
|---|---|---|
| **Run ledger** `_gda/runs/<run>/<worker>.json` | Run parameters, directories touched, counts, bytes, estimated cost | STANDARD |
| **Catalog** `_gda/catalog/*.parquet` (optional) | Every index of the tree combined, for search | STANDARD |

### Example layout

Source:

```text
/data/lab/
├── README.txt                 2 KB
├── results/
│   ├── summary.csv            40 KB
│   ├── plot-001.png ... plot-900.png    900 files, 600 KB each
│   └── model.bin              3 GB
└── raw/
    └── run1.bam               120 GB
```

Bucket after the first run `20260926T120000Z` with `pack-size=256MiB` and
`standalone-min=64MiB`:

```text
s3://bucket/lab/
├── lab.gda.20260926T120000Z.w01.001.tar.zst     DEEP_ARCHIVE  (README.txt, compressed)
├── lab.gda.20260926T120000Z.csv             STANDARD
├── gda-index.csv                            STANDARD
├── results/
│   ├── results.gda.20260926T120000Z.w01.001.tar DEEP_ARCHIVE  (summary.csv, plot-001..plot-436)
│   ├── results.gda.20260926T120000Z.w01.002.tar DEEP_ARCHIVE  (plot-437..plot-873)
│   ├── results.gda.20260926T120000Z.w01.003.tar DEEP_ARCHIVE  (plot-874..plot-900)
│   ├── model.bin                            DEEP_ARCHIVE  (standalone)
│   ├── results.gda.20260926T120000Z.csv     STANDARD
│   └── gda-index.csv                        STANDARD
├── raw/
│   ├── run1.bam                             DEEP_ARCHIVE  (standalone)
│   ├── raw.gda.20260926T120000Z.csv         STANDARD
│   └── gda-index.csv                        STANDARD
└── _gda/
    └── runs/20260926T120000Z/w01.json       STANDARD
```

The tree mirrors the source, so browsing the bucket feels like browsing the
file system. Large files keep their own names and can be restored with any
S3 tool.

## Packing rules

### Which files are packed

- **Scope:** the regular files, symlinks and special files directly in one
  directory. Subdirectories are never included; they get their own packs.
- **Standalone files:** a file of at least `standalone-min` (default
  64 MiB) is uploaded as its own object under its original name. Packing it
  would save at most one PUT (about 2% of its 3-year storage cost) and would
  lose the native name and direct restore.
- **Packed files:** everything smaller goes into packs.

### Rolling up small subtrees

A directory whose **entire subtree** holds less than `rollup-max` (default
16 MiB) and no standalone files is packed as **one unit**, subdirectories
included. The pack sits at the highest such directory, the rollup root, and
its member names keep their relative paths (`sub/dir/file.txt`).

- **Why this doesn't break the one-level rule's purpose:** that rule exists to
  keep restore units small. A rolled-up subtree is small by definition, so
  restoring it is as cheap as restoring one directory.
- **What it saves:** a tree of 10,000 directories with a few KB each costs
  10,000 PUTs and 20,000 metadata PUTs without rolling up, but one PUT and
  two metadata PUTs with it.
- **Browsing:** subdirectories inside a rollup have no `gda-index.csv` of
  their own. Their row in the parent index says `rollup` in the `listing`
  column, and a browser reads the rollup root's index filtered by path
  prefix.
- **Backups:** a change anywhere in the subtree adds a delta pack at the
  rollup root. If the subtree grows past `rollup-max`, later runs treat its
  directories individually. Existing packs are left alone, and the indexes
  show where each file lives.

### Pack size

A pack is closed before adding a file would take it over `pack-size`. The
PUT for a pack as a share of that pack's storage cost:

| `pack-size` | vs 180-day minimum | vs 3 years |
|---|---|---|
| 128 MiB | 6.3% | 1.1% |
| **256 MiB (default)** | **3.1%** | **0.5%** |
| 512 MiB | 1.6% | 0.3% |
| 1 GiB | 0.8% | 0.1% |

- **256 MiB is the recommended default.** Beyond it the savings are
  fractions of a percent, while temp space, retry cost and restore
  granularity keep growing.
- **Upload with one PUT per pack:** set rclone's `upload_cutoff` above
  `pack-size`. Multipart would add requests for no benefit at this size.
- **Directory count sets the cost floor.** Because packs never span
  directories, the minimum is one pack per directory that holds small files.
  In most trees, directories rather than `pack-size` determine the object
  count, and most packs are much smaller than the limit.
- **Fill order:** files sorted by name, filled sequentially. This is
  deterministic, easy to reason about, and puts neighbouring files, which are
  often restored together, in the same pack.

### Pack format

- **Plain POSIX tar (PAX).** Every Linux system can read it, and it keeps
  nanosecond mtimes, long names, owner, group, mode, symlinks and optionally
  xattrs. Member names are just the file names, as in Froster.
- **Compressed with zstd where it helps** (see [Compression](#compression)).
  A compressed pack is a normal `.tar.zst` file.
- **The last member is the pack's own manifest** (`<pack>.csv`). Each pack is
  therefore self-describing even if every hot manifest is lost.
- **Byte offsets in the manifest.** The manifest records where each member's
  bytes are stored in the pack. After a restore, a single file can be fetched
  with a ranged GET (`aws s3api get-object --range bytes=a-b` or
  `rclone cat --offset --count`), then decompressed if needed, instead of
  downloading the whole pack. The restore still covers the whole pack, but
  download and egress cover only the file.
- **Zip is the main alternative.** It has per-member random access built in,
  and rclone's `:archive:` backend can browse zip files after restore. Tar
  plus recorded offsets gives the same random access with better POSIX
  metadata, and matches Froster, so tar was chosen (see [Decisions](#decisions)).

### Naming

`<dirname>.gda.<run>.<worker>.<part>.tar`, plus `.zst` when compressed, for
example `results.gda.20260926T120000Z.w07.002.tar.zst`.

- **`dirname` in the name** makes a downloaded pack recognisable on its own,
  as in the `foldername.1.tar` idea.
- **`run` is the UTC start time of the run.** Names are never reused, so
  nothing is ever overwritten; no counter has to be read and updated; and the
  name shows the pack's age, which matters for the 180-day rule.
- **`worker` is the worker ID**, so parallel workers can never pick the same
  name, even for a directory that is handed from one worker to another after
  a crash.
- **Compressed standalone files** are stored as `<name>.gda.zst`, for example
  `run1.fastq.gda.zst`.
- **`.gda.` is a reserved infix.** A source file whose name already matches
  `*.gda.*.tar` or `*.gda.*.csv`, or is `gda-index.csv`, is reported as a
  conflict rather than silently shadowed.
- **The top-level directory** uses the last component of the destination
  path as its `dirname`, or `root` if the destination is the bucket itself.

## Manifests

The Froster idea of one CSV per directory is kept, but split into an
immutable history and a regenerated current view. This is the answer to
"many manifests are hard to browse, but one editable CSV doesn't scale and
can't be written safely in parallel".

### Changeset: `<dir>.gda.<run>.csv` (immutable)

One per directory per run that changed something in it. It lists every
entry added, modified or deleted in that run, including deletions, which
have no pack to live in.

**Why one per directory per run, not one per pack:** a run can produce
several packs, standalone files and deletions for the same directory. One
changeset holds them all and is written last, so it is the atomic commit
record for that directory (see [Crash safety](#commit-protocol-and-crash-safety)).
Each pack still has its own manifest embedded as the last tar member.

### Index: `gda-index.csv` (regenerated)

This is the current state of the directory, one row per live entry. It is
exactly the "regenerate the CSV after each manifest" idea:

- It is **derived**: previous index plus the new changeset gives the new
  index, and it can always be rebuilt from scratch by replaying all
  changesets in run order.
- It is **never edited in place**. It is written whole, by the one process
  handling that directory, after the changeset. Readers see either the old or
  the new version.
- It has a **fixed name**, so every directory in the bucket has a
  `gda-index.csv` to open.
- It also lists **subdirectories** (type `dir`), so the whole tree can be
  browsed through indexes alone and empty directories can be recreated.

### Columns

The same columns are used in changesets, indexes and embedded pack
manifests.

| Column | Example | Notes |
|---|---|---|
| `name` | `plot-001.png` | File name only; the directory is implied by the location |
| `type` | `file` | `file`, `symlink`, `dir`, `special` |
| `size` | `614400` | Bytes |
| `mtime` | `2026-09-20T14:03:11.123456789Z` | UTC, RFC 3339, nanoseconds |
| `mode` | `0644` | Octal permission bits |
| `owner`, `group` | `jdoe`, `lab` | Names, with numeric uid and gid in `uid`, `gid` |
| `md5` | `9e107d9d...` | Hex; matches S3 Content-MD5 and rclone's S3 hash |
| `link_target` | | Symlinks only |
| `location` | `results.gda.20260926T120000Z.w01.002.tar` | Pack name, or the object key for a standalone file |
| `offset` | `1536` | Data offset inside the uncompressed tar; empty for standalone files |
| `codec` | `zstd` | `none` or `zstd` |
| `stored_offset`, `stored_length` | `1048576`, `2097152` | Byte range of the compressed frames holding this file, for a ranged GET |
| `stored_size`, `stored_md5` | | Standalone objects only: size and MD5 of the bytes actually stored |
| `dedup_of` | `../a/a.gda.20260926T120000Z.w01.001.tar` | Set when this file is stored once elsewhere: the object holding the copy, relative to this index's directory like `location`, with the copy's offsets in the offset columns |
| `version_id` | | S3 version ID for standalone files in a versioned bucket |
| `run` | `20260926T120000Z` | Run that wrote this version |
| `tree_size`, `tree_files` | `52428800`, `913` | Directory rows only: total bytes and files in the whole subtree, so a browser can show folder sizes without walking |
| `listing` | `index` | Directory rows only: `index` (has its own `gda-index.csv`) or `rollup` (listed in this index by path prefix) |
| `hard_link` | `fd01:1a2b3c` | Files with more than one link: device and inode, the same for every link |
| `action` | `add` | Changesets only: `add`, `modify`, `meta`, `delete`, `rebase` (unchanged content packed again) |

Encoding: RFC 4180 CSV, UTF-8, header row, fields quoted when needed. This
handles commas, quotes and newlines in names. File names that aren't valid
UTF-8, or contain `%` or ASCII control characters, are percent-encoded,
with a `name_encoding` column to mark them. (Control characters were added
on 2026-09-27, as backends map them differently in object names; files
named with them in older backups are stored again once.)

### Why CSV, and where it stops scaling

| Format | Browser and spreadsheet friendly | Scales | Notes |
|---|---|---|---|
| **CSV** | Yes | Per directory: yes | Excel stops at 1,048,576 rows |
| JSON Lines | Partly | Per directory: yes | Better for nested fields; nothing needs nesting here |
| Parquet | No | Yes, columnar and compressed | Ideal for tree-wide search |
| SQLite | No | Yes | Single file, must be downloaded; good as a local cache |

- **Per-directory CSVs are the source of truth** (decided 2026-09-26). CSV
  only has to scale per directory, because each index covers one directory.
- **Big directories get a split index.** Parsing a million rows (about
  200 MB) takes only a second or two, but downloading 200 MB on every Motuz
  click, or rewriting it on every run that changes two files, doesn't work.
  Above 100,000 rows (about 20 MB):
  - the entries go into name-sorted parts `gda-index.00001.csv`,
    `gda-index.00002.csv` and so on, each up to 100,000 rows;
  - `gda-index.csv` becomes a short table of contents with each part's name,
    first and last entry name, and row count;
  - readers fetch only the part they need, and a run rewrites only the parts
    that changed. A person still starts at `gda-index.csv`.
- **Tree-wide questions use the Parquet catalog, not the CSVs.** Globbing a
  few thousand CSVs with DuckDB works for one lab:

  ```text
  SELECT * FROM read_csv('s3://bucket/lab/**/gda-index.csv', filename=true)
  WHERE name LIKE '%.bam' AND size > 1e9;
  ```

  At 10 billion files there are hundreds of millions of index files, and
  just listing them takes hours. So every worker also writes the rows of its
  changesets, with full paths and object keys, to one file per run,
  `_gda/catalog/runs/<run>/<worker>.csv.zst` (implemented). The catalog is
  the union of those files; the current state is the latest row per path
  which isn't a deletion:

  ```text
  SELECT path, size, object FROM (
    SELECT *, row_number() OVER (PARTITION BY path ORDER BY run DESC) AS n
    FROM read_csv('s3://bucket/lab/_gda/catalog/runs/*/*.csv.zst')
  ) WHERE n = 1 AND action <> 'delete';
  ```

  The files are zstd compressed CSV because rclone's Parquet writer would
  add a new dependency (Apache Thrift); switching to Parquet, and
  compacting the files sorted by path, are open. The catalog is derived
  and can always be rebuilt from the changesets.
- **Cost of keeping all metadata hot:** 10 million files make about 2 GB of
  indexes plus a similar amount of changesets, roughly $0.09 per month in S3
  Standard. At 10 billion files it is about 4 TB, roughly $90 per month.

## Incremental backup

### Change detection

Each run compares the source against that directory's `gda-index.csv`,
never against Deep Archive. At 10 billion files a full scan of the sources
takes days, so where the source file system can report its own changes, runs
use that instead:

| Source | How changes are found |
|---|---|
| ZFS | `zfs diff` between the snapshot of the last run and a new snapshot |
| GPFS / Spectrum Scale | Policy engine (`mmapplypolicy`) list of files modified since the last run; it scans metadata at millions of files per second |
| Lustre | Changelogs, consumed from the last recorded position |
| NFS, Ceph FS and others | Parallel scan (pwalk style) across many workers |

- **A full reconciliation scan still runs periodically** (for example
  quarterly) on every source, to catch anything a change feed missed.
- **How change runs work** (implemented): `--changes-from` takes one path
  per line or `zfs diff -H` output. The directories holding the listed
  paths and all their ancestors are read from the source; every other
  directory's subtree totals come from its row in the previous index, and
  it isn't read at all. A directory missing from the indexes is new and
  is scanned in full. A directory which has its own index keeps it during
  change runs even if it has shrunk enough to be rolled up; the next full
  run makes that change.
- **Local index cache:** each worker keeps copies of the indexes it owns,
  so a run doesn't download millions of indexes just to compare them. As
  implemented, the copies are trusted when the runs on the destination are
  the ones there when the last run with the cache finished (compared as a
  digest of the run IDs, as another host's clock may be behind), as then no
  other run has written indexes since; otherwise the cache starts afresh.
  This needs one listing of `_gda/runs` rather than a request per index.
  Runs split over several hosts don't use it yet.
- **Back up from snapshots** wherever the file system has them. Otherwise a
  run lasting hours captures different files at different moments.

For each changed entry:

| Source compared with index | Action |
|---|---|
| New name | `add`: pack it, or upload standalone |
| Size differs | `modify`: new copy in a new pack, or new standalone version |
| Same size, mtime differs | Hash the source file (local read, free). Same MD5 → `meta` row, nothing uploaded. Different MD5 → `modify` |
| Same size and mtime | Nothing (an optional `--checksum` mode hashes anyway) |
| In index, missing from source | `delete`: tombstone row only; nothing is deleted from S3 |
| Mode, owner or group differ | `meta` row only |

The `meta` case avoids the rclone trap from analysis section 1: a timestamp
change on an unchanged file never re-uploads data.

### "Two small files changed in a directory with one big pack"

The two files go into a **new small pack** for that run,
`results.gda.<run2>.w01.001.tar`. The index now points those two names at the new
pack and every other name at the old pack. The old pack is never touched:

- no rewrite, so no re-upload of 256 MiB and no early-deletion charge;
- the old versions of the two files stay in the old pack, so the backup keeps
  history for free;
- restoring the directory restores exactly the packs the index points to,
  here the old pack and the new one.

Cost of that run for this directory: one Deep Archive PUT ($0.00005), one
changeset and one index PUT in Standard ($0.00001), and 40 KB of per-object
overhead.

### Many small deltas

Frequent runs over large trees create many small packs. To size this:
nightly runs where 1,000 directories change produce about 1,000 packs and
2,000 Standard PUTs, about **$0.06 per night or $22 per year**. For most
installations this is acceptable. Three levers if it isn't:

1. **Cadence.** Deep Archive restores take 12 to 48 hours, so it suits
   weekly or monthly archive runs, not an hourly backup tier. A faster tier
   in front covers recent changes.
2. **Minimum delta size with a hot staging area.** Deltas smaller than
   `delta-min` (for example 8 MiB) go to Standard under
   `<dir>.gda.<run>.001.stage.tar`. A later run folds the staged packs of a
   directory into one Deep Archive pack once they exceed `delta-min` or reach
   30 days of age, then deletes them (no minimum-duration charge in
   Standard).
3. **Compaction** (below). Rarely worth it in Deep Archive.

The recommendation is to start without staging, record the costs in the run
ledger, and add staging only if the ledger shows it is needed.

### Running for many years

Two things accumulate when backups run for a decade:

- **Changesets.** Nightly runs leave thousands of changesets in a busy
  directory, and rebuilding its index, or its state at a past run, means
  replaying them all. Once a month, a run also saves a dated copy of each
  changed index, `gda-checkpoint.<run>.csv`, as a checkpoint (counted from
  the directory's first changeset, so first runs don't write one). Rebuilds
  and point-in-time views replay only from the latest checkpoint before
  the requested time, found in the same directory listing replay already
  makes (implemented).
- **Fragmented packs.** A busy directory's live files end up spread over
  hundreds of small delta packs that are mostly dead data. Restoring the
  directory then means restoring hundreds of objects. When a directory's live
  files are spread over more than about 20 packs, or more than half of their
  bytes are dead, the next run **rebases** it: it writes a fresh full set of
  packs from the source, which costs nothing to read, and points the index at
  them. The old packs are kept, following the retention policy. As
  implemented, the trigger is the number of packs holding the directory's
  unchanged files: more than 20, and more than twice the packs those files
  would fill, so a big directory isn't repacked on every run; the dead
  share would need a listing of the directory's packs. The changeset
  records each moved file as `rebase`.

With "keep forever", history grows with every change: a 1 TB directory that
is rewritten weekly adds about 52 TB of old versions a year, about $620 a
year. Scratch and temporary areas should be excluded, or given a retention
limit.

The CSV format will also change over the years. The run ledger records the
format version, new columns are only ever added at the end, and readers
ignore columns they don't know.

### Large standalone files that change

GDA supports both of these and picks one per destination by checking the
bucket's versioning status at the start of each run:

1. **Versioned bucket, the preferred setup.** The new version is uploaded
   under the same key, and the old one becomes a noncurrent version. Nothing
   is deleted, so there's no early-deletion charge. A lifecycle rule "expire
   noncurrent versions after N days", with N at least 180, bounds the
   history. The index records the `version_id` so point-in-time restores can
   ask for the exact version.
2. **Unversioned bucket.** The first version keeps its native name, and later
   versions are uploaded as `<name>.gda.<run>`, for example
   `model.bin.gda.20261015T120000Z`. The index's `location` column points at
   the current one. The `.gda.` infix is already reserved, so these can't
   collide with source files.

If versioning is switched on or off later, existing objects stay where they
are, because the index records each version's location either way.

### Deletions and history

- **Deletes are tombstones:** a `delete` row in the changeset, and the entry
  leaves the index. Data stays until garbage collection.
- **Point-in-time restore for free:** replaying changesets up to run R gives
  the directory exactly as it was at run R.
- **Retention policy per destination:** keep everything (the default, and
  cheap), or make superseded and deleted versions eligible for garbage
  collection after N days, with N at least 180.

### Renames and moves

A renamed directory looks like a new directory plus a deleted one, and is
re-uploaded. Phase 2 can match new files by size and MD5 against the tree's
indexes and record a `location` that points into the existing pack instead of
uploading again. This saves requests, but that directory's restore then needs
packs stored under the old path. The manifest handles it; it just weakens
the "one directory, one set of packs" property.

## Deduplication

Identical files of **at least 1 MiB** are stored once (decided 2026-09-26).

- **Why 1 MiB:** files of 64 KiB or less hold 0.31% of the bytes in the CGRB
  census, so deduplicating small files saves almost nothing. Limiting it to
  files of 1 MiB or more keeps the hash index to an estimated few hundred
  million entries (about 10 GB) instead of 10 billion (about 400 GB).
- **The hash index** maps size plus MD5 to the `location` (and `offset`) of
  the stored copy. As implemented in milestone 4, each run writes the
  copies it stored to `_gda/dedup/<run>-<worker>.csv` (index rows with the
  object key as `location`), and the index is the union of those files,
  loaded at the start of a run. Compacting them into shards by hash
  prefix is left for milestone 8.
- **Only files of a size some stored copy has are hashed** before storing,
  so files with unique sizes, which are most of them, are still read
  once.
- **Workers read a snapshot of the index** at the start of a run, and write
  the hashes they store to their own per-run file, which the coordinator
  merges at the end.
- **Races are harmless.** If two workers store the same new file at the
  same moment, it's simply stored twice. Deduplication saves space, and
  nothing depends on it for correctness.
- **A duplicate** gets an index row whose `location` points to the existing
  copy, with `dedup_of` set. No data is uploaded.
- **Restores follow the pointer.** Restoring a directory may therefore
  restore packs that belong to other directories. The restore planner already
  works from `location`, so this needs no special handling.
- **Deleting data now needs reference checks.** Garbage collection and
  rebasing may only remove a stored copy when no index row in the catalog
  still points to it. With "keep forever" this only matters when garbage
  collection is run explicitly.
- **Scope across buckets:** each lab has its own bucket. Deduplicating across
  labs saves more, but a restore in one lab can then depend on another lab's
  bucket, its permissions and its billing. The default is to deduplicate
  within a bucket, with cross-bucket deduplication as an option (open
  question 1).

## Compression

Large files hold 99.7% of the bytes, so storage, and retrieval charged per GB,
is where the money is. A lot of research data is text-like and compresses
well.

- **zstd, level 3 by default.** It compresses at several hundred MB/s per core
  and decompresses at over 1 GB/s whatever the level.
- **Skip what won't compress:**
  - files whose extension or file signature shows they are already
    compressed, such as `.gz`, `.bz2`, `.xz`, `.zst`, BAM, CRAM, JPEG, PNG,
    MP4 and zip;
  - files whose first 1 MiB shrinks by less than 10% in a trial compression
    (the same check rclone's `compress` backend uses).
- **Keep random access.** Data is compressed in independent zstd frames:
  about 16 MiB each for standalone files, and aligned to member boundaries in
  packs. The manifest records each file's `stored_offset` and
  `stored_length`, so after a restore one ranged GET plus decompression reads
  one file.
- **Still a standard format.** Multi-frame zstd is a normal `.zst` file:
  `zstd -d pack.tar.zst | tar x` works without GDA.
- **Never convert formats**, for example gzip to zstd. A restore must return
  exactly the original bytes, verified against the original `md5`.
- **How it is stored** (implemented in milestone 3):
  - packs are built as plain tar, then compressed in a second pass into
    frames cut at member boundaries once a frame reaches 1 MiB, and at
    16 MiB at the latest;
  - the trial compresses up to 1 MiB of the members' data (the first
    64 KiB of each), leaving out tar headers and the manifest, which
    always compress well;
  - each compressed member records `stored_offset` and `stored_length`
    (the frames holding it) and `stored_start`, the uncompressed offset
    where those frames start, so a reader decompresses them and skips
    `offset - stored_start` bytes;
  - the manifest embedded in a pack describes the uncompressed tar;
  - standalone files are streamed through zstd in 16 MiB frames. They
    have no frame index yet, so reading part of one decompresses it from
    the start.
- **Rough ratios** (from general experience, not measured on our data):
  about 3 to 4 times for plain FASTQ and SAM, more for VCF, CSV and logs, and
  none for already-compressed formats. If a third of the bytes shrink 3
  times, storage and Bulk retrieval costs fall by about 20%.

## Paths that don't fit S3 keys

- **S3 keys are limited to 1,024 bytes**, while file paths can be up to
  4,096. A directory whose key would be too long is stored inside the nearest
  ancestor's packs and index, with its path relative to that ancestor, in the
  same way as a rolled-up subtree.
- **S3 keys must be valid UTF-8.** Directory names that aren't are
  percent-encoded in the key, and the index records the original name.

## Garbage collection and compaction

A pack is eligible for compaction when all of these hold:

- it is at least 180 days old (from the `run` in its name);
- the share of its bytes still live in some index is below a threshold, for
  example 50%;
- the dead bytes exceed a minimum, for example 1 GiB.

Compaction writes the live members into a new pack, commits a changeset that
moves their `location`, then deletes the old pack.

- **Repack from the source when possible:** if a live member is unchanged at
  the source (same size, mtime and MD5), read it from there for free.
  Otherwise use a Bulk restore.
- **It is usually not worth it.** Dead data costs about $12 per TB per year
  in Deep Archive, which is often less than the effort and risk. Compaction
  should be an explicit command with a dry-run report of the savings, not
  something that runs automatically.
- **Orphan packs** (a pack with no changeset, left by a crashed run) are
  deleted by garbage collection. They are rare, and the early-deletion charge
  on them is small.
- **Implemented so far:** `rclone gda gc` reads every changeset and index
  and reports stored, live and historical data, orphans and the packs worth
  compacting; `--delete-orphans` removes orphans older than `--min-age`
  while holding the destination lock. Objects listed in the dedup index
  count as referenced, and objects without GDA names are only reported.
  Compaction itself is left. It must count the dedup index as a reference
  too: after a rebase, duplicates may still point at members of the old
  packs.

## Commit protocol and crash safety

Per directory, in order:

1. **Plan** from the source listing and the current index.
2. **Build each pack** in a temporary directory: stream the files in,
   computing each member's MD5 and offset, append the embedded manifest, and
   compute the pack's MD5. The temporary space needed is `pack-size` times the
   number of packs built in parallel.
3. **Upload each pack** with one PUT in DEEP_ARCHIVE with Content-MD5, so S3
   rejects a corrupted upload. Upload standalone files, recording their MD5.
4. **Re-hash sources that changed while being read** (size or mtime differs
   before and after) and defer them to the next run.
5. **Write the changeset.** This is the commit point: a pack is part of the
   backup only once a changeset references it.
6. **Write the new index.**

At the end of the run, write the run ledger.

- **A crash before step 5** leaves orphan packs, which the next run ignores
  and garbage collection removes.
- **A crash between steps 5 and 6** leaves a stale index. In milestone 1
  the next run compares against the stale index and stores the changed
  files again, which is safe but uploads them twice; detecting the newer
  changeset and rebuilding the index from it comes later.
- **Retiring indexes** of subdirectories that were deleted, rolled up or
  replaced by files happens between steps 5 and 6. If it fails, the
  previous index still lists them, so the next run retires them again.
- **One writer per directory:** the coordinator assigns each partition to
  exactly one worker for the run (see
  [Scale and parallel workers](#scale-and-parallel-workers)). A lock object
  `_gda/lock` with the run ID and coordinator host, with a takeover timeout,
  prevents two coordinators from running against the same destination.
  Running workers refresh it every hour, so a run lasting days keeps it, and
  the holder's timeout is stored in it, so a planned run's longer timeout
  covers workers waiting in a queue.

## Scale and parallel workers

Targets: many petabytes, about 10 billion files, 10 to 15 workers per host on
several hosts. The same binary runs standalone for small trees, and as a
coordinator with workers for large ones (decided 2026-09-26).

### Partitions and the single-writer rule

- **The coordinator splits the tree into partitions**, for example per lab or
  per top-level directory, splitting large ones further by subdirectory. A
  partition always contains whole rolled-up subtrees.
- **Each partition has exactly one worker for the whole run.** Only that
  worker writes the packs, changesets and indexes of its directories, so the
  CSVs never see concurrent edits and need no locking.
- **How work is handed out:** on one host the coordinator starts the worker
  processes and gives each its partitions. Across hosts it writes a plan,
  `_gda/runs/<run>/plan.csv`, and each Slurm array task takes its share of
  it. Each worker claims its partition in
  `_gda/runs/<run>/partition-<n>.json` and writes its ledger when it starts,
  so a second worker for the same partition, or a worker ID used twice, is
  refused, and `rclone gda finish` refuses to release the lock until every
  planned partition has a finished ledger. A partition whose worker crashed
  or reported errors is run again with the same worker ID; each attempt
  adds its own suffix to the worker ID in object names, such as `-r2`, so it
  never replaces what an earlier attempt committed. User worker IDs are
  letters, digits and `_`, so generated suffixes can't collide with them.
  Claims are written and read back, which doesn't rule out two workers
  starting the same partition at the same moment; conditional writes would.
- **No locks in S3 per directory.** They would need conditional writes
  (`If-None-Match`), which AWS supports but which I haven't confirmed for
  Ceph RGW.

### Shared files are written per worker

Each worker writes its own run ledger, catalog rows and dedup entries under
`_gda/runs/<run>/<worker>.*`. The coordinator merges them at the end of the
run. Nothing shared is written by two processes.

### Resource budget per host

| Resource | Estimate for 15 workers | Control |
|---|---|---|
| Memory or local NVMe temp | About 30 GB: two 256 MiB packs in progress per worker, plus standalone uploads in 512 MiB parts, four at a time | Limits per worker and per host |
| CPU | MD5 plus zstd level 3 is roughly one core per 300 to 500 MB/s, so 15 busy workers need 32 or more cores | Number of workers from the CPU count |
| Source file system | 15 workers scanning and reading at once load NFS servers and the Lustre metadata server | A throttle per source file system |
| S3 request rates | S3 scales per key prefix; keys that mirror the directory tree spread well | rclone's pacer handles "slow down" responses |

Measured on the implementation (2026-09-27, local disk, 16 cores): a tree
of 100,000 files in 2,000 directories backs up in 2 s and an unchanged
rerun takes 0.6 s; a single directory of 300,000 files takes 10 s and
2 GB of memory, as a directory is held in memory while it is compared
and committed, at about 3 KB per file plus 16 MiB of compression
history per worker. Directories of tens of millions of files would need
the comparison to stream instead. The dedup index takes about 300 bytes
per stored file of at least `--dedup-min` in every process, so tens of
millions of such files need a larger `--dedup-min` or sharding, and the
scan's directory totals take a few hundred bytes per directory. Against
AWS S3 in us-west-2, the same 100,000 files as 2,000 rolled up
directories took 19 s split over four processes, an unchanged rerun 10 s
reading every index, and under a second with the index cache.

### Initial upload

With 40 to 100 Gbit/s to AWS (decided 2026-09-26), 5 PB takes about 12 days
at 40 Gbit/s or 5 days at 100 Gbit/s at full rate; plan for roughly twice
that. Filling 100 Gbit/s (12.5 GB/s) needs about 15 to 25 busy workers across
hosts, limited mainly by compression and hashing speed. The first run is a
project in itself: run it partition by partition, with the run ledger showing
progress and cost.

## Destinations: AWS and Ceph

Each lab has its own bucket, and chooses one or more destinations: AWS Deep
Archive, an on-premises Ceph RGW bucket, or both (decided 2026-09-26).

- **The format is the same everywhere**, so the same packs and CSVs can be
  written to each destination.
- **Without bucket versioning** (for example on a Ceph cluster where it is
  off), later versions of standalone files become `<name>.gda.<run>`. Packs
  and changesets are never overwritten anyway, and indexes can be rebuilt
  from changesets. Recent Ceph RGW releases do support S3 versioning.
- **Ceph has a different cost model:** no request fees, no 180-day minimum
  and no restore step. For destinations that aren't archive storage classes,
  GDA skips restores, and garbage collection and rebasing can run freely.
- **File system destinations** (a local disk or NAS, mostly for tests and
  staging) work too, with one limit: a standalone file keeps its native
  name, and a file can't share a name with a directory there, so if a
  large file is later replaced by a directory of the same name, that
  directory can't be backed up to such a destination. Object stores have no
  such limit.
- **Bundling still pays off on Ceph:** fewer RADOS objects and smaller bucket
  indexes (large RGW buckets need their indexes resharded), and less wasted
  space, because each small object is padded to Ceph's minimum allocation
  size, with more padding under erasure coding.
- **Protection without versioning:** backup runs only need PUT, GET and LIST.
  Their credentials get no delete permission, and garbage collection runs
  with separate credentials. Protection against overwrites by a compromised
  key needs conditional writes, which still have to be checked on Ceph.

## Restore

1. **Find:** browse `gda-index.csv` files, query with DuckDB, or run
   `rclone gda find`, which walks the indexes applying rclone's filters
   (implemented). No Deep Archive access is needed.
2. **Plan:** from the requested paths and optional `--at <run>`, work out the
   exact set of packs and standalone objects needed.
3. **Request restores:** one Bulk `RestoreObject` per needed object, with a
   short lifetime such as 3 days. This can drive
   `rclone backend restore --files-from`, or S3 Batch Operations for very
   large sets.
4. **Wait:** poll with `restore-status`, which reads status straight from the
   listing with no request per object.
5. **Fetch:** use ranged GETs by offset when only a few members of a pack are
   wanted, otherwise download whole packs.
6. **Extract and verify:** check MD5 against the manifest, then restore mtime,
   mode and optionally owner.

Without the GDA tool: download the index, restore the named packs with the
AWS CLI, then `tar xf`.

### Command behaviour (decided 2026-09-26)

- **Submit, exit, re-run to fetch.** The first run of `rclone gda restore`
  plans the restore, requests it, saves the plan as
  `_gda/restores/<id>.csv` (the entries, their packs or objects, and the
  target paths) and exits. Running the same command again, or
  `rclone gda restore --resume <id>`, fetches whatever is ready and reports
  what is still being restored. It is safe to run from cron, and Motuz
  polls it the same way from a Celery job.
- **Existing files at the target:** files whose size and MD5 match are
  skipped. If any differ, the restore stops with a list of them unless
  `--overwrite` is given.
- **Metadata:** permissions and modification times are always restored.
  Owner and group are restored only when running as root, by name first and
  by numeric ID if the name doesn't exist; setuid and setgid bits likewise
  only as root.

## Cost estimates before restoring or copying out

Before any restore or copy out of Deep Archive, the user sees what it will
cost and how long it will take, and chooses the retrieval speed. This works
the same way in the command line and in the Motuz copy dialog, because both
use the same estimate from GDA.

### What a copy out of Deep Archive costs

| Component | What it depends on | Example rate (us-east-1 list price, check current pricing) |
|---|---|---|
| **Retrieval**, per GB restored | Retrieval speed; whole packs are restored even if only one file is wanted | Standard, within 12 hours: $0.02 per GB. Bulk, within 48 hours: $0.0025 per GB |
| **Restore requests**, per object | Number of packs and standalone objects | Standard: $0.10 per 1,000. Bulk: $0.025 per 1,000 |
| **Temporary copy** | Restored copies are billed at the S3 Standard rate for the restore lifetime | $0.023 per GB-month, so about $0.0023 per GB for 3 days |
| **Download requests** | GET and ranged GET requests | $0.0004 per 1,000, usually negligible |
| **Egress** | Bytes downloaded (compressed size) and the network path | Internet: $0.09 per GB for the first 10 TB a month, then lower tiers. Same-region AWS: free. Direct Connect: much lower |

Points the estimate has to get right:

- **"Immediately" doesn't exist for Deep Archive.** The fastest option is
  Standard, within 12 hours. Files are available immediately only if they
  are already restored, and then there is no retrieval charge. The dialog
  says this plainly and shows which selected files are already restored.
- **Restore bytes and download bytes differ.** A restore covers whole packs,
  but the download can be only the ranges of the selected files. The
  estimate uses `stored_offset` and `stored_length` from the indexes for
  egress, and whole-pack sizes for retrieval.
- **Compression lowers both.** Retrieval and egress are charged on stored
  (compressed) bytes.
- **Deduplicated files** may need packs from other directories; the planner
  follows `location` and counts those packs.
- **Egress is shown separately**, because it depends on the destination and
  may be waived (below).
- **Moving out, not copying,** would delete archived objects. Deleting objects
  younger than 180 days adds an early-deletion charge, which the estimate
  shows. With "keep forever", GDA doesn't offer moves out by default.

### Egress waivers

Many research and academic institutions have an AWS **data egress waiver**:
egress is not charged as long as it stays within 15% of the organization's
total monthly AWS bill.

- **Per-destination configuration:** `egress_waiver = true`.
- **The dialog shows both totals:** with egress, and with egress waived. With
  a waiver configured, the waived total is the headline.
- **No cap check** (decided 2026-09-26). GDA doesn't track the 15% cap; the
  separate egress column lets users judge large downloads themselves.

### Where the estimate comes from

- **One estimator in GDA,** used by both the CLI and Motuz. It takes the
  selected paths and returns every option as JSON: for each retrieval speed,
  the time, bytes, object counts, each cost component, and the totals with and
  without egress.
- **Everything is computed from the hot indexes** and one listing of restore
  status. Estimating costs nothing and needs no restore.
- **Prices come from a price table,** not from constants in the code.
  - The table covers region, storage class, retrieval speed, request rates,
    temporary storage and egress tiers.
  - It ships with defaults, can be overridden per destination, for example
    for negotiated rates or Ceph (where most items are zero), and can be
    refreshed from the AWS Price List bulk files with `rclone gda prices`.
    Those files don't list Deep Archive restore request fees or Direct
    Connect egress, so those keep their table values.
  - Each estimate states the date of the prices it used.
  - The same table has storage and upload rates per storage class, and
    each backup run logs what it cost in uploads and adds to the monthly
    storage bill; with `--dry-run` that estimates a backup before making
    it (implemented).
- **The network path defaults to internet egress** (decided 2026-09-26). A
  destination can override it with Direct Connect or same-region AWS, and
  the matching rates in the price table.

### Command line

```text
$ rclone gda restore --estimate s3:lab-bucket/proj/results /fh/fast/lab/restore
Restore 2,340 files (1.20 TiB stored in 5 packs, 1 standalone object)
Prices: us-west-2, 2026-09-26. Egress path: internet. Egress waiver: yes

Option    Ready in   Retrieval  Requests  Temp copy (3 d)  Egress    Total     Total, egress waived
Bulk      48 hours   $3.07      $0.00     $2.83            $110.59   $116.49   $5.90
Standard  12 hours   $24.58     $0.00     $2.83            $110.59   $137.99   $27.40

Already restored: 0 files. Egress assumes all 1.20 TiB is downloaded.
```

- `rclone gda restore --tier bulk` shows the same table for the chosen speed
  and asks for confirmation. `--yes` skips the prompt for scripts, and
  `--max-cost 50` refuses to start if the estimate exceeds a limit.
- `--json` prints the estimate in the same format Motuz uses.

### Motuz copy dialog

When the source of a copy job is a GDA destination, the dialog that confirms
the job shows the table above:

- one row per retrieval speed (Bulk and Standard for Deep Archive), with the
  time until the data is ready, and the cost of each component;
- egress as its own column, and the total with and without egress, with the
  waived total first when the connection has a waiver;
- how many selected files are already restored and so cost nothing to
  retrieve;
- a radio button for the retrieval speed, and "Start" disabled until one is
  chosen.

Motuz doesn't compute any prices itself. It gets everything the dialog
shows from GDA as JSON, and drives the restore with three calls.

#### 1. Estimate: data for the dialog

```text
rclone gda restore --estimate --json <gda root> <target> [paths...]
```

returns one entry per restore option available for the selected data, with
every cost component, so Motuz can render the options as a table of radio
buttons:

```json
{
  "prices": {"region": "us-west-2", "date": "2026-09-26", "currency": "USD"},
  "egress": {"path": "internet", "waiver": true},
  "selection": {
    "files": 2340,
    "bytes": 1319413953331,
    "already_restored_files": 0,
    "objects_to_restore": 6,
    "bytes_to_restore": 1319413953331,
    "bytes_to_download": 1319413953331,
    "download_requests": 6,
    "temporary_copy_days": 3
  },
  "options": [
    {
      "tier": "Bulk",
      "label": "Bulk: ready within 48 hours",
      "ready_within_hours": 48,
      "costs": {
        "retrieval": 3.07,
        "restore_requests": 0.00,
        "temporary_copy": 2.83,
        "download_requests": 0.00,
        "egress": 110.59
      },
      "total": 116.49,
      "total_egress_waived": 5.90
    },
    {
      "tier": "Standard",
      "label": "Standard: ready within 12 hours",
      "ready_within_hours": 12,
      "costs": {
        "retrieval": 24.58,
        "restore_requests": 0.00,
        "temporary_copy": 2.83,
        "download_requests": 0.00,
        "egress": 110.59
      },
      "total": 137.99,
      "total_egress_waived": 27.40
    }
  ],
  "warnings": []
}
```

- **`options`** lists only the tiers the data's storage class allows:
  Bulk and Standard for Deep Archive, plus Expedited for Glacier Flexible
  Retrieval. If everything selected is already restored, there is a single
  option with tier `None` and no retrieval cost.
- **Amounts** are numbers in `prices.currency`, rounded to cents. Motuz shows
  `total_egress_waived` as the headline when `egress.waiver` is true, and
  `total` otherwise, with egress in its own column either way.
- **`warnings`** carries anything the user should see before confirming,
  for example files that are missing from the index.
- The estimate needs no restore and costs nothing, so Motuz can re-run it
  whenever the selection changes.

#### 2. Start: the user's choice

```text
rclone gda restore --tier Bulk --yes --json <gda root> <target> [paths...]
```

requests the restores and returns at once with the restore ID and the
estimate for the chosen tier, which Motuz stores with the job:

```json
{"restore_id": "20260926T190512Z-7f3a", "tier": "Bulk", "state": "restoring", "estimate": {"tier": "Bulk", "total": 116.49, "total_egress_waived": 5.90}}
```

(abbreviated: the full response has the progress fields below).

#### 3. Progress: polled by a Celery job

```text
rclone gda restore --resume <restore_id> --json <gda root> <target>
```

The target directory and `--overwrite` always come from this command
line, never from the restore saved in the bucket, and only one call per
restore fetches at a time: an overlapping call just reports `restoring`.

fetches whatever is ready and reports progress, so the Celery job just
calls it every few minutes until `state` is `done`:

```json
{
  "restore_id": "20260926T190512Z-7f3a",
  "tier": "Bulk",
  "state": "restoring",
  "objects": {"requested": 6, "restoring": 4, "fetched": 2},
  "files": {"total": 2340, "fetched": 781, "skipped_identical": 0, "failed": 0, "unsupported": 0},
  "ready_by": "2026-09-28T19:05:12Z",
  "errors": []
}
```

`state` is one of `restoring`, `done` or `failed`. `errors` lists what
went wrong with individual files. `unsupported` counts entries that
can't be recreated on this system, such as device files when not
running as root. The start call returns the same, plus `estimate` with
the chosen tier's costs. The job
list shows the estimate next to the job, so users can compare it with what
happened.

## Browsing from Motuz and other front ends

Browsing a GDA destination as raw objects shows packs, changesets and index
files instead of the user's files. Front ends should show the **logical file
system** described by the indexes instead.

### Recommended: a read-only `gda` backend in rclone

A read-only rclone backend that wraps the S3 remote, similar to the existing
`:archive:` backend. `List(dir)` reads that directory's `gda-index.csv`
instead of listing objects.

- **Motuz detects GDA folders automatically.** It already runs
  `rclone lsjson` for cloud connections
  (`motuz/src/backend/api/utils/rclone_connection.py:76`). It wraps every S3
  connection in the backend, for example `:gda,remote="s3:bucket":lab/results`.
  - In a folder that has a `gda-index.csv`, the backend lists the logical
    files from it.
  - Anywhere else, it passes the listing through to plain S3, as the
    `:archive:` backend does for paths outside an archive.
  - This costs one extra small GET per folder click on non-GDA folders,
    about $0.0004 per 1,000 clicks. Users need no special connection type,
    and the "Show archive internals" toggle shows the raw objects.
- **Every other rclone tool gets the same view:** `lsf`, `ncdu`, `mount`,
  `serve http` and `serve webdav`.
- **`lsjson` fields come from the index:**
  - `Name`, `Size`, `IsDir`;
  - `ModTime` from the file's own mtime, not the upload time;
  - `Hashes` with the MD5;
  - `Tier` of `DEEP_ARCHIVE`;
  - with `--metadata`: owner, group, mode, the pack name, and the restore
    state described below.
- **Opening a file:** if its pack or object has been restored, the backend
  reads just that file with a ranged GET at the recorded offset. Otherwise it
  returns "restore first", as the S3 backend already does.
- **Restore state:** one LIST of the directory's objects, with restore status
  included (as `restore-status` does today), gives each pack's state:
  archived, restoring, or restored until a given date.

### Cost and speed of a directory click

| | Raw S3 listing today | GDA view |
|---|---|---|
| Requests for a directory of 10,000 files | 10 LISTs plus 10,000 HEADs | 1 GET of the index (plus 1 LIST for restore state) |
| Latency | Seconds to minutes | One round trip |
| Modification time shown | Needs a HEAD per object for the real mtime | From the index, no extra requests |
| What the user sees | Packs and CSV files | The original files and folders |

Side finding: Motuz runs `rclone lsjson` without `--no-modtime`, so every
S3 listing already does a HEAD per object to read the modification time,
which Motuz then discards. That makes large-directory listings slow today,
independently of GDA.

### What Motuz should display

- **A Modified column with relative ages:** "just now", "12 minutes ago",
  "3 hours ago", "3 days ago", "50 days ago", "7 months ago", "2 years ago",
  with the exact timestamp as a tooltip. Suggested cut-offs: minutes under an
  hour, hours under 48 hours, days under 60 days, months under 2 years, then
  years. Sort by the real timestamp. The browser's built-in
  `Intl.RelativeTimeFormat` does this with no extra library.
- **Folder sizes and file counts** from `tree_size` and `tree_files`, without
  walking the tree.
- **An archive state badge:** Archived, Restoring (with the restore tier and
  expected time), or Available until a given date.
- **Optionally, "safe to delete after"** (archive date plus 180 days) for
  administrators, to avoid early-deletion charges.
- **A "Show archive internals" toggle**, like the existing hidden-files
  toggle, to see packs and CSVs when needed.
- **A Restore action:** select files or folders, show the estimated cost and
  time for a Bulk restore, submit a restore job, and offer a copy job once
  the data is available. Motuz already runs long jobs through Celery.

The Motuz changes for the Modified column:

- `src/frontend/js/managers/fileManager.jsx:13`: `convertRcloneFilesToMotuz`
  keeps only name, type and size; it also needs `ModTime` and, for GDA,
  `Tier` and `Metadata`.
- `src/frontend/js/views/App/Pane/PaneFile.jsx`: render the new column.
- `src/backend/api/utils/local_connection.py`: parses `ls -l` output without
  timestamps, so local listings need a machine-readable time format as well.

### Alternatives

- **Motuz reads `gda-index.csv` itself**, using `rclone cat` and parsing the
  CSV in Python. Quick to build, but only Motuz benefits, and the logic for
  rollups and restore state would have to be duplicated.
- **A `rclone gda ls` command** instead of a backend. Simpler than a
  backend, but mount, serve and ncdu wouldn't get the view.

The CSV indexes stay browsable directly in any S3 tool regardless.

## Profiling a file system

Before choosing defaults for a site, measure it. The
[file-system-analysis](https://github.com/dirkpetersen/file-system-analysis)
workflow (pwalk to CSV to Parquet, then DuckDB) already collects what is
needed. This query estimates packs, standalone files and rollup candidates
per directory. Column names are as produced by that workflow's
`csv2parquet.sh`, where files have `pw_fcount = -1`; check them against your
Parquet file.

```text
WITH per_dir AS (
  SELECT st_dev, "parent-inode" AS dir,
         count(*) FILTER (WHERE st_size <  67108864) AS small_files,
         coalesce(sum(st_size) FILTER (WHERE st_size < 67108864), 0) AS small_bytes,
         count(*) FILTER (WHERE st_size >= 67108864) AS standalone_files
  FROM read_parquet('fs.parquet')
  WHERE pw_fcount = -1
  GROUP BY ALL
)
SELECT
  count(*) FILTER (WHERE small_files > 0)                         AS dirs_with_small_files,
  sum(CASE WHEN small_files > 0
           THEN greatest(1, ceil(small_bytes / 268435456.0)) END)  AS packs_at_256MiB,
  sum(standalone_files)                                            AS standalone_objects,
  sum(small_files)                                                 AS packed_files,
  quantile_cont(small_bytes, [0.5, 0.9, 0.99])
    FILTER (WHERE small_files > 0)                                 AS small_bytes_per_dir_p50_p90_p99,
  count(*) FILTER (WHERE small_files > 0 AND small_bytes < 16777216) AS dirs_under_rollup_max
FROM per_dir;
```

This counts packs before rolling up; `dirs_under_rollup_max` shows how many
directories are candidates. An exact rollup count needs subtree totals,
which the GDA planner computes during its own walk.

## Worked cost example

A tree of 10 TB and 10 million files in 100,000 directories: 9.9 million
small files totalling 1 TB, and 100,000 large files totalling 9 TB. The
prices are the us-east-1 list prices from the analysis.

| | `rclone copy` straight to DEEP_ARCHIVE | GDA, `pack-size` 256 MiB |
|---|---|---|
| Deep Archive objects | 10,000,000 | about 204,000 (about 104,000 packs plus 100,000 standalone) |
| Upload requests | **$500** | **$10**, plus $1 of Standard PUTs for manifests |
| Storage per month | $10.1 | $10.1, plus $0.09 for hot manifests |
| Per-object overhead per month | $2.05 | $0.04 |
| Bulk restore of everything | $250 in requests plus $26 retrieval | $5 in requests plus $26 retrieval |
| Find one file | Needs a listing; names only | Instant from the index, with size, MD5 and location |

At the target scale of 10 billion files and 5 PB, with the census file sizes:

| | One object per file | GDA |
|---|---|---|
| Deep Archive objects | 10 billion | About one per directory with small files, after rolling up, plus one per file of 64 MiB or more. With 500 million directories, at most about 500 million |
| Upload requests | **$500,000** | At most about **$25,000**, plus about $5,000 of Standard PUTs for changesets and indexes |
| Restore requests for everything | $250,000 | At most about $12,500 |
| Storage per month | About $5,000 | About $5,000 before compression and deduplication, plus about $100 for hot metadata |

The number of directories is still the most important unknown; the
profiling query measures it.

## Where to implement

| Option | Pros | Cons |
|---|---|---|
| **A. New rclone command group in this fork** (`rclone gda backup/restore/find/index/gc`), recommended | Reuses rclone's walk, filters, accounting and S3 backend (Content-MD5, storage class, `restore`); one static binary; parts can go upstream | Needs to stay a clean addition to rebase easily on upstream |
| B. rclone backend that packs transparently on write (like `compress` or `chunker`) | Invisible to users | Backends work per object; packing needs a directory-level view and deferred writes; `sync` semantics break. Not recommended for writing. A **read-only** backend for browsing is recommended; see [Browsing](#browsing-from-motuz-and-other-front-ends) |
| C. Inside Froster (now Go) using rclone as a library | Keeps Froster's workflow: hotspots, Slurm, NIH metadata, `Where-did-the-files-go.txt` | Froster currently shells out to rclone; importing rclone packages is a bigger dependency change |

Option A, with Froster later calling `rclone gda` in place of its tar and CSV
steps, gets the format working and tested first and lets Froster adopt it.

Pieces that could go upstream independently, all from the analysis:

- `rclone archive create --max-size` splitting and a manifest option;
- the S3 guard against overwriting or deleting young archived objects;
- the `StorageClass` fix for `--s3-versions` listings;
- the Intelligent-Tiering case in the `SetModTime` guard.

## Implementation plan

Code lives in the fork: the format and engine in `lib/gda`, the commands in
`cmd/gda` (`rclone gda ...`). Sources are read with direct POSIX calls
(decided 2026-09-26); destinations are any rclone remote, so tests can use
the local and memory backends and production uses S3 or Ceph.

| Milestone | Delivers | Status |
|---|---|---|
| **1. Core backup, single process** | `rclone gda backup`: two-pass scan (subtree totals, then per-directory processing), standalone/packed/rollup planning, PAX tar packs with offsets, member MD5s and an embedded manifest, changesets and `gda-index.csv` (split above 100,000 rows), commit protocol, incremental runs by scan, the destination lock, the run ledger, dry-run | Done |
| 2. Restore | `rclone gda restore` and `rclone gda ls`: plan from indexes, restore requests, wait, ranged or whole fetch, extract, verify; point-in-time with `--at` | Done |
| 3. Compression | zstd in independent frames, skip heuristics, `stored_*` columns filled | Done |
| 4. Deduplication | Dedup index per destination for files of 1 MiB or more | Done, with compaction of the index files |
| 5. Parallel workers | `--workers` on one host; `rclone gda plan`, `--run`/`--partition` and `rclone gda finish` across hosts | Done; resource limits per host left |
| 6. Cost estimates | Estimator, price table, `--estimate` and `--max-cost`, JSON for Motuz | Done, including `rclone gda prices` to refresh the table |
| 7. Browsing backend | Read-only `gda` backend for `lsjson`, mount and Motuz | Done |
| 8. Change feeds and scale | `--changes-from` for ZFS and path lists, checkpoints, dedup index compaction, per-run catalog | Change runs, checkpoints, dedup compaction and the catalog (as `.csv.zst`) done; orphan removal, the compaction report, the local index cache and rebasing done; catalog compaction and compaction left |
| 9. Operations and fidelity | `rclone gda check`, `find` and `gc`, backup cost estimates, hard links, extended attributes and ACLs, randomized history and failure tests | Done (2026-09-27); extended attributes on Linux only |

Milestone 1 limits, each lifted by a later milestone:

- changed standalone files always get `<name>.gda.<run>` keys; using bucket
  versioning when it is enabled comes later;
- hard links were stored as separate files (lifted: files with several
  links record their device and inode in `hard_link`, the data is stored
  once per run, and restores link them again), and extended attributes
  were not stored (lifted on Linux: `--xattrs` records them, including
  POSIX and NFSv4 ACLs, in an `xattrs` column, and restores set them);
- paths whose S3 key would exceed 1,024 bytes were reported and skipped
  rather than moved into an ancestor's pack (lifted: such a directory is
  packed with the deepest directory that has an index, and such a large
  file is packed rather than stored on its own; only names which need
  encoding are still skipped);
- the scan read file metadata twice (once for subtree totals, once to
  process each directory). Lifted for full runs on one host: they read
  the tree once, depth first, committing each directory after its
  subdirectories, which also means only the directories being read and
  subtrees which may still be rolled up are held in memory, rather than
  totals for every directory. Planned runs still scan once to plan; the
  workers then read their subtrees once each.

## Alternatives considered

- **restic, Kopia, Borg.** Chunk-level deduplication and snapshots, but the
  repository is opaque: nothing can be browsed or restored without the tool,
  and their index and snapshot metadata must stay readable, which needs care
  with Deep Archive. Useful when deduplication matters more than
  transparency.
- **Plain `rclone copy` to DEEP_ARCHIVE.** Simple, but one request per file
  (analysis, "Request charges dominate") and the modification-time and
  overwrite traps.
- **S3 lifecycle transition from Standard.** Adds a transition request per
  object on top of the PUT, and small objects still pay full per-object
  overhead.
- **One archive per directory tree (recursive).** Fewer objects, but one
  change forces a huge rewrite or a large delta, and restoring one directory
  means restoring the whole tree.

## Decisions

Decided on 2026-09-26:

| Topic | Decision |
|---|---|
| Reference data for defaults | The CGRB NFS census. Defaults: `standalone-min` 64 MiB, `pack-size` 256 MiB, `rollup-max` 16 MiB, refined later with the profiling query |
| Pack format | Uncompressed POSIX tar (PAX), with member byte offsets in the manifests |
| Rolling up small subtrees | On by default at 16 MiB |
| Deleting source files after archiving | Not done by GDA. Froster keeps its archive-and-delete workflow and `Where-did-the-files-go.txt` |
| Checksums | MD5 only |
| History of large standalone files | Both methods: bucket versioning when enabled, otherwise `<name>.gda.<run>` key names |
| Owner and group | Stored as names and numeric IDs (`owner`, `group`, `uid`, `gid`) |
| Motuz | Detects `gda-index.csv` automatically on S3 connections, through the read-only `gda` backend |
| Retention | Keep superseded and deleted versions forever by default. Garbage collection runs only when invoked, with a dry-run cost report |
| Encryption | SSE-S3 (bucket default encryption) |
| Where the code lives | In the fork first. Propose `rclone gda` and the `gda` backend upstream once the format is proven; send generic S3 fixes upstream right away |
| First step | The upstream S3 fixes from the analysis, each on its own branch from `master` |
| Scale target | Many petabytes, about 10 billion files, 10 to 15 workers per host on several hosts |
| Source of truth for metadata | Per-directory CSVs. The Parquet catalog is derived from per-worker, per-run files |
| Source file systems | ZFS, GPFS / Spectrum Scale, Lustre, and NFS, Ceph FS and others |
| Execution | Both: standalone for small trees, coordinator with workers for large ones |
| Deduplication | From the start, for files of 1 MiB or more |
| Buckets | One bucket per lab or project |
| Destinations | Chosen per lab: AWS Deep Archive, Ceph, or both |
| Network for the initial upload | 40 to 100 Gbit/s to AWS |
| Compression | zstd level 3 in independent frames, skipping incompressible data (proposed 2026-09-26 after review) |
| Cost estimates | Shown before every restore or copy out, in the CLI and the Motuz copy dialog, from one estimator in GDA |
| Egress path | Internet egress by default; destinations can override |
| Egress waiver | Configured per destination; both totals shown; no 15% cap check |
| Prices | Bundled price table, overridable per destination, refreshable from the AWS Price List API |
| Restore flow | Submit and exit; re-run or `--resume <id>` to fetch what is ready |
| Existing files on restore | Skip identical files; stop on differing files unless `--overwrite` |
| Restored metadata | Mode and mtime always; owner, group and setuid/setgid only as root |
| Source access | Direct POSIX reads; destinations through rclone backends |
| First milestone | Core backup, single process (implemented in `38f260d8f`) |
| Deduplication scope | Within each lab's bucket |

### First step: upstream S3 fixes

In order, smallest and most clearly a bug first:

1. **`--s3-versions` listings drop the storage class**
   (`backend/s3/setfrom.go:71`), so `backend restore` skips archived objects.
   Fixed on branch `fix-s3-versions-storage-class` and verified on AWS.
2. **The restore error says GLACIER for DEEP_ARCHIVE objects**
   (`backend/s3/s3.go:4529`); report the real storage class.
3. **`SetModTime` applies the configured `storage_class`** to the object it
   copies onto itself, so a STANDARD object can move to DEEP_ARCHIVE
   (`s3.go:3074`); keep the object's own class.
4. **`settier` on objects over `copy_cutoff` may lose Content-Type**
   (`s3.go:3138`); confirm with a test first.
5. **Intelligent-Tiering archive access tiers aren't covered** by the
   `SetModTime` guard (`s3.go:4341`). This needs the object's archive status
   from HEAD, since the storage class alone is `INTELLIGENT_TIERING`.
6. **A guard against overwriting or deleting archived objects**, modelled on
   azureblob's `archive_tier_delete`. This is a new feature, so discuss it
   upstream in an issue first.
