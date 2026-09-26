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

Non-goals: block-level deduplication across files (use restic, Kopia or Borg
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

## Overview

For each source directory, one run produces up to four kinds of object:

| Object | Content | Storage class | Mutable? |
|---|---|---|---|
| **Pack** `<dir>.gda.<run>.<part>.tar` | Small files from this directory only, at most `pack-size` | DEEP_ARCHIVE | No, never rewritten |
| **Standalone file** `<original name>` | One large file, stored under its own name | DEEP_ARCHIVE | Only via bucket versioning (see below) |
| **Changeset** `<dir>.gda.<run>.csv` | What this run changed in this directory: added, modified and deleted entries, and where each lives | STANDARD | No, immutable |
| **Index** `gda-index.csv` | Current state of the directory: every live entry and where it lives | STANDARD | Yes, regenerated each run that changes the directory |

Plus, once per run and once globally:

| Object | Content | Storage class |
|---|---|---|
| **Run ledger** `_gda/runs/<run>.json` | Run parameters, directories touched, counts, bytes, estimated cost | STANDARD |
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
├── lab.gda.20260926T120000Z.001.tar         DEEP_ARCHIVE  (README.txt)
├── lab.gda.20260926T120000Z.csv             STANDARD
├── gda-index.csv                            STANDARD
├── results/
│   ├── results.gda.20260926T120000Z.001.tar DEEP_ARCHIVE  (summary.csv, plot-001..plot-436)
│   ├── results.gda.20260926T120000Z.002.tar DEEP_ARCHIVE  (plot-437..plot-873)
│   ├── results.gda.20260926T120000Z.003.tar DEEP_ARCHIVE  (plot-874..plot-900)
│   ├── model.bin                            DEEP_ARCHIVE  (standalone)
│   ├── results.gda.20260926T120000Z.csv     STANDARD
│   └── gda-index.csv                        STANDARD
├── raw/
│   ├── run1.bam                             DEEP_ARCHIVE  (standalone)
│   ├── raw.gda.20260926T120000Z.csv         STANDARD
│   └── gda-index.csv                        STANDARD
└── _gda/
    └── runs/20260926T120000Z.json           STANDARD
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

- **Plain POSIX tar (PAX), uncompressed.** Every Linux system can read it,
  and it keeps nanosecond mtimes, long names, owner, group, mode, symlinks and
  optionally xattrs. Member names are just the file names, as in Froster.
- **No compression by default.** Storage is about $1 per TB-month, research
  data is often already compressed (`.gz`, `.bam`, images), and compression
  would break the byte offsets described below. Compression can be an option
  later.
- **The last member is the pack's own manifest** (`<pack>.csv`). Each pack is
  therefore self-describing even if every hot manifest is lost.
- **Byte offsets in the manifest.** The manifest records each member's data
  offset inside the tar. After a restore, a single file can be fetched with a
  ranged GET (`aws s3api get-object --range bytes=a-b` or
  `rclone cat --offset --count`) instead of downloading the whole pack. The
  restore still covers the whole pack, but download and egress cover only the
  file.
- **Zip is the main alternative.** It has per-member random access built in,
  and rclone's `:archive:` backend can browse zip files after restore. Tar
  plus recorded offsets gives the same random access with better POSIX
  metadata, and matches Froster. Open question 3 asks which to use.

### Naming

`<dirname>.gda.<run>.<part>.tar`, for example
`results.gda.20260926T120000Z.002.tar`.

- **`dirname` in the name** makes a downloaded pack recognisable on its own,
  as in the `foldername.1.tar` idea.
- **`run` is the UTC start time of the run.** Names are never reused, so
  nothing is ever overwritten; no counter has to be read and updated; and the
  name shows the pack's age, which matters for the 180-day rule.
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
| `md5` | `9e107d9d...` | Hex; matches S3 and rclone. An optional `sha256` column can be added |
| `link_target` | | Symlinks only |
| `location` | `results.gda.20260926T120000Z.002.tar` | Pack name, or the object key for a standalone file |
| `offset` | `1536` | Data offset inside the pack; empty for standalone files |
| `version_id` | | S3 version ID for standalone files in a versioned bucket |
| `run` | `20260926T120000Z` | Run that wrote this version |
| `action` | `add` | Changesets only: `add`, `modify`, `meta`, `delete` |

Encoding: RFC 4180 CSV, UTF-8, header row, fields quoted when needed. This
handles commas, quotes and newlines in names. File names that aren't valid
UTF-8 are percent-encoded, with a `name_encoding` column to mark them.

### Why CSV, and where it stops scaling

| Format | Browser and spreadsheet friendly | Scales | Notes |
|---|---|---|---|
| **CSV** | Yes | Per directory: yes | Excel stops at 1,048,576 rows |
| JSON Lines | Partly | Per directory: yes | Better for nested fields; nothing needs nesting here |
| Parquet | No | Yes, columnar and compressed | Ideal for tree-wide search |
| SQLite | No | Yes | Single file, must be downloaded; good as a local cache |

- **CSV only has to scale per directory**, because each index covers one
  directory. At about 200 bytes per row, a directory of 100,000 files has a
  20 MB index. Only directories with more than a million files exceed
  spreadsheet limits, and they remain fine for scripts and DuckDB.
- **Tree-wide questions don't need a merged CSV.** DuckDB and Athena read many
  CSVs in place with a glob, for example:

  ```sql
  SELECT * FROM read_csv('s3://bucket/lab/**/gda-index.csv', filename=true)
  WHERE name LIKE '%.bam' AND size > 1e9;
  ```

- **The optional Parquet catalog** under `_gda/catalog/` is a periodic,
  compressed copy of all indexes for fast search over very large trees. It is
  rebuilt from the indexes and is never the source of truth.
- **Cost of keeping all metadata hot:** 10 million files make about 2 GB of
  indexes plus a similar amount of changesets, roughly $0.09 per month in S3
  Standard.

## Incremental backup

### Change detection

Each run lists the source and compares every entry against that directory's
`gda-index.csv`, never against Deep Archive:

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
`results.gda.<run2>.001.tar`. The index now points those two names at the new
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

### Large standalone files that change

Options, recommended first:

1. **S3 bucket versioning plus a lifecycle rule.** The new version is
   uploaded under the same key, and the old one becomes a noncurrent version.
   Nothing is deleted, so there's no early-deletion charge. A lifecycle rule
   "expire noncurrent versions after N days", with N at least 180, bounds the
   history. The index records the `version_id` so point-in-time restores can
   ask for the exact version.
2. Versioned key names such as `model.bin.gda.<run>`. This works without
   bucket versioning, but only the first version keeps its native name.

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
- **A crash between steps 5 and 6** leaves a stale index. The next run
  detects a changeset newer than the index and rebuilds the index.
- **One writer per destination:** a lock object `_gda/lock` with the run ID
  and host, with a takeover timeout, prevents two runs from writing the same
  tree.

## Restore

1. **Find:** browse `gda-index.csv` files, query with DuckDB, or run
   `gda find`. No Deep Archive access is needed.
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

## Where to implement

| Option | Pros | Cons |
|---|---|---|
| **A. New rclone command group in this fork** (`rclone gda backup/restore/find/index/gc`), recommended | Reuses rclone's walk, filters, accounting and S3 backend (Content-MD5, storage class, `restore`); one static binary; parts can go upstream | Needs to stay a clean addition to rebase easily on upstream |
| B. rclone backend that packs transparently (like `compress` or `chunker`) | Invisible to users | Backends work per object; packing needs a directory-level view and deferred writes; `sync` semantics break. Not recommended |
| C. Inside Froster (now Go) using rclone as a library | Keeps Froster's workflow: hotspots, Slurm, NIH metadata, `Where-did-the-files-go.txt` | Froster currently shells out to rclone; importing rclone packages is a bigger dependency change |

Option A, with Froster later calling `rclone gda` in place of its tar and CSV
steps, gets the format working and tested first and lets Froster adopt it.

Pieces that could go upstream independently, all from the analysis:

- `rclone archive create --max-size` splitting and a manifest option;
- the S3 guard against overwriting or deleting young archived objects;
- the `StorageClass` fix for `--s3-versions` listings;
- the Intelligent-Tiering case in the `SetModTime` guard.

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

## Open questions

1. **Defaults:** `pack-size` 256 MiB and `standalone-min` 64 MiB. Are these
   right for your data? A file-size histogram of a typical project would
   settle it.
2. **Hash:** MD5 only, or MD5 plus SHA-256?
3. **Tar or zip for packs?** Tar keeps POSIX metadata and matches Froster;
   zip is browsable with rclone's `:archive:` backend after restore.
4. **Should very small leaf trees be rolled up?** For example, one pack for
   a subtree under 64 MiB in total. This breaks the one-level rule but cuts
   objects for trees with many tiny directories.
5. **Archive mode:** should GDA delete source files after a verified archive
   and leave a `Where-did-the-files-go.txt`, as Froster does, or leave that to
   Froster?
6. **Bucket versioning:** can we require it (for standalone file history), or
   must versioned key names work too?
7. **Owner and group:** store names, numeric IDs, or both? This matters for
   restores onto other systems.
