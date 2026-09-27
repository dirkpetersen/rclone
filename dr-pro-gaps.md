# DR Pro gap analysis: rsync, plain rclone and rclone-gda

Status: draft for review, 2026-09-27.

This document compares what DR Pro (`drpro`) does today with two ways of
getting its data into S3 Glacier Deep Archive: plain rclone (`sync` or
`copy`), as assessed in `docs/rclone-plan.md`, and the GDA format and
commands in the rclone fork (working name rclone-gda).

Sources read:

- DR Pro code and docs in `oregonstate-ai/cgrb-drpro-linux`: `drpro.bash`,
  `templates/*.txt-template`, `CLAUDE.md`, `docs/rclone-plan.md` and
  `docs/backup-coverage-gap.md`.
- The live configuration in `cgrb-drpro-linux-config-backup2`: `LABS/*/*`
  and `CRONTAB/backup2.txt`.
- rclone-gda on branch `gda` of the rclone fork at commit `c641d580b`:
  `glacier-deep-archive-design.md`, the help texts in `cmd/gda/*/*.go`
  and, to confirm specific behaviour, `lib/gda`. Uncommitted changes in
  the working tree were not assessed. The document was updated after
  commit `d770e238c` for exclude filters, streaming of very large
  directories and an AWS test of restoring old versions.

Section references such as "plan §10.7" point into `rclone-plan.md`.
"Design" means `glacier-deep-archive-design.md`.

## Key findings

- rclone-gda closes most of the structural blockers the plan found for
  plain rclone: small-file aggregation (plan §10.3.1, §10.3.2), bucket
  enumeration cost (§10.7), metadata-only changes (§4.1), directory
  metadata and empty directories on S3 (§10.9), multipart request cost
  (§10.3.3), a restore tool with point-in-time (§11), and restoring old
  versions of archived objects (§11.5: tested on AWS with a noncurrent
  GLACIER version and Expedited retrieval; DEEP_ARCHIVE uses the same
  calls but hasn't been waited out).
- It also goes beyond DR Pro today: it keeps hard links, and with
  `--xattrs` it keeps ACLs and SELinux labels, which `drpro.bash` does not
  (it runs `rsync -aR` without `-H`, `-A` or `-X`).
- What remains is mostly the DR Pro wrapper: per-lab configuration,
  `ENABLE=0` handling, email reports, the LOG files the reports site
  reads, stale-backup detection, snapshot handling and scheduling.
  rclone-gda provides none of these and should not.
- Two gaps sit in rclone-gda itself: `rclone gda backup` takes one source
  directory per run, and reads only local paths (no SSH sources). Exclude
  filters, missing when this was first drafted, now work.
- The largest risks are maturity (the code was written over two days and
  has not run at CGRB scale), the not yet waited-out AWS restore of a
  noncurrent Deep Archive version, and how GDA's deletes interact with the
  planned IAM deny list, Object Lock and lifecycle rules on `osu-drpro`.
- Deep Archive restores still take 12 to 48 hours. The local ZFS copy
  that DR Pro keeps today remains the fast restore path, so the
  recommended shape is plan topology (c): rsync to ZFS as now, then GDA
  from a ZFS snapshot of that copy.

## What DR Pro does today

DR Pro is two bash scripts and a PHP site running as root on
`backup2.cgrb.oregonstate.local`. Root's crontab starts `drpro.bash` once
per lab and server at 00:59 (`CRONTAB/backup2.txt`). Each job reads a
config directory `LABS/<lab>/<fqdn>/` holding `config.txt`, `source.txt`,
`exclude.txt` and `include.txt`. Only 5 of the 15 config directories run
(`core/zfs4`, `garcia/zfs4`, `liston/zfs4`, `tanguay/zfs4`,
`weisberg/zfs6`); see `docs/backup-coverage-gap.md`.

For each line of `source.txt`, `drpro.bash:544` runs:

```text
rsync -aR [-z] --stats --bwlimit=N --delete --exclude-from=exclude.txt \
  --include-from=include.txt --backup --backup-dir=REVISIONS/<ts>/SRC/ \
  <source> DEST_PATH/CURRENT/SRC/
```

So `CURRENT/SRC` mirrors the sources, and each run's
`REVISIONS/<ts>/SRC` holds the pre-images of files that run changed or
deleted. `-R` keeps full source paths so two sources with the same leaf
name can't collide (plan §5.1). Sources are serial within a job. There
is no retention: revisions are pruned by hand, if at all (plan §0).

Around the rsync call the script:

- validates the config (`CONFIG_VERSION_*`, required variables,
  writable `CURRENT` and `REVISIONS`) and supports a dry run with `-d`;
- honours `ENABLE=0` by emailing a disable notice with
  `DISABLE_DATE/USER/DESC` and writing `LOG/disable.txt`;
- takes a lock file per job under `/root/bin/drpro/locks/`, emails a
  NOTICE when a previous run still holds it, and keeps the lock after a
  source pre-flight failure so a person must investigate;
- checks each source: for NFS that it exists, is an NFS mount
  (`stat -f -c %T`) and is not a symlink; for SSH, `sudo test -d` on the
  remote host;
- writes the LOG directory the reports site reads (plan §3): sizes,
  delta, revision size, `ls -lR` of the revision, `find -type d` of
  `CURRENT`, `df` of the destination with 80% and 90% thresholds, the
  ZFS compression ratio, timings and rsync output;
- emails an INFO, NOTICE or WARNING report to the lab contact, with the
  PI and secondary contacts in Cc and admins in Bcc.

`www/rsynclogs.bash` copies the LOG trees hourly to a staging area served
as the reports site, but it is broken and its cron line is commented out
(`CLAUDE.md`, plan §12). There is no restore code: recoveries are done by
CGRB staff from `CURRENT` and `REVISIONS` (plan §1, §11). Every live
config is `SRC_PROTOCOL='NFS'` or `'DFS'` with `BWLIMIT=0`, and every
live `exclude.txt` and `include.txt` is comment-only.

## What the rclone plan concluded

The plan proposes rclone as an optional second engine next to rsync, with
three topologies: local ZFS only, cloud only, and local ZFS plus cloud
(plan §2). Its S3 design ("Design B") stores history as S3 object
versions rather than `--backup-dir`, because server-side copies of Deep
Archive objects fail (plan §10.1).

Its main findings against plain rclone:

- **Fidelity.** rclone syncs metadata only when content changes, so a
  `chmod` or `chown` alone never reaches the backup (§4.1). It can't
  carry SELinux labels or POSIX ACLs, so `/etc` and `/var` should stay on
  rsync (§4, §6, §7). On S3, directories lose their owner, mode and
  mtime, and empty directories vanish (§10.9).
- **Filters and paths.** rclone has no `--relative`, and `exclude.txt`
  patterns must be rewritten per source (§5.1, §5.4).
- **Logging.** rclone writes everything to stderr, which would turn every
  nightly email into a WARNING (§5.5).
- **Scale and cost.** The CGRB census has a median file size of 610 B,
  so per-object overhead and requests dominate (§10.3.1). `rclone sync`
  can't run nightly because enumerating the bucket costs more than the
  storage (§10.7); the cloud stage needs a change list, a high-water mark
  and quarterly reconciliation. Small-file aggregation was deferred to a
  v2 (§10.3.2). Default 5 MiB multipart parts multiply request costs
  (§10.3.3). Keys over 1,024 bytes fail (§10.10).
- **Restore.** A restore tool has to be written (§11). Restoring
  noncurrent Deep Archive versions was unproven and called "the single
  largest open risk to Design B" (§11.5).
- **Operations.** No reporting contract for the cloud stage (§15.1), no
  detection of backups that stopped (§15.2), no cost governance (§15.3),
  and an IAM deny list to limit a compromised `backup2` (§15.4).

The plan also lists live bugs in `drpro.bash` (§14, B1 to B11), of which
B1, B4, B5 and B7 should be fixed before any refactor.

## What rclone-gda adds

rclone-gda is a command group in the fork (`rclone gda`) plus a read-only
`gda` backend. `rclone gda backup` reads a local directory tree with POSIX
calls and writes per-directory tar packs (small files), standalone
objects (files of 64 MiB or more), an immutable changeset CSV per
directory per run and a regenerated `gda-index.csv` per directory, with
data in DEEP_ARCHIVE and CSVs in STANDARD. The other commands are
`restore`, `ls`, `find`, `check`, `gc`, `prices`, `plan` and `finish`.
All nine milestones in the design's implementation plan are marked done,
with a few items left (design, "Implementation plan").

## Gap table

Status values, for plain rclone and rclone-gda:

- **Covered**: meets the DR Pro need.
- **Partly**: meets part of it, or needs wrapper work.
- **Gap**: not provided.
- **N/A**: doesn't apply to this tool or destination.

The "Plan gap" column says whether the plan flagged the item, and whether
rclone-gda closes it: **Closed** (fully, mostly, partly, or in code only),
**Remains**, **New** (not in the plan, or a new conflict) or blank.

### History, deletions and retention

| DR Pro capability | Plain rclone | rclone-gda | Plan gap | Notes and what closes it |
| --- | --- | --- | --- | --- |
| Current copy of the sources (`CURRENT/SRC`) | Partly: `sync` mirrors, but can't run nightly at this scale (§10.7) | Covered | Closed | Each directory's `gda-index.csv` is its current state. Files live inside packs or standalone objects, not as a byte mirror; `rclone gda ls` and the `gda` backend show them as files. |
| Nightly revisions: pre-images of changed and deleted files (`--backup --backup-dir`) | Partly: `--backup-dir` fails on Deep Archive (§10.1); Design B uses bucket versions | Covered | Closed | Old copies stay in older packs or as older standalone objects; each run writes a changeset per changed directory. Backup runs never overwrite packs. |
| Deleted files kept (`--delete` moves them to `REVISIONS`) | Partly: delete markers; whether `delete --files-from` lists the prefix is unverified (§10.7) | Covered | Closed | Deletions are `delete` rows in the changeset; backup runs remove no committed data (design, "Deletions and history"). |
| Point-in-time view and restore | Partly: `--s3-version-at` lists; restoring noncurrent archived versions unproven (§11.2, §11.5) | Covered | Closed | `restore`, `ls`, `find` and `check` take `--at <run ID or RFC 3339 time>`. Versioned standalone files are restored by version ID through a new S3 `RequestRestore` (commit `3b11f75c5`). Tested on AWS (2026-09-27): a noncurrent GLACIER version was restored with Expedited retrieval and fetched with the right content. A DEEP_ARCHIVE version uses the same calls but takes 12 to 48 hours; run the plan's Phase 0d test (§11.5) to close it fully. |
| Retention and pruning (none today; staff prune by hand) | Covered for the cloud tier: lifecycle `NoncurrentVersionExpiration` 360 days (§10.3) | Covered | Closed | `rclone gda gc --keep-history 360d` reports, and `--delete-expired` removes, data only older history needs, and records the cutoff in `_gda/history.json` so older restores are refused. Default keeps everything. Needs delete rights; see the IAM row below. |
| Metadata-only change (`chmod`, `chown`) | Gap: metadata syncs only with content (§4.1) | Covered | Closed | Compared against the index; a mode, owner or group change is a `meta` row with no upload. Same content with a new mtime is also `meta` after an MD5 check. |
| Files changing while read | rsync copies whatever it reads | Covered | New | A file whose size or mtime changed during upload is left for the next run and counted as `Deferred`. |
| Rename or move of a big directory (§10.3 trap 3) | Gap: full re-upload plus delete markers held for the retention period | Partly | Mostly closed | Files of 1 MiB or more are deduplicated per GDA root, so a moved tree's large files are stored by reference. Small files are packed again, which is cheap. There is still no backup-side cost cap (see cost rows). |
| Files rewritten daily, such as database dumps (§10.3 trap 4) | Gap: one new version per night, 180-day minimum each | Partly | Mostly closed | Each change stores a new copy, in a new pack or as a new standalone object or version, unless it is identical to a stored copy. The plan's fix is to exclude such files, which `rclone gda backup` now can with rclone's filter flags (see filter row). |

### File fidelity

| DR Pro capability | Plain rclone | rclone-gda | Plan gap | Notes and what closes it |
| --- | --- | --- | --- | --- |
| Content, size, mtime | Covered | Covered | | Index keeps nanosecond mtimes and an MD5 per file. |
| Mode, owner, group | Covered with `-M` as root; lost for directories on S3 (§10.9) | Covered | Closed | Owner and group kept as names and numeric IDs. Restored only as root, by name first, then numeric ID. |
| setuid, setgid, sticky | Partly: needs `--local-metadata-restore-special-bits` (§6) | Covered | Closed | Restored only as root. |
| Directory metadata and empty directories on S3 | Gap: restored as `root:root 0755`, empty dirs lost (§10.9) | Covered | Closed | Indexes have `dir` rows. Restore sets directory metadata last, deepest first (`lib/gda/restore_unix.go`, `applyMeta`). |
| Symlinks | Covered (`.rclonelink` on S3) | Covered | | Stored in packs with `link_target` in the index. |
| Hard links | Gap (§6) | Covered | New | DR Pro breaks them too: `drpro.bash` uses `-aR` without `-H`. GDA records device and inode in `hard_link`, stores the data once per run and links again on restore. |
| fifos, devices, sockets | Gap: skipped (§4, §6) | Partly | Mostly closed | fifos and device files are packed and recreated (devices only as root). Sockets are recorded in the index only, as tar can't hold them. The DR Pro design says to exclude sockets anyway (§1). |
| xattrs, POSIX ACLs, SELinux labels | Gap: `user.*` only (§4, §6) | Covered with `--xattrs`, Linux only | Closed | DR Pro doesn't keep these today (no `-A`/`-X`). `--xattrs` records every attribute `llistxattr` returns, which includes `security.*` and ACLs; `trusted.*` and `security.*` are set on restore only as root. Costs "a request or two per file on network file systems" (backup help). Whether the NFS mounts on `backup2` expose ACLs and labels is open. |
| Sparse files | Parity: written dense (§6) | Parity | | No sparse handling found in `lib/gda`. Zero runs compress well with zstd where compression applies; restores write files dense. Low priority. |
| Paths over 1,024 bytes, non-UTF-8 names (§10.10) | Gap: per-file failures | Mostly covered | Closed | A directory whose key would be too long is packed with the nearest ancestor that has an index. Non-UTF-8 and control characters are percent-encoded. The design notes that too-long names which also need encoding are still skipped. |
| Very large files (§10.3.3, §10.10) | Partly: 5 MiB parts cap objects at about 50 GiB | Covered | Closed | For S3, `rclone gda backup` sets `--s3-chunk-size 64M` unless given. Compressed standalone uploads are streams capped at 625 GiB, so files over `--compress-max` (32 GiB) are stored uncompressed. The 5 TiB S3 object limit remains. |

### Sources, scope and scheduling

| DR Pro capability | Plain rclone | rclone-gda | Plan gap | Notes and what closes it |
| --- | --- | --- | --- | --- |
| Several source directories per config (`source.txt`) with `-R` layout | Partly: map each source to an explicit sub-path (§5.1) | Partly | Remains | `rclone gda backup` takes one source directory. The wrapper runs it once per source line with destination `<prefix>/<source path without leading />`. Each becomes its own GDA root with its own lock, ledger and dedup index, so there is no dedup across sources. |
| Exclude and include lists | Partly: syntax matches, semantics differ, patterns must be rewritten (§5.4) | Covered | Closed | Since commit `c641d580b`, rclone's filter flags (`--exclude`, `--exclude-from`, `--include`, `--exclude-if-present` and the rest) apply to backups, matched against paths relative to the source; an entry left out that was backed up before is recorded as deleted. The plan's §5.4 lesson still holds: rsync patterns anchored at `/` must be rewritten per source, and each rule wants a test. No live lab uses excludes today. |
| Source pre-flight: exists, is an NFS mount, not a symlink | Reusable in wrapper (§3 C5) | Partly | | GDA requires a local directory and refuses to back up an empty source over a non-empty backup unless `--allow-empty`, which catches an unmounted source. The NFS type and symlink checks (and bug B5, §14) stay in the wrapper. |
| SSH sources (`SRC_PROTOCOL='SSH'`), `/etc` and `/var` of remote hosts (§7) | Partly: `--sftp-server-command`, no delta transfer (§4.2) | Gap | Remains | GDA reads local paths only. No live config uses SSH. Options: back up the rsync-made local copy (topology c), or run `rclone gda` on the host itself, which puts S3 credentials there. |
| DFS (Quobyte) sources | Covered as a local mount | Covered as a local mount | | DFS appears retired (`docs/backup-coverage-gap.md`). |
| Consistent point in time (§2.1) | Gap: needs a ZFS snapshot per run | Partly | | GDA backs up any path, including `.zfs/snapshot/<name>/...`, and its `--changes-format zfs` maps snapshot paths. Creating, keeping and destroying snapshots is left to the wrapper. |
| Finding changes without a full walk (§10.7) | Gap: needs change list, `--files-from --no-traverse`, high-water mark, reconciliation | Covered | Closed | Runs compare the source with the hot indexes, never with Deep Archive, and cache indexes locally. `--changes-from` takes `zfs diff -H` output. A change run that fails leaves its directories unbacked until a later run lists them or a full run, so the wrapper must diff from the last successful snapshot. |
| Nightly cron per job, crontab kept in git | Wrapper | Wrapper | | GDA has no scheduler. Cron and the crontab-in-git habit carry over. |
| Lock per job, NOTICE email while still running | Wrapper | Partly | | GDA holds `_gda/lock` per GDA root, refreshed hourly, taken over after `--lock-timeout` (default 24 h; help says 2 h suits nightly runs). A second run fails with "destination is locked". DR Pro's email and its "keep the lock after a pre-flight failure" rule stay in the wrapper. |
| `ENABLE=0` with reason and email | Wrapper | Wrapper | Remains (§15.6) | Nothing in GDA. |
| Parallelism: jobs in parallel, sources serial (§3 C11) | Partly: `--transfers`, `PARALLEL_SOURCES` to build (§9) | Covered | Closed | `--workers` (default one per CPU, up to 15) on one host; `rclone gda plan`, `--run`/`--partition` and `rclone gda finish` across hosts. Per-host resource limits are still open (design, milestone 5). |
| Bandwidth limit (`BWLIMIT`) | Covered: `--bwlimit` | Covered | | GDA uploads go through rclone's accounting (`lib/gda/dest.go`), where the global `--bwlimit` and its timetable apply. All live configs use `BWLIMIT=0`. |
| Per-lab configuration (`config.txt`, contacts, paths) | Wrapper (§8.1) | Wrapper | | GDA has flags only. Remotes and credentials live in `rclone.conf`. Several plan §8.1 variables become GDA flags or disappear (see migration section). |

### Cloud cost and scale

| DR Pro capability | Plain rclone | rclone-gda | Plan gap | Notes and what closes it |
| --- | --- | --- | --- | --- |
| Small files at acceptable cost (§10.3.1) | Gap: one object per file; aggregation deferred to v2 (§10.3.2) | Covered | Closed | Per-directory packs up to 256 MiB, subtrees under 16 MiB rolled into one pack. The design costs 369M files at $18 to $923 in pack PUTs, depending on files per directory, against $18,450 for one PUT per file. |
| Multipart request cost (§10.3.3) | Gap at 5 MiB parts | Covered | Closed | Packs go in one PUT; standalone files in 64 MiB parts. At 64 MiB, 173 TiB is about 2.8M parts, roughly $140 at the plan's $0.05 per 1,000, against about $1,800 at 5 MiB. (The design doc said "at least 512 MiB" at first; it now matches the code.) |
| No bucket enumeration per run (§10.6, §10.7) | Gap for `sync` | Covered | Closed | Backups read indexes (or the local cache after one `_gda/runs` listing). `gda check` needs one listing per directory, not a HEAD per object. |
| Cost visibility and governance (§15.3) | Gap | Partly | Partly closed | Each run logs its upload cost and added monthly storage; `--dry-run` estimates a backup; restores have `--estimate` and `--max-cost`; `gda prices` refreshes the price table. There is no backup-side cap or "hold for a human" circuit breaker. AWS Budgets alarms remain an operational task. |
| Compression | ZFS compression at the destination only | Covered | New | zstd level 3 in independent frames where it saves at least 10%. The ledger records bytes before compression, which can replace `current-compressratio.txt`. |
| Deduplication | None | Covered | New | Files of 1 MiB or more, per GDA root. |

### Restore and verification

| DR Pro capability | Plain rclone | rclone-gda | Plan gap | Notes and what closes it |
| --- | --- | --- | --- | --- |
| Restore tool (none today; staff copy by hand) | Gap: `drpro-restore.bash` to write (§11) | Covered | Closed | Submit, exit, `--resume <id>` from cron. Skips identical files, stops on differing ones unless `--overwrite`, asks for confirmation with the cost unless `--yes`. |
| Restore safety posture (§11.1: dry-run default, staging target, loud in-place flag) | To build | Partly | | The target directory is always explicit and conflicts stop the restore, but a real restore is the default rather than a dry run. The help warns against restoring as root into a directory other users can write. Staff-only access is enforced by who holds credentials. |
| Fast restores of recently deleted small files | Local ZFS copy only | N/A | | Deep Archive needs 12 hours (Standard) or 48 hours (Bulk). The README's common case, accidental deletion of small files, stays with the local `CURRENT` and `REVISIONS` (plan §10.3.1 rec. 4). |
| Whole-volume DR within the 72-hour RTO (§10.4) | Tight with Bulk | Tight with Bulk, far fewer requests | | Retrieval time doesn't change. Requests drop: the 2019 recovery (1,477,248 files in 53,737 directories) needs roughly one pack per directory with small files (more for directories with over 256 MiB of small files or with many delta packs), plus one object per file of 64 MiB or more, against 1.47M objects file by file. |
| Browsing and finding files without restoring | Gap: needs a manifest, listings cost money (§9.3, §10.6) | Covered | Closed | `gda ls`, `gda find` with rclone filters, the per-run catalog under `_gda/catalog/` for DuckDB, and the `gda` backend for `lsjson`, `mount`, `serve http` and Motuz. |
| Verification (§11.4, §16) | To build | Covered | Closed | `gda check` confirms every file's object exists with the right size, without reading data; `--download` checks MD5 of data readable now. Restores check MD5 against the index. `--checksum` backups re-read unchanged files. |
| Restore drills (§15.5) | Operational | Operational | Remains | `restore --estimate` prices a drill first. |

### Reporting, alerting and operations

| DR Pro capability | Plain rclone | rclone-gda | Plan gap | Notes and what closes it |
| --- | --- | --- | --- | --- |
| LOG files the reports site reads (§3, §10.6) | Wrapper, with placeholders | Wrapper | Remains | GDA writes a run ledger (`_gda/runs/<run>/<worker>.json`, STANDARD) and logs a summary line with counts and cost. With `--ledger-file` it also writes the outcome and ledger to a local JSON file. The wrapper must produce the LOG files (plan §10.5 keeps them local). |
| Email report INFO/NOTICE/WARNING | Wrapper; stderr contract breaks (§5.5) | Wrapper | Remains | rclone logging still goes to stderr, so plan §5.5 applies. `rclone gda backup` does exit non-zero when a run had errors (`lib/gda/backup.go`), unlike `drpro.bash` bug B7. |
| Destination capacity (`df`, thresholds, `backup2du.bash`) | N/A on S3 | N/A on S3 | | Replace with cost reporting and AWS Budgets. `gda gc` reports stored, live and historical data. |
| Detecting a backup that stopped (§15.2) | Gap | Gap | Remains | A status job can read the newest run under `_gda/runs/` per GDA root, as well as the local LOG trees. |
| Credentials and blast radius (§15.4) | Deny list proposed | Partly | New conflict | Backup runs never overwrite committed data, but they do delete: the `_gda/lock` object at the end of a run, uploads that fail their MD5 check (`lib/gda/dest.go`), and a standalone upload whose source changed while being read, which in a versioned bucket removes that version (`lib/gda/backup.go`). `gc --delete-expired` and `--delete-orphans` delete by design. This conflicts with denying `DeleteObjectVersion` to `backup2`. |
| Object Lock and lifecycle on `osu-drpro` (§0, §10.3) | Planned | Partly | New | Every run rewrites `gda-index.csv` of each changed directory and the lock object, so a versioned bucket keeps small noncurrent STANDARD versions. A lifecycle expiry of noncurrent versions would remove old standalone versions without recording a history cutoff, so a restore `--at` an older run would fail part way instead of being refused. |
| Reports site (`rsynclogs.bash`) | Broken today (§12) | Broken today | Remains | Unrelated to the engine; fix as plan Phase 0b. |
| Tool pinning and support (§15.6) | Pin 1.75.0 | Fork build | New | GDA exists only in the fork (commands annotated `v1.76`), format version 1. |

## Remaining gaps and risks, ranked

| Rank | Gap or risk | Why it matters | Next step | Where |
| --- | --- | --- | --- | --- |
| 1 | GDA is new and unproven at CGRB scale | The code was written on 2026-09-26 and 27 (90 commits on `gda` since `master`). Measured runs are 100,000 files locally and against S3. The 10% cloud target is about 369M files. | Pilot one lab (below). Run a full `--dry-run` over a real lab tree to measure time and memory. Pin one fork build for production. | rclone-gda, operational |
| 2 | Restoring a noncurrent Deep Archive version hasn't been waited out on AWS | Plan §11.5's top risk. The same path worked on AWS for a noncurrent GLACIER version with Expedited retrieval; Deep Archive differs only in the retrieval tier and wait. | Plan Phase 0d on `osu-drpro-scratch`: back up a large file, change it twice, then `rclone gda restore --at` the middle run with Bulk and resume it after 48 hours. | rclone-gda test |
| 3 | IAM, Object Lock and lifecycle don't yet fit GDA | Backup runs delete the lock and failed uploads; `gc` deletes expired data. A deny on `DeleteObjectVersion` or a governance retention would make these fail. | List the exact S3 actions `backup` and `gc` need. Use two identities: backup (no `DeleteObjectVersion`, if GDA can live with leftover failed uploads) and a separate gc identity. Choose one retention mechanism: `gc --keep-history` rather than a noncurrent-version lifecycle on data. | rclone-gda, AWS setup |
| 4 | No reporting or alerting for the cloud stage | A GDA failure after the INFO email is invisible (plan §15.1), and absent runs raise nothing (§15.2). | Wrapper writes `s3-*.txt` LOG files from the ledger, adds a `CLOUD :` block to the email, maps a non-zero exit to WARNING, and a status job checks `_gda/runs/`. `gda backup --ledger-file` writes the outcome and the run's ledger locally as JSON for the wrapper to read. | Wrapper |
| 5 | Change runs need a high-water mark | A failed `--changes-from` run isn't retried by the next night's `zfs diff`. A change list with no paths below a source is fine: `gda backup` makes no run and succeeds (outcome `no changes` in `--ledger-file`), so one `zfs diff` can feed every source on a dataset. | Keep the last snapshot whose GDA runs all succeeded and diff from it; listing paths again is harmless, as GDA compares them with the index. Schedule a full run (no `--changes-from`) monthly or quarterly. | Wrapper |
| 6 | Filter rules need translating and testing | `gda backup` now takes rclone's filter flags, but rsync-style `exclude.txt` rules anchored at `/` must be rewritten per source. | Translate per source and test that each rule excludes a fixture (plan §5.4's lesson). | Wrapper |
| 7 | One source per run | Each `source.txt` line becomes its own GDA root, lock and dedup scope, and the per-config totals must be summed by the wrapper. | Accept it for the pilot. Longer term, allow several sources into one root, or use filters (rank 6) to back up a common parent. | rclone-gda, wrapper |
| 8 | Restore time | Deep Archive restores take 12 to 48 hours; the 72-hour RTO is tight with Bulk (plan §10.4). | Keep the local ZFS tier (topology c). Document when to pay for Standard retrieval. | Operational |
| 9 | SSH, `/etc` and `/var` sources | GDA reads only local paths. | Cascade from the rsync-made local copy (topology c), as the plan already recommends for OS directories (§7). | Wrapper |
| 10 | Very large directories and per-host limits | Directories of more than 200,000 entries are now committed in chunks of 50,000, so memory no longer grows with the directory (300,000 files went from 2 GB to 0.8 GB), and full runs read each file's metadata once. The dedup index still takes about 300 bytes per stored file of 1 MiB or more in each process, and per-host resource limits aren't built. | Profile the filers for files per directory and for files of 1 MiB or more with the design's DuckDB query ("Profiling a file system") or the plan's `drpro-profile.bash` (Phase 0a). | rclone-gda, profiling |
| 11 | Live `drpro.bash` bugs (plan §14) | The cascade depends on the local stage: B1 (zero-byte lock defeats locking), B4 (DST collision), B5 (symlinked DFS source), B7 (rsync failure exits 0). | Fix on `master` first, as the plan says. | DR Pro |
| 12 | Self-service browsing versus staff-only recovery | The DR Pro design says recoveries are staff-only (plan §1); the `gda` backend and Motuz make lab browsing easy. Off-campus data needs governance sign-off (§15.8). | Decide the policy per lab before exposing a GDA root in Motuz. | Policy |
| 13 | Sparse files and sockets | Restored dense; sockets not recreated. Same as DR Pro today. | None needed now. | rclone-gda, low |

## Migration approach

### Shape: keep rsync, add GDA behind it

Plan topology (c) fits GDA well. `drpro.bash` keeps writing `CURRENT`
and `REVISIONS` on `backup2`, so fast restores and the reports site keep
working. After it finishes, a wrapper snapshots the lab's ZFS dataset on
`backup2` and runs `rclone gda backup` from the snapshot. This:

- reads production once, as today;
- gives GDA an immutable tree, so a long push can't be torn by the next
  night's rsync (plan §2.1);
- lets `zfs diff` run where the pool is. `backup2` can't run `zfs diff`
  on the filers' pools through NFS;
- leaves `/etc`, `/var` and any future SSH source on rsync, and still
  gets them off site.

`rclone gda backup` exits non-zero on any error, so the wrapper can treat
the cloud stage like the plan's rule in §15.6: cloud failures raise a
WARNING and release the lock, and never block the next local run.

### Per-source commands

A sketch for one source of `garcia/zfs4`, assuming the dataset
`backup2/Garcia_Lab` is mounted at `/backup2/Garcia_Lab` (verify on
`backup2`) and the remote is `osu-drpro:` as in plan §8.1:

```sh
LOG=$REVISIONS_DIR/LOG   # this night's DR Pro LOG directory
DS=backup2/Garcia_Lab
NEW=gda-$(date -u +%Y%m%dT%H%M%SZ)
zfs snapshot "$DS@$NEW"
CUR=/backup2/Garcia_Lab/.zfs/snapshot/$NEW/drpro/zfs4.cgrb.oregonstate.local/CURRENT/SRC
zfs diff -H "$DS@$LAST_OK" "$DS@$NEW" > /var/tmp/gda-changes.txt
SRC=nfs5/FW_HMSC/Garcia_Lab/programs
rclone gda backup "$CUR/$SRC" \
  "osu-drpro:osu-drpro/garcia/zfs4.cgrb.oregonstate.local/$SRC" \
  --changes-from /var/tmp/gda-changes.txt --changes-format zfs \
  --lock-timeout 2h --log-file "$LOG/gda-$(echo "$SRC" | tr / _).txt"
```

The wrapper repeats the last command for each `source.txt` line, skips a
source when no changed path falls below it, advances `LAST_OK` only when
every source succeeded, and destroys older snapshots. The first run for
each source, and a periodic reconciliation run, drop `--changes-from`.

Plan §8.1 variables that change meaning:

| Plan variable | With rclone-gda |
| --- | --- |
| `S3_AGGREGATE_BELOW`, `S3_AGGREGATE_TARGET` | `--standalone-min` (64 MiB), `--pack-size` (256 MiB), `--rollup-max` (16 MiB) |
| `S3_STORAGE_CLASS`, `S3_MANIFEST_STORAGE_CLASS` | `--data-tier`, `--meta-tier`; don't set `storage_class` on the remote |
| `S3_COMPARE`, `S3_RECONCILE_DAYS` | Change runs nightly, full runs on a schedule; `--checksum` for a deep compare |
| `S3_RETENTION_DAYS` | `rclone gda gc --keep-history`, run by a separate identity |
| `RCLONE_TRANSFERS`, `PARALLEL_SOURCES` | `--workers` |

### Pilot one lab

`core/zfs4` is CGRB's own data (`LAB_NAME_SHORT='CQLS'`), so a pilot there
affects no outside lab. It has four sources under `/nfs4/core`, which
exercises the one-root-per-source mapping.

1. Profile the lab tree for file sizes and files per directory, and run a
   full `rclone gda backup --dry-run` against the snapshot to get the
   upload cost estimate, time and memory.
2. Run the first full backup to a prefix in `osu-drpro-scratch`, then
   nightly change runs, for at least a month, with DR Pro unchanged
   alongside.
3. Test the failure paths the plan lists (§16, "Failure paths"): kill a
   run, fail a change run and check the next night catches up, remove
   network access, expire credentials.
4. Settle IAM, Object Lock and retention (rank 3) before moving to
   `osu-drpro`.

### Verifying side by side

- **Nightly**: `rclone gda check <root>` for every root. It reads no data,
  so it costs one listing per directory, and fails if any file's object is
  missing or has the wrong size.
- **Drift**: a monthly full run without `--changes-from` should report
  zero added, modified and deleted entries if the change runs missed
  nothing (plan §16, "Change-list equivalence").
- **Content**: restore a sample to a staging directory with
  `rclone gda restore --estimate` first and then `--tier Bulk`, and
  compare it against the same snapshot on `backup2` with a checksum
  compare such as `rsync -anc --itemize-changes`. Include files with
  ACLs if `--xattrs` is used, hard links, empty directories and odd
  names.
- **Point in time**: restore with `--at` a run from a week earlier and
  compare it with the matching ZFS snapshot. Repeat once standalone
  files have changed in a versioned bucket (rank 2).
- **Cost**: compare the per-run cost lines with the first AWS bill for
  the prefix (plan §16, "Cost dry-run").

### Cut-over criteria

Add a lab to the cloud stage when the pilot has run for a month with
clean `gda check` results, a successful Bulk restore drill including an
older version, a wrapper that reports cloud failures in the nightly
email, and signed-off data governance for that lab (plan §15.8). The
rsync local stage stays in every case.

## Open questions

1. Can AWS restore a noncurrent DEEP_ARCHIVE version through GDA's new
   `RequestRestore`, and does `gda check --at` read it (rank 2)?
2. Which S3 actions do `gda backup`, `gda restore` (it writes
   `_gda/restores/<id>.csv`) and `gda gc` need, and can backup runs work
   without `DeleteObjectVersion` on an Object Lock bucket?
3. Do the NFS mounts on `backup2` expose POSIX or NFSv4 ACLs and SELinux
   labels, and does the rsync copy on ZFS keep them (it doesn't today,
   without `-A`/`-X`)? If not, `--xattrs` on the cascade adds nothing.
4. Is each lab's `DEST_PATH` its own ZFS dataset on `backup2`, so a
   snapshot and `zfs diff` cover one lab only? `drpro.bash` takes the
   dataset from fields 2 and 3 of `DEST_PATH`, and `core/zfs4` and
   `core/kent` share `backup2/core`.
5. How many files does the largest directory hold on the filers in
   scope (rank 10)?
6. Should the cloud tier use one bucket per lab, as the GDA design
   assumes, or per-lab prefixes in `osu-drpro`, as the plan has it
   (plan §17 item 2)? Dedup, locks and `gc` work per GDA root either way.
7. Will rclone-gda go upstream, or stay a fork that CGRB must build and
   pin?
