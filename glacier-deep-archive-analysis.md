# Rclone and S3 Glacier Deep Archive: readiness for low-cost archiving

Analysis of rclone at commit `f36a17bb1` (upstream `9dc8b71ae`, 2026-09-26).
File references are relative to the repository root; `s3.go` means
`backend/s3/s3.go`.

## Summary

Rclone can upload straight into Deep Archive and manage restores, but it has
**no notion of preparing objects to keep Deep Archive cheap**. Nothing
bundles small files, picks a storage class by file size, or protects objects
that are already archived. Some default behaviour works against you:

- `sync` can re-upload unchanged files, paying a new PUT plus an
  early-deletion charge.
- Default multipart part sizes multiply request charges on large files.
- Overwrites and deletes of young archived objects go ahead without warning.

## What costs money in Deep Archive

AWS us-east-1 list prices as known at the time of writing; check current
pricing before relying on these.

| Item | Price |
|---|---|
| Storage | $0.00099 per GB-month, minimum billed duration 180 days |
| PUT, COPY, LIST and each multipart part | $0.05 per 1,000 requests ($0.00005 each) |
| Overhead per object | 40 KB: 32 KB billed at Deep Archive rates, 8 KB at S3 Standard rates |
| Restore, Bulk | $0.0025/GB plus $0.025 per 1,000 objects, about 48 h |
| Restore, Standard | $0.02/GB plus $0.10 per 1,000 objects, about 12 h |

### Request charges dominate for small files

- One PUT ($0.00005) costs the same as 180 days of storage for about
  **8 MB**, or 5 years of storage for about **0.8 MB**.
- One million 100 KB files is about 100 GB. Storage is roughly $0.10 per
  month, but the uploads alone cost **$50**, and a later bulk restore adds
  $25 in per-object request fees.
- The same data packed into 100 archives of 1 GB each costs half a cent in
  PUTs.
- The 40 KB per-object overhead adds about $0.21 per month per million
  objects, which is small next to the PUT cost.

## What rclone already supports

- **Uploading straight into Deep Archive** with
  `--s3-storage-class DEEP_ARCHIVE`. It applies to single-part uploads,
  multipart uploads and server-side copies through `prepareUpload`
  (`s3.go:5152`). Uploading directly avoids paying for a lifecycle
  transition on top of the PUT.
- **Restores:**
  - `rclone backend restore` takes `priority=Bulk|Standard|Expedited` and
    `lifetime=N` days, respects filters and `--dry-run`, and sends one
    `RestoreObject` per object (`s3.go:3662`).
  - `rclone backend restore-status` reads restore status straight from the
    listing via `OptionalObjectAttributes`, so it needs no extra request per
    object (`s3.go:2512`). It does not apply filters.
- **Tier visibility:** `lsjson` and `lsf` (format letter `T`) show each
  object's storage class. `rclone settier` changes it by copying the object
  onto itself (`s3.go:5456`), which is billed again as a COPY.
- **Clear errors:** reading an archived object fails with
  `Object in GLACIER, restore first` (`s3.go:4529`). The message says GLACIER
  even for Deep Archive objects.
- **`rclone archive create`** (v1.72): writes zip, tar, or compressed tar
  (gz, bz2, xz, zst, lz4, br, ...) and streams it straight to S3 without a
  local temporary file (`cmd/archive/create/create.go:374`).
- **Reading one file out of a zip:** the `:archive:` backend can fetch a
  single zip member with range reads (`backend/archive/zip/zip.go`). The
  whole zip must be restored first.

## Gaps and traps, most costly first

### 1. Sync re-uploads unchanged files

If a Deep Archive object has the same content as the source but a different
modification time, rclone re-uploads it: a new PUT plus an early-deletion
charge if the object is younger than 180 days.

- `SetModTime` deliberately returns `fs.ErrorCantSetModTime` for GLACIER
  and DEEP_ARCHIVE objects (`s3.go:4340`), after a HEAD.
- `operations.equal()` then logs "src and dst identical but can't set mod
  time without re-uploading" and treats the pair as different
  (`fs/operations/operations.go:332`).
- Flags that avoid it: `--size-only`, `--checksum`, `--ignore-existing`, or
  `--no-update-modtime` (only when both sides have an MD5).
- `--update` alone still hits it when the source is newer. `--ignore-times`
  re-uploads everything.
- Multipart objects without `md5chksum` metadata have no MD5, so any
  modification time difference leads to a transfer.

### 2. Nothing stops overwrites or deletes of archived objects

The Azure Blob backend refuses to overwrite archive-tier blobs unless
`archive_tier_delete` is set (`backend/azureblob/azureblob.go:3223`). The S3
backend has no equivalent, so all of these go ahead on archived objects:

- sync deletions and overwrites;
- `--backup-dir` and `--suffix`, which server-side copy the old object
  first;
- `--track-renames` and `--fix-case`, which server-side move it.

No generic code consults the tier: `fs/sync` and `fs/march` never call
`GetTier`. Using `copy` instead of `sync`, plus `--immutable`, is the safe
pattern.

Two further risks from reading the code, not tested:

- `--max-age` or `--metadata-exclude tier=...` can hide an existing
  destination object from the listing. The source file is then treated as
  new and uploaded with a plain PUT, bypassing `--immutable` and
  `--ignore-existing`.
- `--no-check-dest` always transfers, so it overwrites archived objects.

### 3. No bundling of small files

- `rclone archive create` walks the whole source and writes exactly **one**
  archive (`cmd/archive/create/create.go:319`). There is no option to split
  by size or file count, and no manifest or index is written.
- The streamed upload has no MD5, because S3 only stores `md5chksum` when
  the MD5 is known before the upload starts (`s3.go:5106`).
- Streams of unknown size use the fixed `chunk_size`, so the default 5 MiB
  caps a single archive at about 48 GiB (`s3.go:4629`).
- The `compress` backend makes things worse: every file becomes two objects,
  the data plus a `.json` sidecar (`backend/compress/compress.go:340`).
- The `chunker` backend's default "rename" transactions upload chunks under
  temporary names and then move them, which on S3 is a copy plus delete.

### 4. Default part sizes multiply requests

Defaults: `upload_cutoff` 200 MiB, `chunk_size` 5 MiB, `max_upload_parts`
10000, `copy_cutoff` 4768 MiB (`s3.go:974-978`). Every part is a billed PUT.

| File size | Requests with defaults | With `--s3-chunk-size 1G` |
|---|---|---|
| 200 MiB | 42 | 3 |
| 1 GiB | 207 | 3 |
| 100 GiB | about 9,300 (about $0.47) | 102 |

The 100 GiB case costs roughly as much in requests as its 180-day storage
minimum (about $0.64).

### 5. No per-size storage class

Every object gets the same storage class; the only size check is the
multipart switch (`s3.go:5228`). This can be approximated today with two runs
using `--max-size` and `--min-size` and different `--s3-storage-class`
values.

### 6. Probable bugs (read from the code, not tested)

- **Versions listings drop the storage class.**
  `setFrom_typesObject_typesObjectVersion` doesn't copy `StorageClass`
  (`backend/s3/setfrom.go:71`), so with `--s3-versions` every object reads as
  STANDARD. `backend restore` would then skip everything as "Not GLACIER or
  DEEP_ARCHIVE or INTELLIGENT_TIERING storage class".
- **Intelligent-Tiering archive tiers are not guarded.** The `SetModTime`
  check only covers GLACIER and DEEP_ARCHIVE (`s3.go:4341`), so objects in
  the Intelligent-Tiering Archive or Deep Archive access tiers attempt a
  CopyObject.
- **`SetModTime` can move objects into Deep Archive.** Its self-copy carries
  no storage class, so `f.copy` applies the configured `storage_class`
  (`s3.go:3074`). Updating the modification time of a STANDARD object on a
  remote configured with `storage_class=DEEP_ARCHIVE` archives it.
- **`settier` on large objects may lose Content-Type.** The multipart copy
  path overwrites the HEAD-derived Content-Type and similar fields with the
  empty values from SetTier's request (`s3.go:3138`).

### 7. Missing large-scale features

- No S3 Batch Operations: restores are one `RestoreObject` per object.
- No S3 Inventory and no lifecycle configuration; the only bucket setting
  rclone manages is versioning.
- No batched deletes: one `DeleteObject` per object.
- `cryptcheck`, `check --download`, `hashsum --download` and
  `bisync --download-hash` open destination objects, so they fail on
  archived objects.
- No support for the newer S3 additional checksums (CRC32, CRC64NVME,
  SHA256); rclone only exposes MD5.

## Suggested settings with today's code

```bash
rclone copy /data s3:bucket/archive \
  --s3-storage-class DEEP_ARCHIVE \
  --s3-chunk-size 512M --s3-upload-cutoff 1G \
  --size-only --immutable \
  --s3-no-check-bucket --fast-list
```

- Use `copy`, not `sync`, so nothing is deleted.
- `--size-only` avoids both modification-time re-uploads and HEAD requests
  for modification times. `--use-server-modtime` also avoids those HEADs.
- Multipart buffers use roughly
  `chunk_size × --s3-upload-concurrency × --transfers` of memory; the
  settings above need about 8 GiB (512 MiB × 4 × 4).
- Keep the default HEAD after upload: it costs $0.0004 per 1,000 and
  verifies the upload.

### Bundling small files today

1. Group files into bins, for example by top-level directory, and write each
   bin's file list to `bin_N.txt`.
2. Write a manifest per bin with `rclone lsf -R --format pst`.
3. Run `rclone archive create --files-from bin_N.txt` once per bin, with a
   larger `--s3-chunk-size` if a bin may exceed 48 GiB.
4. Upload the manifests to STANDARD so you can search them without a
   restore.

## Improvements worth exploring in rclone

| Idea | Where | Notes |
|---|---|---|
| Split archives by byte or file limit, with a manifest | `cmd/archive/create` | Small, self-contained; likely acceptable upstream |
| Treat archived objects as equal when hashes match, instead of re-uploading | `fs/operations/operations.go` or `s3.go` `SetModTime` | Needs a flag or care to keep backwards compatibility |
| Guard against overwriting or deleting young archived objects | `backend/s3` | Model on azureblob `archive_tier_delete` |
| Storage class by size, e.g. `--s3-storage-class-min-size` | `backend/s3` `prepareUpload` | Small files to a cheaper-per-request class |
| Larger part sizes automatically for DEEP_ARCHIVE | `backend/s3` | Trade-off with memory use |
| Copy `StorageClass` in versions listings | `backend/s3/setfrom.go` | Probable bug fix |
| Add Intelligent-Tiering archive tiers to the `SetModTime` guard | `s3.go:4341` | Probable bug fix |
