# CLAUDE.local.md

Personal notes for this checkout, on top of CLAUDE.md / AGENTS.md.

## Remotes and branches

- `origin` is upstream `rclone/rclone` - never push to it.
- `dirkpetersen` is the personal fork `dirkpetersen/rclone`.
- `master` tracks `origin/master` and carries no local commits. Sync it with
  `git checkout master && git pull --ff-only && git push dirkpetersen master`.
- `dirk` is the working branch and tracks `dirkpetersen/dirk`. Update it from
  `master` with `git merge --ff-only master` (or rebase if it has commits).
- This file is committed only on `dirk`. Branch upstream PRs off `master`, not
  `dirk`, so this file does not end up in them.

## Checks and tests

- `make check` is the full quality check: `golangci-lint` plus
  `bin/markdown-lint`, so run it for docs-only changes too.
- Backend integration tests take extra flags defined in `fstest/fstest.go`:
  `-remote`, `-verbose`, `-dump-headers`, `-dump-bodies`, `-individual`,
  `-fast-list`, `-size-limit`. Example:
  `go test -v -remote TestS3: -verbose -dump-headers ./backend/s3/`
- `fstest/test_all/config.yaml` lists the remotes `test_all` runs against and
  their names (`TestS3:` etc.); those remotes must exist in the rclone config.
