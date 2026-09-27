//go:build unix

package gda

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/filter"
	"golang.org/x/sys/unix"
)

// RestoreOptions configure a restore.
type RestoreOptions struct {
	Tier      string   // retrieval tier: Bulk, Standard or Expedited
	Lifetime  int      // days the restored copies stay available
	At        string   // run ID to restore the tree as of, "" for the latest
	Overwrite bool     // replace local files which differ
	Paths     []string // paths below the root to restore; everything if empty
}

// DefaultRestoreOptions returns the default restore options.
func DefaultRestoreOptions() RestoreOptions {
	return RestoreOptions{Tier: "Bulk", Lifetime: 3}
}

// Restore states.
const (
	StateRestoring = "restoring" // waiting for objects to be restored
	StateDone      = "done"      // everything is fetched
	StateFailed    = "failed"    // everything restorable is fetched, but some files failed
)

// RestoreStatus reports the progress of a restore.
type RestoreStatus struct {
	RestoreID string        `json:"restore_id"`
	Tier      string        `json:"tier"`
	State     string        `json:"state"`
	Objects   ObjectCounts  `json:"objects"`
	Files     FileCounts    `json:"files"`
	ReadyBy   time.Time     `json:"ready_by"`
	Errors    []string      `json:"errors,omitempty"`
	Estimate  *Option       `json:"estimate,omitempty"` // estimate for the chosen tier, when starting
	Pending   []string      `json:"-"`                  // objects still being restored
	Record    RestoreRecord `json:"-"`
}

// ObjectCounts counts the objects a restore needs.
type ObjectCounts struct {
	Requested int `json:"requested"` // objects holding data to fetch
	Restoring int `json:"restoring"` // objects not readable yet
	Fetched   int `json:"fetched"`   // objects whose files are all in place
}

// FileCounts counts the files of a restore.
type FileCounts struct {
	Total            int `json:"total"`             // entries to restore
	Fetched          int `json:"fetched"`           // entries in place
	SkippedIdentical int `json:"skipped_identical"` // entries which were already in place when the restore started
	Failed           int `json:"failed"`            // entries which couldn't be restored
}

// RestoreRecord describes a restore. It is saved as
// _gda/restores/<id>.json next to the plan in _gda/restores/<id>.csv.
type RestoreRecord struct {
	ID        string
	Tier      string
	Lifetime  int
	At        string
	Target    string
	Paths     []string
	Overwrite bool
	Requested time.Time
	ReadyBy   time.Time
}

// tierHours is how long each retrieval tier takes at most for Deep Archive.
var tierHours = map[string]int{"Expedited": 5, "Standard": 12, "Bulk": 48}

// actionSkip marks plan rows already in place when the restore started.
const actionSkip = "skip"

// newRestoreID returns a new restore ID.
func newRestoreID() string {
	var b [2]byte
	_, _ = rand.Read(b[:])
	return NewRunID(timeNow()) + "-" + hex.EncodeToString(b[:])
}

func restoreKey(id, ext string) string {
	return joinRemote(MetaDir, "restores", id+ext)
}

// StartRestore plans the restore of the GDA tree at dst into the local
// directory target, requests the restore of the objects it needs, saves
// the plan and fetches whatever is already readable.
//
// If local files differ from the ones to restore and Overwrite isn't
// set, it returns an error before requesting anything.
func StartRestore(ctx context.Context, dst fs.Fs, target string, opt RestoreOptions) (*RestoreStatus, error) {
	if _, ok := tierHours[opt.Tier]; !ok {
		return nil, fmt.Errorf("unknown restore tier %q: use Bulk, Standard or Expedited", opt.Tier)
	}
	at, err := ParseAt(opt.At)
	if err != nil {
		return nil, err
	}
	d := &dest{f: dst, metaTier: "STANDARD", retries: 3, dryRun: fs.GetConfig(ctx).DryRun}
	t := newTree(d, at)
	plan, err := buildPlan(ctx, t, opt.Paths)
	if err != nil {
		return nil, err
	}
	if len(plan) == 0 {
		return nil, errors.New("nothing to restore")
	}
	conflicts, err := markIdentical(plan, target)
	if err != nil {
		return nil, err
	}
	if len(conflicts) > 0 && !opt.Overwrite {
		sort.Strings(conflicts)
		if len(conflicts) > 20 {
			conflicts = append(conflicts[:20], fmt.Sprintf("and %d more", len(conflicts)-20))
		}
		return nil, fmt.Errorf("these local files differ from the backup, use --overwrite to replace them:\n  %s", strings.Join(conflicts, "\n  "))
	}
	now := timeNow().UTC()
	rec := RestoreRecord{
		ID:        newRestoreID(),
		Tier:      opt.Tier,
		Lifetime:  opt.Lifetime,
		At:        at,
		Target:    target,
		Paths:     opt.Paths,
		Overwrite: opt.Overwrite,
		Requested: now,
		ReadyBy:   now.Add(time.Duration(tierHours[opt.Tier]) * time.Hour),
	}
	if err := requestRestore(ctx, dst, planObjects(plan), rec.Tier, rec.Lifetime); err != nil {
		return nil, err
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := d.writeEntries(ctx, restoreKey(rec.ID, ".csv"), plan, planColumns); err != nil {
		return nil, fmt.Errorf("save restore plan: %w", err)
	}
	if err := d.putBytes(ctx, restoreKey(rec.ID, ".json"), data, d.metaTier); err != nil {
		return nil, fmt.Errorf("save restore record: %w", err)
	}
	return fetchPlan(ctx, d, rec, plan), nil
}

// ResumeRestore fetches whatever is readable of the restore with the
// given ID and reports its progress. If target isn't "" it replaces the
// target directory saved with the restore.
func ResumeRestore(ctx context.Context, dst fs.Fs, id, target string) (*RestoreStatus, error) {
	d := &dest{f: dst, metaTier: "STANDARD", retries: 3, dryRun: fs.GetConfig(ctx).DryRun}
	data, err := d.get(ctx, restoreKey(id, ".json"))
	if err != nil {
		return nil, fmt.Errorf("read restore %q: %w", id, err)
	}
	var rec RestoreRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("parse restore %q: %w", id, err)
	}
	if target != "" {
		rec.Target = target
	}
	data, err = d.get(ctx, restoreKey(id, ".csv"))
	if err != nil {
		return nil, fmt.Errorf("read restore plan %q: %w", id, err)
	}
	plan, err := ReadEntries(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parse restore plan %q: %w", id, err)
	}
	return fetchPlan(ctx, d, rec, plan), nil
}

// buildPlan returns the entries to restore. In the plan, Target is the
// path below the target directory, Location is the key of the object
// holding the data relative to the root, and Name stays the entry's name
// in its index, which is also its tar member name.
func buildPlan(ctx context.Context, t *tree, paths []string) ([]Entry, error) {
	if len(paths) == 0 {
		paths = []string{""}
	}
	var plan []Entry
	seen := map[string]bool{}
	for _, p := range paths {
		p = strings.Trim(p, "/")
		_, _, row, _, err := t.resolve(ctx, p)
		if err != nil {
			return nil, err
		}
		base := p
		if row != nil && !row.IsDir() {
			base = path.Dir(p)
			if base == "." {
				base = ""
			}
		}
		err = t.walk(ctx, p, func(l *Located) error {
			name := joinRemote(base, l.LocalPath)
			if name == "" || seen[name] {
				return nil
			}
			seen[name] = true
			e := l.Entry
			e.Target = name
			if e.Location != "" {
				e.Location = l.ObjectKey()
			}
			e.Action = ""
			plan = append(plan, e)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return plan, nil
}

// planObjects returns the keys of the objects holding data of plan
// entries which aren't in place yet.
func planObjects(plan []Entry) []string {
	keys := map[string]bool{}
	for _, e := range plan {
		if e.Type == TypeFile && e.Action != actionSkip {
			keys[e.Location] = true
		}
	}
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// markIdentical marks plan entries already in place under target with
// actionSkip and returns the names of those which exist but differ.
func markIdentical(plan []Entry, target string) (conflicts []string, err error) {
	for i := range plan {
		e := &plan[i]
		same, exists, err := inPlace(e, filepath.Join(target, filepath.FromSlash(e.Target)))
		if err != nil {
			return nil, err
		}
		switch {
		case same:
			e.Action = actionSkip
		case exists && !e.IsDir():
			conflicts = append(conflicts, e.Target)
		}
	}
	return conflicts, nil
}

// inPlace returns whether the local path p exists, and whether it
// already holds entry e.
func inPlace(e *Entry, p string) (same, exists bool, err error) {
	info, err := os.Lstat(p)
	if errors.Is(err, os.ErrNotExist) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	switch e.Type {
	case TypeDir:
		return info.IsDir(), true, nil
	case TypeFile:
		if !info.Mode().IsRegular() || info.Size() != e.Size {
			return false, true, nil
		}
		sum, err := hashFile(p)
		if err != nil {
			return false, true, err
		}
		return sum == e.MD5, true, nil
	case TypeSymlink:
		target, err := os.Readlink(p)
		return err == nil && target == e.LinkTarget, true, nil
	default:
		return entryType(info.Mode()) == e.Type, true, nil
	}
}

// requestRestore asks the backend to restore the objects at keys. It
// does nothing for backends without a restore command, whose objects
// are always readable.
func requestRestore(ctx context.Context, f fs.Fs, keys []string, tier string, lifetime int) error {
	command := f.Features().Command
	if command == nil || len(keys) == 0 {
		return nil
	}
	fi, err := filter.NewFilter(nil)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := fi.AddFile(k); err != nil {
			return err
		}
	}
	// Address exactly the objects in the filter rather than listing the tree.
	ctx = filter.ReplaceConfig(ctx, fi)
	ctx, ci := fs.AddConfig(ctx)
	ci.NoTraverse = true
	out, err := command(ctx, "restore", nil, map[string]string{
		"priority": tier,
		"lifetime": strconv.Itoa(lifetime),
	})
	if errors.Is(err, fs.ErrorCommandNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("request restore: %w", err)
	}
	data, err := json.Marshal(out)
	if err != nil {
		return err
	}
	var results []struct{ Status, Remote string }
	if err := json.Unmarshal(data, &results); err != nil {
		return nil
	}
	for _, r := range results {
		if r.Status != "OK" && !strings.HasPrefix(r.Status, "Not ") && !strings.Contains(r.Status, "RestoreAlreadyInProgress") {
			fs.Errorf(r.Remote, "gda: restore request: %s", r.Status)
		}
	}
	return nil
}

// isRestorePending returns true if err means the object must be
// restored before it can be read.
func isRestorePending(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "restore first") || strings.Contains(err.Error(), "InvalidObjectState"))
}

// fetchPlan fetches the data of every plan entry that isn't in place and
// whose object is readable, then creates the other entries and applies
// metadata.
func fetchPlan(ctx context.Context, d *dest, rec RestoreRecord, plan []Entry) *RestoreStatus {
	st := &RestoreStatus{RestoreID: rec.ID, Tier: rec.Tier, ReadyBy: rec.ReadyBy, Record: rec}
	st.Files.Total = len(plan)
	errorf := func(format string, args ...any) {
		err := fmt.Sprintf(format, args...)
		fs.Errorf(nil, "gda: %s", err)
		if len(st.Errors) < maxLedgerErrors {
			st.Errors = append(st.Errors, err)
		}
	}
	localPath := func(e *Entry) string {
		return filepath.Join(rec.Target, filepath.FromSlash(e.Target))
	}
	if d.dryRun {
		st.State = StateRestoring
		return st
	}
	done := make([]bool, len(plan))
	failed := make([]bool, len(plan))
	for i := range plan {
		if plan[i].Action == actionSkip {
			st.Files.SkippedIdentical++
		}
	}

	// Directories first, so files have somewhere to go.
	for i := range plan {
		e := &plan[i]
		if e.IsDir() {
			// The umask applies, as with mkdir; the recorded mode is set
			// once the directory's contents are in place.
			if err := os.MkdirAll(localPath(e), 0o777); err != nil {
				errorf("create directory %q: %v", e.Target, err)
				failed[i] = true
			}
		}
	}

	// Files, grouped by the object holding their data.
	byObject := map[string][]int{}
	for i := range plan {
		e := &plan[i]
		if e.Type != TypeFile {
			continue
		}
		if e.Action == actionSkip {
			done[i] = true
			continue
		}
		byObject[e.Location] = append(byObject[e.Location], i)
	}
	keys := make([]string, 0, len(byObject))
	for k := range byObject {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	st.Objects.Requested = len(keys)
	for _, key := range keys {
		var todo []int
		for _, i := range byObject[key] {
			same, _, err := inPlace(&plan[i], localPath(&plan[i]))
			if err == nil && same {
				done[i] = true
				continue
			}
			todo = append(todo, i)
		}
		if len(todo) == 0 {
			st.Objects.Fetched++
			continue
		}
		fetched, err := fetchObject(ctx, d, key, plan, todo, localPath)
		switch {
		case isRestorePending(err):
			st.Objects.Restoring++
			st.Pending = append(st.Pending, key)
			continue
		case err != nil:
			errorf("fetch %q: %v", key, err)
		}
		for _, i := range todo {
			if fetched[i] {
				done[i] = true
			} else {
				failed[i] = true
			}
		}
		if err == nil && len(fetched) == len(todo) {
			st.Objects.Fetched++
		}
	}

	// Symlinks and special files need no data.
	for i := range plan {
		e := &plan[i]
		if e.Type == TypeFile || e.IsDir() {
			continue
		}
		if err := createSpecial(e, localPath(e), rec.Overwrite); err != nil {
			errorf("create %q: %v", e.Target, err)
			failed[i] = true
			continue
		}
		done[i] = true
	}

	isRoot := os.Geteuid() == 0
	for i := range plan {
		e := &plan[i]
		if done[i] && e.Type != TypeDir {
			if err := applyMeta(e, localPath(e), isRoot); err != nil {
				errorf("set metadata of %q: %v", e.Target, err)
			}
		}
	}
	if st.Objects.Restoring == 0 {
		// Directory metadata last and deepest first, as creating their
		// contents changes their modification times.
		for i := len(plan) - 1; i >= 0; i-- {
			e := &plan[i]
			if e.IsDir() && !failed[i] {
				if err := applyMeta(e, localPath(e), isRoot); err != nil {
					errorf("set metadata of %q: %v", e.Target, err)
				}
				done[i] = true
			}
		}
	}
	for i := range plan {
		if failed[i] {
			st.Files.Failed++
		} else if done[i] {
			st.Files.Fetched++
		}
	}
	switch {
	case st.Objects.Restoring > 0:
		st.State = StateRestoring
	case st.Files.Failed > 0:
		st.State = StateFailed
	default:
		st.State = StateDone
	}
	return st
}

// wholeObjectMembers is the number of members above which a pack is
// downloaded whole rather than with a ranged read per member.
const wholeObjectMembers = 16

// fetchObject fetches the plan entries at todo from the object at key.
// It returns which of them were written; the error is the first problem
// with the object as a whole.
func fetchObject(ctx context.Context, d *dest, key string, plan []Entry, todo []int, localPath func(*Entry) string) (map[int]bool, error) {
	fetched := map[int]bool{}
	o, err := d.f.NewObject(ctx, key)
	if err != nil {
		return fetched, err
	}
	first := &plan[todo[0]]
	if first.Offset < 0 {
		// A standalone object holds exactly one file.
		in, err := o.Open(ctx)
		if err != nil {
			return fetched, err
		}
		err = writeVerified(in, first, localPath(first))
		if err == nil {
			fetched[todo[0]] = true
		}
		return fetched, err
	}
	var want int64
	for _, i := range todo {
		want += plan[i].StoredLength
	}
	if len(todo) <= wholeObjectMembers && want*2 < o.Size() {
		for _, i := range todo {
			e := &plan[i]
			in, err := o.Open(ctx, &fs.RangeOption{Start: e.StoredOffset, End: e.StoredOffset + e.StoredLength - 1})
			if err != nil {
				return fetched, err
			}
			if err := writeVerified(in, e, localPath(e)); err != nil {
				fs.Errorf(e.Target, "gda: %v", err)
				continue
			}
			fetched[i] = true
		}
		return fetched, nil
	}
	return fetched, extractPack(ctx, o, plan, todo, localPath, fetched)
}

// extractPack reads the whole pack o and writes the members at todo.
func extractPack(ctx context.Context, o fs.Object, plan []Entry, todo []int, localPath func(*Entry) string, fetched map[int]bool) (err error) {
	wanted := make(map[string]int, len(todo))
	for _, i := range todo {
		// Tar member names are the entries' names in their index.
		wanted[plan[i].Name] = i
	}
	in, err := o.Open(ctx)
	if err != nil {
		return err
	}
	defer fs.CheckClose(in, &err)
	tr := tar.NewReader(in)
	for len(wanted) > 0 {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read pack: %w", err)
		}
		i, ok := wanted[hdr.Name]
		if !ok {
			continue
		}
		delete(wanted, hdr.Name)
		e := &plan[i]
		if err := writeVerified(io.NopCloser(tr), e, localPath(e)); err != nil {
			fs.Errorf(e.Target, "gda: %v", err)
			continue
		}
		fetched[i] = true
	}
	for name := range wanted {
		fs.Errorf(name, "gda: not found in pack %q", o.Remote())
	}
	return nil
}

// writeVerified writes in to p through a temporary file, checking the
// size and MD5 against e before putting it in place.
func writeVerified(in io.ReadCloser, e *Entry, p string) (err error) {
	defer fs.CheckClose(in, &err)
	if err := os.MkdirAll(filepath.Dir(p), 0o777); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".gda-restore-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	h := md5.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(in, e.Size))
	if err != nil {
		return err
	}
	if n != e.Size {
		return fmt.Errorf("%q: got %d bytes, want %d", e.Target, n, e.Size)
	}
	if sum := hex.EncodeToString(h.Sum(nil)); e.MD5 != "" && sum != e.MD5 {
		return fmt.Errorf("%q: MD5 is %s, want %s", e.Target, sum, e.MD5)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), p)
}

// createSpecial creates a symlink or special file.
func createSpecial(e *Entry, p string, overwrite bool) error {
	same, exists, err := inPlace(e, p)
	if err != nil || same {
		return err
	}
	if exists {
		if !overwrite {
			return errors.New("exists and differs; use --overwrite to replace it")
		}
		if err := os.Remove(p); err != nil {
			return err
		}
	}
	switch e.Type {
	case TypeSymlink:
		return os.Symlink(e.LinkTarget, p)
	case TypeFifo:
		return syscall.Mkfifo(p, e.Mode&0o777)
	case TypeCharDev, TypeBlockDev:
		if os.Geteuid() != 0 || e.DevMajor < 0 || e.DevMinor < 0 {
			fs.Logf(e.Target, "gda: not restoring %s: needs root and device numbers", e.Type)
			return nil
		}
		kind := uint32(unix.S_IFCHR)
		if e.Type == TypeBlockDev {
			kind = unix.S_IFBLK
		}
		return unix.Mknod(p, kind|e.Mode&0o777, int(unix.Mkdev(uint32(e.DevMajor), uint32(e.DevMinor))))
	default:
		fs.Logf(e.Target, "gda: not restoring %s", e.Type)
		return nil
	}
}

// applyMeta sets the permissions and modification time of p from e, and
// its owner and group when running as root.
func applyMeta(e *Entry, p string, isRoot bool) error {
	if e.Type == TypeSocket {
		return nil
	}
	if _, err := os.Lstat(p); err != nil {
		// Entries which couldn't be created, such as devices when not root.
		return nil
	}
	if isRoot {
		uid, gid := lookupOwner(e)
		if uid >= 0 || gid >= 0 {
			if err := os.Lchown(p, uid, gid); err != nil {
				return err
			}
		}
	}
	if e.Type == TypeSymlink {
		// Symlink permissions can't be set on Linux, but their times can.
		if e.ModTime.IsZero() {
			return nil
		}
		ts := unix.NsecToTimespec(e.ModTime.UnixNano())
		return unix.UtimesNanoAt(unix.AT_FDCWD, p, []unix.Timespec{ts, ts}, unix.AT_SYMLINK_NOFOLLOW)
	}
	mode := iofs.FileMode(e.Mode & 0o777)
	if e.Mode&syscall.S_ISVTX != 0 {
		mode |= iofs.ModeSticky
	}
	if isRoot {
		if e.Mode&syscall.S_ISUID != 0 {
			mode |= iofs.ModeSetuid
		}
		if e.Mode&syscall.S_ISGID != 0 {
			mode |= iofs.ModeSetgid
		}
	}
	if err := os.Chmod(p, mode); err != nil {
		return err
	}
	if e.ModTime.IsZero() {
		return nil
	}
	return os.Chtimes(p, e.ModTime, e.ModTime)
}

// lookupOwner returns the numeric owner and group for e, by name first
// and by the recorded IDs if the names don't exist here.
func lookupOwner(e *Entry) (uid, gid int) {
	uid, gid = int(e.UID), int(e.GID)
	if e.Owner != "" {
		if u, err := user.Lookup(e.Owner); err == nil {
			if n, err := strconv.Atoi(u.Uid); err == nil {
				uid = n
			}
		}
	}
	if e.Group != "" {
		if g, err := user.LookupGroup(e.Group); err == nil {
			if n, err := strconv.Atoi(g.Gid); err == nil {
				gid = n
			}
		}
	}
	return uid, gid
}
