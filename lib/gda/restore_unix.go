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
	"golang.org/x/sync/errgroup"
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
	Errors    []string      `json:"errors"`
	Estimate  *Option       `json:"estimate,omitempty"` // estimate for the chosen tier, when starting
	Pending   []string      `json:"-"`                  // objects still being restored
	Record    RestoreRecord `json:"-"`
}

// ObjectCounts counts the objects a restore needs data from.
type ObjectCounts struct {
	Requested int `json:"requested"` // objects holding data the restore needs
	Restoring int `json:"restoring"` // objects not readable yet
	Fetched   int `json:"fetched"`   // objects whose files are all in place
}

// FileCounts counts the files of a restore.
type FileCounts struct {
	Total            int `json:"total"`             // entries to restore
	Fetched          int `json:"fetched"`           // entries in place
	SkippedIdentical int `json:"skipped_identical"` // entries which were already in place when the restore started
	Failed           int `json:"failed"`            // entries which couldn't be restored
	Unsupported      int `json:"unsupported"`       // entries which can't be recreated here, such as devices when not root
}

// RestoreRecord describes a restore. It is saved as
// _gda/restores/<id>.json next to the plan in _gda/restores/<id>.csv.
//
// Target and Overwrite are kept for information only: a resumed restore
// takes them from its caller, as anyone who can write the destination
// could change the record.
type RestoreRecord struct {
	ID            string
	Tier          string
	Lifetime      int
	At            string
	Target        string
	Paths         []string
	Overwrite     bool
	Requested     time.Time
	ReadyBy       time.Time
	RequestedKeys []string // objects whose restore was requested
}

// actionSkip marks plan rows already in place when the restore started.
const actionSkip = "skip"

// newRestoreID returns a new restore ID.
func newRestoreID() string {
	var b [2]byte
	_, _ = rand.Read(b[:])
	return NewRunID(timeNow()) + "-" + hex.EncodeToString(b[:])
}

// restoreKey returns the key of a restore's record or plan.
func restoreKey(id, ext string) string {
	return joinRemote(MetaDir, "restores", id+ext)
}

// archivedClasses are the storage classes whose objects may need
// restoring before they can be read.
var archivedClasses = map[string]bool{"GLACIER": true, "DEEP_ARCHIVE": true, "INTELLIGENT_TIERING": true}

// planObject is an object a restore needs data from.
type planObject struct {
	o        fs.Object
	class    string // storage class, if the backend reports one
	readable bool   // whether it can be read now
}

// RestorePlan is a restore which has been planned but not started.
type RestorePlan struct {
	f         fs.Fs
	d         *dest
	opt       RestoreOptions
	at        string
	target    string
	entries   []Entry
	conflicts []string
	objects   map[string]*planObject // by key; nil until found
}

// PlanRestore plans restoring the GDA tree at dst, or the paths of it in
// opt, into the local directory target. It reads only indexes and local
// files.
func PlanRestore(ctx context.Context, dst fs.Fs, target string, opt RestoreOptions) (*RestorePlan, error) {
	tier, err := canonicalTier(opt.Tier)
	if err != nil {
		return nil, err
	}
	opt.Tier = tier
	at, err := ParseAt(opt.At)
	if err != nil {
		return nil, err
	}
	d := &dest{f: dst, metaTier: "STANDARD", retries: 3, dryRun: fs.GetConfig(ctx).DryRun}
	entries, err := buildPlan(ctx, newTree(d, at), opt.Paths)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, errors.New("nothing to restore")
	}
	conflicts, err := markIdentical(entries, target)
	if err != nil {
		return nil, err
	}
	return &RestorePlan{f: dst, d: d, opt: opt, at: at, target: target, entries: entries, conflicts: conflicts}, nil
}

// findObjects finds the objects the plan needs data from, with one
// listing per directory, and which of them can be read now. Archived
// objects are probed by reading their first byte, which fails until
// they are restored and succeeds while a restored copy exists.
func (p *RestorePlan) findObjects(ctx context.Context) (map[string]*planObject, error) {
	if p.objects != nil {
		return p.objects, nil
	}
	byObject := map[string][]int{}
	for i := range p.entries {
		if needsData(&p.entries[i]) {
			byObject[p.entries[i].Location] = append(byObject[p.entries[i].Location], i)
		}
	}
	listed, err := listObjects(ctx, p.f, byObject)
	if err != nil {
		return nil, err
	}
	objects := make(map[string]*planObject, len(listed))
	var toProbe []*planObject
	for key, o := range listed {
		po := &planObject{o: o, readable: true}
		if t, ok := o.(fs.GetTierer); ok && p.f.Features().GetTier {
			po.class = strings.ToUpper(t.GetTier())
		}
		if archivedClasses[po.class] {
			toProbe = append(toProbe, po)
		}
		objects[key] = po
	}
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(max(fs.GetConfig(ctx).Checkers, 1))
	for _, po := range toProbe {
		g.Go(func() error {
			in, err := po.o.Open(gCtx, &fs.RangeOption{Start: 0, End: 0})
			if isRestorePending(err) {
				po.readable = false
				return nil
			}
			if err != nil {
				return fmt.Errorf("check %q: %w", po.o.Remote(), err)
			}
			return in.Close()
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	p.objects = objects
	return objects, nil
}

// Start requests restores of the objects the plan needs which can't be
// read yet, saves the plan and fetches whatever is readable.
//
// If local files differ from the ones to restore and Overwrite isn't
// set, it returns an error before requesting anything.
func (p *RestorePlan) Start(ctx context.Context) (*RestoreStatus, error) {
	if len(p.conflicts) > 0 && !p.opt.Overwrite {
		conflicts := append([]string(nil), p.conflicts...)
		sort.Strings(conflicts)
		if len(conflicts) > 20 {
			conflicts = append(conflicts[:20], fmt.Sprintf("and %d more", len(conflicts)-20))
		}
		return nil, fmt.Errorf("these local files differ from the backup, use --overwrite to replace them:\n  %s", strings.Join(conflicts, "\n  "))
	}
	objects, err := p.findObjects(ctx)
	if err != nil {
		return nil, err
	}
	var keys []string
	for key, po := range objects {
		if !po.readable {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	now := timeNow().UTC()
	rec := RestoreRecord{
		ID:            newRestoreID(),
		Tier:          p.opt.Tier,
		Lifetime:      p.opt.Lifetime,
		At:            p.at,
		Target:        p.target,
		Paths:         p.opt.Paths,
		Overwrite:     p.opt.Overwrite,
		Requested:     now.Truncate(time.Second),
		ReadyBy:       readyBy(now, p.opt.Tier),
		RequestedKeys: keys,
	}
	if err := requestRestore(ctx, p.f, keys, rec.Tier, rec.Lifetime); err != nil {
		return nil, err
	}
	if err := p.d.writeEntries(ctx, restoreKey(rec.ID, ".csv"), p.entries, planColumns); err != nil {
		return nil, fmt.Errorf("save restore plan: %w", err)
	}
	if err := saveRecord(ctx, p.d, &rec); err != nil {
		return nil, err
	}
	return runFetch(ctx, p.d, rec, p.entries, p.target, p.opt.Overwrite)
}

// saveRecord writes a restore's record.
func saveRecord(ctx context.Context, d *dest, rec *RestoreRecord) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	if err := d.putBytes(ctx, restoreKey(rec.ID, ".json"), data, d.metaTier); err != nil {
		return fmt.Errorf("save restore record: %w", err)
	}
	return nil
}

// StartRestore plans a restore and starts it; see PlanRestore and
// RestorePlan.Start.
func StartRestore(ctx context.Context, dst fs.Fs, target string, opt RestoreOptions) (*RestoreStatus, error) {
	p, err := PlanRestore(ctx, dst, target, opt)
	if err != nil {
		return nil, err
	}
	return p.Start(ctx)
}

// ResumeRestore fetches whatever is readable of the restore with the
// given ID into target and reports its progress. overwrite says whether
// local files which differ may be replaced.
func ResumeRestore(ctx context.Context, dst fs.Fs, id, target string, overwrite bool) (*RestoreStatus, error) {
	if target == "" {
		return nil, errors.New("resuming a restore needs the target directory")
	}
	d := &dest{f: dst, metaTier: "STANDARD", retries: 3, dryRun: fs.GetConfig(ctx).DryRun}
	data, err := d.get(ctx, restoreKey(id, ".json"))
	if err != nil {
		return nil, fmt.Errorf("read restore %q: %w", id, err)
	}
	var rec RestoreRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("parse restore %q: %w", id, err)
	}
	if rec.ID != id {
		return nil, fmt.Errorf("restore record %q has ID %q", id, rec.ID)
	}
	if _, ok := tierHours[rec.Tier]; !ok {
		return nil, fmt.Errorf("restore %q: unknown tier %q", id, rec.Tier)
	}
	data, err = d.get(ctx, restoreKey(id, ".csv"))
	if err != nil {
		return nil, fmt.Errorf("read restore plan %q: %w", id, err)
	}
	plan, err := ReadEntries(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parse restore plan %q: %w", id, err)
	}
	// The plan comes from the destination, so check it can't write
	// outside the target directory.
	for i := range plan {
		if err := validTarget(plan[i].Target); err != nil {
			return nil, fmt.Errorf("restore plan %q: %w", id, err)
		}
	}
	st, err := runFetch(ctx, d, rec, plan, target, overwrite)
	if err != nil || d.dryRun || len(st.Pending) == 0 {
		return st, err
	}
	// Ask for objects which were never requested, such as those of files
	// which were in place when the restore started but have gone since,
	// and, once the restore is overdue, for everything still unreadable:
	// restored copies expire, and a request can be lost. Objects still
	// being restored just report that.
	now := timeNow().UTC()
	requested := make(map[string]bool, len(rec.RequestedKeys))
	for _, k := range rec.RequestedKeys {
		requested[k] = true
	}
	var again []string
	for _, k := range st.Pending {
		if !requested[k] || now.After(rec.ReadyBy) {
			again = append(again, k)
		}
	}
	if len(again) == 0 {
		return st, nil
	}
	fs.Logf(nil, "gda: restore %s: requesting %d objects which aren't readable", rec.ID, len(again))
	if err := requestRestore(ctx, dst, again, rec.Tier, rec.Lifetime); err != nil {
		st.Errors = append(st.Errors, err.Error())
		return st, nil
	}
	for _, k := range again {
		if !requested[k] {
			rec.RequestedKeys = append(rec.RequestedKeys, k)
		}
	}
	rec.ReadyBy = readyBy(now, rec.Tier)
	st.ReadyBy, st.Record = rec.ReadyBy, rec
	if err := saveRecord(ctx, d, &rec); err != nil {
		st.Errors = append(st.Errors, err.Error())
	}
	return st, nil
}

// runFetch fetches a restore's plan into target while holding a lock in
// target, so that overlapping runs of the same restore don't fetch the
// same data twice. If another run holds the lock, it reports that
// instead.
func runFetch(ctx context.Context, d *dest, rec RestoreRecord, plan []Entry, target string, overwrite bool) (*RestoreStatus, error) {
	if d.dryRun {
		return &RestoreStatus{RestoreID: rec.ID, Tier: rec.Tier, State: StateRestoring, ReadyBy: rec.ReadyBy, Errors: []string{}, Record: rec}, nil
	}
	if err := os.MkdirAll(target, 0o777); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(target, ".gda-restore-"+rec.ID+".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.Close() }()
	if err := tryLock(lock.Fd()); err != nil {
		if errors.Is(err, errLocked) {
			return &RestoreStatus{
				RestoreID: rec.ID, Tier: rec.Tier, State: StateRestoring, ReadyBy: rec.ReadyBy, Record: rec,
				Errors: []string{"another run of this restore is fetching; try again later"},
			}, nil
		}
		return nil, fmt.Errorf("lock %q: %w", lockPath, err)
	}
	st := fetchPlan(ctx, d, rec, plan, target, overwrite)
	if st.State == StateDone {
		_ = os.Remove(lockPath)
	}
	return st, nil
}

// validTarget returns an error if rel, a path below the restore target,
// isn't a clean relative path which stays below it.
func validTarget(rel string) error {
	if rel == "" || strings.HasPrefix(rel, "/") {
		return fmt.Errorf("invalid restore path %q", rel)
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid restore path %q", rel)
		}
	}
	return nil
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
			if err := validTarget(name); err != nil {
				return err
			}
			e := l.Entry
			e.Target = name
			if l.outsideRoot() {
				return l.errOutsideRoot()
			}
			if e.Location != "" || e.DedupOf != "" {
				e.Location = l.objectRef()
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

// hasData returns true if e's content is stored in an object. Empty
// files have none, so they never wait for a restore.
func hasData(e *Entry) bool {
	return e.Type == TypeFile && e.Size > 0
}

// needsData returns true if restoring e needs data from an object when
// the restore starts: it has data and isn't in place already.
func needsData(e *Entry) bool {
	return hasData(e) && e.Action != actionSkip
}

// planObjects returns the keys of the objects holding data of plan
// entries which aren't in place yet.
func planObjects(plan []Entry) []string {
	keys := map[string]bool{}
	for i := range plan {
		if needsData(&plan[i]) {
			keys[plan[i].Location] = true
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
// actionSkip and returns the names of those which exist but differ,
// including anything other than a real directory where a directory goes.
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
		case exists:
			conflicts = append(conflicts, e.Target)
		}
	}
	return conflicts, nil
}

// inPlace returns whether the local path p exists, and whether it
// already holds entry e. A directory is only in place if it is a real
// directory, not a symlink to one.
func inPlace(e *Entry, p string) (same, exists bool, err error) {
	info, err := os.Lstat(p)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
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
// are always readable. It returns an error if any object is missing or
// its restore couldn't be requested.
func requestRestore(ctx context.Context, f fs.Fs, keys []string, tier string, lifetime int) error {
	var plain []string
	var problems []string
	for _, k := range keys {
		if _, version := splitVersionKey(k); version == "" {
			plain = append(plain, k)
			continue
		}
		// The restore command acts on current versions only.
		if err := requestVersionRestore(ctx, f, k, tier, lifetime); err != nil {
			problems = append(problems, err.Error())
		}
	}
	err := requestPlainRestore(ctx, f, plain, tier, lifetime)
	if len(problems) > 0 {
		err = errors.Join(err, fmt.Errorf("request restore of %d object versions failed:\n  %s", len(problems), strings.Join(problems, "\n  ")))
	}
	return err
}

// requestVersionRestore requests the restore of one version of an object.
func requestVersionRestore(ctx context.Context, f fs.Fs, ref, tier string, lifetime int) error {
	o, err := newDataObject(ctx, f, ref)
	if err != nil {
		return err
	}
	r, ok := o.(restorableObject)
	if !ok {
		return fmt.Errorf("%s: can't request the restore of a version", o.Remote())
	}
	err = r.RequestRestore(ctx, tier, int32(lifetime))
	if err != nil && strings.Contains(err.Error(), "RestoreAlreadyInProgress") {
		return nil
	}
	return err
}

// requestPlainRestore requests the restore of the current versions of
// the objects at keys.
func requestPlainRestore(ctx context.Context, f fs.Fs, keys []string, tier string, lifetime int) error {
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
		return fmt.Errorf("request restore: unexpected result: %w", err)
	}
	status := make(map[string]string, len(results))
	for _, r := range results {
		status[r.Remote] = r.Status
	}
	var problems []string
	for _, k := range keys {
		s, ok := status[k]
		switch {
		case !ok:
			// Objects which don't exist are left out of the results.
			problems = append(problems, fmt.Sprintf("%s: not found", k))
		case s == "OK", strings.HasPrefix(s, "Not "), strings.Contains(s, "RestoreAlreadyInProgress"):
			// "Not ..." means the object isn't archived, so is readable.
		default:
			problems = append(problems, fmt.Sprintf("%s: %s", k, s))
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		if len(problems) > 5 {
			problems = append(problems[:5], fmt.Sprintf("and %d more", len(problems)-5))
		}
		return fmt.Errorf("request restore of %d objects failed:\n  %s", len(problems), strings.Join(problems, "\n  "))
	}
	return nil
}

// isRestorePending returns true if err means the object must be
// restored before it can be read. The s3 backend's Open reports this as
// "Object in <class>, restore first".
func isRestorePending(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "restore first") || strings.Contains(err.Error(), "InvalidObjectState"))
}

// errUnsupported is returned for entries which can't be recreated here.
var errUnsupported = errors.New("can't be recreated here")

// makeDirs makes sure that every element of rel below target is a real
// directory, creating the missing ones. Symlinks and other files in the
// way are an error, or replaced if overwrite is set, so nothing is ever
// written outside target by following a symlink.
func makeDirs(target, rel string, overwrite bool) error {
	if err := os.MkdirAll(target, 0o777); err != nil {
		return err
	}
	p := target
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." {
			continue
		}
		p = filepath.Join(p, part)
		info, err := os.Lstat(p)
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			return err
		case info.IsDir():
			continue
		case !overwrite:
			return fmt.Errorf("%q is in the way of a directory; use --overwrite to replace it", p)
		default:
			if err := os.Remove(p); err != nil {
				return err
			}
		}
		// The umask applies, as with mkdir; the recorded mode is set
		// once the directory's contents are in place.
		if err := os.Mkdir(p, 0o777); err != nil {
			// Something appeared since the Lstat: accept only a directory.
			info, lerr := os.Lstat(p)
			if !errors.Is(err, os.ErrExist) || lerr != nil || !info.IsDir() {
				return err
			}
		}
	}
	return nil
}

// fetchPlan fetches the data of every plan entry that isn't in place and
// whose object is readable into target, then creates the other entries
// and applies metadata.
//
// Files which are already identical aren't fetched again, but their
// permissions and times are set from the backup like everything else.
func fetchPlan(ctx context.Context, d *dest, rec RestoreRecord, plan []Entry, target string, overwrite bool) *RestoreStatus {
	f := &fetcher{
		st:          &RestoreStatus{RestoreID: rec.ID, Tier: rec.Tier, ReadyBy: rec.ReadyBy, Record: rec, Errors: []string{}},
		plan:        plan,
		target:      target,
		overwrite:   overwrite,
		pattern:     tempPattern(rec.ID),
		done:        make([]bool, len(plan)),
		failed:      make([]bool, len(plan)),
		unsupported: make([]bool, len(plan)),
	}
	f.st.Files.Total = len(plan)
	for i := range plan {
		if plan[i].Action == actionSkip {
			f.st.Files.SkippedIdentical++
		}
	}
	f.makeDirs()
	f.fetchFiles(ctx, d)
	linkFiles(plan, f.done, f.failed, overwrite, f.localPath, f.errorf, f.pattern)
	f.makeSpecials()
	f.applyMeta()
	f.count()
	return f.st
}

// fetcher is the state of one pass of fetchPlan.
type fetcher struct {
	st          *RestoreStatus
	plan        []Entry
	target      string
	overwrite   bool
	pattern     string // of temporary file names
	done        []bool // by plan index: in place
	failed      []bool // by plan index: couldn't be restored
	unsupported []bool // by plan index: a kind of file this system can't create
}

// errorf records a problem with the restore.
func (f *fetcher) errorf(format string, args ...any) {
	err := fmt.Sprintf(format, args...)
	fs.Errorf(nil, "gda: %s", err)
	if len(f.st.Errors) < maxLedgerErrors {
		f.st.Errors = append(f.st.Errors, err)
	}
}

// localPath returns where e is restored to.
func (f *fetcher) localPath(e *Entry) string {
	return filepath.Join(f.target, filepath.FromSlash(e.Target))
}

// makeDirs creates the directories first, so everything else has
// somewhere to go.
func (f *fetcher) makeDirs() {
	for i := range f.plan {
		e := &f.plan[i]
		if e.IsDir() {
			if err := makeDirs(f.target, e.Target, f.overwrite); err != nil {
				f.errorf("create directory %q: %v", e.Target, err)
				f.failed[i] = true
			}
		}
	}
}

// fetchFiles writes the files, grouped by the object holding their data.
// Every object the plan needs data from counts as requested, and as
// fetched once all its files are in place, so the counts only grow from
// run to run.
func (f *fetcher) fetchFiles(ctx context.Context, d *dest) {
	st, plan := f.st, f.plan
	byObject := map[string][]int{}
	objectFiles := map[string]int{}
	objectDone := map[string]int{}
	swept := map[string]bool{}
	for i := range plan {
		e := &plan[i]
		if e.Type != TypeFile {
			continue
		}
		counted := e.Size > 0 && e.Action != actionSkip
		if counted {
			objectFiles[e.Location]++
		}
		p := f.localPath(e)
		same, exists, err := inPlace(e, p)
		switch {
		case err != nil:
			f.errorf("check %q: %v", e.Target, err)
			f.failed[i] = true
			continue
		case same:
			f.done[i] = true
			if counted {
				objectDone[e.Location]++
			}
			continue
		case exists && !f.overwrite:
			// Changed locally since the restore started.
			f.errorf("%q exists and differs from the backup; use --overwrite to replace it", e.Target)
			f.failed[i] = true
			continue
		}
		parent := path.Dir(e.Target)
		if err := makeDirs(f.target, parent, f.overwrite); err != nil {
			f.errorf("create directory for %q: %v", e.Target, err)
			f.failed[i] = true
			continue
		}
		if err := clearDir(p, f.overwrite); err != nil {
			f.errorf("%q: %v", e.Target, err)
			f.failed[i] = true
			continue
		}
		if !swept[parent] {
			swept[parent] = true
			sweepTemp(filepath.Dir(p), f.pattern)
		}
		if !hasData(e) {
			if err := writeVerified(io.NopCloser(strings.NewReader("")), e, p, f.pattern); err != nil {
				f.errorf("create %q: %v", e.Target, err)
				f.failed[i] = true
				continue
			}
			f.done[i] = true
			continue
		}
		byObject[e.Location] = append(byObject[e.Location], i)
	}
	st.Objects.Requested = len(objectFiles)
	for key, n := range objectFiles {
		if objectDone[key] == n {
			st.Objects.Fetched++
		}
	}
	keys := make([]string, 0, len(byObject))
	for k := range byObject {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		todo := byObject[key]
		fetched, err := fetchObject(ctx, d, key, plan, todo, f.localPath, f.errorf, f.pattern)
		switch {
		case isRestorePending(err):
			st.Objects.Restoring++
			st.Pending = append(st.Pending, key)
			continue
		case err != nil:
			f.errorf("fetch %q: %v", key, err)
		}
		for _, i := range todo {
			if fetched[i] {
				f.done[i] = true
			} else {
				f.failed[i] = true
			}
		}
		if err == nil && len(fetched) == len(todo) && objectDone[key]+len(todo) == objectFiles[key] {
			st.Objects.Fetched++
		}
	}
}

// makeSpecials creates symlinks and special files, which need no data.
func (f *fetcher) makeSpecials() {
	for i := range f.plan {
		e := &f.plan[i]
		if e.Type == TypeFile || e.IsDir() {
			continue
		}
		if err := makeDirs(f.target, path.Dir(e.Target), f.overwrite); err != nil {
			f.errorf("create directory for %q: %v", e.Target, err)
			f.failed[i] = true
			continue
		}
		err := createSpecial(e, f.localPath(e), f.overwrite)
		switch {
		case errors.Is(err, errUnsupported):
			f.unsupported[i] = true
		case err != nil:
			f.errorf("create %q: %v", e.Target, err)
			f.failed[i] = true
		default:
			f.done[i] = true
		}
	}
}

// applyMeta sets the metadata of everything in place. Directories come
// last and deepest first, as creating their contents changes their
// modification times and a read only directory couldn't be filled. They
// wait until nothing is left to write, so a later run can still write
// into them.
func (f *fetcher) applyMeta() {
	isRoot := os.Geteuid() == 0
	anyFailed := false
	for i := range f.plan {
		e := &f.plan[i]
		anyFailed = anyFailed || f.failed[i]
		if f.done[i] && e.Type != TypeDir {
			if err := applyMeta(e, f.localPath(e), isRoot); err != nil {
				f.errorf("set metadata of %q: %v", e.Target, err)
			}
		}
	}
	if f.st.Objects.Restoring > 0 || anyFailed {
		return
	}
	var dirs []int
	for i := range f.plan {
		if f.plan[i].IsDir() {
			dirs = append(dirs, i)
		}
	}
	sort.SliceStable(dirs, func(a, b int) bool {
		return strings.Count(f.plan[dirs[a]].Target, "/") > strings.Count(f.plan[dirs[b]].Target, "/")
	})
	for _, i := range dirs {
		e := &f.plan[i]
		if err := applyMeta(e, f.localPath(e), isRoot); err != nil {
			f.errorf("set metadata of %q: %v", e.Target, err)
		}
		f.done[i] = true
	}
}

// count fills in the file counts and the state of the restore.
func (f *fetcher) count() {
	st := f.st
	for i := range f.plan {
		switch {
		case f.failed[i]:
			st.Files.Failed++
		case f.unsupported[i]:
			st.Files.Unsupported++
		case f.done[i]:
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
}

// linkFiles makes the restored files which were links to the same file
// when backed up into links to the same file again. Without overwrite,
// files which were in place before the restore are left as they are.
func linkFiles(plan []Entry, done, failed []bool, overwrite bool, localPath func(*Entry) string, errorf func(string, ...any), pattern string) {
	groups := map[string][]int{}
	var order []string
	for i := range plan {
		e := &plan[i]
		if e.Type != TypeFile || e.HardLink == "" || !done[i] {
			continue
		}
		// Rows recorded at different times may share an inode number
		// reused by another file, so the data must match too.
		link := fmt.Sprintf("%s %d %s", e.HardLink, e.Size, e.MD5)
		if groups[link] == nil {
			order = append(order, link)
		}
		groups[link] = append(groups[link], i)
	}
	for _, link := range order {
		members := groups[link]
		first := localPath(&plan[members[0]])
		firstInfo, err := os.Lstat(first)
		if err != nil {
			errorf("link %q: %v", plan[members[0]].Target, err)
			continue
		}
		for _, i := range members[1:] {
			if !overwrite && plan[i].Action == actionSkip {
				continue
			}
			p := localPath(&plan[i])
			if info, err := os.Lstat(p); err == nil && os.SameFile(firstInfo, info) {
				continue
			}
			// Link under a temporary name and rename over the copy, so
			// a symlink at p is replaced rather than followed.
			tmp := filepath.Join(filepath.Dir(p), strings.Replace(pattern, "*", "link", 1))
			_ = os.Remove(tmp)
			err := os.Link(first, tmp)
			if err == nil {
				err = os.Rename(tmp, p)
			}
			switch {
			case errors.Is(err, syscall.EXDEV):
				// Across file systems the copy has to do.
				_ = os.Remove(tmp)
				fs.Logf(plan[i].Target, "gda: can't link to %q on another file system, leaving a copy", plan[members[0]].Target)
			case err != nil:
				_ = os.Remove(tmp)
				errorf("link %q to %q: %v", plan[i].Target, plan[members[0]].Target, err)
				failed[i], done[i] = true, false
			}
		}
	}
}

// tempPattern names the temporary files restore id writes into.
func tempPattern(id string) string {
	return ".gda-restore-" + id + "-*"
}

// sweepTemp removes temporary files matching pattern left in dir by an
// interrupted run of the same restore.
func sweepTemp(dir, pattern string) {
	matches, _ := filepath.Glob(filepath.Join(dir, pattern))
	for _, m := range matches {
		_ = os.Remove(m)
	}
}

// clearDir makes way for a file at p if a directory is there: an empty
// one is removed with overwrite, anything else is an error.
func clearDir(p string, overwrite bool) error {
	info, err := os.Lstat(p)
	if err != nil || !info.IsDir() {
		return nil
	}
	if !overwrite {
		return errors.New("a directory is in the way; use --overwrite to replace it if it is empty")
	}
	if err := os.Remove(p); err != nil {
		return fmt.Errorf("a directory which isn't empty is in the way: %w", err)
	}
	return nil
}

// wholeObjectMembers is the number of members above which a pack is
// downloaded whole rather than with a ranged read per member.
const wholeObjectMembers = 16

// fetchObject fetches the plan entries at todo from the object at key.
// It returns which of them were written; the error is the first problem
// with the object as a whole.
func fetchObject(ctx context.Context, d *dest, key string, plan []Entry, todo []int, localPath func(*Entry) string, errorf func(string, ...any), pattern string) (map[int]bool, error) {
	fetched := map[int]bool{}
	o, err := newDataObject(ctx, d.f, key)
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
		if first.Codec == CodecZstd {
			if in, err = decompressRange(in, 0, -1); err != nil {
				return fetched, err
			}
		}
		if err := writeVerified(in, first, localPath(first), pattern); err != nil {
			return fetched, err
		}
		fetched[todo[0]] = true
		// Duplicates of the same file refer to the same object.
		copyDuplicates(plan, todo, localPath, fetched, errorf, pattern)
		return fetched, nil
	}
	spans := packSpans(plan, todo)
	if useSpans(spans, o.Size()) {
		for _, s := range spans {
			if err := fetchSpan(ctx, o, plan, s, localPath, fetched, errorf, pattern); err != nil {
				return fetched, err
			}
		}
		return fetched, nil
	}
	return fetched, extractPack(ctx, o, plan, todo, localPath, fetched, errorf, pattern)
}

// extractPack reads the whole pack o and writes the members at todo.
// Members are found by the offset of their data, which the index
// records, so tar member names are never used as paths, and a duplicate
// recorded under another name finds the copy it refers to. Several plan
// entries can share one member.
func extractPack(ctx context.Context, o fs.Object, plan []Entry, todo []int, localPath func(*Entry) string, fetched map[int]bool, errorf func(string, ...any), pattern string) (err error) {
	wanted := make(map[int64][]int, len(todo))
	for _, i := range todo {
		wanted[plan[i].Offset] = append(wanted[plan[i].Offset], i)
	}
	in, err := o.Open(ctx)
	if err != nil {
		return err
	}
	if plan[todo[0]].Codec == CodecZstd {
		if in, err = decompressRange(in, 0, -1); err != nil {
			return err
		}
	}
	defer fs.CheckClose(in, &err)
	// archive/tar reads headers a block at a time, so once Next returns,
	// the bytes read so far end where the member's data starts.
	counter := &readCounter{r: in}
	tr := tar.NewReader(counter)
	for len(wanted) > 0 {
		_, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read pack: %w", err)
		}
		indexes, ok := wanted[counter.n]
		if !ok {
			continue
		}
		delete(wanted, counter.n)
		first := &plan[indexes[0]]
		if err := writeVerified(io.NopCloser(tr), first, localPath(first), pattern); err != nil {
			errorf("%v", err)
			continue
		}
		fetched[indexes[0]] = true
		copyDuplicates(plan, indexes, localPath, fetched, errorf, pattern)
	}
	for _, indexes := range wanted {
		for _, i := range indexes {
			errorf("%q not found in pack %q", plan[i].Target, o.Remote())
		}
	}
	return nil
}

// copyDuplicates writes the plan entries at indexes[1:], which share the
// data of the entry at indexes[0], from that entry's restored file.
func copyDuplicates(plan []Entry, indexes []int, localPath func(*Entry) string, fetched map[int]bool, errorf func(string, ...any), pattern string) {
	first := &plan[indexes[0]]
	for _, i := range indexes[1:] {
		e := &plan[i]
		copyIn, err := os.Open(localPath(first))
		if err == nil {
			err = writeVerified(copyIn, e, localPath(e), pattern)
		}
		if err != nil {
			errorf("%v", err)
			continue
		}
		fetched[i] = true
	}
}

// span is a range of a pack's stored bytes read with one request.
type span struct {
	start, end int64   // stored bytes, end exclusive
	uStart     int64   // offset in the uncompressed pack where a compressed span starts
	members    [][]int // plan indexes of the entries in the span, grouped by the data they share, in data order
}

// packSpans groups the pack members at todo into spans, so that frames
// holding several wanted members, and members wanted under several
// names, are read once.
func packSpans(plan []Entry, todo []int) []span {
	sorted := append([]int(nil), todo...)
	sort.SliceStable(sorted, func(a, b int) bool { return plan[sorted[a]].Offset < plan[sorted[b]].Offset })
	var spans []span
	for _, i := range sorted {
		e := &plan[i]
		start, end := e.StoredOffset, e.StoredOffset+e.StoredLength
		if n := len(spans); n > 0 && start <= spans[n-1].end {
			s := &spans[n-1]
			s.end = max(s.end, end)
			if last := len(s.members) - 1; plan[s.members[last][0]].Offset == e.Offset {
				s.members[last] = append(s.members[last], i)
			} else {
				s.members = append(s.members, []int{i})
			}
			continue
		}
		spans = append(spans, span{start: start, end: end, uStart: e.StoredStart, members: [][]int{{i}}})
	}
	return spans
}

// useSpans returns true if reading spans is better than downloading the
// whole object of size bytes.
func useSpans(spans []span, size int64) bool {
	var want int64
	for _, s := range spans {
		want += s.end - s.start
	}
	return len(spans) <= wholeObjectMembers && want*2 < size
}

// fetchSpan reads the span s of the pack o and writes its members. The
// error is a problem reading the object at all.
func fetchSpan(ctx context.Context, o fs.Object, plan []Entry, s span, localPath func(*Entry) string, fetched map[int]bool, errorf func(string, ...any), pattern string) (err error) {
	in, err := o.Open(ctx, &fs.RangeOption{Start: s.start, End: s.end - 1})
	if err != nil {
		return err
	}
	pos := s.start
	if plan[s.members[0][0]].Codec == CodecZstd {
		// A compressed span starts at a frame, which may hold the end of
		// an earlier member before the first wanted one.
		if in, err = decompressRange(in, 0, -1); err != nil {
			for _, indexes := range s.members {
				errorf("%q: %v", plan[indexes[0]].Target, err)
			}
			return nil
		}
		pos = s.uStart
	}
	defer fs.CheckClose(in, &err)
	counter := &readCounter{r: in}
	for n, indexes := range s.members {
		first := &plan[indexes[0]]
		skip := first.Offset - pos - counter.n
		if skip >= 0 {
			_, err = io.CopyN(io.Discard, counter, skip)
		} else {
			err = errors.New("data overlaps the previous file")
		}
		if err != nil {
			for _, rest := range s.members[n:] {
				errorf("%q: %v", plan[rest[0]].Target, err)
			}
			return nil
		}
		if err := writeVerified(io.NopCloser(io.LimitReader(counter, first.Size)), first, localPath(first), pattern); err != nil {
			errorf("%v", err)
			continue
		}
		fetched[indexes[0]] = true
		copyDuplicates(plan, indexes, localPath, fetched, errorf, pattern)
	}
	return nil
}

// readCounter counts the bytes read through it.
type readCounter struct {
	r io.Reader
	n int64
}

func (c *readCounter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// writeVerified writes in to p through a temporary file named by pattern
// in the same directory, checking the size and MD5 against e before
// renaming it into place. The rename replaces a symlink at p rather than
// following it.
func writeVerified(in io.ReadCloser, e *Entry, p, pattern string) (err error) {
	defer fs.CheckClose(in, &err)
	tmp, err := os.CreateTemp(filepath.Dir(p), pattern)
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

// createSpecial creates a symlink or special file. It returns
// errUnsupported for entries which can't be recreated here.
func createSpecial(e *Entry, p string, overwrite bool) error {
	switch e.Type {
	case TypeSymlink, TypeFifo:
	case TypeCharDev, TypeBlockDev:
		if os.Geteuid() != 0 || e.DevMajor < 0 || e.DevMinor < 0 {
			return errUnsupported
		}
	default:
		return errUnsupported
	}
	same, exists, err := inPlace(e, p)
	if err != nil || same {
		return err
	}
	if exists {
		if !overwrite {
			return errors.New("exists and differs; use --overwrite to replace it")
		}
		if err := clearDir(p, overwrite); err != nil {
			return err
		}
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	switch e.Type {
	case TypeSymlink:
		return os.Symlink(e.LinkTarget, p)
	case TypeFifo:
		return unix.Mkfifo(p, e.Mode&0o777)
	default:
		kind := uint32(unix.S_IFCHR)
		if e.Type == TypeBlockDev {
			kind = unix.S_IFBLK
		}
		return mknod(p, kind|e.Mode&0o777, e.DevMajor, e.DevMinor)
	}
}

// applyMeta sets the permissions, modification time and extended
// attributes of p from e, and its owner and group when running as root.
func applyMeta(e *Entry, p string, isRoot bool) error {
	// Before the mode, as setting a user attribute needs write
	// permission, which a read only file no longer has after it.
	var err error
	if e.Xattrs != "" {
		err = writeXattrs(p, e.Xattrs, isRoot)
	}
	return errors.Join(err, applyMode(e, p, isRoot))
}

// applyMode sets the owner, permissions and modification time of p from e.
func applyMode(e *Entry, p string, isRoot bool) error {
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
