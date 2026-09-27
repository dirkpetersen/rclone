//go:build unix

package gda

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// treeState records what a restore of a tree must give back.
func treeState(t *testing.T, root string) map[string]string {
	t.Helper()
	state := map[string]string{}
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		require.NoError(t, err)
		rel, _ := filepath.Rel(root, p)
		if rel == "." {
			return nil
		}
		desc := fmt.Sprintf("%v %d", info.Mode(), info.ModTime().UnixNano())
		switch {
		case info.Mode().IsRegular():
			data, err := os.ReadFile(p)
			require.NoError(t, err)
			sum := md5.Sum(data)
			desc += " " + hex.EncodeToString(sum[:])
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(p)
			require.NoError(t, err)
			desc = "symlink " + target
		}
		state[filepath.ToSlash(rel)] = desc
		return nil
	})
	require.NoError(t, err)
	return state
}

// mutator makes random changes to a source tree, remembering the paths
// it changed for change runs.
type mutator struct {
	t       *testing.T
	r       *rand.Rand
	root    string
	changed []string
	clock   time.Time
	noSwap  bool // don't replace files by directories of the same name or the other way round
}

func (m *mutator) path(rel string) string { return filepath.Join(m.root, filepath.FromSlash(rel)) }

// entries returns the files and directories in the tree, sorted.
func (m *mutator) entries() (files, dirs []string) {
	_ = filepath.Walk(m.root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(m.root, p)
		rel = filepath.ToSlash(rel)
		switch {
		case rel == ".":
		case info.IsDir():
			dirs = append(dirs, rel)
		case info.Mode().IsRegular():
			files = append(files, rel)
		}
		return nil
	})
	sort.Strings(files)
	sort.Strings(dirs)
	return files, dirs
}

func (m *mutator) tick() time.Time {
	m.clock = m.clock.Add(time.Duration(1+m.r.Intn(1000)) * time.Second)
	return m.clock
}

// linked returns true if the file at rel has other links, whose changes
// a change list naming only rel wouldn't show.
func (m *mutator) linked(rel string) bool {
	info, err := os.Lstat(m.path(rel))
	if err != nil {
		return false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && st.Nlink > 1
}

func (m *mutator) write(rel string) {
	if m.linked(rel) {
		return
	}
	if os.MkdirAll(filepath.Dir(m.path(rel)), 0o755) != nil {
		// A file is in the way.
		return
	}
	sizes := []int{0, 1, 100, 700, 3000, 5000, 9000, 20000}
	size := sizes[m.r.Intn(len(sizes))]
	data := make([]byte, size)
	if m.r.Intn(2) == 0 {
		m.r.Read(data)
	} else {
		copy(data, strings.Repeat(fmt.Sprintf("%s %d ", rel, m.r.Intn(3)), size/4+1))
	}
	require.NoError(m.t, os.WriteFile(m.path(rel), data, 0o644))
	now := m.tick()
	require.NoError(m.t, os.Chtimes(m.path(rel), now, now))
	m.changed = append(m.changed, rel)
}

// dirTimes returns the modification times of the directories.
func (m *mutator) dirTimes() map[string]time.Time {
	times := map[string]time.Time{}
	_, dirs := m.entries()
	for _, d := range dirs {
		if info, err := os.Lstat(m.path(d)); err == nil {
			times[d] = info.ModTime()
		}
	}
	return times
}

// mutate makes n random changes.
func (m *mutator) mutate(n int) {
	before := m.dirTimes()
	for range n {
		files, dirs := m.entries()
		pick := func(list []string) string { return list[m.r.Intn(len(list))] }
		dir := ""
		if len(dirs) > 0 && m.r.Intn(4) != 0 {
			dir = pick(dirs)
		}
		switch op := m.r.Intn(14); {
		case op < 3 || len(files) == 0:
			m.write(joinRemote(dir, fmt.Sprintf("f%03d.dat", m.r.Intn(1000))))
		case op == 3:
			m.write(joinRemote(dir, fmt.Sprintf("d%02d/f%03d.dat", m.r.Intn(20), m.r.Intn(1000))))
		case op == 4:
			m.write(pick(files))
		case op == 5 && !m.linked(files[0]):
			f := files[0]
			now := m.tick()
			require.NoError(m.t, os.Chtimes(m.path(f), now, now))
			m.changed = append(m.changed, f)
		case op == 6 && !m.linked(files[len(files)-1]):
			f := files[len(files)-1]
			require.NoError(m.t, os.Chmod(m.path(f), []os.FileMode{0o600, 0o644, 0o755}[m.r.Intn(3)]))
			m.changed = append(m.changed, f)
		case op == 7:
			f := pick(files)
			require.NoError(m.t, os.Remove(m.path(f)))
			m.changed = append(m.changed, f)
		case op == 8 && len(dirs) > 0:
			d := pick(dirs)
			to := joinRemote(parentRel(d), fmt.Sprintf("r%03d", m.r.Intn(1000)))
			if _, err := os.Lstat(m.path(to)); err == nil {
				continue
			}
			require.NoError(m.t, os.Rename(m.path(d), m.path(to)))
			m.changed = append(m.changed, d, to)
		case op == 9:
			link := joinRemote(dir, fmt.Sprintf("l%03d", m.r.Intn(1000)))
			if _, err := os.Lstat(m.path(link)); err == nil {
				continue
			}
			require.NoError(m.t, os.MkdirAll(filepath.Dir(m.path(link)), 0o755))
			if m.r.Intn(2) == 0 {
				require.NoError(m.t, os.Symlink(fmt.Sprintf("target%d", m.r.Intn(10)), m.path(link)))
			} else if err := os.Link(m.path(pick(files)), m.path(link)); err != nil {
				continue
			}
			m.changed = append(m.changed, link)
		case op == 10 && len(dirs) > 0:
			d := pick(dirs)
			require.NoError(m.t, os.RemoveAll(m.path(d)))
			m.changed = append(m.changed, d)
		case op == 11 && !m.noSwap:
			// A file replaced by a directory, or the other way round.
			if len(dirs) > 0 && m.r.Intn(2) == 0 {
				d := pick(dirs)
				require.NoError(m.t, os.RemoveAll(m.path(d)))
				m.write(d)
				m.changed = append(m.changed, d)
			} else if f := pick(files); !m.linked(f) {
				require.NoError(m.t, os.Remove(m.path(f)))
				m.write(f + "/inner.dat")
				m.changed = append(m.changed, f)
			}
		case op == 12:
			// Names which need encoding.
			names := []string{"bad\xffname", "per%cent", "sp ace,comma\"quote", "new\nline"}
			m.write(joinRemote(dir, names[m.r.Intn(len(names))]))
		case op == 13:
			fifo := joinRemote(dir, fmt.Sprintf("p%03d", m.r.Intn(1000)))
			if _, err := os.Lstat(m.path(fifo)); err == nil {
				continue
			}
			require.NoError(m.t, os.MkdirAll(filepath.Dir(m.path(fifo)), 0o755))
			require.NoError(m.t, unix.Mkfifo(m.path(fifo), 0o640))
			m.changed = append(m.changed, fifo)
		}
	}
	// Directory times change as their contents do; give the changed
	// ones distinct times, which a change feed reports, and leave the
	// rest alone, so change runs read only some directories.
	after := m.dirTimes()
	_, dirs := m.entries()
	for i := len(dirs) - 1; i >= 0; i-- {
		old, ok := before[dirs[i]]
		if ok && old.Equal(after[dirs[i]]) {
			continue
		}
		now := m.tick()
		require.NoError(m.t, os.Chtimes(m.path(dirs[i]), now, now))
		m.changed = append(m.changed, dirs[i])
	}
}

// browseTree lists the files below dir as the Browser shows them, by
// local path.
func browseTree(t *testing.T, b *Browser, dir, local string, out map[string]int64) {
	t.Helper()
	entries, ok, err := b.List(context.Background(), dir)
	require.NoError(t, err, dir)
	require.True(t, ok, dir)
	for _, e := range entries {
		switch {
		case e.IsDir():
			browseTree(t, b, joinRemote(dir, e.Name[strings.LastIndex(e.Name, "/")+1:]), joinRemote(local, e.LocalPath), out)
		case e.Type == TypeFile:
			out[joinRemote(local, e.LocalPath)] = e.Size
		}
	}
}

func TestRandomHistory(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			fakeClock(t)
			oldRebase := rebaseMaxPacks
			rebaseMaxPacks = 2
			t.Cleanup(func() { rebaseMaxPacks = oldRebase })
			r := rand.New(rand.NewSource(seed))
			// Odd seeds use an object store, where an object and a
			// directory can have the same name, as they can after a file
			// is replaced by a directory; even seeds a file system,
			// where they can't.
			src := t.TempDir()
			dst := fmt.Sprintf(":memory:history%d/lab", seed)
			if seed%2 == 0 {
				dst = filepath.Join(t.TempDir(), "lab")
			}
			m := &mutator{t: t, r: r, root: src, clock: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), noSwap: seed%2 == 0}
			m.mutate(40)
			f := newDst(t, dst)
			var runs []string
			var states []map[string]string
			cache := t.TempDir()
			for run := range 8 {
				opt := testOptions()
				opt.RollupMax = []int64{0, 64, 4096}[r.Intn(3)]
				opt.Workers = 1 + r.Intn(4)
				opt.DedupMin = []int64{-1, 1000}[r.Intn(2)]
				if r.Intn(2) == 0 {
					opt.Compression = CodecZstd
				}
				if run > 0 && r.Intn(3) == 0 {
					opt.Changes = append([]string(nil), m.changed...)
				}
				if r.Intn(2) == 0 {
					opt.IndexCache = cache
				}
				if r.Intn(4) == 0 {
					// A dry run changes nothing.
					ctx, ci := fs.AddConfig(context.Background())
					ci.DryRun = true
					_, err := Backup(ctx, src, f, opt)
					require.NoError(t, err, "dry run %d", run)
				}
				m.changed = nil
				states = append(states, treeState(t, src))
				if len(opt.Changes) == 0 && r.Intn(4) == 0 {
					// Split over two workers.
					runID, err := Plan(context.Background(), src, f, opt, 2)
					require.NoError(t, err, "plan run %d", run)
					for i := range 2 {
						wopt := opt
						wopt.Worker = fmt.Sprintf("p%d", i)
						_, err := BackupPartition(context.Background(), src, f, wopt, runID, i)
						require.NoError(t, err, "run %d partition %d", run, i)
					}
					_, err = FinishRun(context.Background(), f, runID, opt)
					require.NoError(t, err, "finish run %d", run)
					runs = append(runs, runID)
				} else {
					l, err := Backup(context.Background(), src, f, opt)
					require.NoError(t, err, "run %d", run)
					runs = append(runs, l.RunID)
				}
				m.mutate(15)
			}
			for i, runID := range runs {
				ropt := DefaultRestoreOptions()
				ropt.At = runID
				target := t.TempDir()
				st, err := StartRestore(context.Background(), f, target, ropt)
				if len(states[i]) == 0 {
					assert.ErrorContains(t, err, "nothing to restore")
					continue
				}
				require.NoError(t, err, "restore at run %d", i)
				assert.Equal(t, StateDone, st.State, "restore at run %d", i)
				assert.Equal(t, states[i], treeState(t, target), "restore at run %d", i)

				// Browsing shows the same files.
				b, err := NewBrowser(f, runID)
				require.NoError(t, err)
				browsed := map[string]int64{}
				browseTree(t, b, "", "", browsed)
				want := map[string]int64{}
				for p, desc := range states[i] {
					if strings.HasPrefix(desc, "-") {
						info, err := os.Lstat(filepath.Join(target, filepath.FromSlash(p)))
						require.NoError(t, err)
						want[p] = info.Size()
					}
				}
				assert.Equal(t, want, browsed, "browse at run %d", i)
			}
			// The indexes list the files the source held at the last run.
			var walked, want []string
			require.NoError(t, Walk(context.Background(), f, "", runs[len(runs)-1], func(l *Located) error {
				if l.Type == TypeFile {
					walked = append(walked, l.LocalPath)
				}
				return nil
			}))
			for p, desc := range states[len(states)-1] {
				if strings.HasPrefix(desc, "-") {
					want = append(want, p)
				}
			}
			assert.ElementsMatch(t, want, walked)
			report, err := Check(context.Background(), f, "", "", CheckOptions{Download: true})
			require.NoError(t, err)
			assert.False(t, report.Failed(), "%+v", report)
			gc, err := GC(context.Background(), f, GCOptions{})
			require.NoError(t, err)
			assert.Empty(t, gc.Orphans)
		})
	}
}

// flakyFs fails a share of uploads.
type flakyFs struct {
	fs.Fs
	mu   sync.Mutex
	r    *rand.Rand
	rate float64
}

func (f *flakyFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	f.mu.Lock()
	fail := f.r.Float64() < f.rate
	f.mu.Unlock()
	if fail {
		return nil, errors.New("injected upload failure")
	}
	return f.Fs.Put(ctx, in, src, options...)
}

func TestRandomFailures(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			fakeClock(t)
			r := rand.New(rand.NewSource(seed))
			src := t.TempDir()
			f := newDst(t, fmt.Sprintf(":memory:failures%d/lab", seed))
			m := &mutator{t: t, r: r, root: src, clock: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}
			m.mutate(40)
			var runs []string
			var states []map[string]string
			for run := range 6 {
				opt := testOptions()
				opt.RollupMax = []int64{0, 64, 4096}[r.Intn(3)]
				opt.Workers = 1 + r.Intn(4)
				opt.DedupMin = 1000
				opt.Retries = 1
				// A run which fails part way, then one which completes.
				flaky := &flakyFs{Fs: f, r: rand.New(rand.NewSource(seed*100 + int64(run))), rate: 0.2}
				_, _ = Backup(context.Background(), src, flaky, opt)
				states = append(states, treeState(t, src))
				l, err := Backup(context.Background(), src, f, opt)
				require.NoError(t, err, "run %d", run)
				runs = append(runs, l.RunID)
				m.mutate(15)
			}
			gc, err := GC(context.Background(), f, GCOptions{DeleteOrphans: true, LockTimeout: time.Hour})
			require.NoError(t, err)
			t.Logf("removed %d orphans", gc.Deleted)
			for i, runID := range runs {
				ropt := DefaultRestoreOptions()
				ropt.At = runID
				target := t.TempDir()
				st, err := StartRestore(context.Background(), f, target, ropt)
				if len(states[i]) == 0 {
					assert.ErrorContains(t, err, "nothing to restore")
					continue
				}
				require.NoError(t, err, "restore at run %d", i)
				assert.Equal(t, StateDone, st.State, "restore at run %d", i)
				assert.Equal(t, states[i], treeState(t, target), "restore at run %d", i)
			}
			report, err := Check(context.Background(), f, "", "", CheckOptions{Download: true})
			require.NoError(t, err)
			assert.False(t, report.Failed(), "%+v", report)
		})
	}
}
