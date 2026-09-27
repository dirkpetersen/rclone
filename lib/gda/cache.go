package gda

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/rclone/rclone/fs"
)

// indexCache keeps local copies of the indexes of one destination, so a
// run needn't read the indexes which the previous run from the same
// cache wrote.
//
// The copies are trusted only if the latest run on the destination is
// the one which last finished with this cache, as then nothing else has
// written indexes since. Otherwise the cache starts empty.
type indexCache struct {
	dir     string
	trusted bool

	mu     sync.Mutex
	failed bool // a copy couldn't be updated, so the cache can't be trusted later
}

// cacheState records the run which last finished with a cache.
type cacheState struct {
	Destination string
	Run         string
}

const cacheStateName = "state.json"

// openIndexCache opens the cache below root for destination, whose
// latest run is latestRun.
func openIndexCache(root, destination, latestRun string) (*indexCache, error) {
	sum := sha256.Sum256([]byte(destination))
	c := &indexCache{dir: filepath.Join(root, hex.EncodeToString(sum[:16]))}
	statePath := filepath.Join(c.dir, cacheStateName)
	var state cacheState
	if data, err := os.ReadFile(statePath); err == nil && json.Unmarshal(data, &state) == nil {
		c.trusted = latestRun != "" && state.Run == latestRun && state.Destination == destination
	}
	// Until this run finishes, the copies may be ahead of or behind the
	// destination.
	if err := os.Remove(statePath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if !c.trusted {
		if err := os.RemoveAll(c.dir); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return nil, err
	}
	if c.trusted {
		fs.Infof(nil, "gda: using the index cache from run %s", latestRun)
	}
	return c, nil
}

// path returns where the copy of the index of the directory at key is.
func (c *indexCache) path(key string) string {
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:])
	return filepath.Join(c.dir, name[:2], name+".csv")
}

// get returns the copy of the index of the directory at key, if the
// cache is trusted and has one.
func (c *indexCache) get(key string) ([]Entry, bool) {
	if !c.trusted {
		return nil, false
	}
	data, err := os.ReadFile(c.path(key))
	if err != nil {
		return nil, false
	}
	entries, err := ReadEntries(bytes.NewReader(data))
	if err != nil {
		return nil, false
	}
	return entries, true
}

// put saves entries as the index of the directory at key.
func (c *indexCache) put(key string, entries []Entry) {
	var buf bytes.Buffer
	err := WriteEntries(&buf, entries)
	if err == nil {
		err = writeFileAtomic(c.path(key), buf.Bytes())
	}
	if err != nil {
		fs.Errorf(nil, "gda: index cache: %v", err)
		c.fail()
	}
}

// drop removes the copy of the index of the directory at key, for when
// it isn't known whether the destination's index was replaced.
func (c *indexCache) drop(key string) {
	if err := os.Remove(c.path(key)); err != nil && !os.IsNotExist(err) {
		fs.Errorf(nil, "gda: index cache: %v", err)
		c.fail()
	}
}

func (c *indexCache) fail() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failed = true
}

// finish records that run runID finished with the cache matching
// destination.
func (c *indexCache) finish(destination, runID string) {
	c.mu.Lock()
	failed := c.failed
	c.mu.Unlock()
	if failed {
		return
	}
	data, err := json.Marshal(cacheState{Destination: destination, Run: runID})
	if err == nil {
		err = writeFileAtomic(filepath.Join(c.dir, cacheStateName), data)
	}
	if err != nil {
		fs.Errorf(nil, "gda: index cache: %v", err)
	}
}

// writeFileAtomic writes data to p through a temporary file, so that a
// reader never sees part of it.
func writeFileAtomic(p string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	_, err = tmp.Write(data)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), p)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
	}
	return err
}
