//go:build unix

package gda

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"
)

// Every run writes the rows of the changesets it committed, with full
// paths, to _gda/catalog/runs/<run>/<worker>.csv.zst. Together these
// hold the history of the whole tree, so it can be searched with tools
// such as DuckDB or Athena, which read zstd compressed CSV directly,
// without reading every directory's index. The catalog is derived from
// the changesets and can always be rebuilt.

// catalogColumns are the catalog's columns.
var catalogColumns = []string{
	"path", "run", "action", "type", "size", "mtime", "mode", "owner", "group",
	"md5", "object", "offset", "codec", "tree_size", "tree_files",
}

// catalogWriter writes a run's catalog to a local file. It is safe for
// concurrent use.
type catalogWriter struct {
	mu   sync.Mutex
	file *os.File
	fw   *frameWriter
	w    *csv.Writer
	n    int // rows written
}

func newCatalogWriter(tempDir string) (*catalogWriter, error) {
	f, err := os.CreateTemp(tempDir, "gda-catalog-*.csv.zst")
	if err != nil {
		return nil, err
	}
	fw, err := newFrameWriter(f, 3)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, err
	}
	c := &catalogWriter{file: f, fw: fw, w: csv.NewWriter(fw)}
	if err := c.w.Write(catalogColumns); err != nil {
		c.remove()
		return nil, err
	}
	return c, nil
}

// add records the changeset rows of the directory at key.
func (c *catalogWriter) add(key string, rows []Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := make([]string, len(catalogColumns))
	for i := range rows {
		e := &rows[i]
		l := Located{Entry: *e, IndexKey: key}
		object := ""
		if e.Location != "" || e.DedupOf != "" {
			object = l.ObjectKey()
		}
		mtime := ""
		if !e.ModTime.IsZero() {
			mtime = e.ModTime.UTC().Format(time.RFC3339Nano)
		}
		record = append(record[:0],
			joinRemote(key, e.Name), e.Run, e.Action, e.Type, formatInt(e.Size), mtime,
			fmt.Sprintf("%04o", e.Mode), e.Owner, e.Group, e.MD5, object, formatInt(e.Offset),
			e.Codec, formatInt(e.TreeSize), formatInt(e.TreeFiles))
		if err := c.w.Write(record); err != nil {
			return err
		}
		c.n++
	}
	return nil
}

// finish closes the catalog and uploads it to key if it has any rows.
func (c *catalogWriter) finish(ctx context.Context, d *dest, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.remove()
	c.w.Flush()
	if err := c.w.Error(); err != nil {
		return err
	}
	if err := c.fw.Close(); err != nil {
		return err
	}
	if err := c.file.Sync(); err != nil {
		return err
	}
	if c.n == 0 {
		return nil
	}
	info, err := os.Stat(c.file.Name())
	if err != nil {
		return err
	}
	sum, err := hashFile(c.file.Name())
	if err != nil {
		return err
	}
	return d.putFile(ctx, key, c.file.Name(), info.Size(), sum, d.metaTier)
}

// remove deletes the local file.
func (c *catalogWriter) remove() {
	_ = c.file.Close()
	_ = os.Remove(c.file.Name())
}
