package gda

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"sync"

	"github.com/rclone/rclone/fs"
)

// CheckOptions configure Check.
type CheckOptions struct {
	Download bool // read the data of files which can be read now and check their MD5
}

// CheckReport is what Check found.
type CheckReport struct {
	Files      int64    `json:"files"`      // files with data checked
	Objects    int64    `json:"objects"`    // data objects referred to
	Missing    []string `json:"missing"`    // files whose data object doesn't exist
	WrongSize  []string `json:"wrong_size"` // files whose standalone object has the wrong size
	Downloaded int64    `json:"downloaded"` // files whose data was read
	Unreadable int64    `json:"unreadable"` // files whose data couldn't be read now, for example as it is archived
	BadMD5     []string `json:"bad_md5"`    // files whose data read back with the wrong MD5
	Errors     []string `json:"errors"`     // other problems
}

// Failed returns true if the check found a problem.
func (r *CheckReport) Failed() bool {
	return len(r.Missing)+len(r.WrongSize)+len(r.BadMD5)+len(r.Errors) > 0
}

// Check checks that the data of every file below p in the GDA tree at
// dst, as it is now or as it was at the end of run at, is stored: that
// its object exists and, for standalone objects, has the size recorded.
// With Download it also reads the data of the files which can be read
// now and checks their MD5.
func Check(ctx context.Context, dst fs.Fs, p, at string, opt CheckOptions) (*CheckReport, error) {
	r := &CheckReport{Missing: []string{}, WrongSize: []string{}, BadMD5: []string{}, Errors: []string{}}
	var files []Located
	err := Walk(ctx, dst, p, at, func(l *Located) error {
		if l.Type == TypeFile && l.Size > 0 {
			files = append(files, *l)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// One listing per directory holding data objects.
	listings := map[string]map[string]int64{}
	referenced := map[string]bool{}
	for i := range files {
		l := &files[i]
		r.Files++
		if l.outsideRoot() {
			r.Errors = append(r.Errors, l.errOutsideRoot().Error())
			continue
		}
		key := l.ObjectKey()
		if l.VersionID != "" {
			// A listing shows only the current version.
			o, err := newDataObject(ctx, dst, l.objectRef())
			switch {
			case errors.Is(err, fs.ErrorObjectNotFound):
				r.Missing = append(r.Missing, l.LocalPath)
			case err != nil:
				r.Errors = append(r.Errors, fmt.Sprintf("%q: %v", l.LocalPath, err))
			case l.StoredSize >= 0 && o.Size() >= 0 && o.Size() != l.StoredSize:
				r.WrongSize = append(r.WrongSize, l.LocalPath)
			}
			if !referenced[l.objectRef()] {
				referenced[l.objectRef()] = true
				r.Objects++
			}
			continue
		}
		dir := path.Dir(key)
		if dir == "." {
			dir = ""
		}
		objects, ok := listings[dir]
		if !ok {
			objects = map[string]int64{}
			entries, err := dst.List(ctx, dir)
			if err != nil && !errors.Is(err, fs.ErrorDirNotFound) {
				return nil, fmt.Errorf("list %q: %w", dir, err)
			}
			for _, e := range entries {
				if o, ok := e.(fs.Object); ok {
					objects[path.Base(o.Remote())] = o.Size()
				}
			}
			listings[dir] = objects
		}
		if !referenced[key] {
			referenced[key] = true
			r.Objects++
		}
		size, ok := objects[path.Base(key)]
		switch {
		case !ok:
			r.Missing = append(r.Missing, l.LocalPath)
		case l.Offset < 0 && l.StoredSize >= 0 && size >= 0 && size != l.StoredSize:
			r.WrongSize = append(r.WrongSize, l.LocalPath)
		}
	}
	if opt.Download {
		if err := r.download(ctx, dst, at, files); err != nil {
			return r, err
		}
	}
	sort.Strings(r.Missing)
	sort.Strings(r.WrongSize)
	sort.Strings(r.BadMD5)
	return r, nil
}

// download reads the data of files and checks their MD5, with as many
// files at once as --checkers.
func (r *CheckReport) download(ctx context.Context, dst fs.Fs, at string, files []Located) error {
	b, err := NewBrowser(dst, at)
	if err != nil {
		return err
	}
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		jobs = make(chan *Located)
	)
	for range max(fs.GetConfig(ctx).Checkers, 1) {
		wg.Go(func() {
			for l := range jobs {
				sum, opened, err := readMD5(ctx, b, l)
				mu.Lock()
				switch {
				case err != nil && !opened:
					r.Unreadable++
					fs.Debugf(l.LocalPath, "gda: check: can't read: %v", err)
				case err != nil:
					r.Downloaded++
					r.Errors = append(r.Errors, fmt.Sprintf("read %q: %v", l.LocalPath, err))
				case sum != l.MD5:
					r.Downloaded++
					r.BadMD5 = append(r.BadMD5, l.LocalPath)
				default:
					r.Downloaded++
				}
				mu.Unlock()
			}
		})
	}
	for i := range files {
		if files[i].outsideRoot() {
			continue
		}
		jobs <- &files[i]
	}
	close(jobs)
	wg.Wait()
	return nil
}

// readMD5 reads the data of l and returns its MD5. opened is false if
// the data couldn't be opened, as when it must be restored first.
func readMD5(ctx context.Context, b *Browser, l *Located) (sum string, opened bool, err error) {
	in, err := b.Open(ctx, l)
	if err != nil {
		return "", false, err
	}
	defer fs.CheckClose(in, &err)
	h := md5.New()
	if _, err := io.Copy(h, in); err != nil {
		return "", true, err
	}
	return hex.EncodeToString(h.Sum(nil)), true, nil
}
