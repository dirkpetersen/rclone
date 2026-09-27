// Package gda implements the GDA archive format, which stores directory
// trees in S3 Glacier Deep Archive at low cost.
//
// Each source directory is described by an index (gda-index.csv) and by
// one changeset per run that changed it. Small files are packed into
// POSIX tar files per directory, large files are stored as their own
// objects. See glacier-deep-archive-design.md for the design.
package gda

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// FormatVersion is the version of the format written by this package.
const FormatVersion = 1

// Entry types.
const (
	TypeFile     = "file"
	TypeDir      = "dir"
	TypeSymlink  = "symlink"
	TypeFifo     = "fifo"
	TypeSocket   = "socket"
	TypeCharDev  = "chardev"
	TypeBlockDev = "blockdev"
)

// Changeset actions.
const (
	ActionAdd    = "add"
	ActionModify = "modify"
	ActionMeta   = "meta"
	ActionDelete = "delete"
	// ActionRebase records a file whose unchanged content was packed
	// again, so that the directory's files are in fewer packs.
	ActionRebase = "rebase"
)

// Listing values for directory rows.
const (
	// ListingIndex means the directory has its own gda-index.csv.
	ListingIndex = "index"
	// ListingRollup means the directory is listed in this index by path prefix.
	ListingRollup = "rollup"
)

// Codecs.
const (
	CodecNone = "none"
)

// NameEncodingPercent marks a name that was not valid UTF-8 and has been
// percent-encoded.
const NameEncodingPercent = "percent"

// Entry is one row of an index, a changeset or a pack manifest.
//
// Integer fields which don't apply to an entry are -1 and are written as
// empty CSV fields.
type Entry struct {
	Name         string    `json:"name"`             // path relative to the index's directory, "/" separated
	Type         string    `json:"type"`             // one of the Type constants
	Size         int64     `json:"size"`             // bytes
	ModTime      time.Time `json:"mtime"`            // modification time, UTC
	Mode         uint32    `json:"mode"`             // permission bits including setuid, setgid and sticky
	Owner        string    `json:"owner"`            // user name, if known
	Group        string    `json:"group"`            // group name, if known
	UID          int64     `json:"uid"`              // numeric user ID
	GID          int64     `json:"gid"`              // numeric group ID
	MD5          string    `json:"md5"`              // hex MD5 of the original content
	LinkTarget   string    `json:"link_target"`      // symlink target
	Location     string    `json:"location"`         // pack name or object key holding the data
	Offset       int64     `json:"offset"`           // data offset inside the uncompressed tar
	Codec        string    `json:"codec"`            // codec of the stored bytes
	StoredOffset int64     `json:"stored_offset"`    // start of the stored bytes holding this entry
	StoredLength int64     `json:"stored_length"`    // length of the stored bytes holding this entry
	StoredSize   int64     `json:"stored_size"`      // size of a standalone object as stored
	StoredMD5    string    `json:"stored_md5"`       // MD5 of a standalone object as stored
	DedupOf      string    `json:"dedup_of"`         // location of the copy this entry is a duplicate of
	VersionID    string    `json:"version_id"`       // S3 version ID of a standalone object
	Run          string    `json:"run"`              // run that wrote this version
	TreeSize     int64     `json:"tree_size"`        // directories: bytes in the whole subtree
	TreeFiles    int64     `json:"tree_files"`       // directories: non-directory entries in the whole subtree
	Listing      string    `json:"listing"`          // directories: ListingIndex or ListingRollup
	NameEncoding string    `json:"name_encoding"`    // "" or NameEncodingPercent
	Action       string    `json:"action,omitempty"` // changesets only: one of the Action constants
	DevMajor     int64     `json:"dev_major"`        // device files: major device number
	DevMinor     int64     `json:"dev_minor"`        // device files: minor device number
	StoredStart  int64     `json:"stored_start"`     // offset in the uncompressed pack where the stored range starts
	HardLink     string    `json:"hard_link"`        // files with more than one link: device and inode, the same for all links
	Xattrs       string    `json:"xattrs"`           // extended attributes: base64 name:value pairs separated by spaces
	Target       string    `json:"target,omitempty"` // restore plans only: path below the restore target
}

// NewEntry returns an Entry with the integer fields that don't apply set to -1.
func NewEntry(name, typ string) Entry {
	return Entry{
		Name:         name,
		Type:         typ,
		Size:         -1,
		UID:          -1,
		GID:          -1,
		Offset:       -1,
		StoredOffset: -1,
		StoredLength: -1,
		StoredSize:   -1,
		TreeSize:     -1,
		TreeFiles:    -1,
		DevMajor:     -1,
		DevMinor:     -1,
		StoredStart:  -1,
	}
}

// IsDir returns true if the entry is a directory.
func (e *Entry) IsDir() bool {
	return e.Type == TypeDir
}

// sourceEntry is an entry read from the source file system.
type sourceEntry struct {
	Entry
	path   string // full source path
	rel    string // source path relative to the source root, "/" separated
	rebase bool   // packed again although unchanged, so never a dedup copy
}

// columns are the CSV columns in the order they are written. New columns
// may only ever be added at the end, and readers ignore unknown columns.
var columns = []string{
	"name", "type", "size", "mtime", "mode", "owner", "group", "uid", "gid",
	"md5", "link_target", "location", "offset", "codec", "stored_offset",
	"stored_length", "stored_size", "stored_md5", "dedup_of", "version_id",
	"run", "tree_size", "tree_files", "listing", "name_encoding", "action",
	"dev_major", "dev_minor", "stored_start", "hard_link", "xattrs",
}

// planColumns are the columns of a restore plan.
var planColumns = append(append([]string(nil), columns...), "target")

func formatInt(i int64) string {
	if i < 0 {
		return ""
	}
	return strconv.FormatInt(i, 10)
}

func parseInt(s string) (int64, error) {
	if s == "" {
		return -1, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// value returns the CSV value of column col.
func (e *Entry) value(col string) string {
	switch col {
	case "name":
		return e.Name
	case "type":
		return e.Type
	case "size":
		return formatInt(e.Size)
	case "mtime":
		return formatTime(e.ModTime)
	case "mode":
		if e.Type == "" {
			return ""
		}
		return fmt.Sprintf("%04o", e.Mode)
	case "owner":
		return e.Owner
	case "group":
		return e.Group
	case "uid":
		return formatInt(e.UID)
	case "gid":
		return formatInt(e.GID)
	case "md5":
		return e.MD5
	case "link_target":
		return e.LinkTarget
	case "location":
		return e.Location
	case "offset":
		return formatInt(e.Offset)
	case "codec":
		return e.Codec
	case "stored_offset":
		return formatInt(e.StoredOffset)
	case "stored_length":
		return formatInt(e.StoredLength)
	case "stored_size":
		return formatInt(e.StoredSize)
	case "stored_md5":
		return e.StoredMD5
	case "dedup_of":
		return e.DedupOf
	case "version_id":
		return e.VersionID
	case "run":
		return e.Run
	case "tree_size":
		return formatInt(e.TreeSize)
	case "tree_files":
		return formatInt(e.TreeFiles)
	case "listing":
		return e.Listing
	case "name_encoding":
		return e.NameEncoding
	case "action":
		return e.Action
	case "dev_major":
		return formatInt(e.DevMajor)
	case "dev_minor":
		return formatInt(e.DevMinor)
	case "stored_start":
		return formatInt(e.StoredStart)
	case "hard_link":
		return e.HardLink
	case "xattrs":
		return e.Xattrs
	case "target":
		return e.Target
	}
	return ""
}

// setField sets the field for column col from the CSV value v.
func (e *Entry) setField(col, v string) (err error) {
	switch col {
	case "name":
		e.Name = v
	case "type":
		e.Type = v
	case "size":
		e.Size, err = parseInt(v)
	case "mtime":
		if v != "" {
			e.ModTime, err = time.Parse(time.RFC3339Nano, v)
		}
	case "mode":
		if v != "" {
			var mode uint64
			mode, err = strconv.ParseUint(v, 8, 32)
			e.Mode = uint32(mode)
		}
	case "owner":
		e.Owner = v
	case "group":
		e.Group = v
	case "uid":
		e.UID, err = parseInt(v)
	case "gid":
		e.GID, err = parseInt(v)
	case "md5":
		e.MD5 = v
	case "link_target":
		e.LinkTarget = v
	case "location":
		e.Location = v
	case "offset":
		e.Offset, err = parseInt(v)
	case "codec":
		e.Codec = v
	case "stored_offset":
		e.StoredOffset, err = parseInt(v)
	case "stored_length":
		e.StoredLength, err = parseInt(v)
	case "stored_size":
		e.StoredSize, err = parseInt(v)
	case "stored_md5":
		e.StoredMD5 = v
	case "dedup_of":
		e.DedupOf = v
	case "version_id":
		e.VersionID = v
	case "run":
		e.Run = v
	case "tree_size":
		e.TreeSize, err = parseInt(v)
	case "tree_files":
		e.TreeFiles, err = parseInt(v)
	case "listing":
		e.Listing = v
	case "name_encoding":
		e.NameEncoding = v
	case "action":
		e.Action = v
	case "dev_major":
		e.DevMajor, err = parseInt(v)
	case "dev_minor":
		e.DevMinor, err = parseInt(v)
	case "stored_start":
		e.StoredStart, err = parseInt(v)
	case "hard_link":
		e.HardLink = v
	case "xattrs":
		e.Xattrs = v
	case "target":
		e.Target = v
	}
	if err != nil {
		return fmt.Errorf("column %q: %w", col, err)
	}
	return nil
}

// WriteEntries writes entries as CSV with a header row.
func WriteEntries(w io.Writer, entries []Entry) error {
	return writeColumns(w, entries, columns)
}

// writeColumns writes the columns cols of entries as CSV with a header row.
func writeColumns(w io.Writer, entries []Entry, cols []string) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(cols); err != nil {
		return err
	}
	record := make([]string, len(cols))
	for i := range entries {
		for j, col := range cols {
			record[j] = entries[i].value(col)
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

// ReadEntries reads CSV written by WriteEntries. Columns it doesn't know
// are ignored so that files written by newer versions can still be read.
func ReadEntries(r io.Reader) ([]Entry, error) {
	entries := []Entry{}
	err := readEntriesFunc(r, func(e *Entry) error {
		entries = append(entries, *e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// readEntriesFunc reads entries written by WriteEntries, calling fn for
// each, so that a large file needn't be held in memory.
func readEntriesFunc(r io.Reader, fn func(*Entry) error) error {
	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	header, err := cr.Read()
	if errors.Is(err, io.EOF) {
		return errors.New("empty CSV: missing header row")
	}
	if err != nil {
		return err
	}
	header = append([]string(nil), header...)
	cr.FieldsPerRecord = len(header)
	for {
		record, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		e := NewEntry("", "")
		for i, v := range record {
			if err := e.setField(header[i], v); err != nil {
				line, _ := cr.FieldPos(i)
				return fmt.Errorf("line %d: %w", line, err)
			}
		}
		if err := fn(&e); err != nil {
			return err
		}
	}
}

// encodeName returns name as valid UTF-8 without control characters,
// and the name encoding used. Backends map control characters in object
// names differently, so names holding them are encoded to keep keys the
// same whichever backend lists them.
//
// Names which contain "%" are encoded too, so that an encoded name can
// never equal a name which wasn't encoded.
func encodeName(name string) (string, string) {
	if utf8.ValidString(name) && !strings.Contains(name, "%") && !strings.ContainsFunc(name, isControl) {
		return name, ""
	}
	return url.PathEscape(name), NameEncodingPercent
}

// isControl returns true for the ASCII control characters.
func isControl(r rune) bool {
	return r < 0x20 || r == 0x7f
}

// DecodeName returns the original bytes of an entry's name.
func (e *Entry) DecodeName() (string, error) {
	if e.NameEncoding != NameEncodingPercent {
		return e.Name, nil
	}
	return url.PathUnescape(e.Name)
}

// isDirectChild returns true if name is directly in the index's directory
// rather than in a rolled up subdirectory.
func isDirectChild(name string) bool {
	return !strings.Contains(name, "/")
}
