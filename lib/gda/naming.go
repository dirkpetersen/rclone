package gda

import (
	"fmt"
	"path"
	"strings"
	"time"
)

// Names of objects GDA writes.
const (
	// IndexName is the name of the index in each indexed directory.
	IndexName = "gda-index.csv"
	// MetaDir is the directory at the destination root for run metadata.
	MetaDir = "_gda"
	// runIDFormat is the time format of run IDs.
	runIDFormat = "20060102T150405Z"
	// maxKeyLength is the maximum length of an S3 key in bytes.
	maxKeyLength = 1024
)

// NewRunID returns the run ID for a run starting at t.
func NewRunID(t time.Time) string {
	return t.UTC().Format(runIDFormat)
}

// ParseRunID returns the start time of a run.
func ParseRunID(runID string) (time.Time, error) {
	return time.Parse(runIDFormat, runID)
}

// dirLabel returns the name used in pack and changeset names for the
// directory at rel. root is the name used for the top of the tree.
func dirLabel(rel, root string) string {
	if rel == "" {
		return root
	}
	return path.Base(rel)
}

// packName returns the name of a pack.
func packName(label, runID, worker string, part int) string {
	return fmt.Sprintf("%s.gda.%s.%s.%03d.tar", label, runID, worker, part)
}

// changesetName returns the name of a changeset.
func changesetName(label, runID string) string {
	return fmt.Sprintf("%s.gda.%s.csv", label, runID)
}

// versionedName returns the key used for a new version of a standalone
// file whose native name is already taken.
func versionedName(name, runID string) string {
	return fmt.Sprintf("%s.gda.%s", name, runID)
}

// indexPartName returns the name of part n of a split index.
func indexPartName(n int) string {
	return fmt.Sprintf("gda-index.%05d.csv", n)
}

// isReserved returns true if a source entry called name can't be stored
// because it would clash with the objects GDA writes.
func isReserved(name string, atRoot bool) bool {
	if atRoot && name == MetaDir {
		return true
	}
	if name == IndexName || (strings.HasPrefix(name, "gda-index.") && strings.HasSuffix(name, ".csv")) {
		return true
	}
	return strings.Contains(name, ".gda.")
}

// joinRemote joins remote path elements, ignoring empty ones.
func joinRemote(elem ...string) string {
	var parts []string
	for _, e := range elem {
		if e != "" {
			parts = append(parts, e)
		}
	}
	return strings.Join(parts, "/")
}

// keyTooLong returns true if remote, below the destination root prefix,
// would exceed the S3 key length limit.
func keyTooLong(rootPrefix, remote string) bool {
	return len(joinRemote(rootPrefix, remote)) > maxKeyLength
}
