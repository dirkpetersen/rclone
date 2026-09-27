package gda

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// maxIndexRows is the number of rows above which an index is split into
// parts, with gda-index.csv holding a table of contents. Tests lower it.
var maxIndexRows = 100000

// tocColumns are the columns of the table of contents of a split index.
var tocColumns = []string{"part", "first", "last", "rows"}

// indexPart is one part of a split index.
type indexPart struct {
	name  string
	first string
	last  string
	rows  int
}

// sortEntries sorts entries by name, which is the order indexes are
// written in.
func sortEntries(entries []Entry) {
	slices.SortFunc(entries, func(a, b Entry) int {
		return strings.Compare(a.Name, b.Name)
	})
}

// encodeIndex returns the objects making up the index for entries, keyed
// by name. Up to maxIndexRows it is just gda-index.csv; above that the
// entries are split into parts and gda-index.csv is a table of contents.
func encodeIndex(entries []Entry, maxRows int, runID string) (map[string][]byte, error) {
	sortEntries(entries)
	objects := map[string][]byte{}
	if len(entries) <= maxRows {
		var buf bytes.Buffer
		if err := WriteEntries(&buf, entries); err != nil {
			return nil, err
		}
		objects[IndexName] = buf.Bytes()
		return objects, nil
	}
	var toc bytes.Buffer
	cw := csv.NewWriter(&toc)
	if err := cw.Write(tocColumns); err != nil {
		return nil, err
	}
	for n, start := 1, 0; start < len(entries); n, start = n+1, start+maxRows {
		end := min(start+maxRows, len(entries))
		part := entries[start:end]
		var buf bytes.Buffer
		if err := WriteEntries(&buf, part); err != nil {
			return nil, err
		}
		name := indexPartName(runID, n)
		objects[name] = buf.Bytes()
		if err := cw.Write([]string{name, part[0].Name, part[len(part)-1].Name, strconv.Itoa(len(part))}); err != nil {
			return nil, err
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return nil, err
	}
	objects[IndexName] = toc.Bytes()
	return objects, nil
}

// isTOC returns true if data is the table of contents of a split index.
func isTOC(data []byte) bool {
	return bytes.HasPrefix(data, []byte(strings.Join(tocColumns, ",")))
}

// readTOC parses the table of contents of a split index.
func readTOC(r io.Reader) ([]indexPart, error) {
	cr := csv.NewReader(r)
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read index table of contents: %w", err)
	}
	if !slices.Equal(header, tocColumns) {
		return nil, fmt.Errorf("unexpected index table of contents header %q", header)
	}
	var parts []indexPart
	for {
		record, err := cr.Read()
		if errors.Is(err, io.EOF) {
			return parts, nil
		}
		if err != nil {
			return nil, err
		}
		rows, err := strconv.Atoi(record[3])
		if err != nil {
			return nil, fmt.Errorf("index table of contents: rows %q: %w", record[3], err)
		}
		parts = append(parts, indexPart{name: record[0], first: record[1], last: record[2], rows: rows})
	}
}
