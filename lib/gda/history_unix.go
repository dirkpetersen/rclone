//go:build unix

package gda

// history is what a directory's changesets record for each name, from
// which gc finds the data that only history older than a given run
// needs.
type history struct {
	dir    string
	byName map[string][]historyRow
}

// historyRow is a changeset row and the run which wrote it.
type historyRow struct {
	run string
	row Entry
}

func newHistory(dir string) *history {
	return &history{dir: dir, byName: map[string][]historyRow{}}
}

// add records row, written by run. Rows must be added in the order
// history replays them.
func (h *history) add(run string, row *Entry) {
	h.byName[row.Name] = append(h.byName[row.Name], historyRow{run: run, row: *row})
}

// expiry works out, for history kept from run from, which objects the
// rows still live at or after from need, and which only rows which ended
// before it do. A row ends when a later one replaces or deletes its
// name. neededHere and expired hold this directory's objects by name, or
// by versionKey of name and version for versions, and neededElsewhere
// other directories' objects by versionKey of key and version. expired
// maps each reference to the stored size its rows record. With from ""
// everything is needed.
func (h *history) expiry(from string) (neededHere, neededElsewhere map[string]bool, expired map[string]int64) {
	neededHere, neededElsewhere, expired = map[string]bool{}, map[string]bool{}, map[string]int64{}
	for _, rows := range h.byName {
		for i, hr := range rows {
			row := &hr.row
			if row.Action == ActionDelete {
				continue
			}
			end := ""
			if i+1 < len(rows) {
				end = rows[i+1].run
			}
			needed := from == "" || end == "" || end > from
			switch {
			case row.DedupOf != "":
				if needed {
					l := Located{Entry: *row, IndexKey: h.dir}
					neededElsewhere[l.objectRef()] = true
				}
			case row.Location != "":
				ref := versionKey(row.Location, row.VersionID)
				if needed {
					neededHere[ref] = true
					// The object under the name is one of its versions.
					neededHere[row.Location] = true
				} else {
					expired[ref] = max(row.StoredSize, 0)
				}
			}
		}
	}
	return neededHere, neededElsewhere, expired
}
