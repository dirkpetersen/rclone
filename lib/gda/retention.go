package gda

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/rclone/rclone/fs"
)

// historyKey records from which run a GDA tree's history is kept, once
// gc has removed data only older history needed.
var historyKey = joinRemote(MetaDir, "history.json")

// historyState is the content of historyKey.
type historyState struct {
	From string // history before this run may refer to removed data
}

// writeHistoryFrom records that history is kept from run from.
func writeHistoryFrom(ctx context.Context, d *dest, from string) error {
	if old, err := readHistoryFrom(ctx, d); err != nil {
		return err
	} else if old > from {
		from = old
	}
	data, err := json.Marshal(historyState{From: from})
	if err != nil {
		return err
	}
	return d.putBytes(ctx, historyKey, data, d.metaTier)
}

// readHistoryFrom returns the run from which history is kept, or "" if
// all of it is.
func readHistoryFrom(ctx context.Context, d *dest) (string, error) {
	data, err := d.get(ctx, historyKey)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read %q: %w", historyKey, err)
	}
	var state historyState
	if err := json.Unmarshal(data, &state); err != nil {
		return "", fmt.Errorf("parse %q: %w", historyKey, err)
	}
	return state.From, nil
}

// checkHistory returns an error if the tree at d can't be read as it was
// at run at, as gc has removed data that history needs.
func checkHistory(ctx context.Context, d *dest, at string) error {
	if at == "" {
		return nil
	}
	from, err := readHistoryFrom(ctx, d)
	if err != nil {
		return err
	}
	if from != "" && at < from {
		return fmt.Errorf("history before run %s has been removed, so the tree can't be read as it was at %s", from, at)
	}
	return nil
}

// rebaseKey lists the directories gc marked for the next full backup to
// pack again from the source, so their mostly dead packs can expire.
var rebaseKey = joinRemote(MetaDir, "rebase.csv")

// readRebase returns the directory keys marked for packing again.
func readRebase(ctx context.Context, d *dest) (map[string]bool, error) {
	dirs := map[string]bool{}
	data, err := d.get(ctx, rebaseKey)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return dirs, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", rebaseKey, err)
	}
	records, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", rebaseKey, err)
	}
	for i, r := range records {
		if i > 0 && len(r) > 0 {
			dirs[r[0]] = true
		}
	}
	return dirs, nil
}

// markRebase adds dirs to the directories marked for packing again.
func markRebase(ctx context.Context, d *dest, dirs []string) error {
	marked, err := readRebase(ctx, d)
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		marked[dir] = true
	}
	all := make([]string, 0, len(marked))
	for dir := range marked {
		all = append(all, dir)
	}
	sort.Strings(all)
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write([]string{"dir"})
	for _, dir := range all {
		_ = w.Write([]string{dir})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return err
	}
	return d.putBytes(ctx, rebaseKey, buf.Bytes(), d.metaTier)
}

// clearRebase removes the marks, once a full backup has acted on them.
func clearRebase(ctx context.Context, d *dest) {
	if d.dryRun {
		return
	}
	o, err := d.f.NewObject(ctx, rebaseKey)
	if errors.Is(err, fs.ErrorObjectNotFound) {
		return
	}
	if err == nil {
		err = o.Remove(ctx)
	}
	if err != nil {
		// Credentials for backups may not be allowed to delete anything.
		fs.Infof(nil, "gda: can't remove %q, so emptying it: %v", rebaseKey, err)
		err = d.putBytes(ctx, rebaseKey, nil, d.metaTier)
	}
	if err != nil {
		fs.Errorf(nil, "gda: clear %q: %v", rebaseKey, err)
	}
}
