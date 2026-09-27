package gda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
