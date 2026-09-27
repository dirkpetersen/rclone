//go:build unix

package backup

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	libgda "github.com/rclone/rclone/lib/gda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReport(t *testing.T) {
	dir := t.TempDir()
	defer func(old string) { ledgerFile = old }(ledgerFile)

	read := func() map[string]any {
		data, err := os.ReadFile(ledgerFile)
		require.NoError(t, err)
		var got map[string]any
		require.NoError(t, json.Unmarshal(data, &got))
		return got
	}

	ledgerFile = ""
	require.NoError(t, writeReport(outcomeOK, nil, nil))

	ledgerFile = filepath.Join(dir, "ledger.json")
	ledger := &libgda.Ledger{RunID: "20260927T000000Z", Stats: libgda.Stats{Added: 3}}
	require.NoError(t, writeReport(outcomeOK, ledger, nil))
	got := read()
	assert.Equal(t, "ok", got["Outcome"])
	assert.NotContains(t, got, "Error")
	assert.Equal(t, "20260927T000000Z", got["RunID"])
	assert.Equal(t, 3.0, got["Stats"].(map[string]any)["Added"])

	require.NoError(t, writeReport(outcomeOK, ledger, errors.New("2 errors")))
	got = read()
	assert.Equal(t, "failed", got["Outcome"])
	assert.Equal(t, "2 errors", got["Error"])
	assert.Equal(t, "20260927T000000Z", got["RunID"])

	require.NoError(t, writeReport(outcomeNoChanges, nil, nil))
	got = read()
	assert.Equal(t, map[string]any{"Outcome": "no changes"}, got)

	ledgerFile = filepath.Join(dir, "missing", "ledger.json")
	assert.Error(t, writeReport(outcomeOK, nil, nil))
}

func TestReadChangesOutsideSource(t *testing.T) {
	dir := t.TempDir()
	defer func(old string) { changesFrom = old }(changesFrom)
	changesFrom = filepath.Join(dir, "changes")
	require.NoError(t, os.WriteFile(changesFrom, []byte("/other/a\n/data/lab1/b\n"), 0o666))

	changes, err := readChanges("/data/lab2")
	require.NoError(t, err)
	assert.Empty(t, changes)

	changes, err = readChanges("/data/lab1")
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, changes)
}
