package gda

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntriesRoundTrip(t *testing.T) {
	file := NewEntry("a, \"quoted\"\nname.txt", TypeFile)
	file.Size = 1234
	file.ModTime = time.Date(2026, 9, 26, 12, 0, 0, 123456789, time.UTC)
	file.Mode = 0o4755
	file.Owner, file.Group = "jdoe", "lab"
	file.UID, file.GID = 1000, 2000
	file.MD5 = "9e107d9d372bb6826bd81d3542a419d6"
	file.Location = "dir.gda.20260926T120000Z.w01.001.tar"
	file.Offset = 1536
	file.Codec = CodecNone
	file.StoredOffset, file.StoredLength = 1536, 1234
	file.Run = "20260926T120000Z"
	file.Action = ActionAdd
	dir := NewEntry("sub", TypeDir)
	dir.TreeSize, dir.TreeFiles = 99, 3
	dir.Listing = ListingIndex
	link := NewEntry("ünïcode-link", TypeSymlink)
	link.LinkTarget = "../target"

	var buf bytes.Buffer
	require.NoError(t, WriteEntries(&buf, []Entry{file, dir, link}))
	got, err := ReadEntries(&buf)
	require.NoError(t, err)
	assert.Equal(t, []Entry{file, dir, link}, got)
}

func TestReadEntriesIgnoresUnknownColumns(t *testing.T) {
	in := "name,future_column,type,size\nx,whatever,file,5\n"
	got, err := ReadEntries(strings.NewReader(in))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "x", got[0].Name)
	assert.Equal(t, TypeFile, got[0].Type)
	assert.Equal(t, int64(5), got[0].Size)
	assert.Equal(t, int64(-1), got[0].Offset)
}

func TestReadEntriesHeaderOnly(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteEntries(&buf, nil))
	got, err := ReadEntries(&buf)
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func TestReadEntriesErrors(t *testing.T) {
	_, err := ReadEntries(strings.NewReader(""))
	assert.Error(t, err)
	_, err = ReadEntries(strings.NewReader("name,size\nx,notanumber\n"))
	assert.ErrorContains(t, err, `column "size"`)
}

func TestEncodeName(t *testing.T) {
	name, enc := encodeName("plain.txt")
	assert.Equal(t, "plain.txt", name)
	assert.Equal(t, "", enc)

	name, enc = encodeName("100%.txt")
	assert.Equal(t, NameEncodingPercent, enc)
	assert.NotEqual(t, "100%.txt", name)

	name, enc = encodeName("new\nline")
	assert.Equal(t, NameEncodingPercent, enc)
	assert.Equal(t, "new%0Aline", name)

	raw := "bad\xffname%"
	name, enc = encodeName(raw)
	assert.Equal(t, NameEncodingPercent, enc)
	e := NewEntry(name, TypeFile)
	e.NameEncoding = enc
	decoded, err := e.DecodeName()
	require.NoError(t, err)
	assert.Equal(t, raw, decoded)
}

func TestIsReserved(t *testing.T) {
	for _, test := range []struct {
		name   string
		atRoot bool
		want   bool
	}{
		{"file.txt", false, false},
		{IndexName, false, true},
		{"gda-index.20260926T120000Z.00001.csv", false, true},
		{"x.gda.20260926T120000Z.w01.001.tar", false, true},
		{"x.gda.20260926T120000Z.csv", false, true},
		{MetaDir, true, true},
		{MetaDir, false, false},
	} {
		assert.Equal(t, test.want, isReserved(test.name, test.atRoot), test.name)
	}
}

func TestEncodeIndexSplit(t *testing.T) {
	var entries []Entry
	for _, name := range []string{"e", "a", "d", "b", "c"} {
		entries = append(entries, NewEntry(name, TypeFile))
	}
	objects, err := encodeIndex(entries, 2, "20260926T120000Z")
	require.NoError(t, err)
	require.Len(t, objects, 4)
	assert.True(t, isTOC(objects[IndexName]))
	parts, err := readTOC(bytes.NewReader(objects[IndexName]))
	require.NoError(t, err)
	assert.Equal(t, []indexPart{
		{name: "gda-index.20260926T120000Z.00001.csv", first: "a", last: "b", rows: 2},
		{name: "gda-index.20260926T120000Z.00002.csv", first: "c", last: "d", rows: 2},
		{name: "gda-index.20260926T120000Z.00003.csv", first: "e", last: "e", rows: 1},
	}, parts)

	objects, err = encodeIndex(entries, 10, "20260926T120000Z")
	require.NoError(t, err)
	require.Len(t, objects, 1)
	assert.False(t, isTOC(objects[IndexName]))
}

func TestRunID(t *testing.T) {
	start := time.Date(2026, 9, 26, 12, 0, 1, 999, time.FixedZone("PDT", -7*3600))
	id := NewRunID(start)
	assert.Equal(t, "20260926T190001Z", id)
	parsed, err := ParseRunID(id)
	require.NoError(t, err)
	assert.True(t, parsed.Equal(start.Truncate(time.Second)))
}
