package fs

import (
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSemVersion(t *testing.T) {
	old := Version
	defer func() { Version = old }()
	for _, test := range []struct {
		in   string
		want string
	}{
		{"v1.75.1", "1.75.1"},
		{"v1.76.0-DEV", "1.76.0-DEV"},
		{"v1.75.1.1", "1.75.1+gda.1"},
		{"v1.75.1.12-beta.3.abc1234", "1.75.1-beta.3.abc1234+gda.12"},
	} {
		Version = test.in
		got := SemVersion()
		assert.Equal(t, test.want, got)
		_, err := semver.NewVersion(got)
		require.NoError(t, err, got)
	}
}
