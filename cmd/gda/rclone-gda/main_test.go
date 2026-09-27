package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGdaArgs(t *testing.T) {
	for _, test := range []struct {
		in   []string
		want []string
	}{
		{[]string{"rclone-gda"}, []string{"rclone-gda", "gda", "--help"}},
		{[]string{"rclone-gda", "backup", "/src", "s3:b"}, []string{"rclone-gda", "gda", "backup", "/src", "s3:b"}},
		{[]string{"rclone-gda", "-v", "ls", "s3:b"}, []string{"rclone-gda", "gda", "-v", "ls", "s3:b"}},
		{[]string{"rclone-gda", "--help"}, []string{"rclone-gda", "gda", "--help"}},
		{[]string{"rclone-gda", "version"}, []string{"rclone-gda", "version"}},
		{[]string{"rclone-gda", "--version"}, []string{"rclone-gda", "--version"}},
		{[]string{"rclone-gda", "config"}, []string{"rclone-gda", "config"}},
	} {
		assert.Equal(t, test.want, gdaArgs(test.in), test.in)
	}
}
