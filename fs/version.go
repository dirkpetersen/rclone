package fs

import "strings"

// Version of rclone containing the complete version string
var Version string

func init() {
	if Version == "" {
		if VersionSuffix == "" {
			Version = VersionTag
		} else {
			Version = VersionTag + "-" + VersionSuffix
		}
	}
}

// SemVersion returns Version without its leading "v" in a form which
// semantic versioning parses. A fourth number, as in the fork release
// v1.75.1.1, becomes build metadata: 1.75.1+gda.1.
func SemVersion() string {
	v := strings.TrimPrefix(Version, "v")
	core, pre, hasPre := strings.Cut(v, "-")
	parts := strings.Split(core, ".")
	if len(parts) != 4 {
		return v
	}
	v = strings.Join(parts[:3], ".")
	if hasPre {
		v += "-" + pre
	}
	return v + "+gda." + parts[3]
}
