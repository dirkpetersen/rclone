// Rclone-gda runs 'rclone gda', so 'rclone-gda backup /src remote:dst'
// is 'rclone gda backup /src remote:dst'.
package main

import (
	"os"

	_ "github.com/rclone/rclone/backend/all" // import all backends
	"github.com/rclone/rclone/cmd"
	_ "github.com/rclone/rclone/cmd/all"    // import all commands
	_ "github.com/rclone/rclone/lib/plugin" // import plugins
)

// rcloneArgs are the first arguments which are passed to rclone itself
// rather than to 'rclone gda', so remotes can be set up and the version
// shown without a separate rclone binary. Only the first argument is
// looked at, so global flags go after these commands.
var rcloneArgs = map[string]bool{
	"completion":  true,
	"config":      true,
	"listremotes": true,
	"obscure":     true,
	"version":     true,
	"--version":   true,
	"-V":          true,
}

func main() {
	os.Args = gdaArgs(os.Args)
	cmd.Main()
}

// gdaArgs returns the rclone command line for the rclone-gda command
// line args.
func gdaArgs(args []string) []string {
	if len(args) > 1 && rcloneArgs[args[1]] {
		return args
	}
	out := append([]string{args[0], "gda"}, args[1:]...)
	switch {
	case len(args) == 1:
		out = append(out, "--help")
	case args[1] == "help":
		// 'rclone gda' has no help command.
		out = append([]string{args[0], "gda"}, args[2:]...)
		out = append(out, "--help")
	}
	return out
}
