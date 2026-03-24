package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/trainstar/synchro/api/go/internal/releaseversion"
)

func main() {
	root, err := releaseversion.FindRepoRoot(".")
	if err != nil {
		fatal(err)
	}

	if len(os.Args) < 2 {
		usage()
		fatal(fmt.Errorf("missing command"))
	}

	switch os.Args[1] {
	case "print":
		version, err := releaseversion.ReadVersion(root)
		if err != nil {
			fatal(err)
		}
		fmt.Println(version)
	case "check":
		fs := flag.NewFlagSet("check", flag.ExitOnError)
		expectedTag := fs.String("expected-tag", "", "expected release tag in vX.Y.Z form")
		_ = fs.Parse(os.Args[2:])
		if err := releaseversion.Check(root, *expectedTag); err != nil {
			fatal(err)
		}
	case "sync":
		if err := releaseversion.Sync(root); err != nil {
			fatal(err)
		}
	case "set":
		if len(os.Args) != 3 {
			usage()
			fatal(fmt.Errorf("usage: synchro-version set X.Y.Z"))
		}
		if err := releaseversion.Set(root, os.Args[2]); err != nil {
			fatal(err)
		}
	default:
		usage()
		fatal(fmt.Errorf("unknown command %q", os.Args[1]))
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: synchro-version <print|check|sync|set>")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
