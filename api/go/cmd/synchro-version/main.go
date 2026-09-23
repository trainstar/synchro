package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/trainstar/synchro/api/go/internal/releaseversion"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	root, err := releaseversion.FindRepoRoot(".")
	if err != nil {
		return err
	}
	if len(args) < 1 {
		usage(stderr)
		return fmt.Errorf("missing command")
	}
	switch args[0] {
	case "print":
		version, err := releaseversion.ReadVersion(root)
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, version)
		return nil
	case "check":
		fs := flag.NewFlagSet("check", flag.ContinueOnError)
		fs.SetOutput(stderr)
		expectedTag := fs.String("expected-tag", "", "expected release tag in vX.Y.Z form")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		return releaseversion.Check(root, *expectedTag)
	case "sync":
		return releaseversion.Sync(root)
	case "set":
		if len(args) != 2 {
			usage(stderr)
			return fmt.Errorf("usage: synchro-version set X.Y.Z")
		}
		return releaseversion.Set(root, args[1])
	default:
		usage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: synchro-version <print|check|sync|set>")
}
