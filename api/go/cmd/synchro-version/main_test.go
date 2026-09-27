package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/trainstar/synchro/api/go/internal/releaseversion"
)

func TestRunPrintsRepositoryVersion(t *testing.T) {
	root, err := releaseversion.FindRepoRoot(".")
	if err != nil {
		t.Fatalf("find repository root: %v", err)
	}
	version, err := releaseversion.ReadVersion(root)
	if err != nil {
		t.Fatalf("read repository version: %v", err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"print"}, &stdout, &stderr); err != nil {
		t.Fatalf("run print: %v", err)
	}
	if got := strings.TrimSpace(stdout.String()); got != version {
		t.Fatalf("print wrote %q, repository version is %q", got, version)
	}
}

func TestRunRejectsMissingAndUnknownCommands(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(nil, &stdout, &stderr); err == nil {
		t.Fatal("missing command must fail")
	}
	if !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("missing command must print usage, stderr was %q", stderr.String())
	}
	stderr.Reset()
	if err := run([]string{"bogus"}, &stdout, &stderr); err == nil {
		t.Fatal("unknown command must fail")
	}
	if !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("unknown command must print usage, stderr was %q", stderr.String())
	}
}

func TestRunPrintsUpdateBaselineArtifact(t *testing.T) {
	root, err := releaseversion.FindRepoRoot(".")
	if err != nil {
		t.Fatalf("find repository root: %v", err)
	}
	baseline, err := releaseversion.ReadUpdateBaseline(root)
	if err != nil {
		t.Fatalf("read update baseline: %v", err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"update-baseline-artifact"}, &stdout, &stderr); err != nil {
		t.Fatalf("run update-baseline-artifact: %v", err)
	}
	if want := baseline.ArtifactURL + " " + baseline.ArtifactSHA256 + "\n"; stdout.String() != want {
		t.Fatalf("update-baseline-artifact wrote %q, want %q", stdout.String(), want)
	}
	stdout.Reset()
	if err := run([]string{"update-baseline-artifact", "extra"}, &stdout, &stderr); err == nil {
		t.Fatal("update-baseline-artifact with an argument must fail")
	}
	if stdout.Len() != 0 {
		t.Fatalf("update-baseline-artifact with an argument wrote %q", stdout.String())
	}
}
