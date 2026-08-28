package main

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunArgumentValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"missing command", nil, "command is required"},
		{"unknown command", []string{"stop"}, "unknown command"},
		{"start missing required flags", []string{"start"}, "start requires"},
		{"start invalid flag", []string{"start", "--not-a-flag"}, "start flags are invalid"},
		{"prepare missing root", []string{"prepare", "--database-url", "postgres://example"}, "prepare requires"},
		{"prepare blank URL", []string{"prepare", "--repo-root", ".", "--database-url", "  "}, "prepare requires"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("run(%v) error = %v, want message containing %q", tt.args, err, tt.want)
			}
		})
	}
	if err := run(nil, []string{"start"}); err == nil || err.Error() != "context is required" {
		t.Fatalf("run with nil context error = %v", err)
	}
}

func TestEnvironmentAssignmentQuotesShellValues(t *testing.T) {
	got := environmentAssignment("SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL", "host='127.0.0.1' port=5432 password='secret'")
	want := "SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL='host='\"'\"'127.0.0.1'\"'\"' port=5432 password='\"'\"'secret'\"'\"''"
	if got != want {
		t.Fatalf("environmentAssignment() = %q, want %q", got, want)
	}
}

func TestPrivateStateAndCredentialFiles(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	if err := ensurePrivateDirectory(state); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(state)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("state directory mode = %v, want 0700", info.Mode().Perm())
	}
	credentials, err := createCredentials(state)
	if err != nil {
		t.Fatal(err)
	}
	if len(credentials.paths) != 6 {
		t.Fatalf("credential file count = %d, want 6", len(credentials.paths))
	}
	for _, path := range credentials.paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("credential %q mode = %v, want 0600", path, info.Mode().Perm())
		}
		contents, err := os.ReadFile(path)
		if err != nil || len(contents) != 64 {
			t.Fatalf("credential %q contents are invalid", path)
		}
		if _, err := hex.DecodeString(string(contents)); err != nil {
			t.Fatalf("credential %q is not hexadecimal: %v", path, err)
		}
	}
	credentials.remove()
	for _, path := range credentials.paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("credential %q still exists, stat error %v", path, err)
		}
	}
	if err := ensurePrivateDirectory(filepath.Join(dir, "missing", "..", "state2")); err != nil {
		t.Fatal(err)
	}
}

func TestWritePrivateFileRejectsMissingParent(t *testing.T) {
	err := writePrivateFile(filepath.Join(t.TempDir(), "missing", "value"), []byte("x"))
	if err == nil {
		t.Fatal("writePrivateFile accepted a missing parent")
	}
}
