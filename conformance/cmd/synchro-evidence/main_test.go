package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRunRejectsUnknownAndIncompleteCommands(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"inventory"},
		{"validate", "--repo-root", "../..", "--unknown", "value"},
		{"generate", "--repo-root", "../.."},
		{"coverage", "--repo-root", "../.."},
	} {
		if err := run(context.Background(), args); err == nil {
			t.Fatalf("run(%v) unexpectedly passed", args)
		}
	}
}

func TestRunHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, []string{"validate"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("run() error = %v, want canceled", err)
	}
}

func TestPublishReplacesOutputAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "summary.json")
	if err := publish(path, []byte("first\n")); err != nil {
		t.Fatalf("publish first output: %v", err)
	}
	if err := publish(path, []byte("second\n")); err != nil {
		t.Fatalf("publish second output: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if string(data) != "second\n" {
		t.Fatalf("output = %q", data)
	}
}
