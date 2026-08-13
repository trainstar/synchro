package main

import (
	"context"
	"testing"
)

func TestCommandsFailClosed(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}, {"validate"}, {"inventory"}} {
		if err := run(context.Background(), args); err == nil {
			t.Fatalf("run(%q) succeeded", args)
		}
	}
	if err := run(context.Background(), []string{"inventory", "--repo-root", "../..", "--candidate-dir", "candidate"}); err == nil {
		t.Fatal("inventory unexpectedly succeeded")
	}
	if err := run(context.Background(), []string{"inventory", "--repo-root", "../..", "--candidate-dir", "dist/verification/candidate"}); err == nil {
		t.Fatal("inventory unexpectedly succeeded for incomplete candidate")
	}
}

func TestInventoryCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, []string{"inventory", "--repo-root", ".", "--candidate-dir", "dist/verification/candidate"}); err == nil {
		t.Fatal("canceled inventory unexpectedly succeeded")
	}
}
