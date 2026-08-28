package main

import (
	"bytes"
	"strings"
	"testing"
)

func emptyEnv(string) string { return "" }

func TestRunRequiresDatabaseURLAndOutputBeforeAnyConnection(t *testing.T) {
	var stdout, stderr bytes.Buffer

	err := run(nil, emptyEnv, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "database URL is required") {
		t.Fatalf("missing database URL must fail with guidance, got %v", err)
	}

	err = run([]string{"--database-url", "postgres://unused.invalid/db"}, emptyEnv, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "output path is required") {
		t.Fatalf("missing output path must fail before any connection, got %v", err)
	}
}

func TestRunReadsDatabaseURLFromEnvironment(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := func(key string) string {
		if key == "DATABASE_URL" {
			return "postgres://unused.invalid/db"
		}
		return ""
	}

	err := run(nil, env, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "output path is required") {
		t.Fatalf("environment database URL must satisfy the URL requirement, got %v", err)
	}
}

func TestRunRejectsUnknownFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"--bogus"}, emptyEnv, &stdout, &stderr); err == nil {
		t.Fatal("unknown flag must fail")
	}
}
