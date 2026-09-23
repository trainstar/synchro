package main

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"testing"

	synchroapi "github.com/trainstar/synchro/api/go"
)

func TestParseProjectionBootstrapArguments(t *testing.T) {
	t.Parallel()

	generation, err := parseProjectionBootstrapArguments([]string{"--registry-generation", "42"})
	if err != nil {
		t.Fatalf("parseProjectionBootstrapArguments() error = %v", err)
	}
	if generation != 42 {
		t.Fatalf("parseProjectionBootstrapArguments() = %d, want 42", generation)
	}
	for _, arguments := range [][]string{
		nil,
		{"--registry-generation"},
		{"--registry-generation", "0"},
		{"--registry-generation", "-1"},
		{"--registry-generation", "1", "extra"},
		{"--database-url", "secret"},
	} {
		if _, err := parseProjectionBootstrapArguments(arguments); err == nil {
			t.Fatalf("parseProjectionBootstrapArguments(%q) error = nil", arguments)
		}
	}
}

func TestDatabasePoolLimitsAreFiniteAndObservable(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		limits databasePoolLimits
	}{
		{name: "server", limits: serverDatabasePoolLimits},
		{name: "operator", limits: operatorDatabasePoolLimits},
		{name: "worker", limits: workerDatabasePoolLimits},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.limits.maxOpenConns <= 0 || test.limits.maxIdleConns < 0 || test.limits.maxIdleConns > test.limits.maxOpenConns {
				t.Fatalf("invalid pool limits: %#v", test.limits)
			}

			db, err := sql.Open("pgx", "")
			if err != nil {
				t.Fatalf("open database: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
			applyDatabasePoolLimits(db, test.limits)

			if got := db.Stats().MaxOpenConnections; got != test.limits.maxOpenConns {
				t.Fatalf("max open connections = %d, want %d", got, test.limits.maxOpenConns)
			}
		})
	}
}

func TestRunWithContextRejectsUnknownCommand(t *testing.T) {
	t.Parallel()

	if err := runWithContext(context.Background(), []string{"unknown"}, io.Discard); err == nil {
		t.Fatal("runWithContext() error = nil")
	}
}

func TestNewHTTPServerSetsWholeRequestTimeout(t *testing.T) {
	t.Parallel()

	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server := newHTTPServer(":1234", handler)
	if server.ReadHeaderTimeout != serverReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", server.ReadHeaderTimeout, serverReadHeaderTimeout)
	}
	if server.ReadTimeout != serverReadTimeout {
		t.Fatalf("ReadTimeout = %s, want %s", server.ReadTimeout, serverReadTimeout)
	}
	if server.ReadTimeout <= 0 {
		t.Fatal("ReadTimeout must be finite and positive")
	}
	if server.WriteTimeout != serverWriteTimeout || server.WriteTimeout <= 0 {
		t.Fatalf("WriteTimeout = %s, want finite %s", server.WriteTimeout, serverWriteTimeout)
	}
	if synchroapi.DefaultDatabaseQueryTimeout <= 0 || synchroapi.DefaultDatabaseQueryTimeout >= server.WriteTimeout {
		t.Fatalf(
			"DefaultDatabaseQueryTimeout = %s, want positive value below WriteTimeout %s",
			synchroapi.DefaultDatabaseQueryTimeout,
			server.WriteTimeout,
		)
	}
	if server.IdleTimeout != serverIdleTimeout || server.IdleTimeout <= 0 {
		t.Fatalf("IdleTimeout = %s, want finite %s", server.IdleTimeout, serverIdleTimeout)
	}
}
