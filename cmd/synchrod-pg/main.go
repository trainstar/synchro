// synchrod-pg is the sync HTTP server backed by the synchro_pg PostgreSQL
// extension. It is a thin adapter: JWT auth, version check, and passthrough
// to SELECT synchro_*() functions. No Go-side sync engine, no WAL consumer,
// no migrations. The extension handles everything.
//
// Required: PostgreSQL with synchro_pg extension installed.
//
// Environment variables:
//
//	DATABASE_URL         PostgreSQL connection string (required)
//	LISTEN_ADDR          HTTP listen address (default: :8080)
//	JWT_SECRET           HS256 shared secret (mutually exclusive with JWKS_URL)
//	JWKS_URL             RS256/ES256 JWKS endpoint (mutually exclusive with JWT_SECRET)
//	JWT_USER_CLAIM       JWT claim for user ID (default: sub)
//	MIN_CLIENT_VERSION   Minimum client semver (optional)
//	LOG_LEVEL            Log level: debug, info, warn, error (default: info)
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	synchroapi "github.com/trainstar/synchro/api/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "synchrod-pg: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(envOr("LOG_LEVEL", "info")),
	}))

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}

	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging database: %w", err)
	}

	// Verify the extension is installed.
	var extExists bool
	err = db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'synchro_pg')").Scan(&extExists)
	if err != nil {
		return fmt.Errorf("checking extension: %w", err)
	}
	if !extExists {
		return fmt.Errorf("synchro_pg extension is not installed (run: CREATE EXTENSION synchro_pg)")
	}

	syncHandler := synchroapi.Routes(synchroapi.Config{
		DB:               db,
		JWTSecret:        []byte(os.Getenv("JWT_SECRET")),
		JWKSURL:          os.Getenv("JWKS_URL"),
		JWTUserClaim:     envOr("JWT_USER_CLAIM", "sub"),
		MinClientVersion: os.Getenv("MIN_CLIENT_VERSION"),
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/sync/", syncHandler)

	addr := envOr("LISTEN_ADDR", ":8080")
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.InfoContext(ctx, "synchrod-pg starting", "addr", addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server failed: %w", err)
		}
	case <-ctx.Done():
		logger.InfoContext(ctx, "shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("HTTP shutdown: %w", err)
		}
	}

	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
