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
		JWTUserClaim:     os.Getenv("JWT_USER_CLAIM"),
		MinClientVersion: os.Getenv("MIN_CLIENT_VERSION"),
	})

	listenAddr := envOr("LISTEN_ADDR", ":8080")
	server := &http.Server{
		Addr:              listenAddr,
		Handler:           syncHandler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", listenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		logger.Info("server stopped")
		return nil
	case err := <-errCh:
		return fmt.Errorf("serve: %w", err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func parseLogLevel(value string) slog.Level {
	switch value {
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
