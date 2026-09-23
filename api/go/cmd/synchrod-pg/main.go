// synchrod-pg is the sync HTTP server backed by the synchro_pg PostgreSQL
// extension. It is a thin adapter: JWT auth, version check, and passthrough
// to SELECT synchro.synchro_*() functions. No Go-side sync engine, no WAL consumer,
// no migrations. The extension handles everything.
//
// Required: PostgreSQL with synchro_pg extension installed.
//
// Environment variables:
//
//	DATABASE_URL         PostgreSQL connection string (required)
//	WORKER_DATABASE_URL  Worker connection string (required for projection-bootstrap)
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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	synchroapi "github.com/trainstar/synchro/api/go"
	"github.com/trainstar/synchro/api/go/operator"
)

const (
	serverReadHeaderTimeout = 10 * time.Second
	serverReadTimeout       = 30 * time.Second
	serverWriteTimeout      = 30 * time.Second
	serverIdleTimeout       = 60 * time.Second
)

type databasePoolLimits struct {
	maxOpenConns int
	maxIdleConns int
}

var (
	serverDatabasePoolLimits   = databasePoolLimits{maxOpenConns: 16, maxIdleConns: 8}
	operatorDatabasePoolLimits = databasePoolLimits{maxOpenConns: 4, maxIdleConns: 2}
	workerDatabasePoolLimits   = databasePoolLimits{maxOpenConns: 2, maxIdleConns: 2}
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
	return runWithContext(ctx, os.Args[1:], os.Stdout)
}

func runWithContext(ctx context.Context, arguments []string, stdout io.Writer) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if len(arguments) == 0 {
		return runHTTPServer(ctx)
	}
	if arguments[0] != "projection-bootstrap" {
		return errors.New("unknown command")
	}
	registryGeneration, err := parseProjectionBootstrapArguments(arguments[1:])
	if err != nil {
		return err
	}
	return runProjectionBootstrap(ctx, registryGeneration, stdout)
}

func runHTTPServer(ctx context.Context) error {

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
	applyDatabasePoolLimits(db, serverDatabasePoolLimits)
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging database: %w", err)
	}

	if err := synchroapi.RequireCompatibleExtension(ctx, db); err != nil {
		return fmt.Errorf("verifying synchro_pg compatibility: %w", err)
	}

	jwksContext, cancelJWKS := context.WithCancel(ctx)
	defer cancelJWKS()
	syncHandler := synchroapi.Routes(synchroapi.Config{
		DB:               db,
		JWTSecret:        []byte(os.Getenv("JWT_SECRET")),
		JWKSURL:          os.Getenv("JWKS_URL"),
		JWKSContext:      jwksContext,
		JWTUserClaim:     os.Getenv("JWT_USER_CLAIM"),
		MinClientVersion: os.Getenv("MIN_CLIENT_VERSION"),
	})

	listenAddr := envOr("LISTEN_ADDR", ":8080")
	server := newHTTPServer(listenAddr, syncHandler)

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

func newHTTPServer(address string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: serverReadHeaderTimeout,
		ReadTimeout:       serverReadTimeout,
		WriteTimeout:      serverWriteTimeout,
		IdleTimeout:       serverIdleTimeout,
	}
}

func parseProjectionBootstrapArguments(arguments []string) (int64, error) {
	if len(arguments) != 2 || arguments[0] != "--registry-generation" || arguments[1] == "" {
		return 0, errors.New("projection-bootstrap arguments are invalid")
	}
	for _, character := range []byte(arguments[1]) {
		if character < '0' || character > '9' {
			return 0, errors.New("projection-bootstrap arguments are invalid")
		}
	}
	generation, err := strconv.ParseInt(arguments[1], 10, 64)
	if err != nil || generation <= 0 {
		return 0, errors.New("projection-bootstrap arguments are invalid")
	}
	return generation, nil
}

func runProjectionBootstrap(ctx context.Context, registryGeneration int64, stdout io.Writer) error {
	operatorURL := os.Getenv("DATABASE_URL")
	if operatorURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	workerURL := os.Getenv("WORKER_DATABASE_URL")
	if workerURL == "" {
		return errors.New("WORKER_DATABASE_URL is required")
	}
	replicationConfig, err := pgconn.ParseConfig(workerURL)
	if err != nil {
		return errors.New("worker database configuration is invalid")
	}
	if replicationConfig.RuntimeParams == nil {
		replicationConfig.RuntimeParams = make(map[string]string)
	}
	replicationConfig.RuntimeParams["replication"] = "database"

	operatorDB, err := sql.Open("pgx", operatorURL)
	if err != nil {
		return errors.New("open operator database failed")
	}
	applyDatabasePoolLimits(operatorDB, operatorDatabasePoolLimits)
	defer func() { _ = operatorDB.Close() }()
	workerDB, err := sql.Open("pgx", workerURL)
	if err != nil {
		return errors.New("open worker database failed")
	}
	applyDatabasePoolLimits(workerDB, workerDatabasePoolLimits)
	defer func() { _ = workerDB.Close() }()
	if err := operatorDB.PingContext(ctx); err != nil {
		return errors.New("ping operator database failed")
	}
	if err := workerDB.PingContext(ctx); err != nil {
		return errors.New("ping worker database failed")
	}

	coordinator, err := operator.New(operator.Config{
		OperatorDB:        operatorDB,
		WorkerDB:          workerDB,
		ReplicationConfig: replicationConfig,
	})
	if err != nil {
		return errors.New("create projection bootstrap coordinator failed")
	}
	result, err := coordinator.RunProjectionBootstrap(ctx, registryGeneration)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		return errors.New("write projection bootstrap result failed")
	}
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func applyDatabasePoolLimits(db *sql.DB, limits databasePoolLimits) {
	db.SetMaxOpenConns(limits.maxOpenConns)
	db.SetMaxIdleConns(limits.maxIdleConns)
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
