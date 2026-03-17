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
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/trainstar/synchro"
	"github.com/trainstar/synchro/handler"
	"github.com/trainstar/synchro/migrate"
	"github.com/trainstar/synchro/synctest"
	"github.com/trainstar/synchro/wal"
)

type config struct {
	DatabaseURL      string
	ReplicationURL   string
	ListenAddr       string
	SlotName         string
	PublicationName  string
	MinClientVersion string
	LogLevel         string
	JWTSecret        string
	JWKSURL          string
	JWTUserClaim     string
}

func main() {
	if err := mainRun(); err != nil {
		fmt.Fprintf(os.Stderr, "synchrod: %v\n", err)
		os.Exit(1)
	}
}

func mainRun() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config{
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		ReplicationURL:   os.Getenv("REPLICATION_URL"),
		ListenAddr:       envOr("LISTEN_ADDR", ":8080"),
		SlotName:         envOr("SLOT_NAME", "synchro_slot"),
		PublicationName:  envOr("PUBLICATION_NAME", "synchro_pub"),
		MinClientVersion: os.Getenv("MIN_CLIENT_VERSION"),
		LogLevel:         envOr("LOG_LEVEL", "info"),
		JWTSecret:        os.Getenv("JWT_SECRET"),
		JWKSURL:          os.Getenv("JWKS_URL"),
		JWTUserClaim:     envOr("JWT_USER_CLAIM", "sub"),
	}

	return run(ctx, &cfg)
}

func run(ctx context.Context, cfg *config) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(cfg.LogLevel),
	}))

	if cfg.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging database: %w", err)
	}

	for _, stmt := range migrate.Migrations() {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("running migration: %w", err)
		}
	}

	if err := synctest.SetupTestSchema(ctx, db); err != nil {
		return fmt.Errorf("setting up test schema: %w", err)
	}

	registry := synctest.NewTestRegistry()

	engine, err := synchro.NewEngine(ctx, &synchro.Config{
		DB:               db,
		Registry:         registry,
		MinClientVersion: cfg.MinClientVersion,
		Logger:           logger,
	})
	if err != nil {
		return fmt.Errorf("creating engine: %w", err)
	}

	// Start WAL consumer if ReplicationURL is set.
	if cfg.ReplicationURL != "" {
		// Ensure publication exists (idempotent).
		tableNames := registry.TableNames()
		pubSQL := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
			cfg.PublicationName, strings.Join(tableNames, ", "))
		if _, err := db.ExecContext(ctx, pubSQL); err != nil {
			// Ignore "already exists" errors.
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("creating publication: %w", err)
			}
		}

		consumer := wal.NewConsumer(&wal.ConsumerConfig{
			ConnString:      cfg.ReplicationURL,
			SlotName:        cfg.SlotName,
			PublicationName: cfg.PublicationName,
			Registry:        registry,
			Assigner:        synchro.NewJoinResolverWithDB(registry, db),
			ChangelogDB:     db,
			Logger:          logger,
		})
		go func() {
			if err := consumer.Start(ctx); err != nil && ctx.Err() == nil {
				logger.ErrorContext(ctx, "WAL consumer failed", "err", err)
			}
		}()
		logger.InfoContext(ctx, "WAL consumer started")
	}

	h := handler.New(engine)

	var authMiddleware func(http.Handler) http.Handler
	switch {
	case cfg.JWTSecret != "":
		authMiddleware = func(next http.Handler) http.Handler {
			return handler.JWTAuthMiddleware(handler.JWTAuthConfig{
				Secret:    []byte(cfg.JWTSecret),
				UserClaim: cfg.JWTUserClaim,
			}, next)
		}
	case cfg.JWKSURL != "":
		authMiddleware = func(next http.Handler) http.Handler {
			return handler.JWTAuthMiddleware(handler.JWTAuthConfig{
				JWKSURL:   cfg.JWKSURL,
				UserClaim: cfg.JWTUserClaim,
			}, next)
		}
	default:
		authMiddleware = func(next http.Handler) http.Handler {
			return handler.UserIDMiddleware("X-User-ID", next)
		}
	}

	syncHandler := authMiddleware(
		handler.VersionCheckMiddleware("X-App-Version", cfg.MinClientVersion, h.Routes()))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/sync/", syncHandler)

	srv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.InfoContext(ctx, "HTTP server starting", "addr", cfg.ListenAddr)
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
