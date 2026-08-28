// synchro-local-postgres exposes the existing black-box PostgreSQL provisioner
// to the Make local target and the black-box harness workflow.
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/trainstar/synchro/conformance/blackbox"
)

const (
	localStartupTimeout  = 90 * time.Second
	localShutdownTimeout = 15 * time.Second
	localPollInterval    = 250 * time.Millisecond
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "synchro-local-postgres: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if len(args) == 0 {
		return errors.New("command is required")
	}
	switch args[0] {
	case "start":
		return runStart(ctx, args[1:])
	case "prepare":
		return runPrepare(ctx, args[1:])
	default:
		return errors.New("unknown command")
	}
}

func runStart(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("start", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	pg18BinDir := flags.String("pg18-bin-dir", "", "PostgreSQL 18 binary directory")
	extensionArtifact := flags.String("extension-artifact", "", "verified PostgreSQL extension bundle")
	adapterArtifact := flags.String("adapter-artifact", "", "verified adapter artifact")
	stateDir := flags.String("state-dir", "", "private state directory")
	tempParent := flags.String("temp-parent", "", "private temporary directory parent")
	urlFile := flags.String("url-file", "", "administrator URL output file")
	attachEnvironmentFile := flags.String("attach-environment-file", "", "attach-mode environment output file")
	listen := flags.String("listen", "127.0.0.1", "PostgreSQL listen address")
	if err := flags.Parse(args); err != nil {
		return errors.New("start flags are invalid")
	}
	if flags.NArg() != 0 || *pg18BinDir == "" || *extensionArtifact == "" || *adapterArtifact == "" || *stateDir == "" || *tempParent == "" || *urlFile == "" || *attachEnvironmentFile == "" {
		return errors.New("start requires --pg18-bin-dir, --extension-artifact, --adapter-artifact, --state-dir, --temp-parent, --url-file, and --attach-environment-file")
	}
	if err := ensurePrivateDirectory(*stateDir); err != nil {
		return err
	}
	credentials, err := createCredentials(*stateDir)
	if err != nil {
		return err
	}
	restoreEnvironment := setEnvironment(map[string]string{
		"SYNCHRO_CONFORMANCE_PG18_BINDIR":            *pg18BinDir,
		"SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT":     *extensionArtifact,
		"SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT":       *adapterArtifact,
		"SYNCHRO_CONFORMANCE_ADMIN_USER":             credentials.adminUser,
		"SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE":    credentials.adminPassword,
		"SYNCHRO_CONFORMANCE_ADAPTER_USER":           credentials.adapterUser,
		"SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE":  credentials.adapterPassword,
		"SYNCHRO_CONFORMANCE_OBSERVER_USER":          credentials.observerUser,
		"SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE": credentials.observerPassword,
		"SYNCHRO_CONFORMANCE_WORKER_USER":            credentials.workerUser,
		"SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE":   credentials.workerPassword,
		"SYNCHRO_CONFORMANCE_OPERATOR_USER":          credentials.operatorUser,
		"SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE": credentials.operatorPassword,
		"SYNCHRO_CONFORMANCE_JWT_SECRET_FILE":        credentials.jwtSecret,
		"SYNCHRO_CONFORMANCE_INSTALL_LOCK":           filepath.Join(*stateDir, "install.lock"),
	})
	defer restoreEnvironment()
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		return fmt.Errorf("load local provisioner environment: %w", err)
	}
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{
		Environment:     environment,
		TempParent:      *tempParent,
		ListenAddress:   strings.TrimSpace(*listen),
		SkipAdapter:     true,
		StartupTimeout:  localStartupTimeout,
		ShutdownTimeout: localShutdownTimeout,
	})
	if err != nil {
		return fmt.Errorf("provision local PostgreSQL: %w", err)
	}
	url := harness.DatabaseURL()
	if url == "" {
		_ = harness.Close(context.Background())
		return errors.New("local PostgreSQL administrator URL is unavailable")
	}
	if err := writePrivateFile(*urlFile, []byte(url+"\n")); err != nil {
		_ = harness.Close(context.Background())
		return fmt.Errorf("write local PostgreSQL URL: %w", err)
	}
	if err := writePrivateFile(*attachEnvironmentFile, []byte(attachEnvironment(url, credentials))); err != nil {
		_ = harness.Close(context.Background())
		return fmt.Errorf("write attach environment: %w", err)
	}
	<-ctx.Done()
	closeContext, cancel := context.WithTimeout(context.Background(), localShutdownTimeout)
	defer cancel()
	if err := harness.Close(closeContext); err != nil {
		return fmt.Errorf("close local PostgreSQL: %w", err)
	}
	credentials.remove()
	_ = os.Remove(*attachEnvironmentFile)
	return nil
}

// attachEnvironment references credential files through SYNCHRO_ATTACH_DIR,
// so a copied attach bundle works from any consumer directory.
func attachEnvironment(url string, credentials localCredentials) string {
	return strings.Join([]string{
		environmentAssignment("SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL", url),
		environmentAssignment("SYNCHRO_CONFORMANCE_ADMIN_USER", credentials.adminUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE", credentials.adminPassword),
		environmentAssignment("SYNCHRO_CONFORMANCE_ADAPTER_USER", credentials.adapterUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE", credentials.adapterPassword),
		environmentAssignment("SYNCHRO_CONFORMANCE_OBSERVER_USER", credentials.observerUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE", credentials.observerPassword),
		environmentAssignment("SYNCHRO_CONFORMANCE_WORKER_USER", credentials.workerUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE", credentials.workerPassword),
		environmentAssignment("SYNCHRO_CONFORMANCE_OPERATOR_USER", credentials.operatorUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE", credentials.operatorPassword),
		attachDirAssignment("SYNCHRO_CONFORMANCE_JWT_SECRET_FILE", credentials.jwtSecret),
		"",
	}, "\n")
}

func attachDirAssignment(name, path string) string {
	return name + "=\"${SYNCHRO_ATTACH_DIR}/" + filepath.Base(path) + "\""
}

func environmentAssignment(name, value string) string {
	return name + "='" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

type localCredentials struct {
	adminUser, adminPassword       string
	adapterUser, adapterPassword   string
	observerUser, observerPassword string
	workerUser, workerPassword     string
	operatorUser, operatorPassword string
	jwtSecret                      string
	paths                          []string
}

func createCredentials(stateDir string) (localCredentials, error) {
	credentials := localCredentials{
		adminUser:    "synchro_local_admin",
		adapterUser:  "synchro_local_adapter",
		observerUser: "synchro_local_observer",
		workerUser:   "synchro_local_worker",
		operatorUser: "synchro_local_operator",
	}
	values := []struct {
		path *string
		name string
	}{
		{&credentials.adminPassword, "admin-password"},
		{&credentials.adapterPassword, "adapter-password"},
		{&credentials.observerPassword, "observer-password"},
		{&credentials.workerPassword, "worker-password"},
		{&credentials.operatorPassword, "operator-password"},
		{&credentials.jwtSecret, "jwt-secret"},
	}
	for _, value := range values {
		data := make([]byte, 32)
		if _, err := rand.Read(data); err != nil {
			credentials.remove()
			return localCredentials{}, errors.New("generate local provisioner credential failed")
		}
		path := filepath.Join(stateDir, value.name)
		if err := writePrivateFile(path, []byte(hex.EncodeToString(data))); err != nil {
			credentials.remove()
			return localCredentials{}, errors.New("write local provisioner credential failed")
		}
		*value.path = path
		credentials.paths = append(credentials.paths, path)
	}
	return credentials, nil
}

func (credentials localCredentials) remove() {
	for _, path := range credentials.paths {
		_ = os.Remove(path)
	}
}

func setEnvironment(values map[string]string) func() {
	original := make(map[string]string, len(values))
	present := make(map[string]bool, len(values))
	for key, value := range values {
		original[key], present[key] = os.LookupEnv(key)
		_ = os.Setenv(key, value)
	}
	return func() {
		for key := range values {
			if present[key] {
				_ = os.Setenv(key, original[key])
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}
}

func runPrepare(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("prepare", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	repoRoot := flags.String("repo-root", "", "repository root")
	databaseURL := flags.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection string")
	if err := flags.Parse(args); err != nil {
		return errors.New("prepare flags are invalid")
	}
	if flags.NArg() != 0 || *repoRoot == "" || strings.TrimSpace(*databaseURL) == "" {
		return errors.New("prepare requires --repo-root and DATABASE_URL")
	}
	connectionString := strings.TrimSpace(*databaseURL)
	root, err := filepath.Abs(*repoRoot)
	if err != nil {
		return errors.New("repository root is invalid")
	}
	database, err := sql.Open("pgx", connectionString)
	if err != nil {
		return errors.New("open client integration database failed")
	}
	defer database.Close()
	if err := database.PingContext(ctx); err != nil {
		return errors.New("ping client integration database failed")
	}
	if _, err := database.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS synchro_pg CASCADE"); err != nil {
		return errors.New("ensure synchro_pg extension failed")
	}
	for _, script := range []string{"schema.sql", "register.sql"} {
		if err := executeScript(ctx, database, filepath.Join(root, "extensions", "testdata", script)); err != nil {
			return fmt.Errorf("apply client integration %s: %w", script, err)
		}
	}
	if err := waitFor(ctx, database, func(ctx context.Context, database *sql.DB) (bool, error) {
		var ready bool
		err := database.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1
			FROM synchro.sync_registry_generations generation
			WHERE generation.state = 'active'
			  AND (SELECT count(*)
			       FROM synchro.sync_registry registry
			       WHERE registry.registry_generation = generation.generation
			         AND registry.physical_schema = 'public'
			         AND registry.table_name = ANY(ARRAY[
			             'regions', 'nations', 'suppliers', 'parts', 'part_suppliers',
			             'categories', 'customers', 'orders', 'line_items', 'documents',
			             'document_members', 'document_comments', 'type_zoo'
			         ])) = 13
			  AND EXISTS (SELECT 1 FROM synchro.sync_registry registry WHERE registry.registry_generation = generation.generation AND registry.table_name = 'line_items' AND registry.membership_function_name = 'test_line_items_membership')
			  AND EXISTS (SELECT 1 FROM synchro.sync_registry registry WHERE registry.registry_generation = generation.generation AND registry.table_name = 'document_comments' AND registry.membership_function_name = 'test_document_comments_membership')
		)`).Scan(&ready)
		return ready, err
	}); err != nil {
		return errors.New("client integration registry did not activate")
	}
	if err := executeScript(ctx, database, filepath.Join(root, "extensions", "testdata", "canonical-seed.sql")); err != nil {
		return fmt.Errorf("apply client integration canonical seed: %w", err)
	}
	if err := waitFor(ctx, database, func(ctx context.Context, database *sql.DB) (bool, error) {
		var count int
		err := database.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_bucket_edges").Scan(&count)
		return count >= 6, err
	}); err != nil {
		return errors.New("client integration seed rows did not materialize")
	}
	if _, err := database.ExecContext(ctx, "SELECT synchro.synchro_backfill_bucket_edges()"); err != nil {
		return errors.New("backfill client integration scope edges failed")
	}
	return nil
}

func executeScript(ctx context.Context, database *sql.DB, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, err = database.ExecContext(ctx, string(data))
	return err
}

func waitFor(parent context.Context, database *sql.DB, condition func(context.Context, *sql.DB) (bool, error)) error {
	ctx, cancel := context.WithTimeout(parent, localStartupTimeout)
	defer cancel()
	for {
		ready, err := condition(ctx, database)
		if err == nil && ready {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		timer := time.NewTimer(localPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return errors.New("create local provisioner state directory failed")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("local provisioner state directory must be private")
	}
	return nil
}

func writePrivateFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
