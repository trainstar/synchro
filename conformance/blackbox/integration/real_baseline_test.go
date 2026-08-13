package integration

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/blackbox/baseline"
	"github.com/trainstar/synchro/conformance/observer"
)

var (
	provision = flag.Bool("provision", false, "provision the isolated PostgreSQL diagnostic harness")
	install   = flag.Bool("install", false, "install the verified extension bundle")
)

func TestRealHTTPHarness(t *testing.T) {
	if !*provision || !*install {
		t.Fatal("TestRealHTTPHarness requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load real harness environment: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{Environment: environment})
	if err != nil {
		t.Fatalf("provision real harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close real harness: %v", err)
		}
	})
	if harness.RestartCount() < 1 {
		t.Fatal("real harness did not restart PostgreSQL after extension installation")
	}
	observerDatabase, err := harness.OpenObserver(ctx)
	if err != nil {
		t.Fatalf("open observer connection: %v", err)
	}
	t.Cleanup(func() {
		if err := observerDatabase.Close(); err != nil {
			t.Errorf("close observer connection: %v", err)
		}
	})
	verifyRealObserverBoundary(t, ctx, observerDatabase, environment.Observer.Username)
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		t.Fatalf("sign diagnostic token: %v", err)
	}
	output, err := baseline.NewOutputPath(filepath.Join(t.TempDir(), "baseline-real"))
	if err != nil {
		t.Fatalf("create real baseline output: %v", err)
	}
	runner, err := baseline.NewRunner(baseline.RunnerConfig{
		BaseURL:     harness.AdapterURL(),
		HTTPClient:  &http.Client{Timeout: 30 * time.Second},
		BearerToken: token,
		Source:      harness.Source(),
		Operator:    harness.Operator(),
		Output:      output,
	})
	if err != nil {
		t.Fatalf("create real baseline runner: %v", err)
	}
	report, err := runner.Run(ctx)
	if err != nil {
		t.Fatalf("run real baseline probes: %v", err)
	}
	if err := report.Validate(); err != nil {
		t.Fatalf("validate real baseline report: %v", err)
	}
	if report.Format() != "baseline-report-v1" || report.Classification() != "non_release_diagnostic" || len(report.Probes()) != 10 {
		t.Fatal("real baseline report has invalid release isolation")
	}
}

func verifyRealObserverBoundary(t *testing.T, ctx context.Context, database *sql.DB, expectedRole string) {
	t.Helper()
	var currentRole string
	if err := database.QueryRowContext(ctx, "SELECT current_user").Scan(&currentRole); err != nil {
		t.Fatalf("read observer role identity: %v", err)
	}
	if currentRole != expectedRole {
		t.Fatalf("observer current role = %q, want %q", currentRole, expectedRole)
	}

	postgresObserver, err := observer.NewPostgres(observer.PostgresConfig{
		DB: database,
		SourceTables: []observer.SourceTable{
			{
				Name:     "global_items",
				Relation: "public.cf_global_items",
				Columns:  []string{"id", "value", "updated_at", "deleted_at"},
				OrderBy:  []string{"id"},
			},
			{
				Name:     "items",
				Relation: "public.cf_items",
				Columns:  []string{"id", "owner_id", "value", "updated_at", "deleted_at"},
				OrderBy:  []string{"id"},
			},
		},
		MaximumRows: 100,
	})
	if err != nil {
		t.Fatalf("create real PostgreSQL observer: %v", err)
	}
	snapshot, err := postgresObserver.Snapshot(ctx, observer.SnapshotRequest{
		SourceTables: []string{"global_items", "items"},
		OperationalCatalogs: []string{
			"pg_catalog.pg_replication_slots",
			"pg_catalog.pg_publication",
			"pg_catalog.pg_stat_activity",
			"pg_catalog.pg_stat_database",
		},
	})
	if err != nil {
		t.Fatalf("capture real observer snapshot: %v", err)
	}
	if len(snapshot.SourceTables) != 2 || len(snapshot.OperationalCatalogs) != 4 || len(snapshot.Functions) != 0 {
		t.Fatalf("real observer snapshot shape is invalid: %#v", snapshot)
	}

	denied := []struct {
		name      string
		statement string
	}{
		{
			name:      "source write",
			statement: "INSERT INTO public.cf_items (id, owner_id, value) VALUES ('00000000-0000-0000-0000-000000009001', 'observer', 'denied')",
		},
		{name: "internal sync-table read", statement: "SELECT count(*) FROM public.sync_changelog"},
		{name: "temporary object creation", statement: "CREATE TEMP TABLE observer_denied_temp (id integer)"},
		{name: "unapproved function execution", statement: "SELECT public.synchro_schema_manifest()"},
	}
	for _, operation := range denied {
		operation := operation
		t.Run(operation.name, func(t *testing.T) {
			requireDatabasePermissionDenied(t, ctx, database, operation.statement)
		})
	}
}

func requireDatabasePermissionDenied(t *testing.T, ctx context.Context, database *sql.DB, statement string) {
	t.Helper()
	_, err := database.ExecContext(ctx, statement)
	if err == nil {
		t.Fatal("observer database operation succeeded")
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("observer database operation error = %v, want PostgreSQL insufficient_privilege", err)
	}
}
