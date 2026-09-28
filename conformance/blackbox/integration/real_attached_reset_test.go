package integration

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealAttachedDatabaseResetRestoresFreshState(t *testing.T) {
	if !*provision || !*install {
		t.Fatal("TestRealAttachedDatabaseResetRestoresFreshState requires --provision --install")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	environment := startRealAttachedPostgres(t, ctx)
	retainedID := "00000000-0000-0000-0000-000000009301"
	capturedID := "00000000-0000-0000-0000-000000009302"

	first := provisionRealAttachedHarness(t, ctx, environment)
	insertRealDiagnosticItem(t, ctx, first, retainedID)
	waitForRealWALRecord(t, ctx, first, retainedID)
	closeRealAttachedHarness(t, first)

	// Negative control: without the reset, the next harness inherits the prior state.
	reused := provisionRealAttachedHarness(t, ctx, environment)
	if count := countRealWALRecords(t, ctx, reused, retainedID); count != 1 {
		t.Fatalf("attached reuse without reset observed %d prior WAL records, want 1", count)
	}
	closeRealAttachedHarness(t, reused)

	if err := blackbox.ResetAttachedDatabase(ctx, environment); err != nil {
		t.Fatalf("reset attached database: %v", err)
	}
	reset := provisionRealAttachedHarness(t, ctx, environment)
	if count := countRealWALRecords(t, ctx, reset, retainedID); count != 0 {
		t.Fatalf("attached reset retained %d prior WAL records, want 0", count)
	}
	insertRealDiagnosticItem(t, ctx, reset, capturedID)
	waitForRealWALRecord(t, ctx, reset, capturedID)
	closeRealAttachedHarness(t, reset)
}

// startRealAttachedPostgres runs the local provisioner, which owns the cluster
// and serves the lifecycle command that an attached harness requires.
func startRealAttachedPostgres(t *testing.T, ctx context.Context) blackbox.EnvironmentConfig {
	t.Helper()
	binary := os.Getenv("SYNCHRO_LOCAL_POSTGRES_BINARY")
	if binary == "" {
		t.Fatal("SYNCHRO_LOCAL_POSTGRES_BINARY is required")
	}
	owned, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load owned environment: %v", err)
	}
	root := t.TempDir()
	stateDir := filepath.Join(root, "state")
	tempParent := filepath.Join(root, "tmp")
	if err := os.Mkdir(tempParent, 0o700); err != nil {
		t.Fatal(err)
	}
	attachFile := filepath.Join(stateDir, "attach.env")
	command := exec.Command(binary, "start",
		"--pg18-bin-dir", owned.PG18BinDir,
		"--extension-artifact", owned.ExtensionArtifact,
		"--adapter-artifact", owned.AdapterArtifact,
		"--state-dir", stateDir,
		"--temp-parent", tempParent,
		"--url-file", filepath.Join(stateDir, "postgres.url"),
		"--attach-environment-file", attachFile,
		"--listen", "127.0.0.1",
	)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatalf("start local provisioner: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-exited:
			if err != nil {
				t.Errorf("local provisioner stop failed: %v\n%s", err, output.String())
			}
		case <-time.After(2 * time.Minute):
			_ = command.Process.Kill()
			<-exited
			t.Error("local provisioner did not stop")
		}
	})
	for {
		if info, err := os.Stat(attachFile); err == nil && info.Size() > 0 {
			break
		}
		select {
		case err := <-exited:
			exited <- err
			t.Fatalf("local provisioner exited before it was ready: %v\n%s", err, output.String())
		case <-ctx.Done():
			t.Fatalf("local provisioner did not become ready: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	for key, value := range loadRealAttachEnvironment(t, attachFile) {
		t.Setenv(key, value)
	}
	environment, err := blackbox.LoadLocalEnvironment()
	if err != nil {
		t.Fatalf("load attached environment: %v", err)
	}
	if environment.AttachDatabaseURL == "" || environment.AttachDestroyOnClose {
		t.Fatalf("attached environment is invalid: url=%t destroy_on_close=%t", environment.AttachDatabaseURL != "", environment.AttachDestroyOnClose)
	}
	return environment
}

// loadRealAttachEnvironment evaluates the file with a shell, as CI does, so the
// test does not depend on the file's quoting rules.
func loadRealAttachEnvironment(t *testing.T, path string) map[string]string {
	t.Helper()
	command := exec.Command("/bin/sh", "-c", `set -a; . "$1"; set +a; env`, "sh", path)
	command.Env = append(os.Environ(), "SYNCHRO_ATTACH_DIR="+filepath.Dir(path))
	output, err := command.Output()
	if err != nil {
		t.Fatalf("evaluate attach environment: %v", err)
	}
	values := make(map[string]string)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), "=")
		if ok && strings.HasPrefix(key, "SYNCHRO_CONFORMANCE_") {
			values[key] = value
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if values["SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL"] == "" {
		t.Fatal("attach environment has no database URL")
	}
	return values
}

func provisionRealAttachedHarness(t *testing.T, ctx context.Context, environment blackbox.EnvironmentConfig) *blackbox.Harness {
	t.Helper()
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{Environment: environment})
	if err != nil {
		t.Fatalf("provision attached harness: %v", err)
	}
	t.Cleanup(func() { closeRealAttachedHarness(t, harness) })
	return harness
}

func closeRealAttachedHarness(t *testing.T, harness *blackbox.Harness) {
	t.Helper()
	closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := harness.Close(closeContext); err != nil {
		t.Errorf("close attached harness: %v", err)
	}
}

func insertRealDiagnosticItem(t *testing.T, ctx context.Context, harness *blackbox.Harness, recordID string) {
	t.Helper()
	if err := harness.Source().ExecContext(ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID, "diagnostic-user", "attached-reset",
	); err != nil {
		t.Fatalf("insert diagnostic item: %v", err)
	}
}

func countRealWALRecords(t *testing.T, ctx context.Context, harness *blackbox.Harness, recordID string) int {
	t.Helper()
	observation, err := harness.Operator().ObserveWALRecords(ctx, []string{recordID})
	if err != nil {
		t.Fatalf("observe WAL records: %v", err)
	}
	return len(observation.Records)
}
