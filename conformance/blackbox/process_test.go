package blackbox

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestInstallExtensionRejectsBundleIdentityChangeAfterLoad(t *testing.T) {
	for _, test := range []struct {
		name    string
		replace func(*testing.T, extensionBundle)
	}{
		{
			name: "manifest",
			replace: func(t *testing.T, bundle extensionBundle) {
				replaceFileWithSameBytes(t, filepath.Join(bundle.root, extensionBundleManifestName))
			},
		},
		{
			name: "manifest digest",
			replace: func(t *testing.T, bundle extensionBundle) {
				replaceFileWithSameBytes(t, filepath.Join(bundle.root, extensionBundleManifestName+".sha256"))
			},
		},
		{
			name: "payload",
			replace: func(t *testing.T, bundle extensionBundle) {
				replaceFileWithSameBytes(t, filepath.Join(bundle.root, filepath.FromSlash(bundle.files[0].Path)))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := writeExtensionBundleFixture(t)
			loaded, err := verifyExtensionBundle(root)
			if err != nil {
				t.Fatal(err)
			}
			test.replace(t, loaded)
			harness := &Harness{env: EnvironmentConfig{
				ExtensionArtifact: root,
				extension:         loaded,
			}}
			if err := harness.installExtension(context.Background()); err == nil {
				t.Fatal("extension installation accepted a changed bundle identity")
			}
		})
	}
}

func TestInstallVerifiedExtensionFileTracksReplacementBeforeSyncFailure(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "destination")
	if err := os.WriteFile(source, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, err := fileSHA256(source)
	if err != nil {
		t.Fatal(err)
	}
	record := installedExtensionFile{destination: destination, installedDigest: digest}
	if err := installVerifiedExtensionFile(&record, source, 0o600, func(string) error {
		return errors.New("injected directory sync failure")
	}); err == nil {
		t.Fatal("extension installation accepted a post-rename sync failure")
	}
	if !record.installed {
		t.Fatal("extension replacement was not recorded after rename")
	}
	actual, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != "replacement" {
		t.Fatalf("destination = %q", actual)
	}
}

func TestValidateSourceDMLUsesClosedTableSet(t *testing.T) {
	for _, statement := range []string{
		"INSERT INTO cf_unlisted (id) VALUES ($1)",
		"INSERT INTO cf_items (id) VALUES ($1); DELETE FROM cf_items",
		"UPDATE sync_registry SET table_name = $1",
	} {
		if err := validateSourceDML(statement); err == nil {
			t.Fatalf("accepted unsafe source DML %q", statement)
		}
	}
	if err := validateSourceDML("UPDATE cf_items SET value = $1 WHERE id = $2"); err != nil {
		t.Fatalf("rejected source DML: %v", err)
	}
}

func TestOwnedProcessEscalatesFromTermToKill(t *testing.T) {
	root := t.TempDir()
	readyPath := filepath.Join(root, "ready")
	termPath := filepath.Join(root, "term")
	process, err := startOwnedProcess(
		os.Args[0],
		[]string{"-test.run=^TestTermResistantProcessHelper$"},
		append(os.Environ(),
			"GO_WANT_TERM_RESISTANT_PROCESS=1",
			"TERM_RESISTANT_READY_PATH="+readyPath,
			"TERM_RESISTANT_SIGNAL_PATH="+termPath,
		),
		defaultProcessLogBytes,
		nil,
	)
	if err != nil {
		t.Fatalf("start TERM-resistant process: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = process.Stop(cleanupContext, time.Second)
	})
	readyContext, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readyCancel()
	if err := waitUntil(readyContext, func(context.Context) (bool, error) {
		_, err := os.Stat(readyPath)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		t.Fatalf("wait for TERM-resistant process readiness: %v", err)
	}

	stopContext, stopCancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer stopCancel()
	if err := process.Stop(stopContext, time.Second); err != nil {
		t.Fatalf("stop TERM-resistant process: %v", err)
	}
	if _, err := os.Stat(termPath); err != nil {
		t.Fatalf("TERM-resistant process did not observe SIGTERM: %v", err)
	}
	if !process.Exited() {
		t.Fatal("TERM-resistant process survived SIGKILL escalation")
	}
}

func TestCleanupStageUsesAnIndependentBoundedContext(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := runCleanupStage(parent, time.Second, func(stageContext context.Context) error {
		called = true
		if stageContext.Err() != nil {
			return errors.New("cleanup stage inherited cancellation")
		}
		if _, bounded := stageContext.Deadline(); !bounded {
			return errors.New("cleanup stage has no deadline")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("run independent cleanup stage: %v", err)
	}
	if !called {
		t.Fatal("independent cleanup stage did not run")
	}
}

func TestCleanupRetainsInstallationLockWhenRestorationFails(t *testing.T) {
	root := t.TempDir()
	lock, err := acquireInstallationLock(context.Background(), filepath.Join(root, "installation.lock"))
	if err != nil {
		t.Fatalf("acquire installation lock: %v", err)
	}
	destination := filepath.Join(root, "installed-extension")
	if err := os.WriteFile(destination, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	harness := &Harness{
		config: HarnessConfig{ShutdownTimeout: time.Second},
		lock:   lock,
		installed: &installedExtension{files: []installedExtensionFile{{
			destination:     destination,
			installedDigest: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			installed:       true,
		}}},
	}
	if err := harness.cleanup(context.Background()); err == nil {
		t.Fatal("cleanup accepted failed extension restoration")
	}
	lock.mu.Lock()
	released := lock.released
	lock.mu.Unlock()
	if released || harness.lock == nil {
		t.Fatal("cleanup released the installation lock after failed restoration")
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("release retained installation lock after test: %v", err)
	}
}

func TestTermResistantProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_TERM_RESISTANT_PROCESS") != "1" {
		return
	}
	readyPath := os.Getenv("TERM_RESISTANT_READY_PATH")
	termPath := os.Getenv("TERM_RESISTANT_SIGNAL_PATH")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	defer signal.Stop(signals)
	if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
		os.Exit(2)
	}
	for {
		if <-signals == syscall.SIGTERM {
			if err := os.WriteFile(termPath, []byte("term"), 0o600); err != nil {
				os.Exit(3)
			}
		}
	}
}
