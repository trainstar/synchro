package blackbox

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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

func TestCleanupRejectsPostLoadCandidateArtifactChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, EnvironmentConfig)
	}{
		{
			name: "extension manifest replacement",
			mutate: func(t *testing.T, environment EnvironmentConfig) {
				replaceFileWithSameBytes(t, filepath.Join(environment.ExtensionArtifact, extensionBundleManifestName))
			},
		},
		{
			name: "extension payload replacement",
			mutate: func(t *testing.T, environment EnvironmentConfig) {
				relative := environment.extension.files[0].Path
				replaceFileWithSameBytes(t, filepath.Join(environment.ExtensionArtifact, filepath.FromSlash(relative)))
			},
		},
		{
			name: "extension manifest digest replacement",
			mutate: func(t *testing.T, environment EnvironmentConfig) {
				path := filepath.Join(environment.ExtensionArtifact, extensionBundleManifestName+".sha256")
				replaceFileWithSameBytes(t, path)
			},
		},
		{
			name: "extension manifest digest tampering",
			mutate: func(t *testing.T, environment EnvironmentConfig) {
				path := filepath.Join(environment.ExtensionArtifact, extensionBundleManifestName+".sha256")
				appendArtifactWhitespace(t, path)
			},
		},
		{
			name: "extension manifest tampering",
			mutate: func(t *testing.T, environment EnvironmentConfig) {
				path := filepath.Join(environment.ExtensionArtifact, extensionBundleManifestName)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "adapter executable replacement",
			mutate: func(t *testing.T, environment EnvironmentConfig) {
				replaceFileWithSameBytes(t, environment.AdapterArtifact)
			},
		},
		{
			name: "adapter digest replacement",
			mutate: func(t *testing.T, environment EnvironmentConfig) {
				replaceFileWithSameBytes(t, environment.AdapterArtifact+".sha256")
			},
		},
		{
			name: "adapter digest tampering",
			mutate: func(t *testing.T, environment EnvironmentConfig) {
				appendArtifactWhitespace(t, environment.AdapterArtifact+".sha256")
			},
		},
		{
			name: "adapter executable tampering",
			mutate: func(t *testing.T, environment EnvironmentConfig) {
				if err := os.WriteFile(environment.AdapterArtifact, []byte("tampered-adapter"), 0o755); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := candidateArtifactEnvironmentFixture(t)
			test.mutate(t, environment)

			lock, err := acquireInstallationLock(context.Background(), filepath.Join(t.TempDir(), "install.lock"))
			if err != nil {
				t.Fatal(err)
			}
			harness := &Harness{
				config:  HarnessConfig{ShutdownTimeout: time.Second},
				env:     environment,
				runRoot: t.TempDir(),
				lock:    lock,
			}
			runRoot := harness.runRoot
			err = harness.cleanup(context.Background())
			if err == nil || !strings.Contains(err.Error(), "candidate") || !strings.Contains(err.Error(), "artifact identity changed after execution") {
				t.Fatalf("cleanup artifact identity error = %v", err)
			}
			if _, statErr := os.Stat(runRoot); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("cleanup retained the safe run root: %v", statErr)
			}
			lock.mu.Lock()
			released := lock.released
			lock.mu.Unlock()
			if !released || harness.lock != nil {
				t.Fatal("cleanup retained the installation lock after safe restoration")
			}
		})
	}
}

func TestCleanupAcceptsUnchangedCandidateArtifacts(t *testing.T) {
	harness := &Harness{
		config: HarnessConfig{ShutdownTimeout: time.Second},
		env:    candidateArtifactEnvironmentFixture(t),
	}
	if err := harness.cleanup(context.Background()); err != nil {
		t.Fatalf("cleanup rejected unchanged candidate artifacts: %v", err)
	}
}

func appendArtifactWhitespace(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func candidateArtifactEnvironmentFixture(t *testing.T) EnvironmentConfig {
	t.Helper()
	extensionRoot := writeExtensionBundleFixture(t)
	extension, err := verifyExtensionBundle(extensionRoot)
	if err != nil {
		t.Fatal(err)
	}
	adapterRoot := t.TempDir()
	adapterPath := filepath.Join(adapterRoot, "synchrod-pg")
	if err := os.WriteFile(adapterPath, []byte("adapter-artifact"), 0o755); err != nil {
		t.Fatal(err)
	}
	digest, err := fileSHA256(adapterPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(adapterPath+".sha256", []byte(digest+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	adapter, err := loadAdapterArtifactIdentity(adapterPath)
	if err != nil {
		t.Fatal(err)
	}
	return EnvironmentConfig{
		ExtensionArtifact: extensionRoot,
		AdapterArtifact:   adapterPath,
		adapterSHA256:     adapter.sha256,
		adapterIdentity:   adapter,
		extension:         extension,
		verified:          true,
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
		"UPDATE synchro.sync_registry SET table_name = $1",
	} {
		if err := validateSourceDML(statement); err == nil {
			t.Fatalf("accepted unsafe source DML %q", statement)
		}
	}
	if err := validateSourceDML("UPDATE cf_items SET value = $1 WHERE id = $2"); err != nil {
		t.Fatalf("rejected source DML: %v", err)
	}
}

func TestSourceMutationErrorPreservesOnlySQLState(t *testing.T) {
	errorWithState := sourceMutationError("source mutation failed", &pgconn.PgError{
		Code:    "42725",
		Message: "private database detail",
	})
	if errorWithState.Error() != "source mutation failed (SQLSTATE 42725)" || strings.Contains(errorWithState.Error(), "private") {
		t.Fatal("source mutation error did not preserve only SQLSTATE")
	}
	if sourceMutationError("source mutation failed", errors.New("private database detail")).Error() != "source mutation failed" {
		t.Fatal("non-PostgreSQL source mutation error was not redacted")
	}
}

func TestWorkerHBAConfigurationRestrictsCredential(t *testing.T) {
	configuration := workerHBAConfiguration("synchro_conformance_test", "synchro_cf_worker")
	wanted := "# Synchro conformance authentication boundary\n" +
		"local \"synchro_conformance_test\" \"synchro_cf_worker\" scram-sha-256\n" +
		"local all \"synchro_cf_worker\" reject\n" +
		"local all all trust\n" +
		"host \"synchro_conformance_test\" \"synchro_cf_worker\" 127.0.0.1/32 scram-sha-256\n" +
		"host all \"synchro_cf_worker\" 127.0.0.1/32 reject\n" +
		"host all all 127.0.0.1/32 scram-sha-256\n" +
		"host all all ::1/128 scram-sha-256\n"
	if configuration != wanted {
		t.Fatalf("HBA configuration = %q", configuration)
	}
}

func TestScrubPostgresEnvironmentRemovesWorkerConnectionString(t *testing.T) {
	input := []string{
		"DATABASE_URL=operator-dsn",
		"WORKER_DATABASE_URL=worker-dsn",
		"PGPASSWORD=password",
		"KEEP=present",
	}
	output := strings.Join(scrubPostgresEnvironment(input), "\n")
	if strings.Contains(output, "DATABASE_URL") || strings.Contains(output, "WORKER_DATABASE_URL") || strings.Contains(output, "PGPASSWORD") {
		t.Fatalf("scrubbed environment retained a database credential: %q", output)
	}
	if !strings.Contains(output, "KEEP=present") {
		t.Fatalf("scrubbed environment removed unrelated variable: %q", output)
	}
}

func TestParseProjectionBootstrapResultRequiresStrictExactObject(t *testing.T) {
	valid := `{"bootstrap_id":"10000000-0000-4000-8000-000000000001","registry_generation":7,"source_stream_generation":"20000000-0000-4000-8000-000000000001","active_slot_name":"synchro_active","candidate_slot_name":"synchro_active_bootstrap","schema_version":null,"schema_hash":null,"activation_barrier":"0/00000020","affected_scopes":["user:diagnostic-user"]}`
	result, err := parseProjectionBootstrapResult([]byte(valid), 7)
	if err != nil {
		t.Fatalf("valid projection bootstrap result rejected: %v", err)
	}
	if result.RegistryGeneration != 7 || len(result.AffectedScopes) != 1 {
		t.Fatalf("parsed projection bootstrap result = %#v", result)
	}
	for _, invalid := range []string{
		strings.Replace(valid, `,"affected_scopes"`, `,"unknown":true,"affected_scopes"`, 1),
		strings.Replace(valid, `,"registry_generation":7`, `,"registry_generation":7,"registry_generation":7`, 1),
		valid + ` {"extra":true}`,
		strings.Replace(valid, `,"schema_hash":null`, ``, 1),
		strings.Replace(valid, `"schema_version":null,"schema_hash":null`, `"schema_version":1,"schema_hash":null`, 1),
	} {
		if _, err := parseProjectionBootstrapResult([]byte(invalid), 7); err == nil {
			t.Fatalf("accepted invalid projection bootstrap result %s", invalid)
		}
	}
}

func TestProjectionBootstrapProcessSeparatesAndRedactsStreams(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "projection-bootstrap")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' \"$SECRET\"\nprintf '%s' \"$SECRET\" >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	secret := "operator-password"
	_, _, err := runProjectionBootstrapProcess(
		context.Background(), script, nil, []string{"SECRET=" + secret}, 1024, [][]byte{[]byte(secret)},
	)
	if err == nil {
		t.Fatal("failed projection bootstrap process was accepted")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("process failure leaked a secret: %v", err)
	}
}

func TestProjectionBootstrapProcessRejectsTruncatedStdout(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "projection-bootstrap")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%0200d' 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, _, err := runProjectionBootstrapProcess(context.Background(), script, nil, nil, 16, nil)
	if err == nil || !strings.Contains(err.Error(), "stdout is truncated") {
		t.Fatalf("truncated projection bootstrap stdout was accepted: %v", err)
	}
}

func TestProjectionBootstrapResultJSONTagsRemainExact(t *testing.T) {
	result := ProjectionBootstrapResult{AffectedScopes: []string{}}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"bootstrap_id"`) || strings.Contains(string(data), `"BootstrapID"`) {
		t.Fatalf("projection bootstrap result JSON tags are not exact: %s", data)
	}
}

func TestPostmasterConfigurationSetsFiniteHealthLimits(t *testing.T) {
	dataDir := t.TempDir()
	configurationPath := filepath.Join(dataDir, "postgresql.conf")
	if err := os.WriteFile(configurationPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	harness := &Harness{
		dataDir:   dataDir,
		socketDir: filepath.Join(dataDir, "socket"),
		port:      55432,
		names: HarnessNames{
			Database:        "synchro_health_test",
			ReplicationSlot: "synchro_health_slot",
			Publication:     "synchro_health_publication",
		},
		worker: RoleCredential{Username: "synchro_health_worker"},
	}
	if err := harness.writePostmasterConfiguration(); err != nil {
		t.Fatal(err)
	}
	configuration, err := os.ReadFile(configurationPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, setting := range []string{
		"max_replication_slots = 2",
		"synchro.max_worker_heartbeat_age_seconds = 30",
		"synchro.max_wal_lag_bytes = 67108864",
		"synchro.max_wal_lag_seconds = 30",
	} {
		if !strings.Contains(string(configuration), setting+"\n") {
			t.Fatalf("PostgreSQL configuration does not contain %q", setting)
		}
	}
}

func TestCandidateOperationRecoveryPlanRoutesByOperationKind(t *testing.T) {
	streamActivated, err := candidateOperationRecoveryPlan(
		streamResetOperationKind,
		"activated",
		"stream_old",
		"stream_candidate",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !streamActivated.Activated || streamActivated.RetiredSlotName != "stream_old" ||
		streamActivated.CleanupFunction != "synchro_complete_stream_reset_cleanup" {
		t.Fatalf("stream activation recovery plan = %#v", streamActivated)
	}
	bootstrapStaged, err := candidateOperationRecoveryPlan(
		projectionBootstrapOperationKind,
		"catching_up",
		"stream_old",
		"bootstrap_candidate",
	)
	if err != nil {
		t.Fatal(err)
	}
	if bootstrapStaged.Activated || bootstrapStaged.RetiredSlotName != "bootstrap_candidate" ||
		bootstrapStaged.AbortFunction != "synchro_abort_projection_bootstrap" {
		t.Fatalf("bootstrap staging recovery plan = %#v", bootstrapStaged)
	}
	bootstrapActivated, err := candidateOperationRecoveryPlan(
		projectionBootstrapOperationKind,
		"activated",
		"stream_old",
		"bootstrap_candidate",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bootstrapActivated.Activated || bootstrapActivated.RetiredSlotName != "bootstrap_candidate" ||
		bootstrapActivated.CleanupFunction != "synchro_complete_projection_bootstrap_cleanup" {
		t.Fatalf("bootstrap activation recovery plan = %#v", bootstrapActivated)
	}
	if _, err := candidateOperationRecoveryPlan("unknown", "preparing", "old", "candidate"); err == nil {
		t.Fatal("accepted an unknown candidate operation kind")
	}
	if _, err := candidateOperationRecoveryPlan(streamResetOperationKind, "catching_up", "old", "candidate"); err == nil {
		t.Fatal("accepted stream reset catch-up lifecycle")
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

func TestDiagnosticBearerTokenIsRedactedFromProcessDiagnostics(t *testing.T) {
	log := newBoundedLog(defaultProcessLogBytes, nil)
	harness := &Harness{
		sourceReady: true,
		env:         EnvironmentConfig{jwtSecret: []byte("runtime-redaction-secret")},
		adapter:     &ownedProcess{log: log},
	}
	token, err := harness.DiagnosticBearerToken(time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("sign diagnostic bearer token: %v", err)
	}
	if _, err := log.Write([]byte("adapter failure Authorization: Bearer " + token)); err != nil {
		t.Fatalf("write process diagnostic: %v", err)
	}
	diagnostic := harness.adapter.diagnosticText()
	if strings.Contains(diagnostic, token) {
		t.Fatal("process diagnostic retained a runtime bearer token")
	}
	if !strings.Contains(diagnostic, "[REDACTED]") {
		t.Fatalf("process diagnostic did not mark the redaction: %q", diagnostic)
	}
}

func TestBoundedLogRedactsTruncatedCredentialPrefix(t *testing.T) {
	credential := []byte("zQ7-private-credential-token")
	prefix := []byte("failure: ")
	input := append(append([]byte(nil), prefix...), credential...)
	for visibleCredentialBytes := 1; visibleCredentialBytes < len(credential); visibleCredentialBytes++ {
		log := newBoundedLog(len(prefix)+visibleCredentialBytes, [][]byte{credential})
		if _, err := log.Write(input); err != nil {
			t.Fatalf("write bounded credential log: %v", err)
		}
		if !log.isTruncated() {
			t.Fatal("bounded credential log did not record truncation")
		}
		want := string(prefix) + "[REDACTED]"
		if got := string(log.sanitizedBytes()); got != want {
			t.Fatalf("sanitized bounded credential log = %q, want %q", got, want)
		}
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
