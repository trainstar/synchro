package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRunArgumentValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"missing command", nil, "command is required"},
		{"unknown command", []string{"stop"}, "unknown command"},
		{"start missing required flags", []string{"start"}, "start requires"},
		{"start invalid flag", []string{"start", "--not-a-flag"}, "start flags are invalid"},
		{"prepare missing root", []string{"prepare", "--database-url", "postgres://example"}, "prepare requires"},
		{"prepare blank URL", []string{"prepare", "--repo-root", ".", "--database-url", "  "}, "prepare requires"},
		{"lifecycle missing state", []string{"lifecycle", "restart", strings.Repeat("a", 32)}, "lifecycle requires"},
		{"lifecycle malformed identity", []string{"lifecycle", "--state-dir", ".", "restart", "changed"}, "operation or run identity"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(context.Background(), tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("run(%v) error = %v, want message containing %q", tt.args, err, tt.want)
			}
		})
	}
	if err := run(nil, []string{"start"}); err == nil || err.Error() != "context is required" {
		t.Fatalf("run with nil context error = %v", err)
	}
}

func TestAttachEnvironmentIncludesOwnedLifecycleFields(t *testing.T) {
	runID := strings.Repeat("a", 32)
	command := []string{"verified-ssh-wrapper", "fixture", "synchro-local-postgres", "lifecycle", "--state-dir", "/owned/state"}
	environment := attachEnvironment("postgres://fixture", runID, command, localCredentials{})
	for _, wanted := range []string{
		"SYNCHRO_CONFORMANCE_ATTACH_RUN_ID='" + runID + "'",
		`SYNCHRO_CONFORMANCE_ATTACH_LIFECYCLE_COMMAND='["verified-ssh-wrapper","fixture","synchro-local-postgres","lifecycle","--state-dir","/owned/state"]'`,
		"SYNCHRO_CONFORMANCE_ATTACH_DESTROY_ON_CLOSE='false'",
	} {
		if !strings.Contains(environment, wanted) {
			t.Fatalf("attach environment omits %q: %s", wanted, environment)
		}
	}
}

func TestStartLifecycleCommandDefaultAndRemoteOverride(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "owned-state")
	executable := filepath.Join(t.TempDir(), "synchro-local-postgres")
	defaultCommand, err := startLifecycleCommand("", false, executable, stateDir)
	if err != nil {
		t.Fatalf("default lifecycle command rejected: %v", err)
	}
	wantDefault := []string{executable, "lifecycle", "--state-dir", stateDir}
	if strings.Join(defaultCommand, "\x00") != strings.Join(wantDefault, "\x00") {
		t.Fatalf("default lifecycle command = %#v", defaultCommand)
	}

	remote := []string{
		"/controller/verified-ssh-wrapper",
		"fixture.example",
		"/remote/bin/synchro-local-postgres",
		"lifecycle",
		"--state-dir",
		stateDir,
	}
	remoteJSON, err := json.Marshal(remote)
	if err != nil {
		t.Fatal(err)
	}
	command, err := startLifecycleCommand(string(remoteJSON), true, executable, stateDir)
	if err != nil {
		t.Fatalf("valid remote lifecycle command rejected: %v", err)
	}
	if strings.Join(command, "\x00") != strings.Join(remote, "\x00") {
		t.Fatalf("remote lifecycle command = %#v", command)
	}
}

func TestStartLifecycleCommandRejectsWrongStateAndShell(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "owned-state")
	executable := filepath.Join(t.TempDir(), "synchro-local-postgres")
	tests := []struct {
		name    string
		command []string
	}{
		{
			name:    "wrong state directory",
			command: []string{"verified-ssh-wrapper", "fixture", "synchro-local-postgres", "lifecycle", "--state-dir", filepath.Join(t.TempDir(), "other-state")},
		},
		{
			name:    "shell",
			command: []string{"verified-ssh-wrapper", "fixture", "/bin/sh", "lifecycle", "--state-dir", stateDir},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.command)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := startLifecycleCommand(string(data), true, executable, stateDir); err == nil {
				t.Fatal("invalid lifecycle command was accepted")
			}
		})
	}
	if _, err := startLifecycleCommand("", true, executable, stateDir); err == nil {
		t.Fatal("explicit empty lifecycle command was accepted")
	}
}

func TestPostgreSQLInstallationLockPathUsesCanonicalDestinationPair(t *testing.T) {
	root := t.TempDir()
	pkglibdir := filepath.Join(root, "shared", "lib")
	sharedir := filepath.Join(root, "shared", "share")
	otherPKGLibDir := filepath.Join(root, "other", "lib")
	otherShareDir := filepath.Join(root, "other", "share")
	for _, path := range []string{pkglibdir, sharedir, otherPKGLibDir, otherShareDir} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	writePGConfig := func(binDir, libraryDir, shareDir string) {
		t.Helper()
		if err := os.MkdirAll(binDir, 0o700); err != nil {
			t.Fatal(err)
		}
		script := "#!/bin/sh\ncase \"$1\" in\n--pkglibdir) printf '%s\\n' '" + libraryDir +
			"';;\n--sharedir) printf '%s\\n' '" + shareDir + "';;\n*) exit 1;;\nesac\n"
		if err := os.WriteFile(filepath.Join(binDir, "pg_config"), []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	binDir := filepath.Join(root, "postgresql-a", "bin")
	wrapperBinDir := filepath.Join(root, "postgresql-b", "bin")
	distinctBinDir := filepath.Join(root, "postgresql-c", "bin")
	writePGConfig(binDir, pkglibdir, sharedir)
	writePGConfig(wrapperBinDir, pkglibdir, sharedir)
	writePGConfig(distinctBinDir, otherPKGLibDir, otherShareDir)

	tempA := filepath.Join(root, "caller-temp-a")
	tempB := filepath.Join(root, "caller-temp-b")
	for _, path := range []string{tempA, tempB} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("TMPDIR", tempA)
	first, err := blackbox.PostgreSQLInstallationLockPath(context.Background(), binDir)
	if err != nil {
		t.Fatalf("resolve installation lock: %v", err)
	}
	t.Setenv("TMPDIR", tempB)
	second, err := blackbox.PostgreSQLInstallationLockPath(context.Background(), wrapperBinDir)
	if err != nil {
		t.Fatalf("resolve shared destination lock: %v", err)
	}
	distinct, err := blackbox.PostgreSQLInstallationLockPath(context.Background(), distinctBinDir)
	if err != nil {
		t.Fatalf("resolve distinct destination lock: %v", err)
	}
	if first != second {
		t.Fatalf("identical destination pairs resolved different locks: %q and %q", first, second)
	}
	canonicalTmp, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(first) != canonicalTmp {
		t.Fatalf("installation lock parent = %q, want %q", filepath.Dir(first), canonicalTmp)
	}
	if first == distinct {
		t.Fatalf("distinct destination pairs resolved one lock: %q", first)
	}
	if within := strings.HasPrefix(first, root+string(filepath.Separator)); within {
		t.Fatalf("installation lock %q is private to fixture root %q", first, root)
	}
	if _, err := blackbox.PostgreSQLInstallationLockPath(nil, binDir); err == nil {
		t.Fatal("nil installation lock context was accepted")
	}
}

func TestRunLifecycleUsesOwnedControlProtocolAndSameRunDestroyIsIdempotent(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := ensurePrivateDirectory(stateDir); err != nil {
		t.Fatal(err)
	}
	runID := strings.Repeat("b", 32)
	listener, err := openLifecycleListener()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := writeLifecycleState(stateDir, lifecycleState{
		RunID:          runID,
		ControlAddress: listener.Addr().String(),
	}); err != nil {
		t.Fatal(err)
	}
	requestResult := make(chan lifecycleRequest, 1)
	go func() {
		connection, acceptErr := listener.AcceptTCP()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		data, _ := io.ReadAll(connection)
		var request lifecycleRequest
		_ = json.Unmarshal(data, &request)
		requestResult <- request
		_ = writeLifecycleWireResponse(connection, lifecycleResponse{
			RunID:             runID,
			AttachDatabaseURL: "postgres://admin@127.0.0.1:55432/synchro_conformance_owned",
		})
	}()
	output := captureStandardOutput(t, func() error {
		return runLifecycle(context.Background(), []string{"--state-dir", stateDir, "restart", runID})
	})
	request := <-requestResult
	if request.Operation != "restart" || request.RunID != runID {
		t.Fatalf("lifecycle request = %#v", request)
	}
	if !strings.Contains(output, `"run_id":"`+runID+`"`) || !strings.Contains(output, `"destroyed":false`) {
		t.Fatalf("lifecycle output = %q", output)
	}

	if err := writeLifecycleState(stateDir, lifecycleState{
		RunID:     runID,
		Destroyed: true,
	}); err != nil {
		t.Fatal(err)
	}
	output = captureStandardOutput(t, func() error {
		return runLifecycle(context.Background(), []string{"--state-dir", stateDir, "destroy", runID})
	})
	if !strings.Contains(output, `"destroyed":true`) {
		t.Fatalf("idempotent destroy output = %q", output)
	}
	if err := runLifecycle(context.Background(), []string{"--state-dir", stateDir, "destroy", strings.Repeat("c", 32)}); err == nil {
		t.Fatal("destroy accepted a mismatched run identity")
	}
	if err := runLifecycle(context.Background(), []string{"--state-dir", stateDir, "restart", runID}); err == nil {
		t.Fatal("restart accepted a destroyed run")
	}
}

func TestRunLifecycleCancellationUnblocksStalledPeer(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := ensurePrivateDirectory(stateDir); err != nil {
		t.Fatal(err)
	}
	runID := strings.Repeat("d", 32)
	listener, err := openLifecycleListener()
	if err != nil {
		t.Fatal(err)
	}
	if err := writeLifecycleState(stateDir, lifecycleState{
		RunID:          runID,
		ControlAddress: listener.Addr().String(),
	}); err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}

	requestRead := make(chan struct{})
	releasePeer := make(chan struct{})
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		connection, acceptErr := listener.AcceptTCP()
		if acceptErr != nil {
			return
		}
		defer connection.Close()
		_, _ = io.ReadAll(connection)
		close(requestRead)
		<-releasePeer
	}()
	t.Cleanup(func() {
		close(releasePeer)
		_ = listener.Close()
		<-peerDone
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- runLifecycle(ctx, []string{"--state-dir", stateDir, "restart", runID})
	}()
	select {
	case <-requestRead:
	case <-time.After(5 * time.Second):
		t.Fatal("stalled peer did not receive the lifecycle request")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled lifecycle command error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled lifecycle command waited for peer close")
	}
	select {
	case <-peerDone:
		t.Fatal("stalled peer closed before the lifecycle command returned")
	default:
	}
}

func TestLifecycleAttachDatabaseURLContainsNoCredential(t *testing.T) {
	result, err := lifecycleAttachDatabaseURL("postgres://admin:private-value@127.0.0.1:55432/synchro_conformance_owned?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result, "admin") || strings.Contains(result, "private-value") || result != "postgres://127.0.0.1:55432/synchro_conformance_owned?sslmode=disable" {
		t.Fatalf("sanitized lifecycle attach URL = %q", result)
	}
}

func captureStandardOutput(t *testing.T, operation func() error) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdout
	os.Stdout = writer
	err = operation()
	_ = writer.Close()
	os.Stdout = original
	if err != nil {
		_ = reader.Close()
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestEnvironmentAssignmentQuotesShellValues(t *testing.T) {
	got := environmentAssignment("SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL", "host='127.0.0.1' port=5432 password='secret'")
	want := "SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL='host='\"'\"'127.0.0.1'\"'\"' port=5432 password='\"'\"'secret'\"'\"''"
	if got != want {
		t.Fatalf("environmentAssignment() = %q, want %q", got, want)
	}
}

func TestPrivateStateAndCredentialFiles(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	if err := ensurePrivateDirectory(state); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(state)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("state directory mode = %v, want 0700", info.Mode().Perm())
	}
	credentials, err := createCredentials(state)
	if err != nil {
		t.Fatal(err)
	}
	if len(credentials.paths) != 6 {
		t.Fatalf("credential file count = %d, want 6", len(credentials.paths))
	}
	for _, path := range credentials.paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("credential %q mode = %v, want 0600", path, info.Mode().Perm())
		}
		contents, err := os.ReadFile(path)
		if err != nil || len(contents) != 64 {
			t.Fatalf("credential %q contents are invalid", path)
		}
		if _, err := hex.DecodeString(string(contents)); err != nil {
			t.Fatalf("credential %q is not hexadecimal: %v", path, err)
		}
	}
	credentials.remove()
	for _, path := range credentials.paths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("credential %q still exists, stat error %v", path, err)
		}
	}
	if err := ensurePrivateDirectory(filepath.Join(dir, "missing", "..", "state2")); err != nil {
		t.Fatal(err)
	}
}

func TestWritePrivateFileRejectsMissingParent(t *testing.T) {
	err := writePrivateFile(filepath.Join(t.TempDir(), "missing", "value"), []byte("x"))
	if err == nil {
		t.Fatal("writePrivateFile accepted a missing parent")
	}
}
