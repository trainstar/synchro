package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	installationLock := testCanonicalInstallationLock(t)
	command := []string{
		"verified-ssh-wrapper",
		"fixture",
		"synchro-local-postgres",
		"lifecycle",
		"--state-dir",
		"/owned/state",
		"--installation-lock",
		installationLock,
	}
	environment := attachEnvironment("postgres://fixture", runID, command, localCredentials{})
	for _, wanted := range []string{
		"SYNCHRO_CONFORMANCE_ATTACH_RUN_ID='" + runID + "'",
		`SYNCHRO_CONFORMANCE_ATTACH_LIFECYCLE_COMMAND='["verified-ssh-wrapper","fixture","synchro-local-postgres","lifecycle","--state-dir","/owned/state","--installation-lock","` + installationLock + `"]'`,
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
	installationLock := testCanonicalInstallationLock(t)
	defaultCommand, err := startLifecycleCommand("", false, executable, stateDir, installationLock)
	if err != nil {
		t.Fatalf("default lifecycle command rejected: %v", err)
	}
	wantDefault := []string{
		executable,
		"lifecycle",
		"--state-dir",
		stateDir,
		"--installation-lock",
		installationLock,
	}
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
	command, err := startLifecycleCommand(string(remoteJSON), true, executable, stateDir, installationLock)
	if err != nil {
		t.Fatalf("valid remote lifecycle command rejected: %v", err)
	}
	wantRemote := append(append([]string(nil), remote...), "--installation-lock", installationLock)
	if strings.Join(command, "\x00") != strings.Join(wantRemote, "\x00") {
		t.Fatalf("remote lifecycle command = %#v", command)
	}
}

func TestStartLifecycleCommandRejectsWrongStateAndShell(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "owned-state")
	executable := filepath.Join(t.TempDir(), "synchro-local-postgres")
	installationLock := testCanonicalInstallationLock(t)
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
		{
			name: "wrong installation lock",
			command: []string{
				"verified-ssh-wrapper",
				"fixture",
				"synchro-local-postgres",
				"lifecycle",
				"--state-dir",
				stateDir,
				"--installation-lock",
				filepath.Join(t.TempDir(), "wrong.lock"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.command)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := startLifecycleCommand(string(data), true, executable, stateDir, installationLock); err == nil {
				t.Fatal("invalid lifecycle command was accepted")
			}
		})
	}
	if _, err := startLifecycleCommand("", true, executable, stateDir, installationLock); err == nil {
		t.Fatal("explicit empty lifecycle command was accepted")
	}
	if _, err := startLifecycleCommand("", false, executable, stateDir, "relative.lock"); err == nil {
		t.Fatal("relative lifecycle installation lock was accepted")
	}
}

func TestResolveInstallationLockPathUsesCanonicalDestinationPair(t *testing.T) {
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

	first, err := resolveInstallationLockPath(context.Background(), binDir, "")
	if err != nil {
		t.Fatalf("resolve installation lock: %v", err)
	}
	second, err := resolveInstallationLockPath(context.Background(), wrapperBinDir, first)
	if err != nil {
		t.Fatalf("resolve shared destination lock: %v", err)
	}
	distinct, err := resolveInstallationLockPath(context.Background(), distinctBinDir, "")
	if err != nil {
		t.Fatalf("resolve distinct destination lock: %v", err)
	}
	if first != second {
		t.Fatalf("identical destination pairs resolved different locks: %q and %q", first, second)
	}
	if first == distinct {
		t.Fatalf("distinct destination pairs resolved one lock: %q", first)
	}
	if _, err := resolveInstallationLockPath(context.Background(), distinctBinDir, first); err == nil {
		t.Fatal("explicit lock from a different destination pair was accepted")
	}
	if within := strings.HasPrefix(first, root+string(filepath.Separator)); within {
		t.Fatalf("installation lock %q is private to fixture root %q", first, root)
	}
	if _, err := resolveInstallationLockPath(context.Background(), binDir, filepath.Join(root, "private.lock")); err == nil {
		t.Fatal("private per-fixture installation lock was accepted")
	}
	if _, err := resolveInstallationLockPath(nil, binDir, ""); err == nil {
		t.Fatal("nil installation lock context was accepted")
	}
}

func TestRunLifecycleUsesOwnedControlProtocolAndSameRunDestroyIsIdempotent(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := ensurePrivateDirectory(stateDir); err != nil {
		t.Fatal(err)
	}
	runID := strings.Repeat("b", 32)
	installationLock := testCanonicalInstallationLock(t)
	listener, err := openLifecycleListener()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := writeLifecycleState(stateDir, lifecycleState{
		RunID:            runID,
		ControlAddress:   listener.Addr().String(),
		InstallationLock: installationLock,
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
		return runLifecycle(context.Background(), []string{
			"--state-dir",
			stateDir,
			"--installation-lock",
			installationLock,
			"restart",
			runID,
		})
	})
	request := <-requestResult
	if request.Operation != "restart" || request.RunID != runID {
		t.Fatalf("lifecycle request = %#v", request)
	}
	if !strings.Contains(output, `"run_id":"`+runID+`"`) || !strings.Contains(output, `"destroyed":false`) {
		t.Fatalf("lifecycle output = %q", output)
	}

	if err := writeLifecycleState(stateDir, lifecycleState{
		RunID:            runID,
		InstallationLock: installationLock,
		Destroyed:        true,
	}); err != nil {
		t.Fatal(err)
	}
	output = captureStandardOutput(t, func() error {
		return runLifecycle(context.Background(), []string{
			"--state-dir",
			stateDir,
			"--installation-lock",
			installationLock,
			"destroy",
			runID,
		})
	})
	if !strings.Contains(output, `"destroyed":true`) {
		t.Fatalf("idempotent destroy output = %q", output)
	}
	output = captureStandardOutput(t, func() error {
		return runLifecycle(context.Background(), []string{"--state-dir", stateDir, "destroy", runID})
	})
	if !strings.Contains(output, `"destroyed":true`) {
		t.Fatalf("stored-lock idempotent destroy output = %q", output)
	}
	if err := runLifecycle(context.Background(), []string{
		"--state-dir",
		stateDir,
		"--installation-lock",
		installationLock,
		"destroy",
		strings.Repeat("c", 32),
	}); err == nil {
		t.Fatal("destroy accepted a mismatched run identity")
	}
	if err := runLifecycle(context.Background(), []string{
		"--state-dir",
		stateDir,
		"--installation-lock",
		installationLock,
		"restart",
		runID,
	}); err == nil {
		t.Fatal("restart accepted a destroyed run")
	}
	if err := runLifecycle(context.Background(), []string{
		"--state-dir",
		stateDir,
		"--installation-lock",
		filepath.Join(t.TempDir(), "wrong.lock"),
		"destroy",
		runID,
	}); err == nil {
		t.Fatal("destroy accepted a mismatched installation lock")
	}
}

func TestLifecycleStateRejectsMalformedInstallationLock(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := ensurePrivateDirectory(stateDir); err != nil {
		t.Fatal(err)
	}
	state := lifecycleState{
		RunID:            strings.Repeat("d", 32),
		ControlAddress:   "127.0.0.1:5432",
		InstallationLock: "relative.lock",
	}
	if err := writeLifecycleState(stateDir, state); err == nil {
		t.Fatal("lifecycle state accepted a relative installation lock")
	}
	state.InstallationLock = testCanonicalInstallationLock(t)
	if err := writeLifecycleState(stateDir, state); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stateDir, lifecycleStateName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	delete(raw, "installation_lock")
	data, err = json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readLifecycleState(stateDir); err == nil {
		t.Fatal("lifecycle state accepted a missing installation lock")
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

func testCanonicalInstallationLock(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "postgresql-install.lock")
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(parent, filepath.Base(path))
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
