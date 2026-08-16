package execution

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type commandFixture struct {
	root       string
	capability CommandCapability
	issuer     ReceiptIssuer
	runner     TrustedRunner
}

func TestCommandCapabilityRejectsCompletionWithoutLaunch(t *testing.T) {
	fixture := newCommandFixture(t, "@true")
	_, err := PrepareCompletion(fixture.issuer, commandFields(0, ResultPassed, "passed"))
	if !errors.Is(err, ErrInvalidCompletion) {
		t.Fatalf("PrepareCompletion() error = %v, want %v", err, ErrInvalidCompletion)
	}
}

func TestCommandCapabilityPreservesFailingRecipeResult(t *testing.T) {
	fixture := newCommandFixture(t, "@false")
	observed, err := fixture.capability.Execute(context.Background(), []string{"make", "proof"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if observed.ExitCode == 0 {
		t.Fatal("failing Make recipe returned a zero exit code")
	}
	passed := commandFields(0, ResultPassed, "passed")
	passed.Command = observed.Observation()
	passed.StartedAt = observed.StartedAt
	passed.CompletedAt = observed.CompletedAt
	if _, err := PrepareCompletion(fixture.issuer, passed); !errors.Is(err, ErrInvalidCompletion) {
		t.Fatalf("passed completion error = %v, want %v", err, ErrInvalidCompletion)
	}

	fields := commandFields(1, ResultFailed, "failed")
	fields.Command = observed.Observation()
	fields.StartedAt = observed.StartedAt
	fields.CompletedAt = observed.CompletedAt
	completion, err := PrepareCompletion(fixture.issuer, fields)
	if err != nil {
		t.Fatalf("PrepareCompletion() error = %v", err)
	}
	receipt, err := fixture.runner.CompleteReceipt(fixture.issuer, completion)
	if err != nil {
		t.Fatalf("CompleteReceipt() error = %v", err)
	}
	stored, err := receipt.Fields()
	if err != nil {
		t.Fatalf("Receipt.Fields() error = %v", err)
	}
	if stored.ExitCode != 1 || stored.Result != ResultFailed || stored.Command.ExitCode != observed.ExitCode {
		t.Fatalf("receipt result = (%d, %q, command %d)", stored.ExitCode, stored.Result, stored.Command.ExitCode)
	}
}

func TestCommandCapabilityRejectsLauncherIssuerMismatch(t *testing.T) {
	first := newCommandFixture(t, "@true")
	second := newCommandFixture(t, "@true")
	if first.issuer.MatchesCommandCapability(second.capability) || second.capability.MatchesIssuer(first.issuer) {
		t.Fatal("issuer matched a foreign command capability")
	}
	if _, err := second.capability.Execute(context.Background(), []string{"make", "proof"}); err != nil {
		t.Fatalf("foreign Execute() error = %v", err)
	}
	_, err := PrepareCompletion(first.issuer, commandFields(0, ResultPassed, "passed"))
	if !errors.Is(err, ErrInvalidCompletion) {
		t.Fatalf("PrepareCompletion() error = %v, want %v", err, ErrInvalidCompletion)
	}
}

func TestCommandCapabilityRejectsPostCreationSourceDrift(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "Makefile", path: "Makefile"},
		{name: "other tracked source", path: "source.txt"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCommandFixture(t, "@true")
			path := filepath.Join(fixture.root, test.path)
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatalf("open tracked source: %v", err)
			}
			if _, err := file.WriteString("changed\n"); err != nil {
				_ = file.Close()
				t.Fatalf("change tracked source: %v", err)
			}
			if err := file.Close(); err != nil {
				t.Fatalf("close tracked source: %v", err)
			}
			_, err = fixture.capability.Execute(context.Background(), []string{"make", "proof"})
			if !errors.Is(err, ErrInvalidCommandCapability) {
				t.Fatalf("Execute() error = %v, want %v", err, ErrInvalidCommandCapability)
			}
		})
	}
}

func TestCommandCapabilityIgnoresAmbientMakeControlVariables(t *testing.T) {
	fixture := newCommandFixture(t, "@true")
	foreign := filepath.Join(t.TempDir(), "foreign.mk")
	if err := os.WriteFile(foreign, []byte("proof:\n\t@false\n"), 0o600); err != nil {
		t.Fatalf("write foreign Makefile: %v", err)
	}
	t.Setenv("MAKEFILES", foreign)
	t.Setenv("MAKEFLAGS", "-f "+foreign)
	t.Setenv("GNUMAKEFLAGS", "-f "+foreign)
	t.Setenv("MAKE", foreign)
	t.Setenv("GO_TEST_ARGS", "-run TestDoesNotExist")
	t.Setenv("GO_TEST_PKGS", "./missing")
	t.Setenv("DETOX_ARGS", "--help")
	t.Setenv("PATH", t.TempDir())
	t.Setenv("DYLD_INSERT_LIBRARIES", foreign)
	t.Setenv("LD_PRELOAD", foreign)
	result, err := fixture.capability.Execute(context.Background(), []string{"make", "proof"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ambient Make controls changed exit code to %d", result.ExitCode)
	}
}

func TestCommandCapabilityRunsLockedSnapshotDuringWorktreeMutation(t *testing.T) {
	fixture := newCommandFixture(t, "@sleep 1; grep -q '^locked$$' source.txt")
	resultChannel := make(chan CommandResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, err := fixture.capability.Execute(context.Background(), []string{"make", "proof"})
		resultChannel <- result
		errorChannel <- err
	}()
	time.Sleep(500 * time.Millisecond)
	path := filepath.Join(fixture.root, "source.txt")
	if err := os.WriteFile(path, []byte("changed\n"), 0o600); err != nil {
		t.Fatalf("change worktree source: %v", err)
	}
	if err := os.WriteFile(path, []byte("locked\n"), 0o600); err != nil {
		t.Fatalf("restore worktree source: %v", err)
	}
	result := <-resultChannel
	if err := <-errorChannel; err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("locked source snapshot exit code = %d", result.ExitCode)
	}
}

func newCommandFixture(t *testing.T, recipe string) commandFixture {
	t.Helper()
	root := t.TempDir()
	makefile := []byte(".PHONY: proof\n\nproof:\n\t" + recipe + "\n")
	if err := os.WriteFile(filepath.Join(root, "Makefile"), makefile, 0o600); err != nil {
		t.Fatalf("write Makefile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "source.txt"), []byte("locked\n"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	for _, args := range [][]string{
		{"init", "--quiet"},
		{"add", "--all"},
		{"-c", "user.name=Synchro Test", "-c", "user.email=test@synchro.invalid", "commit", "--quiet", "-m", "fixture"},
	} {
		command := exec.Command("git", args...)
		command.Dir = root
		command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	commitCommand := exec.Command("git", "rev-parse", "HEAD")
	commitCommand.Dir = root
	commitOutput, err := commitCommand.Output()
	if err != nil {
		t.Fatalf("git rev-parse HEAD: %v", err)
	}
	capability, err := NewCommandCapability(root, strings.TrimSpace(string(commitOutput)), makefile)
	if err != nil {
		t.Fatalf("NewCommandCapability() error = %v", err)
	}
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate runner key: %v", err)
	}
	runner, err := NewTrustedRunner(privateKey)
	if err != nil {
		t.Fatalf("NewTrustedRunner() error = %v", err)
	}
	authorization, err := runner.AuthorizeExecutable(strings.Repeat("a", 64), strings.Repeat("d", 64))
	if err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}
	issuer, err := NewReceiptIssuerFromAuthorizationAndGeneratorAndCandidateLockAndCommandCapability(
		authorization,
		GeneratorIdentity{Name: "test-generator", Version: "1.0.0", BinarySHA256: strings.Repeat("b", 64)},
		strings.Repeat("c", 64),
		capability,
	)
	if err != nil {
		t.Fatalf("create command-bound issuer: %v", err)
	}
	return commandFixture{root: root, capability: capability, issuer: issuer, runner: runner}
}

func commandFields(exitCode int, result Result, assertionOutcome string) ReceiptFields {
	started := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	return ReceiptFields{
		EvidenceClass:     EvidenceClassCandidate,
		ScenarioID:        "SCN-COMMAND-001",
		ProofObligationID: "OBL-COMMAND-001",
		MakeTarget:        "proof",
		Argv:              []string{"make", "proof"},
		StartedAt:         started,
		CompletedAt:       started.Add(time.Second),
		ExitCode:          exitCode,
		Result:            result,
		Assertions:        []AssertionResult{{AssertionID: "ASSERT-COMMAND-001", Outcome: assertionOutcome}},
	}
}
