package evidence

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

const (
	testCandidateID = "RC-0.3.0-20260811T120000Z-0123456"
)

var (
	testStart = time.Date(2026, time.August, 11, 12, 0, 0, 0, time.UTC)
	testSeed  = [32]byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	}
	testRepositoryRoot  string
	testFixtureMakefile []byte
)

type candidateFixture struct {
	t              *testing.T
	repoRoot       string
	root           string
	privateKey     ed25519.PrivateKey
	publicKey      ed25519.PublicKey
	scenarios      []scenarios.Scenario
	scenarioPaths  map[string]string
	lock           map[string]any
	lockBytes      []byte
	artifacts      []any
	supportCells   []any
	attestations   []any
	payloadByInvID map[string]lockedPayloadDocument
}

type builderFixture struct {
	*candidateFixture
	builder    *Builder
	scenario   scenarios.Scenario
	obligation scenarios.ProofObligation
	fields     execution.ReceiptFields
}

func TestMain(m *testing.M) {
	sourceRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		panic(err)
	}
	temporary, err := os.MkdirTemp("", "synchro-evidence-repository-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(temporary)
	for _, relative := range []string{"Makefile", "conformance", "docs/src/content/docs"} {
		if err := copyTestPath(sourceRoot, temporary, relative); err != nil {
			panic(err)
		}
	}
	for _, args := range [][]string{
		{"init", "--quiet"},
	} {
		command := exec.Command("git", args...)
		command.Dir = temporary
		command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if output, runErr := command.CombinedOutput(); runErr != nil {
			panic(fmt.Sprintf("git %v: %v: %s", args, runErr, output))
		}
	}
	if err := writeFixtureMakefile(temporary); err != nil {
		panic(err)
	}
	for _, args := range [][]string{
		{"add", "--all"},
		{"-c", "user.name=Synchro Test", "-c", "user.email=test@synchro.invalid", "commit", "--quiet", "-m", "test fixture"},
	} {
		command := exec.Command("git", args...)
		command.Dir = temporary
		command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		if output, runErr := command.CombinedOutput(); runErr != nil {
			panic(fmt.Sprintf("git %v: %v: %s", args, runErr, output))
		}
	}
	testRepositoryRoot = temporary
	code := m.Run()
	_ = os.RemoveAll(temporary)
	os.Exit(code)
}

func copyTestPath(sourceRoot, destinationRoot, relative string) error {
	source := filepath.Join(sourceRoot, filepath.FromSlash(relative))
	destination := filepath.Join(destinationRoot, filepath.FromSlash(relative))
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("test source %q is a symbolic link", relative)
	}
	if info.IsDir() {
		if err := os.MkdirAll(destination, info.Mode().Perm()); err != nil {
			return err
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			child := filepath.ToSlash(filepath.Join(relative, entry.Name()))
			if err := copyTestPath(sourceRoot, destinationRoot, child); err != nil {
				return err
			}
		}
		return nil
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("test source %q is not regular", relative)
	}
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	return os.WriteFile(destination, data, info.Mode().Perm())
}

func TestStorePutVerifyAndRejectUnsafeAttachments(t *testing.T) {
	t.Run("put and verify", func(t *testing.T) {
		root := t.TempDir()
		store := Store{Root: root}
		attachment, err := store.Put("log", "text/plain", []byte("authored execution log\n"))
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		if err := store.Verify(attachment); err != nil {
			t.Fatalf("Verify() error = %v", err)
		}
	})

	t.Run("duplicate collision preserves bytes", func(t *testing.T) {
		root := t.TempDir()
		store := Store{Root: root}
		data := []byte("immutable attachment\n")
		attachment, err := store.Put("log", "text/plain", data)
		if err != nil {
			t.Fatalf("first Put() error = %v", err)
		}
		duplicate, err := store.Put("log", "text/plain", append([]byte(nil), data...))
		if !errors.Is(err, ErrDuplicateAttachment) {
			t.Fatalf("second Put() error = %v, want %v", err, ErrDuplicateAttachment)
		}
		if duplicate != attachment {
			t.Fatalf("duplicate attachment = %#v, want %#v", duplicate, attachment)
		}
		got, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(attachment.Path)))
		if err != nil {
			t.Fatalf("ReadFile() error = %v", err)
		}
		if !attachmentBytesEqual(got, data) {
			t.Fatalf("stored bytes = %q, want %q", got, data)
		}
	})

	t.Run("changed bytes", func(t *testing.T) {
		root := t.TempDir()
		store := Store{Root: root}
		attachment, err := store.Put("trace", "application/json", []byte("{\"ok\":true}\n"))
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		writeFile(t, filepath.Join(root, filepath.FromSlash(attachment.Path)), []byte("{\"ok\":fals}\n"), 0o600)
		requireErrorIs(t, store.Verify(attachment), ErrInvalidAttachment)
	})

	t.Run("wrong kind and path", func(t *testing.T) {
		root := t.TempDir()
		store := Store{Root: root}
		attachment, err := store.Put("report", "application/json", []byte("{}\n"))
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		wrongKind := attachment
		wrongKind.Kind = "credentials"
		requireErrorIs(t, store.Verify(wrongKind), ErrInvalidAttachment)
		wrongPath := attachment
		wrongPath.Path = "../outside.bin"
		requireErrorIs(t, store.Verify(wrongPath), ErrInvalidAttachment)
	})

	t.Run("symlink traversal", func(t *testing.T) {
		root := t.TempDir()
		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(root, attachmentDirectory)); err != nil {
			t.Fatalf("Symlink() error = %v", err)
		}
		store := Store{Root: root}
		_, err := store.Put("log", "text/plain", []byte("must stay confined"))
		requireErrorIs(t, err, ErrInvalidStore)
	})
}

func TestBuilderRejectsInvalidReceipts(t *testing.T) {
	t.Run("zero receipt", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		_, err := fixture.builder.Build(context.Background(), execution.Receipt{})
		requireErrorIs(t, err, ErrInvalidEvidence)
	})

	t.Run("fabricated parsed receipt", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		receipt := fixture.issue(t, fixture.fields)
		data, err := receipt.AuthenticatedBytes()
		if err != nil {
			t.Fatalf("AuthenticatedBytes() error = %v", err)
		}
		parsed, err := execution.ParseReceipt(data)
		if err != nil {
			t.Fatalf("ParseReceipt() error = %v", err)
		}
		_, err = fixture.builder.Build(context.Background(), parsed)
		requireErrorIs(t, err, ErrInvalidEvidence)
	})

	t.Run("foreign receipt", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		foreignPublic, foreignPrivate := deterministicKey(t, 0x40)
		issuer, err := execution.NewReceiptIssuer(foreignPublic)
		if err != nil {
			t.Fatalf("NewReceiptIssuer() error = %v", err)
		}
		fields := cloneTestFields(fixture.fields)
		fields.RunnerDigest = ""
		fields.CandidateLockSHA256 = ""
		foreign := issueReceipt(t, issuer, foreignPrivate, fields)
		local := fixture.issue(t, fixture.fields)
		if err := local.Verify(); err != nil {
			t.Fatalf("local receipt Verify() error = %v", err)
		}
		_, err = fixture.builder.Build(context.Background(), foreign)
		requireErrorIs(t, err, ErrInvalidEvidence)
	})

	t.Run("replayed receipt", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		receipt := fixture.issue(t, fixture.fields)
		if _, err := fixture.builder.Build(context.Background(), receipt); err != nil {
			t.Fatalf("first Build() error = %v", err)
		}
		_, err := fixture.builder.Build(context.Background(), receipt)
		requireErrorIs(t, err, ErrInvalidEvidence)
	})

	t.Run("modified receipt", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		receipt := fixture.issue(t, fixture.fields)
		data, err := receipt.AuthenticatedBytes()
		if err != nil {
			t.Fatalf("AuthenticatedBytes() error = %v", err)
		}
		var document map[string]any
		decodeJSON(t, data, &document)
		document["scenario_id"] = "SCN-WAL-ORDER-999"
		changed := marshalJSON(t, document)
		_, err = execution.ParseReceipt(changed)
		if err == nil {
			t.Fatal("modified receipt was accepted")
		}
	})
}

func TestSyntheticRunnerIssuesAuthenticatedHarnessOnlyReceipt(t *testing.T) {
	fixture := newBuilderFixtureFor(t, true, "SCN-MEMBERSHIP-REASSIGNMENT-001", "OBL-MEMBERSHIP-REASSIGNMENT-PG-001")
	receipt, runResult, err, system := runSyntheticEvidence(t, fixture, blackbox.SyntheticCompliant)
	if err != nil {
		t.Fatalf("synthetic evidence run: %v", err)
	}
	if err := receipt.Verify(); err != nil {
		t.Fatalf("synthetic evidence receipt verification: %v", err)
	}
	if !runResult.Passed || system.RequestCount() == 0 {
		t.Fatal("synthetic runner did not complete the authenticated evidence execution")
	}
	fields, err := receipt.Fields()
	if err != nil {
		t.Fatalf("read synthetic receipt fields: %v", err)
	}
	if fields.EvidenceClass != execution.EvidenceClassHarnessOnly {
		t.Fatalf("synthetic receipt evidence class = %q, want %q", fields.EvidenceClass, execution.EvidenceClassHarnessOnly)
	}
	serialized, err := receipt.AuthenticatedBytes()
	if err != nil {
		t.Fatalf("serialize synthetic receipt: %v", err)
	}
	parsed, err := execution.ParseReceipt(serialized)
	if err != nil {
		t.Fatalf("parse synthetic receipt: %v", err)
	}
	parsedFields, err := parsed.Fields()
	if err != nil {
		t.Fatalf("read parsed synthetic receipt fields: %v", err)
	}
	if parsedFields.EvidenceClass != execution.EvidenceClassHarnessOnly {
		t.Fatalf("parsed synthetic receipt evidence class = %q, want %q", parsedFields.EvidenceClass, execution.EvidenceClassHarnessOnly)
	}
	var document map[string]any
	decodeJSON(t, serialized, &document)
	document["evidence_class"] = string(execution.EvidenceClassCandidate)
	if _, err := execution.ParseReceipt(marshalJSON(t, document)); err == nil {
		t.Fatal("synthetic receipt accepted a changed evidence class")
	}
	if _, err := fixture.builder.Build(context.Background(), receipt); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("Build() error = %v, want %v", err, ErrInvalidEvidence)
	}
	projected, err := fixture.builder.projectEvidence(receipt, fields)
	if err != nil {
		t.Fatalf("project harness receipt: %v", err)
	}
	path := fixture.writeEvidence(t, projected, "evidence/synthetic-harness-only.json")
	if err := ValidateEvidence(context.Background(), fixture.repoRoot, fixture.root, path); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("ValidateEvidence() error = %v, want %v", err, ErrInvalidEvidence)
	}
}

func runSyntheticEvidence(t *testing.T, fixture *builderFixture, fault blackbox.SyntheticFault) (execution.Receipt, blackbox.RunResult, error, *blackbox.SyntheticSystem) {
	t.Helper()
	provider, err := blackbox.NewHS256TokenProvider([]byte("runner-evidence-test-secret"), blackbox.Claims{"sub": "synthetic-user", "aud": "blackbox"})
	if err != nil {
		t.Fatalf("create token provider: %v", err)
	}
	token, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("create test token: %v", err)
	}
	system, err := blackbox.NewSyntheticSystem(context.Background(), fixture.scenario, blackbox.SyntheticOptions{Fault: fault, ExpectedToken: token})
	if err != nil {
		return execution.Receipt{}, blackbox.RunResult{}, err, nil
	}
	t.Cleanup(func() {
		if err := system.Close(); err != nil {
			t.Errorf("close compliant synthetic system: %v", err)
		}
	})

	trustedRunner, err := execution.NewTrustedRunner(fixture.privateKey)
	if err != nil {
		t.Fatalf("create trusted runner: %v", err)
	}
	bindings := make([]execution.ArtifactBinding, 0, len(fixture.obligation.ArtifactInventoryIDs))
	for _, inventoryID := range fixture.obligation.ArtifactInventoryIDs {
		payload := fixture.payloadByInvID[string(inventoryID)]
		bindings = append(bindings, execution.ArtifactBinding{
			InventoryID: string(inventoryID), ArtifactID: artifactID(string(inventoryID)),
			Role: artifactRole(t, fixture.repoRoot, string(inventoryID)), Path: payload.Path,
			MediaType: payload.MediaType, Size: payload.SizeBytes, SHA256: payload.SHA256,
		})
	}
	candidate, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
	if err != nil {
		t.Fatalf("load candidate: %v", err)
	}
	cell := candidate.SupportCells[string(*fixture.obligation.SupportCellID)]
	dimensions := make([]execution.EnvironmentDimension, 0, len(cell.Dimensions))
	for _, name := range sortedMapKeys(cell.Dimensions) {
		dimensions = append(dimensions, execution.EnvironmentDimension{Name: name, Value: cell.Dimensions[name]})
	}
	runnerArtifactBindings, err := fixture.builder.RunnerArtifactBindings()
	if err != nil {
		t.Fatalf("load locked runner artifact bindings: %v", err)
	}
	scenarioID, scenarioSHA256, err := fixture.builder.ScenarioBinding(string(fixture.scenario.ID))
	if err != nil {
		t.Fatalf("load locked scenario binding: %v", err)
	}
	runner, err := blackbox.NewRunner(blackbox.RunnerConfig{
		Client: &blackbox.Client{BaseURL: system.BaseURL(), HTTP: &http.Client{}, Tokens: provider},
		Recorder: blackbox.RecorderConfig{
			AttachmentRoot: filepath.Join(t.TempDir(), "private-raw-bodies"),
			MaxRecords:     64,
		},
		ArtifactBindings:       bindings,
		RunnerArtifactBindings: runnerArtifactBindings,
		ScenarioID:             scenarioID,
		ScenarioSHA256:         scenarioSHA256,
		EnvironmentDimensions:  dimensions,
		VectorResults:          fixture.fields.VectorResults,
		AttachmentPublisher:    fixture.builder.AttachmentPublisher(),
		CommandCapability:      fixture.builder.CommandCapability(),
		TrustedRunner:          trustedRunner,
		RunID:                  "RUN-SYNTHETIC-EVIDENCE-001",
		ExecutionLineageID:     "EXEC-SYNTHETIC-EVIDENCE-001",
		RunURL:                 "https://example.test/runs/synthetic-evidence",
		Attempt:                1,
		Now:                    func() time.Time { return testStart },
	})
	if err != nil {
		t.Fatalf("create evidence runner: %v", err)
	}
	receipt, runResult, runErr := runner.Run(context.Background(), fixture.scenario, fixture.obligation, fixture.builder.ReceiptIssuer())
	return receipt, runResult, runErr, system
}

func TestSyntheticFailureProducesAuthenticatedTerminalReceipt(t *testing.T) {
	fixture := newBuilderFixtureFor(t, true, "SCN-PULL-DIVERGENT-CHECKPOINTS-001", "OBL-PULL-DIVERGENT-PG-001")
	receipt, result, runErr, system := runSyntheticEvidence(t, fixture, blackbox.SyntheticDuplicateDelivery)
	if runErr == nil || result.Passed || result.Result != execution.ResultFailed {
		t.Fatalf("faulty synthetic result = %#v, error = %v", result, runErr)
	}
	if system.RequestCount() == 0 {
		t.Fatal("faulty synthetic system received no request")
	}
	if err := receipt.Verify(); err != nil {
		t.Fatalf("failed receipt verification: %v", err)
	}
	fields, err := receipt.Fields()
	if err != nil {
		t.Fatalf("failed receipt fields: %v", err)
	}
	if fields.ExitCode == 0 || fields.Result != execution.ResultFailed || fields.Command.ExitCode != 0 {
		t.Fatalf("failed receipt result = (%d, %q, command %d)", fields.ExitCode, fields.Result, fields.Command.ExitCode)
	}
	if fields.EvidenceClass != execution.EvidenceClassHarnessOnly {
		t.Fatalf("failed synthetic receipt evidence class = %q, want %q", fields.EvidenceClass, execution.EvidenceClassHarnessOnly)
	}
	if _, err := fixture.builder.Build(context.Background(), receipt); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("Build() error = %v, want %v", err, ErrInvalidEvidence)
	}
}

func TestEvidenceRunnerRejectsChangedScenarioObject(t *testing.T) {
	fixture := newBuilderFixtureFor(t, true, "SCN-PULL-DIVERGENT-CHECKPOINTS-001", "OBL-PULL-DIVERGENT-PG-001")
	changed := fixture.scenario
	changed.Title = "Changed scenario title"
	original := fixture.scenario
	fixture.scenario = changed
	_, _, err, system := runSyntheticEvidence(t, fixture, blackbox.SyntheticCompliant)
	fixture.scenario = original
	if err == nil || system == nil || system.RequestCount() != 0 {
		t.Fatalf("changed scenario run error = %v, requests = %d", err, system.RequestCount())
	}
}

func TestBuilderSerializesConcurrentReceiptConsumption(t *testing.T) {
	fixture := newBuilderFixture(t, false)
	receipt := fixture.issue(t, fixture.fields)
	var wait sync.WaitGroup
	errorsByCall := make(chan error, 2)
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := fixture.builder.Build(context.Background(), receipt)
			errorsByCall <- err
		}()
	}
	wait.Wait()
	close(errorsByCall)
	succeeded := 0
	failed := 0
	for err := range errorsByCall {
		if err == nil {
			succeeded++
		} else if errors.Is(err, ErrInvalidEvidence) {
			failed++
		} else {
			t.Fatalf("concurrent Build() error = %v", err)
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("concurrent Build() results = %d passed, %d failed", succeeded, failed)
	}
}

func TestBuilderRejectsAdversarialReceiptFields(t *testing.T) {
	tests := []struct {
		name       string
		withVector bool
		mutate     func(*builderFixture, *execution.ReceiptFields)
	}{
		{
			name: "fabricated passed assertion",
			mutate: func(fixture *builderFixture, fields *execution.ReceiptFields) {
				fields.Assertions[0].AssertionID = "ASSERT-UNRELATED-001"
			},
		},
		{
			name: "secret environment",
			mutate: func(_ *builderFixture, fields *execution.ReceiptFields) {
				fields.EnvironmentDimensions = []execution.EnvironmentDimension{{Name: "password", Value: "do-not-record"}}
			},
		},
		{
			name: "unallowlisted environment",
			mutate: func(_ *builderFixture, fields *execution.ReceiptFields) {
				fields.EnvironmentDimensions = []execution.EnvironmentDimension{{Name: "os", Value: "14.6"}}
			},
		},
		{
			name: "unrelated assertion",
			mutate: func(_ *builderFixture, fields *execution.ReceiptFields) {
				fields.Assertions[0].AssertionID = "ASSERT-UNRELATED-001"
			},
		},
		{
			name: "missing assertion",
			mutate: func(_ *builderFixture, fields *execution.ReceiptFields) {
				fields.Assertions = nil
			},
		},
		{
			name: "omitted support cell",
			mutate: func(_ *builderFixture, fields *execution.ReceiptFields) {
				fields.EnvironmentDimensions = nil
			},
		},
		{
			name:       "wrong vector",
			withVector: true,
			mutate: func(_ *builderFixture, fields *execution.ReceiptFields) {
				fields.VectorResults[0].SourceSHA256 = hashBytes([]byte("wrong vector source"))
			},
		},
		{
			name:       "missing vector",
			withVector: true,
			mutate: func(_ *builderFixture, fields *execution.ReceiptFields) {
				fields.VectorResults = nil
			},
		},
		{
			name: "stale artifact",
			mutate: func(_ *builderFixture, fields *execution.ReceiptFields) {
				fields.ArtifactBindings[0].SHA256 = hashBytes([]byte("stale artifact"))
			},
		},
		{
			name: "missing attachment",
			mutate: func(_ *builderFixture, fields *execution.ReceiptFields) {
				fields.Attachments = nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBuilderFixture(t, test.withVector)
			fields := cloneTestFields(fixture.fields)
			test.mutate(fixture, &fields)
			receipt, err := fixture.tryIssue(fields)
			if err != nil {
				return
			}
			_, err = fixture.builder.Build(context.Background(), receipt)
			requireErrorIs(t, err, ErrInvalidEvidence)
		})
	}
}

func TestBuilderRejectsWrongCandidateCommit(t *testing.T) {
	fixture := newBuilderFixture(t, false)
	authorization := fixture.runnerAuthorization(t)
	fixture.lock["source_commit"] = "abcdef0123456789abcdef0123456789abcdef01"
	fixture.writeLock()
	_, err := NewBuilder(BuilderConfig{
		RepoRoot:            fixture.repoRoot,
		CandidateRoot:       fixture.root,
		RunnerAuthorization: authorization,
		Generator: Generator{
			Name:         "evidence-generator",
			Version:      "1.0.0",
			BinarySHA256: hashBytes([]byte("evidence-generator")),
		},
	})
	if err == nil {
		t.Fatal("NewBuilder() accepted a changed candidate lock")
	}
}

func TestBuilderRejectsArtifactBytesChangedAfterCandidateLoad(t *testing.T) {
	fixture := newBuilderFixture(t, false)
	receipt := fixture.issue(t, fixture.fields)
	payload := fixture.payloadByInvID[string(fixture.obligation.ArtifactInventoryIDs[0])]
	path := filepath.Join(fixture.root, filepath.FromSlash(payload.Path))
	writeFile(t, path, []byte("changed candidate artifact bytes"), 0o600)
	_, err := fixture.builder.Build(context.Background(), receipt)
	requireErrorIs(t, err, ErrInvalidEvidence)
}

func TestBuilderRejectsUntrustedRunnerForLockedArtifact(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	candidate, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
	if err != nil {
		t.Fatalf("LoadCandidate() error = %v", err)
	}
	bindings, err := lockedRunnerArtifactBindings(candidate)
	if err != nil {
		t.Fatalf("lockedRunnerArtifactBindings() error = %v", err)
	}
	artifactDigest, err := execution.RunnerArtifactDigest(bindings)
	if err != nil {
		t.Fatalf("RunnerArtifactDigest() error = %v", err)
	}
	_, foreignPrivate := deterministicKey(t, 0x40)
	foreignRunner, err := execution.NewTrustedRunner(foreignPrivate)
	if err != nil {
		t.Fatalf("NewTrustedRunner() error = %v", err)
	}
	authorization, err := foreignRunner.AuthorizeExecutable(artifactDigest, candidate.ArtifactsByInventoryID["ARTDEF-CONFORMANCE-RUNNER-001"].Payloads[0].SHA256)
	if err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}
	_, err = NewBuilder(BuilderConfig{
		RepoRoot:            fixture.repoRoot,
		CandidateRoot:       fixture.root,
		RunnerAuthorization: authorization,
		Generator: Generator{
			Name:         "evidence-generator",
			Version:      "1.0.0",
			BinarySHA256: hashBytes([]byte("evidence-generator")),
		},
	})
	requireErrorIs(t, err, ErrInvalidCandidate)
}

func TestValidateEvidenceRejectsMissingOrChangedCandidateLockBinding(t *testing.T) {
	t.Run("missing signed candidate lock digest", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		issuer, err := execution.NewReceiptIssuerFromAuthorizationAndGenerator(
			fixture.runnerAuthorization(t),
			execution.GeneratorIdentity{Name: "evidence-generator", Version: "1.0.0", BinarySHA256: hashBytes([]byte("evidence-generator"))},
		)
		if err != nil {
			t.Fatalf("NewReceiptIssuerFromAuthorizationAndGenerator() error = %v", err)
		}
		fields := cloneTestFields(fixture.fields)
		fields.CandidateLockSHA256 = ""
		receipt := issueReceipt(t, issuer, fixture.privateKey, fields)
		authenticated, err := receipt.Fields()
		if err != nil {
			t.Fatalf("Receipt.Fields() error = %v", err)
		}
		projected, err := fixture.builder.projectEvidence(receipt, authenticated)
		if err != nil {
			t.Fatalf("projectEvidence() error = %v", err)
		}
		path := fixture.writeEvidence(t, projected, "evidence/missing-lock.json")
		requireErrorIs(t, ValidateEvidence(context.Background(), fixture.repoRoot, fixture.root, path), ErrInvalidEvidence)
	})

	t.Run("candidate lock changed after receipt", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		projected := fixture.build(t, fixture.fields)
		path := fixture.writeEvidence(t, projected, "evidence/changed-lock.json")
		fixture.lock["created_at"] = testStart.Add(time.Second).Format(time.RFC3339)
		fixture.writeLock()
		requireErrorIs(t, ValidateEvidence(context.Background(), fixture.repoRoot, fixture.root, path), ErrInvalidEvidence)
	})
}

func TestLoadCandidateRejectsDetailedContractMutation(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	detailed := fixture.lock["contract"].(map[string]any)
	requirements := detailed["requirements"].(map[string]any)
	requirements["sha256"] = hashBytes([]byte("changed detailed contract binding"))
	fixture.writeLock()
	_, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
	requireErrorIs(t, err, ErrInvalidCandidate)
}

func TestLoadCandidateRejectsAuthoritativeSourceDrift(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	path := fixture.scenarioPaths["SCN-WAL-ORDER-001"]
	authoritative := filepath.Join(fixture.repoRoot, filepath.FromSlash(path))
	data := readFile(t, authoritative)
	defer writeFile(t, authoritative, data, 0o600)
	writeFile(t, authoritative, append(data, '\n'), 0o600)
	_, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
	requireErrorIs(t, err, ErrInvalidCandidate)
}

func TestLoadCandidateRejectsScenarioOutsideCommittedCatalog(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	locked := fixture.lock["scenarios"].([]any)[0].(map[string]any)
	path := locked["path"].(string)
	data := append(readFile(t, filepath.Join(fixture.root, filepath.FromSlash(path))), '\n')
	writeCandidateFile(t, fixture.root, path, data, 0o600)
	locked["sha256"] = hashBytes(data)
	fixture.writeLock()
	_, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
	requireErrorIs(t, err, ErrInvalidCandidate)
}

func TestLoadCandidateRejectsDirtyRepositorySources(t *testing.T) {
	if _, err := scenarios.LoadAll(context.Background(), repositoryRoot(t)); err != nil {
		t.Fatalf("fixture repository lost Makefile before test: %v", err)
	}
	tests := []struct {
		name string
		path string
		data []byte
	}{
		{name: "same target Makefile recipe", path: "Makefile", data: append(append([]byte(nil), testFixtureMakefile...), []byte("\ntest-blackbox:\n\t@false\n")...)},
		{name: "unrelated untracked source", path: "untracked-source.txt", data: []byte("dirty\n")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
			path := filepath.Join(fixture.repoRoot, filepath.FromSlash(test.path))
			original := []byte(nil)
			if test.path == "Makefile" {
				original = readFile(t, path)
			}
			writeFile(t, path, test.data, 0o600)
			t.Cleanup(func() {
				if original != nil {
					writeFile(t, path, original, 0o600)
					return
				}
				_ = os.Remove(path)
			})
			_, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
			requireErrorIs(t, err, ErrInvalidCandidate)
		})
	}
}

func TestLoadCandidateIgnoresAmbientGitConfiguration(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.worktree")
	t.Setenv("GIT_CONFIG_VALUE_0", t.TempDir())
	t.Setenv("GIT_DIR", t.TempDir())
	t.Setenv("PATH", t.TempDir())
	t.Setenv("DYLD_INSERT_LIBRARIES", filepath.Join(t.TempDir(), "missing.dylib"))
	t.Setenv("LD_PRELOAD", filepath.Join(t.TempDir(), "missing.so"))
	if _, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root); err != nil {
		t.Fatalf("LoadCandidate() used ambient Git configuration: %v", err)
	}
}

func TestBuilderRejectsRepositoryDriftAfterCreation(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	builder := fixture.newBuilder(t)
	path := filepath.Join(fixture.repoRoot, "Makefile")
	data := readFile(t, path)
	writeFile(t, path, append(data, []byte("\n# changed after builder creation\n")...), 0o600)
	t.Cleanup(func() { writeFile(t, path, data, 0o600) })
	_, err := builder.CommandCapability().Execute(context.Background(), []string{"make", "test-blackbox"})
	if err == nil {
		t.Fatal("command capability accepted Makefile drift after builder creation")
	}
}

func TestBuilderCommandCapabilityRejectsMissingLaunchAndIssuerMismatch(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	builder := fixture.newBuilder(t)
	fields := newBuilderFixture(t, false).fields
	fields.ScenarioID = "SCN-WAL-ORDER-001"
	fields.ProofObligationID = "OBL-WAL-ORDER-PG-001"
	fields.MakeTarget = "test-blackbox"
	fields.Argv = []string{"make", "test-blackbox"}
	fields.RunnerDigest = builder.ReceiptIssuer().RunnerDigest()
	fields.CandidateLockSHA256 = builder.candidate.LockSHA256
	completion, err := execution.PrepareCompletion(builder.ReceiptIssuer(), fields)
	if err == nil || completion.SigningBytes() != nil {
		t.Fatal("issuer accepted receipt completion without a command launch")
	}

	other := fixture.newBuilder(t)
	if other.ReceiptIssuer().MatchesCommandCapability(builder.CommandCapability()) {
		t.Fatal("a distinct issuer matched the builder command capability")
	}
}

func TestValidateEvidenceRejectsChangedAttachmentAndMissingPredecessor(t *testing.T) {
	t.Run("valid evidence then changed attachment", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		evidence := fixture.build(t, fixture.fields)
		path := fixture.writeEvidence(t, evidence, "evidence/valid.json")
		if err := ValidateEvidence(context.Background(), fixture.repoRoot, fixture.root, path); err != nil {
			t.Fatalf("ValidateEvidence() error = %v", err)
		}
		writeFile(t, filepath.Join(fixture.root, filepath.FromSlash(evidence.Attachments[0].Path)), []byte("changed bytes with equal length"), 0o600)
		requireErrorIs(t, ValidateEvidence(context.Background(), fixture.repoRoot, fixture.root, path), ErrInvalidEvidence)
	})

	t.Run("missing predecessor", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		fields := cloneTestFields(fixture.fields)
		fields.Attempt = 2
		fields.PreviousEvidenceID = stringPointer("EVD-MISSING-PREDECESSOR-001")
		fields.RerunCause = stringPointer("ci-orchestrator-failure")
		fields.RerunDiagnosis = stringPointer("Runner host stopped before teardown.")
		fields.CorrectiveAction = stringPointer("Use a replacement runner host.")
		fields.RerunApproval = &execution.RerunApproval{
			ApproverIdentity: "github:release-operator",
			ApprovedAt:       testStart.Add(-time.Minute),
			URI:              "https://example.test/approvals/1",
		}
		evidence := fixture.build(t, fields)
		path := fixture.writeEvidence(t, evidence, "evidence/rerun.json")
		err := ValidateEvidence(context.Background(), fixture.repoRoot, fixture.root, path)
		if err == nil {
			t.Fatal("ValidateEvidence() accepted evidence without its predecessor")
		}
	})
}

func TestValidateEvidenceRejectsReusedAttachmentPath(t *testing.T) {
	fixture := newBuilderFixture(t, false)
	evidence := fixture.build(t, fixture.fields)
	duplicate := evidence.Attachments[0]
	duplicate.ID = "ATT-REPORT-" + duplicate.ID[len("ATT-LOG-"):]
	duplicate.Kind = "report"
	evidence.Attachments = append(evidence.Attachments, duplicate)
	evidence.AttachmentIDs = append(evidence.AttachmentIDs, duplicate.ID)
	evidence.Receipt.Fields.Attachments = append(evidence.Receipt.Fields.Attachments, execution.Attachment{
		ID: duplicate.ID, Kind: duplicate.Kind, Path: duplicate.Path, MediaType: duplicate.MediaType,
		SizeBytes: duplicate.SizeBytes, SHA256: duplicate.SHA256,
	})
	evidence.Receipt.Fields.AttachmentIDs = append(evidence.Receipt.Fields.AttachmentIDs, duplicate.ID)
	path := fixture.writeEvidence(t, evidence, "evidence/reused-attachment.json")
	err := ValidateEvidence(context.Background(), fixture.repoRoot, fixture.root, path)
	if err == nil {
		t.Fatal("ValidateEvidence() accepted a reused attachment path")
	}
}

func TestValidateEvidenceRejectsAuthenticatedSemanticMutations(t *testing.T) {
	tests := []struct {
		name       string
		withVector bool
		mutate     func(*testing.T, *execution.ReceiptFields)
	}{
		{
			name:       "empty required vector results",
			withVector: true,
			mutate: func(_ *testing.T, fields *execution.ReceiptFields) {
				fields.VectorResults = nil
			},
		},
		{
			name: "wrong execution attachment kind",
			mutate: func(t *testing.T, fields *execution.ReceiptFields) {
				if len(fields.Attachments) < 4 {
					t.Fatal("fixture does not contain a vector-results attachment")
				}
				fields.ExecutionArtifacts.LogAttachmentIDs = []string{fields.Attachments[3].ID}
			},
		},
		{
			name: "missing barrier",
			mutate: func(t *testing.T, fields *execution.ReceiptFields) {
				if len(fields.Replay.BarrierTraces) == 0 {
					t.Fatal("fixture does not contain a barrier trace")
				}
				fields.Replay.BarrierTraces = fields.Replay.BarrierTraces[1:]
				fields.ExecutionArtifacts.BarrierTraceAttachmentIDs = fields.ExecutionArtifacts.BarrierTraceAttachmentIDs[1:]
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBuilderFixture(t, test.withVector)
			valid := fixture.build(t, fixture.fields)
			fields := cloneTestFields(valid.Receipt.Fields)
			test.mutate(t, &fields)
			mutated := fixture.signedEvidence(t, fields)
			path := fixture.writeEvidence(t, mutated, "evidence/authenticated-semantic-mutation.json")
			requireErrorIs(t, ValidateEvidence(context.Background(), fixture.repoRoot, fixture.root, path), ErrInvalidEvidence)
		})
	}
}

func TestValidateCandidateRejectsNoEvidenceAndPartialClosure(t *testing.T) {
	t.Run("no evidence", func(t *testing.T) {
		fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
		fixture.writeManifest(t, nil, nil)
		requireErrorIs(t, ValidateCandidate(context.Background(), fixture.repoRoot, fixture.root), ErrInvalidCandidate)
	})

	t.Run("partial twenty-four-scenario closure", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		evidence := fixture.build(t, fixture.fields)
		path := fixture.writeEvidence(t, evidence, "evidence/partial.json")
		data := readFile(t, filepath.Join(fixture.root, filepath.FromSlash(path)))
		reference := evidenceReference(evidence, path, data)
		fixture.writeManifest(t, []EvidenceReference{reference}, nil)
		requireErrorIs(t, ValidateCandidate(context.Background(), fixture.repoRoot, fixture.root), ErrIncompleteCandidate)
	})
}

func TestValidateCandidateRejectsReusedEvidenceReceiptAndSemanticRerun(t *testing.T) {
	t.Run("reused evidence path", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		evidence := fixture.build(t, fixture.fields)
		path := fixture.writeEvidence(t, evidence, "evidence/one.json")
		data := readFile(t, filepath.Join(fixture.root, filepath.FromSlash(path)))
		reference := evidenceReference(evidence, path, data)
		second := reference
		second.EvidenceID = "EVD-SECOND-001"
		fixture.writeManifest(t, []EvidenceReference{reference, second}, nil)
		requireErrorIs(t, ValidateCandidate(context.Background(), fixture.repoRoot, fixture.root), ErrInvalidCandidate)
	})

	t.Run("replayed receipt", func(t *testing.T) {
		fixture := newBuilderFixture(t, false)
		evidence := fixture.build(t, fixture.fields)
		firstPath := fixture.writeEvidence(t, evidence, "evidence/first.json")
		second := evidence
		second.EvidenceID = "EVD-SECOND-001"
		second.Run.ID = "RUN-SECOND-001"
		second.Run.ExecutionLineageID = "EXEC-SECOND-001"
		secondPath := fixture.writeEvidence(t, second, "evidence/second.json")
		firstData := readFile(t, filepath.Join(fixture.root, filepath.FromSlash(firstPath)))
		secondData := readFile(t, filepath.Join(fixture.root, filepath.FromSlash(secondPath)))
		fixture.writeManifest(t, []EvidenceReference{
			evidenceReference(evidence, firstPath, firstData),
			evidenceReference(second, secondPath, secondData),
		}, nil)
		requireErrorIs(t, ValidateCandidate(context.Background(), fixture.repoRoot, fixture.root), ErrInvalidCandidate)
	})

	t.Run("semantic failure relabeled as infrastructure", func(t *testing.T) {
		failed := Evidence{
			CandidateID:       "candidate",
			ScenarioID:        "scenario",
			ProofObligationID: "obligation",
			Run:               Run{Attempt: 1, Result: execution.ResultError, ExitCode: 1, CompletedAt: testStart},
			Assertions:        []execution.AssertionResult{{AssertionID: "assertion", Outcome: "failed"}},
		}
		previous := failed.EvidenceID
		passed := failed
		passed.EvidenceID = "EVD-PASSED-001"
		passed.Run = Run{
			Attempt:            2,
			Result:             execution.ResultPassed,
			ExitCode:           0,
			StartedAt:          testStart.Add(2 * time.Minute),
			PreviousEvidenceID: &previous,
			RerunApproval: &execution.RerunApproval{
				ApproverIdentity: "github:release-operator",
				ApprovedAt:       testStart.Add(time.Minute),
				URI:              "https://example.test/approvals/2",
			},
		}
		passed.Assertions = []execution.AssertionResult{{AssertionID: "assertion", Outcome: "passed"}}
		requireErrorIs(t, validateTerminalLineage([]Evidence{failed, passed}), ErrInvalidCandidate)
	})
}

func TestValidateTerminalLineageRejectsChangedRerunBindings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Evidence)
	}{
		{name: "evidence class", mutate: func(item *Evidence) { item.EvidenceClass = execution.EvidenceClassHarnessOnly }},
		{name: "runner", mutate: func(item *Evidence) { item.RunnerDigest = strings.Repeat("c", 64) }},
		{name: "runner artifact", mutate: func(item *Evidence) { item.Receipt.Fields.RunnerArtifactSHA256 = strings.Repeat("d", 64) }},
		{name: "runner executable", mutate: func(item *Evidence) { item.Receipt.Fields.RunnerExecutableSHA256 = strings.Repeat("e", 64) }},
		{name: "generator", mutate: func(item *Evidence) { item.Generator.Version = "1.0.1" }},
		{name: "seed", mutate: func(item *Evidence) { item.Seed = stringPointer("changed-seed") }},
		{name: "vector", mutate: func(item *Evidence) {
			item.VectorResults = []execution.VectorResult{{VectorSetID: "VSET-CANONICAL-001", Language: "go", Outcome: "passed", ExecutedCount: 1, PassedCount: 1}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			predecessor, successor := rerunEvidencePair()
			test.mutate(&successor)
			requireErrorIs(t, validateTerminalLineage([]Evidence{predecessor, successor}), ErrInvalidCandidate)
		})
	}
}

func TestValidateTerminalLineageRejectsFailedPredecessorProof(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Evidence)
	}{
		{name: "vector", mutate: func(item *Evidence) {
			item.VectorResults = []execution.VectorResult{{VectorSetID: "VSET-CANONICAL-001", Language: "go", Outcome: "failed", ExecutedCount: 1, FailedCount: 1}}
		}},
		{name: "performance", mutate: func(item *Evidence) {
			item.PerformanceResults = []execution.PerformanceResult{{BudgetID: "BUDGET-TEST-001", Outcome: "failed"}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			predecessor, successor := rerunEvidencePair()
			test.mutate(&predecessor)
			test.mutate(&successor)
			requireErrorIs(t, validateTerminalLineage([]Evidence{predecessor, successor}), ErrInvalidCandidate)
		})
	}
}

func rerunEvidencePair() (Evidence, Evidence) {
	predecessor := Evidence{
		EvidenceID:                 "EVD-INFRASTRUCTURE-001",
		EvidenceClass:              execution.EvidenceClassCandidate,
		CandidateID:                "candidate",
		ReleaseVersion:             "0.3.0",
		ProtocolVersion:            3,
		ContractSnapshotSHA256:     strings.Repeat("a", 64),
		SourceCommit:               strings.Repeat("b", 40),
		ScenarioID:                 "scenario",
		ProofObligationID:          "obligation",
		ProofType:                  "server-black-box",
		RequirementIDs:             []string{"SYNC-TEST-001"},
		Generator:                  Generator{Name: "generator", Version: "1.0.0", BinarySHA256: strings.Repeat("e", 64)},
		RunnerDigest:               strings.Repeat("f", 64),
		Assertions:                 []execution.AssertionResult{{AssertionID: "assertion", Outcome: "passed"}},
		ArtifactBindings:           []execution.ArtifactBinding{{InventoryID: "inventory", ArtifactID: "artifact", Path: "artifact.bin", SHA256: strings.Repeat("1", 64)}},
		ExecutionArtifacts:         execution.ExecutionArtifacts{LogAttachmentIDs: []string{"log"}},
		Replay:                     execution.ReplayEvidence{BarrierTraces: []execution.BarrierTrace{}},
		PerformanceResults:         []execution.PerformanceResult{},
		RequiredMeasurementResults: []execution.RequiredMeasurementResult{},
		VectorResults:              []execution.VectorResult{},
		Run: Run{
			ID:                 "RUN-INFRASTRUCTURE-001",
			ExecutionLineageID: "EXECUTION-001",
			URL:                "https://example.test/runs/1",
			MakeTarget:         "test-example",
			Argv:               []string{"make", "test-example"},
			Attempt:            1,
			StartedAt:          testStart,
			CompletedAt:        testStart.Add(time.Minute),
			Result:             execution.ResultError,
			ExitCode:           1,
		},
		Receipt: ReceiptProjection{Fields: execution.ReceiptFields{EvidenceClass: execution.EvidenceClassCandidate, RunnerArtifactSHA256: strings.Repeat("2", 64)}},
	}
	previous := predecessor.EvidenceID
	successor := predecessor
	successor.EvidenceID = "EVD-PASSED-001"
	successor.Run = predecessor.Run
	successor.Run.ID = "RUN-PASSED-001"
	successor.Run.Attempt = 2
	successor.Run.StartedAt = testStart.Add(3 * time.Minute)
	successor.Run.CompletedAt = testStart.Add(4 * time.Minute)
	successor.Run.Result = execution.ResultPassed
	successor.Run.ExitCode = 0
	successor.Run.PreviousEvidenceID = &previous
	successor.Run.RerunCause = stringPointer("ci-orchestrator-failure")
	successor.Run.RerunApproval = &execution.RerunApproval{ApproverIdentity: "github:release-operator", ApprovedAt: testStart.Add(2 * time.Minute), URI: "https://example.test/approvals/1"}
	return predecessor, successor
}

func TestValidateCandidateRejectsSelfDeclaredVerified(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	fixture.lock["verified"] = true
	fixture.writeLock()
	requireErrorIs(t, ValidateCandidate(context.Background(), fixture.repoRoot, fixture.root), ErrInvalidCandidate)
}

func TestLoadCandidateRejectsInvalidAttestationBindings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*candidateFixture)
	}{
		{
			name: "changed attestation bytes",
			mutate: func(fixture *candidateFixture) {
				attestation := fixture.attestations[0].(map[string]any)
				writeCandidateFile(fixture.t, fixture.root, attestation["path"].(string), []byte("changed attestation\n"), 0o600)
			},
		},
		{
			name: "missing attestation bytes",
			mutate: func(fixture *candidateFixture) {
				attestation := fixture.attestations[0].(map[string]any)
				if err := os.Remove(filepath.Join(fixture.root, filepath.FromSlash(attestation["path"].(string)))); err != nil {
					fixture.t.Fatalf("Remove() error = %v", err)
				}
			},
		},
		{
			name: "changed Sigstore bundle bytes",
			mutate: func(fixture *candidateFixture) {
				attestation := fixture.attestations[0].(map[string]any)
				verification := attestation["sigstore_verification"].(map[string]any)
				writeCandidateFile(fixture.t, fixture.root, verification["bundle_path"].(string), []byte("changed bundle\n"), 0o600)
			},
		},
		{
			name: "missing Sigstore bundle bytes",
			mutate: func(fixture *candidateFixture) {
				attestation := fixture.attestations[0].(map[string]any)
				verification := attestation["sigstore_verification"].(map[string]any)
				if err := os.Remove(filepath.Join(fixture.root, filepath.FromSlash(verification["bundle_path"].(string)))); err != nil {
					fixture.t.Fatalf("Remove() error = %v", err)
				}
			},
		},
		{
			name: "reused payload path",
			mutate: func(fixture *candidateFixture) {
				attestation := fixture.attestations[0].(map[string]any)
				artifact := fixture.artifacts[0].(map[string]any)
				payload := artifact["payloads"].([]any)[0].(map[string]any)
				attestation["path"] = payload["path"]
				fixture.writeLock()
			},
		},
		{
			name: "incomplete payload subject set",
			mutate: func(fixture *candidateFixture) {
				artifact := fixture.artifacts[0].(map[string]any)
				path := "artifacts/extra-payload.bin"
				data := []byte("second locked payload\n")
				writeCandidateFile(fixture.t, fixture.root, path, data, 0o600)
				payloads := artifact["payloads"].([]any)
				artifact["payloads"] = append(payloads, map[string]any{
					"path": path, "media_type": "application/octet-stream",
					"size_bytes": int64(len(data)), "sha256": hashBytes(data),
				})
				fixture.writeLock()
			},
		},
		{
			name: "duplicate attestation kind",
			mutate: func(fixture *candidateFixture) {
				attestation := fixture.attestations[1].(map[string]any)
				attestation["kind"] = "sbom"
				attestation["format"] = "spdx-json"
				attestation["media_type"] = "application/spdx+json"
				fixture.writeLock()
			},
		},
		{
			name: "missing attestation kind",
			mutate: func(fixture *candidateFixture) {
				fixture.lock["attestations"] = fixture.attestations[:len(fixture.attestations)-1]
				fixture.writeLock()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
			test.mutate(fixture)
			_, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
			requireErrorIs(t, err, ErrInvalidCandidate)
		})
	}
}

func TestValidateCandidateRejectsChangedFinalManifestAttestationMetadata(t *testing.T) {
	fixture := newBuilderFixture(t, false)
	evidence := fixture.build(t, fixture.fields)
	path := fixture.writeEvidence(t, evidence, "evidence/attestation-binding.json")
	data := readFile(t, filepath.Join(fixture.root, filepath.FromSlash(path)))
	fixture.writeManifest(t, []EvidenceReference{evidenceReference(evidence, path, data)}, func(manifest map[string]any) {
		attestation := manifest["attestations"].([]any)[0].(map[string]any)
		attestation["format"] = "cyclonedx-json"
		attestation["media_type"] = "application/vnd.cyclonedx+json"
	})
	requireErrorIs(t, ValidateCandidate(context.Background(), fixture.repoRoot, fixture.root), ErrInvalidCandidate)
}

func TestValidateCandidateRejectsChangedFinalManifestRunnerDigest(t *testing.T) {
	fixture := newBuilderFixture(t, false)
	projected := fixture.build(t, fixture.fields)
	path := fixture.writeEvidence(t, projected, "evidence/runner-binding.json")
	data := readFile(t, filepath.Join(fixture.root, filepath.FromSlash(path)))
	fixture.writeManifest(t, []EvidenceReference{evidenceReference(projected, path, data)}, func(manifest map[string]any) {
		manifest["runner_digest"] = strings.Repeat("f", 64)
	})
	requireErrorIs(t, ValidateCandidate(context.Background(), fixture.repoRoot, fixture.root), ErrInvalidCandidate)
}

func TestValidateCandidateRejectsChangedFinalManifestGenerator(t *testing.T) {
	fixture := newBuilderFixture(t, false)
	projected := fixture.build(t, fixture.fields)
	path := fixture.writeEvidence(t, projected, "evidence/generator-binding.json")
	data := readFile(t, filepath.Join(fixture.root, filepath.FromSlash(path)))
	fixture.writeManifest(t, []EvidenceReference{evidenceReference(projected, path, data)}, func(manifest map[string]any) {
		manifest["generator"] = map[string]any{
			"name": "different-writer", "version": "1.0.0", "binary_sha256": hashBytes([]byte("different-writer")),
		}
	})
	requireErrorIs(t, ValidateCandidate(context.Background(), fixture.repoRoot, fixture.root), ErrInvalidCandidate)
}

func TestBuilderDoesNotRequireEvidenceGeneratorToMatchLockGenerator(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	if fixture.lock["generator"] == nil {
		t.Fatal("fixture lock generator is missing")
	}
	_ = fixture.newBuilder(t)
}

func TestBuilderRejectsCandidateRootReplacement(t *testing.T) {
	fixture := newBuilderFixture(t, false)
	receipt := fixture.issue(t, fixture.fields)
	replacement := t.TempDir()
	if err := os.Remove(replacement); err != nil {
		t.Fatalf("Remove(replacement root) error = %v", err)
	}
	if err := os.Rename(fixture.root, replacement); err != nil {
		t.Fatalf("Rename(candidate root) error = %v", err)
	}
	if err := os.Mkdir(fixture.root, 0o700); err != nil {
		t.Fatalf("Mkdir(replacement root) error = %v", err)
	}
	_, err := fixture.builder.Build(context.Background(), receipt)
	requireErrorIs(t, err, ErrInvalidEvidence)
}

func TestLoadCandidateRejectsExtraBaselineDirectory(t *testing.T) {
	fixture := newCandidateFixture(t, false, "SCN-WAL-ORDER-001")
	writeCandidateFile(t, fixture.root, "baseline/report.json", []byte("{}\n"), 0o600)
	_, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
	requireErrorIs(t, err, ErrInvalidCandidate)
}

func TestValidateEvidenceRejectsCandidateRootReplacement(t *testing.T) {
	fixture := newBuilderFixture(t, false)
	projected := fixture.build(t, fixture.fields)
	path := fixture.writeEvidence(t, projected, "evidence/root-replacement.json")
	candidate, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
	if err != nil {
		t.Fatalf("LoadCandidate() error = %v", err)
	}
	replacement := t.TempDir()
	if err := os.Remove(replacement); err != nil {
		t.Fatalf("Remove(replacement root) error = %v", err)
	}
	if err := os.Rename(fixture.root, replacement); err != nil {
		t.Fatalf("Rename(candidate root) error = %v", err)
	}
	if err := os.Mkdir(fixture.root, 0o700); err != nil {
		t.Fatalf("Mkdir(replacement root) error = %v", err)
	}
	requireErrorIs(t, validateEvidence(context.Background(), fixture.repoRoot, candidate, projected, path, nil), ErrInvalidEvidence)
}

func newBuilderFixture(t *testing.T, withVector bool) *builderFixture {
	t.Helper()
	scenarioID := "SCN-WAL-ORDER-001"
	obligationID := "OBL-WAL-ORDER-PG-001"
	if withVector {
		scenarioID = "SCN-PUSH-RESPONSE-LOSS-001"
		obligationID = "OBL-PUSH-RESPONSE-LOSS-PG-001"
	}
	return newBuilderFixtureFor(t, withVector, scenarioID, obligationID)
}

func newBuilderFixtureFor(t *testing.T, lockAllScenarios bool, scenarioID, obligationID string) *builderFixture {
	t.Helper()
	fixture := newCandidateFixture(t, lockAllScenarios, scenarioID)
	scenario := fixture.scenarioByID(t, scenarioID)
	obligation, found := obligationByID(scenario, obligationID)
	if !found {
		t.Fatalf("obligation %s is missing", obligationID)
	}
	builder := fixture.newBuilder(t)
	fields := fixture.validFields(t, builder, scenario, obligation)
	return &builderFixture{
		candidateFixture: fixture,
		builder:          builder,
		scenario:         scenario,
		obligation:       obligation,
		fields:           fields,
	}
}

func newCandidateFixture(t *testing.T, lockAllScenarios bool, scenarioID string) *candidateFixture {
	t.Helper()
	repoRoot := repositoryRoot(t)
	root := t.TempDir()
	publicKey, privateKey := deterministicKey(t, 0)
	allScenarios, err := scenarios.LoadAll(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("scenarios.LoadAll() error = %v", err)
	}
	selected := allScenarios
	if !lockAllScenarios {
		selected = selectScenarios(t, allScenarios, scenarioID)
	}
	fixture := &candidateFixture{
		t:              t,
		repoRoot:       repoRoot,
		root:           root,
		privateKey:     privateKey,
		publicKey:      publicKey,
		scenarios:      selected,
		scenarioPaths:  make(map[string]string),
		payloadByInvID: make(map[string]lockedPayloadDocument),
	}
	fixture.copyLockedScenarios()
	fixture.supportCells = fixture.makeSupportCells()
	fixture.artifacts = fixture.makeArtifacts()
	fixture.attestations = fixture.makeAttestations()
	snapshot, err := contract.BuildSnapshot(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("contract.BuildSnapshot() error = %v", err)
	}
	digest, err := snapshot.SHA256()
	if err != nil {
		t.Fatalf("Snapshot.SHA256() error = %v", err)
	}
	var contractDocument any
	decodeJSON(t, marshalJSON(t, snapshot), &contractDocument)
	contractMap := contractDocument.(map[string]any)
	delete(contractMap, "release_version")
	delete(contractMap, "protocol_version")
	contractMap["snapshot_sha256"] = fmt.Sprintf("%x", digest)
	fixture.lock = map[string]any{
		"$schema":                 "https://synchro.dev/conformance/schemas/rc-candidate-lock-v1.schema.json",
		"schema_version":          1,
		"candidate_id":            testCandidateID,
		"release_version":         "0.3.0",
		"protocol_version":        3,
		"source_commit":           repositoryCommit(t, repoRoot),
		"runner_digest":           hashBytes(publicKey),
		"created_at":              testStart.Format(time.RFC3339),
		"generator":               map[string]any{"name": "fixture-writer", "version": "1.0.0", "binary_sha256": hashBytes([]byte("fixture-writer"))},
		"trusted_rerun_approvers": []string{"github:release-operator"},
		"contract":                contractMap,
		"scenarios":               fixture.lockedScenarioDocuments(),
		"resolved_support_cells":  fixture.supportCells,
		"artifacts":               fixture.artifacts,
		"attestations":            fixture.attestations,
	}
	fixture.writeLock()
	if _, err := LoadCandidate(context.Background(), repoRoot, root); err != nil {
		t.Fatalf("LoadCandidate() fixture error = %v", err)
	}
	return fixture
}

func (fixture *candidateFixture) newBuilder(t *testing.T) *Builder {
	t.Helper()
	builder, err := NewBuilder(BuilderConfig{
		RepoRoot:            fixture.repoRoot,
		CandidateRoot:       fixture.root,
		RunnerAuthorization: fixture.runnerAuthorization(t),
		Generator: Generator{
			Name:         "evidence-generator",
			Version:      "1.0.0",
			BinarySHA256: hashBytes([]byte("evidence-generator")),
		},
	})
	if err != nil {
		t.Fatalf("NewBuilder() error = %v", err)
	}
	return builder
}

func (fixture *candidateFixture) runnerAuthorization(t *testing.T) execution.RunnerAuthorization {
	t.Helper()
	candidate, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
	if err != nil {
		t.Fatalf("LoadCandidate() error = %v", err)
	}
	bindings, err := lockedRunnerArtifactBindings(candidate)
	if err != nil {
		t.Fatalf("lockedRunnerArtifactBindings() error = %v", err)
	}
	digest, err := execution.RunnerArtifactDigest(bindings)
	if err != nil {
		t.Fatalf("RunnerArtifactDigest() error = %v", err)
	}
	runner, err := execution.NewTrustedRunner(fixture.privateKey)
	if err != nil {
		t.Fatalf("NewTrustedRunner() error = %v", err)
	}
	authorization, err := runner.AuthorizeExecutable(digest, candidate.ArtifactsByInventoryID["ARTDEF-CONFORMANCE-RUNNER-001"].Payloads[0].SHA256)
	if err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}
	return authorization
}

func (fixture *candidateFixture) validFields(t *testing.T, builder *Builder, scenario scenarios.Scenario, obligation scenarios.ProofObligation) execution.ReceiptFields {
	t.Helper()
	attachments := fixture.makeRunAttachments(t)
	fields := execution.ReceiptFields{
		EvidenceClass:      execution.EvidenceClassCandidate,
		ScenarioID:         string(scenario.ID),
		ProofObligationID:  string(obligation.ObligationID),
		MakeTarget:         obligation.MakeTarget,
		Argv:               append([]string(nil), obligation.Argv...),
		StartedAt:          testStart,
		CompletedAt:        testStart.Add(1500 * time.Millisecond),
		ExitCode:           0,
		Result:             execution.ResultPassed,
		RunID:              "RUN-EVIDENCE-001",
		ExecutionLineageID: "EXEC-EVIDENCE-001",
		RunURL:             "https://example.test/runs/1",
		Attempt:            1,
		ExecutionArtifacts: &execution.ExecutionArtifacts{
			LogAttachmentIDs:          []string{attachments[0].ID},
			TraceAttachmentIDs:        []string{attachments[1].ID},
			ReplayDataAttachmentIDs:   []string{attachments[2].ID},
			BarrierTraceAttachmentIDs: nil,
		},
		Replay:               &execution.ReplayEvidence{BarrierTraces: nil},
		VectorResults:        []execution.VectorResult{},
		HTTPObservations:     []execution.HTTPObservation{},
		Observations:         []execution.Observation{},
		PerformanceResults:   []execution.PerformanceResult{},
		RequiredMeasurements: []execution.RequiredMeasurementResult{},
	}
	for _, id := range obligation.AssertionIDs {
		fields.Assertions = append(fields.Assertions, execution.AssertionResult{AssertionID: string(id), Outcome: "passed"})
	}
	for _, attachment := range attachments {
		fields.Attachments = append(fields.Attachments, execution.Attachment{
			ID: attachment.ID, Kind: attachment.Kind, Path: attachment.Path,
			MediaType: attachment.MediaType, SizeBytes: attachment.SizeBytes, SHA256: attachment.SHA256,
		})
		fields.AttachmentIDs = append(fields.AttachmentIDs, attachment.ID)
	}
	barrierIndex := 4
	for _, barrier := range scenario.BarrierPlan.Barriers {
		fields.ExecutionArtifacts.BarrierTraceAttachmentIDs = append(fields.ExecutionArtifacts.BarrierTraceAttachmentIDs, attachments[barrierIndex].ID)
		fields.Replay.BarrierTraces = append(fields.Replay.BarrierTraces, execution.BarrierTrace{BarrierID: string(barrier.ID), AttachmentID: attachments[barrierIndex].ID})
		barrierIndex++
	}
	for _, inventoryID := range obligation.ArtifactInventoryIDs {
		payload, found := fixture.payloadByInvID[string(inventoryID)]
		if !found {
			t.Fatalf("fixture payload for %s is missing", inventoryID)
		}
		fields.ArtifactBindings = append(fields.ArtifactBindings, execution.ArtifactBinding{
			InventoryID: string(inventoryID),
			ArtifactID:  artifactID(string(inventoryID)),
			Role:        artifactRole(t, fixture.repoRoot, string(inventoryID)),
			Path:        payload.Path,
			MediaType:   payload.MediaType,
			Size:        payload.SizeBytes,
			SHA256:      payload.SHA256,
		})
	}
	if obligation.SupportCellID != nil {
		candidate, err := LoadCandidate(context.Background(), fixture.repoRoot, fixture.root)
		if err != nil {
			t.Fatalf("LoadCandidate() error = %v", err)
		}
		cell := candidate.SupportCells[string(*obligation.SupportCellID)]
		for _, name := range sortedMapKeys(cell.Dimensions) {
			fields.EnvironmentDimensions = append(fields.EnvironmentDimensions, execution.EnvironmentDimension{Name: name, Value: cell.Dimensions[name]})
		}
	}
	if len(obligation.RequiredVectorSetIDs) != 0 {
		catalog, err := vectors.Load(context.Background(), fixture.repoRoot)
		if err != nil {
			t.Fatalf("vectors.Load() error = %v", err)
		}
		for _, id := range obligation.RequiredVectorSetIDs {
			set, found := catalog.Set(id)
			if !found {
				t.Fatalf("vector set %s is missing", id)
			}
			fields.VectorResults = append(fields.VectorResults, execution.VectorResult{
				VectorSetID:        string(id),
				SourceSHA256:       set.SourceSHA256,
				AggregateSHA256:    set.AggregateSHA256,
				Language:           "go",
				ArtifactID:         artifactID("ARTDEF-CONFORMANCE-RUNNER-001"),
				Outcome:            "passed",
				ResultAttachmentID: attachments[3].ID,
				ExecutedCount:      len(set.Vectors),
				PassedCount:        len(set.Vectors),
			})
		}
	}
	fields.RunnerDigest = builder.ReceiptIssuer().RunnerDigest()
	fields.CandidateLockSHA256 = hashBytes(fixture.lockBytes)
	return fields
}

func (fixture *builderFixture) issue(t *testing.T, fields execution.ReceiptFields) execution.Receipt {
	t.Helper()
	return fixture.commandReceipt(t, fields)
}

func (fixture *builderFixture) tryIssue(fields execution.ReceiptFields) (execution.Receipt, error) {
	observed, err := fixture.builder.CommandCapability().Execute(context.Background(), fields.Argv)
	if err != nil {
		return execution.Receipt{}, err
	}
	fields.Argv = append([]string(nil), observed.Argv...)
	fields.MakeTarget = observed.Argv[1]
	fields.ExitCode = observed.ExitCode
	fields.Command = observed.Observation()
	fields.StartedAt = observed.StartedAt
	fields.CompletedAt = observed.CompletedAt
	if observed.ExitCode == 0 {
		fields.Result = execution.ResultPassed
	} else {
		fields.Result = execution.ResultFailed
		for index := range fields.Assertions {
			fields.Assertions[index].Outcome = "failed"
		}
	}
	completion, err := execution.PrepareCompletion(fixture.builder.ReceiptIssuer(), fields)
	if err != nil {
		return execution.Receipt{}, err
	}
	runner, err := execution.NewTrustedRunner(fixture.privateKey)
	if err != nil {
		return execution.Receipt{}, err
	}
	return runner.CompleteReceipt(fixture.builder.ReceiptIssuer(), completion)
}

func (fixture *builderFixture) build(t *testing.T, fields execution.ReceiptFields) Evidence {
	t.Helper()
	receipt := fixture.issue(t, fields)
	evidence, err := fixture.builder.Build(context.Background(), receipt)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	return evidence
}

func (fixture *builderFixture) commandReceipt(t *testing.T, fields execution.ReceiptFields) execution.Receipt {
	t.Helper()
	observed, err := fixture.builder.CommandCapability().Execute(context.Background(), fields.Argv)
	if err != nil {
		t.Fatalf("execute evidence command: %v", err)
	}
	fields.Argv = append([]string(nil), observed.Argv...)
	fields.MakeTarget = observed.Argv[1]
	fields.ExitCode = observed.ExitCode
	fields.Command = observed.Observation()
	fields.StartedAt = observed.StartedAt
	fields.CompletedAt = observed.CompletedAt
	if observed.ExitCode == 0 {
		fields.Result = execution.ResultPassed
	} else {
		fields.Result = execution.ResultFailed
		for index := range fields.Assertions {
			fields.Assertions[index].Outcome = "failed"
		}
	}
	completion, err := execution.PrepareCompletion(fixture.builder.ReceiptIssuer(), fields)
	if err != nil {
		t.Fatalf("PrepareCompletion() error = %v", err)
	}
	runner, err := execution.NewTrustedRunner(fixture.privateKey)
	if err != nil {
		t.Fatalf("NewTrustedRunner() error = %v", err)
	}
	receipt, err := runner.CompleteReceipt(fixture.builder.ReceiptIssuer(), completion)
	if err != nil {
		t.Fatalf("CompleteReceipt() error = %v", err)
	}
	return receipt
}

func (fixture *builderFixture) signedEvidence(t *testing.T, fields execution.ReceiptFields) Evidence {
	t.Helper()
	issuer, err := execution.NewReceiptIssuerFromAuthorizationAndGeneratorAndCandidateLock(
		fixture.runnerAuthorization(t),
		execution.GeneratorIdentity{Name: "evidence-generator", Version: "1.0.0", BinarySHA256: hashBytes([]byte("evidence-generator"))},
		hashBytes(fixture.lockBytes),
	)
	if err != nil {
		t.Fatalf("NewReceiptIssuerFromAuthorizationAndGenerator() error = %v", err)
	}
	receipt := issueReceipt(t, issuer, fixture.privateKey, fields)
	authenticated, err := receipt.Fields()
	if err != nil {
		t.Fatalf("Receipt.Fields() error = %v", err)
	}
	evidence, err := fixture.builder.projectEvidence(receipt, authenticated)
	if err != nil {
		t.Fatalf("projectEvidence() error = %v", err)
	}
	return evidence
}

func (fixture *candidateFixture) copyLockedScenarios() {
	fixture.t.Helper()
	for _, scenario := range fixture.scenarios {
		path := scenarioPathByID(fixture.t, fixture.repoRoot, string(scenario.ID))
		data := readFile(fixture.t, filepath.Join(fixture.repoRoot, filepath.FromSlash(path)))
		writeCandidateFile(fixture.t, fixture.root, path, data, 0o600)
		fixture.scenarioPaths[string(scenario.ID)] = path
	}
}

func (fixture *candidateFixture) lockedScenarioDocuments() []any {
	fixture.t.Helper()
	documents := make([]any, 0, len(fixture.scenarios))
	for _, scenario := range fixture.scenarios {
		path := fixture.scenarioPaths[string(scenario.ID)]
		data := readFile(fixture.t, filepath.Join(fixture.root, filepath.FromSlash(path)))
		documents = append(documents, map[string]any{
			"scenario_id": string(scenario.ID),
			"path":        path,
			"sha256":      hashBytes(data),
		})
	}
	sort.Slice(documents, func(left, right int) bool {
		return documents[left].(map[string]any)["scenario_id"].(string) < documents[right].(map[string]any)["scenario_id"].(string)
	})
	return documents
}

func (fixture *candidateFixture) makeSupportCells() []any {
	fixture.t.Helper()
	bundle, err := contract.Load(context.Background(), fixture.repoRoot)
	if err != nil {
		fixture.t.Fatalf("contract.Load() error = %v", err)
	}
	var cells []any
	for _, cell := range bundle.Support.Cells {
		if cell.Policy != "required" {
			continue
		}
		dimensions := supportDimensions(string(cell.ID))
		items := make([]any, 0, len(dimensions))
		for _, name := range sortedMapKeys(dimensions) {
			items = append(items, map[string]any{"name": name, "version": dimensions[name]})
		}
		cells = append(cells, map[string]any{"support_cell_id": string(cell.ID), "dimensions": items})
	}
	return cells
}

func (fixture *candidateFixture) makeArtifacts() []any {
	fixture.t.Helper()
	bundle, err := contract.Load(context.Background(), fixture.repoRoot)
	if err != nil {
		fixture.t.Fatalf("contract.Load() error = %v", err)
	}
	artifacts := make([]any, 0, len(bundle.Artifacts.Artifacts))
	for index, item := range bundle.Artifacts.Artifacts {
		inventoryID := string(item.ID)
		path := fmt.Sprintf("artifacts/%02d-%s.bin", index+1, inventoryID)
		data := []byte("locked artifact payload for " + inventoryID + "\n")
		if inventoryID == "ARTDEF-CONFORMANCE-RUNNER-001" {
			executable, err := os.Executable()
			if err != nil {
				fixture.t.Fatalf("os.Executable() error = %v", err)
			}
			data = readFile(fixture.t, executable)
		}
		writeCandidateFile(fixture.t, fixture.root, path, data, 0o600)
		payload := lockedPayloadDocument{Path: path, MediaType: "application/octet-stream", SizeBytes: int64(len(data)), SHA256: hashBytes(data)}
		fixture.payloadByInvID[inventoryID] = payload
		artifacts = append(artifacts, map[string]any{
			"id":              artifactID(inventoryID),
			"inventory_id":    inventoryID,
			"release_version": "0.3.0",
			"package_version": "0.3.0",
			"payloads": []any{map[string]any{
				"path": payload.Path, "media_type": payload.MediaType,
				"size_bytes": payload.SizeBytes, "sha256": payload.SHA256,
			}},
		})
	}
	return artifacts
}

func (fixture *candidateFixture) makeAttestations() []any {
	fixture.t.Helper()
	attestations := make([]any, 0, len(fixture.artifacts)*2)
	for artifactIndex, rawArtifact := range fixture.artifacts {
		artifact := rawArtifact.(map[string]any)
		artifactIDValue := artifact["id"].(string)
		payload := artifact["payloads"].([]any)[0].(map[string]any)
		for kindIndex, kind := range []string{"sbom", "provenance"} {
			ordinal := artifactIndex*2 + kindIndex + 1
			attestationPath := fmt.Sprintf("attestations/%02d-%s.json", ordinal, kind)
			bundlePath := fmt.Sprintf("attestations/%02d-%s.sigstore.json", ordinal, kind)
			attestationBytes := []byte(fmt.Sprintf("{\"artifact\":%q,\"kind\":%q}\n", artifactIDValue, kind))
			bundleBytes := []byte(fmt.Sprintf("{\"attestation\":%q}\n", attestationPath))
			writeCandidateFile(fixture.t, fixture.root, attestationPath, attestationBytes, 0o600)
			writeCandidateFile(fixture.t, fixture.root, bundlePath, bundleBytes, 0o600)
			format := "spdx-json"
			mediaType := "application/spdx+json"
			if kind == "provenance" {
				format = "slsa-provenance-v1"
				mediaType = "application/vnd.in-toto+json"
			}
			subject := map[string]any{"path": payload["path"], "sha256": payload["sha256"]}
			attestations = append(attestations, map[string]any{
				"id":                  fmt.Sprintf("ATTST-FIXTURE-%03d", ordinal),
				"kind":                kind,
				"format":              format,
				"media_type":          mediaType,
				"subject_artifact_id": artifactIDValue,
				"subject_payloads":    []any{subject},
				"path":                attestationPath,
				"sha256":              hashBytes(attestationBytes),
				"sigstore_verification": map[string]any{
					"bundle_path":               bundlePath,
					"bundle_media_type":         "application/vnd.dev.sigstore.bundle+json;version=0.3",
					"bundle_sha256":             hashBytes(bundleBytes),
					"signed_attestation_sha256": hashBytes(attestationBytes),
					"signed_subjects":           []any{subject},
					"certificate_issuer":        "https://token.actions.githubusercontent.com",
					"certificate_identity":      "https://github.com/trainstar/synchro/.github/workflows/release.yml@refs/tags/v0.3.0",
					"verifier": map[string]any{
						"name": "cosign", "version": "2.4.1", "binary_sha256": hashBytes([]byte("cosign")),
					},
					"verified_at":      testStart.Format(time.RFC3339),
					"verification_uri": fmt.Sprintf("https://search.sigstore.dev/?logIndex=%d", ordinal),
				},
			})
		}
	}
	return attestations
}

func (fixture *candidateFixture) makeRunAttachments(t *testing.T) []Attachment {
	t.Helper()
	inputs := []struct {
		kind      string
		mediaType string
		data      []byte
	}{
		{"log", "text/plain", []byte("deterministic execution log\n")},
		{"trace", "application/json", []byte("{\"trace\":\"bounded\"}\n")},
		{"replay-data", "application/json", []byte("{\"replay\":\"deterministic\"}\n")},
		{"vector-results", "application/json", []byte("{\"vectors\":\"passed\"}\n")},
	}
	for _, scenario := range fixture.scenarios {
		for _, barrier := range scenario.BarrierPlan.Barriers {
			inputs = append(inputs, struct {
				kind      string
				mediaType string
				data      []byte
			}{"barrier-trace", "application/json", []byte(fmt.Sprintf("{\"barrier_id\":%q}\n", barrier.ID))})
		}
	}
	attachments := make([]Attachment, 0, len(inputs))
	for _, input := range inputs {
		attachment, _, err := attachmentFor(input.kind, input.mediaType, input.data)
		if err != nil {
			t.Fatalf("attachmentFor(%q) error = %v", input.kind, err)
		}
		writeCandidateFile(t, fixture.root, attachment.Path, input.data, 0o600)
		attachments = append(attachments, attachment)
	}
	return attachments
}

func (fixture *candidateFixture) writeLock() {
	fixture.t.Helper()
	fixture.lockBytes = marshalJSONIndent(fixture.t, fixture.lock)
	writeCandidateFile(fixture.t, fixture.root, candidateLockFile, fixture.lockBytes, 0o600)
}

func (fixture *candidateFixture) writeEvidence(t *testing.T, evidence Evidence, path string) string {
	t.Helper()
	writeCandidateFile(t, fixture.root, path, marshalJSONIndent(t, evidence), 0o600)
	return path
}

func (fixture *candidateFixture) writeManifest(t *testing.T, references []EvidenceReference, mutate func(map[string]any)) {
	t.Helper()
	lockScenarios := cloneJSONValue(t, fixture.lock["scenarios"])
	lockSupport := cloneJSONValue(t, fixture.lock["resolved_support_cells"])
	lockArtifacts := cloneJSONValue(t, fixture.lock["artifacts"])
	lockContract := cloneJSONValue(t, fixture.lock["contract"])
	manifest := map[string]any{
		"$schema":                 "https://synchro.dev/conformance/schemas/rc-manifest-v2.schema.json",
		"schema_version":          2,
		"candidate_id":            fixture.lock["candidate_id"],
		"release_version":         fixture.lock["release_version"],
		"protocol_version":        fixture.lock["protocol_version"],
		"source_commit":           fixture.lock["source_commit"],
		"runner_digest":           fixture.lock["runner_digest"],
		"created_at":              testStart.Add(time.Minute).Format(time.RFC3339),
		"generator":               fixture.lock["generator"],
		"candidate_lock":          map[string]any{"path": candidateLockFile, "sha256": hashBytes(fixture.lockBytes)},
		"trusted_rerun_approvers": fixture.lock["trusted_rerun_approvers"],
		"contract":                lockContract,
		"scenarios":               lockScenarios,
		"evidence":                references,
		"resolved_support_cells":  lockSupport,
		"artifacts":               lockArtifacts,
		"attestations":            fixture.attestations,
	}
	if mutate != nil {
		mutate(manifest)
	}
	writeCandidateFile(t, fixture.root, finalManifestFile, marshalJSONIndent(t, manifest), 0o600)
}

func (fixture *candidateFixture) scenarioByID(t *testing.T, id string) scenarios.Scenario {
	t.Helper()
	for _, scenario := range fixture.scenarios {
		if string(scenario.ID) == id {
			return scenario
		}
	}
	t.Fatalf("scenario %s is missing", id)
	return scenarios.Scenario{}
}

func issueReceipt(t *testing.T, issuer execution.ReceiptIssuer, privateKey ed25519.PrivateKey, fields execution.ReceiptFields) execution.Receipt {
	t.Helper()
	completion, err := execution.PrepareCompletion(issuer, fields)
	if err != nil {
		t.Fatalf("PrepareCompletion() error = %v", err)
	}
	signature := ed25519.Sign(privateKey, completion.SigningBytes())
	receipt, err := execution.CompleteReceipt(issuer, completion, signature)
	if err != nil {
		t.Fatalf("CompleteReceipt() error = %v", err)
	}
	return receipt
}

func deterministicKey(t *testing.T, offset byte) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	seed := testSeed
	for index := range seed {
		seed[index] += offset
	}
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok {
		t.Fatal("private key did not return an Ed25519 public key")
	}
	return append(ed25519.PublicKey(nil), publicKey...), append(ed25519.PrivateKey(nil), privateKey...)
}

func selectScenarios(t *testing.T, values []scenarios.Scenario, ids ...string) []scenarios.Scenario {
	t.Helper()
	wanted := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	var selected []scenarios.Scenario
	for _, scenario := range values {
		if _, found := wanted[string(scenario.ID)]; found {
			selected = append(selected, scenario)
		}
	}
	if len(selected) != len(ids) {
		t.Fatalf("selected %d scenarios, want %d", len(selected), len(ids))
	}
	return selected
}

func scenarioPathByID(t *testing.T, repoRoot, id string) string {
	t.Helper()
	var catalog struct {
		Scenarios []struct {
			ScenarioID string `json:"scenario_id"`
			Path       string `json:"path"`
		} `json:"scenarios"`
	}
	decodeJSON(t, readFile(t, filepath.Join(repoRoot, "conformance", "catalog.json")), &catalog)
	for _, entry := range catalog.Scenarios {
		if entry.ScenarioID == id {
			return entry.Path
		}
	}
	t.Fatalf("catalog path for %s is missing", id)
	return ""
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	if testRepositoryRoot == "" {
		t.Fatal("test repository root is not initialized")
	}
	return testRepositoryRoot
}

func repositoryCommit(t *testing.T, root string) string {
	t.Helper()
	command := exec.Command("git", "rev-parse", "HEAD")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git rev-parse HEAD error = %v", err)
	}
	return string(output[:len(output)-1])
}

func writeFixtureMakefile(root string) error {
	const makefile = ".PHONY: test-blackbox test-conformance test-swift test-kotlin test-rn-e2e-ios test-rn-e2e-android\n\ntest-blackbox test-conformance test-swift test-kotlin test-rn-e2e-ios test-rn-e2e-android:\n\t@true\n"
	testFixtureMakefile = []byte(makefile)
	return os.WriteFile(filepath.Join(root, "Makefile"), testFixtureMakefile, 0o600)
}

func artifactRole(t *testing.T, repoRoot, inventoryID string) string {
	t.Helper()
	bundle, err := contract.Load(context.Background(), repoRoot)
	if err != nil {
		t.Fatalf("contract.Load() error = %v", err)
	}
	for _, item := range bundle.Artifacts.Artifacts {
		if string(item.ID) == inventoryID {
			return item.Role
		}
	}
	t.Fatalf("artifact role for %s is missing", inventoryID)
	return ""
}

func artifactID(inventoryID string) string {
	suffix := inventoryID[len("ARTDEF-"):]
	return "ART-" + suffix
}

func supportDimensions(id string) map[string]string {
	switch id {
	case "SUP-PG-018":
		return map[string]string{"postgresql": "18.0", "os": "14.6", "rust": "1.85.0", "pgrx": "0.16.1"}
	case "SUP-IOS-MIN-001", "SUP-IOS-CURRENT-001":
		return map[string]string{"ios": "18.6", "xcode": "16.4", "apple-sdk": "18.5", "simulator-runtime": "18.5", "swift": "6.1.2"}
	case "SUP-MACOS-MIN-001", "SUP-MACOS-CURRENT-001":
		return map[string]string{"macos": "15.6", "xcode": "16.4", "apple-sdk": "15.5", "swift": "6.1.2"}
	case "SUP-ANDROID-MIN-001", "SUP-ANDROID-CURRENT-001":
		return map[string]string{"android-api": "35.0", "android-sdk": "35.0", "emulator-image": "35.0", "jdk": "17.0.12", "kotlin": "2.1.0", "gradle": "8.10.2"}
	case "SUP-RN-IOS-MIN-001", "SUP-RN-IOS-CURRENT-001":
		return map[string]string{"ios": "18.6", "xcode": "16.4", "apple-sdk": "18.5", "simulator-runtime": "18.5", "swift": "6.1.2", "node": "22.18.0", "yarn": "4.9.2", "react": "19.1.0", "react-native": "0.83.0", "cocoapods": "1.16.2"}
	case "SUP-RN-ANDROID-MIN-001", "SUP-RN-ANDROID-CURRENT-001":
		return map[string]string{"android-api": "35.0", "android-sdk": "35.0", "emulator-image": "35.0", "jdk": "17.0.12", "kotlin": "2.1.0", "gradle": "8.10.2", "node": "22.18.0", "yarn": "4.9.2", "react": "19.1.0", "react-native": "0.83.0"}
	default:
		panic("unknown required support cell " + id)
	}
}

func evidenceReference(evidence Evidence, path string, data []byte) EvidenceReference {
	return EvidenceReference{
		EvidenceID:        evidence.EvidenceID,
		ScenarioID:        evidence.ScenarioID,
		ProofObligationID: evidence.ProofObligationID,
		SupportCellID:     evidence.SupportCellID,
		ProofType:         evidence.ProofType,
		Path:              path,
		SHA256:            hashBytes(data),
	}
}

func cloneTestFields(source execution.ReceiptFields) execution.ReceiptFields {
	data, err := json.Marshal(source)
	if err != nil {
		panic(err)
	}
	var result execution.ReceiptFields
	if err := json.Unmarshal(data, &result); err != nil {
		panic(err)
	}
	return result
}

func cloneJSONValue(t *testing.T, value any) any {
	t.Helper()
	data := marshalJSON(t, value)
	var result any
	decodeJSON(t, data, &result)
	return result
}

func sortedMapKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func hashBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return fmt.Sprintf("%x", digest)
}

func stringPointer(value string) *string {
	return &value
}

func marshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return data
}

func marshalJSONIndent(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent() error = %v", err)
	}
	return append(data, '\n')
}

func decodeJSON(t *testing.T, data []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
}

func writeCandidateFile(t *testing.T, root, relative string, data []byte, mode os.FileMode) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(path), err)
	}
	writeFile(t, path, data, mode)
}

func writeFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	return data
}

func requireErrorIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want errors.Is(%v)", err, target)
	}
}
