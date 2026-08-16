package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/blackbox/baseline"
	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/modelrunner"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "synchro-conformance: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("operation canceled: %w", err)
	}
	if len(args) == 0 {
		return errors.New("command is required")
	}
	switch args[0] {
	case "catalog":
		return runCatalog(ctx, args[1:])
	case "model":
		return runModel(ctx, args[1:])
	case "blackbox":
		return runBlackbox(ctx, args[1:])
	case "baseline":
		return runBaselineCommand(ctx, args[1:])
	default:
		return errors.New("unknown command")
	}
}

func runCatalog(ctx context.Context, args []string) error {
	flags := newFlagSet("catalog")
	repoRoot := flags.String("repo-root", "", "repository root")
	write := flags.Bool("write", false, "write the catalog")
	check := flags.Bool("check", false, "check the catalog")
	if err := flags.Parse(args); err != nil {
		return errors.New("catalog flags are invalid")
	}
	if flags.NArg() != 0 {
		return errors.New("catalog does not accept positional arguments")
	}
	if *repoRoot == "" {
		return errors.New("catalog requires --repo-root PATH")
	}
	if *write == *check {
		return errors.New("catalog requires exactly one of --write or --check")
	}
	if *write {
		if err := scenarios.WriteGeneratedCatalog(ctx, *repoRoot); err != nil {
			return operationError(ctx, "catalog write", err)
		}
		return nil
	}
	if err := scenarios.CheckGeneratedCatalog(ctx, *repoRoot); err != nil {
		return operationError(ctx, "catalog check", err)
	}
	return nil
}

func runModel(ctx context.Context, args []string) error {
	flags := newFlagSet("model")
	repoRoot := flags.String("repo-root", "", "repository root")
	scenarioID := flags.String("scenario", "", "authored scenario ID")
	if err := flags.Parse(args); err != nil {
		return errors.New("model flags are invalid")
	}
	if flags.NArg() != 0 {
		return errors.New("model does not accept positional arguments")
	}
	if *repoRoot == "" {
		return errors.New("model requires --repo-root PATH")
	}
	authored, err := loadAuthoredScenarios(ctx, *repoRoot)
	if err != nil {
		return operationError(ctx, "model load", err)
	}
	found := false
	for _, scenario := range authored {
		if *scenarioID != "" && string(scenario.ID) != *scenarioID {
			continue
		}
		found = true
		result, runErr := modelrunner.RunScenario(ctx, scenario)
		if runErr != nil {
			return operationError(ctx, "model execute", runErr)
		}
		if !result.Passed {
			return operationError(ctx, "model execute", errors.New("authored scenario did not pass"))
		}
	}
	if !found {
		return errors.New("model scenario is not authored")
	}
	return nil
}

func runBlackbox(ctx context.Context, args []string) error {
	flags := newFlagSet("blackbox")
	repoRoot := flags.String("repo-root", "", "repository root")
	mode := flags.String("mode", "", "black-box mode")
	if err := flags.Parse(args); err != nil {
		return errors.New("blackbox flags are invalid")
	}
	if flags.NArg() != 0 {
		return errors.New("blackbox does not accept positional arguments")
	}
	if *repoRoot == "" {
		return errors.New("blackbox requires --repo-root PATH")
	}
	switch *mode {
	case "harness":
		return runSyntheticHarness(ctx, *repoRoot)
	case "baseline":
		return runDiagnosticBaseline(ctx, *repoRoot, "")
	case "strict":
		return errors.New("strict protocol 3 black-box execution is unavailable")
	default:
		return errors.New("blackbox requires --mode harness, baseline, or strict")
	}
}

func runBaselineCommand(ctx context.Context, args []string) error {
	flags := newFlagSet("baseline")
	repoRoot := flags.String("repo-root", "", "repository root")
	output := flags.String("output", "", "non-release diagnostic output")
	if err := flags.Parse(args); err != nil {
		return errors.New("baseline flags are invalid")
	}
	if flags.NArg() != 0 {
		return errors.New("baseline does not accept positional arguments")
	}
	if *repoRoot == "" {
		return errors.New("baseline requires --repo-root PATH")
	}
	if *output == "" {
		return errors.New("baseline requires --output PATH")
	}
	return runDiagnosticBaseline(ctx, *repoRoot, *output)
}

func runSyntheticHarness(ctx context.Context, repoRoot string) error {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return operationError(ctx, "blackbox harness runner", err)
	}
	trustedRunner, err := execution.NewTrustedRunner(privateKey)
	if err != nil {
		return operationError(ctx, "blackbox harness runner", err)
	}
	attachmentRoot, err := os.MkdirTemp("", "synchro-conformance-harness-")
	if err != nil {
		return operationError(ctx, "blackbox harness initialize", err)
	}
	defer os.RemoveAll(attachmentRoot)
	fixtures := []syntheticHarnessFixture{
		{path: "conformance/scenarios/performance/pending-cycle-001.json", fault: blackbox.SyntheticCompliant, wantPass: true},
		{path: "conformance/scenarios/performance/pending-cycle-001.json", fault: blackbox.SyntheticOmitMutation},
		{path: "conformance/scenarios/performance/steady-pull-001.json", fault: blackbox.SyntheticConstantChecksum},
		{path: "conformance/scenarios/server/pull-divergent-checkpoints-001.json", fault: blackbox.SyntheticDuplicateDelivery},
		{path: "conformance/scenarios/server/pull-divergent-checkpoints-001.json", fault: blackbox.SyntheticWrongScope},
		{path: "conformance/scenarios/performance/pending-cycle-001.json", fault: blackbox.SyntheticReplayCorruption},
		{path: "conformance/scenarios/performance/pending-cycle-001.json", fault: blackbox.SyntheticWrongStatus},
	}
	for _, fixture := range fixtures {
		scenario, err := scenarios.LoadFile(ctx, repoRoot, fixture.path)
		if err != nil {
			return operationError(ctx, "blackbox harness load", err)
		}
		obligation, err := serverBlackboxObligation(scenario)
		if err != nil {
			return operationError(ctx, "blackbox harness load", err)
		}
		if err := runSyntheticScenario(ctx, scenario, obligation, fixture, attachmentRoot, trustedRunner); err != nil {
			return operationError(ctx, "blackbox harness execute", err)
		}
	}
	return nil
}

type syntheticHarnessFixture struct {
	path     string
	fault    blackbox.SyntheticFault
	wantPass bool
}

func serverBlackboxObligation(scenario scenarios.Scenario) (scenarios.ProofObligation, error) {
	for _, obligation := range scenario.ProofObligations {
		if obligation.ProofType == "server-black-box" && obligation.MakeTarget == "test-blackbox" {
			return obligation, nil
		}
	}
	return scenarios.ProofObligation{}, errors.New("authored scenario has no server black-box obligation")
}

func runSyntheticScenario(ctx context.Context, scenario scenarios.Scenario, obligation scenarios.ProofObligation, fixture syntheticHarnessFixture, attachmentRoot string, trustedRunner execution.TrustedRunner) error {
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return errors.New("create synthetic token secret failed")
	}
	provider, err := blackbox.NewHS256TokenProvider(secret[:], blackbox.Claims{"sub": "synthetic-user", "aud": "conformance-harness"})
	if err != nil {
		return errors.New("create synthetic token provider failed")
	}
	token, err := provider.Token(ctx)
	if err != nil {
		return errors.New("create synthetic token failed")
	}
	system, err := blackbox.NewSyntheticSystem(ctx, scenario, blackbox.SyntheticOptions{ExpectedToken: token, Fault: fixture.fault})
	if err != nil {
		return err
	}
	defer system.Close()
	runner, err := blackbox.NewRunner(blackbox.RunnerConfig{
		Client:           &blackbox.Client{BaseURL: system.BaseURL(), HTTP: &http.Client{}, Tokens: provider},
		Recorder:         blackbox.RecorderConfig{AttachmentRoot: filepath.Join(attachmentRoot, string(scenario.ID)), MaxRecords: 256, MaxRawBodyBytes: 1 << 20},
		ArtifactBindings: syntheticArtifactBindings(obligation),
		TrustedRunner:    trustedRunner,
	})
	if err != nil {
		return errors.New("create synthetic black-box runner failed")
	}
	issuer, err := runner.NewReceiptIssuer()
	if err != nil {
		return errors.New("create synthetic receipt issuer failed")
	}
	receipt, result, runErr := runner.Run(ctx, scenario, obligation, issuer)
	if fixture.wantPass && (runErr != nil || !result.Passed) {
		return errors.New("compliant synthetic black-box scenario did not pass")
	}
	if !fixture.wantPass && (runErr == nil || result.Passed || result.Failure.Kind != blackbox.FailureSemantic || !system.FaultApplied()) {
		return errors.New("synthetic black-box fault did not cause a semantic detection")
	}
	if err := receipt.Verify(); err != nil {
		return errors.New("synthetic black-box receipt is invalid")
	}
	return nil
}

func syntheticArtifactBindings(obligation scenarios.ProofObligation) []execution.ArtifactBinding {
	bindings := make([]execution.ArtifactBinding, len(obligation.ArtifactInventoryIDs))
	for index, inventoryID := range obligation.ArtifactInventoryIDs {
		digest := sha256.Sum256([]byte(fmt.Sprintf("synthetic-artifact-%d", index)))
		bindings[index] = execution.ArtifactBinding{
			InventoryID: string(inventoryID),
			ArtifactID:  fmt.Sprintf("ART-SYNTHETIC-%03d", index+1),
			Path:        fmt.Sprintf("synthetic/artifact-%03d", index+1),
			Size:        int64(index + 1),
			SHA256:      hex.EncodeToString(digest[:]),
		}
	}
	return bindings
}

func runDiagnosticBaseline(ctx context.Context, repoRoot, outputPath string) (returnedErr error) {
	if _, err := loadAuthoredScenarios(ctx, repoRoot); err != nil {
		return operationError(ctx, "baseline load", err)
	}
	removeOutput := false
	if outputPath == "" {
		outputPath, returnedErr = os.MkdirTemp("", "baseline-")
		if returnedErr != nil {
			return operationError(ctx, "baseline initialize", returnedErr)
		}
		removeOutput = true
	}
	if removeOutput {
		defer os.RemoveAll(outputPath)
	}
	output, err := baseline.NewOutputPath(outputPath)
	if err != nil {
		return operationError(ctx, "baseline output", err)
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		return operationError(ctx, "baseline environment", err)
	}
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{Environment: environment})
	if err != nil {
		return operationError(ctx, "baseline provision", err)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := harness.Close(cleanupContext); err != nil {
			returnedErr = errors.Join(returnedErr, operationError(cleanupContext, "baseline cleanup", err))
		}
	}()
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		return operationError(ctx, "baseline token", err)
	}
	runner, err := baseline.NewRunner(baseline.RunnerConfig{
		BaseURL: harness.AdapterURL(), HTTPClient: &http.Client{Timeout: 30 * time.Second}, BearerToken: token,
		Source: harness.Source(), Operator: harness.Operator(), Output: output,
	})
	if err != nil {
		return operationError(ctx, "baseline runner", err)
	}
	report, err := runner.Run(ctx)
	if err != nil {
		return operationError(ctx, "baseline execute", err)
	}
	if err := report.Validate(); err != nil {
		return operationError(ctx, "baseline validate", err)
	}
	return nil
}

func loadAuthoredScenarios(ctx context.Context, repoRoot string) ([]scenarios.Scenario, error) {
	return scenarios.LoadAll(ctx, repoRoot)
}

func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func operationError(ctx context.Context, operation string, cause error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(cause, context.DeadlineExceeded) {
		return operationFailure{operation: operation, outcome: "canceled", cause: context.DeadlineExceeded}
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(cause, context.Canceled) {
		return operationFailure{operation: operation, outcome: "canceled", cause: context.Canceled}
	}
	return operationFailure{operation: operation, outcome: "failed", cause: cause}
}

type operationFailure struct {
	operation string
	outcome   string
	cause     error
}

func (failure operationFailure) Error() string {
	message := failure.operation + " " + failure.outcome
	if cause := boundedCauseText(failure.cause); cause != "" {
		message += ": " + cause
	}
	return message
}

func (failure operationFailure) Unwrap() error {
	return failure.cause
}

const maximumCauseTextBytes = 160

func boundedCauseText(cause error) string {
	if cause == nil {
		return ""
	}
	text := cause.Error()
	if index := strings.IndexAny(text, "\r\n"); index >= 0 {
		text = text[:index]
	}
	if index := strings.Index(text, ": "); index >= 0 {
		text = text[:index]
	}
	text = strings.TrimSpace(text)
	if len(text) <= maximumCauseTextBytes {
		return text
	}
	const suffix = "..."
	text = text[:maximumCauseTextBytes-len(suffix)]
	for !utf8.ValidString(text) {
		text = text[:len(text)-1]
	}
	return text + suffix
}
