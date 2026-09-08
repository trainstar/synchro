package integration

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/faults"
	"github.com/trainstar/synchro/conformance/invariants"
	"github.com/trainstar/synchro/conformance/soak"
)

func TestSoak(t *testing.T) {
	if strings.TrimSpace(os.Getenv("SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT")) == "" {
		t.Skip("the black-box environment is not configured")
	}
	if !*provision || !*install {
		t.Fatal("TestSoak requires --provision --install")
	}
	seed, err := parseSoakSeed(os.Getenv("SOAK_SEED"))
	if err != nil {
		t.Fatal(err)
	}
	duration, err := parseSoakDuration(os.Getenv("SOAK_DURATION"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	catalog, err := faults.LoadCatalog(ctx, soakRepositoryRoot)
	if err != nil {
		t.Fatalf("load soak fault catalog: %v", err)
	}

	t.Run("live", func(t *testing.T) {
		config, err := soak.ConfigForDuration(duration)
		if err != nil {
			t.Fatalf("configure live soak: %v", err)
		}
		config.Users = []string{soakUserID}
		config.Clients = []string{soakClientID}
		config.Scopes = []string{soakPrivateScope, soakSharedScope}
		plan, err := soak.Generate(seed, config, catalog)
		if err != nil {
			t.Fatalf("generate live soak plan: %v", err)
		}
		harness, err := newLiveSoakHarness(ctx, seed, false)
		if err != nil {
			t.Fatalf("create live soak harness: %v", err)
		}
		t.Cleanup(func() { closeLiveSoakHarness(t, harness) })
		result, err := soak.Run(ctx, plan, harness, t.TempDir()+"/live-soak.jsonl")
		if err != nil {
			t.Fatalf("run live soak: %v", err)
		}
		if result.OperationsExecuted != len(plan.Operations) || len(result.Violations) != 0 {
			t.Fatalf("live soak result = operations %d/%d, violations %d", result.OperationsExecuted, len(plan.Operations), len(result.Violations))
		}
	})

	t.Run("checksum-corruption-replays-from-seed", func(t *testing.T) {
		config := soak.Config{
			OperationCount: soak.MinimumCoverageOperations,
			Users:          []string{soakUserID},
			Clients:        []string{soakClientID},
			Scopes:         []string{soakPrivateScope, soakSharedScope},
			FaultRate:      0,
		}
		plan, err := soak.Generate(seed, config, catalog)
		if err != nil {
			t.Fatalf("generate checksum replay plan: %v", err)
		}
		journalPath := t.TempDir() + "/checksum-corruption.jsonl"
		firstHarness, err := newLiveSoakHarness(ctx, seed, true)
		if err != nil {
			t.Fatalf("create checksum fault harness: %v", err)
		}
		first, firstErr := soak.Run(ctx, plan, firstHarness, journalPath)
		closeLiveSoakHarness(t, firstHarness)
		if !errors.Is(firstErr, soak.ErrInvariantViolation) {
			t.Fatalf("checksum fault error = %v, want ErrInvariantViolation", firstErr)
		}
		if !hasChecksumRowDigestViolation(first.Violations) {
			t.Fatalf("checksum fault violations = %#v", first.Violations)
		}

		replayHarness, err := newLiveSoakHarness(ctx, seed, true)
		if err != nil {
			t.Fatalf("create checksum replay harness: %v", err)
		}
		t.Cleanup(func() { closeLiveSoakHarness(t, replayHarness) })
		replayed, replayErr := soak.ReplayRun(ctx, journalPath, catalog, replayHarness)
		if !errors.Is(replayErr, soak.ErrInvariantViolation) {
			t.Fatalf("checksum replay error = %v, want ErrInvariantViolation", replayErr)
		}
		if !reflect.DeepEqual(first.Violations, replayed.Violations) {
			t.Fatalf("checksum replay violations differ: first=%#v replay=%#v", first.Violations, replayed.Violations)
		}
	})
}

func hasChecksumRowDigestViolation(violations []invariants.Violation) bool {
	for _, violation := range violations {
		if violation.Family == invariants.InvariantChecksumConvergence && violation.RuleID == invariants.RuleChecksumWireRowDigestMismatch {
			return true
		}
	}
	return false
}

func closeLiveSoakHarness(t *testing.T, harness *liveSoakHarness) {
	t.Helper()
	closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := harness.Close(closeContext); err != nil {
		t.Errorf("close live soak harness: %v", err)
	}
}
