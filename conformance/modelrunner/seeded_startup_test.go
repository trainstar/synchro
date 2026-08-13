package modelrunner

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func TestSeededEmptyStartupRequiresThreeSubstantiveSamplesPerStratum(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repoRoot := filepath.Clean(filepath.Join(workingDirectory, "../.."))
	scenario, err := scenarios.LoadFile(context.Background(), repoRoot, "conformance/scenarios/performance/seeded-empty-startup-001.json")
	if err != nil {
		t.Fatalf("load seeded startup scenario: %v", err)
	}
	result, err := RunScenario(context.Background(), scenario)
	if err != nil {
		t.Fatalf("run seeded startup scenario: %v", err)
	}
	if !seededEmptyStartupSatisfied(result) {
		t.Fatal("seeded startup predicate rejected the complete six-sample workload")
	}

	missingEmpty := result
	missingEmpty.Steps = append([]OperationExecution(nil), result.Steps[:len(result.Steps)-2]...)
	if seededEmptyStartupSatisfied(missingEmpty) {
		t.Fatal("seeded startup predicate accepted fewer than three empty samples")
	}

	missingSeeded := result
	missingSeeded.Steps = append([]OperationExecution(nil), result.Steps[3:]...)
	if seededEmptyStartupSatisfied(missingSeeded) {
		t.Fatal("seeded startup predicate accepted fewer than three seeded samples")
	}
}
