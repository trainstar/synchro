package evidence

import (
	"bytes"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
)

func TestBuildRequirementCoverageIncludesExecutedProofAndGate(t *testing.T) {
	requirements := []contract.Requirement{{
		ID:                 "SYNC-A-001",
		Title:              "Requirement A",
		RequiredProofTypes: []string{"reference-model"},
	}}
	summary := fixtureCoverageSummary(CoverageEntry{
		CoverageID:        "COV-A",
		TestID:            "gate/test-conformance",
		RequirementID:     "SYNC-A-001",
		ScenarioID:        "SCN-A-001",
		ProofObligationID: "OBL-A-001",
		AssertionID:       "ASSERT-A-001",
		ProofType:         "reference-model",
		ProofHome:         "scenario",
	})
	report, err := BuildRequirementCoverage(requirements, summary)
	if err != nil {
		t.Fatalf("BuildRequirementCoverage() error = %v", err)
	}
	if len(report.Requirements) != 1 || len(report.Requirements[0].Proofs) != 1 {
		t.Fatalf("report = %#v", report)
	}
	if got := report.Requirements[0].Proofs[0].Gate; got != "gate/test-conformance" {
		t.Fatalf("proof gate = %q", got)
	}
	var markdown bytes.Buffer
	if err := WriteRequirementCoverageMarkdown(&markdown, report); err != nil {
		t.Fatalf("WriteRequirementCoverageMarkdown() error = %v", err)
	}
	if !strings.Contains(markdown.String(), "SYNC-A-001") || !strings.Contains(markdown.String(), "gate/test-conformance") {
		t.Fatalf("markdown = %q", markdown.String())
	}
}

func TestBuildRequirementCoverageRejectsUncoveredRequirement(t *testing.T) {
	requirements := []contract.Requirement{
		{ID: "SYNC-A-001", Title: "Requirement A", RequiredProofTypes: []string{"reference-model"}},
		{ID: "SYNC-B-001", Title: "Requirement B", RequiredProofTypes: []string{"reference-model"}},
	}
	summary := fixtureCoverageSummary(CoverageEntry{
		CoverageID:        "COV-A",
		TestID:            "gate/test-conformance",
		RequirementID:     "SYNC-A-001",
		ScenarioID:        "SCN-A-001",
		ProofObligationID: "OBL-A-001",
		AssertionID:       "ASSERT-A-001",
		ProofType:         "reference-model",
		ProofHome:         "scenario",
	})
	if _, err := BuildRequirementCoverage(requirements, summary); err == nil || !strings.Contains(err.Error(), "SYNC-B-001") || !strings.Contains(err.Error(), "zero executed proof") {
		t.Fatalf("uncovered requirement error = %v", err)
	}
}

func TestBuildRequirementCoverageRejectsDuplicateProofHomes(t *testing.T) {
	requirements := []contract.Requirement{{
		ID:                 "SYNC-A-001",
		Title:              "Requirement A",
		RequiredProofTypes: []string{"reference-model"},
	}}
	first := CoverageEntry{
		CoverageID:        "COV-A",
		TestID:            "gate/test-conformance",
		RequirementID:     "SYNC-A-001",
		ScenarioID:        "SCN-A-001",
		ProofObligationID: "OBL-A-001",
		AssertionID:       "ASSERT-A-001",
		ProofType:         "reference-model",
		ProofHome:         "scenario",
	}
	second := first
	second.CoverageID = "COV-B"
	second.ProofObligationID = "OBL-A-002"
	if _, err := BuildRequirementCoverage(requirements, fixtureCoverageSummary(first, second)); err == nil || !strings.Contains(err.Error(), "duplicate proof homes") {
		t.Fatalf("duplicate proof-home error = %v", err)
	}
}

func fixtureCoverageSummary(entries ...CoverageEntry) Summary {
	return Summary{
		SourceCommit: strings.Repeat("a", 40),
		Obligations: []Obligation{{
			ID:        "gate/test-conformance",
			Kind:      "gate",
			Status:    "passed",
			Terminal:  true,
			TestCount: 1,
		}},
		Coverage: entries,
	}
}
