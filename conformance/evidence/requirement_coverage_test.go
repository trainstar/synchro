package evidence

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
)

func TestAuthoredRequirementCoverageIsClosed(t *testing.T) {
	ctx := context.Background()
	root := repositoryForTest(t)
	bundle, err := contract.Load(ctx, root)
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	coverage, err := authoredCoverage(ctx, root)
	if err != nil {
		t.Fatalf("load authored coverage: %v", err)
	}
	summary := authoredCoverageSummary(coverage)
	if _, err := BuildRequirementCoverage(bundle.Requirements.Requirements, summary); err != nil {
		if requirementID, proofType, missing := missingAuthoredProofType(bundle.Requirements.Requirements, coverage); missing {
			t.Fatalf("missing authored proof: requirement %s, proof type %s", requirementID, proofType)
		}
		t.Fatalf("BuildRequirementCoverage() with authored ownership: %v", err)
	}

	requirementID, proofType, found := removableAuthoredProofType(bundle.Requirements.Requirements, coverage)
	if !found {
		t.Fatal("authored coverage has no requirement with an alternate proof type")
	}
	missing := make([]CoverageEntry, 0, len(coverage))
	for _, entry := range coverage {
		if entry.RequirementID != requirementID || entry.ProofType != proofType {
			missing = append(missing, entry)
		}
	}
	summary = authoredCoverageSummary(missing)
	_, err = BuildRequirementCoverage(bundle.Requirements.Requirements, summary)
	want := fmt.Sprintf("requirement %s has no executed proof of type %s", requirementID, proofType)
	if err == nil || err.Error() != want {
		t.Fatalf("missing authored proof error = %v, want %q", err, want)
	}
}

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

func authoredCoverageSummary(entries []CoverageEntry) Summary {
	seen := make(map[string]struct{}, len(entries))
	obligations := make([]Obligation, 0, len(entries))
	for _, entry := range entries {
		if _, duplicate := seen[entry.TestID]; duplicate {
			continue
		}
		seen[entry.TestID] = struct{}{}
		obligations = append(obligations, Obligation{
			ID:        entry.TestID,
			Status:    "passed",
			Terminal:  true,
			TestCount: 1,
		})
	}
	return Summary{
		SourceCommit: strings.Repeat("a", 40),
		Obligations:  obligations,
		Coverage:     entries,
	}
}

func removableAuthoredProofType(requirements []contract.Requirement, coverage []CoverageEntry) (string, string, bool) {
	for _, requirement := range requirements {
		for _, proofType := range requirement.RequiredProofTypes {
			hasProofType := false
			hasAlternate := false
			for _, entry := range coverage {
				if entry.RequirementID != string(requirement.ID) {
					continue
				}
				if entry.ProofType == proofType {
					hasProofType = true
				} else {
					hasAlternate = true
				}
			}
			if hasProofType && hasAlternate {
				return string(requirement.ID), proofType, true
			}
		}
	}
	return "", "", false
}

func missingAuthoredProofType(requirements []contract.Requirement, coverage []CoverageEntry) (string, string, bool) {
	proofTypes := make(map[string]map[string]struct{}, len(requirements))
	for _, entry := range coverage {
		if proofTypes[entry.RequirementID] == nil {
			proofTypes[entry.RequirementID] = make(map[string]struct{})
		}
		proofTypes[entry.RequirementID][entry.ProofType] = struct{}{}
	}
	for _, requirement := range requirements {
		for _, proofType := range requirement.RequiredProofTypes {
			if _, found := proofTypes[string(requirement.ID)][proofType]; !found {
				return string(requirement.ID), proofType, true
			}
		}
	}
	return "", "", false
}
