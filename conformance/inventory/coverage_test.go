package inventory

import (
	"bytes"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/evidence"
)

func TestCoverageReportProjectsOneSummaryPath(t *testing.T) {
	summary := evidence.Summary{SourceCommit: strings.Repeat("a", 40), Coverage: []evidence.CoverageEntry{
		{CoverageID: "COV-B", TestID: "gate/test-conformance", RequirementID: "SYNC-B-001", ScenarioID: "SCN-B-001", ProofObligationID: "OBL-B-001", AssertionID: "ASSERT-B-001", ProofType: "reference-model", ProofHome: "scenario"},
		{CoverageID: "COV-A", TestID: "gate/test-blackbox", RequirementID: "SYNC-A-001", ScenarioID: "SCN-A-001", ProofObligationID: "OBL-A-001", AssertionID: "ASSERT-A-001", ProofType: "server-black-box", ProofHome: "real-integration"},
	}}
	report, err := Project(summary)
	if err != nil {
		t.Fatalf("Project() error = %v", err)
	}
	if report.Rows[0].CoverageID != "COV-A" {
		t.Fatalf("coverage rows are not sorted: %#v", report.Rows)
	}
	var data bytes.Buffer
	if err := WriteJSON(&data, report); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}
	if _, err := Decode(data.Bytes()); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	data.Reset()
	if err := WriteMarkdown(&data, report); err != nil || !strings.Contains(data.String(), "real-integration") {
		t.Fatalf("WriteMarkdown() result = %q, error = %v", data.String(), err)
	}
}

func TestCoverageReportRejectsDuplicateProofHomes(t *testing.T) {
	row := evidence.CoverageEntry{CoverageID: "COV-A", TestID: "gate/test-blackbox", RequirementID: "SYNC-A-001", ScenarioID: "SCN-A-001", ProofObligationID: "OBL-A-001", AssertionID: "ASSERT-A-001", ProofType: "server-black-box", ProofHome: "real-integration"}
	duplicate := row
	duplicate.CoverageID = "COV-B"
	duplicate.ProofHome = "scenario"
	report := Report{SchemaVersion: 1, SourceCommit: strings.Repeat("a", 40), Rows: []evidence.CoverageEntry{row, duplicate}}
	if err := report.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate proof homes") {
		t.Fatalf("duplicate proof-home error = %v", err)
	}
}
