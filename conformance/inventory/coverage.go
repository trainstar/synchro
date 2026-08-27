// Package inventory projects CI-summary coverage into a stable report.
package inventory

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/evidence"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

// Report is the generated coverage projection from one validated CI summary.
type Report struct {
	SchemaVersion int                      `json:"schema_version"`
	SourceCommit  string                   `json:"source_commit"`
	Rows          []evidence.CoverageEntry `json:"rows"`
}

// Project creates a sorted coverage report without adding another proof path.
func Project(summary evidence.Summary) (Report, error) {
	report := Report{SchemaVersion: 1, SourceCommit: summary.SourceCommit, Rows: append([]evidence.CoverageEntry(nil), summary.Coverage...)}
	sort.Slice(report.Rows, func(left, right int) bool { return report.Rows[left].CoverageID < report.Rows[right].CoverageID })
	if err := report.Validate(); err != nil {
		return Report{}, err
	}
	return report, nil
}

// Validate rejects incomplete coverage and duplicate proof homes.
func (report Report) Validate() error {
	if report.SchemaVersion != 1 || report.SourceCommit == "" || len(report.Rows) == 0 {
		return errors.New("coverage report is incomplete")
	}
	seenIDs := make(map[string]struct{}, len(report.Rows))
	seenTuples := make(map[string]string, len(report.Rows))
	for _, row := range report.Rows {
		if row.CoverageID == "" || row.TestID == "" || row.RequirementID == "" || row.ScenarioID == "" || row.ProofObligationID == "" || row.AssertionID == "" || row.ProofType == "" || row.ProofHome == "" {
			return errors.New("coverage row is incomplete")
		}
		if _, duplicate := seenIDs[row.CoverageID]; duplicate {
			return errors.New("coverage report repeats a coverage ID")
		}
		seenIDs[row.CoverageID] = struct{}{}
		support := ""
		if row.SupportCellID != nil {
			support = *row.SupportCellID
		}
		tuple := strings.Join([]string{row.RequirementID, row.ScenarioID, row.ProofObligationID, row.AssertionID, support}, "\x00")
		if home, duplicate := seenTuples[tuple]; duplicate {
			if home != row.ProofHome {
				return errors.New("coverage report assigns duplicate proof homes")
			}
			return errors.New("coverage report repeats an ownership tuple")
		}
		seenTuples[tuple] = row.ProofHome
	}
	return nil
}

// Decode rejects unknown fields, duplicate members, and trailing values.
func Decode(data []byte) (Report, error) {
	var report Report
	if err := jsonstrict.Decode(data, &report); err != nil {
		return Report{}, fmt.Errorf("decode coverage report: %w", err)
	}
	if err := report.Validate(); err != nil {
		return Report{}, err
	}
	return report, nil
}

// WriteJSON writes deterministic JSON with one trailing newline.
func WriteJSON(writer io.Writer, report Report) error {
	if writer == nil {
		return errors.New("coverage JSON writer is nil")
	}
	if err := report.Validate(); err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

// WriteMarkdown writes a compact coverage table.
func WriteMarkdown(writer io.Writer, report Report) error {
	if writer == nil {
		return errors.New("coverage Markdown writer is nil")
	}
	if err := report.Validate(); err != nil {
		return err
	}
	var buffer bytes.Buffer
	buffer.WriteString("# CI Coverage\n\n")
	buffer.WriteString("| Requirement | Test | Proof home | Scenario obligation |\n")
	buffer.WriteString("| --- | --- | --- | --- |\n")
	for _, row := range report.Rows {
		fmt.Fprintf(&buffer, "| `%s` | `%s` | `%s` | `%s` |\n", row.RequirementID, row.TestID, row.ProofHome, row.ProofObligationID)
	}
	_, err := writer.Write(buffer.Bytes())
	return err
}
