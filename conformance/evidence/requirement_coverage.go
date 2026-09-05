package evidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/internal/contract"
)

const RequirementCoverageSchemaVersion = 1

// RequirementCoverageReport maps every authored requirement to executed proof.
type RequirementCoverageReport struct {
	SchemaVersion int                      `json:"schema_version"`
	SourceCommit  string                   `json:"source_commit"`
	Requirements  []RequirementCoverageRow `json:"requirements"`
}

// RequirementCoverageRow contains the proof types required and executed for one requirement.
type RequirementCoverageRow struct {
	RequirementID      string                  `json:"requirement_id"`
	Title              string                  `json:"title"`
	RequiredProofTypes []string                `json:"required_proof_types"`
	Proofs             []RequirementProofEntry `json:"proofs"`
}

// RequirementProofEntry binds one authored proof obligation to its executed summary gate.
type RequirementProofEntry struct {
	ScenarioID        string  `json:"scenario_id"`
	ProofObligationID string  `json:"proof_obligation_id"`
	AssertionID       string  `json:"assertion_id"`
	SupportCellID     *string `json:"support_cell_id"`
	ProofType         string  `json:"proof_type"`
	ProofHome         string  `json:"proof_home"`
	Gate              string  `json:"gate"`
}

// GenerateRequirementCoverage validates the terminal summary before it projects authored requirements.
func GenerateRequirementCoverage(ctx context.Context, repoRoot string, summary Summary) (RequirementCoverageReport, error) {
	if err := contextError(ctx); err != nil {
		return RequirementCoverageReport{}, err
	}
	if err := Validate(ctx, repoRoot, summary); err != nil {
		return RequirementCoverageReport{}, fmt.Errorf("validate phase-5 summary: %w", err)
	}
	bundle, err := contract.Load(ctx, repoRoot)
	if err != nil {
		return RequirementCoverageReport{}, fmt.Errorf("load authored contract: %w", err)
	}
	return BuildRequirementCoverage(bundle.Requirements.Requirements, summary)
}

// BuildRequirementCoverage projects a validated summary into requirement coverage.
// It keeps the lower-level projection available for deterministic fixture tests.
func BuildRequirementCoverage(requirements []contract.Requirement, summary Summary) (RequirementCoverageReport, error) {
	if summary.SourceCommit == "" {
		return RequirementCoverageReport{}, errors.New("phase-5 summary source commit is missing")
	}
	if len(requirements) == 0 {
		return RequirementCoverageReport{}, errors.New("authored requirements are empty")
	}

	executed, err := executedSummaryObligations(summary.Obligations)
	if err != nil {
		return RequirementCoverageReport{}, err
	}
	requirementByID := make(map[string]contract.Requirement, len(requirements))
	rows := make([]RequirementCoverageRow, 0, len(requirements))
	for _, requirement := range requirements {
		id := string(requirement.ID)
		if id == "" || requirement.Title == "" {
			return RequirementCoverageReport{}, errors.New("authored requirement is incomplete")
		}
		if _, duplicate := requirementByID[id]; duplicate {
			return RequirementCoverageReport{}, fmt.Errorf("authored requirements repeat %s", id)
		}
		requirementByID[id] = requirement
		rows = append(rows, RequirementCoverageRow{
			RequirementID:      id,
			Title:              requirement.Title,
			RequiredProofTypes: append([]string(nil), requirement.RequiredProofTypes...),
		})
	}

	rowByID := make(map[string]*RequirementCoverageRow, len(rows))
	for index := range rows {
		rowByID[rows[index].RequirementID] = &rows[index]
	}
	seenCoverage := make(map[string]struct{}, len(summary.Coverage))
	seenProofHomes := make(map[string]string, len(summary.Coverage))
	for _, coverage := range summary.Coverage {
		if _, executed := executed[coverage.TestID]; !executed {
			return RequirementCoverageReport{}, fmt.Errorf("coverage %s references unexecuted gate %s", coverage.CoverageID, coverage.TestID)
		}
		row, known := rowByID[coverage.RequirementID]
		if !known {
			return RequirementCoverageReport{}, fmt.Errorf("coverage %s references unknown requirement %s", coverage.CoverageID, coverage.RequirementID)
		}
		if coverage.ProofObligationID == "" || coverage.AssertionID == "" || coverage.ProofType == "" || coverage.ProofHome == "" || coverage.ScenarioID == "" {
			return RequirementCoverageReport{}, fmt.Errorf("coverage %s is incomplete", coverage.CoverageID)
		}
		if _, duplicate := seenCoverage[coverage.CoverageID]; duplicate {
			return RequirementCoverageReport{}, fmt.Errorf("coverage repeats %s", coverage.CoverageID)
		}
		seenCoverage[coverage.CoverageID] = struct{}{}

		proofKey := proofHomeKey(coverage.AssertionID, coverage.ProofType, coverage.SupportCellID)
		if previous, duplicate := seenProofHomes[proofKey]; duplicate && previous != coverage.ProofObligationID {
			return RequirementCoverageReport{}, fmt.Errorf("duplicate proof homes for assertion %s, proof type %s, support cell %s: obligations %s and %s", coverage.AssertionID, coverage.ProofType, optionalString(coverage.SupportCellID), previous, coverage.ProofObligationID)
		}
		seenProofHomes[proofKey] = coverage.ProofObligationID
		row.Proofs = append(row.Proofs, RequirementProofEntry{
			ScenarioID:        coverage.ScenarioID,
			ProofObligationID: coverage.ProofObligationID,
			AssertionID:       coverage.AssertionID,
			SupportCellID:     copyString(coverage.SupportCellID),
			ProofType:         coverage.ProofType,
			ProofHome:         coverage.ProofHome,
			Gate:              coverage.TestID,
		})
	}

	for index := range rows {
		row := &rows[index]
		if len(row.Proofs) == 0 {
			return RequirementCoverageReport{}, fmt.Errorf("requirement %s has zero executed proof", row.RequirementID)
		}
		seenTypes := make(map[string]struct{}, len(row.Proofs))
		for _, proof := range row.Proofs {
			seenTypes[proof.ProofType] = struct{}{}
		}
		for _, required := range row.RequiredProofTypes {
			if _, found := seenTypes[required]; !found {
				return RequirementCoverageReport{}, fmt.Errorf("requirement %s has no executed proof of type %s", row.RequirementID, required)
			}
		}
	}

	sort.Slice(rows, func(left, right int) bool { return rows[left].RequirementID < rows[right].RequirementID })
	for index := range rows {
		sort.Strings(rows[index].RequiredProofTypes)
		sort.Slice(rows[index].Proofs, func(left, right int) bool {
			return proofEntryKey(rows[index].Proofs[left]) < proofEntryKey(rows[index].Proofs[right])
		})
	}
	report := RequirementCoverageReport{
		SchemaVersion: RequirementCoverageSchemaVersion,
		SourceCommit:  summary.SourceCommit,
		Requirements:  rows,
	}
	if err := report.Validate(); err != nil {
		return RequirementCoverageReport{}, err
	}
	return report, nil
}

// Validate checks report shape and the one-proof-home rule.
func (report RequirementCoverageReport) Validate() error {
	if report.SchemaVersion != RequirementCoverageSchemaVersion || report.SourceCommit == "" || len(report.Requirements) == 0 {
		return errors.New("requirement coverage report is incomplete")
	}
	seenRequirements := make(map[string]struct{}, len(report.Requirements))
	seenProofHomes := make(map[string]string)
	for _, requirement := range report.Requirements {
		if requirement.RequirementID == "" || requirement.Title == "" || len(requirement.RequiredProofTypes) == 0 || len(requirement.Proofs) == 0 {
			return fmt.Errorf("requirement coverage row %s is incomplete", requirement.RequirementID)
		}
		if _, duplicate := seenRequirements[requirement.RequirementID]; duplicate {
			return fmt.Errorf("requirement coverage repeats %s", requirement.RequirementID)
		}
		seenRequirements[requirement.RequirementID] = struct{}{}
		requiredTypes := make(map[string]struct{}, len(requirement.RequiredProofTypes))
		for _, proofType := range requirement.RequiredProofTypes {
			if proofType == "" {
				return fmt.Errorf("requirement %s has an empty required proof type", requirement.RequirementID)
			}
			requiredTypes[proofType] = struct{}{}
		}
		seenTypes := make(map[string]struct{}, len(requirement.Proofs))
		for _, proof := range requirement.Proofs {
			if proof.ScenarioID == "" || proof.ProofObligationID == "" || proof.AssertionID == "" || proof.ProofType == "" || proof.ProofHome == "" || proof.Gate == "" {
				return fmt.Errorf("requirement %s has an incomplete proof", requirement.RequirementID)
			}
			if _, required := requiredTypes[proof.ProofType]; !required {
				return fmt.Errorf("requirement %s has non-required proof type %s", requirement.RequirementID, proof.ProofType)
			}
			seenTypes[proof.ProofType] = struct{}{}
			key := proofHomeKey(proof.AssertionID, proof.ProofType, proof.SupportCellID)
			if previous, duplicate := seenProofHomes[key]; duplicate && previous != proof.ProofObligationID {
				return fmt.Errorf("duplicate proof homes for assertion %s, proof type %s, support cell %s: obligations %s and %s", proof.AssertionID, proof.ProofType, optionalString(proof.SupportCellID), previous, proof.ProofObligationID)
			}
			seenProofHomes[key] = proof.ProofObligationID
		}
		for proofType := range requiredTypes {
			if _, found := seenTypes[proofType]; !found {
				return fmt.Errorf("requirement %s has no executed proof of type %s", requirement.RequirementID, proofType)
			}
		}
	}
	return nil
}

// WriteRequirementCoverageJSON writes deterministic JSON with one trailing newline.
func WriteRequirementCoverageJSON(writer io.Writer, report RequirementCoverageReport) error {
	if writer == nil {
		return errors.New("requirement coverage JSON writer is nil")
	}
	if err := report.Validate(); err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

// WriteRequirementCoverageMarkdown writes one row for every executed proof.
func WriteRequirementCoverageMarkdown(writer io.Writer, report RequirementCoverageReport) error {
	if writer == nil {
		return errors.New("requirement coverage Markdown writer is nil")
	}
	if err := report.Validate(); err != nil {
		return err
	}
	var output bytes.Buffer
	output.WriteString("# Requirement Coverage\n\n")
	output.WriteString("| Requirement | Title | Obligation | Proof type | Gate | Proof home |\n")
	output.WriteString("| --- | --- | --- | --- | --- | --- |\n")
	for _, requirement := range report.Requirements {
		for _, proof := range requirement.Proofs {
			fmt.Fprintf(&output, "| `%s` | %s | `%s` | `%s` | `%s` | `%s` |\n", requirement.RequirementID, requirement.Title, proof.ProofObligationID, proof.ProofType, proof.Gate, proof.ProofHome)
		}
	}
	_, err := writer.Write(output.Bytes())
	return err
}

func executedSummaryObligations(obligations []Obligation) (map[string]struct{}, error) {
	if len(obligations) == 0 {
		return nil, errors.New("phase-5 summary obligations are missing")
	}
	executed := make(map[string]struct{}, len(obligations))
	for _, obligation := range obligations {
		if obligation.ID == "" {
			return nil, errors.New("phase-5 summary contains an obligation without an ID")
		}
		if _, duplicate := executed[obligation.ID]; duplicate {
			return nil, fmt.Errorf("phase-5 summary repeats obligation %s", obligation.ID)
		}
		if obligation.Status != "passed" || !obligation.Terminal || obligation.TestCount < 1 {
			return nil, fmt.Errorf("phase-5 summary obligation %s is not an executed pass", obligation.ID)
		}
		executed[obligation.ID] = struct{}{}
	}
	return executed, nil
}

func proofEntryKey(proof RequirementProofEntry) string {
	return strings.Join([]string{proof.ProofObligationID, proof.AssertionID, proof.ProofType, optionalString(proof.SupportCellID), proof.Gate}, "\x00")
}

func proofHomeKey(assertionID, proofType string, supportCellID *string) string {
	return strings.Join([]string{assertionID, proofType, optionalString(supportCellID)}, "\x00")
}

func copyString(value *string) *string {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}
