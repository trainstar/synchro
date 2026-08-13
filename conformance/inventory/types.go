// Package inventory projects authenticated terminal evidence into sorted rows.
package inventory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/trainstar/synchro/conformance/evidence"
	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// Inputs supplies the authored scenario set and one immutable candidate.
type Inputs struct {
	Contract     *contract.Bundle
	Scenarios    []scenarios.Scenario
	EvidenceRoot string
	Candidate    evidence.Candidate
}

// Report is a closed, generated projection of terminal evidence.
type Report struct {
	SchemaVersion   int    `json:"schema_version"`
	CandidateID     string `json:"candidate_id"`
	ProtocolVersion int    `json:"protocol_version"`
	Rows            []Row  `json:"rows"`
}

// UnmarshalJSON rejects fields outside the closed report schema.
func (r *Report) UnmarshalJSON(data []byte) error {
	type report Report
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var value report
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("inventory report has trailing JSON values")
		}
		return err
	}
	*r = Report(value)
	return nil
}

// Row binds one authored ownership tuple to one terminal evidence attempt.
type Row struct {
	RequirementID     string                           `json:"requirement_id"`
	ScenarioID        string                           `json:"scenario_id"`
	ProofObligationID string                           `json:"proof_obligation_id"`
	AssertionID       string                           `json:"assertion_id"`
	SupportCellID     *string                          `json:"support_cell_id"`
	ProofType         string                           `json:"proof_type"`
	ProtocolVersion   int                              `json:"protocol_version"`
	Result            execution.Result                 `json:"result"`
	Lineage           Lineage                          `json:"lineage"`
	Artifacts         []Artifact                       `json:"artifacts"`
	Environment       []execution.EnvironmentDimension `json:"environment"`
	Seed              *string                          `json:"seed"`
	Attachments       []evidence.Attachment            `json:"attachments"`
	NegativeControl   *execution.NegativeControl       `json:"negative_control"`
}

// Lineage identifies the terminal authenticated execution attempt.
type Lineage struct {
	EvidenceID         string `json:"evidence_id"`
	ReceiptID          string `json:"receipt_id"`
	RunID              string `json:"run_id"`
	ExecutionLineageID string `json:"execution_lineage_id"`
	Attempt            int    `json:"attempt"`
}

// Artifact records one exact candidate artifact payload.
type Artifact struct {
	InventoryID string `json:"inventory_id"`
	ArtifactID  string `json:"artifact_id"`
	Role        string `json:"role"`
	Path        string `json:"path"`
	MediaType   string `json:"media_type"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256"`
}
