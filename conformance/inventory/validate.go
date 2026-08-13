package inventory

import (
	"fmt"
	"sort"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// Validate checks the closed report shape and deterministic row ordering.
func (r Report) Validate() error {
	if r.SchemaVersion != 1 || r.CandidateID == "" || r.ProtocolVersion < 1 || len(r.Rows) == 0 {
		return fmt.Errorf("inventory report is incomplete")
	}
	seen := make(map[string]struct{}, len(r.Rows))
	previous := ""
	for _, row := range r.Rows {
		if row.RequirementID == "" || row.ScenarioID == "" || row.ProofObligationID == "" || row.AssertionID == "" || row.ProofType == "" || row.ProtocolVersion != r.ProtocolVersion || row.Result != "passed" || row.Lineage.EvidenceID == "" || row.Lineage.ReceiptID == "" || row.Lineage.RunID == "" || row.Lineage.ExecutionLineageID == "" || row.Lineage.Attempt < 1 || len(row.Artifacts) == 0 || len(row.Attachments) == 0 {
			return fmt.Errorf("inventory row is incomplete")
		}
		if row.ProofType == "negative-control" && row.NegativeControl == nil {
			return fmt.Errorf("inventory negative control is missing")
		}
		if row.ProofType != "negative-control" && row.NegativeControl != nil {
			return fmt.Errorf("inventory row has an unbound negative control")
		}
		key := rowKey(row)
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("inventory report has duplicate rows")
		}
		if previous != "" && key < previous {
			return fmt.Errorf("inventory rows are not sorted")
		}
		if err := validateArtifacts(row.Artifacts); err != nil {
			return err
		}
		if err := validateAttachments(row); err != nil {
			return err
		}
		seen[key] = struct{}{}
		previous = key
	}
	return nil
}

func validateArtifacts(values []Artifact) error {
	seen := make(map[string]struct{}, len(values))
	previous := ""
	for _, item := range values {
		if item.InventoryID == "" || item.ArtifactID == "" || item.Path == "" || item.MediaType == "" || item.SizeBytes < 0 || !isSHA256(item.SHA256) {
			return fmt.Errorf("inventory artifact is invalid")
		}
		key := artifactKey(item)
		if _, duplicate := seen[key]; duplicate || (previous != "" && key < previous) {
			return fmt.Errorf("inventory artifacts are not a sorted unique set")
		}
		seen[key] = struct{}{}
		previous = key
	}
	return nil
}

func validateAttachments(row Row) error {
	seen := make(map[string]struct{}, len(row.Attachments))
	previous := ""
	for _, item := range row.Attachments {
		if item.ID == "" || item.Kind == "" || item.Path == "" || item.MediaType == "" || item.SizeBytes < 0 || !isSHA256(item.SHA256) {
			return fmt.Errorf("inventory attachment is invalid")
		}
		if _, duplicate := seen[item.ID]; duplicate || (previous != "" && item.ID < previous) {
			return fmt.Errorf("inventory attachments are not a sorted unique set")
		}
		seen[item.ID] = struct{}{}
		previous = item.ID
	}
	return nil
}

func rowKey(row Row) string {
	return row.RequirementID + "\x00" + row.ScenarioID + "\x00" + row.ProofObligationID + "\x00" + row.AssertionID + "\x00" + row.ProofType + "\x00" + nullable(row.SupportCellID)
}

func artifactKey(item Artifact) string {
	return item.InventoryID + "\x00" + item.ArtifactID + "\x00" + item.Path
}

func tupleKey(tuple expectedTuple) string {
	return tuple.requirement + "\x00" + tuple.scenario + "\x00" + tuple.obligation + "\x00" + tuple.assertion + "\x00" + tuple.proof + "\x00" + nullable(tuple.support)
}

func nullable(value *string) string {
	if value == nil {
		return "null"
	}
	return *value
}

func isSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func copyString(value *string) *string {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func stringPointer(value *contract.SupportCellID) *string {
	if value == nil {
		return nil
	}
	result := string(*value)
	return &result
}

func sameStringPointers(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameSupport(left *string, right *contract.SupportCellID) bool {
	return sameStringPointers(left, stringPointer(right))
}

func sameContractSupport(left, right *contract.SupportCellID) bool {
	return sameStringPointers(stringPointer(left), stringPointer(right))
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsRequirement(values []contract.RequirementID, wanted contract.RequirementID) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func sameRequirements(left []string, right []contract.RequirementID) bool {
	if len(left) != len(right) {
		return false
	}
	wanted := make([]string, len(right))
	for index, value := range right {
		wanted[index] = string(value)
	}
	sort.Strings(wanted)
	copy := append([]string(nil), left...)
	sort.Strings(copy)
	for index := range wanted {
		if wanted[index] != copy[index] {
			return false
		}
	}
	return true
}

func obligation(scenario scenarios.Scenario, id string) (scenarios.ProofObligation, bool) {
	for _, item := range scenario.ProofObligations {
		if string(item.ObligationID) == id {
			return item, true
		}
	}
	return scenarios.ProofObligation{}, false
}

func assertionOwnsRequirement(scenario scenarios.Scenario, assertionID contract.AssertionID, requirementID contract.RequirementID) bool {
	for _, item := range scenario.Assertions {
		if item.ID != assertionID {
			continue
		}
		return containsRequirement(item.RequirementIDs, requirementID)
	}
	return false
}

func requiredSupportCells(bundle *contract.Bundle) map[string][]contract.SupportCellID {
	result := make(map[string][]contract.SupportCellID)
	for _, cell := range bundle.Support.Cells {
		if cell.Policy == "required" {
			result[cell.Component] = append(result[cell.Component], cell.ID)
		}
	}
	for component := range result {
		sort.Slice(result[component], func(left, right int) bool { return result[component][left] < result[component][right] })
	}
	return result
}

func pointers(values []contract.SupportCellID) []*contract.SupportCellID {
	result := make([]*contract.SupportCellID, 0, len(values))
	for index := range values {
		result = append(result, &values[index])
	}
	return result
}
