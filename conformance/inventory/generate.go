package inventory

import (
	"context"
	"fmt"
	"sort"

	"github.com/trainstar/synchro/conformance/evidence"
	"github.com/trainstar/synchro/conformance/execution"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// Generate creates one row for every authored ownership tuple.
func Generate(ctx context.Context, in Inputs) (Report, error) {
	if ctx == nil {
		return Report{}, fmt.Errorf("inventory context is nil")
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if in.Contract == nil || len(in.Scenarios) == 0 || in.EvidenceRoot == "" || in.Candidate.ID == "" || in.Candidate.ProtocolVersion < 1 {
		return Report{}, fmt.Errorf("inventory inputs are incomplete")
	}
	expected, scenarioIndex, err := expectedOwnership(in.Contract, in.Scenarios)
	if err != nil {
		return Report{}, err
	}
	terminal, err := evidence.LoadTerminalEvidence(ctx, in.Candidate, in.EvidenceRoot)
	if err != nil {
		return Report{}, err
	}
	actual := make(map[string]Row, len(expected))
	for _, item := range terminal {
		scenario, found := scenarioIndex[item.ScenarioID]
		if !found {
			return Report{}, fmt.Errorf("inventory evidence has an unselected scenario %s", item.ScenarioID)
		}
		obligation, found := obligation(scenario, item.ProofObligationID)
		if !found || obligation.ProofType != item.ProofType || !sameSupport(item.SupportCellID, obligation.SupportCellID) || !sameRequirements(item.RequirementIDs, obligation.RequirementIDs) {
			return Report{}, fmt.Errorf("inventory evidence does not match an authored obligation")
		}
		if item.Run.Result != "passed" || item.Run.ExitCode != 0 {
			return Report{}, fmt.Errorf("inventory terminal evidence did not pass")
		}
		for _, assertion := range item.Assertions {
			if assertion.Outcome != "passed" {
				return Report{}, fmt.Errorf("inventory terminal assertion did not pass")
			}
			key, row, found := ownershipRow(item, assertion.AssertionID, expected)
			if !found {
				return Report{}, fmt.Errorf("inventory evidence has an unowned assertion")
			}
			if _, duplicate := actual[key]; duplicate {
				return Report{}, fmt.Errorf("inventory has duplicate ownership tuple")
			}
			actual[key] = row
		}
	}
	if len(actual) != len(expected) {
		return Report{}, fmt.Errorf("inventory ownership tuple count is incomplete")
	}
	for key := range expected {
		if _, found := actual[key]; !found {
			return Report{}, fmt.Errorf("inventory omits an authored ownership tuple")
		}
	}
	rows := make([]Row, 0, len(actual))
	for _, row := range actual {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(left, right int) bool { return rowKey(rows[left]) < rowKey(rows[right]) })
	report := Report{SchemaVersion: 1, CandidateID: in.Candidate.ID, ProtocolVersion: in.Candidate.ProtocolVersion, Rows: rows}
	if err := report.Validate(); err != nil {
		return Report{}, err
	}
	return report, nil
}

type expectedTuple struct {
	requirement string
	scenario    string
	obligation  string
	assertion   string
	proof       string
	support     *string
}

func expectedOwnership(bundle *contract.Bundle, values []scenarios.Scenario) (map[string]expectedTuple, map[string]scenarios.Scenario, error) {
	requirements := make(map[string]contract.Requirement, len(bundle.Requirements.Requirements))
	for _, requirement := range bundle.Requirements.Requirements {
		requirements[string(requirement.ID)] = requirement
	}
	support := requiredSupportCells(bundle)
	expected := make(map[string]expectedTuple)
	indexed := make(map[string]scenarios.Scenario, len(values))
	for _, scenario := range values {
		if scenario.ID == "" {
			return nil, nil, fmt.Errorf("inventory scenario ID is empty")
		}
		if _, duplicate := indexed[string(scenario.ID)]; duplicate {
			return nil, nil, fmt.Errorf("inventory has duplicate scenario ID")
		}
		indexed[string(scenario.ID)] = scenario
		if err := ownershipClosed(scenario); err != nil {
			return nil, nil, err
		}
		for _, requirementID := range scenario.RequirementIDs {
			requirement, found := requirements[string(requirementID)]
			if !found {
				return nil, nil, fmt.Errorf("inventory scenario has an unknown requirement")
			}
			if err := requireProofs(scenario, requirement, support); err != nil {
				return nil, nil, err
			}
		}
		for _, owner := range scenario.Ownership {
			if owner.ScenarioID != scenario.ID {
				return nil, nil, fmt.Errorf("inventory ownership scenario binding is invalid")
			}
			item, found := obligation(scenario, string(owner.ProofObligationID))
			if !found || item.ProofType != owner.ProofType || !sameContractSupport(owner.SupportCellID, item.SupportCellID) || !containsRequirement(item.RequirementIDs, owner.RequirementID) || !assertionOwnsRequirement(scenario, owner.AssertionID, owner.RequirementID) {
				return nil, nil, fmt.Errorf("inventory ownership tuple is invalid")
			}
			supportID := stringPointer(owner.SupportCellID)
			tuple := expectedTuple{requirement: string(owner.RequirementID), scenario: string(owner.ScenarioID), obligation: string(owner.ProofObligationID), assertion: string(owner.AssertionID), proof: owner.ProofType, support: supportID}
			key := tupleKey(tuple)
			if _, duplicate := expected[key]; duplicate {
				return nil, nil, fmt.Errorf("inventory has duplicate authored ownership tuple")
			}
			expected[key] = tuple
		}
	}
	if len(expected) == 0 {
		return nil, nil, fmt.Errorf("inventory has no authored ownership tuples")
	}
	return expected, indexed, nil
}

func ownershipClosed(scenario scenarios.Scenario) error {
	assertions := make(map[contract.AssertionID]scenarios.Assertion, len(scenario.Assertions))
	for _, assertion := range scenario.Assertions {
		assertions[assertion.ID] = assertion
	}
	expected := make(map[string]struct{})
	for _, obligation := range scenario.ProofObligations {
		for _, assertionID := range obligation.AssertionIDs {
			assertion, found := assertions[assertionID]
			if !found {
				return fmt.Errorf("inventory obligation has an unknown assertion")
			}
			for _, requirementID := range assertion.RequirementIDs {
				tuple := expectedTuple{requirement: string(requirementID), scenario: string(scenario.ID), obligation: string(obligation.ObligationID), assertion: string(assertionID), proof: obligation.ProofType, support: stringPointer(obligation.SupportCellID)}
				expected[tupleKey(tuple)] = struct{}{}
			}
		}
	}
	actual := make(map[string]struct{}, len(scenario.Ownership))
	for _, owner := range scenario.Ownership {
		tuple := expectedTuple{requirement: string(owner.RequirementID), scenario: string(owner.ScenarioID), obligation: string(owner.ProofObligationID), assertion: string(owner.AssertionID), proof: owner.ProofType, support: stringPointer(owner.SupportCellID)}
		key := tupleKey(tuple)
		if _, duplicate := actual[key]; duplicate {
			return fmt.Errorf("inventory has duplicate authored ownership tuple")
		}
		actual[key] = struct{}{}
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("inventory ownership tuple closure is incomplete")
	}
	for key := range expected {
		if _, found := actual[key]; !found {
			return fmt.Errorf("inventory ownership tuple closure is incomplete")
		}
	}
	return nil
}

func requireProofs(scenario scenarios.Scenario, requirement contract.Requirement, support map[string][]contract.SupportCellID) error {
	for _, proof := range requirement.RequiredProofTypes {
		var cells []*contract.SupportCellID
		switch proof {
		case "reference-model", "negative-control":
			cells = []*contract.SupportCellID{nil}
		case "fault-injection":
			cells = []*contract.SupportCellID{nil}
		case "server-black-box":
			if containsString(requirement.ApplicableComponents, "postgresql-server") {
				cells = pointers(support["postgresql-server"])
			}
		case "native-e2e":
			for _, component := range []string{"swift-client", "kotlin-client", "react-native-client"} {
				if containsString(requirement.ApplicableComponents, component) {
					cells = append(cells, pointers(support[component])...)
				}
			}
		default:
			return fmt.Errorf("inventory requirement has an unknown proof type")
		}
		if proof == "fault-injection" {
			count := 0
			for _, item := range scenario.ProofObligations {
				if item.ProofType == proof && containsRequirement(item.RequirementIDs, requirement.ID) {
					count++
				}
			}
			if count != 1 {
				return fmt.Errorf("inventory scenario omits or duplicates a required proof type")
			}
			continue
		}
		for _, cell := range cells {
			count := 0
			for _, item := range scenario.ProofObligations {
				if item.ProofType == proof && containsRequirement(item.RequirementIDs, requirement.ID) && sameContractSupport(item.SupportCellID, cell) {
					count++
				}
			}
			if count != 1 {
				return fmt.Errorf("inventory scenario omits or duplicates a required proof type or support cell")
			}
		}
	}
	return nil
}

func ownershipRow(item evidence.Evidence, assertionID string, expected map[string]expectedTuple) (string, Row, bool) {
	for key, tuple := range expected {
		if tuple.scenario != item.ScenarioID || tuple.obligation != item.ProofObligationID || tuple.assertion != assertionID || tuple.proof != item.ProofType || !sameStringPointers(tuple.support, item.SupportCellID) {
			continue
		}
		return key, rowFromEvidence(tuple, item), true
	}
	return "", Row{}, false
}

func rowFromEvidence(tuple expectedTuple, item evidence.Evidence) Row {
	artifacts := make([]Artifact, 0, len(item.ArtifactBindings))
	for _, artifact := range item.ArtifactBindings {
		size := artifact.SizeBytes
		if size == 0 {
			size = artifact.Size
		}
		artifacts = append(artifacts, Artifact{InventoryID: artifact.InventoryID, ArtifactID: artifact.ArtifactID, Role: artifact.Role, Path: artifact.Path, MediaType: artifact.MediaType, SizeBytes: size, SHA256: artifact.SHA256})
	}
	sort.Slice(artifacts, func(left, right int) bool { return artifactKey(artifacts[left]) < artifactKey(artifacts[right]) })
	attachments := append([]evidence.Attachment(nil), item.Attachments...)
	sort.Slice(attachments, func(left, right int) bool { return attachments[left].ID < attachments[right].ID })
	environment := append([]execution.EnvironmentDimension(nil), item.Environment...)
	sort.Slice(environment, func(left, right int) bool { return environment[left].Name < environment[right].Name })
	return Row{RequirementID: tuple.requirement, ScenarioID: tuple.scenario, ProofObligationID: tuple.obligation, AssertionID: tuple.assertion, SupportCellID: copyString(tuple.support), ProofType: tuple.proof, ProtocolVersion: item.ProtocolVersion, Result: item.Run.Result, Lineage: Lineage{EvidenceID: item.EvidenceID, ReceiptID: item.ReceiptID, RunID: item.Run.ID, ExecutionLineageID: item.Run.ExecutionLineageID, Attempt: item.Run.Attempt}, Artifacts: artifacts, Environment: environment, Seed: copyString(item.Seed), Attachments: attachments, NegativeControl: item.NegativeControl}
}
