// Package syntheticproof contains layer-6 harness self-tests and negative controls.
// It is not an authoritative server-proof path. Real server behavior belongs to
// the TestReal* mappings in blackbox/integration/real_proof_map_test.go.
package syntheticproof

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/reference"
)

// AssertionName identifies one semantic black-box assertion.
type AssertionName string

const (
	AssertionRawStatus          AssertionName = "raw-status"
	AssertionMutationOutcomes   AssertionName = "mutation-outcome-conservation"
	AssertionDeliveryUniqueness AssertionName = "delivery-uniqueness"
	AssertionScopeBinding       AssertionName = "scope-binding"
	AssertionChecksum           AssertionName = "authoritative-checksum"
	AssertionSemanticResponse   AssertionName = "semantic-response-equality"
	AssertionExactReplay        AssertionName = "exact-replay"
)

// AssertionCheck records one bounded semantic assertion result.
type AssertionCheck struct {
	Name   AssertionName `json:"name"`
	StepID string        `json:"step_id"`
	Passed bool          `json:"passed"`
	Reason string        `json:"reason"`
}

// ComparisonFailure identifies the exact semantic assertion that rejected data.
type ComparisonFailure struct {
	Assertion AssertionName
	Reason    string
}

func (f *ComparisonFailure) Error() string {
	if f == nil {
		return blackbox.ErrSemanticMismatch.Error()
	}
	return fmt.Sprintf("%s: %s", f.Assertion, f.Reason)
}

func (f *ComparisonFailure) Unwrap() error {
	return blackbox.ErrSemanticMismatch
}

func compareWireSemantics(expected, observed wireEnvelope, rawStatus int) ([]AssertionCheck, error) {
	checks := make([]AssertionCheck, 0, 7)
	expectedStatus := outerHTTPStatus(expected.Result)
	if rawStatus != expectedStatus {
		return appendFailedCheck(checks, AssertionRawStatus, "raw HTTP status changed")
	}
	checks = appendPassedCheck(checks, AssertionRawStatus)

	if expected.SchemaVersion != observed.SchemaVersion || expected.ScenarioID != observed.ScenarioID || expected.StepID != observed.StepID || expected.OperationKey != observed.OperationKey || expected.OpaqueValue != observed.OpaqueValue || observed.RequestID == "" {
		return appendFailedCheck(checks, AssertionSemanticResponse, "response identity or opaque value changed")
	}

	if expected.Result.Push != nil {
		if observed.Result.Push == nil || !reflect.DeepEqual(expected.Result.Push.Mutations, observed.Result.Push.Mutations) {
			return appendFailedCheck(checks, AssertionMutationOutcomes, "ordered mutation outcomes changed")
		}
		checks = appendPassedCheck(checks, AssertionMutationOutcomes)
	}

	if expected.Result.Pull != nil {
		if observed.Result.Pull == nil {
			return appendFailedCheck(checks, AssertionSemanticResponse, "pull response changed")
		}
		expectedChanges := expected.Result.Pull.Changes
		observedChanges := observed.Result.Pull.Changes
		switch {
		case oneDuplicateDelivery(expectedChanges, observedChanges):
			return appendFailedCheck(checks, AssertionDeliveryUniqueness, "one delivered effect was duplicated")
		case !sameChangesIgnoringScope(expectedChanges, observedChanges):
			return appendFailedCheck(checks, AssertionDeliveryUniqueness, "delivered effects changed")
		default:
			checks = appendPassedCheck(checks, AssertionDeliveryUniqueness)
		}
		if !reflect.DeepEqual(expectedChanges, observedChanges) {
			return appendFailedCheck(checks, AssertionScopeBinding, "one delivered row used the wrong scope")
		}
		checks = appendPassedCheck(checks, AssertionScopeBinding)
		if !reflect.DeepEqual(expected.Result.Pull.ScopeChecksums, observed.Result.Pull.ScopeChecksums) {
			return appendFailedCheck(checks, AssertionChecksum, "terminal authoritative checksum changed")
		}
		checks = appendPassedCheck(checks, AssertionChecksum)
	}

	expectedJSON, err := json.Marshal(expected)
	if err != nil {
		return checks, fmt.Errorf("encode expected semantic response: %w", err)
	}
	observedJSON, err := json.Marshal(observed)
	if err != nil {
		return checks, fmt.Errorf("encode observed semantic response: %w", err)
	}
	if err := blackbox.CompareSemanticJSON(expectedJSON, observedJSON, wireNormalizationSpec); err != nil {
		return appendFailedCheck(checks, AssertionSemanticResponse, "normalized response values changed")
	}
	checks = appendPassedCheck(checks, AssertionSemanticResponse)
	return checks, nil
}

func appendPassedCheck(checks []AssertionCheck, name AssertionName) []AssertionCheck {
	return append(checks, AssertionCheck{Name: name, Passed: true, Reason: "semantic assertion passed"})
}

func appendFailedCheck(checks []AssertionCheck, name AssertionName, reason string) ([]AssertionCheck, error) {
	checks = append(checks, AssertionCheck{Name: name, Passed: false, Reason: reason})
	return checks, &ComparisonFailure{Assertion: name, Reason: reason}
}

func oneDuplicateDelivery(expected, observed []reference.PullChangeObservation) bool {
	if len(expected) == 0 || len(observed) != len(expected)+1 {
		return false
	}
	for index := range observed {
		without := make([]reference.PullChangeObservation, 0, len(observed)-1)
		without = append(without, observed[:index]...)
		without = append(without, observed[index+1:]...)
		if reflect.DeepEqual(expected, without) {
			return true
		}
	}
	return false
}

func sameChangesIgnoringScope(expected, observed []reference.PullChangeObservation) bool {
	if len(expected) != len(observed) {
		return false
	}
	for index := range expected {
		copy := observed[index]
		copy.Scope = expected[index].Scope
		if !reflect.DeepEqual(expected[index], copy) {
			return false
		}
	}
	return true
}

func outerHTTPStatus(result reference.StepResult) int {
	if result.HTTP != nil {
		return result.HTTP.Status
	}
	return http.StatusOK
}
