package blackbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"github.com/trainstar/synchro/conformance/reference"
)

var (
	// ErrSemanticMismatch reports a well-formed response with wrong semantics.
	ErrSemanticMismatch = errors.New("response semantics do not match")
	// ErrReplayMismatch reports a replay that changed exact response evidence.
	ErrReplayMismatch = errors.New("replay response does not match")
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
		return ErrSemanticMismatch.Error()
	}
	return fmt.Sprintf("%s: %s", f.Assertion, f.Reason)
}

func (f *ComparisonFailure) Unwrap() error {
	return ErrSemanticMismatch
}

// CompareSemanticJSON compares canonical values after declared normalization.
func CompareSemanticJSON(expected, observed []byte, spec NormalizationSpec) error {
	normalizedExpected, err := NormalizeResponse(expected, spec)
	if err != nil {
		return fmt.Errorf("normalize expected response: %w", err)
	}
	normalizedObserved, err := NormalizeResponse(observed, spec)
	if err != nil {
		return fmt.Errorf("normalize observed response: %w", err)
	}
	if !bytes.Equal(normalizedExpected, normalizedObserved) {
		return ErrSemanticMismatch
	}
	return nil
}

// CompareExactReplay checks raw status, relevant headers, and canonical bytes.
func CompareExactReplay(first, replay Response) error {
	if first.Status != replay.Status {
		return fmt.Errorf("%w: raw status", ErrReplayMismatch)
	}
	if !reflect.DeepEqual(relevantHeaders(first.Headers), relevantHeaders(replay.Headers)) {
		return fmt.Errorf("%w: relevant headers", ErrReplayMismatch)
	}
	firstCanonical, err := responseCanonicalBytes(first)
	if err != nil {
		return fmt.Errorf("%w: first canonical response: %v", ErrReplayMismatch, err)
	}
	replayCanonical, err := responseCanonicalBytes(replay)
	if err != nil {
		return fmt.Errorf("%w: replay canonical response: %v", ErrReplayMismatch, err)
	}
	if !bytes.Equal(firstCanonical, replayCanonical) {
		return fmt.Errorf("%w: canonical response bytes", ErrReplayMismatch)
	}
	return nil
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
	if err := CompareSemanticJSON(expectedJSON, observedJSON, wireNormalizationSpec); err != nil {
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

func relevantHeaders(headers http.Header) []RecordedHeader {
	result := make([]RecordedHeader, 0, len(relevantResponseHeaders))
	for _, name := range relevantResponseHeaders {
		values := headers.Values(name)
		if len(values) == 0 {
			continue
		}
		result = append(result, RecordedHeader{Name: http.CanonicalHeaderKey(name), Values: append([]string(nil), values...)})
	}
	return result
}

func responseCanonicalBytes(response Response) ([]byte, error) {
	if response.CanonicalBody != nil {
		return append([]byte(nil), response.CanonicalBody...), nil
	}
	if response.Body == nil {
		return nil, nil
	}
	if responseIsJSON(response.Headers, response.Body) {
		return CanonicalResponseBytes(response.Body)
	}
	return append([]byte(nil), response.Body...), nil
}
