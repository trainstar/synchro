package blackbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// MeasurementBindingIdentity links one executed server operation to one
// authored measurement sample.
type MeasurementBindingIdentity struct {
	MeasurementID  string
	StratumID      string
	SampleID       string
	OperationID    string
	Family         string
	Boundary       string
	OperationValue json.RawMessage
}

// MeasurementBindingFailureCategory identifies a binding-closure failure.
type MeasurementBindingFailureCategory string

const (
	MeasurementBindingInvalidDeclaration   MeasurementBindingFailureCategory = "invalid-declaration"
	MeasurementBindingDuplicateDeclaration MeasurementBindingFailureCategory = "duplicate-declaration"
	MeasurementBindingUnboundExecution     MeasurementBindingFailureCategory = "unbound-execution"
	MeasurementBindingDoubleBound          MeasurementBindingFailureCategory = "double-bound"
	MeasurementBindingIdentityMismatch     MeasurementBindingFailureCategory = "identity-mismatch"
	MeasurementBindingMissingExecution     MeasurementBindingFailureCategory = "missing-execution"
)

// MeasurementBindingFailure reports one exact measurement-binding failure.
type MeasurementBindingFailure struct {
	Category MeasurementBindingFailureCategory
	Detail   string
}

func (f MeasurementBindingFailure) Error() string {
	return fmt.Sprintf("measurement binding %s: %s", f.Category, f.Detail)
}

// MeasurementBindingLedger closes the one-to-one mapping between authored
// samples and terminal server operations.
type MeasurementBindingLedger struct {
	declared map[string]MeasurementBindingIdentity
	executed map[string]MeasurementBindingIdentity
}

// NewMeasurementBindingLedger loads the complete authored declaration set.
func NewMeasurementBindingLedger(declarations []MeasurementBindingIdentity) (*MeasurementBindingLedger, error) {
	ledger := &MeasurementBindingLedger{
		declared: make(map[string]MeasurementBindingIdentity, len(declarations)),
		executed: make(map[string]MeasurementBindingIdentity, len(declarations)),
	}
	for _, declaration := range declarations {
		key, err := measurementSampleKey(declaration)
		if err != nil {
			return nil, err
		}
		if _, found := ledger.declared[key]; found {
			return nil, MeasurementBindingFailure{Category: MeasurementBindingDuplicateDeclaration, Detail: key}
		}
		ledger.declared[key] = cloneMeasurementBindingIdentity(declaration)
	}
	return ledger, nil
}

// Bind records a completed server operation. Each operation must carry one
// exact authored identity before it can become measurement evidence.
func (ledger *MeasurementBindingLedger) Bind(execution MeasurementBindingIdentity) error {
	if ledger == nil {
		return errors.New("measurement binding ledger is unavailable")
	}
	key, err := measurementSampleKey(execution)
	if err != nil {
		return MeasurementBindingFailure{Category: MeasurementBindingUnboundExecution, Detail: err.Error()}
	}
	declaration, found := ledger.declared[key]
	if !found {
		return MeasurementBindingFailure{Category: MeasurementBindingUnboundExecution, Detail: key}
	}
	if !measurementBindingIdentityEqual(declaration, execution) {
		return MeasurementBindingFailure{Category: MeasurementBindingIdentityMismatch, Detail: key}
	}
	if _, found := ledger.executed[key]; found {
		return MeasurementBindingFailure{Category: MeasurementBindingDoubleBound, Detail: key}
	}
	ledger.executed[key] = cloneMeasurementBindingIdentity(execution)
	return nil
}

// Validate rejects every authored sample that lacks one terminal server operation.
func (ledger *MeasurementBindingLedger) Validate() error {
	if ledger == nil {
		return errors.New("measurement binding ledger is unavailable")
	}
	missing := make([]string, 0)
	for key := range ledger.declared {
		if _, found := ledger.executed[key]; !found {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return MeasurementBindingFailure{Category: MeasurementBindingMissingExecution, Detail: strings.Join(missing, ", ")}
}

func measurementSampleKey(identity MeasurementBindingIdentity) (string, error) {
	if identity.MeasurementID == "" || identity.StratumID == "" || identity.SampleID == "" || identity.OperationID == "" || identity.Family == "" || identity.Boundary == "" || len(identity.OperationValue) == 0 {
		return "", MeasurementBindingFailure{Category: MeasurementBindingInvalidDeclaration, Detail: "measurement, stratum, sample, operation, family, boundary, and value are required"}
	}
	var decoded any
	if err := json.Unmarshal(identity.OperationValue, &decoded); err != nil || decoded == nil {
		return "", MeasurementBindingFailure{Category: MeasurementBindingInvalidDeclaration, Detail: "operation value is invalid"}
	}
	return strings.Join([]string{identity.MeasurementID, identity.StratumID, identity.SampleID}, "|"), nil
}

func measurementBindingIdentityEqual(left, right MeasurementBindingIdentity) bool {
	if left.MeasurementID != right.MeasurementID || left.StratumID != right.StratumID || left.SampleID != right.SampleID || left.OperationID != right.OperationID || left.Family != right.Family || left.Boundary != right.Boundary {
		return false
	}
	var leftValue, rightValue any
	if json.Unmarshal(left.OperationValue, &leftValue) != nil || json.Unmarshal(right.OperationValue, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func cloneMeasurementBindingIdentity(identity MeasurementBindingIdentity) MeasurementBindingIdentity {
	identity.OperationValue = append(json.RawMessage(nil), identity.OperationValue...)
	return identity
}
