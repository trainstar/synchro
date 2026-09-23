package blackbox

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestMeasurementBindingLedgerClosesExactServerOperationSet(t *testing.T) {
	declarations := []MeasurementBindingIdentity{measurementBindingIdentity("SAMPLE-001"), measurementBindingIdentity("SAMPLE-002")}
	ledger, err := NewMeasurementBindingLedger(declarations)
	if err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	for _, declaration := range declarations {
		if err := ledger.Bind(declaration); err != nil {
			t.Fatalf("bind %s: %v", declaration.SampleID, err)
		}
	}
	if err := ledger.Validate(); err != nil {
		t.Fatalf("close ledger: %v", err)
	}
}

func TestMeasurementBindingLedgerRejectsDroppedBinding(t *testing.T) {
	declarations := []MeasurementBindingIdentity{measurementBindingIdentity("SAMPLE-001"), measurementBindingIdentity("SAMPLE-002")}
	ledger, err := NewMeasurementBindingLedger(declarations)
	if err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	if err := ledger.Bind(declarations[0]); err != nil {
		t.Fatalf("bind sample: %v", err)
	}
	requireMeasurementBindingCategory(t, ledger.Validate(), MeasurementBindingMissingExecution)
}

func TestMeasurementBindingLedgerRejectsUnboundAndDoubleBoundOperations(t *testing.T) {
	declaration := measurementBindingIdentity("SAMPLE-001")
	ledger, err := NewMeasurementBindingLedger([]MeasurementBindingIdentity{declaration})
	if err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	unbound := declaration
	unbound.SampleID = "SAMPLE-UNKNOWN"
	requireMeasurementBindingCategory(t, ledger.Bind(unbound), MeasurementBindingUnboundExecution)
	if err := ledger.Bind(declaration); err != nil {
		t.Fatalf("bind declared operation: %v", err)
	}
	requireMeasurementBindingCategory(t, ledger.Bind(declaration), MeasurementBindingDoubleBound)
}

func TestMeasurementBindingLedgerRejectsChangedOperationIdentity(t *testing.T) {
	declaration := measurementBindingIdentity("SAMPLE-001")
	ledger, err := NewMeasurementBindingLedger([]MeasurementBindingIdentity{declaration})
	if err != nil {
		t.Fatalf("create ledger: %v", err)
	}
	changed := declaration
	changed.Boundary = "invalid"
	requireMeasurementBindingCategory(t, ledger.Bind(changed), MeasurementBindingIdentityMismatch)
}

func measurementBindingIdentity(sampleID string) MeasurementBindingIdentity {
	return MeasurementBindingIdentity{
		MeasurementID:  "MEAS-001",
		StratumID:      "STR-001",
		SampleID:       sampleID,
		OperationID:    "MOP-" + sampleID,
		Family:         "fanout",
		Boundary:       "lower",
		OperationValue: json.RawMessage(`{"bound_family":"fanout","boundary":"lower","value":1}`),
	}
}

func requireMeasurementBindingCategory(t *testing.T, err error, want MeasurementBindingFailureCategory) {
	t.Helper()
	if err == nil {
		t.Fatalf("operation succeeded, want category %s", want)
	}
	var failure MeasurementBindingFailure
	if !errors.As(err, &failure) || failure.Category != want {
		t.Fatalf("error = %v, want category %s", err, want)
	}
}
