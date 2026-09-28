package soak

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/trainstar/synchro/conformance/invariants"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

func TestCheckSourceStateRejectsDeliveryThatDiffersFromSource(t *testing.T) {
	operation := Operation{Sequence: 4, Kind: OperationPull, UserID: "user-a", ClientID: "client-a", ScopeID: "scope-a"}
	if violations := sourceStateFixtureViolations(operation, func(*ObservationCapture) {}); len(violations) != 0 {
		t.Fatalf("matching source state produced violations %#v", violations)
	}
	unauthorized := vectors.Row{PK: json.RawMessage(`"row-unauthorized"`), Fields: []vectors.RowField{{FieldID: stablePKFieldID, Value: json.RawMessage(`"row-unauthorized"`)}, {FieldID: stableValueFieldID, Value: json.RawMessage(`"value-unauthorized"`)}}}
	tests := []struct {
		name   string
		rule   invariants.RuleID
		mutate func(*ObservationCapture)
	}{
		{name: "omitted delivered row", rule: RuleSourceStateMembership, mutate: func(capture *ObservationCapture) {
			client := &capture.Clients[0]
			client.Rows = client.Rows[:1]
			client.ScopeRows = []invariants.ClientScopeRowObservation{client.ScopeRows[0], client.ScopeRows[2]}
		}},
		{name: "unauthorized delivered row", rule: RuleSourceStateUnauthorizedRow, mutate: func(capture *ObservationCapture) {
			addClientRow(capture, unauthorized, operation.ScopeID)
		}},
		{name: "equal-size membership swap", rule: RuleSourceStateMembership, mutate: func(capture *ObservationCapture) {
			client := &capture.Clients[0]
			identity := addClientRow(capture, unauthorized, "")
			client.ScopeRows[1].Entry.RowIdentity = identity
		}},
		{name: "changed held value", rule: RuleSourceStateValueMismatch, mutate: func(capture *ObservationCapture) {
			capture.Clients[0].Rows[1].Row.Fields[1].Value = json.RawMessage(`"value-changed"`)
		}},
		{name: "source differs from authored model", rule: RuleSourceStateAuthoredMismatch, mutate: func(capture *ObservationCapture) {
			capture.SourceState.Source = capture.SourceState.Source[:1]
		}},
		{name: "incomplete client", rule: RuleSourceStateClientIncomplete, mutate: func(capture *ObservationCapture) {
			capture.Clients[0].Complete = false
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			violations := sourceStateFixtureViolations(operation, test.mutate)
			found := false
			for _, violation := range violations {
				if violation.Family != InvariantSourceState {
					t.Fatalf("violation family = %s, want %s", violation.Family, InvariantSourceState)
				}
				found = found || violation.RuleID == test.rule
			}
			if !found {
				t.Fatalf("violations %#v do not contain %s", violations, test.rule)
			}
		})
	}
}

// An omitted row whose local digest is recomputed and whose authoritative
// digest is forged to match passes every protocol checker, so only the source
// comparison can fail the run.
func TestRunFailsOmittedDeliveryWithMatchingMetadataThroughSourceState(t *testing.T) {
	catalog := testCatalog(t)
	plan, err := Generate(5, Config{OperationCount: MinimumCoverageOperations}, catalog)
	if err != nil {
		t.Fatalf("generate plan: %v", err)
	}
	result, err := Run(context.Background(), plan, omittingHarness{}, journalPath(t, "omitted-delivery"))
	if !errors.Is(err, ErrInvariantViolation) {
		t.Fatalf("omitted delivery error = %v, want ErrInvariantViolation", err)
	}
	if len(result.Violations) == 0 || result.Failure == nil || result.Failure.Sequence != 1 {
		t.Fatalf("omitted delivery result = %#v", result)
	}
	for _, violation := range result.Violations {
		if violation.Family != InvariantSourceState || violation.RuleID != RuleSourceStateMembership {
			t.Fatalf("omitted delivery violation = %#v, want only source-state membership", violation)
		}
	}
}

type omittingHarness struct{}

func (omittingHarness) Execute(_ context.Context, operation Operation) (ObservationCapture, error) {
	capture := captureForOperation(operation, "pid-stable")
	// The live harness has no server membership facts for protocol rows.
	rowCount := uint64(2)
	capture.ServerState = &scenarios.StateFacts{RowCount: &rowCount}
	client := &capture.Clients[0]
	client.Rows = client.Rows[:1]
	client.ScopeRows = []invariants.ClientScopeRowObservation{client.ScopeRows[0], client.ScopeRows[2]}
	for index, scope := range client.Scopes {
		digest, err := vectors.ScopeDigest(capture.Manifest.Hash(), scope.ScopeID, []vectors.DigestEntry{client.ScopeRows[index].Entry})
		if err != nil {
			panic(err)
		}
		client.Scopes[index].LocalDigest = &digest
		client.Scopes[index].AuthoritativeDigest = &digest
	}
	return capture, nil
}

func sourceStateFixtureViolations(operation Operation, mutate func(*ObservationCapture)) []invariants.Violation {
	capture := captureForOperation(operation, "pid-stable")
	mutate(&capture)
	return CheckSourceState(operation.Sequence, capture.Manifest, capture.Clients, *capture.SourceState)
}

func addClientRow(capture *ObservationCapture, row vectors.Row, scopeID string) []byte {
	identity, err := vectors.RowIdentity(*capture.Manifest, stableTableID, row.PK)
	if err != nil {
		panic(err)
	}
	digest, err := vectors.RowDigest(*capture.Manifest, stableTableID, row, stableServerVersion)
	if err != nil {
		panic(err)
	}
	client := &capture.Clients[0]
	client.Rows = append(client.Rows, invariants.ClientRowObservation{TableID: stableTableID, Row: row, ServerVersion: stableServerVersion, StoredDigest: &digest})
	if scopeID != "" {
		client.ScopeRows = append(client.ScopeRows, invariants.ClientScopeRowObservation{ScopeID: scopeID, Entry: vectors.DigestEntry{RowIdentity: identity, RowDigest: digest}, Generation: 4})
	}
	return identity
}
