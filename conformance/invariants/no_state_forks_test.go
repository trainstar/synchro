package invariants

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

func TestCheckNoStateForksAcceptsOrdinaryCaptureAndRestartBoundary(t *testing.T) {
	observations := noStateForksFixture()
	violations, err := CheckNoStateForks(observations)
	assertNoViolations(t, violations, err)
}

func TestCheckNoStateForksCatchesEachRule(t *testing.T) {
	tests := []struct {
		name   string
		ruleID RuleID
		mutate func(*[]Observation)
	}{
		{name: "capture incomplete", ruleID: RuleStateForkCaptureIncomplete, mutate: func(observations *[]Observation) {
			(*observations)[2].Clients[0].Complete = false
		}},
		{name: "process identity missing", ruleID: RuleStateForkProcessIdentityMissing, mutate: func(observations *[]Observation) {
			(*observations)[2].Clients[0].Process = nil
		}},
		{name: "process identity invalid", ruleID: RuleStateForkProcessIdentityInvalid, mutate: func(observations *[]Observation) {
			(*observations)[2].Clients[0].Process.ProcessID = strings.Repeat("p", 257)
		}},
		{name: "process not replaced", ruleID: RuleStateForkProcessNotReplaced, mutate: func(observations *[]Observation) {
			(*observations)[2].Clients[0].Process.ProcessID = "process-before"
		}},
		{name: "process replaced unexpectedly", ruleID: RuleStateForkProcessReplacedUnexpectedly, mutate: func(observations *[]Observation) {
			(*observations)[1].Clients[0].Process.ProcessID = "process-silent"
		}},
		{name: "database identity changed", ruleID: RuleStateForkDatabaseIdentityChanged, mutate: func(observations *[]Observation) {
			(*observations)[2].Clients[0].Process.DatabaseIdentityFingerprint = strings.Repeat("b", 64)
		}},
		{name: "durable state invalid", ruleID: RuleStateForkDurableStateInvalid, mutate: func(observations *[]Observation) {
			(*observations)[2].Clients[0].Rows[0].Row.PK = json.RawMessage(`{`)
		}},
		{name: "durable state changed", ruleID: RuleStateForkDurableStateChanged, mutate: func(observations *[]Observation) {
			(*observations)[2].Clients[0].State.Outcomes = append(
				(*observations)[2].Clients[0].State.Outcomes,
				scenarios.MutationOutcomeFact{MutationID: "mutation-mutant", State: "applied"},
			)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observations := noStateForksFixture()
			test.mutate(&observations)
			violations, err := CheckNoStateForks(observations)
			assertCaughtRule(t, violations, err, test.ruleID)
		})
	}
}

func TestCheckNoStateForksRejectsBoundaryWithoutPriorCapture(t *testing.T) {
	client := stateForkClientFixture("process-after")
	client.RestartBoundary = true
	violations, err := CheckNoStateForks([]Observation{{Sequence: 1, Clients: []ClientObservation{client}}})
	assertCaughtRule(t, violations, err, RuleStateForkCaptureIncomplete)
}

func TestCheckNoStateForksRejectsMalformedFingerprintAtBoundary(t *testing.T) {
	observations := noStateForksFixture()
	observations[2].Clients[0].Process.DatabaseIdentityFingerprint = strings.Repeat("A", 64)
	violations, err := CheckNoStateForks(observations)
	assertCaughtRule(t, violations, err, RuleStateForkProcessIdentityInvalid)
}

func noStateForksFixture() []Observation {
	first := stateForkClientFixture("process-before")
	ordinary := stateForkClientFixture("process-before")
	restarted := stateForkClientFixture("process-after")
	restarted.RestartBoundary = true
	return []Observation{
		{Sequence: 1, Clients: []ClientObservation{first}},
		{Sequence: 2, Clients: []ClientObservation{ordinary}},
		{Sequence: 3, Clients: []ClientObservation{restarted}},
	}
}

func stateForkClientFixture(processID string) ClientObservation {
	cursor := "cursor-authored"
	digest := [32]byte{1, 2, 3}
	return ClientObservation{
		State: scenarios.ClientDurabilityFact{
			UserID: "user-authored", ClientID: "client-authored",
			Queue: []scenarios.QueuedMutationFact{{
				MutationID: "mutation-authored", TableID: "table-authored", CanonicalWireJSON: `{"value":1}`,
				AuthoredSchema: scenarios.SchemaFact{Version: 1, Hash: "schema-authored"},
				Operation:      "insert", ClientVersion: "client-version-authored", LocalOrder: 1, Status: "pending",
			}},
		},
		Rows: []ClientRowObservation{{
			TableID: "table-authored",
			Row: vectors.Row{
				PK: json.RawMessage(`"row-authored"`),
				Fields: []vectors.RowField{
					{FieldID: "field-value", Value: json.RawMessage(`1`)},
					{FieldID: "field-id", Value: json.RawMessage(`"row-authored"`)},
				},
			},
			ServerVersion: "server-version-authored", StoredDigest: &digest,
		}},
		Scopes: []ClientScopeObservation{{
			ScopeID: "scope-authored", RawCursor: &cursor,
			AuthoritativeDigest: &digest, LocalDigest: &digest, Generation: 1,
		}},
		ScopeRows: []ClientScopeRowObservation{{
			ScopeID: "scope-authored", Generation: 1,
			Entry: vectors.DigestEntry{RowIdentity: []byte("row-identity-authored"), RowDigest: digest},
		}},
		Process: &ProcessIdentityObservation{
			ProcessID: processID, DatabaseIdentityFingerprint: strings.Repeat("a", 64),
		},
		Complete: true,
	}
}
