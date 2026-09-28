package soak

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/trainstar/synchro/conformance/invariants"
	"github.com/trainstar/synchro/conformance/vectors"
)

// InvariantSourceState compares complete client state with the isolated
// source tables and with the rows that the workload authored.
const InvariantSourceState invariants.InvariantFamily = "source-state"

// Stable source-state rule identifiers.
const (
	RuleSourceStateAuthoredMismatch invariants.RuleID = "source-state.authored-mismatch"
	RuleSourceStateClientIncomplete invariants.RuleID = "source-state.client-incomplete"
	RuleSourceStateRowInvalid       invariants.RuleID = "source-state.row-invalid"
	RuleSourceStateMembership       invariants.RuleID = "source-state.client-membership-mismatch"
	RuleSourceStateUnauthorizedRow  invariants.RuleID = "source-state.client-unauthorized-row"
	RuleSourceStateValueMismatch    invariants.RuleID = "source-state.client-value-mismatch"
)

// SourceStateObservation is complete business state at one quiescent point.
// Source comes from direct queries of the isolated source tables. Authored is
// the independent model of every row that the workload authored. Neither
// uses Synchro membership, checkpoint, or checksum output.
type SourceStateObservation struct {
	Authored []SourceRow
	Source   []SourceRow
}

// SourceRow is one live business row. ScopeIDs come from the authored business
// membership rule. Fields holds each compared business field by manifest field
// ID as canonical wire JSON.
type SourceRow struct {
	TableID    string
	PrimaryKey json.RawMessage
	ScopeIDs   []string
	Fields     map[string]json.RawMessage
}

// CheckSourceState compares authored rows with source rows, then compares each
// complete client's scope membership and held field values with the source.
func CheckSourceState(sequence uint64, manifest *vectors.Manifest, clients []invariants.ClientObservation, state SourceStateObservation) []invariants.Violation {
	var violations []invariants.Violation
	if key, equal := equalSourceRows(state.Authored, state.Source); !equal {
		violations = append(violations, sourceStateViolation(sequence, RuleSourceStateAuthoredMismatch,
			invariants.EvidenceField{Name: "authored_rows", Value: strconv.Itoa(len(state.Authored))},
			invariants.EvidenceField{Name: "source_rows", Value: strconv.Itoa(len(state.Source))},
			invariants.EvidenceField{Name: "first_difference", Value: boundedEvidence(key)}))
	}
	expected := make(map[string]SourceRow, len(state.Source))
	scopes := make(map[string]map[string]struct{})
	for _, row := range state.Source {
		if manifest == nil {
			return append(violations, sourceStateViolation(sequence, RuleSourceStateRowInvalid))
		}
		identity, err := vectors.RowIdentity(*manifest, row.TableID, row.PrimaryKey)
		if err != nil {
			violations = append(violations, sourceStateViolation(sequence, RuleSourceStateRowInvalid,
				invariants.EvidenceField{Name: "table_id", Value: row.TableID}))
			continue
		}
		expected[string(identity)] = row
		for _, scopeID := range row.ScopeIDs {
			if scopes[scopeID] == nil {
				scopes[scopeID] = make(map[string]struct{})
			}
			scopes[scopeID][string(identity)] = struct{}{}
		}
	}
	for _, client := range clients {
		violations = append(violations, checkClientSourceState(sequence, manifest, client, expected, scopes)...)
	}
	return violations
}

func checkClientSourceState(sequence uint64, manifest *vectors.Manifest, client invariants.ClientObservation, expected map[string]SourceRow, scopes map[string]map[string]struct{}) []invariants.Violation {
	clientID := invariants.EvidenceField{Name: "client_id", Value: client.State.ClientID}
	if !client.Complete {
		return []invariants.Violation{sourceStateViolation(sequence, RuleSourceStateClientIncomplete, clientID)}
	}
	var violations []invariants.Violation
	held := make(map[string]map[string]struct{}, len(client.Scopes))
	for _, scope := range client.Scopes {
		held[scope.ScopeID] = make(map[string]struct{})
	}
	for _, row := range client.ScopeRows {
		if held[row.ScopeID] == nil {
			held[row.ScopeID] = make(map[string]struct{})
		}
		held[row.ScopeID][string(row.Entry.RowIdentity)] = struct{}{}
	}
	for _, scopeID := range sortedKeys(held) {
		want, got := scopes[scopeID], held[scopeID]
		missing, extra := setDifference(want, got), setDifference(got, want)
		if missing != 0 || extra != 0 {
			violations = append(violations, sourceStateViolation(sequence, RuleSourceStateMembership, clientID,
				invariants.EvidenceField{Name: "scope_id", Value: scopeID},
				invariants.EvidenceField{Name: "missing_rows", Value: strconv.Itoa(missing)},
				invariants.EvidenceField{Name: "unauthorized_rows", Value: strconv.Itoa(extra)}))
		}
	}
	for _, row := range client.Rows {
		identity, err := vectors.RowIdentity(*manifest, row.TableID, row.Row.PK)
		source, found := expected[string(identity)]
		if err != nil || !found || !heldInAnyScope(held, string(identity)) {
			violations = append(violations, sourceStateViolation(sequence, RuleSourceStateUnauthorizedRow, clientID,
				invariants.EvidenceField{Name: "table_id", Value: row.TableID}))
			continue
		}
		values := make(map[string]json.RawMessage, len(row.Row.Fields))
		for _, field := range row.Row.Fields {
			values[field.FieldID] = field.Value
		}
		for _, fieldID := range sortedKeys(source.Fields) {
			if !sameJSON(values[fieldID], source.Fields[fieldID]) {
				violations = append(violations, sourceStateViolation(sequence, RuleSourceStateValueMismatch, clientID,
					invariants.EvidenceField{Name: "table_id", Value: row.TableID},
					invariants.EvidenceField{Name: "field_id", Value: fieldID}))
			}
		}
	}
	return violations
}

func equalSourceRows(left, right []SourceRow) (string, bool) {
	index := func(rows []SourceRow) map[string]SourceRow {
		result := make(map[string]SourceRow, len(rows))
		for _, row := range rows {
			result[row.TableID+"\x00"+string(row.PrimaryKey)] = row
		}
		return result
	}
	leftRows, rightRows := index(left), index(right)
	keys := sortedKeys(leftRows)
	for key := range rightRows {
		if _, found := leftRows[key]; !found {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		leftRow, leftFound := leftRows[key]
		rightRow, rightFound := rightRows[key]
		if !leftFound || !rightFound || !sameScopes(leftRow.ScopeIDs, rightRow.ScopeIDs) || len(leftRow.Fields) != len(rightRow.Fields) {
			return strings.ReplaceAll(key, "\x00", "/"), false
		}
		for fieldID, value := range leftRow.Fields {
			if !sameJSON(value, rightRow.Fields[fieldID]) {
				return strings.ReplaceAll(key, "\x00", "/"), false
			}
		}
	}
	return "", len(left) == len(leftRows) && len(right) == len(rightRows)
}

func sameScopes(left, right []string) bool {
	leftSorted := append([]string(nil), left...)
	rightSorted := append([]string(nil), right...)
	sort.Strings(leftSorted)
	sort.Strings(rightSorted)
	return strings.Join(leftSorted, "\x00") == strings.Join(rightSorted, "\x00")
}

func sameJSON(left, right json.RawMessage) bool {
	var leftCompact, rightCompact bytes.Buffer
	return len(left) != 0 && json.Compact(&leftCompact, left) == nil && json.Compact(&rightCompact, right) == nil &&
		bytes.Equal(leftCompact.Bytes(), rightCompact.Bytes())
}

func heldInAnyScope(held map[string]map[string]struct{}, identity string) bool {
	for _, rows := range held {
		if _, found := rows[identity]; found {
			return true
		}
	}
	return false
}

func setDifference(left, right map[string]struct{}) int {
	count := 0
	for key := range left {
		if _, found := right[key]; !found {
			count++
		}
	}
	return count
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func boundedEvidence(value string) string {
	if len(value) > invariants.MaximumViolationEvidenceValueBytes {
		return value[:invariants.MaximumViolationEvidenceValueBytes]
	}
	return value
}

func sourceStateViolation(sequence uint64, rule invariants.RuleID, evidence ...invariants.EvidenceField) invariants.Violation {
	return invariants.Violation{Family: InvariantSourceState, RuleID: rule, ObservationSequence: sequence, Evidence: evidence}
}
