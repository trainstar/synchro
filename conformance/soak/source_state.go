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
//
// Names maps each runtime table and field ID to its stable authored name.
// Registration allocates runtime IDs at random in each cluster, so violation
// evidence uses these names and replay can compare it across clusters.
type SourceStateObservation struct {
	Authored []SourceRow
	Source   []SourceRow
	Names    map[string]string
}

// SourceRow is one live row. ScopeIDs come from the authored business
// membership rule. Fields holds canonical wire JSON by manifest field ID. A
// source row holds every synced field. An authored row holds only the business
// fields that the workload authored.
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
	name := func(id string) string {
		if value, found := state.Names[id]; found {
			return value
		}
		return "unnamed"
	}
	if key, equal := equalSourceRows(state.Authored, state.Source, name); !equal {
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
				invariants.EvidenceField{Name: "table", Value: name(row.TableID)}))
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
		violations = append(violations, checkClientSourceState(sequence, manifest, client, expected, scopes, name)...)
	}
	return violations
}

func checkClientSourceState(sequence uint64, manifest *vectors.Manifest, client invariants.ClientObservation, expected map[string]SourceRow, scopes map[string]map[string]struct{}, name func(string) string) []invariants.Violation {
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
				invariants.EvidenceField{Name: "table", Value: name(row.TableID)},
				invariants.EvidenceField{Name: "record", Value: boundedEvidence(string(row.Row.PK))}))
			continue
		}
		values := make(map[string]json.RawMessage, len(row.Row.Fields))
		for _, field := range row.Row.Fields {
			values[field.FieldID] = field.Value
		}
		for _, fieldID := range sortedKeys(source.Fields) {
			if !sameJSON(values[fieldID], source.Fields[fieldID]) {
				violations = append(violations, sourceStateViolation(sequence, RuleSourceStateValueMismatch, clientID,
					invariants.EvidenceField{Name: "table", Value: name(row.TableID)},
					invariants.EvidenceField{Name: "record", Value: boundedEvidence(string(row.Row.PK))},
					invariants.EvidenceField{Name: "field", Value: name(fieldID)}))
			}
		}
	}
	return violations
}

// equalSourceRows requires the same live rows and scopes in both sets, and the
// same value for every field that the authored row holds.
func equalSourceRows(authored, source []SourceRow, name func(string) string) (string, bool) {
	index := func(rows []SourceRow) map[string]SourceRow {
		result := make(map[string]SourceRow, len(rows))
		for _, row := range rows {
			result[name(row.TableID)+"/"+string(row.PrimaryKey)] = row
		}
		return result
	}
	authoredRows, sourceRows := index(authored), index(source)
	keys := sortedKeys(authoredRows)
	for key := range sourceRows {
		if _, found := authoredRows[key]; !found {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		authoredRow, authoredFound := authoredRows[key]
		sourceRow, sourceFound := sourceRows[key]
		if !authoredFound || !sourceFound || !sameScopes(authoredRow.ScopeIDs, sourceRow.ScopeIDs) {
			return key, false
		}
		for fieldID, value := range authoredRow.Fields {
			if !sameJSON(value, sourceRow.Fields[fieldID]) {
				return key + "/" + name(fieldID), false
			}
		}
	}
	return "", len(authored) == len(authoredRows) && len(source) == len(sourceRows)
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
