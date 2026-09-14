package invariants

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/scenarios"
)

type processClientKey struct {
	userID   string
	clientID string
}

type durableClientSnapshot struct {
	state     scenarios.ClientDurabilityFact
	rows      []ClientRowObservation
	scopes    []ClientScopeObservation
	scopeRows []ClientScopeRowObservation
}

// CheckNoStateForks checks process replacement, database identity, and durable state equality.
// It generalizes conformance/kotlin/platform.go:1412-1438 and conformance/kotlin/platform.go:1524-1531.
// It also generalizes conformance/reactnative/queue_replay.go:1002-1016.
func CheckNoStateForks(observations []Observation) ([]Violation, error) {
	priorClients := make(map[processClientKey]ClientObservation)
	var violations []Violation
	for _, observation := range orderedObservations(observations) {
		clients := orderedClients(observation.Clients)
		for _, client := range clients {
			key := processClientKey{userID: client.State.UserID, clientID: client.State.ClientID}
			prior, found := priorClients[key]
			priorClients[key] = client
			if !found {
				if client.RestartBoundary {
					violations = append(violations, stateForkViolation(
						observation.Sequence, RuleStateForkCaptureIncomplete, client.State.ClientID, "prior",
					))
				}
				continue
			}

			priorIdentity := classifyProcessIdentity(prior.Process)
			currentIdentity := classifyProcessIdentity(client.Process)
			if !client.RestartBoundary {
				if priorIdentity == processIdentityInvalid || currentIdentity == processIdentityInvalid {
					violations = append(violations, stateForkViolation(
						observation.Sequence, RuleStateForkProcessIdentityInvalid, client.State.ClientID, "",
					))
				} else if priorIdentity == processIdentityValid && currentIdentity == processIdentityValid &&
					prior.Process.ProcessID != client.Process.ProcessID {
					violations = append(violations, stateForkViolation(
						observation.Sequence, RuleStateForkProcessReplacedUnexpectedly, client.State.ClientID, "",
					))
				}
				continue
			}

			if !prior.Complete || !client.Complete {
				detail := "current"
				if !prior.Complete {
					detail = "prior"
				}
				violations = append(violations, stateForkViolation(
					observation.Sequence, RuleStateForkCaptureIncomplete, client.State.ClientID, detail,
				))
			}
			if priorIdentity == processIdentityMissing || currentIdentity == processIdentityMissing {
				violations = append(violations, stateForkViolation(
					observation.Sequence, RuleStateForkProcessIdentityMissing, client.State.ClientID, "",
				))
			} else if priorIdentity == processIdentityInvalid || currentIdentity == processIdentityInvalid {
				violations = append(violations, stateForkViolation(
					observation.Sequence, RuleStateForkProcessIdentityInvalid, client.State.ClientID, "",
				))
			} else {
				if prior.Process.ProcessID == client.Process.ProcessID {
					violations = append(violations, stateForkViolation(
						observation.Sequence, RuleStateForkProcessNotReplaced, client.State.ClientID, "",
					))
				}
				if prior.Process.DatabaseIdentityFingerprint != client.Process.DatabaseIdentityFingerprint {
					violations = append(violations, stateForkViolation(
						observation.Sequence, RuleStateForkDatabaseIdentityChanged, client.State.ClientID, "",
					))
				}
			}
			if !prior.Complete || !client.Complete {
				continue
			}
			priorState, err := canonicalDurableClient(prior)
			if err != nil {
				violations = append(violations, stateForkViolation(
					observation.Sequence, RuleStateForkDurableStateInvalid, client.State.ClientID, "prior",
				))
				continue
			}
			currentState, err := canonicalDurableClient(client)
			if err != nil {
				violations = append(violations, stateForkViolation(
					observation.Sequence, RuleStateForkDurableStateInvalid, client.State.ClientID, "current",
				))
				continue
			}
			if changed := changedDurableFamilies(priorState, currentState); changed != "" {
				violations = append(violations, stateForkViolation(
					observation.Sequence, RuleStateForkDurableStateChanged, client.State.ClientID, changed,
				))
			}
		}
	}
	return orderedViolations(violations), nil
}

type processIdentityClassification uint8

const (
	processIdentityMissing processIdentityClassification = iota
	processIdentityInvalid
	processIdentityValid
)

func classifyProcessIdentity(process *ProcessIdentityObservation) processIdentityClassification {
	if process == nil || process.ProcessID == "" || process.DatabaseIdentityFingerprint == "" {
		return processIdentityMissing
	}
	if len(process.ProcessID) > 256 || !validLowerHexDigest(process.DatabaseIdentityFingerprint) {
		return processIdentityInvalid
	}
	return processIdentityValid
}

func orderedClients(clients []ClientObservation) []ClientObservation {
	ordered := append([]ClientObservation(nil), clients...)
	sort.SliceStable(ordered, func(left, right int) bool {
		leftKey := ordered[left].State.UserID + "\x00" + ordered[left].State.ClientID
		rightKey := ordered[right].State.UserID + "\x00" + ordered[right].State.ClientID
		return leftKey < rightKey
	})
	return ordered
}

func canonicalDurableClient(client ClientObservation) (durableClientSnapshot, error) {
	normalized, err := scenarios.NormalizeStateFacts(scenarios.StateFacts{Clients: []scenarios.ClientDurabilityFact{client.State}})
	if err != nil {
		return durableClientSnapshot{}, err
	}
	return canonicalDurableClientParts(normalized.Clients[0], client.Rows, client.Scopes, client.ScopeRows)
}

func canonicalDurableClientParts(
	state scenarios.ClientDurabilityFact,
	inputRows []ClientRowObservation,
	inputScopes []ClientScopeObservation,
	inputScopeRows []ClientScopeRowObservation,
) (durableClientSnapshot, error) {
	rows := make([]ClientRowObservation, len(inputRows))
	for index, row := range inputRows {
		rows[index] = row
		var err error
		rows[index].Row.PK, err = normalizeRawJSON(row.Row.PK)
		if err != nil {
			return durableClientSnapshot{}, err
		}
		rows[index].Row.Fields = append(rows[index].Row.Fields[:0:0], row.Row.Fields...)
		for fieldIndex := range rows[index].Row.Fields {
			rows[index].Row.Fields[fieldIndex].Value, err = normalizeRawJSON(rows[index].Row.Fields[fieldIndex].Value)
			if err != nil {
				return durableClientSnapshot{}, err
			}
		}
		sort.Slice(rows[index].Row.Fields, func(left, right int) bool {
			return rows[index].Row.Fields[left].FieldID < rows[index].Row.Fields[right].FieldID
		})
	}
	sort.Slice(rows, func(left, right int) bool {
		return compareClientRows(rows[left], rows[right]) < 0
	})

	scopes := append([]ClientScopeObservation(nil), inputScopes...)
	for index := range scopes {
		scopes[index].RawCursor = copyString(scopes[index].RawCursor)
		scopes[index].AuthoritativeDigest = copyDigest(scopes[index].AuthoritativeDigest)
		scopes[index].LocalDigest = copyDigest(scopes[index].LocalDigest)
	}
	sort.Slice(scopes, func(left, right int) bool {
		if scopes[left].ScopeID != scopes[right].ScopeID {
			return scopes[left].ScopeID < scopes[right].ScopeID
		}
		return scopes[left].Generation < scopes[right].Generation
	})

	scopeRows := make([]ClientScopeRowObservation, len(inputScopeRows))
	for index, row := range inputScopeRows {
		scopeRows[index] = row
		scopeRows[index].Entry.RowIdentity = append([]byte(nil), row.Entry.RowIdentity...)
	}
	sort.Slice(scopeRows, func(left, right int) bool {
		if scopeRows[left].ScopeID != scopeRows[right].ScopeID {
			return scopeRows[left].ScopeID < scopeRows[right].ScopeID
		}
		if scopeRows[left].Generation != scopeRows[right].Generation {
			return scopeRows[left].Generation < scopeRows[right].Generation
		}
		return bytes.Compare(scopeRows[left].Entry.RowIdentity, scopeRows[right].Entry.RowIdentity) < 0
	})
	return durableClientSnapshot{state: state, rows: rows, scopes: scopes, scopeRows: scopeRows}, nil
}

func normalizeRawJSON(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("JSON value has trailing content")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func compareClientRows(left, right ClientRowObservation) int {
	if left.TableID != right.TableID {
		return strings.Compare(left.TableID, right.TableID)
	}
	if compared := bytes.Compare(left.Row.PK, right.Row.PK); compared != 0 {
		return compared
	}
	return strings.Compare(left.ServerVersion, right.ServerVersion)
}

func copyString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func copyDigest(value *[32]byte) *[32]byte {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func changedDurableFamilies(prior, current durableClientSnapshot) string {
	changed := make([]string, 0, 4)
	if !reflect.DeepEqual(prior.state, current.state) {
		changed = append(changed, "state")
	}
	if !reflect.DeepEqual(prior.rows, current.rows) {
		changed = append(changed, "rows")
	}
	if !reflect.DeepEqual(prior.scopes, current.scopes) {
		changed = append(changed, "scopes")
	}
	if !reflect.DeepEqual(prior.scopeRows, current.scopeRows) {
		changed = append(changed, "scope_rows")
	}
	return strings.Join(changed, ",")
}

func stateForkViolation(sequence uint64, ruleID RuleID, clientID, detail string) Violation {
	evidence := []EvidenceField{{Name: "client_id", Value: clientID}}
	if detail != "" {
		evidence = append(evidence, EvidenceField{Name: "detail", Value: detail})
	}
	return boundedViolation(InvariantNoStateForks, ruleID, sequence, evidence...)
}
