package invariants

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"

	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

// CheckScopeIsolation checks zero-change controls and complete row-to-scope memberships.
// It generalizes conformance/blackbox/integration/real_mutation_controls_test.go:316-347.
func CheckScopeIsolation(observations []Observation) ([]Violation, error) {
	var violations []Violation
	for _, observation := range orderedObservations(observations) {
		violations = append(violations, checkScopeIsolationWire(observation)...)
		violations = append(violations, checkScopeMemberships(observation)...)
	}
	return orderedViolations(violations), nil
}

func checkScopeIsolationWire(observation Observation) []Violation {
	var violations []Violation
	for _, exchange := range orderedWireExchanges(observation.WireExchanges) {
		if !exchange.ExpectScopeIsolation {
			continue
		}
		if exchange.ResponseStatus != 200 {
			violations = append(violations, scopeExchangeViolation(
				observation.Sequence, exchange.Sequence, RuleScopeUnexpectedStatus,
			))
			continue
		}
		if exchange.OperationClass != "pull" {
			violations = append(violations, scopeExchangeViolation(
				observation.Sequence, exchange.Sequence, RuleScopeWireShapeInvalid,
			))
			continue
		}
		request, response, ok := parsePullWire(exchange.RequestBody, exchange.ResponseBody)
		if !ok {
			violations = append(violations, scopeExchangeViolation(
				observation.Sequence, exchange.Sequence, RuleScopeWireShapeInvalid,
			))
			continue
		}
		if len(request.scopes) != 1 {
			violations = append(violations, scopeExchangeViolation(
				observation.Sequence, exchange.Sequence, RuleScopeSelectionInvalid,
			))
		}
		for changeIndex, change := range response.changes {
			ruleID := RuleScopeUnselectedChange
			if _, selected := request.scopes[change.scope]; selected {
				ruleID = RuleScopeUnexpectedChange
			}
			violations = append(violations, boundedViolation(
				InvariantScopeIsolation,
				ruleID,
				observation.Sequence,
				EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchange.Sequence, 10)},
				EvidenceField{Name: "change_index", Value: strconv.Itoa(changeIndex)},
				EvidenceField{Name: "scope_id", Value: change.scope},
			))
		}
	}
	return violations
}

func checkScopeMemberships(observation Observation) []Violation {
	serverScopes := make(map[string]serverScopeState)
	serverScopesPresent := observation.ServerState != nil && observation.ServerState.Scopes != nil
	var violations []Violation
	if serverScopesPresent {
		orderedScopes := append([]scenarios.ScopeFact(nil), observation.ServerState.Scopes...)
		sort.SliceStable(orderedScopes, func(left, right int) bool {
			return orderedScopes[left].ScopeID < orderedScopes[right].ScopeID
		})
		for _, scope := range orderedScopes {
			if _, duplicate := serverScopes[scope.ScopeID]; duplicate {
				violations = append(violations, scopeViolation(
					observation.Sequence, RuleScopeDuplicate, "server", "", scope.ScopeID,
				))
				continue
			}
			serverScopes[scope.ScopeID] = serverScopeState{generation: scope.MembershipGeneration, cardinality: scope.Cardinality}
		}
	}

	serverMemberships, relationViolations := normalizedServerMemberships(observation, serverScopes)
	violations = append(violations, relationViolations...)
	serverEdgesPresent := observation.ServerState != nil && observation.ServerState.RowScopeEdges != nil
	for _, client := range orderedClients(observation.Clients) {
		clientScopes := make(map[string]ClientScopeObservation, len(client.Scopes))
		for _, scope := range orderedClientScopes(client.Scopes) {
			if _, duplicate := clientScopes[scope.ScopeID]; duplicate {
				violations = append(violations, scopeViolation(
					observation.Sequence, RuleScopeDuplicate, "client", client.State.ClientID, scope.ScopeID,
				))
				continue
			}
			clientScopes[scope.ScopeID] = scope
		}
		clientMemberships := make(map[string]map[string]struct{}, len(clientScopes))
		seenMemberships := make(map[string]struct{}, len(client.ScopeRows))
		orderedRows := append([]ClientScopeRowObservation(nil), client.ScopeRows...)
		sort.SliceStable(orderedRows, func(left, right int) bool {
			leftKey := orderedRows[left].ScopeID + "\x00" + strconv.FormatUint(orderedRows[left].Generation, 10) + "\x00" + string(orderedRows[left].Entry.RowIdentity)
			rightKey := orderedRows[right].ScopeID + "\x00" + strconv.FormatUint(orderedRows[right].Generation, 10) + "\x00" + string(orderedRows[right].Entry.RowIdentity)
			return leftKey < rightKey
		})
		for _, scopeRow := range orderedRows {
			scope, present := clientScopes[scopeRow.ScopeID]
			if !present {
				violations = append(violations, scopeViolation(
					observation.Sequence, RuleScopeMembershipUnknown, "client", client.State.ClientID, scopeRow.ScopeID,
				))
				continue
			}
			if scopeRow.Generation != scope.Generation {
				violations = append(violations, scopeViolation(
					observation.Sequence, RuleScopeMembershipGeneration, "client", client.State.ClientID, scopeRow.ScopeID,
				))
				continue
			}
			membershipKey := scopeRow.ScopeID + "\x00" + strconv.FormatUint(scopeRow.Generation, 10) + "\x00" + string(scopeRow.Entry.RowIdentity)
			if _, duplicate := seenMemberships[membershipKey]; duplicate {
				violations = append(violations, scopeViolation(
					observation.Sequence, RuleScopeMembershipDuplicate, "client", client.State.ClientID, scopeRow.ScopeID,
				))
				continue
			}
			seenMemberships[membershipKey] = struct{}{}
			if clientMemberships[scopeRow.ScopeID] == nil {
				clientMemberships[scopeRow.ScopeID] = make(map[string]struct{})
			}
			clientMemberships[scopeRow.ScopeID][string(scopeRow.Entry.RowIdentity)] = struct{}{}
		}

		if !serverScopesPresent {
			continue
		}
		for _, scopeID := range sortedStringKeys(clientScopes) {
			clientScope := clientScopes[scopeID]
			serverScope, present := serverScopes[scopeID]
			if !present {
				violations = append(violations, scopeViolation(
					observation.Sequence, RuleScopeMembershipUnknown, "server", client.State.ClientID, scopeID,
				))
				continue
			}
			if clientScope.Generation != serverScope.generation {
				violations = append(violations, scopeViolation(
					observation.Sequence, RuleScopeServerGenerationMismatch, "server", client.State.ClientID, scopeID,
				))
			}
			clientRows := clientMemberships[scopeID]
			if client.Complete && uint64(len(clientRows)) != serverScope.cardinality {
				violations = append(violations, boundedViolation(
					InvariantScopeIsolation,
					RuleScopeServerCardinalityMismatch,
					observation.Sequence,
					EvidenceField{Name: "client_id", Value: client.State.ClientID},
					EvidenceField{Name: "scope_id", Value: scopeID},
					EvidenceField{Name: "client_cardinality", Value: strconv.Itoa(len(clientRows))},
					EvidenceField{Name: "server_cardinality", Value: strconv.FormatUint(serverScope.cardinality, 10)},
				))
			}
			if client.Complete && serverEdgesPresent && !equalMembershipSet(clientRows, serverMemberships[scopeID]) {
				violations = append(violations, scopeViolation(
					observation.Sequence, RuleScopeServerMembershipMismatch, "server", client.State.ClientID, scopeID,
				))
			}
		}
	}
	return violations
}

func normalizedServerMemberships(observation Observation, serverScopes map[string]serverScopeState) (map[string]map[string]struct{}, []Violation) {
	memberships := make(map[string]map[string]struct{})
	if observation.ServerState == nil || observation.ServerState.RowScopeEdges == nil {
		return memberships, nil
	}
	relations := make(map[string][]byte, len(observation.ServerRowIdentities))
	var violations []Violation
	orderedRelations := append([]ServerRowIdentityObservation(nil), observation.ServerRowIdentities...)
	sort.SliceStable(orderedRelations, func(left, right int) bool {
		return serverRowKey(orderedRelations[left].TableID, orderedRelations[left].CanonicalWireJSON) <
			serverRowKey(orderedRelations[right].TableID, orderedRelations[right].CanonicalWireJSON)
	})
	for _, relation := range orderedRelations {
		key := serverRowKey(relation.TableID, relation.CanonicalWireJSON)
		valid := relation.TableID != "" && validJSONValue(json.RawMessage(relation.CanonicalWireJSON)) && len(relation.RowIdentity) != 0
		if valid && observation.Manifest != nil {
			expected, err := vectors.RowIdentity(*observation.Manifest, relation.TableID, json.RawMessage(relation.CanonicalWireJSON))
			valid = err == nil && bytes.Equal(expected, relation.RowIdentity)
		}
		if _, duplicate := relations[key]; duplicate {
			valid = false
		}
		if !valid {
			violations = append(violations, scopeViolation(
				observation.Sequence, RuleScopeRowIdentityRelationInvalid, "server", "", "",
			))
			continue
		}
		relations[key] = append([]byte(nil), relation.RowIdentity...)
	}

	usedRelations := make(map[string]struct{}, len(relations))
	orderedEdges := append([]scenarios.RowScopeEdgeFact(nil), observation.ServerState.RowScopeEdges...)
	sort.SliceStable(orderedEdges, func(left, right int) bool {
		leftKey := orderedEdges[left].ScopeID + "\x00" + serverRowKey(orderedEdges[left].TableID, orderedEdges[left].CanonicalWireJSON)
		rightKey := orderedEdges[right].ScopeID + "\x00" + serverRowKey(orderedEdges[right].TableID, orderedEdges[right].CanonicalWireJSON)
		return leftKey < rightKey
	})
	seenEdges := make(map[string]struct{}, len(orderedEdges))
	for _, edge := range orderedEdges {
		if _, present := serverScopes[edge.ScopeID]; !present {
			violations = append(violations, scopeViolation(
				observation.Sequence, RuleScopeMembershipUnknown, "server", "", edge.ScopeID,
			))
			continue
		}
		rowKey := serverRowKey(edge.TableID, edge.CanonicalWireJSON)
		identity, present := relations[rowKey]
		if !present {
			violations = append(violations, scopeViolation(
				observation.Sequence, RuleScopeRowIdentityRelationInvalid, "server", "", edge.ScopeID,
			))
			continue
		}
		usedRelations[rowKey] = struct{}{}
		edgeKey := edge.ScopeID + "\x00" + string(identity)
		if _, duplicate := seenEdges[edgeKey]; duplicate {
			violations = append(violations, scopeViolation(
				observation.Sequence, RuleScopeMembershipDuplicate, "server", "", edge.ScopeID,
			))
			continue
		}
		seenEdges[edgeKey] = struct{}{}
		if memberships[edge.ScopeID] == nil {
			memberships[edge.ScopeID] = make(map[string]struct{})
		}
		memberships[edge.ScopeID][string(identity)] = struct{}{}
	}
	for _, key := range sortedStringKeys(relations) {
		if _, used := usedRelations[key]; !used {
			violations = append(violations, scopeViolation(
				observation.Sequence, RuleScopeRowIdentityRelationInvalid, "server", "", "",
			))
		}
	}
	return memberships, violations
}

type serverScopeState struct {
	generation  uint64
	cardinality uint64
}

func serverRowKey(tableID, canonicalWireJSON string) string {
	return tableID + "\x00" + canonicalWireJSON
}

func equalMembershipSet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for _, key := range sortedStringKeys(left) {
		if _, present := right[key]; !present {
			return false
		}
	}
	return true
}

func scopeExchangeViolation(sequence, exchangeSequence uint64, ruleID RuleID) Violation {
	return boundedViolation(
		InvariantScopeIsolation,
		ruleID,
		sequence,
		EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchangeSequence, 10)},
	)
}

func scopeViolation(sequence uint64, ruleID RuleID, surface, clientID, scopeID string) Violation {
	evidence := []EvidenceField{{Name: "surface", Value: surface}}
	if clientID != "" {
		evidence = append(evidence, EvidenceField{Name: "client_id", Value: clientID})
	}
	if scopeID != "" {
		evidence = append(evidence, EvidenceField{Name: "scope_id", Value: scopeID})
	}
	return boundedViolation(InvariantScopeIsolation, ruleID, sequence, evidence...)
}
