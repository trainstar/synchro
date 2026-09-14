package invariants

import (
	"sort"
	"strconv"
	"strings"
)

type checkpointHistoryKey struct {
	userID           string
	clientID         string
	scopeID          string
	streamGeneration string
}

type clientScopeHistoryKey struct {
	userID   string
	clientID string
	scopeID  string
}

type clientCursorHistory struct {
	generation       uint64
	streamGeneration string
	position         orderedPosition
	hasPosition      bool
}

type cursorIdentityKey struct {
	userID     string
	clientID   string
	scopeID    string
	generation uint64
	cursor     string
}

type issuedTerminalCursor struct {
	sequence     uint64
	relation     CursorPositionObservation
	acknowledged bool
}

type orderedPosition struct {
	generationStart bool
	commitLSN       uint64
	transactionEnd  bool
	eventOrdinal    uint64
	effectOrdinal   uint64
}

// CheckCursorMonotonicity checks raw client cursors against ordered server positions.
// It generalizes conformance/blackbox/integration/real_mutation_controls_test.go:18-74.
func CheckCursorMonotonicity(observations []Observation) ([]Violation, error) {
	checkpointHistory := make(map[checkpointHistoryKey]orderedPosition)
	clientHistory := make(map[clientScopeHistoryKey]clientCursorHistory)
	knownCursors := make(map[cursorIdentityKey]CursorPositionObservation)
	issued := make(map[cursorIdentityKey]issuedTerminalCursor)
	var violations []Violation

	for _, observation := range orderedObservations(observations) {
		exchanges := wireExchangesBySequence(observation.WireExchanges)
		checkpoints, checkpointViolations := validateOperatorCheckpoints(observation, checkpointHistory)
		violations = append(violations, checkpointViolations...)

		for _, result := range orderedPullResults(observation.PullResults) {
			exchange, present := exchanges[result.ExchangeSequence]
			if !present || exchange.OperationClass != "pull" {
				violations = append(violations, cursorExchangeViolation(
					observation.Sequence, result.ExchangeSequence, RuleCursorWireShapeInvalid,
				))
				continue
			}
			if exchange.ResponseStatus != 200 {
				violations = append(violations, cursorExchangeViolation(
					observation.Sequence, exchange.Sequence, RuleCursorUnexpectedStatus,
				))
				continue
			}
			request, response, ok := parsePullWire(exchange.RequestBody, exchange.ResponseBody)
			if !ok || !validPullResultFact(result) || request.clientID != result.ClientID {
				violations = append(violations, cursorExchangeViolation(
					observation.Sequence, exchange.Sequence, RuleCursorWireShapeInvalid,
				))
				continue
			}
			if response.hasMore {
				violations = append(violations, cursorExchangeViolation(
					observation.Sequence, exchange.Sequence, RuleCursorTerminalPageInvalid,
				))
			}
			if !pullChangeIdentitiesEqual(response.changes, result.Changes) {
				violations = append(violations, cursorExchangeViolation(
					observation.Sequence, exchange.Sequence, RuleCursorChangeSetMismatch,
				))
			}

			expectedCursors := make(map[string]CursorPositionObservation, len(result.Cursors))
			for _, cursor := range result.Cursors {
				expectedCursors[cursor.ScopeID] = cursor
			}
			if !pullTerminalScopeSetsEqual(request.scopes, response, expectedCursors) {
				violations = append(violations, boundedViolation(
					InvariantCursorMonotonicity,
					RuleCursorTerminalSetMismatch,
					observation.Sequence,
					EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchange.Sequence, 10)},
					EvidenceField{Name: "expected_count", Value: strconv.Itoa(len(expectedCursors))},
					EvidenceField{Name: "observed_count", Value: strconv.Itoa(len(response.cursors))},
				))
			}
			for _, scopeID := range sortedStringKeys(response.cursors) {
				rawCursor := response.cursors[scopeID]
				if rawCursor == "" {
					violations = append(violations, cursorScopeViolation(
						observation.Sequence, exchange.Sequence, RuleCursorTerminalValueInvalid, result.ClientID, scopeID,
					))
					continue
				}
				relation, present := expectedCursors[scopeID]
				if !present || relation.RawCursor != rawCursor {
					violations = append(violations, cursorScopeViolation(
						observation.Sequence, exchange.Sequence, RuleCursorPositionUnbound, result.ClientID, scopeID,
					))
					continue
				}
				key := cursorIdentityKey{
					userID: relation.UserID, clientID: result.ClientID, scopeID: scopeID,
					generation: relation.Generation, cursor: rawCursor,
				}
				if prior, found := knownCursors[key]; found && !sameCursorPosition(prior, relation) {
					violations = append(violations, cursorScopeViolation(
						observation.Sequence, exchange.Sequence, RuleCursorPositionUnbound, result.ClientID, scopeID,
					))
					continue
				}
				knownCursors[key] = relation
				issued[key] = issuedTerminalCursor{sequence: observation.Sequence, relation: relation}
			}
		}

		for _, acknowledgement := range orderedCursorAcknowledgements(observation.CursorAcknowledgements) {
			exchange, present := exchanges[acknowledgement.ExchangeSequence]
			if !present || exchange.OperationClass != "pull" {
				violations = append(violations, cursorExchangeViolation(
					observation.Sequence, acknowledgement.ExchangeSequence, RuleCursorWireShapeInvalid,
				))
				continue
			}
			if exchange.ResponseStatus != 200 {
				violations = append(violations, cursorExchangeViolation(
					observation.Sequence, exchange.Sequence, RuleCursorUnexpectedStatus,
				))
				continue
			}
			request, _, ok := parsePullWire(exchange.RequestBody, exchange.ResponseBody)
			cursor := acknowledgement.Cursor
			requestCursor, requestHasScope := request.scopes[cursor.ScopeID]
			if !ok || !validCursorPosition(cursor) || request.clientID != cursor.ClientID || !requestHasScope ||
				requestCursor == nil || *requestCursor != cursor.RawCursor {
				violations = append(violations, cursorScopeViolation(
					observation.Sequence, exchange.Sequence, RuleCursorPositionUnbound, cursor.ClientID, cursor.ScopeID,
				))
				continue
			}
			key := cursorIdentityKey{
				userID: cursor.UserID, clientID: cursor.ClientID, scopeID: cursor.ScopeID,
				generation: cursor.Generation, cursor: cursor.RawCursor,
			}
			issuedCursor, found := issued[key]
			if !found || !sameCursorPosition(issuedCursor.relation, cursor) {
				violations = append(violations, cursorScopeViolation(
					observation.Sequence, exchange.Sequence, RuleCursorPositionUnbound, cursor.ClientID, cursor.ScopeID,
				))
				continue
			}
			issuedCursor.acknowledged = true
			issued[key] = issuedCursor
			checkpoint, checkpointPresent := checkpoints[checkpointLookupKey(cursor.UserID, cursor.ClientID, cursor.ScopeID)]
			if !checkpointPresent || checkpoint.StreamGeneration != cursor.StreamGeneration ||
				!samePositionObservation(checkpoint.Position, cursor.Position) || checkpoint.Position.Kind != "transaction_end" {
				violations = append(violations, cursorScopeViolation(
					observation.Sequence, exchange.Sequence, RuleCursorTerminalCheckpointInvalid, cursor.ClientID, cursor.ScopeID,
				))
				continue
			}
		}

		relations := cursorPositionsByClientScope(observation.CursorPositions)
		for _, client := range orderedClients(observation.Clients) {
			scopes := orderedClientScopes(client.Scopes)
			for _, scope := range scopes {
				if scope.RawCursor == nil {
					continue
				}
				if *scope.RawCursor == "" {
					violations = append(violations, clientCursorViolation(
						observation.Sequence, RuleCursorClientValueInvalid, client.State.ClientID, scope.ScopeID, nil,
					))
					continue
				}
				relation, present := relations[checkpointLookupKey(client.State.UserID, client.State.ClientID, scope.ScopeID)]
				if !present || relation.RawCursor != *scope.RawCursor || relation.Generation != scope.Generation || !validCursorPosition(relation) {
					violations = append(violations, clientCursorViolation(
						observation.Sequence, RuleCursorPositionUnbound, client.State.ClientID, scope.ScopeID, nil,
					))
					continue
				}
				key := cursorIdentityKey{
					userID: relation.UserID, clientID: relation.ClientID, scopeID: relation.ScopeID,
					generation: relation.Generation, cursor: relation.RawCursor,
				}
				if prior, found := knownCursors[key]; found && !sameCursorPosition(prior, relation) {
					violations = append(violations, clientCursorViolation(
						observation.Sequence, RuleCursorPositionUnbound, client.State.ClientID, scope.ScopeID, nil,
					))
					continue
				}
				knownCursors[key] = relation
				position, _ := parseOrderedPosition(relation.Position)
				historyKey := clientScopeHistoryKey{userID: relation.UserID, clientID: relation.ClientID, scopeID: relation.ScopeID}
				history, found := clientHistory[historyKey]
				if found && relation.Generation < history.generation {
					violations = append(violations, clientCursorViolation(
						observation.Sequence, RuleCursorClientGenerationRegressed, client.State.ClientID, scope.ScopeID, &history,
					))
					continue
				}
				if !found || relation.Generation > history.generation {
					history = clientCursorHistory{generation: relation.Generation}
				}
				if history.hasPosition && (history.streamGeneration != relation.StreamGeneration || compareOrderedPosition(position, history.position) < 0) {
					violations = append(violations, clientCursorViolation(
						observation.Sequence, RuleCursorClientRegressed, client.State.ClientID, scope.ScopeID, nil,
					))
					continue
				}
				history.streamGeneration = relation.StreamGeneration
				history.position = position
				history.hasPosition = true
				clientHistory[historyKey] = history
			}
		}
	}

	for _, key := range orderedIssuedCursorKeys(issued) {
		value := issued[key]
		if value.acknowledged {
			continue
		}
		violations = append(violations, boundedViolation(
			InvariantCursorMonotonicity,
			RuleCursorTerminalAcknowledgement,
			value.sequence,
			EvidenceField{Name: "client_id", Value: key.clientID},
			EvidenceField{Name: "scope_id", Value: key.scopeID},
		))
	}
	return orderedViolations(violations), nil
}

func wireExchangesBySequence(exchanges []WireExchangeObservation) map[uint64]WireExchangeObservation {
	result := make(map[uint64]WireExchangeObservation, len(exchanges))
	for _, exchange := range orderedWireExchanges(exchanges) {
		result[exchange.Sequence] = exchange
	}
	return result
}

func orderedPullResults(results []PullResultObservation) []PullResultObservation {
	ordered := append([]PullResultObservation(nil), results...)
	sort.SliceStable(ordered, func(left, right int) bool {
		if ordered[left].ExchangeSequence != ordered[right].ExchangeSequence {
			return ordered[left].ExchangeSequence < ordered[right].ExchangeSequence
		}
		return ordered[left].ClientID < ordered[right].ClientID
	})
	return ordered
}

func orderedCursorAcknowledgements(values []CursorAcknowledgementObservation) []CursorAcknowledgementObservation {
	ordered := append([]CursorAcknowledgementObservation(nil), values...)
	sort.SliceStable(ordered, func(left, right int) bool {
		if ordered[left].ExchangeSequence != ordered[right].ExchangeSequence {
			return ordered[left].ExchangeSequence < ordered[right].ExchangeSequence
		}
		leftCursor := ordered[left].Cursor
		rightCursor := ordered[right].Cursor
		return leftCursor.ClientID+"\x00"+leftCursor.ScopeID < rightCursor.ClientID+"\x00"+rightCursor.ScopeID
	})
	return ordered
}

func validPullResultFact(result PullResultObservation) bool {
	if result.ExchangeSequence == 0 || result.UserID == "" || result.ClientID == "" || len(result.Changes) != 1 || len(result.Cursors) != 2 {
		return false
	}
	seenScopes := make(map[string]struct{}, len(result.Cursors))
	for _, cursor := range result.Cursors {
		if cursor.UserID != result.UserID || cursor.ClientID != result.ClientID || !validCursorPosition(cursor) {
			return false
		}
		if _, duplicate := seenScopes[cursor.ScopeID]; duplicate {
			return false
		}
		seenScopes[cursor.ScopeID] = struct{}{}
	}
	return validExpectedPullChange(result.Changes[0])
}

func validExpectedPullChange(change PullChangeIdentityObservation) bool {
	return change.ScopeID != "" && validUUID(change.TableID) && validUUID(change.PrimaryKeyFieldID) && validJSONValue(change.PrimaryKey)
}

func pullChangeIdentitiesEqual(actual []pullWireChange, expected []PullChangeIdentityObservation) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index, change := range actual {
		tableID, tableOK := decodeJSONString(change.object["table"])
		pk, pkErr := decodeRawObject(change.object["pk"])
		if !tableOK || len(pk) != 1 || pkErr != nil || change.scope != expected[index].ScopeID || tableID != expected[index].TableID {
			return false
		}
		primaryKey, present := pk[expected[index].PrimaryKeyFieldID]
		if !present || !equalRawJSON(primaryKey, expected[index].PrimaryKey) {
			return false
		}
	}
	return true
}

func pullTerminalScopeSetsEqual(request map[string]*string, response pullWireResponse, expected map[string]CursorPositionObservation) bool {
	if len(request) != len(expected) || len(response.cursors) != len(expected) || len(response.rebuild) != 0 || len(response.removed) != 0 {
		return false
	}
	for _, scopeID := range sortedStringKeys(expected) {
		if _, requested := request[scopeID]; !requested {
			return false
		}
		if _, returned := response.cursors[scopeID]; !returned {
			return false
		}
	}
	return true
}

func validateOperatorCheckpoints(observation Observation, history map[checkpointHistoryKey]orderedPosition) (map[string]OperatorCheckpointObservation, []Violation) {
	checkpoints := make(map[string]OperatorCheckpointObservation)
	if observation.Operator == nil {
		return checkpoints, nil
	}
	ordered := append([]OperatorCheckpointObservation(nil), observation.Operator.Checkpoints...)
	sort.SliceStable(ordered, func(left, right int) bool {
		leftKey := checkpointLookupKey(ordered[left].UserID, ordered[left].ClientID, ordered[left].ScopeID)
		rightKey := checkpointLookupKey(ordered[right].UserID, ordered[right].ClientID, ordered[right].ScopeID)
		return leftKey < rightKey
	})
	var violations []Violation
	for _, checkpoint := range ordered {
		lookupKey := checkpointLookupKey(checkpoint.UserID, checkpoint.ClientID, checkpoint.ScopeID)
		if _, duplicate := checkpoints[lookupKey]; duplicate {
			violations = append(violations, clientCursorViolation(
				observation.Sequence, RuleCursorCheckpointDuplicate, checkpoint.ClientID, checkpoint.ScopeID, nil,
			))
			continue
		}
		checkpoints[lookupKey] = checkpoint
		position, ok := parseOrderedPosition(checkpoint.Position)
		if !ok || checkpoint.UserID == "" || checkpoint.ClientID == "" || checkpoint.ScopeID == "" || checkpoint.StreamGeneration == "" {
			violations = append(violations, boundedViolation(
				InvariantCursorMonotonicity,
				RuleCursorCheckpointPositionInvalid,
				observation.Sequence,
				EvidenceField{Name: "client_id", Value: checkpoint.ClientID},
				EvidenceField{Name: "scope_id", Value: checkpoint.ScopeID},
				EvidenceField{Name: "position_kind", Value: checkpoint.Position.Kind},
			))
			continue
		}
		historyKey := checkpointHistoryKey{
			userID: checkpoint.UserID, clientID: checkpoint.ClientID,
			scopeID: checkpoint.ScopeID, streamGeneration: checkpoint.StreamGeneration,
		}
		if prior, found := history[historyKey]; found && compareOrderedPosition(position, prior) < 0 {
			violations = append(violations, clientCursorViolation(
				observation.Sequence, RuleCursorCheckpointRegressed, checkpoint.ClientID, checkpoint.ScopeID, nil,
			))
			continue
		}
		history[historyKey] = position
	}
	return checkpoints, violations
}

func cursorPositionsByClientScope(values []CursorPositionObservation) map[string]CursorPositionObservation {
	result := make(map[string]CursorPositionObservation, len(values))
	ordered := append([]CursorPositionObservation(nil), values...)
	sort.SliceStable(ordered, func(left, right int) bool {
		leftKey := checkpointLookupKey(ordered[left].UserID, ordered[left].ClientID, ordered[left].ScopeID)
		rightKey := checkpointLookupKey(ordered[right].UserID, ordered[right].ClientID, ordered[right].ScopeID)
		return leftKey < rightKey
	})
	for _, value := range ordered {
		result[checkpointLookupKey(value.UserID, value.ClientID, value.ScopeID)] = value
	}
	return result
}

func orderedClientScopes(scopes []ClientScopeObservation) []ClientScopeObservation {
	ordered := append([]ClientScopeObservation(nil), scopes...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].ScopeID < ordered[right].ScopeID
	})
	return ordered
}

func validCursorPosition(cursor CursorPositionObservation) bool {
	_, positionOK := parseOrderedPosition(cursor.Position)
	return cursor.UserID != "" && cursor.ClientID != "" && cursor.ScopeID != "" && cursor.Generation != 0 &&
		cursor.RawCursor != "" && cursor.StreamGeneration != "" && positionOK
}

func sameCursorPosition(left, right CursorPositionObservation) bool {
	return left.UserID == right.UserID && left.ClientID == right.ClientID && left.ScopeID == right.ScopeID &&
		left.Generation == right.Generation && left.RawCursor == right.RawCursor && left.StreamGeneration == right.StreamGeneration &&
		samePositionObservation(left.Position, right.Position)
}

func samePositionObservation(left, right PositionObservation) bool {
	leftPosition, leftOK := parseOrderedPosition(left)
	rightPosition, rightOK := parseOrderedPosition(right)
	return leftOK && rightOK && compareOrderedPosition(leftPosition, rightPosition) == 0
}

func checkpointLookupKey(userID, clientID, scopeID string) string {
	return userID + "\x00" + clientID + "\x00" + scopeID
}

func orderedIssuedCursorKeys(values map[cursorIdentityKey]issuedTerminalCursor) []cursorIdentityKey {
	keys := make([]cursorIdentityKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		leftKey := keys[left].userID + "\x00" + keys[left].clientID + "\x00" + keys[left].scopeID + "\x00" +
			strconv.FormatUint(keys[left].generation, 10) + "\x00" + keys[left].cursor
		rightKey := keys[right].userID + "\x00" + keys[right].clientID + "\x00" + keys[right].scopeID + "\x00" +
			strconv.FormatUint(keys[right].generation, 10) + "\x00" + keys[right].cursor
		return leftKey < rightKey
	})
	return keys
}

func parseOrderedPosition(position PositionObservation) (orderedPosition, bool) {
	switch position.Kind {
	case "generation_start":
		if position.CommitLSN != nil || position.EventOrdinal != nil || position.EffectOrdinal != nil {
			return orderedPosition{}, false
		}
		return orderedPosition{generationStart: true}, true
	case "effect":
		if position.CommitLSN == nil || position.EventOrdinal == nil || position.EffectOrdinal == nil {
			return orderedPosition{}, false
		}
		commitLSN, ok := parsePostgreSQLLSN(*position.CommitLSN)
		if !ok {
			return orderedPosition{}, false
		}
		return orderedPosition{commitLSN: commitLSN, eventOrdinal: *position.EventOrdinal, effectOrdinal: *position.EffectOrdinal}, true
	case "transaction_end":
		if position.CommitLSN == nil || position.EventOrdinal != nil || position.EffectOrdinal != nil {
			return orderedPosition{}, false
		}
		commitLSN, ok := parsePostgreSQLLSN(*position.CommitLSN)
		if !ok {
			return orderedPosition{}, false
		}
		return orderedPosition{commitLSN: commitLSN, transactionEnd: true}, true
	default:
		return orderedPosition{}, false
	}
}

func parsePostgreSQLLSN(value string) (uint64, bool) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, false
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, false
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, false
	}
	return high<<32 | low, true
}

func compareOrderedPosition(left, right orderedPosition) int {
	if left.generationStart || right.generationStart {
		if left.generationStart == right.generationStart {
			return 0
		}
		if left.generationStart {
			return -1
		}
		return 1
	}
	if left.commitLSN != right.commitLSN {
		if left.commitLSN < right.commitLSN {
			return -1
		}
		return 1
	}
	if left.transactionEnd || right.transactionEnd {
		if left.transactionEnd == right.transactionEnd {
			return 0
		}
		if left.transactionEnd {
			return 1
		}
		return -1
	}
	if left.eventOrdinal != right.eventOrdinal {
		if left.eventOrdinal < right.eventOrdinal {
			return -1
		}
		return 1
	}
	if left.effectOrdinal < right.effectOrdinal {
		return -1
	}
	if left.effectOrdinal > right.effectOrdinal {
		return 1
	}
	return 0
}

func cursorExchangeViolation(sequence, exchangeSequence uint64, ruleID RuleID) Violation {
	return boundedViolation(
		InvariantCursorMonotonicity,
		ruleID,
		sequence,
		EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchangeSequence, 10)},
	)
}

func cursorScopeViolation(sequence, exchangeSequence uint64, ruleID RuleID, clientID, scopeID string) Violation {
	return boundedViolation(
		InvariantCursorMonotonicity,
		ruleID,
		sequence,
		EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchangeSequence, 10)},
		EvidenceField{Name: "client_id", Value: clientID},
		EvidenceField{Name: "scope_id", Value: scopeID},
	)
}

func clientCursorViolation(sequence uint64, ruleID RuleID, clientID, scopeID string, history *clientCursorHistory) Violation {
	evidence := []EvidenceField{
		{Name: "client_id", Value: clientID},
		{Name: "scope_id", Value: scopeID},
	}
	if history != nil {
		evidence = append(evidence, EvidenceField{Name: "prior_generation", Value: strconv.FormatUint(history.generation, 10)})
	}
	return boundedViolation(InvariantCursorMonotonicity, ruleID, sequence, evidence...)
}
