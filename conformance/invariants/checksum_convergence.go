package invariants

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"

	"github.com/trainstar/synchro/conformance/vectors"
)

type scopeDigestKey struct {
	scopeID    string
	generation uint64
}

// CheckChecksumConvergence validates pull checksum bytes and recomputes client-held digests.
// It generalizes conformance/blackbox/integration/real_mutation_controls_test.go:261-313.
func CheckChecksumConvergence(observations []Observation) ([]Violation, error) {
	var violations []Violation
	for _, observation := range orderedObservations(observations) {
		violations = append(violations, checkWireChecksums(observation)...)
		for _, client := range orderedClients(observation.Clients) {
			violations = append(violations, checkClientChecksums(observation, client)...)
		}
	}
	return orderedViolations(violations), nil
}

func checkWireChecksums(observation Observation) []Violation {
	var violations []Violation
	for _, exchange := range orderedWireExchanges(observation.WireExchanges) {
		if !exchange.ExpectChecksumConvergence {
			continue
		}
		if exchange.ResponseStatus != 200 {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumUnexpectedStatus,
			))
			continue
		}
		if exchange.OperationClass != "pull" || observation.Manifest == nil {
			ruleID := RuleChecksumWireShapeInvalid
			if observation.Manifest == nil {
				ruleID = RuleChecksumManifestMissing
			}
			violations = append(violations, checksumWireViolation(observation.Sequence, exchange.Sequence, ruleID))
			continue
		}
		request, response, ok := parsePullWire(exchange.RequestBody, exchange.ResponseBody)
		if !ok || response.hasMore || len(response.changes) != 1 {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumWireShapeInvalid,
			))
			continue
		}
		change := response.changes[0]
		row, tableID, serverVersion, rowChecksum, ok := parseChecksumPullChange(change)
		if !ok {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumWireShapeInvalid,
			))
			continue
		}
		computedRow, err := vectors.RowDigest(*observation.Manifest, tableID, row, serverVersion)
		if err != nil {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumWireShapeInvalid,
			))
			continue
		}
		observedRow, checksumOK := decodeChecksum(rowChecksum)
		responseObject, responseErr := decodeRawObject(exchange.ResponseBody)
		checksumMap, checksumMapErr := decodeRawObject(responseObject["checksums"])
		observedScope, scopeChecksumOK := decodeChecksum(checksumMap[change.scope])
		if !checksumOK || responseErr != nil || checksumMapErr != nil || !scopeChecksumOK {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumWireMetadataInvalid,
			))
			continue
		}
		if observedRow != computedRow {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumWireRowDigestMismatch,
			))
		}
		identity, err := vectors.RowIdentity(*observation.Manifest, tableID, row.PK)
		if err != nil {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumWireShapeInvalid,
			))
			continue
		}
		scopeEntries, complete := completeWireScopeEntries(observation, request.clientID, change.scope, identity)
		if !complete {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumWireScopeRowsIncomplete,
			))
			continue
		}
		computedScope, err := vectors.ScopeDigest(observation.Manifest.Hash(), change.scope, scopeEntries)
		if err != nil {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumWireShapeInvalid,
			))
			continue
		}
		if observedScope != computedScope {
			violations = append(violations, checksumWireViolation(
				observation.Sequence, exchange.Sequence, RuleChecksumWireScopeDigestMismatch,
			))
		}
	}
	return violations
}

// completeWireScopeEntries returns the post-pull row set only when one matching client reports a complete current scope.
func completeWireScopeEntries(
	observation Observation,
	clientID string,
	scopeID string,
	changedIdentity []byte,
) ([]vectors.DigestEntry, bool) {
	var matchingClient *ClientObservation
	for index := range observation.Clients {
		client := &observation.Clients[index]
		if client.State.ClientID != clientID {
			continue
		}
		if matchingClient != nil {
			return nil, false
		}
		matchingClient = client
	}
	if matchingClient == nil || !matchingClient.Complete {
		return nil, false
	}

	var matchingScope *ClientScopeObservation
	for index := range matchingClient.Scopes {
		scope := &matchingClient.Scopes[index]
		if scope.ScopeID != scopeID {
			continue
		}
		if matchingScope != nil {
			return nil, false
		}
		matchingScope = scope
	}
	if matchingScope == nil {
		return nil, false
	}

	var entries []vectors.DigestEntry
	changedRowPresent := false
	for _, scopeRow := range matchingClient.ScopeRows {
		if scopeRow.ScopeID != scopeID || scopeRow.Generation != matchingScope.Generation {
			continue
		}
		identity := append([]byte(nil), scopeRow.Entry.RowIdentity...)
		entries = append(entries, vectors.DigestEntry{RowIdentity: identity, RowDigest: scopeRow.Entry.RowDigest})
		if bytes.Equal(identity, changedIdentity) {
			changedRowPresent = true
		}
	}
	return entries, changedRowPresent
}

func parseChecksumPullChange(change pullWireChange) (vectors.Row, string, string, json.RawMessage, bool) {
	tableID, tableOK := decodeJSONString(change.object["table"])
	pkObject, pkErr := decodeRawObject(change.object["pk"])
	rowObject, rowErr := decodeRawObject(change.object["row"])
	serverVersion, versionOK := decodeJSONString(change.object["server_version"])
	if !tableOK || !validUUID(tableID) || pkErr != nil || len(pkObject) != 1 || rowErr != nil || len(rowObject) == 0 ||
		!versionOK || !validUUID(serverVersion) {
		return vectors.Row{}, "", "", nil, false
	}
	var primaryKey json.RawMessage
	for primaryKeyField, value := range pkObject {
		rowValue, present := rowObject[primaryKeyField]
		if !validUUID(primaryKeyField) || !present || !equalRawJSON(value, rowValue) {
			return vectors.Row{}, "", "", nil, false
		}
		primaryKey = append(json.RawMessage(nil), value...)
	}
	fieldIDs := sortedStringKeys(rowObject)
	fields := make([]vectors.RowField, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		if !validUUID(fieldID) || !validJSONValue(rowObject[fieldID]) {
			return vectors.Row{}, "", "", nil, false
		}
		fields = append(fields, vectors.RowField{FieldID: fieldID, Value: append(json.RawMessage(nil), rowObject[fieldID]...)})
	}
	return vectors.Row{PK: primaryKey, Fields: fields}, tableID, serverVersion, change.object["row_checksum"], true
}

func checkClientChecksums(observation Observation, client ClientObservation) []Violation {
	if observation.Manifest == nil {
		if clientNeedsManifest(client) {
			return []Violation{checksumViolation(
				observation.Sequence, RuleChecksumManifestMissing, client.State.ClientID, "", -1,
			)}
		}
		return nil
	}
	var violations []Violation
	computedRows := make(map[string][32]byte, len(client.Rows))
	for rowIndex, row := range client.Rows {
		digest, err := vectors.RowDigest(*observation.Manifest, row.TableID, row.Row, row.ServerVersion)
		if err != nil {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumRowInputInvalid, client.State.ClientID, "", rowIndex,
			))
			continue
		}
		identity, err := vectors.RowIdentity(*observation.Manifest, row.TableID, row.Row.PK)
		if err != nil {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumRowInputInvalid, client.State.ClientID, "", rowIndex,
			))
			continue
		}
		if _, duplicate := computedRows[string(identity)]; duplicate {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumRowIdentityDuplicate, client.State.ClientID, "", rowIndex,
			))
		} else {
			computedRows[string(identity)] = digest
		}
		if row.StoredDigest == nil {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumRowDigestMissing, client.State.ClientID, "", rowIndex,
			))
		} else if !bytes.Equal(row.StoredDigest[:], digest[:]) {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumRowDigestMismatch, client.State.ClientID, "", rowIndex,
			))
		}
	}

	entriesByScope := make(map[scopeDigestKey][]vectors.DigestEntry)
	orderedScopeRows := append([]ClientScopeRowObservation(nil), client.ScopeRows...)
	sort.SliceStable(orderedScopeRows, func(left, right int) bool {
		leftKey := orderedScopeRows[left].ScopeID + "\x00" + strconv.FormatUint(orderedScopeRows[left].Generation, 10) + "\x00" + string(orderedScopeRows[left].Entry.RowIdentity)
		rightKey := orderedScopeRows[right].ScopeID + "\x00" + strconv.FormatUint(orderedScopeRows[right].Generation, 10) + "\x00" + string(orderedScopeRows[right].Entry.RowIdentity)
		return leftKey < rightKey
	})
	for _, scopeRow := range orderedScopeRows {
		rowDigest, present := computedRows[string(scopeRow.Entry.RowIdentity)]
		if !present {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumScopeRowUnknown, client.State.ClientID, scopeRow.ScopeID, -1,
			))
			continue
		}
		if rowDigest != scopeRow.Entry.RowDigest {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumScopeRowDigestMismatch, client.State.ClientID, scopeRow.ScopeID, -1,
			))
		}
		key := scopeDigestKey{scopeID: scopeRow.ScopeID, generation: scopeRow.Generation}
		entriesByScope[key] = append(entriesByScope[key], vectors.DigestEntry{
			RowIdentity: append([]byte(nil), scopeRow.Entry.RowIdentity...),
			RowDigest:   rowDigest,
		})
	}

	scopesByID := make(map[string]ClientScopeObservation, len(client.Scopes))
	for _, scope := range orderedClientScopes(client.Scopes) {
		scopesByID[scope.ScopeID] = scope
	}
	checkpointScopes := make(map[string]struct{})
	for _, checkpoint := range client.State.Checkpoints {
		if checkpoint.HasChecksum {
			checkpointScopes[checkpoint.ScopeID] = struct{}{}
		}
	}
	for _, scopeID := range sortedStringKeys(checkpointScopes) {
		if _, present := scopesByID[scopeID]; !present {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumScopeObservationMissing, client.State.ClientID, scopeID, -1,
			))
		}
	}
	for _, scopeID := range sortedStringKeys(scopesByID) {
		scope := scopesByID[scopeID]
		if !scopeDigestRequired(client, scope) {
			continue
		}
		entries := entriesByScope[scopeDigestKey{scopeID: scope.ScopeID, generation: scope.Generation}]
		digest, err := vectors.ScopeDigest(observation.Manifest.Hash(), scope.ScopeID, entries)
		if err != nil {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumScopeInputInvalid, client.State.ClientID, scope.ScopeID, -1,
			))
			continue
		}
		if scope.AuthoritativeDigest == nil {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumAuthoritativeDigestMissing, client.State.ClientID, scope.ScopeID, -1,
			))
		} else if *scope.AuthoritativeDigest != digest {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumAuthoritativeDigestMismatch, client.State.ClientID, scope.ScopeID, -1,
			))
		}
		if scope.LocalDigest == nil {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumLocalDigestMissing, client.State.ClientID, scope.ScopeID, -1,
			))
		} else if *scope.LocalDigest != digest {
			violations = append(violations, checksumViolation(
				observation.Sequence, RuleChecksumLocalDigestMismatch, client.State.ClientID, scope.ScopeID, -1,
			))
		}
	}
	return violations
}

func clientNeedsManifest(client ClientObservation) bool {
	if len(client.Rows) != 0 {
		return true
	}
	for _, scope := range client.Scopes {
		if scopeDigestRequired(client, scope) {
			return true
		}
	}
	return false
}

func scopeDigestRequired(client ClientObservation, scope ClientScopeObservation) bool {
	if scope.AuthoritativeDigest != nil || scope.LocalDigest != nil {
		return true
	}
	for _, checkpoint := range client.State.Checkpoints {
		if checkpoint.ScopeID == scope.ScopeID && checkpoint.HasChecksum {
			return true
		}
	}
	return false
}

func checksumWireViolation(sequence, exchangeSequence uint64, ruleID RuleID) Violation {
	return boundedViolation(
		InvariantChecksumConvergence,
		ruleID,
		sequence,
		EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchangeSequence, 10)},
	)
}

func checksumViolation(sequence uint64, ruleID RuleID, clientID, scopeID string, rowIndex int) Violation {
	evidence := []EvidenceField{{Name: "client_id", Value: clientID}}
	if scopeID != "" {
		evidence = append(evidence, EvidenceField{Name: "scope_id", Value: scopeID})
	}
	if rowIndex >= 0 {
		evidence = append(evidence, EvidenceField{Name: "row_index", Value: strconv.Itoa(rowIndex)})
	}
	return boundedViolation(InvariantChecksumConvergence, ruleID, sequence, evidence...)
}
