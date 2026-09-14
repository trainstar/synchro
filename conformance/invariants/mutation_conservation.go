package invariants

import (
	"encoding/json"
	"strconv"
	"time"
)

type pushMutation struct {
	id      string
	table   string
	pk      map[string]json.RawMessage
	schema  json.RawMessage
	columns map[string]json.RawMessage
}

type mutationPartition struct {
	batchID  string
	accepted []json.RawMessage
	rejected []json.RawMessage
}

// CheckMutationConservation checks the accepted and rejected mutation partition.
// It generalizes conformance/blackbox/integration/real_mutation_controls_test.go:171-258.
func CheckMutationConservation(observations []Observation) ([]Violation, error) {
	var violations []Violation
	for _, observation := range orderedObservations(observations) {
		for _, exchange := range orderedWireExchanges(observation.WireExchanges) {
			if !exchange.ExpectMutationConservation {
				continue
			}
			if exchange.ResponseStatus != 200 {
				violations = append(violations, mutationExchangeViolation(
					observation.Sequence, exchange.Sequence, RuleMutationUnexpectedStatus,
				))
				continue
			}
			if exchange.OperationClass != "push" {
				violations = append(violations, mutationWireShapeViolation(observation.Sequence, exchange.Sequence, "request"))
				continue
			}
			requestBatchID, requested, ok := parsePushRequest(exchange.RequestBody)
			if !ok {
				violations = append(violations, mutationWireShapeViolation(observation.Sequence, exchange.Sequence, "request"))
				continue
			}
			partition, ok := pushResponsePartition(exchange.ResponseBody)
			if !ok {
				violations = append(violations, mutationWireShapeViolation(observation.Sequence, exchange.Sequence, "response"))
				continue
			}
			if partition.batchID != requestBatchID {
				violations = append(violations, mutationExchangeViolation(
					observation.Sequence, exchange.Sequence, RuleMutationBatchMismatch,
				))
			}
			violations = append(violations, checkMutationPartition(observation.Sequence, exchange.Sequence, requested, partition)...)
		}
	}
	return orderedViolations(violations), nil
}

func parsePushRequest(raw []byte) (string, []pushMutation, bool) {
	request, err := decodeRawObject(raw)
	if err != nil {
		return "", nil, false
	}
	clientID, clientOK := decodeJSONString(request["client_id"])
	clientGeneration, generationOK := decodePositiveUint64(request["client_generation"])
	batchID, batchOK := decodeJSONString(request["batch_id"])
	schema, schemaOK := parseSchemaReference(request["schema"])
	if !clientOK || clientID == "" || !generationOK || clientGeneration == 0 || !batchOK || !validUUID(batchID) || !schemaOK {
		return "", nil, false
	}
	rawMutations, err := decodeRawArray(request["mutations"])
	if err != nil || len(rawMutations) == 0 {
		return "", nil, false
	}
	mutations := make([]pushMutation, 0, len(rawMutations))
	seen := make(map[string]struct{}, len(rawMutations))
	for _, rawMutation := range rawMutations {
		mutation, err := decodeRawObject(rawMutation)
		if err != nil {
			return "", nil, false
		}
		id, idOK := decodeJSONString(mutation["mutation_id"])
		table, tableOK := decodeJSONString(mutation["table"])
		pk, pkErr := decodeRawObject(mutation["pk"])
		authoredSchema, authoredSchemaOK := parseSchemaReference(mutation["authored_schema"])
		op, operationOK := decodeJSONString(mutation["op"])
		clientVersion, clientVersionOK := decodeJSONString(mutation["client_version"])
		columns, columnsErr := decodeRawObject(mutation["columns"])
		if !idOK || !validUUID(id) || !tableOK || !validUUID(table) || pkErr != nil || len(pk) != 1 ||
			!authoredSchemaOK || !equalRawJSON(authoredSchema, schema) || !operationOK || op != "insert" ||
			!clientVersionOK || !isCanonicalUTCMicrosecond(clientVersion) || columnsErr != nil || len(columns) == 0 {
			return "", nil, false
		}
		for fieldID, value := range pk {
			if !validUUID(fieldID) || !validJSONValue(value) {
				return "", nil, false
			}
		}
		for fieldID, value := range columns {
			if !validUUID(fieldID) || !validJSONValue(value) {
				return "", nil, false
			}
		}
		if _, duplicate := seen[id]; duplicate {
			return "", nil, false
		}
		seen[id] = struct{}{}
		mutations = append(mutations, pushMutation{id: id, table: table, pk: pk, schema: authoredSchema, columns: columns})
	}
	return batchID, mutations, true
}

func isCanonicalUTCMicrosecond(value string) bool {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && parsed.UTC().Format("2006-01-02T15:04:05.000000Z") == value
}

func parseSchemaReference(raw json.RawMessage) (json.RawMessage, bool) {
	object, err := decodeRawObject(raw)
	if err != nil || len(object) != 2 {
		return nil, false
	}
	version, versionOK := decodePositiveUint64(object["version"])
	hash, hashOK := decodeJSONString(object["hash"])
	if !versionOK || version == 0 || !hashOK || !validLowerHexDigest(hash) {
		return nil, false
	}
	normalized, err := normalizeRawJSON(raw)
	return normalized, err == nil
}

func validJSONValue(raw json.RawMessage) bool {
	_, err := normalizeRawJSON(raw)
	return err == nil
}

func pushResponsePartition(raw []byte) (mutationPartition, bool) {
	response, err := decodeRawObject(raw)
	if err != nil {
		return mutationPartition{}, false
	}
	batchID, ok := decodeJSONString(response["batch_id"])
	if !ok || !validUUID(batchID) {
		return mutationPartition{}, false
	}
	accepted, err := decodeRawArray(response["accepted"])
	if err != nil {
		return mutationPartition{}, false
	}
	rejected, err := decodeRawArray(response["rejected"])
	if err != nil {
		return mutationPartition{}, false
	}
	return mutationPartition{batchID: batchID, accepted: accepted, rejected: rejected}, true
}

func checkMutationPartition(sequence, exchangeSequence uint64, requested []pushMutation, partition mutationPartition) []Violation {
	requestOrder := make(map[string]int, len(requested))
	requestByID := make(map[string]pushMutation, len(requested))
	for index, mutation := range requested {
		requestOrder[mutation.id] = index
		requestByID[mutation.id] = mutation
	}
	seen := make(map[string]struct{}, len(requested))
	var violations []Violation
	violations = append(violations, checkMutationOutcomes(sequence, exchangeSequence, "accepted", partition.accepted, requestOrder, requestByID, seen)...)
	violations = append(violations, checkMutationOutcomes(sequence, exchangeSequence, "rejected", partition.rejected, requestOrder, requestByID, seen)...)
	omitted := 0
	for _, mutation := range requested {
		if _, present := seen[mutation.id]; !present {
			omitted++
		}
	}
	if omitted != 0 {
		violations = append(violations, boundedViolation(
			InvariantMutationConservation,
			RuleMutationOutcomeOmitted,
			sequence,
			EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchangeSequence, 10)},
			EvidenceField{Name: "omitted_count", Value: strconv.Itoa(omitted)},
		))
	}
	return violations
}

func checkMutationOutcomes(
	sequence uint64,
	exchangeSequence uint64,
	partitionName string,
	outcomes []json.RawMessage,
	requestOrder map[string]int,
	requestByID map[string]pushMutation,
	seen map[string]struct{},
) []Violation {
	var violations []Violation
	priorRequestIndex := -1
	orderViolation := false
	for outcomeIndex, rawOutcome := range outcomes {
		outcome, err := decodeRawObject(rawOutcome)
		if err != nil {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeIDInvalid, partitionName, outcomeIndex))
			continue
		}
		id, idOK := decodeJSONString(outcome["mutation_id"])
		if !idOK || !validUUID(id) {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeIDInvalid, partitionName, outcomeIndex))
			continue
		}
		status, statusOK := decodeJSONString(outcome["status"])
		if !statusOK || !validMutationOutcomeStatus(partitionName, status) {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeStatusInvalid, partitionName, outcomeIndex))
		}
		requestIndex, requested := requestOrder[id]
		if !requested {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeUnrequested, partitionName, outcomeIndex))
		} else {
			if !orderViolation && requestIndex < priorRequestIndex {
				violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeOrder, partitionName, outcomeIndex))
				orderViolation = true
			}
			priorRequestIndex = requestIndex
			violations = append(violations, checkCanonicalMutationOutcome(
				sequence, exchangeSequence, partitionName, outcomeIndex, outcome, requestByID[id],
			)...)
		}
		if _, duplicate := seen[id]; duplicate {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeDuplicate, partitionName, outcomeIndex))
		}
		seen[id] = struct{}{}
	}
	return violations
}

func checkCanonicalMutationOutcome(sequence, exchangeSequence uint64, partitionName string, outcomeIndex int, outcome map[string]json.RawMessage, request pushMutation) []Violation {
	var violations []Violation
	table, tableOK := decodeJSONString(outcome["table"])
	if !tableOK || table != request.table {
		violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeTable, partitionName, outcomeIndex))
	}
	pk, pkErr := decodeRawObject(outcome["pk"])
	if pkErr != nil || !equalRawObject(pk, request.pk) {
		violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomePrimaryKey, partitionName, outcomeIndex))
	}
	schema, schemaOK := parseSchemaReference(outcome["outcome_schema"])
	if !schemaOK || !equalRawJSON(schema, request.schema) {
		violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeSchema, partitionName, outcomeIndex))
	}
	if partitionName == "accepted" {
		_, codePresent := outcome["code"]
		if codePresent {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeCode, partitionName, outcomeIndex))
		}
		return append(violations, checkMutationOutcomeReconciliation(sequence, exchangeSequence, partitionName, outcomeIndex, outcome, request)...)
	}
	status, _ := decodeJSONString(outcome["status"])
	code, codeOK := decodeJSONString(outcome["code"])
	if status == "rejected_terminal" {
		if !codeOK || !validTerminalMutationCode(code) {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeCode, partitionName, outcomeIndex))
		}
		if _, present := outcome["server_row"]; present {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeServerRow, partitionName, outcomeIndex))
		}
		if _, present := outcome["server_version"]; present {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeServerVersion, partitionName, outcomeIndex))
		}
		if _, present := outcome["row_checksum"]; present {
			violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeChecksum, partitionName, outcomeIndex))
		}
		return violations
	}
	if !codeOK || !validConflictMutationCode(code) {
		violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeCode, partitionName, outcomeIndex))
	}
	return append(violations, checkMutationOutcomeReconciliation(sequence, exchangeSequence, partitionName, outcomeIndex, outcome, request)...)
}

func checkMutationOutcomeReconciliation(sequence, exchangeSequence uint64, partitionName string, outcomeIndex int, outcome map[string]json.RawMessage, request pushMutation) []Violation {
	var violations []Violation
	serverRow, rowErr := decodeRawObject(outcome["server_row"])
	if rowErr != nil || !rawObjectContains(serverRow, request.columns) {
		violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeServerRow, partitionName, outcomeIndex))
	}
	serverVersion, versionOK := decodeJSONString(outcome["server_version"])
	if !versionOK || !validUUID(serverVersion) {
		violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeServerVersion, partitionName, outcomeIndex))
	}
	if _, ok := decodeChecksum(outcome["row_checksum"]); !ok {
		violations = append(violations, mutationOutcomeViolation(sequence, exchangeSequence, RuleMutationOutcomeChecksum, partitionName, outcomeIndex))
	}
	return violations
}

func validConflictMutationCode(code string) bool {
	switch code {
	case "version_conflict", "row_already_exists", "row_deleted", "row_not_found":
		return true
	default:
		return false
	}
}

func validTerminalMutationCode(code string) bool {
	switch code {
	case "schema_incompatible", "table_not_synced", "policy_rejected", "validation_failed":
		return true
	default:
		return false
	}
}

func equalRawObject(left, right map[string]json.RawMessage) bool {
	return len(left) == len(right) && rawObjectContains(left, right)
}

func rawObjectContains(container, required map[string]json.RawMessage) bool {
	for key, expected := range required {
		actual, present := container[key]
		if !present || !equalRawJSON(actual, expected) {
			return false
		}
	}
	return true
}

func validMutationOutcomeStatus(partitionName, status string) bool {
	if partitionName == "accepted" {
		return status == "applied"
	}
	return status == "conflict" || status == "rejected_terminal"
}

func mutationWireShapeViolation(sequence, exchangeSequence uint64, stage string) Violation {
	return boundedViolation(
		InvariantMutationConservation,
		RuleMutationWireShapeInvalid,
		sequence,
		EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchangeSequence, 10)},
		EvidenceField{Name: "stage", Value: stage},
	)
}

func mutationExchangeViolation(sequence, exchangeSequence uint64, ruleID RuleID) Violation {
	return boundedViolation(
		InvariantMutationConservation,
		ruleID,
		sequence,
		EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchangeSequence, 10)},
	)
}

func mutationOutcomeViolation(sequence, exchangeSequence uint64, ruleID RuleID, partitionName string, outcomeIndex int) Violation {
	return boundedViolation(
		InvariantMutationConservation,
		ruleID,
		sequence,
		EvidenceField{Name: "exchange_sequence", Value: strconv.FormatUint(exchangeSequence, 10)},
		EvidenceField{Name: "partition", Value: partitionName},
		EvidenceField{Name: "outcome_index", Value: strconv.Itoa(outcomeIndex)},
	)
}
