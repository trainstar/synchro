package modelrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/gowebpki/jcs"
	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	workloadMaximumSafeInteger = uint64(9007199254740991)
)

// expandPendingMutationsWorkload creates durable local intent before it uses
// normal push execution, response loss, and replay to resolve every mutation.
func expandPendingMutationsWorkload(snapshot reference.StateSnapshot, payload map[string]json.RawMessage) ([]scenarios.Operation, error) {
	if err := requireWorkloadProfile(payload, "pending_mutations"); err != nil {
		return nil, err
	}
	userID, err := requiredString(payload, "user_id")
	if err != nil {
		return nil, err
	}
	clientID, err := requiredString(payload, "client_id")
	if err != nil {
		return nil, err
	}
	tableID, err := requiredString(payload, "table_id")
	if err != nil {
		return nil, err
	}
	acceptedCount, err := requiredWorkloadUint64(payload, "accepted_count")
	if err != nil {
		return nil, err
	}
	rejectedCount, err := requiredWorkloadUint64(payload, "rejected_count")
	if err != nil {
		return nil, err
	}
	if rejectedCount != 1 || acceptedCount != 1 && acceptedCount != 99 && acceptedCount != 999 {
		return nil, errors.New("workload/prepare pending_mutations counts must be 1/1, 99/1, or 999/1")
	}

	client, err := workloadClient(snapshot, userID, clientID)
	if err != nil {
		return nil, err
	}
	currentSchema, manifest, err := workloadCurrentSchema(snapshot)
	if err != nil {
		return nil, err
	}
	table, err := workloadTable(manifest, tableID)
	if err != nil {
		return nil, err
	}
	rejectionField, err := workloadRejectionField(table)
	if err != nil {
		return nil, err
	}
	acceptedField, err := workloadAcceptedField(table, rejectionField.ID)
	if err != nil {
		return nil, err
	}
	nextSchema, publish, err := workloadSchemaPublication(currentSchema, manifest, table.ID, rejectionField.ID)
	if err != nil {
		return nil, err
	}

	total := acceptedCount + rejectedCount
	operations := make([]scenarios.Operation, 0, int(total)+4)
	wireMutations := make([]map[string]any, 0, int(total))
	for ordinal := uint64(1); ordinal <= total; ordinal++ {
		mutationID := workloadUUID("mutation", userID, clientID, currentSchema.Version, ordinal)
		primaryKey := fmt.Sprintf("queue-%d-%04d", currentSchema.Version, ordinal)
		columns := map[string]any{string(acceptedField.ID): fmt.Sprintf("accepted-%04d", ordinal)}
		if ordinal == total {
			columns[string(rejectionField.ID)] = "terminal-rejection"
		}
		localColumns := make([]map[string]any, 0, len(columns))
		fieldIDs := make([]string, 0, len(columns))
		for fieldID := range columns {
			fieldIDs = append(fieldIDs, fieldID)
		}
		sort.Strings(fieldIDs)
		for _, fieldID := range fieldIDs {
			localColumns = append(localColumns, map[string]any{"field_id": fieldID, "value": columns[fieldID]})
		}
		localPayload := map[string]any{
			"authenticated_user_id": userID,
			"client_id":             clientID,
			"mutation_id":           mutationID,
			"table_id":              tableID,
			"pk":                    map[string]any{string(table.PrimaryKeyFieldID): primaryKey},
			"authored_schema":       workloadSchemaWire(currentSchema),
			"operation":             "insert",
			"client_version":        "2026-08-11T00:00:00.000000Z",
		}
		wireMutation := map[string]any{
			"mutation_id":     mutationID,
			"table":           tableID,
			"pk":              map[string]any{string(table.PrimaryKeyFieldID): primaryKey},
			"authored_schema": workloadSchemaWire(currentSchema),
			"op":              "insert",
			"client_version":  "2026-08-11T00:00:00.000000Z",
		}
		localPayload["columns"] = localColumns
		wireMutation["columns"] = columns
		operations = append(operations, workloadOperation("local", "write", localPayload))
		wireMutations = append(wireMutations, wireMutation)
	}

	operations = append(operations,
		workloadOperation("process", "restart-client", map[string]any{"user_id": userID, "client_id": clientID}),
		publish,
	)
	commitLSN, endLSN, err := workloadNextLSNs(snapshot)
	if err != nil {
		return nil, err
	}
	request := map[string]any{
		"client_id":         clientID,
		"client_generation": uint64(client.CurrentGeneration),
		"batch_id":          workloadUUID("batch", userID, clientID, currentSchema.Version, total),
		"schema":            workloadSchemaWire(nextSchema),
		"mutations":         wireMutations,
	}
	pushPayload := map[string]any{
		"authenticated_user_id": userID,
		"request":               request,
		"commit_lsn":            strconv.FormatUint(commitLSN, 10),
		"end_lsn":               strconv.FormatUint(endLSN, 10),
	}
	firstDelivery := cloneWorkloadObject(pushPayload)
	firstDelivery["delivery"] = "drop_after_server"
	replayDelivery := cloneWorkloadObject(pushPayload)
	replayDelivery["delivery"] = "apply"
	operations = append(operations,
		workloadOperation("push", "submit", firstDelivery),
		workloadOperation("push", "submit", replayDelivery),
	)

	for _, operation := range operations {
		if scenarios.OperationKey(operation) == "workload/prepare" {
			return nil, errors.New("pending_mutations expansion retained workload/prepare")
		}
		if err := scenarios.ValidateOperation(operation); err != nil {
			return nil, fmt.Errorf("validate pending_mutations expansion %s: %w", scenarios.OperationKey(operation), err)
		}
	}
	return operations, nil
}

func requireWorkloadProfile(payload map[string]json.RawMessage, wanted string) error {
	profile, err := requiredString(payload, "profile")
	if err != nil {
		return err
	}
	if profile != wanted {
		return fmt.Errorf("workload/prepare profile must be %q", wanted)
	}
	return nil
}

func requiredWorkloadUint64(payload map[string]json.RawMessage, name string) (uint64, error) {
	raw, ok := payload[name]
	if !ok {
		return 0, fmt.Errorf("workload/prepare %s is required", name)
	}
	var value uint64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("workload/prepare %s must be an unsigned integer", name)
	}
	return value, nil
}

func workloadClient(snapshot reference.StateSnapshot, userID, clientID string) (reference.ClientState, error) {
	key := reference.ClientKey{UserID: reference.UserID(userID), ClientID: reference.ClientID(clientID)}
	for _, entry := range snapshot.Clients {
		if entry.Key != key {
			continue
		}
		if entry.Value.CurrentGeneration == 0 {
			return reference.ClientState{}, errors.New("pending_mutations client generation must be positive")
		}
		for _, local := range snapshot.ClientLocal {
			if local.Key == key {
				return entry.Value, nil
			}
		}
		return reference.ClientState{}, errors.New("pending_mutations client has no durable local state")
	}
	return reference.ClientState{}, errors.New("pending_mutations client is not installed")
}

func workloadCurrentSchema(snapshot reference.StateSnapshot) (reference.SchemaRef, reference.SchemaManifest, error) {
	if snapshot.CurrentSchema == (reference.SchemaRef{}) {
		return reference.SchemaRef{}, reference.SchemaManifest{}, errors.New("pending_mutations current schema is absent")
	}
	for _, entry := range snapshot.Schemas {
		if entry.Key == snapshot.CurrentSchema {
			return entry.Key, entry.Value, nil
		}
	}
	return reference.SchemaRef{}, reference.SchemaManifest{}, errors.New("pending_mutations current schema is not in immutable history")
}

func workloadTable(manifest reference.SchemaManifest, tableID string) (reference.TableManifest, error) {
	for _, table := range manifest.Tables {
		if table.ID == reference.TableID(tableID) {
			return table, nil
		}
	}
	return reference.TableManifest{}, fmt.Errorf("pending_mutations table %q is absent from the current schema", tableID)
}

func workloadRejectionField(table reference.TableManifest) (reference.FieldManifest, error) {
	fields := append([]reference.FieldManifest(nil), table.Fields...)
	sort.Slice(fields, func(left, right int) bool { return fields[left].ID < fields[right].ID })
	for _, field := range fields {
		if field.PrimaryKey || !field.Writable || field.PortableType != reference.PortableType("string") || workloadLifecycleField(table, field.ID) {
			continue
		}
		return field, nil
	}
	return reference.FieldManifest{}, errors.New("pending_mutations requires one writable non-lifecycle string field for a contract terminal rejection")
}

func workloadAcceptedField(table reference.TableManifest, rejected reference.FieldID) (reference.FieldManifest, error) {
	fields := append([]reference.FieldManifest(nil), table.Fields...)
	sort.Slice(fields, func(left, right int) bool { return fields[left].ID < fields[right].ID })
	for _, field := range fields {
		if field.ID == rejected || field.PrimaryKey || !field.Writable || field.PortableType != reference.PortableType("string") || workloadLifecycleField(table, field.ID) {
			continue
		}
		return field, nil
	}
	return reference.FieldManifest{}, errors.New("pending_mutations requires a second writable string field for accepted inserts")
}

func workloadLifecycleField(table reference.TableManifest, field reference.FieldID) bool {
	for _, candidate := range []*reference.FieldID{table.CreatedFieldID, table.UpdatedFieldID, table.DeletedFieldID} {
		if candidate != nil && *candidate == field {
			return true
		}
	}
	return false
}

func workloadSchemaPublication(current reference.SchemaRef, manifest reference.SchemaManifest, tableID reference.TableID, removedField reference.FieldID) (reference.SchemaRef, scenarios.Operation, error) {
	if current.Version >= workloadMaximumSafeInteger {
		return reference.SchemaRef{}, scenarios.Operation{}, errors.New("pending_mutations cannot allocate another schema version")
	}
	nextVersion := current.Version + 1
	newFieldID := workloadNextFieldID(manifest, nextVersion)
	tables := make([]map[string]any, 0, len(manifest.Tables))
	manifestTables := make([]map[string]any, 0, len(manifest.Tables))
	for _, table := range manifest.Tables {
		encoded := workloadSchemaTable(table, "", "")
		manifestEncoded := workloadManifestTable(table, "", "")
		if table.ID == tableID {
			encoded = workloadSchemaTable(table, removedField, newFieldID)
			manifestEncoded = workloadManifestTable(table, removedField, newFieldID)
		}
		tables = append(tables, encoded)
		manifestTables = append(manifestTables, manifestEncoded)
	}
	bodyObject := map[string]any{
		"parent_schema":       workloadSchemaWire(current),
		"schema_version":      nextVersion,
		"transition_class":    string(reference.SchemaClass4),
		"compatibility_floor": nextVersion,
		"tables":              manifestTables,
	}
	bodyWithoutHash, err := json.Marshal(bodyObject)
	if err != nil {
		return reference.SchemaRef{}, scenarios.Operation{}, fmt.Errorf("encode pending_mutations schema body: %w", err)
	}
	canonicalBody, err := jcs.Transform(bodyWithoutHash)
	if err != nil {
		return reference.SchemaRef{}, scenarios.Operation{}, fmt.Errorf("canonicalize pending_mutations schema body: %w", err)
	}
	preimage := append([]byte("synchro:v3:schema-manifest:v1\x00"), canonicalBody...)
	digest := sha256.Sum256(preimage)
	next := reference.SchemaRef{Version: nextVersion, Hash: digest}
	bodyObject["schema_hash"] = hex.EncodeToString(digest[:])
	body, err := json.Marshal(bodyObject)
	if err != nil {
		return reference.SchemaRef{}, scenarios.Operation{}, fmt.Errorf("encode pending_mutations schema body with hash: %w", err)
	}
	body, err = jcs.Transform(body)
	if err != nil {
		return reference.SchemaRef{}, scenarios.Operation{}, fmt.Errorf("canonicalize pending_mutations schema body with hash: %w", err)
	}
	operation := workloadOperation("model", "publish-schema", map[string]any{
		"schema":              workloadSchemaWire(next),
		"body":                string(body),
		"transition_class":    string(reference.SchemaClass4),
		"compatibility_floor": nextVersion,
		"tables":              tables,
		"affected_scopes":     []string{},
	})
	return next, operation, nil
}

func workloadManifestTable(table reference.TableManifest, removedField reference.FieldID, addedField string) map[string]any {
	fields := make([]map[string]any, 0, len(table.Fields)+1)
	for _, field := range table.Fields {
		if field.ID == removedField {
			continue
		}
		fields = append(fields, workloadManifestField(field))
	}
	if addedField != "" {
		fields = append(fields, map[string]any{
			"field_id": addedField,
			"name":     addedField,
			"type":     "string",
			"nullable": false,
			"writable": true,
		})
	}
	sort.Slice(fields, func(left, right int) bool {
		return fields[left]["field_id"].(string) < fields[right]["field_id"].(string)
	})
	indexes := make([]map[string]any, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		containsRemoved := false
		fieldIDs := make([]string, 0, len(index.Fields))
		for _, field := range index.Fields {
			if field == removedField {
				containsRemoved = true
				break
			}
			fieldIDs = append(fieldIDs, string(field))
		}
		if !containsRemoved {
			indexes = append(indexes, map[string]any{"index_id": string(index.ID), "name": index.Name, "field_ids": fieldIDs, "unique": index.Unique})
		}
	}
	sort.Slice(indexes, func(left, right int) bool {
		return indexes[left]["index_id"].(string) < indexes[right]["index_id"].(string)
	})
	return map[string]any{
		"table_id":             string(table.ID),
		"relation_id":          string(table.Relation),
		"name":                 table.Name,
		"composition":          string(table.Composition),
		"primary_key_field_id": string(table.PrimaryKeyFieldID),
		"lifecycle": map[string]any{
			"created_at_field_id": workloadOptionalField(table.CreatedFieldID),
			"updated_at_field_id": workloadOptionalField(table.UpdatedFieldID),
			"deleted_at_field_id": workloadOptionalField(table.DeletedFieldID),
		},
		"fields":  fields,
		"indexes": indexes,
	}
}

func workloadManifestField(field reference.FieldManifest) map[string]any {
	result := map[string]any{
		"field_id": string(field.ID),
		"name":     field.Name,
		"type":     string(field.PortableType),
		"nullable": field.Nullable,
		"writable": field.Writable,
	}
	if field.HasDecimalPrecision {
		result["precision"] = field.DecimalPrecision
	}
	if field.HasDecimalScale {
		result["scale"] = field.DecimalScale
	}
	return result
}

func workloadNextFieldID(manifest reference.SchemaManifest, version uint64) string {
	used := make(map[string]struct{})
	for _, table := range manifest.Tables {
		for _, field := range table.Fields {
			used[string(field.ID)] = struct{}{}
		}
	}
	base := fmt.Sprintf("queue_value_%d", version)
	for suffix := uint64(0); ; suffix++ {
		candidate := base
		if suffix != 0 {
			candidate += "_" + strconv.FormatUint(suffix, 10)
		}
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
}

func workloadSchemaTable(table reference.TableManifest, removedField reference.FieldID, addedField string) map[string]any {
	fields := make([]map[string]any, 0, len(table.Fields)+1)
	for _, field := range table.Fields {
		if field.ID == removedField {
			continue
		}
		fields = append(fields, workloadSchemaField(field))
	}
	if addedField != "" {
		fields = append(fields, map[string]any{
			"field_id":          addedField,
			"name":              addedField,
			"type":              "string",
			"primary_key":       false,
			"nullable":          false,
			"writable":          true,
			"decimal_precision": nil,
			"decimal_scale":     nil,
			"default_wire_json": `""`,
		})
	}
	indexes := make([]map[string]any, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		containsRemoved := false
		fieldIDs := make([]string, 0, len(index.Fields))
		for _, field := range index.Fields {
			if field == removedField {
				containsRemoved = true
				break
			}
			fieldIDs = append(fieldIDs, string(field))
		}
		if containsRemoved {
			continue
		}
		indexes = append(indexes, map[string]any{"index_id": string(index.ID), "name": index.Name, "field_ids": fieldIDs, "unique": index.Unique})
	}
	return map[string]any{
		"table_id":             string(table.ID),
		"relation_id":          string(table.Relation),
		"name":                 table.Name,
		"composition":          string(table.Composition),
		"primary_key_field_id": string(table.PrimaryKeyFieldID),
		"created_at_field_id":  workloadOptionalField(table.CreatedFieldID),
		"updated_at_field_id":  workloadOptionalField(table.UpdatedFieldID),
		"deleted_at_field_id":  workloadOptionalField(table.DeletedFieldID),
		"fields":               fields,
		"indexes":              indexes,
	}
}

func workloadSchemaField(field reference.FieldManifest) map[string]any {
	var precision any
	var scale any
	if field.HasDecimalPrecision {
		precision = field.DecimalPrecision
	}
	if field.HasDecimalScale {
		scale = field.DecimalScale
	}
	var defaultValue any
	if field.DefaultWireJSON != nil {
		defaultValue = *field.DefaultWireJSON
	}
	return map[string]any{
		"field_id":          string(field.ID),
		"name":              field.Name,
		"type":              string(field.PortableType),
		"primary_key":       field.PrimaryKey,
		"nullable":          field.Nullable,
		"writable":          field.Writable,
		"decimal_precision": precision,
		"decimal_scale":     scale,
		"default_wire_json": defaultValue,
	}
}

func workloadOptionalField(field *reference.FieldID) any {
	if field == nil {
		return nil
	}
	return string(*field)
}

func workloadSchemaWire(schema reference.SchemaRef) map[string]any {
	return map[string]any{"version": schema.Version, "hash": hex.EncodeToString(schema.Hash[:])}
}

func workloadNextLSNs(snapshot reference.StateSnapshot) (uint64, uint64, error) {
	maximum := uint64(0)
	for _, transaction := range snapshot.Stream.Transactions {
		if value := uint64(transaction.ReplayKey.CommitLSN); value > maximum {
			maximum = value
		}
		if value := uint64(transaction.EndLSN); value > maximum {
			maximum = value
		}
	}
	if maximum > math.MaxUint64-2 {
		return 0, 0, errors.New("pending_mutations cannot allocate source transaction LSNs")
	}
	return maximum + 1, maximum + 2, nil
}

func workloadUUID(kind, userID, clientID string, schemaVersion, ordinal uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("synchro:workload:%s:%s:%s:%d:%d", kind, userID, clientID, schemaVersion, ordinal)))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func workloadOperation(contractOperation, name string, payload map[string]any) scenarios.Operation {
	return scenarios.Operation{ContractOperation: contractOperation, Name: name, Payload: mustJSON(payload)}
}

func cloneWorkloadObject(source map[string]any) map[string]any {
	result := make(map[string]any, len(source)+1)
	for key, value := range source {
		result[key] = value
	}
	return result
}
