package scenarios

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/gowebpki/jcs"
)

// QueueReplaySchema holds authored schema inputs for the Swift, Kotlin, and React Native drivers.
type QueueReplaySchema struct {
	SchemaFact
	Tables []QueueReplaySchemaTable
}

type QueueReplaySchemaTable struct {
	TableID           string                   `json:"table_id"`
	RelationID        string                   `json:"relation_id"`
	Name              string                   `json:"name"`
	Composition       string                   `json:"composition"`
	PrimaryKeyFieldID string                   `json:"primary_key_field_id"`
	CreatedAtFieldID  *string                  `json:"created_at_field_id"`
	UpdatedAtFieldID  *string                  `json:"updated_at_field_id"`
	DeletedAtFieldID  *string                  `json:"deleted_at_field_id"`
	Fields            []QueueReplaySchemaField `json:"fields"`
	Indexes           []QueueReplaySchemaIndex `json:"indexes"`
}

type QueueReplaySchemaField struct {
	FieldID          string  `json:"field_id"`
	Name             string  `json:"name"`
	Type             string  `json:"type"`
	PrimaryKey       bool    `json:"primary_key"`
	Nullable         bool    `json:"nullable"`
	Writable         bool    `json:"writable"`
	DecimalPrecision *uint32 `json:"decimal_precision"`
	DecimalScale     *uint32 `json:"decimal_scale"`
	DefaultWireJSON  *string `json:"default_wire_json"`
}

type QueueReplaySchemaIndex struct {
	IndexID  string   `json:"index_id"`
	Name     string   `json:"name"`
	FieldIDs []string `json:"field_ids"`
	Unique   bool     `json:"unique"`
}

// QueueReplayWorkload holds the authored counts used by input construction and outcome capture.
type QueueReplayWorkload struct {
	Profile       string `json:"profile"`
	UserID        string `json:"user_id"`
	ClientID      string `json:"client_id"`
	TableID       string `json:"table_id"`
	AcceptedCount uint64 `json:"accepted_count"`
	RejectedCount uint64 `json:"rejected_count"`
}

// QueueReplayInputs contains only inputs. Each platform retains its own execution and capture.
type QueueReplayInputs struct {
	Local      []Operation
	Publish    Operation
	DropPush   Operation
	BatchID    string
	NextSchema QueueReplaySchema
}

func InitialQueueReplaySchema(operation Operation) (QueueReplaySchema, error) {
	if OperationKey(operation) != "model/install-current-contract" {
		return QueueReplaySchema{}, errors.New("queue-replay setup is not a contract installation")
	}
	if err := ValidateOperation(operation); err != nil {
		return QueueReplaySchema{}, fmt.Errorf("validate queue-replay setup: %w", err)
	}
	var payload struct {
		InitialSchema struct {
			Schema SchemaFact               `json:"schema"`
			Tables []QueueReplaySchemaTable `json:"tables"`
		} `json:"initial_schema"`
	}
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return QueueReplaySchema{}, fmt.Errorf("decode queue-replay initial schema: %w", err)
	}
	current := QueueReplaySchema{SchemaFact: payload.InitialSchema.Schema, Tables: payload.InitialSchema.Tables}
	if err := validateQueueReplaySchema(current); err != nil {
		return QueueReplaySchema{}, err
	}
	return current, nil
}

// CRUDSchema supplies the existing CRUD plan without platform-specific schema copies.
func (schema QueueReplaySchema) CRUDSchema() NativeCRUDSchema {
	tables := make([]NativeCRUDSchemaTable, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		fields := make([]NativeCRUDSchemaField, 0, len(table.Fields))
		for _, field := range table.Fields {
			fields = append(fields, NativeCRUDSchemaField{
				FieldID: field.FieldID, Type: field.Type, PrimaryKey: field.PrimaryKey, Writable: field.Writable,
			})
		}
		tables = append(tables, NativeCRUDSchemaTable{
			TableID: table.TableID, PrimaryKeyFieldID: table.PrimaryKeyFieldID, Fields: fields,
		})
	}
	return NativeCRUDSchema{Version: schema.Version, Hash: schema.Hash, Tables: tables}
}

// BuildQueueReplayInputs expands one authored queue step for all three native drivers.
func BuildQueueReplayInputs(step Step, current QueueReplaySchema, commitLSN uint64) (QueueReplayInputs, error) {
	if err := validateQueueReplaySchema(current); err != nil {
		return QueueReplayInputs{}, err
	}
	binding := step.NativeBinding
	if binding == nil || binding.Kind != "workload" || binding.Workload == nil || binding.UserID == "" || binding.ClientID == "" {
		return QueueReplayInputs{}, fmt.Errorf("queue-replay step %s workload binding is invalid", step.ID)
	}
	parameters := binding.Workload
	if parameters.RecordCount == 0 || parameters.RecordCount > maxNativeWorkloadRecords || len(parameters.Targets) != 1 {
		return QueueReplayInputs{}, fmt.Errorf("queue-replay step %s workload bounds are invalid", step.ID)
	}
	validator := scenarioValidator{}
	validator.validateNativeWorkload(step, *binding)
	if err := joinScenarioErrors(validator.errors); err != nil {
		return QueueReplayInputs{}, fmt.Errorf("validate queue-replay step %s: %w", step.ID, err)
	}
	if parameters.AuthoredSchema != current.SchemaFact || current.Version >= uint64(maxNativeIdentityInteger) || commitLSN == 0 || commitLSN == ^uint64(0) {
		return QueueReplayInputs{}, fmt.Errorf("queue-replay step %s schema or commit boundary is invalid", step.ID)
	}
	var payload QueueReplayWorkload
	if err := json.Unmarshal(step.Operation.Payload, &payload); err != nil {
		return QueueReplayInputs{}, fmt.Errorf("decode queue-replay step %s workload: %w", step.ID, err)
	}
	if OperationKey(step.Operation) != "workload/prepare" || payload.Profile != "pending_mutations" ||
		payload.UserID != binding.UserID || payload.ClientID != binding.ClientID ||
		payload.RejectedCount != 1 || payload.AcceptedCount != parameters.RecordCount-1 {
		return QueueReplayInputs{}, fmt.Errorf("queue-replay step %s workload payload is invalid", step.ID)
	}
	table := current.Tables[0]
	target := parameters.Targets[0]
	if target.TableID != table.TableID || payload.TableID != table.TableID || target.PrimaryKeyFieldID != table.PrimaryKeyFieldID {
		return QueueReplayInputs{}, fmt.Errorf("queue-replay step %s workload target is invalid", step.ID)
	}
	writable := make([]string, 0, len(table.Fields))
	for _, field := range table.Fields {
		if !field.PrimaryKey && field.Writable && field.Type == "string" {
			writable = append(writable, field.FieldID)
		}
	}
	sort.Strings(writable)
	if len(writable) < 2 {
		return QueueReplayInputs{}, errors.New("queue-replay table has fewer than two writable string fields")
	}
	rejectedField, acceptedField := writable[0], writable[1]
	local := make([]Operation, 0, parameters.RecordCount)
	wire := make([]map[string]any, 0, parameters.RecordCount)
	for _, kind := range parameters.MutationKinds {
		fieldIDs := append([]string(nil), kind.FieldIDs...)
		sort.Strings(fieldIDs)
		for count := uint64(0); count < kind.Count; count++ {
			ordinal := uint64(len(local))
			columns := make([]map[string]string, 0, len(fieldIDs))
			wireColumns := make(map[string]string, len(fieldIDs))
			for _, fieldID := range fieldIDs {
				if fieldID != acceptedField && (fieldID != rejectedField || ordinal+1 != parameters.RecordCount) {
					return QueueReplayInputs{}, fmt.Errorf("queue-replay local write %d has an unexpected field %s", ordinal+1, fieldID)
				}
				value := fmt.Sprintf("workload-%d-%06d", parameters.Seed, ordinal+1)
				columns = append(columns, map[string]string{"field_id": fieldID, "value": value})
				wireColumns[fieldID] = value
			}
			wantFields := 1
			if ordinal+1 == parameters.RecordCount {
				wantFields = 2
			}
			if len(wireColumns) != wantFields || wireColumns[acceptedField] == "" {
				return QueueReplayInputs{}, fmt.Errorf("queue-replay local write %d lacks a required field", ordinal+1)
			}
			mutationID := queueReplayUUID(fmt.Sprintf(
				"synchro:native-workload:v1:%d:%s:%s:%d:%d",
				parameters.Seed, target.ScopeID, target.TableID, ordinal/parameters.BatchSize, ordinal%parameters.BatchSize,
			))
			pk := map[string]string{target.PrimaryKeyFieldID: fmt.Sprintf("workload-%d-%s-%06d", parameters.Seed, target.ScopeID, ordinal+1)}
			schema := map[string]any{"version": current.Version, "hash": current.Hash}
			encoded, err := json.Marshal(map[string]any{
				"authenticated_user_id": binding.UserID, "client_id": binding.ClientID,
				"mutation_id": mutationID, "table_id": table.TableID, "pk": pk, "authored_schema": schema,
				"operation": kind.Operation, "client_version": parameters.ClientVersion, "columns": columns,
			})
			if err != nil {
				return QueueReplayInputs{}, fmt.Errorf("encode queue-replay local write %d: %w", ordinal+1, err)
			}
			operation := Operation{ContractOperation: "local", Name: "write", Payload: encoded}
			if err := ValidateOperation(operation); err != nil {
				return QueueReplayInputs{}, fmt.Errorf("validate queue-replay local write %d: %w", ordinal+1, err)
			}
			local = append(local, operation)
			wire = append(wire, map[string]any{
				"mutation_id": mutationID, "table": table.TableID, "pk": pk, "authored_schema": schema,
				"op": kind.Operation, "client_version": parameters.ClientVersion, "columns": wireColumns,
			})
		}
	}
	encoded, err := json.Marshal(local)
	if err != nil {
		return QueueReplayInputs{}, fmt.Errorf("encode queue-replay local operations: %w", err)
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != parameters.Expectation.OperationDigest {
		return QueueReplayInputs{}, fmt.Errorf("queue-replay step %s generated operation digest does not match expectation", step.ID)
	}
	next, publish, err := queueReplayNextSchema(current, rejectedField, current.Version+1)
	if err != nil {
		return QueueReplayInputs{}, err
	}
	batchID := queueReplayUUID(fmt.Sprintf("synchro:workload:batch:%s:%s:%d:%d", binding.UserID, binding.ClientID, current.Version, parameters.RecordCount))
	encoded, err = json.Marshal(map[string]any{
		"authenticated_user_id": binding.UserID,
		"request": map[string]any{
			"client_id": binding.ClientID, "client_generation": 1, "batch_id": batchID,
			"schema": map[string]any{"version": next.Version, "hash": next.Hash}, "mutations": wire,
		},
		"delivery": "drop_after_server", "commit_lsn": strconv.FormatUint(commitLSN, 10), "end_lsn": strconv.FormatUint(commitLSN+1, 10),
	})
	if err != nil {
		return QueueReplayInputs{}, fmt.Errorf("encode queue-replay push: %w", err)
	}
	push := Operation{ContractOperation: "push", Name: "submit", Payload: encoded}
	if err := ValidateOperation(push); err != nil {
		return QueueReplayInputs{}, fmt.Errorf("validate queue-replay push: %w", err)
	}
	return QueueReplayInputs{Local: local, Publish: publish, DropPush: push, BatchID: batchID, NextSchema: next}, nil
}

func validateQueueReplaySchema(schema QueueReplaySchema) error {
	if schema.Version == 0 || !isNativeSHA256(schema.Hash) || len(schema.Tables) != 1 {
		return errors.New("queue-replay schema reference or table count is invalid")
	}
	table := schema.Tables[0]
	if table.TableID == "" || table.RelationID == "" || table.Name == "" || table.PrimaryKeyFieldID == "" {
		return errors.New("queue-replay table identity is incomplete")
	}
	fields := make(map[string]struct{}, len(table.Fields))
	for _, field := range table.Fields {
		if field.FieldID == "" || field.Name == "" {
			return errors.New("queue-replay field identity is incomplete")
		}
		if _, duplicate := fields[field.FieldID]; duplicate {
			return fmt.Errorf("queue-replay field %s is duplicated", field.FieldID)
		}
		fields[field.FieldID] = struct{}{}
		if field.PrimaryKey != (field.FieldID == table.PrimaryKeyFieldID) {
			return fmt.Errorf("queue-replay field %s has an inconsistent primary-key flag", field.FieldID)
		}
		if field.DefaultWireJSON != nil && !json.Valid([]byte(*field.DefaultWireJSON)) {
			return fmt.Errorf("queue-replay field %s has an invalid default wire value", field.FieldID)
		}
	}
	if _, exists := fields[table.PrimaryKeyFieldID]; !exists {
		return errors.New("queue-replay primary-key field is missing")
	}
	indexes := make(map[string]struct{}, len(table.Indexes))
	for _, index := range table.Indexes {
		if index.IndexID == "" {
			return errors.New("queue-replay index identity is missing")
		}
		if _, duplicate := indexes[index.IndexID]; duplicate {
			return fmt.Errorf("queue-replay index %s is duplicated", index.IndexID)
		}
		indexes[index.IndexID] = struct{}{}
		for _, fieldID := range index.FieldIDs {
			if _, exists := fields[fieldID]; !exists {
				return fmt.Errorf("queue-replay index %s references missing field %s", index.IndexID, fieldID)
			}
		}
	}
	return nil
}

func queueReplayNextSchema(current QueueReplaySchema, removedField string, version uint64) (QueueReplaySchema, Operation, error) {
	if err := validateQueueReplaySchema(current); err != nil {
		return QueueReplaySchema{}, Operation{}, err
	}
	if version != current.Version+1 || version > uint64(maxNativeIdentityInteger) {
		return QueueReplaySchema{}, Operation{}, errors.New("queue-replay next schema version is invalid")
	}
	table := current.Tables[0]
	addedField := "queue_value_" + strconv.FormatUint(version, 10)
	fields := make([]QueueReplaySchemaField, 0, len(table.Fields))
	removed := false
	for _, field := range table.Fields {
		if field.FieldID == addedField {
			return QueueReplaySchema{}, Operation{}, fmt.Errorf("queue-replay field %s already exists", addedField)
		}
		if field.FieldID == removedField {
			if field.PrimaryKey {
				return QueueReplaySchema{}, Operation{}, errors.New("queue-replay cannot remove the primary-key field")
			}
			removed = true
		} else {
			fields = append(fields, field)
		}
	}
	if !removed {
		return QueueReplaySchema{}, Operation{}, fmt.Errorf("queue-replay removed field %s is missing", removedField)
	}
	emptyString := `""`
	fields = append(fields, QueueReplaySchemaField{
		FieldID: addedField, Name: addedField, Type: "string", Writable: true, DefaultWireJSON: &emptyString,
	})
	table.Fields = fields
	table.Indexes = append([]QueueReplaySchemaIndex{}, table.Indexes...)
	next := QueueReplaySchema{SchemaFact: SchemaFact{Version: version, Hash: current.Hash}, Tables: []QueueReplaySchemaTable{table}}
	if err := validateQueueReplaySchema(next); err != nil {
		return QueueReplaySchema{}, Operation{}, err
	}
	manifestFields := make([]map[string]any, 0, len(fields))
	for _, field := range fields {
		manifestFields = append(manifestFields, map[string]any{
			"field_id": field.FieldID, "name": field.Name, "type": field.Type, "nullable": field.Nullable, "writable": field.Writable,
		})
	}
	sort.Slice(manifestFields, func(i, j int) bool {
		return manifestFields[i]["field_id"].(string) < manifestFields[j]["field_id"].(string)
	})
	indexes := append([]QueueReplaySchemaIndex{}, table.Indexes...)
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].IndexID < indexes[j].IndexID })
	body := map[string]any{
		"parent_schema":  map[string]any{"version": current.Version, "hash": current.Hash},
		"schema_version": version, "transition_class": "class_4", "compatibility_floor": version,
		"tables": []map[string]any{{
			"table_id": table.TableID, "relation_id": table.RelationID, "name": table.Name,
			"composition": table.Composition, "primary_key_field_id": table.PrimaryKeyFieldID,
			"lifecycle": map[string]any{
				"created_at_field_id": table.CreatedAtFieldID, "updated_at_field_id": table.UpdatedAtFieldID, "deleted_at_field_id": table.DeletedAtFieldID,
			},
			"fields": manifestFields, "indexes": indexes,
		}},
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return QueueReplaySchema{}, Operation{}, fmt.Errorf("encode queue-replay manifest: %w", err)
	}
	canonical, err := jcs.Transform(encoded)
	if err != nil {
		return QueueReplaySchema{}, Operation{}, fmt.Errorf("canonicalize queue-replay manifest: %w", err)
	}
	digest := sha256.Sum256(append([]byte("synchro:v3:schema-manifest:v1\x00"), canonical...))
	next.Hash = hex.EncodeToString(digest[:])
	body["schema_hash"] = next.Hash
	encoded, err = json.Marshal(body)
	if err != nil {
		return QueueReplaySchema{}, Operation{}, fmt.Errorf("encode queue-replay manifest hash: %w", err)
	}
	canonical, err = jcs.Transform(encoded)
	if err != nil {
		return QueueReplaySchema{}, Operation{}, fmt.Errorf("canonicalize queue-replay manifest hash: %w", err)
	}
	encoded, err = json.Marshal(map[string]any{
		"schema": next.SchemaFact, "body": string(canonical), "transition_class": "class_4",
		"compatibility_floor": version, "tables": next.Tables, "affected_scopes": []string{},
	})
	if err != nil {
		return QueueReplaySchema{}, Operation{}, fmt.Errorf("encode queue-replay schema publication: %w", err)
	}
	publish := Operation{ContractOperation: "model", Name: "publish-schema", Payload: encoded}
	if err := ValidateOperation(publish); err != nil {
		return QueueReplaySchema{}, Operation{}, fmt.Errorf("validate queue-replay schema publication: %w", err)
	}
	return next, publish, nil
}

func queueReplayUUID(input string) string {
	digest := sha256.Sum256([]byte(input))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
