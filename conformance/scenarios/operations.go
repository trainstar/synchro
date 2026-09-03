package scenarios

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const wireFaultTemporaryUnavailable = "temporary_unavailable"

var closedOperationFields = map[string]operationFields{
	"artifact/install-portable-seed":                {required: []string{"user_id", "client_id", "portable_seed_artifact_id", "seed_fixture_id"}},
	"connect/send":                                  {required: []string{"user_id", "client_id", "runtime_version", "protocol_version", "schema_reset", "schema", "scope_set_version", "known_scopes"}, optional: []string{"client_generation", "seed_receipts"}},
	"local/apply-pull-page":                         {required: []string{"user_id", "client_id", "source_step_id"}},
	"local/apply-rebuild-page":                      {required: []string{"user_id", "client_id", "scope_id", "rebuild_id", "page_ordinal", "request_token_source"}},
	"local/begin-rebuild":                           {required: []string{"user_id", "client_id", "client_generation", "schema", "scope_id", "rebuild_id", "limit"}},
	"local/finalize-rebuild":                        {required: []string{"user_id", "client_id", "scope_id", "rebuild_id"}},
	"local/write":                                   {required: []string{"authenticated_user_id", "client_id", "mutation_id", "table_id", "pk", "authored_schema", "operation", "client_version"}, optional: []string{"base_version", "columns", "origin"}},
	"model/activate-registry-membership-generation": {required: []string{"registry_generation"}},
	"model/commit-source-transaction":               {required: []string{"stream_generation", "commit_lsn", "end_lsn", "events"}},
	"model/compact-scope":                           {required: []string{"scope_id", "batch_size"}},
	"model/expire-client-generation":                {required: []string{"user_id", "client_id"}},
	"model/install-current-contract":                {required: []string{"installation", "initial_schema", "initial_registry", "stream", "empty_scopes", "clients", "write_policies", "configured_limits"}},
	"model/publish-schema":                          {required: []string{"schema", "body", "transition_class", "compatibility_floor", "tables", "affected_scopes"}},
	"model/set-client-assignments":                  {required: []string{"user_id", "client_id", "assignments"}},
	"model/stage-registry-membership-generation":    {required: []string{"registry_generation", "membership_generation", "batch_size", "activation_boundary", "affected_scopes", "scope_rules", "dependency_impacts"}},
	"process/acknowledge-contiguous-prefix":         {required: []string{"stream_generation"}},
	"process/materialize-source-transaction":        {required: []string{"stream_generation", "commit_lsn"}, optional: []string{"failure_class"}},
	"process/repair-and-retry-source-transaction":   {required: []string{"stream_generation", "commit_lsn"}},
	"process/response-loss":                         {required: []string{"authenticated_user_id", "client_id", "batch_id"}},
	"process/restart-client":                        {required: []string{"user_id", "client_id"}},
	"process/restart-wal-worker":                    {required: []string{"worker_id"}},
	"pull/request-page":                             {required: []string{"user_id", "client_id", "client_generation", "schema", "scope_set_version", "scopes", "limit"}},
	"push/submit":                                   {required: []string{"authenticated_user_id", "request", "delivery", "commit_lsn", "end_lsn"}},
	"rebuild/request-page":                          {required: []string{"user_id", "client_id", "client_generation", "schema", "scope_id", "rebuild_id", "cursor_source", "limit"}},
	"workload/prepare":                              {},
}

type payloadShape struct {
	fields    operationFields
	children  map[string]payloadChild
	allowNull bool
}

type payloadChild struct {
	object            *payloadShape
	arrayItem         *payloadShape
	dynamicObject     bool
	objectOrArrayItem *payloadShape
}

var (
	schemaReferenceShape    = shape(required("version", "hash"))
	rowIdentityShape        = shape(required("canonical_identity_bytes", "table_id", "primary_key_field_id", "portable_type", "canonical_wire_json"))
	fieldValueShape         = shape(required("field", "type", "wire_json"))
	registeredIdentityShape = shapeWith(operationFields{required: []string{"kind"}, optional: []string{"synced_row", "capture_key"}}, map[string]payloadChild{
		"synced_row":  objectChild(nullableShape(rowIdentityShape)),
		"capture_key": objectChild(nullableShape(shape(required("canonical_key_bytes")))),
	})
	registeredImageShape = shapeWith(required("identity", "fields", "version", "checksum", "deleted"), map[string]payloadChild{
		"identity": objectChild(registeredIdentityShape),
		"fields":   arrayChild(fieldValueShape),
	})
	membershipEvaluationShape = shapeWith(required("row", "scopes"), map[string]payloadChild{
		"row": objectChild(rowIdentityShape),
	})
	membershipScopeRuleShape = shapeWith(required("scope_rule_id", "relation", "membership_function", "positive_fanout_bound", "evaluations"), map[string]payloadChild{
		"evaluations": arrayChild(membershipEvaluationShape),
	})
	membershipImpactShape = shapeWith(required("dependency_impact_id", "relation", "function", "captured_field_ids", "positive_row_bound", "affected_rows", "requires_rebuild"), map[string]payloadChild{
		"affected_rows": arrayChild(rowIdentityShape),
	})
	schemaFieldShape = shape(required("field_id", "name", "type", "primary_key", "nullable", "writable", "decimal_precision", "decimal_scale", "default_wire_json"))
	schemaIndexShape = shape(required("index_id", "name", "field_ids", "unique"))
	schemaTableShape = shapeWith(required("table_id", "relation_id", "name", "composition", "primary_key_field_id", "created_at_field_id", "updated_at_field_id", "deleted_at_field_id", "fields", "indexes"), map[string]payloadChild{
		"fields":  arrayChild(schemaFieldShape),
		"indexes": arrayChild(schemaIndexShape),
	})
	publishSchemaShape = shapeWith(required("schema", "body", "transition_class", "compatibility_floor", "tables", "affected_scopes"), map[string]payloadChild{
		"schema": objectChild(schemaReferenceShape),
		"tables": arrayChild(schemaTableShape),
	})
)

var closedPayloadShapes = map[string]*payloadShape{
	"connect/send": shapeWith(closedOperationFields["connect/send"], map[string]payloadChild{
		"schema":        objectChild(schemaReferenceShape),
		"known_scopes":  arrayChild(shape(required("scope_id"))),
		"seed_receipts": dynamicObjectChild(),
	}),
	"local/begin-rebuild": shapeWith(closedOperationFields["local/begin-rebuild"], map[string]payloadChild{"schema": objectChild(schemaReferenceShape)}),
	"local/write": shapeWith(closedOperationFields["local/write"], map[string]payloadChild{
		"pk":              dynamicObjectChild(),
		"authored_schema": objectChild(schemaReferenceShape),
		"columns":         objectOrArrayChild(shape(operationFields{required: []string{"field_id", "value"}, optional: []string{"support"}})),
	}),
	"model/commit-source-transaction": shapeWith(closedOperationFields["model/commit-source-transaction"], map[string]payloadChild{
		"events": arrayChild(shapeWith(required("event_ordinal", "relation", "operation", "before", "after"), map[string]payloadChild{
			"before": objectChild(nullableShape(registeredImageShape)),
			"after":  objectChild(nullableShape(registeredImageShape)),
		})),
	}),
	"model/install-current-contract": installContractShape(),
	"model/publish-schema":           publishSchemaShape,
	"model/set-client-assignments": shapeWith(closedOperationFields["model/set-client-assignments"], map[string]payloadChild{
		"assignments": arrayChild(shape(required("scope_id"))),
	}),
	"model/stage-registry-membership-generation": shapeWith(closedOperationFields["model/stage-registry-membership-generation"], map[string]payloadChild{
		"activation_boundary": objectChild(shape(required("stream_generation", "kind", "commit_lsn"))),
		"scope_rules":         arrayChild(membershipScopeRuleShape),
		"dependency_impacts":  arrayChild(membershipImpactShape),
	}),
	"pull/request-page": shapeWith(closedOperationFields["pull/request-page"], map[string]payloadChild{
		"schema": objectChild(schemaReferenceShape),
		"scopes": arrayChild(shape(required("scope_id", "cursor_source"))),
	}),
	"push/submit": shapeWith(closedOperationFields["push/submit"], map[string]payloadChild{
		"request": objectChild(shapeWith(required("client_id", "client_generation", "batch_id", "schema", "mutations"), map[string]payloadChild{
			"schema": objectChild(schemaReferenceShape),
			"mutations": arrayChild(shapeWith(operationFields{required: []string{"mutation_id", "table", "pk", "authored_schema", "op", "client_version"}, optional: []string{"base_version", "columns"}}, map[string]payloadChild{
				"pk":              dynamicObjectChild(),
				"authored_schema": objectChild(schemaReferenceShape),
				"columns":         dynamicObjectChild(),
			})),
		})),
	}),
	"rebuild/request-page": shapeWith(closedOperationFields["rebuild/request-page"], map[string]payloadChild{"schema": objectChild(schemaReferenceShape)}),
}

// OperationClass identifies the package that executes one closed scenario operation.
type OperationClass string

const (
	OperationClassReference        OperationClass = "reference"
	OperationClassModelRunnerMacro OperationClass = "model_runner_macro"
)

type operationFields struct {
	required []string
	optional []string
}

var forbiddenSetupMembers = map[string]struct{}{
	"rows": {}, "effects": {}, "projections": {}, "cursors": {}, "checkpoints": {},
	"fences": {}, "batches": {}, "mutations": {}, "rebuilds": {}, "retention_floors": {},
	"seed_exports": {}, "seed_records": {}, "seed_receipts": {}, "source_transactions": {},
	"materializations": {}, "acknowledgements": {}, "poison": {}, "local_rows": {},
	"provenance": {}, "durable_queue": {}, "outcomes": {}, "events": {}, "tokens": {},
}

// OperationKey returns the stable closed key for one operation.
func OperationKey(operation Operation) string {
	return operation.ContractOperation + "/" + operation.Name
}

// OperationKeys returns the complete closed operation registry in stable order.
func OperationKeys() []string {
	keys := make([]string, 0, len(closedOperationFields))
	for key := range closedOperationFields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// LookupOperationClass returns the execution class for one closed operation key.
func LookupOperationClass(key string) (OperationClass, bool) {
	if _, found := closedOperationFields[key]; !found {
		return "", false
	}
	if key == "workload/prepare" {
		return OperationClassModelRunnerMacro, true
	}
	return OperationClassReference, true
}

// ValidateOperation validates an operation name and its closed payload shape.
func ValidateOperation(operation Operation) error {
	key := OperationKey(operation)
	fields, known := closedOperationFields[key]
	if !known {
		return fmt.Errorf("unknown operation %q", key)
	}
	if err := validateWireFaultControl(key, operation.WireFault); err != nil {
		return fmt.Errorf("validate %s wire fault: %w", key, err)
	}
	if key == "workload/prepare" {
		return validateWorkloadPayload(operation.Payload)
	}
	object, err := decodePayloadObject(operation.Payload)
	if err != nil {
		return fmt.Errorf("validate %s payload: %w", key, err)
	}
	if err := validateObjectFields(object, fields); err != nil {
		return fmt.Errorf("validate %s payload: %w", key, err)
	}
	if shape, found := closedPayloadShapes[key]; found {
		if err := validatePayloadShape(operation.Payload, shape, "payload"); err != nil {
			return fmt.Errorf("validate %s payload: %w", key, err)
		}
	}
	if key == "model/install-current-contract" {
		var value any
		if err := json.Unmarshal(operation.Payload, &value); err != nil {
			return fmt.Errorf("validate %s payload: %w", key, err)
		}
		if err := rejectForbiddenSetupMembers(value, "payload"); err != nil {
			return fmt.Errorf("validate %s payload: %w", key, err)
		}
	}
	if key == "artifact/install-portable-seed" {
		if stringValue(object["portable_seed_artifact_id"]) != "ARTDEF-PORTABLE-SEED-001" {
			return errors.New("validate artifact/install-portable-seed payload: portable_seed_artifact_id is invalid")
		}
		if stringValue(object["seed_fixture_id"]) != "SEEDFIX-PORTABLE-SHARED-1000-001" {
			return errors.New("validate artifact/install-portable-seed payload: seed_fixture_id is invalid")
		}
	}
	if operation.WireFault != nil {
		if _, _, err := TemporaryUnavailablePushTarget(operation); err != nil {
			return fmt.Errorf("validate %s wire fault target: %w", key, err)
		}
	}
	return nil
}

// TemporaryUnavailablePushTarget returns the request targeted by the one fixed native push fault.
func TemporaryUnavailablePushTarget(operation Operation) (PushWireFaultTarget, bool, error) {
	if operation.WireFault == nil {
		return PushWireFaultTarget{}, false, nil
	}
	if err := validateWireFaultControl(OperationKey(operation), operation.WireFault); err != nil {
		return PushWireFaultTarget{}, false, err
	}
	var payload struct {
		Request struct {
			ClientID string `json:"client_id"`
			BatchID  string `json:"batch_id"`
		} `json:"request"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || payload.Request.ClientID == "" || payload.Request.BatchID == "" {
		return PushWireFaultTarget{}, false, errors.New("push wire-fault target is invalid")
	}
	return PushWireFaultTarget{ClientID: payload.Request.ClientID, BatchID: payload.Request.BatchID}, true, nil
}

func validateWireFaultControl(key string, control *WireFaultControl) error {
	if control == nil {
		return nil
	}
	if key != "push/submit" {
		return errors.New("wire fault requires push/submit")
	}
	if control.Mode != wireFaultTemporaryUnavailable {
		return errors.New("wire fault mode is unsupported")
	}
	return nil
}

func knownOperation(operation Operation) bool {
	return ValidateOperation(operation) == nil
}

func decodePayloadObject(payload json.RawMessage) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := jsonstrict.Decode(payload, &object); err != nil {
		return nil, err
	}
	return object, nil
}

func validateObjectFields(object map[string]json.RawMessage, fields operationFields) error {
	allowed := make(map[string]struct{}, len(fields.required)+len(fields.optional))
	for _, field := range fields.required {
		allowed[field] = struct{}{}
		if _, found := object[field]; !found {
			return fmt.Errorf("required member %q is absent", field)
		}
	}
	for _, field := range fields.optional {
		allowed[field] = struct{}{}
	}
	for field := range object {
		if _, known := allowed[field]; !known {
			return fmt.Errorf("unknown member %q", field)
		}
	}
	return nil
}

func installContractShape() *payloadShape {
	physical := shape(required("schema", "name", "oid", "replica_identity"))
	relation := shapeWith(required("relation", "registration_kind", "table_id", "physical", "primary_key_field_id", "primary_key_physical_column", "primary_key_portable_type", "capture_key_field_ids", "captured_field_ids", "membership_function", "positive_fanout_bound", "dependency_impact_function", "dependency_captured_field_ids", "positive_dependency_row_bound"), map[string]payloadChild{
		"physical": objectChild(physical),
	})
	registry := shapeWith(required("registry_generation", "relations", "capture_dependencies", "scope_rules", "dependency_impacts"), map[string]payloadChild{
		"relations":            arrayChild(relation),
		"capture_dependencies": arrayChild(shape(required("capture_dependency_id", "relation", "depends_on"))),
		"scope_rules":          arrayChild(membershipScopeRuleShape),
		"dependency_impacts":   arrayChild(membershipImpactShape),
	})
	return shapeWith(closedOperationFields["model/install-current-contract"], map[string]payloadChild{
		"installation": objectChild(shapeWith(required("installed", "schema_name", "extension_version", "protocol_version", "minimum_client_runtime", "stale_client_interval_milliseconds", "endpoints", "capabilities"), map[string]payloadChild{
			"capabilities": arrayChild(shape(required("capability_id", "enabled"))),
		})),
		"initial_schema":   objectChild(publishSchemaShape),
		"initial_registry": objectChild(registry),
		"stream":           objectChild(shape(required("stream_generation", "database", "worker_id", "slot_id"))),
		"empty_scopes":     arrayChild(shape(required("scope_id", "membership_generation", "retention_generation"))),
		"clients": arrayChild(shapeWith(required("user_id", "client_id", "client_generation", "scope_set_version", "accepted_write_epoch", "last_cursor_acknowledged_at", "assigned_scope_ids", "local_schema", "local_lifecycle"), map[string]payloadChild{
			"local_schema": objectChild(schemaReferenceShape),
		})),
		"write_policies":    arrayChild(shape(required("user_id", "table_id", "allowed"))),
		"configured_limits": objectChild(shape(required("max_scope_fanout", "max_impact_rows", "pull_maximum", "rebuild_maximum", "compaction_batch_maximum", "backfill_batch_maximum"))),
	})
}

func required(names ...string) operationFields {
	return operationFields{required: names}
}

func shape(fields operationFields) *payloadShape {
	return &payloadShape{fields: fields}
}

func shapeWith(fields operationFields, children map[string]payloadChild) *payloadShape {
	return &payloadShape{fields: fields, children: children}
}

func nullableShape(value *payloadShape) *payloadShape {
	copy := *value
	copy.allowNull = true
	return &copy
}

func objectChild(value *payloadShape) payloadChild {
	return payloadChild{object: value}
}

func arrayChild(value *payloadShape) payloadChild {
	return payloadChild{arrayItem: value}
}

func dynamicObjectChild() payloadChild {
	return payloadChild{dynamicObject: true}
}

func objectOrArrayChild(arrayItem *payloadShape) payloadChild {
	return payloadChild{objectOrArrayItem: arrayItem}
}

func validatePayloadShape(raw json.RawMessage, shape *payloadShape, path string) error {
	if shape == nil {
		return nil
	}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		if shape.allowNull {
			return nil
		}
		return fmt.Errorf("%s must be an object", path)
	}
	object, err := decodePayloadObject(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if err := validateObjectFields(object, shape.fields); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	for name, child := range shape.children {
		value, found := object[name]
		if !found || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			if child.object != nil && child.object.allowNull {
				continue
			}
			continue
		}
		switch {
		case child.object != nil:
			if err := validatePayloadShape(value, child.object, path+"."+name); err != nil {
				return err
			}
		case child.arrayItem != nil:
			var items []json.RawMessage
			if err := json.Unmarshal(value, &items); err != nil {
				return fmt.Errorf("%s.%s must be an array", path, name)
			}
			for index, item := range items {
				if err := validatePayloadShape(item, child.arrayItem, fmt.Sprintf("%s.%s[%d]", path, name, index)); err != nil {
					return err
				}
			}
		case child.dynamicObject:
			var dynamic map[string]json.RawMessage
			if err := json.Unmarshal(value, &dynamic); err != nil {
				return fmt.Errorf("%s.%s must be an object", path, name)
			}
		case child.objectOrArrayItem != nil:
			trimmedValue := bytes.TrimSpace(value)
			if len(trimmedValue) != 0 && trimmedValue[0] == '{' {
				var dynamic map[string]json.RawMessage
				if err := json.Unmarshal(value, &dynamic); err != nil {
					return fmt.Errorf("%s.%s must be an object or array", path, name)
				}
				continue
			}
			var items []json.RawMessage
			if err := json.Unmarshal(value, &items); err != nil {
				return fmt.Errorf("%s.%s must be an object or array", path, name)
			}
			for index, item := range items {
				if err := validatePayloadShape(item, child.objectOrArrayItem, fmt.Sprintf("%s.%s[%d]", path, name, index)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateWorkloadPayload(payload json.RawMessage) error {
	object, err := decodePayloadObject(payload)
	if err != nil {
		return fmt.Errorf("validate workload/prepare payload: %w", err)
	}
	profile := stringValue(object["profile"])
	var fields operationFields
	switch profile {
	case "scope_topology":
		fields.required = []string{"profile", "scope_fanout", "impact_rows"}
	case "scope_cardinality":
		fields.required = []string{"profile", "scope_id", "record_count", "page_size"}
	case "pending_mutations":
		fields.required = []string{"profile", "user_id", "client_id", "table_id", "accepted_count", "rejected_count"}
	case "configured_limits":
		fields.required = []string{"profile", "max_scope_fanout", "max_impact_rows", "pull_maximum", "rebuild_maximum", "compaction_batch_maximum", "backfill_batch_maximum"}
	default:
		return fmt.Errorf("validate workload/prepare payload: unknown profile %q", profile)
	}
	if err := validateObjectFields(object, fields); err != nil {
		return fmt.Errorf("validate workload/prepare payload: %w", err)
	}
	return nil
}

func rejectForbiddenSetupMembers(value any, path string) error {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if _, forbidden := forbiddenSetupMembers[key]; forbidden {
				return fmt.Errorf("%s contains forbidden member %q", path, key)
			}
			if err := rejectForbiddenSetupMembers(child, path+"."+key); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range value {
			if err := rejectForbiddenSetupMembers(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func stringValue(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}
