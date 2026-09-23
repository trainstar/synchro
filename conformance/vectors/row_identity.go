package vectors

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

var (
	schemaManifestDomain = []byte("synchro:v3:schema-manifest:v1\x00")
	rowIdentityDomain    = []byte("synchro:v3:row-identity:v1\x00")
)

type manifestBodyJSON struct {
	SchemaVersion      uint64               `json:"schema_version"`
	ParentSchema       *schemaReferenceJSON `json:"parent_schema"`
	TransitionClass    string               `json:"transition_class"`
	CompatibilityFloor uint64               `json:"compatibility_floor"`
	Tables             []manifestTableJSON  `json:"tables"`
}

type schemaReferenceJSON struct {
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

type manifestTableJSON struct {
	TableID           string                `json:"table_id"`
	RelationID        string                `json:"relation_id"`
	Name              string                `json:"name"`
	Composition       string                `json:"composition"`
	PrimaryKeyFieldID string                `json:"primary_key_field_id"`
	Lifecycle         manifestLifecycleJSON `json:"lifecycle"`
	Fields            []manifestFieldJSON   `json:"fields"`
	Indexes           []manifestIndexJSON   `json:"indexes"`
}

type manifestLifecycleJSON struct {
	CreatedAtFieldID *string `json:"created_at_field_id"`
	UpdatedAtFieldID *string `json:"updated_at_field_id"`
	DeletedAtFieldID *string `json:"deleted_at_field_id"`
}

type manifestFieldJSON struct {
	FieldID   string `json:"field_id"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Nullable  bool   `json:"nullable"`
	Writable  bool   `json:"writable"`
	Precision *int   `json:"precision,omitempty"`
	Scale     *int   `json:"scale,omitempty"`
}

type manifestIndexJSON struct {
	IndexID  string   `json:"index_id"`
	Name     string   `json:"name"`
	FieldIDs []string `json:"field_ids"`
	Unique   bool     `json:"unique"`
}

// ParseManifest strictly parses, canonicalizes, and verifies one manifest.
func ParseManifest(raw json.RawMessage) (Manifest, error) {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return Manifest{}, fmt.Errorf("validate manifest JSON: %w", err)
	}
	if err := requireObjectKeys(object, []string{
		"schema_version", "schema_hash", "parent_schema", "transition_class", "compatibility_floor", "tables",
	}, nil); err != nil {
		return Manifest{}, err
	}

	schemaVersion, err := decodePositiveSafeUint(object["schema_version"], "schema_version")
	if err != nil {
		return Manifest{}, err
	}
	schemaHashText, err := decodeRequiredString(object["schema_hash"], "schema_hash")
	if err != nil {
		return Manifest{}, err
	}
	schemaHash, err := decodeLowerSHA256(schemaHashText)
	if err != nil {
		return Manifest{}, fmt.Errorf("schema_hash: %w", err)
	}
	parent, err := parseParentSchema(object["parent_schema"])
	if err != nil {
		return Manifest{}, err
	}
	transitionClass, err := decodeRequiredString(object["transition_class"], "transition_class")
	if err != nil {
		return Manifest{}, err
	}
	switch transitionClass {
	case "initial", "class_2", "class_3", "class_4":
	default:
		return Manifest{}, fmt.Errorf("invalid transition_class %q", transitionClass)
	}
	if transitionClass == "initial" && parent != nil || transitionClass != "initial" && parent == nil {
		return Manifest{}, errors.New("parent_schema does not match transition_class")
	}
	compatibilityFloor, err := decodePositiveSafeUint(object["compatibility_floor"], "compatibility_floor")
	if err != nil {
		return Manifest{}, err
	}
	if compatibilityFloor > schemaVersion {
		return Manifest{}, errors.New("compatibility_floor exceeds schema_version")
	}

	tableValues, err := decodeJSONArray(object["tables"], jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return Manifest{}, fmt.Errorf("decode tables: %w", err)
	}
	if len(tableValues) == 0 {
		return Manifest{}, errors.New("manifest has no tables")
	}
	tables := make([]manifestTable, 0, len(tableValues))
	seenTables := make(map[string]struct{}, len(tableValues))
	for _, tableValue := range tableValues {
		table, err := parseManifestTable(tableValue)
		if err != nil {
			return Manifest{}, err
		}
		if _, duplicate := seenTables[table.TableID]; duplicate {
			return Manifest{}, fmt.Errorf("duplicate table_id %q", table.TableID)
		}
		seenTables[table.TableID] = struct{}{}
		tables = append(tables, table)
	}
	sort.Slice(tables, func(left, right int) bool {
		return tables[left].TableID < tables[right].TableID
	})

	manifest := Manifest{
		schemaVersion:      schemaVersion,
		schemaHash:         schemaHash,
		parentSchema:       parent,
		transitionClass:    transitionClass,
		compatibilityFloor: compatibilityFloor,
		tables:             tables,
	}
	body, err := marshalManifestBody(manifest)
	if err != nil {
		return Manifest{}, err
	}
	manifest.canonicalBody = body
	preimage, err := ManifestPreimage(manifest)
	if err != nil {
		return Manifest{}, err
	}
	computed := sha256.Sum256(preimage)
	if computed != schemaHash {
		return Manifest{}, errors.New("declared schema_hash does not match canonical manifest body")
	}
	return manifest, nil
}

func parseParentSchema(raw json.RawMessage) (*SchemaReference, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return nil, fmt.Errorf("decode parent_schema: %w", err)
	}
	if err := requireObjectKeys(object, []string{"version", "hash"}, nil); err != nil {
		return nil, fmt.Errorf("parent_schema: %w", err)
	}
	version, err := decodePositiveSafeUint(object["version"], "parent_schema.version")
	if err != nil {
		return nil, err
	}
	hashText, err := decodeRequiredString(object["hash"], "parent_schema.hash")
	if err != nil {
		return nil, err
	}
	hash, err := decodeLowerSHA256(hashText)
	if err != nil {
		return nil, fmt.Errorf("parent_schema.hash: %w", err)
	}
	return &SchemaReference{Version: version, Hash: hash}, nil
}

func parseManifestTable(raw json.RawMessage) (manifestTable, error) {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return manifestTable{}, fmt.Errorf("decode manifest table: %w", err)
	}
	if err := requireObjectKeys(object, []string{
		"table_id", "relation_id", "name", "composition", "primary_key_field_id", "lifecycle", "fields", "indexes",
	}, nil); err != nil {
		return manifestTable{}, fmt.Errorf("manifest table: %w", err)
	}
	tableID, err := decodeRequiredString(object["table_id"], "table_id")
	if err != nil {
		return manifestTable{}, err
	}
	relationID, err := decodeRequiredString(object["relation_id"], "relation_id")
	if err != nil {
		return manifestTable{}, err
	}
	name, err := decodeRequiredString(object["name"], "table name")
	if err != nil {
		return manifestTable{}, err
	}
	composition, err := decodeRequiredString(object["composition"], "composition")
	if err != nil {
		return manifestTable{}, err
	}
	if composition != "single_scope" && composition != "multi_scope" {
		return manifestTable{}, fmt.Errorf("invalid composition %q", composition)
	}
	primaryKeyFieldID, err := decodeRequiredString(object["primary_key_field_id"], "primary_key_field_id")
	if err != nil {
		return manifestTable{}, err
	}
	lifecycle, err := parseManifestLifecycle(object["lifecycle"])
	if err != nil {
		return manifestTable{}, err
	}
	fields, err := parseManifestFields(object["fields"])
	if err != nil {
		return manifestTable{}, fmt.Errorf("table %q fields: %w", tableID, err)
	}
	indexes, err := parseManifestIndexes(object["indexes"], fields)
	if err != nil {
		return manifestTable{}, fmt.Errorf("table %q indexes: %w", tableID, err)
	}

	fieldByID := make(map[string]manifestField, len(fields))
	for _, field := range fields {
		fieldByID[field.FieldID] = field
	}
	primary, ok := fieldByID[primaryKeyFieldID]
	if !ok {
		return manifestTable{}, errors.New("primary_key_field_id does not identify a field")
	}
	if primary.Spec.Nullable || primary.Writable || primary.Spec.Type != "string" && primary.Spec.Type != "int" && primary.Spec.Type != "int64" {
		return manifestTable{}, errors.New("primary key field has an invalid type, nullability, or writability")
	}
	for _, lifecycleID := range []*string{lifecycle.CreatedAtFieldID, lifecycle.UpdatedAtFieldID, lifecycle.DeletedAtFieldID} {
		if lifecycleID == nil {
			continue
		}
		field, ok := fieldByID[*lifecycleID]
		if !ok || field.Spec.Type != "datetime" || field.Writable {
			return manifestTable{}, errors.New("lifecycle field is missing or invalid")
		}
	}
	return manifestTable{
		TableID: tableID, RelationID: relationID, Name: name, Composition: composition,
		PrimaryKeyFieldID: primaryKeyFieldID, Lifecycle: lifecycle, Fields: fields, Indexes: indexes,
	}, nil
}

func parseManifestLifecycle(raw json.RawMessage) (manifestLifecycle, error) {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return manifestLifecycle{}, fmt.Errorf("decode lifecycle: %w", err)
	}
	if err := requireObjectKeys(object, []string{
		"created_at_field_id", "updated_at_field_id", "deleted_at_field_id",
	}, nil); err != nil {
		return manifestLifecycle{}, fmt.Errorf("lifecycle: %w", err)
	}
	created, err := decodeNullableString(object["created_at_field_id"], "created_at_field_id")
	if err != nil {
		return manifestLifecycle{}, err
	}
	updated, err := decodeNullableString(object["updated_at_field_id"], "updated_at_field_id")
	if err != nil {
		return manifestLifecycle{}, err
	}
	deleted, err := decodeNullableString(object["deleted_at_field_id"], "deleted_at_field_id")
	if err != nil {
		return manifestLifecycle{}, err
	}
	return manifestLifecycle{CreatedAtFieldID: created, UpdatedAtFieldID: updated, DeletedAtFieldID: deleted}, nil
}

func decodeNullableString(raw json.RawMessage, name string) (*string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	value, err := decodeRequiredString(raw, name)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func parseManifestFields(raw json.RawMessage) ([]manifestField, error) {
	values, err := decodeJSONArray(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, errors.New("field list is empty")
	}
	fields := make([]manifestField, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		object, err := strictJSONObject(value, jsonValidation{iJSON: true, safeInteger: true})
		if err != nil {
			return nil, err
		}
		if err := requireObjectKeys(object, []string{"field_id", "name", "type", "nullable", "writable"}, []string{"precision", "scale"}); err != nil {
			return nil, err
		}
		fieldID, err := decodeRequiredString(object["field_id"], "field_id")
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[fieldID]; duplicate {
			return nil, fmt.Errorf("duplicate field_id %q", fieldID)
		}
		seen[fieldID] = struct{}{}
		name, err := decodeRequiredString(object["name"], "field name")
		if err != nil {
			return nil, err
		}
		typeName, err := decodeRequiredString(object["type"], "field type")
		if err != nil {
			return nil, err
		}
		nullable, err := decodeBoolean(object["nullable"], "nullable")
		if err != nil {
			return nil, err
		}
		writable, err := decodeBoolean(object["writable"], "writable")
		if err != nil {
			return nil, err
		}
		spec := FieldSpec{Type: typeName, Nullable: nullable}
		if precisionRaw, ok := object["precision"]; ok {
			precision, err := decodePositiveInt(precisionRaw, "precision")
			if err != nil {
				return nil, err
			}
			spec.Precision = &precision
		}
		if scaleRaw, ok := object["scale"]; ok {
			scale, err := decodeNonnegativeInt(scaleRaw, "scale")
			if err != nil {
				return nil, err
			}
			spec.Scale = &scale
		}
		if _, err := validateFieldSpec(spec); err != nil {
			return nil, fmt.Errorf("field %q: %w", fieldID, err)
		}
		fields = append(fields, manifestField{FieldID: fieldID, Name: name, Spec: spec, Writable: writable})
	}
	sort.Slice(fields, func(left, right int) bool {
		return fields[left].FieldID < fields[right].FieldID
	})
	return fields, nil
}

func decodePositiveInt(raw json.RawMessage, name string) (int, error) {
	value, err := decodePositiveSafeUint(raw, name)
	if err != nil {
		return 0, err
	}
	maxInt := uint64(^uint(0) >> 1)
	if value > maxInt {
		return 0, fmt.Errorf("%s exceeds int", name)
	}
	return int(value), nil
}

func decodeNonnegativeInt(raw json.RawMessage, name string) (int, error) {
	trimmed := bytes.TrimSpace(raw)
	if !canonicalInteger.Match(trimmed) || bytes.HasPrefix(trimmed, []byte("-")) {
		return 0, fmt.Errorf("%s is not a canonical nonnegative integer", name)
	}
	value, err := json.Number(trimmed).Int64()
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s is outside int", name)
	}
	return int(value), nil
}

func parseManifestIndexes(raw json.RawMessage, fields []manifestField) ([]manifestIndex, error) {
	values, err := decodeJSONArray(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return nil, err
	}
	knownFields := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		knownFields[field.FieldID] = struct{}{}
	}
	indexes := make([]manifestIndex, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		object, err := strictJSONObject(value, jsonValidation{iJSON: true, safeInteger: true})
		if err != nil {
			return nil, err
		}
		if err := requireObjectKeys(object, []string{"index_id", "name", "field_ids", "unique"}, nil); err != nil {
			return nil, err
		}
		indexID, err := decodeRequiredString(object["index_id"], "index_id")
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[indexID]; duplicate {
			return nil, fmt.Errorf("duplicate index_id %q", indexID)
		}
		seen[indexID] = struct{}{}
		name, err := decodeRequiredString(object["name"], "index name")
		if err != nil {
			return nil, err
		}
		fieldValues, err := decodeJSONArray(object["field_ids"], jsonValidation{iJSON: true, safeInteger: true})
		if err != nil || len(fieldValues) == 0 {
			return nil, errors.New("index field_ids is empty or invalid")
		}
		fieldIDs := make([]string, 0, len(fieldValues))
		for _, fieldValue := range fieldValues {
			fieldID, err := decodeRequiredString(fieldValue, "index field_id")
			if err != nil {
				return nil, err
			}
			if _, ok := knownFields[fieldID]; !ok {
				return nil, fmt.Errorf("index references unknown field_id %q", fieldID)
			}
			fieldIDs = append(fieldIDs, fieldID)
		}
		unique, err := decodeBoolean(object["unique"], "index unique")
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, manifestIndex{IndexID: indexID, Name: name, FieldIDs: fieldIDs, Unique: unique})
	}
	sort.Slice(indexes, func(left, right int) bool {
		return indexes[left].IndexID < indexes[right].IndexID
	})
	return indexes, nil
}

func marshalManifestBody(manifest Manifest) ([]byte, error) {
	body := manifestBodyJSON{
		SchemaVersion:      manifest.schemaVersion,
		TransitionClass:    manifest.transitionClass,
		CompatibilityFloor: manifest.compatibilityFloor,
		Tables:             make([]manifestTableJSON, 0, len(manifest.tables)),
	}
	if manifest.parentSchema != nil {
		body.ParentSchema = &schemaReferenceJSON{
			Version: manifest.parentSchema.Version,
			Hash:    hex.EncodeToString(manifest.parentSchema.Hash[:]),
		}
	}
	for _, table := range manifest.tables {
		tableJSON := manifestTableJSON{
			TableID: table.TableID, RelationID: table.RelationID, Name: table.Name,
			Composition: table.Composition, PrimaryKeyFieldID: table.PrimaryKeyFieldID,
			Lifecycle: manifestLifecycleJSON{
				CreatedAtFieldID: table.Lifecycle.CreatedAtFieldID,
				UpdatedAtFieldID: table.Lifecycle.UpdatedAtFieldID,
				DeletedAtFieldID: table.Lifecycle.DeletedAtFieldID,
			},
			Fields:  make([]manifestFieldJSON, 0, len(table.Fields)),
			Indexes: make([]manifestIndexJSON, 0, len(table.Indexes)),
		}
		for _, field := range table.Fields {
			tableJSON.Fields = append(tableJSON.Fields, manifestFieldJSON{
				FieldID: field.FieldID, Name: field.Name, Type: field.Spec.Type,
				Nullable: field.Spec.Nullable, Writable: field.Writable,
				Precision: field.Spec.Precision, Scale: field.Spec.Scale,
			})
		}
		for _, index := range table.Indexes {
			tableJSON.Indexes = append(tableJSON.Indexes, manifestIndexJSON{
				IndexID: index.IndexID, Name: index.Name,
				FieldIDs: append([]string(nil), index.FieldIDs...), Unique: index.Unique,
			})
		}
		body.Tables = append(body.Tables, tableJSON)
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest body: %w", err)
	}
	canonical, err := canonicalizeJCS(encoded)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

// ManifestPreimage returns the exact schema-manifest hash input.
func ManifestPreimage(manifest Manifest) ([]byte, error) {
	if len(manifest.canonicalBody) == 0 {
		return nil, errors.New("manifest is not parsed and canonicalized")
	}
	preimage := append([]byte(nil), schemaManifestDomain...)
	preimage = append(preimage, manifest.canonicalBody...)
	return preimage, nil
}

// RowIdentity derives the canonical identity from the exact manifest PK.
func RowIdentity(manifest Manifest, tableID string, pk json.RawMessage) ([]byte, error) {
	table, err := manifestTableByID(manifest, tableID)
	if err != nil {
		return nil, err
	}
	primary, err := manifestFieldByID(table, table.PrimaryKeyFieldID)
	if err != nil {
		return nil, err
	}
	encodedPK, err := EncodeTypedValue(primary.Spec, pk)
	if err != nil {
		return nil, fmt.Errorf("encode primary key: %w", err)
	}
	if len(encodedPK) < 2 || encodedPK[1] != 0x01 {
		return nil, errors.New("primary key is null")
	}
	identity := append([]byte(nil), rowIdentityDomain...)
	identity, err = appendText(identity, table.TableID)
	if err != nil {
		return nil, err
	}
	identity, err = appendText(identity, table.PrimaryKeyFieldID)
	if err != nil {
		return nil, err
	}
	identity = append(identity, encodedPK...)
	return identity, nil
}

func manifestTableByID(manifest Manifest, tableID string) (manifestTable, error) {
	for _, table := range manifest.tables {
		if table.TableID == tableID {
			return table, nil
		}
	}
	return manifestTable{}, fmt.Errorf("unknown table_id %q", tableID)
}

func manifestFieldByID(table manifestTable, fieldID string) (manifestField, error) {
	for _, field := range table.Fields {
		if field.FieldID == fieldID {
			return field, nil
		}
	}
	return manifestField{}, fmt.Errorf("unknown field_id %q", fieldID)
}

func decodeLowerSHA256(value string) ([32]byte, error) {
	var digest [32]byte
	if len(value) != hex.EncodedLen(len(digest)) {
		return digest, errors.New("digest does not have 64 characters")
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return digest, errors.New("digest is not lowercase hexadecimal")
		}
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return digest, err
	}
	copy(digest[:], decoded)
	return digest, nil
}
