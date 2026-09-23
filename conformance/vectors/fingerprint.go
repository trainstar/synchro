package vectors

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"
)

const (
	maxMutationColumns    = 256
	maxBatchMutations     = 1000
	maxNormalizedMutation = 65536
	maxBatchRequestBytes  = 1048576
)

var (
	batchFingerprintDomain    = []byte("synchro:v3:push-batch-fingerprint:v1\x00")
	mutationFingerprintDomain = []byte("synchro:v3:push-mutation-fingerprint:v1\x00")
	canonicalUUID             = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

// ParseNormalizedMutation strictly parses one mutation with trusted identity.
func ParseNormalizedMutation(authenticatedUserID, clientID string, raw json.RawMessage) (NormalizedMutation, error) {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true})
	if err != nil {
		return NormalizedMutation{}, fmt.Errorf("decode mutation: %w", err)
	}
	if err := requireObjectKeys(object, []string{
		"mutation_id", "table_id", "pk", "authored_schema", "operation", "client_version",
	}, []string{"base_version", "columns"}); err != nil {
		return NormalizedMutation{}, err
	}
	mutationID, err := decodeRequiredString(object["mutation_id"], "mutation_id")
	if err != nil {
		return NormalizedMutation{}, err
	}
	tableID, err := decodeRequiredString(object["table_id"], "table_id")
	if err != nil {
		return NormalizedMutation{}, err
	}
	pk, err := parseMutationColumn(object["pk"], "pk")
	if err != nil {
		return NormalizedMutation{}, err
	}
	if bytes.Equal(bytes.TrimSpace(pk.Value), []byte("null")) {
		return NormalizedMutation{}, errors.New("mutation primary key is null")
	}
	authoredSchema, err := parseSchemaReference(object["authored_schema"], "authored_schema")
	if err != nil {
		return NormalizedMutation{}, err
	}
	operation, err := decodeRequiredString(object["operation"], "operation")
	if err != nil {
		return NormalizedMutation{}, err
	}
	clientVersion, err := decodeRequiredString(object["client_version"], "client_version")
	if err != nil {
		return NormalizedMutation{}, err
	}

	var baseVersion *string
	if rawBase, present := object["base_version"]; present {
		base, err := decodeRequiredString(rawBase, "base_version")
		if err != nil {
			return NormalizedMutation{}, err
		}
		baseVersion = &base
	}
	var columns *[]MutationColumn
	if rawColumns, present := object["columns"]; present {
		values, err := decodeJSONArray(rawColumns, jsonValidation{iJSON: true})
		if err != nil {
			return NormalizedMutation{}, fmt.Errorf("decode columns: %w", err)
		}
		parsed := make([]MutationColumn, 0, len(values))
		for _, value := range values {
			column, err := parseMutationColumn(value, "column")
			if err != nil {
				return NormalizedMutation{}, err
			}
			parsed = append(parsed, column)
		}
		columns = &parsed
	}
	mutation := NormalizedMutation{
		AuthenticatedUserID: authenticatedUserID,
		ClientID:            clientID,
		MutationID:          mutationID,
		TableID:             tableID,
		PK:                  pk,
		AuthoredSchema:      authoredSchema,
		Operation:           operation,
		BaseVersion:         baseVersion,
		ClientVersion:       clientVersion,
		Columns:             columns,
	}
	if _, err := canonicalNormalizedMutation(mutation); err != nil {
		return NormalizedMutation{}, err
	}
	return mutation, nil
}

// ParseNormalizedBatch strictly parses one sealed batch with trusted identity.
func ParseNormalizedBatch(authenticatedUserID string, raw json.RawMessage) (NormalizedBatch, error) {
	if len(raw) > maxBatchRequestBytes {
		return NormalizedBatch{}, errors.New("batch request exceeds byte limit")
	}
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true})
	if err != nil {
		return NormalizedBatch{}, fmt.Errorf("decode batch: %w", err)
	}
	if err := requireObjectKeys(object, []string{
		"client_id", "client_generation", "batch_id", "request_schema", "mutations",
	}, nil); err != nil {
		return NormalizedBatch{}, err
	}
	clientID, err := decodeRequiredString(object["client_id"], "client_id")
	if err != nil {
		return NormalizedBatch{}, err
	}
	generation, err := decodePositiveSafeUint(object["client_generation"], "client_generation")
	if err != nil {
		return NormalizedBatch{}, err
	}
	batchID, err := decodeRequiredString(object["batch_id"], "batch_id")
	if err != nil {
		return NormalizedBatch{}, err
	}
	requestSchema, err := parseSchemaReference(object["request_schema"], "request_schema")
	if err != nil {
		return NormalizedBatch{}, err
	}
	mutationValues, err := decodeJSONArray(object["mutations"], jsonValidation{iJSON: true})
	if err != nil {
		return NormalizedBatch{}, fmt.Errorf("decode mutations: %w", err)
	}
	if len(mutationValues) == 0 || len(mutationValues) > maxBatchMutations {
		return NormalizedBatch{}, errors.New("batch mutation count is outside 1..1000")
	}
	mutations := make([]NormalizedMutation, 0, len(mutationValues))
	seen := make(map[string]struct{}, len(mutationValues))
	for _, value := range mutationValues {
		mutation, err := ParseNormalizedMutation(authenticatedUserID, clientID, value)
		if err != nil {
			return NormalizedBatch{}, err
		}
		if _, duplicate := seen[mutation.MutationID]; duplicate {
			return NormalizedBatch{}, fmt.Errorf("duplicate mutation_id %q", mutation.MutationID)
		}
		seen[mutation.MutationID] = struct{}{}
		mutations = append(mutations, mutation)
	}
	batch := NormalizedBatch{
		AuthenticatedUserID: authenticatedUserID,
		ClientID:            clientID,
		ClientGeneration:    generation,
		BatchID:             batchID,
		RequestSchema:       requestSchema,
		Mutations:           mutations,
	}
	if _, err := canonicalNormalizedBatch(batch); err != nil {
		return NormalizedBatch{}, err
	}
	return batch, nil
}

func parseSchemaReference(raw json.RawMessage, name string) (SchemaReference, error) {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true})
	if err != nil {
		return SchemaReference{}, fmt.Errorf("decode %s: %w", name, err)
	}
	if err := requireObjectKeys(object, []string{"version", "hash"}, nil); err != nil {
		return SchemaReference{}, fmt.Errorf("%s: %w", name, err)
	}
	version, err := decodePositiveSafeUint(object["version"], name+".version")
	if err != nil {
		return SchemaReference{}, err
	}
	hashText, err := decodeRequiredString(object["hash"], name+".hash")
	if err != nil {
		return SchemaReference{}, err
	}
	hash, err := decodeLowerSHA256(hashText)
	if err != nil {
		return SchemaReference{}, fmt.Errorf("%s.hash: %w", name, err)
	}
	return SchemaReference{Version: version, Hash: hash}, nil
}

func parseMutationColumn(raw json.RawMessage, name string) (MutationColumn, error) {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true})
	if err != nil {
		return MutationColumn{}, fmt.Errorf("decode %s: %w", name, err)
	}
	if err := requireObjectKeys(object, []string{"field_id", "value"}, nil); err != nil {
		return MutationColumn{}, fmt.Errorf("%s: %w", name, err)
	}
	fieldID, err := decodeRequiredString(object["field_id"], name+".field_id")
	if err != nil {
		return MutationColumn{}, err
	}
	value := bytes.TrimSpace(object["value"])
	if err := validateJSONDocument(value, jsonValidation{iJSON: true}); err != nil {
		return MutationColumn{}, fmt.Errorf("%s.value: %w", name, err)
	}
	return MutationColumn{FieldID: fieldID, Value: append(json.RawMessage(nil), value...)}, nil
}

// BatchFingerprint computes the canonical batch fingerprint.
func BatchFingerprint(batch NormalizedBatch) ([32]byte, error) {
	preimage, err := BatchFingerprintPreimage(batch)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(preimage), nil
}

// BatchFingerprintPreimage returns the exact batch-fingerprint hash input.
func BatchFingerprintPreimage(batch NormalizedBatch) ([]byte, error) {
	canonical, err := canonicalNormalizedBatch(batch)
	if err != nil {
		return nil, err
	}
	preimage := append([]byte(nil), batchFingerprintDomain...)
	preimage = append(preimage, canonical...)
	return preimage, nil
}

// MutationFingerprint computes the canonical scoped mutation fingerprint.
func MutationFingerprint(mutation NormalizedMutation) ([32]byte, error) {
	preimage, err := MutationFingerprintPreimage(mutation)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(preimage), nil
}

// MutationFingerprintPreimage returns the exact mutation-fingerprint hash input.
func MutationFingerprintPreimage(mutation NormalizedMutation) ([]byte, error) {
	normalized, err := canonicalNormalizedMutation(mutation)
	if err != nil {
		return nil, err
	}
	if err := validateIdentityString(mutation.AuthenticatedUserID, "authenticated_user_id"); err != nil {
		return nil, err
	}
	if err := validateIdentityString(mutation.ClientID, "client_id"); err != nil {
		return nil, err
	}
	scope := []any{
		"mutation-scope-v1",
		mutation.AuthenticatedUserID,
		mutation.ClientID,
		json.RawMessage(normalized),
	}
	encoded, err := json.Marshal(scope)
	if err != nil {
		return nil, fmt.Errorf("marshal normalized mutation scope: %w", err)
	}
	canonical, err := canonicalizeJCS(encoded)
	if err != nil {
		return nil, err
	}
	preimage := append([]byte(nil), mutationFingerprintDomain...)
	preimage = append(preimage, canonical...)
	return preimage, nil
}

func canonicalNormalizedMutation(mutation NormalizedMutation) ([]byte, error) {
	value, err := normalizedMutationValue(mutation)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal normalized mutation: %w", err)
	}
	canonical, err := canonicalizeJCS(encoded)
	if err != nil {
		return nil, err
	}
	if len(canonical) > maxNormalizedMutation {
		return nil, errors.New("normalized mutation exceeds byte limit")
	}
	return canonical, nil
}

func normalizedMutationValue(mutation NormalizedMutation) ([]any, error) {
	if err := validateIdentityString(mutation.AuthenticatedUserID, "authenticated_user_id"); err != nil {
		return nil, err
	}
	if err := validateIdentityString(mutation.ClientID, "client_id"); err != nil {
		return nil, err
	}
	if err := validateUUID(mutation.MutationID, "mutation_id"); err != nil {
		return nil, err
	}
	if err := validateIdentityString(mutation.TableID, "table_id"); err != nil {
		return nil, err
	}
	if err := validateIdentityString(mutation.PK.FieldID, "pk.field_id"); err != nil {
		return nil, err
	}
	pkValue := bytes.TrimSpace(mutation.PK.Value)
	if len(pkValue) == 0 || bytes.Equal(pkValue, []byte("null")) {
		return nil, errors.New("mutation primary key is missing or null")
	}
	if err := validateJSONDocument(pkValue, jsonValidation{iJSON: true}); err != nil {
		return nil, fmt.Errorf("pk.value: %w", err)
	}
	if err := validateSchemaReference(mutation.AuthoredSchema, "authored_schema"); err != nil {
		return nil, err
	}
	if !canonicalDateTime.MatchString(mutation.ClientVersion) {
		return nil, errors.New("client_version is not canonical UTC microsecond time")
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000000Z", mutation.ClientVersion); err != nil {
		return nil, errors.New("client_version is invalid")
	}

	var base []any
	switch mutation.Operation {
	case "insert":
		if mutation.BaseVersion != nil || mutation.Columns == nil || len(*mutation.Columns) == 0 {
			return nil, errors.New("insert has an invalid base_version or columns shape")
		}
		base = []any{0}
	case "update":
		if mutation.BaseVersion == nil || *mutation.BaseVersion == "" || mutation.Columns == nil || len(*mutation.Columns) == 0 {
			return nil, errors.New("update has an invalid base_version or columns shape")
		}
		base = []any{1, *mutation.BaseVersion}
	case "delete":
		if mutation.BaseVersion == nil || *mutation.BaseVersion == "" || mutation.Columns != nil {
			return nil, errors.New("delete has an invalid base_version or columns shape")
		}
		base = []any{1, *mutation.BaseVersion}
	default:
		return nil, fmt.Errorf("unsupported mutation operation %q", mutation.Operation)
	}
	if mutation.BaseVersion != nil {
		if err := validateJSONStringValue(*mutation.BaseVersion, true); err != nil {
			return nil, fmt.Errorf("base_version: %w", err)
		}
	}

	columnsValue := []any{0}
	if mutation.Columns != nil {
		if len(*mutation.Columns) > maxMutationColumns {
			return nil, errors.New("mutation has more than 256 columns")
		}
		columns := append([]MutationColumn(nil), (*mutation.Columns)...)
		sort.Slice(columns, func(left, right int) bool {
			return columns[left].FieldID < columns[right].FieldID
		})
		pairs := make([]any, 0, len(columns))
		for index, column := range columns {
			if err := validateIdentityString(column.FieldID, "column.field_id"); err != nil {
				return nil, err
			}
			if index > 0 && columns[index-1].FieldID == column.FieldID {
				return nil, fmt.Errorf("duplicate column field_id %q", column.FieldID)
			}
			value := bytes.TrimSpace(column.Value)
			if err := validateJSONDocument(value, jsonValidation{iJSON: true}); err != nil {
				return nil, fmt.Errorf("column %q value: %w", column.FieldID, err)
			}
			pairs = append(pairs, []any{column.FieldID, json.RawMessage(value)})
		}
		columnsValue = []any{1, pairs}
	}
	return []any{
		"mutation-v1",
		mutation.MutationID,
		mutation.TableID,
		[]any{mutation.PK.FieldID, json.RawMessage(pkValue)},
		schemaReferenceValue(mutation.AuthoredSchema),
		mutation.Operation,
		base,
		mutation.ClientVersion,
		columnsValue,
	}, nil
}

func canonicalNormalizedBatch(batch NormalizedBatch) ([]byte, error) {
	if err := validateIdentityString(batch.AuthenticatedUserID, "authenticated_user_id"); err != nil {
		return nil, err
	}
	if err := validateIdentityString(batch.ClientID, "client_id"); err != nil {
		return nil, err
	}
	if batch.ClientGeneration == 0 || batch.ClientGeneration > maxSafeJSONInteger {
		return nil, errors.New("client_generation is outside the positive portable range")
	}
	if err := validateUUID(batch.BatchID, "batch_id"); err != nil {
		return nil, err
	}
	if err := validateSchemaReference(batch.RequestSchema, "request_schema"); err != nil {
		return nil, err
	}
	if len(batch.Mutations) == 0 || len(batch.Mutations) > maxBatchMutations {
		return nil, errors.New("batch mutation count is outside 1..1000")
	}
	seen := make(map[string]struct{}, len(batch.Mutations))
	normalizedMutations := make([]any, 0, len(batch.Mutations))
	for _, mutation := range batch.Mutations {
		if mutation.AuthenticatedUserID != batch.AuthenticatedUserID || mutation.ClientID != batch.ClientID {
			return nil, errors.New("mutation identity scope does not match batch")
		}
		if _, duplicate := seen[mutation.MutationID]; duplicate {
			return nil, fmt.Errorf("duplicate mutation_id %q", mutation.MutationID)
		}
		seen[mutation.MutationID] = struct{}{}
		normalized, err := canonicalNormalizedMutation(mutation)
		if err != nil {
			return nil, err
		}
		normalizedMutations = append(normalizedMutations, json.RawMessage(normalized))
	}
	value := []any{
		"batch-v1",
		batch.AuthenticatedUserID,
		batch.ClientID,
		strconv.FormatUint(batch.ClientGeneration, 10),
		batch.BatchID,
		schemaReferenceValue(batch.RequestSchema),
		normalizedMutations,
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal normalized batch: %w", err)
	}
	return canonicalizeJCS(encoded)
}

func validateSchemaReference(reference SchemaReference, name string) error {
	if reference.Version == 0 || reference.Version > maxSafeJSONInteger {
		return fmt.Errorf("%s version is outside the positive portable range", name)
	}
	return nil
}

func schemaReferenceValue(reference SchemaReference) []any {
	return []any{strconv.FormatUint(reference.Version, 10), hex.EncodeToString(reference.Hash[:])}
}

func validateIdentityString(value, name string) error {
	if value == "" {
		return fmt.Errorf("%s is empty", name)
	}
	if err := validateJSONStringValue(value, true); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func validateUUID(value, name string) error {
	if !canonicalUUID.MatchString(value) || value == "00000000-0000-0000-0000-000000000000" {
		return fmt.Errorf("%s is not a canonical non-nil UUID", name)
	}
	return nil
}
