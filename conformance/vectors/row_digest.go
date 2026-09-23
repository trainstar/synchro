package vectors

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"
)

var rowDigestDomain = []byte("synchro:v3:row-digest:v1\x00")

// RowDigest computes the canonical row digest.
func RowDigest(manifest Manifest, tableID string, row Row, serverVersion string) ([32]byte, error) {
	preimage, err := RowDigestPreimage(manifest, tableID, row, serverVersion)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(preimage), nil
}

// RowDigestPreimage returns the exact row-digest hash input.
func RowDigestPreimage(manifest Manifest, tableID string, row Row, serverVersion string) ([]byte, error) {
	if serverVersion == "" {
		return nil, errors.New("server_version is empty")
	}
	if err := validateJSONStringValue(serverVersion, false); err != nil {
		return nil, fmt.Errorf("server_version: %w", err)
	}
	table, err := manifestTableByID(manifest, tableID)
	if err != nil {
		return nil, err
	}
	if len(row.Fields) != len(table.Fields) {
		return nil, errors.New("row field count does not match manifest")
	}
	if len(row.Fields) > math.MaxUint32 {
		return nil, errors.New("row field count exceeds u32")
	}

	type encodedField struct {
		fieldID string
		value   []byte
	}
	encodedFields := make([]encodedField, 0, len(row.Fields))
	seen := make(map[string]struct{}, len(row.Fields))
	var encodedRowPK []byte
	for _, rowField := range row.Fields {
		if _, duplicate := seen[rowField.FieldID]; duplicate {
			return nil, fmt.Errorf("duplicate row field_id %q", rowField.FieldID)
		}
		seen[rowField.FieldID] = struct{}{}
		field, err := manifestFieldByID(table, rowField.FieldID)
		if err != nil {
			return nil, err
		}
		encoded, err := EncodeTypedValue(field.Spec, rowField.Value)
		if err != nil {
			return nil, fmt.Errorf("encode row field %q: %w", rowField.FieldID, err)
		}
		if rowField.FieldID == table.PrimaryKeyFieldID {
			encodedRowPK = append([]byte(nil), encoded...)
		}
		encodedFields = append(encodedFields, encodedField{fieldID: rowField.FieldID, value: encoded})
	}
	for _, field := range table.Fields {
		if _, ok := seen[field.FieldID]; !ok {
			return nil, fmt.Errorf("row omits field_id %q", field.FieldID)
		}
	}
	primary, err := manifestFieldByID(table, table.PrimaryKeyFieldID)
	if err != nil {
		return nil, err
	}
	separatePK, err := EncodeTypedValue(primary.Spec, row.PK)
	if err != nil {
		return nil, fmt.Errorf("encode separate primary key: %w", err)
	}
	if !bytes.Equal(separatePK, encodedRowPK) {
		return nil, errors.New("row primary key does not match separate primary key")
	}

	sort.Slice(encodedFields, func(left, right int) bool {
		return encodedFields[left].fieldID < encodedFields[right].fieldID
	})
	body := appendU32(nil, uint32(len(encodedFields)))
	for _, field := range encodedFields {
		body, err = appendText(body, field.fieldID)
		if err != nil {
			return nil, err
		}
		body = append(body, field.value...)
	}
	identity, err := RowIdentity(manifest, tableID, row.PK)
	if err != nil {
		return nil, err
	}
	preimage := append([]byte(nil), rowDigestDomain...)
	preimage = append(preimage, manifest.schemaHash[:]...)
	preimage = appendBlob(preimage, identity)
	preimage = appendBlob(preimage, body)
	preimage, err = appendText(preimage, serverVersion)
	if err != nil {
		return nil, err
	}
	return preimage, nil
}
