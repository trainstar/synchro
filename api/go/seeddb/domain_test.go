package seeddb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The shared typed vectors are the contract that the Rust digest encoder also
// consumes. Each one passes through the production seed JSON decoder first.
func TestEncodeTypedValueMatchesSharedTypedVectors(t *testing.T) {
	raw, err := os.ReadFile("../../../conformance/vectors/canonical-v1.json")
	if err != nil {
		t.Fatalf("read shared vectors: %v", err)
	}
	var file struct {
		Vectors []struct {
			ID    string `json:"vector_id"`
			Kind  string `json:"kind"`
			Valid bool   `json:"valid"`
			Input struct {
				FieldSpec struct {
					Type      string `json:"type"`
					Nullable  bool   `json:"nullable"`
					Precision *int   `json:"precision"`
					Scale     *int   `json:"scale"`
				} `json:"field_spec"`
				RawJSON string `json:"raw_json"`
			} `json:"input"`
			Expected struct {
				CanonicalBytesHex *string `json:"canonical_bytes_hex"`
			} `json:"expected"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode shared vectors: %v", err)
	}
	consumed := map[bool]int{}
	for _, vector := range file.Vectors {
		if vector.Kind != "typed_value" {
			continue
		}
		consumed[vector.Valid]++
		t.Run(vector.ID, func(t *testing.T) {
			spec := vector.Input.FieldSpec
			column := localSchemaColumn{LogicalType: spec.Type, Nullable: spec.Nullable, Precision: spec.Precision, Scale: spec.Scale}
			var value any
			decodeErr := decodeJSON([]byte(vector.Input.RawJSON), &value)
			var encoded []byte
			encodeErr := decodeErr
			if decodeErr == nil {
				encoded, encodeErr = encodeTypedValue(column, value)
			}
			if !vector.Valid {
				if encodeErr == nil {
					t.Fatalf("accepted invalid typed value %s as %x", vector.Input.RawJSON, encoded)
				}
				return
			}
			if encodeErr != nil || vector.Expected.CanonicalBytesHex == nil {
				t.Fatalf("rejected valid typed value %s: %v", vector.Input.RawJSON, encodeErr)
			}
			if got := hex.EncodeToString(encoded); got != *vector.Expected.CanonicalBytesHex {
				t.Fatalf("typed value %s encoded %s, want %s", vector.Input.RawJSON, got, *vector.Expected.CanonicalBytesHex)
			}
		})
	}
	if consumed[true] == 0 || consumed[false] == 0 {
		t.Fatalf("typed vector family is incomplete: valid=%d invalid=%d", consumed[true], consumed[false])
	}
}

// A seed row whose digest matches its bytes must still fail when a value is
// outside the declared field domain.
func TestVerifyPortableSeedRecordRejectsDomainViolationsWithMatchingDigest(t *testing.T) {
	precision, scale := 6, 2
	table := localSchemaTable{
		TableID: "tbl_domain",
		Columns: []localSchemaColumn{
			{FieldID: "fld_id", LogicalType: "string", IsPrimaryKey: true},
			{FieldID: "fld_amount", LogicalType: "decimal", Precision: &precision, Scale: &scale},
			{FieldID: "fld_payload", LogicalType: "json", Nullable: true},
		},
	}
	env := manifestEnvelope{SchemaHash: strings.Repeat("0", 64)}
	nested := strings.Repeat("[", maxJSONDepth+1) + strings.Repeat("]", maxJSONDepth+1)
	tests := []struct {
		name    string
		amount  string
		payload string
		valid   bool
	}{
		{name: "declared domain", amount: "1234.56", payload: "{\"a\":[1,\"\ufffd\"]}", valid: true},
		{name: "decimal precision", amount: "12345.67", payload: `{}`},
		{name: "decimal scale", amount: "1.234", payload: `{}`},
		{name: "JSON noncharacter value", amount: "1", payload: "[\"\ufdd0\"]"},
		{name: "JSON noncharacter member name", amount: "1", payload: "{\"\U0001fffe\":1}"},
		{name: "JSON nesting depth", amount: "1", payload: nested},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := portableSeedRecord{
				PK:            map[string]any{"fld_id": "row-1"},
				Row:           map[string]any{"fld_id": "row-1", "fld_amount": test.amount, "fld_payload": test.payload},
				ServerVersion: "sv-1",
			}
			record.RowChecksum = checksumObject{Algorithm: "sha256", Version: 1, Encoding: "hex", Digest: lexicalSeedRowDigest(env.SchemaHash, table.TableID, "row-1", test.amount, test.payload, record.ServerVersion)}
			_, _, err := verifyPortableSeedRecord(env, table, record)
			if test.valid {
				if err != nil {
					t.Fatalf("rejected a row inside its declared domain: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted a digest-matching row outside its declared domain")
			}
			if strings.Contains(err.Error(), "digest") {
				t.Fatalf("rejected the row for its digest, not its domain: %v", err)
			}
		})
	}
}

// lexicalSeedRowDigest computes the row digest from the lexical text of each
// value without domain checks, so the digest matches even for invalid values.
func lexicalSeedRowDigest(schemaHash, tableID, id, amount, payload, serverVersion string) string {
	typed := func(tag byte, text string) []byte {
		return appendText([]byte{tag, 1}, text)
	}
	fields := []struct {
		id    string
		value []byte
	}{
		{id: "fld_amount", value: typed(0x04, amount)},
		{id: "fld_id", value: typed(0x01, id)},
		{id: "fld_payload", value: typed(0x0a, payload)},
	}
	body := appendU32(nil, uint32(len(fields)))
	for _, field := range fields {
		body = appendText(body, field.id)
		body = append(body, field.value...)
	}
	identity := append([]byte(nil), rowIdentityDomain...)
	identity = appendText(identity, tableID)
	identity = appendText(identity, "fld_id")
	identity = append(identity, typed(0x01, id)...)
	schema, _ := hex.DecodeString(schemaHash)
	hasher := sha256.New()
	_, _ = hasher.Write(rowDigestDomain)
	_, _ = hasher.Write(schema)
	writeBlob(hasher, identity)
	writeBlob(hasher, body)
	writeText(hasher, serverVersion)
	return hex.EncodeToString(hasher.Sum(nil))
}

func TestCanonicalJSONEnforcesIJSONLimits(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		valid bool
	}{
		{name: "maximum array depth", raw: strings.Repeat("[", maxJSONDepth) + strings.Repeat("]", maxJSONDepth), valid: true},
		{name: "array depth above maximum", raw: strings.Repeat("[", maxJSONDepth+1) + strings.Repeat("]", maxJSONDepth+1)},
		{name: "maximum object depth", raw: strings.Repeat(`{"a":`, maxJSONDepth-1) + "{}" + strings.Repeat("}", maxJSONDepth-1), valid: true},
		{name: "object depth above maximum", raw: strings.Repeat(`{"a":`, maxJSONDepth) + "{}" + strings.Repeat("}", maxJSONDepth)},
		{name: "maximum value count", raw: "[" + strings.TrimSuffix(strings.Repeat("0,", maxJSONValuesAndNames-1), ",") + "]", valid: true},
		{name: "value count above maximum", raw: "[" + strings.TrimSuffix(strings.Repeat("0,", maxJSONValuesAndNames), ",") + "]"},
		{name: "member names count toward the limit", raw: "[" + strings.TrimSuffix(strings.Repeat("0,", maxJSONValuesAndNames-3), ",") + `,{"a":0}]`},
		{name: "replacement character", raw: "\"\ufffd\"", valid: true},
		{name: "noncharacter range", raw: "\"\ufdef\""},
		{name: "plane noncharacter", raw: "\"\U0010ffff\""},
		{name: "escaped noncharacter", raw: `"\ufffe"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := canonicalizeRFC8785JSON([]byte(test.raw))
			if test.valid && err != nil {
				t.Fatalf("rejected valid I-JSON: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("accepted JSON outside the I-JSON limits")
			}
		})
	}
}

func TestDecodeJSONRejectsUnpairedSurrogateEscapes(t *testing.T) {
	for _, raw := range []string{`"\ud800"`, `"\udc00"`, `"\ud800\u0041"`, `"\ud800x"`} {
		var value any
		if err := decodeJSON([]byte(raw), &value); err == nil {
			t.Fatalf("decoded unpaired surrogate escape %s as %q", raw, value)
		}
	}
	for raw, want := range map[string]string{`"\ud83d\ude00"`: "\U0001f600", `"\\ud800"`: `\ud800`, `"\u0041"`: "A"} {
		var value any
		if err := decodeJSON([]byte(raw), &value); err != nil || value != want {
			t.Fatalf("decode %s = %q, %v, want %q", raw, value, err, want)
		}
	}
}
