// Package vectors loads and evaluates independent protocol version 3 vectors.
package vectors

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
	"github.com/trainstar/synchro/conformance/internal/contract"
)

const (
	maxSafeJSONInteger    uint64 = 9007199254740991
	maxJSONNestingDepth          = 128
	maxJSONValuesAndNames        = 1_000_000
)

// FieldSpec declares one portable field type.
type FieldSpec struct {
	Type      string `json:"type"`
	Nullable  bool   `json:"nullable"`
	Precision *int   `json:"precision,omitempty"`
	Scale     *int   `json:"scale,omitempty"`
}

// Row retains its separate primary key and complete field list.
type Row struct {
	PK     json.RawMessage
	Fields []RowField
}

// RowField is one authored field value.
type RowField struct {
	FieldID string
	Value   json.RawMessage
}

// DigestEntry pairs one complete canonical row identity with its row digest.
type DigestEntry struct {
	RowIdentity []byte
	RowDigest   [32]byte
}

// SchemaReference identifies one immutable schema manifest.
type SchemaReference struct {
	Version uint64
	Hash    [32]byte
}

// MutationColumn retains one authored JSON value.
type MutationColumn struct {
	FieldID string
	Value   json.RawMessage
}

// NormalizedMutation contains the trusted scope and authored mutation input.
type NormalizedMutation struct {
	AuthenticatedUserID string
	ClientID            string
	MutationID          string
	TableID             string
	PK                  MutationColumn
	AuthoredSchema      SchemaReference
	Operation           string
	BaseVersion         *string
	ClientVersion       string
	Columns             *[]MutationColumn
}

// NormalizedBatch contains the trusted identity and sealed request input.
type NormalizedBatch struct {
	AuthenticatedUserID string
	ClientID            string
	ClientGeneration    uint64
	BatchID             string
	RequestSchema       SchemaReference
	Mutations           []NormalizedMutation
}

// Manifest is one validated immutable schema manifest.
type Manifest struct {
	schemaVersion      uint64
	schemaHash         [32]byte
	parentSchema       *SchemaReference
	transitionClass    string
	compatibilityFloor uint64
	tables             []manifestTable
	canonicalBody      []byte
}

type manifestTable struct {
	TableID           string
	RelationID        string
	Name              string
	Composition       string
	PrimaryKeyFieldID string
	Lifecycle         manifestLifecycle
	Fields            []manifestField
	Indexes           []manifestIndex
}

type manifestLifecycle struct {
	CreatedAtFieldID *string
	UpdatedAtFieldID *string
	DeletedAtFieldID *string
}

type manifestField struct {
	FieldID  string
	Name     string
	Spec     FieldSpec
	Writable bool
}

type manifestIndex struct {
	IndexID  string
	Name     string
	FieldIDs []string
	Unique   bool
}

// Hash returns the verified raw schema hash.
func (m Manifest) Hash() [32]byte {
	return m.schemaHash
}

// CanonicalBody returns a copy of the RFC 8785 manifest body.
func (m Manifest) CanonicalBody() []byte {
	return append([]byte(nil), m.canonicalBody...)
}

// Vector is one authored expected-value case.
type Vector struct {
	ID       string
	Kind     string
	Valid    bool
	Input    json.RawMessage
	Expected Expected
}

// Expected contains authored canonical bytes and digests.
type Expected struct {
	CanonicalBytesHex   *string `json:"canonical_bytes_hex"`
	ExpectedBytesSHA256 *string `json:"expected_bytes_sha256"`
	ExpectedSHA256      *string `json:"expected_sha256"`
}

// VectorSet is one validated vector source.
type VectorSet struct {
	ID              contract.VectorSetID
	Path            string
	SourceSHA256    string
	AggregateSHA256 string
	Vectors         []Vector
	sourceBytes     []byte
}

// Catalog is one validated vector catalog and its bound sources.
type Catalog struct {
	sets map[contract.VectorSetID]VectorSet
}

// Has reports whether the catalog contains id.
func (c Catalog) Has(id contract.VectorSetID) bool {
	_, ok := c.sets[id]
	return ok
}

// IDs returns the vector-set IDs in deterministic order.
func (c Catalog) IDs() []contract.VectorSetID {
	ids := make([]contract.VectorSetID, 0, len(c.sets))
	for id := range c.sets {
		ids = append(ids, id)
	}
	sortVectorSetIDs(ids)
	return ids
}

// Set returns a defensive copy of one vector set.
func (c Catalog) Set(id contract.VectorSetID) (VectorSet, bool) {
	set, ok := c.sets[id]
	if !ok {
		return VectorSet{}, false
	}
	vectors := make([]Vector, len(set.Vectors))
	for index, vector := range set.Vectors {
		vector.Input = append(json.RawMessage(nil), vector.Input...)
		vector.Expected.CanonicalBytesHex = copyStringPointer(vector.Expected.CanonicalBytesHex)
		vector.Expected.ExpectedBytesSHA256 = copyStringPointer(vector.Expected.ExpectedBytesSHA256)
		vector.Expected.ExpectedSHA256 = copyStringPointer(vector.Expected.ExpectedSHA256)
		vectors[index] = vector
	}
	set.Vectors = vectors
	set.sourceBytes = append([]byte(nil), set.sourceBytes...)
	return set, true
}

func copyStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func sortVectorSetIDs(ids []contract.VectorSetID) {
	for index := 1; index < len(ids); index++ {
		for cursor := index; cursor > 0 && ids[cursor] < ids[cursor-1]; cursor-- {
			ids[cursor], ids[cursor-1] = ids[cursor-1], ids[cursor]
		}
	}
}

type jsonValidation struct {
	iJSON       bool
	safeInteger bool
}

type jsonInspection struct {
	valuesAndNames int
}

func (inspection *jsonInspection) consume() error {
	inspection.valuesAndNames++
	if inspection.valuesAndNames > maxJSONValuesAndNames {
		return fmt.Errorf("JSON exceeds the value and member name limit of %d", maxJSONValuesAndNames)
	}
	return nil
}

func validateJSONDocument(data []byte, mode jsonValidation) error {
	if !utf8.Valid(data) {
		return errors.New("JSON contains invalid UTF-8")
	}
	if err := validateUnicodeEscapes(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	inspection := jsonInspection{}
	if err := inspectJSONValue(decoder, mode, &inspection, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON document contains more than one value")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return nil
}

func inspectJSONValue(decoder *json.Decoder, mode jsonValidation, inspection *jsonInspection, depth int) error {
	if err := inspection.consume(); err != nil {
		return err
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON value: %w", err)
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			if depth >= maxJSONNestingDepth {
				return fmt.Errorf("JSON exceeds the nesting limit of %d", maxJSONNestingDepth)
			}
			return inspectJSONObject(decoder, mode, inspection, depth+1)
		case '[':
			if depth >= maxJSONNestingDepth {
				return fmt.Errorf("JSON exceeds the nesting limit of %d", maxJSONNestingDepth)
			}
			return inspectJSONArray(decoder, mode, inspection, depth+1)
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", value)
		}
	case string:
		return validateJSONStringValue(value, mode.iJSON)
	case json.Number:
		if !mode.iJSON {
			return nil
		}
		return validateIJSONNumber(value.String(), mode.safeInteger)
	case bool, nil:
		return nil
	default:
		return fmt.Errorf("unsupported JSON token %T", token)
	}
}

func inspectJSONObject(decoder *json.Decoder, mode jsonValidation, inspection *jsonInspection, depth int) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON object name: %w", err)
		}
		name, ok := token.(string)
		if !ok {
			return errors.New("JSON object member name is not a string")
		}
		if err := inspection.consume(); err != nil {
			return err
		}
		if err := validateJSONStringValue(name, mode.iJSON); err != nil {
			return err
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate JSON object member %q", name)
		}
		seen[name] = struct{}{}
		if err := inspectJSONValue(decoder, mode, inspection, depth); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("close JSON object: %w", err)
	}
	if closing != json.Delim('}') {
		return errors.New("JSON object has an invalid closing delimiter")
	}
	return nil
}

func inspectJSONArray(decoder *json.Decoder, mode jsonValidation, inspection *jsonInspection, depth int) error {
	for decoder.More() {
		if err := inspectJSONValue(decoder, mode, inspection, depth); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("close JSON array: %w", err)
	}
	if closing != json.Delim(']') {
		return errors.New("JSON array has an invalid closing delimiter")
	}
	return nil
}

func validateJSONStringValue(value string, iJSON bool) error {
	if !utf8.ValidString(value) {
		return errors.New("JSON string contains invalid UTF-8")
	}
	if !iJSON {
		return nil
	}
	for _, character := range value {
		if isUnicodeNoncharacter(character) {
			return fmt.Errorf("I-JSON string contains Unicode noncharacter U+%04X", character)
		}
	}
	return nil
}

func isUnicodeNoncharacter(character rune) bool {
	return character >= 0xfdd0 && character <= 0xfdef || character&0xffff == 0xfffe || character&0xffff == 0xffff
}

func validateIJSONNumber(text string, safeInteger bool) error {
	value, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
		return fmt.Errorf("JSON number is outside finite binary64: %q", text)
	}
	if safeInteger && jsonNumberIsUnsafeInteger(text) {
		return fmt.Errorf("JSON integer is outside the safe range: %q", text)
	}
	return nil
}

func jsonNumberIsUnsafeInteger(text string) bool {
	unsigned := strings.TrimPrefix(text, "-")
	exponent := 0
	if position := strings.IndexAny(unsigned, "eE"); position >= 0 {
		exponentText := unsigned[position+1:]
		unsigned = unsigned[:position]
		if len(exponentText) > 7 {
			return true
		}
		parsed, err := strconv.Atoi(exponentText)
		if err != nil {
			return true
		}
		exponent = parsed
	}
	fractionDigits := 0
	if point := strings.IndexByte(unsigned, '.'); point >= 0 {
		fractionDigits = len(unsigned) - point - 1
		unsigned = unsigned[:point] + unsigned[point+1:]
	}
	unsigned = strings.TrimLeft(unsigned, "0")
	if unsigned == "" {
		return false
	}
	scale := fractionDigits - exponent
	if scale > 0 {
		if scale >= len(unsigned) {
			return false
		}
		fraction := unsigned[len(unsigned)-scale:]
		if strings.Trim(fraction, "0") != "" {
			return false
		}
		unsigned = unsigned[:len(unsigned)-scale]
	} else if scale < 0 {
		if len(unsigned)-scale > 16 {
			return true
		}
		unsigned += strings.Repeat("0", -scale)
	}
	unsigned = strings.TrimLeft(unsigned, "0")
	if len(unsigned) < 16 {
		return false
	}
	if len(unsigned) > 16 {
		return true
	}
	return unsigned > strconv.FormatUint(maxSafeJSONInteger, 10)
}

func canonicalizeJCS(data []byte) ([]byte, error) {
	if err := validateJSONDocument(data, jsonValidation{iJSON: true}); err != nil {
		return nil, err
	}
	canonical, err := jcs.Transform(data)
	if err != nil {
		return nil, fmt.Errorf("canonicalize RFC 8785 JSON: %w", err)
	}
	return canonical, nil
}

func strictJSONObject(data []byte, mode jsonValidation) (map[string]json.RawMessage, error) {
	if err := validateJSONDocument(data, mode); err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil || object == nil {
		return nil, errors.New("JSON value is not an object")
	}
	return object, nil
}

func requireObjectKeys(object map[string]json.RawMessage, required, optional []string) error {
	allowed := make(map[string]struct{}, len(required)+len(optional))
	for _, key := range required {
		allowed[key] = struct{}{}
		if _, ok := object[key]; !ok {
			return fmt.Errorf("JSON object is missing member %q", key)
		}
	}
	for _, key := range optional {
		allowed[key] = struct{}{}
	}
	for key := range object {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("JSON object has unknown member %q", key)
		}
	}
	return nil
}

func decodeRequiredString(raw json.RawMessage, name string) (string, error) {
	value, err := decodeJSONString(bytes.TrimSpace(raw), true)
	if err != nil {
		return "", fmt.Errorf("decode %s: %w", name, err)
	}
	if value == "" {
		return "", fmt.Errorf("%s is empty", name)
	}
	return value, nil
}

func decodeBoolean(raw json.RawMessage, name string) (bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("true")) {
		return true, nil
	}
	if bytes.Equal(trimmed, []byte("false")) {
		return false, nil
	}
	return false, fmt.Errorf("%s is not a JSON boolean", name)
}

func decodePositiveSafeUint(raw json.RawMessage, name string) (uint64, error) {
	trimmed := bytes.TrimSpace(raw)
	if !canonicalInteger.Match(trimmed) || bytes.HasPrefix(trimmed, []byte("-")) {
		return 0, fmt.Errorf("%s is not a canonical unsigned integer", name)
	}
	value, err := strconv.ParseUint(string(trimmed), 10, 64)
	if err != nil || value == 0 || value > maxSafeJSONInteger {
		return 0, fmt.Errorf("%s is outside the positive portable range", name)
	}
	return value, nil
}

func decodeJSONArray(raw json.RawMessage, mode jsonValidation) ([]json.RawMessage, error) {
	if err := validateJSONDocument(raw, mode); err != nil {
		return nil, err
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return nil, errors.New("JSON value is not an array")
	}
	return values, nil
}

func validateUnicodeEscapes(data []byte) error {
	for index := 0; index < len(data); {
		if data[index] != '"' {
			index++
			continue
		}
		index++
		for index < len(data) && data[index] != '"' {
			if data[index] != '\\' {
				_, width := utf8.DecodeRune(data[index:])
				index += width
				continue
			}
			if index+1 >= len(data) {
				return errors.New("JSON contains an incomplete escape")
			}
			if data[index+1] != 'u' {
				index += 2
				continue
			}
			value, ok := parseUnicodeEscape(data, index)
			if !ok {
				return errors.New("JSON contains an invalid Unicode escape")
			}
			switch {
			case value >= 0xd800 && value <= 0xdbff:
				next := index + 6
				low, paired := parseUnicodeEscape(data, next)
				if !paired || low < 0xdc00 || low > 0xdfff {
					return errors.New("JSON contains a lone UTF-16 surrogate")
				}
				index = next + 6
			case value >= 0xdc00 && value <= 0xdfff:
				return errors.New("JSON contains a lone UTF-16 surrogate")
			default:
				index += 6
			}
		}
		if index < len(data) {
			index++
		}
	}
	return nil
}

func parseUnicodeEscape(data []byte, start int) (uint16, bool) {
	if start+6 > len(data) || data[start] != '\\' || data[start+1] != 'u' {
		return 0, false
	}
	var value uint16
	for _, character := range data[start+2 : start+6] {
		value <<= 4
		switch {
		case character >= '0' && character <= '9':
			value += uint16(character - '0')
		case character >= 'a' && character <= 'f':
			value += uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			value += uint16(character-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}
