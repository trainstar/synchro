package invariants

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

const (
	issue49RowIdentityDomain = "synchro:v3:row-identity:v1\x00"
	issue49RowDigestDomain   = "synchro:v3:row-digest:v1\x00"
	issue49ScopeDigestDomain = "synchro:v3:scope-digest:v1\x00"
	issue49ManifestDomain    = "synchro:v3:schema-manifest:v1\x00"
)

type issue49TypedFieldDefinition struct {
	ID      string `json:"field_id"`
	Type    string `json:"type"`
	Primary bool   `json:"-"`
}

type issue49TypedFieldValue struct {
	ID    string
	Type  string
	Value string
	Null  bool
}

type issue49TypedRow struct {
	TableID        string
	PrimaryFieldID string
	PrimaryValue   string
	Manifest       []issue49TypedFieldDefinition
	Fields         []issue49TypedFieldValue
}

type issue49MalformedTypedRow struct {
	Kind    string
	Row     issue49TypedRow
	Applied bool
}

type issue49CanonicalRowObservation struct {
	Row             issue49TypedRow
	ExpectedBodyHex string
	Implementations map[string]string
	Malformed       []issue49MalformedTypedRow
}

func issue49CanonicalTypedRowsValid(observation issue49CanonicalRowObservation) bool {
	body, ok := issue49CanonicalRowBody(observation.Row)
	if !ok || hex.EncodeToString(body) != observation.ExpectedBodyHex {
		return false
	}
	wantImplementations := []string{"postgresql", "swift", "kotlin", "react-native"}
	if len(observation.Implementations) != len(wantImplementations) {
		return false
	}
	for _, implementation := range wantImplementations {
		if observation.Implementations[implementation] != observation.ExpectedBodyHex {
			return false
		}
	}
	wantMalformed := []string{"unknown", "duplicate", "omitted", "mistyped", "alias", "alternate-case", "physical-type", "primary-key-mismatch"}
	if len(observation.Malformed) != len(wantMalformed) {
		return false
	}
	seen := make(map[string]struct{}, len(observation.Malformed))
	for _, malformed := range observation.Malformed {
		if _, duplicate := seen[malformed.Kind]; duplicate {
			return false
		}
		seen[malformed.Kind] = struct{}{}
		if _, ok := issue49CanonicalRowBody(malformed.Row); ok || malformed.Applied {
			return false
		}
	}
	return hasExactKeys(seen, wantMalformed...)
}

func issue49CanonicalRowBody(row issue49TypedRow) ([]byte, bool) {
	if row.TableID == "" || row.PrimaryFieldID == "" || len(row.Manifest) == 0 || len(row.Fields) != len(row.Manifest) {
		return nil, false
	}
	definitions := make(map[string]issue49TypedFieldDefinition, len(row.Manifest))
	primaryCount := 0
	for _, definition := range row.Manifest {
		if definition.ID == "" || issue49TypedTag(definition.Type) == 0 {
			return nil, false
		}
		if _, duplicate := definitions[definition.ID]; duplicate {
			return nil, false
		}
		definitions[definition.ID] = definition
		if definition.Primary {
			primaryCount++
			if definition.ID != row.PrimaryFieldID {
				return nil, false
			}
		}
	}
	if primaryCount != 1 {
		return nil, false
	}
	values := append([]issue49TypedFieldValue(nil), row.Fields...)
	sort.Slice(values, func(left, right int) bool { return values[left].ID < values[right].ID })
	seen := make(map[string]struct{}, len(values))
	body := bytes.NewBuffer(nil)
	issue49WriteUint32(body, uint32(len(values)))
	for _, value := range values {
		definition, exists := definitions[value.ID]
		if !exists || value.Type != definition.Type {
			return nil, false
		}
		if _, duplicate := seen[value.ID]; duplicate {
			return nil, false
		}
		seen[value.ID] = struct{}{}
		if definition.Primary && (value.Null || value.Value != row.PrimaryValue) {
			return nil, false
		}
		encoded, ok := issue49CanonicalTypedValue(value, definition.Primary)
		if !ok {
			return nil, false
		}
		issue49WriteText(body, value.ID)
		_, _ = body.Write(encoded)
	}
	if len(seen) != len(definitions) {
		return nil, false
	}
	return body.Bytes(), true
}

func issue49CanonicalTypedValue(field issue49TypedFieldValue, primary bool) ([]byte, bool) {
	tag := issue49TypedTag(field.Type)
	if tag == 0 || (field.Null && primary) || (field.Null && field.Value != "") {
		return nil, false
	}
	encoded := bytes.NewBuffer(nil)
	_ = encoded.WriteByte(tag)
	if field.Null {
		_ = encoded.WriteByte(0)
		return encoded.Bytes(), true
	}
	_ = encoded.WriteByte(1)
	switch field.Type {
	case "string":
		if !utf8.ValidString(field.Value) {
			return nil, false
		}
		issue49WriteText(encoded, field.Value)
	case "int":
		value, err := strconv.ParseInt(field.Value, 10, 32)
		if err != nil || strconv.FormatInt(value, 10) != field.Value {
			return nil, false
		}
		var payload [4]byte
		binary.BigEndian.PutUint32(payload[:], uint32(int32(value)))
		_, _ = encoded.Write(payload[:])
	case "int64":
		value, err := strconv.ParseInt(field.Value, 10, 64)
		if err != nil || strconv.FormatInt(value, 10) != field.Value {
			return nil, false
		}
		var payload [8]byte
		binary.BigEndian.PutUint64(payload[:], uint64(value))
		_, _ = encoded.Write(payload[:])
	case "decimal":
		if !issue49CanonicalDecimal(field.Value) {
			return nil, false
		}
		issue49WriteBlob(encoded, []byte(field.Value))
	case "float":
		value, err := strconv.ParseFloat(field.Value, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || math.Signbit(value) && value == 0 || strconv.FormatFloat(value, 'g', -1, 64) != field.Value {
			return nil, false
		}
		var payload [8]byte
		binary.BigEndian.PutUint64(payload[:], math.Float64bits(value))
		_, _ = encoded.Write(payload[:])
	case "boolean":
		switch field.Value {
		case "false":
			_ = encoded.WriteByte(0)
		case "true":
			_ = encoded.WriteByte(1)
		default:
			return nil, false
		}
	case "datetime":
		parsed, err := time.Parse("2006-01-02T15:04:05.000000Z", field.Value)
		if err != nil || parsed.Format("2006-01-02T15:04:05.000000Z") != field.Value {
			return nil, false
		}
		issue49WriteBlob(encoded, []byte(field.Value))
	case "date":
		parsed, err := time.Parse("2006-01-02", field.Value)
		if err != nil || parsed.Format("2006-01-02") != field.Value {
			return nil, false
		}
		issue49WriteBlob(encoded, []byte(field.Value))
	case "time":
		parsed, err := time.Parse("15:04:05.000000", field.Value)
		if err != nil || parsed.Format("15:04:05.000000") != field.Value {
			return nil, false
		}
		issue49WriteBlob(encoded, []byte(field.Value))
	case "json":
		canonical, err := jcs.Transform([]byte(field.Value))
		if err != nil || string(canonical) != field.Value {
			return nil, false
		}
		issue49WriteBlob(encoded, canonical)
	case "bytes":
		decoded, err := base64.RawURLEncoding.DecodeString(field.Value)
		if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != field.Value {
			return nil, false
		}
		issue49WriteBlob(encoded, decoded)
	default:
		return nil, false
	}
	return encoded.Bytes(), true
}

func issue49TypedTag(portableType string) byte {
	return map[string]byte{
		"string": 0x01, "int": 0x02, "int64": 0x03, "decimal": 0x04,
		"float": 0x05, "boolean": 0x06, "datetime": 0x07, "date": 0x08,
		"time": 0x09, "json": 0x0a, "bytes": 0x0b,
	}[portableType]
}

func issue49CanonicalDecimal(value string) bool {
	if value == "0" {
		return true
	}
	if strings.HasPrefix(value, "-") {
		value = value[1:]
		if value == "" || value == "0" {
			return false
		}
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" || !issue49Numeric(parts[0]) || len(parts[0]) > 1 && parts[0][0] == '0' {
		return false
	}
	if len(parts) == 1 {
		return parts[0][0] != '0'
	}
	fraction := parts[1]
	return fraction != "" && issue49Numeric(fraction) && fraction[len(fraction)-1] != '0'
}

type issue49RowDigestRecord struct {
	Kind           string
	Row            issue49TypedRow
	SchemaHash     string
	ServerVersion  string
	Digest         string
	Verified       bool
	Reconciled     bool
	CursorAdvanced bool
}

func issue49RowDigestBindingsValid(records []issue49RowDigestRecord) bool {
	if len(records) != 4 {
		return false
	}
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		if _, duplicate := seen[record.Kind]; duplicate {
			return false
		}
		seen[record.Kind] = struct{}{}
		digest, ok := issue49CanonicalRowDigest(record.Row, record.SchemaHash, record.ServerVersion)
		if !ok || record.Digest != hex.EncodeToString(digest) || !record.Verified || !record.Reconciled || record.CursorAdvanced != (record.Kind == "pull") {
			return false
		}
	}
	return hasExactKeys(seen, "push", "pull", "rebuild", "seed")
}

func issue49CanonicalRowDigest(row issue49TypedRow, schemaHash, serverVersion string) ([]byte, bool) {
	schema, ok := issue49DecodeDigest(schemaHash)
	if !ok || serverVersion == "" || !utf8.ValidString(serverVersion) {
		return nil, false
	}
	identity, ok := issue49CanonicalRowIdentity(row)
	if !ok {
		return nil, false
	}
	body, ok := issue49CanonicalRowBody(row)
	if !ok {
		return nil, false
	}
	input := bytes.NewBufferString(issue49RowDigestDomain)
	_, _ = input.Write(schema)
	issue49WriteBlob(input, identity)
	issue49WriteBlob(input, body)
	issue49WriteText(input, serverVersion)
	digest := sha256.Sum256(input.Bytes())
	return digest[:], true
}

func issue49CanonicalRowIdentity(row issue49TypedRow) ([]byte, bool) {
	var primaryDefinition issue49TypedFieldDefinition
	foundDefinition := false
	for _, definition := range row.Manifest {
		if definition.ID == row.PrimaryFieldID && definition.Primary {
			primaryDefinition = definition
			foundDefinition = true
			break
		}
	}
	if !foundDefinition {
		return nil, false
	}
	var primaryValue issue49TypedFieldValue
	foundValue := false
	for _, field := range row.Fields {
		if field.ID == row.PrimaryFieldID {
			if foundValue {
				return nil, false
			}
			primaryValue = field
			foundValue = true
		}
	}
	if !foundValue || primaryValue.Type != primaryDefinition.Type || primaryValue.Value != row.PrimaryValue || primaryValue.Null {
		return nil, false
	}
	value, ok := issue49CanonicalTypedValue(primaryValue, true)
	if !ok {
		return nil, false
	}
	identity := bytes.NewBufferString(issue49RowIdentityDomain)
	issue49WriteText(identity, row.TableID)
	issue49WriteText(identity, row.PrimaryFieldID)
	_, _ = identity.Write(value)
	return identity.Bytes(), true
}

type issue49ScopeDigestEntry struct {
	RowIdentity string
	RowDigest   string
}

type issue49ScopeDigestObservation struct {
	SchemaHash  string
	ScopeID     string
	Cardinality uint64
	Entries     []issue49ScopeDigestEntry
	Digest      string
	Verified    bool
}

func issue49ScopeDigestBindingValid(observation issue49ScopeDigestObservation) bool {
	computed, ok := issue49CanonicalScopeDigest(observation)
	return ok && observation.Verified && observation.Digest == hex.EncodeToString(computed)
}

func issue49CanonicalScopeDigest(observation issue49ScopeDigestObservation) ([]byte, bool) {
	schema, ok := issue49DecodeDigest(observation.SchemaHash)
	if !ok || observation.ScopeID == "" || observation.Cardinality != uint64(len(observation.Entries)) {
		return nil, false
	}
	identities := make([][]byte, 0, len(observation.Entries))
	digests := make([][]byte, 0, len(observation.Entries))
	for index, entry := range observation.Entries {
		identity, err := hex.DecodeString(entry.RowIdentity)
		if err != nil || len(identity) == 0 || hex.EncodeToString(identity) != entry.RowIdentity {
			return nil, false
		}
		digest, validDigest := issue49DecodeDigest(entry.RowDigest)
		if !validDigest || index > 0 && bytes.Compare(identities[index-1], identity) >= 0 {
			return nil, false
		}
		identities = append(identities, identity)
		digests = append(digests, digest)
	}
	input := bytes.NewBufferString(issue49ScopeDigestDomain)
	_, _ = input.Write(schema)
	issue49WriteText(input, observation.ScopeID)
	issue49WriteUint64(input, observation.Cardinality)
	for index, identity := range identities {
		issue49WriteBlob(input, identity)
		_, _ = input.Write(digests[index])
	}
	digest := sha256.Sum256(input.Bytes())
	return digest[:], true
}

type issue49ChecksumObject struct {
	Algorithm        string
	Version          uint64
	Encoding         string
	Digest           string
	AdditionalFields int
}

type issue49BoundScopeChecksum struct {
	ScopeID    string
	SchemaHash string
	Boundary   issue49EffectPosition
	Checksum   issue49ChecksumObject
}

type issue49PullChecksumPage struct {
	HasMore        bool
	SchemaHash     string
	Boundary       issue49EffectPosition
	AddScopes      []string
	RemoveScopes   []string
	RebuildScopes  []string
	Checksums      map[string]issue49BoundScopeChecksum
	Accepted       bool
	CursorAdvanced bool
}

type issue49ChecksumFailure struct {
	Kind string
	Page issue49PullChecksumPage
}

type issue49TerminalChecksumObservation struct {
	ActiveBefore []string
	Nonterminal  issue49PullChecksumPage
	Terminal     issue49PullChecksumPage
	Failures     []issue49ChecksumFailure
}

func issue49TerminalChecksumMapsValid(observation issue49TerminalChecksumObservation) bool {
	if !uniqueNonemptyStrings(observation.ActiveBefore) || !issue49PullChecksumPageValid(observation.ActiveBefore, observation.Nonterminal) || !observation.Nonterminal.Accepted || !issue49PullChecksumPageValid(observation.ActiveBefore, observation.Terminal) || !observation.Terminal.Accepted || !observation.Terminal.CursorAdvanced {
		return false
	}
	wantFailures := []string{"nonterminal-checksums", "missing", "extra", "malformed", "wrong-bound"}
	if len(observation.Failures) != len(wantFailures) {
		return false
	}
	seen := make(map[string]struct{}, len(observation.Failures))
	for _, failure := range observation.Failures {
		if _, duplicate := seen[failure.Kind]; duplicate {
			return false
		}
		seen[failure.Kind] = struct{}{}
		if issue49PullChecksumPageValid(observation.ActiveBefore, failure.Page) || failure.Page.Accepted || failure.Page.CursorAdvanced {
			return false
		}
	}
	return hasExactKeys(seen, wantFailures...)
}

func issue49PullChecksumPageValid(activeBefore []string, page issue49PullChecksumPage) bool {
	if page.SchemaHash == "" || page.Boundary.CommitLSN == 0 || !uniqueStrings(page.AddScopes) || !uniqueStrings(page.RemoveScopes) || !uniqueStrings(page.RebuildScopes) {
		return false
	}
	if page.HasMore {
		return page.Checksums == nil
	}
	if page.Checksums == nil {
		return false
	}
	active := make(map[string]struct{}, len(activeBefore)+len(page.AddScopes))
	for _, scope := range activeBefore {
		active[scope] = struct{}{}
	}
	for _, scope := range page.RemoveScopes {
		delete(active, scope)
	}
	for _, scope := range page.AddScopes {
		if scope == "" {
			return false
		}
		active[scope] = struct{}{}
	}
	for _, scope := range page.RebuildScopes {
		if _, exists := active[scope]; !exists {
			return false
		}
	}
	if len(page.Checksums) != len(active) {
		return false
	}
	for scope := range active {
		bound, exists := page.Checksums[scope]
		if !exists || bound.ScopeID != scope || bound.SchemaHash != page.SchemaHash || bound.Boundary != page.Boundary || !issue49ChecksumObjectValid(bound.Checksum) {
			return false
		}
	}
	return true
}

func issue49ChecksumObjectValid(checksum issue49ChecksumObject) bool {
	_, ok := issue49DecodeDigest(checksum.Digest)
	return ok && checksum.Algorithm == "sha256" && checksum.Version == 1 && checksum.Encoding == "hex" && checksum.AdditionalFields == 0
}

type issue49ManifestField struct {
	FieldID string `json:"field_id"`
	Type    string `json:"type"`
}

type issue49ManifestTable struct {
	TableID     string                 `json:"table_id"`
	Composition string                 `json:"composition"`
	Fields      []issue49ManifestField `json:"fields"`
}

type issue49PublishedManifest struct {
	Version            uint64
	Hash               string
	Body               string
	ParentVersion      uint64
	ParentHash         string
	TransitionClass    string
	CompatibilityFloor uint64
	Tables             []issue49ManifestTable
}

type issue49ManifestObservation struct {
	Published             issue49PublishedManifest
	Historical            issue49PublishedManifest
	Served                issue49PublishedManifest
	LiveCatalogBody       string
	PublishedAtomically   bool
	UpdatedInPlace        bool
	ServedFromLiveCatalog bool
}

func issue49ImmutableManifestValid(observation issue49ManifestObservation) bool {
	if !observation.PublishedAtomically || observation.UpdatedInPlace || observation.ServedFromLiveCatalog || observation.LiveCatalogBody == "" || observation.LiveCatalogBody == observation.Published.Body {
		return false
	}
	if !issue49ManifestRecordValid(observation.Published) || !issue49ManifestRecordValid(observation.Historical) || !issue49ManifestRecordValid(observation.Served) {
		return false
	}
	return reflect.DeepEqual(observation.Published, observation.Historical) && reflect.DeepEqual(observation.Published, observation.Served)
}

func issue49ManifestRecordValid(manifest issue49PublishedManifest) bool {
	if manifest.Version == 0 || manifest.CompatibilityFloor == 0 || manifest.CompatibilityFloor > manifest.Version || manifest.Body == "" || len(manifest.Tables) == 0 {
		return false
	}
	if !containsString([]string{"initial", "class_2", "class_3", "class_4"}, manifest.TransitionClass) {
		return false
	}
	canonical, err := jcs.Transform([]byte(manifest.Body))
	if err != nil || string(canonical) != manifest.Body || issue49ManifestHash(manifest.Body) != manifest.Hash {
		return false
	}
	var body struct {
		SchemaVersion uint64 `json:"schema_version"`
		ParentSchema  *struct {
			Version uint64 `json:"version"`
			Hash    string `json:"hash"`
		} `json:"parent_schema"`
		TransitionClass    string                 `json:"transition_class"`
		CompatibilityFloor uint64                 `json:"compatibility_floor"`
		Tables             []issue49ManifestTable `json:"tables"`
	}
	if err := json.Unmarshal([]byte(manifest.Body), &body); err != nil || body.SchemaVersion != manifest.Version || body.TransitionClass != manifest.TransitionClass || body.CompatibilityFloor != manifest.CompatibilityFloor || !reflect.DeepEqual(body.Tables, manifest.Tables) {
		return false
	}
	if manifest.TransitionClass == "initial" {
		if body.ParentSchema != nil || manifest.ParentVersion != 0 || manifest.ParentHash != "" {
			return false
		}
	} else if body.ParentSchema == nil || body.ParentSchema.Version != manifest.ParentVersion || body.ParentSchema.Hash != manifest.ParentHash || manifest.ParentVersion == 0 || manifest.ParentHash == "" {
		return false
	}
	seenTables := make(map[string]struct{}, len(manifest.Tables))
	previousTable := ""
	for _, table := range manifest.Tables {
		if table.TableID == "" || table.Composition == "" || len(table.Fields) == 0 || previousTable != "" && table.TableID <= previousTable {
			return false
		}
		if _, duplicate := seenTables[table.TableID]; duplicate {
			return false
		}
		seenTables[table.TableID] = struct{}{}
		previousTable = table.TableID
		seenFields := make(map[string]struct{}, len(table.Fields))
		previousField := ""
		for _, field := range table.Fields {
			if field.FieldID == "" || issue49TypedTag(field.Type) == 0 || previousField != "" && field.FieldID <= previousField {
				return false
			}
			if _, duplicate := seenFields[field.FieldID]; duplicate {
				return false
			}
			seenFields[field.FieldID] = struct{}{}
			previousField = field.FieldID
		}
	}
	return true
}

func issue49ManifestHash(body string) string {
	digest := sha256.Sum256(append([]byte(issue49ManifestDomain), []byte(body)...))
	return hex.EncodeToString(digest[:])
}

func issue49DecodeDigest(value string) ([]byte, bool) {
	decoded, err := hex.DecodeString(value)
	return decoded, err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func issue49WriteText(buffer *bytes.Buffer, value string) {
	issue49WriteBlob(buffer, []byte(value))
}

func issue49WriteBlob(buffer *bytes.Buffer, value []byte) {
	issue49WriteUint64(buffer, uint64(len(value)))
	_, _ = buffer.Write(value)
}

func issue49WriteUint32(buffer *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = buffer.Write(encoded[:])
}

func issue49WriteUint64(buffer *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = buffer.Write(encoded[:])
}
