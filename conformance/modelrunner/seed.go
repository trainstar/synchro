package modelrunner

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/gowebpki/jcs"
	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/vectors"
)

const (
	PortableSeedFixtureID  = "SEEDFIX-PORTABLE-SHARED-1000-001"
	PortableSeedArtifactID = "ARTDEF-PORTABLE-SEED-001"
	portableSeedExportID   = "00000000-0000-4000-8000-000000005001"
	portableSeedRowCount   = 1000
)

type portableManifestBody struct {
	SchemaVersion      uint64                  `json:"schema_version"`
	SchemaHash         string                  `json:"schema_hash"`
	ParentSchema       *portableSchemaRef      `json:"parent_schema"`
	TransitionClass    string                  `json:"transition_class"`
	CompatibilityFloor uint64                  `json:"compatibility_floor"`
	Tables             []portableManifestTable `json:"tables"`
}

type portableSchemaRef struct {
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

type portableManifestTable struct {
	TableID           string                  `json:"table_id"`
	RelationID        string                  `json:"relation_id"`
	Name              string                  `json:"name"`
	Composition       string                  `json:"composition"`
	PrimaryKeyFieldID string                  `json:"primary_key_field_id"`
	Lifecycle         portableLifecycle       `json:"lifecycle"`
	Fields            []portableManifestField `json:"fields"`
	Indexes           []portableManifestIndex `json:"indexes"`
}

type portableLifecycle struct {
	CreatedAtFieldID *string `json:"created_at_field_id"`
	UpdatedAtFieldID *string `json:"updated_at_field_id"`
	DeletedAtFieldID *string `json:"deleted_at_field_id"`
}

type portableManifestField struct {
	FieldID   string `json:"field_id"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Nullable  bool   `json:"nullable"`
	Writable  bool   `json:"writable"`
	Precision *int   `json:"precision,omitempty"`
	Scale     *int   `json:"scale,omitempty"`
}

type portableManifestIndex struct {
	IndexID  string   `json:"index_id"`
	Name     string   `json:"name"`
	FieldIDs []string `json:"field_ids"`
	Unique   bool     `json:"unique"`
}

type portableArtifact struct {
	ArtifactDefinitionID string                   `json:"artifact_definition_id"`
	FixtureID            string                   `json:"fixture_id"`
	ExportID             string                   `json:"export_id"`
	ManifestSHA256       string                   `json:"manifest_sha256"`
	Schema               portableArtifactSchema   `json:"schema"`
	RegistryGeneration   uint64                   `json:"registry_generation"`
	StreamGeneration     string                   `json:"stream_generation"`
	SnapshotBoundary     reference.StreamPosition `json:"snapshot_boundary"`
	PortableScopeIDs     []string                 `json:"portable_scope_ids"`
	Scopes               []portableArtifactScope  `json:"scopes"`
	Rows                 []portableArtifactRow    `json:"rows"`
}

type portableArtifactSchema struct {
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

type portableArtifactScope struct {
	Scope                string `json:"scope"`
	MembershipGeneration uint64 `json:"membership_generation"`
	RetentionGeneration  uint64 `json:"retention_generation"`
	Cardinality          uint64 `json:"cardinality"`
	Checksum             string `json:"checksum"`
}

type portableArtifactRow struct {
	Scope   string                   `json:"scope"`
	Ordinal uint64                   `json:"ordinal"`
	Row     portableArtifactRowValue `json:"row"`
}

type portableArtifactRowValue struct {
	Identity    reference.RowIdentity  `json:"identity"`
	FieldValues []reference.FieldValue `json:"field_values"`
	Version     string                 `json:"version"`
	Checksum    string                 `json:"checksum"`
	Deleted     bool                   `json:"deleted"`
}

// BuildPortableSeedFixture builds the single closed portable seed from an
// installed reference snapshot and independent conformance vectors.
func BuildPortableSeedFixture(snapshot reference.StateSnapshot) (reference.PortableSeedFixture, error) {
	if snapshot.ProtocolVersion != 3 {
		return reference.PortableSeedFixture{}, errors.New("portable seed requires protocol version 3")
	}
	manifestEntry, found := findSchema(snapshot, snapshot.CurrentSchema)
	if !found {
		return reference.PortableSeedFixture{}, errors.New("portable seed current schema is absent")
	}
	manifestBytes, vectorManifest, err := installedVectorManifest(snapshot.CurrentSchema, manifestEntry.Value)
	if err != nil {
		return reference.PortableSeedFixture{}, err
	}
	relation, tableID, err := seedRelationAndTable(snapshot, vectorManifest)
	if err != nil {
		return reference.PortableSeedFixture{}, err
	}
	scopeID, scope, err := portableScope(snapshot)
	if err != nil {
		return reference.PortableSeedFixture{}, err
	}
	if snapshot.Stream.Authority.ActiveGeneration == "" {
		return reference.PortableSeedFixture{}, errors.New("portable seed requires an active stream generation")
	}
	boundary := snapshot.Stream.Authority.GlobalMaterializationBoundary
	if boundary.StreamGeneration != snapshot.Stream.Authority.ActiveGeneration {
		return reference.PortableSeedFixture{}, errors.New("portable seed boundary is outside the active stream")
	}

	rows := make([]reference.PortableSeedRowFixture, 0, portableSeedRowCount)
	digestEntries := make([]vectors.DigestEntry, 0, portableSeedRowCount)
	for ordinal := uint64(1); ordinal <= portableSeedRowCount; ordinal++ {
		row, identityBytes, digest, err := buildSeedRow(vectorManifest, tableID, ordinal)
		if err != nil {
			return reference.PortableSeedFixture{}, fmt.Errorf("build portable seed row %d: %w", ordinal, err)
		}
		identity := row.Identity
		if relation.TableID != identity.TableID || identity.TableID != reference.TableID(tableID) {
			return reference.PortableSeedFixture{}, errors.New("portable seed relation and row table bindings differ")
		}
		rows = append(rows, reference.PortableSeedRowFixture{Scope: scopeID, Ordinal: ordinal, Row: row})
		digestEntries = append(digestEntries, vectors.DigestEntry{RowIdentity: identityBytes, RowDigest: digest})
	}
	scopeChecksum, err := vectors.ScopeDigest(snapshot.CurrentSchema.Hash, string(scopeID), digestEntries)
	if err != nil {
		return reference.PortableSeedFixture{}, fmt.Errorf("compute portable seed scope checksum: %w", err)
	}
	manifestSHA := sha256.Sum256(manifestBytes)
	fixture := reference.PortableSeedFixture{
		FixtureID:            PortableSeedFixtureID,
		ArtifactDefinitionID: PortableSeedArtifactID,
		ManifestBytes:        append([]byte(nil), manifestBytes...),
		ManifestSHA256:       manifestSHA,
		ExportID:             reference.ExportID(portableSeedExportID),
		Schema:               snapshot.CurrentSchema,
		RegistryGeneration:   reference.Generation(snapshot.Registry.CurrentGeneration),
		StreamGeneration:     snapshot.Stream.Authority.ActiveGeneration,
		SnapshotBoundary:     boundary,
		PortableScopeIDs:     []reference.ScopeID{scopeID},
		Scopes: []reference.PortableSeedScopeFixture{{
			Scope: scopeID, MembershipGeneration: scope.MembershipGeneration,
			RetentionGeneration: scope.RetentionGeneration, Cardinality: portableSeedRowCount,
			Checksum: reference.Checksum(scopeChecksum),
		}},
		Rows: rows,
	}
	artifactBytes, err := marshalPortableArtifact(fixture)
	if err != nil {
		return reference.PortableSeedFixture{}, err
	}
	fixture.ArtifactBytes = artifactBytes
	fixture.ArtifactSHA256 = sha256.Sum256(artifactBytes)
	if err := ValidatePortableSeedFixture(fixture, snapshot); err != nil {
		return reference.PortableSeedFixture{}, err
	}
	return fixture, nil
}

// BuildPortableSeedFixtureFromModel builds a fixture from a model snapshot.
func BuildPortableSeedFixtureFromModel(model *reference.Model) (reference.PortableSeedFixture, error) {
	if model == nil {
		return reference.PortableSeedFixture{}, errors.New("portable seed model is required")
	}
	return BuildPortableSeedFixture(model.Snapshot())
}

func buildSeedForScenario(snapshot reference.StateSnapshot, scenario scenarios.Scenario) (reference.PortableSeedFixture, error) {
	fixture, err := BuildPortableSeedFixture(snapshot)
	if err != nil {
		return reference.PortableSeedFixture{}, fmt.Errorf("build %s: %w", PortableSeedFixtureID, err)
	}
	if scenario.ID == "" {
		return reference.PortableSeedFixture{}, errors.New("portable seed scenario ID is required")
	}
	return fixture, nil
}

func findSchema(snapshot reference.StateSnapshot, ref reference.SchemaRef) (reference.SnapshotEntry[reference.SchemaRef, reference.SchemaManifest], bool) {
	for _, entry := range snapshot.Schemas {
		if entry.Key == ref {
			return entry, true
		}
	}
	return reference.SnapshotEntry[reference.SchemaRef, reference.SchemaManifest]{}, false
}

func installedVectorManifest(ref reference.SchemaRef, installed reference.SchemaManifest) ([]byte, vectors.Manifest, error) {
	body := bytes.TrimSpace(installed.Body)
	var object map[string]json.RawMessage
	if len(body) != 0 && json.Unmarshal(body, &object) == nil && object != nil {
		if _, exists := object["schema_hash"]; !exists {
			object["schema_hash"] = mustJSON(hex.EncodeToString(ref.Hash[:]))
		}
		candidate, err := json.Marshal(object)
		if err == nil {
			manifest, parseErr := vectors.ParseManifest(candidate)
			if parseErr == nil && manifest.Hash() == ref.Hash {
				return candidate, manifest, nil
			}
		}
	}

	generated := portableManifestBody{
		SchemaVersion:      ref.Version,
		SchemaHash:         hex.EncodeToString(ref.Hash[:]),
		TransitionClass:    string(installed.Class),
		CompatibilityFloor: installed.CompatibilityFloor,
		Tables:             make([]portableManifestTable, 0, len(installed.Tables)),
	}
	for _, table := range installed.Tables {
		encoded := portableManifestTable{
			TableID: string(table.ID), RelationID: string(table.Relation), Name: table.Name,
			Composition: string(table.Composition), PrimaryKeyFieldID: string(table.PrimaryKeyFieldID),
			Lifecycle: portableLifecycle{CreatedAtFieldID: cloneFieldString(table.CreatedFieldID), UpdatedAtFieldID: cloneFieldString(table.UpdatedFieldID), DeletedAtFieldID: cloneFieldString(table.DeletedFieldID)},
			Fields:    make([]portableManifestField, 0, len(table.Fields)), Indexes: make([]portableManifestIndex, 0, len(table.Indexes)),
		}
		for _, field := range table.Fields {
			item := portableManifestField{FieldID: string(field.ID), Name: field.Name, Type: string(field.PortableType), Nullable: field.Nullable, Writable: field.Writable}
			if field.HasDecimalPrecision {
				value := int(field.DecimalPrecision)
				item.Precision = &value
			}
			if field.HasDecimalScale {
				value := int(field.DecimalScale)
				item.Scale = &value
			}
			encoded.Fields = append(encoded.Fields, item)
		}
		for _, index := range table.Indexes {
			fieldIDs := make([]string, 0, len(index.Fields))
			for _, fieldID := range index.Fields {
				fieldIDs = append(fieldIDs, string(fieldID))
			}
			encoded.Indexes = append(encoded.Indexes, portableManifestIndex{IndexID: string(index.ID), Name: index.Name, FieldIDs: fieldIDs, Unique: index.Unique})
		}
		sort.Slice(encoded.Fields, func(left, right int) bool { return encoded.Fields[left].FieldID < encoded.Fields[right].FieldID })
		sort.Slice(encoded.Indexes, func(left, right int) bool { return encoded.Indexes[left].IndexID < encoded.Indexes[right].IndexID })
		generated.Tables = append(generated.Tables, encoded)
	}
	sort.Slice(generated.Tables, func(left, right int) bool { return generated.Tables[left].TableID < generated.Tables[right].TableID })
	candidate, err := json.Marshal(generated)
	if err != nil {
		return nil, vectors.Manifest{}, fmt.Errorf("marshal installed seed manifest: %w", err)
	}
	manifest, err := vectors.ParseManifest(candidate)
	if err != nil {
		return nil, vectors.Manifest{}, fmt.Errorf("parse installed seed manifest: %w", err)
	}
	if manifest.Hash() != ref.Hash {
		return nil, vectors.Manifest{}, errors.New("installed schema hash does not match the independent manifest vector")
	}
	return candidate, manifest, nil
}

func cloneFieldString(value *reference.FieldID) *string {
	if value == nil {
		return nil
	}
	copy := string(*value)
	return &copy
}

func seedRelationAndTable(snapshot reference.StateSnapshot, manifest vectors.Manifest) (reference.RelationDefinition, string, error) {
	generation, found := registryGeneration(snapshot.Registry)
	if !found {
		return reference.RelationDefinition{}, "", errors.New("portable seed current registry generation is absent")
	}
	for _, registered := range generation.Relations {
		definition := registered.Definition
		if definition.RegistrationKind != reference.RegistrationKindSynced || !definition.HasTableID {
			continue
		}
		for _, table := range snapshotSchemaTables(snapshot, snapshot.CurrentSchema) {
			if table.ID == definition.TableID && table.Relation == definition.Relation {
				return definition, string(table.ID), nil
			}
		}
	}
	_ = manifest
	return reference.RelationDefinition{}, "", errors.New("portable seed requires one registered synced table")
}

func registryGeneration(registry reference.RegistryState) (reference.RegistryGenerationState, bool) {
	for _, generation := range registry.Generations {
		if generation.Generation == registry.CurrentGeneration {
			return generation, true
		}
	}
	return reference.RegistryGenerationState{}, false
}

func snapshotSchemaTables(snapshot reference.StateSnapshot, ref reference.SchemaRef) []reference.TableManifest {
	entry, found := findSchema(snapshot, ref)
	if !found {
		return nil
	}
	return entry.Value.Tables
}

func portableScope(snapshot reference.StateSnapshot) (reference.ScopeID, reference.ScopeState, error) {
	if len(snapshot.Scopes) == 0 {
		return "", reference.ScopeState{}, errors.New("portable seed requires one authoritative scope")
	}
	scopes := append([]reference.SnapshotEntry[reference.ScopeID, reference.ScopeState](nil), snapshot.Scopes...)
	sort.Slice(scopes, func(left, right int) bool { return scopes[left].Key < scopes[right].Key })
	for _, entry := range scopes {
		if strings.Contains(strings.ToLower(string(entry.Key)), "shared") {
			return entry.Key, entry.Value, nil
		}
	}
	return scopes[0].Key, scopes[0].Value, nil
}

func buildSeedRow(manifest vectors.Manifest, tableID string, ordinal uint64) (reference.AuthoritativeRow, []byte, [32]byte, error) {
	tableFields, err := manifestFields(manifest, tableID)
	if err != nil {
		return reference.AuthoritativeRow{}, nil, [32]byte{}, err
	}
	primaryField, err := manifestPrimaryField(manifest, tableID)
	if err != nil {
		return reference.AuthoritativeRow{}, nil, [32]byte{}, err
	}
	primaryRaw := json.RawMessage(strconv.Quote(fmt.Sprintf("seed-%06d", ordinal)))
	fields := make([]vectors.RowField, 0, len(tableFields))
	referenceFields := make([]reference.FieldValue, 0, len(tableFields))
	for _, field := range tableFields {
		raw := seedFieldJSON(field, ordinal, field.FieldID == primaryField.FieldID)
		fields = append(fields, vectors.RowField{FieldID: field.FieldID, Value: raw})
		referenceFields = append(referenceFields, reference.FieldValue{Field: reference.FieldID(field.FieldID), Type: reference.PortableType(field.Type), WireJSON: string(raw)})
	}
	rowInput := vectors.Row{PK: primaryRaw, Fields: fields}
	identityBytes, err := vectors.RowIdentity(manifest, tableID, primaryRaw)
	if err != nil {
		return reference.AuthoritativeRow{}, nil, [32]byte{}, fmt.Errorf("row identity: %w", err)
	}
	version := fmt.Sprintf("seed-v%06d", ordinal)
	digest, err := vectors.RowDigest(manifest, tableID, rowInput, version)
	if err != nil {
		return reference.AuthoritativeRow{}, nil, [32]byte{}, fmt.Errorf("row digest: %w", err)
	}
	identity := reference.RowIdentity{CanonicalIdentityBytes: string(identityBytes), TableID: reference.TableID(tableID), PrimaryKeyFieldID: reference.FieldID(primaryField.FieldID), PortableType: reference.PortableType(primaryField.Type), CanonicalWireJSON: string(primaryRaw)}
	return reference.AuthoritativeRow{Identity: identity, FieldValues: referenceFields, Version: reference.RowVersion(version), Checksum: reference.Checksum(digest)}, identityBytes, digest, nil
}

type manifestFieldInfo struct {
	FieldID string
	Type    string
}

func manifestFields(manifest vectors.Manifest, tableID string) ([]manifestFieldInfo, error) {
	// vectors.Manifest intentionally exposes only digest APIs. Re-read its
	// canonical body so the fixture builder does not duplicate vector rules.
	var body struct {
		Tables []struct {
			TableID string `json:"table_id"`
			Fields  []struct {
				FieldID string `json:"field_id"`
				Type    string `json:"type"`
			} `json:"fields"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(manifest.CanonicalBody(), &body); err != nil {
		return nil, err
	}
	for _, table := range body.Tables {
		if table.TableID != tableID {
			continue
		}
		result := make([]manifestFieldInfo, 0, len(table.Fields))
		for _, field := range table.Fields {
			result = append(result, manifestFieldInfo{FieldID: field.FieldID, Type: field.Type})
		}
		sort.Slice(result, func(left, right int) bool { return result[left].FieldID < result[right].FieldID })
		return result, nil
	}
	return nil, fmt.Errorf("manifest table %q is absent", tableID)
}

func manifestPrimaryField(manifest vectors.Manifest, tableID string) (manifestFieldInfo, error) {
	var body struct {
		Tables []struct {
			TableID           string `json:"table_id"`
			PrimaryKeyFieldID string `json:"primary_key_field_id"`
			Fields            []struct {
				FieldID string `json:"field_id"`
				Type    string `json:"type"`
			} `json:"fields"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(manifest.CanonicalBody(), &body); err != nil {
		return manifestFieldInfo{}, err
	}
	for _, table := range body.Tables {
		if table.TableID != tableID {
			continue
		}
		for _, field := range table.Fields {
			if field.FieldID == table.PrimaryKeyFieldID {
				return manifestFieldInfo{FieldID: field.FieldID, Type: field.Type}, nil
			}
		}
	}
	return manifestFieldInfo{}, fmt.Errorf("manifest primary key for table %q is absent", tableID)
}

func seedFieldJSON(field manifestFieldInfo, ordinal uint64, primary bool) json.RawMessage {
	if primary {
		return json.RawMessage(strconv.Quote(fmt.Sprintf("seed-%06d", ordinal)))
	}
	switch field.Type {
	case "string":
		return json.RawMessage(strconv.Quote(fmt.Sprintf("seed-%s-%06d", field.FieldID, ordinal)))
	case "int", "float":
		return json.RawMessage(strconv.FormatUint(ordinal, 10))
	case "int64":
		return json.RawMessage(strconv.Quote(strconv.FormatUint(ordinal, 10)))
	case "decimal":
		return json.RawMessage(strconv.Quote(fmt.Sprintf("%d.01", ordinal)))
	case "boolean":
		if ordinal%2 == 0 {
			return json.RawMessage("false")
		}
		return json.RawMessage("true")
	case "datetime":
		return json.RawMessage(strconv.Quote("2024-01-01T00:00:00.000000Z"))
	case "date":
		return json.RawMessage(strconv.Quote("2024-01-01"))
	case "time":
		return json.RawMessage(strconv.Quote("00:00:00.000000"))
	case "json":
		return json.RawMessage(strconv.Quote(fmt.Sprintf("{\"ordinal\":%d}", ordinal)))
	case "bytes":
		return json.RawMessage(strconv.Quote(base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("seed-%06d", ordinal)))))
	default:
		return json.RawMessage("null")
	}
}

func marshalPortableArtifact(fixture reference.PortableSeedFixture) ([]byte, error) {
	artifact := portableArtifact{
		ArtifactDefinitionID: fixture.ArtifactDefinitionID, FixtureID: fixture.FixtureID,
		ExportID: string(fixture.ExportID), ManifestSHA256: hex.EncodeToString(fixture.ManifestSHA256[:]),
		Schema:             portableArtifactSchema{Version: fixture.Schema.Version, Hash: hex.EncodeToString(fixture.Schema.Hash[:])},
		RegistryGeneration: uint64(fixture.RegistryGeneration), StreamGeneration: string(fixture.StreamGeneration), SnapshotBoundary: fixture.SnapshotBoundary,
		PortableScopeIDs: make([]string, 0, len(fixture.PortableScopeIDs)), Scopes: make([]portableArtifactScope, 0, len(fixture.Scopes)), Rows: make([]portableArtifactRow, 0, len(fixture.Rows)),
	}
	for _, scope := range fixture.PortableScopeIDs {
		artifact.PortableScopeIDs = append(artifact.PortableScopeIDs, string(scope))
	}
	for _, scope := range fixture.Scopes {
		artifact.Scopes = append(artifact.Scopes, portableArtifactScope{Scope: string(scope.Scope), MembershipGeneration: uint64(scope.MembershipGeneration), RetentionGeneration: uint64(scope.RetentionGeneration), Cardinality: uint64(scope.Cardinality), Checksum: hex.EncodeToString(scope.Checksum[:])})
	}
	for _, row := range fixture.Rows {
		fields := append([]reference.FieldValue(nil), row.Row.FieldValues...)
		artifact.Rows = append(artifact.Rows, portableArtifactRow{Scope: string(row.Scope), Ordinal: row.Ordinal, Row: portableArtifactRowValue{Identity: row.Row.Identity, FieldValues: fields, Version: string(row.Row.Version), Checksum: hex.EncodeToString(row.Row.Checksum[:]), Deleted: row.Row.Deleted}})
	}
	encoded, err := json.Marshal(artifact)
	if err != nil {
		return nil, err
	}
	return jcs.Transform(encoded)
}

// ValidatePortableSeedFixture rechecks all fixture facts that the runner can
// verify before the reference handler receives its defensive copy.
func ValidatePortableSeedFixture(fixture reference.PortableSeedFixture, snapshot reference.StateSnapshot) error {
	if fixture.FixtureID != PortableSeedFixtureID || fixture.ArtifactDefinitionID != PortableSeedArtifactID {
		return errors.New("portable seed fixture identifiers are not closed")
	}
	if sha256.Sum256(fixture.ArtifactBytes) != fixture.ArtifactSHA256 {
		return errors.New("portable seed artifact SHA-256 does not match")
	}
	if sha256.Sum256(fixture.ManifestBytes) != fixture.ManifestSHA256 {
		return errors.New("portable seed manifest SHA-256 does not match")
	}
	if fixture.Schema != snapshot.CurrentSchema || fixture.RegistryGeneration != reference.Generation(snapshot.Registry.CurrentGeneration) || fixture.StreamGeneration != snapshot.Stream.Authority.ActiveGeneration {
		return errors.New("portable seed lineage does not match the installed contract")
	}
	if len(fixture.PortableScopeIDs) != 1 || len(fixture.Scopes) != 1 || fixture.PortableScopeIDs[0] != fixture.Scopes[0].Scope {
		return errors.New("portable seed scope declaration is not exact")
	}
	if fixture.SnapshotBoundary.StreamGeneration != fixture.StreamGeneration || !positionAtOrBefore(fixture.SnapshotBoundary, snapshot.Stream.Authority.GlobalMaterializationBoundary) {
		return errors.New("portable seed snapshot boundary is invalid")
	}
	if fixture.Scopes[0].Cardinality != portableSeedRowCount || len(fixture.Rows) != portableSeedRowCount {
		return errors.New("portable seed cardinality is not exactly 1000")
	}
	manifest, err := vectors.ParseManifest(fixture.ManifestBytes)
	if err != nil || manifest.Hash() != fixture.Schema.Hash {
		return errors.New("portable seed manifest is not independently valid")
	}
	digestEntries := make([]vectors.DigestEntry, 0, len(fixture.Rows))
	for index, row := range fixture.Rows {
		if row.Scope != fixture.PortableScopeIDs[0] || row.Ordinal != uint64(index+1) || row.Row.Deleted || row.Row.Version == "" {
			return fmt.Errorf("portable seed row %d has an invalid ordinal or live state", index+1)
		}
		identityBytes, digest, err := referenceRowDigest(manifest, row.Row)
		if err != nil {
			return fmt.Errorf("portable seed row %d cannot be recomputed: %w", index+1, err)
		}
		if string(identityBytes) != row.Row.Identity.CanonicalIdentityBytes || digest != row.Row.Checksum {
			return fmt.Errorf("portable seed row %d checksum or identity changed", index+1)
		}
		digestEntries = append(digestEntries, vectors.DigestEntry{RowIdentity: identityBytes, RowDigest: digest})
	}
	scopeChecksum, err := vectors.ScopeDigest(fixture.Schema.Hash, string(fixture.PortableScopeIDs[0]), digestEntries)
	if err != nil || reference.Checksum(scopeChecksum) != fixture.Scopes[0].Checksum {
		return errors.New("portable seed scope checksum changed")
	}
	return nil
}

func referenceRowDigest(manifest vectors.Manifest, row reference.AuthoritativeRow) ([]byte, [32]byte, error) {
	fields := make([]vectors.RowField, 0, len(row.FieldValues))
	for _, field := range row.FieldValues {
		fields = append(fields, vectors.RowField{FieldID: string(field.Field), Value: json.RawMessage(field.WireJSON)})
	}
	identity, err := vectors.RowIdentity(manifest, string(row.Identity.TableID), json.RawMessage(row.Identity.CanonicalWireJSON))
	if err != nil {
		return nil, [32]byte{}, err
	}
	digest, err := vectors.RowDigest(manifest, string(row.Identity.TableID), vectors.Row{PK: json.RawMessage(row.Identity.CanonicalWireJSON), Fields: fields}, string(row.Version))
	if err != nil {
		return nil, [32]byte{}, err
	}
	return identity, digest, nil
}

func positionAtOrBefore(left, right reference.StreamPosition) bool {
	if left.StreamGeneration != right.StreamGeneration {
		return left.StreamGeneration < right.StreamGeneration
	}
	if left.CommitLSN != right.CommitLSN {
		return left.CommitLSN <= right.CommitLSN
	}
	if left.EventOrdinal != right.EventOrdinal {
		return left.EventOrdinal <= right.EventOrdinal
	}
	return left.EffectOrdinal <= right.EffectOrdinal
}
