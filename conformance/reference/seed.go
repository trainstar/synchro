package reference

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/trainstar/synchro/conformance/vectors"
)

const (
	portableSeedArtifactDefinitionID = "ARTDEF-PORTABLE-SEED-001"
	portableSeedFixtureID            = "SEEDFIX-PORTABLE-SHARED-1000-001"
	portableSeedRowCount             = 1000
)

type installPortableSeedPayload struct {
	UserID                 string `json:"user_id"`
	ClientID               string `json:"client_id"`
	PortableSeedArtifactID string `json:"portable_seed_artifact_id"`
	SeedFixtureID          string `json:"seed_fixture_id"`
}

type verifiedPortableSeed struct {
	scope PortableSeedScopeFixture
	rows  []PortableSeedRowFixture
}

func init() {
	registerResolvedOperation("artifact/install-portable-seed", installPortableSeed)
}

func installPortableSeed(_ context.Context, model *Model, payload json.RawMessage, input ResolvedOperationInput) (StepResult, error) {
	var request installPortableSeedPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode install-portable-seed payload: %w", err)
	}
	if request.UserID == "" || request.ClientID == "" || request.PortableSeedArtifactID != portableSeedArtifactDefinitionID || request.SeedFixtureID != portableSeedFixtureID {
		return StepResult{}, fmt.Errorf("install-portable-seed payload binding is invalid")
	}
	if input.PortableSeed == nil || input.SourceStep != nil {
		return StepResult{}, fmt.Errorf("install-portable-seed requires only a resolved portable seed")
	}
	fixture := input.PortableSeed
	if fixture.FixtureID != request.SeedFixtureID || fixture.ArtifactDefinitionID != request.PortableSeedArtifactID {
		return StepResult{}, fmt.Errorf("install-portable-seed fixture identity is misbound")
	}
	verified, err := verifyPortableSeedFixture(model.state, *fixture)
	if err != nil {
		return StepResult{}, err
	}

	client := ClientKey{UserID: UserID(request.UserID), ClientID: ClientID(request.ClientID)}
	local, found := model.state.ClientLocal[client]
	if !found {
		return StepResult{}, fmt.Errorf("install-portable-seed local client is absent")
	}
	if _, found := model.state.Clients[client]; !found {
		return StepResult{}, fmt.Errorf("install-portable-seed server client is absent")
	}
	if err := validatePortableSeedTarget(model.state, client, local); err != nil {
		return StepResult{}, err
	}
	if !canMintTokens(model.authority, len(fixture.Scopes)) {
		return StepResult{}, fmt.Errorf("install-portable-seed receipt authority is exhausted")
	}

	rows := make([]LocalRow, 0, len(verified.rows))
	provenance := make([]LocalProvenance, 0, len(verified.rows))
	for _, record := range verified.rows {
		row := record.Row
		rows = append(rows, LocalRow{
			Identity:         row.Identity,
			Fields:           cloneFieldValues(row.FieldValues),
			Deleted:          false,
			HasServerVersion: true,
			ServerVersion:    row.Version,
			HasChecksum:      true,
			Checksum:         row.Checksum,
			UpdatedAt:        cloneTime(row.UpdatedAt),
		})
		provenance = append(provenance, LocalProvenance{Row: row.Identity, Scopes: []ScopeID{record.Scope}, Version: row.Version})
	}

	issuedAt := referenceNow(model.clock)
	bindings := portableSeedReceiptBindings(*fixture, verified.scope, issuedAt)
	receipt := model.authority.Mint(string(TokenKindSeedReceipt), bindings)
	if receipt == (OpaqueToken{}) {
		return StepResult{}, fmt.Errorf("install-portable-seed receipt mint failed")
	}
	local.Rows = rows
	local.Provenance = provenance
	local.SeedReceipts = []LocalSeedReceipt{{
		Scope:                verified.scope.Scope,
		HasReceipt:           true,
		Receipt:              receipt,
		ExportID:             fixture.ExportID,
		ExportManifestHash:   fixture.ManifestSHA256,
		Schema:               fixture.Schema,
		RegistryGeneration:   fixture.RegistryGeneration,
		MembershipGeneration: verified.scope.MembershipGeneration,
		RetentionGeneration:  verified.scope.RetentionGeneration,
		StreamGeneration:     fixture.StreamGeneration,
		SnapshotBoundary:     fixture.SnapshotBoundary,
		Cardinality:          verified.scope.Cardinality,
		Checksum:             verified.scope.Checksum,
	}}
	model.state.ClientLocal[client] = local
	return localOperationResult(client, LocalMutationStatusAccepted), nil
}

func verifyPortableSeedFixture(state State, fixture PortableSeedFixture) (verifiedPortableSeed, error) {
	if len(fixture.ArtifactBytes) == 0 || sha256.Sum256(fixture.ArtifactBytes) != fixture.ArtifactSHA256 {
		return verifiedPortableSeed{}, fmt.Errorf("install-portable-seed artifact digest is invalid")
	}
	if len(fixture.ManifestBytes) == 0 || sha256.Sum256(fixture.ManifestBytes) != fixture.ManifestSHA256 {
		return verifiedPortableSeed{}, fmt.Errorf("install-portable-seed manifest digest is invalid")
	}
	if !validCanonicalUUID(string(fixture.ExportID)) {
		return verifiedPortableSeed{}, fmt.Errorf("install-portable-seed export lineage is invalid")
	}
	if fixture.Schema != state.CurrentSchema || fixture.RegistryGeneration == 0 || fixture.RegistryGeneration != state.Registry.CurrentGeneration {
		return verifiedPortableSeed{}, fmt.Errorf("install-portable-seed contract lineage changed")
	}
	if _, err := loadPushManifest(state, fixture.Schema); err != nil {
		return verifiedPortableSeed{}, fmt.Errorf("install-portable-seed schema manifest is invalid: %w", err)
	}
	if !activeRegistryGeneration(state.Registry, fixture.RegistryGeneration) {
		return verifiedPortableSeed{}, fmt.Errorf("install-portable-seed registry generation is not active and validated")
	}
	if !validPortableSeedBoundary(state, fixture.StreamGeneration, fixture.SnapshotBoundary) {
		return verifiedPortableSeed{}, fmt.Errorf("install-portable-seed snapshot boundary is invalid")
	}
	scope, err := verifyPortableSeedScopes(state, fixture)
	if err != nil {
		return verifiedPortableSeed{}, err
	}
	rows, err := verifyPortableSeedRows(state, fixture, scope)
	if err != nil {
		return verifiedPortableSeed{}, err
	}
	return verifiedPortableSeed{scope: scope, rows: rows}, nil
}

func activeRegistryGeneration(registry RegistryState, wanted Generation) bool {
	matchCount := 0
	for _, generation := range registry.Generations {
		if generation.Generation == wanted && generation.Validated {
			matchCount++
		}
	}
	return matchCount == 1
}

func validPortableSeedBoundary(state State, generation StreamGeneration, boundary StreamPosition) bool {
	if generation == "" || generation != state.Stream.Authority.ActiveGeneration || boundary.StreamGeneration != generation {
		return false
	}
	switch boundary.Kind {
	case PositionKindGenerationStart:
		if boundary.CommitLSN != 0 || boundary.EventOrdinal != 0 || boundary.EffectOrdinal != 0 {
			return false
		}
	case PositionKindTransactionEnd:
		if boundary.CommitLSN == 0 || boundary.EventOrdinal != 0 || boundary.EffectOrdinal != 0 {
			return false
		}
	default:
		return false
	}
	materialized := currentPullBoundary(state)
	return materialized.StreamGeneration == generation && positionAtOrBefore(boundary, materialized)
}

func verifyPortableSeedScopes(state State, fixture PortableSeedFixture) (PortableSeedScopeFixture, error) {
	if len(fixture.PortableScopeIDs) != 1 || len(fixture.Scopes) != 1 || fixture.PortableScopeIDs[0] == "" || fixture.PortableScopeIDs[0] != fixture.Scopes[0].Scope {
		return PortableSeedScopeFixture{}, fmt.Errorf("install-portable-seed portable scope declaration is not exact")
	}
	for index := 1; index < len(fixture.PortableScopeIDs); index++ {
		if fixture.PortableScopeIDs[index-1] >= fixture.PortableScopeIDs[index] {
			return PortableSeedScopeFixture{}, fmt.Errorf("install-portable-seed portable scope declaration is not sorted and unique")
		}
	}
	scope := fixture.Scopes[0]
	authoritative, found := state.Scopes[scope.Scope]
	if !found || scope.MembershipGeneration == 0 || scope.RetentionGeneration == 0 || scope.MembershipGeneration != authoritative.MembershipGeneration || scope.RetentionGeneration != authoritative.RetentionGeneration {
		return PortableSeedScopeFixture{}, fmt.Errorf("install-portable-seed scope generation changed")
	}
	if scope.Cardinality != portableSeedRowCount {
		return PortableSeedScopeFixture{}, fmt.Errorf("install-portable-seed scope cardinality is invalid")
	}
	return scope, nil
}

func verifyPortableSeedRows(state State, fixture PortableSeedFixture, scope PortableSeedScopeFixture) ([]PortableSeedRowFixture, error) {
	if len(fixture.Rows) != portableSeedRowCount {
		return nil, fmt.Errorf("install-portable-seed row count is invalid")
	}
	manifest, err := loadPushManifest(state, fixture.Schema)
	if err != nil {
		return nil, fmt.Errorf("install-portable-seed schema manifest is invalid: %w", err)
	}
	rows := make([]PortableSeedRowFixture, len(fixture.Rows))
	for index, record := range fixture.Rows {
		rows[index] = record
		rows[index].Row = cloneAuthoritativeRow(record.Row)
	}
	sort.Slice(rows, func(left, right int) bool { return rows[left].Ordinal < rows[right].Ordinal })
	digestRows := make([]rebuildDigestRow, 0, len(rows))
	seenIdentities := make(map[RowIdentity]struct{}, len(rows))
	for index, record := range rows {
		ordinal := uint64(index + 1)
		if record.Ordinal != ordinal || record.Scope != scope.Scope {
			return nil, fmt.Errorf("install-portable-seed row ordinal or scope is invalid")
		}
		row := record.Row
		if row.Deleted || row.DeletedAt != nil || row.DeleteReason != nil || row.Version == "" {
			return nil, fmt.Errorf("install-portable-seed row is not live and versioned")
		}
		if _, duplicate := seenIdentities[row.Identity]; duplicate {
			return nil, fmt.Errorf("install-portable-seed row identity is duplicated")
		}
		seenIdentities[row.Identity] = struct{}{}
		table, found := manifest.Tables[row.Identity.TableID]
		if !found {
			return nil, fmt.Errorf("install-portable-seed row table is absent from the manifest")
		}
		relation, registered := currentSyncedRelation(state, row.Identity.TableID)
		if !registered || relation.Relation != table.Relation || relation.PrimaryKeyFieldID != table.PrimaryKeyFieldID {
			return nil, fmt.Errorf("install-portable-seed row table is not registered as synced")
		}
		if err := verifyPortableSeedRowFields(table, row); err != nil {
			return nil, err
		}
		expectedPK, _ := json.Marshal(fmt.Sprintf("seed-%06d", ordinal))
		expectedIdentity, err := derivePushRowIdentity(manifest, row.Identity.TableID, table.PrimaryKeyFieldID, expectedPK)
		if err != nil || expectedIdentity != row.Identity {
			return nil, fmt.Errorf("install-portable-seed row identity does not match its deterministic ordinal")
		}
		checksum, err := pushRowChecksum(manifest, row.Identity.TableID, row, row.Version)
		if err != nil || checksum != row.Checksum {
			return nil, fmt.Errorf("install-portable-seed row checksum is invalid")
		}
		digestRows = append(digestRows, rebuildDigestRow{Identity: row.Identity, Checksum: row.Checksum})
	}
	checksum, valid := referenceScopeChecksum(fixture.Schema, scope.Scope, digestRows)
	if !valid || Cardinality(len(rows)) != scope.Cardinality || checksum != scope.Checksum {
		return nil, fmt.Errorf("install-portable-seed scope digest or cardinality is invalid")
	}
	return rows, nil
}

func verifyPortableSeedRowFields(table pushManifestTable, row AuthoritativeRow) error {
	if len(row.FieldValues) != len(table.Fields) {
		return fmt.Errorf("install-portable-seed row field set is incomplete")
	}
	seen := make(map[FieldID]struct{}, len(row.FieldValues))
	for _, value := range row.FieldValues {
		field, found := table.Fields[value.Field]
		if !found || value.Type != field.Portable {
			return fmt.Errorf("install-portable-seed row field is not in the manifest")
		}
		if _, duplicate := seen[value.Field]; duplicate {
			return fmt.Errorf("install-portable-seed row field is duplicated")
		}
		seen[value.Field] = struct{}{}
		spec := vectors.FieldSpec{Type: string(field.Portable), Nullable: field.Nullable, Precision: field.Precision, Scale: field.Scale}
		if _, err := vectors.EncodeTypedValue(spec, json.RawMessage(value.WireJSON)); err != nil {
			return fmt.Errorf("install-portable-seed row field value is invalid")
		}
	}
	return nil
}

func validatePortableSeedTarget(state State, client ClientKey, local ClientLocalState) error {
	if local.CurrentSchema != state.CurrentSchema || len(local.Rows) != 0 || len(local.LocalOnlyRows) != 0 || len(local.Provenance) != 0 || len(local.ScopeCheckpoints) != 0 || len(local.SeedReceipts) != 0 || len(local.DurableQueue) != 0 || len(local.SealedBatches) != 0 || len(local.RebuildAttempts) != 0 || len(local.RebuildStaging) != 0 {
		return fmt.Errorf("install-portable-seed target local client is not empty")
	}
	for key := range state.Rebuilds {
		if key.Client == client {
			return fmt.Errorf("install-portable-seed target client has rebuild state")
		}
	}
	return nil
}

func portableSeedReceiptBindings(fixture PortableSeedFixture, scope PortableSeedScopeFixture, issuedAt time.Time) BindingSet {
	return BindingSet{
		HasRegistryGeneration:   true,
		RegistryGeneration:      fixture.RegistryGeneration,
		HasMembershipGeneration: true,
		MembershipGeneration:    scope.MembershipGeneration,
		HasRetentionGeneration:  true,
		RetentionGeneration:     scope.RetentionGeneration,
		HasStreamGeneration:     true,
		StreamGeneration:        fixture.StreamGeneration,
		HasSchema:               true,
		Schema:                  fixture.Schema,
		HasScope:                true,
		Scope:                   scope.Scope,
		HasSnapshotBoundary:     true,
		SnapshotBoundary:        fixture.SnapshotBoundary,
		HasExportID:             true,
		ExportID:                fixture.ExportID,
		HasExportManifestHash:   true,
		ExportManifestHash:      fixture.ManifestSHA256,
		HasIssuedAt:             true,
		IssuedAt:                issuedAt,
		HasCardinality:          true,
		Cardinality:             scope.Cardinality,
		HasChecksum:             true,
		Checksum:                scope.Checksum,
	}
}
