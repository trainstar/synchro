package reference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type connectKnownScopePayload struct {
	ScopeID *string `json:"scope_id"`
}

type seedReceiptSource string

const (
	seedReceiptSourceLocal seedReceiptSource = "local_seed_receipt"
)

type connectPayload struct {
	UserID           *string                       `json:"user_id"`
	ClientID         *string                       `json:"client_id"`
	RuntimeVersion   *uint64                       `json:"runtime_version"`
	ProtocolVersion  *int                          `json:"protocol_version"`
	ClientGeneration *uint64                       `json:"client_generation"`
	SchemaReset      *bool                         `json:"schema_reset"`
	Schema           *schemaReferencePayload       `json:"schema"`
	ScopeSetVersion  *uint64                       `json:"scope_set_version"`
	KnownScopes      *[]connectKnownScopePayload   `json:"known_scopes"`
	SeedReceipts     *map[string]seedReceiptSource `json:"seed_receipts"`
}

type connectCursorPlan struct {
	Scope       ScopeID
	Disposition CursorDisposition
	Replace     bool
	Position    StreamPosition
	Cursor      OpaqueToken
	Invalidate  bool
	SeedReceipt bool
}

func init() {
	registerOperation("connect/send", sendConnect)
}

func sendConnect(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	var request connectPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return StepResult{}, fmt.Errorf("decode connect payload: %w", err)
	}
	clientKey, err := decodeClientKey(request.UserID, request.ClientID)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate connect payload: %w", err)
	}
	knownScopes, seedReceipts, err := validateConnectEnvelope(request)
	if err != nil {
		return StepResult{}, fmt.Errorf("validate connect payload: %w", err)
	}

	if *request.ProtocolVersion != supportedProtocolVersion || runtimeTooOld(model.state.Installation, *request.RuntimeVersion) {
		return connectFailure(model, clientKey, SchemaRef{}, 426, HTTPCode("upgrade_required"), ReasonCode("upgrade_required"))
	}
	client, clientKnown := model.state.Clients[clientKey]
	if clientKnown && client.Retirement != nil {
		return connectFailure(model, clientKey, SchemaRef{}, 409, HTTPCode("client_retired"), ReasonCode("client_retired"))
	}

	source, fresh, schemaErr := decodeSchemaReference(request.Schema, true)
	if schemaErr != nil {
		return connectFailure(model, clientKey, SchemaRef{}, 400, HTTPCode("invalid_schema_reference"), ReasonCode("invalid_schema_reference"))
	}
	localBefore, localKnown := model.state.ClientLocal[clientKey]
	priorIdentity := hasDurablePriorClientState(client, clientKnown, localBefore, localKnown)
	if fresh {
		if request.ClientGeneration != nil || *request.SchemaReset || priorIdentity {
			return connectFailure(model, clientKey, source, 400, HTTPCode("invalid_schema_reference"), ReasonCode("invalid_schema_reference"))
		}
	} else {
		if (*request.SchemaReset && request.ClientGeneration == nil) || (priorIdentity && request.ClientGeneration == nil) {
			return connectFailure(model, clientKey, source, 400, HTTPCode("invalid_schema_reference"), ReasonCode("invalid_schema_reference"))
		}
		if localKnown && localBefore.CurrentSchema != (SchemaRef{}) && localBefore.CurrentSchema != source {
			return connectFailure(model, clientKey, source, 400, HTTPCode("invalid_schema_reference"), ReasonCode("invalid_schema_reference"))
		}
	}

	currentManifest, currentFound := model.state.Schemas[model.state.CurrentSchema]
	if !currentFound || model.state.CurrentSchema == (SchemaRef{}) {
		return StepResult{}, errors.New("connect requires a current immutable schema")
	}

	local, err := prepareConnectLocal(model, clientKey)
	if err != nil {
		return StepResult{}, err
	}
	if client.CurrentGeneration == 0 {
		if request.ClientGeneration != nil {
			return connectFailure(model, clientKey, source, 400, HTTPCode("invalid_request"), ReasonCode("invalid_request"))
		}
		if _, err := renewClientGeneration(model, clientKey, &client, &local); err != nil {
			return StepResult{}, fmt.Errorf("create client generation: %w", err)
		}
	} else {
		if request.ClientGeneration == nil || Generation(*request.ClientGeneration) != client.CurrentGeneration {
			return connectFailure(model, clientKey, source, 400, HTTPCode("invalid_request"), ReasonCode("invalid_request"))
		}
		now := model.clock.Now().Round(0).UTC()
		expired, err := clientGenerationExpired(model, client, now)
		if err != nil {
			return StepResult{}, fmt.Errorf("check client generation expiry: %w", err)
		}
		if expired {
			generationIndex := currentClientGenerationIndex(client)
			if generationIndex < 0 {
				return StepResult{}, errors.New("current client generation is missing from history")
			}
			if client.Generations[generationIndex].ExpiresAt == nil {
				client.Generations[generationIndex].ExpiresAt = &now
			}
			if _, err := renewClientGeneration(model, clientKey, &client, &local); err != nil {
				return StepResult{}, fmt.Errorf("renew client generation: %w", err)
			}
		} else {
			local.ClientGeneration = client.CurrentGeneration
		}
	}
	if *request.ScopeSetVersion > uint64(client.ScopeSetVersion) {
		return connectFailure(model, clientKey, source, 400, HTTPCode("invalid_request"), ReasonCode("invalid_request"))
	}
	if err := knownScopesMatchLocalState(local, knownScopes); err != nil {
		return connectFailure(model, clientKey, source, 400, HTTPCode("invalid_request"), ReasonCode("invalid_request"))
	}

	decision := schemaLineageDecision{Action: SchemaActionReplace}
	affected := []ScopeID(nil)
	if *request.SchemaReset {
		affected = assignedScopeIDs(client.ScopeAssignments)
		if len(affected) == 0 {
			decision = schemaLineageDecision{Action: SchemaActionReplace}
		} else {
			decision = schemaLineageDecision{Action: SchemaActionRebuildLocal}
		}
	} else if !fresh {
		decision = resolveSchemaLineage(model.state, source)
		affected = intersectAssignedScopes(client.ScopeAssignments, decision.AffectedScopes)
		if decision.Action == SchemaActionReplace && len(affected) != 0 {
			decision.Action = SchemaActionRebuildLocal
		}
	}

	if decision.Action == SchemaActionUnsupported {
		model.state.Clients[clientKey] = client
		local.ClientGeneration = client.CurrentGeneration
		if err := transitionLocalToError(model, &local, decision.Reason, false); err != nil {
			return StepResult{}, fmt.Errorf("record unsupported schema: %w", err)
		}
		model.state.ClientLocal[clientKey] = local
		appendClientEvent(model, ModelEventLocalApplyFailure, clientKey, decision.Reason)
		return StepResult{
			Kind: StepResultKindConnect,
			HTTP: &HTTPObservation{Status: 200},
			Connect: &ConnectObservation{
				Client:          clientKey,
				Generation:      client.CurrentGeneration,
				ScopeSetVersion: client.ScopeSetVersion,
				Schema: SchemaObservation{
					Source: source,
					Target: model.state.CurrentSchema,
					Action: SchemaActionUnsupported,
					Reason: decision.Reason,
				},
			},
		}, nil
	}

	cursorPlans, err := planConnectCursors(model, clientKey, client, local, source, fresh, decision.Action, affected, seedReceipts)
	if err != nil {
		return StepResult{}, err
	}
	seedRemoved := cleanupUnassignedSeedScopes(&local, client)
	added, removed, err := applyConnectAssignments(&client, &local)
	if err != nil {
		return StepResult{}, err
	}
	removed = appendUniqueScopeIDs(removed, seedRemoved)
	if decision.Action == SchemaActionReplace || decision.Action == SchemaActionRebuildLocal {
		if err := transitionClientLifecycleForConnect(model, &local, ClientLifecycleSchemaApplying); err != nil {
			return StepResult{}, err
		}
		if err := appendSchemaJournal(&local, source, model.state.CurrentSchema, currentManifest, decision.Action, affected, hasCursorReplacement(cursorPlans)); err != nil {
			return StepResult{}, fmt.Errorf("persist schema journal: %w", err)
		}
		local.CurrentSchema = model.state.CurrentSchema
	}
	if decision.Action == SchemaActionNone && local.CurrentSchema == (SchemaRef{}) {
		return StepResult{}, errors.New("current-schema connect requires a local schema")
	}
	if err := applyConnectCursorPlans(&client, &local, cursorPlans); err != nil {
		return StepResult{}, err
	}
	local.AuthoritativeScopeSetVersion = client.ScopeSetVersion
	if local.Backoff != nil && local.Backoff.InterruptedLifecycle == ClientLifecycleConnecting {
		local.Backoff = nil
	}
	if local.ErrorState != nil && local.ErrorState.Acknowledged {
		local.ErrorState = nil
	}

	finalState := ClientLifecycleReady
	if decision.Action == SchemaActionRebuildLocal && !clientHasSendableIntent(local) {
		finalState = ClientLifecycleRebuilding
	}
	if err := transitionClientLifecycleForConnect(model, &local, finalState); err != nil {
		return StepResult{}, err
	}
	model.state.Clients[clientKey] = client
	model.state.ClientLocal[clientKey] = local
	appendClientEvent(model, ModelEventConnected, clientKey, "")

	return StepResult{
		Kind: StepResultKindConnect,
		HTTP: &HTTPObservation{Status: 200},
		Connect: &ConnectObservation{
			Client:          clientKey,
			Generation:      client.CurrentGeneration,
			ScopeSetVersion: client.ScopeSetVersion,
			Schema: SchemaObservation{
				Source:         source,
				Target:         model.state.CurrentSchema,
				Action:         decision.Action,
				AffectedScopes: cloneScopeIDs(affected),
			},
			AddedScopes:   added,
			RemovedScopes: removed,
			ScopeCursors:  cursorObservations(cursorPlans),
		},
	}, nil
}

func validateConnectEnvelope(request connectPayload) ([]ScopeID, map[ScopeID]seedReceiptSource, error) {
	if request.RuntimeVersion == nil || *request.RuntimeVersion > maxProtocolCounter {
		return nil, nil, errors.New("runtime_version is required and must be in range")
	}
	if request.ProtocolVersion == nil {
		return nil, nil, errors.New("protocol_version is required")
	}
	if request.SchemaReset == nil {
		return nil, nil, errors.New("schema_reset is required")
	}
	if request.Schema == nil {
		return nil, nil, errors.New("schema is required")
	}
	if request.ScopeSetVersion == nil || *request.ScopeSetVersion > maxProtocolCounter {
		return nil, nil, errors.New("scope_set_version is required and must be in range")
	}
	if request.KnownScopes == nil {
		return nil, nil, errors.New("known_scopes is required")
	}
	if request.ClientGeneration != nil && (*request.ClientGeneration == 0 || *request.ClientGeneration > maxProtocolCounter) {
		return nil, nil, errors.New("client_generation must be positive and in range when present")
	}
	scopes := make([]ScopeID, 0, len(*request.KnownScopes))
	for index, known := range *request.KnownScopes {
		if known.ScopeID == nil || *known.ScopeID == "" {
			return nil, nil, fmt.Errorf("known scope %d has no scope_id", index)
		}
		scopeID := ScopeID(*known.ScopeID)
		if containsScopeID(scopes, scopeID) {
			return nil, nil, fmt.Errorf("known scope %d duplicates scope_id", index)
		}
		scopes = append(scopes, scopeID)
	}
	sortScopeIDs(scopes)
	receipts := make(map[ScopeID]seedReceiptSource)
	if request.SeedReceipts == nil {
		return scopes, receipts, nil
	}
	if len(*request.SeedReceipts) == 0 {
		return nil, nil, errors.New("seed_receipts must be nonempty when present")
	}
	for rawScope, source := range *request.SeedReceipts {
		if rawScope == "" || source != seedReceiptSourceLocal {
			return nil, nil, errors.New("seed_receipts contains an invalid scope or source")
		}
		receipts[ScopeID(rawScope)] = source
	}
	return scopes, receipts, nil
}

func runtimeTooOld(installation InstallationCapabilities, runtime uint64) bool {
	if installation.MinimumClientRuntime <= 0 {
		return false
	}
	return runtime < uint64(installation.MinimumClientRuntime)
}

func hasDurablePriorClientState(client ClientState, clientKnown bool, local ClientLocalState, localKnown bool) bool {
	if clientKnown && (client.CurrentGeneration != 0 || len(client.Generations) != 0) {
		return true
	}
	return localKnown && (local.ClientGeneration != 0 || local.CurrentSchema != (SchemaRef{}))
}

func prepareConnectLocal(model *Model, clientKey ClientKey) (ClientLocalState, error) {
	local, found := model.state.ClientLocal[clientKey]
	if !found {
		local = ClientLocalState{Lifecycle: ClientLifecycleState{State: ClientLifecycleUninitialized}}
	}
	switch local.Lifecycle.State {
	case ClientLifecycleUninitialized:
		if _, err := transitionClientLifecycle(model, &local, ClientLifecycleLocalReady); err != nil {
			return ClientLocalState{}, err
		}
		fallthrough
	case ClientLifecycleLocalReady, ClientLifecycleReady, ClientLifecycleBackoff:
		if _, err := transitionClientLifecycle(model, &local, ClientLifecycleConnecting); err != nil {
			return ClientLocalState{}, err
		}
	case ClientLifecycleConnecting:
	default:
		return ClientLocalState{}, fmt.Errorf("connect is illegal from lifecycle %q", local.Lifecycle.State)
	}
	return local, nil
}

func connectFailure(model *Model, clientKey ClientKey, source SchemaRef, status int, code HTTPCode, reason ReasonCode) (StepResult, error) {
	local, found := model.state.ClientLocal[clientKey]
	if found && local.Lifecycle.State == ClientLifecycleError {
		local.ErrorState = &ClientErrorState{Reason: reason, Retryable: false, At: modelNow(model)}
	} else {
		var err error
		local, err = prepareConnectLocal(model, clientKey)
		if err != nil {
			return StepResult{}, err
		}
		if err := transitionLocalToError(model, &local, reason, false); err != nil {
			return StepResult{}, fmt.Errorf("record connect error: %w", err)
		}
	}
	model.state.ClientLocal[clientKey] = local
	appendClientEvent(model, ModelEventLocalApplyFailure, clientKey, reason)
	client := model.state.Clients[clientKey]
	return StepResult{
		Kind: StepResultKindConnect,
		HTTP: &HTTPObservation{Status: status, HasCode: true, Code: code, Retryable: false},
		Connect: &ConnectObservation{
			Client:          clientKey,
			Generation:      client.CurrentGeneration,
			ScopeSetVersion: client.ScopeSetVersion,
			Schema: SchemaObservation{
				Source: source,
				Target: model.state.CurrentSchema,
				Action: SchemaActionUnsupported,
				Reason: reason,
			},
		},
	}, nil
}

func knownScopesMatchLocalState(local ClientLocalState, known []ScopeID) error {
	localScopes := make([]ScopeID, 0, len(local.ScopeAssignments))
	for _, assignment := range local.ScopeAssignments {
		if assignment.Assigned {
			localScopes = append(localScopes, assignment.Scope)
		}
	}
	sortScopeIDs(localScopes)
	if len(localScopes) != len(known) {
		return errors.New("known_scopes does not match durable local assignments")
	}
	for index := range localScopes {
		if localScopes[index] != known[index] {
			return errors.New("known_scopes does not match durable local assignments")
		}
	}
	return nil
}

func affectedAssignedScopes(client ClientState, local ClientLocalState) []ScopeID {
	affected := make([]ScopeID, 0, len(client.ScopeAssignments))
	for _, assignment := range client.ScopeAssignments {
		if !assignment.Assigned {
			continue
		}
		localIndex, exists := findLocalScopeAssignment(local.ScopeAssignments, assignment.Scope)
		if !exists {
			affected = append(affected, assignment.Scope)
			continue
		}
		localAssignment := local.ScopeAssignments[localIndex]
		if !localAssignment.Assigned || localAssignment.MembershipGeneration != assignment.MembershipGeneration || localAssignment.RetentionGeneration != assignment.RetentionGeneration {
			affected = append(affected, assignment.Scope)
		}
	}
	sortScopeIDs(affected)
	return affected
}

func assignedScopeIDs(assignments []ScopeAssignment) []ScopeID {
	result := make([]ScopeID, 0, len(assignments))
	for _, assignment := range assignments {
		if assignment.Assigned {
			result = append(result, assignment.Scope)
		}
	}
	sortScopeIDs(result)
	return result
}

func intersectAssignedScopes(assignments []ScopeAssignment, affected []ScopeID) []ScopeID {
	result := make([]ScopeID, 0, len(affected))
	for _, assignment := range assignments {
		if assignment.Assigned && containsScopeID(affected, assignment.Scope) {
			result = append(result, assignment.Scope)
		}
	}
	sortScopeIDs(result)
	return result
}

func planConnectCursors(model *Model, clientKey ClientKey, client ClientState, local ClientLocalState, source SchemaRef, fresh bool, action SchemaAction, affected []ScopeID, seedReceipts map[ScopeID]seedReceiptSource) ([]connectCursorPlan, error) {
	plans := make([]connectCursorPlan, 0, len(client.ScopeAssignments))
	now := model.clock.Now().Round(0).UTC()
	for _, assignment := range client.ScopeAssignments {
		if !assignment.Assigned {
			continue
		}
		if _, presented := seedReceipts[assignment.Scope]; presented {
			if receipt, found := localSeedReceipt(local, assignment.Scope); found &&
				seedReceiptUsable(model, clientKey, assignment, receipt, source, action, now) {
				token, err := mintCurrentSchemaCursor(model, clientKey, client.CurrentGeneration, assignment, receipt.SnapshotBoundary, now)
				if err != nil {
					return nil, err
				}
				plans = append(plans, connectCursorPlan{Scope: assignment.Scope, Disposition: CursorDispositionIssued, Replace: true, Position: receipt.SnapshotBoundary, Cursor: token, SeedReceipt: true})
			} else {
				plans = append(plans, connectCursorPlan{Scope: assignment.Scope, Disposition: CursorDispositionRebuildRequired, Invalidate: true, SeedReceipt: true})
			}
			continue
		}
		localAssignmentIndex, exists := findLocalScopeAssignment(local.ScopeAssignments, assignment.Scope)
		if !exists {
			plans = append(plans, connectCursorPlan{Scope: assignment.Scope, Disposition: CursorDispositionRebuildRequired, Invalidate: true})
			continue
		}
		localAssignment := local.ScopeAssignments[localAssignmentIndex]
		if !localAssignment.Assigned || assignment.RebuildRequired || localAssignment.MembershipGeneration != assignment.MembershipGeneration || localAssignment.RetentionGeneration != assignment.RetentionGeneration || containsScopeID(affected, assignment.Scope) {
			plans = append(plans, connectCursorPlan{Scope: assignment.Scope, Disposition: CursorDispositionRebuildRequired, Invalidate: true})
			continue
		}
		checkpointIndex, hasCheckpoint := findLocalScopeCheckpoint(local.ScopeCheckpoints, assignment.Scope)
		if !hasCheckpoint {
			plans = append(plans, connectCursorPlan{Scope: assignment.Scope, Disposition: CursorDispositionRebuildRequired, Invalidate: true})
			continue
		}
		checkpoint := local.ScopeCheckpoints[checkpointIndex]
		if fresh || !connectCursorUsable(model, clientKey, client.CurrentGeneration, source, assignment, checkpoint, now) {
			plans = append(plans, connectCursorPlan{Scope: assignment.Scope, Disposition: CursorDispositionRebuildRequired, Invalidate: true})
			continue
		}
		if action == SchemaActionReplace && source != model.state.CurrentSchema {
			token, err := mintCurrentSchemaCursor(model, clientKey, client.CurrentGeneration, assignment, checkpoint.Position, now)
			if err != nil {
				return nil, err
			}
			plans = append(plans, connectCursorPlan{Scope: assignment.Scope, Disposition: CursorDispositionIssued, Replace: true, Position: checkpoint.Position, Cursor: token})
			continue
		}
		plans = append(plans, connectCursorPlan{Scope: assignment.Scope, Disposition: CursorDispositionUnchanged})
	}
	sortConnectCursorPlans(plans)
	return plans, nil
}

func localSeedReceipt(local ClientLocalState, scope ScopeID) (LocalSeedReceipt, bool) {
	for _, receipt := range local.SeedReceipts {
		if receipt.Scope == scope {
			return receipt, true
		}
	}
	return LocalSeedReceipt{}, false
}

func seedReceiptUsable(model *Model, client ClientKey, assignment ScopeAssignment, receipt LocalSeedReceipt, source SchemaRef, action SchemaAction, now time.Time) bool {
	if !receipt.HasReceipt || receipt.Receipt == (OpaqueToken{}) || source != model.state.CurrentSchema || action != SchemaActionNone ||
		receipt.Schema != model.state.CurrentSchema || receipt.RegistryGeneration != model.state.Registry.CurrentGeneration ||
		receipt.MembershipGeneration != assignment.MembershipGeneration || receipt.RetentionGeneration != assignment.RetentionGeneration {
		return false
	}
	scope, found := model.state.Scopes[assignment.Scope]
	if !found || scope.MembershipGeneration != assignment.MembershipGeneration || scope.RetentionGeneration != assignment.RetentionGeneration || receipt.StreamGeneration != scope.StreamGeneration || !validPortableSeedBoundary(model.state, receipt.StreamGeneration, receipt.SnapshotBoundary) {
		return false
	}
	if floor, found := model.state.RetentionFloors[assignment.Scope]; found {
		if !retentionFloorMatchesScope(floor, scope) || !cursorPositionAtOrAboveFloor(receipt.SnapshotBoundary, floor) {
			return false
		}
	}
	local, found := model.state.ClientLocal[client]
	if !found || !localSeedScopeMatchesReceipt(local, receipt) {
		return false
	}
	bindings := localSeedReceiptBindings(receipt)
	return validateTokenAgainstCurrent(model.authority, receipt.Receipt, string(TokenKindSeedReceipt), bindings, now) == TokenStatusValid
}

func localSeedReceiptBindings(receipt LocalSeedReceipt) BindingSet {
	return BindingSet{
		HasRegistryGeneration: true, RegistryGeneration: receipt.RegistryGeneration,
		HasMembershipGeneration: true, MembershipGeneration: receipt.MembershipGeneration,
		HasRetentionGeneration: true, RetentionGeneration: receipt.RetentionGeneration,
		HasStreamGeneration: true, StreamGeneration: receipt.StreamGeneration,
		HasSchema: true, Schema: receipt.Schema,
		HasScope: true, Scope: receipt.Scope,
		HasSnapshotBoundary: true, SnapshotBoundary: receipt.SnapshotBoundary,
		HasExportID: true, ExportID: receipt.ExportID,
		HasExportManifestHash: true, ExportManifestHash: receipt.ExportManifestHash,
		HasCardinality: true, Cardinality: receipt.Cardinality,
		HasChecksum: true, Checksum: receipt.Checksum,
	}
}

func localSeedScopeMatchesReceipt(local ClientLocalState, receipt LocalSeedReceipt) bool {
	rows := make(map[RowIdentity]LocalRow, len(local.Rows))
	for _, row := range local.Rows {
		if _, duplicate := rows[row.Identity]; duplicate {
			return false
		}
		rows[row.Identity] = row
	}
	digestRows := make([]rebuildDigestRow, 0, receipt.Cardinality)
	seen := make(map[RowIdentity]struct{}, receipt.Cardinality)
	for _, provenance := range local.Provenance {
		if !containsScopeID(provenance.Scopes, receipt.Scope) {
			continue
		}
		if _, duplicate := seen[provenance.Row]; duplicate {
			return false
		}
		row, found := rows[provenance.Row]
		if !found || row.Deleted || !row.HasServerVersion || row.ServerVersion == "" || row.ServerVersion != provenance.Version || !row.HasChecksum || row.Checksum == (Checksum{}) {
			return false
		}
		seen[provenance.Row] = struct{}{}
		digestRows = append(digestRows, rebuildDigestRow{Identity: row.Identity, Checksum: row.Checksum})
	}
	checksum, valid := referenceScopeChecksum(receipt.Schema, receipt.Scope, digestRows)
	return valid && Cardinality(len(digestRows)) == receipt.Cardinality && checksum == receipt.Checksum
}

func connectCursorUsable(model *Model, clientKey ClientKey, generation Generation, schema SchemaRef, assignment ScopeAssignment, checkpoint LocalScopeCheckpoint, now time.Time) bool {
	if !checkpoint.HasCursor || checkpoint.Cursor == (OpaqueToken{}) {
		return false
	}
	scope, found := model.state.Scopes[assignment.Scope]
	if !found || scope.MembershipGeneration != assignment.MembershipGeneration || scope.RetentionGeneration != assignment.RetentionGeneration {
		return false
	}
	if floor, hasFloor := model.state.RetentionFloors[assignment.Scope]; hasFloor {
		if !retentionFloorMatchesScope(floor, scope) || !cursorPositionAtOrAboveFloor(checkpoint.Position, floor) {
			return false
		}
	}
	bindings, err := connectIncrementalCursorBindings(model, clientKey, generation, assignment, checkpoint.Position, schema, nil)
	if err != nil {
		return false
	}
	return validateTokenAgainstCurrent(model.authority, checkpoint.Cursor, string(TokenKindIncrementalCursor), bindings, now) == TokenStatusValid
}

func mintCurrentSchemaCursor(model *Model, clientKey ClientKey, generation Generation, assignment ScopeAssignment, position StreamPosition, now time.Time) (OpaqueToken, error) {
	bindings, err := connectIncrementalCursorBindings(model, clientKey, generation, assignment, position, model.state.CurrentSchema, &now)
	if err != nil {
		return OpaqueToken{}, err
	}
	token := model.authority.Mint(string(TokenKindIncrementalCursor), bindings)
	if token == (OpaqueToken{}) {
		return OpaqueToken{}, errors.New("incremental cursor allocation failed")
	}
	return token, nil
}

func connectIncrementalCursorBindings(model *Model, clientKey ClientKey, generation Generation, assignment ScopeAssignment, position StreamPosition, schema SchemaRef, issuedAt *time.Time) (BindingSet, error) {
	if generation == 0 || assignment.Scope == "" || assignment.MembershipGeneration == 0 || assignment.RetentionGeneration == 0 || schema == (SchemaRef{}) {
		return BindingSet{}, errors.New("cursor binding has an incomplete client, scope, or schema identity")
	}
	scope, found := model.state.Scopes[assignment.Scope]
	if !found || scope.StreamGeneration == "" || scope.MembershipGeneration != assignment.MembershipGeneration || scope.RetentionGeneration != assignment.RetentionGeneration {
		return BindingSet{}, errors.New("cursor binding has an obsolete scope lineage")
	}
	if position.StreamGeneration != scope.StreamGeneration {
		return BindingSet{}, errors.New("cursor position has another stream generation")
	}
	bindings := BindingSet{
		HasUser:                 true,
		User:                    clientKey.UserID,
		HasClient:               true,
		Client:                  clientKey,
		HasClientGeneration:     true,
		ClientGeneration:        generation,
		HasRegistryGeneration:   true,
		RegistryGeneration:      model.state.Registry.CurrentGeneration,
		HasMembershipGeneration: true,
		MembershipGeneration:    assignment.MembershipGeneration,
		HasRetentionGeneration:  true,
		RetentionGeneration:     assignment.RetentionGeneration,
		HasStreamGeneration:     true,
		StreamGeneration:        scope.StreamGeneration,
		HasSchema:               true,
		Schema:                  schema,
		HasScope:                true,
		Scope:                   assignment.Scope,
		HasStreamPosition:       true,
		StreamPosition:          position,
	}
	if issuedAt != nil {
		bindings.HasIssuedAt = true
		bindings.IssuedAt = issuedAt.Round(0).UTC()
	}
	return bindings, nil
}

func applyConnectAssignments(client *ClientState, local *ClientLocalState) ([]ScopeID, []ScopeID, error) {
	if client == nil || local == nil {
		return nil, nil, errors.New("client and local state are required")
	}
	serverScopes := assignedScopeIDs(client.ScopeAssignments)
	removed := make([]ScopeID, 0, len(local.ScopeAssignments))
	for _, assignment := range local.ScopeAssignments {
		if assignment.Assigned && !containsScopeID(serverScopes, assignment.Scope) {
			removed = append(removed, assignment.Scope)
		}
	}
	for _, scopeID := range removed {
		removeLocalScopeState(local, scopeID)
	}

	added := make([]ScopeID, 0, len(client.ScopeAssignments))
	for index := range client.ScopeAssignments {
		assignment := &client.ScopeAssignments[index]
		if !assignment.Assigned {
			continue
		}
		if assignment.Scope == "" || assignment.MembershipGeneration == 0 || assignment.RetentionGeneration == 0 {
			return nil, nil, errors.New("authoritative assignment has incomplete scope lineage")
		}
		localIndex, exists := findLocalScopeAssignment(local.ScopeAssignments, assignment.Scope)
		if !exists {
			added = append(added, assignment.Scope)
			local.ScopeAssignments = append(local.ScopeAssignments, LocalScopeAssignment{
				Scope:                assignment.Scope,
				MembershipGeneration: assignment.MembershipGeneration,
				RetentionGeneration:  assignment.RetentionGeneration,
				Assigned:             true,
				RebuildRequired:      true,
			})
			local.ScopeCheckpoints = append(local.ScopeCheckpoints, LocalScopeCheckpoint{Scope: assignment.Scope})
			assignment.RebuildRequired = true
			continue
		}
		localAssignment := &local.ScopeAssignments[localIndex]
		if localAssignment.MembershipGeneration != assignment.MembershipGeneration || localAssignment.RetentionGeneration != assignment.RetentionGeneration {
			localAssignment.RebuildRequired = true
			assignment.RebuildRequired = true
			invalidateLocalScopeCheckpoint(local, assignment.Scope)
		}
		localAssignment.MembershipGeneration = assignment.MembershipGeneration
		localAssignment.RetentionGeneration = assignment.RetentionGeneration
		localAssignment.Assigned = true
		if assignment.RebuildRequired {
			localAssignment.RebuildRequired = true
		}
	}
	sortScopeIDs(added)
	sortScopeIDs(removed)
	return added, removed, nil
}

func applyConnectCursorPlans(client *ClientState, local *ClientLocalState, plans []connectCursorPlan) error {
	if client == nil || local == nil {
		return errors.New("client and local state are required")
	}
	for _, plan := range plans {
		localIndex, localFound := findLocalScopeAssignment(local.ScopeAssignments, plan.Scope)
		clientIndex, clientFound := findScopeAssignment(client.ScopeAssignments, plan.Scope)
		if !localFound || !clientFound {
			return errors.New("cursor plan does not identify an active assignment")
		}
		localAssignment := &local.ScopeAssignments[localIndex]
		clientAssignment := &client.ScopeAssignments[clientIndex]
		switch plan.Disposition {
		case CursorDispositionIssued:
			if !plan.Replace {
				return errors.New("issued connect cursor has no replacement")
			}
			if plan.SeedReceipt {
				if err := installLocalSeedCursor(local, plan.Scope, plan.Position, plan.Cursor); err != nil {
					return err
				}
			} else if err := installLocalScopeCursor(local, plan.Scope, plan.Position, plan.Cursor); err != nil {
				return err
			}
			localAssignment.RebuildRequired = false
			clientAssignment.RebuildRequired = false
			if plan.SeedReceipt {
				consumeLocalSeedReceipt(local, plan.Scope)
			}
		case CursorDispositionUnchanged:
			if plan.Replace || plan.Invalidate {
				return errors.New("unchanged connect cursor has a hidden state change")
			}
		case CursorDispositionRebuildRequired:
			if !plan.Invalidate {
				return errors.New("rebuild-required connect cursor was not invalidated")
			}
			invalidateLocalScopeCheckpoint(local, plan.Scope)
			localAssignment.RebuildRequired = true
			clientAssignment.RebuildRequired = true
			if plan.SeedReceipt {
				consumeLocalSeedReceipt(local, plan.Scope)
			}
		default:
			return errors.New("connect cursor disposition is unknown")
		}
	}
	return nil
}

func installLocalSeedCursor(local *ClientLocalState, scope ScopeID, position StreamPosition, cursor OpaqueToken) error {
	if local == nil {
		return errors.New("local state is required")
	}
	if _, found := findLocalScopeCheckpoint(local.ScopeCheckpoints, scope); !found {
		local.ScopeCheckpoints = append(local.ScopeCheckpoints, LocalScopeCheckpoint{Scope: scope})
	}
	return installLocalScopeCursor(local, scope, position, cursor)
}

func consumeLocalSeedReceipt(local *ClientLocalState, scope ScopeID) {
	if local == nil {
		return
	}
	for index := 0; index < len(local.SeedReceipts); {
		if local.SeedReceipts[index].Scope == scope {
			local.SeedReceipts = append(local.SeedReceipts[:index], local.SeedReceipts[index+1:]...)
			continue
		}
		index++
	}
}

func cleanupUnassignedSeedScopes(local *ClientLocalState, client ClientState) []ScopeID {
	if local == nil {
		return nil
	}
	removed := make([]ScopeID, 0)
	for _, receipt := range append([]LocalSeedReceipt(nil), local.SeedReceipts...) {
		assignmentIndex, found := findScopeAssignment(client.ScopeAssignments, receipt.Scope)
		if found && client.ScopeAssignments[assignmentIndex].Assigned {
			continue
		}
		removeLocalScopeState(local, receipt.Scope)
		removed = appendUniqueScopeIDs(removed, []ScopeID{receipt.Scope})
	}
	return removed
}

func appendUniqueScopeIDs(scopes, additions []ScopeID) []ScopeID {
	for _, scope := range additions {
		if !containsScopeID(scopes, scope) {
			scopes = append(scopes, scope)
		}
	}
	sortScopeIDs(scopes)
	return scopes
}

func transitionClientLifecycleForConnect(model *Model, local *ClientLocalState, next ClientLifecycle) error {
	if local == nil {
		return errors.New("connect lifecycle is absent")
	}
	if _, err := transitionClientLifecycle(model, local, next); err != nil {
		return err
	}
	return nil
}

func hasCursorReplacement(plans []connectCursorPlan) bool {
	for _, plan := range plans {
		if plan.Replace {
			return true
		}
	}
	return false
}

func cursorObservations(plans []connectCursorPlan) []ScopeCursorObservation {
	observations := make([]ScopeCursorObservation, 0, len(plans))
	for _, plan := range plans {
		observations = append(observations, ScopeCursorObservation{Scope: plan.Scope, Disposition: plan.Disposition})
	}
	return observations
}

func sortConnectCursorPlans(plans []connectCursorPlan) {
	for left := 0; left < len(plans); left++ {
		for right := left + 1; right < len(plans); right++ {
			if plans[right].Scope < plans[left].Scope {
				plans[left], plans[right] = plans[right], plans[left]
			}
		}
	}
}
