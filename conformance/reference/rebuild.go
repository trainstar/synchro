package reference

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

const rebuildSessionLifetime = 24 * time.Hour

type rebuildRequestPayload struct {
	UserID           string                  `json:"user_id"`
	ClientID         string                  `json:"client_id"`
	ClientGeneration uint64                  `json:"client_generation"`
	Schema           *schemaReferencePayload `json:"schema"`
	ScopeID          string                  `json:"scope_id"`
	RebuildID        string                  `json:"rebuild_id"`
	CursorSource     tokenSource             `json:"cursor_source"`
	Limit            uint32                  `json:"limit"`
}

type localBeginRebuildPayload struct {
	UserID           string                  `json:"user_id"`
	ClientID         string                  `json:"client_id"`
	ClientGeneration uint64                  `json:"client_generation"`
	Schema           *schemaReferencePayload `json:"schema"`
	ScopeID          string                  `json:"scope_id"`
	RebuildID        string                  `json:"rebuild_id"`
	Limit            uint32                  `json:"limit"`
}

type localApplyRebuildPagePayload struct {
	UserID             string      `json:"user_id"`
	ClientID           string      `json:"client_id"`
	ScopeID            string      `json:"scope_id"`
	RebuildID          string      `json:"rebuild_id"`
	PageOrdinal        uint64      `json:"page_ordinal"`
	RequestTokenSource tokenSource `json:"request_token_source"`
}

type localFinalizeRebuildPayload struct {
	UserID    string `json:"user_id"`
	ClientID  string `json:"client_id"`
	ScopeID   string `json:"scope_id"`
	RebuildID string `json:"rebuild_id"`
}

type rebuildDigestRow struct {
	Identity RowIdentity
	Checksum Checksum
}

type rebuildStoredResponse struct {
	PageOrdinal     uint64                `json:"page_ordinal"`
	Records         []rebuildStoredRecord `json:"records"`
	HasContinuation bool                  `json:"has_continuation"`
	HasFinalCursor  bool                  `json:"has_final_cursor"`
	HasChecksum     bool                  `json:"has_checksum"`
	Checksum        Checksum              `json:"checksum"`
}

type rebuildStoredRecord struct {
	Identity     RowIdentity  `json:"identity"`
	FieldValues  []FieldValue `json:"field_values"`
	Version      RowVersion   `json:"version"`
	Deleted      bool         `json:"deleted"`
	DeletedAt    *time.Time   `json:"deleted_at"`
	DeleteReason *string      `json:"delete_reason"`
	UpdatedAt    *time.Time   `json:"updated_at"`
	Checksum     Checksum     `json:"checksum"`
}

func init() {
	registerOperation("rebuild/request-page", rebuildRequestPage)
	registerOperation("local/begin-rebuild", localBeginRebuild)
	registerOperation("local/apply-rebuild-page", localApplyRebuildPage)
	registerOperation("local/finalize-rebuild", localFinalizeRebuild)
}

func rebuildRequestPage(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	request, err := decodeRebuildRequest(payload)
	if err != nil {
		return rebuildEndpointError(400, "invalid_request", false), nil
	}
	if request.Limit > model.state.ConfiguredLimits.RebuildMaximum {
		return rebuildEndpointError(400, "invalid_request", false), nil
	}

	client := ClientKey{UserID: UserID(request.UserID), ClientID: ClientID(request.ClientID)}
	clientState, gate := endpointClientGate(model.state, client, Generation(request.ClientGeneration), request.Schema)
	if gate != nil {
		return rebuildEndpointError(gate.Status, gate.Code, gate.Retryable), nil
	}
	scope := ScopeID(request.ScopeID)
	assignment, assigned := scopeAssignmentFor(clientState, scope)
	if !assigned || !assignment.Assigned {
		return rebuildEndpointError(400, "invalid_request", false), nil
	}
	scopeState, exists := model.state.Scopes[scope]
	if !exists {
		return rebuildEndpointError(500, "sync_integrity_failure", false), nil
	}
	key := RebuildKey{Client: client, Scope: scope, Rebuild: RebuildID(request.RebuildID)}
	now := referenceNow(model.clock)

	if request.CursorSource == tokenSourceNone {
		if session, found := model.state.Rebuilds[key]; found {
			if request.Limit != session.PageLimit {
				return rebuildEndpointError(400, "invalid_request", false), nil
			}
			if !rebuildSessionCurrent(model.state, clientState, assignment, scopeState, session, now) {
				return rebuildEndpointError(409, "rebuild_restart_required", false), nil
			}
			if hasPendingAcceptedFence(model.state, client) {
				return rebuildEndpointError(503, "capture_pending", true), nil
			}
			page, found := rebuildPageForRequest(session, false, OpaqueToken{}, 1)
			if !found {
				return rebuildEndpointError(503, "temporary_unavailable", true), nil
			}
			result, valid := rebuildPageResult(key, page, true)
			if !valid {
				return rebuildEndpointError(500, "sync_integrity_failure", false), nil
			}
			return result, nil
		}

		if hasPendingAcceptedFence(model.state, client) {
			return rebuildEndpointError(503, "capture_pending", true), nil
		}
		session, staged := stageRebuildSession(model.state, clientState, assignment, scopeState, key, request.Limit, now)
		if !staged {
			return rebuildEndpointError(503, "temporary_unavailable", true), nil
		}
		if !canMintTokens(model.authority, 1) {
			return rebuildEndpointError(500, "sync_integrity_failure", false), nil
		}
		updated, page, pageOK, err := createRebuildPage(model, client, assignment, scopeState, key.Rebuild, session, false, OpaqueToken{}, 1, now)
		if err != nil {
			return StepResult{}, err
		}
		if !pageOK {
			return rebuildEndpointError(503, "temporary_unavailable", true), nil
		}
		model.state.Rebuilds[key] = updated
		result, valid := rebuildPageResult(key, page, false)
		if !valid {
			return StepResult{}, fmt.Errorf("create rebuild page: generated an invalid page")
		}
		return result, nil
	}

	token, found := resolveModelToken(model, client, scope, RebuildID(request.RebuildID), request.CursorSource)
	if !found {
		return rebuildEndpointError(400, "invalid_request", false), nil
	}
	session, hasSession := model.state.Rebuilds[key]
	status := validateRebuildContinuation(model, client, clientState, assignment, scopeState, key, session, hasSession, request.Limit, token, now)
	switch status {
	case TokenStatusValid:
	case TokenStatusStale:
		return rebuildEndpointError(409, "rebuild_restart_required", false), nil
	default:
		return rebuildEndpointError(400, "invalid_request", false), nil
	}
	if !hasSession || !rebuildSessionCurrent(model.state, clientState, assignment, scopeState, session, now) {
		return rebuildEndpointError(409, "rebuild_restart_required", false), nil
	}
	if hasPendingAcceptedFence(model.state, client) {
		return rebuildEndpointError(503, "capture_pending", true), nil
	}
	page, found := rebuildPageForToken(session, token)
	if found {
		result, valid := rebuildPageResult(key, page, true)
		if !valid {
			return rebuildEndpointError(500, "sync_integrity_failure", false), nil
		}
		return result, nil
	}
	stored, tokenFound := tokenBindings(model.authority, token)
	if !tokenFound || !stored.HasOrdinal || !session.HasContinuation || session.Continuation != token {
		return rebuildEndpointError(409, "rebuild_restart_required", false), nil
	}
	if !canMintTokens(model.authority, 1) {
		return rebuildEndpointError(500, "sync_integrity_failure", false), nil
	}
	updated, page, pageOK, err := createRebuildPage(model, client, assignment, scopeState, key.Rebuild, session, true, token, stored.Ordinal, now)
	if err != nil {
		return StepResult{}, err
	}
	if !pageOK {
		return rebuildEndpointError(409, "rebuild_restart_required", false), nil
	}
	model.state.Rebuilds[key] = updated
	result, valid := rebuildPageResult(key, page, false)
	if !valid {
		return rebuildEndpointError(500, "sync_integrity_failure", false), nil
	}
	return result, nil
}

func decodeRebuildRequest(payload json.RawMessage) (rebuildRequestPayload, error) {
	var request rebuildRequestPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return rebuildRequestPayload{}, err
	}
	if request.UserID == "" || request.ClientID == "" || request.ClientGeneration == 0 || request.ClientGeneration > maxReferenceJSONInteger || request.ScopeID == "" || !validCanonicalUUID(request.RebuildID) || request.Limit == 0 || !validTokenSource(request.CursorSource) {
		return rebuildRequestPayload{}, fmt.Errorf("rebuild request is invalid")
	}
	if _, _, err := decodeSchemaReference(request.Schema, false); err != nil {
		return rebuildRequestPayload{}, fmt.Errorf("rebuild request schema: %w", err)
	}
	return request, nil
}

func validCanonicalUUID(value string) bool {
	if len(value) != 36 || value == "00000000-0000-0000-0000-000000000000" {
		return false
	}
	for index := 0; index < len(value); index++ {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if value[index] != '-' {
				return false
			}
			continue
		}
		if (value[index] < '0' || value[index] > '9') && (value[index] < 'a' || value[index] > 'f') {
			return false
		}
	}
	return true
}

func rebuildEndpointError(status int, code HTTPCode, retryable bool) StepResult {
	return StepResult{
		Kind:    StepResultKindRebuild,
		HTTP:    endpointHTTPObservation(status, code, retryable),
		Rebuild: &RebuildObservation{},
	}
}

func rebuildSessionCurrent(state State, client ClientState, assignment ScopeAssignment, scopeState ScopeState, session RebuildSession, now time.Time) bool {
	if session.Status != RebuildStatusStaged && session.Status != RebuildStatusComplete || session.ExpiresAt == nil || !now.Before(session.ExpiresAt.Round(0).UTC()) {
		return false
	}
	return session.ClientGeneration == client.CurrentGeneration &&
		session.Schema == state.CurrentSchema &&
		session.MembershipGeneration == assignment.MembershipGeneration &&
		session.RetentionGeneration == assignment.RetentionGeneration &&
		session.StreamGeneration == scopeState.StreamGeneration &&
		session.SnapshotBoundary.StreamGeneration == scopeState.StreamGeneration &&
		session.AcceptedWriteEpoch == client.AcceptedWriteEpoch &&
		session.PageLimit > 0
}

func stageRebuildSession(state State, clientState ClientState, assignment ScopeAssignment, scopeState ScopeState, key RebuildKey, limit uint32, now time.Time) (RebuildSession, bool) {
	if limit == 0 || scopeState.StreamGeneration == "" {
		return RebuildSession{}, false
	}
	rows := make([]RebuildStagedRow, 0, len(scopeState.Membership))
	seen := make(map[RowIdentity]struct{}, len(scopeState.Membership))
	for _, membership := range scopeState.Membership {
		if !membership.Included {
			continue
		}
		if _, duplicate := seen[membership.Row]; duplicate {
			return RebuildSession{}, false
		}
		seen[membership.Row] = struct{}{}
		row, exists := state.Rows[membership.Row]
		if !exists || row.Identity != membership.Row || row.Identity.CanonicalIdentityBytes == "" || row.Version == "" {
			return RebuildSession{}, false
		}
		stagedAt := now
		rows = append(rows, RebuildStagedRow{Row: cloneAuthoritativeRow(row), StagedAt: &stagedAt})
	}
	sort.Slice(rows, func(left, right int) bool {
		if rows[left].Row.Identity.TableID != rows[right].Row.Identity.TableID {
			return rows[left].Row.Identity.TableID < rows[right].Row.Identity.TableID
		}
		return lessRowIdentity(rows[left].Row.Identity, rows[right].Row.Identity)
	})
	for index := range rows {
		rows[index].Ordinal = uint64(index + 1)
	}
	checksum, valid := rebuildChecksum(state.CurrentSchema, key.Scope, rows)
	if !valid {
		return RebuildSession{}, false
	}
	createdAt := now
	expiresAt := now.Add(rebuildSessionLifetime).Round(0).UTC()
	return RebuildSession{
		SessionID:            generatedRebuildSessionID(key),
		ClientGeneration:     clientState.CurrentGeneration,
		Scope:                key.Scope,
		Schema:               state.CurrentSchema,
		MembershipGeneration: assignment.MembershipGeneration,
		RetentionGeneration:  assignment.RetentionGeneration,
		StreamGeneration:     scopeState.StreamGeneration,
		SnapshotBoundary:     rebuildSnapshotBoundary(state, scopeState.StreamGeneration),
		PageLimit:            limit,
		StagedRows:           rows,
		NextRowOrdinal:       1,
		Checksum:             checksum,
		CreatedAt:            &createdAt,
		ExpiresAt:            &expiresAt,
		AcceptedWriteEpoch:   clientState.AcceptedWriteEpoch,
		Status:               RebuildStatusStaged,
	}, true
}

func generatedRebuildSessionID(key RebuildKey) SessionID {
	digest := sha256.Sum256([]byte(string(key.Client.UserID) + "\x00" + string(key.Client.ClientID) + "\x00" + string(key.Scope) + "\x00" + string(key.Rebuild)))
	return SessionID(fmt.Sprintf("reference-session-%x", digest[:16]))
}

func rebuildSnapshotBoundary(state State, generation StreamGeneration) StreamPosition {
	boundary := currentPullBoundary(state)
	if boundary.StreamGeneration == "" {
		boundary.StreamGeneration = generation
	}
	if boundary.StreamGeneration != generation {
		return StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart}
	}
	if boundary.Kind == PositionKindGenerationStart || boundary.Kind == PositionKindTransactionEnd {
		return boundary
	}
	return StreamPosition{
		StreamGeneration: generation,
		Kind:             PositionKindTransactionEnd,
		CommitLSN:        boundary.CommitLSN,
	}
}

func rebuildChecksum(schema SchemaRef, scope ScopeID, rows []RebuildStagedRow) (Checksum, bool) {
	digestRows := make([]rebuildDigestRow, 0, len(rows))
	for _, row := range rows {
		digestRows = append(digestRows, rebuildDigestRow{Identity: row.Row.Identity, Checksum: row.Row.Checksum})
	}
	return referenceScopeChecksum(schema, scope, digestRows)
}

func referenceScopeChecksum(schema SchemaRef, scope ScopeID, rows []rebuildDigestRow) (Checksum, bool) {
	if scope == "" {
		return Checksum{}, false
	}
	ordered := append([]rebuildDigestRow(nil), rows...)
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].Identity.CanonicalIdentityBytes < ordered[right].Identity.CanonicalIdentityBytes
	})
	for index, row := range ordered {
		if row.Identity.CanonicalIdentityBytes == "" || row.Identity.TableID == "" || row.Identity.PrimaryKeyFieldID == "" || row.Identity.PortableType == "" {
			return Checksum{}, false
		}
		if index > 0 && ordered[index-1].Identity.CanonicalIdentityBytes == row.Identity.CanonicalIdentityBytes {
			return Checksum{}, false
		}
	}

	hash := sha256.New()
	_, _ = hash.Write([]byte("synchro:v3:scope-digest:v1\x00"))
	_, _ = hash.Write(schema.Hash[:])
	writeRebuildDigestText(hash, string(scope))
	writeRebuildDigestUint64(hash, uint64(len(ordered)))
	for _, row := range ordered {
		identity := []byte(row.Identity.CanonicalIdentityBytes)
		writeRebuildDigestUint64(hash, uint64(len(identity)))
		_, _ = hash.Write(identity)
		_, _ = hash.Write(row.Checksum[:])
	}
	var checksum Checksum
	copy(checksum[:], hash.Sum(nil))
	return checksum, true
}

func writeRebuildDigestText(hash interface{ Write([]byte) (int, error) }, value string) {
	writeRebuildDigestUint64(hash, uint64(len([]byte(value))))
	_, _ = hash.Write([]byte(value))
}

func writeRebuildDigestUint64(hash interface{ Write([]byte) (int, error) }, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = hash.Write(encoded[:])
}

func createRebuildPage(model *Model, client ClientKey, assignment ScopeAssignment, scopeState ScopeState, rebuild RebuildID, session RebuildSession, hasRequestToken bool, requestToken OpaqueToken, requestOrdinal uint64, now time.Time) (RebuildSession, RebuildPage, bool, error) {
	if requestOrdinal == 0 || requestOrdinal != session.NextRowOrdinal {
		return RebuildSession{}, RebuildPage{}, false, nil
	}
	start := len(session.StagedRows)
	for index, row := range session.StagedRows {
		if row.Ordinal == requestOrdinal {
			start = index
			break
		}
	}
	if start == len(session.StagedRows) && requestOrdinal != uint64(len(session.StagedRows)+1) {
		return RebuildSession{}, RebuildPage{}, false, nil
	}
	end := start + int(session.PageLimit)
	if end > len(session.StagedRows) {
		end = len(session.StagedRows)
	}
	rows := make([]AuthoritativeRow, 0, end-start)
	for _, staged := range session.StagedRows[start:end] {
		rows = append(rows, cloneAuthoritativeRow(staged.Row))
	}
	nextOrdinal := uint64(end + 1)
	final := end == len(session.StagedRows)
	page := RebuildPage{
		Ordinal:  requestOrdinal,
		Rows:     rows,
		HasToken: hasRequestToken,
		Token:    requestToken,
	}
	if !final {
		bindings := rebuildContinuationBindings(model.state, client, assignment, scopeState, rebuild, session, nextOrdinal, session.PageLimit, now)
		continuation := model.authority.Mint(string(TokenKindRebuildContinuation), bindings)
		if continuation == (OpaqueToken{}) {
			return RebuildSession{}, RebuildPage{}, false, fmt.Errorf("mint rebuild continuation: token authority exhausted")
		}
		page.HasContinuation = true
		page.Continuation = continuation
		session.HasContinuation = true
		session.Continuation = continuation
		session.NextRowOrdinal = nextOrdinal
	} else {
		bindings := incrementalCursorBindings(model.state, client, assignment, scopeState, session.Scope, session.SnapshotBoundary, now)
		finalCursor := model.authority.Mint(string(TokenKindIncrementalCursor), bindings)
		if finalCursor == (OpaqueToken{}) {
			return RebuildSession{}, RebuildPage{}, false, fmt.Errorf("mint rebuild final cursor: token authority exhausted")
		}
		page.HasFinalCursor = true
		page.FinalCursor = finalCursor
		page.HasChecksum = true
		page.Checksum = session.Checksum
		session.HasContinuation = false
		session.Continuation = OpaqueToken{}
		session.NextRowOrdinal = nextOrdinal
		session.HasFinalCursor = true
		session.FinalCursor = finalCursor
		session.Status = RebuildStatusComplete
	}
	canonical, err := canonicalRebuildPage(page)
	if err != nil {
		return RebuildSession{}, RebuildPage{}, false, fmt.Errorf("encode rebuild page: %w", err)
	}
	page.CanonicalResponse = canonical
	session.Pages = append(session.Pages, page)
	return session, page, true, nil
}

func rebuildContinuationBindings(state State, client ClientKey, assignment ScopeAssignment, scopeState ScopeState, rebuild RebuildID, session RebuildSession, ordinal uint64, pageLimit uint32, issuedAt time.Time) BindingSet {
	bindings := BindingSet{
		HasUser:                 true,
		User:                    client.UserID,
		HasClient:               true,
		Client:                  client,
		HasClientGeneration:     true,
		ClientGeneration:        currentClientGeneration(state, client),
		HasRegistryGeneration:   true,
		RegistryGeneration:      state.Registry.CurrentGeneration,
		HasMembershipGeneration: true,
		MembershipGeneration:    assignment.MembershipGeneration,
		HasRetentionGeneration:  true,
		RetentionGeneration:     assignment.RetentionGeneration,
		HasStreamGeneration:     true,
		StreamGeneration:        scopeState.StreamGeneration,
		HasSchema:               true,
		Schema:                  state.CurrentSchema,
		HasScope:                true,
		Scope:                   session.Scope,
		HasSnapshotBoundary:     true,
		SnapshotBoundary:        session.SnapshotBoundary,
		HasSessionID:            true,
		SessionID:               session.SessionID,
		HasRebuildID:            true,
		RebuildID:               rebuild,
		HasOrdinal:              true,
		Ordinal:                 ordinal,
		HasPageLimit:            true,
		PageLimit:               pageLimit,
		HasAcceptedWriteEpoch:   true,
		AcceptedWriteEpoch:      session.AcceptedWriteEpoch,
		HasIssuedAt:             true,
		IssuedAt:                issuedAt,
		HasExpiresAt:            session.ExpiresAt != nil,
	}
	if session.ExpiresAt != nil {
		bindings.ExpiresAt = *session.ExpiresAt
	}
	return bindings
}

func canonicalRebuildPage(page RebuildPage) ([]byte, error) {
	response := rebuildStoredResponse{
		PageOrdinal:     page.Ordinal,
		Records:         make([]rebuildStoredRecord, 0, len(page.Rows)),
		HasContinuation: page.HasContinuation,
		HasFinalCursor:  page.HasFinalCursor,
		HasChecksum:     page.HasChecksum,
		Checksum:        page.Checksum,
	}
	for _, row := range page.Rows {
		response.Records = append(response.Records, rebuildStoredRecord{
			Identity:     row.Identity,
			FieldValues:  cloneFieldValues(row.FieldValues),
			Version:      row.Version,
			Deleted:      row.Deleted,
			DeletedAt:    cloneTime(row.DeletedAt),
			DeleteReason: cloneString(row.DeleteReason),
			UpdatedAt:    cloneTime(row.UpdatedAt),
			Checksum:     row.Checksum,
		})
	}
	return json.Marshal(response)
}

func validateRebuildContinuation(model *Model, client ClientKey, clientState ClientState, assignment ScopeAssignment, scopeState ScopeState, key RebuildKey, session RebuildSession, hasSession bool, requestLimit uint32, token OpaqueToken, now time.Time) TokenStatus {
	stored, found := tokenBindings(model.authority, token)
	if !found {
		return validateTokenAgainstCurrent(model.authority, token, string(TokenKindRebuildContinuation), BindingSet{}, now)
	}
	if !hasSession {
		current := BindingSet{
			HasUser:             true,
			User:                client.UserID,
			HasClient:           true,
			Client:              client,
			HasClientGeneration: true,
			ClientGeneration:    clientState.CurrentGeneration,
			HasScope:            true,
			Scope:               key.Scope,
			HasRebuildID:        true,
			RebuildID:           key.Rebuild,
			HasPageLimit:        true,
			PageLimit:           requestLimit,
		}
		status := validateTokenAgainstCurrent(model.authority, token, string(TokenKindRebuildContinuation), current, now)
		if status == TokenStatusMisbound || status == TokenStatusWrongKind || status == TokenStatusForged {
			return status
		}
		return TokenStatusStale
	}
	ordinal := uint64(0)
	if stored.HasOrdinal {
		ordinal = stored.Ordinal
	}
	current := rebuildContinuationBindings(model.state, client, assignment, scopeState, key.Rebuild, session, ordinal, requestLimit, time.Time{})
	current.HasIssuedAt = false
	current.HasExpiresAt = false
	status := validateTokenAgainstCurrent(model.authority, token, string(TokenKindRebuildContinuation), current, now)
	if status != TokenStatusValid {
		return status
	}
	if !stored.HasOrdinal || stored.Ordinal == 0 || !rebuildSessionCurrent(model.state, clientState, assignment, scopeState, session, now) {
		return TokenStatusStale
	}
	return TokenStatusValid
}

func rebuildPageForRequest(session RebuildSession, hasToken bool, token OpaqueToken, ordinal uint64) (RebuildPage, bool) {
	for _, page := range session.Pages {
		if page.Ordinal == ordinal && page.HasToken == hasToken && (!hasToken || page.Token == token) {
			return cloneRebuildPage(page), true
		}
	}
	return RebuildPage{}, false
}

func rebuildPageForToken(session RebuildSession, token OpaqueToken) (RebuildPage, bool) {
	for _, page := range session.Pages {
		if page.HasToken && page.Token == token {
			return cloneRebuildPage(page), true
		}
	}
	return RebuildPage{}, false
}

func cloneRebuildPage(page RebuildPage) RebuildPage {
	page.Rows = cloneAuthoritativeRows(page.Rows)
	page.CanonicalResponse = cloneBytes(page.CanonicalResponse)
	return page
}

func rebuildPageResult(key RebuildKey, page RebuildPage, replayed bool) (StepResult, bool) {
	if !validRebuildPageShape(page) {
		return StepResult{}, false
	}
	records := make([]RebuildRecordObservation, 0, len(page.Rows))
	for _, row := range page.Rows {
		if row.Identity.CanonicalIdentityBytes == "" || row.Identity.TableID == "" || row.Identity.PrimaryKeyFieldID == "" || row.Identity.PortableType == "" || row.Version == "" {
			return StepResult{}, false
		}
		records = append(records, RebuildRecordObservation{
			Row:         row.Identity,
			Version:     row.Version,
			Deleted:     row.Deleted,
			HasChecksum: true,
			Checksum:    row.Checksum,
		})
	}
	return StepResult{
		Kind: StepResultKindRebuild,
		HTTP: &HTTPObservation{
			Status: 200,
		},
		Rebuild: &RebuildObservation{
			Attempt:         key,
			PageOrdinal:     page.Ordinal,
			Replayed:        replayed,
			Records:         records,
			HasContinuation: page.HasContinuation,
			Continuation:    page.Continuation,
			HasFinalCursor:  page.HasFinalCursor,
			FinalCursor:     page.FinalCursor,
			HasChecksum:     page.HasChecksum,
			Checksum:        page.Checksum,
		},
	}, true
}

func validRebuildPageShape(page RebuildPage) bool {
	if page.Ordinal == 0 || len(page.CanonicalResponse) == 0 {
		return false
	}
	if page.HasContinuation {
		return !page.HasFinalCursor && !page.HasChecksum && page.Continuation != (OpaqueToken{})
	}
	return page.HasFinalCursor && page.FinalCursor != (OpaqueToken{}) && page.HasChecksum
}

func localBeginRebuild(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	request, err := decodeLocalBeginRebuild(payload)
	if err != nil {
		return StepResult{}, fmt.Errorf("decode local begin-rebuild payload: %w", err)
	}
	client := ClientKey{UserID: UserID(request.UserID), ClientID: ClientID(request.ClientID)}
	local, found := model.state.ClientLocal[client]
	if !found {
		return StepResult{}, fmt.Errorf("begin local rebuild: client local state is absent")
	}
	schema, _, _ := decodeSchemaReference(request.Schema, false)
	if local.ClientGeneration != Generation(request.ClientGeneration) || local.CurrentSchema != schema {
		return StepResult{}, fmt.Errorf("begin local rebuild: local generation or schema is stale")
	}
	scope := ScopeID(request.ScopeID)
	assignmentIndex := localScopeAssignmentIndex(local.ScopeAssignments, scope)
	if assignmentIndex < 0 || !local.ScopeAssignments[assignmentIndex].Assigned {
		return StepResult{}, fmt.Errorf("begin local rebuild: scope is not assigned")
	}

	for _, attempt := range local.RebuildAttempts {
		if attempt.Scope != scope {
			continue
		}
		if attempt.Rebuild == RebuildID(request.RebuildID) {
			if attempt.ClientGeneration != Generation(request.ClientGeneration) || attempt.Schema != schema || attempt.PageLimit != request.Limit {
				return StepResult{}, fmt.Errorf("begin local rebuild: immutable attempt fields differ")
			}
			return localOperationResult(client, LocalMutationStatusPending), nil
		}
		if attempt.Phase == LocalRebuildAttemptPhaseCreated || attempt.Phase == LocalRebuildAttemptPhaseApplying || attempt.Phase == LocalRebuildAttemptPhasePendingFinality {
			return StepResult{}, fmt.Errorf("begin local rebuild: another attempt is active")
		}
	}

	removed := removeLocalScopeProvenance(&local, scope)
	pruneUnjustifiedLocalRows(&local, removed)
	checkpointIndex := localScopeCheckpointIndex(local.ScopeCheckpoints, scope)
	if checkpointIndex >= 0 {
		local.ScopeCheckpoints[checkpointIndex] = LocalScopeCheckpoint{Scope: scope}
	} else {
		local.ScopeCheckpoints = append(local.ScopeCheckpoints, LocalScopeCheckpoint{Scope: scope})
	}
	local.ScopeAssignments[assignmentIndex].RebuildRequired = true
	local.RebuildStaging = removeLocalRebuildStages(local.RebuildStaging, RebuildID(request.RebuildID))
	local.RebuildAttempts = append(local.RebuildAttempts, LocalRebuildAttempt{
		Rebuild:          RebuildID(request.RebuildID),
		Scope:            scope,
		ClientGeneration: Generation(request.ClientGeneration),
		Schema:           schema,
		PageLimit:        request.Limit,
		Phase:            LocalRebuildAttemptPhaseCreated,
	})
	model.state.ClientLocal[client] = local
	return localOperationResult(client, LocalMutationStatusPending), nil
}

func decodeLocalBeginRebuild(payload json.RawMessage) (localBeginRebuildPayload, error) {
	var request localBeginRebuildPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return localBeginRebuildPayload{}, err
	}
	if request.UserID == "" || request.ClientID == "" || request.ClientGeneration == 0 || request.ClientGeneration > maxReferenceJSONInteger || request.ScopeID == "" || !validCanonicalUUID(request.RebuildID) || request.Limit == 0 {
		return localBeginRebuildPayload{}, fmt.Errorf("local begin-rebuild request is invalid")
	}
	if _, _, err := decodeSchemaReference(request.Schema, false); err != nil {
		return localBeginRebuildPayload{}, err
	}
	return request, nil
}

func localApplyRebuildPage(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	request, err := decodeLocalApplyRebuildPage(payload)
	if err != nil {
		return StepResult{}, fmt.Errorf("decode local apply-rebuild-page payload: %w", err)
	}
	client := ClientKey{UserID: UserID(request.UserID), ClientID: ClientID(request.ClientID)}
	local, found := model.state.ClientLocal[client]
	if !found {
		return StepResult{}, fmt.Errorf("apply local rebuild page: client local state is absent")
	}
	scope := ScopeID(request.ScopeID)
	rebuild := RebuildID(request.RebuildID)
	attemptIndex := localRebuildAttemptIndex(local.RebuildAttempts, scope, rebuild)
	if attemptIndex < 0 {
		return StepResult{}, fmt.Errorf("apply local rebuild page: attempt is absent")
	}
	attempt := &local.RebuildAttempts[attemptIndex]
	key := RebuildKey{Client: client, Scope: scope, Rebuild: rebuild}
	session, found := model.state.Rebuilds[key]
	if !found {
		return StepResult{}, fmt.Errorf("apply local rebuild page: server session is absent")
	}
	if attempt.ClientGeneration != session.ClientGeneration || attempt.Schema != session.Schema || attempt.PageLimit != session.PageLimit || attempt.Scope != session.Scope {
		return StepResult{}, fmt.Errorf("apply local rebuild page: attempt identity differs from the server session")
	}

	hasRequestToken := request.RequestTokenSource != tokenSourceNone
	requestToken := OpaqueToken{}
	if hasRequestToken {
		requestToken, found = resolveModelToken(model, client, scope, rebuild, request.RequestTokenSource)
		if !found {
			return StepResult{}, fmt.Errorf("apply local rebuild page: request token is absent")
		}
	}
	page, found := rebuildPageForRequest(session, hasRequestToken, requestToken, request.PageOrdinal)
	if !found {
		return StepResult{}, fmt.Errorf("apply local rebuild page: page identity is invalid")
	}
	if attempt.Phase == LocalRebuildAttemptPhaseCompleted || attempt.Phase == LocalRebuildAttemptPhaseAbandoned || !validRebuildPageShape(page) || uint64(len(page.Rows)) > uint64(attempt.PageLimit) {
		return StepResult{}, fmt.Errorf("apply local rebuild page: page is invalid for the attempt")
	}
	canonical, err := canonicalRebuildPage(page)
	if err != nil || !bytes.Equal(canonical, page.CanonicalResponse) {
		return StepResult{}, fmt.Errorf("apply local rebuild page: stored page content is invalid")
	}
	pageDigest := sha256.Sum256(canonical)
	if applied, exists := appliedRebuildPage(*attempt, request.PageOrdinal, hasRequestToken, requestToken); exists {
		if !applied.HasPageDigest || applied.PageDigest != pageDigest {
			return StepResult{}, fmt.Errorf("apply local rebuild page: repeated page content differs")
		}
		return localOperationResult(client, LocalMutationStatusPending), nil
	}

	stages := make([]LocalRebuildStage, 0, len(page.Rows))
	seen := make(map[uint64]struct{}, len(page.Rows))
	for _, row := range page.Rows {
		staged, found := stagedRowForIdentity(session, row.Identity)
		if !found || !equivalentRebuildRows(staged.Row, row) || row.Version == "" {
			return StepResult{}, fmt.Errorf("apply local rebuild page: record is not an immutable staged record")
		}
		if _, duplicate := seen[staged.Ordinal]; duplicate || localRebuildStageExists(local.RebuildStaging, rebuild, staged.Ordinal) {
			return StepResult{}, fmt.Errorf("apply local rebuild page: record is duplicated")
		}
		seen[staged.Ordinal] = struct{}{}
		stages = append(stages, LocalRebuildStage{
			Rebuild: rebuild,
			Ordinal: staged.Ordinal,
			Row: LocalRow{
				Identity:         row.Identity,
				Fields:           cloneFieldValues(row.FieldValues),
				Deleted:          row.Deleted,
				HasServerVersion: true,
				ServerVersion:    row.Version,
				HasChecksum:      true,
				Checksum:         row.Checksum,
				UpdatedAt:        cloneTime(row.UpdatedAt),
			},
		})
	}

	local.RebuildStaging = append(local.RebuildStaging, stages...)
	appliedAt := referenceNow(model.clock)
	attempt.AppliedPages = append(attempt.AppliedPages, AppliedRebuildPage{
		RequestPageOrdinal: request.PageOrdinal,
		HasRequestToken:    hasRequestToken,
		RequestToken:       requestToken,
		HasPageDigest:      true,
		PageDigest:         pageDigest,
		AppliedAt:          &appliedAt,
	})
	if page.HasContinuation {
		attempt.HasContinuation = true
		attempt.Continuation = page.Continuation
		attempt.HasPendingFinalResult = false
		attempt.PendingFinalResult = PendingRebuildFinalResult{}
		attempt.Phase = LocalRebuildAttemptPhaseApplying
	} else {
		attempt.HasContinuation = false
		attempt.Continuation = OpaqueToken{}
		attempt.HasPendingFinalResult = true
		attempt.PendingFinalResult = PendingRebuildFinalResult{
			HasFinalCursor: true,
			FinalCursor:    page.FinalCursor,
			ScopeChecksum:  page.Checksum,
			Cardinality:    Cardinality(len(session.StagedRows)),
		}
		attempt.Phase = LocalRebuildAttemptPhasePendingFinality
	}
	model.state.ClientLocal[client] = local
	return localOperationResult(client, LocalMutationStatusPending), nil
}

func decodeLocalApplyRebuildPage(payload json.RawMessage) (localApplyRebuildPagePayload, error) {
	var request localApplyRebuildPagePayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return localApplyRebuildPagePayload{}, err
	}
	if request.UserID == "" || request.ClientID == "" || request.ScopeID == "" || !validCanonicalUUID(request.RebuildID) || request.PageOrdinal == 0 || request.PageOrdinal > maxReferenceJSONInteger || !validTokenSource(request.RequestTokenSource) {
		return localApplyRebuildPagePayload{}, fmt.Errorf("local apply-rebuild-page request is invalid")
	}
	return request, nil
}

func localFinalizeRebuild(_ context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	request, err := decodeLocalFinalizeRebuild(payload)
	if err != nil {
		return StepResult{}, fmt.Errorf("decode local finalize-rebuild payload: %w", err)
	}
	client := ClientKey{UserID: UserID(request.UserID), ClientID: ClientID(request.ClientID)}
	local, found := model.state.ClientLocal[client]
	if !found {
		return StepResult{}, fmt.Errorf("finalize local rebuild: client local state is absent")
	}
	scope := ScopeID(request.ScopeID)
	rebuild := RebuildID(request.RebuildID)
	attemptIndex := localRebuildAttemptIndex(local.RebuildAttempts, scope, rebuild)
	if attemptIndex < 0 {
		return StepResult{}, fmt.Errorf("finalize local rebuild: attempt is absent")
	}
	attempt := &local.RebuildAttempts[attemptIndex]
	if attempt.Phase != LocalRebuildAttemptPhasePendingFinality || !attempt.HasPendingFinalResult || !attempt.PendingFinalResult.HasFinalCursor || attempt.PendingFinalResult.FinalCursor == (OpaqueToken{}) {
		return StepResult{}, fmt.Errorf("finalize local rebuild: final result is absent")
	}

	staged, checksum, complete := completeLocalRebuildStage(local.RebuildStaging, rebuild, attempt.Schema, scope, attempt.PendingFinalResult.Cardinality)
	if !complete || checksum != attempt.PendingFinalResult.ScopeChecksum {
		return StepResult{}, fmt.Errorf("finalize local rebuild: checksum verification failed")
	}
	clientState, exists := model.state.Clients[client]
	if !exists {
		return StepResult{}, fmt.Errorf("finalize local rebuild: server client state is absent")
	}
	assignment, assigned := scopeAssignmentFor(clientState, scope)
	scopeState, hasScopeState := model.state.Scopes[scope]
	if !assigned || !hasScopeState {
		return StepResult{}, fmt.Errorf("finalize local rebuild: server scope state is absent")
	}
	status, finalPosition := validateIncrementalCursor(model, client, assignment, scopeState, scope, attempt.PendingFinalResult.FinalCursor, referenceNow(model.clock))
	if status != TokenStatusValid {
		return StepResult{}, fmt.Errorf("finalize local rebuild: final cursor is invalid")
	}

	removed := removeLocalScopeProvenance(&local, scope)
	pruneUnjustifiedLocalRows(&local, removed)
	for _, stage := range staged {
		putLocalRow(&local, stage.Row)
		addLocalScopeProvenance(&local, stage.Row.Identity, scope, stage.Row.ServerVersion)
	}
	assignmentIndex := localScopeAssignmentIndex(local.ScopeAssignments, scope)
	if assignmentIndex < 0 {
		return StepResult{}, fmt.Errorf("finalize local rebuild: local scope assignment is absent")
	}
	local.ScopeAssignments[assignmentIndex].RebuildRequired = false
	checkpointIndex := localScopeCheckpointIndex(local.ScopeCheckpoints, scope)
	checkpoint := LocalScopeCheckpoint{
		Scope:       scope,
		Position:    finalPosition,
		HasCursor:   true,
		Cursor:      attempt.PendingFinalResult.FinalCursor,
		HasChecksum: true,
		Checksum:    attempt.PendingFinalResult.ScopeChecksum,
		Verified:    true,
	}
	if checkpointIndex < 0 {
		local.ScopeCheckpoints = append(local.ScopeCheckpoints, checkpoint)
	} else {
		local.ScopeCheckpoints[checkpointIndex] = checkpoint
	}
	attempt.HasContinuation = false
	attempt.Continuation = OpaqueToken{}
	attempt.Phase = LocalRebuildAttemptPhaseCompleted
	local.RebuildStaging = removeLocalRebuildStages(local.RebuildStaging, rebuild)
	model.state.ClientLocal[client] = local
	return localOperationResult(client, LocalMutationStatusAccepted), nil
}

func decodeLocalFinalizeRebuild(payload json.RawMessage) (localFinalizeRebuildPayload, error) {
	var request localFinalizeRebuildPayload
	if err := decodeStrictPayload(payload, &request); err != nil {
		return localFinalizeRebuildPayload{}, err
	}
	if request.UserID == "" || request.ClientID == "" || request.ScopeID == "" || !validCanonicalUUID(request.RebuildID) {
		return localFinalizeRebuildPayload{}, fmt.Errorf("local finalize-rebuild request is invalid")
	}
	return request, nil
}

func localOperationResult(client ClientKey, status LocalMutationStatus) StepResult {
	return StepResult{
		Kind: StepResultKindLocal,
		Local: &LocalObservation{
			Client: client,
			Status: status,
		},
	}
}

func localScopeAssignmentIndex(assignments []LocalScopeAssignment, scope ScopeID) int {
	for index := range assignments {
		if assignments[index].Scope == scope {
			return index
		}
	}
	return -1
}

func localScopeCheckpointIndex(checkpoints []LocalScopeCheckpoint, scope ScopeID) int {
	for index := range checkpoints {
		if checkpoints[index].Scope == scope {
			return index
		}
	}
	return -1
}

func localRebuildAttemptIndex(attempts []LocalRebuildAttempt, scope ScopeID, rebuild RebuildID) int {
	for index := range attempts {
		if attempts[index].Scope == scope && attempts[index].Rebuild == rebuild {
			return index
		}
	}
	return -1
}

func appliedRebuildPage(attempt LocalRebuildAttempt, ordinal uint64, hasToken bool, token OpaqueToken) (AppliedRebuildPage, bool) {
	for _, page := range attempt.AppliedPages {
		if page.RequestPageOrdinal == ordinal && page.HasRequestToken == hasToken && (!hasToken || page.RequestToken == token) {
			return page, true
		}
	}
	return AppliedRebuildPage{}, false
}

func stagedRowForIdentity(session RebuildSession, identity RowIdentity) (RebuildStagedRow, bool) {
	for _, row := range session.StagedRows {
		if row.Row.Identity == identity {
			return row, true
		}
	}
	return RebuildStagedRow{}, false
}

func equivalentRebuildRows(left, right AuthoritativeRow) bool {
	if left.Identity != right.Identity || left.Version != right.Version || left.Checksum != right.Checksum || left.Deleted != right.Deleted || len(left.FieldValues) != len(right.FieldValues) {
		return false
	}
	for index := range left.FieldValues {
		if left.FieldValues[index] != right.FieldValues[index] {
			return false
		}
	}
	return true
}

func localRebuildStageExists(stages []LocalRebuildStage, rebuild RebuildID, ordinal uint64) bool {
	for _, stage := range stages {
		if stage.Rebuild == rebuild && stage.Ordinal == ordinal {
			return true
		}
	}
	return false
}

func completeLocalRebuildStage(stages []LocalRebuildStage, rebuild RebuildID, schema SchemaRef, scope ScopeID, cardinality Cardinality) ([]LocalRebuildStage, Checksum, bool) {
	rows := make([]LocalRebuildStage, 0, cardinality)
	seen := make(map[uint64]struct{}, cardinality)
	for _, stage := range stages {
		if stage.Rebuild != rebuild {
			continue
		}
		if stage.Ordinal == 0 || !stage.Row.HasServerVersion || stage.Row.ServerVersion == "" || !stage.Row.HasChecksum || stage.Row.Identity.CanonicalIdentityBytes == "" {
			return nil, Checksum{}, false
		}
		if _, duplicate := seen[stage.Ordinal]; duplicate {
			return nil, Checksum{}, false
		}
		seen[stage.Ordinal] = struct{}{}
		rows = append(rows, stage)
	}
	if Cardinality(len(rows)) != cardinality {
		return nil, Checksum{}, false
	}
	sort.Slice(rows, func(left, right int) bool {
		return rows[left].Ordinal < rows[right].Ordinal
	})
	digestRows := make([]rebuildDigestRow, 0, len(rows))
	for index, row := range rows {
		if row.Ordinal != uint64(index+1) {
			return nil, Checksum{}, false
		}
		digestRows = append(digestRows, rebuildDigestRow{Identity: row.Row.Identity, Checksum: row.Row.Checksum})
	}
	checksum, valid := referenceScopeChecksum(schema, scope, digestRows)
	return rows, checksum, valid
}

func removeLocalScopeProvenance(local *ClientLocalState, scope ScopeID) map[RowIdentity]struct{} {
	removed := make(map[RowIdentity]struct{})
	provenance := make([]LocalProvenance, 0, len(local.Provenance))
	for _, entry := range local.Provenance {
		remaining := make([]ScopeID, 0, len(entry.Scopes))
		removedScope := false
		for _, candidate := range entry.Scopes {
			if candidate == scope {
				removedScope = true
				continue
			}
			remaining = append(remaining, candidate)
		}
		if removedScope {
			removed[entry.Row] = struct{}{}
		}
		if len(remaining) == 0 {
			continue
		}
		entry.Scopes = remaining
		provenance = append(provenance, entry)
	}
	local.Provenance = provenance
	return removed
}

func pruneUnjustifiedLocalRows(local *ClientLocalState, candidates map[RowIdentity]struct{}) {
	if len(candidates) == 0 {
		return
	}
	remaining := make([]LocalRow, 0, len(local.Rows))
	for _, row := range local.Rows {
		if _, candidate := candidates[row.Identity]; !candidate || localRowHasProvenance(local.Provenance, row.Identity) || localRowHasPendingIntent(local.DurableQueue, row.Identity) {
			remaining = append(remaining, row)
		}
	}
	local.Rows = remaining
}

func localRowHasProvenance(provenance []LocalProvenance, identity RowIdentity) bool {
	for _, entry := range provenance {
		if entry.Row == identity && len(entry.Scopes) > 0 {
			return true
		}
	}
	return false
}

func localRowHasPendingIntent(queue []QueuedMutation, identity RowIdentity) bool {
	for _, mutation := range queue {
		if mutation.Row != identity {
			continue
		}
		switch mutation.Status {
		case LocalMutationStatusAccepted,
			LocalMutationStatusServerRejected,
			LocalMutationStatusSupersededBeforeSend,
			LocalMutationStatusCancelledBeforeSend:
			continue
		default:
			return true
		}
	}
	return false
}

func removeLocalRebuildStages(stages []LocalRebuildStage, rebuild RebuildID) []LocalRebuildStage {
	remaining := make([]LocalRebuildStage, 0, len(stages))
	for _, stage := range stages {
		if stage.Rebuild != rebuild {
			remaining = append(remaining, stage)
		}
	}
	return remaining
}

func putLocalRow(local *ClientLocalState, row LocalRow) {
	for index := range local.Rows {
		if local.Rows[index].Identity == row.Identity {
			local.Rows[index] = row
			return
		}
	}
	local.Rows = append(local.Rows, row)
}

func addLocalScopeProvenance(local *ClientLocalState, identity RowIdentity, scope ScopeID, version RowVersion) {
	for index := range local.Provenance {
		if local.Provenance[index].Row != identity {
			continue
		}
		for _, existing := range local.Provenance[index].Scopes {
			if existing == scope {
				local.Provenance[index].Version = version
				return
			}
		}
		local.Provenance[index].Scopes = append(local.Provenance[index].Scopes, scope)
		local.Provenance[index].Version = version
		return
	}
	local.Provenance = append(local.Provenance, LocalProvenance{Row: identity, Scopes: []ScopeID{scope}, Version: version})
}
