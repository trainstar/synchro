package baseline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

const diagnosticUser = "diagnostic-user"

// Probe executes one known non-release protocol 2 diagnostic family.
type Probe interface {
	Family() DefectFamily
	Run(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error)
}

type legacyProbe struct {
	family DefectFamily
	run    func(context.Context, *ProbeRuntime) (ProbeResult, error)
}

func (probe legacyProbe) Family() DefectFamily { return probe.family }

func (probe legacyProbe) Run(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error) {
	if runtime == nil {
		return ProbeResult{}, errors.New("diagnostic runtime is required")
	}
	return probe.run(ctx, runtime)
}

// DefaultProbes returns ten observable current divergences in a fixed order.
func DefaultProbes() []Probe {
	return []Probe{
		legacyProbe{family: DefectCommitOrder, run: probeCommitOrder},
		legacyProbe{family: DefectPullStarvation, run: probePullStarvation},
		legacyProbe{family: DefectHydrationFailure, run: probeHydrationFailure},
		legacyProbe{family: DefectDecodeFailure, run: probeDecodeFailure},
		legacyProbe{family: DefectRegistryReload, run: probeRegistryReload},
		legacyProbe{family: DefectResponseLoss, run: probeResponseLoss},
		legacyProbe{family: DefectForgedRebuild, run: probeForgedRebuildCursor},
		legacyProbe{family: DefectSchemaIntent, run: probeSchemaIntent},
		legacyProbe{family: DefectCompactionInterval, run: probeCompactionInterval},
		legacyProbe{family: DefectOwnershipChange, run: probeOwnershipChange},
	}
}

type diagnosticSession struct {
	clientID        string
	schema          SchemaRef
	scopeSetVersion int64
	scopes          map[string]ScopeCursor
	userScope       string
}

func bootstrapSession(ctx context.Context, runtime *ProbeRuntime, family DefectFamily) (diagnosticSession, []exchange, error) {
	clientID := "diagnostic-" + string(family)
	response, observed, err := runtime.Connect(ctx, ConnectRequest{
		ClientID: clientID, Platform: "diagnostic", AppVersion: "0.0.0-diagnostic",
		ProtocolVersion: ProtocolVersion, Schema: SchemaRef{}, ScopeSetVersion: 0,
		KnownScopes: map[string]ScopeCursor{},
	})
	if err != nil {
		return diagnosticSession{}, []exchange{observed}, err
	}
	scopes := make(map[string]ScopeCursor, len(response.Scopes.Add)+1)
	for _, scope := range response.Scopes.Add {
		scopes[scope.ID] = ScopeCursor{Cursor: scope.Cursor}
	}
	userScope := "user:" + diagnosticUser
	if _, found := scopes[userScope]; !found {
		scopes[userScope] = ScopeCursor{}
	}
	return diagnosticSession{
		clientID: clientID, schema: SchemaRef{Version: response.Schema.Version, Hash: response.Schema.Hash},
		scopeSetVersion: response.ScopeSetVersion, scopes: scopes, userScope: userScope,
	}, []exchange{observed}, nil
}

func prepareSession(ctx context.Context, runtime *ProbeRuntime, family DefectFamily) (diagnosticSession, []exchange, error) {
	session, exchanges, err := bootstrapSession(ctx, runtime, family)
	if err != nil {
		return session, exchanges, err
	}
	var rebuildScopes []string
	for scope, cursor := range session.scopes {
		if cursor.Cursor == nil {
			rebuildScopes = append(rebuildScopes, scope)
		}
	}
	sort.Strings(rebuildScopes)
	for _, scope := range rebuildScopes {
		rebuilt, err := rebuildScopeToTerminal(ctx, runtime, &session, scope, 256)
		exchanges = append(exchanges, rebuilt...)
		if err != nil {
			return session, exchanges, err
		}
	}
	drained, err := drainSession(ctx, runtime, &session, 256)
	exchanges = append(exchanges, drained...)
	return session, exchanges, err
}

func (session diagnosticSession) pullRequest(limit int) PullRequest {
	return PullRequest{
		ProtocolVersion: ProtocolVersion, ClientID: session.clientID, Schema: session.schema,
		ScopeSetVersion: session.scopeSetVersion, Scopes: cloneScopes(session.scopes), Limit: limit,
	}
}

func cloneScopes(source map[string]ScopeCursor) map[string]ScopeCursor {
	result := make(map[string]ScopeCursor, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func applyPullResponse(session *diagnosticSession, response PullResponse) {
	if session == nil {
		return
	}
	session.scopeSetVersion = response.ScopeSetVersion
	for _, scope := range response.ScopeUpdates.Remove {
		delete(session.scopes, scope)
	}
	for _, scope := range response.ScopeUpdates.Add {
		session.scopes[scope.ID] = ScopeCursor{Cursor: scope.Cursor}
	}
	for scope, cursor := range response.ScopeCursors {
		value := cursor
		session.scopes[scope] = ScopeCursor{Cursor: &value}
	}
}

func drainSession(ctx context.Context, runtime *ProbeRuntime, session *diagnosticSession, limit int) ([]exchange, error) {
	var exchanges []exchange
	for pages := 0; pages < 256; pages++ {
		response, observed, err := runtime.Pull(ctx, session.pullRequest(limit))
		exchanges = append(exchanges, observed)
		if err != nil {
			return exchanges, err
		}
		applyPullResponse(session, response)
		if !response.HasMore {
			return exchanges, nil
		}
	}
	return exchanges, errors.New("diagnostic pull did not reach a terminal page")
}

func pullUntil(ctx context.Context, runtime *ProbeRuntime, session *diagnosticSession, limit int, match func(ChangeRecord) bool) ([]ChangeRecord, []exchange, error) {
	waitContext, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	selectedScopes := make(map[string]struct{}, len(session.scopes))
	for scope := range session.scopes {
		selectedScopes[scope] = struct{}{}
	}
	var changes []ChangeRecord
	var exchanges []exchange
	for {
		response, observed, err := runtime.Pull(waitContext, session.pullRequest(limit))
		exchanges = append(exchanges, observed)
		if err != nil {
			return changes, exchanges, err
		}
		applyPullResponse(session, response)
		// Some probes isolate one assigned scope to control its cursor independently.
		for scope := range session.scopes {
			if _, selected := selectedScopes[scope]; !selected {
				delete(session.scopes, scope)
			}
		}
		if len(response.Rebuild) != 0 {
			return changes, exchanges, errors.New("diagnostic pull unexpectedly required rebuild")
		}
		changes = append(changes, response.Changes...)
		for _, change := range response.Changes {
			if match(change) {
				return changes, exchanges, nil
			}
		}
		if response.HasMore {
			continue
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-waitContext.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return changes, exchanges, errors.New("diagnostic change did not become visible")
		case <-timer.C:
		}
	}
}

func probeCommitOrder(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectCommitOrder)
	if err != nil {
		return failedProbe(DefectCommitOrder, "commit-LSN transaction order with opaque row versions", exchanges, err)
	}
	firstID := "00000000-0000-0000-0000-000000000101"
	secondID := "00000000-0000-0000-0000-000000000102"
	firstVersion := "2030-01-01T00:00:00Z"
	secondVersion := "2020-01-01T00:00:00Z"
	err = runtime.commitInReverseBeginOrder(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value, updated_at) VALUES ($1, $2, $3, $4)",
		[]any{firstID, diagnosticUser, "first-begun-last-committed", firstVersion},
		"INSERT INTO cf_items (id, owner_id, value, updated_at) VALUES ($1, $2, $3, $4)",
		[]any{secondID, diagnosticUser, "second-begun-first-committed", secondVersion},
	)
	if err != nil {
		return failedProbe(DefectCommitOrder, "commit-LSN transaction order with opaque row versions", exchanges, err)
	}
	changes, observed, err := pullUntil(ctx, runtime, &session, 16, changeWithID(firstID))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectCommitOrder, "commit-LSN transaction order with opaque row versions", exchanges, err)
	}
	firstIndex, secondIndex := -1, -1
	var firstObservedVersion, secondObservedVersion string
	for index, change := range changes {
		switch primaryKeyID(change.PrimaryKey) {
		case firstID:
			firstIndex = index
			firstObservedVersion = change.ServerVersion
		case secondID:
			secondIndex = index
			secondObservedVersion = change.ServerVersion
		}
	}
	if secondIndex < 0 || firstIndex <= secondIndex {
		return failedProbe(DefectCommitOrder, "commit-LSN transaction order with opaque row versions", exchanges, fmt.Errorf("observed transaction order first=%d second=%d", firstIndex, secondIndex))
	}
	captured := firstObservedVersion == "2030-01-01T00:00:00.000Z" && secondObservedVersion == "2020-01-01T00:00:00.000Z"
	if !captured {
		return failedProbe(DefectCommitOrder, "commit-LSN transaction order with opaque row versions", exchanges, fmt.Errorf("observed versions first=%q second=%q", firstObservedVersion, secondObservedVersion))
	}
	return capturedProbe(DefectCommitOrder, "commit-LSN transaction order with opaque row versions", captured, "commit-ordered changes expose authored timestamps as row versions", exchanges), nil
}

func probePullStarvation(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectPullStarvation)
	if err != nil {
		return failedProbe(DefectPullStarvation, "independent per-scope pull progress", exchanges, err)
	}
	global := diagnosticSession{clientID: session.clientID, schema: session.schema, scopeSetVersion: session.scopeSetVersion, scopes: map[string]ScopeCursor{"cf:global": session.scopes["cf:global"]}, userScope: session.userScope}
	for index, id := range []string{"00000000-0000-0000-0000-000000000201", "00000000-0000-0000-0000-000000000202"} {
		if err := runtime.sourceDML(ctx, "INSERT INTO cf_global_items (id, value) VALUES ($1, $2)", id, fmt.Sprintf("global-history-%d", index+1)); err != nil {
			return failedProbe(DefectPullStarvation, "independent per-scope pull progress", exchanges, err)
		}
	}
	_, observed, err := pullUntil(ctx, runtime, &global, 16, changeWithID("00000000-0000-0000-0000-000000000202"))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectPullStarvation, "independent per-scope pull progress", exchanges, err)
	}
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", "00000000-0000-0000-0000-000000000203", diagnosticUser, "starved-user-row"); err != nil {
		return failedProbe(DefectPullStarvation, "independent per-scope pull progress", exchanges, err)
	}
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_global_items (id, value) VALUES ($1, $2)", "00000000-0000-0000-0000-000000000204", "post-user-sentinel"); err != nil {
		return failedProbe(DefectPullStarvation, "independent per-scope pull progress", exchanges, err)
	}
	_, observed, err = pullUntil(ctx, runtime, &global, 16, changeWithID("00000000-0000-0000-0000-000000000204"))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectPullStarvation, "independent per-scope pull progress", exchanges, err)
	}
	session.scopes["cf:global"] = global.scopes["cf:global"]
	response, observedExchange, err := runtime.Pull(ctx, session.pullRequest(1))
	exchanges = append(exchanges, observedExchange)
	if err != nil {
		return failedProbe(DefectPullStarvation, "independent per-scope pull progress", exchanges, err)
	}
	captured := len(response.Changes) == 0 && !response.HasMore && response.ScopeCursors[session.userScope] == ""
	return capturedProbe(DefectPullStarvation, "independent per-scope pull progress", captured, "the acknowledged global history hides an eligible user-scope change", exchanges), nil
}

func probeHydrationFailure(ctx context.Context, runtime *ProbeRuntime) (result ProbeResult, returnedErr error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectHydrationFailure)
	if err != nil {
		return failedProbe(DefectHydrationFailure, "atomic pull failure without cursor progress", exchanges, err)
	}
	id := "00000000-0000-0000-0000-000000000301"
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_schema_queue (id, owner_id, authored_mutation, legacy_value) VALUES ($1, $2, $3::jsonb, $4)", id, diagnosticUser, `{"value":"retained"}`, "legacy"); err != nil {
		return failedProbe(DefectHydrationFailure, "atomic pull failure without cursor progress", exchanges, err)
	}
	witness := session
	witness.scopes = cloneScopes(session.scopes)
	_, observed, err := pullUntil(ctx, runtime, &witness, 16, changeWithID(id))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectHydrationFailure, "atomic pull failure without cursor progress", exchanges, err)
	}
	if err := runtime.dropHydrationColumn(ctx); err != nil {
		return failedProbe(DefectHydrationFailure, "atomic pull failure without cursor progress", exchanges, err)
	}
	restored := false
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if !restored {
			if cleanupErr := runtime.restoreHydrationColumn(cleanupContext); cleanupErr != nil {
				returnedErr = errors.Join(returnedErr, cleanupErr)
			}
		}
	}()
	originalRequest := session.pullRequest(16)
	_, observedExchange, pullErr := runtime.Pull(ctx, originalRequest)
	exchanges = append(exchanges, observedExchange)
	canonical, legacy, noProgress, decodeErr := inspectHydrationError(observedExchange.body)
	if decodeErr != nil {
		return failedProbe(DefectHydrationFailure, "atomic pull failure without cursor progress", exchanges, decodeErr)
	}
	cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	restoreErr := runtime.restoreHydrationColumn(cleanupContext)
	cancel()
	if restoreErr != nil {
		return failedProbe(DefectHydrationFailure, "atomic pull failure without cursor progress", exchanges, restoreErr)
	}
	restored = true
	followup, followupExchange, followupErr := runtime.Pull(ctx, originalRequest)
	exchanges = append(exchanges, followupExchange)
	if followupErr != nil {
		return failedProbe(DefectHydrationFailure, "atomic pull failure without cursor progress", exchanges, followupErr)
	}
	rowReturned := hasChange(followup.Changes, "cf_schema_queue", id, "upsert", session.userScope)
	captured := capturesHydrationFailure(pullErr != nil, observedExchange.status, canonical, legacy, noProgress, rowReturned)
	if !captured {
		return failedProbe(DefectHydrationFailure, "atomic pull failure without cursor progress", exchanges, fmt.Errorf("observed hydration status=%d error=%t canonical=%t legacy=%t no_progress=%t row_returned=%t", observedExchange.status, pullErr != nil, canonical, legacy, noProgress, rowReturned))
	}
	return capturedProbe(DefectHydrationFailure, "atomic pull failure without cursor progress", captured, "a stale projection returns a legacy untyped error, while the original cursor still returns its row after restoration", exchanges), nil
}

func probeDecodeFailure(ctx context.Context, runtime *ProbeRuntime) (result ProbeResult, returnedErr error) {
	if err := runtime.configureDecodeTrap(ctx); err != nil {
		return failedProbe(DefectDecodeFailure, "blocking durable poison quarantine", nil, err)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if cleanupErr := runtime.restoreDecodeTrap(cleanupContext); cleanupErr != nil {
			returnedErr = errors.Join(returnedErr, cleanupErr)
		}
	}()
	session, exchanges, err := prepareSession(ctx, runtime, DefectDecodeFailure)
	if err != nil {
		return failedProbe(DefectDecodeFailure, "blocking durable poison quarantine", exchanges, err)
	}
	trapID := "00000000-0000-0000-0000-000000000401"
	sentinelID := "00000000-0000-0000-0000-000000000402"
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_decode_trap (id, owner_id, unsupported_value) VALUES ($1, $2, point($3, $4))", trapID, diagnosticUser, 7, 11); err != nil {
		return failedProbe(DefectDecodeFailure, "blocking durable poison quarantine", exchanges, err)
	}
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", sentinelID, diagnosticUser, "post-decode-sentinel"); err != nil {
		return failedProbe(DefectDecodeFailure, "blocking durable poison quarantine", exchanges, err)
	}
	changes, observed, err := pullUntil(ctx, runtime, &session, 16, changeWithID(sentinelID))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectDecodeFailure, "blocking durable poison quarantine", exchanges, err)
	}
	captured := !hasChange(changes, "cf_decode_trap", trapID, "upsert", session.userScope)
	return capturedProbe(DefectDecodeFailure, "blocking durable poison quarantine", captured, "the worker skips an undecodable source change and serves later progress", exchanges), nil
}

func probeChecksumEncoding(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectChecksumEncoding)
	if err != nil {
		return failedProbe(DefectChecksumEncoding, "SHA-256 row and scope digest objects", exchanges, err)
	}
	id := "00000000-0000-0000-0000-000000000301"
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", id, diagnosticUser, "checksum-control"); err != nil {
		return failedProbe(DefectChecksumEncoding, "SHA-256 row and scope digest objects", exchanges, err)
	}
	changes, observed, err := pullUntil(ctx, runtime, &session, 16, changeWithID(id))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectChecksumEncoding, "SHA-256 row and scope digest objects", exchanges, err)
	}
	var target *ChangeRecord
	for index := range changes {
		if changeWithID(id)(changes[index]) {
			target = &changes[index]
		}
	}
	response, terminal, err := runtime.Pull(ctx, session.pullRequest(16))
	exchanges = append(exchanges, terminal)
	if err != nil {
		return failedProbe(DefectChecksumEncoding, "SHA-256 row and scope digest objects", exchanges, err)
	}
	scopeChecksum := response.Checksums[session.userScope]
	captured := target != nil && target.RowChecksum != nil && scopeChecksum != "" && !isLowerHexSHA256(scopeChecksum)
	return capturedProbe(DefectChecksumEncoding, "SHA-256 row and scope digest objects", captured, "protocol 2 emits integer and decimal checksums instead of canonical SHA-256 digests", exchanges), nil
}

func probeResponseLoss(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectResponseLoss)
	if err != nil {
		return failedProbe(DefectResponseLoss, "exact batch replay after upstream success", exchanges, err)
	}
	request := PushRequest{ProtocolVersion: ProtocolVersion, ClientID: session.clientID, BatchID: "diagnostic-response-loss-batch", Schema: session.schema, Mutations: []Mutation{{
		MutationID: "diagnostic-response-loss-mutation", Table: "cf_items", Operation: "insert",
		PrimaryKey: json.RawMessage(`{"id":"00000000-0000-0000-0000-000000000401"}`), ClientVersion: stringPointer("2026-01-01T00:00:00Z"),
		Columns: json.RawMessage(`{"owner_id":"diagnostic-user","value":"upstream-success"}`),
	}}}
	first, firstExchange, dropErr := runtime.PushDropAfterSuccess(ctx, request)
	exchanges = append(exchanges, firstExchange)
	if dropErr == nil {
		return failedProbe(DefectResponseLoss, "exact batch replay after upstream success", exchanges, errors.New("response-drop control did not drop a response"))
	}
	second, secondExchange, replayErr := runtime.Push(ctx, request)
	exchanges = append(exchanges, secondExchange)
	if replayErr != nil {
		return failedProbe(DefectResponseLoss, "exact batch replay after upstream success", exchanges, replayErr)
	}
	var firstResponse PushResponse
	if err := decodeProtocol2JSON(first.body, &firstResponse); err != nil {
		return failedProbe(DefectResponseLoss, "exact batch replay after upstream success", exchanges, err)
	}
	captured := len(firstResponse.Accepted) == 1 && firstResponse.Accepted[0].MutationID == request.Mutations[0].MutationID &&
		!bytes.Equal(first.body, secondExchange.body) && len(second.Rejected) == 1 && second.Rejected[0].MutationID == request.Mutations[0].MutationID
	return capturedProbe(DefectResponseLoss, "exact batch replay after upstream success", captured, "equal batch replay reevaluates the mutation and changes its terminal response", exchanges), nil
}

func probeForgedRebuildCursor(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectForgedRebuild)
	if err != nil {
		return failedProbe(DefectForgedRebuild, "authenticated opaque rebuild continuation", exchanges, err)
	}
	id := "00000000-0000-0000-0000-000000000501"
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", id, diagnosticUser, "forged-rebuild-control"); err != nil {
		return failedProbe(DefectForgedRebuild, "authenticated opaque rebuild continuation", exchanges, err)
	}
	_, observed, err := pullUntil(ctx, runtime, &session, 16, changeWithID(id))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectForgedRebuild, "authenticated opaque rebuild continuation", exchanges, err)
	}
	forged := "zzzzzzzz|zzzzzzzz"
	response, observedExchange, err := runtime.Rebuild(ctx, RebuildRequest{ProtocolVersion: ProtocolVersion, ClientID: session.clientID, Scope: session.userScope, Cursor: &forged, Limit: 8})
	exchanges = append(exchanges, observedExchange)
	if err != nil {
		return failedProbe(DefectForgedRebuild, "authenticated opaque rebuild continuation", exchanges, err)
	}
	captured := observedExchange.status == 200 && !response.HasMore && len(response.Records) == 0 && response.FinalScopeCursor != nil
	return capturedProbe(DefectForgedRebuild, "authenticated opaque rebuild continuation", captured, "a lexical table-and-row continuation is accepted as a rebuild cursor", exchanges), nil
}

func probeRebuildLiveSnapshot(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectRebuildSnapshot)
	if err != nil {
		return failedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", exchanges, err)
	}
	firstID := "ffffffff-ffff-ffff-ffff-000000000601"
	insertedID := "ffffffff-ffff-ffff-ffff-000000000602"
	lastID := "ffffffff-ffff-ffff-ffff-000000000603"
	for _, id := range []string{firstID, lastID} {
		if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", id, diagnosticUser, "rebuild-snapshot-control"); err != nil {
			return failedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", exchanges, err)
		}
	}
	_, observed, err := pullUntil(ctx, runtime, &session, 16, changeWithID(lastID))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", exchanges, err)
	}
	var cursor *string
	for pages := 0; pages < 256; pages++ {
		response, observedExchange, rebuildErr := runtime.Rebuild(ctx, RebuildRequest{ProtocolVersion: ProtocolVersion, ClientID: session.clientID, Scope: session.userScope, Cursor: cursor, Limit: 1})
		exchanges = append(exchanges, observedExchange)
		if rebuildErr != nil {
			return failedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", exchanges, rebuildErr)
		}
		if len(response.Records) == 1 && recordHasID(response.Records[0], firstID) {
			cursor = response.Cursor
			break
		}
		if response.Cursor == nil {
			return failedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", exchanges, errors.New("rebuild did not reach the boundary control row"))
		}
		cursor = response.Cursor
	}
	if cursor == nil {
		return failedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", exchanges, errors.New("rebuild boundary cursor is absent"))
	}
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", insertedID, diagnosticUser, "post-boundary-row"); err != nil {
		return failedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", exchanges, err)
	}
	_, observed, err = pullUntil(ctx, runtime, &session, 16, changeWithID(insertedID))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", exchanges, err)
	}
	response, observedExchange, err := runtime.Rebuild(ctx, RebuildRequest{ProtocolVersion: ProtocolVersion, ClientID: session.clientID, Scope: session.userScope, Cursor: cursor, Limit: 1})
	exchanges = append(exchanges, observedExchange)
	if err != nil {
		return failedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", exchanges, err)
	}
	captured := len(response.Records) == 1 && recordHasID(response.Records[0], insertedID) && !recordHasID(response.Records[0], lastID)
	return capturedProbe(DefectRebuildSnapshot, "immutable rebuild boundary across every page", captured, "a later rebuild page reads post-boundary live membership", exchanges), nil
}

func probeSchemaIntent(ctx context.Context, runtime *ProbeRuntime) (result ProbeResult, returnedErr error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectSchemaIntent)
	if err != nil {
		return failedProbe(DefectSchemaIntent, "queued mutation retains its authored schema intent", exchanges, err)
	}
	request := PushRequest{ProtocolVersion: ProtocolVersion, ClientID: session.clientID, BatchID: "diagnostic-schema-intent-batch", Schema: session.schema, Mutations: []Mutation{{
		MutationID: "diagnostic-schema-intent-mutation", Table: "cf_schema_queue", Operation: "insert",
		PrimaryKey: json.RawMessage(`{"id":"00000000-0000-0000-0000-000000000701"}`), ClientVersion: stringPointer("2026-01-01T00:00:01Z"),
		Columns: json.RawMessage(`{"owner_id":"diagnostic-user","authored_mutation":{"value":"retained"},"legacy_value":"authored-before-ddl"}`),
	}}}
	if err := runtime.dropHydrationColumn(ctx); err != nil {
		return failedProbe(DefectSchemaIntent, "queued mutation retains its authored schema intent", exchanges, err)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if cleanupErr := runtime.restoreSchemaQueue(cleanupContext); cleanupErr != nil {
			returnedErr = errors.Join(returnedErr, cleanupErr)
		}
	}()
	if err := runtime.registerSchemaQueue(ctx); err != nil {
		return failedProbe(DefectSchemaIntent, "queued mutation retains its authored schema intent", exchanges, err)
	}
	current, prepared, err := prepareSession(ctx, runtime, DefectSchemaIntent)
	exchanges = append(exchanges, prepared...)
	if err != nil {
		return failedProbe(DefectSchemaIntent, "queued mutation retains its authored schema intent", exchanges, err)
	}
	request.ClientID = current.clientID
	request.Schema = current.schema
	response, observed, err := runtime.Push(ctx, request)
	exchanges = append(exchanges, observed)
	if err != nil {
		return failedProbe(DefectSchemaIntent, "queued mutation retains its authored schema intent", exchanges, err)
	}
	captured := len(response.Accepted) == 1 && response.Accepted[0].MutationID == request.Mutations[0].MutationID && len(response.Rejected) == 0
	return capturedProbe(DefectSchemaIntent, "queued mutation retains its authored schema intent", captured, "a field removed after authoring is silently discarded and the queued mutation is applied", exchanges), nil
}

func probeCompactionInterval(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error) {
	_, exchanges, err := prepareSession(ctx, runtime, DefectCompactionInterval)
	if err != nil {
		return failedProbe(DefectCompactionInterval, "strict positive compaction stale interval", exchanges, err)
	}
	result, err := runtime.compactWithPositiveInterval(ctx)
	if err != nil {
		return failedProbe(DefectCompactionInterval, "strict positive compaction stale interval", exchanges, err)
	}
	captured := capturesPositiveCompactionInterval(result)
	if !captured {
		return failedProbe(DefectCompactionInterval, "strict positive compaction stale interval", exchanges, fmt.Errorf("positive near-zero interval did not deactivate a recently active client: %#v", result))
	}
	return capturedProbe(DefectCompactionInterval, "strict positive compaction stale interval", captured, "the positive one-microsecond interval deactivates a recently active client", exchanges), nil
}

func probeOwnershipChange(ctx context.Context, runtime *ProbeRuntime) (ProbeResult, error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectOwnershipChange)
	if err != nil {
		return failedProbe(DefectOwnershipChange, "dependency-driven ownership reassignment", exchanges, err)
	}
	documentID := "00000000-0000-0000-0000-000000000801"
	memberID := "00000000-0000-0000-0000-000000000802"
	noteID := "00000000-0000-0000-0000-000000000803"
	statements := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, $2, $3)", []any{documentID, diagnosticUser, "ownership-control"}},
		{"INSERT INTO cf_document_members (id, document_id, member_id) VALUES ($1, $2, $3)", []any{memberID, documentID, "diagnostic-member-two"}},
		{"INSERT INTO cf_document_notes (id, document_id, author_id, body) VALUES ($1, $2, $3, $4)", []any{noteID, documentID, "diagnostic-author-two", "dependent-row"}},
	}
	for _, statement := range statements {
		if err := runtime.sourceDML(ctx, statement.query, statement.args...); err != nil {
			return failedProbe(DefectOwnershipChange, "dependency-driven ownership reassignment", exchanges, err)
		}
	}
	_, observed, err := pullUntil(ctx, runtime, &session, 16, changeWithID(noteID))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectOwnershipChange, "dependency-driven ownership reassignment", exchanges, err)
	}
	if err := runtime.sourceDML(ctx, "UPDATE cf_documents SET owner_id = $1, updated_at = clock_timestamp() WHERE id = $2", "diagnostic-owner-two", documentID); err != nil {
		return failedProbe(DefectOwnershipChange, "dependency-driven ownership reassignment", exchanges, err)
	}
	sentinelID := "00000000-0000-0000-0000-000000000804"
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", sentinelID, diagnosticUser, "ownership-sentinel"); err != nil {
		return failedProbe(DefectOwnershipChange, "dependency-driven ownership reassignment", exchanges, err)
	}
	changes, observed, err := pullUntil(ctx, runtime, &session, 16, changeWithID(sentinelID))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectOwnershipChange, "dependency-driven ownership reassignment", exchanges, err)
	}
	documentDelete := hasChange(changes, "cf_documents", documentID, "delete", session.userScope)
	memberDelete := hasChange(changes, "cf_document_members", memberID, "delete", session.userScope)
	noteDelete := hasChange(changes, "cf_document_notes", noteID, "delete", session.userScope)
	captured := documentDelete && !memberDelete && !noteDelete
	return capturedProbe(DefectOwnershipChange, "dependency-driven ownership reassignment", captured, "dependent rows retain stale ownership membership after the parent owner changes", exchanges), nil
}

func probeCrossScopeDedup(ctx context.Context, runtime *ProbeRuntime) (result ProbeResult, returnedErr error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectCrossScopeDedup)
	if err != nil {
		return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, err)
	}
	if err := runtime.configureCrossScopeTable(ctx); err != nil {
		return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, err)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if cleanupErr := runtime.restoreCrossScopeTable(cleanupContext); cleanupErr != nil {
			returnedErr = errors.Join(returnedErr, cleanupErr)
		}
	}()
	session, connected, err := bootstrapSession(ctx, runtime, DefectCrossScopeDedup)
	exchanges = append(exchanges, connected...)
	if err != nil {
		return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, err)
	}
	if _, found := session.scopes["cf:dedup"]; !found {
		return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, errors.New("shared diagnostic scope was not assigned"))
	}
	reloaded := false
	for attempt := 0; attempt < 8 && !reloaded; attempt++ {
		if err := runtime.reloadRegistry(ctx); err != nil {
			return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, err)
		}
		warmupID := fmt.Sprintf("00000000-0000-0000-0000-%012d", 900+attempt)
		if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", warmupID, diagnosticUser, "registry-reload-warmup"); err != nil {
			return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, err)
		}
		for polls := 0; polls < 20; polls++ {
			response, observed, rebuildErr := runtime.Rebuild(ctx, RebuildRequest{ProtocolVersion: ProtocolVersion, ClientID: session.clientID, Scope: "cf:dedup", Limit: 1000})
			exchanges = append(exchanges, observed)
			if rebuildErr != nil {
				return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, rebuildErr)
			}
			for _, record := range response.Records {
				if recordHasID(record, warmupID) {
					reloaded = true
					break
				}
			}
			if reloaded {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !reloaded {
		return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, errors.New("worker did not reload the controlled registry"))
	}
	for _, scope := range []string{session.userScope, "cf:dedup"} {
		observed, rebuildErr := rebuildScopeToTerminal(ctx, runtime, &session, scope, 1000)
		exchanges = append(exchanges, observed...)
		if rebuildErr != nil {
			return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, rebuildErr)
		}
	}
	targetID := "00000000-0000-0000-0000-000000000999"
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", targetID, diagnosticUser, "cross-scope-control"); err != nil {
		return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, err)
	}
	for polls := 0; polls < 100; polls++ {
		response, observed, rebuildErr := runtime.Rebuild(ctx, RebuildRequest{ProtocolVersion: ProtocolVersion, ClientID: session.clientID, Scope: "cf:dedup", Limit: 1000})
		exchanges = append(exchanges, observed)
		if rebuildErr != nil {
			return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, rebuildErr)
		}
		if recordsContainID(response.Records, targetID) {
			break
		}
		if polls == 99 {
			return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, errors.New("cross-scope row did not materialize"))
		}
		time.Sleep(50 * time.Millisecond)
	}
	response, observed, err := runtime.Pull(ctx, session.pullRequest(16))
	exchanges = append(exchanges, observed)
	if err != nil {
		return failedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", exchanges, err)
	}
	count := 0
	seenScopes := make(map[string]struct{})
	for _, change := range response.Changes {
		if changeWithID(targetID)(change) {
			count++
			seenScopes[change.Scope] = struct{}{}
		}
	}
	captured := count == 1 && len(seenScopes) == 1
	return capturedProbe(DefectCrossScopeDedup, "scope-inclusive pull deduplication", captured, "one row assigned to two scopes is delivered under only one scope", exchanges), nil
}

func probeRegistryReload(ctx context.Context, runtime *ProbeRuntime) (result ProbeResult, returnedErr error) {
	session, exchanges, err := prepareSession(ctx, runtime, DefectRegistryReload)
	if err != nil {
		return failedProbe(DefectRegistryReload, "runtime registry activation", exchanges, err)
	}
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_late_registration (id, owner_id, value) VALUES ($1, $2, $3)", "00000000-0000-0000-0000-000000001001", diagnosticUser, "before-registration"); err != nil {
		return failedProbe(DefectRegistryReload, "runtime registry activation", exchanges, err)
	}
	if err := runtime.registerLateSourceTable(ctx); err != nil {
		return failedProbe(DefectRegistryReload, "runtime registry activation", exchanges, err)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if cleanupErr := runtime.unregisterLateSourceTable(cleanupContext); cleanupErr != nil {
			returnedErr = errors.Join(returnedErr, cleanupErr)
		}
	}()
	session, prepared, err := prepareSession(ctx, runtime, DefectRegistryReload)
	exchanges = append(exchanges, prepared...)
	if err != nil {
		return failedProbe(DefectRegistryReload, "runtime registry activation", exchanges, err)
	}
	targetID := "00000000-0000-0000-0000-000000001002"
	sentinelID := "00000000-0000-0000-0000-000000001003"
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_late_registration (id, owner_id, value) VALUES ($1, $2, $3)", targetID, diagnosticUser, "after-registration"); err != nil {
		return failedProbe(DefectRegistryReload, "runtime registry activation", exchanges, err)
	}
	if err := runtime.sourceDML(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", sentinelID, diagnosticUser, "registry-sentinel"); err != nil {
		return failedProbe(DefectRegistryReload, "runtime registry activation", exchanges, err)
	}
	changes, observed, err := pullUntil(ctx, runtime, &session, 16, changeWithID(sentinelID))
	exchanges = append(exchanges, observed...)
	if err != nil {
		return failedProbe(DefectRegistryReload, "runtime registry activation", exchanges, err)
	}
	captured := !hasChange(changes, "cf_late_registration", targetID, "upsert", session.userScope)
	return capturedProbe(DefectRegistryReload, "runtime registry activation", captured, "registration notification does not activate the table in the running worker", exchanges), nil
}

func rebuildScopeToTerminal(ctx context.Context, runtime *ProbeRuntime, session *diagnosticSession, scope string, limit int) ([]exchange, error) {
	var cursor *string
	var exchanges []exchange
	for pages := 0; pages < 256; pages++ {
		response, observed, err := runtime.Rebuild(ctx, RebuildRequest{
			ProtocolVersion: ProtocolVersion, ClientID: session.clientID, Scope: scope, Cursor: cursor, Limit: limit,
		})
		exchanges = append(exchanges, observed)
		if err != nil {
			return exchanges, err
		}
		if !response.HasMore {
			if response.FinalScopeCursor == nil || *response.FinalScopeCursor == "" {
				return exchanges, errors.New("terminal rebuild cursor is absent")
			}
			value := *response.FinalScopeCursor
			session.scopes[scope] = ScopeCursor{Cursor: &value}
			return exchanges, nil
		}
		if response.Cursor == nil || *response.Cursor == "" {
			return exchanges, errors.New("rebuild continuation is absent")
		}
		value := *response.Cursor
		cursor = &value
	}
	return exchanges, errors.New("diagnostic rebuild did not reach a terminal page")
}

func changeWithID(id string) func(ChangeRecord) bool {
	return func(change ChangeRecord) bool { return primaryKeyID(change.PrimaryKey) == id }
}

func primaryKeyID(raw json.RawMessage) string {
	var value map[string]string
	if json.Unmarshal(raw, &value) != nil || len(value) != 1 {
		return ""
	}
	return value["id"]
}

func recordHasID(record RebuildRecord, id string) bool { return primaryKeyID(record.PrimaryKey) == id }

func recordsContainID(records []RebuildRecord, id string) bool {
	for _, record := range records {
		if recordHasID(record, id) {
			return true
		}
	}
	return false
}

func hasChange(changes []ChangeRecord, table, id, operation, scope string) bool {
	for _, change := range changes {
		if change.Table == table && primaryKeyID(change.PrimaryKey) == id && change.Operation == operation && change.Scope == scope {
			return true
		}
	}
	return false
}

func isLowerHexSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func inspectHydrationError(body []byte) (canonical, legacy, noProgress bool, err error) {
	var envelope map[string]json.RawMessage
	if err := decodeProtocol2JSON(body, &envelope); err != nil {
		return false, false, false, errors.New("hydration error body is not valid JSON")
	}
	errorRaw, found := envelope["error"]
	if !found || len(envelope) != 1 {
		return false, false, false, errors.New("hydration error body has an invalid envelope")
	}
	noProgress = true
	for _, key := range []string{"changes", "records", "cursor", "final_scope_cursor", "scope_cursors", "checksums", "checkpoint", "scope_updates", "rebuild", "has_more"} {
		if _, found := envelope[key]; found {
			noProgress = false
		}
	}
	var protocol struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	}
	if json.Unmarshal(errorRaw, &protocol) == nil && protocol.Code != "" {
		canonical = protocol.Code == "sync_integrity_failure" && !protocol.Retryable && protocol.Message != "" && len(protocol.Message) <= 256
		return canonical, false, noProgress, nil
	}
	var message string
	if json.Unmarshal(errorRaw, &message) == nil {
		legacy = message == "internal error"
	}
	return false, legacy, noProgress, nil
}

func capturesHydrationFailure(hasError bool, status int, canonical, legacy, noProgress, rowReturned bool) bool {
	return hasError && status == 500 && !canonical && legacy && noProgress && rowReturned
}

func capturesPositiveCompactionInterval(result CompactionResult) bool {
	return result.DeactivatedClients > 0
}

func capturedProbe(family DefectFamily, expected string, captured bool, divergence string, exchanges []exchange) ProbeResult {
	return ProbeResult{Family: family, ExpectedContract: expected, Divergence: divergence, Captured: captured, ReceiptIDs: exchangeReceiptIDs(exchanges)}
}

func failedProbe(family DefectFamily, expected string, exchanges []exchange, cause error) (ProbeResult, error) {
	result := ProbeResult{Family: family, ExpectedContract: expected, Divergence: "diagnostic execution did not reach a contract comparison", Captured: false, ReceiptIDs: exchangeReceiptIDs(exchanges)}
	return result, fmt.Errorf("run %s diagnostic: %w", family, cause)
}

func exchangeReceiptIDs(exchanges []exchange) []string {
	seen := make(map[string]struct{}, len(exchanges))
	identifiers := make([]string, 0, len(exchanges))
	for _, exchange := range exchanges {
		identifier := exchange.receipt.ID()
		if identifier == "" {
			continue
		}
		if _, found := seen[identifier]; found {
			continue
		}
		seen[identifier] = struct{}{}
		identifiers = append(identifiers, identifier)
	}
	return identifiers
}

func stringPointer(value string) *string { return &value }
