package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealS02DivergentPullPaginationIsStarvationFree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	anchorID := "00000000-0000-4000-8000-00000000b010"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		anchorID,
		"diagnostic-user",
		"s02-scope-anchor",
	); err != nil {
		t.Fatalf("insert S-02 scope anchor: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", anchorID)
	client := connectRealProtocolClient(t, ctx, harness, token, "s02-pull-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8000-00000000b001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8000-00000000b002")
	userStartingScope := client.Scopes["user:diagnostic-user"]

	globalTable := requireRealTable(t, client, "cf_global_items")
	userTable := requireRealTable(t, client, "cf_items")
	globalFirstID := "00000000-0000-4000-8000-00000000b011"
	globalSecondID := "00000000-0000-4000-8000-00000000b012"
	userID := "00000000-0000-4000-8000-00000000b013"
	globalAfterUserID := "00000000-0000-4000-8000-00000000b014"

	for _, row := range []struct {
		id    string
		value string
	}{
		{globalFirstID, "s02-global-history-one"},
		{globalSecondID, "s02-global-history-two"},
	} {
		if err := harness.Source().ExecContext(
			ctx,
			"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
			row.id,
			row.value,
		); err != nil {
			t.Fatalf("insert S-02 global history row %s: %v", row.id, err)
		}
	}
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", globalFirstID, globalSecondID)

	firstGlobalPage := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 16)
	firstGlobalChanges := requireRealChanges(t, firstGlobalPage)
	if len(firstGlobalChanges) != 2 || firstGlobalPage["has_more"] != false {
		t.Fatalf("S-02 global history page = %#v", firstGlobalPage)
	}
	requireRealPullChange(t, firstGlobalChanges, "cf:global", globalTable, globalFirstID, "s02-global-history-one")
	requireRealPullChange(t, firstGlobalChanges, "cf:global", globalTable, globalSecondID, "s02-global-history-two")
	if _, ok := firstGlobalPage["checksums"]; !ok {
		t.Fatal("S-02 terminal global history page omitted checksums")
	}
	client.Scopes["user:diagnostic-user"] = userStartingScope

	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		userID,
		"diagnostic-user",
		"s02-user-row",
	); err != nil {
		t.Fatalf("insert S-02 user row: %v", err)
	}
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
		globalAfterUserID,
		"s02-global-after-user",
	); err != nil {
		t.Fatalf("insert S-02 post-user global row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", userID)
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", globalAfterUserID)

	firstDivergentPage := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 1)
	firstDivergentChanges := requireRealChanges(t, firstDivergentPage)
	if len(firstDivergentChanges) != 1 || firstDivergentPage["has_more"] != true {
		t.Fatalf("S-02 first divergent page = %#v", firstDivergentPage)
	}
	requireRealPullChange(t, firstDivergentChanges, "user:diagnostic-user", userTable, userID, "s02-user-row")
	if _, ok := firstDivergentPage["checksums"]; ok {
		t.Fatal("S-02 nonterminal divergent page returned checksums")
	}
	if _, ok := firstDivergentPageScopeCursor(t, firstDivergentPage, "user:diagnostic-user"); !ok {
		t.Fatal("S-02 user scope did not receive represented progress")
	}
	if _, ok := firstDivergentPageScopeCursor(t, firstDivergentPage, "cf:global"); ok {
		t.Fatal("S-02 blocked global scope advanced past its unselected candidate")
	}

	secondDivergentPage := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 1)
	secondDivergentChanges := requireRealChanges(t, secondDivergentPage)
	if len(secondDivergentChanges) != 1 || secondDivergentPage["has_more"] != false {
		t.Fatalf("S-02 terminal divergent page = %#v", secondDivergentPage)
	}
	requireRealPullChange(t, secondDivergentChanges, "cf:global", globalTable, globalAfterUserID, "s02-global-after-user")
	checksums, ok := secondDivergentPage["checksums"].(map[string]any)
	if !ok || len(checksums) != 2 {
		t.Fatalf("S-02 terminal checksum map = %#v", secondDivergentPage["checksums"])
	}

	acknowledgement := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 1)
	if changes := requireRealChanges(t, acknowledgement); len(changes) != 0 || acknowledgement["has_more"] != false {
		t.Fatalf("S-02 acknowledgement page = %#v", acknowledgement)
	}

	collisionID := "00000000-0000-4000-8000-00000000b015"
	transaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin S-02 typed-collision transaction: %v", err)
	}
	if _, err := transaction.ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		collisionID,
		"diagnostic-user",
		"s02-user-collision",
	); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("insert S-02 user collision row: %v", err)
	}
	if _, err := transaction.ExecContext(
		ctx,
		"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
		collisionID,
		"s02-global-collision",
	); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("insert S-02 global collision row: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit S-02 typed-collision transaction: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", collisionID)
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", collisionID)
	collisionPage := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 2)
	collisionChanges := requireRealChanges(t, collisionPage)
	if len(collisionChanges) != 2 || collisionPage["has_more"] != false {
		t.Fatalf("S-02 typed-collision page = %#v", collisionPage)
	}
	requireRealPullChange(t, collisionChanges, "user:diagnostic-user", userTable, collisionID, "s02-user-collision")
	requireRealPullChange(t, collisionChanges, "cf:global", globalTable, collisionID, "s02-global-collision")
	acknowledgeRealClientCursors(t, ctx, harness, token, client)

	checkpoints := observeCheckpointMap(t, ctx, harness, client.ID)
	assertDiagnosticCheckpointScopes(t, checkpoints)
}

func TestRealS03PullHydrationFailurePreservesCursors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "s03-hydration-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8000-00000000b101")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8000-00000000b102")

	recordID := "00000000-0000-4000-8000-00000000b111"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_schema_queue (id, owner_id, authored_mutation, legacy_value) VALUES ($1, $2, $3::jsonb, $4)",
		recordID,
		"diagnostic-user",
		`{"value":"s03-authored"}`,
		"s03-captured-value",
	); err != nil {
		t.Fatalf("insert S-03 hydration row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_schema_queue", recordID)

	witness := cloneRealProtocolClient(client)
	witnessResponse := pullRealClient(t, ctx, harness, token, witness)
	witnessChanges := requireRealChanges(t, witnessResponse)
	change := requireSchemaQueuePullChange(t, witnessChanges, "user:diagnostic-user", recordID, "s03-captured-value")
	if change["table"] == "" {
		t.Fatal("S-03 witness change has no logical table identity")
	}
	beforeFailure := observeCheckpointMap(t, ctx, harness, client.ID)

	if err := harness.Operator().DropHydrationColumn(ctx); err != nil {
		t.Fatalf("drop S-03 hydration column: %v", err)
	}
	dropped := true
	t.Cleanup(func() {
		if dropped {
			cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if err := harness.Operator().RestoreHydrationColumn(cleanupContext); err != nil {
				t.Errorf("restore S-03 hydration column: %v", err)
			}
		}
	})

	status, failed := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, client.Scopes, 100))
	requireRealProtocolError(t, status, failed, http.StatusInternalServerError, "sync_integrity_failure")
	afterFailure := observeCheckpointMap(t, ctx, harness, client.ID)
	assertCheckpointMapsEqual(t, beforeFailure, afterFailure)

	if err := harness.Operator().RestoreHydrationColumn(ctx); err != nil {
		t.Fatalf("restore S-03 hydration column: %v", err)
	}
	dropped = false

	restored := pullRealClient(t, ctx, harness, token, client)
	restoredChanges := requireRealChanges(t, restored)
	restoredChange := requireSchemaQueuePullChange(t, restoredChanges, "user:diagnostic-user", recordID, "s03-captured-value")
	if restoredChange["table"] != change["table"] {
		t.Fatalf("S-03 source table identity changed: first=%v restored=%v", change["table"], restoredChange["table"])
	}
	if afterSuccess := observeCheckpointMap(t, ctx, harness, client.ID); len(afterSuccess) != len(beforeFailure) {
		t.Fatal("S-03 successful response acknowledged a cursor before presentation")
	}

	acknowledgement := pullRealClient(t, ctx, harness, token, client)
	if changes := requireRealChanges(t, acknowledgement); len(changes) != 0 {
		t.Fatalf("S-03 acknowledgement delivered duplicate changes: %#v", changes)
	}
	final := observeCheckpointMap(t, ctx, harness, client.ID)
	assertDiagnosticCheckpointScopes(t, final)
	if sameCheckpointPosition(final["user:diagnostic-user"], beforeFailure["user:diagnostic-user"]) {
		t.Fatal("S-03 restored row did not advance the user checkpoint after acknowledgement")
	}
}

func TestRealS04RebuildRejectsForgedCursorAndFreezesBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "s04-rebuild-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8000-00000000b201")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8000-00000000b202")

	table := requireRealTable(t, client, "cf_items")
	firstID := "00000000-0000-4000-8000-00000000b211"
	lastID := "00000000-0000-4000-8000-00000000b213"
	postBoundaryID := "00000000-0000-4000-8000-00000000b212"
	for _, row := range []struct {
		id    string
		value string
	}{
		{firstID, "s04-first"},
		{lastID, "s04-last"},
	} {
		if err := harness.Source().ExecContext(
			ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
			row.id,
			"diagnostic-user",
			row.value,
		); err != nil {
			t.Fatalf("insert S-04 source row %s: %v", row.id, err)
		}
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", firstID, lastID)
	pullUntilRealRecords(t, ctx, harness, token, client, []realRecordExpectation{
		{scopeID: "user:diagnostic-user", table: table, recordID: firstID, value: "s04-first"},
		{scopeID: "user:diagnostic-user", table: table, recordID: lastID, value: "s04-last"},
	})
	acknowledgeRealClientCursors(t, ctx, harness, token, client)
	beforeRebuild := observeCheckpointMap(t, ctx, harness, client.ID)

	rebuildID := "00000000-0000-4000-8000-00000000b221"
	status, firstPage := requestRealRebuildPage(t, ctx, harness, token, client, "user:diagnostic-user", rebuildID, nil, 1)
	if status != http.StatusOK {
		t.Fatalf("S-04 first rebuild page status = %d: %#v", status, firstPage)
	}
	firstRecords := requireRealRebuildRecords(t, firstPage)
	if len(firstRecords) != 1 || firstPage["has_more"] != true {
		t.Fatalf("S-04 first rebuild page = %#v", firstPage)
	}
	requireRebuildRecordVersion(t, firstRecords, table, firstID, "s04-first")
	continuation, ok := firstPage["cursor"].(string)
	if !ok || continuation == "" || firstPage["final_scope_cursor"] != nil || firstPage["checksum"] != nil {
		t.Fatalf("S-04 intermediate rebuild continuation is invalid: %#v", firstPage)
	}

	firstBoundary, err := harness.Operator().ObserveRebuildSession(ctx, client.ID, rebuildID)
	if err != nil {
		t.Fatalf("observe S-04 rebuild boundary: %v", err)
	}
	if firstBoundary.PageLimit != 1 || firstBoundary.StagedRowCount != 2 || firstBoundary.BoundaryPositionKind != "transaction_end" {
		t.Fatalf("S-04 rebuild session boundary is invalid: %#v", firstBoundary)
	}
	checkpointBeforeForge := observeCheckpointMap(t, ctx, harness, client.ID)

	status, forged := requestRealRebuildPage(t, ctx, harness, token, client, "user:diagnostic-user", rebuildID, "zzzzzzzz|zzzzzzzz", 1)
	requireRealProtocolError(t, status, forged, http.StatusBadRequest, "invalid_request")
	checkpointAfterForge := observeCheckpointMap(t, ctx, harness, client.ID)
	assertCheckpointMapsEqual(t, checkpointBeforeForge, checkpointAfterForge)
	afterForgeBoundary, err := harness.Operator().ObserveRebuildSession(ctx, client.ID, rebuildID)
	if err != nil {
		t.Fatalf("observe S-04 boundary after forged cursor: %v", err)
	}
	if afterForgeBoundary != firstBoundary {
		t.Fatalf("S-04 forged cursor changed rebuild session: before=%#v after=%#v", firstBoundary, afterForgeBoundary)
	}

	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		postBoundaryID,
		"diagnostic-user",
		"s04-post-boundary",
	); err != nil {
		t.Fatalf("insert S-04 post-boundary row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", postBoundaryID)

	status, finalPage := requestRealRebuildPage(t, ctx, harness, token, client, "user:diagnostic-user", rebuildID, continuation, 1)
	if status != http.StatusOK {
		t.Fatalf("S-04 final rebuild page status = %d: %#v", status, finalPage)
	}
	finalRecords := requireRealRebuildRecords(t, finalPage)
	if len(finalRecords) != 1 || finalPage["has_more"] != false || finalPage["cursor"] != nil {
		t.Fatalf("S-04 final rebuild page = %#v", finalPage)
	}
	requireRebuildRecordVersion(t, finalRecords, table, lastID, "s04-last")
	if realRebuildRecordsContainID(finalRecords, table, postBoundaryID) {
		t.Fatal("S-04 post-boundary row appeared in the immutable rebuild snapshot")
	}
	finalScopeCursor, ok := finalPage["final_scope_cursor"].(string)
	if !ok || finalScopeCursor == "" {
		t.Fatalf("S-04 final scope cursor is invalid: %#v", finalPage)
	}
	if checksum, ok := finalPage["checksum"].(map[string]any); !ok || len(checksum) == 0 {
		t.Fatalf("S-04 final checksum is invalid: %#v", finalPage["checksum"])
	}

	secondBoundary, err := harness.Operator().ObserveRebuildSession(ctx, client.ID, rebuildID)
	if err != nil {
		t.Fatalf("observe S-04 final rebuild boundary: %v", err)
	}
	if secondBoundary != firstBoundary {
		t.Fatalf("S-04 rebuild boundary changed across pages: first=%#v final=%#v", firstBoundary, secondBoundary)
	}
	checkpointAfterRebuild := observeCheckpointMap(t, ctx, harness, client.ID)
	assertCheckpointMapsEqual(t, beforeRebuild, checkpointAfterRebuild)

	client.Scopes["user:diagnostic-user"] = map[string]any{"cursor": finalScopeCursor}
	pullAfterRebuild := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 100)
	pullAfterRebuildChanges := requireRealChanges(t, pullAfterRebuild)
	if len(pullAfterRebuildChanges) != 1 {
		t.Fatalf("S-04 post-boundary pull changes = %#v", pullAfterRebuildChanges)
	}
	requireRealPullChange(t, pullAfterRebuildChanges, "user:diagnostic-user", table, postBoundaryID, "s04-post-boundary")
	acknowledgeRealClientCursors(t, ctx, harness, token, client)
}

func pullRealClientWithLimit(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	token string,
	client *realProtocolClient,
	scopes map[string]any,
	limit int,
) map[string]any {
	t.Helper()
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, scopes, limit))
	if status != http.StatusOK {
		t.Fatalf("real client pull status = %d, want 200: %#v", status, response)
	}
	if rebuild, ok := response["rebuild"].([]any); !ok || len(rebuild) != 0 {
		t.Fatalf("real client pull requested rebuild: %#v", response)
	}
	scopeCursors, ok := response["scope_cursors"].(map[string]any)
	if !ok {
		t.Fatalf("real client pull cursors are invalid: %#v", response)
	}
	for scopeID, rawCursor := range scopeCursors {
		cursor, ok := rawCursor.(string)
		if !ok || cursor == "" {
			t.Fatalf("real client pull cursor is invalid: %#v", rawCursor)
		}
		if _, assigned := scopes[scopeID]; !assigned {
			t.Fatalf("real client pull returned an unassigned scope cursor: %s", scopeID)
		}
		scopes[scopeID] = map[string]any{"cursor": cursor}
		client.Scopes[scopeID] = map[string]any{"cursor": cursor}
	}
	return response
}

func realPullPayload(client *realProtocolClient, scopes map[string]any, limit int) map[string]any {
	return map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"schema":            client.Schema,
		"scope_set_version": client.ScopeSetVersion,
		"scopes":            scopes,
		"limit":             limit,
	}
}

func requestRealRebuildPage(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	token string,
	client *realProtocolClient,
	scopeID string,
	rebuildID string,
	cursor any,
	limit int,
) (int, map[string]any) {
	t.Helper()
	return postSync(t, ctx, harness.AdapterURL(), token, "/sync/rebuild", map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"schema":            client.Schema,
		"scope":             scopeID,
		"rebuild_id":        rebuildID,
		"cursor":            cursor,
		"limit":             limit,
	})
}

func requireRealChanges(t *testing.T, response map[string]any) []map[string]any {
	t.Helper()
	rawChanges, ok := response["changes"].([]any)
	if !ok {
		t.Fatalf("real pull changes are invalid: %#v", response)
	}
	changes := make([]map[string]any, 0, len(rawChanges))
	for _, rawChange := range rawChanges {
		change, ok := rawChange.(map[string]any)
		if !ok {
			t.Fatalf("real pull change is invalid: %#v", rawChange)
		}
		changes = append(changes, change)
	}
	return changes
}

func requireRealPullChange(t *testing.T, changes []map[string]any, scopeID string, table realProtocolTable, recordID, value string) map[string]any {
	t.Helper()
	for _, change := range changes {
		if change["scope"] != scopeID || change["table"] != table.ID {
			continue
		}
		pk, ok := change["pk"].(map[string]any)
		if !ok || pk[table.PrimaryKeyField] != recordID {
			continue
		}
		row, ok := change["row"].(map[string]any)
		if !ok || row[table.ValueField] != value {
			t.Fatalf("real pull row for %s is invalid: %#v", recordID, change)
		}
		return change
	}
	t.Fatalf("real pull did not return %s in scope %s: %#v", recordID, scopeID, changes)
	return nil
}

func requireSchemaQueuePullChange(t *testing.T, changes []map[string]any, scopeID, recordID, value string) map[string]any {
	t.Helper()
	for _, change := range changes {
		if change["scope"] != scopeID || change["table"] == "" {
			continue
		}
		pk, ok := change["pk"].(map[string]any)
		if !ok || !mapContainsValue(pk, recordID) {
			continue
		}
		row, ok := change["row"].(map[string]any)
		if !ok || !mapContainsValue(row, value) {
			t.Fatalf("S-03 captured row is invalid: %#v", change)
		}
		return change
	}
	t.Fatalf("S-03 pull did not return source identity %s: %#v", recordID, changes)
	return nil
}

func mapContainsValue(values map[string]any, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func firstDivergentPageScopeCursor(t *testing.T, response map[string]any, scopeID string) (string, bool) {
	t.Helper()
	cursors, ok := response["scope_cursors"].(map[string]any)
	if !ok {
		t.Fatalf("pull scope cursors are invalid: %#v", response)
	}
	cursor, present := cursors[scopeID]
	if !present {
		return "", false
	}
	value, ok := cursor.(string)
	if !ok || value == "" {
		t.Fatalf("pull scope cursor is invalid: %#v", cursor)
	}
	return value, true
}

func requireRealRebuildRecords(t *testing.T, response map[string]any) []map[string]any {
	t.Helper()
	rawRecords, ok := response["records"].([]any)
	if !ok {
		t.Fatalf("real rebuild records are invalid: %#v", response)
	}
	records := make([]map[string]any, 0, len(rawRecords))
	for _, rawRecord := range rawRecords {
		record, ok := rawRecord.(map[string]any)
		if !ok {
			t.Fatalf("real rebuild record is invalid: %#v", rawRecord)
		}
		records = append(records, record)
	}
	return records
}

func realRebuildRecordsContainID(records []map[string]any, table realProtocolTable, recordID string) bool {
	for _, record := range records {
		if record["table"] != table.ID {
			continue
		}
		pk, ok := record["pk"].(map[string]any)
		if ok && pk[table.PrimaryKeyField] == recordID {
			return true
		}
	}
	return false
}

func requireRealProtocolError(t *testing.T, status int, response map[string]any, wantStatus int, wantCode string) {
	t.Helper()
	if status != wantStatus {
		t.Fatalf("protocol error status = %d, want %d: %#v", status, wantStatus, response)
	}
	if len(response) != 1 {
		t.Fatalf("protocol error contains success fields: %#v", response)
	}
	errorBody, ok := response["error"].(map[string]any)
	if !ok || errorBody["code"] != wantCode || errorBody["retryable"] != false {
		t.Fatalf("protocol error body = %#v, want code %q and retryable false", response, wantCode)
	}
}

func waitForRealWALRecords(t *testing.T, ctx context.Context, harness *blackbox.Harness, tableName string, recordIDs ...string) {
	t.Helper()
	var lastObservation blackbox.WALPipelineObservation
	var lastErr error
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		observation, err := harness.Operator().ObserveWALRecordsForTable(ctx, tableName, recordIDs)
		lastObservation = observation
		lastErr = err
		if err == nil && len(observation.Records) == len(recordIDs) && observation.WorkerRunning && !observation.BlockingPoison && observation.ContiguousAcknowledged {
			return
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			t.Fatalf("wait for %s WAL records: %v", tableName, ctx.Err())
		case <-timer.C:
		}
	}
	stages, stageErr := harness.Operator().ObserveWALRecordStages(ctx, tableName, recordIDs)
	diagnostics, diagnosticsErr := harness.Operator().WALDiagnostics(ctx)
	peekDiagnostics, peekDiagnosticsErr := harness.Operator().WorkerPeekDiagnostics(ctx)
	t.Fatalf(
		"real WAL did not materialize %s records %v: observation=%#v err=%v stages=%#v stage_err=%v diagnostics=%s diagnostics_err=%v peek=%s peek_err=%v; %s",
		tableName,
		recordIDs,
		lastObservation,
		lastErr,
		stages,
		stageErr,
		diagnostics,
		diagnosticsErr,
		peekDiagnostics,
		peekDiagnosticsErr,
		harness.FailureDiagnostics(),
	)
}

func assertCheckpointMapsEqual(t *testing.T, left, right map[string]blackbox.ClientCheckpointObservation) {
	t.Helper()
	if len(left) != len(right) {
		t.Fatalf("checkpoint map lengths differ: before=%d after=%d", len(left), len(right))
	}
	for scopeID, checkpoint := range left {
		other, ok := right[scopeID]
		if !ok || !sameCheckpointPosition(checkpoint, other) {
			t.Fatalf("checkpoint for %s changed: before=%#v after=%#v", scopeID, checkpoint, right[scopeID])
		}
	}
}

func cloneRealProtocolClient(client *realProtocolClient) *realProtocolClient {
	clone := *client
	clone.Scopes = make(map[string]any, len(client.Scopes))
	for scopeID, scope := range client.Scopes {
		clone.Scopes[scopeID] = scope
	}
	clone.Tables = make(map[string]realProtocolTable, len(client.Tables))
	for tableName, table := range client.Tables {
		clone.Tables[tableName] = table
	}
	return &clone
}
