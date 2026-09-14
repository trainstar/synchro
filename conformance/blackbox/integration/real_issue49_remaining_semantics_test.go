package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

func TestRealIssue49RemainingSemantics(t *testing.T) {
	t.Run("assertion", issue49RemainingCanonicalTime)
	t.Run("assertion", issue49RemainingSchemaAuthority)
	t.Run("assertion", issue49RemainingSynchroCRUD)
	t.Run("assertion", issue49RemainingVersionFence)
	t.Run("assertion", issue49RemainingProgressIntegritySeparation)
	t.Run("assertion", issue49RemainingSeedContinuation)
	t.Run("assertion", issue49RemainingExplicitFailure)
	t.Run("assertion", issue49RemainingNoCustomBackend)
	t.Run("assertion", issue49RemainingVocabulary)
	t.Run("assertion", issue49RemainingLedgerRetention)
	t.Run("assertion", issue49RemainingAtomicPush)
	t.Run("assertion", issue49RemainingCompareAndSwap)
	t.Run("assertion", issue49RemainingClientTime)
	t.Run("assertion", issue49RemainingSealedRetry)
	t.Run("assertion", issue49RemainingTypedDeduplication)
	t.Run("assertion", issue49RemainingCursorBindings)
	t.Run("assertion", issue49RemainingRebuildIsolation)
	t.Run("assertion", issue49RemainingTypedRows)
	t.Run("assertion", issue49RemainingRowDigest)
	t.Run("assertion", issue49RemainingScopeDigest)
	t.Run("assertion", issue49RemainingTerminalChecksums)
	t.Run("assertion", issue49RemainingOperationalRedaction)
	t.Run("assertion", issue49RemainingSeedTransaction)
	t.Run("assertion", issue49RemainingSeedTokenBinding)
	t.Run("assertion", issue49RemainingSeedArtifactVerification)
	t.Run("assertion", issue49RemainingRetentionFloor)
	t.Run("assertion", issue49RemainingPortableInteger)
	t.Run("assertion", issue49RemainingOutcomeSchema)
	t.Run("assertion", issue49RemainingEffectProgress)
	t.Run("assertion", issue49RemainingProjectionBootstrap)
	t.Run("assertion", issue49RemainingSchemaCursorContinuity)
}

func issue49RemainingHarness(t *testing.T, timeout time.Duration) (context.Context, *blackbox.Harness, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	harness, token := provisionRealProofHarness(t, ctx)
	return ctx, harness, token
}

func issue49RemainingCanonicalTime(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 3*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-time")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	recordIDs := []string{
		"00000000-0000-4000-8e01-000000000001",
		"00000000-0000-4000-8e01-000000000002",
	}
	for index, timestamp := range []string{
		"2032-01-02T03:04:05Z",
		"2032-01-02T04:04:05.000000+01:00",
	} {
		mutation := phase4InsertMutation(
			client,
			table,
			ownerField,
			fmt.Sprintf("00000000-0000-4000-8e01-%012x", index+11),
			recordIDs[index],
			"noncanonical-time",
		)
		mutation["client_version"] = timestamp
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
			client,
			fmt.Sprintf("00000000-0000-4000-8e01-%012x", index+21),
			[]map[string]any{mutation},
		))
		requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_request")
	}
	observation, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, recordIDs)
	if err != nil {
		t.Fatalf("observe noncanonical time requests: %v", err)
	}
	if observation.BatchCount != 0 || observation.MutationCount != 0 || observation.SourceRowCount != 0 {
		t.Fatalf("noncanonical time changed durable state: %#v", observation)
	}
}

func issue49RemainingSchemaAuthority(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 3*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-schema-authority")
	status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": client.ScopeSetVersion,
		"known_scopes":      client.Scopes,
	})
	requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_schema_reference")
}

func issue49RemainingSynchroCRUD(t *testing.T) {
	issue49RemainingExerciseCRUD(t, "issue49-remaining-crud", "8e02")
}

func issue49RemainingNoCustomBackend(t *testing.T) {
	issue49RemainingExerciseCRUD(t, "issue49-remaining-no-backend", "8e03")
}

func issue49RemainingExerciseCRUD(t *testing.T, clientID, group string) {
	t.Helper()
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, clientID)
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	recordID := fmt.Sprintf("00000000-0000-4000-%s-000000000001", group)
	insertID := fmt.Sprintf("00000000-0000-4000-%s-000000000002", group)
	insertStatus, inserted := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		fmt.Sprintf("00000000-0000-4000-%s-000000000003", group),
		[]map[string]any{phase4InsertMutation(client, table, ownerField, insertID, recordID, "created")},
	))
	if insertStatus != http.StatusOK {
		t.Fatalf("canonical insert status = %d: %#v", insertStatus, inserted)
	}
	insertVersion := issue49RequireOpaqueVersion(t, issue49RequireAcceptedOutcome(t, inserted, insertID, "applied"))
	updateID := fmt.Sprintf("00000000-0000-4000-%s-000000000004", group)
	updateStatus, updated := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		fmt.Sprintf("00000000-0000-4000-%s-000000000005", group),
		[]map[string]any{issue49Mutation(client, table, updateID, recordID, "update", insertVersion, phase4ClientVersion, map[string]any{
			table.ValueField: "updated",
		})},
	))
	if updateStatus != http.StatusOK {
		t.Fatalf("canonical update status = %d: %#v", updateStatus, updated)
	}
	updateVersion := issue49RequireOpaqueVersion(t, issue49RequireAcceptedOutcome(t, updated, updateID, "applied"))
	deleteID := fmt.Sprintf("00000000-0000-4000-%s-000000000006", group)
	deleteStatus, deleted := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		fmt.Sprintf("00000000-0000-4000-%s-000000000007", group),
		[]map[string]any{issue49Mutation(client, table, deleteID, recordID, "delete", updateVersion, phase4ClientVersion, nil)},
	))
	if deleteStatus != http.StatusOK {
		t.Fatalf("canonical delete status = %d: %#v", deleteStatus, deleted)
	}
	deleteVersion := issue49RequireOpaqueVersion(t, issue49RequireAcceptedOutcome(t, deleted, deleteID, "applied"))
	state, err := harness.Operator().ObserveItemStateMatch(ctx, recordID, "updated", deleteVersion)
	if err != nil {
		t.Fatalf("observe canonical CRUD state: %v", err)
	}
	if state.Live || !state.ValueMatches || !state.VersionMatches || insertVersion == updateVersion || updateVersion == deleteVersion {
		t.Fatalf("canonical CRUD did not preserve its authoritative transitions: %#v", state)
	}
}

func issue49RemainingVersionFence(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-version")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	recordID := "00000000-0000-4000-8e04-000000000001"
	insertID := "00000000-0000-4000-8e04-000000000002"
	_, inserted := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8e04-000000000003",
		[]map[string]any{phase4InsertMutation(client, table, ownerField, insertID, recordID, "before")},
	))
	priorVersion := issue49RequireOpaqueVersion(t, issue49RequireAcceptedOutcome(t, inserted, insertID, "applied"))
	updateID := "00000000-0000-4000-8e04-000000000004"
	clientTime := "2099-12-31T23:59:59.999999Z"
	status, updated := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8e04-000000000005",
		[]map[string]any{issue49Mutation(client, table, updateID, recordID, "update", priorVersion, clientTime, map[string]any{
			table.ValueField: "after",
		})},
	))
	if status != http.StatusOK {
		t.Fatalf("version-fence update status = %d: %#v", status, updated)
	}
	newVersion := issue49RequireOpaqueVersion(t, issue49RequireAcceptedOutcome(t, updated, updateID, "applied"))
	state, err := harness.Operator().ObserveItemStateMatch(ctx, recordID, "after", newVersion)
	if err != nil {
		t.Fatalf("observe accepted version fence: %v", err)
	}
	if !state.Live || !state.ValueMatches || !state.VersionMatches || newVersion == priorVersion || newVersion == clientTime {
		t.Fatalf("accepted transition did not mint a server version: prior=%q new=%q state=%#v", priorVersion, newVersion, state)
	}
}

func issue49RemainingProgressIntegritySeparation(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-integrity-health")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8e05-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8e05-000000000002")
	manifest := loadMutationControlManifest(t, ctx, harness)
	table := requireRealTable(t, client, "cf_items")
	recordID := "00000000-0000-4000-8e05-000000000003"
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", recordID, "diagnostic-user", "integrity-health"); err != nil {
		t.Fatalf("insert integrity-health row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	response := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 100)
	changes, ok := mutationControlChanges(response)
	if !ok || len(changes) != 1 {
		t.Fatalf("integrity-health change set is invalid: %#v", response)
	}
	change, ok := mutationControlChange(changes, "user:diagnostic-user", table, recordID)
	if !ok {
		t.Fatal("integrity-health change is missing")
	}
	_, expectedScopeDigest, err := independentlyComputeMutationControlDigests(manifest, "user:diagnostic-user", table, change)
	if err != nil {
		t.Fatalf("compute independent integrity digest: %v", err)
	}
	checksums, ok := response["checksums"].(map[string]any)
	actualScopeDigest, digestOK := mutationControlChecksumDigest(checksums["user:diagnostic-user"])
	if !ok || !digestOK || actualScopeDigest != expectedScopeDigest {
		t.Fatalf("valid cursor response omitted independent integrity evidence: %#v", response)
	}
}

func issue49RemainingSeedContinuation(t *testing.T) {
	ctx, harness, _ := issue49RemainingHarness(t, 4*time.Minute)
	recordID := "00000000-0000-4000-8e06-000000000001"
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_global_items (id, value) VALUES ($1, $2)", recordID, "seed-continuation"); err != nil {
		t.Fatalf("insert seed-continuation row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", recordID)
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open seed-continuation database: %v", err)
	}
	defer database.Close()
	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire seed-continuation connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"); err != nil {
		t.Fatalf("begin seed-continuation transaction: %v", err)
	}
	defer connection.ExecContext(context.Background(), "ROLLBACK")
	manifest := issue49QueryJSONObject(t, ctx, connection, "SELECT synchro.synchro_portable_seed_manifest(1)")
	scope := issue49RemainingSeedScope(t, manifest)
	receipt, receiptOK := scope["continuation"].(string)
	pageToken, tokenOK := scope["page_token"].(string)
	_, checksumOK := mutationControlChecksumDigest(scope["checksum"])
	if !receiptOK || receipt == "" || !tokenOK || pageToken == "" || !checksumOK || scope["cardinality"] != float64(1) {
		t.Fatalf("portable seed omitted resumable metadata: %#v", scope)
	}
	page := issue49QueryJSONObject(
		t,
		ctx,
		connection,
		"SELECT synchro.synchro_portable_seed_scope($1, $2, $3, $4, $5)",
		"cf:global",
		pageToken,
		receipt,
		int64(0),
		1,
	)
	records, ok := page["records"].([]any)
	if !ok || len(records) != 1 || page["has_more"] != false {
		t.Fatalf("portable seed continuation page is invalid: %#v", page)
	}
}

func issue49RemainingExplicitFailure(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 3*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-explicit-failure")
	scopes := issue49CloneObject(t, client.Scopes)
	scope := scopes["user:diagnostic-user"].(map[string]any)
	cursor, _ := scope["cursor"].(string)
	if cursor == "" {
		_, cursor = rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8e07-000000000001")
	}
	scopes["user:diagnostic-user"] = map[string]any{"cursor": issue49CorruptToken(cursor)}
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, scopes, 100))
	requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_request")
	errorBody := response["error"].(map[string]any)
	if errorBody["retryable"] != false {
		t.Fatalf("non-retryable cursor failure was not explicit: %#v", response)
	}
}

func issue49RemainingVocabulary(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 3*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-vocabulary")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	mutation := phase4InsertMutation(
		client,
		table,
		ownerField,
		"00000000-0000-4000-8e08-000000000001",
		"00000000-0000-4000-8e08-000000000002",
		"forbidden-upsert",
	)
	mutation["op"] = "upsert"
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8e08-000000000003",
		[]map[string]any{mutation},
	))
	requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_request")
}

func issue49RemainingLedgerRetention(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-ledger")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	recordID := "00000000-0000-4000-8e09-000000000001"
	mutationID := "00000000-0000-4000-8e09-000000000002"
	batchID := "00000000-0000-4000-8e09-000000000003"
	payload := phase4PushPayload(client, batchID, []map[string]any{
		phase4InsertMutation(client, table, ownerField, mutationID, recordID, "ledger-retention"),
	})
	firstStatus, first := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", payload)
	if firstStatus != http.StatusOK {
		t.Fatalf("ledger-retention first push status = %d: %#v", firstStatus, first)
	}
	if _, err := harness.Operator().RunDiagnosticRetentionCompaction(ctx); err != nil {
		t.Fatalf("run ledger-retention compaction: %v", err)
	}
	replayStatus, replay := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", payload)
	if replayStatus != http.StatusOK || !reflect.DeepEqual(first, replay) {
		t.Fatalf("compaction removed a reusable idempotency ledger: first=%#v replay=%#v", first, replay)
	}
	observation, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{recordID})
	if err != nil {
		t.Fatalf("observe retained idempotency ledger: %v", err)
	}
	if observation.BatchCount != 1 || observation.MutationCount != 1 || observation.SourceRowCount != 1 {
		t.Fatalf("retained idempotency ledger state is invalid: %#v", observation)
	}
}

func issue49RemainingAtomicPush(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-atomicity")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	const mutationCount = 17
	const requestLimit = 1 << 20
	recordIDs := make([]string, 0, mutationCount)
	mutations := make([]map[string]any, 0, mutationCount)
	for index := 1; index <= mutationCount; index++ {
		recordID := fmt.Sprintf("00000000-0000-4000-8e10-%012x", index)
		recordIDs = append(recordIDs, recordID)
		mutations = append(mutations, phase4InsertMutation(
			client,
			table,
			ownerField,
			fmt.Sprintf("00000000-0000-4000-8e11-%012x", index),
			recordID,
			"",
		))
	}
	payload := phase4PushPayload(client, "00000000-0000-4000-8e10-000000000000", mutations)
	emptyBody, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode atomic push control: %v", err)
	}
	available := requestLimit - len(emptyBody) - 1
	if available <= mutationCount {
		t.Fatal("atomic push control has no bounded payload capacity")
	}
	for index, mutation := range mutations {
		length := available / mutationCount
		if index < available%mutationCount {
			length++
		}
		mutation["columns"].(map[string]any)[table.ValueField] = strings.Repeat("x", length)
	}
	body, err := json.Marshal(payload)
	if err != nil || len(body) != requestLimit-1 {
		t.Fatalf("atomic push control size = %d: %v", len(body), err)
	}
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", payload)
	requireRealProtocolError(t, status, response, http.StatusInternalServerError, "sync_integrity_failure")
	observation, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, recordIDs)
	if err != nil {
		t.Fatalf("observe failed atomic push: %v", err)
	}
	if observation.BatchCount != 0 || observation.MutationCount != 0 || observation.SourceRowCount != 0 || observation.AcceptedWriteEpoch != 1 {
		t.Fatalf("failed first push committed a durable subset: %#v", observation)
	}
}

func issue49RemainingCompareAndSwap(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	recordID := "00000000-0000-4000-8e12-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		"compare-and-swap-base",
	); err != nil {
		t.Fatalf("insert compare-and-swap row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	first := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-cas-first")
	second := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-cas-second")
	firstRecords, _ := rebuildRealScope(t, ctx, harness, token, first, "user:diagnostic-user", "00000000-0000-4000-8e12-000000000002")
	secondRecords, _ := rebuildRealScope(t, ctx, harness, token, second, "user:diagnostic-user", "00000000-0000-4000-8e12-000000000003")
	table := requireRealTable(t, first, "cf_items")
	baseVersion := requireRebuildRecordVersion(t, firstRecords, table, recordID, "compare-and-swap-base")
	if otherVersion := requireRebuildRecordVersion(t, secondRecords, table, recordID, "compare-and-swap-base"); otherVersion != baseVersion {
		t.Fatal("compare-and-swap clients received different base versions")
	}
	control, err := harness.Operator().HoldItemForConcurrentPush(ctx, recordID)
	if err != nil {
		t.Fatalf("hold compare-and-swap row: %v", err)
	}
	t.Cleanup(func() { _ = control.Release() })
	type result struct {
		status   int
		response map[string]any
		err      error
	}
	results := make(chan result, 2)
	start := func(client *realProtocolClient, suffix string) {
		go func() {
			status, response, requestErr := executeSyncRequest(
				ctx,
				harness.AdapterURL(),
				token,
				"/sync/push",
				phase4PushPayload(client, "00000000-0000-4000-8e12-0000000000"+suffix, []map[string]any{
					issue49Mutation(
						client,
						table,
						"00000000-0000-4000-8e13-0000000000"+suffix,
						recordID,
						"update",
						baseVersion,
						phase4ClientVersion,
						map[string]any{table.ValueField: "compare-and-swap-" + suffix},
					),
				}),
			)
			results <- result{status: status, response: response, err: requestErr}
		}()
	}
	start(first, "11")
	waitContext, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	err = control.WaitForBlockedPushes(waitContext, 1)
	waitCancel()
	if err != nil {
		_ = control.Release()
		t.Fatalf("observe first blocked compare-and-swap push: %v", err)
	}
	start(second, "22")
	waitContext, waitCancel = context.WithTimeout(ctx, 10*time.Second)
	err = control.WaitForBlockedPushes(waitContext, 2)
	waitCancel()
	if err != nil {
		_ = control.Release()
		t.Fatalf("observe second blocked compare-and-swap push: %v", err)
	}
	if err := control.Release(); err != nil {
		t.Fatalf("release compare-and-swap row: %v", err)
	}
	accepted := 0
	rejected := 0
	for range 2 {
		select {
		case outcome := <-results:
			if outcome.err != nil || outcome.status != http.StatusOK {
				t.Fatalf("compare-and-swap request failed: status=%d err=%v response=%#v", outcome.status, outcome.err, outcome.response)
			}
			accepted += len(requireOutcomeList(t, outcome.response, "accepted"))
			rejectedOutcomes := requireOutcomeList(t, outcome.response, "rejected")
			rejected += len(rejectedOutcomes)
			if len(rejectedOutcomes) == 1 && (rejectedOutcomes[0]["status"] != "conflict" || rejectedOutcomes[0]["code"] != "version_conflict") {
				t.Fatalf("compare-and-swap loser is not authoritative: %#v", rejectedOutcomes[0])
			}
		case <-ctx.Done():
			t.Fatal("compare-and-swap requests did not complete")
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Fatalf("compare-and-swap outcomes = %d accepted and %d rejected", accepted, rejected)
	}
}

func issue49RemainingClientTime(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-client-time")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	recordID := "00000000-0000-4000-8e14-000000000001"
	insertID := "00000000-0000-4000-8e14-000000000002"
	_, inserted := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8e14-000000000003",
		[]map[string]any{phase4InsertMutation(client, table, ownerField, insertID, recordID, "client-time-before")},
	))
	baseVersion := issue49RequireOpaqueVersion(t, issue49RequireAcceptedOutcome(t, inserted, insertID, "applied"))
	clientTime := "2099-12-31T23:59:59.999999Z"
	updateID := "00000000-0000-4000-8e14-000000000004"
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8e14-000000000005",
		[]map[string]any{issue49Mutation(client, table, updateID, recordID, "update", baseVersion, clientTime, map[string]any{
			table.ValueField: "client-time-after",
		})},
	))
	if status != http.StatusOK {
		t.Fatalf("client-time update status = %d: %#v", status, response)
	}
	version := issue49RequireOpaqueVersion(t, issue49RequireAcceptedOutcome(t, response, updateID, "applied"))
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open client-time database: %v", err)
	}
	defer database.Close()
	var updatedAt time.Time
	if err := database.QueryRowContext(ctx, "SELECT updated_at FROM public.cf_items WHERE id = $1::uuid", recordID).Scan(&updatedAt); err != nil {
		t.Fatalf("read client-time source timestamp: %v", err)
	}
	diagnostic, err := time.Parse(time.RFC3339Nano, clientTime)
	if err != nil {
		t.Fatalf("parse client-time control: %v", err)
	}
	if updatedAt.Equal(diagnostic) || version == clientTime || version == baseVersion {
		t.Fatalf("client time gained authority: updated_at=%s version=%q", updatedAt, version)
	}
}

func issue49RemainingSealedRetry(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-sealed-retry")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	payload := phase4PushPayload(client, "00000000-0000-4000-8e15-000000000001", []map[string]any{
		phase4InsertMutation(
			client,
			table,
			ownerField,
			"00000000-0000-4000-8e15-000000000002",
			"00000000-0000-4000-8e15-000000000003",
			"sealed-retry",
		),
	})
	sealed, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("seal retry request: %v", err)
	}
	var attempts [][]byte
	var committedStatus int
	var committedBody []byte
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, readErr := io.ReadAll(io.LimitReader(request.Body, (1<<20)+1))
		if readErr != nil || len(body) > 1<<20 {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		attempts = append(attempts, append([]byte(nil), body...))
		switch len(attempts) {
		case 2:
			writer.Header().Set("Content-Type", "application/json")
			writer.Header().Set("Retry-After", "5")
			writer.WriteHeader(http.StatusTooManyRequests)
			_, _ = writer.Write([]byte(`{"error":{"code":"retry_later","message":"retry later","retryable":true}}`))
			return
		case 3:
			writer.Header().Set("Content-Type", "application/json")
			writer.Header().Set("Retry-After", "5")
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte(`{"error":{"code":"temporary_unavailable","message":"unavailable","retryable":true}}`))
			return
		}
		forwarded, forwardErr := http.NewRequestWithContext(request.Context(), http.MethodPost, harness.AdapterURL()+"/sync/push", bytes.NewReader(body))
		if forwardErr != nil {
			http.Error(writer, "forward failed", http.StatusInternalServerError)
			return
		}
		forwarded.Header = request.Header.Clone()
		response, forwardErr := (&http.Client{Timeout: 30 * time.Second}).Do(forwarded)
		if forwardErr != nil {
			http.Error(writer, "forward failed", http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		responseBody, forwardErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		if forwardErr != nil {
			http.Error(writer, "forward failed", http.StatusBadGateway)
			return
		}
		if len(attempts) == 1 {
			committedStatus = response.StatusCode
			committedBody = append([]byte(nil), responseBody...)
			panic(http.ErrAbortHandler)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(response.StatusCode)
		_, _ = writer.Write(responseBody)
	}))
	defer proxy.Close()
	for expectedAttempt, expectedStatus := range []int{0, http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusOK} {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, proxy.URL+"/sync/push", bytes.NewReader(sealed))
		if err != nil {
			t.Fatalf("create sealed retry %d: %v", expectedAttempt+1, err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
		if expectedStatus == 0 {
			if err == nil {
				_ = response.Body.Close()
				t.Fatal("lost response was unexpectedly delivered")
			}
			continue
		}
		if err != nil {
			t.Fatalf("send sealed retry %d: %v", expectedAttempt+1, err)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		if response.StatusCode != expectedStatus {
			t.Fatalf("sealed retry %d status = %d, want %d", expectedAttempt+1, response.StatusCode, expectedStatus)
		}
	}
	if committedStatus != http.StatusOK || len(attempts) != 4 {
		t.Fatalf("sealed retries changed canonical content: attempts=%d status=%d", len(attempts), committedStatus)
	}
	for _, attempt := range attempts {
		if !bytes.Equal(attempt, sealed) {
			t.Fatal("sealed retry changed canonical request bytes")
		}
	}
	finalStatus, final := issue49RawSync(t, ctx, harness.AdapterURL(), token, "/sync/push", payload)
	if finalStatus.Status != committedStatus || !bytes.Equal(finalStatus.Body, committedBody) || final["batch_id"] != payload["batch_id"] {
		t.Fatalf("sealed retry did not replay its canonical response: status=%d response=%#v", finalStatus.Status, final)
	}
}

func issue49RemainingTypedDeduplication(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	if err := harness.Operator().ConfigureTypedKeyCollisionTables(ctx); err != nil {
		t.Fatalf("configure typed deduplication tables: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_, stringErr := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_string_keys")
		_, integerErr := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_int_keys")
		if stringErr == nil && integerErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_string_keys"); err != nil {
		t.Fatalf("typed string-key table was not published: %v", err)
	}
	if _, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_int_keys"); err != nil {
		t.Fatalf("typed integer-key table was not published: %v", err)
	}
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-typed-dedup")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8e16-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8e16-000000000002")
	stringTable := requireRealTable(t, client, "cf_string_keys")
	integerTable := requireRealTable(t, client, "cf_int_keys")
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_string_keys (id, owner_id, value) VALUES ($1, $2, $3)", "1", "diagnostic-user", "typed-string"); err != nil {
		t.Fatalf("insert typed string key: %v", err)
	}
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_int_keys (id, owner_id, value) VALUES ($1, $2, $3)", 1, "diagnostic-user", "typed-integer"); err != nil {
		t.Fatalf("insert typed integer key: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_string_keys", "1")
	waitForRealWALRecords(t, ctx, harness, "cf_int_keys", "1")
	response := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 2)
	changes := requireRealChanges(t, response)
	if len(changes) != 2 || response["has_more"] != false {
		t.Fatalf("typed deduplication collapsed logical identities: %#v", response)
	}
	requireRealPullChange(t, changes, "user:diagnostic-user", stringTable, "1", "typed-string")
	requireRealPullChange(t, changes, "user:diagnostic-user", integerTable, float64(1), "typed-integer")
}

func issue49RemainingCursorBindings(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-cursor-first")
	_, cursor := rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8e17-000000000001")
	other := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-cursor-second")
	otherScopes := issue49CloneObject(t, other.Scopes)
	otherScopes["user:diagnostic-user"] = map[string]any{"cursor": cursor}
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(other, otherScopes, 100))
	requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_request")
}

func issue49RemainingRebuildIsolation(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-rebuild-isolation")
	beforeCheckpoints := observeCheckpointMap(t, ctx, harness, client.ID)
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open rebuild-isolation database: %v", err)
	}
	defer database.Close()
	var beforeScopeSetVersion int64
	if err := database.QueryRowContext(ctx, "SELECT scope_set_version FROM synchro.sync_clients WHERE user_id = 'diagnostic-user' AND client_id = $1", client.ID).Scan(&beforeScopeSetVersion); err != nil {
		t.Fatalf("read pre-rebuild scope-set version: %v", err)
	}
	status, response := requestRealRebuildPage(
		t,
		ctx,
		harness,
		token,
		client,
		"user:diagnostic-user",
		"00000000-0000-4000-8e18-000000000001",
		nil,
		1,
	)
	if status != http.StatusOK {
		t.Fatalf("rebuild-isolation page status = %d: %#v", status, response)
	}
	var afterScopeSetVersion int64
	if err := database.QueryRowContext(ctx, "SELECT scope_set_version FROM synchro.sync_clients WHERE user_id = 'diagnostic-user' AND client_id = $1", client.ID).Scan(&afterScopeSetVersion); err != nil {
		t.Fatalf("read post-rebuild scope-set version: %v", err)
	}
	afterCheckpoints := observeCheckpointMap(t, ctx, harness, client.ID)
	if beforeScopeSetVersion != afterScopeSetVersion || !issue49CheckpointMapsEqual(beforeCheckpoints, afterCheckpoints) {
		t.Fatalf("rebuild changed incremental state: versions=%d/%d checkpoints=%#v/%#v", beforeScopeSetVersion, afterScopeSetVersion, beforeCheckpoints, afterCheckpoints)
	}
}

func issue49RemainingTypedRows(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 3*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-typed-row")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	recordID := "AAAAAAAA-0000-4000-8E19-000000000001"
	mutationID := "00000000-0000-4000-8e19-000000000002"
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8e19-000000000003",
		[]map[string]any{phase4InsertMutation(client, table, ownerField, mutationID, recordID, "alternate-primary-key")},
	))
	if status != http.StatusOK {
		t.Fatalf("typed-row rejection status = %d: %#v", status, response)
	}
	if accepted := requireOutcomeList(t, response, "accepted"); len(accepted) != 0 {
		t.Fatalf("alternate primary-key representation was applied: %#v", accepted)
	}
	rejected := requireOutcomeList(t, response, "rejected")
	if len(rejected) != 1 || rejected[0]["mutation_id"] != mutationID || rejected[0]["status"] != "rejected_terminal" || rejected[0]["code"] != "validation_failed" {
		t.Fatalf("alternate primary-key representation was not rejected canonically: %#v", response)
	}
	observation, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{strings.ToLower(recordID)})
	if err != nil {
		t.Fatalf("observe alternate primary-key rejection: %v", err)
	}
	if observation.SourceRowCount != 0 {
		t.Fatalf("alternate primary-key representation reached source apply: %#v", observation)
	}
}

func issue49RemainingRowDigest(t *testing.T) {
	issue49RemainingVerifyIndependentDigest(t, true)
}

func issue49RemainingScopeDigest(t *testing.T) {
	issue49RemainingVerifyIndependentDigest(t, false)
}

func issue49RemainingVerifyIndependentDigest(t *testing.T, rowDigest bool) {
	t.Helper()
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	clientID := "issue49-remaining-scope-digest"
	recordID := "00000000-0000-4000-8e20-000000000001"
	if rowDigest {
		clientID = "issue49-remaining-row-digest"
		recordID = "00000000-0000-4000-8e20-000000000002"
	}
	client := connectRealProtocolClient(t, ctx, harness, token, clientID)
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8e20-000000000003")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8e20-000000000004")
	manifest := loadMutationControlManifest(t, ctx, harness)
	table := requireRealTable(t, client, "cf_items")
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", recordID, "diagnostic-user", "independent-digest"); err != nil {
		t.Fatalf("insert independent digest row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	response := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 100)
	changes := requireRealChanges(t, response)
	change, ok := mutationControlChange(changes, "user:diagnostic-user", table, recordID)
	if !ok {
		t.Fatal("independent digest change is missing")
	}
	expectedRow, expectedScope, err := independentlyComputeMutationControlDigests(manifest, "user:diagnostic-user", table, change)
	if err != nil {
		t.Fatalf("compute independent digest: %v", err)
	}
	if rowDigest {
		actual, valid := mutationControlChecksumDigest(change["row_checksum"])
		if !valid || actual != expectedRow {
			t.Fatalf("row digest does not bind the complete canonical row: %#v", change["row_checksum"])
		}
		return
	}
	checksums, valid := response["checksums"].(map[string]any)
	actual, digestValid := mutationControlChecksumDigest(checksums["user:diagnostic-user"])
	if !valid || !digestValid || actual != expectedScope {
		t.Fatalf("scope digest does not bind its canonical stream: %#v", response["checksums"])
	}
}

func issue49RemainingTerminalChecksums(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-terminal-checksums")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8e21-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8e21-000000000002")
	for index := 1; index <= 2; index++ {
		recordID := fmt.Sprintf("00000000-0000-4000-8e21-%012x", index+10)
		if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", recordID, "diagnostic-user", fmt.Sprintf("terminal-%d", index)); err != nil {
			t.Fatalf("insert terminal-checksum row: %v", err)
		}
		waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	}
	first := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 1)
	if first["has_more"] != true {
		t.Fatalf("terminal-checksum control did not create a nonterminal page: %#v", first)
	}
	if _, present := first["checksums"]; present {
		t.Fatalf("nonterminal pull included checksums: %#v", first)
	}
	second := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 1)
	checksums, ok := second["checksums"].(map[string]any)
	if second["has_more"] != false || !ok || len(checksums) != 2 || checksums["cf:global"] == nil || checksums["user:diagnostic-user"] == nil {
		t.Fatalf("terminal pull omitted the complete active checksum map: %#v", second)
	}
}

func issue49RemainingOperationalRedaction(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 4*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-redaction-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	valueCanary := "issue49-remaining-protected-value-24f97a"
	recordCanary := "00000000-0000-4000-8e22-000000000001"
	mutationCanary := "00000000-0000-4000-8e22-000000000002"
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8e22-000000000003",
		[]map[string]any{phase4InsertMutation(client, table, ownerField, mutationCanary, recordCanary, valueCanary)},
	))
	if status != http.StatusOK {
		t.Fatalf("redaction control push status = %d: %#v", status, response)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordCanary)
	disclosed, err := harness.StopAdapterAndObserveLogDisclosure(ctx, []string{client.ID, valueCanary, recordCanary, mutationCanary, token})
	if err != nil {
		t.Fatalf("observe operational logs: %v", err)
	}
	if disclosed {
		t.Fatal("operational logs disclosed protected data")
	}
}

func issue49RemainingSeedTransaction(t *testing.T) {
	ctx, harness, _ := issue49RemainingHarness(t, 3*time.Minute)
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open seed-transaction database: %v", err)
	}
	defer database.Close()
	var raw []byte
	if err := database.QueryRowContext(ctx, "SELECT synchro.synchro_portable_seed_manifest(1)").Scan(&raw); err == nil {
		t.Fatal("portable seed export succeeded outside its enforced transaction")
	}
}

func issue49RemainingSeedTokenBinding(t *testing.T) {
	ctx, harness, _ := issue49RemainingHarness(t, 4*time.Minute)
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open seed-token database: %v", err)
	}
	defer database.Close()
	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire seed-token connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"); err != nil {
		t.Fatalf("begin seed-token transaction: %v", err)
	}
	defer connection.ExecContext(context.Background(), "ROLLBACK")
	manifest := issue49QueryJSONObject(t, ctx, connection, "SELECT synchro.synchro_portable_seed_manifest(1)")
	scope := issue49RemainingSeedScope(t, manifest)
	response := issue49QueryJSONObject(
		t,
		ctx,
		connection,
		"SELECT synchro.synchro_portable_seed_scope($1, $2, $3, $4, $5)",
		"cf:global",
		scope["page_token"],
		scope["continuation"],
		int64(1),
		1,
	)
	issue49RequireJSONProtocolError(t, response, "invalid_request")
}

func issue49RemainingSeedScope(t *testing.T, manifest map[string]any) map[string]any {
	t.Helper()
	scopes, ok := manifest["portable_scopes"].([]any)
	if !ok || len(scopes) != 1 {
		t.Fatalf("portable seed scope set is invalid: %#v", manifest)
	}
	scope, ok := scopes[0].(map[string]any)
	if !ok || scope["id"] != "cf:global" {
		t.Fatalf("portable seed scope is invalid: %#v", scopes[0])
	}
	return scope
}

func issue49RemainingSeedArtifactVerification(t *testing.T) {
	ctx, harness, _ := issue49RemainingHarness(t, 4*time.Minute)
	recordID := "00000000-0000-4000-8e23-000000000001"
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_global_items (id, value) VALUES ($1, $2)", recordID, "seed-artifact-verification"); err != nil {
		t.Fatalf("insert seed-artifact control row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", recordID)
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open seed-artifact database: %v", err)
	}
	defer database.Close()
	table := loadRealSchemaTableReference(t, ctx, harness, "cf_global_items")
	valueField := requireRealSchemaField(t, table, "value")
	if _, err := database.ExecContext(ctx, fmt.Sprintf(`
		ALTER FUNCTION synchro.synchro_portable_seed_scope(text, text, text, bigint, integer)
		RENAME TO synchro_portable_seed_scope_verified;
		CREATE FUNCTION synchro.synchro_portable_seed_scope(
			p_scope_id text,
			p_page_token text,
			p_continuation_receipt text,
			p_expected_row_ordinal bigint,
			p_limit integer
		) RETURNS jsonb
		LANGUAGE plpgsql
		AS $function$
		DECLARE
			response jsonb;
		BEGIN
			response := synchro.synchro_portable_seed_scope_verified(
				p_scope_id,
				p_page_token,
				p_continuation_receipt,
				p_expected_row_ordinal,
				p_limit
			);
			IF jsonb_typeof(response->'records') = 'array'
				AND jsonb_array_length(response->'records') > 0 THEN
				response := jsonb_set(
					response,
					ARRAY['records', '0', 'row', '%s'],
					to_jsonb('tampered-after-export'::text),
					false
				);
			END IF;
			RETURN response;
		END
		$function$`, valueField)); err != nil {
		t.Fatalf("install seed-artifact tamper boundary: %v", err)
	}
	seedTool := os.Getenv("SYNCHRO_CONFORMANCE_SEED_ARTIFACT")
	if seedTool == "" {
		t.Fatal("production seed artifact is unavailable")
	}
	output := filepath.Join(t.TempDir(), "issue49-remaining-seed.sqlite")
	command := exec.CommandContext(ctx, seedTool, "--database-url", harness.DatabaseURL(), "--output", output)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	commandErr := command.Run()
	_, statErr := os.Lstat(output)
	if commandErr == nil || !os.IsNotExist(statErr) {
		t.Fatalf("failed seed verification published an artifact: command_error=%t artifact_error=%v", commandErr != nil, statErr)
	}
}

func issue49RemainingRetentionFloor(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 5*time.Minute)
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open retention-floor database: %v", err)
	}
	defer database.Close()
	// A portable seed supplies the initial cursor without a live rebuild pin.
	// The pin would prevent compaction at the tested boundary.
	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire retention seed connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"); err != nil {
		t.Fatalf("begin retention seed transaction: %v", err)
	}
	defer connection.ExecContext(context.Background(), "ROLLBACK")
	manifest := issue49QueryJSONObject(t, ctx, connection, "SELECT synchro.synchro_portable_seed_manifest(1)")
	seedScope := issue49RemainingSeedScope(t, manifest)
	if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit retention seed transaction: %v", err)
	}
	const clientID = "issue49-remaining-retention-floor"
	status, connected := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
		"client_id": clientID, "platform": "conformance", "app_version": "0.3.0", "protocol_version": 3,
		"schema": map[string]any{"version": 0, "hash": ""}, "scope_set_version": 0, "known_scopes": map[string]any{},
		"seed_receipts": map[string]any{"cf:global": seedScope["continuation"]},
	})
	if status != http.StatusOK {
		t.Fatalf("connect seeded retention client: status=%d", status)
	}
	client := parseRealProtocolClient(t, connected, clientID)
	oldScopeCursor := client.Scopes["cf:global"].(map[string]any)["cursor"].(string)
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8e24-000000000001")
	recordID := "00000000-0000-4000-8e24-000000000003"
	secondRecordID := "00000000-0000-4000-8e24-000000000004"
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_global_items (id, value) VALUES ($1, $2), ($3, $2)", recordID, "retention-floor", secondRecordID); err != nil {
		t.Fatalf("insert retention-floor row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", recordID, secondRecordID)
	first := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 1)
	if len(requireRealChanges(t, first)) != 1 || first["has_more"] != true {
		t.Fatalf("retention-floor control did not create an effect-boundary page: %#v", first)
	}
	boundaryCursor := client.Scopes["cf:global"].(map[string]any)["cursor"].(string)
	second := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 1)
	if len(requireRealChanges(t, second)) != 1 || second["has_more"] != false {
		t.Fatalf("retention-floor control did not finish the remaining page: %#v", second)
	}
	partialCompaction, err := harness.Operator().RunDiagnosticRetentionCompaction(ctx)
	if err != nil || partialCompaction.DeletedEntries != 1 || partialCompaction.DeactivatedClients != 0 {
		t.Fatalf("compact acknowledged effect boundary: result=%#v error=%v", partialCompaction, err)
	}
	boundary := issue49DecodeOpaqueToken(t, boundaryCursor, "ic1")
	boundaryPosition, err := json.Marshal(boundary["position"])
	if err != nil {
		t.Fatalf("encode retention boundary position: %v", err)
	}
	var boundaryEqualsFloor bool
	if err := database.QueryRowContext(ctx, `
		SELECT floor_position_kind = 'effect'
		   AND floor_position_kind = $1::jsonb->>'position_kind'
		   AND floor_commit_lsn = ($1::jsonb->>'commit_lsn')::pg_lsn
		   AND floor_event_ordinal = ($1::jsonb->>'event_ordinal')::bigint
		   AND floor_effect_ordinal = ($1::jsonb->>'effect_ordinal')::integer
		FROM synchro.sync_scope_state WHERE scope_id = 'cf:global'`, boundaryPosition).Scan(&boundaryEqualsFloor); err != nil {
		t.Fatalf("compare retained floor with the issued effect cursor: %v", err)
	}
	if !boundaryEqualsFloor {
		t.Fatal("retention boundary control did not produce a cursor exactly at the floor")
	}
	client.Scopes["cf:global"] = map[string]any{"cursor": boundaryCursor}
	atFloor := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 100)
	if !reflect.DeepEqual(requireRealChanges(t, atFloor), requireRealChanges(t, second)) || atFloor["has_more"] != false {
		t.Fatalf("floor-equal cursor did not replay the remaining effect: %#v", atFloor)
	}
	acknowledgedScopeCursor := client.Scopes["cf:global"].(map[string]any)["cursor"].(string)
	acknowledgeRealClientCursors(t, ctx, harness, token, client)
	if err := harness.Operator().ExpireRetentionClient(ctx, "diagnostic-user", client.ID); err != nil {
		t.Fatalf("expire retention-floor client: %v", err)
	}
	compaction, err := harness.Operator().RunDiagnosticRetentionCompaction(ctx)
	if err != nil {
		t.Fatalf("compact retention-floor effect: %v", err)
	}
	if compaction.DeletedEntries < 1 {
		t.Fatalf("retention-floor compaction deleted no effects: %#v", compaction)
	}
	type floorState struct {
		StreamGeneration     string
		MembershipGeneration int64
		RetentionGeneration  int64
		PositionKind         string
		CommitLSN            string
		EventOrdinal         int64
		EffectOrdinal        int32
	}
	var remainingEffects int64
	if err := database.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_changelog WHERE bucket_id = 'cf:global'").Scan(&remainingEffects); err != nil {
		t.Fatalf("read retention-floor effect count: %v", err)
	}
	if remainingEffects != 0 {
		t.Fatalf("retention-floor compaction left %d scope effects", remainingEffects)
	}
	readFloor := func() floorState {
		t.Helper()
		var state floorState
		if err := database.QueryRowContext(ctx, `
			SELECT stream_generation, membership_generation, retention_generation,
			       floor_position_kind, floor_commit_lsn::text,
			       floor_event_ordinal, floor_effect_ordinal
			FROM synchro.sync_scope_state
			WHERE scope_id = 'cf:global'`).Scan(
			&state.StreamGeneration,
			&state.MembershipGeneration,
			&state.RetentionGeneration,
			&state.PositionKind,
			&state.CommitLSN,
			&state.EventOrdinal,
			&state.EffectOrdinal,
		); err != nil {
			t.Fatalf("read retention-floor lineage: %v", err)
		}
		if state.StreamGeneration == "" || state.MembershipGeneration <= 0 || state.RetentionGeneration <= 0 || state.PositionKind != "effect" || state.CommitLSN == "" || state.EventOrdinal < 0 || state.EffectOrdinal < 0 {
			t.Fatalf("retention-floor lineage is incomplete: %#v", state)
		}
		return state
	}
	retainedFloor := readFloor()
	emptyCompaction, err := harness.Operator().RunDiagnosticRetentionCompaction(ctx)
	if err != nil {
		t.Fatalf("compact empty retention-floor effect log: %v", err)
	}
	if emptyCompaction.DeletedEntries != 0 {
		t.Fatalf("empty retention-floor compaction deleted effects: %#v", emptyCompaction)
	}
	if afterEmptyCompaction := readFloor(); afterEmptyCompaction != retainedFloor {
		t.Fatalf("empty effect log changed the durable retention floor: before=%#v after=%#v", retainedFloor, afterEmptyCompaction)
	}
	compareCursorToFloor := func(cursor string) int {
		t.Helper()
		decoded := issue49DecodeOpaqueToken(t, cursor, "ic1")
		if decoded["stream_generation"] != retainedFloor.StreamGeneration {
			t.Fatalf("retention cursor has the wrong stream generation: %#v", decoded)
		}
		position, ok := decoded["position"].(map[string]any)
		if !ok {
			t.Fatalf("retention cursor position is invalid: %#v", decoded)
		}
		kind, ok := position["position_kind"].(string)
		if !ok {
			t.Fatalf("retention cursor position kind is invalid: %#v", position)
		}
		var commitLSN any
		var eventOrdinal, effectOrdinal int64
		switch kind {
		case "generation_start":
		case "transaction_end":
			var valid bool
			commitLSN, valid = position["commit_lsn"].(string)
			if !valid || commitLSN == "" {
				t.Fatalf("transaction-end cursor position is invalid: %#v", position)
			}
		case "effect":
			var commitValid, eventValid, effectValid bool
			commitLSN, commitValid = position["commit_lsn"].(string)
			rawEvent, eventValid := position["event_ordinal"].(float64)
			rawEffect, effectValid := position["effect_ordinal"].(float64)
			eventOrdinal = int64(rawEvent)
			effectOrdinal = int64(rawEffect)
			if !commitValid || commitLSN == "" || !eventValid || !effectValid ||
				rawEvent < 0 || rawEvent != float64(eventOrdinal) ||
				rawEffect < 0 || rawEffect != float64(effectOrdinal) {
				t.Fatalf("effect cursor position is invalid: %#v", position)
			}
		default:
			t.Fatalf("retention cursor position kind is unsupported: %#v", position)
		}
		var comparison int
		if err := database.QueryRowContext(ctx, `
			SELECT CASE
				WHEN $1 = 'generation_start' THEN -1
				WHEN $2::pg_lsn < $5::pg_lsn THEN -1
				WHEN $2::pg_lsn > $5::pg_lsn THEN 1
				WHEN $1 = 'transaction_end' THEN 1
				WHEN ($3::bigint, $4::bigint) < ($6::bigint, $7::bigint) THEN -1
				WHEN ($3::bigint, $4::bigint) = ($6::bigint, $7::bigint) THEN 0
				ELSE 1
			END`,
			kind,
			commitLSN,
			eventOrdinal,
			effectOrdinal,
			retainedFloor.CommitLSN,
			retainedFloor.EventOrdinal,
			retainedFloor.EffectOrdinal,
		).Scan(&comparison); err != nil {
			t.Fatalf("compare production cursor with retention floor: %v", err)
		}
		return comparison
	}
	if oldComparison := compareCursorToFloor(oldScopeCursor); oldComparison >= 0 {
		t.Fatalf("pre-effect cursor is not below the retained floor: comparison=%d floor=%#v", oldComparison, retainedFloor)
	}
	if currentComparison := compareCursorToFloor(acknowledgedScopeCursor); currentComparison < 0 {
		t.Fatalf("acknowledged cursor is below the retained floor: comparison=%d floor=%#v", currentComparison, retainedFloor)
	}
	reconnectStatus, reconnected := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            client.Schema,
		"scope_set_version": client.ScopeSetVersion,
		"known_scopes":      client.Scopes,
	})
	reconnectedGeneration, generationOK := reconnected["client_generation"].(float64)
	if reconnectStatus != http.StatusOK || !generationOK || int64(reconnectedGeneration) != client.Generation+1 {
		t.Fatalf("retention-floor reconnect did not renew the client: status=%d response=%#v", reconnectStatus, reconnected)
	}
	cursorUpdates, updatesOK := reconnected["scope_cursor_updates"].(map[string]any)
	if !updatesOK || len(cursorUpdates) != len(client.Scopes) {
		t.Fatalf("retention-floor renewal cursor updates are incomplete: %#v", reconnected)
	}
	for scopeID := range client.Scopes {
		if update, present := cursorUpdates[scopeID]; !present || update != nil {
			t.Fatalf("retention-floor renewal did not clear scope %q: %#v", scopeID, cursorUpdates)
		}
	}
	client.Generation = int64(reconnectedGeneration)
	if afterRenewal := readFloor(); afterRenewal != retainedFloor {
		t.Fatalf("client renewal changed the durable retention floor: before=%#v after=%#v", retainedFloor, afterRenewal)
	}
}

func issue49RemainingPortableInteger(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 3*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-portable-integer")
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open portable-integer database: %v", err)
	}
	defer database.Close()
	const maximumSafeInteger int64 = 9_007_199_254_740_991
	if _, err := database.ExecContext(ctx, `
		UPDATE synchro.sync_clients
		SET scope_set_version = $1
		WHERE user_id = 'diagnostic-user' AND client_id = $2`, maximumSafeInteger, client.ID); err != nil {
		t.Fatalf("stage maximum portable integer: %v", err)
	}
	status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            client.Schema,
		"scope_set_version": maximumSafeInteger,
		"known_scopes":      client.Scopes,
	})
	version, ok := response["scope_set_version"].(float64)
	if status != http.StatusOK || !ok || int64(version) != maximumSafeInteger {
		t.Fatalf("maximum portable integer did not round trip: status=%d response=%#v", status, response)
	}
	overflowErr := func() error {
		_, err := database.ExecContext(ctx, "SELECT synchro.synchro_register_shared_scope('cf:issue49-remaining-overflow', false)")
		return err
	}()
	var retained int64
	if err := database.QueryRowContext(ctx, "SELECT scope_set_version FROM synchro.sync_clients WHERE user_id = 'diagnostic-user' AND client_id = $1", client.ID).Scan(&retained); err != nil {
		t.Fatalf("observe portable counter overflow: %v", err)
	}
	if overflowErr == nil || retained != maximumSafeInteger {
		t.Fatalf("portable counter allocated outside the safe range: err=%v retained=%d", overflowErr, retained)
	}
}

func issue49RemainingOutcomeSchema(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 5*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-outcome-schema")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	oldSchema := issue49CloneObject(t, client.Schema)
	recordID := "00000000-0000-4000-8e25-000000000001"
	mutationID := "00000000-0000-4000-8e25-000000000002"
	mutation := phase4InsertMutation(client, table, ownerField, mutationID, recordID, "historical-outcome")
	firstStatus, first := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8e25-000000000003",
		[]map[string]any{mutation},
	))
	if firstStatus != http.StatusOK {
		t.Fatalf("historical outcome first status = %d: %#v", firstStatus, first)
	}
	firstOutcome := issue49RequireAcceptedOutcome(t, first, mutationID, "applied")
	if !sameRealSchemaReference(firstOutcome["outcome_schema"], oldSchema) {
		t.Fatalf("first outcome has the wrong schema binding: %#v", firstOutcome)
	}
	_, currentTable := transitionRealSchemaQueue(t, ctx, harness)
	client.Schema = currentTable.Schema
	replayStatus, replay := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8e25-000000000004",
		[]map[string]any{mutation},
	))
	if replayStatus != http.StatusOK {
		t.Fatalf("historical outcome replay status = %d: %#v", replayStatus, replay)
	}
	replayedOutcome := issue49RequireAcceptedOutcome(t, replay, mutationID, "applied")
	if !sameRealSchemaReference(replayedOutcome["outcome_schema"], oldSchema) || !reflect.DeepEqual(firstOutcome, replayedOutcome) {
		t.Fatalf("historical outcome lost its immutable schema binding: first=%#v replay=%#v", firstOutcome, replayedOutcome)
	}
}

func issue49RemainingEffectProgress(t *testing.T) {
	ctx, harness, _ := issue49RemainingHarness(t, 5*time.Minute)
	token, err := harness.NativeBearerToken(ctx, "issue49-owner-after", time.Now())
	if err != nil {
		t.Fatalf("sign effect-progress owner token: %v", err)
	}
	const targetScope = "user:issue49-owner-after"
	documentID := "00000000-0000-4000-8e26-000000000001"
	memberIDs := []string{
		"00000000-0000-4000-8e26-000000000002",
		"00000000-0000-4000-8e26-000000000003",
	}
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_documents (id, owner_id, title) VALUES ($1, $2, $3)", documentID, "issue49-owner-before", "effect progress"); err != nil {
		t.Fatalf("insert effect-progress document: %v", err)
	}
	for index, memberID := range memberIDs {
		if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_document_members (id, document_id, member_id) VALUES ($1, $2, $3)", memberID, documentID, fmt.Sprintf("issue49-member-%d", index+1)); err != nil {
			t.Fatalf("insert effect-progress member: %v", err)
		}
	}
	waitForMembershipBuckets(t, ctx, harness, memberIDs[0], []string{"user:issue49-member-1", "user:issue49-owner-before"})
	waitForMembershipBuckets(t, ctx, harness, memberIDs[1], []string{"user:issue49-member-2", "user:issue49-owner-before"})
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-effect-progress", "cf:global", targetScope)
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8e26-000000000011")
	rebuildRealScope(t, ctx, harness, token, client, targetScope, "00000000-0000-4000-8e26-000000000013")
	if err := harness.Source().ExecContext(ctx, "UPDATE cf_documents SET owner_id = $2, updated_at = clock_timestamp() WHERE id = $1", documentID, "issue49-owner-after"); err != nil {
		t.Fatalf("update effect-progress document: %v", err)
	}
	waitForMembershipBuckets(t, ctx, harness, memberIDs[0], []string{"user:issue49-member-1", targetScope})
	waitForMembershipBuckets(t, ctx, harness, memberIDs[1], []string{"user:issue49-member-2", targetScope})
	deadline := time.Now().Add(20 * time.Second)
	var observedEffects []blackbox.MembershipEffectObservation
	for {
		effects, observeErr := harness.Operator().ObserveDependencyEffects(ctx, documentID, []string{documentID, memberIDs[0], memberIDs[1]})
		count := 0
		for _, effect := range effects {
			if effect.BucketID == targetScope {
				count++
			}
		}
		if observeErr == nil && count == 3 {
			observedEffects = effects
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("same-event effects did not materialize: count=%d err=%v", count, observeErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	var changes []map[string]any
	var positions []map[string]any
	for page := 0; page < 4; page++ {
		response := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 1)
		changes = append(changes, requireRealChanges(t, response)...)
		if cursorUpdates, ok := response["scope_cursors"].(map[string]any); ok {
			if cursor, ok := cursorUpdates[targetScope].(string); ok {
				position, ok := issue49DecodeOpaqueToken(t, cursor, "ic1")["position"].(map[string]any)
				if ok {
					positions = append(positions, position)
				}
			}
		}
		if response["has_more"] == false {
			break
		}
	}
	seen := make(map[string]bool)
	for _, change := range changes {
		if change["scope"] != targetScope {
			t.Fatalf("effect-progress page returned an unrelated change: %#v", change)
		}
		pk := change["pk"].(map[string]any)
		for _, value := range pk {
			if id, ok := value.(string); ok {
				seen[id] = true
			}
		}
	}
	effectOrdinals := make([]int32, 0, 3)
	var eventOrdinal int64 = -1
	for _, effect := range observedEffects {
		if effect.BucketID != targetScope {
			continue
		}
		if eventOrdinal < 0 {
			eventOrdinal = effect.EventOrdinal
		}
		if effect.EventOrdinal != eventOrdinal {
			t.Fatalf("same-event effects changed event identity: %#v", observedEffects)
		}
		effectOrdinals = append(effectOrdinals, effect.EffectOrdinal)
	}
	if len(positions) != 3 || positions[0]["position_kind"] != "effect" || positions[1]["position_kind"] != "effect" || positions[2]["position_kind"] != "transaction_end" ||
		positions[0]["commit_lsn"] != positions[1]["commit_lsn"] || positions[1]["commit_lsn"] != positions[2]["commit_lsn"] ||
		positions[0]["event_ordinal"] != float64(eventOrdinal) || positions[1]["event_ordinal"] != float64(eventOrdinal) ||
		positions[0]["effect_ordinal"] != float64(0) || positions[1]["effect_ordinal"] != float64(1) {
		t.Fatalf("same-event cursor progression is invalid: %#v", positions)
	}
	if len(changes) != 3 || !seen[documentID] || !seen[memberIDs[0]] || !seen[memberIDs[1]] || !slices.Equal(effectOrdinals, []int32{0, 1, 2}) {
		t.Fatalf("same-event sibling progress is invalid: changes=%#v positions=%#v", changes, positions)
	}
}

func issue49RemainingProjectionBootstrap(t *testing.T) {
	ctx, harness, _ := issue49RemainingHarness(t, 5*time.Minute)
	historicalID := "00000000-0000-4000-8e27-000000000001"
	catchupID := "00000000-0000-4000-8e27-000000000002"
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_late_registration (id, owner_id, value) VALUES ($1, $2, $3)", historicalID, "diagnostic-user", "bootstrap-historical"); err != nil {
		t.Fatalf("insert projection-bootstrap history: %v", err)
	}
	if err := harness.Source().ExecContext(ctx, `
		INSERT INTO cf_late_registration (id, owner_id, value)
		SELECT ('20000000-0000-4000-8000-' || lpad(value::text, 12, '0'))::uuid,
		       'diagnostic-user', 'bootstrap-filler-' || value::text
		FROM generate_series(1, 1024) value`); err != nil {
		t.Fatalf("insert projection-bootstrap filler: %v", err)
	}
	if err := harness.Operator().RegisterLateSourceTable(ctx); err != nil {
		t.Fatalf("register projection-bootstrap source: %v", err)
	}
	generation, err := harness.Operator().PendingLateSourceRegistryGeneration(ctx)
	if err != nil {
		t.Fatalf("load projection-bootstrap generation: %v", err)
	}
	barrier, err := harness.Operator().NewProjectionBootstrapBarrier()
	if err != nil {
		t.Fatalf("create projection-bootstrap barrier: %v", err)
	}
	t.Cleanup(func() { _ = barrier.Close() })
	type outcome struct {
		result blackbox.ProjectionBootstrapResult
		err    error
	}
	completed := make(chan outcome, 1)
	go func() {
		result, runErr := harness.Operator().RunProjectionBootstrap(ctx, generation)
		completed <- outcome{result: result, err: runErr}
	}()
	deadline := time.Now().Add(20 * time.Second)
	candidateObserved := false
	for time.Now().Before(deadline) {
		select {
		case finished := <-completed:
			t.Fatalf("projection bootstrap ended before candidate catch-up: %v", finished.err)
		default:
		}
		_, present, observeErr := harness.Operator().ObservePreparingReset(ctx)
		if observeErr != nil {
			t.Fatalf("observe projection-bootstrap candidate: %v", observeErr)
		}
		if present {
			candidateObserved = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !candidateObserved {
		t.Fatal("projection bootstrap did not create a permanent candidate slot")
	}
	if err := barrier.AcquireBarrier(ctx); err != nil {
		t.Fatalf("queue projection-bootstrap barrier: %v", err)
	}
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_late_registration (id, owner_id, value) VALUES ($1, $2, $3)", catchupID, "diagnostic-user", "bootstrap-catchup"); err != nil {
		t.Fatalf("insert projection-bootstrap catch-up row: %v", err)
	}
	if err := barrier.ReleaseBarrier(); err != nil {
		t.Fatalf("release projection-bootstrap barrier: %v", err)
	}
	var finished outcome
	select {
	case finished = <-completed:
	case <-ctx.Done():
		t.Fatalf("projection bootstrap did not complete: %s", harness.FailureDiagnostics())
	}
	if finished.err != nil {
		t.Fatalf("run projection bootstrap: %v", finished.err)
	}
	result := finished.result
	observation, err := harness.Operator().ObserveProjectionBootstrap(ctx, result.BootstrapID, historicalID, catchupID)
	if err != nil {
		t.Fatalf("observe projection bootstrap: %v", err)
	}
	if result.RegistryGeneration != generation || result.SourceStreamGeneration == "" || result.ActiveSlotName == "" ||
		result.CandidateSlotName == result.ActiveSlotName || result.ActivationBarrier == "" || observation.Lifecycle != "cleanup_complete" ||
		!observation.StreamUnchanged || !observation.ActiveSlotUnchanged || !observation.CandidateSlotAbsent ||
		!observation.RegistryActive || !observation.ManifestPublished || !observation.HistoricalRecordPresent ||
		!observation.CatchupRecordPresent || !observation.HistoricalMembershipPresent || !observation.CatchupMembershipPresent ||
		observation.CatchupFenceCoverage != "projection_bootstrap" || !observation.CatchupFenceProvenanceMatches ||
		!observation.NoPendingFences || !observation.StageCleared {
		t.Fatalf("projection bootstrap was not complete and causally bound: result=%#v observation=%#v", result, observation)
	}
}

func issue49RemainingSchemaCursorContinuity(t *testing.T) {
	ctx, harness, token := issue49RemainingHarness(t, 5*time.Minute)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-remaining-schema-cursor")
	_, oldUserCursor := rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8e28-000000000001")
	_, oldGlobalCursor := rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8e28-000000000002")
	oldSchema := issue49CloneObject(t, client.Schema)
	knownScopes := issue49CloneObject(t, client.Scopes)
	if err := harness.Operator().TransitionSyncedTableField(ctx, "cf_schema_queue", "", "compatible_value", "", ""); err != nil {
		t.Fatalf("commit compatible schema transition: %v", err)
	}
	var currentTable realSchemaTableReference
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		current, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_schema_queue")
		if err == nil && !sameRealSchemaReference(current.Schema, oldSchema) {
			currentTable = current
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if currentTable.TableID == "" {
		t.Fatalf("compatible schema transition did not activate: %s", harness.FailureDiagnostics())
	}
	status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            oldSchema,
		"scope_set_version": client.ScopeSetVersion,
		"known_scopes":      knownScopes,
	})
	if status != http.StatusOK {
		t.Fatalf("schema-cursor connect status = %d: %#v", status, response)
	}
	updates, ok := response["scope_cursor_updates"].(map[string]any)
	newGlobalCursor, globalOK := updates["cf:global"].(string)
	newUserCursor, userOK := updates["user:diagnostic-user"].(string)
	schema, schemaOK := response["schema"].(map[string]any)
	if !ok || !globalOK || newGlobalCursor == "" || !userOK || newUserCursor == "" || !schemaOK || schema["action"] != "replace" {
		t.Fatalf("schema-cursor replacements are invalid: %#v", response)
	}
	oldGlobal := issue49DecodeOpaqueToken(t, oldGlobalCursor, "ic1")
	newGlobal := issue49DecodeOpaqueToken(t, newGlobalCursor, "ic1")
	oldUser := issue49DecodeOpaqueToken(t, oldUserCursor, "ic1")
	newUser := issue49DecodeOpaqueToken(t, newUserCursor, "ic1")
	if !reflect.DeepEqual(oldGlobal["position"], newGlobal["position"]) || oldGlobal["schema_hash"] != oldSchema["hash"] ||
		newGlobal["schema_hash"] != currentTable.Schema["hash"] || newGlobalCursor == oldGlobalCursor ||
		!reflect.DeepEqual(oldUser["position"], newUser["position"]) || oldUser["schema_hash"] != oldSchema["hash"] ||
		newUser["schema_hash"] != currentTable.Schema["hash"] || newUserCursor == oldUserCursor {
		t.Fatalf("schema-cursor continuity changed position or reused an old token: global=%#v/%#v user=%#v/%#v", oldGlobal, newGlobal, oldUser, newUser)
	}
}
