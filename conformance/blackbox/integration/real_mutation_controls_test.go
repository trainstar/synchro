package integration

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/vectors"
)

func TestRealMutationControlCursorAdvancement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "mutation-cursor-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8c01-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8c01-000000000002")

	recordID := "00000000-0000-4000-8c01-000000000003"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		"mutation-cursor-advancement",
	); err != nil {
		t.Fatalf("insert cursor control source row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	response := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 100)
	acknowledgeRealClientCursors(t, ctx, harness, token, client)
	checkpoints := observeCheckpointMap(t, ctx, harness, client.ID)
	t.Run("assertion", func(t *testing.T) {
		if response["has_more"] != false {
			t.Fatal("predicate cursor_terminal_page failed")
		}
		changes, changesValid := mutationControlChanges(response)
		if !changesValid || len(changes) != 1 {
			t.Fatalf("predicate cursor_change_count failed: valid=%t count=%d", changesValid, len(changes))
		}
		cursors, ok := response["scope_cursors"].(map[string]any)
		if !ok || len(cursors) != 2 {
			t.Fatalf("predicate cursor_set_shape failed: valid=%t count=%d", ok, len(cursors))
		}
		terminalCursorCount := 0
		for _, scopeID := range []string{"cf:global", "user:diagnostic-user"} {
			cursor, ok := cursors[scopeID].(string)
			if ok && cursor != "" {
				terminalCursorCount++
			}
		}
		if terminalCursorCount != 2 {
			t.Fatalf("predicate cursor_terminal_progress failed: count=%d", terminalCursorCount)
		}
		checkpoint, ok := checkpoints["user:diagnostic-user"]
		if !ok || checkpoint.PositionKind != "transaction_end" || !checkpoint.CommitLSNValid ||
			checkpoint.EventOrdinalValid || checkpoint.EffectOrdinalValid {
			t.Fatalf(
				"predicate cursor_acknowledgement_position failed: present=%t kind=%q commit=%t event=%t effect=%t",
				ok,
				checkpoint.PositionKind,
				checkpoint.CommitLSNValid,
				checkpoint.EventOrdinalValid,
				checkpoint.EffectOrdinalValid,
			)
		}
	})
}

func TestRealMutationControlWALAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionMutationControlWALHarness(t, ctx)
	connectRealProtocolClient(t, ctx, harness, token, "mutation-wal-client")

	prior, err := harness.Operator().ObserveWALProgress(ctx)
	if err != nil {
		t.Fatalf("observe WAL progress before control transaction: %v", err)
	}
	recordID := "00000000-0000-4000-8c02-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		"mutation-wal-acknowledgement",
	); err != nil {
		t.Fatalf("insert WAL control source row: %v", err)
	}
	observation := waitForMutationControlWALRecord(t, ctx, harness, recordID, prior.AcknowledgedEndLSN)
	if !observation.WorkerRunning || observation.BlockingPoison || len(observation.Records) != 1 {
		t.Fatalf(
			"predicate wal_control_setup failed: worker=%t poison=%t record_count=%d",
			observation.WorkerRunning,
			observation.BlockingPoison,
			len(observation.Records),
		)
	}
	record := observation.Records[0]
	if record.CommitLSN == "" || record.EndLSN == "" || record.CommitLSN == record.EndLSN {
		t.Fatalf(
			"predicate wal_control_boundaries failed: commit_present=%t end_present=%t distinct=%t",
			record.CommitLSN != "",
			record.EndLSN != "",
			record.CommitLSN != record.EndLSN,
		)
	}

	t.Run("assertion", func(t *testing.T) {
		if !observation.AcknowledgementMatchesObservedEnd || observation.AcknowledgedEndLSN != record.EndLSN ||
			!observation.SlotMatchesObservedEnd || observation.SlotConfirmedFlushLSN != record.EndLSN {
			t.Fatalf(
				"predicate wal_acknowledgement_exact failed: durable=%t durable_value=%t slot=%t slot_value=%t",
				observation.AcknowledgementMatchesObservedEnd,
				observation.AcknowledgedEndLSN == record.EndLSN,
				observation.SlotMatchesObservedEnd,
				observation.SlotConfirmedFlushLSN == record.EndLSN,
			)
		}
	})
}

func TestRealMutationControlProgressOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	recordID := "00000000-0000-4000-8c06-000000000001"
	if err := harness.Operator().CreateWALProgressOrderViolation(ctx, recordID); err != nil {
		t.Fatalf("create persisted WAL progress order violation: %v", err)
	}

	var observation blackbox.WALProgressOrderObservation
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		observation, err = harness.Operator().ObserveWALProgressOrder(ctx, recordID)
		if err != nil {
			t.Fatalf("observe WAL progress order control: %v", err)
		}
		if observation.RecordMaterialized || (observation.PoisonActive && observation.WorkerBlocked) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Run("assertion", func(t *testing.T) {
		if !observation.PoisonActive || observation.FailureClass != "validation_failed" ||
			!observation.RelationIDMatchesRegistry || !observation.SourceRowPresent ||
			!observation.ProgressCommitAtOrAhead || observation.RecordMaterialized || !observation.WorkerBlocked {
			t.Fatalf(
				"predicate persisted_progress_order failed: poison=%t class=%q relation=%t source=%t ahead=%t materialized=%t blocked=%t",
				observation.PoisonActive,
				observation.FailureClass,
				observation.RelationIDMatchesRegistry,
				observation.SourceRowPresent,
				observation.ProgressCommitAtOrAhead,
				observation.RecordMaterialized,
				observation.WorkerBlocked,
			)
		}
	})
}

func TestRealMutationControlMutationConservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "mutation-conservation-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")

	recordIDs := []string{
		"00000000-0000-4000-8c03-000000000001",
		"00000000-0000-4000-8c03-000000000002",
		"00000000-0000-4000-8c03-000000000003",
		"00000000-0000-4000-8c03-000000000004",
	}
	if err := harness.Source().ExecContext(ctx, `
		INSERT INTO cf_items (id, owner_id, value) VALUES
		($1, $2, $3), ($4, $5, $6)`,
		recordIDs[1], "diagnostic-user", "mutation-conservation-conflict-one",
		recordIDs[3], "diagnostic-user", "mutation-conservation-conflict-two",
	); err != nil {
		t.Fatalf("insert mutation conservation conflict rows: %v", err)
	}

	mutationIDs := []string{
		"00000000-0000-4000-8c03-000000000011",
		"00000000-0000-4000-8c03-000000000012",
		"00000000-0000-4000-8c03-000000000013",
		"00000000-0000-4000-8c03-000000000014",
	}
	values := []string{
		"mutation-conservation-applied-one",
		"mutation-conservation-conflict-one",
		"mutation-conservation-applied-two",
		"mutation-conservation-conflict-two",
	}
	mutations := make([]map[string]any, 0, len(mutationIDs))
	for index := range mutationIDs {
		mutations = append(mutations, phase4InsertMutation(
			client,
			table,
			ownerField,
			mutationIDs[index],
			recordIDs[index],
			values[index],
		))
	}
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8c03-000000000020",
		mutations,
	))

	t.Run("assertion", func(t *testing.T) {
		if status != http.StatusOK {
			t.Fatalf("mutation conservation push status = %d, want 200", status)
		}
		accepted := requireOutcomeList(t, response, "accepted")
		rejected := requireOutcomeList(t, response, "rejected")
		if !slices.Equal(phase4OutcomeIDs(t, accepted), []string{mutationIDs[0], mutationIDs[2]}) ||
			!slices.Equal(phase4OutcomeIDs(t, rejected), []string{mutationIDs[1], mutationIDs[3]}) {
			t.Fatalf(
				"predicate mutation_partition_order failed: accepted_count=%d rejected_count=%d",
				len(accepted),
				len(rejected),
			)
		}
		if len(accepted)+len(rejected) != len(mutationIDs) {
			t.Fatalf("mutation conservation returned %d outcomes for %d mutations", len(accepted)+len(rejected), len(mutationIDs))
		}
		seen := make(map[string]struct{}, len(mutationIDs))
		for _, outcome := range append(append([]map[string]any(nil), accepted...), rejected...) {
			mutationID, ok := outcome["mutation_id"].(string)
			if !ok {
				t.Fatal("predicate mutation_identity_present failed")
			}
			if _, duplicate := seen[mutationID]; duplicate {
				t.Fatalf("predicate mutation_identity_unique failed: observed_count=%d", len(seen)+1)
			}
			seen[mutationID] = struct{}{}
		}
		if len(seen) != len(mutationIDs) {
			t.Fatalf("predicate mutation_conservation failed: observed_count=%d expected_count=%d", len(seen), len(mutationIDs))
		}
		assertCanonicalPhase4Outcome(t, accepted[0], client, table, ownerField, mutationIDs[0], recordIDs[0], values[0], "applied", "")
		assertCanonicalPhase4Outcome(t, accepted[1], client, table, ownerField, mutationIDs[2], recordIDs[2], values[2], "applied", "")
		assertCanonicalPhase4Outcome(t, rejected[0], client, table, ownerField, mutationIDs[1], recordIDs[1], values[1], "conflict", "row_already_exists")
		assertCanonicalPhase4Outcome(t, rejected[1], client, table, ownerField, mutationIDs[3], recordIDs[3], values[3], "conflict", "row_already_exists")
	})
}

func TestRealMutationControlChecksumCorrectness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "mutation-checksum-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8c04-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8c04-000000000002")
	manifest := loadMutationControlManifest(t, ctx, harness)
	table := requireRealTable(t, client, "cf_items")
	recordID := "00000000-0000-4000-8c04-000000000003"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		"mutation-checksum",
	); err != nil {
		t.Fatalf("insert checksum control source row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)
	response := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 100)

	t.Run("assertion", func(t *testing.T) {
		changes, ok := mutationControlChanges(response)
		if !ok || len(changes) != 1 {
			t.Fatalf("predicate checksum_change_count failed: valid=%t count=%d", ok, len(changes))
		}
		change, ok := mutationControlChange(changes, "user:diagnostic-user", table, recordID)
		if !ok {
			t.Fatal("predicate checksum_change_identity failed")
		}
		expectedRowDigest, expectedScopeDigest, err := independentlyComputeMutationControlDigests(
			manifest,
			"user:diagnostic-user",
			table,
			change,
		)
		if err != nil {
			t.Fatal("predicate checksum_independent_computation failed")
		}
		actualRowDigest, ok := mutationControlChecksumDigest(change["row_checksum"])
		if !ok || actualRowDigest != expectedRowDigest {
			t.Fatalf("predicate row_checksum_exact failed: valid=%t equal=%t", ok, actualRowDigest == expectedRowDigest)
		}
		checksums, ok := response["checksums"].(map[string]any)
		if !ok {
			t.Fatal("predicate terminal_checksum_map failed")
		}
		actualScopeDigest, ok := mutationControlChecksumDigest(checksums["user:diagnostic-user"])
		if !ok || actualScopeDigest != expectedScopeDigest {
			t.Fatalf("predicate scope_checksum_exact failed: valid=%t equal=%t", ok, actualScopeDigest == expectedScopeDigest)
		}
	})
}

func TestRealMutationControlScopeIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "mutation-scope-client")
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8c05-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8c05-000000000002")

	recordID := "00000000-0000-4000-8c05-000000000003"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
		recordID,
		"mutation-scope-global-only",
	); err != nil {
		t.Fatalf("insert scope isolation source row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", recordID)
	userScopes := map[string]any{
		"user:diagnostic-user": client.Scopes["user:diagnostic-user"],
	}
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, userScopes, 100))

	t.Run("assertion", func(t *testing.T) {
		if status != http.StatusOK {
			t.Fatalf("scope isolation pull status = %d, want 200", status)
		}
		changes, changesValid := mutationControlChanges(response)
		if !changesValid || len(changes) != 0 {
			t.Fatalf("predicate scope_isolation failed: valid=%t change_count=%d", changesValid, len(changes))
		}
	})
}

func provisionMutationControlWALHarness(t *testing.T, ctx context.Context) (*blackbox.Harness, string) {
	t.Helper()
	if !*provision || !*install {
		t.Fatal("WAL mutation control requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load WAL mutation control environment: %v", err)
	}
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{
		Environment:                         environment,
		AllowInitialCaptureReadinessFailure: true,
	})
	if err != nil {
		t.Fatalf("provision WAL mutation control harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close WAL mutation control harness: %v", err)
		}
	})
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		t.Fatalf("sign WAL mutation control token: %v", err)
	}
	return harness, token
}

func waitForMutationControlWALRecord(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	recordID string,
	priorAcknowledgement string,
) blackbox.WALPipelineObservation {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var latest blackbox.WALPipelineObservation
	for time.Now().Before(deadline) {
		observation, err := harness.Operator().ObserveWALRecords(ctx, []string{recordID})
		if err == nil {
			latest = observation
			if len(observation.Records) == 1 && observation.AcknowledgedEndLSN != "" && observation.AcknowledgedEndLSN != priorAcknowledgement {
				return observation
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf(
		"predicate wal_control_progress failed: worker=%t poison=%t record_count=%d acknowledged=%t",
		latest.WorkerRunning,
		latest.BlockingPoison,
		len(latest.Records),
		latest.AcknowledgedEndLSN != "" && latest.AcknowledgedEndLSN != priorAcknowledgement,
	)
	return blackbox.WALPipelineObservation{}
}

func loadMutationControlManifest(t *testing.T, ctx context.Context, harness *blackbox.Harness) vectors.Manifest {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, harness.AdapterURL()+"/sync/schema", nil)
	if err != nil {
		t.Fatalf("create checksum control schema request: %v", err)
	}
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("request checksum control schema: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("checksum control schema status = %d, want 200", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatalf("read checksum control schema: %v", err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode checksum control schema envelope: %v", err)
	}
	rawManifest, ok := envelope["manifest"]
	if !ok {
		t.Fatal("checksum control schema has no manifest")
	}
	manifest, err := vectors.ParseManifest(rawManifest)
	if err != nil {
		t.Fatalf("parse independently verified checksum manifest: %v", err)
	}
	return manifest
}

func mutationControlChanges(response map[string]any) ([]map[string]any, bool) {
	rawChanges, ok := response["changes"].([]any)
	if !ok {
		return nil, false
	}
	changes := make([]map[string]any, 0, len(rawChanges))
	for _, rawChange := range rawChanges {
		change, ok := rawChange.(map[string]any)
		if !ok {
			return nil, false
		}
		changes = append(changes, change)
	}
	return changes, true
}

func mutationControlChange(changes []map[string]any, scopeID string, table realProtocolTable, recordID string) (map[string]any, bool) {
	for _, change := range changes {
		if change["scope"] != scopeID || change["table"] != table.ID {
			continue
		}
		pk, ok := change["pk"].(map[string]any)
		if ok && pk[table.PrimaryKeyField] == recordID {
			return change, true
		}
	}
	return nil, false
}

func independentlyComputeMutationControlDigests(
	manifest vectors.Manifest,
	scopeID string,
	table realProtocolTable,
	change map[string]any,
) (string, string, error) {
	pk, ok := change["pk"].(map[string]any)
	if !ok || len(pk) != 1 {
		return "", "", fmt.Errorf("change primary key is invalid")
	}
	primaryKey, present := pk[table.PrimaryKeyField]
	if !present {
		return "", "", fmt.Errorf("change primary key field is missing")
	}
	primaryKeyJSON, err := json.Marshal(primaryKey)
	if err != nil {
		return "", "", fmt.Errorf("encode change primary key: %w", err)
	}
	rowValues, ok := change["row"].(map[string]any)
	if !ok || len(rowValues) == 0 {
		return "", "", fmt.Errorf("change row is invalid")
	}
	fields := make([]vectors.RowField, 0, len(rowValues))
	for fieldID, value := range rowValues {
		valueJSON, err := json.Marshal(value)
		if err != nil {
			return "", "", fmt.Errorf("encode field %q: %w", fieldID, err)
		}
		fields = append(fields, vectors.RowField{FieldID: fieldID, Value: valueJSON})
	}
	serverVersion, ok := change["server_version"].(string)
	if !ok || serverVersion == "" {
		return "", "", fmt.Errorf("change server version is invalid")
	}
	row := vectors.Row{PK: primaryKeyJSON, Fields: fields}
	rowDigest, err := vectors.RowDigest(manifest, table.ID, row, serverVersion)
	if err != nil {
		return "", "", fmt.Errorf("compute independent row digest: %w", err)
	}
	identity, err := vectors.RowIdentity(manifest, table.ID, primaryKeyJSON)
	if err != nil {
		return "", "", fmt.Errorf("compute independent row identity: %w", err)
	}
	scopeDigest, err := vectors.ScopeDigest(manifest.Hash(), scopeID, []vectors.DigestEntry{{
		RowIdentity: identity,
		RowDigest:   rowDigest,
	}})
	if err != nil {
		return "", "", fmt.Errorf("compute independent scope digest: %w", err)
	}
	return hex.EncodeToString(rowDigest[:]), hex.EncodeToString(scopeDigest[:]), nil
}

func mutationControlChecksumDigest(value any) (string, bool) {
	checksum, ok := value.(map[string]any)
	if !ok || len(checksum) != 4 || checksum["algorithm"] != "sha256" || checksum["encoding"] != "hex" || checksum["version"] != float64(1) {
		return "", false
	}
	digest, ok := checksum["digest"].(string)
	if !ok || len(digest) != 64 {
		return "", false
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != digest {
		return "", false
	}
	return digest, true
}
