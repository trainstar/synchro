package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

type realSchemaTableReference struct {
	Schema  map[string]any
	TableID string
	PKField string
	Fields  map[string]string
}

func TestRealRegistryGenerationReloadAtCommitBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)

	priorRecordID := "00000000-0000-4000-8c01-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		priorRecordID,
		"diagnostic-user",
		"registry-before",
	); err != nil {
		t.Fatalf("insert pre-activation registry row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", priorRecordID)

	transitionRealSchemaQueue(t, ctx, harness)

	postItemRecordID := "00000000-0000-4000-8c01-000000000002"
	postSchemaRecordID := "00000000-0000-4000-8c01-000000000003"
	transaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin post-activation source transaction: %v", err)
	}
	if _, err := transaction.ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		postItemRecordID,
		"diagnostic-user",
		"registry-after",
	); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("insert post-activation item: %v", err)
	}
	if _, err := transaction.ExecContext(
		ctx,
		"INSERT INTO cf_schema_queue (id, owner_id, authored_mutation) VALUES ($1, $2, $3::jsonb)",
		postSchemaRecordID,
		"diagnostic-user",
		`{"value":"registry-after"}`,
	); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("insert post-activation schema row: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit post-activation source transaction: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", postItemRecordID)
	waitForRealWALRecords(t, ctx, harness, "cf_schema_queue", postSchemaRecordID)

	observation, err := harness.Operator().ObserveRegistryActivation(
		ctx,
		priorRecordID,
		postItemRecordID,
		postSchemaRecordID,
	)
	if err != nil {
		t.Fatalf("observe registry activation: %v", err)
	}
	if observation.SourceGeneration <= 0 || observation.ActiveGeneration <= observation.SourceGeneration {
		t.Fatalf("registry generations did not advance: %#v", observation)
	}
	if observation.PriorTransactionGeneration != observation.SourceGeneration ||
		observation.ActivationTransactionGeneration != observation.SourceGeneration ||
		observation.PostTransactionGeneration != observation.ActiveGeneration {
		t.Fatalf("source transactions mixed registry generations: %#v", observation)
	}
	if !observation.ActivationBoundaryComplete ||
		!observation.PostTransactionSingleCommit ||
		!observation.PostProjectionGenerationMatches ||
		!observation.RuntimeGenerationMatches ||
		!observation.WorkerGenerationMatches ||
		!observation.NoPendingRegistryGeneration {
		t.Fatalf("registry activation was not complete and commit-ordered: %#v", observation)
	}
}

func TestRealSchemaIncompatibleMutationPersistsCanonicalIntent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)

	oldTable, newTable := transitionRealSchemaQueue(t, ctx, harness)
	legacyField := requireRealSchemaField(t, oldTable, "legacy_value")
	ownerField := requireRealSchemaField(t, oldTable, "owner_id")
	authoredMutationField := requireRealSchemaField(t, oldTable, "authored_mutation")
	if _, present := newTable.Fields["legacy_value"]; present {
		t.Fatal("schema transition retained the removed legacy field")
	}
	if newTable.TableID != oldTable.TableID || newTable.PKField != oldTable.PKField {
		t.Fatal("schema transition changed stable table or primary-key identity")
	}

	client := connectRealProtocolClient(t, ctx, harness, token, "schema-intent-client")
	if !sameRealSchemaReference(client.Schema, newTable.Schema) {
		t.Fatalf("connected client did not receive the active schema: client=%#v active=%#v", client.Schema, newTable.Schema)
	}
	recordID := "00000000-0000-4000-8c02-000000000001"
	mutationID := "00000000-0000-4000-8c02-000000000002"
	batchID := "00000000-0000-4000-8c02-000000000003"
	mutation := map[string]any{
		"mutation_id":     mutationID,
		"table":           oldTable.TableID,
		"pk":              map[string]any{oldTable.PKField: recordID},
		"authored_schema": oldTable.Schema,
		"op":              "insert",
		"client_version":  phase4ClientVersion,
		"columns": map[string]any{
			ownerField:            "diagnostic-user",
			authoredMutationField: map[string]any{"value": "retain-this-authored-value"},
			legacyField:           "retain-this-legacy-value",
		},
	}
	payload := phase4PushPayload(client, batchID, []map[string]any{mutation})
	requestBody, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode schema-incompatible push: %v", err)
	}
	request := blackbox.Request{
		Method: http.MethodPost,
		Path:   "/sync/push",
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:  requestBody,
		Class: "phase4/schema-incompatible-intent",
	}
	firstClient := newRealBlackboxClient(harness.AdapterURL(), token)
	first, err := firstClient.Do(ctx, request)
	if err != nil {
		t.Fatalf("submit schema-incompatible push: %v", err)
	}
	if first.Status != http.StatusOK {
		t.Fatalf("schema-incompatible push status = %d, want 200", first.Status)
	}
	response := decodeRealResponseObject(t, first.Body)
	if accepted := requireOutcomeList(t, response, "accepted"); len(accepted) != 0 {
		t.Fatal("schema-incompatible push returned an accepted mutation")
	}
	rejected := requireOutcomeList(t, response, "rejected")
	if response["batch_id"] != batchID || len(rejected) != 1 {
		t.Fatal("schema-incompatible push returned an invalid partition")
	}
	outcome := rejected[0]
	if outcome["mutation_id"] != mutationID || outcome["status"] != "rejected_terminal" ||
		outcome["code"] != "schema_incompatible" || outcome["retryable"] != false {
		t.Fatalf("schema-incompatible outcome is invalid: %#v", outcome)
	}
	if !sameRealSchemaReference(outcome["authored_schema"], oldTable.Schema) ||
		!sameRealSchemaReference(outcome["current_schema"], newTable.Schema) {
		t.Fatalf("schema-incompatible outcome lost schema identity: %#v", outcome)
	}
	incompatibleFields := stringListFromRealJSON(t, outcome["incompatible_field_ids"])
	if !slices.Equal(incompatibleFields, []string{legacyField}) {
		t.Fatalf("schema-incompatible fields = %v, want [%s]", incompatibleFields, legacyField)
	}

	restartedClient := newRealBlackboxClient(harness.AdapterURL(), token)
	replay, err := restartedClient.Do(ctx, request)
	if err != nil {
		t.Fatalf("replay schema-incompatible push after client restart: %v", err)
	}
	if err := blackbox.CompareExactReplay(first, replay); err != nil || !bytes.Equal(first.Body, replay.Body) {
		t.Fatalf("schema-incompatible replay changed its canonical outcome: %v", err)
	}

	mutationBody, err := json.Marshal(mutation)
	if err != nil {
		t.Fatalf("encode authored mutation: %v", err)
	}
	canonicalMutation, err := blackbox.CanonicalResponseBytes(mutationBody)
	if err != nil {
		t.Fatalf("canonicalize authored mutation: %v", err)
	}
	observation, err := harness.Operator().ObserveSchemaIncompatibleMutation(
		ctx,
		client.ID,
		mutationID,
		recordID,
		canonicalMutation,
	)
	if err != nil {
		t.Fatalf("observe schema-incompatible mutation ledger: %v", err)
	}
	oldVersion, oldHash := realSchemaReference(t, oldTable.Schema)
	newVersion, newHash := realSchemaReference(t, newTable.Schema)
	if observation.LedgerCount != 1 || observation.RequestOrdinal != 1 ||
		observation.AuthoredSchemaVersion != oldVersion || observation.AuthoredSchemaHash != oldHash ||
		observation.SubmittedSchemaVersion != newVersion || observation.SubmittedSchemaHash != newHash ||
		observation.OutcomeSchemaVersion != newVersion || observation.OutcomeSchemaHash != newHash ||
		observation.OutcomeStatus != "rejected_terminal" || observation.RejectionCode != "schema_incompatible" ||
		!observation.CanonicalRequestMatches || observation.SourceRowCount != 0 {
		t.Fatalf("schema-incompatible durable state is incomplete: %#v", observation)
	}
}

func transitionRealSchemaQueue(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
) (realSchemaTableReference, realSchemaTableReference) {
	t.Helper()
	oldTable := loadRealSchemaTableReference(t, ctx, harness, "cf_schema_queue")
	if err := harness.Operator().TransitionSchemaQueue(ctx); err != nil {
		t.Fatalf("commit atomic schema transition: %v", err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		current, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), "cf_schema_queue")
		if err == nil && !sameRealSchemaReference(current.Schema, oldTable.Schema) {
			return oldTable, current
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			t.Fatalf("wait for schema transition: %v", ctx.Err())
		case <-timer.C:
		}
	}
	t.Fatalf("schema transition did not activate; %s", harness.FailureDiagnostics())
	return realSchemaTableReference{}, realSchemaTableReference{}
}

func loadRealSchemaTableReference(t *testing.T, ctx context.Context, harness *blackbox.Harness, tableName string) realSchemaTableReference {
	t.Helper()
	reference, err := fetchRealSchemaTableReference(ctx, harness.AdapterURL(), tableName)
	if err != nil {
		t.Fatalf("load real schema table %s: %v", tableName, err)
	}
	return reference
}

func fetchRealSchemaTableReference(ctx context.Context, adapterURL, tableName string) (realSchemaTableReference, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, adapterURL+"/sync/schema", nil)
	if err != nil {
		return realSchemaTableReference{}, errors.New("create schema request failed")
	}
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return realSchemaTableReference{}, errors.New("request schema failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return realSchemaTableReference{}, fmt.Errorf("schema status = %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return realSchemaTableReference{}, errors.New("read schema response failed")
	}
	var envelope struct {
		Manifest json.RawMessage `json:"manifest"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Manifest) == 0 {
		return realSchemaTableReference{}, errors.New("decode schema envelope failed")
	}
	var manifest map[string]any
	if err := json.Unmarshal(envelope.Manifest, &manifest); err != nil {
		return realSchemaTableReference{}, errors.New("decode schema manifest failed")
	}
	version, versionOK := manifest["schema_version"].(float64)
	hash, hashOK := manifest["schema_hash"].(string)
	if !versionOK || version <= 0 || !hashOK || len(hash) != 64 {
		return realSchemaTableReference{}, errors.New("schema reference is invalid")
	}
	tables, ok := manifest["tables"].([]any)
	if !ok {
		return realSchemaTableReference{}, errors.New("schema tables are invalid")
	}
	for _, rawTable := range tables {
		table, ok := rawTable.(map[string]any)
		if !ok || table["name"] != tableName {
			continue
		}
		reference := realSchemaTableReference{
			Schema: map[string]any{"version": int64(version), "hash": hash},
			Fields: make(map[string]string),
		}
		reference.TableID, _ = table["table_id"].(string)
		reference.PKField, _ = table["primary_key_field_id"].(string)
		fields, ok := table["fields"].([]any)
		if !ok {
			return realSchemaTableReference{}, errors.New("schema table fields are invalid")
		}
		for _, rawField := range fields {
			field, ok := rawField.(map[string]any)
			if !ok {
				return realSchemaTableReference{}, errors.New("schema field is invalid")
			}
			name, _ := field["name"].(string)
			fieldID, _ := field["field_id"].(string)
			if name == "" || !uuidPattern.MatchString(fieldID) {
				return realSchemaTableReference{}, errors.New("schema field identity is invalid")
			}
			reference.Fields[name] = fieldID
		}
		if !uuidPattern.MatchString(reference.TableID) || !uuidPattern.MatchString(reference.PKField) {
			return realSchemaTableReference{}, errors.New("schema table identity is invalid")
		}
		return reference, nil
	}
	return realSchemaTableReference{}, errors.New("schema table is missing")
}

func requireRealSchemaField(t *testing.T, table realSchemaTableReference, name string) string {
	t.Helper()
	fieldID, ok := table.Fields[name]
	if !ok {
		t.Fatalf("real schema field %s is missing", name)
	}
	return fieldID
}

func sameRealSchemaReference(left any, right map[string]any) bool {
	leftObject, ok := left.(map[string]any)
	if !ok {
		return false
	}
	leftVersion, leftVersionOK := realSchemaVersion(leftObject["version"])
	rightVersion, rightVersionOK := realSchemaVersion(right["version"])
	leftHash, leftHashOK := leftObject["hash"].(string)
	rightHash, rightHashOK := right["hash"].(string)
	return leftVersionOK && rightVersionOK && leftVersion == rightVersion &&
		leftHashOK && rightHashOK && leftHash == rightHash
}

func realSchemaReference(t *testing.T, reference map[string]any) (int64, string) {
	t.Helper()
	version, versionOK := realSchemaVersion(reference["version"])
	hash, hashOK := reference["hash"].(string)
	if !versionOK || version <= 0 || !hashOK || len(hash) != 64 {
		t.Fatalf("real schema reference is invalid: %#v", reference)
	}
	return version, hash
}

func realSchemaVersion(value any) (int64, bool) {
	switch value := value.(type) {
	case int64:
		return value, value > 0
	case float64:
		return int64(value), value > 0 && value == float64(int64(value))
	default:
		return 0, false
	}
}

func stringListFromRealJSON(t *testing.T, value any) []string {
	t.Helper()
	raw, ok := value.([]any)
	if !ok {
		t.Fatalf("real JSON string list is invalid: %#v", value)
	}
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		text, ok := item.(string)
		if !ok {
			t.Fatalf("real JSON string list member is invalid: %#v", item)
		}
		result = append(result, text)
	}
	return result
}

func newRealBlackboxClient(adapterURL, token string) *blackbox.Client {
	return &blackbox.Client{
		BaseURL: adapterURL,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
		Tokens: blackbox.TokenProviderFunc(func(context.Context) (string, error) {
			return token, nil
		}),
	}
}
