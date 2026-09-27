//go:build datasetcharacterization

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/dataset"
)

// This characterization records complete correct work for one seeded
// dataset. It has no numerical pass or fail rule (decision D-06). The run
// fails only when its exact data, authorization, or progress checks fail.
const characterizationFormat = "synchro-dataset-characterization-v1"

// characterizationRebuildUsers bounds the users whose complete assigned
// scopes are rebuilt and compared in the final reconciliation.
const characterizationRebuildUsers = 24

type characterizationTransaction struct {
	Kind             string `json:"kind"`
	Statements       int    `json:"statements"`
	EstimatedRecords int    `json:"estimated_records"`
	CommitNS         int64  `json:"commit_ns"`
	VisibleNS        int64  `json:"commit_to_materialized_ns"`
}

type characterizationRebuild struct {
	User        string `json:"user"`
	Scope       string `json:"scope"`
	Records     int    `json:"records"`
	Pages       int    `json:"pages"`
	Bytes       int    `json:"response_bytes"`
	FirstPageNS int64  `json:"first_page_ns"`
	TotalNS     int64  `json:"total_ns"`
}

type characterizationPush struct {
	User         string `json:"user"`
	Mutations    int    `json:"mutations"`
	Applied      int    `json:"applied"`
	HTTPNS       int64  `json:"http_acceptance_ns"`
	VisibleNS    int64  `json:"acceptance_to_materialized_ns"`
	OwnerPullNS  int64  `json:"owner_pull_ns"`
	OwnerChanges int    `json:"owner_pull_changes"`
}

type characterizationExport struct {
	Scope       string `json:"scope"`
	Records     int    `json:"records"`
	Pages       int    `json:"pages"`
	PageLimit   int    `json:"page_limit"`
	ManifestNS  int64  `json:"manifest_ns"`
	FirstPageNS int64  `json:"first_page_ns"`
	TotalNS     int64  `json:"total_ns"`
}

type characterizationResult struct {
	Format   string `json:"format"`
	Revision string `json:"revision"`
	// Completed is false when a check or bound stopped the run. The retained
	// samples then describe unfinished work, not complete throughput.
	Completed   bool   `json:"completed"`
	Phase       string `json:"last_phase"`
	Environment struct {
		GOOS                    string    `json:"goos"`
		GOARCH                  string    `json:"goarch"`
		LogicalCPUs             int       `json:"logical_cpu_count"`
		LoadAverageStart        []float64 `json:"load_average_start"`
		LoadAverageEnd          []float64 `json:"load_average_end"`
		AdapterSHA256           string    `json:"adapter_sha256"`
		ExtensionManifestSHA256 string    `json:"extension_manifest_sha256"`
		PostgresVersion         string    `json:"postgres_version"`
	} `json:"environment"`
	Workload struct {
		Seed          uint64        `json:"seed"`
		Size          dataset.Size  `json:"size"`
		Stats         dataset.Stats `json:"stats"`
		RebuildUsers  int           `json:"rebuild_users"`
		PushUsers     int           `json:"push_users"`
		MutationBatch int           `json:"mutations_per_push"`
	} `json:"workload"`
	Observer struct {
		MaterializationPollNS int64  `json:"materialization_poll_ns"`
		Boundary              string `json:"boundary"`
	} `json:"observer"`
	Load             []characterizationTransaction `json:"initial_load"`
	InitialRebuilds  []characterizationRebuild     `json:"initial_rebuilds"`
	Pushes           []characterizationPush        `json:"pushes"`
	PushReconcileNS  int64                         `json:"push_reconciliation_ns"`
	History          []characterizationTransaction `json:"history"`
	FinalRebuilds    []characterizationRebuild     `json:"final_rebuilds"`
	Export           characterizationExport        `json:"catalog_export"`
	PullAfterHistory map[string]int                `json:"pull_after_history_status_counts"`
	Resources        struct {
		WorkerRSSBaselineBytes int64            `json:"wal_worker_rss_baseline_bytes"`
		WorkerRSSPeakBytes     int64            `json:"wal_worker_observed_peak_rss_bytes"`
		MaxPendingFences       int              `json:"max_observed_pending_fences"`
		RelationBytes          map[string]int64 `json:"synchro_relation_bytes"`
		RelationRows           map[string]int64 `json:"synchro_relation_rows"`
	} `json:"resources"`
}

func TestRealDatasetCharacterization(t *testing.T) {
	t.Run("assertion", runDatasetCharacterization)
}

func runDatasetCharacterization(t *testing.T) {
	seed, err := strconv.ParseUint(os.Getenv("DATASET_SEED"), 10, 64)
	if err != nil {
		t.Fatalf("DATASET_SEED is invalid: %v", err)
	}
	size, ok := dataset.LookupSize(os.Getenv("DATASET_SIZE"))
	if !ok {
		t.Fatal("DATASET_SIZE is invalid")
	}
	resultPath := os.Getenv("DATASET_CHARACTERIZATION_RESULT")
	if !filepath.IsAbs(resultPath) {
		t.Fatal("DATASET_CHARACTERIZATION_RESULT must be an absolute path")
	}
	plan, err := dataset.Generate(seed, size)
	if err != nil {
		t.Fatalf("generate dataset: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 170*time.Minute)
	defer cancel()
	harness := provisionRealDatasetHarness(t, ctx)
	run := newDatasetRuntime(t, ctx, harness)
	var result characterizationResult
	result.Format = characterizationFormat
	result.Revision = os.Getenv("DATASET_REVISION")
	defer func() {
		result.Completed = !t.Failed()
		result.Environment.LoadAverageEnd = characterizationLoadAverage()
		data, err := json.MarshalIndent(result, "", "  ")
		if err == nil {
			err = os.WriteFile(resultPath, append(data, '\n'), 0o644)
		}
		if err != nil {
			t.Errorf("write characterization result: %v", err)
		}
	}()
	result.Phase = "provision"
	recordCharacterizationEnvironment(t, run, &result)
	result.Workload.Seed, result.Workload.Size, result.Workload.Stats = seed, size, plan.Stats
	result.Observer.MaterializationPollNS = int64(20 * time.Millisecond)
	result.Observer.Boundary = "source commit or push response to zero pending write fences"

	workerPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
	if err != nil {
		t.Fatalf("observe WAL worker: %v", err)
	}
	baseline, stopRSS, err := startRealProcessRSSSampler(ctx, workerPID, 0)
	if err != nil {
		t.Fatalf("start WAL worker RSS sampling: %v", err)
	}
	result.Resources.WorkerRSSBaselineBytes = baseline
	result.Phase = "initial-load"
	for _, transaction := range plan.Initial {
		result.Load = append(result.Load, applyCharacterizationTransaction(run, transaction))
	}
	peak, err := stopRSS()
	if err != nil {
		t.Fatalf("sample WAL worker RSS: %v", err)
	}
	result.Resources.WorkerRSSPeakBytes = peak

	result.Phase = "initial-rebuild"
	users := plan.Users[:min(len(plan.Users), characterizationRebuildUsers)]
	result.Workload.RebuildUsers = len(users)
	expected := run.expected()
	clients := make(map[string]*datasetClient, len(users))
	for _, user := range users {
		clients[user], result.InitialRebuilds = characterizationConnect(run, user, "characterization-"+user, expected, result.InitialRebuilds)
	}

	result.Phase = "push"
	const batch = 50
	result.Workload.MutationBatch = batch
	for _, user := range users {
		sets := plan.SetsByUser[user]
		if len(sets) == 0 {
			continue
		}
		result.Workload.PushUsers++
		result.Pushes = append(result.Pushes, characterizationPushSets(run, clients[user], sets[:min(len(sets), batch)], plan.Seed))
	}
	started := time.Now()
	expected = run.expected()
	for _, user := range users {
		run.converge(clients[user], expected, 5*time.Minute)
	}
	result.PushReconcileNS = int64(time.Since(started))

	result.Phase = "history"
	for _, transaction := range plan.History {
		result.History = append(result.History, applyCharacterizationTransaction(run, transaction))
	}
	result.Phase = "final-reconciliation"
	result.PullAfterHistory = map[string]int{}
	for _, user := range users {
		status, _, err := run.post(clients[user].Token, "/sync/pull", characterizationPullRequest(clients[user]))
		if err != nil {
			t.Fatalf("pull after history for %s: %v", user, err)
		}
		result.PullAfterHistory[strconv.Itoa(status)]++
	}
	expected = run.expected()
	for _, user := range users {
		_, result.FinalRebuilds = characterizationConnect(run, user, "characterization-final-"+user, expected, result.FinalRebuilds)
	}
	result.Export = characterizationCatalogExport(run, clients[users[0]], expected)
	result.Resources.MaxPendingFences = run.maxPending
	result.Resources.RelationBytes, result.Resources.RelationRows = characterizationRelations(run)
	result.Phase = "complete"
}

func recordCharacterizationEnvironment(t *testing.T, run *datasetRuntime, result *characterizationResult) {
	t.Helper()
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load characterization environment: %v", err)
	}
	digest := func(path string) string {
		data, err := os.ReadFile(path)
		fields := strings.Fields(string(data))
		if err != nil || len(fields) == 0 || len(fields[0]) != 64 {
			t.Fatalf("artifact digest %s is invalid", filepath.Base(path))
		}
		return fields[0]
	}
	result.Environment.GOOS, result.Environment.GOARCH, result.Environment.LogicalCPUs = runtime.GOOS, runtime.GOARCH, runtime.NumCPU()
	result.Environment.AdapterSHA256 = digest(environment.AdapterArtifact + ".sha256")
	result.Environment.ExtensionManifestSHA256 = digest(filepath.Join(environment.ExtensionArtifact, "artifact-manifest.json.sha256"))
	if err := run.admin.QueryRowContext(run.ctx, "SELECT current_setting('server_version')").Scan(&result.Environment.PostgresVersion); err != nil {
		t.Fatalf("read PostgreSQL version: %v", err)
	}
	result.Environment.LoadAverageStart = characterizationLoadAverage()
}

// characterizationLoadAverage returns the host load averages, or nil where
// /proc/loadavg does not exist.
func characterizationLoadAverage() []float64 {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return nil
	}
	fields := strings.Fields(string(data))
	values := make([]float64, 0, 3)
	for _, field := range fields[:min(3, len(fields))] {
		value, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return nil
		}
		values = append(values, value)
	}
	return values
}

func applyCharacterizationTransaction(run *datasetRuntime, transaction dataset.Transaction) characterizationTransaction {
	sample := characterizationTransaction{
		Kind: transaction.Kind, Statements: len(transaction.Statements), EstimatedRecords: transaction.Records,
	}
	if len(transaction.Statements) != 0 {
		sample.CommitNS = int64(run.applyTransaction(transaction.Statements))
	}
	run.applyAssignments(transaction.Grants, transaction.Revokes)
	sample.VisibleNS = int64(run.waitMaterialized(5 * time.Minute))
	return sample
}

// characterizationConnect connects a fresh client, rebuilds every assigned
// scope, and requires the exact expected state before it records timings.
func characterizationConnect(run *datasetRuntime, user, clientID string, expected expectedState, samples []characterizationRebuild) (*datasetClient, []characterizationRebuild) {
	run.t.Helper()
	client := &datasetClient{
		User: user, Token: run.token(user), ID: clientID, Cursors: map[string]*string{},
		Rows: map[string]map[string]map[string]json.RawMessage{}, Versions: map[string]string{},
	}
	var response datasetConnectResponse
	run.sync(client.Token, "/sync/connect", map[string]any{
		"client_id": clientID, "platform": "conformance", "app_version": "0.3.0", "protocol_version": 3,
		"schema": map[string]any{"version": 0, "hash": ""}, "scope_set_version": 0, "known_scopes": map[string]any{},
	}, &response)
	for _, added := range response.Scopes.Add {
		if added.Cursor != nil {
			run.t.Fatalf("fresh %s scope %s has a cursor", user, added.ID)
		}
	}
	// Apply the manifest without the connect-time rebuild, then time each scope.
	adds := response.Scopes.Add
	response.Scopes.Add = nil
	run.applyConnect(client, response)
	for _, added := range adds {
		timings := run.rebuild(client, added.ID)
		samples = append(samples, characterizationRebuild{
			User: user, Scope: added.ID, Records: timings.Records, Pages: timings.Pages, Bytes: timings.Bytes,
			FirstPageNS: int64(timings.FirstPage), TotalNS: int64(timings.Total),
		})
	}
	if difference := client.mismatch(expected, run.assignedScopes(user)); difference != "" {
		run.t.Fatalf("characterization rebuild is not exact: %s", difference)
	}
	return client, samples
}

func characterizationPushSets(run *datasetRuntime, client *datasetClient, sets []string, seed uint64) characterizationPush {
	run.t.Helper()
	random := dataset.NewRandom(seed ^ uint64(len(client.User)))
	mutations := make([]map[string]any, 0, len(sets))
	for _, id := range sets {
		version := client.Versions["exercise_sets/"+id]
		if version == "" {
			run.t.Fatalf("characterization client %s has no version for set %s", client.User, id)
		}
		// The wire decimal has no insignificant zeros.
		weight := strings.TrimSuffix(strings.TrimRight(random.Weight(), "0"), ".")
		weightJSON, _ := json.Marshal(weight)
		noteJSON, _ := json.Marshal(random.Paragraph(0, 80))
		mutations = append(mutations, client.mutation(run.t, "exercise_sets", id, "update", version, map[string]string{
			"reps": strconv.Itoa(random.IntN(20)), "weight_kg": string(weightJSON), "note": string(noteJSON),
		}))
	}
	accepted, rejected, elapsed := run.push(client, mutations)
	if len(rejected) != 0 {
		run.t.Fatalf("characterization push for %s rejected %d mutations: %+v", client.User, len(rejected), rejected[0])
	}
	sample := characterizationPush{User: client.User, Mutations: len(mutations), HTTPNS: int64(elapsed)}
	for _, outcome := range accepted {
		if outcome.Status == "applied" {
			sample.Applied++
		}
	}
	if sample.Applied != len(mutations) {
		run.t.Fatalf("characterization push for %s applied %d of %d", client.User, sample.Applied, len(mutations))
	}
	sample.VisibleNS = int64(run.waitMaterialized(5 * time.Minute))
	started := time.Now()
	sample.OwnerChanges = run.pull(client)
	sample.OwnerPullNS = int64(time.Since(started))
	return sample
}

func characterizationPullRequest(client *datasetClient) map[string]any {
	scopes := make(map[string]any, len(client.Cursors))
	for scope, cursor := range client.Cursors {
		scopes[scope] = map[string]any{"cursor": cursor}
	}
	return map[string]any{
		"client_id": client.ID, "client_generation": client.Generation, "schema": json.RawMessage(client.Schema),
		"scope_set_version": client.ScopeSetVersion, "scopes": scopes, "limit": 1000,
	}
}

// characterizationCatalogExport exports the portable catalog scope in one
// read-only snapshot and requires the exact expected catalog rows.
func characterizationCatalogExport(run *datasetRuntime, client *datasetClient, expected expectedState) characterizationExport {
	run.t.Helper()
	const pageLimit = 500
	sample := characterizationExport{Scope: dataset.CatalogScope, PageLimit: pageLimit}
	connection, err := run.admin.Conn(run.ctx)
	if err != nil {
		run.t.Fatalf("acquire export connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(run.ctx, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"); err != nil {
		run.t.Fatalf("begin export: %v", err)
	}
	defer connection.ExecContext(context.Background(), "ROLLBACK")
	started := time.Now()
	var raw []byte
	if err := connection.QueryRowContext(run.ctx, "SELECT synchro.synchro_portable_seed_manifest($1)::text", pageLimit).Scan(&raw); err != nil {
		run.t.Fatalf("export manifest: %v", err)
	}
	sample.ManifestNS = int64(time.Since(started))
	var manifest struct {
		PortableScopes []struct {
			ID           string `json:"id"`
			Cardinality  int    `json:"cardinality"`
			Continuation string `json:"continuation"`
			PageToken    string `json:"page_token"`
		} `json:"portable_scopes"`
	}
	if err := decodeDataset(raw, &manifest); err != nil || len(manifest.PortableScopes) != 1 || manifest.PortableScopes[0].ID != dataset.CatalogScope {
		run.t.Fatalf("export manifest is not the catalog scope: %v %.300s", err, raw)
	}
	scope := manifest.PortableScopes[0]
	token := scope.PageToken
	exported := map[string]map[string]map[string]json.RawMessage{}
	exporter := &datasetClient{User: client.User, ByID: client.ByID, Tables: client.Tables, Rows: exported, Versions: map[string]string{}}
	for ordinal := 0; ; {
		var page []byte
		if err := connection.QueryRowContext(run.ctx,
			"SELECT synchro.synchro_portable_seed_scope($1, $2, $3, $4, $5)::text",
			scope.ID, token, scope.Continuation, ordinal, pageLimit,
		).Scan(&page); err != nil {
			run.t.Fatalf("export catalog page: %v", err)
		}
		if sample.Pages == 0 {
			sample.FirstPageNS = int64(time.Since(started))
		}
		sample.Pages++
		var response struct {
			Records   []datasetRecord `json:"records"`
			PageToken *string         `json:"page_token"`
			HasMore   bool            `json:"has_more"`
		}
		if err := decodeDataset(page, &response); err != nil {
			run.t.Fatalf("decode export page: %v %.300s", err, page)
		}
		for _, record := range response.Records {
			record.Op = "upsert"
			exporter.store(scope.ID, record, run.t)
		}
		ordinal += len(response.Records)
		if !response.HasMore {
			break
		}
		if response.PageToken == nil {
			run.t.Fatal("export page continuation is missing")
		}
		token = *response.PageToken
	}
	sample.TotalNS = int64(time.Since(started))
	sample.Records = len(exported[scope.ID])
	if difference := expected.scopeMismatch(scope.ID, exported[scope.ID]); difference != "" || sample.Records != scope.Cardinality {
		run.t.Fatalf("catalog export is not exact: records=%d cardinality=%d %s", sample.Records, scope.Cardinality, difference)
	}
	return sample
}

func characterizationRelations(run *datasetRuntime) (map[string]int64, map[string]int64) {
	run.t.Helper()
	sizes, counts := map[string]int64{}, map[string]int64{}
	for _, name := range []string{
		"sync_bucket_edges", "sync_captured_projections", "sync_captured_rows", "sync_changelog",
		"sync_row_versions", "sync_wal_events", "sync_wal_transactions", "sync_write_fences",
	} {
		var size, count int64
		if err := run.admin.QueryRowContext(run.ctx, fmt.Sprintf(
			"SELECT pg_total_relation_size('synchro.%s'), (SELECT count(*) FROM synchro.%s)", name, name,
		)).Scan(&size, &count); err != nil {
			run.t.Fatalf("observe synchro relation %s: %v", name, err)
		}
		sizes[name], counts[name] = size, count
	}
	return sizes, counts
}
