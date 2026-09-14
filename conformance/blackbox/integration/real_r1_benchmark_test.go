//go:build r1benchmark

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const (
	r1BenchmarkFormat          = "synchro-r1-benchmark-v1"
	r1BenchmarkModeRecord      = "record"
	r1BenchmarkModeCompare     = "compare"
	r1BenchmarkWarmupSamples   = 2
	r1BenchmarkMeasuredSamples = 10
	r1BenchmarkPullRows        = 1000
	r1BenchmarkChangedRows     = 100
	r1BenchmarkPushMutations   = 100
	r1BenchmarkWALRows         = 100
	r1BenchmarkAckTransactions = 100
	r1BenchmarkAckRows         = 100
	r1BenchmarkWorkerRSSRows   = 1000
	r1BenchmarkTotalSourceRows = r1BenchmarkPullRows + r1BenchmarkPushMutations + r1BenchmarkWALRows +
		r1BenchmarkAckRows + r1BenchmarkWorkerRSSRows
	r1BenchmarkMaximumBytes    = 1 << 20
	r1BenchmarkMaximumRSSBytes = int64(1 << 50)

	r1PullGroup         = 9100
	r1PushGroup         = 9300
	r1PushMutationGroup = 9400
	r1PushBatchGroup    = 9500
	r1WALGroup          = 9600
	r1AckGroup          = 9700
	r1WorkerRSSGroup    = 9800
)

const r1InsertRowsStatement = `
	INSERT INTO cf_items (id, owner_id, value)
	SELECT ('00000000-0000-4000-' || lpad(($1::integer)::text, 4, '0') || '-' || lpad(value::text, 12, '0'))::uuid,
	       'diagnostic-user',
	       $2::text || '-' || lpad(value::text, 4, '0')
	FROM generate_series(1, $3::integer) value`

const r1UpdateRowsStatement = `
	UPDATE cf_items AS item
	SET value = $2::text || '-' || lpad(source.value::text, 4, '0'),
	    updated_at = clock_timestamp()
	FROM generate_series(1, $3::integer) source(value)
	WHERE item.id = ('00000000-0000-4000-' || lpad(($1::integer)::text, 4, '0') || '-' || lpad(source.value::text, 12, '0'))::uuid`

const r1UpdateOneRowStatement = `
	UPDATE cf_items
	SET value = $2::text || '-' || lpad(($3::integer)::text, 4, '0'),
	    updated_at = clock_timestamp()
	WHERE id = ('00000000-0000-4000-' || lpad(($1::integer)::text, 4, '0') || '-' || lpad(($3::integer)::text, 12, '0'))::uuid`

var (
	r1RevisionPattern         = regexp.MustCompile(`^[0-9a-f]{40}$`)
	r1SHA256Pattern           = regexp.MustCompile(`^[0-9a-f]{64}$`)
	r1HardwareIdentityPattern = regexp.MustCompile(`^[A-Fa-f0-9-]{16,64}$`)
	r1DarwinUUIDPattern       = regexp.MustCompile(`"IOPlatformUUID" = "([A-Fa-f0-9-]+)"`)
	r1GoIdentityPattern       = regexp.MustCompile(`^[a-z0-9_]{2,32}$`)
)

// No old fixture qualifies for deletion because R1 is the first executable benchmark gate.
type r1BenchmarkResult struct {
	Format           string                 `json:"format"`
	Revision         string                 `json:"revision"`
	DefinitionSHA256 string                 `json:"definition_sha256"`
	Environment      r1BenchmarkEnvironment `json:"environment"`
	Workload         r1BenchmarkWorkload    `json:"workload"`
	Metrics          r1BenchmarkMetrics     `json:"metrics"`
}

type r1BenchmarkEnvironment struct {
	GOOS                      string `json:"goos"`
	GOARCH                    string `json:"goarch"`
	LogicalCPUs               int    `json:"logical_cpu_count"`
	HardwareFingerprintSHA256 string `json:"hardware_fingerprint_sha256"`
	AdapterSHA256             string `json:"adapter_sha256"`
	ExtensionManifestSHA256   string `json:"extension_manifest_sha256"`
}

type r1BenchmarkWorkload struct {
	WarmupSamples               int `json:"warmup_samples"`
	MeasuredSamples             int `json:"measured_samples"`
	TotalSourceRows             int `json:"total_source_rows"`
	PullFixtureRows             int `json:"pull_fixture_rows"`
	ChangedPullRows             int `json:"changed_pull_rows"`
	PushExistingRows            int `json:"push_existing_rows"`
	PushBatchMutations          int `json:"push_batch_mutations"`
	WALObserverRows             int `json:"wal_observer_rows"`
	WALTransactionRows          int `json:"wal_transaction_rows"`
	AcknowledgementRows         int `json:"acknowledgement_rows"`
	AcknowledgementTransactions int `json:"acknowledgement_transactions"`
	WorkerRSSRows               int `json:"worker_rss_rows"`
}

type r1BenchmarkMetrics struct {
	TerminalPull         r1LatencyMetric     `json:"terminal_steady_state_pull_latency_ns"`
	ChangedPull          r1LatencyMetric     `json:"changed_pull_latency_ns"`
	Push                 r1LatencyMetric     `json:"push_latency_ns"`
	WALObserverDetection r1LatencyMetric     `json:"wal_commit_to_acknowledgement_observer_detection_latency_ns"`
	TransactionAck       r1LatencyMetric     `json:"one_row_transaction_batch_to_final_contiguous_acknowledgement_observer_detection_latency_ns"`
	WorkerObservedRSS    r1ObservedRSSMetric `json:"wal_worker_maximum_observed_rss_bytes"`
	PushThroughputMilli  int64               `json:"push_throughput_milli_mutations_per_second"`
}

type r1LatencyMetric struct {
	Samples  []int64 `json:"samples"`
	MedianNS int64   `json:"median"`
}

type r1ObservedRSSMetric struct {
	Samples      []int64 `json:"samples"`
	MaximumBytes int64   `json:"maximum"`
}

type r1BenchmarkConfig struct {
	Mode         string
	Revision     string
	ResultPath   string
	BaselinePath string
}

type r1SourceFixture struct {
	Group  int
	Prefix string
	Rows   int
}

var r1BenchmarkWorkloadDefinition = r1BenchmarkWorkload{
	WarmupSamples:               r1BenchmarkWarmupSamples,
	MeasuredSamples:             r1BenchmarkMeasuredSamples,
	TotalSourceRows:             r1BenchmarkTotalSourceRows,
	PullFixtureRows:             r1BenchmarkPullRows,
	ChangedPullRows:             r1BenchmarkChangedRows,
	PushExistingRows:            r1BenchmarkPushMutations,
	PushBatchMutations:          r1BenchmarkPushMutations,
	WALObserverRows:             r1BenchmarkWALRows,
	WALTransactionRows:          r1BenchmarkWALRows,
	AcknowledgementRows:         r1BenchmarkAckRows,
	AcknowledgementTransactions: r1BenchmarkAckTransactions,
	WorkerRSSRows:               r1BenchmarkWorkerRSSRows,
}

var r1SourceFixtures = []r1SourceFixture{
	{Group: r1PullGroup, Prefix: "r1-pull-setup", Rows: r1BenchmarkPullRows},
	{Group: r1PushGroup, Prefix: "r1-push-setup", Rows: r1BenchmarkPushMutations},
	{Group: r1WALGroup, Prefix: "r1-wal-setup", Rows: r1BenchmarkWALRows},
	{Group: r1AckGroup, Prefix: "r1-ack-setup", Rows: r1BenchmarkAckRows},
	{Group: r1WorkerRSSGroup, Prefix: "r1-rss-setup", Rows: r1BenchmarkWorkerRSSRows},
}

func TestRealR1PerformanceBenchmark(t *testing.T) {
	t.Run("assertion", runRealR1PerformanceBenchmark)
}

func runRealR1PerformanceBenchmark(t *testing.T) {
	if !*provision || !*install {
		t.Fatal("TestRealR1PerformanceBenchmark requires --provision --install")
	}
	definitionSHA256, repoRoot, err := loadR1BenchmarkDefinition()
	if err != nil {
		t.Fatalf("load R1 benchmark definition: %v", err)
	}
	config, err := loadR1BenchmarkConfig(repoRoot)
	if err != nil {
		t.Fatalf("load R1 benchmark configuration: %v", err)
	}
	environment, err := loadR1BenchmarkEnvironment()
	if err != nil {
		t.Fatalf("load R1 benchmark environment identity: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	requireR1LoopbackHTTP(t, harness.AdapterURL())

	for _, fixture := range r1SourceFixtures {
		insertR1SourceRows(t, ctx, harness, fixture.Group, fixture.Prefix, fixture.Rows)
	}
	waitForR1WALAcknowledgement(t, ctx, harness, r1RecordID(r1WorkerRSSGroup, r1BenchmarkWorkerRSSRows))

	client := connectRealProtocolClient(t, ctx, harness, token, "r1-pull-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	assigned, _ := rebuildR1Scope(
		t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-9000-000000000001",
	)
	pushVersions := requireR1SetupRows(t, assigned, table, ownerField)
	global, _ := rebuildR1Scope(
		t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-9000-000000000002",
	)
	if len(global) != 0 {
		t.Fatalf("R1 global rebuild row count = %d, want 0", len(global))
	}

	terminalPullSamples := collectR1Samples(t, func(_ int) time.Duration {
		return measureR1Pull(t, ctx, harness, token, client, table, ownerField, 0, "", 0)
	})
	changedPullSamples := collectR1Samples(t, func(sample int) time.Duration {
		prefix := fmt.Sprintf("r1-changed-%02d", sample)
		markerID := r1RecordID(r1PullGroup, r1BenchmarkChangedRows)
		priorCount := r1WALRecordCount(t, ctx, harness, markerID)
		updateR1SourceRows(t, ctx, harness, r1PullGroup, prefix, r1BenchmarkChangedRows)
		waitForR1WALRecordAdvance(t, ctx, harness, markerID, priorCount)
		return measureR1Pull(
			t, ctx, harness, token, client, table, ownerField, r1PullGroup, prefix, r1BenchmarkChangedRows,
		)
	})

	pushSamples := collectR1Samples(t, func(sample int) time.Duration {
		prefix := fmt.Sprintf("r1-push-%02d", sample)
		mutations, recordIDs := r1PushUpdateMutations(
			t,
			client,
			table,
			pushVersions,
			sample,
			prefix,
		)
		markerID := recordIDs[len(recordIDs)-1]
		priorCount := r1WALRecordCount(t, ctx, harness, markerID)
		batchID := r1RecordID(r1PushBatchGroup, sample+1)
		payload := phase4PushPayload(client, batchID, mutations)
		started := time.Now()
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", payload)
		elapsed := time.Since(started)
		pushVersions = requireR1PushOutcomes(
			t, status, response, client, table, ownerField, batchID, mutations, recordIDs, pushVersions, prefix,
		)
		waitForR1WALRecordAdvance(t, ctx, harness, markerID, priorCount)
		return elapsed
	})

	walObserverSamples := collectR1Samples(t, func(sample int) time.Duration {
		markerID := r1RecordID(r1WALGroup, r1BenchmarkWALRows)
		priorCount := r1WALRecordCount(t, ctx, harness, markerID)
		transaction, err := harness.Source().BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin R1 WAL source transaction: %v", err)
		}
		if _, err := transaction.ExecContext(
			ctx,
			r1UpdateRowsStatement,
			r1WALGroup,
			fmt.Sprintf("r1-wal-%02d", sample),
			r1BenchmarkWALRows,
		); err != nil {
			_ = transaction.Rollback()
			t.Fatalf("stage R1 WAL source transaction: %v", err)
		}
		started := time.Now()
		if err := transaction.Commit(); err != nil {
			t.Fatalf("commit R1 WAL source transaction: %v", err)
		}
		waitForR1WALRecordAdvance(t, ctx, harness, markerID, priorCount)
		elapsed := time.Since(started)
		requireR1WALUpdateTransaction(t, ctx, harness, r1WALGroup, r1BenchmarkWALRows, priorCount+1)
		return elapsed
	})

	transactionAckSamples := collectR1Samples(t, func(sample int) time.Duration {
		markerID := r1RecordID(r1AckGroup, r1BenchmarkAckRows)
		priorCount := r1WALRecordCount(t, ctx, harness, markerID)
		started := time.Now()
		for index := 1; index <= r1BenchmarkAckTransactions; index++ {
			if err := harness.Source().ExecContext(
				ctx,
				r1UpdateOneRowStatement,
				r1AckGroup,
				fmt.Sprintf("r1-ack-%02d", sample),
				index,
			); err != nil {
				t.Fatalf("commit R1 one-row source transaction: %v", err)
			}
		}
		waitForR1WALRecordAdvance(t, ctx, harness, markerID, priorCount)
		elapsed := time.Since(started)
		requireR1OneRowTransactions(t, ctx, harness, sample+2)
		return elapsed
	})

	workerPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
	if err != nil {
		t.Fatalf("observe R1 WAL worker process: %v", err)
	}
	workerObservedRSSSamples := make([]int64, 0, r1BenchmarkMeasuredSamples)
	for sample := 0; sample < r1BenchmarkWarmupSamples+r1BenchmarkMeasuredSamples; sample++ {
		maximumObserved := measureR1WorkerObservedRSS(t, ctx, harness, workerPID, sample)
		if sample >= r1BenchmarkWarmupSamples {
			workerObservedRSSSamples = append(workerObservedRSSSamples, maximumObserved)
		}
	}
	cardinalityClient := connectRealProtocolClient(t, ctx, harness, token, "r1-cardinality-client")
	cardinalityRows, _ := rebuildR1Scope(
		t,
		ctx,
		harness,
		token,
		cardinalityClient,
		"user:diagnostic-user",
		"00000000-0000-4000-9000-000000000003",
	)
	if len(cardinalityRows) != r1BenchmarkTotalSourceRows {
		t.Fatalf("R1 final source row count = %d, want %d", len(cardinalityRows), r1BenchmarkTotalSourceRows)
	}

	result := r1BenchmarkResult{
		Format:           r1BenchmarkFormat,
		Revision:         config.Revision,
		DefinitionSHA256: definitionSHA256,
		Environment:      environment,
		Workload:         r1BenchmarkWorkloadDefinition,
		Metrics: r1BenchmarkMetrics{
			TerminalPull:         newR1LatencyMetric(terminalPullSamples),
			ChangedPull:          newR1LatencyMetric(changedPullSamples),
			Push:                 newR1LatencyMetric(pushSamples),
			WALObserverDetection: newR1LatencyMetric(walObserverSamples),
			TransactionAck:       newR1LatencyMetric(transactionAckSamples),
			WorkerObservedRSS:    newR1ObservedRSSMetric(workerObservedRSSSamples),
			PushThroughputMilli:  r1PushThroughputMilli(pushSamples),
		},
	}
	if err := writeR1BenchmarkResult(config.ResultPath, result); err != nil {
		t.Fatalf("write R1 benchmark result: %v", err)
	}
	if config.Mode == r1BenchmarkModeCompare {
		baseline, err := readR1BenchmarkResult(config.BaselinePath)
		if err != nil {
			t.Fatalf("read R1 benchmark baseline: %v", err)
		}
		if err := compareR1BenchmarkResults(result, baseline); err != nil {
			t.Fatalf("compare R1 benchmark result: %v", err)
		}
	}
}

func loadR1BenchmarkConfig(repoRoot string) (r1BenchmarkConfig, error) {
	config := r1BenchmarkConfig{
		Mode:         os.Getenv("R1_BENCHMARK_MODE"),
		Revision:     os.Getenv("R1_BENCHMARK_REVISION"),
		ResultPath:   os.Getenv("R1_BENCHMARK_RESULT"),
		BaselinePath: filepath.Join(repoRoot, "conformance", "blackbox", "integration", "testdata", "r1-benchmark-baseline.json"),
	}
	if config.Mode != r1BenchmarkModeRecord && config.Mode != r1BenchmarkModeCompare {
		return r1BenchmarkConfig{}, fmt.Errorf("R1_BENCHMARK_MODE is invalid")
	}
	if !r1RevisionPattern.MatchString(config.Revision) {
		return r1BenchmarkConfig{}, fmt.Errorf("R1_BENCHMARK_REVISION is invalid")
	}
	safeResultPath, err := safeR1BenchmarkResultPath(repoRoot, config.ResultPath)
	if err != nil {
		return r1BenchmarkConfig{}, err
	}
	config.ResultPath = safeResultPath
	if config.Mode == r1BenchmarkModeCompare {
		if config.ResultPath == config.BaselinePath {
			return r1BenchmarkConfig{}, fmt.Errorf("R1 benchmark result must not replace the baseline")
		}
	}
	return config, nil
}

func loadR1BenchmarkEnvironment() (r1BenchmarkEnvironment, error) {
	hardwareFingerprint, err := loadR1HardwareFingerprint()
	if err != nil {
		return r1BenchmarkEnvironment{}, err
	}
	adapterSHA256, err := readR1ArtifactDigest(os.Getenv("SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT") + ".sha256")
	if err != nil {
		return r1BenchmarkEnvironment{}, fmt.Errorf("read adapter artifact identity: %w", err)
	}
	extensionManifestSHA256, err := readR1ArtifactDigest(filepath.Join(
		os.Getenv("SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT"),
		"artifact-manifest.json.sha256",
	))
	if err != nil {
		return r1BenchmarkEnvironment{}, fmt.Errorf("read extension artifact identity: %w", err)
	}
	return r1BenchmarkEnvironment{
		GOOS:                      runtime.GOOS,
		GOARCH:                    runtime.GOARCH,
		LogicalCPUs:               runtime.NumCPU(),
		HardwareFingerprintSHA256: hardwareFingerprint,
		AdapterSHA256:             adapterSHA256,
		ExtensionManifestSHA256:   extensionManifestSHA256,
	}, nil
}

func loadR1HardwareFingerprint() (string, error) {
	var identity string
	switch runtime.GOOS {
	case "darwin":
		output, err := exec.Command("ioreg", "-rd1", "-c", "IOPlatformExpertDevice").Output()
		if err != nil {
			return "", fmt.Errorf("read Darwin hardware identity: %w", err)
		}
		match := r1DarwinUUIDPattern.FindSubmatch(output)
		if len(match) != 2 {
			return "", fmt.Errorf("Darwin hardware identity is unavailable")
		}
		identity = string(match[1])
	case "linux":
		data, err := os.ReadFile("/sys/class/dmi/id/product_uuid")
		if err != nil {
			return "", fmt.Errorf("read Linux hardware identity: %w", err)
		}
		identity = strings.TrimSpace(string(data))
	default:
		return "", fmt.Errorf("R1 benchmark hardware identity is unsupported on %s", runtime.GOOS)
	}
	if !r1HardwareIdentityPattern.MatchString(identity) {
		return "", fmt.Errorf("R1 benchmark hardware identity is invalid")
	}
	digest := sha256.Sum256([]byte(runtime.GOOS + "\x00" + identity))
	return hex.EncodeToString(digest[:]), nil
}

func readR1ArtifactDigest(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 || !r1SHA256Pattern.MatchString(fields[0]) {
		return "", fmt.Errorf("artifact digest is invalid")
	}
	return fields[0], nil
}

func loadR1BenchmarkDefinition() (string, string, error) {
	_, sourcePath, _, ok := runtime.Caller(0)
	if !ok {
		return "", "", fmt.Errorf("benchmark definition source is unavailable")
	}
	absoluteSource, err := filepath.Abs(sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("resolve benchmark definition source: %w", err)
	}
	resolvedSource, err := filepath.EvalSymlinks(absoluteSource)
	if err != nil {
		return "", "", fmt.Errorf("resolve benchmark definition links: %w", err)
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(resolvedSource), "..", "..", ".."))
	expectedSource := filepath.Join(repoRoot, "conformance", "blackbox", "integration", "real_r1_benchmark_test.go")
	if resolvedSource != expectedSource {
		return "", "", fmt.Errorf("benchmark definition source path is invalid")
	}
	info, err := os.Lstat(resolvedSource)
	if err != nil {
		return "", "", fmt.Errorf("stat benchmark definition: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > r1BenchmarkMaximumBytes {
		return "", "", fmt.Errorf("benchmark definition file is invalid")
	}
	data, err := os.ReadFile(resolvedSource)
	if err != nil {
		return "", "", fmt.Errorf("read benchmark definition: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), repoRoot, nil
}

func safeR1BenchmarkResultPath(repoRoot, resultPath string) (string, error) {
	if strings.TrimSpace(resultPath) == "" {
		return "", fmt.Errorf("R1_BENCHMARK_RESULT is required")
	}
	absoluteRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w", err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return "", fmt.Errorf("resolve repository root links: %w", err)
	}
	absoluteResult, err := filepath.Abs(resultPath)
	if err != nil {
		return "", fmt.Errorf("resolve R1_BENCHMARK_RESULT: %w", err)
	}
	if info, statErr := os.Lstat(absoluteResult); statErr == nil {
		if info.Mode()&os.ModeSymlink != 0 || info.IsDir() {
			return "", fmt.Errorf("R1_BENCHMARK_RESULT must be a regular output path")
		}
	} else if !os.IsNotExist(statErr) {
		return "", fmt.Errorf("inspect R1_BENCHMARK_RESULT: %w", statErr)
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(absoluteResult))
	if err != nil {
		return "", fmt.Errorf("R1_BENCHMARK_RESULT directory must exist")
	}
	info, err := os.Stat(resolvedParent)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("R1_BENCHMARK_RESULT directory is invalid")
	}
	resolvedResult := filepath.Join(resolvedParent, filepath.Base(absoluteResult))
	if r1PathIsWithin(resolvedRoot, resolvedResult) {
		return "", fmt.Errorf("R1_BENCHMARK_RESULT must be outside the repository")
	}
	return resolvedResult, nil
}

func r1PathIsWithin(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil || filepath.IsAbs(relative) {
		return false
	}
	return relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func requireR1LoopbackHTTP(t *testing.T, adapterURL string) {
	t.Helper()
	endpoint, err := url.Parse(adapterURL)
	if err != nil || endpoint.Scheme != "http" || endpoint.Port() == "" {
		t.Fatal("R1 benchmark adapter endpoint is not loopback HTTP")
	}
	address := net.ParseIP(endpoint.Hostname())
	if address == nil || !address.IsLoopback() {
		t.Fatal("R1 benchmark adapter endpoint is not loopback HTTP")
	}
}

func rebuildR1Scope(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	token string,
	client *realProtocolClient,
	scopeID string,
	rebuildID string,
) ([]map[string]any, string) {
	t.Helper()
	var cursor any
	var records []map[string]any
	for page := 0; page < 4; page++ {
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/rebuild", map[string]any{
			"client_id":         client.ID,
			"client_generation": client.Generation,
			"schema":            client.Schema,
			"scope":             scopeID,
			"rebuild_id":        rebuildID,
			"cursor":            cursor,
			"limit":             r1BenchmarkPullRows,
		})
		if status != 200 || response["scope"] != scopeID {
			t.Fatalf("R1 scope rebuild status = %d or scope is invalid", status)
		}
		pageRecords := r1ResponseObjects(t, response["records"], "R1 rebuild records")
		records = append(records, pageRecords...)
		hasMore, ok := response["has_more"].(bool)
		if !ok {
			t.Fatal("R1 scope rebuild finality is invalid")
		}
		if hasMore {
			nextCursor, ok := response["cursor"].(string)
			if !ok || nextCursor == "" {
				t.Fatal("R1 scope rebuild continuation is invalid")
			}
			cursor = nextCursor
			continue
		}
		finalCursor, ok := response["final_scope_cursor"].(string)
		if !ok || finalCursor == "" {
			t.Fatal("R1 scope rebuild final cursor is invalid")
		}
		client.Scopes[scopeID] = map[string]any{"cursor": finalCursor}
		return records, finalCursor
	}
	t.Fatal("R1 scope rebuild exceeded its page bound")
	return nil, ""
}

func requireR1SetupRows(
	t *testing.T,
	rows []map[string]any,
	table realProtocolTable,
	ownerField string,
) map[string]string {
	t.Helper()
	expected := make(map[string]string, r1BenchmarkTotalSourceRows)
	for _, fixture := range r1SourceFixtures {
		for index := 1; index <= fixture.Rows; index++ {
			expected[r1RecordID(fixture.Group, index)] = fmt.Sprintf("%s-%04d", fixture.Prefix, index)
		}
	}
	if len(expected) != r1BenchmarkTotalSourceRows || len(rows) != r1BenchmarkTotalSourceRows {
		t.Fatalf("R1 setup row count = %d, want %d", len(rows), r1BenchmarkTotalSourceRows)
	}
	pushVersions := make(map[string]string, r1BenchmarkPushMutations)
	seen := make(map[string]struct{}, len(expected))
	for _, rowObject := range rows {
		if rowObject["table"] != table.ID {
			t.Fatal("R1 setup row used an unexpected logical table")
		}
		primaryKey, ok := rowObject["pk"].(map[string]any)
		recordID, idOK := primaryKey[table.PrimaryKeyField].(string)
		expectedValue, expectedRow := expected[recordID]
		row, rowOK := rowObject["row"].(map[string]any)
		version, versionOK := rowObject["server_version"].(string)
		if !ok || !idOK || len(primaryKey) != 1 || !expectedRow || !rowOK ||
			row[ownerField] != "diagnostic-user" || row[table.ValueField] != expectedValue ||
			!versionOK || !uuidPattern.MatchString(version) {
			t.Fatal("R1 setup row state is invalid")
		}
		if _, duplicate := seen[recordID]; duplicate {
			t.Fatal("R1 setup returned a duplicate row")
		}
		seen[recordID] = struct{}{}
		if strings.Contains(recordID, fmt.Sprintf("-%04d-", r1PushGroup)) {
			pushVersions[recordID] = version
		}
	}
	if len(seen) != len(expected) || len(pushVersions) != r1BenchmarkPushMutations {
		t.Fatal("R1 setup row cardinality is invalid")
	}
	return pushVersions
}

func insertR1SourceRows(t *testing.T, ctx context.Context, harness *blackbox.Harness, group int, prefix string, count int) {
	t.Helper()
	if err := harness.Source().ExecContext(ctx, r1InsertRowsStatement, group, prefix, count); err != nil {
		t.Fatalf("insert R1 source rows: %v", err)
	}
}

func updateR1SourceRows(t *testing.T, ctx context.Context, harness *blackbox.Harness, group int, prefix string, count int) {
	t.Helper()
	if err := harness.Source().ExecContext(ctx, r1UpdateRowsStatement, group, prefix, count); err != nil {
		t.Fatalf("update R1 source rows: %v", err)
	}
}

func measureR1Pull(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	token string,
	client *realProtocolClient,
	table realProtocolTable,
	ownerField string,
	group int,
	prefix string,
	rowCount int,
) time.Duration {
	t.Helper()
	payload := realPullPayload(client, client.Scopes, r1BenchmarkPullRows)
	started := time.Now()
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", payload)
	elapsed := time.Since(started)
	if status != 200 {
		t.Fatalf("R1 pull status = %d, want 200", status)
	}
	rebuild, ok := response["rebuild"].([]any)
	if !ok || len(rebuild) != 0 || response["has_more"] != false {
		t.Fatal("R1 pull did not return one terminal incremental page")
	}
	changes := r1ResponseObjects(t, response["changes"], "R1 pull changes")
	requireR1Rows(t, changes, table, ownerField, group, prefix, rowCount, true)
	cursors, ok := response["scope_cursors"].(map[string]any)
	if !ok || len(cursors) != 2 {
		t.Fatal("R1 pull did not return exactly two scope cursors")
	}
	for _, scopeID := range []string{"cf:global", "user:diagnostic-user"} {
		cursor, ok := cursors[scopeID].(string)
		if !ok || cursor == "" {
			t.Fatal("R1 pull returned an invalid scope cursor")
		}
		client.Scopes[scopeID] = map[string]any{"cursor": cursor}
	}
	checksums, ok := response["checksums"].(map[string]any)
	if !ok || len(checksums) != 2 {
		t.Fatal("R1 pull did not return exactly two terminal checksums")
	}
	version, ok := response["scope_set_version"].(float64)
	if !ok || int64(version) != client.ScopeSetVersion {
		t.Fatal("R1 pull changed the scope-set version")
	}
	return elapsed
}

func requireR1Rows(
	t *testing.T,
	rows []map[string]any,
	table realProtocolTable,
	ownerField string,
	group int,
	prefix string,
	rowCount int,
	requireScope bool,
) {
	t.Helper()
	if len(rows) != rowCount {
		t.Fatalf("R1 row count = %d, want %d", len(rows), rowCount)
	}
	expected := make(map[string]string, rowCount)
	for index := 1; index <= rowCount; index++ {
		expected[r1RecordID(group, index)] = fmt.Sprintf("%s-%04d", prefix, index)
	}
	seen := make(map[string]struct{}, rowCount)
	for _, rowObject := range rows {
		if rowObject["table"] != table.ID {
			t.Fatal("R1 row used an unexpected logical table")
		}
		if requireScope && rowObject["scope"] != "user:diagnostic-user" {
			t.Fatal("R1 changed row used an unexpected scope")
		}
		primaryKey, ok := rowObject["pk"].(map[string]any)
		recordID, idOK := primaryKey[table.PrimaryKeyField].(string)
		value, expectedRow := expected[recordID]
		if !ok || !idOK || len(primaryKey) != 1 || !expectedRow {
			t.Fatal("R1 row identity is invalid")
		}
		if _, duplicate := seen[recordID]; duplicate {
			t.Fatal("R1 response returned a duplicate row")
		}
		row, ok := rowObject["row"].(map[string]any)
		version, versionOK := rowObject["server_version"].(string)
		if !ok || row[ownerField] != "diagnostic-user" || row[table.ValueField] != value ||
			!versionOK || !uuidPattern.MatchString(version) {
			t.Fatal("R1 row state is invalid")
		}
		seen[recordID] = struct{}{}
	}
}

func r1PushUpdateMutations(
	t *testing.T,
	client *realProtocolClient,
	table realProtocolTable,
	baseVersions map[string]string,
	sample int,
	prefix string,
) ([]map[string]any, []string) {
	t.Helper()
	mutations := make([]map[string]any, 0, r1BenchmarkPushMutations)
	recordIDs := make([]string, 0, r1BenchmarkPushMutations)
	for index := 1; index <= r1BenchmarkPushMutations; index++ {
		recordID := r1RecordID(r1PushGroup, index)
		baseVersion := baseVersions[recordID]
		if !uuidPattern.MatchString(baseVersion) {
			t.Fatal("R1 push base version is invalid")
		}
		recordIDs = append(recordIDs, recordID)
		mutations = append(mutations, map[string]any{
			"mutation_id":     r1RecordID(r1PushMutationGroup, sample*1000+index),
			"table":           table.ID,
			"pk":              map[string]any{table.PrimaryKeyField: recordID},
			"authored_schema": client.Schema,
			"op":              "update",
			"base_version":    baseVersion,
			"client_version":  fmt.Sprintf("2032-01-02T03:04:05.%06dZ", sample),
			"columns":         map[string]any{table.ValueField: fmt.Sprintf("%s-%04d", prefix, index)},
		})
	}
	return mutations, recordIDs
}

func requireR1PushOutcomes(
	t *testing.T,
	status int,
	response map[string]any,
	client *realProtocolClient,
	table realProtocolTable,
	ownerField string,
	batchID string,
	mutations []map[string]any,
	recordIDs []string,
	baseVersions map[string]string,
	prefix string,
) map[string]string {
	t.Helper()
	if status != 200 || response["batch_id"] != batchID {
		t.Fatalf("R1 push status = %d or batch identity is invalid", status)
	}
	accepted := requireOutcomeList(t, response, "accepted")
	rejected := requireOutcomeList(t, response, "rejected")
	if len(accepted) != r1BenchmarkPushMutations || len(rejected) != 0 {
		t.Fatalf("R1 push outcomes = %d accepted and %d rejected", len(accepted), len(rejected))
	}
	nextVersions := make(map[string]string, len(accepted))
	for index, outcome := range accepted {
		mutationID, _ := mutations[index]["mutation_id"].(string)
		if outcome["mutation_id"] != mutationID || outcome["status"] != "applied" {
			t.Fatalf("R1 push outcome %d is invalid", index)
		}
		assertCanonicalPhase4Outcome(
			t,
			outcome,
			client,
			table,
			ownerField,
			mutationID,
			recordIDs[index],
			fmt.Sprintf("%s-%04d", prefix, index+1),
			"applied",
			"",
		)
		serverVersion, _ := outcome["server_version"].(string)
		if serverVersion == baseVersions[recordIDs[index]] {
			t.Fatal("R1 push did not advance an exact base version")
		}
		nextVersions[recordIDs[index]] = serverVersion
	}
	if len(nextVersions) != r1BenchmarkPushMutations {
		t.Fatal("R1 push version cardinality is invalid")
	}
	return nextVersions
}

func waitForR1WALAcknowledgement(t *testing.T, ctx context.Context, harness *blackbox.Harness, recordID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		observation, err := harness.Operator().ObserveWALRecords(ctx, []string{recordID})
		if err == nil && len(observation.Records) == 1 && observation.WorkerRunning &&
			!observation.BlockingPoison && observation.ContiguousAcknowledged &&
			observation.AcknowledgementMatchesObservedEnd && observation.SlotMatchesObservedEnd {
			return
		}
		if err == nil && observation.BlockingPoison {
			t.Fatal("R1 WAL worker entered a blocking poison state")
		}
		select {
		case <-ctx.Done():
			t.Fatal("R1 WAL acknowledgement context expired")
		case <-time.After(time.Millisecond):
		}
	}
	t.Fatal("R1 WAL transaction did not materialize and acknowledge")
}

func r1WALRecordCount(t *testing.T, ctx context.Context, harness *blackbox.Harness, recordID string) int {
	t.Helper()
	observation, err := harness.Operator().ObserveWALRecords(ctx, []string{recordID})
	if err != nil {
		t.Fatalf("observe R1 WAL record count: %v", err)
	}
	if len(observation.Records) == 0 || !observation.WorkerRunning || observation.BlockingPoison ||
		!observation.ContiguousAcknowledged || observation.AcknowledgedEndLSN == "" ||
		observation.AcknowledgedEndLSN != observation.SlotConfirmedFlushLSN {
		t.Fatal("R1 WAL record count observation is incomplete")
	}
	return len(observation.Records)
}

func waitForR1WALRecordAdvance(t *testing.T, ctx context.Context, harness *blackbox.Harness, recordID string, priorCount int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		observation, err := harness.Operator().ObserveWALRecords(ctx, []string{recordID})
		if err == nil && len(observation.Records) == priorCount+1 && observation.WorkerRunning &&
			!observation.BlockingPoison && observation.ContiguousAcknowledged &&
			observation.AcknowledgementMatchesObservedEnd && observation.SlotMatchesObservedEnd {
			return
		}
		if err == nil && len(observation.Records) > priorCount+1 {
			t.Fatal("R1 changed-pull WAL marker advanced more than once")
		}
		select {
		case <-ctx.Done():
			t.Fatal("R1 changed-pull WAL context expired")
		case <-time.After(time.Millisecond):
		}
	}
	t.Fatal("R1 changed-pull WAL marker did not advance")
}

func requireR1WALUpdateTransaction(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	group int,
	rowCount int,
	expectedHistory int,
) {
	t.Helper()
	seenRecords := make(map[string]struct{}, rowCount)
	seenOrdinals := make(map[int64]struct{}, rowCount)
	var commitLSN string
	var endLSN string
	for start := 1; start <= rowCount; start += 16 {
		end := min(start+16, rowCount+1)
		recordIDs := make([]string, 0, end-start)
		for index := start; index < end; index++ {
			recordIDs = append(recordIDs, r1RecordID(group, index))
		}
		observation, err := harness.Operator().ObserveWALRecords(ctx, recordIDs)
		if err != nil {
			t.Fatalf("observe R1 WAL transaction: %v", err)
		}
		if len(observation.Records) != len(recordIDs)*expectedHistory || !observation.WorkerRunning ||
			observation.BlockingPoison || !observation.ContiguousAcknowledged ||
			!observation.AcknowledgementMatchesObservedEnd || !observation.SlotMatchesObservedEnd {
			t.Fatal("R1 WAL transaction observation is incomplete")
		}
		latestCommitLSN := observation.Records[len(observation.Records)-1].CommitLSN
		latestRecords := 0
		for _, record := range observation.Records {
			if record.CommitLSN != latestCommitLSN {
				continue
			}
			latestRecords++
			if _, duplicate := seenRecords[record.RecordID]; duplicate {
				t.Fatal("R1 WAL transaction returned a duplicate record")
			}
			if _, duplicate := seenOrdinals[record.EventOrdinal]; duplicate {
				t.Fatal("R1 WAL transaction returned a duplicate event ordinal")
			}
			if commitLSN == "" {
				commitLSN = record.CommitLSN
				endLSN = record.EndLSN
			}
			if record.CommitLSN == "" || record.EndLSN == "" || record.CommitLSN == record.EndLSN ||
				record.CommitLSN != commitLSN || record.EndLSN != endLSN ||
				record.FenceCoverage != "materialized" || record.EffectOrdinal != 0 ||
				!uuidPattern.MatchString(record.RowVersion) {
				t.Fatal("R1 WAL record state is invalid")
			}
			seenRecords[record.RecordID] = struct{}{}
			seenOrdinals[record.EventOrdinal] = struct{}{}
		}
		if latestRecords != len(recordIDs) {
			t.Fatal("R1 WAL transaction latest-record cardinality is invalid")
		}
	}
	if len(seenRecords) != rowCount || len(seenOrdinals) != rowCount {
		t.Fatalf("R1 WAL transaction cardinality = %d, want %d", len(seenRecords), rowCount)
	}
	for index := 1; index <= rowCount; index++ {
		if _, ok := seenRecords[r1RecordID(group, index)]; !ok {
			t.Fatal("R1 WAL transaction omitted a fixed record")
		}
		if _, ok := seenOrdinals[int64(index-1)]; !ok {
			t.Fatal("R1 WAL transaction omitted a fixed event ordinal")
		}
	}
}

func requireR1OneRowTransactions(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	expectedHistory int,
) {
	t.Helper()
	latestCommitLSNs := make(map[string]struct{}, r1BenchmarkAckRows)
	for start := 1; start <= r1BenchmarkAckRows; start += 16 {
		end := min(start+16, r1BenchmarkAckRows+1)
		recordIDs := make([]string, 0, end-start)
		for index := start; index < end; index++ {
			recordIDs = append(recordIDs, r1RecordID(r1AckGroup, index))
		}
		observation, err := harness.Operator().ObserveWALRecords(ctx, recordIDs)
		if err != nil {
			t.Fatalf("observe R1 one-row transactions: %v", err)
		}
		if len(observation.Records) != len(recordIDs)*expectedHistory || !observation.WorkerRunning ||
			observation.BlockingPoison || !observation.ContiguousAcknowledged {
			t.Fatal("R1 one-row transaction observation is incomplete")
		}
		if end == r1BenchmarkAckRows+1 &&
			(!observation.AcknowledgementMatchesObservedEnd || !observation.SlotMatchesObservedEnd) {
			t.Fatal("R1 final one-row transaction is not durably acknowledged")
		}
		latestByRecord := make(map[string]string, len(recordIDs))
		for _, record := range observation.Records {
			latestByRecord[record.RecordID] = record.CommitLSN
		}
		if len(latestByRecord) != len(recordIDs) {
			t.Fatal("R1 one-row transaction record cardinality is invalid")
		}
		for _, recordID := range recordIDs {
			commitLSN := latestByRecord[recordID]
			if commitLSN == "" {
				t.Fatal("R1 one-row transaction omitted a fixed record")
			}
			if _, duplicate := latestCommitLSNs[commitLSN]; duplicate {
				t.Fatal("R1 one-row updates did not materialize as separate transactions")
			}
			latestCommitLSNs[commitLSN] = struct{}{}
		}
	}
	if len(latestCommitLSNs) != r1BenchmarkAckTransactions {
		t.Fatal("R1 one-row transaction count is invalid")
	}
}

func collectR1Samples(t *testing.T, operation func(int) time.Duration) []int64 {
	t.Helper()
	samples := make([]int64, 0, r1BenchmarkMeasuredSamples)
	for sample := 0; sample < r1BenchmarkWarmupSamples+r1BenchmarkMeasuredSamples; sample++ {
		elapsed := operation(sample).Nanoseconds()
		if elapsed <= 0 {
			t.Fatal("R1 benchmark produced a zero latency sample")
		}
		if sample >= r1BenchmarkWarmupSamples {
			samples = append(samples, elapsed)
		}
	}
	return samples
}

func measureR1WorkerObservedRSS(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	expectedPID int,
	sample int,
) int64 {
	t.Helper()
	markerID := r1RecordID(r1WorkerRSSGroup, r1BenchmarkWorkerRSSRows)
	priorCount := r1WALRecordCount(t, ctx, harness, markerID)
	transaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin R1 worker RSS source transaction: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = transaction.Rollback()
		}
	}()
	if _, err := transaction.ExecContext(
		ctx,
		r1UpdateRowsStatement,
		r1WorkerRSSGroup,
		fmt.Sprintf("r1-rss-%02d", sample),
		r1BenchmarkWorkerRSSRows,
	); err != nil {
		t.Fatalf("stage R1 worker RSS source transaction: %v", err)
	}

	type samplingResult struct {
		peak int64
		err  error
	}
	samplingContext, stopSampling := context.WithCancel(ctx)
	ready := make(chan error, 1)
	result := make(chan samplingResult, 1)
	go func() {
		first := true
		var peak int64
		for {
			rss, rssErr := readR1ProcessRSSBytes(samplingContext, expectedPID)
			if samplingContext.Err() != nil {
				result <- samplingResult{peak: peak}
				return
			}
			if rssErr != nil {
				sampleErr := rssErr
				if first {
					ready <- sampleErr
				}
				result <- samplingResult{peak: peak, err: sampleErr}
				return
			}
			if rss > peak {
				peak = rss
			}
			if first {
				ready <- nil
				first = false
			}
			timer := time.NewTimer(time.Millisecond)
			select {
			case <-samplingContext.Done():
				if !timer.Stop() {
					<-timer.C
				}
				result <- samplingResult{peak: peak}
				return
			case <-timer.C:
			}
		}
	}()
	if err := <-ready; err != nil {
		stopSampling()
		<-result
		t.Fatalf("start R1 WAL worker RSS sampling: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		stopSampling()
		<-result
		t.Fatalf("commit R1 worker RSS source transaction: %v", err)
	}
	committed = true
	waitForR1WALRecordAdvance(t, ctx, harness, markerID, priorCount)
	stopSampling()
	observation := <-result
	if observation.err != nil {
		t.Fatalf("sample R1 WAL worker RSS: %v", observation.err)
	}
	currentPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
	if err != nil || currentPID != expectedPID {
		t.Fatal("R1 WAL worker process changed during RSS measurement")
	}
	if observation.peak <= 0 || observation.peak > r1BenchmarkMaximumRSSBytes {
		t.Fatal("R1 WAL worker maximum observed RSS is invalid")
	}
	return observation.peak
}

func readR1ProcessRSSBytes(ctx context.Context, pid int) (int64, error) {
	if ctx == nil || pid <= 0 {
		return 0, fmt.Errorf("RSS process observation is invalid")
	}
	output, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, fmt.Errorf("read process RSS: %w", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 1 {
		return 0, fmt.Errorf("process RSS output is invalid")
	}
	kibibytes, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || kibibytes <= 0 || kibibytes > r1BenchmarkMaximumRSSBytes/1024 {
		return 0, fmt.Errorf("process RSS value is invalid")
	}
	return kibibytes * 1024, nil
}

func r1ResponseObjects(t *testing.T, value any, name string) []map[string]any {
	t.Helper()
	rawObjects, ok := value.([]any)
	if !ok {
		t.Fatalf("%s are invalid", name)
	}
	objects := make([]map[string]any, 0, len(rawObjects))
	for _, rawObject := range rawObjects {
		object, ok := rawObject.(map[string]any)
		if !ok {
			t.Fatalf("%s contain an invalid object", name)
		}
		objects = append(objects, object)
	}
	return objects
}

func r1RecordID(group, index int) string {
	return fmt.Sprintf("00000000-0000-4000-%04d-%012d", group, index)
}

func newR1LatencyMetric(samples []int64) r1LatencyMetric {
	return r1LatencyMetric{Samples: append([]int64(nil), samples...), MedianNS: medianR1Latency(samples)}
}

func newR1ObservedRSSMetric(samples []int64) r1ObservedRSSMetric {
	metric := r1ObservedRSSMetric{Samples: append([]int64(nil), samples...)}
	for _, sample := range samples {
		metric.MaximumBytes = max(metric.MaximumBytes, sample)
	}
	return metric
}

func medianR1Latency(samples []int64) int64 {
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	if len(ordered) == 0 {
		return 0
	}
	middle := len(ordered) / 2
	if len(ordered)%2 != 0 {
		return ordered[middle]
	}
	return ordered[middle-1]/2 + ordered[middle]/2 + (ordered[middle-1]%2+ordered[middle]%2)/2
}

func r1PushThroughputMilli(samples []int64) int64 {
	var total int64
	for _, sample := range samples {
		total += sample
	}
	if total <= 0 {
		return 0
	}
	mutations := int64(r1BenchmarkPushMutations * len(samples))
	return mutations * int64(time.Second) * 1000 / total
}

func writeR1BenchmarkResult(path string, result r1BenchmarkResult) error {
	if err := validateR1BenchmarkResult(result); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	data = append(data, '\n')
	if _, err := parseR1BenchmarkResult(data); err != nil {
		return fmt.Errorf("validate encoded result: %w", err)
	}

	file, err := os.CreateTemp(filepath.Dir(path), ".r1-benchmark-result-*")
	if err != nil {
		return fmt.Errorf("create temporary result file: %w", err)
	}
	temporaryPath := file.Name()
	defer os.Remove(temporaryPath)

	if err := file.Chmod(0o644); err != nil {
		file.Close()
		return fmt.Errorf("set result file permissions: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("write temporary result file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary result file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish result file: %w", err)
	}
	return nil
}

func readR1BenchmarkResult(path string) (r1BenchmarkResult, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return r1BenchmarkResult{}, fmt.Errorf("stat benchmark file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > r1BenchmarkMaximumBytes {
		return r1BenchmarkResult{}, fmt.Errorf("benchmark file size is invalid")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return r1BenchmarkResult{}, fmt.Errorf("read benchmark file: %w", err)
	}
	return parseR1BenchmarkResult(data)
}

func parseR1BenchmarkResult(data []byte) (r1BenchmarkResult, error) {
	if len(data) == 0 || len(data) > r1BenchmarkMaximumBytes {
		return r1BenchmarkResult{}, fmt.Errorf("benchmark JSON size is invalid")
	}
	if err := jsonstrict.ValidateValue(data); err != nil {
		return r1BenchmarkResult{}, fmt.Errorf("benchmark JSON is malformed: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result r1BenchmarkResult
	if err := decoder.Decode(&result); err != nil {
		return r1BenchmarkResult{}, fmt.Errorf("benchmark JSON schema is invalid: %w", err)
	}
	if err := validateR1BenchmarkResult(result); err != nil {
		return r1BenchmarkResult{}, err
	}
	return result, nil
}

func validateR1BenchmarkResult(result r1BenchmarkResult) error {
	if result.Format != r1BenchmarkFormat {
		return fmt.Errorf("benchmark format is invalid")
	}
	if !r1RevisionPattern.MatchString(result.Revision) {
		return fmt.Errorf("benchmark revision is invalid")
	}
	if !r1SHA256Pattern.MatchString(result.DefinitionSHA256) {
		return fmt.Errorf("benchmark definition SHA-256 is invalid")
	}
	if !r1GoIdentityPattern.MatchString(result.Environment.GOOS) ||
		!r1GoIdentityPattern.MatchString(result.Environment.GOARCH) ||
		result.Environment.LogicalCPUs <= 0 || result.Environment.LogicalCPUs > 65536 ||
		!r1SHA256Pattern.MatchString(result.Environment.HardwareFingerprintSHA256) ||
		!r1SHA256Pattern.MatchString(result.Environment.AdapterSHA256) ||
		!r1SHA256Pattern.MatchString(result.Environment.ExtensionManifestSHA256) {
		return fmt.Errorf("benchmark environment is invalid")
	}
	if result.Workload != r1BenchmarkWorkloadDefinition {
		return fmt.Errorf("benchmark workload is invalid")
	}
	metrics := []struct {
		name   string
		metric r1LatencyMetric
	}{
		{name: "terminal pull", metric: result.Metrics.TerminalPull},
		{name: "changed pull", metric: result.Metrics.ChangedPull},
		{name: "push", metric: result.Metrics.Push},
		{name: "WAL observer detection", metric: result.Metrics.WALObserverDetection},
		{name: "transaction acknowledgement", metric: result.Metrics.TransactionAck},
	}
	for _, entry := range metrics {
		if len(entry.metric.Samples) != r1BenchmarkMeasuredSamples {
			return fmt.Errorf("%s samples are missing", entry.name)
		}
		for _, sample := range entry.metric.Samples {
			if sample <= 0 || sample > int64(time.Hour) {
				return fmt.Errorf("%s sample is invalid", entry.name)
			}
		}
		if entry.metric.MedianNS <= 0 || entry.metric.MedianNS != medianR1Latency(entry.metric.Samples) {
			return fmt.Errorf("%s median is invalid", entry.name)
		}
	}
	if len(result.Metrics.WorkerObservedRSS.Samples) != r1BenchmarkMeasuredSamples {
		return fmt.Errorf("WAL worker observed RSS samples are missing")
	}
	var maximumRSS int64
	for _, sample := range result.Metrics.WorkerObservedRSS.Samples {
		if sample <= 0 || sample > r1BenchmarkMaximumRSSBytes {
			return fmt.Errorf("WAL worker observed RSS sample is invalid")
		}
		maximumRSS = max(maximumRSS, sample)
	}
	if result.Metrics.WorkerObservedRSS.MaximumBytes != maximumRSS {
		return fmt.Errorf("WAL worker observed RSS maximum is invalid")
	}
	expectedThroughput := r1PushThroughputMilli(result.Metrics.Push.Samples)
	if result.Metrics.PushThroughputMilli <= 0 ||
		result.Metrics.PushThroughputMilli != expectedThroughput {
		return fmt.Errorf("push throughput is invalid")
	}
	return nil
}

func compareR1BenchmarkResults(candidate, baseline r1BenchmarkResult) error {
	if candidate.Format != baseline.Format {
		return fmt.Errorf("benchmark formats do not match")
	}
	if candidate.DefinitionSHA256 != baseline.DefinitionSHA256 {
		return fmt.Errorf("benchmark definitions do not match")
	}
	if candidate.Environment.GOOS != baseline.Environment.GOOS ||
		candidate.Environment.GOARCH != baseline.Environment.GOARCH ||
		candidate.Environment.LogicalCPUs != baseline.Environment.LogicalCPUs ||
		candidate.Environment.HardwareFingerprintSHA256 != baseline.Environment.HardwareFingerprintSHA256 {
		return fmt.Errorf("benchmark environments do not match")
	}
	if candidate.Workload != baseline.Workload {
		return fmt.Errorf("benchmark workloads do not match")
	}
	latencies := []struct {
		name      string
		candidate int64
		baseline  int64
	}{
		{name: "terminal pull", candidate: candidate.Metrics.TerminalPull.MedianNS, baseline: baseline.Metrics.TerminalPull.MedianNS},
		{name: "changed pull", candidate: candidate.Metrics.ChangedPull.MedianNS, baseline: baseline.Metrics.ChangedPull.MedianNS},
		{name: "push", candidate: candidate.Metrics.Push.MedianNS, baseline: baseline.Metrics.Push.MedianNS},
		{name: "WAL observer detection", candidate: candidate.Metrics.WALObserverDetection.MedianNS, baseline: baseline.Metrics.WALObserverDetection.MedianNS},
		{name: "transaction acknowledgement", candidate: candidate.Metrics.TransactionAck.MedianNS, baseline: baseline.Metrics.TransactionAck.MedianNS},
	}
	for _, latency := range latencies {
		if latency.candidate*100 > latency.baseline*115 {
			return fmt.Errorf("%s latency median regressed by more than 15 percent", latency.name)
		}
	}
	if candidate.Metrics.PushThroughputMilli*100 < baseline.Metrics.PushThroughputMilli*85 {
		return fmt.Errorf("push throughput regressed by more than 15 percent")
	}
	if candidate.Metrics.WorkerObservedRSS.MaximumBytes*100 > baseline.Metrics.WorkerObservedRSS.MaximumBytes*115 {
		return fmt.Errorf("WAL worker observed RSS regressed by more than 15 percent")
	}
	return nil
}

func TestR1BenchmarkStrictParser(t *testing.T) {
	validResult := newR1UnitResult()
	valid, err := json.Marshal(validResult)
	if err != nil {
		t.Fatalf("encode valid R1 result: %v", err)
	}
	if _, err := parseR1BenchmarkResult(valid); err != nil {
		t.Fatalf("parse valid R1 result: %v", err)
	}

	invalidResult := func(change func(*r1BenchmarkResult)) []byte {
		result := newR1UnitResult()
		change(&result)
		data, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			t.Fatalf("encode invalid R1 result: %v", marshalErr)
		}
		return data
	}
	tests := []struct {
		name string
		data []byte
	}{
		{name: "malformed", data: []byte(`{"format":`)},
		{name: "unknown field", data: bytes.Replace(valid, []byte(`"format"`), []byte(`"unknown":1,"format"`), 1)},
		{name: "duplicate field", data: bytes.Replace(valid, []byte(`"format"`), []byte(`"format":"duplicate","format"`), 1)},
		{name: "trailing data", data: append(append([]byte(nil), valid...), []byte(` {}`)...)},
		{name: "missing samples", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Metrics.ChangedPull.Samples = nil
		})},
		{name: "zero sample", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Metrics.Push.Samples[0] = 0
		})},
		{name: "zero median", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Metrics.WALObserverDetection.MedianNS = 0
		})},
		{name: "missing transaction acknowledgement samples", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Metrics.TransactionAck.Samples = nil
		})},
		{name: "missing worker RSS samples", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Metrics.WorkerObservedRSS.Samples = nil
		})},
		{name: "zero worker RSS sample", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Metrics.WorkerObservedRSS.Samples[0] = 0
		})},
		{name: "inconsistent worker RSS maximum", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Metrics.WorkerObservedRSS.MaximumBytes++
		})},
		{name: "invalid definition SHA-256", data: invalidResult(func(result *r1BenchmarkResult) {
			result.DefinitionSHA256 = strings.Repeat("A", 64)
		})},
		{name: "invalid environment", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Environment.LogicalCPUs = 0
		})},
		{name: "invalid adapter provenance", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Environment.AdapterSHA256 = strings.Repeat("A", 64)
		})},
		{name: "invalid throughput", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Metrics.PushThroughputMilli = -1
		})},
		{name: "inconsistent throughput", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Metrics.PushThroughputMilli++
		})},
		{name: "invalid total source rows", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.TotalSourceRows++
		})},
		{name: "invalid pull fixture rows", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.PullFixtureRows++
		})},
		{name: "invalid changed pull rows", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.ChangedPullRows++
		})},
		{name: "invalid push existing rows", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.PushExistingRows++
		})},
		{name: "invalid push mutation count", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.PushBatchMutations++
		})},
		{name: "invalid WAL observer rows", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.WALObserverRows++
		})},
		{name: "invalid WAL transaction rows", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.WALTransactionRows++
		})},
		{name: "invalid acknowledgement rows", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.AcknowledgementRows++
		})},
		{name: "invalid acknowledgement transactions", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.AcknowledgementTransactions++
		})},
		{name: "invalid worker RSS rows", data: invalidResult(func(result *r1BenchmarkResult) {
			result.Workload.WorkerRSSRows++
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseR1BenchmarkResult(test.data); err == nil {
				t.Fatal("strict R1 parser accepted invalid JSON")
			}
		})
	}
}

func TestR1BenchmarkThresholdLogic(t *testing.T) {
	baseline := newR1UnitResult()
	baseline.Metrics.TerminalPull.MedianNS = 100_000
	baseline.Metrics.ChangedPull.MedianNS = 100_000
	baseline.Metrics.Push.MedianNS = 100_000
	baseline.Metrics.WALObserverDetection.MedianNS = 100_000
	baseline.Metrics.TransactionAck.MedianNS = 100_000
	baseline.Metrics.WorkerObservedRSS.MaximumBytes = 100_000
	baseline.Metrics.PushThroughputMilli = 1_000_000
	candidate := baseline
	candidate.Metrics.TerminalPull.MedianNS = 115_000
	candidate.Metrics.ChangedPull.MedianNS = 115_000
	candidate.Metrics.Push.MedianNS = 115_000
	candidate.Metrics.WALObserverDetection.MedianNS = 115_000
	candidate.Metrics.TransactionAck.MedianNS = 115_000
	candidate.Metrics.WorkerObservedRSS.MaximumBytes = 115_000
	candidate.Metrics.PushThroughputMilli = 850_000
	if err := compareR1BenchmarkResults(candidate, baseline); err != nil {
		t.Fatalf("exact R1 thresholds failed: %v", err)
	}
	candidate.Environment.AdapterSHA256 = strings.Repeat("e", 64)
	candidate.Environment.ExtensionManifestSHA256 = strings.Repeat("f", 64)
	if err := compareR1BenchmarkResults(candidate, baseline); err != nil {
		t.Fatalf("R1 comparison rejected changed artifact provenance: %v", err)
	}

	tests := []struct {
		name   string
		change func(*r1BenchmarkResult)
	}{
		{name: "terminal pull latency", change: func(result *r1BenchmarkResult) {
			result.Metrics.TerminalPull.MedianNS++
		}},
		{name: "changed pull latency", change: func(result *r1BenchmarkResult) {
			result.Metrics.ChangedPull.MedianNS++
		}},
		{name: "push latency", change: func(result *r1BenchmarkResult) {
			result.Metrics.Push.MedianNS++
		}},
		{name: "WAL latency", change: func(result *r1BenchmarkResult) {
			result.Metrics.WALObserverDetection.MedianNS++
		}},
		{name: "transaction acknowledgement latency", change: func(result *r1BenchmarkResult) {
			result.Metrics.TransactionAck.MedianNS++
		}},
		{name: "worker observed RSS", change: func(result *r1BenchmarkResult) {
			result.Metrics.WorkerObservedRSS.MaximumBytes++
		}},
		{name: "push throughput", change: func(result *r1BenchmarkResult) {
			result.Metrics.PushThroughputMilli--
		}},
		{name: "format mismatch", change: func(result *r1BenchmarkResult) {
			result.Format = "other"
		}},
		{name: "definition drift", change: func(result *r1BenchmarkResult) {
			result.DefinitionSHA256 = strings.Repeat("b", 64)
		}},
		{name: "hardware mismatch", change: func(result *r1BenchmarkResult) {
			result.Environment.HardwareFingerprintSHA256 = strings.Repeat("e", 64)
		}},
		{name: "environment drift", change: func(result *r1BenchmarkResult) {
			result.Environment.LogicalCPUs++
		}},
		{name: "workload mismatch", change: func(result *r1BenchmarkResult) {
			result.Workload.TotalSourceRows++
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			regressed := candidate
			test.change(&regressed)
			if err := compareR1BenchmarkResults(regressed, baseline); err == nil {
				t.Fatal("R1 comparison accepted a regression or mismatch")
			}
		})
	}
}

func TestR1BenchmarkResultPathSafety(t *testing.T) {
	workspace := t.TempDir()
	repoRoot := filepath.Join(workspace, "repo")
	baselineDirectory := filepath.Join(repoRoot, "conformance", "blackbox", "integration", "testdata")
	resultDirectory := filepath.Join(workspace, "results")
	if err := os.MkdirAll(baselineDirectory, 0o755); err != nil {
		t.Fatalf("create unit repository: %v", err)
	}
	if err := os.MkdirAll(resultDirectory, 0o755); err != nil {
		t.Fatalf("create unit result directory: %v", err)
	}
	for _, path := range []string{
		filepath.Join(repoRoot, "result.json"),
		filepath.Join(baselineDirectory, "r1-benchmark-baseline.json"),
	} {
		if _, err := safeR1BenchmarkResultPath(repoRoot, path); err == nil {
			t.Fatal("R1 result path accepted a repository location")
		}
	}
	validPath := filepath.Join(resultDirectory, "candidate.json")
	resolved, err := safeR1BenchmarkResultPath(repoRoot, validPath)
	if err != nil {
		t.Fatalf("accept external R1 result path: %v", err)
	}
	resolvedDirectory, err := filepath.EvalSymlinks(resultDirectory)
	if err != nil {
		t.Fatalf("resolve unit result directory: %v", err)
	}
	if resolved != filepath.Join(resolvedDirectory, "candidate.json") {
		t.Fatal("R1 result path resolution changed an external path")
	}
	t.Setenv("R1_BENCHMARK_MODE", r1BenchmarkModeCompare)
	t.Setenv("R1_BENCHMARK_REVISION", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	t.Setenv("R1_BENCHMARK_RESULT", validPath)
	t.Setenv("R1_BENCHMARK_BASELINE", filepath.Join(resultDirectory, "untrusted-baseline.json"))
	config, err := loadR1BenchmarkConfig(repoRoot)
	if err != nil {
		t.Fatalf("load R1 path-safety configuration: %v", err)
	}
	canonicalBaseline := filepath.Join(baselineDirectory, "r1-benchmark-baseline.json")
	if config.BaselinePath != canonicalBaseline {
		t.Fatal("R1 configuration accepted an external baseline path")
	}

	repositorySentinel := filepath.Join(repoRoot, "sentinel")
	if err := os.WriteFile(repositorySentinel, []byte("repository data"), 0o644); err != nil {
		t.Fatalf("create unit repository sentinel: %v", err)
	}
	hardLinkPath := filepath.Join(resultDirectory, "existing-result.json")
	if err := os.Link(repositorySentinel, hardLinkPath); err != nil {
		t.Fatalf("create unit hard link: %v", err)
	}
	resolvedHardLink, err := safeR1BenchmarkResultPath(repoRoot, hardLinkPath)
	if err != nil {
		t.Fatalf("accept external hard-link path: %v", err)
	}
	if err := writeR1BenchmarkResult(resolvedHardLink, newR1UnitResult()); err != nil {
		t.Fatalf("replace external hard-link result: %v", err)
	}
	sentinelData, err := os.ReadFile(repositorySentinel)
	if err != nil {
		t.Fatalf("read unit repository sentinel: %v", err)
	}
	if string(sentinelData) != "repository data" {
		t.Fatal("R1 result write changed a repository hard-link target")
	}

	linkedRepository := filepath.Join(workspace, "linked-repo")
	if err := os.Symlink(repoRoot, linkedRepository); err != nil {
		t.Fatalf("create unit repository link: %v", err)
	}
	if _, err := safeR1BenchmarkResultPath(repoRoot, filepath.Join(linkedRepository, "result.json")); err == nil {
		t.Fatal("R1 result path accepted a linked repository location")
	}
}

func newR1UnitResult() r1BenchmarkResult {
	samples := []int64{100, 110, 120, 130, 140, 150, 160, 170, 180, 190}
	result := r1BenchmarkResult{
		Format:           r1BenchmarkFormat,
		Revision:         "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		DefinitionSHA256: strings.Repeat("a", 64),
		Environment: r1BenchmarkEnvironment{
			GOOS:                      "unitos",
			GOARCH:                    "unitarch",
			LogicalCPUs:               8,
			HardwareFingerprintSHA256: strings.Repeat("b", 64),
			AdapterSHA256:             strings.Repeat("c", 64),
			ExtensionManifestSHA256:   strings.Repeat("d", 64),
		},
		Workload: r1BenchmarkWorkloadDefinition,
		Metrics: r1BenchmarkMetrics{
			TerminalPull:         newR1LatencyMetric(samples),
			ChangedPull:          newR1LatencyMetric(samples),
			Push:                 newR1LatencyMetric(samples),
			WALObserverDetection: newR1LatencyMetric(samples),
			TransactionAck:       newR1LatencyMetric(samples),
			WorkerObservedRSS:    newR1ObservedRSSMetric(samples),
		},
	}
	result.Metrics.PushThroughputMilli = r1PushThroughputMilli(result.Metrics.Push.Samples)
	return result
}
