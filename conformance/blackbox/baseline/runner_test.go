package baseline

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var (
	baselineDriverOnce sync.Once
)

const baselineTestDriverName = "synchro-baseline-test-driver"

type baselineTestDriver struct{}

func (baselineTestDriver) Open(string) (driver.Conn, error) {
	return baselineTestConn{}, nil
}

type baselineTestConn struct{}

func (baselineTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements are not used")
}

func (baselineTestConn) Close() error {
	return nil
}

func (baselineTestConn) Begin() (driver.Tx, error) {
	return baselineTestTx{}, nil
}

type baselineTestTx struct{}

func (baselineTestTx) Commit() error {
	return nil
}

func (baselineTestTx) Rollback() error {
	return nil
}

type baselineTestOperations struct {
	database *sql.DB
}

func baselineSource(t *testing.T) *baselineTestOperations {
	t.Helper()
	baselineDriverOnce.Do(func() {
		sql.Register(baselineTestDriverName, baselineTestDriver{})
	})
	database, err := sql.Open(baselineTestDriverName, "")
	if err != nil {
		t.Fatalf("open baseline source: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close baseline source: %v", err)
		}
	})
	return &baselineTestOperations{database: database}
}

func (operations *baselineTestOperations) ExecContext(ctx context.Context, statement string, arguments ...any) error {
	_, err := operations.database.ExecContext(ctx, statement, arguments...)
	return err
}

func (*baselineTestOperations) CommitInReverseBeginOrder(context.Context, string, []any, string, []any) error {
	return nil
}

func (*baselineTestOperations) DropHydrationColumn(context.Context) error         { return nil }
func (*baselineTestOperations) RestoreHydrationColumn(context.Context) error      { return nil }
func (*baselineTestOperations) RegisterSchemaQueue(context.Context) error         { return nil }
func (*baselineTestOperations) ConfigureDecodeTrap(context.Context, string) error { return nil }
func (*baselineTestOperations) RegisterLateSourceTable(context.Context) error     { return nil }
func (*baselineTestOperations) UnregisterLateSourceTable(context.Context) error   { return nil }
func (*baselineTestOperations) ConfigureCrossScopeTable(context.Context) error    { return nil }
func (*baselineTestOperations) RestoreCrossScopeTable(context.Context) error      { return nil }
func (*baselineTestOperations) ReloadRegistry(context.Context) error              { return nil }
func (*baselineTestOperations) CompactPositiveInterval(context.Context) ([]byte, error) {
	return []byte(`{"deactivated_clients":0,"safe_seq":0,"deleted_entries":0}`), nil
}

func baselineOutput(t *testing.T) OutputPath {
	t.Helper()
	output, err := NewOutputPath(filepath.Join(t.TempDir(), "baseline"))
	if err != nil {
		t.Fatalf("create baseline output: %v", err)
	}
	return output
}

func TestConnectSendsOnlyProtocol2DiagnosticRequest(t *testing.T) {
	var observedProtocol any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/sync/connect" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if request.Header.Get("X-Synchro-Diagnostic-Class") != string(nonReleaseClass) {
			t.Errorf("diagnostic classification header = %q", request.Header.Get("X-Synchro-Diagnostic-Class"))
		}
		if request.Header.Get("X-Synchro-Protocol-Version") != "2" {
			t.Errorf("protocol header = %q", request.Header.Get("X-Synchro-Protocol-Version"))
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			return
		}
		var value map[string]any
		if err := json.Unmarshal(body, &value); err != nil {
			t.Errorf("decode request body: %v", err)
			return
		}
		observedProtocol = value["protocol_version"]
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"server_time":"2026-01-01T00:00:00Z","protocol_version":2,"scope_set_version":1,"schema":{"version":1,"hash":"schema","action":"replace"},"scopes":{"add":[],"remove":[]}}`))
	}))
	defer server.Close()
	operations := baselineSource(t)
	runner, err := NewRunner(RunnerConfig{
		BaseURL:     server.URL,
		HTTPClient:  server.Client(),
		BearerToken: "diagnostic-token",
		Source:      operations,
		Operator:    operations,
		Output:      baselineOutput(t),
	})
	if err != nil {
		t.Fatalf("create runner: %v", err)
	}
	response, _, err := (&ProbeRuntime{runner: runner}).Connect(context.Background(), ConnectRequest{
		ClientID:        "client",
		Platform:        "diagnostic",
		AppVersion:      "0.0.0-diagnostic",
		ProtocolVersion: ProtocolVersion,
		KnownScopes:     map[string]ScopeCursor{},
	})
	if err != nil {
		t.Fatalf("execute connect: %v", err)
	}
	if response.ProtocolVersion != ProtocolVersion || observedProtocol != float64(ProtocolVersion) {
		t.Fatalf("protocol version response=%d request=%v", response.ProtocolVersion, observedProtocol)
	}
}

func TestProtocol2RejectsAnotherVersion(t *testing.T) {
	runner := &Runner{}
	if _, _, err := (&ProbeRuntime{runner: runner}).Connect(context.Background(), ConnectRequest{
		ClientID:        "client",
		Platform:        "diagnostic",
		AppVersion:      "0.0.0-diagnostic",
		ProtocolVersion: 3,
		KnownScopes:     map[string]ScopeCursor{},
	}); err == nil {
		t.Fatal("connect accepted a non-protocol-2 request")
	}
}

func TestNewOutputPathRejectsRCAncestorLock(t *testing.T) {
	root := t.TempDir()
	locked := filepath.Join(root, "candidate-artifacts")
	if err := os.Mkdir(locked, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "rc-candidate-lock.json"), []byte(`{"locked":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewOutputPath(filepath.Join(locked, "private", "baseline")); err == nil {
		t.Fatal("output beneath an RC candidate lock was accepted")
	}
}

func TestNewOutputPathAllowsUnlockedAncestor(t *testing.T) {
	root := t.TempDir()
	output, err := NewOutputPath(filepath.Join(root, "private", "baseline"))
	if err != nil {
		t.Fatalf("unlocked ancestor rejected: %v", err)
	}
	if output.Class() != nonReleaseClass {
		t.Fatal("unlocked output lost diagnostic classification")
	}
}

func TestHydrationProbePredicatesRejectSyntheticNonDefect(t *testing.T) {
	canonical, legacy, noProgress, err := inspectHydrationError([]byte(`{"error":{"code":"sync_integrity_failure","message":"projection hydration failed","retryable":false}}`))
	if err != nil || !canonical || legacy || !noProgress {
		t.Fatalf("canonical hydration control = canonical:%t legacy:%t no_progress:%t err:%v", canonical, legacy, noProgress, err)
	}
	if capturesHydrationFailure(true, http.StatusInternalServerError, canonical, legacy, noProgress, true) {
		t.Fatal("canonical hydration error satisfied the legacy defect predicate")
	}
	if capturesPositiveCompactionInterval(CompactionResult{}) {
		t.Fatal("zero compaction result satisfied the positive-interval defect predicate")
	}
}

func TestDefaultProbesHaveTenUniqueFamilies(t *testing.T) {
	probes := DefaultProbes()
	want := []DefectFamily{
		DefectCommitOrder,
		DefectPullStarvation,
		DefectHydrationFailure,
		DefectDecodeFailure,
		DefectRegistryReload,
		DefectResponseLoss,
		DefectForgedRebuild,
		DefectSchemaIntent,
		DefectCompactionInterval,
		DefectOwnershipChange,
	}
	if len(probes) != len(want) {
		t.Fatalf("probe count = %d", len(probes))
	}
	seen := make(map[DefectFamily]struct{}, len(probes))
	for index, probe := range probes {
		if probe == nil || probe.Family() == "" {
			t.Fatal("default probes contain an invalid family")
		}
		if probe.Family() != want[index] {
			t.Fatalf("probe %d family = %q, want %q", index, probe.Family(), want[index])
		}
		if _, found := seen[probe.Family()]; found {
			t.Fatalf("duplicate diagnostic family %q", probe.Family())
		}
		seen[probe.Family()] = struct{}{}
	}
}

func TestPrepareSessionRebuildsScopesWithoutCursors(t *testing.T) {
	var rebuilt []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/sync/connect":
			_, _ = writer.Write([]byte(`{"server_time":"2026-01-01T00:00:00Z","protocol_version":2,"scope_set_version":1,"schema":{"version":1,"hash":"schema","action":"replace"},"scopes":{"add":[{"id":"user:diagnostic-user","cursor":null},{"id":"cf:global","cursor":null}],"remove":[]}}`))
		case "/sync/rebuild":
			var rebuild RebuildRequest
			if err := json.NewDecoder(request.Body).Decode(&rebuild); err != nil {
				t.Errorf("decode rebuild request: %v", err)
				return
			}
			rebuilt = append(rebuilt, rebuild.Scope)
			_, _ = writer.Write([]byte(`{"scope":"` + rebuild.Scope + `","records":[],"cursor":null,"has_more":false,"final_scope_cursor":"cursor:` + rebuild.Scope + `","checksum":"0"}`))
		case "/sync/pull":
			_, _ = writer.Write([]byte(`{"changes":[],"scope_set_version":1,"scope_cursors":{},"scope_updates":{"add":[],"remove":[]},"rebuild":[],"has_more":false,"checksums":{"cf:global":"0","user:diagnostic-user":"0"}}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	operations := baselineSource(t)
	runner, err := NewRunner(RunnerConfig{
		BaseURL:     server.URL,
		HTTPClient:  server.Client(),
		BearerToken: "diagnostic-token",
		Source:      operations,
		Operator:    operations,
		Output:      baselineOutput(t),
	})
	if err != nil {
		t.Fatalf("create runner: %v", err)
	}
	session, _, err := prepareSession(context.Background(), &ProbeRuntime{runner: runner}, DefectPullStarvation)
	if err != nil {
		t.Fatalf("prepare session: %v", err)
	}
	if len(rebuilt) != 2 || rebuilt[0] != "cf:global" || rebuilt[1] != "user:diagnostic-user" {
		t.Fatalf("rebuilt scopes = %v", rebuilt)
	}
	for scope, cursor := range session.scopes {
		if cursor.Cursor == nil || *cursor.Cursor != "cursor:"+scope {
			t.Fatalf("scope %q cursor = %#v", scope, cursor.Cursor)
		}
	}
}

func TestReportIsPermanentlyNonRelease(t *testing.T) {
	output := baselineOutput(t)
	attachment := Attachment{
		id:       "baseline-raw_http_request-sha256:test",
		kind:     "raw_http_request",
		path:     output,
		relative: "attachments/request.bin",
		sha256:   "test",
		size:     1,
	}
	receipt := DiagnosticReceipt{
		id:       "baseline-receipt-sha256:test",
		endpoint: EndpointConnect,
		status:   http.StatusOK,
		request:  attachment,
	}
	report := Report{
		createdAt: time.Now(),
		output:    output,
		probes: []ProbeResult{{
			Family:           DefectCommitOrder,
			ExpectedContract: "commit order",
			Divergence:       "diagnostic divergence",
			Captured:         true,
			ReceiptIDs:       []string{receipt.id},
		}},
		receipts: []DiagnosticReceipt{receipt},
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if document["format"] != string(baselineReportFormat) || document["classification"] != string(nonReleaseClass) {
		t.Fatalf("report tags = %#v", document)
	}
	if report.Format() != baselineReportFormat || report.Classification() != nonReleaseClass || report.Output().Class() != nonReleaseClass {
		t.Fatal("report type markers changed")
	}
}
