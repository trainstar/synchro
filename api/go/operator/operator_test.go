package operator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

const testBootstrapID = "12345678-1234-4234-8234-123456789abc"

func TestCandidateSlotName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		activeSlot string
		want       string
		wantError  bool
	}{
		{name: "appends suffix", activeSlot: "synchro_slot", want: "synchro_slot_bootstrap_0123456789abcdef"},
		{
			name:       "truncates to PostgreSQL limit",
			activeSlot: strings.Repeat("a", maximumSlotNameBytes),
			want: strings.Repeat(
				"a",
				maximumSlotNameBytes-len(projectionBootstrapSlotSuffix)-projectionBootstrapNonceBytes*2,
			) + projectionBootstrapSlotSuffix + "0123456789abcdef",
		},
		{name: "rejects empty", wantError: true},
		{name: "rejects invalid characters", activeSlot: "Invalid", wantError: true},
		{name: "rejects invalid nonce", activeSlot: "synchro_slot", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			nonce := "0123456789abcdef"
			if test.name == "rejects invalid nonce" {
				nonce = "invalid"
			}
			got, err := candidateSlotName(test.activeSlot, nonce)
			if test.wantError {
				if err == nil {
					t.Fatal("candidateSlotName() error = nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("candidateSlotName() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("candidateSlotName() = %q, want %q", got, test.want)
			}
			if len(got) > maximumSlotNameBytes {
				t.Fatalf("candidate slot has %d bytes", len(got))
			}
		})
	}
}

func TestPreparedResponseValidationIsStrict(t *testing.T) {
	t.Parallel()

	valid := `{"bootstrap_id":"` + testBootstrapID + `","registry_generation":7,"schema_version":null,"schema_hash":null,"candidate_slot_name":"synchro_bootstrap"}`
	tests := []struct {
		name string
		raw  string
	}{
		{name: "unknown field", raw: strings.TrimSuffix(valid, "}") + `,"extra":true}`},
		{name: "missing nullable field", raw: `{"bootstrap_id":"` + testBootstrapID + `","registry_generation":7,"schema_version":null,"candidate_slot_name":"synchro_bootstrap"}`},
		{name: "duplicate field", raw: strings.TrimSuffix(valid, "}") + `,"registry_generation":7}`},
		{name: "trailing value", raw: valid + `{}`},
		{name: "mismatched generation", raw: strings.Replace(valid, `"registry_generation":7`, `"registry_generation":8`, 1)},
	}
	if _, err := parsePrepared([]byte(valid), 7, "synchro_bootstrap"); err != nil {
		t.Fatalf("parsePrepared(valid) error = %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parsePrepared([]byte(test.raw), 7, "synchro_bootstrap"); err == nil {
				t.Fatal("parsePrepared() error = nil")
			}
		})
	}
}

func TestSlotDropStateResponseValidationIsStrict(t *testing.T) {
	t.Parallel()

	valid := `{"present":true,"active":false,"valid":true}`
	tests := []struct {
		name string
		raw  string
	}{
		{name: "unknown field", raw: `{"present":true,"active":false,"valid":true,"extra":true}`},
		{name: "missing field", raw: `{"present":true,"active":false}`},
		{name: "inactive absent slot", raw: `{"present":false,"active":true,"valid":true}`},
		{name: "invalid absent slot", raw: `{"present":false,"active":false,"valid":false}`},
	}
	state, err := parseSlotDropState([]byte(valid))
	if err != nil {
		t.Fatalf("parseSlotDropState(valid) error = %v", err)
	}
	if !state.Present || state.Active || !state.Valid {
		t.Fatalf("parseSlotDropState(valid) = %+v", state)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseSlotDropState([]byte(test.raw)); err == nil {
				t.Fatal("parseSlotDropState() error = nil")
			}
		})
	}
}

func TestCandidateReadyRequiresAllBarrierEvidence(t *testing.T) {
	t.Parallel()

	barrier := "0/30"
	ready := projectionBootstrapStatus{
		Lifecycle:                   "catching_up",
		ActivationBarrier:           stringPointer(barrier),
		CandidateMaterializedEndLSN: stringPointer(barrier),
		CandidateAcknowledgedEndLSN: stringPointer(barrier),
		CandidateVerified:           true,
	}
	if !candidateReady(ready, barrier) {
		t.Fatal("candidateReady() = false for complete evidence")
	}

	tests := []struct {
		name   string
		mutate func(*projectionBootstrapStatus)
	}{
		{name: "wrong lifecycle", mutate: func(status *projectionBootstrapStatus) { status.Lifecycle = "baseline_staged" }},
		{name: "missing activation", mutate: func(status *projectionBootstrapStatus) { status.ActivationBarrier = nil }},
		{name: "wrong materialized barrier", mutate: func(status *projectionBootstrapStatus) { status.CandidateMaterializedEndLSN = stringPointer("0/20") }},
		{name: "missing acknowledgement", mutate: func(status *projectionBootstrapStatus) { status.CandidateAcknowledgedEndLSN = nil }},
		{name: "not verified", mutate: func(status *projectionBootstrapStatus) { status.CandidateVerified = false }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			status := ready
			test.mutate(&status)
			if candidateReady(status, barrier) {
				t.Fatal("candidateReady() = true")
			}
		})
	}
}

func TestValidSchemaPair(t *testing.T) {
	t.Parallel()

	version := int64(2)
	zero := int64(0)
	lowerHash := strings.Repeat("ab", 32)
	upperHash := strings.ToUpper(lowerHash)
	tests := []struct {
		name    string
		version *int64
		hash    *string
		want    bool
	}{
		{name: "absent pair", want: true},
		{name: "valid pair", version: &version, hash: &lowerHash, want: true},
		{name: "missing hash", version: &version},
		{name: "missing version", hash: &lowerHash},
		{name: "nonpositive version", version: &zero, hash: &lowerHash},
		{name: "uppercase hash", version: &version, hash: &upperHash},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := validSchemaPair(test.version, test.hash); got != test.want {
				t.Fatalf("validSchemaPair() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestProjectionBootstrapRecoveryPlan(t *testing.T) {
	t.Parallel()

	for _, lifecycle := range []string{"preparing", "baseline_staged", "catching_up"} {
		t.Run(lifecycle, func(t *testing.T) {
			t.Parallel()
			plan, err := projectionBootstrapRecoveryPlan(lifecycle, "active_slot", "candidate_slot")
			if err != nil {
				t.Fatalf("projectionBootstrapRecoveryPlan() error = %v", err)
			}
			if plan.activated || plan.retiredSlotName != "candidate_slot" {
				t.Fatalf("projectionBootstrapRecoveryPlan() = %+v", plan)
			}
		})
	}

	activated, err := projectionBootstrapRecoveryPlan("activated", "active_slot", "candidate_slot")
	if err != nil {
		t.Fatalf("projectionBootstrapRecoveryPlan(activated) error = %v", err)
	}
	if !activated.activated || activated.retiredSlotName != "candidate_slot" {
		t.Fatalf("projectionBootstrapRecoveryPlan(activated) = %+v", activated)
	}
	for _, lifecycle := range []string{"", "cleanup_complete", "aborted", "stream_reset"} {
		if _, err := projectionBootstrapRecoveryPlan(lifecycle, "active_slot", "candidate_slot"); err == nil {
			t.Fatalf("projectionBootstrapRecoveryPlan(%q) error = nil", lifecycle)
		}
	}
	if _, err := projectionBootstrapRecoveryPlan("preparing", "same_slot", "same_slot"); err == nil {
		t.Fatal("projectionBootstrapRecoveryPlan(equal slots) error = nil")
	}
}

func TestActivationResponseRequiresAffectedScope(t *testing.T) {
	t.Parallel()

	valid := `{"bootstrap_id":"` + testBootstrapID + `","registry_generation":7,"schema_version":null,"schema_hash":null,"activation_barrier":"0/30","affected_scopes":["user:u1"]}`
	if _, err := parseActivation([]byte(valid), testBootstrapID, 7, "candidate_slot", "0/30"); err != nil {
		t.Fatalf("parseActivation(valid) error = %v", err)
	}
	empty := strings.Replace(valid, `["user:u1"]`, `[]`, 1)
	if _, err := parseActivation([]byte(empty), testBootstrapID, 7, "candidate_slot", "0/30"); err == nil {
		t.Fatal("parseActivation(empty affected scopes) error = nil")
	}
}

func TestUnlockFailureDiscardsLockConnection(t *testing.T) {
	registerUnlockFailureDriver.Do(func() {
		sql.Register(unlockFailureDriverName, unlockFailureDriver{})
	})

	tests := []struct {
		name    string
		cleanup func(context.Context, *sql.Conn) error
	}{
		{name: "operation lock", cleanup: releaseOperationLock},
		{name: "source locks", cleanup: closeSourceLocks},
		{name: "downgrade lock", cleanup: downgradeOperationLock},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dataSource := strings.ReplaceAll(t.Name(), "/", "-")
			state := &unlockFailureState{}
			unlockFailureStates.Store(dataSource, state)
			t.Cleanup(func() { unlockFailureStates.Delete(dataSource) })

			database, err := sql.Open(unlockFailureDriverName, dataSource)
			if err != nil {
				t.Fatalf("open unlock failure database: %v", err)
			}
			database.SetMaxOpenConns(1)
			t.Cleanup(func() { _ = database.Close() })
			connection, err := database.Conn(context.Background())
			if err != nil {
				t.Fatalf("open lock connection: %v", err)
			}

			if err := test.cleanup(context.Background(), connection); err == nil {
				t.Fatal("lock cleanup error = nil")
			}
			replacement, err := database.Conn(context.Background())
			if err != nil {
				t.Fatalf("open replacement connection: %v", err)
			}
			defer func() { _ = replacement.Close() }()

			opens, closes, unlocks := state.snapshot()
			if unlocks != 1 {
				t.Fatalf("unlock attempts = %d, want 1", unlocks)
			}
			if opens != 2 {
				t.Fatalf("physical connections opened = %d, want 2 after discard", opens)
			}
			if closes != 1 {
				t.Fatalf("physical connections closed = %d, want 1 after discard", closes)
			}
		})
	}
}

func TestProjectionBootstrapRecoveryExecutesEveryLifecycle(t *testing.T) {
	registerRecoveryDriver.Do(func() {
		sql.Register(recoveryDriverName, recoveryDriver{})
	})

	for _, lifecycle := range []string{"preparing", "baseline_staged", "catching_up", "activated"} {
		t.Run(lifecycle, func(t *testing.T) {
			script := projectionBootstrapRecoveryScript(lifecycle)
			dataSource := strings.ReplaceAll(t.Name(), "/", "-")
			recoveryScripts.Store(dataSource, script)
			t.Cleanup(func() { recoveryScripts.Delete(dataSource) })

			database, err := sql.Open(recoveryDriverName, dataSource)
			if err != nil {
				t.Fatalf("open recovery database: %v", err)
			}
			database.SetMaxOpenConns(2)
			t.Cleanup(func() { _ = database.Close() })
			ctx := context.Background()
			operationLock, err := database.Conn(ctx)
			if err != nil {
				t.Fatalf("open operation connection: %v", err)
			}
			t.Cleanup(func() { _ = operationLock.Close() })
			worker, err := database.Conn(ctx)
			if err != nil {
				t.Fatalf("open worker connection: %v", err)
			}
			t.Cleanup(func() { _ = worker.Close() })

			result, err := (&Coordinator{}).recoverInterruptedProjectionBootstrap(
				ctx,
				operationLock,
				worker,
				worker,
				7,
			)
			if err != nil {
				t.Fatalf("recoverInterruptedProjectionBootstrap() error = %v", err)
			}
			if lifecycle == "activated" {
				if result == nil || result.BootstrapID != testBootstrapID || result.RegistryGeneration != 7 ||
					result.CandidateSlotName != "candidate_slot" || len(result.AffectedScopes) != 1 {
					t.Fatalf("activated recovery result = %#v", result)
				}
			} else if result != nil {
				t.Fatalf("pre-activation recovery result = %#v", result)
			}
			if remaining := script.remaining(); remaining != 0 {
				t.Fatalf("recovery script has %d unconsumed steps", remaining)
			}
		})
	}
}

func TestCanonicalRuntimeReadPreservesSQLState(t *testing.T) {
	registerRecoveryDriver.Do(func() {
		sql.Register(recoveryDriverName, recoveryDriver{})
	})

	dataSource := t.Name()
	recoveryScripts.Store(dataSource, &recoveryScript{steps: []recoveryStep{{
		queryContains: "synchro_projection_bootstrap_active_stream",
		err:           operatorSQLStateError("55P03"),
	}}})
	t.Cleanup(func() { recoveryScripts.Delete(dataSource) })
	database, err := sql.Open(recoveryDriverName, dataSource)
	if err != nil {
		t.Fatalf("open runtime state database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	_, _, err = loadActiveStream(context.Background(), database)
	if err == nil {
		t.Fatal("loadActiveStream() error = nil")
	}
	var state interface{ SQLState() string }
	if !errors.As(err, &state) || state.SQLState() != "55P03" {
		t.Fatalf("loadActiveStream() did not preserve SQLSTATE: %v", err)
	}
}

const recoveryDriverName = "synchro-operator-recovery-test"

const unlockFailureDriverName = "synchro-operator-unlock-failure-test"

var (
	registerRecoveryDriver      sync.Once
	recoveryScripts             sync.Map
	registerUnlockFailureDriver sync.Once
	unlockFailureStates         sync.Map
)

type unlockFailureState struct {
	mu      sync.Mutex
	opens   int
	closes  int
	unlocks int
}

func (state *unlockFailureState) snapshot() (int, int, int) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.opens, state.closes, state.unlocks
}

type unlockFailureDriver struct{}

func (unlockFailureDriver) Open(dataSource string) (driver.Conn, error) {
	value, ok := unlockFailureStates.Load(dataSource)
	if !ok {
		return nil, errors.New("unlock failure state is unavailable")
	}
	state := value.(*unlockFailureState)
	state.mu.Lock()
	state.opens++
	state.mu.Unlock()
	return &unlockFailureConnection{state: state}, nil
}

type unlockFailureConnection struct {
	state *unlockFailureState
}

func (*unlockFailureConnection) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (connection *unlockFailureConnection) Close() error {
	connection.state.mu.Lock()
	connection.state.closes++
	connection.state.mu.Unlock()
	return nil
}

func (*unlockFailureConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unlock failure transactions are unsupported")
}

func (connection *unlockFailureConnection) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if query == "SELECT pg_catalog.pg_advisory_lock_shared($1::bigint)" {
		return driver.RowsAffected(1), nil
	}
	if query != "SELECT pg_catalog.pg_advisory_unlock_all()" {
		return nil, fmt.Errorf("unexpected unlock failure query %q", query)
	}
	connection.state.mu.Lock()
	connection.state.unlocks++
	connection.state.mu.Unlock()
	return nil, errors.New("unlock failed")
}

func (connection *unlockFailureConnection) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT pg_catalog.pg_advisory_unlock($1::bigint)" {
		return nil, fmt.Errorf("unexpected unlock failure query %q", query)
	}
	connection.state.mu.Lock()
	connection.state.unlocks++
	connection.state.mu.Unlock()
	return &unlockFailureRows{}, nil
}

type unlockFailureRows struct {
	read bool
}

func (*unlockFailureRows) Columns() []string { return []string{"unlocked"} }
func (*unlockFailureRows) Close() error      { return nil }
func (rows *unlockFailureRows) Next(values []driver.Value) error {
	if rows.read {
		return io.EOF
	}
	rows.read = true
	values[0] = false
	return nil
}

type recoveryStep struct {
	queryContains string
	columns       []string
	values        []driver.Value
	exec          bool
	err           error
}

type recoveryScript struct {
	mu    sync.Mutex
	steps []recoveryStep
}

func projectionBootstrapRecoveryScript(lifecycle string) *recoveryScript {
	barrier := ""
	affectedScopes := "[]"
	if lifecycle == "activated" {
		barrier = "0/30"
		affectedScopes = `["user:diagnostic-user"]`
	}
	interrupted := `{"present":true,"bootstrap_id":"` + testBootstrapID +
		`","source_stream_generation":"stream-1","target_stream_generation":"stream-1",` +
		`"source_registry_generation":6,"target_registry_generation":7,"old_slot_name":"active_slot",` +
		`"candidate_slot_name":"candidate_slot","target_schema_version":null,"target_schema_hash":null,` +
		`"activation_barrier":` + jsonStringOrNull(barrier) + `,"affected_scopes":` + affectedScopes +
		`,"lifecycle":"` + lifecycle + `"}`
	steps := []recoveryStep{
		{
			queryContains: "synchro_projection_bootstrap_interrupted",
			columns:       []string{"result"},
			values:        []driver.Value{[]byte(interrupted)},
		},
	}
	if lifecycle != "activated" {
		steps = append(steps, recoveryStep{
			queryContains: "synchro_abort_projection_bootstrap",
			columns:       []string{"result"},
			values: []driver.Value{[]byte(
				`{"reset_id":"` + testBootstrapID + `","candidate_slot_name":"candidate_slot"}`,
			)},
		})
	}
	steps = append(steps,
		recoveryStep{
			queryContains: "synchro_projection_bootstrap_slot_drop_state",
			columns:       []string{"result"},
			values:        []driver.Value{[]byte(`{"present":true,"active":false,"valid":true}`)},
		},
		recoveryStep{queryContains: "pg_drop_replication_slot", exec: true},
	)
	if lifecycle == "activated" {
		steps = append(steps, recoveryStep{
			queryContains: "synchro_complete_projection_bootstrap_cleanup",
			columns:       []string{"complete"},
			values:        []driver.Value{true},
		})
	}
	return &recoveryScript{steps: steps}
}

func jsonStringOrNull(value string) string {
	if value == "" {
		return "null"
	}
	return `"` + value + `"`
}

func (script *recoveryScript) next(query string, exec bool) (recoveryStep, error) {
	script.mu.Lock()
	defer script.mu.Unlock()
	if len(script.steps) == 0 {
		return recoveryStep{}, errors.New("unexpected recovery query")
	}
	step := script.steps[0]
	if step.exec != exec || !strings.Contains(query, step.queryContains) {
		return recoveryStep{}, fmt.Errorf("recovery query %q does not match %q", query, step.queryContains)
	}
	script.steps = script.steps[1:]
	return step, nil
}

func (script *recoveryScript) remaining() int {
	script.mu.Lock()
	defer script.mu.Unlock()
	return len(script.steps)
}

type recoveryDriver struct{}

func (recoveryDriver) Open(dataSource string) (driver.Conn, error) {
	value, ok := recoveryScripts.Load(dataSource)
	if !ok {
		return nil, errors.New("recovery script is unavailable")
	}
	return &recoveryConnection{script: value.(*recoveryScript)}, nil
}

type recoveryConnection struct {
	script *recoveryScript
}

func (*recoveryConnection) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (*recoveryConnection) Close() error {
	return nil
}

func (*recoveryConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("recovery transactions are unsupported")
}

func (connection *recoveryConnection) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	step, err := connection.script.next(query, false)
	if err != nil {
		return nil, err
	}
	if step.err != nil {
		return nil, step.err
	}
	return &recoveryRows{columns: step.columns, values: step.values}, nil
}

func (connection *recoveryConnection) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if _, err := connection.script.next(query, true); err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}

type recoveryRows struct {
	columns []string
	values  []driver.Value
	read    bool
}

func (rows *recoveryRows) Columns() []string {
	return rows.columns
}

func (*recoveryRows) Close() error {
	return nil
}

func (rows *recoveryRows) Next(destination []driver.Value) error {
	if rows.read {
		return io.EOF
	}
	rows.read = true
	copy(destination, rows.values)
	return nil
}

func stringPointer(value string) *string {
	return &value
}

type operatorSQLStateError string

func (errorCode operatorSQLStateError) Error() string {
	return "operator SQLSTATE " + string(errorCode)
}

func (errorCode operatorSQLStateError) SQLState() string {
	return string(errorCode)
}
