package swift

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/scenarios"
)

var scopeEmptyPullTestClient = Client{Key: "client-a", UserID: "user-a", ClientID: "client-a", DatabaseKey: "scope-empty-pull-client-a"}

var scopeEmptyPullTestEvidence = scopeEmptyPullEvidence{
	grantedScope: "cf:global",
	rebuildID:    "rebuild-granted",
	tableName:    "cf_global_items",
	primaryField: "id",
	recordID:     "row-granted",
}

const scopeEmptyPullTestCursor = "cursor-granted"

func loadScopeEmptyPullScenario(t *testing.T) (scenarios.Scenario, scopeEmptyPullOperations) {
	t.Helper()
	scenario, err := scenarios.LoadFile(context.Background(), filepath.Join("..", ".."), "conformance/scenarios/server/scope-empty-pull-001.json")
	if err != nil {
		t.Fatalf("load scope-empty-pull scenario: %v", err)
	}
	operations, err := loadScopeEmptyPullOperations(scenario, scopeEmptyPullTestClient)
	if err != nil {
		t.Fatalf("bind scope-empty-pull operations: %v", err)
	}
	return scenario, operations
}

func scopeEmptyPullTestPull(sequence uint64, rebuildScopes int) transportObservation {
	generation := int64(1)
	version := int64(1)
	scopes := 0
	limit := 100
	complete := true
	return transportObservation{
		Sequence: sequence, OperationClass: "pull", StatusCode: 200, DurationNanoseconds: 1,
		CursorFingerprints: []string{}, CursorFingerprintsComplete: &complete,
		RequestFacts: &transportRequestFacts{ClientGeneration: &generation, SchemaVersion: 1, SchemaHash: strings.Repeat("a", 64), ScopeSetVersion: &version, ScopeCount: &scopes, Limit: &limit},
		PullResponseFacts: &transportPullResponseFacts{
			RebuildScopeCount: rebuildScopes, ChecksumCount: rebuildScopes,
			ScopeCursorFingerprints: []string{}, ScopeCursorFingerprintsComplete: true,
		},
	}
}

func scopeEmptyPullTestStart() SynchronizationResult {
	protocol := 3
	version := int64(0)
	scopes := 0
	connect := transportObservation{
		Sequence: 1, OperationClass: "connect", StatusCode: 200, DurationNanoseconds: 1,
		RequestFacts: &transportRequestFacts{ProtocolVersion: &protocol, ScopeSetVersion: &version, ScopeCount: &scopes},
	}
	return SynchronizationResult{Completion: "idle", transportObservations: []transportObservation{connect, scopeEmptyPullTestPull(2, 0)}}
}

func scopeEmptyPullTestSync() SynchronizationResult {
	generation := int64(1)
	limit := 100
	present := false
	scope := cursorFingerprint(scopeEmptyPullTestEvidence.grantedScope)
	rebuildID := cursorFingerprint(scopeEmptyPullTestEvidence.rebuildID)
	finalCursor := cursorFingerprint(scopeEmptyPullTestCursor)
	rebuild := transportObservation{
		Sequence: 4, OperationClass: "rebuild", StatusCode: 200, DurationNanoseconds: 1,
		RequestFacts: &transportRequestFacts{ClientGeneration: &generation, SchemaVersion: 1, SchemaHash: strings.Repeat("a", 64), Limit: &limit, ScopeFingerprint: &scope, RebuildIDFingerprint: &rebuildID, CursorPresent: &present},
		RebuildResponseFacts: &transportRebuildResponseFacts{
			RecordCount: 1, HasFinalScopeCursor: true, HasChecksum: true,
			ScopeFingerprint: scope, FinalScopeCursorFingerprint: &finalCursor,
		},
	}
	return SynchronizationResult{Completion: "idle", transportObservations: []transportObservation{scopeEmptyPullTestPull(3, 1), rebuild}}
}

func scopeEmptyPullTestFinal() runnerResult {
	cursor := scopeEmptyPullTestCursor
	checksum := strings.Repeat("c", 64)
	rows := 1
	return runnerResult{
		ApplicationRowCount: &rows,
		ApplicationRows:     []map[string]json.RawMessage{{"id": json.RawMessage(`"row-granted"`)}},
		ScopeStates:         []scopeStateRecord{{ScopeID: "cf:global", Cursor: &cursor, Checksum: &checksum}},
		ScopeRows:           []scopeRowRecord{{ScopeID: "cf:global", TableName: "cf_global_items", RecordID: "row-granted"}},
	}
}

func TestScopeEmptyPullAcceptsAuthoredFlow(t *testing.T) {
	scenario, operations := loadScopeEmptyPullScenario(t)
	start := scopeEmptyPullTestStart()
	if _, err := mapTransportOperations(RequestOperations{operations.connect, operations.firstPull}, start.transportObservations, runnerResult{}); err != nil {
		t.Fatalf("map authored start call: %v", err)
	}
	version, err := validateScopeEmptyPullStart(scenario, operations, start)
	if err != nil {
		t.Fatalf("validate authored start call: %v", err)
	}
	synced := scopeEmptyPullTestSync()
	if _, err := mapTransportOperations(RequestOperations{operations.syncPull, operations.rebuild}, synced.transportObservations, runnerResult{}); err != nil {
		t.Fatalf("map authored sync call: %v", err)
	}
	if err := validateScopeEmptyPullSync(scenario, operations, synced, version, scopeEmptyPullTestEvidence); err != nil {
		t.Fatalf("validate authored sync call: %v", err)
	}
	if err := validateScopeEmptyPullFinalState(scopeEmptyPullTestFinal(), synced, scopeEmptyPullTestEvidence); err != nil {
		t.Fatalf("validate authored final state: %v", err)
	}
}

// CTRL-SCOPE-009 restores the guard that returns from the pull loop when the
// local scope set is empty. The client then sends no pull in either call.
func TestScopeEmptyPullRejectsEmptyScopeGuard(t *testing.T) {
	scenario, operations := loadScopeEmptyPullScenario(t)
	guardedStart := scopeEmptyPullTestStart()
	guardedStart.transportObservations = guardedStart.transportObservations[:1]
	if _, err := mapTransportOperations(RequestOperations{operations.connect, operations.firstPull}, guardedStart.transportObservations, runnerResult{}); err == nil {
		t.Fatal("platform mapped a start call with no pull")
	}
	if _, err := validateScopeEmptyPullStart(scenario, operations, guardedStart); err == nil {
		t.Fatal("start call with no pull passed")
	}
	guardedSync := SynchronizationResult{Completion: "idle"}
	if _, err := mapTransportOperations(RequestOperations{operations.syncPull, operations.rebuild}, guardedSync.transportObservations, runnerResult{}); err == nil {
		t.Fatal("platform mapped a sync cycle with no request")
	}
	if err := validateScopeEmptyPullSync(scenario, operations, guardedSync, 1, scopeEmptyPullTestEvidence); err == nil {
		t.Fatal("sync cycle with no pull passed")
	}
	if err := validateScopeEmptyPullFinalState(runnerResult{}, guardedSync, scopeEmptyPullTestEvidence); err == nil {
		t.Fatal("final state with no granted scope passed")
	}
}

func TestScopeEmptyPullRejectsDivergentCalls(t *testing.T) {
	scenario, operations := loadScopeEmptyPullScenario(t)
	for _, test := range []struct {
		name   string
		change func(*SynchronizationResult)
	}{
		{"known scope on connect", func(result *SynchronizationResult) {
			scopes := 1
			result.transportObservations[0].RequestFacts.ScopeCount = &scopes
		}},
		{"scope in pull", func(result *SynchronizationResult) {
			scopes := 1
			result.transportObservations[1].RequestFacts.ScopeCount = &scopes
		}},
		{"added scope at start", func(result *SynchronizationResult) {
			result.transportObservations[1].PullResponseFacts.RebuildScopeCount = 1
		}},
		{"error completion", func(result *SynchronizationResult) { result.Completion = "error" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			start := scopeEmptyPullTestStart()
			test.change(&start)
			if _, err := validateScopeEmptyPullStart(scenario, operations, start); err == nil {
				t.Fatal("divergent start call passed")
			}
		})
	}
	for _, test := range []struct {
		name   string
		change func(*SynchronizationResult)
	}{
		{"reconnect", func(result *SynchronizationResult) {
			start := scopeEmptyPullTestStart()
			result.transportObservations = append([]transportObservation{start.transportObservations[0]}, result.transportObservations...)
		}},
		{"no added scope", func(result *SynchronizationResult) {
			result.transportObservations[0].PullResponseFacts.RebuildScopeCount = 0
		}},
		{"missing rebuild", func(result *SynchronizationResult) {
			result.transportObservations = result.transportObservations[:1]
		}},
		{"changed scope set version", func(result *SynchronizationResult) {
			version := int64(2)
			result.transportObservations[0].RequestFacts.ScopeSetVersion = &version
		}},
		{"other rebuild scope", func(result *SynchronizationResult) {
			scope := cursorFingerprint("user:user-a")
			result.transportObservations[1].RequestFacts.ScopeFingerprint = &scope
		}},
		{"empty rebuild", func(result *SynchronizationResult) {
			result.transportObservations[1].RebuildResponseFacts.RecordCount = 0
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synced := scopeEmptyPullTestSync()
			test.change(&synced)
			if err := validateScopeEmptyPullSync(scenario, operations, synced, 1, scopeEmptyPullTestEvidence); err == nil {
				t.Fatal("divergent sync cycle passed")
			}
		})
	}
}

func TestScopeEmptyPullRejectsIncompleteFinalState(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*runnerResult)
	}{
		{"no scope", func(result *runnerResult) { result.ScopeStates = nil }},
		{"other cursor", func(result *runnerResult) {
			cursor := "cursor-other"
			result.ScopeStates[0].Cursor = &cursor
		}},
		{"no provenance", func(result *runnerResult) { result.ScopeRows = nil }},
		{"no row", func(result *runnerResult) {
			rows := 0
			result.ApplicationRowCount = &rows
			result.ApplicationRows = nil
		}},
		{"other row", func(result *runnerResult) {
			result.ApplicationRows[0]["id"] = json.RawMessage(`"row-other"`)
		}},
		{"unfinished rebuild", func(result *runnerResult) { result.RebuildAttempts = []rebuildAttemptRecord{{ScopeID: "cf:global"}} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			final := scopeEmptyPullTestFinal()
			test.change(&final)
			if err := validateScopeEmptyPullFinalState(final, scopeEmptyPullTestSync(), scopeEmptyPullTestEvidence); err == nil {
				t.Fatal("incomplete final state passed")
			}
		})
	}
}

func TestScopeEmptyPullBindingsRejectReconnectCycle(t *testing.T) {
	scenario, _ := loadScopeEmptyPullScenario(t)
	for index := range scenario.Steps {
		if scenario.Steps[index].ID != "STEP-SCOPE-EMPTY-PULL-SYNC-PULL-001" {
			continue
		}
		binding := *scenario.Steps[index].NativeBinding
		binding.Method = "start"
		scenario.Steps[index].NativeBinding = &binding
	}
	if _, err := loadScopeEmptyPullOperations(scenario, scopeEmptyPullTestClient); err == nil {
		t.Fatal("sync cycle bound to a reconnecting method passed")
	}
}
