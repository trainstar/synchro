package reactnative

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/faults"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	pendingCycleScenarioPath                                  = "conformance/scenarios/performance/pending-cycle-001.json"
	pendingCycleScenarioID                                    = "SCN-PERF-PENDING-CYCLE-001"
	pendingCycleUnprotectedCommitStepID      scenarios.StepID = "STEP-PERF-PENDING-CYCLE-UNPROTECTED-COMMIT-001"
	pendingCycleLocalWriteStepID             scenarios.StepID = "STEP-PERF-PENDING-CYCLE-001"
	pendingCyclePushStepID                   scenarios.StepID = "STEP-PERF-PENDING-CYCLE-002"
	pendingCycleCapturePendingStepID         scenarios.StepID = "STEP-PERF-PENDING-CYCLE-CAPTURE-PENDING-001"
	pendingCycleUnprotectedMaterializeStepID scenarios.StepID = "STEP-PERF-PENDING-CYCLE-UNPROTECTED-MATERIALIZE-001"
	pendingCycleMaterializeStepID            scenarios.StepID = "STEP-PERF-PENDING-CYCLE-MATERIALIZE-001"
	pendingCyclePullStepID                   scenarios.StepID = "STEP-PERF-PENDING-CYCLE-003"
	pendingCycleCallID                                        = "pending_push"

	// Installing the current schema performs connect, rebuild, and pull.
	pendingCycleBootstrapRequests = 3

	// A pull before its push materializes returns capture_pending.
	pendingCycleCapturePendingStatus = 503
)

var pendingCycleStepOrder = []scenarios.StepID{
	pendingCycleUnprotectedCommitStepID,
	pendingCycleLocalWriteStepID,
	pendingCyclePushStepID,
	pendingCycleCapturePendingStepID,
	pendingCycleUnprotectedMaterializeStepID,
	pendingCycleMaterializeStepID,
	pendingCyclePullStepID,
}

var pendingCycleAliasNames = []string{
	"pending-mutation",
	"pending-batch",
	"client-generation-one",
	"current-schema",
	"items-table",
	"pending-row-primary-key",
	"unprotected-row-primary-key",
	"scope-a",
	"scope-set-version-one",
}

// LoadPendingCycleScenario loads only the authored pending-cycle scenario.
func LoadPendingCycleScenario(ctx context.Context, repoRoot string) (scenarios.Scenario, error) {
	scenario, err := scenarios.LoadFile(ctx, repoRoot, pendingCycleScenarioPath)
	if err != nil {
		return scenarios.Scenario{}, fmt.Errorf("load React Native pending-cycle scenario: %w", err)
	}
	if err := ValidatePendingCycleScenario(scenario); err != nil {
		return scenarios.Scenario{}, err
	}
	return scenario, nil
}

// ValidatePendingCycleScenario rejects changes to the closed RN pending-cycle contract.
func ValidatePendingCycleScenario(scenario scenarios.Scenario) error {
	if string(scenario.ID) != pendingCycleScenarioID || len(scenario.Model.Setup) != 1 ||
		scenarios.OperationKey(scenario.Model.Setup[0]) != "model/install-current-contract" {
		return errors.New("React Native pending-cycle scenario contract is invalid")
	}
	if !orderedIdentifiersEqual(scenario.RequirementIDs, []string{
		"SYNC-MUTATION-002", "SYNC-CONFLICT-001", "SYNC-CONFLICT-002", "SYNC-TIME-002", "SYNC-VOCAB-001",
		"SYNC-LOCALSQL-001", "SYNC-CRUD-001", "SYNC-APPLY-001", "SYNC-VERSION-002", "SYNC-SCOPE-006",
		"SYNC-CLEANUP-001", "SYNC-CURSOR-003", "SYNC-BOUNDARY-003", "SYNC-CRUD-002",
	}) {
		return errors.New("React Native pending-cycle requirement set changed")
	}
	if len(scenario.Steps) != len(pendingCycleStepOrder) {
		return errors.New("React Native pending-cycle step set changed")
	}
	for index, step := range scenario.Steps {
		if step.ID != pendingCycleStepOrder[index] || step.NativeBinding == nil {
			return errors.New("React Native pending-cycle step order or binding changed")
		}
	}
	if len(scenario.NativeLifecycleBoundaries) != 0 || len(scenario.NativeIdentityAliases) != len(pendingCycleAliasNames) {
		return errors.New("React Native pending-cycle lifecycle or identity contract changed")
	}
	aliases := make(map[string]struct{}, len(scenario.NativeIdentityAliases))
	for _, alias := range scenario.NativeIdentityAliases {
		if alias.Alias == "" {
			return errors.New("React Native pending-cycle identity alias is invalid")
		}
		if _, duplicate := aliases[alias.Alias]; duplicate {
			return errors.New("React Native pending-cycle identity alias is duplicated")
		}
		aliases[alias.Alias] = struct{}{}
	}
	for _, name := range pendingCycleAliasNames {
		if _, found := aliases[name]; !found {
			return fmt.Errorf("React Native pending-cycle identity alias %q is absent", name)
		}
	}
	expectedSteps := []struct {
		operation  string
		kind       string
		stage      string
		method     string
		completion string
	}{
		{"model/commit-source-transaction", "controller", "", "", ""},
		{"local/write", "local-write", "", "", ""},
		{"push/submit", "public-call", "begin", "start", ""},
		{"pull/request-page", "public-call", "await-step", "", ""},
		{"process/materialize-source-transaction", "controller", "", "", ""},
		{"process/materialize-source-transaction", "controller", "", "", ""},
		{"pull/request-page", "public-call", "await-call", "", "idle"},
	}
	for index, expected := range expectedSteps {
		step := scenario.Steps[index]
		binding := step.NativeBinding
		if scenarios.OperationKey(step.Operation) != expected.operation || binding.Kind != expected.kind ||
			binding.Stage != expected.stage || binding.Method != expected.method || binding.Completion != expected.completion ||
			step.ExpectedOutcome.Disposition != "success" {
			return fmt.Errorf("React Native pending-cycle step %s binding changed", step.ID)
		}
		if expected.kind == "public-call" && (binding.CallID == nil || string(*binding.CallID) != pendingCycleCallID) {
			return fmt.Errorf("React Native pending-cycle step %s call identity changed", step.ID)
		}
	}
	capturePendingCode := "capture_pending"
	expectedWires := []struct {
		stepID       scenarios.StepID
		contractCase string
		status       int
		errorCode    *string
		retryable    bool
	}{
		{pendingCyclePushStepID, "push_success", http.StatusOK, nil, false},
		{pendingCycleCapturePendingStepID, "capture_pending", pendingCycleCapturePendingStatus, &capturePendingCode, true},
		{pendingCyclePullStepID, "pull_success", http.StatusOK, nil, false},
	}
	if len(scenario.WireExpectations) != len(expectedWires) {
		return errors.New("React Native pending-cycle wire expectation set changed")
	}
	for index, expected := range expectedWires {
		wire := scenario.WireExpectations[index]
		sameCode := wire.ErrorCode == nil && expected.errorCode == nil ||
			wire.ErrorCode != nil && expected.errorCode != nil && *wire.ErrorCode == *expected.errorCode
		if wire.StepID != expected.stepID || wire.ContractCase != expected.contractCase || wire.HTTPStatus != expected.status ||
			wire.Retryable != expected.retryable || !sameCode {
			return fmt.Errorf("React Native pending-cycle wire expectation %s changed", expected.stepID)
		}
	}
	semantic, performance := false, false
	claimSpecs := map[string]struct{ requirement, control string }{
		"ASSERT-PERF-PENDING-CYCLE-CAS-001":      {"SYNC-CONFLICT-001", "CTRL-CONFLICT-001"},
		"ASSERT-PERF-PENDING-CYCLE-LOCALSQL-001": {"SYNC-LOCALSQL-001", "CTRL-LOCALSQL-001"},
		"ASSERT-PERF-PENDING-CYCLE-CRUD-001":     {"SYNC-CRUD-001", "CTRL-CRUD-001"},
		"ASSERT-PERF-PENDING-CYCLE-APPLY-001":    {"SYNC-APPLY-001", "CTRL-APPLY-001"},
		"ASSERT-PERF-PENDING-CYCLE-VERSION-002":  {"SYNC-VERSION-002", "CTRL-VERSION-002"},
		"ASSERT-PERF-PENDING-CYCLE-SCOPE-006":    {"SYNC-SCOPE-006", "CTRL-SCOPE-006"},
		"ASSERT-PERF-PENDING-CYCLE-CLEANUP-001":  {"SYNC-CLEANUP-001", "CTRL-CLEANUP-001"},
		"ASSERT-PERF-PENDING-CYCLE-CURSOR-003":   {"SYNC-CURSOR-003", "CTRL-CURSOR-003"},
		"ASSERT-PERF-PENDING-CYCLE-BOUNDARY-003": {"SYNC-BOUNDARY-003", "CTRL-BOUNDARY-003"},
		"ASSERT-PERF-PENDING-CYCLE-CRUD-002":     {"SYNC-CRUD-002", "CTRL-CRUD-002"},
	}
	matchedClaims := make(map[string]int, len(claimSpecs))
	for _, assertion := range scenario.Assertions {
		switch assertion.ID {
		case "ASSERT-PERF-PENDING-CYCLE-SEMANTIC-001":
			semantic = assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Oracle.ExpectedSource == "authored-model"
		case "ASSERT-PERF-PENDING-CYCLE-PERFORMANCE-001":
			performance = assertion.Predicate.ContractPredicate == "performance-measurement" && assertion.Oracle.ExpectedSource == "authored-model"
		}
		if spec, found := claimSpecs[string(assertion.ID)]; found &&
			orderedIdentifiersEqual(assertion.RequirementIDs, []string{spec.requirement}) &&
			orderedIdentifiersEqual(assertion.ExpectationIDs, []string{"EXPECT-PERF-PENDING-CYCLE-SEMANTIC-001"}) &&
			assertion.Predicate.ContractPredicate == "wire-outcome" && assertion.Predicate.Name == "canonical-wire-outcome" &&
			assertion.Oracle.Kind == "wire-contract" && assertion.Oracle.ExpectedSource == "authored-model" &&
			assertion.Oracle.ObservedSource == "system-under-test" &&
			orderedIdentifiersEqual(assertion.DetectsControlIDs, []string{spec.control}) {
			matchedClaims[string(assertion.ID)]++
		}
	}
	if !semantic || !performance {
		return errors.New("React Native pending-cycle assertion contract changed")
	}
	for id := range claimSpecs {
		if matchedClaims[id] != 1 {
			return fmt.Errorf("React Native pending-cycle assertion %s changed", id)
		}
	}
	obligations := map[string]int{}
	for _, obligation := range scenario.ProofObligations {
		id := string(obligation.ObligationID)
		switch id {
		case "OBL-PERF-PENDING-CYCLE-RN-IOS-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-IOS-CURRENT-001", "test-rn-e2e-ios", "", "") && pendingCycleNativeClaimsMatch(obligation) {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-RN-ANDROID-CURRENT-001":
			if proofTargetMatches(obligation, "native-e2e", "SUP-RN-ANDROID-CURRENT-001", "test-rn-e2e-android", "", "") && pendingCycleNativeClaimsMatch(obligation) {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-PENDING-CYCLE-001", "CTRL-MUTATION-002") {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-SCOPE-006-FAULT-001":
			if proofTargetMatches(obligation, "fault-injection", "SUP-MACOS-CURRENT-001", "test-swift", "FPL-PERF-PENDING-CYCLE-SCOPE-006", "CTRL-SCOPE-006") {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-SCOPE-006-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-conformance", "FPL-PERF-PENDING-CYCLE-SCOPE-006", "CTRL-SCOPE-006") {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-CONFLICT-001-RM-001":
			if proofTargetMatches(obligation, "reference-model", "", "test-conformance", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-CONFLICT-001-PG-LINUX-X64-001":
			if proofTargetMatches(obligation, "server-black-box", "SUP-PG-LINUX-X64-001", "test-blackbox", "", "") {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-CONFLICT-001-FAULT-LINUX-X64-001":
			if proofTargetMatches(obligation, "fault-injection", "SUP-PG-LINUX-X64-001", "test-blackbox", "FPL-PERF-PENDING-CYCLE-CONFLICT-ATOMIC-001", "CTRL-CONFLICT-001") {
				obligations[id]++
			}
		case "OBL-PERF-PENDING-CYCLE-CONFLICT-001-CONTROL-001":
			if proofTargetMatches(obligation, "negative-control", "", "test-integration-mutants", "FPL-PERF-PENDING-CYCLE-CONFLICT-ATOMIC-001", "CTRL-CONFLICT-001") {
				obligations[id]++
			}
		}
	}
	if obligations["OBL-PERF-PENDING-CYCLE-RN-IOS-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-RN-ANDROID-CURRENT-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-CONTROL-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-SCOPE-006-FAULT-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-SCOPE-006-CONTROL-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-CONFLICT-001-RM-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-CONFLICT-001-PG-LINUX-X64-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-CONFLICT-001-FAULT-LINUX-X64-001"] != 1 ||
		obligations["OBL-PERF-PENDING-CYCLE-CONFLICT-001-CONTROL-001"] != 1 {
		return errors.New("React Native pending-cycle proof obligations are invalid")
	}
	return nil
}

func pendingCycleNativeClaimsMatch(obligation scenarios.ProofObligation) bool {
	return orderedIdentifiersEqual(obligation.RequirementIDs, []string{
		"SYNC-MUTATION-002", "SYNC-LOCALSQL-001", "SYNC-CRUD-001", "SYNC-APPLY-001", "SYNC-VERSION-002",
		"SYNC-SCOPE-006", "SYNC-CLEANUP-001", "SYNC-CURSOR-003", "SYNC-BOUNDARY-003", "SYNC-CRUD-002",
	}) && orderedIdentifiersEqual(obligation.AssertionIDs, []string{
		"ASSERT-PERF-PENDING-CYCLE-SEMANTIC-001", "ASSERT-PERF-PENDING-CYCLE-PERFORMANCE-001",
		"ASSERT-PERF-PENDING-CYCLE-LOCALSQL-001", "ASSERT-PERF-PENDING-CYCLE-CRUD-001",
		"ASSERT-PERF-PENDING-CYCLE-APPLY-001", "ASSERT-PERF-PENDING-CYCLE-VERSION-002",
		"ASSERT-PERF-PENDING-CYCLE-SCOPE-006", "ASSERT-PERF-PENDING-CYCLE-CLEANUP-001", "ASSERT-PERF-PENDING-CYCLE-CURSOR-003",
		"ASSERT-PERF-PENDING-CYCLE-BOUNDARY-003", "ASSERT-PERF-PENDING-CYCLE-CRUD-002",
	})
}

// PendingCycleCoordinatorConfig configures one authenticated RN pending-cycle sidecar.
type PendingCycleCoordinatorConfig struct {
	Scenario   scenarios.Scenario
	Harness    *blackbox.Harness
	Controller *blackbox.NativeController
	Platform   string
	ServerURL  string
	AuthToken  string
	AppVersion string
	Database   string
}

// PendingCycleCoordinator is the command sidecar for one RN pending-cycle run.
type PendingCycleCoordinator struct {
	config    PendingCycleCoordinatorConfig
	listener  net.Listener
	server    *http.Server
	token     string
	adapter   string
	upstream  string
	database  string
	transport *http.Client

	steps      map[scenarios.StepID]scenarios.Step
	identities []scenarios.NativeIdentityAlias
	runtimeIDs map[string]json.RawMessage
	userID     string
	clientID   string
	clientKey  string
	tableName  string
	primaryKey string
	target     scenarios.PendingCycleNativeTarget
	updated    string
	updateStep scenarios.PendingCycleNativeCRUDStep
	deleteStep scenarios.PendingCycleNativeCRUDStep

	proxyMu                 sync.Mutex
	faultArmed              bool
	faultPushes             int
	proxyFailureCause       error
	initialPushDone         chan struct{}
	capturePendingDone      chan struct{}
	retryPullDone           chan struct{}
	materializationDone     chan struct{}
	initialPushErr          error
	capturePendingErr       error
	retryPullErr            error
	initialPushRecorded     bool
	capturePendingRecorded  bool
	retryPullRecorded       bool
	materializationSignaled bool

	mu        sync.Mutex
	prepared  bool
	closed    bool
	completed bool
	failed    error
	stage     pendingCycleStage
	nextSeq   uint64
	process   *actionProcessIdentity
	resumeWAL func(context.Context) error
	captures  map[pendingCycleStage]finalCapture
	states    map[pendingCycleStage]scenarios.PendingCycleNativeState
	result    PendingCycleCoordinatorResult
}

type pendingCycleStage uint8

const (
	pendingCycleStageOpen pendingCycleStage = iota
	pendingCycleStageOpened
	pendingCycleStageBootstrapped
	pendingCycleStageBeforeWrite
	pendingCycleStageStoppedForInitialWrite
	pendingCycleStageInitialLocalWrite
	pendingCycleStageAfterInitialWrite
	pendingCycleStageInitialPushBegun
	pendingCycleStageInitialPushObserved
	pendingCycleStageCapturePendingObserved
	pendingCycleStageAfterInitialPush
	pendingCycleStageBeforePull
	pendingCycleStageInitialPull
	pendingCycleStageAfterInitialPull
	pendingCycleStageDeviceRestarted
	pendingCycleStageRestartOpened
	pendingCycleStageAfterRestart
	pendingCycleStageStoppedForUpdate
	pendingCycleStageUpdateLocalWrite
	pendingCycleStageBeforeCleanup
	pendingCycleStageCleanupCall
	pendingCycleStageAfterCleanup
	pendingCycleStageStoppedForUpdatePush
	pendingCycleStageUpdatePushed
	pendingCycleStageAfterUpdate
	pendingCycleStageStoppedForDelete
	pendingCycleStageDeleteLocalWrite
	pendingCycleStageBeforeDelete
	pendingCycleStageDeletePushed
	pendingCycleStageAfterDelete
	pendingCycleStageComplete
)

// PendingCycleCoordinatorResult contains validated server and native identity evidence.
type PendingCycleCoordinatorResult struct {
	ServerFacts        scenarios.StateFacts
	IdentityResolution []blackbox.NativeIdentityResolution
	Evidence           scenarios.PendingCycleNativeEvidence
}

// NewPendingCycleCoordinator creates an authenticated host-loopback listener.
func NewPendingCycleCoordinator(config PendingCycleCoordinatorConfig) (*PendingCycleCoordinator, error) {
	if err := ValidatePendingCycleScenario(config.Scenario); err != nil {
		return nil, err
	}
	if config.Platform != "ios" && config.Platform != "android" {
		return nil, errors.New("React Native pending-cycle coordinator platform must be ios or android")
	}
	identity, err := extractPendingCycleClientIdentity(config.Scenario)
	if err != nil {
		return nil, err
	}
	if config.AppVersion == "" {
		config.AppVersion = defaultAppVersion
	}
	if config.AuthToken == "" && config.Harness == nil {
		return nil, errors.New("React Native pending-cycle coordinator auth token is required")
	}
	serverURL := config.ServerURL
	if serverURL == "" && config.Harness != nil {
		serverURL = config.Harness.AdapterURL()
	}
	upstream, err := nativeAdapterURL(serverURL, "ios")
	if err != nil {
		return nil, err
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, errors.New("create React Native pending-cycle coordinator capability")
	}
	database := config.Database
	if database == "" {
		database, err = randomDatabaseNameWithPrefix("rn-pending-cycle-")
		if err != nil {
			return nil, errors.New("create React Native pending-cycle private database name")
		}
	}
	if !validDatabaseName(database) {
		return nil, errors.New("React Native pending-cycle database name is invalid")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("listen for React Native pending-cycle coordinator")
	}
	adapterURL, err := nativeAdapterURL("http://"+listener.Addr().String(), config.Platform)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	steps := make(map[scenarios.StepID]scenarios.Step, len(config.Scenario.Steps))
	for _, step := range config.Scenario.Steps {
		steps[step.ID] = step
	}
	coordinator := &PendingCycleCoordinator{
		config: config, listener: listener, token: token, adapter: adapterURL, upstream: upstream, database: database,
		transport: &http.Client{Timeout: 2 * time.Minute},
		steps:     steps, identities: append([]scenarios.NativeIdentityAlias(nil), config.Scenario.NativeIdentityAliases...),
		runtimeIDs: make(map[string]json.RawMessage), userID: identity.userID, clientID: identity.clientID, clientKey: identity.clientID,
		nextSeq: 1, captures: make(map[pendingCycleStage]finalCapture), states: make(map[pendingCycleStage]scenarios.PendingCycleNativeState),
		initialPushDone: make(chan struct{}), capturePendingDone: make(chan struct{}), retryPullDone: make(chan struct{}), materializationDone: make(chan struct{}),
		server: &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 2 * time.Minute, WriteTimeout: 2 * time.Minute, IdleTimeout: 30 * time.Second},
	}
	coordinator.server.Handler = coordinator
	return coordinator, nil
}

// Prepare installs the authored model and binds initial runtime identities.
func (c *PendingCycleCoordinator) Prepare(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errCoordinatorUnavailable
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCoordinatorUnavailable
	}
	if c.prepared {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.config.AuthToken == "" && c.config.Harness != nil {
		token, err := c.config.Harness.NativeBearerToken(ctx, c.userID, time.Now())
		if err != nil {
			return errors.New("mint React Native pending-cycle adapter bearer token")
		}
		c.config.AuthToken = token
	}
	if c.config.Controller == nil || c.config.Harness == nil {
		return errors.New("React Native pending-cycle coordinator dependencies are unavailable")
	}
	if err := c.config.Controller.Install(ctx, c.config.Scenario.Model.Setup[0]); err != nil {
		return fmt.Errorf("install React Native pending-cycle contract: %w", err)
	}
	resumeWAL, err := c.config.Controller.PauseWALMaterialization(ctx)
	if err != nil {
		return fmt.Errorf("pause React Native pending-cycle WAL materialization: %w", err)
	}
	c.mu.Lock()
	c.resumeWAL = resumeWAL
	c.mu.Unlock()
	prepared := false
	defer func() {
		if prepared {
			return
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.releaseWALMaterialization(cleanupContext)
	}()
	unprotectedCommit := c.steps[pendingCycleUnprotectedCommitStepID].Operation
	result, err := c.config.Controller.ApplyStep(ctx, unprotectedCommit)
	if err != nil || result.Disposition != c.steps[pendingCycleUnprotectedCommitStepID].ExpectedOutcome.Disposition {
		return fmt.Errorf("commit React Native pending-cycle unprotected row: %w", nativeResultError(err, result.Disposition))
	}
	alias, runtimeRecordID, err := c.unprotectedRowIdentity()
	if err != nil {
		return err
	}
	authoredRecordID, unprotectedValue, err := scenarios.PendingCycleUnprotectedRowTarget(unprotectedCommit, []scenarios.NativeIdentityAlias{alias}, runtimeRecordID)
	if err != nil {
		return err
	}
	localWrite, err := c.config.Controller.ApplicationWrite(c.steps[pendingCycleLocalWriteStepID].Operation)
	if err != nil {
		return fmt.Errorf("bind React Native pending mutation to the application schema: %w", err)
	}
	localStep := c.steps[pendingCycleLocalWriteStepID]
	localStep.Operation = localWrite
	c.steps[pendingCycleLocalWriteStepID] = localStep
	target, err := pendingCycleNativeTarget(localWrite)
	if err != nil {
		return fmt.Errorf("decode React Native pending-cycle runtime target: %w", err)
	}
	c.target = target
	c.target.UnprotectedAuthoredRecordID = authoredRecordID
	c.target.UnprotectedRecordID = runtimeRecordID
	c.target.UnprotectedValue = unprotectedValue
	c.updated = target.Value + "-updated"
	c.tableName = target.TableName
	c.primaryKey = target.PrimaryKeyField
	if err := c.bindRuntimeIdentities(false); err != nil {
		return err
	}
	c.mu.Lock()
	c.prepared = true
	c.mu.Unlock()
	prepared = true
	return nil
}

// Serve serves the sidecar until the context ends or the listener closes.
func (c *PendingCycleCoordinator) Serve(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errCoordinatorUnavailable
	}
	if err := c.Prepare(ctx); err != nil {
		return err
	}
	shutdown := make(chan struct{})
	defer close(shutdown)
	go func() {
		select {
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.Close(shutdownContext)
			cancel()
		case <-shutdown:
		}
	}()
	err := c.server.Serve(c.listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (c *PendingCycleCoordinator) Handler() http.Handler { return c }

// URL returns the host-loopback sidecar URL for every platform.
func (c *PendingCycleCoordinator) URL() string {
	if c == nil || c.listener == nil {
		return ""
	}
	return "http://" + c.listener.Addr().String()
}

func (c *PendingCycleCoordinator) Token() string {
	if c == nil {
		return ""
	}
	return c.token
}

func (c *PendingCycleCoordinator) Completed() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.completed && c.failed == nil
}

func (c *PendingCycleCoordinator) Result() (PendingCycleCoordinatorResult, error) {
	if c == nil {
		return PendingCycleCoordinatorResult{}, errCoordinatorUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed != nil {
		return PendingCycleCoordinatorResult{}, c.failed
	}
	if !c.completed {
		return PendingCycleCoordinatorResult{}, errors.New("React Native pending-cycle coordinator has not completed")
	}
	return c.result, nil
}

func (c *PendingCycleCoordinator) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return errCoordinatorUnavailable
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	resumeWAL := c.resumeWAL
	c.resumeWAL = nil
	c.mu.Unlock()
	c.signalInitialPullMaterialized()
	cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	walErr := releasePendingCycleWAL(cleanupContext, resumeWAL)
	cancel()
	shutdownErr := c.server.Shutdown(ctx)
	listenErr := c.listener.Close()
	if errors.Is(listenErr, net.ErrClosed) {
		listenErr = nil
	}
	return errors.Join(walErr, shutdownErr, listenErr)
}

func (c *PendingCycleCoordinator) releaseWALMaterialization(ctx context.Context) error {
	if c == nil || ctx == nil {
		return errCoordinatorUnavailable
	}
	c.mu.Lock()
	resumeWAL := c.resumeWAL
	c.resumeWAL = nil
	c.mu.Unlock()
	return releasePendingCycleWAL(ctx, resumeWAL)
}

func (c *PendingCycleCoordinator) releaseWALMaterializationLocked(ctx context.Context) error {
	resumeWAL := c.resumeWAL
	c.resumeWAL = nil
	return releasePendingCycleWAL(ctx, resumeWAL)
}

func releasePendingCycleWAL(ctx context.Context, resumeWAL func(context.Context) error) error {
	if resumeWAL == nil {
		return nil
	}
	if ctx == nil {
		return errCoordinatorUnavailable
	}
	return resumeWAL(ctx)
}

func (c *PendingCycleCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/exchange" {
		c.proxyAdapter(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writeExchangeError(writer, http.StatusMethodNotAllowed)
		return
	}
	if !validBearer(request.Header.Get("Authorization"), c.token) {
		writeExchangeError(writer, http.StatusUnauthorized)
		return
	}
	if request.Header.Get("Content-Type") != "application/json" {
		writeExchangeError(writer, http.StatusUnsupportedMediaType)
		return
	}
	if request.ContentLength > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusRequestEntityTooLarge)
		return
	}
	body, err := ioReadAll(request)
	if err != nil || len(body) > maximumExchangeBytes {
		writeExchangeError(writer, http.StatusRequestEntityTooLarge)
		return
	}
	exchange, err := decodeExchangeRequest(body)
	if err != nil {
		writeExchangeError(writer, http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer func() {
		if c.failed != nil {
			cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = c.releaseWALMaterializationLocked(cleanupContext)
			cancel()
		}
		c.mu.Unlock()
	}()
	if c.closed || !c.prepared || c.failed != nil || c.completed {
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if exchange.Sequence != c.nextSeq {
		c.failed = errors.New("React Native pending-cycle exchange sequence is not monotonic")
		writeExchangeError(writer, http.StatusConflict)
		return
	}
	if err := c.acceptResultLocked(exchange.Result); err != nil {
		c.failed = fmt.Errorf("React Native pending-cycle exchange sequence %d failed: %w", exchange.Sequence, err)
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	response, err := c.advanceLocked(request.Context(), exchange.Sequence)
	if err != nil {
		c.failed = err
		writeExchangeError(writer, http.StatusUnprocessableEntity)
		return
	}
	c.nextSeq++
	encoded, err := json.Marshal(response)
	if err != nil || len(encoded) > maximumExchangeBytes {
		c.failed = errors.New("React Native pending-cycle exchange response is invalid")
		writeExchangeError(writer, http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

func (c *PendingCycleCoordinator) acceptResultLocked(raw json.RawMessage) error {
	if c.stage == pendingCycleStageOpen {
		if !isJSONNull(raw) {
			return errInvalidExchange
		}
		return nil
	}
	if c.stage == pendingCycleStageDeviceRestarted {
		if !isJSONNull(raw) {
			return errInvalidExchange
		}
		return nil
	}
	envelope, err := decodeResultEnvelope(raw)
	if err != nil || envelope.Outcome != "passed" {
		return errInvalidExchange
	}
	switch c.stage {
	case pendingCycleStageOpened:
		process, err := validateOpenedResult(envelope.Result)
		if err != nil {
			return err
		}
		c.process = &process
		return nil
	case pendingCycleStageBootstrapped:
		return c.validateSynchronizedResult(envelope.Result, "idle")
	case pendingCycleStageBeforeWrite:
		return c.captureState(envelope.Result, pendingCycleStageBeforeWrite)
	case pendingCycleStageStoppedForInitialWrite:
		if c.process == nil {
			return errors.New("React Native pending-cycle process identity is unavailable")
		}
		return validateStoppedLifecycleResult(envelope.Result, *c.process)
	case pendingCycleStageInitialLocalWrite:
		return c.validateLocalResult(envelope.Result)
	case pendingCycleStageAfterInitialWrite:
		return c.captureState(envelope.Result, pendingCycleStageAfterInitialWrite)
	case pendingCycleStageInitialPushBegun:
		return c.validateInitialCallBegun(envelope.Result)
	case pendingCycleStageInitialPushObserved:
		return c.validateInitialPushAwaited(envelope.Result)
	case pendingCycleStageCapturePendingObserved:
		return c.validateCapturePendingAwaited(envelope.Result)
	case pendingCycleStageAfterInitialPush:
		return c.captureState(envelope.Result, pendingCycleStageAfterInitialPush)
	case pendingCycleStageBeforePull:
		return c.captureState(envelope.Result, pendingCycleStageBeforePull)
	case pendingCycleStageInitialPull:
		return c.validateInitialCallCompleted(envelope.Result)
	case pendingCycleStageAfterInitialPull:
		return c.captureState(envelope.Result, pendingCycleStageAfterInitialPull)
	case pendingCycleStageRestartOpened:
		return c.validateRestartOpened(envelope.Result)
	case pendingCycleStageAfterRestart:
		return c.captureState(envelope.Result, pendingCycleStageAfterRestart)
	case pendingCycleStageStoppedForUpdate:
		return c.validateStoppedResult(envelope.Result)
	case pendingCycleStageUpdateLocalWrite:
		return c.validateLocalResult(envelope.Result)
	case pendingCycleStageBeforeCleanup:
		return c.captureState(envelope.Result, pendingCycleStageBeforeCleanup)
	case pendingCycleStageCleanupCall:
		return c.validateCleanupCall(envelope.Result)
	case pendingCycleStageAfterCleanup:
		return c.captureState(envelope.Result, pendingCycleStageAfterCleanup)
	case pendingCycleStageStoppedForUpdatePush:
		return c.validateStoppedResult(envelope.Result)
	case pendingCycleStageUpdatePushed:
		return c.validateSynchronizedResult(envelope.Result, "idle")
	case pendingCycleStageAfterUpdate:
		return c.captureState(envelope.Result, pendingCycleStageAfterUpdate)
	case pendingCycleStageStoppedForDelete:
		return c.validateStoppedResult(envelope.Result)
	case pendingCycleStageDeleteLocalWrite:
		return c.validateLocalResult(envelope.Result)
	case pendingCycleStageBeforeDelete:
		return c.captureState(envelope.Result, pendingCycleStageBeforeDelete)
	case pendingCycleStageDeletePushed:
		return c.validateSynchronizedResult(envelope.Result, "idle")
	case pendingCycleStageComplete:
		return c.captureState(envelope.Result, pendingCycleStageComplete)
	default:
		return errInvalidExchange
	}
}

func (c *PendingCycleCoordinator) advanceLocked(ctx context.Context, sequence uint64) (exchangeResponse, error) {
	response := exchangeResponse{SchemaVersion: 1, Sequence: sequence, State: "command"}
	switch c.stage {
	case pendingCycleStageOpen:
		response.Command = c.command("client", "open", map[string]any{"client_key": c.clientKey, "database_mode": "create", "initialization": "empty", "seed_step_id": nil}, nil)
	case pendingCycleStageOpened:
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "start", "completion": "idle"}, nil)
	case pendingCycleStageBootstrapped:
		response.Command = c.captureCommand()
	case pendingCycleStageBeforeWrite:
		response.Command = c.command("client", "lifecycle", map[string]any{"client_key": c.clientKey, "operation": "stop"}, nil)
	case pendingCycleStageStoppedForInitialWrite:
		response.Command = c.command("client", "execute-step", map[string]any{"client_key": c.clientKey}, []scenarios.StepID{pendingCycleLocalWriteStepID})
	case pendingCycleStageInitialLocalWrite:
		response.Command = c.captureCommand()
	case pendingCycleStageAfterInitialWrite:
		response.Command = c.command("client", "begin-call", map[string]any{"client_key": c.clientKey, "call_id": pendingCycleCallID, "method": c.steps[pendingCyclePushStepID].NativeBinding.Method}, []scenarios.StepID{pendingCyclePushStepID})
	case pendingCycleStageInitialPushBegun:
		if err := c.waitForInitialPush(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("observer", "await-step", map[string]any{"client_key": c.clientKey, "call_id": pendingCycleCallID}, []scenarios.StepID{pendingCyclePushStepID})
	case pendingCycleStageInitialPushObserved:
		if err := c.waitForCapturePending(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("observer", "await-step", map[string]any{"client_key": c.clientKey, "call_id": pendingCycleCallID}, []scenarios.StepID{pendingCycleCapturePendingStepID})
	case pendingCycleStageCapturePendingObserved:
		response.Command = c.captureCommand()
	case pendingCycleStageAfterInitialPush:
		if err := c.materializeInitialPull(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.captureCommand()
	case pendingCycleStageBeforePull:
		if err := c.waitForRetryPull(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("client", "await-call", map[string]any{"client_key": c.clientKey, "call_id": pendingCycleCallID}, []scenarios.StepID{pendingCyclePullStepID})
	case pendingCycleStageInitialPull:
		response.Command = c.captureCommand()
	case pendingCycleStageAfterInitialPull:
		response.Command = c.command("device", "restart", nil, nil)
	case pendingCycleStageDeviceRestarted:
		response.Command = c.command("client", "open", map[string]any{"client_key": c.clientKey, "database_mode": "reuse", "initialization": "empty", "seed_step_id": nil}, nil)
	case pendingCycleStageRestartOpened:
		response.Command = c.captureCommand()
	case pendingCycleStageAfterRestart:
		response.Command = c.command("client", "lifecycle", map[string]any{"client_key": c.clientKey, "operation": "stop"}, nil)
	case pendingCycleStageStoppedForUpdate:
		if err := c.prepareUpdate(); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("client", "execute-step", map[string]any{"client_key": c.clientKey}, []scenarios.StepID{pendingCycleLocalWriteStepID})
		response.Command.Action.Steps[0].Operation = conformanceOperationFromScenario(c.updateStep.LocalWrite)
	case pendingCycleStageUpdateLocalWrite:
		response.Command = c.captureCommand()
	case pendingCycleStageBeforeCleanup:
		if err := c.applyCleanupAssignment(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "start", "completion": "blocked"}, nil)
	case pendingCycleStageCleanupCall:
		response.Command = c.captureCommand()
	case pendingCycleStageAfterCleanup:
		response.Command = c.command("client", "lifecycle", map[string]any{"client_key": c.clientKey, "operation": "stop"}, nil)
	case pendingCycleStageStoppedForUpdatePush:
		c.releaseCleanupFault()
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "start", "completion": "idle"}, nil)
	case pendingCycleStageUpdatePushed:
		if err := c.materializeGenerated(ctx, c.updateStep, "update"); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.captureCommand()
	case pendingCycleStageAfterUpdate:
		response.Command = c.command("client", "lifecycle", map[string]any{"client_key": c.clientKey, "operation": "stop"}, nil)
	case pendingCycleStageStoppedForDelete:
		if err := c.prepareDelete(); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.command("client", "execute-step", map[string]any{"client_key": c.clientKey}, []scenarios.StepID{pendingCycleLocalWriteStepID})
		response.Command.Action.Steps[0].Operation = conformanceOperationFromScenario(c.deleteStep.LocalWrite)
	case pendingCycleStageDeleteLocalWrite:
		response.Command = c.captureCommand()
	case pendingCycleStageBeforeDelete:
		response.Command = c.command("client", "synchronize-step", map[string]any{"client_key": c.clientKey, "method": "start", "completion": "idle"}, nil)
	case pendingCycleStageDeletePushed:
		if err := c.materializeGenerated(ctx, c.deleteStep, "delete"); err != nil {
			return exchangeResponse{}, err
		}
		response.Command = c.captureCommand()
	case pendingCycleStageComplete:
		if err := c.validateCompletionLocked(ctx); err != nil {
			return exchangeResponse{}, err
		}
		response.State = "complete"
		response.Command = nil
		c.completed = true
	}
	if c.stage != pendingCycleStageComplete {
		c.stage++
	}
	return response, nil
}

func (c *PendingCycleCoordinator) materializeInitialPull(ctx context.Context) error {
	if c.config.Controller == nil {
		return errors.New("React Native pending-cycle coordinator controller is unavailable")
	}
	push := c.steps[pendingCyclePushStepID].Operation
	if err := c.config.Controller.BindApplicationPush(push); err != nil {
		return fmt.Errorf("bind React Native pending-cycle push transaction: %w", err)
	}
	if err := c.releaseWALMaterializationLocked(ctx); err != nil {
		return fmt.Errorf("resume React Native pending-cycle WAL materialization: %w", err)
	}
	unprotectedMaterialize := c.steps[pendingCycleUnprotectedMaterializeStepID]
	result, err := c.config.Controller.ProcessStep(ctx, nil, unprotectedMaterialize.Operation)
	if err != nil || result.Disposition != unprotectedMaterialize.ExpectedOutcome.Disposition {
		return fmt.Errorf("materialize React Native pending-cycle unprotected row: %w", nativeResultError(err, result.Disposition))
	}
	materialize := c.steps[pendingCycleMaterializeStepID].Operation
	result, err = c.config.Controller.ProcessStep(ctx, nil, materialize)
	if err != nil || result.Disposition != c.steps[pendingCycleMaterializeStepID].ExpectedOutcome.Disposition {
		return fmt.Errorf("materialize React Native pending mutation: %w", nativeResultError(err, result.Disposition))
	}
	if err := c.bindRuntimeIdentities(true); err != nil {
		return err
	}
	c.signalInitialPullMaterialized()
	return nil
}

func (c *PendingCycleCoordinator) validateLocalResult(raw json.RawMessage) error {
	if err := validateActionResult(raw, "local-action"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "pending-cycle local result"); err != nil {
		return err
	}
	var rows uint64
	if json.Unmarshal(members["rows_affected"], &rows) != nil || rows == 0 {
		return errors.New("React Native pending-cycle local write affected no rows")
	}
	return c.validateProcess(members["process"])
}

func (c *PendingCycleCoordinator) validateInitialCallBegun(raw json.RawMessage) error {
	if err := validateActionResult(raw, "call-begun"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "pending-cycle begun call result"); err != nil {
		return err
	}
	var callID, state string
	if json.Unmarshal(members["call_id"], &callID) != nil || callID != pendingCycleCallID ||
		json.Unmarshal(members["state"], &state) != nil || state != "in_flight" {
		return errors.New("React Native pending-cycle initial call did not enter flight")
	}
	return c.validateProcess(members["process"])
}

func (c *PendingCycleCoordinator) validateInitialPushAwaited(raw json.RawMessage) error {
	if err := validateActionResult(raw, "awaited"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "pending-cycle push observation result"); err != nil {
		return err
	}
	if err := validateSyncStatusShape(members["status"]); err != nil {
		return err
	}
	return c.validateProcess(members["process"])
}

func (c *PendingCycleCoordinator) validateCapturePendingAwaited(raw json.RawMessage) error {
	if err := validateActionResult(raw, "awaited"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "pending-cycle capture-pending observation result"); err != nil {
		return err
	}
	if err := validatePendingCycleCapturePendingStatus(members["status"]); err != nil {
		return err
	}
	if err := c.capturePendingResponseError(); err != nil {
		return err
	}
	return c.validateProcess(members["process"])
}

func (c *PendingCycleCoordinator) validateInitialCallCompleted(raw json.RawMessage) error {
	if err := validateActionResult(raw, "call-completed"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 6, "pending-cycle completed call result"); err != nil {
		return err
	}
	var callID, state, completion string
	if json.Unmarshal(members["call_id"], &callID) != nil || callID != pendingCycleCallID ||
		json.Unmarshal(members["state"], &state) != nil || state != "completed" ||
		json.Unmarshal(members["completion"], &completion) != nil || completion != c.steps[pendingCyclePullStepID].NativeBinding.Completion {
		return errors.New("React Native pending-cycle initial call did not complete idle")
	}
	if err := validateReadyStatus(members["status"]); err != nil {
		return err
	}
	if err := c.retryPullResponseError(); err != nil {
		return err
	}
	return c.validateProcess(members["process"])
}

func validatePendingCycleCapturePendingStatus(raw json.RawMessage) error {
	var status syncStatus
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "pending-cycle capture-pending status"); err != nil ||
		json.Unmarshal(raw, &status) != nil || status.State != "backoff" || isJSONNull(status.RetryAt) ||
		!isJSONNull(status.Failure) {
		return errors.New("React Native pending-cycle capture-pending status is invalid")
	}
	var operation string
	if json.Unmarshal(status.Operation, &operation) != nil || operation != "pulling" {
		return errors.New("React Native pending-cycle capture-pending operation is invalid")
	}
	return nil
}

func (c *PendingCycleCoordinator) validateSynchronizedResult(raw json.RawMessage, completion string) error {
	if err := validateActionResult(raw, "synchronized"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 4, "pending-cycle synchronized result"); err != nil {
		return err
	}
	var actualCompletion string
	if json.Unmarshal(members["completion"], &actualCompletion) != nil || actualCompletion != completion || validateSyncStatusShape(members["status"]) != nil {
		return errors.New("React Native pending-cycle synchronized result is invalid")
	}
	return c.validateProcess(members["process"])
}

func (c *PendingCycleCoordinator) validateStoppedResult(raw json.RawMessage) error {
	if c.process == nil {
		return errors.New("React Native pending-cycle process identity is unavailable")
	}
	return validateStoppedLifecycleResult(raw, *c.process)
}

func (c *PendingCycleCoordinator) validateRestartOpened(raw json.RawMessage) error {
	process, err := validateOpenedResult(raw)
	if err != nil || c.process == nil || process.DatabaseIdentityFingerprint != c.process.DatabaseIdentityFingerprint || process.ProcessID == c.process.ProcessID {
		return errors.New("React Native pending-cycle restart did not replace the process and retain its database")
	}
	c.process = &process
	return nil
}

func (c *PendingCycleCoordinator) validateCleanupCall(raw json.RawMessage) error {
	if err := c.validateSynchronizedResult(raw, "blocked"); err != nil {
		return err
	}
	var members map[string]json.RawMessage
	var status syncStatus
	if err := decodeStrictMembers(raw, &members, 4, "pending-cycle cleanup result"); err != nil ||
		json.Unmarshal(members["status"], &status) != nil || status.State != "backoff" || isJSONNull(status.RetryAt) {
		return errors.New("React Native pending-cycle cleanup did not retain retryable push backoff")
	}
	return c.validateCleanupFault()
}

func (c *PendingCycleCoordinator) validateProcess(raw json.RawMessage) error {
	process, err := decodeActionProcessIdentity(raw)
	if err != nil || c.process == nil {
		return errors.New("React Native pending-cycle process identity is invalid")
	}
	if process.ProcessID != c.process.ProcessID || process.DatabaseIdentityFingerprint != c.process.DatabaseIdentityFingerprint {
		return errors.New("React Native pending-cycle process identity changed")
	}
	return nil
}

func (c *PendingCycleCoordinator) validateCompletionLocked(ctx context.Context) error {
	if c.config.Controller == nil {
		return errors.New("React Native pending-cycle final evidence is unavailable")
	}
	evidence, err := c.nativeEvidence()
	if err != nil {
		return err
	}
	finalCapture, found := c.captures[pendingCycleStageComplete]
	if !found || validateReadyStatus(finalCapture.Status) != nil {
		return errors.New("React Native pending-cycle final status is not ready")
	}
	initialCapture, found := c.captures[pendingCycleStageAfterInitialPull]
	if !found {
		return errors.New("React Native pending-cycle initial trace is unavailable")
	}
	if err := validatePendingCycleTrace(c.config.Scenario, initialCapture.Trace); err != nil {
		return err
	}
	if err := c.validateCleanupFault(); err != nil {
		return err
	}
	serverCaptures, err := c.config.Controller.Capture(ctx, []string{c.clientKey}, []string{"server-state"})
	if err != nil || len(serverCaptures) != 1 {
		return fmt.Errorf("capture React Native pending-cycle server state: %w", nativeResultError(err, ""))
	}
	if err := scenarios.ValidatePendingCycleNativeEvidence(evidence); err != nil {
		return fmt.Errorf("validate React Native pending-cycle native evidence: %w", err)
	}
	if err := scenarios.ValidatePendingCycleServerFacts(serverCaptures[0].StateFacts, c.target); err != nil {
		return fmt.Errorf("validate React Native pending-cycle server evidence: %w", err)
	}
	resolutions, err := c.resolveIdentities()
	if err != nil {
		return err
	}
	c.result = PendingCycleCoordinatorResult{ServerFacts: serverCaptures[0].StateFacts, IdentityResolution: resolutions, Evidence: evidence}
	return nil
}

var pendingCycleCaptureSources = []string{
	"application-rows",
	"scope-state",
	"pending-mutations",
	"rejected-mutations",
	"sync-status",
	"sync-events",
	"provenance",
	"request-trace",
	"durable-proof",
}

var pendingCycleCaptureKeys = []string{
	"application_rows",
	"client_state",
	"pending_mutations",
	"rejected_mutations",
	"sync_status",
	"sync_events",
	"provenance",
	"request_trace",
	"durable_proof",
}

// ExchangeCount returns the exact number of coordinator exchanges required by
// the direct pending-cycle proof.
func (c *PendingCycleCoordinator) ExchangeCount() int {
	return int(pendingCycleStageComplete) + 1
}

func (c *PendingCycleCoordinator) captureCommand() *conformanceCommand {
	return c.command("observer", "capture", map[string]any{
		"client_keys": []string{c.clientKey},
		"sources":     pendingCycleCaptureSources,
		"row_selectors": []map[string]any{
			{
				"table_name":        c.target.TableName,
				"primary_key_field": c.target.PrimaryKeyField,
				"primary_key":       c.target.RecordID,
			},
			{
				"table_name":        c.target.TableName,
				"primary_key_field": c.target.PrimaryKeyField,
				"primary_key":       c.target.UnprotectedRecordID,
			},
		},
		"durable_proof_identity": map[string]any{
			"table_name": c.target.TableName,
			"record_id":  c.target.RecordID,
		},
	}, nil)
}

func conformanceOperationFromScenario(operation scenarios.Operation) conformanceOperation {
	return conformanceOperation{
		ContractOperation: operation.ContractOperation,
		Name:              operation.Name,
		Payload:           copyRaw(operation.Payload),
	}
}

func (c *PendingCycleCoordinator) captureState(raw json.RawMessage, stage pendingCycleStage) error {
	var members map[string]json.RawMessage
	if err := decodeStrictMembers(raw, &members, 3, "pending-cycle capture result"); err != nil {
		return err
	}
	if err := c.validateProcess(members["process"]); err != nil {
		return err
	}
	capture, err := decodeCapture(raw, pendingCycleCaptureKeys)
	if err != nil {
		return err
	}
	if stage == pendingCycleStageAfterCleanup {
		var status syncStatus
		if json.Unmarshal(capture.Status, &status) != nil || status.State != "backoff" || isJSONNull(status.RetryAt) {
			return errors.New("React Native pending-cycle cleanup capture is not in retryable backoff")
		}
		if err := c.validateCleanupFault(); err != nil {
			return err
		}
	}
	state, err := pendingCycleNativeState(c.target, *c.process, capture)
	if err != nil {
		return err
	}
	c.captures[stage] = capture
	c.states[stage] = state
	return nil
}

func (c *PendingCycleCoordinator) nativeEvidence() (scenarios.PendingCycleNativeEvidence, error) {
	state := func(stage pendingCycleStage) (scenarios.PendingCycleNativeState, error) {
		value, found := c.states[stage]
		if !found {
			return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle inspection sequence is incomplete")
		}
		return value, nil
	}
	beforeWrite, err := state(pendingCycleStageBeforeWrite)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	afterWrite, err := state(pendingCycleStageAfterInitialWrite)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	afterPush, err := state(pendingCycleStageAfterInitialPush)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	beforePull, err := state(pendingCycleStageBeforePull)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	afterPull, err := state(pendingCycleStageAfterInitialPull)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	afterRestart, err := state(pendingCycleStageAfterRestart)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	beforeCleanup, err := state(pendingCycleStageBeforeCleanup)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	afterCleanup, err := state(pendingCycleStageAfterCleanup)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	afterUpdate, err := state(pendingCycleStageAfterUpdate)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	beforeDelete, err := state(pendingCycleStageBeforeDelete)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	afterDelete, err := state(pendingCycleStageComplete)
	if err != nil {
		return scenarios.PendingCycleNativeEvidence{}, err
	}
	return scenarios.PendingCycleNativeEvidence{
		Target:        c.target,
		UpdatedValue:  c.updated,
		BeforeWrite:   beforeWrite,
		AfterWrite:    afterWrite,
		AfterPush:     afterPush,
		BeforePull:    beforePull,
		AfterPull:     afterPull,
		AfterRestart:  afterRestart,
		BeforeCleanup: beforeCleanup,
		AfterCleanup:  afterCleanup,
		AfterUpdate:   afterUpdate,
		BeforeDelete:  beforeDelete,
		AfterDelete:   afterDelete,
	}, nil
}

func (c *PendingCycleCoordinator) prepareUpdate() error {
	state, found := c.states[pendingCycleStageAfterRestart]
	if !found || state.TargetServerVersion == "" {
		return errors.New("React Native pending-cycle update base version is unavailable")
	}
	step, err := scenarios.PendingCycleSynchronizedCRUDOperation(
		c.steps[pendingCycleLocalWriteStepID].Operation,
		c.steps[pendingCyclePushStepID].Operation,
		c.steps[pendingCycleMaterializeStepID].Operation,
		"update", c.target.ValueField, c.target.Value, c.updated, state.TargetServerVersion,
	)
	if err != nil {
		return err
	}
	c.updateStep = step
	return nil
}

func (c *PendingCycleCoordinator) prepareDelete() error {
	state, found := c.states[pendingCycleStageAfterUpdate]
	if !found || state.TargetServerVersion == "" {
		return errors.New("React Native pending-cycle delete base version is unavailable")
	}
	step, err := scenarios.PendingCycleSynchronizedCRUDOperation(
		c.steps[pendingCycleLocalWriteStepID].Operation,
		c.steps[pendingCyclePushStepID].Operation,
		c.steps[pendingCycleMaterializeStepID].Operation,
		"delete", c.target.ValueField, "", "", state.TargetServerVersion,
	)
	if err != nil {
		return err
	}
	c.deleteStep = step
	return nil
}

func (c *PendingCycleCoordinator) applyCleanupAssignment(ctx context.Context) error {
	assignment, err := scenarios.PendingCycleCleanupAssignment(c.userID, c.clientID)
	if err != nil {
		return err
	}
	result, err := c.config.Controller.ApplyStep(ctx, assignment)
	if err != nil || result.Disposition != "success" {
		return fmt.Errorf("assign React Native pending-cycle cleanup scope: %w", nativeResultError(err, result.Disposition))
	}
	c.proxyMu.Lock()
	c.faultArmed = true
	c.faultPushes = 0
	c.proxyFailureCause = nil
	c.proxyMu.Unlock()
	return nil
}

func (c *PendingCycleCoordinator) materializeGenerated(ctx context.Context, step scenarios.PendingCycleNativeCRUDStep, name string) error {
	if c.config.Controller == nil {
		return errors.New("React Native pending-cycle coordinator controller is unavailable")
	}
	if err := c.config.Controller.BindApplicationPush(step.ApplicationPush); err != nil {
		return fmt.Errorf("bind React Native pending-cycle %s push transaction: %w", name, err)
	}
	result, err := c.config.Controller.ProcessStep(ctx, nil, step.Materialize)
	if err != nil || result.Disposition != "success" {
		return fmt.Errorf("materialize React Native pending-cycle %s: %w", name, nativeResultError(err, result.Disposition))
	}
	return nil
}

func (c *PendingCycleCoordinator) releaseCleanupFault() {
	c.proxyMu.Lock()
	c.faultArmed = false
	c.proxyMu.Unlock()
}

func (c *PendingCycleCoordinator) validateCleanupFault() error {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if c.proxyFailureCause != nil {
		return fmt.Errorf("React Native pending-cycle cleanup proxy failed: %w", c.proxyFailureCause)
	}
	if c.faultPushes != 1 {
		return fmt.Errorf("React Native pending-cycle cleanup temporary-unavailable pushes = %d, want 1", c.faultPushes)
	}
	return nil
}

type pendingCycleMutationInspection struct {
	TableName     string `json:"tableName"`
	RecordID      string `json:"recordID"`
	Operation     string `json:"operation"`
	Status        string `json:"status"`
	ClientVersion string `json:"clientVersion"`
}

func pendingCycleNativeTarget(operation scenarios.Operation) (scenarios.PendingCycleNativeTarget, error) {
	var payload struct {
		TableID string                     `json:"table_id"`
		PK      map[string]json.RawMessage `json:"pk"`
		Columns map[string]json.RawMessage `json:"columns"`
	}
	if scenarios.OperationKey(operation) != "local/write" || json.Unmarshal(operation.Payload, &payload) != nil ||
		payload.TableID == "" || len(payload.PK) != 1 || len(payload.Columns) == 0 {
		return scenarios.PendingCycleNativeTarget{}, errors.New("React Native pending-cycle local runtime target is invalid")
	}
	var target scenarios.PendingCycleNativeTarget
	for field, raw := range payload.PK {
		if field == "" || json.Unmarshal(raw, &target.RecordID) != nil || target.RecordID == "" {
			return scenarios.PendingCycleNativeTarget{}, errors.New("React Native pending-cycle runtime primary key is invalid")
		}
		target.PrimaryKeyField = field
	}
	for field, raw := range payload.Columns {
		var value string
		if json.Unmarshal(raw, &value) != nil || value != "pending" {
			continue
		}
		if target.ValueField != "" {
			return scenarios.PendingCycleNativeTarget{}, errors.New("React Native pending-cycle runtime value field is ambiguous")
		}
		target.ValueField = field
		target.Value = value
	}
	if target.ValueField == "" {
		return scenarios.PendingCycleNativeTarget{}, errors.New("React Native pending-cycle runtime value field is absent")
	}
	target.TableName = payload.TableID
	return target, nil
}

func pendingCycleNativeState(target scenarios.PendingCycleNativeTarget, process actionProcessIdentity, capture finalCapture) (scenarios.PendingCycleNativeState, error) {
	if target.TableName == "" || target.PrimaryKeyField == "" || target.RecordID == "" || target.ValueField == "" || target.Value == "" ||
		target.UnprotectedRecordID == "" || target.UnprotectedValue == "" ||
		process.ProcessID == "" || process.DatabaseIdentityFingerprint == "" {
		return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle capture target is incomplete")
	}
	state, err := decodeClientState(capture.ClientState)
	if err != nil || state.ScopeStateCount != uint64(len(state.ScopeStates)) || state.ScopeRowCount != uint64(len(state.ScopeRows)) ||
		state.ProvenanceCount != uint64(len(state.ScopeRows)) {
		return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle durable state is invalid")
	}
	rows, err := decodeRows(capture.Rows)
	if err != nil || state.ApplicationRowCount != uint64(len(rows)) || len(rows) > 2 {
		return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle application row inspection is invalid")
	}
	var pending []pendingCycleMutationInspection
	if err := decodeStrictValue(capture.Pending, &pending); err != nil || pending == nil {
		return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle mutation inspection is invalid")
	}
	var rejected []json.RawMessage
	if err := decodeStrictValue(capture.Rejected, &rejected); err != nil || rejected == nil || state.RejectedMutationCount != uint64(len(rejected)) {
		return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle rejected mutation inspection is invalid")
	}
	encodedScopeRows, err := json.Marshal(state.ScopeRows)
	if err != nil || !semanticRawJSONEqual(encodedScopeRows, capture.Provenance) {
		return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle provenance inspection is inconsistent")
	}
	proof, err := decodeDurableProof(capture.DurableProof)
	if err != nil {
		return scenarios.PendingCycleNativeState{}, err
	}

	result := scenarios.PendingCycleNativeState{
		ProcessID:                   process.ProcessID,
		DatabaseIdentityFingerprint: process.DatabaseIdentityFingerprint,
		ApplicationRowCount:         int(state.ApplicationRowCount),
		PendingChangeCount:          len(pending),
		MutationLedgerCount:         int(state.MutationLedgerCount),
		MutationOutcomeCount:        int(state.MutationOutcomeCount),
		RejectedMutationCount:       int(state.RejectedMutationCount),
		ScopeStateCount:             int(state.ScopeStateCount),
		ScopeRowCount:               int(state.ScopeRowCount),
		RowMetadataCount:            int(state.RowMetadataCount),
	}
	if len(state.ScopeStates) != 1 {
		return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle scope state inspection is ambiguous")
	}
	scope := state.ScopeStates[0]
	result.ScopeID = scope.ScopeID
	if scope.Cursor != nil {
		result.ScopeCursor = *scope.Cursor
	}
	scopeChecksum, err := checksumDigest(scope.Checksum)
	if err != nil {
		return scenarios.PendingCycleNativeState{}, err
	}
	if scopeChecksum != nil {
		result.ScopeChecksum = *scopeChecksum
	}
	if scope.LocalChecksum != "" {
		localChecksum, err := checksumDigest(&scope.LocalChecksum)
		if err != nil {
			return scenarios.PendingCycleNativeState{}, err
		}
		if localChecksum != nil {
			result.LocalScopeChecksum = *localChecksum
		}
	}

	for _, row := range rows {
		var value string
		if json.Unmarshal(row[target.ValueField], &value) != nil || value == "" {
			return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle application row value is invalid")
		}
		switch {
		case rowUsesRuntimePrimary(row, target.PrimaryKeyField, target.RecordID):
			if result.TargetRowPresent {
				return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle target row is duplicated")
			}
			result.TargetRowPresent = true
			result.TargetRowValue = value
		case rowUsesRuntimePrimary(row, target.PrimaryKeyField, target.UnprotectedRecordID):
			if result.UnprotectedRowPresent {
				return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle unprotected row is duplicated")
			}
			result.UnprotectedRowPresent = true
			result.UnprotectedRowValue = value
		default:
			return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle application row differs from its targets")
		}
	}
	for _, mutation := range pending {
		if mutation.TableName != target.TableName || mutation.RecordID != target.RecordID {
			return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle retained mutation differs from its target")
		}
		result.TargetMutations = append(result.TargetMutations, scenarios.PendingCycleNativeMutation{
			Operation: mutation.Operation, Status: mutation.Status, ClientVersion: mutation.ClientVersion,
		})
	}
	for _, scopeRow := range state.ScopeRows {
		if scopeRow.TableName != target.TableName {
			return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle scope row differs from its table")
		}
		switch scopeRow.RecordID {
		case target.RecordID:
			if result.TargetScopeRowPresent {
				return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle target scope provenance is ambiguous")
			}
			result.TargetScopeRowPresent = true
			result.TargetScopeRowChecksum = scopeRow.Checksum
		case target.UnprotectedRecordID:
			if result.UnprotectedScopeRowPresent {
				return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle unprotected scope provenance is ambiguous")
			}
			result.UnprotectedScopeRowPresent = true
			result.UnprotectedScopeRowChecksum = scopeRow.Checksum
		default:
			return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle scope row differs from its targets")
		}
	}
	if proof.RowMetadata != nil {
		metadata := *proof.RowMetadata
		if metadata.TableName != target.TableName || metadata.RecordID != target.RecordID || metadata.ServerVersion == "" || metadata.RowChecksum == nil || *metadata.RowChecksum == "" {
			return scenarios.PendingCycleNativeState{}, errors.New("React Native pending-cycle durable metadata differs from its target")
		}
		checksum, err := checksumDigest(metadata.RowChecksum)
		if err != nil {
			return scenarios.PendingCycleNativeState{}, err
		}
		result.TargetServerVersion = metadata.ServerVersion
		if checksum != nil {
			result.TargetRowChecksum = *checksum
		}
	}
	return result, nil
}

func (c *PendingCycleCoordinator) proxyAdapter(writer http.ResponseWriter, request *http.Request) {
	if c == nil || c.transport == nil || c.upstream == "" {
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maximumExchangeBytes+1))
	if err != nil || len(body) > maximumExchangeBytes {
		c.recordProxyFailure(errors.New("React Native pending-cycle proxy request is invalid"))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	if request.Method == http.MethodPost && request.URL.Path == "/sync/push" && c.cleanupFaultActive() {
		if err := c.recordTemporaryUnavailablePush(body); err != nil {
			c.recordProxyFailure(err)
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
		response := faults.NewTemporaryUnavailableResponse(request)
		defer response.Body.Close()
		responseBody, err := io.ReadAll(response.Body)
		if err != nil || response.Header.Get("Retry-After") != faults.TemporaryUnavailableRetryAfter {
			c.recordProxyFailure(errors.New("React Native pending-cycle temporary-unavailable response is invalid"))
			writeExchangeError(writer, http.StatusBadGateway)
			return
		}
		writePendingCycleProxyResponse(writer, response.StatusCode, response.Header, responseBody)
		return
	}
	if request.Method == http.MethodPost && request.URL.Path == "/sync/pull" {
		if err := c.waitForInitialPullMaterialization(request.Context()); err != nil {
			return
		}
	}
	target := strings.TrimRight(c.upstream, "/") + request.URL.RequestURI()
	upstreamRequest, err := http.NewRequestWithContext(request.Context(), request.Method, target, bytes.NewReader(body))
	if err != nil {
		c.recordProxyFailure(fmt.Errorf("create React Native pending-cycle upstream request: %w", err))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	for name, values := range request.Header {
		if strings.EqualFold(name, "Host") {
			continue
		}
		for _, value := range values {
			upstreamRequest.Header.Add(name, value)
		}
	}
	response, err := c.transport.Do(upstreamRequest)
	if err != nil {
		c.recordProxyFailure(fmt.Errorf("execute React Native pending-cycle upstream request: %w", err))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maximumExchangeBytes+1))
	if err != nil || len(responseBody) > maximumExchangeBytes {
		c.recordProxyFailure(errors.New("React Native pending-cycle upstream response is invalid"))
		writeExchangeError(writer, http.StatusBadGateway)
		return
	}
	if request.Method == http.MethodPost {
		switch request.URL.Path {
		case "/sync/push":
			c.observeInitialPushResponse(response.StatusCode, responseBody)
		case "/sync/pull":
			c.observeInitialPullResponse(response.StatusCode, responseBody)
		}
	}
	writePendingCycleProxyResponse(writer, response.StatusCode, response.Header, responseBody)
}

func (c *PendingCycleCoordinator) waitForInitialPullMaterialization(ctx context.Context) error {
	c.proxyMu.Lock()
	wait := c.initialPushRecorded && c.capturePendingRecorded && !c.retryPullRecorded
	done := c.materializationDone
	c.proxyMu.Unlock()
	if !wait || done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *PendingCycleCoordinator) signalInitialPullMaterialized() {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if c.materializationSignaled || c.materializationDone == nil {
		return
	}
	c.materializationSignaled = true
	close(c.materializationDone)
}

func (c *PendingCycleCoordinator) observeInitialPushResponse(status int, body []byte) {
	c.proxyMu.Lock()
	if c.initialPushRecorded || c.initialPushDone == nil {
		c.proxyMu.Unlock()
		return
	}
	c.initialPushRecorded = true
	c.initialPushErr = pendingCycleValidateHTTPWire(c.config.Scenario, pendingCyclePushStepID, status, body)
	close(c.initialPushDone)
	c.proxyMu.Unlock()
}

func (c *PendingCycleCoordinator) observeInitialPullResponse(status int, body []byte) {
	c.proxyMu.Lock()
	if !c.initialPushRecorded || c.capturePendingDone == nil || c.retryPullDone == nil {
		c.proxyMu.Unlock()
		return
	}
	if !c.capturePendingRecorded {
		c.capturePendingRecorded = true
		c.capturePendingErr = pendingCycleValidateHTTPWire(c.config.Scenario, pendingCycleCapturePendingStepID, status, body)
		close(c.capturePendingDone)
		c.proxyMu.Unlock()
		return
	}
	if !c.retryPullRecorded {
		c.retryPullRecorded = true
		c.retryPullErr = pendingCycleValidateHTTPWire(c.config.Scenario, pendingCyclePullStepID, status, body)
		close(c.retryPullDone)
		c.proxyMu.Unlock()
		return
	}
	c.proxyMu.Unlock()
}

func pendingCycleValidateHTTPWire(scenario scenarios.Scenario, stepID scenarios.StepID, status int, body []byte) error {
	wire, err := pendingCycleWireExpectation(scenario, stepID)
	if err != nil {
		return err
	}
	if status != wire.HTTPStatus {
		return fmt.Errorf("status = %d, want %d", status, wire.HTTPStatus)
	}
	if wire.ErrorCode == nil {
		var members map[string]json.RawMessage
		if err := jsonstrict.Decode(body, &members); err != nil || members == nil {
			return errors.New("pending-cycle successful response is not an object")
		}
		return nil
	}
	var envelope map[string]json.RawMessage
	if err := decodeStrictMembers(body, &envelope, 1, "pending-cycle error response"); err != nil {
		return err
	}
	errorBody, found := envelope["error"]
	if !found {
		return errors.New("pending-cycle error response is missing its error")
	}
	var errorMembers map[string]json.RawMessage
	if err := decodeStrictMembers(errorBody, &errorMembers, 3, "pending-cycle error body"); err != nil {
		return err
	}
	var code, message string
	var retryable bool
	if json.Unmarshal(errorMembers["code"], &code) != nil || code != *wire.ErrorCode ||
		json.Unmarshal(errorMembers["message"], &message) != nil || message == "" ||
		json.Unmarshal(errorMembers["retryable"], &retryable) != nil || retryable != wire.Retryable {
		return errors.New("pending-cycle error response differs from the authored wire")
	}
	return nil
}

func (c *PendingCycleCoordinator) waitForInitialPush(ctx context.Context) error {
	if c == nil || ctx == nil || c.initialPushDone == nil {
		return errCoordinatorUnavailable
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for React Native pending-cycle accepted push: %w", ctx.Err())
	case <-c.initialPushDone:
	}
	c.proxyMu.Lock()
	err := c.initialPushErr
	c.proxyMu.Unlock()
	if err != nil {
		return fmt.Errorf("React Native pending-cycle accepted push wire is invalid: %w", err)
	}
	return nil
}

func (c *PendingCycleCoordinator) waitForCapturePending(ctx context.Context) error {
	if c == nil || ctx == nil || c.capturePendingDone == nil {
		return errCoordinatorUnavailable
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for React Native pending-cycle capture-pending pull: %w", ctx.Err())
	case <-c.capturePendingDone:
	}
	return c.capturePendingResponseError()
}

func (c *PendingCycleCoordinator) waitForRetryPull(ctx context.Context) error {
	if c == nil || ctx == nil || c.retryPullDone == nil {
		return errCoordinatorUnavailable
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for React Native pending-cycle retry pull: %w", ctx.Err())
	case <-c.retryPullDone:
	}
	return c.retryPullResponseError()
}

func (c *PendingCycleCoordinator) capturePendingResponseError() error {
	if c == nil {
		return errCoordinatorUnavailable
	}
	c.proxyMu.Lock()
	err := c.capturePendingErr
	recorded := c.capturePendingRecorded
	c.proxyMu.Unlock()
	if !recorded {
		return errors.New("React Native pending-cycle capture-pending pull is absent")
	}
	if err != nil {
		return fmt.Errorf("React Native pending-cycle capture-pending wire is invalid: %w", err)
	}
	return nil
}

func (c *PendingCycleCoordinator) retryPullResponseError() error {
	if c == nil {
		return errCoordinatorUnavailable
	}
	c.proxyMu.Lock()
	err := c.retryPullErr
	recorded := c.retryPullRecorded
	c.proxyMu.Unlock()
	if !recorded {
		return errors.New("React Native pending-cycle retry pull is absent")
	}
	if err != nil {
		return fmt.Errorf("React Native pending-cycle retry pull wire is invalid: %w", err)
	}
	return nil
}

func (c *PendingCycleCoordinator) cleanupFaultActive() bool {
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	return c.faultArmed
}

func (c *PendingCycleCoordinator) recordTemporaryUnavailablePush(raw []byte) error {
	var request struct {
		Mutations []struct {
			Operation string `json:"op"`
		} `json:"mutations"`
	}
	if json.Unmarshal(raw, &request) != nil || len(request.Mutations) != 1 || request.Mutations[0].Operation != "update" {
		return errors.New("React Native pending-cycle temporary-unavailable push is not the generated update")
	}
	c.proxyMu.Lock()
	defer c.proxyMu.Unlock()
	if !c.faultArmed {
		return errors.New("React Native pending-cycle temporary-unavailable push arrived after release")
	}
	c.faultPushes++
	return nil
}

func (c *PendingCycleCoordinator) recordProxyFailure(err error) {
	if err == nil {
		return
	}
	c.proxyMu.Lock()
	if c.proxyFailureCause == nil {
		c.proxyFailureCause = err
	}
	c.proxyMu.Unlock()
}

func writePendingCycleProxyResponse(writer http.ResponseWriter, status int, header http.Header, body []byte) {
	for name, values := range header {
		if strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Transfer-Encoding") {
			continue
		}
		for _, value := range values {
			writer.Header().Add(name, value)
		}
	}
	writer.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
	writer.WriteHeader(status)
	_, _ = writer.Write(body)
}

func (c *PendingCycleCoordinator) unprotectedRowIdentity() (scenarios.NativeIdentityAlias, string, error) {
	alias, err := scenarios.PendingCycleUnprotectedIdentityAlias(c.identities)
	if err != nil {
		return scenarios.NativeIdentityAlias{}, "", err
	}
	values, err := c.config.Controller.IdentityValues([]scenarios.NativeIdentityAlias{alias})
	if err != nil || len(values) != 1 || values[0].Alias != alias.Alias {
		return scenarios.NativeIdentityAlias{}, "", errors.New("React Native pending-cycle unprotected identity has no runtime binding")
	}
	var runtimeRecordID string
	if json.Unmarshal(values[0].RuntimeValue, &runtimeRecordID) != nil || runtimeRecordID == "" {
		return scenarios.NativeIdentityAlias{}, "", errors.New("React Native pending-cycle unprotected runtime identity is invalid")
	}
	c.runtimeIDs[alias.Alias] = copyRaw(values[0].RuntimeValue)
	return alias, runtimeRecordID, nil
}

func (c *PendingCycleCoordinator) bindRuntimeIdentities(includePrimary bool) error {
	aliases := make([]scenarios.NativeIdentityAlias, 0, len(c.identities))
	for _, alias := range c.identities {
		if alias.Kind == "schema" || alias.Kind == "scope" || alias.Kind == "table" || includePrimary {
			aliases = append(aliases, alias)
		}
	}
	values, err := c.config.Controller.IdentityValues(aliases)
	if err != nil {
		return fmt.Errorf("resolve React Native pending-cycle runtime identities: %w", err)
	}
	for _, value := range values {
		c.runtimeIDs[value.Alias] = copyRaw(value.RuntimeValue)
		switch value.Alias {
		case "items-table":
			c.tableName = value.ApplicationIdentifier
		case "pending-row-primary-key":
			c.primaryKey = value.ApplicationIdentifier
		}
	}
	if c.tableName == "" || includePrimary && c.primaryKey == "" {
		return errors.New("React Native pending-cycle runtime application identities are unavailable")
	}
	return nil
}

func (c *PendingCycleCoordinator) runtimeRecordID() string {
	var value string
	_ = json.Unmarshal(c.runtimeIDs["pending-row-primary-key"], &value)
	return value
}

func (c *PendingCycleCoordinator) resolveIdentities() ([]blackbox.NativeIdentityResolution, error) {
	// The controller binds only server-owned identities. The client generation
	// and scope set version are observed on the wire, exactly as the steady-pull
	// consumer resolves them.
	initialCapture, found := c.captures[pendingCycleStageAfterInitialPull]
	if !found {
		return nil, errors.New("React Native pending-cycle initial identity trace is unavailable")
	}
	trace, err := captureTraceFromRaw(initialCapture.Trace)
	if err != nil {
		return nil, err
	}
	push, pull, err := pendingCycleAuthoredObservations(trace)
	if err != nil {
		return nil, err
	}
	generation, err := requestInteger(push, "client_generation")
	if err != nil {
		return nil, err
	}
	scopeSetVersion, err := requestInteger(pull, "scope_set_version")
	if err != nil {
		return nil, err
	}
	for alias, value := range map[string]any{
		"client-generation-one": generation,
		"scope-set-version-one": scopeSetVersion,
	} {
		encoded, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			return nil, fmt.Errorf("encode React Native pending-cycle alias %q: %w", alias, marshalErr)
		}
		c.runtimeIDs[alias] = encoded
	}
	if len(c.runtimeIDs) != len(pendingCycleAliasNames) {
		missing := make([]string, 0, len(pendingCycleAliasNames))
		for _, name := range pendingCycleAliasNames {
			if _, found := c.runtimeIDs[name]; !found {
				missing = append(missing, name)
			}
		}
		return nil, fmt.Errorf("React Native pending-cycle identity evidence is incomplete: missing %v", missing)
	}
	observations := make([]blackbox.NativeIdentityObservation, 0)
	for _, alias := range c.identities {
		value := c.runtimeIDs[alias.Alias]
		for _, stepID := range alias.StepIDs {
			owner := stepID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, StepID: &owner, RuntimeValue: value})
		}
		for _, expectationID := range alias.ExpectationIDs {
			owner := expectationID
			observations = append(observations, blackbox.NativeIdentityObservation{Kind: alias.Kind, Alias: alias.Alias, ExpectationID: &owner, RuntimeValue: value})
		}
	}
	return blackbox.ResolveNativeIdentityAliases(c.identities, observations)
}

func (c *PendingCycleCoordinator) command(actor, name string, parameters map[string]any, stepIDs []scenarios.StepID) *conformanceCommand {
	steps := make([]conformanceStep, 0, len(stepIDs))
	for _, id := range stepIDs {
		step := c.steps[id]
		steps = append(steps, conformanceStep{Operation: conformanceOperation{ContractOperation: step.Operation.ContractOperation, Name: step.Operation.Name, Payload: copyRaw(step.Operation.Payload)}})
	}
	return &conformanceCommand{SchemaVersion: 1, Action: conformanceManifest{Action: conformanceAction{Actor: actor, Command: name, Parameters: parameters}, Steps: steps}, Runtime: conformanceRuntime{ClientKey: c.clientKey, Database: c.database, ClientID: c.clientID, ServerURL: c.adapter, AuthToken: c.config.AuthToken}}
}

type pendingCycleClientIdentity struct{ userID, clientID string }

func extractPendingCycleClientIdentity(scenario scenarios.Scenario) (pendingCycleClientIdentity, error) {
	var payload struct {
		AuthenticatedUserID string `json:"authenticated_user_id"`
		ClientID            string `json:"client_id"`
	}
	if err := json.Unmarshal(scenario.Steps[1].Operation.Payload, &payload); err != nil || payload.AuthenticatedUserID == "" || payload.ClientID == "" {
		return pendingCycleClientIdentity{}, errors.New("React Native pending-cycle client identity is invalid")
	}
	for _, step := range scenario.Steps {
		if step.NativeBinding.UserID != "" && step.NativeBinding.UserID != payload.AuthenticatedUserID || step.NativeBinding.ClientID != "" && step.NativeBinding.ClientID != payload.ClientID {
			return pendingCycleClientIdentity{}, errors.New("React Native pending-cycle native identity differs across steps")
		}
	}
	return pendingCycleClientIdentity{userID: payload.AuthenticatedUserID, clientID: payload.ClientID}, nil
}

func pendingCyclePullScopeCount(operation scenarios.Operation) (int, error) {
	var payload struct {
		Scopes []json.RawMessage `json:"scopes"`
	}
	if err := json.Unmarshal(operation.Payload, &payload); err != nil || len(payload.Scopes) == 0 {
		return 0, errors.New("React Native pending-cycle pull scopes are invalid")
	}
	return len(payload.Scopes), nil
}

func validatePendingCycleCapture(scenario scenarios.Scenario, capture finalCapture) error {
	if len(capture.ClientState) == 0 || len(capture.Pending) == 0 || len(capture.Rejected) == 0 || len(capture.Status) == 0 || len(capture.Provenance) == 0 || len(capture.Trace) == 0 {
		return errors.New("React Native pending-cycle capture is incomplete")
	}
	for _, step := range scenario.Steps {
		if step.ExpectedOutcome.Disposition != "success" {
			return errors.New("React Native pending-cycle authored outcome is not successful")
		}
	}
	return nil
}

// pendingCycleAuthoredObservations returns the accepted push and retry pull.
// The staged start reconnects, pushes, observes capture_pending, and retries
// one pull after the WAL worker materializes the authored transactions.
func pendingCycleAuthoredObservations(trace traceSnapshot) (transportObservation, transportObservation, error) {
	var push, pull transportObservation
	if trace.Overflowed || len(trace.Observations) != pendingCycleBootstrapRequests+4 ||
		trace.SequenceCheckpoint != uint64(len(trace.Observations)) ||
		validateTraceSequence(trace.Observations) != nil {
		return push, pull, errors.New("React Native pending-cycle request trace is incomplete")
	}
	bootstrap := traceSnapshot{Observations: trace.Observations[:pendingCycleBootstrapRequests], SequenceCheckpoint: pendingCycleBootstrapRequests}
	if err := validateBootstrapTrace(bootstrap); err != nil {
		return push, pull, fmt.Errorf("React Native pending-cycle bootstrap trace is invalid: %w", err)
	}
	bootstrapPull, err := decodePullResponseFacts(bootstrap.Observations[2].PullResponseFacts)
	if err != nil || len(bootstrapPull.ScopeCursorFingerprints) != 1 {
		return push, pull, errors.New("React Native pending-cycle bootstrap cursor evidence is invalid")
	}
	checkpointFingerprint := bootstrapPull.ScopeCursorFingerprints[0]
	authored := trace.Observations[pendingCycleBootstrapRequests:]
	if err := validateTraceOperation(authored[0], "connect"); err != nil {
		return push, pull, fmt.Errorf("React Native pending-cycle start connect trace is invalid: %w", err)
	}
	push = authored[1]
	if err := validatePendingCyclePushTrace(push); err != nil {
		return transportObservation{}, transportObservation{}, err
	}
	if err := validatePendingCycleCapturePendingTrace(authored[2], checkpointFingerprint); err != nil {
		return transportObservation{}, transportObservation{}, err
	}
	pull = authored[3]
	if err := validateTraceOperation(pull, "pull"); err != nil {
		return transportObservation{}, transportObservation{}, fmt.Errorf("React Native pending-cycle retry pull trace is invalid: %w", err)
	}
	if len(pull.CursorFingerprints) != 1 || pull.CursorFingerprints[0] != checkpointFingerprint {
		return transportObservation{}, transportObservation{}, errors.New("React Native pending-cycle retry pull checkpoint evidence is invalid")
	}
	return push, pull, nil
}

func validatePendingCycleTrace(scenario scenarios.Scenario, raw json.RawMessage) error {
	trace, err := captureTraceFromRaw(raw)
	if err != nil {
		return err
	}
	push, pull, err := pendingCycleAuthoredObservations(trace)
	if err != nil {
		return err
	}
	if push.StatusCode != pendingCycleWireStatus(scenario, pendingCyclePushStepID) {
		return errors.New("React Native pending-cycle push trace is invalid")
	}
	if pull.StatusCode != pendingCycleWireStatus(scenario, pendingCyclePullStepID) {
		return fmt.Errorf("React Native pending-cycle pull trace is invalid: status %d", pull.StatusCode)
	}
	return nil
}

func validatePendingCyclePushTrace(observation transportObservation) error {
	if observation.OperationClass != "push" || observation.StatusCode != http.StatusOK ||
		observation.DurationNanoseconds == 0 || observation.DurationNanoseconds > warmConnectMaximumSafeInteger ||
		!hasJSONValue(observation.RequestFacts) || observation.CursorFingerprints != nil ||
		observation.CursorFingerprintsComplete != nil || hasJSONValue(observation.RebuildResponseFacts) ||
		hasJSONValue(observation.PullResponseFacts) {
		return errors.New("React Native pending-cycle push trace is invalid")
	}
	return validatePortableRequestIntegers(observation.RequestFacts)
}

func validatePendingCycleCapturePendingTrace(observation transportObservation, checkpointFingerprint string) error {
	if observation.OperationClass != "pull" || observation.StatusCode != pendingCycleCapturePendingStatus ||
		observation.DurationNanoseconds == 0 || observation.DurationNanoseconds > warmConnectMaximumSafeInteger ||
		!hasJSONValue(observation.RequestFacts) || len(observation.CursorFingerprints) != 1 || observation.CursorFingerprints[0] != checkpointFingerprint ||
		observation.CursorFingerprintsComplete == nil || !*observation.CursorFingerprintsComplete ||
		!validCursorFingerprintSet(observation.CursorFingerprints) || hasJSONValue(observation.RebuildResponseFacts) ||
		hasJSONValue(observation.PullResponseFacts) {
		return errors.New("React Native pending-cycle capture-pending trace is invalid")
	}
	return validatePortableRequestIntegers(observation.RequestFacts)
}

func pendingCycleWireStatus(scenario scenarios.Scenario, stepID scenarios.StepID) int {
	wire, err := pendingCycleWireExpectation(scenario, stepID)
	if err != nil {
		return 0
	}
	return wire.HTTPStatus
}

func pendingCycleWireExpectation(scenario scenarios.Scenario, stepID scenarios.StepID) (scenarios.WireExpectation, error) {
	var result scenarios.WireExpectation
	count := 0
	for _, wire := range scenario.WireExpectations {
		if wire.StepID != stepID {
			continue
		}
		result = wire
		count++
	}
	if count != 1 {
		return scenarios.WireExpectation{}, fmt.Errorf("React Native pending-cycle wire expectation %s count = %d, want 1", stepID, count)
	}
	return result, nil
}
