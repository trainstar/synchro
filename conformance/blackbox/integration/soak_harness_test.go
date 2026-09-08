package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/faults"
	"github.com/trainstar/synchro/conformance/invariants"
	"github.com/trainstar/synchro/conformance/scenarios"
	"github.com/trainstar/synchro/conformance/soak"
	"github.com/trainstar/synchro/conformance/vectors"
)

const (
	soakRepositoryRoot = "../../.."
	soakUserID         = "diagnostic-user"
	soakClientID       = "soak-client-a"
	soakPrivateScope   = "user:diagnostic-user"
	soakSharedScope    = "cf:global"
	soakBodyLimit      = int64(1 << 20)
)

type liveSoakHarness struct {
	server   *blackbox.Harness
	native   *blackbox.NativeController
	recorder *blackbox.Recorder
	client   blackbox.Client
	seed     uint64
	root     string

	mu                  sync.Mutex
	closed              bool
	identitySequence    uint64
	processGeneration   uint64
	schemaTransition    uint64
	corruptNextChecksum bool
	authoredTable       map[string]any

	protocol            soakProtocolClient
	manifest            vectors.Manifest
	manifestDocument    soakManifestDocument
	rows                map[string]soakHeldRow
	scopeRows           map[string]map[string]struct{}
	authoritativeDigest map[string][32]byte
	databaseFingerprint string
}

type soakProtocolClient struct {
	Generation      int64
	Schema          map[string]any
	ScopeSetVersion int64
	Scopes          map[string]string
	Tables          map[string]soakProtocolTable
}

type soakProtocolTable struct {
	Name            string
	ID              string
	PrimaryKeyField string
	ValueField      string
}

type soakManifestDocument struct {
	SchemaVersion uint64              `json:"schema_version"`
	SchemaHash    string              `json:"schema_hash"`
	Tables        []soakManifestTable `json:"tables"`
}

type soakManifestTable struct {
	Name              string              `json:"name"`
	ID                string              `json:"table_id"`
	PrimaryKeyFieldID string              `json:"primary_key_field_id"`
	Fields            []soakManifestField `json:"fields"`
}

type soakManifestField struct {
	Name string `json:"name"`
	ID   string `json:"field_id"`
}

type soakHeldRow struct {
	TableID     string
	Row         vectors.Row
	Version     string
	Digest      [32]byte
	Memberships map[string]struct{}
}

type soakRecordedCall struct {
	metadata blackbox.ExchangeMetadata
}

func newLiveSoakHarness(ctx context.Context, seed uint64, corruptChecksum bool) (*liveSoakHarness, error) {
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		return nil, fmt.Errorf("load soak environment: %w", err)
	}
	server, err := blackbox.Provision(ctx, blackbox.HarnessConfig{Environment: environment})
	if err != nil {
		return nil, fmt.Errorf("provision soak harness: %w", err)
	}
	cleanupServer := true
	defer func() {
		if cleanupServer {
			closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = server.Close(closeContext)
		}
	}()

	native, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: server})
	if err != nil {
		return nil, fmt.Errorf("create soak native controller: %w", err)
	}
	attachmentRoot, err := os.MkdirTemp("", "synchro-soak-attachments-")
	if err != nil {
		return nil, fmt.Errorf("create soak attachment root: %w", err)
	}
	cleanupAttachments := true
	defer func() {
		if cleanupAttachments {
			_ = os.RemoveAll(attachmentRoot)
		}
	}()
	recorder, err := blackbox.NewRecorder(blackbox.RecorderConfig{
		AttachmentRoot:  attachmentRoot,
		MaxRecords:      4096,
		MaxRawBodyBytes: soakBodyLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("create soak recorder: %w", err)
	}
	tokenProvider := blackbox.TokenProviderFunc(func(tokenContext context.Context) (string, error) {
		return server.NativeBearerToken(tokenContext, soakUserID, time.Now())
	})
	result := &liveSoakHarness{
		server:              server,
		native:              native,
		recorder:            recorder,
		client:              (blackbox.Client{BaseURL: server.AdapterURL(), Tokens: tokenProvider}).WithRecorder(recorder, soakBodyLimit),
		seed:                seed,
		root:                attachmentRoot,
		processGeneration:   1,
		corruptNextChecksum: corruptChecksum,
		rows:                make(map[string]soakHeldRow),
		scopeRows:           make(map[string]map[string]struct{}),
		authoritativeDigest: make(map[string][32]byte),
		databaseFingerprint: lowerSHA256("soak-database:" + soakClientID),
		protocol: soakProtocolClient{
			Schema: map[string]any{"version": 0, "hash": ""},
			Scopes: make(map[string]string),
			Tables: make(map[string]soakProtocolTable),
		},
	}
	install, authoredTable, err := soakInstallOperation(ctx)
	if err != nil {
		return nil, err
	}
	if err := native.Install(ctx, install); err != nil {
		return nil, fmt.Errorf("install soak native contract: %w", err)
	}
	result.authoredTable = authoredTable
	if _, err := result.loadManifest(ctx, "soak-setup-schema"); err != nil {
		return nil, err
	}
	if _, err := result.connect(ctx, "soak-setup-connect"); err != nil {
		return nil, err
	}
	if err := result.requireScopeSet(); err != nil {
		return nil, err
	}
	for _, scopeID := range result.scopeIDs() {
		if _, err := result.rebuildScope(ctx, scopeID, "soak-setup-rebuild"); err != nil {
			return nil, err
		}
	}
	if err := result.drainPulls(ctx); err != nil {
		return nil, err
	}
	cleanupServer = false
	cleanupAttachments = false
	return result, nil
}

func soakInstallOperation(ctx context.Context) (scenarios.Operation, map[string]any, error) {
	scenario, err := scenarios.LoadFile(ctx, soakRepositoryRoot, "conformance/scenarios/performance/pending-cycle-001.json")
	if err != nil {
		return scenarios.Operation{}, nil, fmt.Errorf("load soak installation fixture: %w", err)
	}
	if len(scenario.Model.Setup) != 1 {
		return scenarios.Operation{}, nil, errors.New("soak installation fixture has no unique setup operation")
	}
	operation := scenario.Model.Setup[0]
	var payload map[string]any
	if err := json.Unmarshal(operation.Payload, &payload); err != nil {
		return scenarios.Operation{}, nil, errors.New("decode soak installation fixture failed")
	}
	clients, ok := payload["clients"].([]any)
	if !ok || len(clients) != 1 {
		return scenarios.Operation{}, nil, errors.New("soak installation client fixture is invalid")
	}
	client, ok := clients[0].(map[string]any)
	if !ok {
		return scenarios.Operation{}, nil, errors.New("soak installation client is invalid")
	}
	client["user_id"] = soakUserID
	client["client_id"] = soakClientID
	client["assigned_scope_ids"] = []any{}
	policies, ok := payload["write_policies"].([]any)
	if !ok || len(policies) != 1 {
		return scenarios.Operation{}, nil, errors.New("soak installation write policy fixture is invalid")
	}
	policy, ok := policies[0].(map[string]any)
	if !ok {
		return scenarios.Operation{}, nil, errors.New("soak installation write policy is invalid")
	}
	policy["user_id"] = soakUserID
	initialSchema, ok := payload["initial_schema"].(map[string]any)
	if !ok {
		return scenarios.Operation{}, nil, errors.New("soak installation schema fixture is invalid")
	}
	tables, ok := initialSchema["tables"].([]any)
	if !ok || len(tables) != 1 {
		return scenarios.Operation{}, nil, errors.New("soak installation table fixture is invalid")
	}
	authoredTable, ok := tables[0].(map[string]any)
	if !ok {
		return scenarios.Operation{}, nil, errors.New("soak installation authored table is invalid")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return scenarios.Operation{}, nil, errors.New("encode soak installation fixture failed")
	}
	operation.Payload = encoded
	if err := scenarios.ValidateOperation(operation); err != nil {
		return scenarios.Operation{}, nil, fmt.Errorf("validate soak installation fixture: %w", err)
	}
	clonedTable, err := cloneJSONObject(authoredTable)
	if err != nil {
		return scenarios.Operation{}, nil, err
	}
	return operation, clonedTable, nil
}

func (h *liveSoakHarness) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	h.mu.Unlock()
	var closeErr error
	if h.native != nil {
		closeErr = h.native.Close(ctx)
	} else if h.server != nil {
		closeErr = h.server.Close(ctx)
	}
	if h.root != "" {
		closeErr = errors.Join(closeErr, os.RemoveAll(h.root))
	}
	return closeErr
}

func (h *liveSoakHarness) Execute(ctx context.Context, operation soak.Operation) (soak.ObservationCapture, error) {
	if ctx == nil {
		return soak.ObservationCapture{}, errors.New("live soak operation context is required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return soak.ObservationCapture{}, errors.New("live soak harness is closed")
	}
	if operation.UserID != soakUserID || operation.ClientID != soakClientID {
		return soak.ObservationCapture{}, errors.New("live soak operation identity is not installed")
	}
	if operation.ScopeID != soakPrivateScope && operation.ScopeID != soakSharedScope {
		return soak.ObservationCapture{}, errors.New("live soak operation scope is not installed")
	}

	wires := make([]invariants.WireExchangeObservation, 0, 4)
	activation, faultWire, err := h.activateOperationFault(ctx, operation)
	if err != nil {
		return soak.ObservationCapture{}, err
	}
	if faultWire != nil {
		wires = append(wires, *faultWire)
	}

	var pullResult *soakPullCapture
	switch operation.Kind {
	case soak.OperationConnect:
		call, err := h.connect(ctx, "soak-connect")
		if err != nil {
			return soak.ObservationCapture{}, err
		}
		wire, err := h.wireFromCall(operation, call, false, false, false)
		if err != nil {
			return soak.ObservationCapture{}, err
		}
		wires = append(wires, wire)
	case soak.OperationPush:
		call, _, err := h.submitInsert(ctx, operation.ScopeID, "operation-push")
		if err != nil {
			return soak.ObservationCapture{}, err
		}
		wire, err := h.wireFromCall(operation, call, true, false, false)
		if err != nil {
			return soak.ObservationCapture{}, err
		}
		wires = append(wires, wire)
	case soak.OperationPull:
		pullResult, err = h.executePullControl(ctx, operation)
		if err != nil {
			return soak.ObservationCapture{}, err
		}
		wires = append(wires, pullResult.wires...)
	case soak.OperationRebuild:
		calls, err := h.rebuildScope(ctx, operation.ScopeID, "soak-rebuild")
		if err != nil {
			return soak.ObservationCapture{}, err
		}
		if err := h.drainPulls(ctx); err != nil {
			return soak.ObservationCapture{}, err
		}
		for _, call := range calls {
			wire, err := h.wireFromCall(operation, call, false, false, false)
			if err != nil {
				return soak.ObservationCapture{}, err
			}
			wires = append(wires, wire)
		}
	case soak.OperationSchemaTransition:
		call, err := h.transitionSchema(ctx)
		if err != nil {
			return soak.ObservationCapture{}, err
		}
		wire, err := h.wireFromCall(operation, call, false, false, false)
		if err != nil {
			return soak.ObservationCapture{}, err
		}
		wires = append(wires, wire)
	case soak.OperationProcessDeath:
		if err := h.executeProcessDeath(ctx); err != nil {
			return soak.ObservationCapture{}, err
		}
		h.processGeneration++
	case soak.OperationWireFault:
		if faultWire == nil {
			return soak.ObservationCapture{}, errors.New("wire-fault operation did not activate a wire fault")
		}
	default:
		return soak.ObservationCapture{}, fmt.Errorf("unsupported live soak operation %q", operation.Kind)
	}

	for index := range wires {
		wires[index].Sequence = uint64(index + 1)
	}
	capture, err := h.capture(ctx, operation, wires, activation)
	if err != nil {
		return soak.ObservationCapture{}, err
	}
	if pullResult != nil {
		result, acknowledgements, err := pullResult.bind(capture.CursorPositions, wires)
		if err != nil {
			return soak.ObservationCapture{}, err
		}
		capture.PullResults = []invariants.PullResultObservation{result}
		capture.CursorAcknowledgements = acknowledgements
	}
	return capture, nil
}

type soakPullCapture struct {
	wires           []invariants.WireExchangeObservation
	change          invariants.PullChangeIdentityObservation
	responseCursors map[string]string
}

func (p *soakPullCapture) bind(positions []invariants.CursorPositionObservation, wires []invariants.WireExchangeObservation) (invariants.PullResultObservation, []invariants.CursorAcknowledgementObservation, error) {
	if p == nil {
		return invariants.PullResultObservation{}, nil, errors.New("soak pull capture is incomplete")
	}
	var mainSequence uint64
	for _, wire := range wires {
		if !wire.ExpectChecksumConvergence {
			continue
		}
		if mainSequence != 0 {
			return invariants.PullResultObservation{}, nil, errors.New("soak pull capture has multiple checksum exchanges")
		}
		mainSequence = wire.Sequence
	}
	if mainSequence == 0 {
		return invariants.PullResultObservation{}, nil, errors.New("soak pull capture has no checksum exchange")
	}
	cursors := make([]invariants.CursorPositionObservation, 0, len(p.responseCursors))
	for _, position := range positions {
		raw, found := p.responseCursors[position.ScopeID]
		if !found || raw != position.RawCursor {
			continue
		}
		cursors = append(cursors, position)
	}
	if len(cursors) != 2 {
		return invariants.PullResultObservation{}, nil, errors.New("soak pull cursor positions are incomplete")
	}
	result := invariants.PullResultObservation{
		ExchangeSequence: mainSequence,
		UserID:           soakUserID,
		ClientID:         soakClientID,
		Changes:          []invariants.PullChangeIdentityObservation{p.change},
		Cursors:          cursors,
	}
	acknowledgements := make([]invariants.CursorAcknowledgementObservation, len(cursors))
	for index, cursor := range cursors {
		acknowledgements[index] = invariants.CursorAcknowledgementObservation{ExchangeSequence: mainSequence, Cursor: cursor}
	}
	return result, acknowledgements, nil
}

func (h *liveSoakHarness) capture(ctx context.Context, operation soak.Operation, wires []invariants.WireExchangeObservation, activation *soak.FaultActivationObservation) (soak.ObservationCapture, error) {
	facts, err := h.native.Capture(ctx, nil, []string{"server-state"})
	if err != nil || len(facts) != 1 {
		return soak.ObservationCapture{}, fmt.Errorf("capture soak server state: %w", err)
	}
	serverState := facts[0].StateFacts
	// The native controller knows authored row aliases only. Keep its independent
	// ledger and stream projection, and omit scope families it cannot observe for
	// direct protocol rows.
	serverState.Rows = nil
	serverState.Scopes = nil
	serverState.RowScopeEdges = nil
	checkpoints, positions, err := h.captureCheckpoints(ctx)
	if err != nil {
		return soak.ObservationCapture{}, err
	}
	client, err := h.captureClient(operation.Kind == soak.OperationProcessDeath)
	if err != nil {
		return soak.ObservationCapture{}, err
	}
	manifest := h.manifest
	capture := soak.ObservationCapture{
		Manifest:        &manifest,
		ServerState:     &serverState,
		Operator:        &invariants.OperatorObservation{Checkpoints: checkpoints},
		Clients:         []invariants.ClientObservation{client},
		WireExchanges:   wires,
		CursorPositions: positions,
		FaultActivation: activation,
	}
	if operation.Kind == soak.OperationPull || operation.Kind == soak.OperationRebuild {
		capture.ServerRowIdentities = make([]invariants.ServerRowIdentityObservation, 0)
	}
	return capture, nil
}

func (h *liveSoakHarness) captureClient(restart bool) (invariants.ClientObservation, error) {
	rows := make([]invariants.ClientRowObservation, 0, len(h.rows))
	rowKeys := make([]string, 0, len(h.rows))
	for key := range h.rows {
		rowKeys = append(rowKeys, key)
	}
	sort.Strings(rowKeys)
	for _, key := range rowKeys {
		row := h.rows[key]
		digest := row.Digest
		rows = append(rows, invariants.ClientRowObservation{
			TableID:       row.TableID,
			Row:           cloneVectorRow(row.Row),
			ServerVersion: row.Version,
			StoredDigest:  &digest,
		})
	}
	scopes := make([]invariants.ClientScopeObservation, 0, len(h.protocol.Scopes))
	scopeRows := make([]invariants.ClientScopeRowObservation, 0)
	stateCheckpoints := make([]scenarios.CheckpointFact, 0, len(h.protocol.Scopes))
	for _, scopeID := range h.scopeIDs() {
		cursor := h.protocol.Scopes[scopeID]
		entries, err := h.scopeDigestEntries(scopeID)
		if err != nil {
			return invariants.ClientObservation{}, err
		}
		local, err := vectors.ScopeDigest(h.manifest.Hash(), scopeID, entries)
		if err != nil {
			return invariants.ClientObservation{}, fmt.Errorf("compute soak client scope digest: %w", err)
		}
		scope := invariants.ClientScopeObservation{
			ScopeID:    scopeID,
			Generation: 1,
		}
		authoritative, hasChecksum := h.authoritativeDigest[scopeID]
		if hasChecksum {
			scope.AuthoritativeDigest = &authoritative
			scope.LocalDigest = &local
		}
		if cursor != "" {
			cursorCopy := cursor
			scope.RawCursor = &cursorCopy
		}
		scopes = append(scopes, scope)
		stateCheckpoints = append(stateCheckpoints, scenarios.CheckpointFact{
			ScopeID: scopeID, HasCursor: cursor != "", HasChecksum: hasChecksum, Verified: hasChecksum && authoritative == local,
		})
		for _, entry := range entries {
			scopeRows = append(scopeRows, invariants.ClientScopeRowObservation{ScopeID: scopeID, Entry: entry, Generation: 1})
		}
	}
	rowCount := uint64(len(rows))
	checkpointCount := uint64(len(stateCheckpoints))
	zero := uint64(0)
	return invariants.ClientObservation{
		State: scenarios.ClientDurabilityFact{
			UserID:          soakUserID,
			ClientID:        soakClientID,
			CurrentSchema:   &scenarios.SchemaFact{Version: h.manifestDocument.SchemaVersion, Hash: h.manifestDocument.SchemaHash},
			RowCount:        &rowCount,
			CheckpointCount: &checkpointCount,
			QueueCount:      &zero,
			OutcomeCount:    &zero,
			Checkpoints:     stateCheckpoints,
		},
		Rows:      rows,
		Scopes:    scopes,
		ScopeRows: scopeRows,
		Process: &invariants.ProcessIdentityObservation{
			ProcessID:                   fmt.Sprintf("soak-protocol-process-%d", h.processGeneration),
			DatabaseIdentityFingerprint: h.databaseFingerprint,
		},
		RestartBoundary: restart,
		Complete:        true,
	}, nil
}

func (h *liveSoakHarness) captureCheckpoints(ctx context.Context) ([]invariants.OperatorCheckpointObservation, []invariants.CursorPositionObservation, error) {
	observed, err := h.server.Operator().ObserveClientCheckpoints(ctx, soakClientID)
	if err != nil {
		return nil, nil, fmt.Errorf("observe soak client checkpoints: %w", err)
	}
	byScope := make(map[string]blackbox.ClientCheckpointObservation, len(observed))
	for _, checkpoint := range observed {
		byScope[checkpoint.ScopeID] = checkpoint
	}
	checkpoints := make([]invariants.OperatorCheckpointObservation, 0, len(h.protocol.Scopes))
	positions := make([]invariants.CursorPositionObservation, 0, len(h.protocol.Scopes))
	for _, scopeID := range h.scopeIDs() {
		checkpoint, found := byScope[scopeID]
		if !found {
			return nil, nil, fmt.Errorf("soak checkpoint for scope %q is absent", scopeID)
		}
		position, err := soakPosition(checkpoint)
		if err != nil {
			return nil, nil, err
		}
		checkpoints = append(checkpoints, invariants.OperatorCheckpointObservation{
			UserID:           soakUserID,
			ClientID:         soakClientID,
			ScopeID:          scopeID,
			StreamGeneration: checkpoint.StreamGeneration,
			Position:         position,
		})
		cursor := h.protocol.Scopes[scopeID]
		if cursor == "" {
			continue
		}
		positions = append(positions, invariants.CursorPositionObservation{
			UserID:           soakUserID,
			ClientID:         soakClientID,
			ScopeID:          scopeID,
			Generation:       1,
			RawCursor:        cursor,
			StreamGeneration: checkpoint.StreamGeneration,
			Position:         position,
		})
	}
	return checkpoints, positions, nil
}

func soakPosition(value blackbox.ClientCheckpointObservation) (invariants.PositionObservation, error) {
	position := invariants.PositionObservation{Kind: value.PositionKind}
	if value.CommitLSNValid {
		commit := value.CommitLSN
		position.CommitLSN = &commit
	}
	if value.EventOrdinalValid {
		if value.EventOrdinal < 0 {
			return invariants.PositionObservation{}, errors.New("soak checkpoint event ordinal is negative")
		}
		event := uint64(value.EventOrdinal)
		position.EventOrdinal = &event
	}
	if value.EffectOrdinalValid {
		if value.EffectOrdinal < 0 {
			return invariants.PositionObservation{}, errors.New("soak checkpoint effect ordinal is negative")
		}
		effect := uint64(value.EffectOrdinal)
		position.EffectOrdinal = &effect
	}
	if position.Kind == "" || value.StreamGeneration == "" {
		return invariants.PositionObservation{}, errors.New("soak checkpoint position is incomplete")
	}
	return position, nil
}

func (h *liveSoakHarness) connect(ctx context.Context, requestClass string) (soakRecordedCall, error) {
	knownScopes := make(map[string]any, len(h.protocol.Scopes))
	for scopeID, cursor := range h.protocol.Scopes {
		var value any
		if cursor != "" {
			value = cursor
		}
		knownScopes[scopeID] = map[string]any{"cursor": value}
	}
	payload := map[string]any{
		"client_id":         soakClientID,
		"platform":          "conformance-soak",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            h.protocol.Schema,
		"scope_set_version": h.protocol.ScopeSetVersion,
		"known_scopes":      knownScopes,
	}
	response, body, call, err := h.doJSON(ctx, &h.client, http.MethodPost, "/sync/connect", requestClass, payload)
	if err != nil {
		return soakRecordedCall{}, err
	}
	if response.Status != http.StatusOK {
		return soakRecordedCall{}, fmt.Errorf("soak connect status = %d", response.Status)
	}
	generation, ok := jsonInt64(body["client_generation"])
	if !ok || generation <= 0 {
		return soakRecordedCall{}, errors.New("soak connect client generation is invalid")
	}
	scopeVersion, ok := jsonInt64(body["scope_set_version"])
	if !ok || scopeVersion <= 0 {
		return soakRecordedCall{}, errors.New("soak connect scope version is invalid")
	}
	schema, ok := body["schema"].(map[string]any)
	if !ok {
		return soakRecordedCall{}, errors.New("soak connect schema is invalid")
	}
	h.protocol.Generation = generation
	h.protocol.ScopeSetVersion = scopeVersion
	h.protocol.Schema = cloneAnyMap(schema)
	delta, ok := body["scopes"].(map[string]any)
	if !ok {
		return soakRecordedCall{}, errors.New("soak connect scope delta is invalid")
	}
	if additions, ok := delta["add"].([]any); ok {
		for _, raw := range additions {
			addition, ok := raw.(map[string]any)
			if !ok {
				return soakRecordedCall{}, errors.New("soak connect scope addition is invalid")
			}
			scopeID, _ := addition["id"].(string)
			if scopeID == "" {
				return soakRecordedCall{}, errors.New("soak connect scope identity is invalid")
			}
			cursor, _ := addition["cursor"].(string)
			h.protocol.Scopes[scopeID] = cursor
		}
	}
	if removals, ok := delta["remove"].([]any); ok {
		for _, raw := range removals {
			scopeID, ok := raw.(string)
			if ok {
				delete(h.protocol.Scopes, scopeID)
				delete(h.scopeRows, scopeID)
			}
		}
	}
	return call, nil
}

func (h *liveSoakHarness) loadManifest(ctx context.Context, requestClass string) (soakRecordedCall, error) {
	response, body, call, err := h.doJSON(ctx, &h.client, http.MethodGet, "/sync/schema", requestClass, nil)
	if err != nil {
		return soakRecordedCall{}, err
	}
	if response.Status != http.StatusOK {
		return soakRecordedCall{}, fmt.Errorf("soak schema status = %d", response.Status)
	}
	rawManifest, err := json.Marshal(body["manifest"])
	if err != nil || string(rawManifest) == "null" {
		return soakRecordedCall{}, errors.New("soak schema manifest is absent")
	}
	manifest, err := vectors.ParseManifest(rawManifest)
	if err != nil {
		return soakRecordedCall{}, fmt.Errorf("parse soak manifest: %w", err)
	}
	var document soakManifestDocument
	if err := json.Unmarshal(rawManifest, &document); err != nil || document.SchemaVersion == 0 || document.SchemaHash == "" {
		return soakRecordedCall{}, errors.New("decode soak manifest document failed")
	}
	tables, err := soakProtocolTables(document)
	if err != nil {
		return soakRecordedCall{}, err
	}
	h.manifest = manifest
	h.manifestDocument = document
	h.protocol.Tables = tables
	return call, nil
}

func soakProtocolTables(document soakManifestDocument) (map[string]soakProtocolTable, error) {
	result := make(map[string]soakProtocolTable)
	for _, table := range document.Tables {
		if table.Name != "cf_items" && table.Name != "cf_global_items" {
			continue
		}
		valueField := ""
		for _, field := range table.Fields {
			if field.Name == "value" {
				valueField = field.ID
			}
		}
		if table.ID == "" || table.PrimaryKeyFieldID == "" || valueField == "" {
			return nil, fmt.Errorf("soak protocol table %q is incomplete", table.Name)
		}
		result[table.Name] = soakProtocolTable{
			Name: table.Name, ID: table.ID, PrimaryKeyField: table.PrimaryKeyFieldID, ValueField: valueField,
		}
	}
	if len(result) != 2 {
		return nil, errors.New("soak protocol manifest lacks diagnostic tables")
	}
	return result, nil
}

func (h *liveSoakHarness) requireScopeSet() error {
	if len(h.protocol.Scopes) != 2 {
		return fmt.Errorf("soak client scope count = %d, want 2", len(h.protocol.Scopes))
	}
	for _, scopeID := range []string{soakPrivateScope, soakSharedScope} {
		if _, found := h.protocol.Scopes[scopeID]; !found {
			return fmt.Errorf("soak client scope %q is absent", scopeID)
		}
	}
	return nil
}

func (h *liveSoakHarness) submitInsert(ctx context.Context, scopeID, label string) (soakRecordedCall, string, error) {
	tableName := "cf_items"
	if scopeID == soakSharedScope {
		tableName = "cf_global_items"
	}
	table, found := h.protocol.Tables[tableName]
	if !found {
		return soakRecordedCall{}, "", fmt.Errorf("soak table %q is absent", tableName)
	}
	recordID := h.nextUUID(label + "-record")
	mutationID := h.nextUUID(label + "-mutation")
	batchID := h.nextUUID(label + "-batch")
	clientVersion := h.nextUUID(label + "-client-version")
	pk := map[string]any{table.PrimaryKeyField: recordID}
	columns := map[string]any{table.ValueField: fmt.Sprintf("soak-%d-%s", h.seed, label)}
	payload := map[string]any{
		"client_id":         soakClientID,
		"client_generation": h.protocol.Generation,
		"batch_id":          batchID,
		"schema":            h.protocol.Schema,
		"mutations": []any{map[string]any{
			"mutation_id": mutationID, "table": table.ID, "pk": pk,
			"authored_schema": h.protocol.Schema, "op": "insert", "client_version": clientVersion, "columns": columns,
		}},
	}
	response, body, call, err := h.doJSON(ctx, &h.client, http.MethodPost, "/sync/push", "soak-push", payload)
	if err != nil {
		return soakRecordedCall{}, "", err
	}
	if response.Status != http.StatusOK {
		return soakRecordedCall{}, "", fmt.Errorf("soak push status = %d", response.Status)
	}
	accepted, ok := body["accepted"].([]any)
	if !ok || len(accepted) != 1 {
		return soakRecordedCall{}, "", errors.New("soak push accepted partition is invalid")
	}
	rejected, ok := body["rejected"].([]any)
	if !ok || len(rejected) != 0 {
		return soakRecordedCall{}, "", errors.New("soak push rejected partition is not empty")
	}
	outcome, ok := accepted[0].(map[string]any)
	if !ok {
		return soakRecordedCall{}, "", errors.New("soak push outcome is invalid")
	}
	if err := h.applyWireRow(scopeID, outcome); err != nil {
		return soakRecordedCall{}, "", err
	}
	if err := h.waitForWALRecord(ctx, tableName, recordID); err != nil {
		return soakRecordedCall{}, "", err
	}
	return call, recordID, nil
}

func (h *liveSoakHarness) waitForWALRecord(ctx context.Context, tableName, recordID string) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		observation, err := h.server.Operator().ObserveWALRecordsForTable(ctx, tableName, []string{recordID})
		if err == nil && len(observation.Records) == 1 && observation.WorkerRunning && !observation.BlockingPoison && observation.ContiguousAcknowledged {
			return nil
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("soak WAL record %q did not materialize", recordID)
}

func (h *liveSoakHarness) rebuildScope(ctx context.Context, scopeID, requestClass string) ([]soakRecordedCall, error) {
	rebuildID := h.nextUUID(requestClass + "-id")
	var cursor any
	var calls []soakRecordedCall
	rebuiltRows := make(map[string]soakHeldRow)
	for page := 0; page < 64; page++ {
		payload := map[string]any{
			"client_id": soakClientID, "client_generation": h.protocol.Generation,
			"schema": h.protocol.Schema, "scope": scopeID, "rebuild_id": rebuildID, "cursor": cursor, "limit": 1000,
		}
		response, body, call, err := h.doJSON(ctx, &h.client, http.MethodPost, "/sync/rebuild", requestClass, payload)
		if err != nil {
			return nil, err
		}
		if response.Status != http.StatusOK {
			return nil, fmt.Errorf("soak rebuild status = %d", response.Status)
		}
		calls = append(calls, call)
		records, ok := body["records"].([]any)
		if !ok {
			return nil, errors.New("soak rebuild records are invalid")
		}
		for _, raw := range records {
			record, ok := raw.(map[string]any)
			if !ok {
				return nil, errors.New("soak rebuild record is invalid")
			}
			row, key, err := h.decodeWireRow(record)
			if err != nil {
				return nil, err
			}
			row.Memberships = map[string]struct{}{scopeID: {}}
			rebuiltRows[key] = row
		}
		hasMore, ok := body["has_more"].(bool)
		if !ok {
			return nil, errors.New("soak rebuild finality is invalid")
		}
		if hasMore {
			next, ok := body["cursor"].(string)
			if !ok || next == "" {
				return nil, errors.New("soak rebuild continuation is invalid")
			}
			cursor = next
			continue
		}
		finalCursor, ok := body["final_scope_cursor"].(string)
		if !ok || finalCursor == "" {
			return nil, errors.New("soak rebuild final cursor is invalid")
		}
		digest, err := checksumDigest(body["checksum"])
		if err != nil {
			return nil, err
		}
		h.replaceScopeRows(scopeID, rebuiltRows)
		h.protocol.Scopes[scopeID] = finalCursor
		h.authoritativeDigest[scopeID] = digest
		return calls, nil
	}
	return nil, errors.New("soak rebuild exceeded its page bound")
}

func (h *liveSoakHarness) replaceScopeRows(scopeID string, rebuilt map[string]soakHeldRow) {
	for key, row := range h.rows {
		delete(row.Memberships, scopeID)
		if len(row.Memberships) == 0 {
			delete(h.rows, key)
			continue
		}
		h.rows[key] = row
	}
	for key, replacement := range rebuilt {
		if existing, found := h.rows[key]; found {
			replacement.Memberships = existing.Memberships
			replacement.Memberships[scopeID] = struct{}{}
		}
		h.rows[key] = replacement
	}
	h.rebuildScopeIndex()
}

func (h *liveSoakHarness) drainPulls(ctx context.Context) error {
	for page := 0; page < 64; page++ {
		requestCursors := cloneStringMap(h.protocol.Scopes)
		response, body, _, err := h.pull(ctx, h.protocol.Scopes, "soak-drain")
		if err != nil {
			return err
		}
		if response.Status != http.StatusOK {
			return fmt.Errorf("soak drain pull status = %d", response.Status)
		}
		changeCount, err := h.applyPullResponse(body)
		if err != nil {
			return err
		}
		hasMore, ok := body["has_more"].(bool)
		if !ok {
			return errors.New("soak drain pull finality is invalid")
		}
		if !hasMore && changeCount == 0 && equalStringMap(requestCursors, h.protocol.Scopes) {
			return nil
		}
	}
	return errors.New("soak drain pull exceeded its page bound")
}

func (h *liveSoakHarness) executePullControl(ctx context.Context, operation soak.Operation) (*soakPullCapture, error) {
	if err := h.drainPulls(ctx); err != nil {
		return nil, err
	}
	if _, _, err := h.submitInsert(ctx, operation.ScopeID, "pull-control"); err != nil {
		return nil, err
	}
	response, body, mainCall, err := h.pull(ctx, h.protocol.Scopes, "soak-pull-control")
	if err != nil {
		return nil, err
	}
	if response.Status != http.StatusOK {
		return nil, fmt.Errorf("soak pull control status = %d", response.Status)
	}
	changes, ok := body["changes"].([]any)
	if !ok || len(changes) != 1 {
		return nil, fmt.Errorf("soak pull control change count = %d, want 1", len(changes))
	}
	change, ok := changes[0].(map[string]any)
	if !ok || change["scope"] != operation.ScopeID {
		return nil, errors.New("soak pull control returned the wrong scope")
	}
	identity, err := pullChangeIdentity(change)
	if err != nil {
		return nil, err
	}
	if _, err := h.applyPullResponse(body); err != nil {
		return nil, err
	}
	responseCursors := cloneStringMap(h.protocol.Scopes)
	ackResponse, ackBody, ackCall, err := h.pull(ctx, responseCursors, "soak-pull-acknowledgement")
	if err != nil {
		return nil, err
	}
	if ackResponse.Status != http.StatusOK || pullChangeCount(ackBody) != 0 {
		return nil, errors.New("soak pull acknowledgement did not terminate empty")
	}
	selected := map[string]string{operation.ScopeID: responseCursors[operation.ScopeID]}
	zeroResponse, zeroBody, zeroCall, err := h.pull(ctx, selected, "soak-pull-scope-control")
	if err != nil {
		return nil, err
	}
	if zeroResponse.Status != http.StatusOK || pullChangeCount(zeroBody) != 0 {
		return nil, errors.New("soak selected-scope control did not terminate empty")
	}
	h.protocol.Scopes = responseCursors
	mainWire, err := h.wireFromCalls(operation, ackCall, mainCall, false, true, false)
	if err != nil {
		return nil, err
	}
	if h.corruptNextChecksum {
		mainWire.ResponseBody, err = corruptPullRowChecksum(mainWire.ResponseBody)
		if err != nil {
			return nil, err
		}
		h.corruptNextChecksum = false
	}
	zeroWire, err := h.wireFromCall(operation, zeroCall, false, false, true)
	if err != nil {
		return nil, err
	}
	return &soakPullCapture{
		wires:  []invariants.WireExchangeObservation{mainWire, zeroWire},
		change: identity, responseCursors: responseCursors,
	}, nil
}

func (h *liveSoakHarness) pull(ctx context.Context, cursors map[string]string, requestClass string) (blackbox.Response, map[string]any, soakRecordedCall, error) {
	scopes := make(map[string]any, len(cursors))
	for scopeID, cursor := range cursors {
		var value any
		if cursor != "" {
			value = cursor
		}
		scopes[scopeID] = map[string]any{"cursor": value}
	}
	payload := map[string]any{
		"client_id": soakClientID, "client_generation": h.protocol.Generation,
		"schema": h.protocol.Schema, "scope_set_version": h.protocol.ScopeSetVersion,
		"scopes": scopes, "limit": 1000,
	}
	return h.doJSON(ctx, &h.client, http.MethodPost, "/sync/pull", requestClass, payload)
}

func (h *liveSoakHarness) applyPullResponse(body map[string]any) (int, error) {
	changes, ok := body["changes"].([]any)
	if !ok {
		return 0, errors.New("soak pull changes are invalid")
	}
	for _, raw := range changes {
		change, ok := raw.(map[string]any)
		if !ok {
			return 0, errors.New("soak pull change is invalid")
		}
		scopeID, _ := change["scope"].(string)
		if scopeID == "" {
			return 0, errors.New("soak pull change scope is invalid")
		}
		if err := h.applyWireRow(scopeID, change); err != nil {
			return 0, err
		}
	}
	cursors, ok := body["scope_cursors"].(map[string]any)
	if !ok {
		return 0, errors.New("soak pull cursors are invalid")
	}
	for scopeID, raw := range cursors {
		cursor, ok := raw.(string)
		if !ok || cursor == "" {
			return 0, errors.New("soak pull cursor is invalid")
		}
		if _, assigned := h.protocol.Scopes[scopeID]; !assigned {
			return 0, fmt.Errorf("soak pull returned unassigned scope %q", scopeID)
		}
		h.protocol.Scopes[scopeID] = cursor
	}
	if checksums, ok := body["checksums"].(map[string]any); ok {
		for scopeID, raw := range checksums {
			digest, err := checksumDigest(raw)
			if err != nil {
				return 0, err
			}
			h.authoritativeDigest[scopeID] = digest
		}
	}
	return len(changes), nil
}

func pullChangeCount(body map[string]any) int {
	changes, _ := body["changes"].([]any)
	return len(changes)
}

func pullChangeIdentity(change map[string]any) (invariants.PullChangeIdentityObservation, error) {
	scopeID, _ := change["scope"].(string)
	tableID, _ := change["table"].(string)
	pk, ok := change["pk"].(map[string]any)
	if scopeID == "" || tableID == "" || !ok || len(pk) != 1 {
		return invariants.PullChangeIdentityObservation{}, errors.New("soak pull change identity is invalid")
	}
	for fieldID, value := range pk {
		raw, err := json.Marshal(value)
		if err != nil {
			return invariants.PullChangeIdentityObservation{}, errors.New("encode soak pull primary key failed")
		}
		return invariants.PullChangeIdentityObservation{
			ScopeID: scopeID, TableID: tableID, PrimaryKeyFieldID: fieldID, PrimaryKey: raw,
		}, nil
	}
	return invariants.PullChangeIdentityObservation{}, errors.New("soak pull primary key is absent")
}

func (h *liveSoakHarness) applyWireRow(scopeID string, object map[string]any) error {
	row, key, err := h.decodeWireRow(object)
	if err != nil {
		return err
	}
	if existing, found := h.rows[key]; found {
		row.Memberships = existing.Memberships
	}
	if row.Memberships == nil {
		row.Memberships = make(map[string]struct{})
	}
	row.Memberships[scopeID] = struct{}{}
	h.rows[key] = row
	h.rebuildScopeIndex()
	delete(h.authoritativeDigest, scopeID)
	return nil
}

func (h *liveSoakHarness) decodeWireRow(object map[string]any) (soakHeldRow, string, error) {
	tableID, _ := object["table"].(string)
	pkObject, pkOK := object["pk"].(map[string]any)
	rowObject, rowOK := object["row"].(map[string]any)
	if !rowOK {
		rowObject, rowOK = object["server_row"].(map[string]any)
	}
	version, _ := object["server_version"].(string)
	if tableID == "" || !pkOK || len(pkObject) != 1 || !rowOK || len(rowObject) == 0 || version == "" {
		return soakHeldRow{}, "", errors.New("soak wire row is incomplete")
	}
	var primaryKey json.RawMessage
	for _, value := range pkObject {
		var err error
		primaryKey, err = json.Marshal(value)
		if err != nil {
			return soakHeldRow{}, "", errors.New("encode soak row primary key failed")
		}
	}
	fieldIDs := make([]string, 0, len(rowObject))
	for fieldID := range rowObject {
		fieldIDs = append(fieldIDs, fieldID)
	}
	sort.Strings(fieldIDs)
	fields := make([]vectors.RowField, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		raw, err := json.Marshal(rowObject[fieldID])
		if err != nil {
			return soakHeldRow{}, "", errors.New("encode soak row field failed")
		}
		fields = append(fields, vectors.RowField{FieldID: fieldID, Value: raw})
	}
	row := vectors.Row{PK: primaryKey, Fields: fields}
	digest, err := checksumDigest(object["row_checksum"])
	if err != nil {
		return soakHeldRow{}, "", err
	}
	computed, err := vectors.RowDigest(h.manifest, tableID, row, version)
	if err != nil {
		return soakHeldRow{}, "", fmt.Errorf("compute soak row digest: %w", err)
	}
	if computed != digest {
		return soakHeldRow{}, "", errors.New("soak row checksum does not match its canonical row")
	}
	identity, err := vectors.RowIdentity(h.manifest, tableID, primaryKey)
	if err != nil {
		return soakHeldRow{}, "", fmt.Errorf("compute soak row identity: %w", err)
	}
	return soakHeldRow{TableID: tableID, Row: row, Version: version, Digest: digest}, string(identity), nil
}

func (h *liveSoakHarness) rebuildScopeIndex() {
	h.scopeRows = make(map[string]map[string]struct{}, len(h.protocol.Scopes))
	for scopeID := range h.protocol.Scopes {
		h.scopeRows[scopeID] = make(map[string]struct{})
	}
	for key, row := range h.rows {
		for scopeID := range row.Memberships {
			if h.scopeRows[scopeID] == nil {
				h.scopeRows[scopeID] = make(map[string]struct{})
			}
			h.scopeRows[scopeID][key] = struct{}{}
		}
	}
}

func (h *liveSoakHarness) scopeDigestEntries(scopeID string) ([]vectors.DigestEntry, error) {
	keys := make([]string, 0, len(h.scopeRows[scopeID]))
	for key := range h.scopeRows[scopeID] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]vectors.DigestEntry, 0, len(keys))
	for _, key := range keys {
		row, found := h.rows[key]
		if !found {
			return nil, errors.New("soak scope row has no held row")
		}
		entries = append(entries, vectors.DigestEntry{RowIdentity: []byte(key), RowDigest: row.Digest})
	}
	return entries, nil
}

func (h *liveSoakHarness) transitionSchema(ctx context.Context) (soakRecordedCall, error) {
	h.schemaTransition++
	table, err := cloneJSONObject(h.authoredTable)
	if err != nil {
		return soakRecordedCall{}, err
	}
	fields, ok := table["fields"].([]any)
	if !ok {
		return soakRecordedCall{}, errors.New("soak authored schema fields are invalid")
	}
	nextFields := make([]any, 0, len(fields)+1)
	for _, raw := range fields {
		field, ok := raw.(map[string]any)
		if !ok {
			return soakRecordedCall{}, errors.New("soak authored schema field is invalid")
		}
		name, _ := field["name"].(string)
		if strings.HasPrefix(name, "soak_value_") {
			continue
		}
		nextFields = append(nextFields, field)
	}
	fieldName := fmt.Sprintf("soak_value_%03d", h.schemaTransition)
	nextFields = append(nextFields, map[string]any{
		"field_id": fieldName, "name": fieldName, "type": "string", "primary_key": false,
		"nullable": true, "writable": true, "decimal_precision": nil, "decimal_scale": nil, "default_wire_json": nil,
	})
	table["fields"] = nextFields
	hash := lowerSHA256(fmt.Sprintf("soak-schema:%d:%d", h.seed, h.schemaTransition))
	version := 1000 + h.schemaTransition
	body, err := json.Marshal(map[string]any{"schema_version": version, "schema_hash": hash})
	if err != nil {
		return soakRecordedCall{}, errors.New("encode soak authored schema body failed")
	}
	payload := map[string]any{
		"schema": map[string]any{"version": version, "hash": hash}, "body": string(body),
		"transition_class": "class_2", "compatibility_floor": 1,
		"tables": []any{table}, "affected_scopes": []any{"scope-a", "scope-b"},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return soakRecordedCall{}, errors.New("encode soak schema transition failed")
	}
	operation := scenarios.Operation{ContractOperation: "model", Name: "publish-schema", Payload: raw}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return soakRecordedCall{}, fmt.Errorf("validate soak schema transition: %w", err)
	}
	observation, err := h.native.ApplyStep(ctx, operation)
	if err != nil {
		return soakRecordedCall{}, fmt.Errorf("apply soak schema transition: %w", err)
	}
	if observation.Disposition != "success" {
		return soakRecordedCall{}, errors.New("soak schema transition did not succeed")
	}
	h.authoredTable = table
	call, err := h.loadManifest(ctx, "soak-schema-transition")
	if err != nil {
		return soakRecordedCall{}, err
	}
	if _, err := h.connect(ctx, "soak-schema-reconnect"); err != nil {
		return soakRecordedCall{}, err
	}
	for _, scopeID := range h.scopeIDs() {
		if _, err := h.rebuildScope(ctx, scopeID, "soak-schema-rebuild"); err != nil {
			return soakRecordedCall{}, err
		}
	}
	if err := h.drainPulls(ctx); err != nil {
		return soakRecordedCall{}, err
	}
	return call, nil
}

func (h *liveSoakHarness) executeProcessDeath(ctx context.Context) error {
	recordID := h.nextUUID("process-death")
	observation, err := h.server.Operator().RunWALReplayRestartControl(ctx, recordID)
	if err != nil {
		return fmt.Errorf("execute soak WAL process death: %w", err)
	}
	if !observation.WorkerExitedBeforeAcknowledgement || !observation.WorkerRestarted {
		return errors.New("soak WAL process death did not cross the required restart boundary")
	}
	return nil
}

func (h *liveSoakHarness) activateOperationFault(ctx context.Context, operation soak.Operation) (*soak.FaultActivationObservation, *invariants.WireExchangeObservation, error) {
	if operation.FaultPlan == nil {
		return nil, nil, nil
	}
	activation := &soak.FaultActivationObservation{
		ControlID: string(operation.FaultPlan.ControlID), Target: operation.FaultPlan.Injection.Target, Activated: true,
	}
	if operation.Kind == soak.OperationProcessDeath {
		activation.CleanedUp = true
		return activation, nil, nil
	}
	owner, err := faults.NewController(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("create soak fault controller: %w", err)
	}
	options := soakWireOptions(operation.FaultPlan.Injection.Operator)
	wireFault, err := faults.NewWireFault(ctx, owner, http.DefaultTransport, options)
	if err != nil {
		_ = owner.Close()
		return nil, nil, fmt.Errorf("create soak wire fault: %w", err)
	}
	faultClient := h.client
	faultClient.HTTP = &http.Client{Transport: wireFault, Timeout: 30 * time.Second}
	payload := map[string]any{
		"client_id": soakClientID, "platform": "conformance-soak", "app_version": "0.3.0",
		"protocol_version": 3, "schema": h.protocol.Schema, "scope_set_version": h.protocol.ScopeSetVersion,
		"known_scopes": map[string]any{},
	}
	offset := h.recorder.Len()
	_, _ = faultClient.Do(ctx, blackbox.Request{
		Method: http.MethodPost, Path: "/sync/connect", Headers: http.Header{"Content-Type": []string{"application/json"}},
		Body: mustMarshalJSON(payload), Class: "soak-wire-fault",
	})
	call, captureErr := h.recordedCall(offset)
	closeErr := errors.Join(wireFault.Close(), owner.Close())
	if captureErr != nil || closeErr != nil {
		return nil, nil, errors.Join(captureErr, closeErr)
	}
	activation.CleanedUp = true
	wire, err := h.wireFromCall(operation, call, false, false, false)
	if err != nil {
		return nil, nil, err
	}
	return activation, &wire, nil
}

func soakWireOptions(operator string) faults.WireOptions {
	switch operator {
	case "crash":
		return faults.WireOptions{Mode: faults.WireResponseLoss}
	case "delay":
		return faults.WireOptions{Mode: faults.WireTimeout}
	case "duplicate":
		return faults.WireOptions{Mode: faults.WireDuplicate}
	case "replay":
		return faults.WireOptions{Mode: faults.WireReplay, ReplayCount: 2}
	default:
		return faults.WireOptions{Mode: faults.WireTemporaryUnavailable}
	}
}

func (h *liveSoakHarness) doJSON(ctx context.Context, client *blackbox.Client, method, path, requestClass string, payload any) (blackbox.Response, map[string]any, soakRecordedCall, error) {
	body := []byte(nil)
	if payload != nil {
		body = mustMarshalJSON(payload)
	}
	offset := h.recorder.Len()
	response, err := client.Do(ctx, blackbox.Request{
		Method: method, Path: path, Headers: http.Header{"Content-Type": []string{"application/json"}}, Body: body, Class: requestClass,
	})
	if err != nil {
		return blackbox.Response{}, nil, soakRecordedCall{}, fmt.Errorf("execute soak HTTP request %s: %w", path, err)
	}
	call, err := h.recordedCall(offset)
	if err != nil {
		return blackbox.Response{}, nil, soakRecordedCall{}, err
	}
	var object map[string]any
	if err := json.Unmarshal(response.Body, &object); err != nil || object == nil {
		return blackbox.Response{}, nil, soakRecordedCall{}, fmt.Errorf("decode soak HTTP response %s failed", path)
	}
	return response, object, call, nil
}

func (h *liveSoakHarness) recordedCall(offset int) (soakRecordedCall, error) {
	records, err := h.recorder.Snapshot(offset)
	if err != nil {
		return soakRecordedCall{}, fmt.Errorf("snapshot soak recorder: %w", err)
	}
	if len(records) != 1 {
		return soakRecordedCall{}, fmt.Errorf("soak recorder captured %d exchanges, want 1", len(records))
	}
	return soakRecordedCall{metadata: records[0]}, nil
}

func (h *liveSoakHarness) wireFromCall(operation soak.Operation, call soakRecordedCall, mutation, checksum, isolation bool) (invariants.WireExchangeObservation, error) {
	return h.wireFromCalls(operation, call, call, mutation, checksum, isolation)
}

func (h *liveSoakHarness) wireFromCalls(operation soak.Operation, requestCall, responseCall soakRecordedCall, mutation, checksum, isolation bool) (invariants.WireExchangeObservation, error) {
	requestBody, err := h.attachment(requestCall.metadata.RequestAttachmentID)
	if err != nil {
		return invariants.WireExchangeObservation{}, err
	}
	responseBody, err := h.attachment(responseCall.metadata.ResponseAttachmentID)
	if err != nil {
		return invariants.WireExchangeObservation{}, err
	}
	requestBody, err = bindWireTarget(requestBody, operation)
	if err != nil {
		return invariants.WireExchangeObservation{}, err
	}
	return invariants.WireExchangeObservation{
		OperationClass: string(operation.Kind), RequestBody: requestBody,
		ResponseStatus: responseCall.metadata.Status, ResponseBody: responseBody,
		ExpectMutationConservation: mutation, ExpectChecksumConvergence: checksum, ExpectScopeIsolation: isolation,
	}, nil
}

func (h *liveSoakHarness) attachment(id string) ([]byte, error) {
	path, err := h.recorder.AttachmentPath(id)
	if err != nil {
		return nil, fmt.Errorf("resolve soak recorder attachment: %w", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read soak recorder attachment: %w", err)
	}
	if int64(len(body)) > soakBodyLimit {
		return nil, errors.New("soak recorder attachment exceeds its bound")
	}
	return body, nil
}

func bindWireTarget(raw []byte, operation soak.Operation) ([]byte, error) {
	object := make(map[string]any)
	if len(strings.TrimSpace(string(raw))) != 0 {
		if err := json.Unmarshal(raw, &object); err != nil {
			return nil, errors.New("decode soak request attachment failed")
		}
	}
	object["authenticated_user_id"] = operation.UserID
	if _, present := object["client_id"]; !present {
		object["client_id"] = operation.ClientID
	}
	if _, present := object["scopes"]; !present {
		object["scope_id"] = operation.ScopeID
	}
	return json.Marshal(object)
}

func corruptPullRowChecksum(raw []byte) ([]byte, error) {
	var response map[string]any
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, errors.New("decode soak checksum fault response failed")
	}
	changes, ok := response["changes"].([]any)
	if !ok || len(changes) != 1 {
		return nil, errors.New("soak checksum fault has no unique pull change")
	}
	change, ok := changes[0].(map[string]any)
	if !ok {
		return nil, errors.New("soak checksum fault change is invalid")
	}
	checksum, ok := change["row_checksum"].(map[string]any)
	if !ok {
		return nil, errors.New("soak checksum fault descriptor is invalid")
	}
	digest, ok := checksum["digest"].(string)
	if !ok || len(digest) != sha256.Size*2 {
		return nil, errors.New("soak checksum fault digest is invalid")
	}
	replacement := byte('0')
	if digest[0] == replacement {
		replacement = '1'
	}
	checksum["digest"] = string(replacement) + digest[1:]
	return json.Marshal(response)
}

func checksumDigest(raw any) ([32]byte, error) {
	descriptor, ok := raw.(map[string]any)
	if !ok || descriptor["algorithm"] != "sha256" || descriptor["encoding"] != "hex" {
		return [32]byte{}, errors.New("soak checksum descriptor is invalid")
	}
	version, ok := jsonInt64(descriptor["version"])
	if !ok || version != 1 {
		return [32]byte{}, errors.New("soak checksum version is invalid")
	}
	value, ok := descriptor["digest"].(string)
	if !ok {
		return [32]byte{}, errors.New("soak checksum digest is invalid")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return [32]byte{}, errors.New("soak checksum digest is invalid")
	}
	var result [32]byte
	copy(result[:], decoded)
	return result, nil
}

func (h *liveSoakHarness) scopeIDs() []string {
	result := make([]string, 0, len(h.protocol.Scopes))
	for scopeID := range h.protocol.Scopes {
		result = append(result, scopeID)
	}
	sort.Strings(result)
	return result
}

func (h *liveSoakHarness) nextUUID(label string) string {
	h.identitySequence++
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%d:%s", h.seed, h.identitySequence, label)))
	digest[6] = (digest[6] & 0x0f) | 0x40
	digest[8] = (digest[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func lowerSHA256(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func jsonInt64(value any) (int64, bool) {
	switch number := value.(type) {
	case float64:
		converted := int64(number)
		return converted, float64(converted) == number
	case json.Number:
		converted, err := number.Int64()
		return converted, err == nil
	case int64:
		return number, true
	case int:
		return int64(number), true
	default:
		return 0, false
	}
}

func mustMarshalJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func cloneJSONObject(source map[string]any) (map[string]any, error) {
	encoded, err := json.Marshal(source)
	if err != nil {
		return nil, errors.New("encode soak object clone failed")
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, errors.New("decode soak object clone failed")
	}
	return result, nil
}

func cloneAnyMap(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func equalStringMap(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func cloneVectorRow(source vectors.Row) vectors.Row {
	result := vectors.Row{PK: append(json.RawMessage(nil), source.PK...), Fields: make([]vectors.RowField, len(source.Fields))}
	for index, field := range source.Fields {
		result.Fields[index] = vectors.RowField{FieldID: field.FieldID, Value: append(json.RawMessage(nil), field.Value...)}
	}
	return result
}

func parseSoakSeed(raw string) (uint64, error) {
	if strings.TrimSpace(raw) == "" {
		return 1, nil
	}
	seed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, errors.New("SOAK_SEED must be an unsigned decimal integer")
	}
	return seed, nil
}

func parseSoakDuration(raw string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return time.Second, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 || duration > soak.MaximumSoakDuration {
		return 0, fmt.Errorf("SOAK_DURATION must be greater than zero and at most %s", soak.MaximumSoakDuration)
	}
	return duration, nil
}
