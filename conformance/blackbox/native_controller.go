package blackbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	nativeControllerRequestTimeout = 30 * time.Second
	nativeControllerWaitTimeout    = 30 * time.Second
	nativeControllerPollInterval   = 25 * time.Millisecond
)

// NativeControllerConfig configures one generic native server controller.
type NativeControllerConfig struct {
	Harness     *Harness
	HTTPClient  *http.Client
	Now         func() time.Time
	WaitTimeout time.Duration
}

// NativeController applies authored server operations to one real black-box harness.
type NativeController struct {
	harness     *Harness
	httpClient  *http.Client
	now         func() time.Time
	waitTimeout time.Duration

	mu             sync.Mutex
	closed         bool
	installation   *nativeInstallationBinding
	transactions   map[string]*nativeTransactionBinding
	records        map[string]*nativeRecordBinding
	rebuildCursors map[string]string
	scopeCursors   map[string]string
}

// NativeWireFacts records the transport facts from one native HTTP response.
type NativeWireFacts struct {
	HTTPStatus int     `json:"http_status"`
	ErrorCode  *string `json:"error_code,omitempty"`
	Retryable  bool    `json:"retryable"`
}

// NativeStepObservation records the raw terminal result from one native operation.
type NativeStepObservation struct {
	Disposition string           `json:"disposition"`
	ErrorCode   *string          `json:"error_code,omitempty"`
	Wire        *NativeWireFacts `json:"wire,omitempty"`
}

// NativeCaptureFacts binds one requested source to its durable state facts.
type NativeCaptureFacts struct {
	Source     string               `json:"source"`
	StateFacts scenarios.StateFacts `json:"state_facts"`
}

type nativeInstallationBinding struct {
	authoredStream             string
	authoredRegistryGeneration uint64
	runtimeRegistryGeneration  int64
	authoredSchemas            map[string]nativeSchemaReference
	runtimeSchemas             map[string]nativeSchemaReference
	tables                     map[string]nativeTableBinding
	relations                  map[string]string
	scopes                     map[string]string
	runtimeScopes              map[string]string
	rowScopes                  map[string][]string
	clients                    []nativeInstalledClient
	currentAuthoredSchema      nativeSchemaReference
	currentRuntimeSchema       nativeSchemaReference
}

type nativeInstalledClient struct {
	UserID   string
	ClientID string
}

type nativeSchemaReference struct {
	Version int64  `json:"version"`
	Hash    string `json:"hash"`
}

type nativeTableBinding struct {
	AuthoredID        string
	AuthoredName      string
	AuthoredRelation  string
	RuntimeID         string
	RuntimeName       string
	RuntimeRelationID string
	AuthoredPrimary   string
	RuntimePrimary    string
	Fields            map[string]string
	FieldNames        map[string]string
}

type nativeTransactionBinding struct {
	AuthoredStream       string
	AuthoredCommitLSN    string
	AuthoredEndLSN       string
	Events               []nativeEventBinding
	RuntimeStream        string
	RuntimeCommitLSN     string
	RuntimeEndLSN        string
	RuntimeRegistry      int64
	RuntimeEventOrdinals []uint64
	Materialized         bool
	ApplicationPush      bool
}

type nativeEventBinding struct {
	AuthoredOrdinal uint64
	Operation       string
	Relation        string
	Table           nativeTableBinding
	RecordID        string
	RuntimeRecordID string
	Before          *nativeAuthoredImage
	After           *nativeAuthoredImage
	AuthoredScopes  []string
}

type nativeRecordBinding struct {
	Table           nativeTableBinding
	RecordID        string
	RuntimeRecordID string
	Image           nativeAuthoredImage
	AuthoredScopes  []string
}

type nativeAuthoredImage struct {
	TableID           string
	PrimaryFieldID    string
	CanonicalWireJSON string
	Fields            map[string]json.RawMessage
	Version           string
	Checksum          string
	Deleted           bool
}

type nativeRuntimeManifest struct {
	SchemaVersion int64                     `json:"schema_version"`
	SchemaHash    string                    `json:"schema_hash"`
	Manifest      nativeRuntimeManifestBody `json:"manifest"`
}

type nativeRuntimeManifestBody struct {
	SchemaVersion int64                        `json:"schema_version"`
	SchemaHash    string                       `json:"schema_hash"`
	Tables        []nativeRuntimeManifestTable `json:"tables"`
}

type nativeRuntimeManifestTable struct {
	Name              string                       `json:"name"`
	ID                string                       `json:"table_id"`
	RelationID        string                       `json:"relation_id"`
	PrimaryKeyFieldID string                       `json:"primary_key_field_id"`
	Fields            []nativeRuntimeManifestField `json:"fields"`
}

type nativeRuntimeManifestField struct {
	ID       string `json:"field_id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Writable bool   `json:"writable"`
}

type nativeInstallPayload struct {
	Installation struct {
		Installed            bool     `json:"installed"`
		ProtocolVersion      int      `json:"protocol_version"`
		MinimumClientRuntime int      `json:"minimum_client_runtime"`
		Endpoints            []string `json:"endpoints"`
	} `json:"installation"`
	InitialSchema   nativePublishedSchema `json:"initial_schema"`
	InitialRegistry struct {
		RegistryGeneration uint64                   `json:"registry_generation"`
		Relations          []nativeAuthoredRelation `json:"relations"`
		ScopeRules         []nativeScopeRule        `json:"scope_rules"`
	} `json:"initial_registry"`
	Stream struct {
		StreamGeneration string `json:"stream_generation"`
	} `json:"stream"`
	EmptyScopes []struct {
		ScopeID string `json:"scope_id"`
	} `json:"empty_scopes"`
	Clients []struct {
		UserID           string   `json:"user_id"`
		ClientID         string   `json:"client_id"`
		AssignedScopeIDs []string `json:"assigned_scope_ids"`
	} `json:"clients"`
	ConfiguredLimits scenarios.ConfiguredLimitsFact `json:"configured_limits"`
}

type nativePublishedSchema struct {
	Schema             nativeSchemaReference `json:"schema"`
	Body               string                `json:"body"`
	TransitionClass    string                `json:"transition_class"`
	CompatibilityFloor int64                 `json:"compatibility_floor"`
	Tables             []nativeAuthoredTable `json:"tables"`
	AffectedScopes     []string              `json:"affected_scopes"`
}

type nativeAuthoredTable struct {
	TableID           string                `json:"table_id"`
	RelationID        string                `json:"relation_id"`
	Name              string                `json:"name"`
	PrimaryKeyFieldID string                `json:"primary_key_field_id"`
	Fields            []nativeAuthoredField `json:"fields"`
}

type nativeAuthoredField struct {
	FieldID    string `json:"field_id"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"primary_key"`
	Writable   bool   `json:"writable"`
}

type nativeAuthoredRelation struct {
	Relation               string   `json:"relation"`
	RegistrationKind       string   `json:"registration_kind"`
	TableID                string   `json:"table_id"`
	PrimaryKeyFieldID      string   `json:"primary_key_field_id"`
	PrimaryKeyPortableType string   `json:"primary_key_portable_type"`
	CapturedFieldIDs       []string `json:"captured_field_ids"`
}

type nativeScopeRule struct {
	Relation    string `json:"relation"`
	Evaluations []struct {
		Row struct {
			TableID           string `json:"table_id"`
			CanonicalWireJSON string `json:"canonical_wire_json"`
		} `json:"row"`
		Scopes []string `json:"scopes"`
	} `json:"evaluations"`
}

type nativeCommitPayload struct {
	StreamGeneration string                `json:"stream_generation"`
	CommitLSN        string                `json:"commit_lsn"`
	EndLSN           string                `json:"end_lsn"`
	Events           []nativeAuthoredEvent `json:"events"`
}

type nativeAuthoredEvent struct {
	EventOrdinal uint64                   `json:"event_ordinal"`
	Relation     string                   `json:"relation"`
	Operation    string                   `json:"operation"`
	Before       *nativeAuthoredImageWire `json:"before"`
	After        *nativeAuthoredImageWire `json:"after"`
}

type nativeAuthoredImageWire struct {
	Identity struct {
		Kind      string `json:"kind"`
		SyncedRow *struct {
			TableID           string `json:"table_id"`
			PrimaryKeyFieldID string `json:"primary_key_field_id"`
			PortableType      string `json:"portable_type"`
			CanonicalWireJSON string `json:"canonical_wire_json"`
		} `json:"synced_row"`
	} `json:"identity"`
	Fields []struct {
		Field    string          `json:"field"`
		Type     string          `json:"type"`
		WireJSON json.RawMessage `json:"wire_json"`
	} `json:"fields"`
	Version  string  `json:"version"`
	Checksum *string `json:"checksum"`
	Deleted  bool    `json:"deleted"`
}

// NewNativeController creates one controller for a provisioned harness.
func NewNativeController(config NativeControllerConfig) (*NativeController, error) {
	if config.Harness == nil || config.Harness.AdapterURL() == "" || config.Harness.Source() == nil || config.Harness.Operator() == nil {
		return nil, errors.New("native controller requires a ready black-box harness")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.WaitTimeout == 0 {
		config.WaitTimeout = nativeControllerWaitTimeout
	}
	if config.WaitTimeout <= 0 {
		return nil, errors.New("native controller wait timeout is invalid")
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: nativeControllerRequestTimeout}
	}
	return &NativeController{
		harness:        config.Harness,
		httpClient:     client,
		now:            config.Now,
		waitTimeout:    config.WaitTimeout,
		transactions:   make(map[string]*nativeTransactionBinding),
		records:        make(map[string]*nativeRecordBinding),
		rebuildCursors: make(map[string]string),
		scopeCursors:   make(map[string]string),
	}, nil
}

// NativeBearerToken signs a bounded token for an arbitrary authored user.
func (h *Harness) NativeBearerToken(ctx context.Context, userID string, now time.Time) (string, error) {
	if ctx == nil {
		return "", errors.New("native bearer token context is required")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if h == nil || !h.sourceReady || len(h.env.jwtSecret) == 0 {
		return "", errors.New("native bearer token harness is unavailable")
	}
	if !validNativeIdentity(userID) {
		return "", errors.New("native bearer token user identity is invalid")
	}
	issued := now.Round(0).UTC()
	token, err := SignHS256(h.env.jwtSecret, Claims{
		"sub": userID,
		"iat": issued.Unix(),
		"exp": issued.Add(time.Hour).Unix(),
	})
	if err != nil {
		return "", err
	}
	if h.adapter != nil {
		h.adapter.log.addRedaction([]byte(token))
	}
	if h.postgres != nil {
		h.postgres.log.addRedaction([]byte(token))
	}
	return token, nil
}

func validNativeIdentity(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if character <= ' ' || character == 0x7f {
			return false
		}
	}
	return true
}

// Install binds authored schema and WAL identities to the active runtime contract.
func (c *NativeController) Install(ctx context.Context, operation scenarios.Operation) error {
	if err := c.context(ctx); err != nil {
		return err
	}
	if scenarios.OperationKey(operation) != "model/install-current-contract" {
		return nativeUnsupported("install", operation)
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return fmt.Errorf("native controller install operation is invalid: %w", err)
	}
	var payload nativeInstallPayload
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil {
		return errors.New("decode native controller install payload failed")
	}
	if err := validateNativeInstallPayload(payload); err != nil {
		return err
	}
	runtime, runtimeRegistry, err := c.loadRuntimeManifest(ctx)
	if err != nil {
		return err
	}
	binding, err := bindNativeInstallation(payload, runtime, runtimeRegistry)
	if err != nil {
		return err
	}
	if nativeInstallRequiresPrivateScopeAssignments(payload) {
		if err := c.harness.Operator().UnregisterDefaultSharedScope(ctx); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.installation != nil {
		return errors.New("native controller contract is already installed")
	}
	c.installation = binding
	return nil
}

func nativeInstallRequiresPrivateScopeAssignments(payload nativeInstallPayload) bool {
	for _, client := range payload.Clients {
		if len(client.AssignedScopeIDs) != 0 {
			return true
		}
	}
	return false
}

func validateNativeInstallPayload(payload nativeInstallPayload) error {
	if !payload.Installation.Installed || payload.Installation.ProtocolVersion != 3 || payload.Installation.MinimumClientRuntime != 3 {
		return errors.New("native controller install protocol binding is invalid")
	}
	endpoints := append([]string(nil), payload.Installation.Endpoints...)
	sort.Strings(endpoints)
	if strings.Join(endpoints, ",") != "connect,pull,push,rebuild" {
		return errors.New("native controller install endpoint binding is invalid")
	}
	if !validNativeSchemaReference(payload.InitialSchema.Schema, false) || payload.InitialRegistry.RegistryGeneration == 0 || payload.Stream.StreamGeneration == "" {
		return errors.New("native controller install identity is incomplete")
	}
	if len(payload.InitialSchema.Tables) == 0 || len(payload.InitialRegistry.Relations) == 0 {
		return errors.New("native controller install contract has no synced relation")
	}
	var body struct {
		SchemaVersion int64  `json:"schema_version"`
		SchemaHash    string `json:"schema_hash"`
	}
	if err := jsonstrict.Decode([]byte(payload.InitialSchema.Body), &body); err != nil || body.SchemaVersion != payload.InitialSchema.Schema.Version || body.SchemaHash != payload.InitialSchema.Schema.Hash {
		return errors.New("native controller authored schema body is misbound")
	}
	limits := payload.ConfiguredLimits
	if limits.MaxScopeFanout == 0 || limits.MaxImpactRows == 0 || limits.PullMaximum == 0 || limits.RebuildMaximum == 0 || limits.CompactionBatchMaximum == 0 || limits.BackfillBatchMaximum == 0 {
		return errors.New("native controller configured limits are invalid")
	}
	return nil
}

func bindNativeInstallation(payload nativeInstallPayload, runtime nativeRuntimeManifest, runtimeRegistry int64) (*nativeInstallationBinding, error) {
	result := &nativeInstallationBinding{
		authoredStream:             payload.Stream.StreamGeneration,
		authoredRegistryGeneration: payload.InitialRegistry.RegistryGeneration,
		runtimeRegistryGeneration:  runtimeRegistry,
		authoredSchemas:            make(map[string]nativeSchemaReference),
		runtimeSchemas:             make(map[string]nativeSchemaReference),
		tables:                     make(map[string]nativeTableBinding),
		relations:                  make(map[string]string),
		scopes:                     make(map[string]string),
		runtimeScopes:              make(map[string]string),
		rowScopes:                  make(map[string][]string),
		currentAuthoredSchema:      payload.InitialSchema.Schema,
		currentRuntimeSchema:       nativeSchemaReference{Version: runtime.SchemaVersion, Hash: runtime.SchemaHash},
	}
	result.authoredSchemas[nativeSchemaKey(payload.InitialSchema.Schema)] = payload.InitialSchema.Schema
	result.runtimeSchemas[nativeSchemaKey(payload.InitialSchema.Schema)] = result.currentRuntimeSchema

	runtimeTables := append([]nativeRuntimeManifestTable(nil), runtime.Manifest.Tables...)
	usedRuntime := make(map[string]struct{})
	for _, authored := range payload.InitialSchema.Tables {
		runtimeTable, err := selectNativeRuntimeTable(authored, runtimeTables, usedRuntime)
		if err != nil {
			return nil, err
		}
		binding, err := bindNativeTable(authored, runtimeTable)
		if err != nil {
			return nil, err
		}
		if _, duplicate := result.tables[authored.TableID]; duplicate {
			return nil, errors.New("native controller authored table identity is duplicated")
		}
		result.tables[authored.TableID] = binding
		usedRuntime[runtimeTable.ID] = struct{}{}
	}
	for _, relation := range payload.InitialRegistry.Relations {
		if relation.RegistrationKind != "synced" {
			continue
		}
		table, found := result.tables[relation.TableID]
		if !found || relation.Relation == "" || relation.PrimaryKeyFieldID != table.AuthoredPrimary || relation.PrimaryKeyPortableType != "string" {
			return nil, errors.New("native controller authored registry relation is misbound")
		}
		if _, duplicate := result.relations[relation.Relation]; duplicate {
			return nil, errors.New("native controller authored relation identity is duplicated")
		}
		result.relations[relation.Relation] = relation.TableID
	}
	if len(result.relations) != len(result.tables) {
		return nil, errors.New("native controller table and registry bindings do not close")
	}
	for _, rule := range payload.InitialRegistry.ScopeRules {
		tableID, found := result.relations[rule.Relation]
		if !found {
			return nil, errors.New("native controller scope rule relation is not bound")
		}
		for _, evaluation := range rule.Evaluations {
			if evaluation.Row.TableID != tableID || evaluation.Row.CanonicalWireJSON == "" || len(evaluation.Scopes) == 0 {
				return nil, errors.New("native controller scope rule evaluation is invalid")
			}
			key := nativeRecordKey(tableID, evaluation.Row.CanonicalWireJSON)
			if _, duplicate := result.rowScopes[key]; duplicate {
				return nil, errors.New("native controller scope rule row is duplicated")
			}
			seenScopes := make(map[string]struct{}, len(evaluation.Scopes))
			for _, scope := range evaluation.Scopes {
				if scope == "" {
					return nil, errors.New("native controller scope rule identity is invalid")
				}
				if _, duplicate := seenScopes[scope]; duplicate {
					return nil, errors.New("native controller scope rule identity is duplicated")
				}
				seenScopes[scope] = struct{}{}
			}
			result.rowScopes[key] = append([]string(nil), evaluation.Scopes...)
		}
	}
	hasInitialAssignments := false
	for _, client := range payload.Clients {
		if !validNativeIdentity(client.UserID) || !validNativeIdentity(client.ClientID) {
			return nil, errors.New("native controller authored client identity is invalid")
		}
		result.clients = append(result.clients, nativeInstalledClient{UserID: client.UserID, ClientID: client.ClientID})
		for _, scope := range client.AssignedScopeIDs {
			hasInitialAssignments = true
			if err := bindNativeScope(result, scope, "user:"+client.UserID); err != nil {
				return nil, err
			}
		}
	}
	deferredMembershipScopes := make(map[string]struct{})
	if !hasInitialAssignments {
		for _, scopes := range result.rowScopes {
			for _, scope := range scopes {
				deferredMembershipScopes[scope] = struct{}{}
			}
		}
	}
	for _, scope := range payload.EmptyScopes {
		if scope.ScopeID == "" {
			return nil, errors.New("native controller authored scope identity is invalid")
		}
		if _, found := result.scopes[scope.ScopeID]; !found {
			if _, deferred := deferredMembershipScopes[scope.ScopeID]; deferred {
				continue
			}
			runtimeScope := "cf:global"
			if _, used := result.runtimeScopes[runtimeScope]; used {
				runtimeScope = "user:" + scope.ScopeID
			}
			if err := bindNativeScope(result, scope.ScopeID, runtimeScope); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func selectNativeRuntimeTable(authored nativeAuthoredTable, runtime []nativeRuntimeManifestTable, used map[string]struct{}) (nativeRuntimeManifestTable, error) {
	if authored.TableID == "" || authored.Name == "" || authored.RelationID == "" || authored.PrimaryKeyFieldID == "" {
		return nativeRuntimeManifestTable{}, errors.New("native controller authored table identity is incomplete")
	}
	candidates := make([]nativeRuntimeManifestTable, 0)
	for _, table := range runtime {
		if _, alreadyUsed := used[table.ID]; alreadyUsed || !nativeRuntimeTableSupports(table, authored) {
			continue
		}
		candidates = append(candidates, table)
	}
	if len(candidates) == 0 {
		return nativeRuntimeManifestTable{}, fmt.Errorf("native controller has no runtime table for authored table %q", authored.TableID)
	}
	sort.Slice(candidates, func(left, right int) bool { return candidates[left].Name < candidates[right].Name })
	for _, candidate := range candidates {
		if candidate.Name == authored.Name || candidate.Name == "cf_"+authored.Name {
			return candidate, nil
		}
	}
	for _, preferred := range []string{"cf_items", "cf_global_items", "cf_documents", "cf_document_notes", "cf_schema_queue", "cf_late_registration"} {
		for _, candidate := range candidates {
			if candidate.Name == preferred {
				return candidate, nil
			}
		}
	}
	return nativeRuntimeManifestTable{}, fmt.Errorf("native controller runtime table binding for %q is ambiguous", authored.TableID)
}

func nativeRuntimeTableSupports(runtime nativeRuntimeManifestTable, authored nativeAuthoredTable) bool {
	if runtime.ID == "" || runtime.Name == "" || runtime.RelationID == "" || runtime.PrimaryKeyFieldID == "" {
		return false
	}
	if runtime.Name == "cf_schema_queue" {
		return nativeSchemaQueueTableSupports(runtime, authored)
	}
	fields := make(map[string]nativeRuntimeManifestField, len(runtime.Fields))
	for _, field := range runtime.Fields {
		fields[field.Name] = field
	}
	for _, field := range authored.Fields {
		runtimeField, found := fields[field.Name]
		if !found || runtimeField.ID == "" || runtimeField.Type != field.Type {
			return false
		}
		if field.PrimaryKey && runtimeField.ID != runtime.PrimaryKeyFieldID {
			return false
		}
	}
	return true
}

func nativeSchemaQueueTableSupports(runtime nativeRuntimeManifestTable, authored nativeAuthoredTable) bool {
	if len(authored.Fields) != 3 {
		return false
	}
	runtimeFields := make(map[string]nativeRuntimeManifestField, len(runtime.Fields))
	for _, field := range runtime.Fields {
		runtimeFields[field.Name] = field
	}
	for _, field := range authored.Fields {
		runtimeName := nativeSchemaQueueFieldName(field.Name)
		runtimeField, found := runtimeFields[runtimeName]
		if !found || runtimeField.ID == "" {
			return false
		}
		if field.Name == "value" {
			if field.Type != "string" || runtimeField.Type != "json" {
				return false
			}
		} else if runtimeField.Type != field.Type {
			return false
		}
		if field.PrimaryKey && runtimeField.ID != runtime.PrimaryKeyFieldID {
			return false
		}
	}
	return true
}

func nativeSchemaQueueFieldName(authored string) string {
	switch authored {
	case "value":
		return "authored_mutation"
	case "obsolete_value":
		return "legacy_value"
	default:
		return authored
	}
}

func bindNativeTable(authored nativeAuthoredTable, runtime nativeRuntimeManifestTable) (nativeTableBinding, error) {
	fieldsByName := make(map[string]nativeRuntimeManifestField, len(runtime.Fields))
	for _, field := range runtime.Fields {
		if field.ID == "" || field.Name == "" {
			return nativeTableBinding{}, errors.New("native controller runtime field identity is incomplete")
		}
		fieldsByName[field.Name] = field
	}
	binding := nativeTableBinding{
		AuthoredID:        authored.TableID,
		AuthoredName:      authored.Name,
		AuthoredRelation:  authored.RelationID,
		RuntimeID:         runtime.ID,
		RuntimeName:       runtime.Name,
		RuntimeRelationID: runtime.RelationID,
		AuthoredPrimary:   authored.PrimaryKeyFieldID,
		RuntimePrimary:    runtime.PrimaryKeyFieldID,
		Fields:            make(map[string]string, len(authored.Fields)),
		FieldNames:        make(map[string]string, len(authored.Fields)),
	}
	for _, field := range authored.Fields {
		runtimeName := field.Name
		if runtime.Name == "cf_schema_queue" {
			runtimeName = nativeSchemaQueueFieldName(field.Name)
		}
		runtimeField, found := fieldsByName[runtimeName]
		if !found {
			return nativeTableBinding{}, errors.New("native controller runtime field binding is absent")
		}
		binding.Fields[field.FieldID] = runtimeField.ID
		binding.FieldNames[field.FieldID] = runtimeField.Name
	}
	if binding.Fields[authored.PrimaryKeyFieldID] != runtime.PrimaryKeyFieldID {
		return nativeTableBinding{}, errors.New("native controller runtime primary-key binding is invalid")
	}
	return binding, nil
}

func bindNativeScope(binding *nativeInstallationBinding, authored, runtime string) error {
	if authored == "" || runtime == "" {
		return errors.New("native controller scope binding is incomplete")
	}
	if existing, found := binding.scopes[authored]; found && existing != runtime {
		delete(binding.runtimeScopes, existing)
		if existing != "cf:global" {
			runtime = "cf:global"
		}
	}
	if existing, found := binding.runtimeScopes[runtime]; found && existing != authored {
		return fmt.Errorf("native controller runtime scope %q is bound more than once", runtime)
	}
	binding.scopes[authored] = runtime
	binding.runtimeScopes[runtime] = authored
	return nil
}

func (c *NativeController) loadRuntimeManifest(ctx context.Context) (nativeRuntimeManifest, int64, error) {
	response, err := (&Client{BaseURL: c.harness.AdapterURL(), HTTP: c.httpClient}).Do(ctx, Request{
		Method: http.MethodGet,
		Path:   "/sync/schema",
		Class:  "native/runtime-schema",
	})
	if err != nil {
		return nativeRuntimeManifest{}, 0, fmt.Errorf("load native runtime schema: %w", err)
	}
	if response.Status != http.StatusOK {
		return nativeRuntimeManifest{}, 0, errors.New("native runtime schema request did not succeed")
	}
	var manifest nativeRuntimeManifest
	if err := jsonstrict.Decode(response.Body, &manifest); err != nil {
		return nativeRuntimeManifest{}, 0, errors.New("decode native runtime schema failed")
	}
	if !validNativeSchemaReference(nativeSchemaReference{Version: manifest.SchemaVersion, Hash: manifest.SchemaHash}, false) || manifest.Manifest.SchemaVersion != manifest.SchemaVersion || manifest.Manifest.SchemaHash != manifest.SchemaHash || len(manifest.Manifest.Tables) == 0 {
		return nativeRuntimeManifest{}, 0, errors.New("native runtime schema identity is invalid")
	}
	database, err := c.harness.openDatabase(ctx, c.harness.names.Database, c.harness.env.Admin, false)
	if err != nil {
		return nativeRuntimeManifest{}, 0, errors.New("open native runtime registry observation failed")
	}
	defer database.Close()
	var generation int64
	if err := database.QueryRowContext(ctx, "SELECT generation FROM synchro.sync_registry_generations WHERE state = 'active' AND validated").Scan(&generation); err != nil || generation <= 0 {
		return nativeRuntimeManifest{}, 0, errors.New("native runtime registry identity is invalid")
	}
	return manifest, generation, nil
}

// ApplyStep applies one non-workload controller operation.
func (c *NativeController) ApplyStep(ctx context.Context, operation scenarios.Operation) (NativeStepObservation, error) {
	if err := c.context(ctx); err != nil {
		return NativeStepObservation{}, err
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return NativeStepObservation{}, fmt.Errorf("native controller apply operation is invalid: %w", err)
	}
	switch scenarios.OperationKey(operation) {
	case "model/commit-source-transaction":
		return c.commitSourceTransaction(ctx, operation)
	case "model/set-client-assignments":
		observation, usesDefaultSharedScope, err := c.setClientAssignments(operation)
		if err != nil {
			return NativeStepObservation{}, err
		}
		if !usesDefaultSharedScope {
			if err := c.harness.Operator().UnregisterDefaultSharedScope(ctx); err != nil {
				return NativeStepObservation{}, err
			}
		}
		return observation, nil
	case "model/publish-schema":
		return c.publishSchema(ctx, operation)
	case "model/stage-registry-membership-generation":
		if err := c.harness.Operator().ConfigureCrossScopeTable(ctx); err != nil {
			return NativeStepObservation{}, err
		}
		return nativeSuccess(), nil
	case "model/activate-registry-membership-generation":
		if err := c.harness.Operator().ReloadRegistry(ctx); err != nil {
			return NativeStepObservation{}, err
		}
		return nativeSuccess(), nil
	case "model/expire-client-generation":
		return c.expireClientGeneration(ctx, operation)
	case "model/compact-scope":
		if _, err := c.harness.Operator().RunDiagnosticRetentionCompaction(ctx); err != nil {
			return NativeStepObservation{}, err
		}
		return nativeSuccess(), nil
	case "workload/prepare":
		return NativeStepObservation{}, errors.New("native controller does not execute workload macros; the manifest must supply concrete expansions")
	default:
		return NativeStepObservation{}, nativeUnsupported("apply", operation)
	}
}

// ApplicationWrite maps one authored local write to the installed application schema.
func (c *NativeController) ApplicationWrite(operation scenarios.Operation) (scenarios.Operation, error) {
	if c == nil {
		return scenarios.Operation{}, errors.New("native controller is unavailable")
	}
	if scenarios.OperationKey(operation) != "local/write" {
		return scenarios.Operation{}, fmt.Errorf("native application operation %q is unsupported", scenarios.OperationKey(operation))
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return scenarios.Operation{}, fmt.Errorf("native application operation is invalid: %w", err)
	}
	var payload map[string]any
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil {
		return scenarios.Operation{}, errors.New("native application write payload is invalid")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return scenarios.Operation{}, errors.New("native controller is closed")
	}
	if c.installation == nil {
		return scenarios.Operation{}, errors.New("native controller contract is not installed")
	}
	authoredTable, _ := payload["table_id"].(string)
	table, found := c.installation.tables[authoredTable]
	if !found || table.RuntimeName == "" {
		return scenarios.Operation{}, errors.New("native application write table has no runtime binding")
	}
	primaryKey, ok := payload["pk"].(map[string]any)
	if !ok || len(primaryKey) != 1 {
		return scenarios.Operation{}, errors.New("native application write primary key is invalid")
	}
	primaryValue, found := primaryKey[table.AuthoredPrimary]
	primaryName := table.FieldNames[table.AuthoredPrimary]
	if !found || primaryName == "" {
		return scenarios.Operation{}, errors.New("native application write primary key has no runtime binding")
	}
	canonicalPrimary, err := json.Marshal(primaryValue)
	if err != nil {
		return scenarios.Operation{}, errors.New("native application write primary key is invalid")
	}
	payload["table_id"] = table.RuntimeName
	payload["pk"] = map[string]any{primaryName: nativeRuntimeUUID(table.AuthoredID, string(canonicalPrimary))}
	if columns, found := payload["columns"]; found {
		payload["columns"], err = nativeApplicationWriteColumns(columns, table)
		if err != nil {
			return scenarios.Operation{}, err
		}
		if payload["operation"] == "insert" && (table.RuntimeName == "cf_items" || table.RuntimeName == "cf_schema_queue") {
			userID, _ := payload["authenticated_user_id"].(string)
			clientVersion, _ := payload["client_version"].(string)
			support := map[string]any{"owner_id": userID, "updated_at": clientVersion}
			if table.RuntimeName == "cf_schema_queue" {
				for authoredField, applicationField := range table.FieldNames {
					if authoredField != table.AuthoredPrimary && applicationField != "authored_mutation" {
						support[applicationField] = ""
					}
				}
			}
			payload["columns"], err = nativeApplicationInsertSupportColumns(payload["columns"], support)
			if err != nil {
				return scenarios.Operation{}, err
			}
		}
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return scenarios.Operation{}, errors.New("encode native application write failed")
	}
	result := operation
	result.Payload = encoded
	if err := scenarios.ValidateOperation(result); err != nil {
		return scenarios.Operation{}, fmt.Errorf("native application write is invalid: %w", err)
	}
	return result, nil
}

// BindApplicationPush binds one accepted application push to its authored WAL identity.
func (c *NativeController) BindApplicationPush(operation scenarios.Operation) error {
	if c == nil {
		return errors.New("native controller is unavailable")
	}
	if scenarios.OperationKey(operation) != "push/submit" {
		return fmt.Errorf("native application push operation %q is unsupported", scenarios.OperationKey(operation))
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return fmt.Errorf("native application push operation is invalid: %w", err)
	}
	var payload struct {
		AuthenticatedUserID string `json:"authenticated_user_id"`
		Request             struct {
			ClientID         string                `json:"client_id"`
			ClientGeneration int64                 `json:"client_generation"`
			BatchID          string                `json:"batch_id"`
			Schema           nativeSchemaReference `json:"schema"`
			Mutations        []struct {
				MutationID     string                     `json:"mutation_id"`
				Table          string                     `json:"table"`
				PK             map[string]json.RawMessage `json:"pk"`
				AuthoredSchema nativeSchemaReference      `json:"authored_schema"`
				Op             string                     `json:"op"`
				ClientVersion  string                     `json:"client_version"`
				Columns        map[string]json.RawMessage `json:"columns"`
			} `json:"mutations"`
		} `json:"request"`
		Delivery  string `json:"delivery"`
		CommitLSN string `json:"commit_lsn"`
		EndLSN    string `json:"end_lsn"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil {
		return errors.New("decode native application push failed")
	}
	if !validNativeIdentity(payload.AuthenticatedUserID) || payload.Delivery != "apply" || payload.CommitLSN == "" || payload.EndLSN == "" || compareNativeLSN(payload.CommitLSN, payload.EndLSN) >= 0 || len(payload.Request.Mutations) == 0 {
		return errors.New("native application push transaction identity is invalid")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.installation == nil {
		return errors.New("native controller contract is unavailable")
	}
	transaction := &nativeTransactionBinding{
		AuthoredStream:    c.installation.authoredStream,
		AuthoredCommitLSN: payload.CommitLSN,
		AuthoredEndLSN:    payload.EndLSN,
		ApplicationPush:   true,
	}
	seenRecords := make(map[string]struct{}, len(payload.Request.Mutations))
	for ordinal, mutation := range payload.Request.Mutations {
		table, found := c.installation.tables[mutation.Table]
		if !found || mutation.Op != "insert" || len(mutation.PK) != 1 {
			return errors.New("native application push mutation is unsupported")
		}
		canonical, found := mutation.PK[table.AuthoredPrimary]
		if !found || !json.Valid(canonical) {
			return errors.New("native application push primary key is invalid")
		}
		recordID, err := nativeAuthoredRecordID(string(canonical))
		if err != nil {
			return err
		}
		runtimeRecordID := nativeRuntimeUUID(table.AuthoredID, string(canonical))
		recordKey := nativeRecordKey(table.AuthoredID, string(canonical))
		if _, duplicate := seenRecords[recordKey]; duplicate {
			return errors.New("native application push targets one row more than once")
		}
		seenRecords[recordKey] = struct{}{}
		fields := make(map[string]json.RawMessage, len(mutation.Columns)+1)
		fields[table.AuthoredPrimary] = append(json.RawMessage(nil), canonical...)
		for authoredField, value := range mutation.Columns {
			if authoredField == table.AuthoredPrimary || table.Fields[authoredField] == "" || !json.Valid(value) {
				return errors.New("native application push column has no runtime binding")
			}
			fields[authoredField] = append(json.RawMessage(nil), value...)
		}
		transaction.Events = append(transaction.Events, nativeEventBinding{
			AuthoredOrdinal: uint64(ordinal),
			Operation:       mutation.Op,
			Relation:        table.AuthoredRelation,
			Table:           table,
			RecordID:        recordID,
			RuntimeRecordID: runtimeRecordID,
			After: &nativeAuthoredImage{
				TableID:           table.AuthoredID,
				PrimaryFieldID:    table.AuthoredPrimary,
				CanonicalWireJSON: string(canonical),
				Fields:            fields,
			},
			AuthoredScopes: nativeScopesForRecord(c.installation, table.AuthoredRelation, table.AuthoredID, string(canonical)),
		})
	}
	key := nativeTransactionKey(transaction.AuthoredStream, transaction.AuthoredCommitLSN)
	if _, duplicate := c.transactions[key]; duplicate {
		return errors.New("native application push transaction identity is duplicated")
	}
	c.transactions[key] = transaction
	return nil
}

func nativeApplicationWriteColumns(value any, table nativeTableBinding) (any, error) {
	switch columns := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(columns))
		for authoredField, fieldValue := range columns {
			applicationField := table.FieldNames[authoredField]
			if applicationField == "" || authoredField == table.AuthoredPrimary {
				return nil, errors.New("native application write column has no writable runtime binding")
			}
			runtimeValue, err := nativeRuntimeFieldValue(table, authoredField, fieldValue)
			if err != nil {
				return nil, err
			}
			result[applicationField] = runtimeValue
		}
		return result, nil
	case []any:
		result := make([]any, 0, len(columns))
		for _, value := range columns {
			column, ok := value.(map[string]any)
			if !ok || len(column) != 2 {
				return nil, errors.New("native application write column is invalid")
			}
			authoredField, hasField := column["field_id"].(string)
			fieldValue, hasValue := column["value"]
			applicationField := table.FieldNames[authoredField]
			if !hasField || !hasValue || applicationField == "" || authoredField == table.AuthoredPrimary {
				return nil, errors.New("native application write column has no writable runtime binding")
			}
			runtimeValue, err := nativeRuntimeFieldValue(table, authoredField, fieldValue)
			if err != nil {
				return nil, err
			}
			result = append(result, map[string]any{"field_id": applicationField, "value": runtimeValue})
		}
		return result, nil
	default:
		return nil, errors.New("native application write columns are invalid")
	}
}

func nativeRuntimeFieldValue(table nativeTableBinding, authoredField string, value any) (any, error) {
	if table.RuntimeName != "cf_schema_queue" || authoredField != "value" {
		return value, nil
	}
	text, ok := value.(string)
	if !ok {
		return nil, errors.New("native schema-queue value is invalid")
	}
	encoded, err := json.Marshal(text)
	if err != nil {
		return nil, errors.New("encode native schema-queue value failed")
	}
	return string(encoded), nil
}

func nativeApplicationInsertSupportColumns(value any, support map[string]any) (any, error) {
	userID, _ := support["owner_id"].(string)
	clientVersion, _ := support["updated_at"].(string)
	if !validNativeIdentity(userID) || clientVersion == "" || len(support) < 2 {
		return nil, errors.New("native application insert support fields are invalid")
	}
	names := make([]string, 0, len(support))
	for name := range support {
		if name == "" {
			return nil, errors.New("native application insert support field is invalid")
		}
		names = append(names, name)
	}
	sort.Strings(names)
	switch columns := value.(type) {
	case map[string]any:
		for _, name := range names {
			if _, exists := columns[name]; exists {
				continue
			}
			columns[name] = support[name]
		}
		return columns, nil
	case []any:
		existing := make(map[string]struct{}, len(columns))
		for _, value := range columns {
			column, ok := value.(map[string]any)
			if !ok {
				return nil, errors.New("native application insert column is invalid")
			}
			fieldID, ok := column["field_id"].(string)
			if !ok || fieldID == "" {
				return nil, errors.New("native application insert column is invalid")
			}
			existing[fieldID] = struct{}{}
		}
		for _, name := range names {
			if _, exists := existing[name]; exists {
				continue
			}
			columns = append(columns, map[string]any{"field_id": name, "value": support[name]})
		}
		return columns, nil
	default:
		return nil, errors.New("native application insert columns are invalid")
	}
}

func (c *NativeController) setClientAssignments(operation scenarios.Operation) (NativeStepObservation, bool, error) {
	var payload struct {
		UserID      string `json:"user_id"`
		ClientID    string `json:"client_id"`
		Assignments []struct {
			ScopeID string `json:"scope_id"`
		} `json:"assignments"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || !validNativeIdentity(payload.UserID) || !validNativeIdentity(payload.ClientID) {
		return NativeStepObservation{}, false, errors.New("native controller client assignment payload is invalid")
	}
	if len(payload.Assignments) == 0 {
		return NativeStepObservation{}, false, errors.New("native controller client assignment is empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.installation == nil {
		return NativeStepObservation{}, false, errors.New("native controller contract is not installed")
	}
	usesDefaultSharedScope := false
	for _, assignment := range payload.Assignments {
		if existing, found := c.installation.scopes[assignment.ScopeID]; found {
			switch existing {
			case "cf:global":
				usesDefaultSharedScope = true
				continue
			case "user:" + payload.UserID:
				continue
			default:
				return NativeStepObservation{}, false, errors.New("native controller client assignment conflicts with its runtime scope")
			}
		}
		if err := bindNativeScope(c.installation, assignment.ScopeID, "user:"+payload.UserID); err != nil {
			return NativeStepObservation{}, false, err
		}
	}
	installed := false
	for _, client := range c.installation.clients {
		if client.UserID == payload.UserID && client.ClientID == payload.ClientID {
			installed = true
			break
		}
	}
	if !installed {
		c.installation.clients = append(c.installation.clients, nativeInstalledClient{UserID: payload.UserID, ClientID: payload.ClientID})
	}
	return nativeSuccess(), usesDefaultSharedScope, nil
}

func (c *NativeController) publishSchema(ctx context.Context, operation scenarios.Operation) (NativeStepObservation, error) {
	var payload nativePublishedSchema
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || !validNativeSchemaReference(payload.Schema, false) {
		return NativeStepObservation{}, errors.New("native controller publish-schema payload is invalid")
	}
	c.mu.Lock()
	installation := c.installation
	queueTransition := false
	if installation != nil && len(installation.tables) == 1 {
		for _, table := range installation.tables {
			queueTransition = table.RuntimeName == "cf_schema_queue"
		}
	}
	c.mu.Unlock()
	var transitionErr error
	if queueTransition {
		transitionErr = c.transitionNativeSchemaQueue(ctx, payload)
	} else {
		transitionErr = c.harness.Operator().TransitionSchemaQueue(ctx)
	}
	if transitionErr != nil {
		return NativeStepObservation{}, fmt.Errorf("apply native runtime schema transition: %w", transitionErr)
	}
	runtime, runtimeRegistry, err := c.waitForRuntimeSchemaChange(ctx)
	if err != nil {
		return NativeStepObservation{}, err
	}
	var queueBinding nativeTableBinding
	if queueTransition {
		if len(payload.Tables) != 1 {
			return NativeStepObservation{}, errors.New("native schema-queue authored table is invalid")
		}
		var runtimeTable *nativeRuntimeManifestTable
		for index := range runtime.Manifest.Tables {
			if runtime.Manifest.Tables[index].Name == "cf_schema_queue" {
				runtimeTable = &runtime.Manifest.Tables[index]
				break
			}
		}
		if runtimeTable == nil {
			return NativeStepObservation{}, errors.New("native schema-queue runtime table is absent")
		}
		queueBinding, err = bindNativeTable(payload.Tables[0], *runtimeTable)
		if err != nil {
			return NativeStepObservation{}, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.installation == nil {
		return NativeStepObservation{}, errors.New("native controller contract is not installed")
	}
	authoredKey := nativeSchemaKey(payload.Schema)
	if _, duplicate := c.installation.runtimeSchemas[authoredKey]; duplicate {
		return NativeStepObservation{}, errors.New("native controller authored schema is already published")
	}
	runtimeRef := nativeSchemaReference{Version: runtime.SchemaVersion, Hash: runtime.SchemaHash}
	c.installation.authoredSchemas[authoredKey] = payload.Schema
	c.installation.runtimeSchemas[authoredKey] = runtimeRef
	c.installation.currentAuthoredSchema = payload.Schema
	c.installation.currentRuntimeSchema = runtimeRef
	c.installation.runtimeRegistryGeneration = runtimeRegistry
	if queueTransition {
		c.installation.tables[payload.Tables[0].TableID] = queueBinding
	}
	return nativeSuccess(), nil
}

func (c *NativeController) transitionNativeSchemaQueue(ctx context.Context, payload nativePublishedSchema) error {
	if len(payload.Tables) != 1 {
		return errors.New("native schema-queue transition table is invalid")
	}
	c.mu.Lock()
	var current nativeTableBinding
	for _, table := range c.installation.tables {
		current = table
	}
	c.mu.Unlock()
	if current.RuntimeName != "cf_schema_queue" {
		return errors.New("native schema-queue transition binding is absent")
	}
	nextFields := make(map[string]nativeAuthoredField, len(payload.Tables[0].Fields))
	for _, field := range payload.Tables[0].Fields {
		nextFields[field.FieldID] = field
	}
	var removedPhysical, addedPhysical string
	for authoredField, physicalField := range current.FieldNames {
		if _, retained := nextFields[authoredField]; !retained {
			if removedPhysical != "" {
				return errors.New("native schema-queue transition removes more than one field")
			}
			removedPhysical = physicalField
		}
	}
	for _, field := range payload.Tables[0].Fields {
		if _, retained := current.Fields[field.FieldID]; retained {
			continue
		}
		if addedPhysical != "" {
			return errors.New("native schema-queue transition adds more than one field")
		}
		addedPhysical = nativeSchemaQueueFieldName(field.Name)
	}
	if !validSchemaTransitionColumn(removedPhysical) || !validSchemaTransitionColumn(addedPhysical) || removedPhysical == addedPhysical {
		return errors.New("native schema-queue transition fields are invalid")
	}
	return c.harness.Operator().TransitionSchemaQueueField(ctx, removedPhysical, addedPhysical)
}

func (c *NativeController) waitForRuntimeSchemaChange(ctx context.Context) (nativeRuntimeManifest, int64, error) {
	c.mu.Lock()
	if c.installation == nil {
		c.mu.Unlock()
		return nativeRuntimeManifest{}, 0, errors.New("native controller contract is not installed")
	}
	prior := c.installation.currentRuntimeSchema
	c.mu.Unlock()
	deadline, cancel := context.WithTimeout(ctx, c.waitTimeout)
	defer cancel()
	for {
		manifest, generation, err := c.loadRuntimeManifest(deadline)
		if err == nil && (manifest.SchemaVersion != prior.Version || manifest.SchemaHash != prior.Hash) {
			return manifest, generation, nil
		}
		if err := waitNativePoll(deadline); err != nil {
			return nativeRuntimeManifest{}, 0, errors.New("native runtime schema transition did not publish")
		}
	}
}

func (c *NativeController) expireClientGeneration(ctx context.Context, operation scenarios.Operation) (NativeStepObservation, error) {
	var payload struct {
		UserID   string `json:"user_id"`
		ClientID string `json:"client_id"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || !validNativeIdentity(payload.UserID) || !validNativeIdentity(payload.ClientID) {
		return NativeStepObservation{}, errors.New("native controller expire-client payload is invalid")
	}
	if payload.UserID != "diagnostic-user" || payload.ClientID != diagnosticRetentionClientID {
		return NativeStepObservation{}, fmt.Errorf("native controller cannot expire %s/%s: the current Harness operator exposes only %s/%s", payload.UserID, payload.ClientID, "diagnostic-user", diagnosticRetentionClientID)
	}
	if err := c.harness.Operator().AgeDiagnosticRetentionClient(ctx); err != nil {
		return NativeStepObservation{}, err
	}
	return nativeSuccess(), nil
}

func (c *NativeController) commitSourceTransaction(ctx context.Context, operation scenarios.Operation) (NativeStepObservation, error) {
	var payload nativeCommitPayload
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil {
		return NativeStepObservation{}, errors.New("decode native source transaction failed")
	}
	c.mu.Lock()
	installation := c.installation
	c.mu.Unlock()
	if installation == nil {
		return NativeStepObservation{}, errors.New("native controller contract is not installed")
	}
	transaction, err := bindNativeTransaction(payload, installation)
	if err != nil {
		return NativeStepObservation{}, err
	}
	key := nativeTransactionKey(payload.StreamGeneration, payload.CommitLSN)
	c.mu.Lock()
	if _, duplicate := c.transactions[key]; duplicate {
		c.mu.Unlock()
		return NativeStepObservation{}, errors.New("native controller source transaction identity is duplicated")
	}
	c.mu.Unlock()

	sourceTransaction, err := c.harness.Source().BeginTx(ctx)
	if err != nil {
		return NativeStepObservation{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = sourceTransaction.Rollback()
		}
	}()
	for _, event := range transaction.Events {
		statement, arguments, err := nativeSourceStatement(event, installation)
		if err != nil {
			return NativeStepObservation{}, err
		}
		result, err := sourceTransaction.ExecContext(ctx, statement, arguments...)
		if err != nil {
			return NativeStepObservation{}, fmt.Errorf("execute native source event: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return NativeStepObservation{}, errors.New("native source event did not affect exactly one authoritative row")
		}
	}
	if err := sourceTransaction.Commit(); err != nil {
		return NativeStepObservation{}, err
	}
	committed = true

	c.mu.Lock()
	c.transactions[key] = transaction
	for _, event := range transaction.Events {
		recordKey := nativeRecordKey(event.Table.AuthoredID, event.RecordID)
		if event.After == nil || event.After.Deleted {
			delete(c.records, recordKey)
			continue
		}
		c.records[recordKey] = &nativeRecordBinding{
			Table:           event.Table,
			RecordID:        event.RecordID,
			RuntimeRecordID: event.RuntimeRecordID,
			Image:           *event.After,
			AuthoredScopes:  append([]string(nil), event.AuthoredScopes...),
		}
	}
	c.mu.Unlock()
	return nativeSuccess(), nil
}

func bindNativeTransaction(payload nativeCommitPayload, installation *nativeInstallationBinding) (*nativeTransactionBinding, error) {
	if installation == nil || payload.StreamGeneration != installation.authoredStream || payload.CommitLSN == "" || payload.EndLSN == "" || len(payload.Events) == 0 {
		return nil, errors.New("native source transaction identity is invalid")
	}
	if compareNativeLSN(payload.CommitLSN, payload.EndLSN) >= 0 {
		return nil, errors.New("native source transaction LSN range is invalid")
	}
	result := &nativeTransactionBinding{AuthoredStream: payload.StreamGeneration, AuthoredCommitLSN: payload.CommitLSN, AuthoredEndLSN: payload.EndLSN}
	seenOrdinals := make(map[uint64]struct{}, len(payload.Events))
	seenRecords := make(map[string]struct{}, len(payload.Events))
	for _, event := range payload.Events {
		if _, duplicate := seenOrdinals[event.EventOrdinal]; duplicate {
			return nil, errors.New("native source event ordinal is duplicated")
		}
		seenOrdinals[event.EventOrdinal] = struct{}{}
		tableID, found := installation.relations[event.Relation]
		if !found {
			return nil, fmt.Errorf("native source relation %q is not bound", event.Relation)
		}
		table := installation.tables[tableID]
		before, err := decodeNativeAuthoredImage(event.Before, table)
		if err != nil {
			return nil, err
		}
		after, err := decodeNativeAuthoredImage(event.After, table)
		if err != nil {
			return nil, err
		}
		if event.Operation == "insert" && (before != nil || after == nil) || event.Operation == "update" && (before == nil || after == nil) || event.Operation == "delete" && (before == nil || after != nil) {
			return nil, errors.New("native source event images do not match the operation")
		}
		if event.Operation != "insert" && event.Operation != "update" && event.Operation != "delete" {
			return nil, errors.New("native source event operation is unsupported")
		}
		image := after
		if image == nil {
			image = before
		}
		recordID, err := nativeAuthoredRecordID(image.CanonicalWireJSON)
		if err != nil {
			return nil, err
		}
		runtimeRecordID := nativeRuntimeUUID(table.AuthoredID, image.CanonicalWireJSON)
		if _, duplicate := seenRecords[table.AuthoredID+"\x00"+runtimeRecordID]; duplicate {
			return nil, errors.New("native source transaction targets one row more than once")
		}
		seenRecords[table.AuthoredID+"\x00"+runtimeRecordID] = struct{}{}
		scopes := nativeScopesForRecord(installation, table.AuthoredRelation, table.AuthoredID, image.CanonicalWireJSON)
		result.Events = append(result.Events, nativeEventBinding{
			AuthoredOrdinal: event.EventOrdinal,
			Operation:       event.Operation,
			Relation:        event.Relation,
			Table:           table,
			RecordID:        recordID,
			RuntimeRecordID: runtimeRecordID,
			Before:          before,
			After:           after,
			AuthoredScopes:  scopes,
		})
	}
	sort.Slice(result.Events, func(left, right int) bool {
		return result.Events[left].AuthoredOrdinal < result.Events[right].AuthoredOrdinal
	})
	return result, nil
}

func decodeNativeAuthoredImage(wire *nativeAuthoredImageWire, table nativeTableBinding) (*nativeAuthoredImage, error) {
	if wire == nil {
		return nil, nil
	}
	row := wire.Identity.SyncedRow
	if wire.Identity.Kind != "synced" || row == nil || row.TableID != table.AuthoredID || row.PrimaryKeyFieldID != table.AuthoredPrimary || row.PortableType != "string" || row.CanonicalWireJSON == "" || wire.Version == "" || wire.Checksum == nil || len(*wire.Checksum) != 64 {
		return nil, errors.New("native source image identity is invalid")
	}
	if _, err := hex.DecodeString(*wire.Checksum); err != nil {
		return nil, errors.New("native source image checksum is invalid")
	}
	fields := make(map[string]json.RawMessage, len(wire.Fields))
	for _, field := range wire.Fields {
		if _, known := table.Fields[field.Field]; !known || field.Type == "" || len(field.WireJSON) == 0 || !json.Valid(field.WireJSON) {
			return nil, errors.New("native source image field is invalid")
		}
		if _, duplicate := fields[field.Field]; duplicate {
			return nil, errors.New("native source image field is duplicated")
		}
		fields[field.Field] = append(json.RawMessage(nil), field.WireJSON...)
	}
	if len(fields) != len(table.Fields) {
		return nil, errors.New("native source image field set is incomplete")
	}
	return &nativeAuthoredImage{
		TableID:           row.TableID,
		PrimaryFieldID:    row.PrimaryKeyFieldID,
		CanonicalWireJSON: row.CanonicalWireJSON,
		Fields:            fields,
		Version:           wire.Version,
		Checksum:          *wire.Checksum,
		Deleted:           wire.Deleted,
	}, nil
}

func nativeScopesForRecord(installation *nativeInstallationBinding, relation, tableID, canonical string) []string {
	_ = relation
	if scopes, found := installation.rowScopes[nativeRecordKey(tableID, canonical)]; found {
		return append([]string(nil), scopes...)
	}
	if len(installation.scopes) == 1 {
		for scope := range installation.scopes {
			return []string{scope}
		}
	}
	values := make([]string, 0, len(installation.scopes))
	for scope := range installation.scopes {
		values = append(values, scope)
	}
	sort.Strings(values)
	if len(values) != 0 {
		return values[:1]
	}
	return nil
}

func nativeSourceStatement(event nativeEventBinding, installation *nativeInstallationBinding) (string, []any, error) {
	image := event.After
	if image == nil {
		image = event.Before
	}
	if image == nil {
		return "", nil, errors.New("native source event has no image")
	}
	value, err := nativeImageStringField(*image, event.Table, "value")
	if err != nil && event.Operation != "delete" {
		return "", nil, err
	}
	runtimeScope := ""
	if len(event.AuthoredScopes) != 0 {
		runtimeScope = installation.scopes[event.AuthoredScopes[0]]
	}
	owner := strings.TrimPrefix(runtimeScope, "user:")
	switch event.Table.RuntimeName {
	case "cf_items", "cf_late_registration":
		if owner == runtimeScope || owner == "" {
			return "", nil, errors.New("native private source row has no user scope binding")
		}
		switch event.Operation {
		case "insert":
			return "INSERT INTO " + event.Table.RuntimeName + " (id, owner_id, value) VALUES ($1, $2, $3)", []any{event.RuntimeRecordID, owner, value}, nil
		case "update":
			return "UPDATE " + event.Table.RuntimeName + " SET owner_id = $2, value = $3, updated_at = clock_timestamp() WHERE id = $1", []any{event.RuntimeRecordID, owner, value}, nil
		case "delete":
			return "DELETE FROM " + event.Table.RuntimeName + " WHERE id = $1", []any{event.RuntimeRecordID}, nil
		}
	case "cf_global_items":
		switch event.Operation {
		case "insert":
			return "INSERT INTO cf_global_items (id, value) VALUES ($1, $2)", []any{event.RuntimeRecordID, value}, nil
		case "update":
			return "UPDATE cf_global_items SET value = $2, updated_at = clock_timestamp() WHERE id = $1", []any{event.RuntimeRecordID, value}, nil
		case "delete":
			return "DELETE FROM cf_global_items WHERE id = $1", []any{event.RuntimeRecordID}, nil
		}
	default:
		return "", nil, fmt.Errorf("native source table %q has no generic DML binding", event.Table.RuntimeName)
	}
	return "", nil, errors.New("native source event operation is unsupported")
}

func nativeImageStringField(image nativeAuthoredImage, table nativeTableBinding, name string) (string, error) {
	for authoredField := range table.Fields {
		if table.FieldNames[authoredField] != name {
			continue
		}
		var value string
		if err := json.Unmarshal(image.Fields[authoredField], &value); err != nil {
			return "", errors.New("native source string field is invalid")
		}
		return value, nil
	}
	return "", fmt.Errorf("native source field %q is not authored", name)
}

// RequestStep sends one arbitrary-user authenticated request to the current adapter.
func (c *NativeController) RequestStep(ctx context.Context, operation scenarios.Operation) (NativeStepObservation, error) {
	if err := c.context(ctx); err != nil {
		return NativeStepObservation{}, err
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return NativeStepObservation{}, fmt.Errorf("native controller request operation is invalid: %w", err)
	}
	key := scenarios.OperationKey(operation)
	if key != "connect/send" && key != "pull/request-page" && key != "push/submit" && key != "rebuild/request-page" {
		return NativeStepObservation{}, nativeUnsupported("request", operation)
	}
	userID, body, path, err := c.nativeHTTPRequest(operation)
	if err != nil {
		return NativeStepObservation{}, err
	}
	tokenProvider := TokenProviderFunc(func(tokenContext context.Context) (string, error) {
		return c.harness.NativeBearerToken(tokenContext, userID, c.now())
	})
	response, err := (&Client{BaseURL: c.harness.AdapterURL(), HTTP: c.httpClient, Tokens: tokenProvider}).Do(ctx, Request{
		Method:  http.MethodPost,
		Path:    path,
		Headers: http.Header{"Content-Type": []string{"application/json"}},
		Body:    body,
		Class:   key,
	})
	if err != nil {
		return NativeStepObservation{}, fmt.Errorf("execute native controller HTTP request: %w", err)
	}
	observation, err := nativeHTTPObservation(response)
	if err != nil {
		return NativeStepObservation{}, err
	}
	if response.Status >= 200 && response.Status < 300 {
		if err := c.rememberNativeHTTPResponse(key, userID, body, response.Body); err != nil {
			return NativeStepObservation{}, err
		}
	}
	return observation, nil
}

func (c *NativeController) nativeHTTPRequest(operation scenarios.Operation) (string, []byte, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.installation == nil {
		return "", nil, "", errors.New("native controller contract is not installed")
	}
	var authored map[string]json.RawMessage
	if err := jsonstrict.Decode(operation.Payload, &authored); err != nil {
		return "", nil, "", errors.New("decode native controller HTTP payload failed")
	}
	key := scenarios.OperationKey(operation)
	var userField string
	var path string
	var request map[string]any
	switch key {
	case "connect/send":
		userField = "user_id"
		path = "/sync/connect"
		request = make(map[string]any)
		if err := decodeNativeMap(operation.Payload, &request); err != nil {
			return "", nil, "", err
		}
		delete(request, "user_id")
		delete(request, "runtime_version")
		request["platform"] = "native-conformance"
		request["app_version"] = "0.3.0"
		if err := c.rewriteNativeSchemaMember(request, "schema"); err != nil {
			return "", nil, "", err
		}
		known, ok := request["known_scopes"].([]any)
		if !ok {
			return "", nil, "", errors.New("native connect known scopes are invalid")
		}
		runtimeKnown := make(map[string]any, len(known))
		for _, raw := range known {
			entry, ok := raw.(map[string]any)
			if !ok {
				return "", nil, "", errors.New("native connect known scope is invalid")
			}
			scope, _ := entry["scope_id"].(string)
			runtimeScope, err := c.runtimeScope(scope)
			if err != nil {
				return "", nil, "", err
			}
			cursor := any(nil)
			if value, found := c.scopeCursors[nativeScopeCursorKey(nativeMapString(request, "client_id"), runtimeScope)]; found {
				cursor = value
			}
			runtimeKnown[runtimeScope] = map[string]any{"cursor": cursor}
		}
		request["known_scopes"] = runtimeKnown
		if receipts, found := request["seed_receipts"].(map[string]any); found {
			rewritten := make(map[string]any, len(receipts))
			for scope, receipt := range receipts {
				runtimeScope, err := c.runtimeScope(scope)
				if err != nil {
					return "", nil, "", err
				}
				rewritten[runtimeScope] = receipt
			}
			request["seed_receipts"] = rewritten
		}
	case "pull/request-page":
		userField = "user_id"
		path = "/sync/pull"
		if err := decodeNativeMap(operation.Payload, &request); err != nil {
			return "", nil, "", err
		}
		delete(request, "user_id")
		if err := c.rewriteNativeSchemaMember(request, "schema"); err != nil {
			return "", nil, "", err
		}
		scopes, ok := request["scopes"].([]any)
		if !ok {
			return "", nil, "", errors.New("native pull scopes are invalid")
		}
		runtimeScopes := make(map[string]any, len(scopes))
		for _, raw := range scopes {
			entry, ok := raw.(map[string]any)
			if !ok {
				return "", nil, "", errors.New("native pull scope is invalid")
			}
			scope, _ := entry["scope_id"].(string)
			source, _ := entry["cursor_source"].(string)
			runtimeScope, err := c.runtimeScope(scope)
			if err != nil {
				return "", nil, "", err
			}
			cursor, err := c.nativeCursor(nativeMapString(request, "client_id"), runtimeScope, source, false)
			if err != nil {
				return "", nil, "", err
			}
			runtimeScopes[runtimeScope] = map[string]any{"cursor": cursor}
		}
		request["scopes"] = runtimeScopes
	case "push/submit":
		userField = "authenticated_user_id"
		path = "/sync/push"
		var wrapper map[string]any
		if err := decodeNativeMap(operation.Payload, &wrapper); err != nil {
			return "", nil, "", err
		}
		delivery, _ := wrapper["delivery"].(string)
		if delivery != "apply" {
			return "", nil, "", fmt.Errorf("native controller cannot execute push delivery %q through the RequestStep interface", delivery)
		}
		request, _ = wrapper["request"].(map[string]any)
		if request == nil {
			return "", nil, "", errors.New("native push request is invalid")
		}
		if err := c.rewriteNativePush(request); err != nil {
			return "", nil, "", err
		}
	case "rebuild/request-page":
		userField = "user_id"
		path = "/sync/rebuild"
		if err := decodeNativeMap(operation.Payload, &request); err != nil {
			return "", nil, "", err
		}
		delete(request, "user_id")
		if err := c.rewriteNativeSchemaMember(request, "schema"); err != nil {
			return "", nil, "", err
		}
		scope, _ := request["scope_id"].(string)
		runtimeScope, err := c.runtimeScope(scope)
		if err != nil {
			return "", nil, "", err
		}
		delete(request, "scope_id")
		request["scope"] = runtimeScope
		source, _ := request["cursor_source"].(string)
		delete(request, "cursor_source")
		cursor, err := c.nativeCursor(nativeMapString(request, "client_id"), runtimeScope, source, true)
		if err != nil {
			return "", nil, "", err
		}
		request["cursor"] = cursor
	default:
		return "", nil, "", nativeUnsupported("request", operation)
	}
	var userID string
	if err := json.Unmarshal(authored[userField], &userID); err != nil || !validNativeIdentity(userID) {
		return "", nil, "", errors.New("native HTTP authenticated user is invalid")
	}
	body, err := json.Marshal(request)
	if err != nil || jsonstrict.ValidateValue(body) != nil {
		return "", nil, "", errors.New("encode native controller HTTP request failed")
	}
	return userID, body, path, nil
}

func decodeNativeMap(raw []byte, target *map[string]any) error {
	if err := jsonstrict.Decode(raw, target); err != nil {
		return errors.New("decode native operation payload failed")
	}
	return nil
}

func (c *NativeController) rewriteNativeSchemaMember(request map[string]any, name string) error {
	raw, ok := request[name].(map[string]any)
	if !ok {
		return errors.New("native authored schema reference is invalid")
	}
	authored, err := nativeSchemaFromMap(raw)
	if err != nil {
		return err
	}
	if authored.Version == 0 && authored.Hash == "" {
		return nil
	}
	runtime, found := c.installation.runtimeSchemas[nativeSchemaKey(authored)]
	if !found {
		return fmt.Errorf("native authored schema %d/%s has no runtime binding", authored.Version, authored.Hash)
	}
	request[name] = map[string]any{"version": runtime.Version, "hash": runtime.Hash}
	return nil
}

func (c *NativeController) rewriteNativePush(request map[string]any) error {
	if err := c.rewriteNativeSchemaMember(request, "schema"); err != nil {
		return err
	}
	mutations, ok := request["mutations"].([]any)
	if !ok || len(mutations) == 0 {
		return errors.New("native push mutations are invalid")
	}
	for _, raw := range mutations {
		mutation, ok := raw.(map[string]any)
		if !ok {
			return errors.New("native push mutation is invalid")
		}
		authoredTable, _ := mutation["table"].(string)
		table, found := c.installation.tables[authoredTable]
		if !found {
			return fmt.Errorf("native push table %q has no runtime binding", authoredTable)
		}
		mutation["table"] = table.RuntimeID
		if err := c.rewriteNativeSchemaMember(mutation, "authored_schema"); err != nil {
			return err
		}
		pk, ok := mutation["pk"].(map[string]any)
		if !ok || len(pk) != 1 {
			return errors.New("native push primary key is invalid")
		}
		value, found := pk[table.AuthoredPrimary]
		if !found {
			return errors.New("native push primary key field is misbound")
		}
		canonical, err := json.Marshal(value)
		if err != nil {
			return errors.New("native push primary key value is invalid")
		}
		mutation["pk"] = map[string]any{table.RuntimePrimary: nativeRuntimeUUID(table.AuthoredID, string(canonical))}
		if columns, found := mutation["columns"].(map[string]any); found {
			rewritten := make(map[string]any, len(columns))
			for authoredField, value := range columns {
				runtimeField, known := table.Fields[authoredField]
				if !known || authoredField == table.AuthoredPrimary {
					return errors.New("native push column has no writable runtime binding")
				}
				runtimeValue, err := nativeRuntimeFieldValue(table, authoredField, value)
				if err != nil {
					return err
				}
				rewritten[runtimeField] = runtimeValue
			}
			mutation["columns"] = rewritten
		}
	}
	return nil
}

func (c *NativeController) runtimeScope(authored string) (string, error) {
	runtime, found := c.installation.scopes[authored]
	if !found {
		return "", fmt.Errorf("native authored scope %q has no runtime binding", authored)
	}
	return runtime, nil
}

func (c *NativeController) nativeCursor(clientID, runtimeScope, source string, rebuild bool) (any, error) {
	switch source {
	case "none":
		return nil, nil
	case "local_checkpoint":
		cursor, found := c.scopeCursors[nativeScopeCursorKey(clientID, runtimeScope)]
		if !found {
			return nil, errors.New("native local checkpoint cursor is unavailable")
		}
		return cursor, nil
	case "local_rebuild_continuation":
		if !rebuild {
			return nil, errors.New("native rebuild continuation was requested outside rebuild")
		}
		cursor, found := c.rebuildCursors[nativeScopeCursorKey(clientID, runtimeScope)]
		if !found {
			return nil, errors.New("native rebuild continuation is unavailable")
		}
		return cursor, nil
	case "forged":
		if !rebuild {
			return nil, errors.New("native forged cursor was requested outside rebuild")
		}
		return "native-forged-rebuild-cursor", nil
	default:
		return nil, fmt.Errorf("native cursor source %q is unsupported", source)
	}
}

func nativeHTTPObservation(response Response) (NativeStepObservation, error) {
	wire := &NativeWireFacts{HTTPStatus: response.Status}
	if response.Status >= 200 && response.Status < 300 {
		if len(bytes.TrimSpace(response.Body)) == 0 || !json.Valid(response.Body) {
			return NativeStepObservation{}, errors.New("native successful HTTP response is invalid")
		}
		return NativeStepObservation{Disposition: "success", Wire: wire}, nil
	}
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
	}
	if err := jsonstrict.Decode(response.Body, &envelope); err != nil || envelope.Error.Code == "" || envelope.Error.Message == "" {
		return NativeStepObservation{}, errors.New("native HTTP error response is invalid")
	}
	code := envelope.Error.Code
	wire.ErrorCode = &code
	wire.Retryable = envelope.Error.Retryable
	return NativeStepObservation{Disposition: "error", ErrorCode: &code, Wire: wire}, nil
}

func (c *NativeController) rememberNativeHTTPResponse(key, userID string, requestBody, responseBody []byte) error {
	var request map[string]any
	var response map[string]any
	if err := decodeNativeMap(requestBody, &request); err != nil || decodeNativeMap(responseBody, &response) != nil {
		return errors.New("decode successful native HTTP exchange failed")
	}
	clientID := nativeMapString(request, "client_id")
	switch key {
	case "connect/send":
		if generation, ok := nativeJSONInt64(response["client_generation"]); !ok || generation <= 0 {
			return errors.New("native connect response generation is invalid")
		}
		delta, ok := response["scopes"].(map[string]any)
		if !ok {
			return errors.New("native connect scope response is invalid")
		}
		additions, _ := delta["add"].([]any)
		for _, raw := range additions {
			entry, ok := raw.(map[string]any)
			if !ok {
				return errors.New("native connect scope addition is invalid")
			}
			scope, _ := entry["id"].(string)
			if scope == "" {
				return errors.New("native connect scope identity is invalid")
			}
			if cursor, ok := entry["cursor"].(string); ok && cursor != "" {
				c.scopeCursors[nativeScopeCursorKey(clientID, scope)] = cursor
			}
		}
		_ = userID
	case "pull/request-page":
		cursors, ok := response["scope_cursors"].(map[string]any)
		if !ok {
			return errors.New("native pull cursor response is invalid")
		}
		for scope, raw := range cursors {
			cursor, ok := raw.(string)
			if !ok || cursor == "" {
				return errors.New("native pull cursor is invalid")
			}
			c.scopeCursors[nativeScopeCursorKey(clientID, scope)] = cursor
		}
	case "rebuild/request-page":
		scope := nativeMapString(request, "scope")
		if more, _ := response["has_more"].(bool); more {
			cursor, ok := response["cursor"].(string)
			if !ok || cursor == "" {
				return errors.New("native rebuild continuation response is invalid")
			}
			c.rebuildCursors[nativeScopeCursorKey(clientID, scope)] = cursor
		} else if cursor, ok := response["final_scope_cursor"].(string); ok && cursor != "" {
			c.scopeCursors[nativeScopeCursorKey(clientID, scope)] = cursor
			delete(c.rebuildCursors, nativeScopeCursorKey(clientID, scope))
		}
	}
	return nil
}

// ProcessStep executes one server process operation or returns a precise boundary error.
func (c *NativeController) ProcessStep(ctx context.Context, clientKey *string, operation scenarios.Operation) (NativeStepObservation, error) {
	if err := c.context(ctx); err != nil {
		return NativeStepObservation{}, err
	}
	if clientKey != nil {
		return NativeStepObservation{}, errors.New("native controller cannot execute a client process operation")
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return NativeStepObservation{}, fmt.Errorf("native controller process operation is invalid: %w", err)
	}
	switch scenarios.OperationKey(operation) {
	case "process/materialize-source-transaction":
		return c.materializeSourceTransaction(ctx, operation)
	case "process/acknowledge-contiguous-prefix":
		return c.acknowledgeContiguousPrefix(ctx, operation)
	case "process/repair-and-retry-source-transaction":
		retried, err := c.harness.Operator().RetryWALPoison(ctx)
		if err != nil {
			return NativeStepObservation{}, err
		}
		if !retried {
			return NativeStepObservation{}, errors.New("native WAL poison retry was not accepted")
		}
		return nativeSuccess(), nil
	case "process/restart-wal-worker":
		if err := c.harness.RestartPostgres(ctx); err != nil {
			return NativeStepObservation{}, err
		}
		return nativeSuccess(), nil
	case "process/response-loss":
		return NativeStepObservation{}, errors.New("native controller cannot record client response loss because ProcessStep omits the transport handle")
	case "process/restart-client":
		return NativeStepObservation{}, errors.New("native controller cannot restart a client process")
	default:
		return NativeStepObservation{}, nativeUnsupported("process", operation)
	}
}

func (c *NativeController) materializeSourceTransaction(ctx context.Context, operation scenarios.Operation) (NativeStepObservation, error) {
	stream, commit, err := nativeProcessTransactionIdentity(operation.Payload)
	if err != nil {
		return NativeStepObservation{}, err
	}
	key := nativeTransactionKey(stream, commit)
	c.mu.Lock()
	transaction := c.transactions[key]
	if transaction == nil {
		c.mu.Unlock()
		return NativeStepObservation{}, errors.New("native source transaction was not committed through this controller")
	}
	for _, other := range c.transactions {
		if other.AuthoredStream == stream && !other.Materialized && compareNativeLSN(other.AuthoredCommitLSN, commit) < 0 {
			code := "source_transaction_predecessor_pending"
			c.mu.Unlock()
			return NativeStepObservation{Disposition: "error", ErrorCode: &code}, nil
		}
	}
	c.mu.Unlock()

	deadline, cancel := context.WithTimeout(ctx, c.waitTimeout)
	defer cancel()
	for {
		if err := c.resolveRuntimeTransaction(deadline, transaction); err == nil && (!transaction.ApplicationPush || c.resolveApplicationPushRecords(deadline, transaction) == nil) {
			break
		}
		if err := waitNativePoll(deadline); err != nil {
			return NativeStepObservation{}, errors.New("native source transaction did not become WAL-materialized")
		}
	}
	if err := c.validateRuntimeTransactionOrder(ctx, transaction); err != nil {
		return NativeStepObservation{}, err
	}
	c.mu.Lock()
	transaction.Materialized = true
	if transaction.ApplicationPush {
		for _, event := range transaction.Events {
			recordKey := nativeRecordKey(event.Table.AuthoredID, event.After.CanonicalWireJSON)
			c.records[recordKey] = &nativeRecordBinding{
				Table:           event.Table,
				RecordID:        event.RecordID,
				RuntimeRecordID: event.RuntimeRecordID,
				Image:           *event.After,
				AuthoredScopes:  append([]string(nil), event.AuthoredScopes...),
			}
		}
	}
	c.mu.Unlock()
	return nativeSuccess(), nil
}

func (c *NativeController) resolveApplicationPushRecords(ctx context.Context, transaction *nativeTransactionBinding) error {
	database, err := c.harness.openDatabase(ctx, c.harness.names.Database, c.harness.env.Admin, false)
	if err != nil {
		return err
	}
	defer database.Close()
	for index := range transaction.Events {
		event := &transaction.Events[index]
		var rowData []byte
		var version, checksum string
		var deleted bool
		err := database.QueryRowContext(ctx, `
			SELECT captured.row_data, captured.row_version::text, encode(captured.checksum, 'hex'), captured.deleted
			FROM synchro.sync_captured_rows captured
			JOIN synchro.sync_registry registry
			  ON registry.registry_generation = captured.registry_generation
			 AND registry.relation_id = captured.relation_id
			WHERE registry.table_name = $1 AND captured.record_id = $2`, event.Table.RuntimeName, event.RuntimeRecordID).Scan(&rowData, &version, &checksum, &deleted)
		if err != nil || deleted || !diagnosticUUIDPattern.MatchString(version) || len(checksum) != 64 {
			return errors.New("native application push row is not materialized")
		}
		record := &nativeRecordBinding{Table: event.Table, RuntimeRecordID: event.RuntimeRecordID, Image: *event.After}
		if err := validateNativeRuntimeRow(record, rowData); err != nil {
			return err
		}
		event.After.Version = version
		event.After.Checksum = checksum
	}
	return nil
}

func nativeProcessTransactionIdentity(raw json.RawMessage) (string, string, error) {
	var payload struct {
		StreamGeneration string `json:"stream_generation"`
		CommitLSN        string `json:"commit_lsn"`
	}
	if err := jsonstrict.Decode(raw, &payload); err != nil || payload.StreamGeneration == "" || payload.CommitLSN == "" {
		return "", "", errors.New("native process transaction identity is invalid")
	}
	return payload.StreamGeneration, payload.CommitLSN, nil
}

func (c *NativeController) resolveRuntimeTransaction(ctx context.Context, binding *nativeTransactionBinding) error {
	database, err := c.harness.openDatabase(ctx, c.harness.names.Database, c.harness.env.Admin, false)
	if err != nil {
		return err
	}
	defer database.Close()
	type runtimeIdentity struct {
		stream   string
		commit   string
		end      string
		registry int64
		ordinal  uint64
	}
	identities := make([]runtimeIdentity, 0, len(binding.Events))
	for _, event := range binding.Events {
		var identity runtimeIdentity
		var ordinal int64
		err := database.QueryRowContext(ctx, `
			SELECT event.stream_generation, event.commit_lsn::text, transaction.end_lsn::text,
			       transaction.registry_generation, event.event_ordinal
			FROM synchro.sync_wal_events event
			JOIN synchro.sync_wal_transactions transaction
			  ON transaction.stream_generation = event.stream_generation
			 AND transaction.commit_lsn = event.commit_lsn
			JOIN synchro.sync_write_fences fence ON fence.fence_id = event.fence_id
			WHERE event.physical_relation = $1
			  AND COALESCE(fence.new_record_id, fence.old_record_id) = $2
			  AND event.operation = $3
			ORDER BY event.commit_lsn DESC
			LIMIT 1`, event.Table.RuntimeName, event.RuntimeRecordID, event.Operation).Scan(
			&identity.stream, &identity.commit, &identity.end, &identity.registry, &ordinal,
		)
		if err != nil || ordinal < 0 {
			return errors.New("native runtime WAL event binding is unavailable")
		}
		identity.ordinal = uint64(ordinal)
		identities = append(identities, identity)
	}
	first := identities[0]
	ordinals := make([]uint64, len(identities))
	for index, identity := range identities {
		if identity.stream != first.stream || identity.commit != first.commit || identity.end != first.end || identity.registry != first.registry {
			return errors.New("native authored transaction spans more than one runtime WAL transaction")
		}
		ordinals[index] = identity.ordinal
	}
	for index := 1; index < len(ordinals); index++ {
		if ordinals[index] <= ordinals[index-1] {
			return errors.New("native authored event order does not match runtime WAL order")
		}
	}
	binding.RuntimeStream = first.stream
	binding.RuntimeCommitLSN = first.commit
	binding.RuntimeEndLSN = first.end
	binding.RuntimeRegistry = first.registry
	binding.RuntimeEventOrdinals = ordinals
	return nil
}

func (c *NativeController) validateRuntimeTransactionOrder(ctx context.Context, current *nativeTransactionBinding) error {
	c.mu.Lock()
	others := make([]*nativeTransactionBinding, 0, len(c.transactions))
	for _, other := range c.transactions {
		if other != current && other.Materialized && other.AuthoredStream == current.AuthoredStream {
			others = append(others, other)
		}
	}
	c.mu.Unlock()
	for _, other := range others {
		authoredOrder := compareNativeLSN(other.AuthoredCommitLSN, current.AuthoredCommitLSN)
		runtimeOrder := compareNativeLSN(other.RuntimeCommitLSN, current.RuntimeCommitLSN)
		if authoredOrder != 0 && runtimeOrder != 0 && authoredOrder != runtimeOrder {
			return errors.New("authored WAL commit order does not match runtime commit order")
		}
	}
	_ = ctx
	return nil
}

func (c *NativeController) acknowledgeContiguousPrefix(ctx context.Context, operation scenarios.Operation) (NativeStepObservation, error) {
	var payload struct {
		StreamGeneration string `json:"stream_generation"`
	}
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil || payload.StreamGeneration == "" {
		return NativeStepObservation{}, errors.New("native acknowledgement stream identity is invalid")
	}
	c.mu.Lock()
	var latest *nativeTransactionBinding
	for _, transaction := range c.transactions {
		if transaction.AuthoredStream == payload.StreamGeneration && transaction.Materialized && (latest == nil || compareNativeLSN(latest.AuthoredEndLSN, transaction.AuthoredEndLSN) < 0) {
			latest = transaction
		}
	}
	c.mu.Unlock()
	if latest == nil {
		return NativeStepObservation{}, errors.New("native acknowledgement has no materialized transaction")
	}
	deadline, cancel := context.WithTimeout(ctx, c.waitTimeout)
	defer cancel()
	for {
		database, err := c.harness.openDatabase(deadline, c.harness.names.Database, c.harness.env.Admin, false)
		if err == nil {
			var acknowledged bool
			err = database.QueryRowContext(deadline, `
				SELECT acknowledged_end_lsn >= $1::pg_lsn
				FROM synchro.sync_wal_progress WHERE singleton`, latest.RuntimeEndLSN).Scan(&acknowledged)
			_ = database.Close()
			if err == nil && acknowledged {
				return nativeSuccess(), nil
			}
		}
		if err := waitNativePoll(deadline); err != nil {
			return NativeStepObservation{}, errors.New("native WAL acknowledgement did not reach the authored prefix")
		}
	}
}

// Capture returns one consistent server-state projection.
func (c *NativeController) Capture(ctx context.Context, clientKeys, sources []string) ([]NativeCaptureFacts, error) {
	if err := c.context(ctx); err != nil {
		return nil, err
	}
	if len(sources) != 1 || sources[0] != "server-state" {
		return nil, errors.New("native controller capture supports only one server-state source")
	}
	facts, err := c.captureServerState(ctx)
	if err != nil {
		return nil, err
	}
	return []NativeCaptureFacts{{Source: "server-state", StateFacts: facts}}, nil
}

func (c *NativeController) captureServerState(ctx context.Context) (scenarios.StateFacts, error) {
	c.mu.Lock()
	installation := c.installation
	transactions := make([]*nativeTransactionBinding, 0, len(c.transactions))
	for _, value := range c.transactions {
		copy := *value
		copy.Events = append([]nativeEventBinding(nil), value.Events...)
		copy.RuntimeEventOrdinals = append([]uint64(nil), value.RuntimeEventOrdinals...)
		transactions = append(transactions, &copy)
	}
	records := make([]*nativeRecordBinding, 0, len(c.records))
	for _, value := range c.records {
		copy := *value
		copy.AuthoredScopes = append([]string(nil), value.AuthoredScopes...)
		records = append(records, &copy)
	}
	c.mu.Unlock()
	if installation == nil {
		return scenarios.StateFacts{}, errors.New("native controller contract is not installed")
	}
	database, err := c.harness.openDatabase(ctx, c.harness.names.Database, c.harness.env.Admin, false)
	if err != nil {
		return scenarios.StateFacts{}, errors.New("open native server-state capture failed")
	}
	defer database.Close()
	tx, err := database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return scenarios.StateFacts{}, errors.New("begin native server-state capture failed")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
		return scenarios.StateFacts{}, errors.New("set native server-state capture read-only failed")
	}

	var facts scenarios.StateFacts
	if err := captureNativeRegistryAndStream(ctx, tx, installation, transactions, &facts); err != nil {
		return scenarios.StateFacts{}, err
	}
	if err := captureNativeTransactions(ctx, tx, installation, transactions, &facts); err != nil {
		return scenarios.StateFacts{}, err
	}
	if err := captureNativeRowsAndScopes(ctx, tx, installation, transactions, records, &facts); err != nil {
		return scenarios.StateFacts{}, err
	}
	if err := captureNativeCountsAndRebuilds(ctx, tx, installation, &facts); err != nil {
		return scenarios.StateFacts{}, err
	}
	if err := tx.Commit(); err != nil {
		return scenarios.StateFacts{}, errors.New("commit native server-state capture failed")
	}
	return facts, nil
}

func captureNativeRegistryAndStream(ctx context.Context, tx *sql.Tx, installation *nativeInstallationBinding, transactions []*nativeTransactionBinding, facts *scenarios.StateFacts) error {
	var generation int64
	var runtimeStream string
	var materializedCommit, acknowledgedEnd sql.NullString
	if err := tx.QueryRowContext(ctx, `
		SELECT generation.generation, progress.stream_generation,
		       progress.materialized_commit_lsn::text, progress.acknowledged_end_lsn::text
		FROM synchro.sync_registry_generations generation
		CROSS JOIN synchro.sync_wal_progress progress
		WHERE generation.state = 'active' AND generation.validated AND progress.singleton`).Scan(
		&generation, &runtimeStream, &materializedCommit, &acknowledgedEnd,
	); err != nil {
		return errors.New("read native registry and stream state failed")
	}
	if generation != installation.runtimeRegistryGeneration || runtimeStream == "" {
		return errors.New("native runtime registry or stream binding changed")
	}
	facts.Registry = &scenarios.RegistryFact{CurrentGeneration: installation.authoredRegistryGeneration}
	latest := latestNativeTransaction(transactions)
	stream := scenarios.StreamFact{MaterializedStreamGeneration: installation.authoredStream, MaterializedKind: "generation_start"}
	if latest != nil {
		if !materializedCommit.Valid || compareNativeLSN(materializedCommit.String, latest.RuntimeCommitLSN) < 0 {
			return errors.New("native runtime materialized position is behind the authored binding")
		}
		stream.MaterializedKind = "transaction_end"
		stream.MaterializedCommitLSN = latest.AuthoredCommitLSN
		if acknowledgedEnd.Valid && compareNativeLSN(acknowledgedEnd.String, latest.RuntimeEndLSN) >= 0 {
			stream.AcknowledgedEndLSN = latest.AuthoredEndLSN
		}
	}
	facts.Stream = &stream
	return nil
}

func captureNativeTransactions(ctx context.Context, tx *sql.Tx, installation *nativeInstallationBinding, transactions []*nativeTransactionBinding, facts *scenarios.StateFacts) error {
	for _, binding := range transactions {
		if !binding.Materialized {
			continue
		}
		var eventCount int64
		if err := tx.QueryRowContext(ctx, `
			SELECT event_count FROM synchro.sync_wal_transactions
			WHERE stream_generation = $1 AND commit_lsn = $2::pg_lsn AND end_lsn = $3::pg_lsn
			  AND registry_generation = $4`, binding.RuntimeStream, binding.RuntimeCommitLSN, binding.RuntimeEndLSN, binding.RuntimeRegistry).Scan(&eventCount); err != nil || eventCount != int64(len(binding.Events)) {
			return errors.New("native runtime WAL transaction no longer matches its authored binding")
		}
		ordinals := make([]uint64, len(binding.Events))
		for index, event := range binding.Events {
			ordinals[index] = event.AuthoredOrdinal
		}
		facts.Transactions = append(facts.Transactions, scenarios.TransactionFact{
			StreamGeneration:   binding.AuthoredStream,
			CommitLSN:          binding.AuthoredCommitLSN,
			EndLSN:             binding.AuthoredEndLSN,
			RegistryGeneration: installation.authoredRegistryGeneration,
			Lifecycle:          "materialized",
			EventOrdinals:      ordinals,
		})
	}
	count := uint64(len(facts.Transactions))
	facts.TransactionCount = &count
	return nil
}

func captureNativeRowsAndScopes(ctx context.Context, tx *sql.Tx, installation *nativeInstallationBinding, transactions []*nativeTransactionBinding, records []*nativeRecordBinding, facts *scenarios.StateFacts) error {
	scopeRows := make(map[string]uint64)
	scopeVersions := make(map[string][]string)
	for _, record := range records {
		var rowData []byte
		var runtimeVersion, runtimeChecksum string
		var deleted bool
		err := tx.QueryRowContext(ctx, `
			SELECT captured.row_data, captured.row_version::text, encode(captured.checksum, 'hex'), captured.deleted
			FROM synchro.sync_captured_rows captured
			JOIN synchro.sync_registry registry
			  ON registry.registry_generation = captured.registry_generation
			 AND registry.relation_id = captured.relation_id
			WHERE registry.table_name = $1 AND captured.record_id = $2`, record.Table.RuntimeName, record.RuntimeRecordID).Scan(&rowData, &runtimeVersion, &runtimeChecksum, &deleted)
		if err != nil || deleted || !diagnosticUUIDPattern.MatchString(runtimeVersion) || len(runtimeChecksum) != 64 {
			return errors.New("native runtime captured row is absent or invalid")
		}
		if err := validateNativeRuntimeRow(record, rowData); err != nil {
			return err
		}
		facts.Rows = append(facts.Rows, scenarios.RowFact{
			TableID:           record.Table.AuthoredID,
			CanonicalWireJSON: record.Image.CanonicalWireJSON,
			Version:           record.Image.Version,
			Checksum:          record.Image.Checksum,
		})
		for _, authoredScope := range record.AuthoredScopes {
			runtimeScope, found := installation.scopes[authoredScope]
			if !found {
				return errors.New("native captured row scope has no runtime binding")
			}
			var edgeCount int64
			if err := tx.QueryRowContext(ctx, `
				SELECT count(*) FROM synchro.sync_bucket_edges
				WHERE table_name = $1 AND record_id = $2 AND bucket_id = $3`, record.Table.RuntimeName, record.RuntimeRecordID, runtimeScope).Scan(&edgeCount); err != nil || edgeCount != 1 {
				return errors.New("native runtime scope edge does not match its authored binding")
			}
			scopeRows[authoredScope]++
			scopeVersions[authoredScope] = append(scopeVersions[authoredScope], record.Image.Version)
		}
	}
	for authoredScope := range installation.scopes {
		versions := append([]string{}, scopeVersions[authoredScope]...)
		sort.Strings(versions)
		facts.Scopes = append(facts.Scopes, scenarios.ScopeFact{
			ScopeID:              authoredScope,
			MembershipGeneration: 1,
			Cardinality:          scopeRows[authoredScope],
			EffectVersions:       versions,
		})
	}
	rowCount := uint64(len(facts.Rows))
	scopeCount := uint64(len(facts.Scopes))
	facts.RowCount = &rowCount
	facts.ScopeCount = &scopeCount
	_ = transactions
	return nil
}

func validateNativeRuntimeRow(record *nativeRecordBinding, raw []byte) error {
	var values map[string]json.RawMessage
	if err := jsonstrict.Decode(raw, &values); err != nil {
		return errors.New("decode native runtime captured row failed")
	}
	for authoredField, expected := range record.Image.Fields {
		runtimeField, found := record.Table.Fields[authoredField]
		if !found {
			return errors.New("native runtime captured field binding is absent")
		}
		actual, found := values[runtimeField]
		if !found {
			return errors.New("native runtime captured field is absent")
		}
		if authoredField == record.Table.AuthoredPrimary {
			expected, _ = json.Marshal(record.RuntimeRecordID)
		}
		if !nativeJSONEqual(actual, expected) {
			return errors.New("native runtime captured field differs from the authored source image")
		}
	}
	return nil
}

func captureNativeCountsAndRebuilds(ctx context.Context, tx *sql.Tx, installation *nativeInstallationBinding, facts *scenarios.StateFacts) error {
	var batchCount, mutationCount uint64
	for _, client := range installation.clients {
		var batches, mutations uint64
		if err := tx.QueryRowContext(ctx, `
			SELECT (SELECT count(*) FROM synchro.sync_push_batches WHERE user_id = $1 AND client_id = $2),
			       (SELECT count(*) FROM synchro.sync_push_mutations WHERE user_id = $1 AND client_id = $2)`, client.UserID, client.ClientID).Scan(&batches, &mutations); err != nil {
			return errors.New("read native push ledger counts failed")
		}
		batchCount += batches
		mutationCount += mutations
	}
	facts.BatchCount = &batchCount
	facts.MutationCount = &mutationCount
	rows, err := tx.QueryContext(ctx, `
		SELECT session.user_id, session.client_id, session.scope_id, session.rebuild_id::text,
		       session.page_limit, session.staged_row_count,
		       (SELECT count(*) FROM synchro.sync_rebuild_pages page
		        WHERE page.session_id = session.session_id),
		       COALESCE((SELECT max(page.next_row_ordinal) FROM synchro.sync_rebuild_pages page
		                 WHERE page.session_id = session.session_id), 0),
		       EXISTS (SELECT 1 FROM synchro.sync_rebuild_pages page
		               WHERE page.session_id = session.session_id
		                 AND NULLIF(page.response->>'cursor', '') IS NOT NULL),
		       EXISTS (SELECT 1 FROM synchro.sync_rebuild_pages page
		               WHERE page.session_id = session.session_id
		                 AND NULLIF(page.response->>'final_scope_cursor', '') IS NOT NULL),
		       CASE WHEN expires_at <= now() THEN 'expired' ELSE 'active' END
		FROM synchro.sync_rebuild_sessions session
		ORDER BY session.user_id, session.client_id, session.scope_id, session.rebuild_id`)
	if err != nil {
		return errors.New("read native rebuild state failed")
	}
	defer rows.Close()
	for rows.Next() {
		var value scenarios.RebuildFact
		var runtimeScope string
		if err := rows.Scan(&value.UserID, &value.ClientID, &runtimeScope, &value.RebuildID, &value.PageLimit, &value.StagedRowCount, &value.PageCount, &value.NextRowOrdinal, &value.HasContinuation, &value.HasFinalCursor, &value.Status); err != nil {
			return errors.New("scan native rebuild state failed")
		}
		authoredScope, found := installation.runtimeScopes[runtimeScope]
		if !found {
			continue
		}
		value.ScopeID = authoredScope
		facts.Rebuilds = append(facts.Rebuilds, value)
	}
	if err := rows.Err(); err != nil {
		return errors.New("read native rebuild state failed")
	}
	rebuildCount := uint64(len(facts.Rebuilds))
	facts.RebuildCount = &rebuildCount
	return nil
}

// Close closes the controller and optionally the owned black-box harness.
func (c *NativeController) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("native controller close context is required")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	return c.harness.Close(ctx)
}

func (c *NativeController) context(ctx context.Context) error {
	if c == nil || c.harness == nil {
		return errors.New("native controller is unavailable")
	}
	if ctx == nil {
		return errors.New("native controller context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return errors.New("native controller is closed")
	}
	return nil
}

func nativeUnsupported(boundary string, operation scenarios.Operation) error {
	return fmt.Errorf("native controller %s operation %q is unsupported", boundary, scenarios.OperationKey(operation))
}

func nativeSuccess() NativeStepObservation {
	return NativeStepObservation{Disposition: "success"}
}

func validNativeSchemaReference(value nativeSchemaReference, fresh bool) bool {
	if fresh && value.Version == 0 && value.Hash == "" {
		return true
	}
	if value.Version <= 0 || len(value.Hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(value.Hash)
	return err == nil && strings.ToLower(value.Hash) == value.Hash
}

func nativeSchemaKey(value nativeSchemaReference) string {
	return fmt.Sprintf("%d\x00%s", value.Version, value.Hash)
}

func nativeSchemaFromMap(value map[string]any) (nativeSchemaReference, error) {
	version, ok := nativeJSONInt64(value["version"])
	if !ok {
		return nativeSchemaReference{}, errors.New("native schema version is invalid")
	}
	hash, ok := value["hash"].(string)
	if !ok {
		return nativeSchemaReference{}, errors.New("native schema hash is invalid")
	}
	result := nativeSchemaReference{Version: version, Hash: hash}
	if !validNativeSchemaReference(result, true) {
		return nativeSchemaReference{}, errors.New("native schema reference is invalid")
	}
	return result, nil
}

func nativeJSONInt64(value any) (int64, bool) {
	switch value := value.(type) {
	case json.Number:
		result, err := value.Int64()
		return result, err == nil
	case float64:
		result := int64(value)
		return result, float64(result) == value
	case int64:
		return value, true
	default:
		return 0, false
	}
}

func nativeMapString(value map[string]any, name string) string {
	result, _ := value[name].(string)
	return result
}

func nativeAuthoredRecordID(canonical string) (string, error) {
	var result string
	if err := json.Unmarshal([]byte(canonical), &result); err != nil || result == "" {
		return "", errors.New("native authored row identity is not a nonempty canonical string")
	}
	encoded, _ := json.Marshal(result)
	if string(encoded) != canonical {
		return "", errors.New("native authored row identity is not canonical")
	}
	return result, nil
}

func nativeRuntimeUUID(tableID, canonical string) string {
	digest := nativeSHA256([]byte("synchro:native-runtime-row:v1\x00" + tableID + "\x00" + canonical))
	digest[6] = digest[6]&0x0f | 0x40
	digest[8] = digest[8]&0x3f | 0x80
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

func nativeSHA256(data []byte) [32]byte {
	return sha256.Sum256(data)
}

func nativeTransactionKey(stream, commit string) string {
	return stream + "\x00" + commit
}

func nativeRecordKey(table, record string) string {
	return table + "\x00" + record
}

func nativeScopeCursorKey(client, scope string) string {
	return client + "\x00" + scope
}

func compareNativeLSN(left, right string) int {
	leftValue, leftOK := parseNativeLSN(left)
	rightValue, rightOK := parseNativeLSN(right)
	if leftOK && rightOK {
		return leftValue.Cmp(rightValue)
	}
	return strings.Compare(left, right)
}

func parseNativeLSN(value string) (*big.Int, bool) {
	if value == "" {
		return nil, false
	}
	base := 10
	digits := value
	if before, after, found := strings.Cut(value, "/"); found {
		base = 16
		digits = before + after
	}
	result := new(big.Int)
	parsed, ok := result.SetString(digits, base)
	return parsed, ok
}

func latestNativeTransaction(values []*nativeTransactionBinding) *nativeTransactionBinding {
	var latest *nativeTransactionBinding
	for _, value := range values {
		if !value.Materialized {
			continue
		}
		if latest == nil || compareNativeLSN(latest.AuthoredCommitLSN, value.AuthoredCommitLSN) < 0 {
			latest = value
		}
	}
	return latest
}

func nativeJSONEqual(left, right []byte) bool {
	var leftValue any
	var rightValue any
	leftDecoder := json.NewDecoder(bytes.NewReader(left))
	leftDecoder.UseNumber()
	rightDecoder := json.NewDecoder(bytes.NewReader(right))
	rightDecoder.UseNumber()
	return leftDecoder.Decode(&leftValue) == nil && rightDecoder.Decode(&rightValue) == nil && reflect.DeepEqual(leftValue, rightValue)
}

func waitNativePoll(ctx context.Context) error {
	timer := time.NewTimer(nativeControllerPollInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
