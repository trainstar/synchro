package reference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

// OperationHandler applies one strictly decoded operation to an isolated model.
type OperationHandler func(context.Context, *Model, json.RawMessage, ResolvedOperationInput) (StepResult, error)

type operationImplementation func(context.Context, *Model, json.RawMessage) (StepResult, error)

// StepResultKind identifies one closed operation result shape.
type StepResultKind string

const (
	StepResultKindContractInstalled StepResultKind = "contract_installed"
	StepResultKindConnect           StepResultKind = "connect"
	StepResultKindLocal             StepResultKind = "local"
	StepResultKindLifecycle         StepResultKind = "lifecycle"
	StepResultKindPush              StepResultKind = "push"
	StepResultKindPull              StepResultKind = "pull"
	StepResultKindRebuild           StepResultKind = "rebuild"
	StepResultKindWAL               StepResultKind = "wal"
	StepResultKindSchema            StepResultKind = "schema"
	StepResultKindRetention         StepResultKind = "retention"
	StepResultKindClient            StepResultKind = "client"
)

// HTTPCode is one stable endpoint result code.
type HTTPCode string

// HTTPObservation contains bounded endpoint transport results.
type HTTPObservation struct {
	Status                    int
	HasCode                   bool
	Code                      HTTPCode
	Retryable                 bool
	HasRetryAfterMilliseconds bool
	RetryAfterMilliseconds    uint64
	Body                      []byte
}

// LifecycleObservation contains one client lifecycle transition.
type LifecycleObservation struct {
	Client ClientKey
	Prior  ClientLifecycle
	Next   ClientLifecycle
}

// ConnectObservation contains one connect generation, schema, and assignment result.
type ConnectObservation struct {
	Client          ClientKey
	Generation      Generation
	ScopeSetVersion ScopeSetVersion
	Schema          SchemaObservation
	AddedScopes     []ScopeID
	RemovedScopes   []ScopeID
	ScopeCursors    []ScopeCursorObservation
}

// LocalObservation contains one durable local operation result.
type LocalObservation struct {
	Client   ClientKey
	Mutation MutationID
	Batch    BatchID
	Status   LocalMutationStatus
}

// ReplayDisposition identifies first execution or replay.
type ReplayDisposition string

const (
	ReplayDispositionExecuted ReplayDisposition = "executed"
	ReplayDispositionReplayed ReplayDisposition = "replayed"
)

// MutationObservation contains one ordered push mutation result.
type MutationObservation struct {
	Mutation MutationID
	State    MutationOutcomeState
	Reason   ReasonCode
}

// PushObservation contains one atomic batch result.
type PushObservation struct {
	Batch     BatchKey
	Replay    ReplayDisposition
	Mutations []MutationObservation
}

// PullChangeObservation contains one ordered pull change.
type PullChangeObservation struct {
	Scope       ScopeID
	Row         RowIdentity
	Operation   EffectOperation
	Version     RowVersion
	HasChecksum bool
	Checksum    Checksum
}

// CursorDisposition identifies one scope cursor result.
type CursorDisposition string

const (
	CursorDispositionIssued          CursorDisposition = "issued"
	CursorDispositionAcknowledged    CursorDisposition = "acknowledged"
	CursorDispositionUnchanged       CursorDisposition = "unchanged"
	CursorDispositionRebuildRequired CursorDisposition = "rebuild_required"
)

// ScopeCursorObservation contains one ordered scope cursor disposition.
type ScopeCursorObservation struct {
	Scope       ScopeID
	Disposition CursorDisposition
}

// ScopeChecksumObservation contains one ordered optional scope checksum.
type ScopeChecksumObservation struct {
	Scope       ScopeID
	HasChecksum bool
	Checksum    Checksum
}

// PullObservation contains one pull merge result.
type PullObservation struct {
	Changes        []PullChangeObservation
	ScopeCursors   []ScopeCursorObservation
	AddedScopes    []ScopeID
	RemovedScopes  []ScopeID
	RebuildScopes  []ScopeID
	HasMore        bool
	ScopeChecksums []ScopeChecksumObservation
}

// RebuildRecordObservation contains one ordered rebuild record.
type RebuildRecordObservation struct {
	Row         RowIdentity
	Version     RowVersion
	Deleted     bool
	HasChecksum bool
	Checksum    Checksum
}

// RebuildObservation contains one rebuild page or final result.
type RebuildObservation struct {
	Attempt         RebuildKey
	PageOrdinal     uint64
	Replayed        bool
	Restarted       bool
	Records         []RebuildRecordObservation
	HasContinuation bool
	Continuation    OpaqueToken
	HasFinalCursor  bool
	FinalCursor     OpaqueToken
	HasChecksum     bool
	Checksum        Checksum
}

// WALPoisonState identifies the observed poison state.
type WALPoisonState string

const (
	WALPoisonStateClear    WALPoisonState = "clear"
	WALPoisonStatePoisoned WALPoisonState = "poisoned"
	WALPoisonStateRepaired WALPoisonState = "repaired"
)

// WALObservation contains one transaction materialization result.
type WALObservation struct {
	Transaction          TransactionReplayKey
	RegistryGeneration   Generation
	PriorMaterialization StreamPosition
	NewMaterialization   StreamPosition
	PriorAcknowledgement EndLSN
	NewAcknowledgement   EndLSN
	AffectedScopes       []ScopeID
	Poison               WALPoisonState
}

// SchemaObservation contains one schema decision.
type SchemaObservation struct {
	Source         SchemaRef
	Target         SchemaRef
	Action         SchemaAction
	Reason         ReasonCode
	AffectedScopes []ScopeID
	BatchSize      uint64
	BatchCount     uint64
}

// RetentionObservation contains one scope retention transition.
type RetentionObservation struct {
	Scope        ScopeID
	PriorFloor   RetentionFloor
	NewFloor     RetentionFloor
	BatchSize    uint64
	DeletedCount uint64
	Pinned       bool
}

// ClientObservation contains client generation and scope-set changes.
type ClientObservation struct {
	Client               ClientKey
	PriorGeneration      Generation
	NewGeneration        Generation
	PriorScopeSetVersion ScopeSetVersion
	NewScopeSetVersion   ScopeSetVersion
}

// StepResult contains one closed typed operation observation.
type StepResult struct {
	Kind      StepResultKind
	HTTP      *HTTPObservation
	Connect   *ConnectObservation
	Local     *LocalObservation
	Lifecycle *LifecycleObservation
	Push      *PushObservation
	Pull      *PullObservation
	Rebuild   *RebuildObservation
	WAL       *WALObservation
	Schema    *SchemaObservation
	Retention *RetentionObservation
	Client    *ClientObservation
}

var operationHandlers = map[string]OperationHandler{
	"model/install-current-contract": withoutResolvedInput(installCurrentContract),
}

func registerOperation(key string, handler operationImplementation) {
	registerResolvedOperation(key, withoutResolvedInput(handler))
}

func registerResolvedOperation(key string, handler OperationHandler) {
	if key == "" || handler == nil {
		panic("reference operation registration requires a key and handler")
	}
	if _, exists := operationHandlers[key]; exists {
		panic("reference operation registered more than once: " + key)
	}
	operationHandlers[key] = handler
}

func withoutResolvedInput(handler operationImplementation) OperationHandler {
	return func(ctx context.Context, model *Model, payload json.RawMessage, _ ResolvedOperationInput) (StepResult, error) {
		return handler(ctx, model, payload)
	}
}

// OperationRegistry returns a defensive copy of the closed operation registry.
func OperationRegistry() map[string]OperationHandler {
	result := make(map[string]OperationHandler, len(operationHandlers))
	for key, handler := range operationHandlers {
		result[key] = handler
	}
	return result
}

func decodeStrictPayload[T interface{}](payload json.RawMessage, destination *T) error {
	if err := jsonstrict.ValidateValue(payload); err != nil {
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode typed JSON object: %w", err)
	}
	var trailing struct{}
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON document contains more than one value")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return nil
}

func validateStepResult(result StepResult) error {
	domainCount := 0
	for _, present := range []bool{
		result.Connect != nil,
		result.Local != nil,
		result.Lifecycle != nil,
		result.Push != nil,
		result.Pull != nil,
		result.Rebuild != nil,
		result.WAL != nil,
		result.Schema != nil,
		result.Retention != nil,
		result.Client != nil,
	} {
		if present {
			domainCount++
		}
	}

	switch result.Kind {
	case StepResultKindContractInstalled:
		if result.HTTP != nil || domainCount != 0 {
			return errors.New("contract-installed result contains an observation")
		}
	case StepResultKindConnect:
		if result.HTTP == nil || result.Connect == nil || domainCount != 1 {
			return errors.New("connect result has an invalid observation combination")
		}
	case StepResultKindLocal:
		if result.HTTP != nil || result.Local == nil || domainCount != 1 {
			return errors.New("local result has an invalid observation combination")
		}
	case StepResultKindLifecycle:
		if result.HTTP != nil || result.Lifecycle == nil || domainCount != 1 {
			return errors.New("lifecycle result has an invalid observation combination")
		}
	case StepResultKindPush:
		if result.HTTP == nil || result.Push == nil || domainCount != 1 {
			return errors.New("push result has an invalid observation combination")
		}
	case StepResultKindPull:
		if result.HTTP == nil || result.Pull == nil || domainCount != 1 {
			return errors.New("pull result has an invalid observation combination")
		}
	case StepResultKindRebuild:
		if result.HTTP == nil || result.Rebuild == nil || domainCount != 1 {
			return errors.New("rebuild result has an invalid observation combination")
		}
	case StepResultKindWAL:
		if result.HTTP != nil || result.WAL == nil || domainCount != 1 {
			return errors.New("WAL result has an invalid observation combination")
		}
	case StepResultKindSchema:
		if result.HTTP != nil || result.Schema == nil || domainCount != 1 {
			return errors.New("schema result has an invalid observation combination")
		}
	case StepResultKindRetention:
		if result.HTTP != nil || result.Retention == nil || domainCount != 1 {
			return errors.New("retention result has an invalid observation combination")
		}
	case StepResultKindClient:
		if result.HTTP != nil || result.Client == nil || domainCount != 1 {
			return errors.New("client result has an invalid observation combination")
		}
	default:
		return errors.New("operation result has an unknown kind")
	}
	return validateHTTPObservation(result.HTTP)
}

func validateHTTPObservation(observation *HTTPObservation) error {
	if observation == nil {
		return nil
	}
	if observation.HasCode != (observation.Code != "") {
		return errors.New("HTTP result has inconsistent code presence")
	}
	if !observation.HasRetryAfterMilliseconds && observation.RetryAfterMilliseconds != 0 {
		return errors.New("HTTP result has a hidden retry-after value")
	}
	return nil
}
