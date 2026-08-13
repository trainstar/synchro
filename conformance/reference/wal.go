package reference

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"time"
)

const walMaximumSafeInteger = uint64(9007199254740991)

const walTruncateOperation = "truncate"

var walFailureClasses = map[ReasonCode]struct{}{
	"application_failed":        {},
	"decode_failed":             {},
	"fence_correlation_failed":  {},
	"materialization_failed":    {},
	"projection_write_failed":   {},
	"scope_evaluation_failed":   {},
	"transaction_commit_failed": {},
	"validation_failed":         {},
}

type walTransactionKeyPayload struct {
	StreamGeneration StreamGeneration `json:"stream_generation"`
	CommitLSN        string           `json:"commit_lsn"`
}

type walCommitSourceTransactionPayload struct {
	StreamGeneration StreamGeneration        `json:"stream_generation"`
	CommitLSN        string                  `json:"commit_lsn"`
	EndLSN           string                  `json:"end_lsn"`
	Events           []walSourceEventPayload `json:"events"`
}

type walSourceEventPayload struct {
	EventOrdinal uint64                            `json:"event_ordinal"`
	Relation     RelationID                        `json:"relation"`
	Operation    string                            `json:"operation"`
	Before       walNullableRegisteredImagePayload `json:"before"`
	After        walNullableRegisteredImagePayload `json:"after"`
}

type walNullableRegisteredImagePayload struct {
	Set   bool                      `json:"-"`
	Valid bool                      `json:"-"`
	Value walRegisteredImagePayload `json:"-"`
}

type walRegisteredImagePayload struct {
	Identity walRegisteredIdentityPayload `json:"identity"`
	Fields   []walFieldValuePayload       `json:"fields"`
	Version  RowVersion                   `json:"version"`
	Checksum walNullableChecksumPayload   `json:"checksum"`
	Deleted  bool                         `json:"deleted"`
}

type walRegisteredIdentityPayload struct {
	Kind       RegistrationKind       `json:"kind"`
	SyncedRow  *walRowIdentityPayload `json:"synced_row"`
	CaptureKey *walCaptureKeyPayload  `json:"capture_key"`
}

type walRowIdentityPayload struct {
	CanonicalIdentityBytes string       `json:"canonical_identity_bytes"`
	TableID                TableID      `json:"table_id"`
	PrimaryKeyFieldID      FieldID      `json:"primary_key_field_id"`
	PortableType           PortableType `json:"portable_type"`
	CanonicalWireJSON      string       `json:"canonical_wire_json"`
}

type walCaptureKeyPayload struct {
	CanonicalKeyBytes string `json:"canonical_key_bytes"`
}

type walFieldValuePayload struct {
	Field    FieldID      `json:"field"`
	Type     PortableType `json:"type"`
	WireJSON string       `json:"wire_json"`
}

type walNullableChecksumPayload struct {
	Set   bool     `json:"-"`
	Valid bool     `json:"-"`
	Value Checksum `json:"-"`
}

type walMaterializeSourceTransactionPayload struct {
	StreamGeneration StreamGeneration `json:"stream_generation"`
	CommitLSN        string           `json:"commit_lsn"`
	FailureClass     *ReasonCode      `json:"failure_class"`
}

type walAcknowledgeContiguousPrefixPayload struct {
	StreamGeneration StreamGeneration `json:"stream_generation"`
}

type walRestartWorkerPayload struct {
	WorkerID WorkerID `json:"worker_id"`
}

type walMaterializationFailure struct {
	reason      ReasonCode
	hasRelation bool
	relation    RelationID
	detail      string
}

type walPendingEffect struct {
	scope         ScopeID
	row           RowIdentity
	source        EventReplayKey
	operation     EffectOperation
	version       RowVersion
	hasProjection bool
	projection    ProjectionKey
	hasChecksum   bool
	checksum      Checksum
}

func init() {
	registerOperation("model/commit-source-transaction", walCommitSourceTransaction)
	registerOperation("process/materialize-source-transaction", walMaterializeSourceTransaction)
	registerOperation("process/repair-and-retry-source-transaction", walRepairAndRetrySourceTransaction)
	registerOperation("process/acknowledge-contiguous-prefix", walAcknowledgeContiguousPrefix)
	registerOperation("process/restart-wal-worker", walRestartWorker)
}

func (payload *walNullableRegisteredImagePayload) UnmarshalJSON(data []byte) error {
	payload.Set = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		payload.Valid = false
		payload.Value = walRegisteredImagePayload{}
		return nil
	}
	var value walRegisteredImagePayload
	if err := decodeStrictPayload(json.RawMessage(data), &value); err != nil {
		return err
	}
	payload.Valid = true
	payload.Value = value
	return nil
}

func (payload walNullableRegisteredImagePayload) MarshalJSON() ([]byte, error) {
	if !payload.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(payload.Value)
}

func (payload *walNullableChecksumPayload) UnmarshalJSON(data []byte) error {
	payload.Set = true
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) {
		payload.Valid = false
		payload.Value = Checksum{}
		return nil
	}
	var encoded string
	if err := json.Unmarshal(trimmed, &encoded); err != nil {
		return errors.New("checksum must be null or a lowercase hexadecimal string")
	}
	if len(encoded) != sha256.Size*2 || encoded != string(bytes.ToLower([]byte(encoded))) {
		return errors.New("checksum must contain 64 lowercase hexadecimal characters")
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return errors.New("checksum must contain 64 lowercase hexadecimal characters")
	}
	copy(payload.Value[:], decoded)
	payload.Valid = true
	return nil
}

func (payload walNullableChecksumPayload) MarshalJSON() ([]byte, error) {
	if !payload.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(hex.EncodeToString(payload.Value[:]))
}

func (failure *walMaterializationFailure) Error() string {
	return failure.detail
}

func walCommitSourceTransaction(ctx context.Context, model *Model, raw json.RawMessage) (StepResult, error) {
	var payload walCommitSourceTransactionPayload
	if err := decodeStrictPayload(raw, &payload); err != nil {
		return StepResult{}, fmt.Errorf("decode commit-source-transaction payload: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return StepResult{}, err
	}

	key, err := walDecodeTransactionKey(payload.StreamGeneration, payload.CommitLSN)
	if err != nil {
		return StepResult{}, err
	}
	existing, replay := walFindTransaction(model.state.Stream.Transactions, key)
	var retainedGeneration *Generation
	if replay {
		retainedGeneration = &existing.RegistryGeneration
	}
	transaction, err := walDecodeCommittedTransaction(model.state, payload, retainedGeneration)
	if err != nil {
		return StepResult{}, err
	}
	if replay {
		if !walEqualCommittedTransactions(existing, transaction) {
			return StepResult{}, errors.New("source transaction replay identity has different committed content")
		}
		return walStepResult(model.state, existing.ReplayKey, existing.RegistryGeneration, nil, walCurrentPoisonState(model.state)), nil
	}
	if err := walValidateEndLSNOrder(model.state.Stream.Transactions, transaction); err != nil {
		return StepResult{}, err
	}

	now := model.clock.Now()
	transaction.CommittedAt = &now
	for index := range transaction.Events {
		transaction.Events[index].CapturedAt = &now
	}
	model.state.Stream.Transactions = append(model.state.Stream.Transactions, transaction)
	walSortTransactions(model.state.Stream.Transactions)
	if err := walCreateCommittedFences(&model.state, transaction); err != nil {
		return StepResult{}, err
	}
	walRefreshLiveSourceRows(&model.state, transaction)

	return walStepResult(model.state, transaction.ReplayKey, transaction.RegistryGeneration, nil, walCurrentPoisonState(model.state)), nil
}

func walMaterializeSourceTransaction(ctx context.Context, model *Model, raw json.RawMessage) (StepResult, error) {
	var payload walMaterializeSourceTransactionPayload
	if err := decodeStrictPayload(raw, &payload); err != nil {
		return StepResult{}, fmt.Errorf("decode materialize-source-transaction payload: %w", err)
	}
	key, err := walDecodeTransactionKey(payload.StreamGeneration, payload.CommitLSN)
	if err != nil {
		return StepResult{}, err
	}
	if payload.FailureClass != nil {
		if _, valid := walFailureClasses[*payload.FailureClass]; !valid {
			return StepResult{}, fmt.Errorf("failure_class %q is not bounded", *payload.FailureClass)
		}
	}
	return walRunMaterialization(ctx, model, key, payload.FailureClass, false)
}

func walRepairAndRetrySourceTransaction(ctx context.Context, model *Model, raw json.RawMessage) (StepResult, error) {
	var payload walTransactionKeyPayload
	if err := decodeStrictPayload(raw, &payload); err != nil {
		return StepResult{}, fmt.Errorf("decode repair-and-retry-source-transaction payload: %w", err)
	}
	key, err := walDecodeTransactionKey(payload.StreamGeneration, payload.CommitLSN)
	if err != nil {
		return StepResult{}, err
	}
	poison, found := walFindActivePoison(model.state, key)
	if !found {
		return StepResult{}, errors.New("repair requires active poison for the same source transaction")
	}
	if poison.Reason == "truncate_unsupported" {
		return StepResult{}, errors.New("truncate poison requires an authorized stream reset")
	}
	return walRunMaterialization(ctx, model, key, nil, true)
}

func walRunMaterialization(ctx context.Context, model *Model, key TransactionReplayKey, injected *ReasonCode, repair bool) (StepResult, error) {
	if err := ctx.Err(); err != nil {
		return StepResult{}, err
	}
	transaction, found := walFindTransaction(model.state.Stream.Transactions, key)
	if !found {
		return StepResult{}, errors.New("source transaction is not committed")
	}
	priorMaterialization := model.state.Stream.Authority.GlobalMaterializationBoundary
	priorAcknowledgement := model.state.Stream.Authority.AcknowledgedEndLSN

	if walTransactionCompleted(model.state, key) {
		if repair {
			return StepResult{}, errors.New("completed source transaction has no repair work")
		}
		walMarkReplay(&model.state, key)
		return walStepResultWithPrior(model.state, transaction, priorMaterialization, priorAcknowledgement, nil, walCurrentPoisonState(model.state)), nil
	}
	if err := walRequireNextTransaction(model.state, key, repair); err != nil {
		return StepResult{}, err
	}

	if injected != nil {
		now := model.clock.Now()
		failure := walMaterializationFailure{
			reason:      *injected,
			hasRelation: len(transaction.Events) != 0,
			detail:      "injected bounded materialization failure",
		}
		if failure.hasRelation {
			failure.relation = transaction.Events[0].Relation
		}
		walPoisonTransaction(&model.state, transaction, failure, &now)
		return walStepResultWithPrior(model.state, transaction, priorMaterialization, priorAcknowledgement, nil, WALPoisonStatePoisoned), nil
	}
	for _, event := range transaction.Events {
		if string(event.Operation) == walTruncateOperation {
			now := model.clock.Now()
			failure := walMaterializationFailure{
				reason:      "truncate_unsupported",
				hasRelation: true,
				relation:    event.Relation,
				detail:      "registered relation truncate is unsupported",
			}
			walPoisonTransaction(&model.state, transaction, failure, &now)
			return walStepResultWithPrior(model.state, transaction, priorMaterialization, priorAcknowledgement, nil, WALPoisonStatePoisoned), nil
		}
	}

	staged := cloneState(model.state)
	now := model.clock.Now()
	affectedScopes, materializeErr := walMaterializeTransaction(&staged, key, &now)
	if materializeErr != nil {
		var failure *walMaterializationFailure
		if !errors.As(materializeErr, &failure) {
			return StepResult{}, materializeErr
		}
		walPoisonTransaction(&model.state, transaction, *failure, &now)
		return walStepResultWithPrior(model.state, transaction, priorMaterialization, priorAcknowledgement, nil, WALPoisonStatePoisoned), nil
	}
	if repair {
		walMarkReplay(&staged, key)
		walMarkPoisonRepaired(&staged, key, &now)
	}
	model.state = staged
	poisonState := WALPoisonStateClear
	if repair {
		poisonState = WALPoisonStateRepaired
	}
	return walStepResultWithPrior(model.state, transaction, priorMaterialization, priorAcknowledgement, affectedScopes, poisonState), nil
}

func walAcknowledgeContiguousPrefix(ctx context.Context, model *Model, raw json.RawMessage) (StepResult, error) {
	var payload walAcknowledgeContiguousPrefixPayload
	if err := decodeStrictPayload(raw, &payload); err != nil {
		return StepResult{}, fmt.Errorf("decode acknowledge-contiguous-prefix payload: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return StepResult{}, err
	}
	if payload.StreamGeneration == "" || payload.StreamGeneration != model.state.Stream.Authority.ActiveGeneration {
		return StepResult{}, errors.New("acknowledgement requires the active stream generation")
	}

	priorMaterialization := model.state.Stream.Authority.GlobalMaterializationBoundary
	priorAcknowledgement := model.state.Stream.Authority.AcknowledgedEndLSN
	transactions := append([]StreamTransaction(nil), model.state.Stream.Transactions...)
	walSortTransactions(transactions)
	newAcknowledgement := priorAcknowledgement
	last := StreamTransaction{ReplayKey: TransactionReplayKey{StreamGeneration: payload.StreamGeneration}}
	for _, transaction := range transactions {
		if transaction.ReplayKey.StreamGeneration != payload.StreamGeneration || transaction.EndLSN <= priorAcknowledgement {
			continue
		}
		if !walTransactionCompleted(model.state, transaction.ReplayKey) {
			break
		}
		newAcknowledgement = transaction.EndLSN
		last = transaction
	}
	if newAcknowledgement > priorAcknowledgement {
		now := model.clock.Now()
		model.state.Stream.Authority.AcknowledgedEndLSN = newAcknowledgement
		model.state.Stream.Acknowledgements = append(model.state.Stream.Acknowledgements, SlotAcknowledgement{
			StreamGeneration: payload.StreamGeneration,
			EndLSN:           newAcknowledgement,
			AcknowledgedAt:   &now,
		})
		for index := range model.state.Readiness.Slots {
			slot := &model.state.Readiness.Slots[index]
			if model.state.Stream.Authority.HasActiveSlot && slot.ID == model.state.Stream.Authority.ActiveSlot {
				slot.AcknowledgedEndLSN = newAcknowledgement
			}
		}
	}
	registryGeneration := model.state.Registry.CurrentGeneration
	if last.RegistryGeneration != 0 {
		registryGeneration = last.RegistryGeneration
	}
	result := walStepResult(model.state, last.ReplayKey, registryGeneration, nil, walCurrentPoisonState(model.state))
	result.WAL.PriorMaterialization = priorMaterialization
	result.WAL.NewMaterialization = model.state.Stream.Authority.GlobalMaterializationBoundary
	result.WAL.PriorAcknowledgement = priorAcknowledgement
	result.WAL.NewAcknowledgement = newAcknowledgement
	return result, nil
}

func walRestartWorker(ctx context.Context, model *Model, raw json.RawMessage) (StepResult, error) {
	var payload walRestartWorkerPayload
	if err := decodeStrictPayload(raw, &payload); err != nil {
		return StepResult{}, fmt.Errorf("decode restart-wal-worker payload: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return StepResult{}, err
	}
	if payload.WorkerID == "" {
		return StepResult{}, errors.New("worker_id is required")
	}

	now := model.clock.Now()
	found := false
	for index := range model.state.Readiness.Workers {
		worker := &model.state.Readiness.Workers[index]
		if worker.ID != payload.WorkerID {
			continue
		}
		worker.Running = true
		worker.HeartbeatAt = &now
		found = true
		break
	}
	if !found {
		model.state.Readiness.Workers = append(model.state.Readiness.Workers, WorkerReadiness{
			ID:                   payload.WorkerID,
			Database:             model.state.Readiness.ConfiguredDatabase,
			Running:              true,
			HeartbeatAt:          &now,
			RegistryGeneration:   model.state.Registry.CurrentGeneration,
			MaterializedPosition: model.state.Stream.Authority.GlobalMaterializationBoundary,
		})
	}
	event := ModelEvent{
		Ordinal: walNextEventOrdinal(model.state.Events),
		Kind:    ModelEventWorkerRestart,
		At:      &now,
		Reason:  "worker_restart",
	}
	if poison, active := walEarliestActivePoison(model.state); active {
		event.HasTransaction = true
		event.Transaction = poison.Transaction
	}
	model.state.Events = append(model.state.Events, event)

	key := TransactionReplayKey{StreamGeneration: model.state.Stream.Authority.ActiveGeneration}
	if event.HasTransaction {
		key = event.Transaction
	}
	return walStepResult(model.state, key, model.state.Registry.CurrentGeneration, nil, walCurrentPoisonState(model.state)), nil
}

func walDecodeCommittedTransaction(state State, payload walCommitSourceTransactionPayload, retainedGeneration *Generation) (StreamTransaction, error) {
	key, err := walDecodeTransactionKey(payload.StreamGeneration, payload.CommitLSN)
	if err != nil {
		return StreamTransaction{}, err
	}
	if key.StreamGeneration != state.Stream.Authority.ActiveGeneration {
		return StreamTransaction{}, errors.New("source transaction uses a nonactive stream generation")
	}
	endLSN, err := walParseCanonicalUnsigned(payload.EndLSN, "end_lsn")
	if err != nil {
		return StreamTransaction{}, err
	}
	if endLSN < uint64(key.CommitLSN) {
		return StreamTransaction{}, errors.New("end_lsn precedes commit_lsn")
	}
	if payload.Events == nil {
		return StreamTransaction{}, errors.New("events must be present, including for an empty transaction")
	}

	var generation RegistryGenerationState
	if retainedGeneration == nil {
		generation, err = walSelectRegistryGeneration(state.Registry, key)
		if err != nil {
			return StreamTransaction{}, err
		}
	} else {
		var found bool
		generation, found = walRegistryGeneration(state.Registry, *retainedGeneration)
		if !found || !generation.Validated {
			return StreamTransaction{}, errors.New("retained registry generation is unavailable for source transaction replay")
		}
	}
	events := append([]walSourceEventPayload(nil), payload.Events...)
	sort.Slice(events, func(left, right int) bool {
		return events[left].EventOrdinal < events[right].EventOrdinal
	})
	decoded := make([]SourceEvent, 0, len(events))
	for index, event := range events {
		if event.EventOrdinal > walMaximumSafeInteger {
			return StreamTransaction{}, errors.New("event_ordinal exceeds the portable safe-integer range")
		}
		if index != 0 && events[index-1].EventOrdinal == event.EventOrdinal {
			return StreamTransaction{}, errors.New("source transaction contains a duplicate event_ordinal")
		}
		definition, found := walRegistryRelation(generation, event.Relation)
		if !found {
			return StreamTransaction{}, fmt.Errorf("event relation %q is not registered in the selected generation", event.Relation)
		}
		decodedEvent, err := walDecodeSourceEvent(key, event, definition)
		if err != nil {
			return StreamTransaction{}, fmt.Errorf("decode event ordinal %d: %w", event.EventOrdinal, err)
		}
		decoded = append(decoded, decodedEvent)
	}

	return StreamTransaction{
		ReplayKey:          key,
		End:                StreamPosition{StreamGeneration: key.StreamGeneration, Kind: PositionKindTransactionEnd, CommitLSN: key.CommitLSN},
		EndLSN:             EndLSN(endLSN),
		RegistryGeneration: generation.Generation,
		Lifecycle:          TransactionLifecycleCommitted,
		Events:             decoded,
	}, nil
}

func walDecodeSourceEvent(key TransactionReplayKey, payload walSourceEventPayload, definition RelationDefinition) (SourceEvent, error) {
	if payload.Relation == "" {
		return SourceEvent{}, errors.New("relation is required")
	}
	if !payload.Before.Set || !payload.After.Set {
		return SourceEvent{}, errors.New("before and after must be explicitly present")
	}

	switch payload.Operation {
	case string(DMLOperationInsert):
		if payload.Before.Valid || !payload.After.Valid {
			return SourceEvent{}, errors.New("insert requires null before and nonnull after")
		}
	case string(DMLOperationUpdate):
		if !payload.Before.Valid || !payload.After.Valid {
			return SourceEvent{}, errors.New("update requires nonnull before and after")
		}
	case string(DMLOperationDelete):
		if !payload.Before.Valid || payload.After.Valid {
			return SourceEvent{}, errors.New("delete requires nonnull before and null after")
		}
	case walTruncateOperation:
		if payload.Before.Valid || payload.After.Valid {
			return SourceEvent{}, errors.New("truncate requires null before and after")
		}
	default:
		return SourceEvent{}, fmt.Errorf("operation %q is not supported", payload.Operation)
	}

	event := SourceEvent{
		ReplayKey: EventReplayKey{Transaction: key, EventOrdinal: EventOrdinal(payload.EventOrdinal)},
		Position: StreamPosition{
			StreamGeneration: key.StreamGeneration,
			Kind:             PositionKindEffect,
			CommitLSN:        key.CommitLSN,
			EventOrdinal:     EventOrdinal(payload.EventOrdinal),
		},
		Relation:  payload.Relation,
		Operation: DMLOperation(payload.Operation),
		HasBefore: payload.Before.Valid,
		HasAfter:  payload.After.Valid,
	}
	var err error
	if payload.Before.Valid {
		event.Before, err = walDecodeRegisteredImage(payload.Before.Value, definition)
		if err != nil {
			return SourceEvent{}, fmt.Errorf("before image: %w", err)
		}
	}
	if payload.After.Valid {
		event.After, err = walDecodeRegisteredImage(payload.After.Value, definition)
		if err != nil {
			return SourceEvent{}, fmt.Errorf("after image: %w", err)
		}
	}
	if event.HasBefore && event.HasAfter && event.Before.Identity != event.After.Identity {
		return SourceEvent{}, errors.New("update changes the registered identity")
	}
	if payload.Operation == string(DMLOperationInsert) && event.After.Deleted {
		return SourceEvent{}, errors.New("insert after image cannot be deleted")
	}
	return event, nil
}

func walDecodeRegisteredImage(payload walRegisteredImagePayload, definition RelationDefinition) (SourceImage, error) {
	identity, err := walDecodeRegisteredIdentity(payload.Identity)
	if err != nil {
		return SourceImage{}, err
	}
	if identity.Kind != definition.RegistrationKind {
		return SourceImage{}, errors.New("registered image kind differs from its relation registration")
	}
	if identity.Kind == RegistrationKindSynced {
		if !definition.HasTableID || identity.SyncedRow.TableID != definition.TableID {
			return SourceImage{}, errors.New("synced image uses the wrong logical table")
		}
		if identity.SyncedRow.PrimaryKeyFieldID != definition.PrimaryKeyFieldID || identity.SyncedRow.PortableType != definition.PrimaryKeyPortableType {
			return SourceImage{}, errors.New("synced image uses the wrong primary-key contract")
		}
	}
	if payload.Fields == nil {
		return SourceImage{}, errors.New("registered image fields must be present")
	}
	fields := make([]FieldValue, 0, len(payload.Fields))
	seen := make(map[FieldID]struct{}, len(payload.Fields))
	for _, field := range payload.Fields {
		if field.Field == "" || field.Type == "" || field.WireJSON == "" || !json.Valid([]byte(field.WireJSON)) {
			return SourceImage{}, errors.New("registered image contains an invalid field")
		}
		if _, duplicate := seen[field.Field]; duplicate {
			return SourceImage{}, errors.New("registered image contains a duplicate field")
		}
		seen[field.Field] = struct{}{}
		fields = append(fields, FieldValue{Field: field.Field, Type: field.Type, WireJSON: field.WireJSON})
	}
	if definition.CapturedFieldIDs != nil {
		if len(seen) != len(definition.CapturedFieldIDs) {
			return SourceImage{}, errors.New("registered image field set differs from the captured field set")
		}
		for _, field := range definition.CapturedFieldIDs {
			if _, found := seen[field]; !found {
				return SourceImage{}, errors.New("registered image omits a captured field")
			}
		}
	}
	if payload.Version == "" {
		return SourceImage{}, errors.New("registered image version is required")
	}
	if !payload.Checksum.Set {
		return SourceImage{}, errors.New("registered image checksum must be explicitly present")
	}
	if definition.RegistrationKind == RegistrationKindSynced && !payload.Checksum.Valid {
		return SourceImage{}, errors.New("synced image requires a checksum")
	}
	if definition.RegistrationKind == RegistrationKindCaptureDependency && payload.Checksum.Valid {
		return SourceImage{}, errors.New("capture-dependency image cannot contain a row checksum")
	}
	return SourceImage{
		Identity:    identity,
		Fields:      fields,
		Version:     payload.Version,
		HasChecksum: payload.Checksum.Valid,
		Checksum:    payload.Checksum.Value,
		Deleted:     payload.Deleted,
	}, nil
}

func walDecodeRegisteredIdentity(payload walRegisteredIdentityPayload) (RegisteredIdentity, error) {
	switch payload.Kind {
	case RegistrationKindSynced:
		if payload.SyncedRow == nil || payload.CaptureKey != nil {
			return RegisteredIdentity{}, errors.New("synced identity requires only synced_row")
		}
		row, err := walDecodeRowIdentity(*payload.SyncedRow)
		if err != nil {
			return RegisteredIdentity{}, err
		}
		return RegisteredIdentity{Kind: RegistrationKindSynced, SyncedRow: row}, nil
	case RegistrationKindCaptureDependency:
		if payload.CaptureKey == nil || payload.SyncedRow != nil || payload.CaptureKey.CanonicalKeyBytes == "" {
			return RegisteredIdentity{}, errors.New("capture-dependency identity requires only capture_key")
		}
		return RegisteredIdentity{
			Kind:       RegistrationKindCaptureDependency,
			CaptureKey: CanonicalCaptureKey{CanonicalKeyBytes: payload.CaptureKey.CanonicalKeyBytes},
		}, nil
	default:
		return RegisteredIdentity{}, errors.New("registered identity has an unknown kind")
	}
}

func walDecodeRowIdentity(payload walRowIdentityPayload) (RowIdentity, error) {
	row := RowIdentity{
		CanonicalIdentityBytes: payload.CanonicalIdentityBytes,
		TableID:                payload.TableID,
		PrimaryKeyFieldID:      payload.PrimaryKeyFieldID,
		PortableType:           payload.PortableType,
		CanonicalWireJSON:      payload.CanonicalWireJSON,
	}
	if row.CanonicalIdentityBytes == "" || row.TableID == "" || row.PrimaryKeyFieldID == "" || row.CanonicalWireJSON == "" {
		return RowIdentity{}, errors.New("row identity is incomplete")
	}
	switch row.PortableType {
	case "string", "int", "int64":
	default:
		return RowIdentity{}, errors.New("row identity has an unsupported portable type")
	}
	if !json.Valid([]byte(row.CanonicalWireJSON)) {
		return RowIdentity{}, errors.New("row identity has invalid canonical wire JSON")
	}
	return row, nil
}

func walDecodeTransactionKey(streamGeneration StreamGeneration, encodedCommitLSN string) (TransactionReplayKey, error) {
	if streamGeneration == "" {
		return TransactionReplayKey{}, errors.New("stream_generation is required")
	}
	commitLSN, err := walParseCanonicalUnsigned(encodedCommitLSN, "commit_lsn")
	if err != nil {
		return TransactionReplayKey{}, err
	}
	return TransactionReplayKey{StreamGeneration: streamGeneration, CommitLSN: CommitLSN(commitLSN)}, nil
}

func walParseCanonicalUnsigned(value, field string) (uint64, error) {
	if value == "" {
		return 0, fmt.Errorf("%s is required", field)
	}
	if len(value) > 1 && value[0] == '0' {
		return 0, fmt.Errorf("%s is not canonical unsigned decimal", field)
	}
	for _, character := range []byte(value) {
		if character < '0' || character > '9' {
			return 0, fmt.Errorf("%s is not canonical unsigned decimal", field)
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is outside the unsigned 64-bit range", field)
	}
	return parsed, nil
}

func walSelectRegistryGeneration(registry RegistryState, key TransactionReplayKey) (RegistryGenerationState, error) {
	var selected RegistryGenerationState
	found := false
	for _, generation := range registry.Generations {
		if !generation.Validated || generation.HasBootstrapStage || !walActivationPrecedesTransaction(generation.ActivationBoundary, key) {
			continue
		}
		if !found || lessStreamPosition(selected.ActivationBoundary, generation.ActivationBoundary) || selected.ActivationBoundary == generation.ActivationBoundary && generation.Generation > selected.Generation {
			selected = generation
			found = true
		}
	}
	if !found {
		return RegistryGenerationState{}, errors.New("no validated registry generation precedes the source transaction")
	}
	return selected, nil
}

func walActivationPrecedesTransaction(boundary StreamPosition, key TransactionReplayKey) bool {
	if boundary.StreamGeneration != key.StreamGeneration {
		return false
	}
	switch boundary.Kind {
	case PositionKindGenerationStart:
		return true
	case PositionKindTransactionEnd:
		return boundary.CommitLSN < key.CommitLSN
	default:
		return false
	}
}

func walRegistryRelation(generation RegistryGenerationState, relation RelationID) (RelationDefinition, bool) {
	for _, registered := range generation.Relations {
		if registered.Definition.Relation == relation {
			return registered.Definition, true
		}
	}
	return RelationDefinition{}, false
}

func walRegistryGeneration(registry RegistryState, generation Generation) (RegistryGenerationState, bool) {
	for _, candidate := range registry.Generations {
		if candidate.Generation == generation {
			return candidate, true
		}
	}
	return RegistryGenerationState{}, false
}

func walFindTransaction(transactions []StreamTransaction, key TransactionReplayKey) (StreamTransaction, bool) {
	for _, transaction := range transactions {
		if transaction.ReplayKey == key {
			return transaction, true
		}
	}
	return StreamTransaction{}, false
}

func walFindTransactionIndex(transactions []StreamTransaction, key TransactionReplayKey) int {
	for index := range transactions {
		if transactions[index].ReplayKey == key {
			return index
		}
	}
	return -1
}

func walEqualCommittedTransactions(left, right StreamTransaction) bool {
	if left.ReplayKey != right.ReplayKey || left.End != right.End || left.EndLSN != right.EndLSN || left.RegistryGeneration != right.RegistryGeneration || len(left.Events) != len(right.Events) {
		return false
	}
	for index := range left.Events {
		leftEvent := left.Events[index]
		rightEvent := right.Events[index]
		leftEvent.CapturedAt = nil
		rightEvent.CapturedAt = nil
		if !walEqualSourceEvents(leftEvent, rightEvent) {
			return false
		}
	}
	return true
}

func walEqualSourceEvents(left, right SourceEvent) bool {
	left.CapturedAt = nil
	right.CapturedAt = nil
	return reflect.DeepEqual(left, right)
}

func walValidateEndLSNOrder(transactions []StreamTransaction, candidate StreamTransaction) error {
	for _, existing := range transactions {
		if existing.ReplayKey.StreamGeneration != candidate.ReplayKey.StreamGeneration {
			continue
		}
		if existing.ReplayKey.CommitLSN < candidate.ReplayKey.CommitLSN && existing.EndLSN >= candidate.EndLSN {
			return errors.New("end_lsn does not increase with commit_lsn")
		}
		if existing.ReplayKey.CommitLSN > candidate.ReplayKey.CommitLSN && existing.EndLSN <= candidate.EndLSN {
			return errors.New("end_lsn does not increase with commit_lsn")
		}
	}
	return nil
}

func walSortTransactions(transactions []StreamTransaction) {
	sort.Slice(transactions, func(left, right int) bool {
		return lessTransactionReplayKey(transactions[left].ReplayKey, transactions[right].ReplayKey)
	})
}

func walCreateCommittedFences(state *State, transaction StreamTransaction) error {
	dmlOrdinal := uint64(1)
	for _, event := range transaction.Events {
		if string(event.Operation) == walTruncateOperation {
			continue
		}
		generation, found := walRegistryGeneration(state.Registry, transaction.RegistryGeneration)
		if !found {
			return errors.New("selected registry generation disappeared")
		}
		definition, found := walRegistryRelation(generation, event.Relation)
		if !found {
			return errors.New("selected relation disappeared")
		}
		if _, found := walMatchingFence(*state, event, dmlOrdinal); !found {
			fence := walFenceFromEvent(definition, event, dmlOrdinal)
			state.Fences[fence.ID] = fence
		}
		dmlOrdinal++
	}
	return nil
}

func walFenceFromEvent(definition RelationDefinition, event SourceEvent, dmlOrdinal uint64) VersionFence {
	fence := VersionFence{
		ID:               FenceID(fmt.Sprintf("wal:%s:%d:%d", event.ReplayKey.Transaction.StreamGeneration, event.ReplayKey.Transaction.CommitLSN, event.ReplayKey.EventOrdinal)),
		RegistrationKind: definition.RegistrationKind,
		Relation:         event.Relation,
		Physical:         definition.Physical,
		Operation:        event.Operation,
		DMLOrdinal:       dmlOrdinal,
		Coverage:         FenceCoveragePending,
	}
	if event.HasBefore {
		fence.HasOldRegisteredIdentity = true
		fence.OldRegisteredIdentity = event.Before.Identity
	}
	if event.HasAfter {
		fence.HasNewRegisteredIdentity = true
		fence.NewRegisteredIdentity = event.After.Identity
		fence.RowVersion = event.After.Version
	} else if event.HasBefore {
		fence.RowVersion = event.Before.Version
	}
	return fence
}

func walMatchingFence(state State, event SourceEvent, dmlOrdinal uint64) (FenceID, bool) {
	matches := make([]FenceID, 0, 1)
	for id, fence := range state.Fences {
		if fence.Relation != event.Relation || fence.Operation != event.Operation || fence.HasEventReplayKey && fence.EventReplayKey != event.ReplayKey {
			continue
		}
		if fence.HasOldRegisteredIdentity != event.HasBefore || fence.HasNewRegisteredIdentity != event.HasAfter {
			continue
		}
		if event.HasBefore && fence.OldRegisteredIdentity != event.Before.Identity || event.HasAfter && fence.NewRegisteredIdentity != event.After.Identity {
			continue
		}
		if event.HasAfter && fence.RowVersion != event.After.Version {
			continue
		}
		if fence.DMLOrdinal != dmlOrdinal {
			continue
		}
		matches = append(matches, id)
	}
	if len(matches) != 1 {
		return "", false
	}
	return matches[0], true
}

func walRefreshLiveSourceRows(state *State, committed StreamTransaction) {
	impacted := make(map[RowIdentity]struct{})
	for _, event := range committed.Events {
		if event.HasBefore && event.Before.Identity.Kind == RegistrationKindSynced {
			impacted[event.Before.Identity.SyncedRow] = struct{}{}
		}
		if event.HasAfter && event.After.Identity.Kind == RegistrationKindSynced {
			impacted[event.After.Identity.SyncedRow] = struct{}{}
		}
	}
	for row := range impacted {
		latest, found := walLatestCommittedRowEvent(state.Stream.Transactions, row)
		if !found || !latest.HasAfter {
			walDeleteSourceRow(&state.Stream.SourceRows, row)
			continue
		}
		authoritative := walAuthoritativeRowFromImage(latest.After, latest.After.Deleted, latest.CapturedAt)
		walSetSourceRow(&state.Stream.SourceRows, authoritative)
	}
}

func walLatestCommittedRowEvent(transactions []StreamTransaction, row RowIdentity) (SourceEvent, bool) {
	var selected SourceEvent
	found := false
	for _, transaction := range transactions {
		for _, event := range transaction.Events {
			matches := event.HasBefore && event.Before.Identity.Kind == RegistrationKindSynced && event.Before.Identity.SyncedRow == row || event.HasAfter && event.After.Identity.Kind == RegistrationKindSynced && event.After.Identity.SyncedRow == row
			if !matches {
				continue
			}
			if !found || lessEventReplayKey(selected.ReplayKey, event.ReplayKey) {
				selected = event
				found = true
			}
		}
	}
	return selected, found
}

func walSetSourceRow(rows *[]SourceRowEntry, row AuthoritativeRow) {
	for index := range *rows {
		if (*rows)[index].Identity == row.Identity {
			(*rows)[index] = SourceRowEntry{Identity: row.Identity, Row: row}
			return
		}
	}
	*rows = append(*rows, SourceRowEntry{Identity: row.Identity, Row: row})
}

func walDeleteSourceRow(rows *[]SourceRowEntry, identity RowIdentity) {
	for index := range *rows {
		if (*rows)[index].Identity != identity {
			continue
		}
		*rows = append((*rows)[:index], (*rows)[index+1:]...)
		return
	}
}

func walRequireNextTransaction(state State, key TransactionReplayKey, repair bool) error {
	if poison, active := walEarliestActivePoison(state); active {
		if poison.Transaction != key {
			return sourceTransactionBlockedError{
				code:    "source_transaction_poison_blocked",
				message: fmt.Sprintf("source transaction is blocked by poison at commit_lsn %d", poison.Transaction.CommitLSN),
			}
		}
		if !repair {
			return errors.New("poisoned source transaction requires repair-and-retry")
		}
	}
	transactions := append([]StreamTransaction(nil), state.Stream.Transactions...)
	walSortTransactions(transactions)
	for _, transaction := range transactions {
		if transaction.ReplayKey.StreamGeneration != key.StreamGeneration || walTransactionCompleted(state, transaction.ReplayKey) {
			continue
		}
		if transaction.ReplayKey != key {
			return sourceTransactionBlockedError{
				code:    "source_transaction_predecessor_pending",
				message: fmt.Sprintf("earlier source transaction at commit_lsn %d must materialize first", transaction.ReplayKey.CommitLSN),
			}
		}
		return nil
	}
	return errors.New("source transaction has no pending materialization")
}

type sourceTransactionBlockedError struct {
	code    string
	message string
}

func (e sourceTransactionBlockedError) Error() string {
	return e.message
}

func (e sourceTransactionBlockedError) ErrorCode() string {
	return e.code
}

func walTransactionCompleted(state State, key TransactionReplayKey) bool {
	for _, replay := range state.Stream.TransactionReplays {
		if replay.Key == key {
			return replay.Completed
		}
	}
	return false
}

func walMaterializeTransaction(state *State, key TransactionReplayKey, now *time.Time) ([]ScopeID, error) {
	transactionIndex := walFindTransactionIndex(state.Stream.Transactions, key)
	if transactionIndex < 0 {
		return nil, errors.New("materialization transaction disappeared")
	}
	transaction := state.Stream.Transactions[transactionIndex]
	generation, found := walRegistryGeneration(state.Registry, transaction.RegistryGeneration)
	if !found || !generation.Validated {
		return nil, walFailure("validation_failed", false, "", "selected registry generation is unavailable")
	}
	if walTransactionReplayRecordExists(*state, key) {
		return nil, walFailure("materialization_failed", false, "", "incomplete transaction replay record already exists")
	}
	for _, event := range transaction.Events {
		if walEventReplayRecordExists(*state, event.ReplayKey) {
			return nil, walFailure("materialization_failed", true, event.Relation, "incomplete event replay record already exists")
		}
	}

	causalEvents := make(map[RowIdentity]EventReplayKey)
	directChanges := make(map[RowIdentity]bool)
	deleteVersions := make(map[RowIdentity]RowVersion)
	dmlOrdinal := uint64(1)
	for _, event := range transaction.Events {
		definition, registered := walRegistryRelation(generation, event.Relation)
		if !registered {
			return nil, walFailure("validation_failed", true, event.Relation, "event relation is absent from its retained registry generation")
		}
		fenceID, fence, err := walCorrelateFence(*state, definition, event, dmlOrdinal)
		if err != nil {
			return nil, err
		}
		dmlOrdinal++
		fence.HasEventReplayKey = true
		fence.EventReplayKey = event.ReplayKey
		fence.Coverage = FenceCoverageMaterialized
		state.Fences[fenceID] = fence

		if err := walCaptureEventProjections(state, event, now); err != nil {
			return nil, err
		}
		if definition.RegistrationKind == RegistrationKindSynced {
			row, err := walApplySyncedEvent(state, event, fence, now)
			if err != nil {
				return nil, err
			}
			directChanges[row] = true
			walSetGreatestCausalEvent(causalEvents, row, event.ReplayKey)
			if event.Operation == DMLOperationDelete || event.HasAfter && event.After.Deleted {
				deleteVersions[row] = fence.RowVersion
			}
		}
		if err := walCollectDependencyImpacts(generation, event, causalEvents); err != nil {
			return nil, err
		}
	}

	pendingEffects := make([]walPendingEffect, 0)
	affectedScopes := make(map[ScopeID]struct{})
	impactedRows := make([]RowIdentity, 0, len(causalEvents))
	for row := range causalEvents {
		impactedRows = append(impactedRows, row)
	}
	sort.Slice(impactedRows, func(left, right int) bool {
		return lessRowIdentity(impactedRows[left], impactedRows[right])
	})
	for _, rowIdentity := range impactedRows {
		row, rowExists := state.Rows[rowIdentity]
		rowVisible := rowExists && !row.Deleted
		newScopes := []ScopeID{}
		if rowVisible {
			var err error
			newScopes, err = walEvaluateMembership(generation, rowIdentity)
			if err != nil {
				return nil, err
			}
		}
		oldScopes, err := walCurrentMembershipScopes(*state, rowIdentity)
		if err != nil {
			return nil, err
		}
		oldSet := walScopeSet(oldScopes)
		newSet := walScopeSet(newScopes)
		union := walScopeUnion(oldSet, newSet)
		for _, scopeID := range union {
			oldIncluded := oldSet[scopeID]
			newIncluded := newSet[scopeID]
			scope := walEnsureScope(state, scopeID)
			walSetScopeMembership(&scope, rowIdentity, newIncluded)
			state.Scopes[scopeID] = scope
			affectedScopes[scopeID] = struct{}{}

			switch {
			case oldIncluded && !newIncluded:
				version := row.Version
				if deletedVersion, deleted := deleteVersions[rowIdentity]; deleted {
					version = deletedVersion
				}
				if version == "" {
					return nil, walFailure("scope_evaluation_failed", false, "", "scope leaving has no current projected row version")
				}
				pendingEffects = append(pendingEffects, walPendingEffect{
					scope:     scopeID,
					row:       rowIdentity,
					source:    causalEvents[rowIdentity],
					operation: EffectOperationDelete,
					version:   version,
				})
			case newIncluded && (!oldIncluded || directChanges[rowIdentity]):
				if !rowVisible || row.Version == "" {
					return nil, walFailure("scope_evaluation_failed", false, "", "scope upsert has no complete projected row")
				}
				projection, projectionFound := walLatestProjectionForRow(*state, rowIdentity)
				if !projectionFound {
					return nil, walFailure("projection_write_failed", false, "", "scope upsert has no captured row projection")
				}
				pendingEffects = append(pendingEffects, walPendingEffect{
					scope:         scopeID,
					row:           rowIdentity,
					source:        causalEvents[rowIdentity],
					operation:     EffectOperationUpsert,
					version:       row.Version,
					hasProjection: true,
					projection:    projection,
					hasChecksum:   true,
					checksum:      row.Checksum,
				})
			}
		}
	}

	walAppendPendingEffects(state, pendingEffects)
	affected := walSortedScopeSet(affectedScopes)
	for _, scopeID := range affected {
		scope := state.Scopes[scopeID]
		scope.HighWatermark = transaction.End
		if err := walRecomputeScopeState(*state, scopeID, &scope); err != nil {
			return nil, err
		}
		state.Scopes[scopeID] = scope
	}
	for _, event := range transaction.Events {
		state.Stream.EventReplays = append(state.Stream.EventReplays, EventReplayRecord{Key: event.ReplayKey})
		state.Stream.Materializations = append(state.Stream.Materializations, MaterializationRecord{Event: event.ReplayKey, Materialized: true})
	}
	state.Stream.TransactionReplays = append(state.Stream.TransactionReplays, TransactionReplayRecord{
		Key:                transaction.ReplayKey,
		RegistryGeneration: transaction.RegistryGeneration,
		EndLSN:             transaction.EndLSN,
		Completed:          true,
	})
	state.Stream.Transactions[transactionIndex].Lifecycle = TransactionLifecycleMaterialized
	state.Stream.Authority.GlobalMaterializationBoundary = transaction.End
	for index := range state.Readiness.Workers {
		worker := &state.Readiness.Workers[index]
		if !worker.Running {
			continue
		}
		worker.HeartbeatAt = now
		worker.RegistryGeneration = transaction.RegistryGeneration
		worker.MaterializedPosition = transaction.End
	}
	return affected, nil
}

func walCorrelateFence(state State, definition RelationDefinition, event SourceEvent, dmlOrdinal uint64) (FenceID, VersionFence, error) {
	type fenceCandidate struct {
		id    FenceID
		fence VersionFence
	}
	candidates := make([]fenceCandidate, 0, 1)
	for id, fence := range state.Fences {
		if fence.Relation != event.Relation || fence.Operation != event.Operation || fence.DMLOrdinal != dmlOrdinal {
			continue
		}
		if fence.HasOldRegisteredIdentity != event.HasBefore || fence.HasNewRegisteredIdentity != event.HasAfter {
			continue
		}
		if event.HasBefore && fence.OldRegisteredIdentity != event.Before.Identity || event.HasAfter && fence.NewRegisteredIdentity != event.After.Identity {
			continue
		}
		if event.HasAfter && fence.RowVersion != event.After.Version {
			continue
		}
		if fence.HasEventReplayKey && fence.EventReplayKey != event.ReplayKey {
			continue
		}
		candidates = append(candidates, fenceCandidate{id: id, fence: fence})
	}
	if len(candidates) != 1 {
		return "", VersionFence{}, walFailure("fence_correlation_failed", true, event.Relation, "source event does not have exactly one matching fence")
	}
	selected := candidates[0]
	if selected.fence.ID != selected.id || selected.fence.RegistrationKind != definition.RegistrationKind || selected.fence.Physical != definition.Physical || selected.fence.RowVersion == "" {
		return "", VersionFence{}, walFailure("fence_correlation_failed", true, event.Relation, "source event fence has inconsistent registered identity")
	}
	if selected.fence.Coverage != FenceCoveragePending && !(selected.fence.Coverage == FenceCoverageMaterialized && selected.fence.HasEventReplayKey && selected.fence.EventReplayKey == event.ReplayKey) {
		return "", VersionFence{}, walFailure("fence_correlation_failed", true, event.Relation, "source event fence has invalid coverage")
	}
	return selected.id, selected.fence, nil
}

func walCaptureEventProjections(state *State, event SourceEvent, now *time.Time) error {
	if event.HasBefore {
		if err := walCaptureProjection(state, event, ProjectionImageBefore, event.Before, now); err != nil {
			return err
		}
	}
	if event.HasAfter {
		if err := walCaptureProjection(state, event, ProjectionImageAfter, event.After, now); err != nil {
			return err
		}
	}
	return nil
}

func walCaptureProjection(state *State, event SourceEvent, imageKind ProjectionImage, image SourceImage, now *time.Time) error {
	key := ProjectionKey{Relation: event.Relation, Event: event.ReplayKey, Image: imageKind}
	if _, exists := state.Projections[key]; exists {
		return walFailure("projection_write_failed", true, event.Relation, "captured projection already exists without a completed replay")
	}
	projection := CapturedProjection{
		Event:      event.ReplayKey,
		Image:      imageKind,
		Fields:     append([]FieldValue(nil), image.Fields...),
		Version:    image.Version,
		Checksum:   image.Checksum,
		CapturedAt: now,
	}
	if image.Identity.Kind == RegistrationKindSynced {
		projection.Row = image.Identity.SyncedRow
	}
	state.Projections[key] = projection
	return nil
}

func walApplySyncedEvent(state *State, event SourceEvent, fence VersionFence, now *time.Time) (RowIdentity, error) {
	switch event.Operation {
	case DMLOperationInsert:
		identity := event.After.Identity.SyncedRow
		if _, found := state.Rows[identity]; found {
			return RowIdentity{}, walFailure("materialization_failed", true, event.Relation, "insert projects an existing row")
		}
		state.Rows[identity] = walAuthoritativeRowFromImage(event.After, event.After.Deleted, now)
		return identity, nil
	case DMLOperationUpdate:
		identity := event.After.Identity.SyncedRow
		current, found := state.Rows[identity]
		if !found || current.Version != event.Before.Version {
			return RowIdentity{}, walFailure("materialization_failed", true, event.Relation, "update before image differs from projected state")
		}
		if event.After.Version == event.Before.Version || fence.RowVersion != event.After.Version {
			return RowIdentity{}, walFailure("fence_correlation_failed", true, event.Relation, "update did not advance through its fence version")
		}
		state.Rows[identity] = walAuthoritativeRowFromImage(event.After, event.After.Deleted, now)
		return identity, nil
	case DMLOperationDelete:
		identity := event.Before.Identity.SyncedRow
		current, found := state.Rows[identity]
		if !found || current.Version != event.Before.Version {
			return RowIdentity{}, walFailure("materialization_failed", true, event.Relation, "delete before image differs from projected state")
		}
		delete(state.Rows, identity)
		return identity, nil
	default:
		return RowIdentity{}, walFailure("validation_failed", true, event.Relation, "synced event has an invalid operation")
	}
}

func walAuthoritativeRowFromImage(image SourceImage, deleted bool, at *time.Time) AuthoritativeRow {
	row := AuthoritativeRow{
		Identity:    image.Identity.SyncedRow,
		FieldValues: append([]FieldValue(nil), image.Fields...),
		Version:     image.Version,
		Checksum:    image.Checksum,
		Deleted:     deleted,
		UpdatedAt:   at,
	}
	if deleted {
		row.DeletedAt = at
	}
	return row
}

func walCollectDependencyImpacts(generation RegistryGenerationState, event SourceEvent, causal map[RowIdentity]EventReplayKey) error {
	for _, impact := range generation.DependencyImpacts {
		if impact.Relation != event.Relation {
			continue
		}
		if impact.PositiveRowBound == 0 || uint64(len(impact.AffectedRows)) > impact.PositiveRowBound {
			return walFailure("scope_evaluation_failed", true, event.Relation, "dependency impact exceeds its positive bound")
		}
		seen := make(map[RowIdentity]struct{}, len(impact.AffectedRows))
		for _, row := range impact.AffectedRows {
			if row.CanonicalIdentityBytes == "" {
				return walFailure("scope_evaluation_failed", true, event.Relation, "dependency impact contains an invalid row identity")
			}
			if _, duplicate := seen[row]; duplicate {
				continue
			}
			seen[row] = struct{}{}
			if !walGenerationHasTable(generation, row.TableID) {
				return walFailure("scope_evaluation_failed", true, event.Relation, "dependency impact targets an undeclared logical table")
			}
			walSetGreatestCausalEvent(causal, row, event.ReplayKey)
		}
	}
	return nil
}

func walGenerationHasTable(generation RegistryGenerationState, table TableID) bool {
	for _, relation := range generation.Relations {
		if relation.Definition.RegistrationKind == RegistrationKindSynced && relation.Definition.HasTableID && relation.Definition.TableID == table {
			return true
		}
	}
	return false
}

func walSetGreatestCausalEvent(causal map[RowIdentity]EventReplayKey, row RowIdentity, event EventReplayKey) {
	prior, exists := causal[row]
	if !exists || lessEventReplayKey(prior, event) {
		causal[row] = event
	}
}

func walEvaluateMembership(generation RegistryGenerationState, row RowIdentity) ([]ScopeID, error) {
	matchedRule := false
	set := make(map[ScopeID]struct{})
	for _, rule := range generation.ScopeRules {
		definition, found := walRegistryRelation(generation, rule.Relation)
		if !found || definition.RegistrationKind != RegistrationKindSynced || definition.TableID != row.TableID {
			continue
		}
		matchedRule = true
		if rule.PositiveFanoutBound == 0 {
			return nil, walFailure("scope_evaluation_failed", true, rule.Relation, "membership rule has no positive fanout bound")
		}
		var evaluation *MembershipEvaluation
		for index := range rule.Evaluations {
			if rule.Evaluations[index].Row != row {
				continue
			}
			if evaluation != nil {
				return nil, walFailure("scope_evaluation_failed", true, rule.Relation, "membership rule contains duplicate row evaluations")
			}
			evaluation = &rule.Evaluations[index]
		}
		if evaluation == nil {
			return nil, walFailure("scope_evaluation_failed", true, rule.Relation, "membership rule omits the projected row evaluation")
		}
		local := make(map[ScopeID]struct{}, len(evaluation.Scopes))
		for _, scope := range evaluation.Scopes {
			if scope == "" {
				return nil, walFailure("scope_evaluation_failed", true, rule.Relation, "membership evaluation contains an empty scope")
			}
			local[scope] = struct{}{}
			set[scope] = struct{}{}
		}
		if uint64(len(local)) > rule.PositiveFanoutBound {
			return nil, walFailure("scope_evaluation_failed", true, rule.Relation, "membership evaluation exceeds its positive fanout bound")
		}
	}
	if !matchedRule {
		return nil, walFailure("scope_evaluation_failed", false, "", "projected row has no membership rule")
	}
	return walSortedScopeSet(set), nil
}

func walCurrentMembershipScopes(state State, row RowIdentity) ([]ScopeID, error) {
	set := make(map[ScopeID]struct{})
	for scopeID, scope := range state.Scopes {
		seen := false
		for _, membership := range scope.Membership {
			if membership.Row != row {
				continue
			}
			if seen {
				return nil, walFailure("scope_evaluation_failed", false, "", "scope has duplicate membership for one row")
			}
			seen = true
			if membership.Included {
				set[scopeID] = struct{}{}
			}
		}
	}
	return walSortedScopeSet(set), nil
}

func walScopeSet(scopes []ScopeID) map[ScopeID]bool {
	result := make(map[ScopeID]bool, len(scopes))
	for _, scope := range scopes {
		result[scope] = true
	}
	return result
}

func walScopeUnion(left, right map[ScopeID]bool) []ScopeID {
	set := make(map[ScopeID]struct{}, len(left)+len(right))
	for scope := range left {
		set[scope] = struct{}{}
	}
	for scope := range right {
		set[scope] = struct{}{}
	}
	return walSortedScopeSet(set)
}

func walEnsureScope(state *State, scopeID ScopeID) ScopeState {
	if scope, found := state.Scopes[scopeID]; found {
		return scope
	}
	return ScopeState{
		Schema:               state.CurrentSchema,
		MembershipGeneration: 1,
		RetentionGeneration:  1,
		StreamGeneration:     state.Stream.Authority.ActiveGeneration,
		Membership:           []ScopeMembership{},
		Effects:              []ScopeEffect{},
	}
}

func walSetScopeMembership(scope *ScopeState, row RowIdentity, included bool) {
	for index := range scope.Membership {
		if scope.Membership[index].Row != row {
			continue
		}
		scope.Membership[index].Included = included
		scope.Membership[index].Generation = scope.MembershipGeneration
		return
	}
	scope.Membership = append(scope.Membership, ScopeMembership{Row: row, Generation: scope.MembershipGeneration, Included: included})
}

func walLatestProjectionForRow(state State, row RowIdentity) (ProjectionKey, bool) {
	var selected ProjectionKey
	found := false
	for key, projection := range state.Projections {
		if projection.Row != row || key.Image != ProjectionImageAfter {
			continue
		}
		if !found || lessEventReplayKey(selected.Event, key.Event) {
			selected = key
			found = true
		}
	}
	return selected, found
}

func walAppendPendingEffects(state *State, pending []walPendingEffect) {
	sort.Slice(pending, func(left, right int) bool {
		if pending[left].scope != pending[right].scope {
			return pending[left].scope < pending[right].scope
		}
		if pending[left].source != pending[right].source {
			return lessEventReplayKey(pending[left].source, pending[right].source)
		}
		if pending[left].row.TableID != pending[right].row.TableID {
			return pending[left].row.TableID < pending[right].row.TableID
		}
		if pending[left].row != pending[right].row {
			return lessRowIdentity(pending[left].row, pending[right].row)
		}
		return effectOperationRank(pending[left].operation) < effectOperationRank(pending[right].operation)
	})
	var priorScope ScopeID
	var priorEvent EventReplayKey
	var effectOrdinal EffectOrdinal
	for index, pendingEffect := range pending {
		if index == 0 || pendingEffect.scope != priorScope || pendingEffect.source != priorEvent {
			effectOrdinal = 0
		} else {
			effectOrdinal++
		}
		position := StreamPosition{
			StreamGeneration: pendingEffect.source.Transaction.StreamGeneration,
			Kind:             PositionKindEffect,
			CommitLSN:        pendingEffect.source.Transaction.CommitLSN,
			EventOrdinal:     pendingEffect.source.EventOrdinal,
			EffectOrdinal:    effectOrdinal,
		}
		scope := state.Scopes[pendingEffect.scope]
		scope.Effects = append(scope.Effects, ScopeEffect{
			Position:              position,
			Row:                   pendingEffect.row,
			SourceEvent:           pendingEffect.source,
			Operation:             pendingEffect.operation,
			Version:               pendingEffect.version,
			HasCapturedProjection: pendingEffect.hasProjection,
			CapturedProjection:    pendingEffect.projection,
			HasChecksum:           pendingEffect.hasChecksum,
			Checksum:              pendingEffect.checksum,
		})
		state.Scopes[pendingEffect.scope] = scope
		priorScope = pendingEffect.scope
		priorEvent = pendingEffect.source
	}
}

func walRecomputeScopeState(state State, scopeID ScopeID, scope *ScopeState) error {
	type digestRow struct {
		identity string
		checksum Checksum
	}
	rows := make([]digestRow, 0)
	seen := make(map[RowIdentity]struct{})
	for _, membership := range scope.Membership {
		if !membership.Included {
			continue
		}
		if _, duplicate := seen[membership.Row]; duplicate {
			return walFailure("scope_evaluation_failed", false, "", "scope contains duplicate included membership")
		}
		seen[membership.Row] = struct{}{}
		row, found := state.Rows[membership.Row]
		if !found || row.Deleted || row.Identity != membership.Row {
			return walFailure("scope_evaluation_failed", false, "", "scope membership has no visible projected row")
		}
		rows = append(rows, digestRow{identity: membership.Row.CanonicalIdentityBytes, checksum: row.Checksum})
	}
	sort.Slice(rows, func(left, right int) bool {
		return rows[left].identity < rows[right].identity
	})
	preimage := append([]byte("synchro:v3:scope-digest:v1\x00"), scope.Schema.Hash[:]...)
	preimage = walAppendText(preimage, string(scopeID))
	preimage = walAppendUint64(preimage, uint64(len(rows)))
	for _, row := range rows {
		preimage = walAppendBlob(preimage, []byte(row.identity))
		preimage = append(preimage, row.checksum[:]...)
	}
	scope.Cardinality = Cardinality(len(rows))
	scope.Checksum = sha256.Sum256(preimage)
	return nil
}

func walAppendText(destination []byte, value string) []byte {
	return walAppendBlob(destination, []byte(value))
}

func walAppendBlob(destination, value []byte) []byte {
	destination = walAppendUint64(destination, uint64(len(value)))
	return append(destination, value...)
}

func walAppendUint64(destination []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(destination, encoded[:]...)
}

func walTransactionReplayRecordExists(state State, key TransactionReplayKey) bool {
	for _, replay := range state.Stream.TransactionReplays {
		if replay.Key == key {
			return true
		}
	}
	return false
}

func walEventReplayRecordExists(state State, key EventReplayKey) bool {
	for _, replay := range state.Stream.EventReplays {
		if replay.Key == key {
			return true
		}
	}
	return false
}

func walFailure(reason ReasonCode, hasRelation bool, relation RelationID, detail string) error {
	return &walMaterializationFailure{reason: reason, hasRelation: hasRelation, relation: relation, detail: detail}
}

func walPoisonTransaction(state *State, transaction StreamTransaction, failure walMaterializationFailure, now *time.Time) {
	transactionIndex := walFindTransactionIndex(state.Stream.Transactions, transaction.ReplayKey)
	if transactionIndex >= 0 {
		state.Stream.Transactions[transactionIndex].Lifecycle = TransactionLifecyclePoisoned
	}
	if _, exists := walFindActivePoison(*state, transaction.ReplayKey); !exists {
		state.Stream.Poison = append(state.Stream.Poison, PoisonRecord{
			Transaction: transaction.ReplayKey,
			HasRelation: failure.hasRelation,
			Relation:    failure.relation,
			Reason:      failure.reason,
			Lifecycle:   PoisonLifecycleActive,
			PoisonedAt:  now,
		})
	}
	walAddReadinessReason(state, "wal_poison")
	walSetPoisonReadinessCheck(state, ReadinessCheckFailed, failure.reason, now)
}

func walMarkPoisonRepaired(state *State, key TransactionReplayKey, now *time.Time) {
	for index := range state.Stream.Poison {
		if state.Stream.Poison[index].Transaction == key && state.Stream.Poison[index].Lifecycle == PoisonLifecycleActive {
			state.Stream.Poison[index].Lifecycle = PoisonLifecycleRepaired
		}
	}
	if _, active := walEarliestActivePoison(*state); !active {
		walRemoveReadinessReason(state, "wal_poison")
		walSetPoisonReadinessCheck(state, ReadinessCheckOK, "", now)
	}
}

func walFindActivePoison(state State, key TransactionReplayKey) (PoisonRecord, bool) {
	for _, poison := range state.Stream.Poison {
		if poison.Transaction == key && poison.Lifecycle == PoisonLifecycleActive {
			return poison, true
		}
	}
	return PoisonRecord{}, false
}

func walEarliestActivePoison(state State) (PoisonRecord, bool) {
	var selected PoisonRecord
	found := false
	for _, poison := range state.Stream.Poison {
		if poison.Lifecycle != PoisonLifecycleActive {
			continue
		}
		if !found || lessTransactionReplayKey(poison.Transaction, selected.Transaction) {
			selected = poison
			found = true
		}
	}
	return selected, found
}

func walAddReadinessReason(state *State, reason ReasonCode) {
	for _, existing := range state.Readiness.Reasons {
		if existing == reason {
			return
		}
	}
	state.Readiness.Reasons = append(state.Readiness.Reasons, reason)
}

func walRemoveReadinessReason(state *State, reason ReasonCode) {
	for index := range state.Readiness.Reasons {
		if state.Readiness.Reasons[index] != reason {
			continue
		}
		state.Readiness.Reasons = append(state.Readiness.Reasons[:index], state.Readiness.Reasons[index+1:]...)
		return
	}
}

func walSetPoisonReadinessCheck(state *State, checkState ReadinessCheckState, reason ReasonCode, at *time.Time) {
	const checkID CheckID = "wal_poison"
	for index := range state.Readiness.Checks {
		if state.Readiness.Checks[index].ID != checkID {
			continue
		}
		state.Readiness.Checks[index].State = checkState
		state.Readiness.Checks[index].Reason = reason
		state.Readiness.Checks[index].CheckedAt = at
		return
	}
	state.Readiness.Checks = append(state.Readiness.Checks, ReadinessCheck{ID: checkID, State: checkState, Reason: reason, CheckedAt: at})
}

func walMarkReplay(state *State, key TransactionReplayKey) {
	for index := range state.Stream.TransactionReplays {
		if state.Stream.TransactionReplays[index].Key == key {
			state.Stream.TransactionReplays[index].Replayed = true
		}
	}
	for index := range state.Stream.EventReplays {
		if state.Stream.EventReplays[index].Key.Transaction == key {
			state.Stream.EventReplays[index].Replayed = true
		}
	}
}

func walCurrentPoisonState(state State) WALPoisonState {
	if _, active := walEarliestActivePoison(state); active {
		return WALPoisonStatePoisoned
	}
	return WALPoisonStateClear
}

func walStepResult(state State, key TransactionReplayKey, registryGeneration Generation, affected []ScopeID, poison WALPoisonState) StepResult {
	position := state.Stream.Authority.GlobalMaterializationBoundary
	acknowledgement := state.Stream.Authority.AcknowledgedEndLSN
	return StepResult{
		Kind: StepResultKindWAL,
		WAL: &WALObservation{
			Transaction:          key,
			RegistryGeneration:   registryGeneration,
			PriorMaterialization: position,
			NewMaterialization:   position,
			PriorAcknowledgement: acknowledgement,
			NewAcknowledgement:   acknowledgement,
			AffectedScopes:       walSortedScopes(affected),
			Poison:               poison,
		},
	}
}

func walStepResultWithPrior(state State, transaction StreamTransaction, priorPosition StreamPosition, priorAcknowledgement EndLSN, affected []ScopeID, poison WALPoisonState) StepResult {
	result := walStepResult(state, transaction.ReplayKey, transaction.RegistryGeneration, affected, poison)
	result.WAL.PriorMaterialization = priorPosition
	result.WAL.PriorAcknowledgement = priorAcknowledgement
	return result
}

func walSortedScopes(scopes []ScopeID) []ScopeID {
	set := make(map[ScopeID]struct{}, len(scopes))
	for _, scope := range scopes {
		if scope != "" {
			set[scope] = struct{}{}
		}
	}
	return walSortedScopeSet(set)
}

func walSortedScopeSet(set map[ScopeID]struct{}) []ScopeID {
	result := make([]ScopeID, 0, len(set))
	for scope := range set {
		result = append(result, scope)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left] < result[right]
	})
	return result
}

func walNextEventOrdinal(events []ModelEvent) uint64 {
	var maximum uint64
	for _, event := range events {
		if event.Ordinal > maximum {
			maximum = event.Ordinal
		}
	}
	return maximum + 1
}
