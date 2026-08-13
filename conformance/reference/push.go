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
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/vectors"
)

const (
	pushRequestLimit  = 1 << 20
	pushResponseLimit = 1 << 20

	pushBatchDomain    = "synchro:v3:push-batch-fingerprint:v1"
	pushMutationDomain = "synchro:v3:push-mutation-fingerprint:v1"
	pushRetryAfter     = time.Second

	pushHTTPInvalidRequest       = HTTPCode("invalid_request")
	pushHTTPAuthRequired         = HTTPCode("auth_required")
	pushHTTPIdempotencyConflict  = HTTPCode("idempotency_conflict")
	pushHTTPClientRetired        = HTTPCode("client_retired")
	pushHTTPGenerationExpired    = HTTPCode("client_generation_expired")
	pushHTTPSchemaMismatch       = HTTPCode("schema_mismatch")
	pushHTTPTemporaryUnavailable = HTTPCode("temporary_unavailable")
)

var errPushOperational = errors.New("push operation failed")

func init() {
	registerOperation("local/write", localWriteOperation)
	registerOperation("push/submit", pushSubmitOperation)
	registerOperation("process/response-loss", processResponseLossOperation)
}

// pushSubmitEnvelope is deliberately separate from the protocol request. The
// envelope is model input, while Request is the exact protocol object.
type pushSubmitEnvelope struct {
	AuthenticatedUser UserID
	Request           json.RawMessage
	Delivery          string
	CommitLSN         uint64
	EndLSN            uint64
}

type parsedPushBatch struct {
	AuthenticatedUser UserID
	Client            ClientID
	Batch             BatchID
	Generation        Generation
	Schema            SchemaRef
	Request           []byte
	CanonicalRequest  []byte
	Fingerprint       [32]byte
	Mutations         []parsedPushMutation
}

type parsedPushMutation struct {
	Normalized        vectors.NormalizedMutation
	CanonicalMutation []byte
	Fingerprint       [32]byte
	Mutation          MutationID
	Table             TableID
	PKField           FieldID
	PKValue           json.RawMessage
	AuthoredSchema    SchemaRef
	Operation         DMLOperation
	BaseVersion       *RowVersion
	ClientVersion     ClientVersion
	Columns           []pushColumn
}

type pushColumn struct {
	Field FieldID
	Value json.RawMessage
}

type pushManifest struct {
	Reference SchemaRef
	Vector    vectors.Manifest
	Tables    map[TableID]pushManifestTable
}

type pushManifestTable struct {
	ID                TableID
	Relation          RelationID
	Composition       Composition
	PrimaryKeyFieldID FieldID
	CreatedFieldID    *FieldID
	UpdatedFieldID    *FieldID
	DeletedFieldID    *FieldID
	Fields            map[FieldID]pushManifestField
}

type pushManifestField struct {
	ID         FieldID
	Portable   PortableType
	Nullable   bool
	Writable   bool
	Precision  *int
	Scale      *int
	DefaultRaw *json.RawMessage
}

type pushSchemaWire struct {
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
}

type pushResponse struct {
	BatchID    string            `json:"batch_id"`
	ServerTime string            `json:"server_time"`
	Accepted   []json.RawMessage `json:"accepted"`
	Rejected   []json.RawMessage `json:"rejected"`
}

type pushChecksumWire struct {
	Algorithm string `json:"algorithm"`
	Version   uint64 `json:"version"`
	Encoding  string `json:"encoding"`
	Digest    string `json:"digest"`
}

type pushErrorEnvelope struct {
	Error pushErrorWire `json:"error"`
}

type pushErrorWire struct {
	Code                    string          `json:"code"`
	Message                 string          `json:"message"`
	Retryable               bool            `json:"retryable"`
	CurrentClientGeneration *uint64         `json:"current_client_generation,omitempty"`
	CurrentSchema           *pushSchemaWire `json:"current_schema,omitempty"`
	ReceivedSchema          *pushSchemaWire `json:"received_schema,omitempty"`
}

type pushOutcomeWire struct {
	MutationID         string                     `json:"mutation_id"`
	Table              string                     `json:"table"`
	PK                 map[string]json.RawMessage `json:"pk"`
	OutcomeSchema      pushSchemaWire             `json:"outcome_schema"`
	Status             string                     `json:"status"`
	Code               *string                    `json:"code,omitempty"`
	Message            *string                    `json:"message,omitempty"`
	Retryable          *bool                      `json:"retryable,omitempty"`
	ServerRow          map[string]json.RawMessage `json:"server_row,omitempty"`
	RowChecksum        *pushChecksumWire          `json:"row_checksum,omitempty"`
	ServerVersion      *string                    `json:"server_version,omitempty"`
	AuthoredSchema     *pushSchemaWire            `json:"authored_schema,omitempty"`
	CurrentSchema      *pushSchemaWire            `json:"current_schema,omitempty"`
	IncompatibleFields *[]string                  `json:"incompatible_field_ids,omitempty"`
}

type evaluatedPushMutation struct {
	Parsed       parsedPushMutation
	Outcome      pushOutcomeWire
	OutcomeBytes []byte
	State        MutationOutcomeState
	Reason       ReasonCode
	Accepted     bool
	NewWrite     bool
	Before       *AuthoritativeRow
	After        *AuthoritativeRow
	Relation     RelationDefinition
}

type sourceTransition struct {
	Parsed     parsedPushMutation
	Relation   RelationDefinition
	Operation  DMLOperation
	Before     *AuthoritativeRow
	After      AuthoritativeRow
	SoftDelete bool
}

type storedPushOutcome struct {
	Wire          pushOutcomeWire
	State         MutationOutcomeState
	Reason        ReasonCode
	OutcomeSchema SchemaRef
	HasRow        bool
	Row           map[string]json.RawMessage
	HasChecksum   bool
	Checksum      Checksum
	HasVersion    bool
	Version       RowVersion
}

type localWriteInput struct {
	AuthenticatedUser UserID
	Client            ClientID
	Mutation          MutationID
	Table             TableID
	PKField           FieldID
	PKValue           json.RawMessage
	AuthoredSchema    SchemaRef
	Operation         DMLOperation
	HasPresentedBase  bool
	PresentedBase     RowVersion
	ClientVersion     ClientVersion
	Columns           []pushColumn
	Origin            string
}

func localWriteOperation(ctx context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	if err := ctx.Err(); err != nil {
		return StepResult{}, err
	}
	input, err := parseLocalWritePayload(payload)
	if err != nil {
		return StepResult{}, fmt.Errorf("decode local/write payload: %w", err)
	}
	clientKey := ClientKey{UserID: input.AuthenticatedUser, ClientID: input.Client}
	local, ok := model.state.ClientLocal[clientKey]
	if !ok {
		return StepResult{}, fmt.Errorf("local/write: client is not initialized")
	}

	ref := input.AuthoredSchema
	manifest, err := loadPushManifest(model.state, ref)
	if err != nil {
		return StepResult{}, fmt.Errorf("local/write: load authored schema: %w", err)
	}
	table, ok := manifest.Tables[input.Table]
	if !ok {
		return StepResult{}, fmt.Errorf("local/write: table %q is absent from the authored schema", input.Table)
	}
	if input.PKField != table.PrimaryKeyFieldID {
		return StepResult{}, errors.New("local/write: primary-key field does not match the manifest")
	}
	identity, err := derivePushRowIdentity(manifest, input.Table, input.PKField, input.PKValue)
	if err != nil {
		return StepResult{}, fmt.Errorf("local/write: derive row identity: %w", err)
	}
	if err := validatePushColumns(manifest, table, input.Operation, input.Columns); err != nil {
		return StepResult{}, fmt.Errorf("local/write: %w", err)
	}

	rowIndex := localRowIndex(local.Rows, identity)
	var current *LocalRow
	if rowIndex >= 0 {
		copy := cloneLocalRowForPush(local.Rows[rowIndex])
		current = &copy
	}
	if current != nil && current.Deleted {
		return StepResult{}, errors.New("local/write: deleted rows cannot be resurrected")
	}

	baseVersion, hasBase := localBaseVersion(current)
	if input.HasPresentedBase {
		if !hasBase || input.PresentedBase != baseVersion {
			return StepResult{}, errors.New("local/write: presented base version differs from local server state")
		}
	}
	predecessor, hasPredecessor := latestUnresolvedSameRow(local.DurableQueue, identity)
	if hasPredecessor {
		hasBase = false
		baseVersion = ""
	}

	now := canonicalClockTime(model.clock.Now())
	var next LocalRow
	switch input.Operation {
	case DMLOperationInsert:
		if current != nil {
			return StepResult{}, errors.New("local/write: insert target already exists")
		}
		authoritative, err := buildPushInsertRow(manifest, table, identity, input.PKValue, input.Columns, now)
		if err != nil {
			return StepResult{}, fmt.Errorf("local/write: build inserted row: %w", err)
		}
		next = localRowFromAuthoritative(authoritative, false, "", Checksum{})
	case DMLOperationUpdate:
		if current == nil {
			return StepResult{}, errors.New("local/write: update target is absent")
		}
		next = *current
		next.Fields = mergeLocalFields(next.Fields, input.Columns, table)
		next.Deleted = false
	case DMLOperationDelete:
		if current == nil {
			return StepResult{}, errors.New("local/write: delete target is absent")
		}
		next = *current
		next.Deleted = true
	default:
		return StepResult{}, errors.New("local/write: unsupported operation")
	}
	next.Identity = identity
	next.UpdatedAt = &now
	if input.Origin == "server_apply" {
		if rowIndex >= 0 {
			local.Rows[rowIndex] = next
		} else {
			local.Rows = append(local.Rows, next)
		}
		model.state.ClientLocal[clientKey] = local
		return StepResult{
			Kind:  StepResultKindLocal,
			Local: &LocalObservation{Client: clientKey, Mutation: input.Mutation, Status: LocalMutationStatusAccepted},
		}, nil
	}

	if queuedMutationExists(local.DurableQueue, input.Mutation) {
		return StepResult{}, errors.New("local/write: mutation ID was already used")
	}
	if rowIndex >= 0 {
		local.Rows[rowIndex] = next
	} else {
		local.Rows = append(local.Rows, next)
	}
	localOrder := nextLocalOrder(local.DurableQueue)
	queued := QueuedMutation{
		Mutation:        input.Mutation,
		Table:           input.Table,
		Row:             identity,
		AuthoredSchema:  input.AuthoredSchema,
		Operation:       input.Operation,
		HasBaseVersion:  hasBase && input.Operation != DMLOperationInsert,
		BaseVersion:     baseVersion,
		ClientVersion:   input.ClientVersion,
		AuthoredColumns: fieldValuesFromPushColumns(table, input.Columns),
		LocalOrder:      localOrder,
		HasPredecessor:  hasPredecessor,
		Predecessor:     predecessor,
		Status:          LocalMutationStatusPending,
		QueuedAt:        &now,
	}
	queued.Request = canonicalQueuedMutationBytes(queued)
	local.DurableQueue = append(local.DurableQueue, queued)
	normalizePendingSameRow(&local)
	model.state.ClientLocal[clientKey] = local

	status := queued.Status
	for index := range local.DurableQueue {
		if local.DurableQueue[index].Mutation == input.Mutation {
			status = local.DurableQueue[index].Status
			break
		}
	}
	return StepResult{
		Kind:  StepResultKindLocal,
		Local: &LocalObservation{Client: clientKey, Mutation: input.Mutation, Status: status},
	}, nil
}

func pushSubmitOperation(ctx context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	if err := ctx.Err(); err != nil {
		return StepResult{}, err
	}
	envelope, err := parsePushSubmitPayload(payload)
	if err != nil {
		return StepResult{}, fmt.Errorf("decode push/submit payload: %w", err)
	}
	batch, err := parseProtocolPushRequest(envelope.AuthenticatedUser, envelope.Request)
	if err != nil {
		return pushFailureResult(BatchKey{Client: ClientKey{UserID: envelope.AuthenticatedUser}}, ReplayDispositionExecuted, 400, pushHTTPInvalidRequest, false, nil), nil
	}
	return executePush(ctx, model, envelope, batch)
}

func processResponseLossOperation(ctx context.Context, model *Model, payload json.RawMessage) (StepResult, error) {
	object, err := strictObject(payload)
	if err != nil {
		return StepResult{}, fmt.Errorf("decode process/response-loss payload: %w", err)
	}
	if err := requirePushKeys(object, []string{"authenticated_user_id", "client_id", "batch_id"}, []string{"commit_lsn", "end_lsn"}); err != nil {
		return StepResult{}, fmt.Errorf("decode process/response-loss payload: %w", err)
	}
	userText, err := requiredJSONString(object["authenticated_user_id"], "authenticated_user_id")
	if err != nil {
		return StepResult{}, err
	}
	clientText, err := requiredJSONString(object["client_id"], "client_id")
	if err != nil {
		return StepResult{}, err
	}
	batchText, err := requiredJSONString(object["batch_id"], "batch_id")
	if err != nil {
		return StepResult{}, err
	}
	clientKey := ClientKey{UserID: UserID(userText), ClientID: ClientID(clientText)}
	local, ok := model.state.ClientLocal[clientKey]
	if !ok {
		return StepResult{}, errors.New("process/response-loss: client is not initialized")
	}
	batchIndex := localSealedBatchIndex(local.SealedBatches, BatchID(batchText))
	if batchIndex < 0 {
		return StepResult{}, errors.New("process/response-loss: sealed batch is absent")
	}
	sealed := local.SealedBatches[batchIndex]
	if sealed.State != LocalSealedBatchStateResponseLost && sealed.State != LocalSealedBatchStateSent {
		return StepResult{}, errors.New("process/response-loss: batch is not retryable")
	}

	key := BatchKey{Client: clientKey, Batch: sealed.Batch}
	local.SealedBatches[batchIndex].State = LocalSealedBatchStateResponseLost
	model.state.ClientLocal[clientKey] = local
	markLocalPushBackoff(model, key, RetryClassificationTransport)
	appendPushEvent(model, clientKey, ModelEventResponseLoss, "response_lost", nil)
	return pushFailureResult(key, ReplayDispositionExecuted, 503, pushHTTPTemporaryUnavailable, true, nil), nil
}

func executePush(ctx context.Context, model *Model, envelope pushSubmitEnvelope, batch parsedPushBatch) (StepResult, error) {
	clientKey := ClientKey{UserID: envelope.AuthenticatedUser, ClientID: batch.Client}
	key := BatchKey{Client: clientKey, Batch: batch.Batch}

	// A transport failure occurs before the request reaches server dispatch.
	if envelope.Delivery == "transport_failure" {
		if err := markLocalBatchResponseLost(model, batch, key, 503); err != nil {
			return StepResult{}, err
		}
		markLocalPushBackoff(model, key, RetryClassificationTransport)
		appendPushEvent(model, clientKey, ModelEventTransportFailure, "transport_failure", nil)
		return pushFailureResult(key, ReplayDispositionExecuted, 503, pushHTTPTemporaryUnavailable, true, nil), nil
	}

	client, ok := model.state.Clients[clientKey]
	if !ok {
		return pushFailureResult(key, ReplayDispositionExecuted, 401, pushHTTPAuthRequired, false, nil), nil
	}
	if client.Retirement != nil {
		return pushFailureResult(key, ReplayDispositionExecuted, 409, pushHTTPClientRetired, false, nil), nil
	}
	if err := ensureLocalSealedBatch(model, batch, key); err != nil {
		recordLocalPushIntegrityFailure(model, key.Client, "local_seal_invalid")
		return pushFailureResult(key, ReplayDispositionExecuted, 400, pushHTTPInvalidRequest, false, nil), nil
	}

	if existing, exists := model.state.Batches[key]; exists {
		if !equalFingerprintRecord(existing.Fingerprint, fingerprintRecord(pushBatchDomain, batch.Fingerprint)) {
			return pushFailureResult(key, ReplayDispositionExecuted, 409, pushHTTPIdempotencyConflict, false, nil), nil
		}
		if existing.Execution != BatchExecutionCompleted {
			markLocalPushBackoff(model, key, RetryClassificationUnavailable)
			return pushFailureResult(key, ReplayDispositionExecuted, 503, pushHTTPTemporaryUnavailable, true, nil), nil
		}
		return replayCompletedBatch(model, envelope, batch, key, existing)
	}

	mutationReplay := make(map[MutationID]MutationLedger, len(batch.Mutations))
	for _, mutation := range batch.Mutations {
		mutationKey := MutationKey{Client: clientKey, Mutation: mutation.Mutation}
		if existing, exists := model.state.Mutations[mutationKey]; exists {
			if !equalFingerprintRecord(existing.Fingerprint, fingerprintRecord(pushMutationDomain, mutation.Fingerprint)) {
				return pushFailureResult(key, ReplayDispositionExecuted, 409, pushHTTPIdempotencyConflict, false, nil), nil
			}
			mutationReplay[mutation.Mutation] = existing
		}
	}
	generationIndex := currentClientGenerationIndex(client)
	if generationIndex < 0 || client.Generations[generationIndex].ExpiresAt != nil {
		generation := uint64(client.CurrentGeneration)
		return pushFailureResult(key, ReplayDispositionExecuted, 409, pushHTTPGenerationExpired, false, &pushErrorWire{CurrentClientGeneration: &generation}), nil
	}

	if client.CurrentGeneration == 0 || client.CurrentGeneration != batch.Generation {
		generation := uint64(client.CurrentGeneration)
		return pushFailureResult(key, ReplayDispositionExecuted, 409, pushHTTPGenerationExpired, false, &pushErrorWire{CurrentClientGeneration: &generation}), nil
	}
	if model.state.CurrentSchema != batch.Schema {
		current := pushSchemaWireFromRef(model.state.CurrentSchema)
		received := pushSchemaWireFromRef(batch.Schema)
		return pushFailureResult(key, ReplayDispositionExecuted, 422, pushHTTPSchemaMismatch, false, &pushErrorWire{CurrentSchema: &current, ReceivedSchema: &received}), nil
	}
	if envelope.CommitLSN == 0 || envelope.EndLSN < envelope.CommitLSN {
		markLocalPushBackoff(model, key, RetryClassificationUnavailable)
		return pushFailureResult(key, ReplayDispositionExecuted, 503, pushHTTPTemporaryUnavailable, true, nil), nil
	}

	server := &Model{state: cloneState(model.state), clock: model.clock, seed: model.seed, authority: model.authority}
	response, outcomes, _, _, err := executeFirstPush(ctx, server, envelope, batch, key, mutationReplay)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return StepResult{}, err
		}
		markLocalPushBackoff(model, key, RetryClassificationUnavailable)
		return pushFailureResult(key, ReplayDispositionExecuted, 503, pushHTTPTemporaryUnavailable, true, nil), nil
	}
	model.state = server.state
	if envelope.Delivery == "drop_after_server" {
		if err := markLocalBatchResponseLost(model, batch, key, 503); err != nil {
			return StepResult{}, err
		}
		markLocalPushBackoff(model, key, RetryClassificationTransport)
		appendPushEvent(model, clientKey, ModelEventResponseLoss, "response_lost", transactionForBatch(model, key))
		return pushFailureResult(key, ReplayDispositionExecuted, 503, pushHTTPTemporaryUnavailable, true, nil), nil
	}
	if err := reconcileLocalBatchFromOutcomes(model, batch, key, response, outcomes, true); err != nil {
		recordLocalPushIntegrityFailure(model, key.Client, "push_reconciliation_failed")
	}
	return pushHTTPResult(key, ReplayDispositionExecuted, HTTPObservation{Status: 200, Body: cloneBytes(response)}, batchMutationObservations(outcomes)), nil
}

func executeFirstPush(ctx context.Context, model *Model, envelope pushSubmitEnvelope, batch parsedPushBatch, key BatchKey, mutationReplay map[MutationID]MutationLedger) ([]byte, []MutationOutcome, bool, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, false, time.Time{}, err
	}
	serverTime := canonicalClockTime(model.clock.Now())
	currentManifest, err := loadPushManifest(model.state, batch.Schema)
	if err != nil {
		return nil, nil, false, time.Time{}, fmt.Errorf("push/submit: load current schema: %w", err)
	}

	evaluated := make([]evaluatedPushMutation, 0, len(batch.Mutations))
	transitions := make([]sourceTransition, 0, len(batch.Mutations))
	acceptedWrite := false
	for index, mutation := range batch.Mutations {
		if err := ctx.Err(); err != nil {
			return nil, nil, false, time.Time{}, err
		}
		if replay, exists := mutationReplay[mutation.Mutation]; exists {
			stored, err := decodeStoredPushOutcome(replay.Outcome.Response)
			if err != nil {
				return nil, nil, false, time.Time{}, fmt.Errorf("push/submit: decode mutation replay: %w", err)
			}
			evaluated = append(evaluated, evaluatedPushMutation{
				Parsed:       mutation,
				Outcome:      stored.Wire,
				OutcomeBytes: cloneBytes(replay.Outcome.Response),
				State:        replay.Outcome.State,
				Reason:       replay.Outcome.Reason,
			})
			_ = index
			continue
		}

		evaluation, transition, err := evaluateNewPushMutation(model, currentManifest, mutation, key, serverTime, len(transitions)+1)
		if err != nil {
			return nil, nil, false, time.Time{}, err
		}
		if evaluation.Accepted {
			acceptedWrite = true
			applySourceTransition(model, transition)
			transitions = append(transitions, transition)
		}
		evaluated = append(evaluated, evaluation)
	}

	if acceptedWrite {
		if model.state.Clients[key.Client].AcceptedWriteEpoch == math.MaxUint64 {
			return nil, nil, false, time.Time{}, errors.New("push/submit: accepted-write epoch exhausted")
		}
		client := model.state.Clients[key.Client]
		client.AcceptedWriteEpoch++
		model.state.Clients[key.Client] = client
	}
	if len(transitions) > 0 {
		if err := appendSourceTransaction(model, key, envelope, serverTime, transitions); err != nil {
			return nil, nil, false, time.Time{}, err
		}
	}

	accepted := make([]json.RawMessage, 0, len(evaluated))
	rejected := make([]json.RawMessage, 0, len(evaluated))
	mutationOutcomes := make([]MutationOutcome, 0, len(evaluated))
	for ordinal, evaluation := range evaluated {
		if len(evaluation.OutcomeBytes) == 0 {
			bytes, err := canonicalPushOutcome(evaluation.Outcome)
			if err != nil {
				return nil, nil, false, time.Time{}, err
			}
			evaluation.OutcomeBytes = bytes
			evaluated[ordinal].OutcomeBytes = bytes
		}
		if evaluation.State == MutationOutcomeApplied {
			accepted = append(accepted, json.RawMessage(cloneBytes(evaluation.OutcomeBytes)))
		} else {
			rejected = append(rejected, json.RawMessage(cloneBytes(evaluation.OutcomeBytes)))
		}
		mutationOutcomes = append(mutationOutcomes, MutationOutcome{
			Mutation: evaluation.Parsed.Mutation,
			State:    evaluation.State,
			Reason:   evaluation.Reason,
			Response: cloneBytes(evaluation.OutcomeBytes),
		})
	}
	response, err := canonicalPushResponse(pushResponse{
		BatchID:    string(batch.Batch),
		ServerTime: formatCanonicalTime(serverTime),
		Accepted:   accepted,
		Rejected:   rejected,
	})
	if err != nil {
		return nil, nil, false, time.Time{}, err
	}
	if len(response) > pushResponseLimit {
		return nil, nil, false, time.Time{}, errors.New("push/submit: canonical response exceeds byte limit")
	}

	for ordinal, evaluation := range evaluated {
		if _, replayed := mutationReplay[evaluation.Parsed.Mutation]; replayed {
			continue
		}
		outcome := mutationOutcomes[ordinal]
		rowIdentity, identityErr := pushLedgerRowIdentity(model.state, currentManifest, evaluation.Parsed)
		if identityErr != nil {
			return nil, nil, false, time.Time{}, fmt.Errorf("push/submit: derive mutation ledger row: %w", identityErr)
		}
		model.state.Mutations[MutationKey{Client: key.Client, Mutation: evaluation.Parsed.Mutation}] = MutationLedger{
			Fingerprint:             fingerprintRecord(pushMutationDomain, evaluation.Parsed.Fingerprint),
			FirstBatch:              batch.Batch,
			RequestOrdinal:          uint64(ordinal + 1),
			Table:                   evaluation.Parsed.Table,
			Row:                     rowIdentity,
			Operation:               evaluation.Parsed.Operation,
			AuthoredSchema:          evaluation.Parsed.AuthoredSchema,
			SubmittedSchema:         batch.Schema,
			OutcomeSchema:           schemaRefFromWire(evaluation.Outcome.OutcomeSchema),
			SealedCanonicalRequest:  cloneBytes(evaluation.Parsed.CanonicalMutation),
			SealedCanonicalResponse: cloneBytes(outcome.Response),
			Outcome:                 cloneMutationOutcomeForPush(outcome),
			ResolvedAt:              cloneTime(&serverTime),
		}
	}
	model.state.Batches[key] = BatchLedger{
		Fingerprint:             fingerprintRecord(pushBatchDomain, batch.Fingerprint),
		ProtocolVersion:         supportedProtocolVersion,
		ClientGeneration:        batch.Generation,
		Schema:                  batch.Schema,
		SealedCanonicalRequest:  cloneBytes(batch.CanonicalRequest),
		SealedCanonicalResponse: cloneBytes(response),
		Execution:               BatchExecutionCompleted,
		Mutations:               mutationIDs(batch.Mutations),
		Outcomes:                cloneMutationOutcomes(outcomeValues(mutationOutcomes)),
		HTTPStatus:              200,
		ServerTime:              cloneTime(&serverTime),
		CreatedAt:               cloneTime(&serverTime),
		CompletedAt:             cloneTime(&serverTime),
		SealedAt:                cloneTime(&serverTime),
	}
	return response, mutationOutcomes, acceptedWrite, serverTime, nil
}

func evaluateNewPushMutation(model *Model, manifest pushManifest, mutation parsedPushMutation, batchKey BatchKey, serverTime time.Time, dmlOrdinal int) (evaluatedPushMutation, sourceTransition, error) {
	outcome := pushOutcomeWire{
		MutationID:    string(mutation.Mutation),
		Table:         string(mutation.Table),
		PK:            map[string]json.RawMessage{string(mutation.PKField): cloneBytes(mutation.PKValue)},
		OutcomeSchema: pushSchemaWireFromRef(manifest.Reference),
	}
	authoredManifest, authoredErr := loadPushManifest(model.state, mutation.AuthoredSchema)
	if authoredErr != nil {
		return terminalPushEvaluation(mutation, outcome, "schema_incompatible", "authored mutation cannot be represented by the current schema", true, mutationFieldNames(mutation))
	}
	authoredTable, authoredTableOK := authoredManifest.Tables[mutation.Table]
	currentTable, currentTableOK := manifest.Tables[mutation.Table]
	if !authoredTableOK {
		if tableWasEverSynced(model.state, mutation.Table) {
			return terminalPushEvaluation(mutation, outcome, "schema_incompatible", "authored mutation cannot be represented by the current schema", true, mutationFieldNames(mutation))
		}
		return terminalPushEvaluation(mutation, outcome, "table_not_synced", "target table is not registered for synchronization", false, nil)
	}
	if !currentTableOK {
		return terminalPushEvaluation(mutation, outcome, "schema_incompatible", "authored mutation cannot be represented by the current schema", true, mutationFieldNames(mutation))
	}
	if fields := incompatibleAuthoredMutationFields(mutation, authoredTable, currentTable); fields != nil {
		return terminalPushEvaluation(mutation, outcome, "schema_incompatible", "authored mutation cannot be represented by the current schema", true, fields)
	}
	if policy, explicit := pushWritePolicy(model.state.Authorization, batchKey.Client.UserID, mutation.Table); explicit && !policy {
		return terminalPushEvaluation(mutation, outcome, "policy_rejected", "authenticated write policy rejected the mutation", false, nil)
	}
	if mutation.PKField != currentTable.PrimaryKeyFieldID {
		return terminalPushEvaluation(mutation, outcome, "validation_failed", "primary-key field does not match the current schema", false, nil)
	}
	if err := validatePushColumns(manifest, currentTable, mutation.Operation, mutation.Columns); err != nil {
		return terminalPushEvaluation(mutation, outcome, "validation_failed", boundedPushMessage(err.Error()), false, nil)
	}

	identity, err := derivePushRowIdentity(manifest, mutation.Table, mutation.PKField, mutation.PKValue)
	if err != nil {
		return terminalPushEvaluation(mutation, outcome, "validation_failed", "primary-key value is invalid", false, nil)
	}
	relation, registered := currentSyncedRelation(model.state, mutation.Table)
	if !registered || relation.Relation != currentTable.Relation || relation.PrimaryKeyFieldID != currentTable.PrimaryKeyFieldID || relation.Drifted || relation.CaptureBlocked || model.state.Stream.Authority.ActiveGeneration == "" {
		return evaluatedPushMutation{}, sourceTransition{}, fmt.Errorf("%w: active WAL capture is unavailable", errPushOperational)
	}
	current, found, fenceOnlyDeleted, fenceVersion, err := currentSourceRow(model.state, identity)
	if err != nil {
		return evaluatedPushMutation{}, sourceTransition{}, err
	}
	if fenceOnlyDeleted {
		return conflictPushEvaluation(mutation, outcome, "row_deleted", "the row has been deleted", nil, fenceVersion, manifest, currentTable, identity), sourceTransition{}, nil
	}
	if found && current.Deleted {
		evaluation, err := conflictPushEvaluationChecked(mutation, outcome, "row_deleted", "the row has been deleted", &current, current.Version, manifest, currentTable, identity)
		return evaluation, sourceTransition{}, err
	}

	switch mutation.Operation {
	case DMLOperationInsert:
		if found {
			evaluation, err := conflictPushEvaluationChecked(mutation, outcome, "row_already_exists", "the row already exists", &current, current.Version, manifest, currentTable, identity)
			return evaluation, sourceTransition{}, err
		}
		after, err := buildPushInsertRow(manifest, currentTable, identity, mutation.PKValue, mutation.Columns, serverTime)
		if err != nil {
			return terminalPushEvaluation(mutation, outcome, "validation_failed", boundedPushMessage(err.Error()), false, nil)
		}
		version := allocatePushVersion(model, identity, mutation, batchKey, dmlOrdinal)
		after.Version = version
		checksum, err := pushRowChecksum(manifest, mutation.Table, after, version)
		if err != nil {
			return evaluatedPushMutation{}, sourceTransition{}, err
		}
		after.Checksum = checksum
		outcome.Status = string(MutationOutcomeApplied)
		outcome.ServerRow = authoritativeRowWire(after)
		outcome.RowChecksum = &pushChecksumWire{Algorithm: "sha256", Version: 1, Encoding: "hex", Digest: hex.EncodeToString(checksum[:])}
		outcome.ServerVersion = stringPointer(string(version))
		return acceptedPushEvaluation(mutation, outcome, relation, nil, after), sourceTransition{Parsed: mutation, Relation: relation, Operation: mutation.Operation, After: after, SoftDelete: currentTable.DeletedFieldID != nil}, nil
	case DMLOperationUpdate:
		if !found {
			return conflictPushEvaluation(mutation, outcome, "row_not_found", "the row does not exist", nil, "", manifest, currentTable, identity), sourceTransition{}, nil
		}
		if mutation.BaseVersion == nil || string(*mutation.BaseVersion) != string(current.Version) {
			evaluation, err := conflictPushEvaluationChecked(mutation, outcome, "version_conflict", "the base version does not match the current row", &current, current.Version, manifest, currentTable, identity)
			return evaluation, sourceTransition{}, err
		}
		after := cloneAuthoritativeRowForPush(current)
		after.Identity = identity
		after.FieldValues = mergeAuthoritativeFields(after.FieldValues, mutation.Columns, currentTable, serverTime, false)
		after.Deleted = false
		after.DeletedAt = nil
		setPushLifecycleFields(&after, currentTable, serverTime, false, false)
		version := allocatePushVersion(model, identity, mutation, batchKey, dmlOrdinal)
		after.Version = version
		checksum, err := pushRowChecksum(manifest, mutation.Table, after, version)
		if err != nil {
			return evaluatedPushMutation{}, sourceTransition{}, err
		}
		after.Checksum = checksum
		outcome.Status = string(MutationOutcomeApplied)
		outcome.ServerRow = authoritativeRowWire(after)
		outcome.RowChecksum = &pushChecksumWire{Algorithm: "sha256", Version: 1, Encoding: "hex", Digest: hex.EncodeToString(checksum[:])}
		outcome.ServerVersion = stringPointer(string(version))
		return acceptedPushEvaluation(mutation, outcome, relation, &current, after), sourceTransition{Parsed: mutation, Relation: relation, Operation: mutation.Operation, Before: &current, After: after, SoftDelete: currentTable.DeletedFieldID != nil}, nil
	case DMLOperationDelete:
		if !found {
			return conflictPushEvaluation(mutation, outcome, "row_not_found", "the row does not exist", nil, "", manifest, currentTable, identity), sourceTransition{}, nil
		}
		if mutation.BaseVersion == nil || string(*mutation.BaseVersion) != string(current.Version) {
			evaluation, err := conflictPushEvaluationChecked(mutation, outcome, "version_conflict", "the base version does not match the current row", &current, current.Version, manifest, currentTable, identity)
			return evaluation, sourceTransition{}, err
		}
		after := cloneAuthoritativeRowForPush(current)
		after.Identity = identity
		version := allocatePushVersion(model, identity, mutation, batchKey, dmlOrdinal)
		after.Version = version
		setPushLifecycleFields(&after, currentTable, serverTime, true, true)
		after.Deleted = true
		after.DeletedAt = timePointer(serverTime)
		if currentTable.DeletedFieldID == nil {
			// A hard-delete relation leaves a durable fence without a source tombstone.
			outcome.Status = string(MutationOutcomeApplied)
			outcome.ServerVersion = stringPointer(string(version))
			return acceptedPushEvaluation(mutation, outcome, relation, &current, after), sourceTransition{Parsed: mutation, Relation: relation, Operation: DMLOperationDelete, Before: &current, After: after}, nil
		}
		checksum, err := pushRowChecksum(manifest, mutation.Table, after, version)
		if err != nil {
			return evaluatedPushMutation{}, sourceTransition{}, err
		}
		after.Checksum = checksum
		outcome.Status = string(MutationOutcomeApplied)
		outcome.ServerRow = authoritativeRowWire(after)
		outcome.RowChecksum = &pushChecksumWire{Algorithm: "sha256", Version: 1, Encoding: "hex", Digest: hex.EncodeToString(checksum[:])}
		outcome.ServerVersion = stringPointer(string(version))
		return acceptedPushEvaluation(mutation, outcome, relation, &current, after), sourceTransition{Parsed: mutation, Relation: relation, Operation: DMLOperationUpdate, Before: &current, After: after, SoftDelete: true}, nil
	default:
		return terminalPushEvaluation(mutation, outcome, "validation_failed", "unsupported mutation operation", false, nil)
	}
}

func acceptedPushEvaluation(mutation parsedPushMutation, outcome pushOutcomeWire, relation RelationDefinition, before *AuthoritativeRow, after AuthoritativeRow) evaluatedPushMutation {
	return evaluatedPushMutation{
		Parsed: mutation, Outcome: outcome, State: MutationOutcomeApplied,
		Accepted: true, NewWrite: true, Before: before, After: &after, Relation: relation,
	}
}

func terminalPushEvaluation(mutation parsedPushMutation, outcome pushOutcomeWire, code, message string, incompatible bool, fields []string) (evaluatedPushMutation, sourceTransition, error) {
	outcome.Status = string(MutationOutcomeRejectedTerminal)
	outcome.Code = stringPointer(code)
	outcome.Message = stringPointer(boundedPushMessage(message))
	if incompatible {
		falseValue := false
		outcome.Retryable = &falseValue
		if fields == nil {
			fields = []string{}
		}
		sort.Strings(fields)
		outcome.AuthoredSchema = schemaPointer(pushSchemaWireFromRef(mutation.AuthoredSchema))
		outcome.CurrentSchema = schemaPointer(outcome.OutcomeSchema)
		copy := append([]string(nil), fields...)
		outcome.IncompatibleFields = &copy
	}
	return evaluatedPushMutation{Parsed: mutation, Outcome: outcome, State: MutationOutcomeRejectedTerminal, Reason: ReasonCode(code)}, sourceTransition{}, nil
}

func conflictPushEvaluation(mutation parsedPushMutation, outcome pushOutcomeWire, code, message string, row *AuthoritativeRow, version RowVersion, manifest pushManifest, table pushManifestTable, identity RowIdentity) evaluatedPushMutation {
	outcome.Status = string(MutationOutcomeConflict)
	outcome.Code = stringPointer(code)
	outcome.Message = stringPointer(boundedPushMessage(message))
	if row != nil {
		outcome.ServerRow = authoritativeRowWire(*row)
		checksum, err := pushRowChecksum(manifest, table.ID, *row, version)
		if err != nil {
			return evaluatedPushMutation{Parsed: mutation, Outcome: outcome, State: MutationOutcomeConflict, Reason: ReasonCode(code)}
		}
		outcome.RowChecksum = &pushChecksumWire{Algorithm: "sha256", Version: 1, Encoding: "hex", Digest: hex.EncodeToString(checksum[:])}
		if version != "" {
			outcome.ServerVersion = stringPointer(string(version))
		}
	}
	if version != "" && outcome.ServerVersion == nil {
		outcome.ServerVersion = stringPointer(string(version))
	}
	return evaluatedPushMutation{Parsed: mutation, Outcome: outcome, State: MutationOutcomeConflict, Reason: ReasonCode(code)}
}

// conflictPushEvaluationChecked is the conflict constructor used by first
// execution. A row-bearing conflict must carry a verified checksum.
func conflictPushEvaluationChecked(mutation parsedPushMutation, outcome pushOutcomeWire, code, message string, row *AuthoritativeRow, version RowVersion, manifest pushManifest, table pushManifestTable, identity RowIdentity) (evaluatedPushMutation, error) {
	if row != nil {
		checksum, err := pushRowChecksum(manifest, table.ID, *row, version)
		if err != nil {
			return evaluatedPushMutation{}, fmt.Errorf("%w: conflict row checksum: %v", errPushOperational, err)
		}
		outcome.ServerRow = authoritativeRowWire(*row)
		outcome.RowChecksum = &pushChecksumWire{Algorithm: "sha256", Version: 1, Encoding: "hex", Digest: hex.EncodeToString(checksum[:])}
	}
	if version != "" {
		outcome.ServerVersion = stringPointer(string(version))
	}
	outcome.Status = string(MutationOutcomeConflict)
	outcome.Code = stringPointer(code)
	outcome.Message = stringPointer(boundedPushMessage(message))
	return evaluatedPushMutation{Parsed: mutation, Outcome: outcome, State: MutationOutcomeConflict, Reason: ReasonCode(code)}, nil
}

func equalFingerprintRecord(left, right FingerprintRecord) bool {
	return left.Algorithm == right.Algorithm && left.Version == right.Version && left.Domain == right.Domain && left.Digest == right.Digest
}

func latestUnresolvedSameRow(queue []QueuedMutation, identity RowIdentity) (MutationID, bool) {
	var selected QueuedMutation
	found := false
	for _, mutation := range queue {
		if mutation.Row.CanonicalIdentityBytes != identity.CanonicalIdentityBytes || !pushUnresolvedBeforeSend(mutation.Status) {
			continue
		}
		if !found || mutation.LocalOrder > selected.LocalOrder || mutation.LocalOrder == selected.LocalOrder && mutation.Mutation > selected.Mutation {
			selected = mutation
			found = true
		}
	}
	if !found {
		return "", false
	}
	return selected.Mutation, true
}

func pushUnresolvedBeforeSend(status LocalMutationStatus) bool {
	return status == LocalMutationStatusPending || status == LocalMutationStatusSealed
}

func pushLedgerRowIdentity(state State, currentManifest pushManifest, mutation parsedPushMutation) (RowIdentity, error) {
	if identity, err := derivePushRowIdentity(currentManifest, mutation.Table, mutation.PKField, mutation.PKValue); err == nil {
		return identity, nil
	}
	if authored, err := loadPushManifest(state, mutation.AuthoredSchema); err == nil {
		if identity, identityErr := derivePushRowIdentity(authored, mutation.Table, mutation.PKField, mutation.PKValue); identityErr == nil {
			return identity, nil
		}
	}
	canonicalPK, err := canonicalJSONValue(mutation.PKValue)
	if err != nil {
		return RowIdentity{}, err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("synchro:v3:push-ledger-row:v1\x00"))
	_, _ = hash.Write([]byte(mutation.Table))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(mutation.PKField))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(canonicalPK)
	digest := hash.Sum(nil)
	return RowIdentity{
		CanonicalIdentityBytes: "push-ledger:" + hex.EncodeToString(digest),
		TableID:                mutation.Table,
		PrimaryKeyFieldID:      mutation.PKField,
		PortableType:           PortableType("json"),
		CanonicalWireJSON:      string(canonicalPK),
	}, nil
}

func mutationFieldNames(mutation parsedPushMutation) []string {
	fields := make(map[string]struct{}, len(mutation.Columns))
	for _, column := range mutation.Columns {
		fields[string(column.Field)] = struct{}{}
	}
	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}
	sort.Strings(result)
	return result
}

func tableWasEverSynced(state State, tableID TableID) bool {
	for _, manifest := range state.Schemas {
		for _, table := range manifest.Tables {
			if table.ID == tableID {
				return true
			}
		}
	}
	for _, generation := range state.Registry.Generations {
		for _, relation := range generation.Relations {
			definition := relation.Definition
			if definition.RegistrationKind == RegistrationKindSynced && definition.HasTableID && definition.TableID == tableID {
				return true
			}
		}
	}
	for _, relation := range state.Relations {
		definition := relation.Definition
		if definition.RegistrationKind == RegistrationKindSynced && definition.HasTableID && definition.TableID == tableID {
			return true
		}
	}
	return false
}

func pushWritePolicy(authorization AuthorizationState, user UserID, table TableID) (bool, bool) {
	var allowed bool
	found := false
	for _, decision := range authorization.WritePolicies {
		if decision.User != user || decision.Table != table {
			continue
		}
		if found && decision.Allowed != allowed {
			return false, true
		}
		allowed = decision.Allowed
		found = true
	}
	return allowed, found
}

func incompatibleAuthoredMutationFields(mutation parsedPushMutation, authored, current pushManifestTable) []string {
	fields := make(map[string]struct{})
	tableIncompatible := authored.ID != current.ID || authored.Relation != current.Relation || authored.Composition != current.Composition
	if authored.PrimaryKeyFieldID != current.PrimaryKeyFieldID || mutation.PKField != authored.PrimaryKeyFieldID || mutation.PKField != current.PrimaryKeyFieldID {
		fields[string(mutation.PKField)] = struct{}{}
	}
	authoredPK, authoredPKOK := authored.Fields[authored.PrimaryKeyFieldID]
	currentPK, currentPKOK := current.Fields[current.PrimaryKeyFieldID]
	if !authoredPKOK || !currentPKOK || !pushFieldsCompatible(authoredPK, currentPK) {
		fields[string(mutation.PKField)] = struct{}{}
	}
	for _, column := range mutation.Columns {
		authoredField, authoredOK := authored.Fields[column.Field]
		currentField, currentOK := current.Fields[column.Field]
		if !authoredOK || !currentOK || !pushFieldsCompatible(authoredField, currentField) {
			fields[string(column.Field)] = struct{}{}
		}
	}
	if !tableIncompatible && len(fields) == 0 {
		return nil
	}
	result := make([]string, 0, len(fields))
	for field := range fields {
		if field != "" {
			result = append(result, field)
		}
	}
	sort.Strings(result)
	return result
}

func pushFieldsCompatible(authored, current pushManifestField) bool {
	if authored.Portable != current.Portable {
		return false
	}
	if authored.Nullable && !current.Nullable {
		return false
	}
	if authored.Writable && !current.Writable {
		return false
	}
	if authored.Portable == PortableType("decimal") && !pushDecimalCompatible(authored, current) {
		return false
	}
	return true
}

func pushDecimalCompatible(authored, current pushManifestField) bool {
	if authored.Precision == nil || authored.Scale == nil || current.Precision == nil || current.Scale == nil {
		return authored.Precision == nil && authored.Scale == nil && current.Precision == nil && current.Scale == nil
	}
	if *current.Precision < *authored.Precision || *current.Scale < *authored.Scale {
		return false
	}
	return *current.Precision-*current.Scale >= *authored.Precision-*authored.Scale
}

func queuedMutationMatchesParsed(queued QueuedMutation, parsed parsedPushMutation) bool {
	if queued.Mutation != parsed.Mutation || queued.Table != parsed.Table || queued.Operation != parsed.Operation || queued.AuthoredSchema != parsed.AuthoredSchema || queued.ClientVersion != parsed.ClientVersion || queued.Row.TableID != parsed.Table || queued.Row.PrimaryKeyFieldID != parsed.PKField {
		return false
	}
	if queued.HasBaseVersion != (parsed.BaseVersion != nil) {
		return false
	}
	if queued.HasBaseVersion && queued.BaseVersion != *parsed.BaseVersion {
		return false
	}
	queuedPK, err := canonicalJSONValue([]byte(queued.Row.CanonicalWireJSON))
	if err != nil {
		return false
	}
	parsedPK, err := canonicalJSONValue(parsed.PKValue)
	if err != nil || !bytes.Equal(queuedPK, parsedPK) {
		return false
	}
	queuedColumns := append([]FieldValue(nil), queued.AuthoredColumns...)
	parsedColumns := fieldValuesFromParsedMutation(parsed)
	if len(queuedColumns) != len(parsedColumns) {
		return false
	}
	sort.Slice(queuedColumns, func(left, right int) bool { return queuedColumns[left].Field < queuedColumns[right].Field })
	for index := range queuedColumns {
		if queuedColumns[index].Field != parsedColumns[index].Field {
			return false
		}
		left, leftErr := canonicalJSONValue([]byte(queuedColumns[index].WireJSON))
		right, rightErr := canonicalJSONValue([]byte(parsedColumns[index].WireJSON))
		if leftErr != nil || rightErr != nil || !bytes.Equal(left, right) {
			return false
		}
	}
	return true
}

func appendSourceTransaction(model *Model, key BatchKey, envelope pushSubmitEnvelope, serverTime time.Time, transitions []sourceTransition) error {
	streamGeneration := model.state.Stream.Authority.ActiveGeneration
	if streamGeneration == "" {
		return fmt.Errorf("%w: active stream generation is absent", errPushOperational)
	}
	transactionKey := TransactionReplayKey{StreamGeneration: streamGeneration, CommitLSN: CommitLSN(envelope.CommitLSN)}
	for _, existing := range model.state.Stream.Transactions {
		if existing.ReplayKey == transactionKey {
			return errors.New("push/submit: source transaction replay identity already exists")
		}
	}
	transaction := StreamTransaction{
		ReplayKey:          transactionKey,
		End:                StreamPosition{StreamGeneration: streamGeneration, Kind: PositionKindTransactionEnd, CommitLSN: CommitLSN(envelope.CommitLSN)},
		EndLSN:             EndLSN(envelope.EndLSN),
		RegistryGeneration: model.state.Registry.CurrentGeneration,
		Lifecycle:          TransactionLifecycleCommitted,
		CommittedAt:        timePointer(serverTime),
		Events:             make([]SourceEvent, 0, len(transitions)),
	}
	for ordinal, transition := range transitions {
		eventOrdinal := EventOrdinal(ordinal + 1)
		eventKey := EventReplayKey{Transaction: transactionKey, EventOrdinal: eventOrdinal}
		position := StreamPosition{StreamGeneration: streamGeneration, Kind: PositionKindEffect, CommitLSN: CommitLSN(envelope.CommitLSN), EventOrdinal: eventOrdinal, EffectOrdinal: 1}
		before, hasBefore := sourceImageFromRow(transition.Before, false)
		hardDelete := transition.Operation == DMLOperationDelete && !transition.SoftDelete
		after, hasAfter := sourceImageFromRow(&transition.After, false)
		if hardDelete {
			after = SourceImage{}
			hasAfter = false
		}
		transaction.Events = append(transaction.Events, SourceEvent{
			ReplayKey:  eventKey,
			Position:   position,
			Relation:   transition.Relation.Relation,
			Operation:  transition.Operation,
			HasBefore:  hasBefore,
			Before:     before,
			HasAfter:   hasAfter,
			After:      after,
			CapturedAt: timePointer(serverTime),
		})
		fenceID := allocateFenceID(model, key, transition.Parsed.Mutation, ordinal+1)
		if _, exists := model.state.Fences[fenceID]; exists {
			return errors.New("push/submit: write-fence identity collision")
		}
		fence := VersionFence{
			ID:                fenceID,
			RegistrationKind:  RegistrationKindSynced,
			Relation:          transition.Relation.Relation,
			Physical:          transition.Relation.Physical,
			Operation:         transition.Operation,
			DMLOrdinal:        uint64(ordinal + 1),
			RowVersion:        transition.After.Version,
			HasEventReplayKey: true,
			EventReplayKey:    eventKey,
			HasMutationKey:    true,
			MutationKey:       MutationKey{Client: key.Client, Mutation: transition.Parsed.Mutation},
			Coverage:          FenceCoveragePending,
		}
		if transition.Before != nil {
			fence.HasOldRegisteredIdentity = true
			fence.OldRegisteredIdentity = RegisteredIdentity{Kind: RegistrationKindSynced, SyncedRow: transition.Before.Identity}
		}
		if hasAfter {
			fence.HasNewRegisteredIdentity = true
			fence.NewRegisteredIdentity = RegisteredIdentity{Kind: RegistrationKindSynced, SyncedRow: transition.After.Identity}
		}
		model.state.Fences[fenceID] = fence
	}
	model.state.Stream.Transactions = append(model.state.Stream.Transactions, transaction)
	for _, transition := range transitions {
		if transition.Operation != DMLOperationDelete || transition.SoftDelete {
			continue
		}
		for index, entry := range model.state.Stream.SourceRows {
			if entry.Identity == transition.After.Identity || entry.Identity.CanonicalIdentityBytes == transition.After.Identity.CanonicalIdentityBytes {
				model.state.Stream.SourceRows = append(model.state.Stream.SourceRows[:index], model.state.Stream.SourceRows[index+1:]...)
				break
			}
		}
	}
	return nil
}

func applySourceTransition(model *Model, transition sourceTransition) {
	identity := transition.After.Identity
	for index := range model.state.Stream.SourceRows {
		entry := &model.state.Stream.SourceRows[index]
		if entry.Identity == identity || entry.Identity.CanonicalIdentityBytes == identity.CanonicalIdentityBytes {
			entry.Identity = identity
			entry.Row = cloneAuthoritativeRowForPush(transition.After)
			return
		}
	}
	model.state.Stream.SourceRows = append(model.state.Stream.SourceRows, SourceRowEntry{Identity: identity, Row: cloneAuthoritativeRowForPush(transition.After)})
}

func sourceImageFromRow(row *AuthoritativeRow, deleted bool) (SourceImage, bool) {
	if row == nil {
		return SourceImage{}, false
	}
	return SourceImage{
		Identity:    RegisteredIdentity{Kind: RegistrationKindSynced, SyncedRow: row.Identity},
		Fields:      cloneFieldValues(row.FieldValues),
		Version:     row.Version,
		HasChecksum: row.Checksum != (Checksum{}),
		Checksum:    row.Checksum,
		Deleted:     deleted || row.Deleted,
	}, true
}

func replayCompletedBatch(model *Model, envelope pushSubmitEnvelope, batch parsedPushBatch, key BatchKey, ledger BatchLedger) (StepResult, error) {
	if envelope.Delivery == "transport_failure" {
		markLocalPushBackoff(model, key, RetryClassificationTransport)
		return pushFailureResult(key, ReplayDispositionReplayed, 503, pushHTTPTemporaryUnavailable, true, nil), nil
	}
	if envelope.Delivery == "drop_after_server" {
		if err := markLocalBatchResponseLost(model, batch, key, ledger.HTTPStatus); err != nil {
			return StepResult{}, err
		}
		markLocalPushBackoff(model, key, RetryClassificationTransport)
		appendPushEvent(model, key.Client, ModelEventResponseLoss, "response_lost", transactionForBatch(model, key))
		return pushFailureResult(key, ReplayDispositionReplayed, 503, pushHTTPTemporaryUnavailable, true, nil), nil
	}
	if ledger.HTTPStatus != 200 || len(ledger.SealedCanonicalResponse) == 0 {
		markLocalPushBackoff(model, key, RetryClassificationUnavailable)
		return pushFailureResult(key, ReplayDispositionReplayed, 503, pushHTTPTemporaryUnavailable, true, nil), nil
	}
	if err := reconcileLocalBatch(model, key.Client, key.Batch, ledger, true); err != nil {
		recordLocalPushIntegrityFailure(model, key.Client, "push_reconciliation_failed")
	}
	return pushHTTPResult(key, ReplayDispositionReplayed, HTTPObservation{Status: ledger.HTTPStatus, Body: cloneBytes(ledger.SealedCanonicalResponse)}, batchMutationObservations(ledger.Outcomes)), nil
}

func reconcileLocalBatchFromOutcomes(model *Model, batch parsedPushBatch, key BatchKey, response []byte, outcomes []MutationOutcome, first bool) error {
	ledger, ok := model.state.Batches[key]
	if !ok {
		return errors.New("push/submit: completed batch ledger is missing")
	}
	return reconcileLocalBatchWithResponse(model, key.Client, key.Batch, ledger, response, outcomes, first)
}

func reconcileLocalBatch(model *Model, client ClientKey, batch BatchID, ledger BatchLedger, first bool) error {
	return reconcileLocalBatchWithResponse(model, client, batch, ledger, ledger.SealedCanonicalResponse, ledger.Outcomes, first)
}

func reconcileLocalBatchWithResponse(model *Model, client ClientKey, batch BatchID, ledger BatchLedger, response []byte, outcomes []MutationOutcome, _ bool) error {
	local, ok := model.state.ClientLocal[client]
	if !ok {
		return nil
	}
	local = cloneClientLocalState(local)
	batchIndex := localSealedBatchIndex(local.SealedBatches, batch)
	if batchIndex < 0 {
		return nil
	}
	sealed := &local.SealedBatches[batchIndex]
	if sealed.HasCanonicalResponse && !bytes.Equal(sealed.CanonicalResponse, response) {
		return errors.New("reconcile local push outcome: previously stored response differs from the completed response")
	}
	if sealed.State == LocalSealedBatchStateReconciled && sealed.HasCanonicalResponse {
		return nil
	}
	if ledger.Execution != BatchExecutionCompleted || ledger.HTTPStatus != 200 || !bytes.Equal(response, ledger.SealedCanonicalResponse) {
		return errors.New("reconcile local push outcome: batch response is not the completed sealed response")
	}
	if err := validateLocalPushOutcomePartition(*sealed, response, outcomes); err != nil {
		return err
	}
	for _, outcome := range outcomes {
		stored, err := decodeStoredPushOutcome(outcome.Response)
		if err != nil {
			return fmt.Errorf("reconcile local push outcome: %w", err)
		}
		if err := reconcileOneLocalOutcome(model.state, &local, outcome, stored); err != nil {
			return err
		}
	}
	sealed.State = LocalSealedBatchStateReconciled
	sealed.HasCanonicalResponse = true
	sealed.CanonicalResponse = cloneBytes(response)
	sealed.HTTPStatus = ledger.HTTPStatus
	sealed.ReconciledAt = cloneTime(ledger.CompletedAt)
	if sealed.ReconciledAt == nil {
		sealed.ReconciledAt = cloneTime(ledger.ServerTime)
	}
	local.ErrorState = nil
	if local.Backoff != nil && local.Backoff.Work.Kind == ResumableWorkPush && local.Backoff.Work.HasBatch && local.Backoff.Work.Batch == (BatchKey{Client: client, Batch: batch}) {
		local.Backoff = nil
	}
	model.state.ClientLocal[client] = local
	return nil
}

func validateLocalPushOutcomePartition(sealed LocalSealedBatch, response []byte, outcomes []MutationOutcome) error {
	if len(response) == 0 || len(response) > pushResponseLimit {
		return errors.New("reconcile local push outcome: response is outside the bounded size")
	}
	object, err := strictObject(response)
	if err != nil {
		return fmt.Errorf("reconcile local push outcome: invalid response object: %w", err)
	}
	if err := requirePushKeys(object, []string{"batch_id", "server_time", "accepted", "rejected"}, nil); err != nil {
		return fmt.Errorf("reconcile local push outcome: response shape: %w", err)
	}
	batchID, err := requiredJSONString(object["batch_id"], "batch_id")
	if err != nil || BatchID(batchID) != sealed.Batch {
		return errors.New("reconcile local push outcome: response batch identity differs from sealed batch")
	}
	serverTime, err := requiredJSONString(object["server_time"], "server_time")
	if err != nil {
		return err
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000000Z", serverTime); err != nil || formatCanonicalTime(parseCanonicalTimeOrZero(serverTime)) != serverTime {
		return errors.New("reconcile local push outcome: response server time is not canonical")
	}
	accepted, err := parseJSONArray(object["accepted"], "accepted")
	if err != nil {
		return err
	}
	rejected, err := parseJSONArray(object["rejected"], "rejected")
	if err != nil {
		return err
	}
	canonical, err := jcs.Transform(response)
	if err != nil || !bytes.Equal(canonical, response) {
		return errors.New("reconcile local push outcome: response is not canonical JSON")
	}

	requestOrder := make(map[MutationID]int, len(sealed.Mutations))
	for index, mutation := range sealed.Mutations {
		if mutation == "" {
			return errors.New("reconcile local push outcome: sealed mutation identity is empty")
		}
		if _, duplicate := requestOrder[mutation]; duplicate {
			return errors.New("reconcile local push outcome: sealed mutation identities are not unique")
		}
		requestOrder[mutation] = index
	}
	responseOutcomes := make(map[MutationID]struct {
		state    MutationOutcomeState
		reason   ReasonCode
		response []byte
	}, len(accepted)+len(rejected))
	lastOrder := -1
	for index, raw := range accepted {
		stored, err := decodeStoredPushOutcome(raw)
		if err != nil {
			return fmt.Errorf("reconcile local push outcome: accepted[%d]: %w", index, err)
		}
		if stored.State != MutationOutcomeApplied {
			return errors.New("reconcile local push outcome: accepted array contains a non-applied mutation")
		}
		canonicalOutcome, err := canonicalPushOutcome(stored.Wire)
		if err != nil || !bytes.Equal(canonicalOutcome, raw) {
			return errors.New("reconcile local push outcome: accepted outcome is not canonical")
		}
		mutation := MutationID(stored.Wire.MutationID)
		order, exists := requestOrder[mutation]
		if !exists || order <= lastOrder {
			return errors.New("reconcile local push outcome: accepted partition is incomplete or out of order")
		}
		lastOrder = order
		if _, duplicate := responseOutcomes[mutation]; duplicate {
			return errors.New("reconcile local push outcome: mutation appears more than once")
		}
		responseOutcomes[mutation] = struct {
			state    MutationOutcomeState
			reason   ReasonCode
			response []byte
		}{state: stored.State, reason: stored.Reason, response: cloneBytes(raw)}
	}
	lastOrder = -1
	for index, raw := range rejected {
		stored, err := decodeStoredPushOutcome(raw)
		if err != nil {
			return fmt.Errorf("reconcile local push outcome: rejected[%d]: %w", index, err)
		}
		if stored.State != MutationOutcomeConflict && stored.State != MutationOutcomeRejectedTerminal {
			return errors.New("reconcile local push outcome: rejected array contains an applied mutation")
		}
		canonicalOutcome, err := canonicalPushOutcome(stored.Wire)
		if err != nil || !bytes.Equal(canonicalOutcome, raw) {
			return errors.New("reconcile local push outcome: rejected outcome is not canonical")
		}
		mutation := MutationID(stored.Wire.MutationID)
		order, exists := requestOrder[mutation]
		if !exists || order <= lastOrder {
			return errors.New("reconcile local push outcome: rejected partition is incomplete or out of order")
		}
		lastOrder = order
		if _, duplicate := responseOutcomes[mutation]; duplicate {
			return errors.New("reconcile local push outcome: mutation appears more than once")
		}
		responseOutcomes[mutation] = struct {
			state    MutationOutcomeState
			reason   ReasonCode
			response []byte
		}{state: stored.State, reason: stored.Reason, response: cloneBytes(raw)}
	}
	if len(responseOutcomes) != len(requestOrder) || len(outcomes) != len(requestOrder) {
		return errors.New("reconcile local push outcome: response does not partition every sealed mutation")
	}
	seen := make(map[MutationID]struct{}, len(outcomes))
	for _, outcome := range outcomes {
		if _, duplicate := seen[outcome.Mutation]; duplicate {
			return errors.New("reconcile local push outcome: durable outcome list contains a duplicate mutation")
		}
		seen[outcome.Mutation] = struct{}{}
		responseOutcome, exists := responseOutcomes[outcome.Mutation]
		if !exists || responseOutcome.state != outcome.State || responseOutcome.reason != outcome.Reason || !bytes.Equal(responseOutcome.response, outcome.Response) {
			return errors.New("reconcile local push outcome: durable outcome differs from canonical response")
		}
	}
	return nil
}

func parseCanonicalTimeOrZero(value string) time.Time {
	parsed, err := time.Parse("2006-01-02T15:04:05.000000Z", value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

func reconcileOneLocalOutcome(state State, local *ClientLocalState, outcome MutationOutcome, stored storedPushOutcome) error {
	queueIndex := queuedMutationIndex(local.DurableQueue, outcome.Mutation)
	if queueIndex < 0 {
		return errors.New("reconcile local push outcome: mutation is absent from the durable queue")
	}
	queued := local.DurableQueue[queueIndex]
	if stored.Wire.MutationID != string(outcome.Mutation) || stored.Wire.Table != string(queued.Table) || stored.State != outcome.State || stored.Reason != outcome.Reason {
		return errors.New("reconcile local push outcome: outcome identity or classification differs from queued intent")
	}
	if len(stored.Wire.PK) != 1 || firstPKField(stored.Wire.PK) != queued.Row.PrimaryKeyFieldID {
		return errors.New("reconcile local push outcome: outcome primary key differs from queued intent")
	}
	pk, err := canonicalJSONValue(firstPKValue(stored.Wire.PK))
	queuedPK, queuedErr := canonicalJSONValue([]byte(queued.Row.CanonicalWireJSON))
	if err != nil || queuedErr != nil || !bytes.Equal(pk, queuedPK) {
		return errors.New("reconcile local push outcome: outcome primary-key value differs from queued intent")
	}

	var authoritative *LocalRow
	if stored.HasRow {
		manifest, err := loadPushManifest(state, stored.OutcomeSchema)
		if err != nil {
			return fmt.Errorf("reconcile local push outcome: load outcome schema: %w", err)
		}
		identity, err := derivePushRowIdentity(manifest, TableID(stored.Wire.Table), firstPKField(stored.Wire.PK), firstPKValue(stored.Wire.PK))
		if err != nil {
			return fmt.Errorf("reconcile local push outcome: derive row identity: %w", err)
		}
		if identity.CanonicalIdentityBytes != queued.Row.CanonicalIdentityBytes || identity.TableID != queued.Row.TableID || identity.PrimaryKeyFieldID != queued.Row.PrimaryKeyFieldID || identity.PortableType != queued.Row.PortableType || !stored.HasVersion || !stored.HasChecksum {
			return errors.New("reconcile local push outcome: row-bearing outcome has incomplete authoritative identity")
		}
		row, err := localRowFromWire(manifest, identity, stored.Wire, stored.Row, stored.Version, stored.Checksum)
		if err != nil {
			return err
		}
		digestRow := AuthoritativeRow{Identity: identity, FieldValues: cloneFieldValues(row.Fields), Version: stored.Version, Deleted: row.Deleted}
		expected, err := pushRowChecksum(manifest, queued.Table, digestRow, stored.Version)
		if err != nil || expected != stored.Checksum {
			return errors.New("reconcile local push outcome: authoritative row checksum does not verify")
		}
		row, err = projectHistoricalLocalRow(state, local.CurrentSchema, stored.OutcomeSchema, queued.Table, row)
		if err != nil {
			return err
		}
		authoritative = &row
	} else if stored.HasChecksum {
		return errors.New("reconcile local push outcome: checksum is present without a server row")
	} else if stored.HasVersion {
		if stored.State != MutationOutcomeApplied && stored.State != MutationOutcomeConflict {
			return errors.New("reconcile local push outcome: terminal rejection contains a server version")
		}
		row := LocalRow{Identity: queued.Row, Deleted: true, HasServerVersion: true, ServerVersion: stored.Version}
		authoritative = &row
	}

	switch outcome.State {
	case MutationOutcomeApplied:
		local.DurableQueue[queueIndex].Status = LocalMutationStatusAccepted
		if !stored.HasVersion {
			return errors.New("reconcile local push outcome: accepted mutation has no server version")
		}
		refreshAcceptedSuccessors(local, queueIndex, stored.Version)
	case MutationOutcomeConflict, MutationOutcomeRejectedTerminal:
		local.DurableQueue[queueIndex].Status = LocalMutationStatusServerRejected
		blockRejectedSuccessors(local, queueIndex)
	default:
		return errors.New("reconcile local push outcome: unknown mutation state")
	}
	if authoritative != nil {
		installAuthoritativeLocalRow(local, *authoritative, queueIndex)
		updateLocalProvenanceVersion(local, authoritative.Identity, authoritative.ServerVersion)
	} else if outcome.State == MutationOutcomeConflict && outcome.Reason == "row_not_found" {
		removeLocalRowWithoutLaterIntent(local, queued.Row, queueIndex)
	}
	for index := range local.Outcomes {
		if local.Outcomes[index].Mutation == outcome.Mutation && bytes.Equal(local.Outcomes[index].Response, outcome.Response) {
			return nil
		}
	}
	local.Outcomes = append(local.Outcomes, cloneMutationOutcomeForPush(outcome))
	return nil
}

func localRowFromWire(manifest pushManifest, identity RowIdentity, wire pushOutcomeWire, fields map[string]json.RawMessage, version RowVersion, checksum Checksum) (LocalRow, error) {
	table, ok := manifest.Tables[TableID(wire.Table)]
	if !ok {
		return LocalRow{}, errors.New("reconcile local push outcome: outcome table is absent")
	}
	if len(fields) != len(table.Fields) {
		return LocalRow{}, errors.New("reconcile local push outcome: server row is not a complete manifest row")
	}
	values := make([]FieldValue, 0, len(fields))
	for fieldID, value := range fields {
		field, ok := table.Fields[FieldID(fieldID)]
		if !ok {
			return LocalRow{}, fmt.Errorf("reconcile local push outcome: unknown outcome field %q", fieldID)
		}
		canonical, err := canonicalJSONValue(value)
		if err != nil {
			return LocalRow{}, fmt.Errorf("reconcile local push outcome: field %q is not canonical JSON", fieldID)
		}
		spec := vectors.FieldSpec{Type: string(field.Portable), Nullable: field.Nullable, Precision: field.Precision, Scale: field.Scale}
		if _, err := vectors.EncodeTypedValue(spec, canonical); err != nil {
			return LocalRow{}, fmt.Errorf("reconcile local push outcome: field %q has an invalid value", fieldID)
		}
		values = append(values, FieldValue{Field: field.ID, Type: field.Portable, WireJSON: string(canonical)})
	}
	sort.Slice(values, func(left, right int) bool { return values[left].Field < values[right].Field })
	row := LocalRow{Identity: identity, Fields: values, HasServerVersion: version != "", ServerVersion: version, HasChecksum: checksum != (Checksum{}), Checksum: checksum}
	if table.DeletedFieldID != nil {
		if value, ok := fields[string(*table.DeletedFieldID)]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			row.Deleted = true
		}
	}
	pkValue, ok := fields[string(table.PrimaryKeyFieldID)]
	if !ok {
		return LocalRow{}, errors.New("reconcile local push outcome: server row omits its primary key")
	}
	canonicalPK, err := canonicalJSONValue(pkValue)
	if err != nil || !bytes.Equal(canonicalPK, []byte(identity.CanonicalWireJSON)) {
		return LocalRow{}, errors.New("reconcile local push outcome: server row primary key differs from outcome identity")
	}
	return row, nil
}

func projectHistoricalLocalRow(state State, currentSchema, outcomeSchema SchemaRef, tableID TableID, row LocalRow) (LocalRow, error) {
	if currentSchema == outcomeSchema {
		return cloneLocalRowForPush(row), nil
	}
	currentManifest, err := loadPushManifest(state, currentSchema)
	if err != nil {
		return LocalRow{}, fmt.Errorf("reconcile local push outcome: load current local schema: %w", err)
	}
	historicalManifest, err := loadPushManifest(state, outcomeSchema)
	if err != nil {
		return LocalRow{}, fmt.Errorf("reconcile local push outcome: load historical outcome schema: %w", err)
	}
	historicalTable, ok := historicalManifest.Tables[tableID]
	if !ok {
		return LocalRow{}, errors.New("reconcile local push outcome: historical outcome table is absent")
	}
	currentTable, ok := currentManifest.Tables[tableID]
	if !ok || historicalTable.PrimaryKeyFieldID != currentTable.PrimaryKeyFieldID {
		return LocalRow{}, errors.New("reconcile local push outcome: historical row cannot be projected into the current table")
	}
	values := make(map[FieldID]FieldValue, len(currentTable.Fields))
	for _, field := range row.Fields {
		values[field.Field] = field
	}
	projected := make([]FieldValue, 0, len(currentTable.Fields))
	for fieldID, currentField := range currentTable.Fields {
		value, present := values[fieldID]
		if !present {
			if currentField.DefaultRaw != nil {
				canonical, err := canonicalJSONValue(*currentField.DefaultRaw)
				if err != nil {
					return LocalRow{}, fmt.Errorf("reconcile local push outcome: current default for field %q is invalid", fieldID)
				}
				value = FieldValue{Field: fieldID, Type: currentField.Portable, WireJSON: string(canonical)}
			} else if currentField.Nullable {
				value = FieldValue{Field: fieldID, Type: currentField.Portable, WireJSON: "null"}
			} else {
				return LocalRow{}, fmt.Errorf("reconcile local push outcome: current field %q has no safe projected value", fieldID)
			}
		} else {
			oldField, oldOK := historicalTable.Fields[fieldID]
			if !oldOK || !pushFieldsCompatible(oldField, currentField) {
				return LocalRow{}, fmt.Errorf("reconcile local push outcome: field %q is incompatible with the current schema", fieldID)
			}
			canonical, err := canonicalJSONValue([]byte(value.WireJSON))
			if err != nil {
				return LocalRow{}, fmt.Errorf("reconcile local push outcome: field %q is not canonical", fieldID)
			}
			if _, err := vectors.EncodeTypedValue(vectors.FieldSpec{Type: string(currentField.Portable), Nullable: currentField.Nullable, Precision: currentField.Precision, Scale: currentField.Scale}, canonical); err != nil {
				return LocalRow{}, fmt.Errorf("reconcile local push outcome: field %q cannot be represented by the current schema", fieldID)
			}
			value = FieldValue{Field: fieldID, Type: currentField.Portable, WireJSON: string(canonical)}
		}
		projected = append(projected, value)
	}
	sort.Slice(projected, func(left, right int) bool { return projected[left].Field < projected[right].Field })
	row.Identity = RowIdentity{
		CanonicalIdentityBytes: row.Identity.CanonicalIdentityBytes,
		TableID:                tableID,
		PrimaryKeyFieldID:      currentTable.PrimaryKeyFieldID,
		PortableType:           currentTable.Fields[currentTable.PrimaryKeyFieldID].Portable,
		CanonicalWireJSON:      row.Identity.CanonicalWireJSON,
	}
	row.Fields = projected
	row.HasChecksum = false
	row.Checksum = Checksum{}
	if row.HasServerVersion {
		authoritative := AuthoritativeRow{Identity: row.Identity, FieldValues: cloneFieldValues(projected), Version: row.ServerVersion, Deleted: row.Deleted}
		checksum, err := pushRowChecksum(currentManifest, tableID, authoritative, row.ServerVersion)
		if err != nil {
			return LocalRow{}, fmt.Errorf("reconcile local push outcome: recompute projected row checksum: %w", err)
		}
		row.HasChecksum = true
		row.Checksum = checksum
	}
	return row, nil
}

func refreshAcceptedSuccessors(local *ClientLocalState, acceptedIndex int, version RowVersion) {
	if local == nil || acceptedIndex < 0 || acceptedIndex >= len(local.DurableQueue) || version == "" {
		return
	}
	accepted := local.DurableQueue[acceptedIndex]
	for index := range local.DurableQueue {
		mutation := &local.DurableQueue[index]
		if index == acceptedIndex || mutation.Row.CanonicalIdentityBytes != accepted.Row.CanonicalIdentityBytes || mutation.Status != LocalMutationStatusPending || (mutation.Operation != DMLOperationUpdate && mutation.Operation != DMLOperationDelete) || !pushMutationDependsOn(local.DurableQueue, mutation.Mutation, accepted.Mutation) {
			continue
		}
		mutation.HasBaseVersion = true
		mutation.BaseVersion = version
		mutation.Request = canonicalQueuedMutationBytes(*mutation)
	}
}

func pushMutationDependsOn(queue []QueuedMutation, mutation, predecessor MutationID) bool {
	if mutation == predecessor {
		return true
	}
	byID := make(map[MutationID]QueuedMutation, len(queue))
	for _, item := range queue {
		byID[item.Mutation] = item
	}
	seen := make(map[MutationID]struct{})
	current := mutation
	for {
		if _, exists := seen[current]; exists {
			return false
		}
		seen[current] = struct{}{}
		item, exists := byID[current]
		if !exists || !item.HasPredecessor {
			return false
		}
		if item.Predecessor == predecessor {
			return true
		}
		current = item.Predecessor
	}
}

func blockRejectedSuccessors(local *ClientLocalState, rejectedIndex int) {
	if local == nil || rejectedIndex < 0 || rejectedIndex >= len(local.DurableQueue) {
		return
	}
	blocked := map[MutationID]struct{}{local.DurableQueue[rejectedIndex].Mutation: {}}
	changed := true
	for changed {
		changed = false
		for index := range local.DurableQueue {
			mutation := &local.DurableQueue[index]
			if !mutation.HasPredecessor {
				continue
			}
			if _, depends := blocked[mutation.Predecessor]; !depends {
				continue
			}
			if _, seen := blocked[mutation.Mutation]; !seen {
				blocked[mutation.Mutation] = struct{}{}
				changed = true
			}
			if mutation.Status == LocalMutationStatusPending {
				mutation.Status = LocalMutationStatusBlockedByPredecessor
				appendLocalTerminalOutcome(local, mutation.Mutation, "blocked_by_predecessor")
			}
		}
	}
}

func installAuthoritativeLocalRow(local *ClientLocalState, authoritative LocalRow, resolvedIndex int) {
	if local == nil {
		return
	}
	index := localRowIndex(local.Rows, authoritative.Identity)
	if index < 0 {
		local.Rows = append(local.Rows, cloneLocalRowForPush(authoritative))
		index = len(local.Rows) - 1
	} else {
		local.Rows[index] = cloneLocalRowForPush(authoritative)
	}
	resolved := local.DurableQueue[resolvedIndex]
	for _, mutation := range sortedLaterLocalMutations(local.DurableQueue, resolved.Row, resolved.LocalOrder) {
		if mutation.Status != LocalMutationStatusPending && mutation.Status != LocalMutationStatusSealed {
			continue
		}
		if local.Rows[index].Deleted && mutation.Operation == DMLOperationUpdate {
			continue
		}
		switch mutation.Operation {
		case DMLOperationUpdate:
			local.Rows[index].Fields = mergeLocalFieldValues(local.Rows[index].Fields, mutation.AuthoredColumns)
			local.Rows[index].Deleted = false
		case DMLOperationDelete:
			local.Rows[index].Deleted = true
		}
	}
}

func sortedLaterLocalMutations(queue []QueuedMutation, row RowIdentity, order uint64) []QueuedMutation {
	result := make([]QueuedMutation, 0)
	for _, mutation := range queue {
		if mutation.Row.CanonicalIdentityBytes == row.CanonicalIdentityBytes && mutation.LocalOrder > order {
			result = append(result, mutation)
		}
	}
	sort.SliceStable(result, func(left, right int) bool {
		if result[left].LocalOrder != result[right].LocalOrder {
			return result[left].LocalOrder < result[right].LocalOrder
		}
		return result[left].Mutation < result[right].Mutation
	})
	return result
}

func mergeLocalFieldValues(existing []FieldValue, authored []FieldValue) []FieldValue {
	values := make(map[FieldID]FieldValue, len(existing)+len(authored))
	for _, field := range existing {
		values[field.Field] = field
	}
	for _, field := range authored {
		prior := values[field.Field]
		if field.Type == "" {
			field.Type = prior.Type
		}
		values[field.Field] = field
	}
	return fieldValuesFromMap(values)
}

func updateLocalProvenanceVersion(local *ClientLocalState, identity RowIdentity, version RowVersion) {
	if local == nil || version == "" {
		return
	}
	for index := range local.Provenance {
		if local.Provenance[index].Row == identity || local.Provenance[index].Row.CanonicalIdentityBytes == identity.CanonicalIdentityBytes {
			local.Provenance[index].Version = version
		}
	}
}

func removeLocalRowWithoutLaterIntent(local *ClientLocalState, identity RowIdentity, resolvedIndex int) {
	if local == nil || resolvedIndex < 0 || resolvedIndex >= len(local.DurableQueue) {
		return
	}
	resolved := local.DurableQueue[resolvedIndex]
	for _, mutation := range local.DurableQueue {
		if mutation.Row.CanonicalIdentityBytes != identity.CanonicalIdentityBytes || mutation.LocalOrder <= resolved.LocalOrder {
			continue
		}
		if mutation.Status == LocalMutationStatusPending || mutation.Status == LocalMutationStatusSealed || mutation.Status == LocalMutationStatusBlockedByPredecessor {
			return
		}
	}
	if index := localRowIndex(local.Rows, identity); index >= 0 {
		local.Rows = append(local.Rows[:index], local.Rows[index+1:]...)
	}
}

func decodeStoredPushOutcome(raw []byte) (storedPushOutcome, error) {
	object, err := strictObject(raw)
	if err != nil {
		return storedPushOutcome{}, err
	}
	if err := requirePushKeys(object, []string{"mutation_id", "table", "pk", "outcome_schema", "status"}, []string{"code", "message", "retryable", "server_row", "row_checksum", "server_version", "authored_schema", "current_schema", "incompatible_field_ids"}); err != nil {
		return storedPushOutcome{}, err
	}
	var wire pushOutcomeWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		return storedPushOutcome{}, err
	}
	if wire.MutationID == "" || wire.Table == "" || len(wire.PK) != 1 {
		return storedPushOutcome{}, errors.New("stored push outcome has an incomplete identity")
	}
	if wire.Status != string(MutationOutcomeApplied) && wire.Status != string(MutationOutcomeConflict) && wire.Status != string(MutationOutcomeRejectedTerminal) {
		return storedPushOutcome{}, errors.New("stored push outcome has an unknown status")
	}
	parsedSchema, err := parseSchemaWire(object["outcome_schema"], "outcome_schema")
	if err != nil {
		return storedPushOutcome{}, errors.New("stored push outcome has an invalid outcome schema")
	}
	result := storedPushOutcome{Wire: wire, OutcomeSchema: SchemaRef{Version: parsedSchema.Version, Hash: parsedSchema.HashBytes}}
	if wire.Status == string(MutationOutcomeApplied) {
		result.State = MutationOutcomeApplied
	} else if wire.Status == string(MutationOutcomeConflict) {
		result.State = MutationOutcomeConflict
	} else {
		result.State = MutationOutcomeRejectedTerminal
	}
	if wire.Code != nil {
		result.Reason = ReasonCode(*wire.Code)
	}
	if result.State == MutationOutcomeApplied && (wire.Code != nil || wire.Message != nil || wire.Retryable != nil) || result.State != MutationOutcomeApplied && (wire.Code == nil || wire.Message == nil || *wire.Code == "" || *wire.Message == "") {
		return storedPushOutcome{}, errors.New("stored push outcome has an invalid status shape")
	}
	if result.State == MutationOutcomeConflict {
		switch ReasonCode(*wire.Code) {
		case "version_conflict", "row_already_exists", "row_deleted", "row_not_found":
		default:
			return storedPushOutcome{}, errors.New("stored push outcome has an invalid conflict code")
		}
		if wire.Retryable != nil || wire.AuthoredSchema != nil || wire.CurrentSchema != nil || wire.IncompatibleFields != nil {
			return storedPushOutcome{}, errors.New("stored push outcome has an invalid conflict member")
		}
	}
	if result.State == MutationOutcomeRejectedTerminal {
		switch ReasonCode(*wire.Code) {
		case "schema_incompatible":
			if wire.Retryable == nil || *wire.Retryable || wire.AuthoredSchema == nil || wire.CurrentSchema == nil || wire.IncompatibleFields == nil {
				return storedPushOutcome{}, errors.New("stored schema-incompatible outcome has an invalid shape")
			}
		case "policy_rejected", "validation_failed", "table_not_synced":
			if wire.Retryable != nil || wire.AuthoredSchema != nil || wire.CurrentSchema != nil || wire.IncompatibleFields != nil {
				return storedPushOutcome{}, errors.New("stored terminal outcome has an invalid member")
			}
		default:
			return storedPushOutcome{}, errors.New("stored terminal outcome has an invalid code")
		}
	}
	if wire.ServerRow != nil {
		serverRowRaw := object["server_row"]
		serverRow, err := strictObject(serverRowRaw)
		if err != nil {
			return storedPushOutcome{}, errors.New("stored push outcome has an invalid server row")
		}
		result.HasRow = true
		result.Row = cloneRawMap(serverRow)
	}
	if wire.RowChecksum != nil {
		checksumObject, err := strictObject(object["row_checksum"])
		if err != nil {
			return storedPushOutcome{}, errors.New("stored push outcome has an invalid checksum")
		}
		if err := requirePushKeys(checksumObject, []string{"algorithm", "version", "encoding", "digest"}, nil); err != nil {
			return storedPushOutcome{}, errors.New("stored push outcome has an invalid checksum")
		}
		if wire.RowChecksum.Algorithm != "sha256" || wire.RowChecksum.Version != 1 || wire.RowChecksum.Encoding != "hex" {
			return storedPushOutcome{}, errors.New("stored push outcome has an invalid checksum")
		}
		decoded, err := hex.DecodeString(wire.RowChecksum.Digest)
		if err != nil || len(decoded) != len(result.Checksum) {
			return storedPushOutcome{}, errors.New("stored push outcome has an invalid checksum digest")
		}
		copy(result.Checksum[:], decoded)
		result.HasChecksum = true
	}
	if wire.ServerVersion != nil {
		if *wire.ServerVersion == "" {
			return storedPushOutcome{}, errors.New("stored push outcome has an empty server version")
		}
		result.HasVersion = true
		result.Version = RowVersion(*wire.ServerVersion)
	}
	if wire.IncompatibleFields != nil {
		seen := make(map[string]struct{}, len(*wire.IncompatibleFields))
		for index, field := range *wire.IncompatibleFields {
			if field == "" {
				return storedPushOutcome{}, errors.New("stored push outcome has an empty incompatible field")
			}
			if _, duplicate := seen[field]; duplicate || index > 0 && (*wire.IncompatibleFields)[index-1] >= field {
				return storedPushOutcome{}, errors.New("stored push outcome has unsorted incompatible fields")
			}
			seen[field] = struct{}{}
		}
	}
	if result.State == MutationOutcomeApplied {
		if !result.HasVersion || (result.HasRow != result.HasChecksum) {
			return storedPushOutcome{}, errors.New("stored applied outcome has incomplete authoritative data")
		}
	} else if result.State == MutationOutcomeRejectedTerminal && (result.HasRow || result.HasChecksum || result.HasVersion) {
		return storedPushOutcome{}, errors.New("stored terminal outcome contains authoritative data")
	} else if result.State == MutationOutcomeConflict && result.HasChecksum && !result.HasRow {
		return storedPushOutcome{}, errors.New("stored conflict checksum has no server row")
	}
	return result, nil
}

func ensureLocalSealedBatch(model *Model, batch parsedPushBatch, key BatchKey) error {
	local, ok := model.state.ClientLocal[key.Client]
	if !ok {
		return nil
	}
	index := localSealedBatchIndex(local.SealedBatches, batch.Batch)
	if index >= 0 {
		sealed := local.SealedBatches[index]
		if !equalFingerprintRecord(sealed.Fingerprint, fingerprintRecord(pushBatchDomain, batch.Fingerprint)) || sealed.ClientGeneration != batch.Generation || sealed.Schema != batch.Schema || len(sealed.CanonicalRequest) == 0 || !bytes.Equal(sealed.CanonicalRequest, batch.CanonicalRequest) || !equalMutationIDs(sealed.Mutations, mutationIDs(batch.Mutations)) {
			return errors.New("push/submit: local sealed batch is immutable")
		}
		return nil
	}
	queueIndexes := make([]int, 0, len(batch.Mutations))
	for _, mutation := range batch.Mutations {
		queueIndex := queuedMutationIndex(local.DurableQueue, mutation.Mutation)
		if queueIndex < 0 {
			queueIndexes = append(queueIndexes, -1)
			continue
		}
		queueIndexes = append(queueIndexes, queueIndex)
	}
	for mutationIndex, queueIndex := range queueIndexes {
		if queueIndex < 0 || !queuedMutationMatchesParsed(local.DurableQueue[queueIndex], batch.Mutations[mutationIndex]) || !pushLocalIntentEligible(local.DurableQueue[queueIndex].Status) {
			return errors.New("push/submit: request differs from eligible durable local intent")
		}
	}
	now := canonicalClockTime(model.clock.Now())
	local.SealedBatches = append(local.SealedBatches, LocalSealedBatch{
		Batch: batch.Batch, ClientGeneration: batch.Generation, Schema: batch.Schema,
		Mutations: mutationIDs(batch.Mutations), CanonicalRequest: cloneBytes(batch.CanonicalRequest),
		Fingerprint: fingerprintRecord(pushBatchDomain, batch.Fingerprint), State: LocalSealedBatchStateSealed,
		SealedAt: timePointer(now),
	})
	for _, queueIndex := range queueIndexes {
		if local.DurableQueue[queueIndex].Status == LocalMutationStatusPending {
			local.DurableQueue[queueIndex].Status = LocalMutationStatusSealed
		}
	}
	model.state.ClientLocal[key.Client] = local
	return nil
}

func pushLocalIntentEligible(status LocalMutationStatus) bool {
	switch status {
	case LocalMutationStatusPending, LocalMutationStatusSealed, LocalMutationStatusAccepted, LocalMutationStatusServerRejected:
		return true
	default:
		return false
	}
}

func markLocalBatchResponseLost(model *Model, batch parsedPushBatch, key BatchKey, status int) error {
	local, ok := model.state.ClientLocal[key.Client]
	if !ok {
		return nil
	}
	if index := localSealedBatchIndex(local.SealedBatches, batch.Batch); index >= 0 {
		if local.SealedBatches[index].State != LocalSealedBatchStateReconciled {
			local.SealedBatches[index].State = LocalSealedBatchStateResponseLost
			local.SealedBatches[index].HTTPStatus = status
		}
	} else {
		if err := ensureLocalSealedBatch(model, batch, key); err != nil {
			return err
		}
		local = model.state.ClientLocal[key.Client]
		index := localSealedBatchIndex(local.SealedBatches, batch.Batch)
		if index < 0 {
			return nil
		}
		if local.SealedBatches[index].State != LocalSealedBatchStateReconciled {
			local.SealedBatches[index].State = LocalSealedBatchStateResponseLost
			local.SealedBatches[index].HTTPStatus = status
		}
	}
	now := canonicalClockTime(model.clock.Now())
	local.ErrorState = &ClientErrorState{Reason: "transport", Retryable: true, At: timePointer(now)}
	model.state.ClientLocal[key.Client] = local
	return nil
}

func markLocalPushBackoff(model *Model, key BatchKey, classification RetryClassification) {
	local, ok := model.state.ClientLocal[key.Client]
	if !ok {
		return
	}
	local = cloneClientLocalState(local)
	attempt := uint32(1)
	interrupted := ClientLifecyclePushing
	if local.Backoff != nil && local.Backoff.Work.Kind == ResumableWorkPush && local.Backoff.Work.HasBatch && local.Backoff.Work.Batch == key {
		attempt = local.Backoff.Attempt + 1
		interrupted = local.Backoff.InterruptedLifecycle
	} else if local.Lifecycle.State != "" && local.Lifecycle.State != ClientLifecycleBackoff {
		interrupted = local.Lifecycle.State
	}
	now := canonicalClockTime(model.clock.Now())
	next := now.Add(pushRetryAfter)
	local.Backoff = &DurableBackoff{
		InterruptedLifecycle: interrupted,
		Work:                 ResumableWorkIdentity{Kind: ResumableWorkPush, HasBatch: true, Batch: key},
		Retry:                classification,
		Attempt:              attempt,
		NextEligibleAt:       &next,
	}
	local.ErrorState = &ClientErrorState{Reason: ReasonCode(classification), Retryable: true, At: &now}
	if lifecycleTransitionAllowed(local.Lifecycle.State, ClientLifecycleBackoff) {
		local.Lifecycle = ClientLifecycleState{State: ClientLifecycleBackoff, ChangedAt: &now}
	}
	model.state.ClientLocal[key.Client] = local
}

func recordLocalPushIntegrityFailure(model *Model, client ClientKey, reason ReasonCode) {
	local, ok := model.state.ClientLocal[client]
	if !ok {
		return
	}
	local = cloneClientLocalState(local)
	now := canonicalClockTime(model.clock.Now())
	local.ErrorState = &ClientErrorState{Reason: reason, Retryable: false, At: &now}
	for index := range local.ScopeAssignments {
		if local.ScopeAssignments[index].Assigned {
			local.ScopeAssignments[index].RebuildRequired = true
		}
	}
	if lifecycleTransitionAllowed(local.Lifecycle.State, ClientLifecycleError) {
		local.Lifecycle = ClientLifecycleState{State: ClientLifecycleError, ChangedAt: &now}
	}
	model.state.ClientLocal[client] = local
}

func appendPushEvent(model *Model, client ClientKey, kind ModelEventKind, reason ReasonCode, transaction *TransactionReplayKey) {
	event := ModelEvent{Ordinal: nextModelEventOrdinal(model.state.Events), Kind: kind, At: timePointer(canonicalClockTime(model.clock.Now())), HasClient: true, Client: client, Reason: reason}
	if transaction != nil {
		event.HasTransaction = true
		event.Transaction = *transaction
	}
	model.state.Events = append(model.state.Events, event)
}

func transactionForBatch(model *Model, key BatchKey) *TransactionReplayKey {
	for index := len(model.state.Stream.Transactions) - 1; index >= 0; index-- {
		transaction := model.state.Stream.Transactions[index]
		for _, outcome := range transaction.Events {
			for _, mutation := range model.state.Batches[key].Mutations {
				if fence, ok := model.state.Fences[outcomeFenceID(model.state, outcome.ReplayKey)]; ok && fence.HasMutationKey && fence.MutationKey.Mutation == mutation {
					keyCopy := transaction.ReplayKey
					return &keyCopy
				}
			}
		}
	}
	return nil
}

func outcomeFenceID(state State, event EventReplayKey) FenceID {
	for id, fence := range state.Fences {
		if fence.HasEventReplayKey && fence.EventReplayKey == event {
			return id
		}
	}
	return ""
}

func pushHTTPResult(batch BatchKey, replay ReplayDisposition, http HTTPObservation, mutations []MutationObservation) StepResult {
	return StepResult{Kind: StepResultKindPush, HTTP: &http, Push: &PushObservation{Batch: batch, Replay: replay, Mutations: mutations}}
}

func pushFailureResult(batch BatchKey, replay ReplayDisposition, status int, code HTTPCode, retryable bool, details *pushErrorWire) StepResult {
	wire := pushErrorWire{Code: string(code), Message: pushErrorMessage(code), Retryable: retryable}
	if details != nil {
		wire.CurrentClientGeneration = details.CurrentClientGeneration
		wire.CurrentSchema = details.CurrentSchema
		wire.ReceivedSchema = details.ReceivedSchema
	}
	encoded, err := json.Marshal(pushErrorEnvelope{Error: wire})
	if err != nil {
		panic("marshal bounded push error: " + err.Error())
	}
	body, err := jcs.Transform(encoded)
	if err != nil {
		panic("canonicalize bounded push error: " + err.Error())
	}
	http := HTTPObservation{Status: status, HasCode: true, Code: code, Retryable: retryable, Body: body}
	if status == 429 || status == 503 {
		http.HasRetryAfterMilliseconds = true
		http.RetryAfterMilliseconds = uint64(pushRetryAfter / time.Millisecond)
	}
	return pushHTTPResult(batch, replay, http, nil)
}

func pushErrorMessage(code HTTPCode) string {
	switch code {
	case pushHTTPInvalidRequest:
		return "request is invalid"
	case pushHTTPAuthRequired:
		return "authentication is required"
	case pushHTTPIdempotencyConflict:
		return "request identity is already bound to different content"
	case pushHTTPClientRetired:
		return "client identity is retired"
	case pushHTTPGenerationExpired:
		return "client generation is not current"
	case pushHTTPSchemaMismatch:
		return "request schema is not current"
	case pushHTTPTemporaryUnavailable:
		return "request could not be committed"
	default:
		return "request failed"
	}
}

func batchMutationObservations(outcomes []MutationOutcome) []MutationObservation {
	result := make([]MutationObservation, 0, len(outcomes))
	for _, outcome := range outcomes {
		result = append(result, MutationObservation{Mutation: outcome.Mutation, State: outcome.State, Reason: outcome.Reason})
	}
	return result
}

func mutationIDs(mutations []parsedPushMutation) []MutationID {
	result := make([]MutationID, 0, len(mutations))
	for _, mutation := range mutations {
		result = append(result, mutation.Mutation)
	}
	return result
}

func outcomeValues(outcomes []MutationOutcome) []MutationOutcome { return outcomes }

func cloneMutationOutcomeForPush(outcome MutationOutcome) MutationOutcome {
	outcome.Response = cloneBytes(outcome.Response)
	return outcome
}

func fingerprintRecord(domain string, digest [32]byte) FingerprintRecord {
	return FingerprintRecord{Algorithm: "sha256", Version: 1, Domain: domain, Digest: Fingerprint(digest)}
}

func allocatePushVersion(model *Model, identity RowIdentity, mutation parsedPushMutation, key BatchKey, ordinal int) RowVersion {
	for attempt := 0; ; attempt++ {
		hash := sha256.New()
		_, _ = hash.Write([]byte("synchro:v3:server-version:v1\x00"))
		var number [8]byte
		binary.BigEndian.PutUint64(number[:], uint64(model.seed))
		_, _ = hash.Write(number[:])
		binary.BigEndian.PutUint64(number[:], uint64(len(model.state.Fences)+len(model.state.Stream.Transactions)+ordinal+attempt+1))
		_, _ = hash.Write(number[:])
		_, _ = hash.Write([]byte(identity.CanonicalIdentityBytes))
		_, _ = hash.Write([]byte(mutation.Mutation))
		_, _ = hash.Write([]byte(key.Batch))
		digest := hash.Sum(nil)
		candidate := RowVersion("sv-" + hex.EncodeToString(digest[:16]))
		if !pushVersionExists(model.state, candidate) {
			return candidate
		}
	}
}

func allocateFenceID(model *Model, key BatchKey, mutation MutationID, ordinal int) FenceID {
	hash := sha256.New()
	_, _ = hash.Write([]byte("synchro:v3:write-fence:v1\x00"))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(model.seed))
	_, _ = hash.Write(number[:])
	binary.BigEndian.PutUint64(number[:], uint64(len(model.state.Fences)+ordinal))
	_, _ = hash.Write(number[:])
	_, _ = hash.Write([]byte(key.Client.UserID))
	_, _ = hash.Write([]byte(key.Client.ClientID))
	_, _ = hash.Write([]byte(key.Batch))
	_, _ = hash.Write([]byte(mutation))
	digest := hash.Sum(nil)
	digest[6] = (digest[6] & 0x0f) | 0x40
	digest[8] = (digest[8] & 0x3f) | 0x80
	text := hex.EncodeToString(digest[:16])
	return FenceID(text[0:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:32])
}

func pushVersionExists(state State, version RowVersion) bool {
	for _, row := range state.Rows {
		if row.Version == version {
			return true
		}
	}
	for _, row := range state.Stream.SourceRows {
		if row.Row.Version == version {
			return true
		}
	}
	for _, fence := range state.Fences {
		if fence.RowVersion == version {
			return true
		}
	}
	return false
}

func derivePushRowIdentityFromState(state State, schema SchemaRef, mutation parsedPushMutation) (RowIdentity, error) {
	manifest, err := loadPushManifest(state, schema)
	if err != nil {
		return RowIdentity{}, err
	}
	return derivePushRowIdentity(manifest, mutation.Table, mutation.PKField, mutation.PKValue)
}

func derivePushRowIdentity(manifest pushManifest, tableID TableID, fieldID FieldID, pk json.RawMessage) (RowIdentity, error) {
	table, ok := manifest.Tables[tableID]
	if !ok {
		return RowIdentity{}, fmt.Errorf("unknown table %q", tableID)
	}
	if table.PrimaryKeyFieldID != fieldID {
		return RowIdentity{}, errors.New("primary-key field does not match manifest")
	}
	identity, err := vectors.RowIdentity(manifest.Vector, string(tableID), pk)
	if err != nil {
		return RowIdentity{}, err
	}
	canonicalPK, err := canonicalJSONValue(pk)
	if err != nil {
		return RowIdentity{}, err
	}
	field := table.Fields[fieldID]
	return RowIdentity{CanonicalIdentityBytes: string(identity), TableID: tableID, PrimaryKeyFieldID: fieldID, PortableType: field.Portable, CanonicalWireJSON: string(canonicalPK)}, nil
}

func pushRowChecksum(manifest pushManifest, tableID TableID, row AuthoritativeRow, version RowVersion) (Checksum, error) {
	fields := make([]vectors.RowField, 0, len(row.FieldValues))
	for _, field := range row.FieldValues {
		fields = append(fields, vectors.RowField{FieldID: string(field.Field), Value: json.RawMessage(field.WireJSON)})
	}
	canonicalPK, err := canonicalJSONValue(json.RawMessage(row.Identity.CanonicalWireJSON))
	if err != nil {
		return Checksum{}, err
	}
	digest, err := vectors.RowDigest(manifest.Vector, string(tableID), vectors.Row{PK: canonicalPK, Fields: fields}, string(version))
	if err != nil {
		return Checksum{}, err
	}
	return Checksum(digest), nil
}

func buildPushInsertRow(manifest pushManifest, table pushManifestTable, identity RowIdentity, pk json.RawMessage, columns []pushColumn, now time.Time) (AuthoritativeRow, error) {
	values := make(map[FieldID]FieldValue, len(table.Fields))
	for fieldID, field := range table.Fields {
		value := []byte("null")
		if field.DefaultRaw != nil {
			value = cloneBytes(*field.DefaultRaw)
		} else if !field.Nullable && fieldID != table.PrimaryKeyFieldID && fieldID != dereferenceFieldID(table.CreatedFieldID) && fieldID != dereferenceFieldID(table.UpdatedFieldID) && fieldID != dereferenceFieldID(table.DeletedFieldID) {
			// A missing required authored value is a semantic validation error.
			value = nil
		}
		if value != nil {
			values[fieldID] = FieldValue{Field: fieldID, Type: field.Portable, WireJSON: string(value)}
		}
	}
	values[table.PrimaryKeyFieldID] = FieldValue{Field: table.PrimaryKeyFieldID, Type: table.Fields[table.PrimaryKeyFieldID].Portable, WireJSON: string(bytes.TrimSpace(pk))}
	for _, column := range columns {
		field := table.Fields[column.Field]
		values[column.Field] = FieldValue{Field: column.Field, Type: field.Portable, WireJSON: string(bytes.TrimSpace(column.Value))}
	}
	for fieldID := range table.Fields {
		if _, ok := values[fieldID]; !ok {
			return AuthoritativeRow{}, fmt.Errorf("missing required field %q", fieldID)
		}
	}
	row := AuthoritativeRow{Identity: identity, FieldValues: fieldValuesFromMap(values), UpdatedAt: timePointer(now)}
	setPushLifecycleFields(&row, table, now, false, true)
	return row, nil
}

func mergeAuthoritativeFields(existing []FieldValue, columns []pushColumn, table pushManifestTable, now time.Time, deleting bool) []FieldValue {
	values := make(map[FieldID]FieldValue, len(existing)+len(columns))
	for _, field := range existing {
		values[field.Field] = field
	}
	for _, column := range columns {
		field := table.Fields[column.Field]
		values[column.Field] = FieldValue{Field: column.Field, Type: field.Portable, WireJSON: string(bytes.TrimSpace(column.Value))}
	}
	result := fieldValuesFromMap(values)
	_ = deleting
	_ = now
	return result
}

func setPushLifecycleFields(row *AuthoritativeRow, table pushManifestTable, now time.Time, deleted, insert bool) {
	if table.CreatedFieldID != nil && insert {
		setFieldValue(row, *table.CreatedFieldID, "datetime", strconv.Quote(formatCanonicalTime(now)))
	}
	if table.UpdatedFieldID != nil {
		setFieldValue(row, *table.UpdatedFieldID, "datetime", strconv.Quote(formatCanonicalTime(now)))
	}
	if table.DeletedFieldID != nil {
		if deleted {
			setFieldValue(row, *table.DeletedFieldID, "datetime", strconv.Quote(formatCanonicalTime(now)))
		} else {
			field := table.Fields[*table.DeletedFieldID]
			setFieldValue(row, *table.DeletedFieldID, field.Portable, "null")
		}
	}
	if deleted {
		row.Deleted = true
		row.DeletedAt = timePointer(now)
	} else {
		row.Deleted = false
		row.DeletedAt = nil
	}
}

func setFieldValue(row *AuthoritativeRow, field FieldID, portable PortableType, value string) {
	for index := range row.FieldValues {
		if row.FieldValues[index].Field == field {
			row.FieldValues[index].Type = portable
			row.FieldValues[index].WireJSON = value
			return
		}
	}
	row.FieldValues = append(row.FieldValues, FieldValue{Field: field, Type: portable, WireJSON: value})
}

func authoritativeRowWire(row AuthoritativeRow) map[string]json.RawMessage {
	result := make(map[string]json.RawMessage, len(row.FieldValues))
	for _, field := range row.FieldValues {
		result[string(field.Field)] = json.RawMessage(cloneBytes([]byte(field.WireJSON)))
	}
	return result
}

func currentSourceRow(state State, identity RowIdentity) (AuthoritativeRow, bool, bool, RowVersion, error) {
	for _, entry := range state.Stream.SourceRows {
		if entry.Identity == identity || entry.Identity.CanonicalIdentityBytes == identity.CanonicalIdentityBytes {
			return cloneAuthoritativeRowForPush(entry.Row), true, false, entry.Row.Version, nil
		}
	}
	var deletedVersion RowVersion
	var deletedEvent EventReplayKey
	for _, fence := range state.Fences {
		if fence.RegistrationKind != RegistrationKindSynced || fence.Operation != DMLOperationDelete || fence.RowVersion == "" || !pushFenceMatchesRow(fence, identity) {
			continue
		}
		if deletedVersion == "" || fence.HasEventReplayKey && (deletedEvent == (EventReplayKey{}) || lessEventReplayKey(deletedEvent, fence.EventReplayKey)) {
			deletedVersion = fence.RowVersion
			if fence.HasEventReplayKey {
				deletedEvent = fence.EventReplayKey
			}
		}
	}
	if deletedVersion != "" {
		return AuthoritativeRow{Identity: identity, Version: deletedVersion, Deleted: true}, false, true, deletedVersion, nil
	}
	if row, ok := state.Rows[identity]; ok {
		return cloneAuthoritativeRowForPush(row), true, false, row.Version, nil
	}
	for rowIdentity, row := range state.Rows {
		if rowIdentity.CanonicalIdentityBytes == identity.CanonicalIdentityBytes {
			return cloneAuthoritativeRowForPush(row), true, false, row.Version, nil
		}
	}
	return AuthoritativeRow{}, false, false, "", nil
}

func pushFenceMatchesRow(fence VersionFence, identity RowIdentity) bool {
	for _, registered := range []RegisteredIdentity{fence.OldRegisteredIdentity, fence.NewRegisteredIdentity} {
		if registered.Kind != RegistrationKindSynced {
			continue
		}
		row := registered.SyncedRow
		if row == identity || row.CanonicalIdentityBytes == identity.CanonicalIdentityBytes && row.TableID == identity.TableID && row.PrimaryKeyFieldID == identity.PrimaryKeyFieldID {
			return true
		}
	}
	return false
}

func currentSyncedRelation(state State, table TableID) (RelationDefinition, bool) {
	if state.Registry.CurrentGeneration == 0 {
		return RelationDefinition{}, false
	}
	for _, generation := range state.Registry.Generations {
		if generation.Generation != state.Registry.CurrentGeneration || !generation.Validated {
			continue
		}
		var matched RelationDefinition
		found := false
		for _, relation := range generation.Relations {
			definition := relation.Definition
			if definition.RegistrationKind != RegistrationKindSynced || !definition.HasTableID || definition.TableID != table {
				continue
			}
			if found {
				return RelationDefinition{}, false
			}
			matched = definition
			found = true
		}
		return matched, found
	}
	return RelationDefinition{}, false
}

func loadPushManifest(state State, ref SchemaRef) (pushManifest, error) {
	record, ok := state.Schemas[ref]
	if !ok || len(record.Body) == 0 {
		return pushManifest{}, errors.New("immutable schema manifest is absent")
	}
	vector, err := vectors.ParseManifest(record.Body)
	if err != nil {
		return pushManifest{}, err
	}
	if vector.Hash() != ref.Hash {
		return pushManifest{}, errors.New("schema manifest hash does not match schema reference")
	}
	if vectorHashVersion(vector, ref.Version) == 0 {
		return pushManifest{}, errors.New("schema manifest version does not match schema reference")
	}
	var document pushManifestDocument
	if err := decodeStrictManifest(record.Body, &document); err != nil {
		return pushManifest{}, err
	}
	if document.SchemaVersion != ref.Version {
		return pushManifest{}, errors.New("schema manifest version does not match schema reference")
	}
	manifest := pushManifest{Reference: ref, Vector: vector, Tables: make(map[TableID]pushManifestTable, len(document.Tables))}
	for _, table := range document.Tables {
		parsed := pushManifestTable{ID: TableID(table.TableID), Relation: RelationID(table.RelationID), Composition: Composition(table.Composition), PrimaryKeyFieldID: FieldID(table.PrimaryKeyFieldID), Fields: make(map[FieldID]pushManifestField, len(table.Fields))}
		for _, lifecycle := range []*struct {
			value  *string
			target **FieldID
		}{
			{table.Lifecycle.CreatedAtFieldID, &parsed.CreatedFieldID},
			{table.Lifecycle.UpdatedAtFieldID, &parsed.UpdatedFieldID},
			{table.Lifecycle.DeletedAtFieldID, &parsed.DeletedFieldID},
		} {
			if lifecycle.value != nil {
				copy := FieldID(*lifecycle.value)
				*lifecycle.target = &copy
			}
		}
		for _, field := range table.Fields {
			parsedField := pushManifestField{ID: FieldID(field.FieldID), Portable: PortableType(field.Type), Nullable: field.Nullable, Writable: field.Writable}
			if field.Precision != nil {
				value := *field.Precision
				parsedField.Precision = &value
			}
			if field.Scale != nil {
				value := *field.Scale
				parsedField.Scale = &value
			}
			parsed.Fields[parsedField.ID] = parsedField
		}
		for _, referenceTable := range record.Tables {
			if referenceTable.ID != parsed.ID {
				continue
			}
			if parsed.CreatedFieldID == nil {
				parsed.CreatedFieldID = cloneFieldID(referenceTable.CreatedFieldID)
			}
			if parsed.UpdatedFieldID == nil {
				parsed.UpdatedFieldID = cloneFieldID(referenceTable.UpdatedFieldID)
			}
			if parsed.DeletedFieldID == nil {
				parsed.DeletedFieldID = cloneFieldID(referenceTable.DeletedFieldID)
			}
			for fieldID, parsedField := range parsed.Fields {
				for _, referenceField := range referenceTable.Fields {
					if referenceField.ID == fieldID && referenceField.DefaultWireJSON != nil {
						raw := json.RawMessage(cloneBytes([]byte(*referenceField.DefaultWireJSON)))
						parsedField.DefaultRaw = &raw
						parsed.Fields[fieldID] = parsedField
					}
				}
			}
		}
		manifest.Tables[parsed.ID] = parsed
	}
	return manifest, nil
}

func vectorHashVersion(manifest vectors.Manifest, version uint64) uint64 {
	if manifest.Hash() == ([32]byte{}) && version != 0 {
		return 0
	}
	// vectors.Manifest intentionally exposes only its verified hash. The
	// reference schema body is the version authority, so this check is made by
	// the strict body decoder below.
	return version
}

type pushManifestDocument struct {
	SchemaVersion      uint64                      `json:"schema_version"`
	SchemaHash         string                      `json:"schema_hash"`
	ParentSchema       *pushSchemaWire             `json:"parent_schema"`
	TransitionClass    string                      `json:"transition_class"`
	CompatibilityFloor uint64                      `json:"compatibility_floor"`
	Tables             []pushManifestTableDocument `json:"tables"`
}

type pushManifestTableDocument struct {
	TableID           string                      `json:"table_id"`
	RelationID        string                      `json:"relation_id"`
	Name              string                      `json:"name"`
	Composition       string                      `json:"composition"`
	PrimaryKeyFieldID string                      `json:"primary_key_field_id"`
	Lifecycle         pushManifestLifecycle       `json:"lifecycle"`
	Fields            []pushManifestFieldDocument `json:"fields"`
	Indexes           []json.RawMessage           `json:"indexes"`
}

type pushManifestLifecycle struct {
	CreatedAtFieldID *string `json:"created_at_field_id"`
	UpdatedAtFieldID *string `json:"updated_at_field_id"`
	DeletedAtFieldID *string `json:"deleted_at_field_id"`
}

type pushManifestFieldDocument struct {
	FieldID   string `json:"field_id"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Nullable  bool   `json:"nullable"`
	Writable  bool   `json:"writable"`
	Precision *int   `json:"precision,omitempty"`
	Scale     *int   `json:"scale,omitempty"`
}

func decodeStrictManifest(raw []byte, destination *pushManifestDocument) error {
	if err := jsonstrict.ValidateValue(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return nil
}

func validatePushColumns(manifest pushManifest, table pushManifestTable, operation DMLOperation, columns []pushColumn) error {
	if operation == DMLOperationDelete {
		if columns != nil {
			return errors.New("delete must omit columns")
		}
		return nil
	}
	if len(columns) == 0 {
		return errors.New("insert and update require nonempty columns")
	}
	seen := make(map[FieldID]struct{}, len(columns))
	for _, column := range columns {
		if _, exists := seen[column.Field]; exists {
			return fmt.Errorf("duplicate column field_id %q", column.Field)
		}
		seen[column.Field] = struct{}{}
		field, ok := table.Fields[column.Field]
		if !ok {
			return fmt.Errorf("unknown field_id %q", column.Field)
		}
		if column.Field == table.PrimaryKeyFieldID || !field.Writable || (table.CreatedFieldID != nil && column.Field == *table.CreatedFieldID) || (table.UpdatedFieldID != nil && column.Field == *table.UpdatedFieldID) || (table.DeletedFieldID != nil && column.Field == *table.DeletedFieldID) {
			return fmt.Errorf("field_id %q is not writable", column.Field)
		}
		spec := vectors.FieldSpec{Type: string(field.Portable), Nullable: field.Nullable, Precision: field.Precision, Scale: field.Scale}
		if _, err := vectors.EncodeTypedValue(spec, column.Value); err != nil {
			return fmt.Errorf("field_id %q has an invalid value", column.Field)
		}
	}
	_ = manifest
	return nil
}

func incompatibleMutationFields(mutation parsedPushMutation, table pushManifestTable) []string {
	fields := make([]string, 0, len(mutation.Columns))
	for _, column := range mutation.Columns {
		if _, ok := table.Fields[column.Field]; !ok {
			fields = append(fields, string(column.Field))
		}
	}
	sort.Strings(fields)
	return fields
}

func fieldValuesFromPushColumns(table pushManifestTable, columns []pushColumn) []FieldValue {
	if columns == nil {
		return nil
	}
	result := make([]FieldValue, 0, len(columns))
	for _, column := range columns {
		field := table.Fields[column.Field]
		result = append(result, FieldValue{Field: column.Field, Type: field.Portable, WireJSON: string(bytes.TrimSpace(column.Value))})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Field < result[right].Field })
	return result
}

func fieldValuesFromParsedMutation(mutation parsedPushMutation) []FieldValue {
	result := make([]FieldValue, 0, len(mutation.Columns))
	for _, column := range mutation.Columns {
		result = append(result, FieldValue{Field: column.Field, WireJSON: string(bytes.TrimSpace(column.Value))})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Field < result[right].Field })
	return result
}

func localRowFromAuthoritative(row AuthoritativeRow, hasVersion bool, version RowVersion, checksum Checksum) LocalRow {
	return LocalRow{Identity: row.Identity, Fields: cloneFieldValues(row.FieldValues), Deleted: row.Deleted, HasServerVersion: hasVersion, ServerVersion: version, HasChecksum: checksum != (Checksum{}), Checksum: checksum, UpdatedAt: cloneTime(row.UpdatedAt)}
}

func cloneLocalRowForPush(row LocalRow) LocalRow {
	row.Fields = cloneFieldValues(row.Fields)
	row.UpdatedAt = cloneTime(row.UpdatedAt)
	return row
}

func cloneAuthoritativeRowForPush(row AuthoritativeRow) AuthoritativeRow {
	row.FieldValues = cloneFieldValues(row.FieldValues)
	row.DeletedAt = cloneTime(row.DeletedAt)
	row.DeleteReason = cloneString(row.DeleteReason)
	row.UpdatedAt = cloneTime(row.UpdatedAt)
	return row
}

func localBaseVersion(row *LocalRow) (RowVersion, bool) {
	if row == nil || !row.HasServerVersion || row.ServerVersion == "" {
		return "", false
	}
	return row.ServerVersion, true
}

func localRowIndex(rows []LocalRow, identity RowIdentity) int {
	for index, row := range rows {
		if row.Identity == identity || row.Identity.CanonicalIdentityBytes == identity.CanonicalIdentityBytes {
			return index
		}
	}
	return -1
}

func queuedMutationExists(queue []QueuedMutation, mutation MutationID) bool {
	return queuedMutationIndex(queue, mutation) >= 0
}

func queuedMutationIndex(queue []QueuedMutation, mutation MutationID) int {
	for index, item := range queue {
		if item.Mutation == mutation {
			return index
		}
	}
	return -1
}

func localSealedBatchIndex(batches []LocalSealedBatch, batch BatchID) int {
	for index, item := range batches {
		if item.Batch == batch {
			return index
		}
	}
	return -1
}

func equalMutationIDs(left, right []MutationID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func nextLocalOrder(queue []QueuedMutation) uint64 {
	var result uint64
	for _, item := range queue {
		if item.LocalOrder >= result {
			result = item.LocalOrder + 1
		}
	}
	if result == 0 {
		return 1
	}
	return result
}

func nextModelEventOrdinal(events []ModelEvent) uint64 {
	var result uint64
	for _, event := range events {
		if event.Ordinal >= result {
			result = event.Ordinal + 1
		}
	}
	if result == 0 {
		return 1
	}
	return result
}

func mergeLocalFields(existing []FieldValue, columns []pushColumn, table pushManifestTable) []FieldValue {
	values := make(map[FieldID]FieldValue, len(existing)+len(columns))
	for _, field := range existing {
		values[field.Field] = field
	}
	for _, column := range columns {
		field := table.Fields[column.Field]
		values[column.Field] = FieldValue{Field: column.Field, Type: field.Portable, WireJSON: string(bytes.TrimSpace(column.Value))}
	}
	return fieldValuesFromMap(values)
}

func fieldValuesFromMap(values map[FieldID]FieldValue) []FieldValue {
	result := make([]FieldValue, 0, len(values))
	for _, field := range values {
		result = append(result, field)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Field < result[right].Field })
	return result
}

func normalizePendingSameRow(local *ClientLocalState) {
	groups := make(map[string][]int)
	for index, mutation := range local.DurableQueue {
		if mutation.Status == LocalMutationStatusPending {
			groups[mutation.Row.CanonicalIdentityBytes] = append(groups[mutation.Row.CanonicalIdentityBytes], index)
		}
	}
	for _, indexes := range groups {
		sort.Slice(indexes, func(left, right int) bool {
			return local.DurableQueue[indexes[left]].LocalOrder < local.DurableQueue[indexes[right]].LocalOrder
		})
		if len(indexes) < 2 {
			continue
		}
		first := local.DurableQueue[indexes[0]]
		if first.Operation == DMLOperationDelete {
			if local.DurableQueue[indexes[1]].Operation == DMLOperationUpdate {
				blocked := &local.DurableQueue[indexes[1]]
				blocked.Status = LocalMutationStatusBlockedByPredecessor
				blocked.HasPredecessor = true
				blocked.Predecessor = first.Mutation
				appendLocalTerminalOutcome(local, blocked.Mutation, "blocked_by_predecessor")
			}
			continue
		}
		operation := first.Operation
		columns := make(map[FieldID]FieldValue)
		for _, column := range first.AuthoredColumns {
			columns[column.Field] = column
		}
		baseVersion := first.BaseVersion
		hasBase := first.HasBaseVersion
		cancel := false
		last := first
		for _, queueIndex := range indexes[1:] {
			current := local.DurableQueue[queueIndex]
			last = current
			switch {
			case operation == DMLOperationInsert && current.Operation == DMLOperationUpdate:
				for _, column := range current.AuthoredColumns {
					columns[column.Field] = column
				}
			case operation == DMLOperationInsert && current.Operation == DMLOperationDelete:
				cancel = true
			case operation == DMLOperationUpdate && current.Operation == DMLOperationUpdate:
				for _, column := range current.AuthoredColumns {
					columns[column.Field] = column
				}
			case operation == DMLOperationUpdate && current.Operation == DMLOperationDelete:
				operation = DMLOperationDelete
			case operation == DMLOperationDelete:
				break
			default:
				operation = ""
			}
			if operation == "" {
				break
			}
		}
		if operation == "" {
			continue
		}
		if operation == DMLOperationDelete {
			columns = nil
		}
		for _, queueIndex := range indexes {
			local.DurableQueue[queueIndex].Status = LocalMutationStatusSupersededBeforeSend
			appendLocalTerminalOutcome(local, local.DurableQueue[queueIndex].Mutation, "superseded_before_send")
		}
		if cancel {
			local.DurableQueue[indexes[len(indexes)-1]].Status = LocalMutationStatusCancelledBeforeSend
			appendLocalTerminalOutcome(local, local.DurableQueue[indexes[len(indexes)-1]].Mutation, "cancelled_before_send")
			continue
		}
		newMutation := deriveNormalizedMutationID(first, last, operation)
		var authoredColumns []FieldValue
		if columns != nil {
			authoredColumns = fieldValuesFromMap(columns)
		}
		normalized := QueuedMutation{
			Mutation: newMutation, Table: first.Table, Row: first.Row, AuthoredSchema: first.AuthoredSchema,
			Operation: operation, HasBaseVersion: hasBase && operation != DMLOperationInsert, BaseVersion: baseVersion,
			ClientVersion: last.ClientVersion, AuthoredColumns: authoredColumns, LocalOrder: first.LocalOrder,
			HasPredecessor: true, Predecessor: last.Mutation, Status: LocalMutationStatusPending,
			QueuedAt: cloneTime(last.QueuedAt),
		}
		normalized.Request = canonicalQueuedMutationBytes(normalized)
		local.DurableQueue = append(local.DurableQueue, normalized)
	}
}

func appendLocalTerminalOutcome(local *ClientLocalState, mutation MutationID, reason ReasonCode) {
	for _, outcome := range local.Outcomes {
		if outcome.Mutation == mutation && outcome.Reason == reason {
			return
		}
	}
	local.Outcomes = append(local.Outcomes, MutationOutcome{Mutation: mutation, State: MutationOutcomeRejectedTerminal, Reason: reason})
}

func deriveNormalizedMutationID(first, last QueuedMutation, operation DMLOperation) MutationID {
	hash := sha256.New()
	_, _ = hash.Write([]byte("synchro:v3:normalized-mutation:v1\x00"))
	_, _ = hash.Write([]byte(first.Mutation))
	_, _ = hash.Write([]byte(last.Mutation))
	_, _ = hash.Write([]byte(operation))
	digest := hash.Sum(nil)
	digest[6] = (digest[6] & 0x0f) | 0x40
	digest[8] = (digest[8] & 0x3f) | 0x80
	text := hex.EncodeToString(digest[:16])
	return MutationID(text[0:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:32])
}

func canonicalQueuedMutationBytes(mutation QueuedMutation) []byte {
	object := map[string]any{
		"mutation_id": string(mutation.Mutation), "table_id": string(mutation.Table),
		"pk":              map[string]any{"field_id": string(mutation.Row.PrimaryKeyFieldID), "value": json.RawMessage(mutation.Row.CanonicalWireJSON)},
		"authored_schema": pushSchemaWireFromRef(mutation.AuthoredSchema), "operation": string(mutation.Operation),
		"client_version": string(mutation.ClientVersion),
	}
	if mutation.HasBaseVersion {
		object["base_version"] = string(mutation.BaseVersion)
	}
	if mutation.AuthoredColumns != nil {
		columns := make([]map[string]any, 0, len(mutation.AuthoredColumns))
		for _, column := range mutation.AuthoredColumns {
			columns = append(columns, map[string]any{"field_id": string(column.Field), "value": json.RawMessage(column.WireJSON)})
		}
		object["columns"] = columns
	}
	encoded, _ := json.Marshal(object)
	canonical, _ := canonicalJSONValue(encoded)
	return canonical
}

func parsePushSubmitPayload(payload json.RawMessage) (pushSubmitEnvelope, error) {
	object, err := strictObject(payload)
	if err != nil {
		return pushSubmitEnvelope{}, err
	}
	if err := requirePushKeys(object, []string{"authenticated_user_id", "request", "delivery", "commit_lsn", "end_lsn"}, nil); err != nil {
		return pushSubmitEnvelope{}, err
	}
	user, err := requiredJSONString(object["authenticated_user_id"], "authenticated_user_id")
	if err != nil {
		return pushSubmitEnvelope{}, err
	}
	delivery, err := requiredJSONString(object["delivery"], "delivery")
	if err != nil {
		return pushSubmitEnvelope{}, err
	}
	if delivery != "apply" && delivery != "drop_after_server" && delivery != "transport_failure" {
		return pushSubmitEnvelope{}, errors.New("delivery is not one of apply, drop_after_server, transport_failure")
	}
	commit, err := parseCanonicalLSN(object["commit_lsn"], "commit_lsn")
	if err != nil {
		return pushSubmitEnvelope{}, err
	}
	end, err := parseCanonicalLSN(object["end_lsn"], "end_lsn")
	if err != nil {
		return pushSubmitEnvelope{}, err
	}
	request := cloneBytes(object["request"])
	if len(request) == 0 {
		return pushSubmitEnvelope{}, errors.New("request is empty")
	}
	return pushSubmitEnvelope{AuthenticatedUser: UserID(user), Request: request, Delivery: delivery, CommitLSN: commit, EndLSN: end}, nil
}

func parseCanonicalLSN(raw json.RawMessage, name string) (uint64, error) {
	value, err := requiredJSONString(raw, name)
	if err != nil {
		return 0, err
	}
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, fmt.Errorf("%s is not a canonical decimal string", name)
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return 0, fmt.Errorf("%s is not a canonical decimal string", name)
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is outside uint64", name)
	}
	return parsed, nil
}

func parseProtocolPushRequest(authenticatedUser UserID, raw []byte) (parsedPushBatch, error) {
	if len(raw) == 0 || len(raw) > pushRequestLimit {
		return parsedPushBatch{}, errors.New("request exceeds byte limit")
	}
	object, err := strictObject(raw)
	if err != nil {
		return parsedPushBatch{}, err
	}
	if err := requirePushKeys(object, []string{"client_id", "client_generation", "batch_id", "schema", "mutations"}, nil); err != nil {
		return parsedPushBatch{}, err
	}
	client, err := requiredJSONString(object["client_id"], "client_id")
	if err != nil {
		return parsedPushBatch{}, err
	}
	generation, err := parsePositiveJSONUint(object["client_generation"], "client_generation")
	if err != nil {
		return parsedPushBatch{}, err
	}
	batchID, err := requiredJSONString(object["batch_id"], "batch_id")
	if err != nil {
		return parsedPushBatch{}, err
	}
	schema, err := parseSchemaWire(object["schema"], "schema")
	if err != nil {
		return parsedPushBatch{}, err
	}
	mutationsRaw, err := parseJSONArray(object["mutations"], "mutations")
	if err != nil {
		return parsedPushBatch{}, err
	}
	if len(mutationsRaw) == 0 || len(mutationsRaw) > 1000 {
		return parsedPushBatch{}, errors.New("mutations count is outside 1..1000")
	}

	normalizedMutations := make([]json.RawMessage, 0, len(mutationsRaw))
	parsedMutations := make([]parsedPushMutation, 0, len(mutationsRaw))
	seen := make(map[MutationID]struct{}, len(mutationsRaw))
	for _, mutationRaw := range mutationsRaw {
		mutationObject, err := strictObject(mutationRaw)
		if err != nil {
			return parsedPushBatch{}, err
		}
		converted, err := convertWireMutationToNormalized(mutationObject)
		if err != nil {
			return parsedPushBatch{}, err
		}
		parsed, err := parseNormalizedMutation(string(authenticatedUser), client, converted)
		if err != nil {
			return parsedPushBatch{}, err
		}
		if _, exists := seen[parsed.Mutation]; exists {
			return parsedPushBatch{}, fmt.Errorf("duplicate mutation_id %q", parsed.Mutation)
		}
		seen[parsed.Mutation] = struct{}{}
		canonicalMutation, err := jcs.Transform(mutationRaw)
		if err != nil {
			return parsedPushBatch{}, err
		}
		parsed.CanonicalMutation = canonicalMutation
		normalizedMutations = append(normalizedMutations, mustMarshalRawObject(converted))
		parsedMutations = append(parsedMutations, parsed)
	}
	normalizedBatchObject := map[string]any{
		"client_id":         client,
		"client_generation": generation,
		"batch_id":          batchID,
		"request_schema":    map[string]any{"version": schema.Version, "hash": schema.Wire.Hash},
		"mutations":         normalizedMutations,
	}
	normalizedBatchBytes, err := json.Marshal(normalizedBatchObject)
	if err != nil {
		return parsedPushBatch{}, err
	}
	normalized, err := vectors.ParseNormalizedBatch(string(authenticatedUser), normalizedBatchBytes)
	if err != nil {
		return parsedPushBatch{}, err
	}
	preimage, err := vectors.BatchFingerprintPreimage(normalized)
	if err != nil {
		return parsedPushBatch{}, err
	}
	fingerprint, err := vectors.BatchFingerprint(normalized)
	if err != nil {
		return parsedPushBatch{}, err
	}
	if _, err := stripFingerprintDomain(preimage, pushBatchDomain+"\x00"); err != nil {
		return parsedPushBatch{}, err
	}
	canonicalRequest, err := jcs.Transform(raw)
	if err != nil {
		return parsedPushBatch{}, err
	}
	for index := range parsedMutations {
		parsedMutations[index].Normalized = normalized.Mutations[index]
	}
	return parsedPushBatch{AuthenticatedUser: authenticatedUser, Client: ClientID(client), Batch: BatchID(batchID), Generation: Generation(generation), Schema: SchemaRef{Version: schema.Version, Hash: schema.HashBytes}, Request: cloneBytes(raw), CanonicalRequest: canonicalRequest, Fingerprint: fingerprint, Mutations: parsedMutations}, nil
}

func parseNormalizedMutation(authenticatedUser, client string, object map[string]json.RawMessage) (parsedPushMutation, error) {
	if err := requirePushKeys(object, []string{"mutation_id", "table_id", "pk", "authored_schema", "operation", "client_version"}, []string{"base_version", "columns"}); err != nil {
		return parsedPushMutation{}, err
	}
	mutationID, err := requiredJSONString(object["mutation_id"], "mutation_id")
	if err != nil {
		return parsedPushMutation{}, err
	}
	tableID, err := requiredJSONString(object["table_id"], "table_id")
	if err != nil {
		return parsedPushMutation{}, err
	}
	pkObject, err := strictObject(object["pk"])
	if err != nil {
		return parsedPushMutation{}, err
	}
	if err := requirePushKeys(pkObject, []string{"field_id", "value"}, nil); err != nil {
		return parsedPushMutation{}, err
	}
	pkField, err := requiredJSONString(pkObject["field_id"], "pk.field_id")
	if err != nil {
		return parsedPushMutation{}, err
	}
	authoredSchema, err := parseSchemaWire(object["authored_schema"], "authored_schema")
	if err != nil {
		return parsedPushMutation{}, err
	}
	operation, err := requiredJSONString(object["operation"], "operation")
	if err != nil {
		return parsedPushMutation{}, err
	}
	clientVersion, err := requiredJSONString(object["client_version"], "client_version")
	if err != nil {
		return parsedPushMutation{}, err
	}
	converted := cloneRawObject(object)
	_ = authenticatedUser
	_ = client
	// ParseNormalizedMutation performs the frozen operation matrix and all
	// normalized mutation limits. It remains the semantic parser authority.
	normalizedBytes, err := json.Marshal(converted)
	if err != nil {
		return parsedPushMutation{}, err
	}
	normalized, err := vectors.ParseNormalizedMutation(authenticatedUser, client, normalizedBytes)
	if err != nil {
		return parsedPushMutation{}, err
	}
	mutationFingerprint, err := vectors.MutationFingerprint(normalized)
	if err != nil {
		return parsedPushMutation{}, err
	}
	mutationPreimage, err := vectors.MutationFingerprintPreimage(normalized)
	if err != nil {
		return parsedPushMutation{}, err
	}
	canonicalMutation, err := stripFingerprintDomain(mutationPreimage, pushMutationDomain+"\x00")
	if err != nil {
		return parsedPushMutation{}, err
	}
	var baseVersion *RowVersion
	if normalized.BaseVersion != nil {
		value := RowVersion(*normalized.BaseVersion)
		baseVersion = &value
	}
	columns, err := parseNormalizedColumns(object["columns"])
	if err != nil {
		return parsedPushMutation{}, err
	}
	return parsedPushMutation{Normalized: normalized, CanonicalMutation: canonicalMutation, Fingerprint: mutationFingerprint, Mutation: MutationID(mutationID), Table: TableID(tableID), PKField: FieldID(pkField), PKValue: cloneBytes(pkObject["value"]), AuthoredSchema: SchemaRef{Version: authoredSchema.Version, Hash: authoredSchema.HashBytes}, Operation: DMLOperation(operation), BaseVersion: baseVersion, ClientVersion: ClientVersion(clientVersion), Columns: columns}, nil
}

func convertWireMutationToNormalized(object map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	if err := requirePushKeys(object, []string{"mutation_id", "table", "pk", "authored_schema", "op", "client_version"}, []string{"base_version", "columns"}); err != nil {
		return nil, err
	}
	mutationID := object["mutation_id"]
	table := object["table"]
	authoredSchema := object["authored_schema"]
	op := object["op"]
	clientVersion := object["client_version"]
	pkObject, err := strictObject(object["pk"])
	if err != nil {
		return nil, err
	}
	if len(pkObject) != 1 {
		return nil, errors.New("pk must contain exactly one field")
	}
	var pkField string
	var pkValue json.RawMessage
	for key, value := range pkObject {
		pkField, pkValue = key, cloneBytes(value)
	}
	normalized := map[string]json.RawMessage{
		"mutation_id":     cloneBytes(mutationID),
		"table_id":        cloneBytes(table),
		"pk":              mustMarshalRawObject(map[string]json.RawMessage{"field_id": json.RawMessage(strconv.Quote(pkField)), "value": pkValue}),
		"authored_schema": cloneBytes(authoredSchema),
		"operation":       cloneBytes(op),
		"client_version":  cloneBytes(clientVersion),
	}
	if base, ok := object["base_version"]; ok {
		normalized["base_version"] = cloneBytes(base)
	}
	if columns, ok := object["columns"]; ok {
		columnObject, err := strictObject(columns)
		if err != nil {
			return nil, errors.New("columns must be an object")
		}
		if len(columnObject) == 0 {
			return nil, errors.New("columns must be nonempty")
		}
		keys := make([]string, 0, len(columnObject))
		for key := range columnObject {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		array := make([]map[string]json.RawMessage, 0, len(keys))
		for _, key := range keys {
			array = append(array, map[string]json.RawMessage{"field_id": json.RawMessage(strconv.Quote(key)), "value": cloneBytes(columnObject[key])})
		}
		normalized["columns"] = mustMarshalRaw(array)
	}
	return normalized, nil
}

func parseNormalizedColumns(raw json.RawMessage) ([]pushColumn, error) {
	if raw == nil {
		return nil, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return nil, errors.New("columns cannot be null")
	}
	if len(trimmed) > 0 && trimmed[0] == '{' {
		object, err := strictObject(trimmed)
		if err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(object))
		for key := range object {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result := make([]pushColumn, 0, len(keys))
		for _, key := range keys {
			result = append(result, pushColumn{Field: FieldID(key), Value: cloneBytes(object[key])})
		}
		return result, nil
	}
	values, err := parseJSONArray(raw, "columns")
	if err != nil {
		return nil, err
	}
	result := make([]pushColumn, 0, len(values))
	for _, value := range values {
		object, err := strictObject(value)
		if err != nil {
			return nil, err
		}
		if err := requirePushKeys(object, []string{"field_id", "value"}, nil); err != nil {
			return nil, err
		}
		field, err := requiredJSONString(object["field_id"], "column.field_id")
		if err != nil {
			return nil, err
		}
		result = append(result, pushColumn{Field: FieldID(field), Value: cloneBytes(object["value"])})
	}
	return result, nil
}

func parseLocalWritePayload(payload json.RawMessage) (localWriteInput, error) {
	object, err := strictObject(payload)
	if err != nil {
		return localWriteInput{}, err
	}
	allowed := []string{"authenticated_user_id", "user_id", "client_id", "mutation_id", "table_id", "table", "pk", "authored_schema", "operation", "op", "base_version", "client_version", "columns", "origin"}
	if err := requirePushKeys(object, []string{"client_id", "mutation_id", "pk", "authored_schema", "client_version"}, allowed); err != nil {
		return localWriteInput{}, err
	}
	userRaw := object["authenticated_user_id"]
	if userRaw == nil {
		userRaw = object["user_id"]
	}
	if userRaw == nil {
		return localWriteInput{}, errors.New("local/write requires authenticated_user_id")
	}
	user, err := requiredJSONString(userRaw, "authenticated_user_id")
	if err != nil {
		return localWriteInput{}, err
	}
	client, err := requiredJSONString(object["client_id"], "client_id")
	if err != nil {
		return localWriteInput{}, err
	}
	mutation, err := requiredJSONString(object["mutation_id"], "mutation_id")
	if err != nil {
		return localWriteInput{}, err
	}
	tableRaw := object["table_id"]
	if tableRaw == nil {
		tableRaw = object["table"]
	}
	if tableRaw == nil {
		return localWriteInput{}, errors.New("local/write requires table_id")
	}
	table, err := requiredJSONString(tableRaw, "table_id")
	if err != nil {
		return localWriteInput{}, err
	}
	operationRaw := object["operation"]
	if operationRaw == nil {
		operationRaw = object["op"]
	}
	if operationRaw == nil {
		return localWriteInput{}, errors.New("local/write requires operation")
	}
	operation, err := requiredJSONString(operationRaw, "operation")
	if err != nil {
		return localWriteInput{}, err
	}
	if operation != string(DMLOperationInsert) && operation != string(DMLOperationUpdate) && operation != string(DMLOperationDelete) {
		return localWriteInput{}, errors.New("local/write operation is not supported")
	}
	pkField, pkValue, err := parseLocalPrimaryKey(object["pk"])
	if err != nil {
		return localWriteInput{}, err
	}
	authored, err := parseSchemaWire(object["authored_schema"], "authored_schema")
	if err != nil {
		return localWriteInput{}, err
	}
	clientVersion, err := requiredJSONString(object["client_version"], "client_version")
	if err != nil {
		return localWriteInput{}, err
	}
	columns, err := parseNormalizedColumns(object["columns"])
	if err != nil {
		return localWriteInput{}, err
	}
	input := localWriteInput{AuthenticatedUser: UserID(user), Client: ClientID(client), Mutation: MutationID(mutation), Table: TableID(table), PKField: FieldID(pkField), PKValue: pkValue, AuthoredSchema: SchemaRef{Version: authored.Version, Hash: authored.HashBytes}, Operation: DMLOperation(operation), ClientVersion: ClientVersion(clientVersion), Columns: columns, Origin: "application"}
	if origin, ok := object["origin"]; ok {
		value, err := requiredJSONString(origin, "origin")
		if err != nil {
			return localWriteInput{}, err
		}
		if value != "application" && value != "server_apply" {
			return localWriteInput{}, errors.New("local/write origin is not supported")
		}
		input.Origin = value
	}
	if rawBase, ok := object["base_version"]; ok {
		value, err := requiredJSONString(rawBase, "base_version")
		if err != nil {
			return localWriteInput{}, err
		}
		input.HasPresentedBase = true
		input.PresentedBase = RowVersion(value)
	}
	// Validate the exact mutation vocabulary, timestamp, and values before the
	// local state transaction captures a base version.
	normalized := map[string]json.RawMessage{
		"mutation_id": json.RawMessage(strconv.Quote(mutation)), "table_id": json.RawMessage(strconv.Quote(table)),
		"pk":              mustMarshalRawObject(map[string]json.RawMessage{"field_id": json.RawMessage(strconv.Quote(pkField)), "value": pkValue}),
		"authored_schema": object["authored_schema"], "operation": json.RawMessage(strconv.Quote(operation)),
		"client_version": json.RawMessage(strconv.Quote(clientVersion)),
	}
	if input.HasPresentedBase {
		normalized["base_version"] = json.RawMessage(strconv.Quote(string(input.PresentedBase)))
	}
	if object["columns"] != nil {
		if columns == nil {
			normalized["columns"] = []byte("null")
		} else {
			normalized["columns"] = mustMarshalRaw(columnsToNormalized(columns))
		}
	}
	if _, err := vectors.ParseNormalizedMutation(user, client, mustMarshalRawObject(normalized)); err != nil {
		// A dependent local update/delete has no legal wire base yet. Validate it
		// with a non-authoritative placeholder, then retain HasBaseVersion=false.
		if (operation == string(DMLOperationUpdate) || operation == string(DMLOperationDelete)) && !input.HasPresentedBase {
			normalized["base_version"] = json.RawMessage(`"pending-local-predecessor"`)
			if _, retryErr := vectors.ParseNormalizedMutation(user, client, mustMarshalRawObject(normalized)); retryErr != nil {
				return localWriteInput{}, err
			}
		} else {
			return localWriteInput{}, err
		}
	}
	return input, nil
}

func parseLocalPrimaryKey(raw json.RawMessage) (string, json.RawMessage, error) {
	object, err := strictObject(raw)
	if err != nil {
		return "", nil, err
	}
	if len(object) == 2 {
		if err := requirePushKeys(object, []string{"field_id", "value"}, nil); err != nil {
			return "", nil, err
		}
		field, err := requiredJSONString(object["field_id"], "pk.field_id")
		return field, cloneBytes(object["value"]), err
	}
	if len(object) != 1 {
		return "", nil, errors.New("pk must contain exactly one field")
	}
	for field, value := range object {
		return field, cloneBytes(value), nil
	}
	return "", nil, errors.New("pk is empty")
}

func columnsToNormalized(columns []pushColumn) []map[string]json.RawMessage {
	result := make([]map[string]json.RawMessage, 0, len(columns))
	for _, column := range columns {
		result = append(result, map[string]json.RawMessage{"field_id": json.RawMessage(strconv.Quote(string(column.Field))), "value": cloneBytes(column.Value)})
	}
	return result
}

func parseSchemaWire(raw json.RawMessage, name string) (schemaWireParsed, error) {
	object, err := strictObject(raw)
	if err != nil {
		return schemaWireParsed{}, fmt.Errorf("decode %s: %w", name, err)
	}
	if err := requirePushKeys(object, []string{"version", "hash"}, nil); err != nil {
		return schemaWireParsed{}, fmt.Errorf("%s: %w", name, err)
	}
	version, err := parsePositiveJSONUint(object["version"], name+".version")
	if err != nil {
		return schemaWireParsed{}, err
	}
	hashText, err := requiredJSONString(object["hash"], name+".hash")
	if err != nil {
		return schemaWireParsed{}, err
	}
	if len(hashText) != 64 || strings.ToLower(hashText) != hashText {
		return schemaWireParsed{}, fmt.Errorf("%s.hash is not lowercase SHA-256", name)
	}
	decoded, err := hex.DecodeString(hashText)
	if err != nil || len(decoded) != 32 {
		return schemaWireParsed{}, fmt.Errorf("%s.hash is not SHA-256", name)
	}
	var hash [32]byte
	copy(hash[:], decoded)
	return schemaWireParsed{Version: version, Hash: hash, HashBytes: hash, Wire: pushSchemaWire{Version: version, Hash: hashText}}, nil
}

type schemaWireParsed struct {
	Version   uint64
	Hash      [32]byte
	HashBytes [32]byte
	Wire      pushSchemaWire
}

func strictObject(raw []byte) (map[string]json.RawMessage, error) {
	if err := jsonstrict.ValidateValue(raw); err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("JSON value is not an object")
	}
	return object, nil
}

func requirePushKeys(object map[string]json.RawMessage, required, optional []string) error {
	allowed := make(map[string]struct{}, len(required)+len(optional))
	for _, key := range required {
		allowed[key] = struct{}{}
		if _, ok := object[key]; !ok {
			return fmt.Errorf("JSON object is missing member %q", key)
		}
	}
	for _, key := range optional {
		allowed[key] = struct{}{}
	}
	for key := range object {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("JSON object has unknown member %q", key)
		}
	}
	return nil
}

func requiredJSONString(raw json.RawMessage, name string) (string, error) {
	var value string
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil || value == "" {
		return "", fmt.Errorf("%s must be a nonempty JSON string", name)
	}
	return value, nil
}

func parsePositiveJSONUint(raw json.RawMessage, name string) (uint64, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || (len(trimmed) > 1 && trimmed[0] == '0') || trimmed[0] == '-' {
		return 0, fmt.Errorf("%s is not a positive canonical integer", name)
	}
	for _, character := range string(trimmed) {
		if character < '0' || character > '9' {
			return 0, fmt.Errorf("%s is not a positive canonical integer", name)
		}
	}
	value, err := strconv.ParseUint(string(trimmed), 10, 64)
	if err != nil || value == 0 || value > 9007199254740991 {
		return 0, fmt.Errorf("%s is outside the positive portable range", name)
	}
	return value, nil
}

func parseJSONArray(raw json.RawMessage, name string) ([]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, fmt.Errorf("%s must be an array", name)
	}
	var values []json.RawMessage
	if err := json.Unmarshal(trimmed, &values); err != nil || values == nil {
		return nil, fmt.Errorf("%s must be an array", name)
	}
	return values, nil
}

func cloneRawObject(object map[string]json.RawMessage) map[string]json.RawMessage {
	result := make(map[string]json.RawMessage, len(object))
	for key, value := range object {
		result[key] = cloneBytes(value)
	}
	return result
}

func cloneRawMap(object map[string]json.RawMessage) map[string]json.RawMessage {
	return cloneRawObject(object)
}

func mustMarshalRaw(value any) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}

func mustMarshalRawObject(value map[string]json.RawMessage) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}

func stripFingerprintDomain(preimage []byte, domain string) ([]byte, error) {
	prefix := []byte(domain)
	if !bytes.HasPrefix(preimage, prefix) {
		return nil, errors.New("fingerprint preimage has an unexpected domain")
	}
	return cloneBytes(preimage[len(prefix):]), nil
}

func canonicalJSONValue(raw []byte) ([]byte, error) {
	if err := jsonstrict.ValidateValue(raw); err != nil {
		// jsonstrict intentionally requires an object. Scalar wire values are
		// still canonicalized by a small decoder below.
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || trimmed[0] == '{' || trimmed[0] == '[' {
			return nil, err
		}
		var value any
		decoder := json.NewDecoder(bytes.NewReader(trimmed))
		decoder.UseNumber()
		if decodeErr := decoder.Decode(&value); decodeErr != nil {
			return nil, decodeErr
		}
		encoded, encodeErr := json.Marshal(value)
		if encodeErr != nil {
			return nil, encodeErr
		}
		return jcs.Transform(encoded)
	}
	return jcs.Transform(raw)
}

func canonicalPushOutcome(outcome pushOutcomeWire) ([]byte, error) {
	encoded, err := json.Marshal(outcome)
	if err != nil {
		return nil, err
	}
	canonical, err := jcs.Transform(encoded)
	if err != nil {
		return nil, err
	}
	if len(canonical) > pushResponseLimit {
		return nil, errors.New("push outcome exceeds byte limit")
	}
	return canonical, nil
}

func canonicalPushResponse(response pushResponse) ([]byte, error) {
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	return jcs.Transform(encoded)
}

func pushSchemaWireFromRef(ref SchemaRef) pushSchemaWire {
	return pushSchemaWire{Version: ref.Version, Hash: hex.EncodeToString(ref.Hash[:])}
}

func schemaRefFromWire(ref pushSchemaWire) SchemaRef {
	decoded, _ := hex.DecodeString(ref.Hash)
	var hash [32]byte
	copy(hash[:], decoded)
	return SchemaRef{Version: ref.Version, Hash: hash}
}

func schemaPointer(value pushSchemaWire) *pushSchemaWire { return &value }
func stringPointer(value string) *string                 { return &value }
func timePointer(value time.Time) *time.Time             { return &value }
func dereferenceFieldID(value *FieldID) FieldID {
	if value == nil {
		return ""
	}
	return *value
}
func dereferenceRowVersion(value *RowVersion) RowVersion {
	if value == nil {
		return ""
	}
	return *value
}
func firstPKField(value map[string]json.RawMessage) FieldID {
	for key := range value {
		return FieldID(key)
	}
	return ""
}
func firstPKValue(value map[string]json.RawMessage) json.RawMessage {
	for _, raw := range value {
		return raw
	}
	return nil
}

func canonicalClockTime(value time.Time) time.Time {
	return value.Round(0).UTC().Truncate(time.Microsecond)
}

func formatCanonicalTime(value time.Time) string {
	return canonicalClockTime(value).Format("2006-01-02T15:04:05.000000Z")
}

func boundedPushMessage(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return "push mutation was rejected"
	}
	if len(message) > 256 {
		return message[:256]
	}
	return message
}
