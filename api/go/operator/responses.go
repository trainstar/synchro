package operator

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
)

const (
	maximumSlotNameBytes          = 63
	maximumSnapshotNameBytes      = 128
	projectionBootstrapSlotSuffix = "_bootstrap_"
	projectionBootstrapNonceBytes = 8
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ProjectionBootstrapResult identifies one activated projection bootstrap.
type ProjectionBootstrapResult struct {
	BootstrapID            string   `json:"bootstrap_id"`
	RegistryGeneration     int64    `json:"registry_generation"`
	SourceStreamGeneration string   `json:"source_stream_generation"`
	ActiveSlotName         string   `json:"active_slot_name"`
	CandidateSlotName      string   `json:"candidate_slot_name"`
	SchemaVersion          *int64   `json:"schema_version"`
	SchemaHash             *string  `json:"schema_hash"`
	ActivationBarrier      string   `json:"activation_barrier"`
	AffectedScopes         []string `json:"affected_scopes"`
}

type projectionBootstrapPrepared struct {
	BootstrapID        string  `json:"bootstrap_id"`
	RegistryGeneration int64   `json:"registry_generation"`
	SchemaVersion      *int64  `json:"schema_version"`
	SchemaHash         *string `json:"schema_hash"`
	CandidateSlotName  string  `json:"candidate_slot_name"`
}

type snapshotMarker struct {
	XID   string `json:"marker_xid"`
	Nonce string `json:"marker_nonce"`
}

type projectionBootstrapWALMarker struct {
	BootstrapID string `json:"bootstrap_id"`
	MarkerLSN   string `json:"marker_lsn"`
}

type projectionBootstrapStage struct {
	BootstrapID        string `json:"bootstrap_id"`
	RegistryGeneration int64  `json:"registry_generation"`
	CandidateSlotName  string `json:"candidate_slot_name"`
	ConsistentPoint    string `json:"consistent_point"`
	Rows               int64  `json:"rows"`
	Versions           int64  `json:"versions"`
	Edges              int64  `json:"edges"`
	Fences             int64  `json:"fences"`
	Scopes             int64  `json:"scopes"`
}

type projectionBootstrapBarrier struct {
	BootstrapID      string `json:"bootstrap_id"`
	StreamGeneration string `json:"stream_generation"`
	CommitLSN        string `json:"commit_lsn"`
	EndLSN           string `json:"end_lsn"`
}

type projectionBootstrapStatus struct {
	BootstrapID                    string          `json:"bootstrap_id"`
	Lifecycle                      string          `json:"lifecycle"`
	SourceRegistryGeneration       int64           `json:"source_registry_generation"`
	TargetRegistryGeneration       int64           `json:"target_registry_generation"`
	CandidateSlotName              string          `json:"candidate_slot_name"`
	ConsistentPoint                *string         `json:"consistent_point"`
	ActivationBarrier              *string         `json:"activation_barrier"`
	CandidateMaterializedCommitLSN *string         `json:"candidate_materialized_commit_lsn"`
	CandidateMaterializedEndLSN    *string         `json:"candidate_materialized_end_lsn"`
	CandidateAcknowledgedEndLSN    *string         `json:"candidate_acknowledged_end_lsn"`
	CandidateVerified              bool            `json:"candidate_verified"`
	AffectedScopes                 json.RawMessage `json:"affected_scopes"`
}

type projectionBootstrapActivation struct {
	BootstrapID        string   `json:"bootstrap_id"`
	RegistryGeneration int64    `json:"registry_generation"`
	SchemaVersion      *int64   `json:"schema_version"`
	SchemaHash         *string  `json:"schema_hash"`
	ActivationBarrier  string   `json:"activation_barrier"`
	AffectedScopes     []string `json:"affected_scopes"`
}

type projectionBootstrapAbort struct {
	BootstrapID       string `json:"reset_id"`
	CandidateSlotName string `json:"candidate_slot_name"`
}

type projectionBootstrapSlotDropState struct {
	Present bool `json:"present"`
	Active  bool `json:"active"`
	Valid   bool `json:"valid"`
}

func newCandidateSlotName(activeSlot string) (string, error) {
	var entropy [projectionBootstrapNonceBytes]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", errors.New("projection bootstrap candidate entropy is unavailable")
	}
	return candidateSlotName(activeSlot, hex.EncodeToString(entropy[:]))
}

func candidateSlotName(activeSlot, nonce string) (string, error) {
	if !validSlotName(activeSlot) || len(nonce) != projectionBootstrapNonceBytes*2 {
		return "", errors.New("active projection bootstrap slot is invalid")
	}
	if decoded, err := hex.DecodeString(nonce); err != nil || nonce != hex.EncodeToString(decoded) {
		return "", errors.New("projection bootstrap candidate nonce is invalid")
	}
	maximumPrefixBytes := maximumSlotNameBytes - len(projectionBootstrapSlotSuffix)
	maximumPrefixBytes -= len(nonce)
	if maximumPrefixBytes < 1 {
		return "", errors.New("projection bootstrap candidate suffix is invalid")
	}
	prefix := activeSlot
	if len(prefix) > maximumPrefixBytes {
		prefix = prefix[:maximumPrefixBytes]
	}
	candidate := prefix + projectionBootstrapSlotSuffix + nonce
	if candidate == activeSlot || !validSlotName(candidate) {
		return "", errors.New("projection bootstrap candidate name is invalid")
	}
	return candidate, nil
}

func parseWALMarker(raw []byte, bootstrapID string) (string, error) {
	var marker projectionBootstrapWALMarker
	if decodeStrictObject(raw, &marker, "bootstrap_id", "marker_lsn") != nil ||
		marker.BootstrapID != bootstrapID || marker.MarkerLSN == "" {
		return "", errors.New("projection bootstrap WAL marker is invalid")
	}
	return marker.MarkerLSN, nil
}

func validSlotName(value string) bool {
	if value == "" || len(value) > maximumSlotNameBytes {
		return false
	}
	for _, character := range []byte(value) {
		if character != '_' && (character < '0' || character > '9') && (character < 'a' || character > 'z') {
			return false
		}
	}
	return true
}

func validSnapshotName(value string) bool {
	if value == "" || len(value) > maximumSnapshotNameBytes {
		return false
	}
	for _, character := range []byte(value) {
		if character != '-' && character != '_' &&
			(character < '0' || character > '9') &&
			(character < 'A' || character > 'Z') &&
			(character < 'a' || character > 'z') {
			return false
		}
	}
	return true
}

func parsePrepared(raw []byte, registryGeneration int64, candidateSlot string) (projectionBootstrapPrepared, error) {
	var prepared projectionBootstrapPrepared
	if decodeStrictObject(raw, &prepared,
		"bootstrap_id", "registry_generation", "schema_version", "schema_hash", "candidate_slot_name",
	) != nil ||
		!uuidPattern.MatchString(prepared.BootstrapID) ||
		prepared.RegistryGeneration != registryGeneration ||
		prepared.CandidateSlotName != candidateSlot ||
		!validSchemaPair(prepared.SchemaVersion, prepared.SchemaHash) {
		return prepared, errors.New("prepared projection bootstrap response is invalid")
	}
	return prepared, nil
}

func parseSnapshotMarker(raw []byte) (snapshotMarker, error) {
	var marker snapshotMarker
	if decodeStrictObject(raw, &marker, "marker_xid", "marker_nonce") != nil ||
		marker.XID == "" || !uuidPattern.MatchString(marker.Nonce) {
		return snapshotMarker{}, errors.New("projection bootstrap snapshot marker is invalid")
	}
	return marker, nil
}

func parseStage(raw []byte, bootstrapID string, registryGeneration int64, candidateSlot, consistentPoint string) error {
	var stage projectionBootstrapStage
	if decodeStrictObject(raw, &stage,
		"bootstrap_id", "registry_generation", "candidate_slot_name", "consistent_point",
		"rows", "versions", "edges", "fences", "scopes",
	) != nil ||
		stage.BootstrapID != bootstrapID ||
		stage.RegistryGeneration != registryGeneration ||
		stage.CandidateSlotName != candidateSlot ||
		stage.ConsistentPoint != consistentPoint ||
		stage.Rows < 0 || stage.Versions < 0 || stage.Edges < 0 || stage.Fences < 0 || stage.Scopes < 0 {
		return errors.New("staged projection bootstrap response is invalid")
	}
	return nil
}

func parseBarrier(raw []byte, bootstrapID, streamGeneration string) (string, error) {
	var barrier projectionBootstrapBarrier
	if decodeStrictObject(raw, &barrier,
		"bootstrap_id", "stream_generation", "commit_lsn", "end_lsn",
	) != nil ||
		barrier.BootstrapID != bootstrapID ||
		barrier.StreamGeneration != streamGeneration ||
		barrier.CommitLSN == "" || barrier.EndLSN == "" {
		return "", errors.New("projection bootstrap barrier response is invalid")
	}
	return barrier.EndLSN, nil
}

func parseStatus(raw []byte, bootstrapID, candidateSlot string) (projectionBootstrapStatus, error) {
	var status projectionBootstrapStatus
	if decodeStrictObject(raw, &status,
		"bootstrap_id", "lifecycle", "source_registry_generation", "target_registry_generation",
		"candidate_slot_name", "consistent_point", "activation_barrier",
		"candidate_materialized_commit_lsn", "candidate_materialized_end_lsn",
		"candidate_acknowledged_end_lsn", "candidate_verified", "affected_scopes",
	) != nil ||
		status.BootstrapID != bootstrapID ||
		status.Lifecycle == "" ||
		status.SourceRegistryGeneration <= 0 || status.TargetRegistryGeneration <= 0 ||
		status.CandidateSlotName != candidateSlot ||
		status.ConsistentPoint == nil || *status.ConsistentPoint == "" ||
		len(status.AffectedScopes) == 0 {
		return projectionBootstrapStatus{}, errors.New("projection bootstrap status response is invalid")
	}
	return status, nil
}

func candidateReady(status projectionBootstrapStatus, barrier string) bool {
	return barrier != "" &&
		status.Lifecycle == "catching_up" &&
		status.ActivationBarrier != nil && *status.ActivationBarrier == barrier &&
		status.CandidateMaterializedEndLSN != nil && *status.CandidateMaterializedEndLSN == barrier &&
		status.CandidateAcknowledgedEndLSN != nil && *status.CandidateAcknowledgedEndLSN == barrier &&
		status.CandidateVerified
}

func parseActivation(raw []byte, bootstrapID string, registryGeneration int64, candidateSlot, barrier string) (ProjectionBootstrapResult, error) {
	var activation projectionBootstrapActivation
	if decodeStrictObject(raw, &activation,
		"bootstrap_id", "registry_generation", "schema_version", "schema_hash",
		"activation_barrier", "affected_scopes",
	) != nil ||
		activation.BootstrapID != bootstrapID ||
		activation.RegistryGeneration != registryGeneration ||
		activation.ActivationBarrier != barrier ||
		len(activation.AffectedScopes) == 0 ||
		!validSchemaPair(activation.SchemaVersion, activation.SchemaHash) ||
		candidateSlot == "" {
		return ProjectionBootstrapResult{}, errors.New("activated projection bootstrap response is invalid")
	}
	return ProjectionBootstrapResult{
		BootstrapID:        activation.BootstrapID,
		RegistryGeneration: activation.RegistryGeneration,
		CandidateSlotName:  candidateSlot,
		SchemaVersion:      activation.SchemaVersion,
		SchemaHash:         activation.SchemaHash,
		ActivationBarrier:  activation.ActivationBarrier,
		AffectedScopes:     append([]string(nil), activation.AffectedScopes...),
	}, nil
}

func parseAbort(raw []byte, bootstrapID, candidateSlot string) error {
	var aborted projectionBootstrapAbort
	if decodeStrictObject(raw, &aborted, "reset_id", "candidate_slot_name") != nil ||
		aborted.BootstrapID != bootstrapID || aborted.CandidateSlotName != candidateSlot {
		return errors.New("aborted projection bootstrap response is invalid")
	}
	return nil
}

func parseSlotDropState(raw []byte) (projectionBootstrapSlotDropState, error) {
	var state projectionBootstrapSlotDropState
	if decodeStrictObject(raw, &state, "present", "active", "valid") != nil ||
		(!state.Present && (state.Active || !state.Valid)) {
		return projectionBootstrapSlotDropState{}, errors.New("projection bootstrap candidate slot state is invalid")
	}
	return state, nil
}

func validSchemaPair(version *int64, hash *string) bool {
	if version == nil || hash == nil {
		return version == nil && hash == nil
	}
	if *version <= 0 || len(*hash) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(*hash)
	return err == nil && *hash == hex.EncodeToString(decoded)
}

func decodeStrictObject(raw []byte, destination any, fields ...string) error {
	if err := json.Unmarshal(raw, destination); err != nil {
		return errors.New("JSON response is invalid")
	}
	allowed := make(map[string]bool, len(fields))
	for _, field := range fields {
		allowed[field] = false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("JSON response is invalid")
	}
	for decoder.More() {
		token, err = decoder.Token()
		key, keyOK := token.(string)
		seen, allowedKey := allowed[key]
		if err != nil || !keyOK || !allowedKey || seen {
			return errors.New("JSON response is invalid")
		}
		allowed[key] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return errors.New("JSON response is invalid")
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return errors.New("JSON response is invalid")
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("JSON response is invalid")
	}
	for _, seen := range allowed {
		if !seen {
			return errors.New("JSON response is invalid")
		}
	}
	return nil
}
