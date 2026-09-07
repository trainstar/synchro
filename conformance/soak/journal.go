package soak

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/trainstar/synchro/conformance/invariants"
)

const (
	// DefaultJournalBytes bounds one journal file.
	DefaultJournalBytes int64 = 16 << 20
	// DefaultJournalRecords bounds operation, observation, and outcome records together.
	DefaultJournalRecords = 1 << 16
	// DefaultJournalLineBytes bounds one encoded JSON line.
	DefaultJournalLineBytes = 1 << 20
	// MaximumJournalAttachments bounds private wire-body identities in one observation.
	MaximumJournalAttachments = 1024
)

var (
	// ErrJournalPathRequired reports a missing caller-owned journal path.
	ErrJournalPathRequired = errors.New("soak journal path is required")
	// ErrJournalBound reports a journal size or record bound violation.
	ErrJournalBound = errors.New("soak journal bound exceeded")
	// ErrInvalidJournal reports malformed journal content.
	ErrInvalidJournal = errors.New("soak journal is invalid")
)

// JournalLimits bounds journal bytes, lines, and encoded line size.
type JournalLimits struct {
	MaxBytes     int64 `json:"max_bytes"`
	MaxRecords   int   `json:"max_records"`
	MaxLineBytes int   `json:"max_line_bytes"`
}

func (l JournalLimits) normalized() (JournalLimits, error) {
	if l.MaxBytes == 0 {
		l.MaxBytes = DefaultJournalBytes
	}
	if l.MaxRecords == 0 {
		l.MaxRecords = DefaultJournalRecords
	}
	if l.MaxLineBytes == 0 {
		l.MaxLineBytes = DefaultJournalLineBytes
	}
	if l.MaxBytes < 1 || l.MaxBytes > DefaultJournalBytes || l.MaxRecords < 1 || l.MaxRecords > DefaultJournalRecords || l.MaxLineBytes < 1 || l.MaxLineBytes > DefaultJournalLineBytes {
		return JournalLimits{}, fmt.Errorf("%w: limits are outside the supported range", ErrJournalBound)
	}
	return l, nil
}

// Journal is the decoded bounded run journal.
type Journal struct {
	Seed            uint64              `json:"seed"`
	Config          Config              `json:"config"`
	CatalogIdentity CatalogIdentity     `json:"catalog_identity"`
	PlanDigest      string              `json:"plan_digest"`
	Operations      []Operation         `json:"operations"`
	OperationFacts  []OperationFact     `json:"operation_facts"`
	Observations    []ObservationRecord `json:"observations"`
}

func validLowerHexDigest64(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

// planDigest binds the seed, configuration, catalog identity, and operations.
// Replay verifies it so a tampered journal field cannot reproduce silently.
func planDigest(seed uint64, config Config, identity CatalogIdentity, operations []Operation) (string, error) {
	payload, err := json.Marshal(struct {
		Seed            uint64          `json:"seed"`
		Config          Config          `json:"config"`
		CatalogIdentity CatalogIdentity `json:"catalog_identity"`
		Operations      []Operation     `json:"operations"`
	}{seed, config, identity, operations})
	if err != nil {
		return "", fmt.Errorf("encode soak plan digest: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// OperationFact records completion or failure for one deterministic operation.
type OperationFact struct {
	Sequence            uint64 `json:"sequence"`
	Status              string `json:"status"`
	ObservationSequence uint64 `json:"observation_sequence,omitempty"`
	FailureCode         string `json:"failure_code,omitempty"`
}

// ObservationRecord records one observation sequence and bounded wire-body identities.
type ObservationRecord struct {
	Sequence    uint64                   `json:"sequence"`
	Attachments []WireAttachmentIdentity `json:"attachments"`
}

// WireAttachmentIdentity identifies a raw body without storing private body bytes.
type WireAttachmentIdentity struct {
	ExchangeSequence uint64 `json:"exchange_sequence"`
	RequestBytes     int64  `json:"request_bytes"`
	RequestSHA256    string `json:"request_sha256"`
	ResponseBytes    int64  `json:"response_bytes"`
	ResponseSHA256   string `json:"response_sha256"`
}

type journalWriter struct {
	file            *os.File
	limits          JournalLimits
	bytesWritten    int64
	recordsWritten  int
	lastOperation   uint64
	lastObservation uint64
	lastFact        uint64
	closed          bool
}

type journalRecord struct {
	Type            string             `json:"type"`
	Seed            *uint64            `json:"seed,omitempty"`
	Config          *Config            `json:"config,omitempty"`
	CatalogIdentity *CatalogIdentity   `json:"catalog_identity,omitempty"`
	PlanDigest      *string            `json:"plan_digest,omitempty"`
	Operation       *Operation         `json:"operation,omitempty"`
	OperationFact   *OperationFact     `json:"operation_fact,omitempty"`
	Observation     *ObservationRecord `json:"observation,omitempty"`
}

func newJournalWriter(path string, plan Plan) (*journalWriter, error) {
	if path == "" {
		return nil, ErrJournalPathRequired
	}
	if err := plan.Config.validate(); err != nil {
		return nil, err
	}
	if err := plan.CatalogIdentity.validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidJournal, err)
	}
	normalized, err := (JournalLimits{}).normalized()
	if err != nil {
		return nil, err
	}
	if info, statErr := os.Lstat(path); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: journal path is a symlink", ErrInvalidJournal)
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect journal path: %w", statErr)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open soak journal: %w", err)
	}
	digest, err := planDigest(plan.Seed, plan.Config, plan.CatalogIdentity, plan.Operations)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	writer := &journalWriter{file: file, limits: normalized}
	if err := writer.writeRecord(journalRecord{
		Type:            "header",
		Seed:            uint64Pointer(plan.Seed),
		Config:          configPointer(plan.Config),
		CatalogIdentity: catalogIdentityPointer(plan.CatalogIdentity),
		PlanDigest:      &digest,
	}); err != nil {
		_ = file.Close()
		return nil, err
	}
	return writer, nil
}

func (w *journalWriter) RecordOperation(operation Operation) error {
	if w == nil || w.closed || w.file == nil {
		return ErrInvalidJournal
	}
	if operation.Sequence == 0 || operation.Sequence != w.lastOperation+1 || !validOperationKind(operation.Kind) || operation.UserID == "" || operation.ClientID == "" || operation.ScopeID == "" || operation.SchemaVersion == 0 {
		return fmt.Errorf("%w: operation sequence is not contiguous", ErrInvalidJournal)
	}
	if !equalObservationSurfaces(operation.RequiredObservationSurfaces, requiredObservationSurfaces(operation.Kind)) {
		return fmt.Errorf("%w: operation observation surfaces are invalid", ErrInvalidJournal)
	}
	if err := validateOperationInput(operation); err != nil {
		return fmt.Errorf("%w: operation input: %w", ErrInvalidJournal, err)
	}
	if w.recordsWritten >= w.limits.MaxRecords {
		return ErrJournalBound
	}
	if err := w.writeRecord(journalRecord{Type: "operation", Operation: &operation}); err != nil {
		return err
	}
	w.lastOperation = operation.Sequence
	return nil
}

func (w *journalWriter) RecordObservation(observation invariants.Observation) error {
	if w == nil || w.closed || w.file == nil {
		return ErrInvalidJournal
	}
	if observation.Sequence == 0 || observation.Sequence != w.lastObservation+1 || observation.Sequence > w.lastOperation {
		return fmt.Errorf("%w: observation sequence is not contiguous", ErrInvalidJournal)
	}
	attachments, err := wireAttachmentIdentities(observation.WireExchanges)
	if err != nil {
		return err
	}
	record := ObservationRecord{Sequence: observation.Sequence, Attachments: attachments}
	if w.recordsWritten >= w.limits.MaxRecords {
		return ErrJournalBound
	}
	if err := w.writeRecord(journalRecord{Type: "observation", Observation: &record}); err != nil {
		return err
	}
	w.lastObservation = observation.Sequence
	return nil
}

func (w *journalWriter) RecordCompletion(sequence, observationSequence uint64) error {
	return w.recordFact(OperationFact{Sequence: sequence, Status: "completed", ObservationSequence: observationSequence})
}

func (w *journalWriter) RecordFailure(sequence uint64, code string) error {
	if code == "" || len(code) > 64 {
		return fmt.Errorf("%w: failure code is invalid", ErrInvalidJournal)
	}
	return w.recordFact(OperationFact{Sequence: sequence, Status: "failed", FailureCode: code})
}

func (w *journalWriter) recordFact(fact OperationFact) error {
	if w == nil || w.closed || w.file == nil {
		return ErrInvalidJournal
	}
	if fact.Sequence == 0 || fact.Sequence != w.lastFact+1 || fact.Sequence > w.lastOperation {
		return fmt.Errorf("%w: operation fact sequence is not contiguous", ErrInvalidJournal)
	}
	if fact.Status != "completed" && fact.Status != "failed" {
		return fmt.Errorf("%w: operation fact status is invalid", ErrInvalidJournal)
	}
	if fact.Status == "completed" && fact.ObservationSequence == 0 {
		return fmt.Errorf("%w: completed operation has no observation", ErrInvalidJournal)
	}
	if fact.Status == "failed" && fact.ObservationSequence != 0 {
		return fmt.Errorf("%w: failed operation has an observation", ErrInvalidJournal)
	}
	if w.recordsWritten >= w.limits.MaxRecords {
		return ErrJournalBound
	}
	if err := w.writeRecord(journalRecord{Type: "operation-fact", OperationFact: &fact}); err != nil {
		return err
	}
	w.lastFact = fact.Sequence
	return nil
}

// Close flushes and closes the journal.
func (w *journalWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}
	w.closed = true
	if w.file == nil {
		return nil
	}
	syncErr := w.file.Sync()
	closeErr := w.file.Close()
	return errors.Join(syncErr, closeErr)
}

// ReadJournal reads and validates one bounded JSON-lines journal.
func ReadJournal(path string) (Journal, error) {
	if path == "" {
		return Journal{}, ErrJournalPathRequired
	}
	info, err := os.Lstat(path)
	if err != nil {
		return Journal{}, fmt.Errorf("inspect soak journal: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return Journal{}, fmt.Errorf("%w: journal is not a regular file", ErrInvalidJournal)
	}
	limits, err := (JournalLimits{}).normalized()
	if err != nil {
		return Journal{}, err
	}
	if info.Size() > limits.MaxBytes {
		return Journal{}, fmt.Errorf("%w: journal bytes", ErrJournalBound)
	}
	file, err := os.Open(path)
	if err != nil {
		return Journal{}, fmt.Errorf("open soak journal: %w", err)
	}
	defer file.Close()

	journal := Journal{}
	reader := bufio.NewReader(file)
	var totalBytes int64
	lineCount := 0
	headerRead := false
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) != 0 {
			if len(line) > limits.MaxLineBytes {
				return Journal{}, fmt.Errorf("%w: journal line bytes", ErrJournalBound)
			}
			totalBytes += int64(len(line))
			if totalBytes > limits.MaxBytes {
				return Journal{}, fmt.Errorf("%w: journal bytes", ErrJournalBound)
			}
			lineCount++
			if lineCount > limits.MaxRecords {
				return Journal{}, fmt.Errorf("%w: journal records", ErrJournalBound)
			}
			if err := decodeJournalLine(line, &journal, &headerRead); err != nil {
				return Journal{}, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return Journal{}, fmt.Errorf("read soak journal: %w", readErr)
		}
	}
	if !headerRead || len(journal.Operations) == 0 || len(journal.Operations) != journal.Config.OperationCount {
		return Journal{}, fmt.Errorf("%w: header or operations are missing", ErrInvalidJournal)
	}
	if err := journal.CatalogIdentity.validate(); err != nil {
		return Journal{}, fmt.Errorf("%w: catalog identity: %v", ErrInvalidJournal, err)
	}
	if len(journal.OperationFacts) > len(journal.Operations) || len(journal.Observations) > len(journal.Operations) {
		return Journal{}, fmt.Errorf("%w: journal records exceed operations", ErrInvalidJournal)
	}
	return journal, nil
}

func (w *journalWriter) writeRecord(record journalRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode soak journal record: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > w.limits.MaxLineBytes || w.bytesWritten+int64(len(encoded)) > w.limits.MaxBytes {
		return ErrJournalBound
	}
	if _, err := w.file.Write(encoded); err != nil {
		return fmt.Errorf("write soak journal record: %w", err)
	}
	w.bytesWritten += int64(len(encoded))
	w.recordsWritten++
	return nil
}

func decodeJournalLine(line []byte, journal *Journal, headerRead *bool) error {
	if journal == nil || headerRead == nil {
		return ErrInvalidJournal
	}
	decoder := json.NewDecoder(bytesTrimNewline(line))
	decoder.DisallowUnknownFields()
	var record journalRecord
	if err := decoder.Decode(&record); err != nil {
		return fmt.Errorf("%w: decode record: %v", ErrInvalidJournal, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: trailing record data", ErrInvalidJournal)
		}
		return fmt.Errorf("%w: trailing record data: %v", ErrInvalidJournal, err)
	}
	switch record.Type {
	case "header":
		if *headerRead || record.Seed == nil || record.Config == nil || record.CatalogIdentity == nil || record.PlanDigest == nil || record.Operation != nil || record.OperationFact != nil || record.Observation != nil {
			return fmt.Errorf("%w: header is malformed or repeated", ErrInvalidJournal)
		}
		if err := record.Config.validate(); err != nil {
			return err
		}
		if err := record.CatalogIdentity.validate(); err != nil {
			return fmt.Errorf("%w: catalog identity: %v", ErrInvalidJournal, err)
		}
		if !validLowerHexDigest64(*record.PlanDigest) {
			return fmt.Errorf("%w: plan digest is malformed", ErrInvalidJournal)
		}
		journal.Seed = *record.Seed
		journal.Config = cloneConfig(*record.Config)
		journal.CatalogIdentity = *record.CatalogIdentity
		journal.PlanDigest = *record.PlanDigest
		*headerRead = true
	case "operation":
		if !*headerRead || record.Operation == nil || record.Seed != nil || record.Config != nil || record.CatalogIdentity != nil || record.OperationFact != nil || record.Observation != nil {
			return fmt.Errorf("%w: operation record is malformed", ErrInvalidJournal)
		}
		operation := *record.Operation
		if operation.Sequence != uint64(len(journal.Operations)+1) || !validOperationKind(operation.Kind) || operation.UserID == "" || operation.ClientID == "" || operation.ScopeID == "" || operation.SchemaVersion == 0 {
			return fmt.Errorf("%w: operation sequence or identity is invalid", ErrInvalidJournal)
		}
		if !equalObservationSurfaces(operation.RequiredObservationSurfaces, requiredObservationSurfaces(operation.Kind)) {
			return fmt.Errorf("%w: operation observation surfaces are invalid", ErrInvalidJournal)
		}
		if err := validateOperationInput(operation); err != nil {
			return fmt.Errorf("%w: operation input: %v", ErrInvalidJournal, err)
		}
		journal.Operations = append(journal.Operations, operation)
	case "operation-fact":
		if !*headerRead || record.OperationFact == nil || record.Seed != nil || record.Config != nil || record.CatalogIdentity != nil || record.Operation != nil || record.Observation != nil {
			return fmt.Errorf("%w: operation fact record is malformed", ErrInvalidJournal)
		}
		fact := *record.OperationFact
		if fact.Sequence != uint64(len(journal.OperationFacts)+1) || fact.Sequence > uint64(len(journal.Operations)) {
			return fmt.Errorf("%w: operation fact sequence is invalid", ErrInvalidJournal)
		}
		if fact.Status != "completed" && fact.Status != "failed" || fact.Status == "completed" && fact.ObservationSequence == 0 || fact.Status == "failed" && fact.ObservationSequence != 0 {
			return fmt.Errorf("%w: operation fact is invalid", ErrInvalidJournal)
		}
		journal.OperationFacts = append(journal.OperationFacts, fact)
	case "observation":
		if !*headerRead || record.Observation == nil || record.Seed != nil || record.Config != nil || record.CatalogIdentity != nil || record.Operation != nil || record.OperationFact != nil {
			return fmt.Errorf("%w: observation record is malformed", ErrInvalidJournal)
		}
		observation := *record.Observation
		if observation.Sequence == 0 || observation.Sequence != uint64(len(journal.Observations)+1) || observation.Sequence > uint64(len(journal.Operations)) {
			return fmt.Errorf("%w: observation sequence is invalid", ErrInvalidJournal)
		}
		if len(observation.Attachments) > MaximumJournalAttachments {
			return fmt.Errorf("%w: observation attachments exceed limit", ErrJournalBound)
		}
		for _, attachment := range observation.Attachments {
			if err := validateAttachment(attachment); err != nil {
				return err
			}
		}
		journal.Observations = append(journal.Observations, observation)
	default:
		return fmt.Errorf("%w: unknown record type %q", ErrInvalidJournal, record.Type)
	}
	return nil
}

func wireAttachmentIdentities(exchanges []invariants.WireExchangeObservation) ([]WireAttachmentIdentity, error) {
	if len(exchanges) > MaximumJournalAttachments {
		return nil, ErrJournalBound
	}
	attachments := make([]WireAttachmentIdentity, 0, len(exchanges))
	seen := make(map[uint64]struct{}, len(exchanges))
	for _, exchange := range exchanges {
		if exchange.Sequence == 0 {
			return nil, fmt.Errorf("%w: wire exchange sequence is zero", ErrInvalidJournal)
		}
		if _, exists := seen[exchange.Sequence]; exists {
			return nil, fmt.Errorf("%w: wire exchange sequence is duplicated", ErrInvalidJournal)
		}
		seen[exchange.Sequence] = struct{}{}
		requestDigest := sha256.Sum256(exchange.RequestBody)
		responseDigest := sha256.Sum256(exchange.ResponseBody)
		attachments = append(attachments, WireAttachmentIdentity{
			ExchangeSequence: exchange.Sequence,
			RequestBytes:     int64(len(exchange.RequestBody)),
			RequestSHA256:    hex.EncodeToString(requestDigest[:]),
			ResponseBytes:    int64(len(exchange.ResponseBody)),
			ResponseSHA256:   hex.EncodeToString(responseDigest[:]),
		})
	}
	return attachments, nil
}

func validateAttachment(attachment WireAttachmentIdentity) error {
	if attachment.ExchangeSequence == 0 || attachment.RequestBytes < 0 || attachment.ResponseBytes < 0 || !validSHA256(attachment.RequestSHA256) || !validSHA256(attachment.ResponseSHA256) {
		return fmt.Errorf("%w: wire attachment identity is invalid", ErrInvalidJournal)
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}

func configPointer(value Config) *Config {
	copy := cloneConfig(value)
	return &copy
}

func catalogIdentityPointer(value CatalogIdentity) *CatalogIdentity {
	return &value
}

func bytesTrimNewline(line []byte) io.Reader {
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	return bytes.NewReader(line)
}
