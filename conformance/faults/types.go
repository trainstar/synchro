// Package faults loads typed fault recipes and provides cleanup-safe controls.
package faults

import "errors"

const (
	// CatalogSchemaURI is the only accepted fault catalog schema URI.
	CatalogSchemaURI = "https://synchro.dev/conformance/schemas/fault-catalog-v1.schema.json"
	// CatalogRelease is the release bound by the fault catalog.
	CatalogRelease = "0.3.0"
)

// Catalog is the strict typed representation of conformance/faults/catalog.json.
type Catalog struct {
	SchemaURI     string    `json:"$schema"`
	SchemaVersion int       `json:"schema_version"`
	Release       string    `json:"release"`
	Faults        []Fault   `json:"faults"`
	Controls      []Control `json:"controls"`
}

// Fault identifies one cataloged fault.
type Fault struct {
	ID          string `json:"id"`
	Description string `json:"description"`
}

// Control is one requirement-owned negative control.
type Control struct {
	ID                  string    `json:"id"`
	FaultID             string    `json:"fault_id"`
	SubjectType         string    `json:"subject_type"`
	RequirementIDs      []string  `json:"requirement_ids"`
	NormativeReferences []string  `json:"normative_references"`
	Injection           Injection `json:"injection"`
	ExpectedDetection   string    `json:"expected_detection"`
}

// Injection is one exact cataloged injection recipe.
type Injection struct {
	Mechanism  string     `json:"mechanism"`
	Target     string     `json:"target"`
	Operator   string     `json:"operator"`
	Parameters Parameters `json:"parameters"`
}

// Parameters are the exact descriptive parameters of an injection recipe.
type Parameters struct {
	Scenario     string `json:"scenario"`
	Defect       string `json:"defect"`
	Precondition string `json:"precondition,omitempty"`
}

// Handle is a cleanup-safe fault resource.
type Handle interface {
	Close() error
	Done() <-chan struct{}
}

// WireMode selects a deterministic transport fault.
type WireMode string

const (
	// WireResponseLoss discards a completed upstream response.
	WireResponseLoss WireMode = "response_loss"
	// WireTimeout returns a timeout after upstream completion.
	WireTimeout WireMode = "timeout"
	// WireTruncate returns a response body that terminates unexpectedly.
	WireTruncate WireMode = "truncate"
	// WireDuplicate sends the same replayable request exactly twice.
	WireDuplicate WireMode = "duplicate"
	// WireReplay sends the same replayable request a configured number of times.
	WireReplay WireMode = "replay"
	// WireTemporaryUnavailable returns the canonical retryable 503 response without upstream dispatch.
	WireTemporaryUnavailable WireMode = "temporary_unavailable"
)

// WireOptions configures one typed wire fault.
type WireOptions struct {
	Mode          WireMode
	TruncateAfter int64
	ReplayCount   int
}

var (
	// ErrNilContext reports a missing cancellation boundary.
	ErrNilContext = errors.New("fault context is nil")
	// ErrNilCatalog reports a missing authoritative catalog.
	ErrNilCatalog = errors.New("fault catalog is nil")
	// ErrInvalidCatalog reports malformed catalog content.
	ErrInvalidCatalog = errors.New("fault catalog is invalid")
	// ErrInvalidPlan reports a malformed fault plan.
	ErrInvalidPlan = errors.New("fault plan is invalid")
	// ErrFaultClosed reports an operation attempted after cleanup.
	ErrFaultClosed = errors.New("fault handle is closed")
	// ErrControllerClosed reports an owner that already cleaned its resources.
	ErrControllerClosed = errors.New("fault controller is closed")
	// ErrResponseLost reports deliberate loss after upstream completion.
	ErrResponseLost = errors.New("response lost after upstream completion")
	// ErrInjectedTimeout reports a deterministic injected timeout.
	ErrInjectedTimeout = errors.New("timeout after upstream completion")
	// ErrRequestNotReplayable reports a body that cannot be replayed safely.
	ErrRequestNotReplayable = errors.New("request body is not replayable")
	// ErrInvalidWireOptions reports an incomplete or contradictory wire fault.
	ErrInvalidWireOptions = errors.New("wire fault options are invalid")
	// ErrNilRoundTripper reports a missing upstream transport.
	ErrNilRoundTripper = errors.New("wire fault upstream is nil")
	// ErrNilCommand reports a missing process command.
	ErrNilCommand = errors.New("process command is nil")
	// ErrNilBarrierController reports a missing named-barrier controller.
	ErrNilBarrierController = errors.New("barrier controller is nil")
	// ErrInvalidArtifact reports an unsafe artifact target.
	ErrInvalidArtifact = errors.New("artifact target is invalid")
	// ErrArtifactTooLarge reports an artifact that exceeds the bounded limit.
	ErrArtifactTooLarge = errors.New("artifact exceeds the fault limit")
)

var validSubjectTypes = map[string]struct{}{
	"mutant":               {},
	"synthetic-fault":      {},
	"known-defect":         {},
	"source-mutant":        {},
	"wire-fault":           {},
	"state-fault":          {},
	"process-fault":        {},
	"infrastructure-fault": {},
	"artifact-tamper":      {},
}

var validMechanisms = map[string]struct{}{
	"source-mutant":        {},
	"wire-fault":           {},
	"state-fault":          {},
	"process-fault":        {},
	"infrastructure-fault": {},
	"artifact-tamper":      {},
}

var validOperators = map[string]struct{}{
	"replace":   {},
	"inject":    {},
	"omit":      {},
	"duplicate": {},
	"reorder":   {},
	"replay":    {},
	"misbind":   {},
	"bypass":    {},
	"truncate":  {},
	"crash":     {},
	"corrupt":   {},
	"reroute":   {},
	"expose":    {},
	"delay":     {},
}
