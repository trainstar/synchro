// Package soak provides a bounded seeded workload driver for the invariant
// engine. The package does not own a database or an HTTP client.
package soak

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/trainstar/synchro/conformance/faults"
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	// DefaultOperationCount is the bounded plan size used when no count is set.
	DefaultOperationCount = 32
	// MaximumOperationCount keeps generated plans and fault identifiers bounded.
	MaximumOperationCount = 999
	// MaximumIdentityCount bounds each configured identity set.
	MaximumIdentityCount = 8
	// MaximumIdentityBytes bounds one configured identity.
	MaximumIdentityBytes = 64
	// DefaultFaultRate is the percentage of eligible operations with a cataloged fault.
	DefaultFaultRate = 25
	// MaximumFaultRate is the largest accepted fault percentage.
	MaximumFaultRate = 100
	// MinimumCoverageOperations is the count that covers every operation family.
	MinimumCoverageOperations = 7
	// MaximumSoakDuration bounds duration-to-plan conversion.
	MaximumSoakDuration = 72 * time.Hour
)

var (
	// ErrCatalogRequired reports that no authoritative fault catalog was supplied.
	ErrCatalogRequired = errors.New("soak fault catalog is required")
	// ErrInvalidConfig reports a malformed generator configuration.
	ErrInvalidConfig = errors.New("soak generator configuration is invalid")
	// ErrInvalidPlan reports a malformed generated plan.
	ErrInvalidPlan = errors.New("soak plan is invalid")
	// ErrReplayMismatch reports a journal that does not reproduce its operation plan.
	ErrReplayMismatch = errors.New("soak replay plan does not match journal")
)

// OperationKind identifies one bounded workload family.
type OperationKind string

const (
	OperationConnect          OperationKind = "connect"
	OperationPush             OperationKind = "push"
	OperationPull             OperationKind = "pull"
	OperationRebuild          OperationKind = "rebuild"
	OperationSchemaTransition OperationKind = "schema-transition"
	OperationProcessDeath     OperationKind = "process-death"
	OperationWireFault        OperationKind = "wire-fault"
)

var operationKinds = []OperationKind{
	OperationConnect,
	OperationPush,
	OperationPull,
	OperationRebuild,
	OperationSchemaTransition,
	OperationProcessDeath,
	OperationWireFault,
}

// Config controls deterministic plan generation.
//
// All fields are part of the replay configuration and are written to journals.
type Config struct {
	OperationCount int      `json:"operation_count"`
	Users          []string `json:"users"`
	Clients        []string `json:"clients"`
	Scopes         []string `json:"scopes"`
	FaultRate      int      `json:"fault_rate"`
}

// Operation is one generated workload instruction.
//
// The harness maps this neutral instruction to its black-box surfaces.
type Operation struct {
	Sequence                    uint64               `json:"sequence"`
	Kind                        OperationKind        `json:"kind"`
	UserID                      string               `json:"user_id"`
	ClientID                    string               `json:"client_id"`
	ScopeID                     string               `json:"scope_id"`
	SchemaVersion               uint64               `json:"schema_version"`
	FaultPlan                   *scenarios.FaultPlan `json:"fault_plan,omitempty"`
	Input                       json.RawMessage      `json:"input"`
	RequiredObservationSurfaces []ObservationSurface `json:"required_observation_surfaces"`
}

// Plan is the complete deterministic workload for one seed and configuration.
type Plan struct {
	Seed            uint64          `json:"seed"`
	Config          Config          `json:"config"`
	CatalogIdentity CatalogIdentity `json:"catalog_identity"`
	Operations      []Operation     `json:"operations"`
}

// Generator produces one deterministic plan from a seed and catalog.
type Generator struct {
	seed    uint64
	config  Config
	catalog *faults.Catalog
}

// NewGenerator validates inputs and creates a deterministic generator.
func NewGenerator(seed uint64, config Config, catalog *faults.Catalog) (*Generator, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if catalog == nil {
		return nil, ErrCatalogRequired
	}
	if len(catalog.Controls) == 0 {
		return nil, fmt.Errorf("%w: catalog has no controls", ErrCatalogRequired)
	}
	return &Generator{seed: seed, config: normalized, catalog: catalog}, nil
}

// Generate returns the deterministic plan for seed, configuration, and catalog.
func Generate(seed uint64, config Config, catalog *faults.Catalog) (Plan, error) {
	generator, err := NewGenerator(seed, config, catalog)
	if err != nil {
		return Plan{}, err
	}
	return generator.Generate()
}

// Generate returns the generator's complete deterministic operation plan.
func (g *Generator) Generate() (Plan, error) {
	if g == nil || g.catalog == nil {
		return Plan{}, ErrCatalogRequired
	}
	if err := g.config.validate(); err != nil {
		return Plan{}, err
	}
	controls := sortedControls(g.catalog.Controls)
	if len(controls) == 0 {
		return Plan{}, fmt.Errorf("%w: catalog has no controls", ErrCatalogRequired)
	}
	wireControls := controlsWithMechanism(controls, "wire-fault")
	processControls := controlsWithMechanism(controls, "process-fault")
	if len(wireControls) == 0 || len(processControls) == 0 {
		return Plan{}, fmt.Errorf("%w: wire-fault and process-fault controls are required", ErrCatalogRequired)
	}
	catalogIdentity, err := catalogIdentityOf(g.catalog)
	if err != nil {
		return Plan{}, err
	}

	random := newPlannerRandom(g.seed)
	operations := make([]Operation, 0, g.config.OperationCount)
	for index := 0; index < g.config.OperationCount; index++ {
		sequence := uint64(index + 1)
		kind := operationKinds[index%len(operationKinds)]
		if index >= len(operationKinds) {
			kind = operationKinds[random.intn(len(operationKinds))]
		}
		operation := Operation{
			Sequence:                    sequence,
			Kind:                        kind,
			UserID:                      g.config.Users[random.intn(len(g.config.Users))],
			ClientID:                    g.config.Clients[random.intn(len(g.config.Clients))],
			ScopeID:                     g.config.Scopes[random.intn(len(g.config.Scopes))],
			SchemaVersion:               1 + uint64(random.intn(3)),
			RequiredObservationSurfaces: requiredObservationSurfaces(kind),
		}

		attachFault := kind == OperationWireFault || kind == OperationProcessDeath
		if supportsWireFault(kind) && random.percent() < g.config.FaultRate {
			attachFault = true
		}
		if attachFault {
			selected := wireControls
			switch kind {
			case OperationWireFault:
				selected = wireControls
			case OperationProcessDeath:
				selected = processControls
			}
			control := selected[random.intn(len(selected))]
			faultPlan, err := makeFaultPlan(control, sequence)
			if err != nil {
				return Plan{}, err
			}
			if err := faults.ValidatePlan(faultPlan, g.catalog); err != nil {
				return Plan{}, fmt.Errorf("validate generated fault plan %d: %w", sequence, err)
			}
			operation.FaultPlan = &faultPlan
		}
		operation.Input, err = operationInput(operation)
		if err != nil {
			return Plan{}, err
		}
		operations = append(operations, operation)
	}

	plan := Plan{Seed: g.seed, Config: cloneConfig(g.config), CatalogIdentity: catalogIdentity, Operations: operations}
	if err := plan.validateShape(g.catalog); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

// ConfigForDuration converts a bounded soak duration to a reproducible plan size.
func ConfigForDuration(duration time.Duration) (Config, error) {
	if duration <= 0 || duration > MaximumSoakDuration {
		return Config{}, fmt.Errorf("%w: duration must be greater than zero and at most %s", ErrInvalidConfig, MaximumSoakDuration)
	}
	// One live operation drives the real adapter, the extension, and often a
	// WAL wait or a process restart, so it costs seconds, not milliseconds.
	const secondsPerOperation = 5
	unit := secondsPerOperation * time.Second
	count := int64(duration / unit)
	if duration%unit != 0 {
		count++
	}
	if count < MinimumCoverageOperations {
		count = MinimumCoverageOperations
	}
	if count > MaximumOperationCount {
		count = MaximumOperationCount
	}
	return Config{OperationCount: int(count), FaultRate: DefaultFaultRate}, nil
}

// GenerateForDuration returns a plan with a count derived from duration.
func GenerateForDuration(seed uint64, duration time.Duration, catalog *faults.Catalog) (Plan, error) {
	config, err := ConfigForDuration(duration)
	if err != nil {
		return Plan{}, err
	}
	return Generate(seed, config, catalog)
}

func (c Config) validate() error {
	if c.OperationCount < MinimumCoverageOperations || c.OperationCount > MaximumOperationCount {
		return fmt.Errorf("%w: operation count must be between %d and %d", ErrInvalidConfig, MinimumCoverageOperations, MaximumOperationCount)
	}
	if c.FaultRate < 0 || c.FaultRate > MaximumFaultRate {
		return fmt.Errorf("%w: fault rate must be between 0 and %d", ErrInvalidConfig, MaximumFaultRate)
	}
	identitySets := []struct {
		name   string
		values []string
	}{
		{name: "users", values: c.Users},
		{name: "clients", values: c.Clients},
		{name: "scopes", values: c.Scopes},
	}
	for _, identitySet := range identitySets {
		name, values := identitySet.name, identitySet.values
		if len(values) < 1 || len(values) > MaximumIdentityCount {
			return fmt.Errorf("%w: %s count must be between 1 and %d", ErrInvalidConfig, name, MaximumIdentityCount)
		}
		seen := make(map[string]struct{}, len(values))
		for _, value := range values {
			if value == "" || len(value) > MaximumIdentityBytes || strings.IndexFunc(value, func(r rune) bool { return r <= ' ' || r == 0x7f }) >= 0 {
				return fmt.Errorf("%w: %s contains an invalid identity", ErrInvalidConfig, name)
			}
			if _, exists := seen[value]; exists {
				return fmt.Errorf("%w: %s contains a duplicate identity", ErrInvalidConfig, name)
			}
			seen[value] = struct{}{}
		}
	}
	return nil
}

func normalizeConfig(config Config) (Config, error) {
	useDefaults := config.OperationCount == 0 && config.FaultRate == 0 && config.Users == nil && config.Clients == nil && config.Scopes == nil
	if config.OperationCount == 0 {
		config.OperationCount = DefaultOperationCount
	}
	if len(config.Users) == 0 {
		config.Users = []string{"user-a"}
	}
	if len(config.Clients) == 0 {
		config.Clients = []string{"client-a"}
	}
	if len(config.Scopes) == 0 {
		config.Scopes = []string{"scope-a", "scope-b"}
	}
	if useDefaults {
		config.FaultRate = DefaultFaultRate
	}
	config = cloneConfig(config)
	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func cloneConfig(config Config) Config {
	config.Users = append([]string(nil), config.Users...)
	config.Clients = append([]string(nil), config.Clients...)
	config.Scopes = append([]string(nil), config.Scopes...)
	return config
}

func (p Plan) validateShape(catalog *faults.Catalog) error {
	if err := p.Config.validate(); err != nil {
		return err
	}
	if len(p.Operations) == 0 || len(p.Operations) != p.Config.OperationCount {
		return fmt.Errorf("%w: operation count does not match configuration", ErrInvalidPlan)
	}
	catalogIdentity, err := catalogIdentityOf(catalog)
	if err != nil {
		return err
	}
	if p.CatalogIdentity != catalogIdentity {
		return fmt.Errorf("%w: catalog identity does not match plan", ErrInvalidPlan)
	}
	for index, operation := range p.Operations {
		if operation.Sequence != uint64(index+1) || !validOperationKind(operation.Kind) || operation.UserID == "" || operation.ClientID == "" || operation.ScopeID == "" || operation.SchemaVersion == 0 {
			return fmt.Errorf("%w: operation %d identity", ErrInvalidPlan, index+1)
		}
		if !equalObservationSurfaces(operation.RequiredObservationSurfaces, requiredObservationSurfaces(operation.Kind)) {
			return fmt.Errorf("%w: operation %d observation surfaces", ErrInvalidPlan, index+1)
		}
		if err := validateOperationInput(operation); err != nil {
			return fmt.Errorf("%w: operation %d input: %w", ErrInvalidPlan, index+1, err)
		}
		if operation.FaultPlan != nil {
			if err := faults.ValidatePlan(*operation.FaultPlan, catalog); err != nil {
				return fmt.Errorf("%w: operation %d fault: %w", ErrInvalidPlan, index+1, err)
			}
			if !faultSupportsOperation(operation.Kind, *operation.FaultPlan) {
				return fmt.Errorf("%w: operation %d fault has no supported trigger", ErrInvalidPlan, index+1)
			}
		}
		if index < len(operationKinds) && operation.Kind != operationKinds[index] {
			return fmt.Errorf("%w: operation %d does not satisfy coverage prefix", ErrInvalidPlan, index+1)
		}
	}
	return nil
}

func sortedControls(controls []faults.Control) []faults.Control {
	result := append([]faults.Control(nil), controls...)
	sort.Slice(result, func(left, right int) bool {
		return result[left].ID < result[right].ID
	})
	return result
}

func controlsWithMechanism(controls []faults.Control, mechanism string) []faults.Control {
	result := make([]faults.Control, 0, len(controls))
	for _, control := range controls {
		if control.Injection.Mechanism == mechanism {
			result = append(result, control)
		}
	}
	return result
}

func supportsWireFault(kind OperationKind) bool {
	switch kind {
	case OperationConnect, OperationPush, OperationPull, OperationWireFault:
		return true
	default:
		return false
	}
}

func faultSupportsOperation(kind OperationKind, plan scenarios.FaultPlan) bool {
	switch plan.Injection.Mechanism {
	case "wire-fault":
		return supportsWireFault(kind)
	case "process-fault":
		return kind == OperationProcessDeath
	default:
		return false
	}
}

func operationInput(operation Operation) (json.RawMessage, error) {
	type inputRecord struct {
		Kind             OperationKind        `json:"kind"`
		UserID           string               `json:"user_id"`
		ClientID         string               `json:"client_id"`
		ScopeID          string               `json:"scope_id"`
		SchemaVersion    uint64               `json:"schema_version"`
		ProcessTarget    string               `json:"process_target,omitempty"`
		SchemaTransition uint64               `json:"schema_transition,omitempty"`
		FaultPlan        *scenarios.FaultPlan `json:"fault_plan,omitempty"`
	}
	record := inputRecord{
		Kind:             operation.Kind,
		UserID:           operation.UserID,
		ClientID:         operation.ClientID,
		ScopeID:          operation.ScopeID,
		SchemaVersion:    operation.SchemaVersion,
		FaultPlan:        operation.FaultPlan,
		ProcessTarget:    operation.ClientID,
		SchemaTransition: operation.SchemaVersion,
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("encode operation input: %w", err)
	}
	return encoded, nil
}

func validateOperationInput(operation Operation) error {
	if len(operation.Input) == 0 || !json.Valid(operation.Input) {
		return errors.New("operation input is not valid JSON")
	}
	var input map[string]json.RawMessage
	if err := json.Unmarshal(operation.Input, &input); err != nil || input == nil {
		return errors.New("operation input is not an object")
	}
	return nil
}

func makeFaultPlan(control faults.Control, sequence uint64) (scenarios.FaultPlan, error) {
	if len(control.RequirementIDs) != 1 {
		return scenarios.FaultPlan{}, fmt.Errorf("%w: selected control %q has no single requirement", ErrInvalidPlan, control.ID)
	}
	return scenarios.FaultPlan{
		ID:                   contract.FaultPlanID(fmt.Sprintf("FPL-SOAK-%03d", sequence)),
		RequirementID:        contract.RequirementID(control.RequirementIDs[0]),
		FaultID:              contract.FaultID(control.FaultID),
		ControlID:            contract.ControlID(control.ID),
		BarrierID:            scenarios.BarrierID(fmt.Sprintf("BAR-SOAK-%03d", sequence)),
		ExpectedAssertionIDs: []contract.AssertionID{contract.AssertionID(fmt.Sprintf("ASSERT-SOAK-%03d", sequence))},
		Injection: scenarios.InjectionRecipe{
			Mechanism: control.Injection.Mechanism,
			Target:    control.Injection.Target,
			Operator:  control.Injection.Operator,
			Parameters: scenarios.InjectionParameters{
				Scenario:     control.Injection.Parameters.Scenario,
				Defect:       control.Injection.Parameters.Defect,
				Precondition: control.Injection.Parameters.Precondition,
			},
		},
	}, nil
}

func validOperationKind(kind OperationKind) bool {
	for _, candidate := range operationKinds {
		if kind == candidate {
			return true
		}
	}
	return false
}

// plannerRandom is a small fixed algorithm whose output is independent of the
// Go standard library random implementation.
type plannerRandom struct {
	state uint64
}

func newPlannerRandom(seed uint64) *plannerRandom {
	return &plannerRandom{state: seed}
}

func (r *plannerRandom) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *plannerRandom) intn(limit int) int {
	return int(r.next() % uint64(limit))
}

func (r *plannerRandom) percent() int {
	return r.intn(100)
}
