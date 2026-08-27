package blackbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const maxExactNativeIdentityInteger = int64(1<<53 - 1)

var nativeIdentityKinds = map[string]struct{}{
	"batch-id":          {},
	"checksum":          {},
	"client-generation": {},
	"cursor":            {},
	"mutation-id":       {},
	"primary-key":       {},
	"rebuild-id":        {},
	"row-version":       {},
	"schema":            {},
	"scope":             {},
	"scope-set-version": {},
	"server-version":    {},
	"table":             {},
}

// ErrNativeIdentityEvidence reports incomplete or inconsistent runtime identity evidence.
var ErrNativeIdentityEvidence = errors.New("native identity evidence is invalid")

// NativeIdentityValue is one runtime value for a declared alias.
type NativeIdentityValue struct {
	Kind                  string          `json:"kind"`
	Alias                 string          `json:"alias"`
	RuntimeValue          json.RawMessage `json:"runtime_value"`
	ApplicationIdentifier string          `json:"application_identifier,omitempty"`
}

// NativeIdentityObservation binds one declared alias use to one observed runtime value.
// Direct Swift, Kotlin, and React Native scenario drivers produce these observations.
type NativeIdentityObservation struct {
	Kind          string                   `json:"kind"`
	Alias         string                   `json:"alias"`
	StepID        *scenarios.StepID        `json:"step_id,omitempty"`
	ExpectationID *scenarios.ExpectationID `json:"expectation_id,omitempty"`
	RuntimeValue  json.RawMessage          `json:"runtime_value"`
}

// NativeIdentityResolution records one complete authored-to-runtime identity binding.
type NativeIdentityResolution struct {
	Kind          string          `json:"kind"`
	Alias         string          `json:"alias"`
	AuthoredValue json.RawMessage `json:"authored_value"`
	RuntimeValue  json.RawMessage `json:"runtime_value"`
}

// ResolveNativeIdentityAliases validates complete runtime evidence for declared aliases.
func ResolveNativeIdentityAliases(aliases []scenarios.NativeIdentityAlias, observations []NativeIdentityObservation) ([]NativeIdentityResolution, error) {
	if len(aliases) == 0 {
		return nil, fmt.Errorf("%w: no aliases are declared", ErrNativeIdentityEvidence)
	}

	type declaration struct {
		kind         string
		authored     json.RawMessage
		steps        map[scenarios.StepID]struct{}
		expectations map[scenarios.ExpectationID]struct{}
	}
	declarations := make(map[string]declaration, len(aliases))
	authoredValues := make(map[string]string, len(aliases))
	for _, alias := range aliases {
		if _, known := nativeIdentityKinds[alias.Kind]; !known || alias.Alias == "" {
			return nil, fmt.Errorf("%w: one alias declaration is incomplete", ErrNativeIdentityEvidence)
		}
		if _, duplicate := declarations[alias.Alias]; duplicate {
			return nil, fmt.Errorf("%w: alias %q is declared more than once", ErrNativeIdentityEvidence, alias.Alias)
		}
		canonical, err := canonicalNativeIdentityJSON(alias.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: alias %q has an invalid authored value: %v", ErrNativeIdentityEvidence, alias.Alias, err)
		}
		if err := validateNativeIdentityShape(alias.Kind, canonical); err != nil {
			return nil, fmt.Errorf("%w: alias %q has an invalid authored value: %v", ErrNativeIdentityEvidence, alias.Alias, err)
		}
		valueKey := alias.Kind + "\x00" + string(canonical)
		if previous, duplicate := authoredValues[valueKey]; duplicate {
			return nil, fmt.Errorf("%w: aliases %q and %q share one authored %s value", ErrNativeIdentityEvidence, previous, alias.Alias, alias.Kind)
		}
		authoredValues[valueKey] = alias.Alias
		steps := make(map[scenarios.StepID]struct{}, len(alias.StepIDs))
		for _, stepID := range alias.StepIDs {
			steps[stepID] = struct{}{}
		}
		expectations := make(map[scenarios.ExpectationID]struct{}, len(alias.ExpectationIDs))
		for _, expectationID := range alias.ExpectationIDs {
			expectations[expectationID] = struct{}{}
		}
		if len(steps) == 0 && len(expectations) == 0 {
			return nil, fmt.Errorf("%w: alias %q has no declared owners", ErrNativeIdentityEvidence, alias.Alias)
		}
		declarations[alias.Alias] = declaration{kind: alias.Kind, authored: canonical, steps: steps, expectations: expectations}
	}

	resolvedValues := make(map[string]json.RawMessage, len(aliases))
	observedOwners := make(map[string]struct{})
	for _, observation := range observations {
		declared, found := declarations[observation.Alias]
		if !found {
			return nil, fmt.Errorf("%w: observation alias %q is unresolved", ErrNativeIdentityEvidence, observation.Alias)
		}
		if observation.Kind != declared.kind {
			return nil, fmt.Errorf("%w: alias %q observation has kind %q instead of %q", ErrNativeIdentityEvidence, observation.Alias, observation.Kind, declared.kind)
		}
		if (observation.StepID == nil) == (observation.ExpectationID == nil) {
			return nil, fmt.Errorf("%w: alias %q observation must identify one owner", ErrNativeIdentityEvidence, observation.Alias)
		}
		ownerKey := observation.Alias + "\x00"
		if observation.StepID != nil {
			if _, found := declared.steps[*observation.StepID]; !found {
				return nil, fmt.Errorf("%w: alias %q observation references undeclared step %s", ErrNativeIdentityEvidence, observation.Alias, *observation.StepID)
			}
			ownerKey += "step\x00" + string(*observation.StepID)
		} else {
			if _, found := declared.expectations[*observation.ExpectationID]; !found {
				return nil, fmt.Errorf("%w: alias %q observation references undeclared expectation %s", ErrNativeIdentityEvidence, observation.Alias, *observation.ExpectationID)
			}
			ownerKey += "expectation\x00" + string(*observation.ExpectationID)
		}
		canonical, err := canonicalNativeIdentityJSON(observation.RuntimeValue)
		if err != nil {
			return nil, fmt.Errorf("%w: alias %q has an invalid runtime value: %v", ErrNativeIdentityEvidence, observation.Alias, err)
		}
		if err := validateNativeIdentityShape(observation.Kind, canonical); err != nil {
			return nil, fmt.Errorf("%w: alias %q has an invalid runtime value: %v", ErrNativeIdentityEvidence, observation.Alias, err)
		}
		if previous, found := resolvedValues[observation.Alias]; found && !bytes.Equal(previous, canonical) {
			return nil, fmt.Errorf("%w: equal alias %q resolved inconsistently", ErrNativeIdentityEvidence, observation.Alias)
		}
		resolvedValues[observation.Alias] = canonical
		observedOwners[ownerKey] = struct{}{}
	}

	runtimeValues := make(map[string]string, len(aliases))
	resolutions := make([]NativeIdentityResolution, 0, len(aliases))
	for _, alias := range aliases {
		runtime, found := resolvedValues[alias.Alias]
		if !found {
			return nil, fmt.Errorf("%w: alias %q has no runtime observation", ErrNativeIdentityEvidence, alias.Alias)
		}
		for _, stepID := range alias.StepIDs {
			if _, found := observedOwners[alias.Alias+"\x00step\x00"+string(stepID)]; !found {
				return nil, fmt.Errorf("%w: alias %q has no runtime observation for step %s", ErrNativeIdentityEvidence, alias.Alias, stepID)
			}
		}
		for _, expectationID := range alias.ExpectationIDs {
			if _, found := observedOwners[alias.Alias+"\x00expectation\x00"+string(expectationID)]; !found {
				return nil, fmt.Errorf("%w: alias %q has no runtime observation for expectation %s", ErrNativeIdentityEvidence, alias.Alias, expectationID)
			}
		}
		key := alias.Kind + "\x00" + string(runtime)
		if previous, duplicate := runtimeValues[key]; duplicate && previous != alias.Alias {
			return nil, fmt.Errorf("%w: distinct aliases %q and %q collapse to one runtime %s value", ErrNativeIdentityEvidence, previous, alias.Alias, alias.Kind)
		}
		runtimeValues[key] = alias.Alias
		declared := declarations[alias.Alias]
		resolutions = append(resolutions, NativeIdentityResolution{
			Kind:          alias.Kind,
			Alias:         alias.Alias,
			AuthoredValue: append(json.RawMessage(nil), declared.authored...),
			RuntimeValue:  append(json.RawMessage(nil), runtime...),
		})
	}
	return resolutions, nil
}

// IdentityValues returns the semantic handle mappings owned by the server controller.
func (c *NativeController) IdentityValues(aliases []scenarios.NativeIdentityAlias) ([]NativeIdentityValue, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: native controller is unavailable", ErrNativeIdentityEvidence)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("%w: native controller is closed", ErrNativeIdentityEvidence)
	}
	if c.installation == nil {
		return nil, fmt.Errorf("%w: native controller contract is not installed", ErrNativeIdentityEvidence)
	}

	values := make([]NativeIdentityValue, 0, len(aliases))
	for _, alias := range aliases {
		var runtime any
		var applicationIdentifier string
		switch alias.Kind {
		case "schema":
			var authored nativeSchemaReference
			if err := json.Unmarshal(alias.Value, &authored); err != nil || !validNativeSchemaReference(authored, false) {
				return nil, fmt.Errorf("%w: schema alias %q has an invalid authored value", ErrNativeIdentityEvidence, alias.Alias)
			}
			value, found := c.installation.runtimeSchemas[nativeSchemaKey(authored)]
			if !found {
				return nil, fmt.Errorf("%w: schema alias %q has no runtime binding", ErrNativeIdentityEvidence, alias.Alias)
			}
			runtime = value
		case "scope":
			authored, err := nativeIdentityString(alias)
			if err != nil {
				return nil, err
			}
			value, found := c.installation.scopes[authored]
			if !found {
				return nil, fmt.Errorf("%w: scope alias %q has no runtime binding", ErrNativeIdentityEvidence, alias.Alias)
			}
			runtime = value
		case "table":
			authored, err := nativeIdentityString(alias)
			if err != nil {
				return nil, err
			}
			value, found := c.installation.tables[authored]
			if !found || value.RuntimeID == "" {
				return nil, fmt.Errorf("%w: table alias %q has no runtime binding", ErrNativeIdentityEvidence, alias.Alias)
			}
			runtime = value.RuntimeID
			applicationIdentifier = value.RuntimeName
		case "primary-key":
			authored, err := nativeIdentityString(alias)
			if err != nil {
				return nil, err
			}
			matches := 0
			for _, table := range c.installation.tables {
				if table.AuthoredPrimary == authored {
					matches++
					runtime = table.RuntimePrimary
					applicationIdentifier = table.FieldNames[authored]
				}
			}
			if matches != 1 || runtime == "" || applicationIdentifier == "" {
				return nil, fmt.Errorf("%w: primary-key alias %q has no unique runtime binding", ErrNativeIdentityEvidence, alias.Alias)
			}
		default:
			continue
		}
		encoded, err := json.Marshal(runtime)
		if err != nil {
			return nil, fmt.Errorf("%w: encode alias %q runtime value: %v", ErrNativeIdentityEvidence, alias.Alias, err)
		}
		values = append(values, NativeIdentityValue{Kind: alias.Kind, Alias: alias.Alias, RuntimeValue: encoded, ApplicationIdentifier: applicationIdentifier})
	}
	return values, nil
}

func nativeIdentityString(alias scenarios.NativeIdentityAlias) (string, error) {
	canonical, err := canonicalNativeIdentityJSON(alias.Value)
	if err != nil {
		return "", fmt.Errorf("%w: alias %q has an invalid authored value: %v", ErrNativeIdentityEvidence, alias.Alias, err)
	}
	var value string
	if err := json.Unmarshal(canonical, &value); err != nil || value == "" {
		return "", fmt.Errorf("%w: alias %q authored value is not a nonempty string", ErrNativeIdentityEvidence, alias.Alias)
	}
	return value, nil
}

func canonicalNativeIdentityJSON(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, errors.New("value is required")
	}
	wrapped := make([]byte, 0, len(raw)+10)
	wrapped = append(wrapped, `{"value":`...)
	wrapped = append(wrapped, raw...)
	wrapped = append(wrapped, '}')
	if err := jsonstrict.ValidateValue(wrapped); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if value == nil {
		return nil, errors.New("value must not be null")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("value contains trailing data")
	}
	if err := validateNativeIdentityJSONNumbers(value); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func validateNativeIdentityJSONNumbers(value any) error {
	switch value := value.(type) {
	case json.Number:
		integer, err := value.Int64()
		if err != nil {
			return errors.New("number must be an integer")
		}
		if integer < -maxExactNativeIdentityInteger || integer > maxExactNativeIdentityInteger {
			return errors.New("integer exceeds exact JSON range")
		}
	case []any:
		for _, item := range value {
			if err := validateNativeIdentityJSONNumbers(item); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, item := range value {
			if err := validateNativeIdentityJSONNumbers(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateNativeIdentityShape(kind string, raw json.RawMessage) error {
	switch kind {
	case "schema":
		var value nativeSchemaReference
		if err := json.Unmarshal(raw, &value); err != nil || !validNativeSchemaReference(value, false) {
			return errors.New("schema identity must contain one valid version and hash pair")
		}
		var members map[string]json.RawMessage
		if err := json.Unmarshal(raw, &members); err != nil || len(members) != 2 || members["version"] == nil || members["hash"] == nil {
			return errors.New("schema identity must contain only version and hash")
		}
	case "client-generation", "scope-set-version":
		var value json.Number
		if err := json.Unmarshal(raw, &value); err != nil {
			return errors.New("generation identity must be a nonnegative integer")
		}
		integer, err := value.Int64()
		if err != nil || integer < 0 {
			return errors.New("generation identity must be a nonnegative integer")
		}
	default:
		var value string
		if err := json.Unmarshal(raw, &value); err != nil || value == "" {
			return errors.New("identity must be a nonempty string")
		}
	}
	return nil
}
