package faults

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	catalogPath     = "conformance/faults/catalog.json"
	maximumCatalog  = 16 << 20
	catalogVersion  = 1
	maximumIDLength = 256
)

var (
	requirementIDPattern = regexp.MustCompile(`^SYNC-[A-Z][A-Z0-9]*(?:-[A-Z][A-Z0-9]*)*-[0-9]{3}$`)
	faultIDPattern       = regexp.MustCompile(`^FAULT-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)
	controlIDPattern     = regexp.MustCompile(`^CTRL-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)
	faultPlanIDPattern   = regexp.MustCompile(`^FPL-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)
	barrierIDPattern     = regexp.MustCompile(`^BAR-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)
	assertionIDPattern   = regexp.MustCompile(`^ASSERT-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)
)

// LoadCatalog reads and strictly validates the repository fault catalog.
//
// The loader rejects symlinked catalog paths, duplicate JSON members, trailing
// data, unknown members, and values outside the pinned catalog schema.
func LoadCatalog(ctx context.Context, repoRoot string) (*Catalog, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return nil, err
	}
	defer root.Close()

	data, err := readCatalog(ctx, root)
	if err != nil {
		return nil, err
	}
	catalog, err := decodeCatalog(data)
	if err != nil {
		return nil, err
	}
	if err := validateCatalog(catalog); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	return catalog, nil
}

// ValidatePlan verifies the exact catalog recipe selected by a scenario plan.
//
// scenarios.FaultPlan has no subject-type field. The selected catalog control
// is therefore the authoritative subject-type binding. It also has no barrier
// definitions. This function validates the plan's canonical barrier ID. The
// scenario validator validates that the ID belongs to barrier_plan.
func ValidatePlan(plan scenarios.FaultPlan, catalog *Catalog) error {
	if catalog == nil {
		return ErrNilCatalog
	}
	if err := validateCatalog(catalog); err != nil {
		return err
	}

	var failures []error
	if !faultPlanIDPattern.MatchString(string(plan.ID)) {
		failures = append(failures, fmt.Errorf("%w: plan ID", ErrInvalidPlan))
	}
	if !requirementIDPattern.MatchString(string(plan.RequirementID)) {
		failures = append(failures, fmt.Errorf("%w: requirement", ErrInvalidPlan))
	}
	if !faultIDPattern.MatchString(string(plan.FaultID)) {
		failures = append(failures, fmt.Errorf("%w: fault", ErrInvalidPlan))
	}
	if !controlIDPattern.MatchString(string(plan.ControlID)) {
		failures = append(failures, fmt.Errorf("%w: control", ErrInvalidPlan))
	}
	if !barrierIDPattern.MatchString(string(plan.BarrierID)) {
		failures = append(failures, fmt.Errorf("%w: barrier", ErrInvalidPlan))
	}
	if !validPlanAssertionIDs(plan) {
		failures = append(failures, fmt.Errorf("%w: expected assertions", ErrInvalidPlan))
	}
	if err := validatePlanInjection(plan); err != nil {
		failures = append(failures, err)
	}

	control, controlExists := catalogControl(catalog, string(plan.ControlID))
	faultExists := catalogFaultExists(catalog, string(plan.FaultID))
	if !faultExists {
		failures = append(failures, fmt.Errorf("%w: fault is not cataloged", ErrInvalidPlan))
	}
	if !controlExists {
		failures = append(failures, fmt.Errorf("%w: control is not cataloged", ErrInvalidPlan))
	} else {
		if control.FaultID != string(plan.FaultID) {
			failures = append(failures, fmt.Errorf("%w: fault does not match control", ErrInvalidPlan))
		}
		if len(control.RequirementIDs) != 1 || control.RequirementIDs[0] != string(plan.RequirementID) {
			failures = append(failures, fmt.Errorf("%w: requirement does not match control", ErrInvalidPlan))
		}
		if _, valid := validSubjectTypes[control.SubjectType]; !valid {
			failures = append(failures, fmt.Errorf("%w: control subject type", ErrInvalidPlan))
		}
		if control.Injection.Mechanism != plan.Injection.Mechanism {
			failures = append(failures, fmt.Errorf("%w: mechanism does not match control", ErrInvalidPlan))
		}
		if control.Injection.Target != plan.Injection.Target {
			failures = append(failures, fmt.Errorf("%w: target does not match control", ErrInvalidPlan))
		}
		if control.Injection.Operator != plan.Injection.Operator {
			failures = append(failures, fmt.Errorf("%w: operator does not match control", ErrInvalidPlan))
		}
		if control.Injection.Parameters.Scenario != plan.Injection.Parameters.Scenario ||
			control.Injection.Parameters.Defect != plan.Injection.Parameters.Defect ||
			control.Injection.Parameters.Precondition != plan.Injection.Parameters.Precondition {
			failures = append(failures, fmt.Errorf("%w: parameters do not match control", ErrInvalidPlan))
		}
	}
	return errors.Join(failures...)
}

func openRepositoryRoot(repoRoot string) (*os.Root, error) {
	if repoRoot == "" {
		return nil, fmt.Errorf("%w: repository root is empty", ErrInvalidCatalog)
	}
	absRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve repository root: %w", err)
	}
	realRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve repository root: %w", err)
	}
	info, err := os.Stat(realRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect repository root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: repository root is not a directory", ErrInvalidCatalog)
	}
	root, err := os.OpenRoot(realRoot)
	if err != nil {
		return nil, fmt.Errorf("open repository root: %w", err)
	}
	return root, nil
}

func readCatalog(ctx context.Context, root *os.Root) ([]byte, error) {
	if root == nil {
		return nil, fmt.Errorf("%w: repository root is nil", ErrInvalidCatalog)
	}
	if err := rejectSymlinkComponents(root, catalogPath); err != nil {
		return nil, err
	}
	file, err := root.Open(filepath.FromSlash(catalogPath))
	if err != nil {
		return nil, fmt.Errorf("open fault catalog: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect fault catalog: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: fault catalog is not regular", ErrInvalidCatalog)
	}
	return readBounded(ctx, file, maximumCatalog)
}

func rejectSymlinkComponents(root *os.Root, path string) error {
	prefix := ""
	for _, component := range strings.Split(path, "/") {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("%w: catalog path is not canonical", ErrInvalidCatalog)
		}
		if prefix == "" {
			prefix = component
		} else {
			prefix += "/" + component
		}
		info, err := root.Lstat(filepath.FromSlash(prefix))
		if err != nil {
			return fmt.Errorf("inspect fault catalog path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: fault catalog path contains a symlink", ErrInvalidCatalog)
		}
		if prefix != path && !info.IsDir() {
			return fmt.Errorf("%w: fault catalog path has a non-directory component", ErrInvalidCatalog)
		}
	}
	return nil
}

func readBounded(ctx context.Context, reader io.Reader, limit int) ([]byte, error) {
	var output bytes.Buffer
	buffer := make([]byte, 32*1024)
	for {
		if err := checkContext(ctx); err != nil {
			return nil, err
		}
		count, err := reader.Read(buffer)
		if count > 0 {
			if output.Len()+count > limit {
				return nil, fmt.Errorf("%w: catalog exceeds %d bytes", ErrInvalidCatalog, limit)
			}
			_, _ = output.Write(buffer[:count])
		}
		if errors.Is(err, io.EOF) {
			return output.Bytes(), nil
		}
		if err != nil {
			return nil, fmt.Errorf("read fault catalog: %w", err)
		}
	}
}

func decodeCatalog(data []byte) (*Catalog, error) {
	if err := validateJSONDocument(data); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCatalog, err)
	}
	top, err := decodeObject(data, []string{"$schema", "schema_version", "release", "faults", "controls"}, []string{"$schema", "schema_version", "release", "faults", "controls"})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidCatalog, err)
	}

	schemaURI, err := decodeString(top["$schema"])
	if err != nil {
		return nil, catalogDecodeError(err)
	}
	schemaVersion, err := decodeInteger(top["schema_version"])
	if err != nil {
		return nil, catalogDecodeError(err)
	}
	release, err := decodeString(top["release"])
	if err != nil {
		return nil, catalogDecodeError(err)
	}
	faultValues, err := decodeArray(top["faults"])
	if err != nil {
		return nil, catalogDecodeError(err)
	}
	controlValues, err := decodeArray(top["controls"])
	if err != nil {
		return nil, catalogDecodeError(err)
	}

	catalog := &Catalog{
		SchemaURI:     schemaURI,
		SchemaVersion: schemaVersion,
		Release:       release,
		Faults:        make([]Fault, 0, len(faultValues)),
		Controls:      make([]Control, 0, len(controlValues)),
	}
	for index, value := range faultValues {
		fault, err := decodeFault(value)
		if err != nil {
			return nil, fmt.Errorf("%w: fault %d: %v", ErrInvalidCatalog, index, err)
		}
		catalog.Faults = append(catalog.Faults, fault)
	}
	for index, value := range controlValues {
		control, err := decodeControl(value)
		if err != nil {
			return nil, fmt.Errorf("%w: control %d: %v", ErrInvalidCatalog, index, err)
		}
		catalog.Controls = append(catalog.Controls, control)
	}
	return catalog, nil
}

func catalogDecodeError(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidCatalog, err)
}

func decodeFault(data json.RawMessage) (Fault, error) {
	object, err := decodeObject(data, []string{"id", "description"}, []string{"id", "description"})
	if err != nil {
		return Fault{}, err
	}
	id, err := decodeString(object["id"])
	if err != nil {
		return Fault{}, err
	}
	description, err := decodeString(object["description"])
	if err != nil {
		return Fault{}, err
	}
	return Fault{ID: id, Description: description}, nil
}

func decodeControl(data json.RawMessage) (Control, error) {
	object, err := decodeObject(data, []string{"id", "fault_id", "subject_type", "requirement_ids", "normative_references", "injection", "expected_detection"}, []string{"id", "fault_id", "subject_type", "requirement_ids", "normative_references", "injection", "expected_detection"})
	if err != nil {
		return Control{}, err
	}
	id, err := decodeString(object["id"])
	if err != nil {
		return Control{}, err
	}
	faultID, err := decodeString(object["fault_id"])
	if err != nil {
		return Control{}, err
	}
	subjectType, err := decodeString(object["subject_type"])
	if err != nil {
		return Control{}, err
	}
	requirementIDs, err := decodeStringArray(object["requirement_ids"])
	if err != nil {
		return Control{}, err
	}
	normativeReferences, err := decodeStringArray(object["normative_references"])
	if err != nil {
		return Control{}, err
	}
	injection, err := decodeInjection(object["injection"])
	if err != nil {
		return Control{}, err
	}
	expectedDetection, err := decodeString(object["expected_detection"])
	if err != nil {
		return Control{}, err
	}
	return Control{
		ID:                  id,
		FaultID:             faultID,
		SubjectType:         subjectType,
		RequirementIDs:      requirementIDs,
		NormativeReferences: normativeReferences,
		Injection:           injection,
		ExpectedDetection:   expectedDetection,
	}, nil
}

func decodeInjection(data json.RawMessage) (Injection, error) {
	object, err := decodeObject(data, []string{"mechanism", "target", "operator", "parameters"}, []string{"mechanism", "target", "operator", "parameters"})
	if err != nil {
		return Injection{}, err
	}
	mechanism, err := decodeString(object["mechanism"])
	if err != nil {
		return Injection{}, err
	}
	target, err := decodeString(object["target"])
	if err != nil {
		return Injection{}, err
	}
	operator, err := decodeString(object["operator"])
	if err != nil {
		return Injection{}, err
	}
	parameters, err := decodeParameters(object["parameters"])
	if err != nil {
		return Injection{}, err
	}
	return Injection{Mechanism: mechanism, Target: target, Operator: operator, Parameters: parameters}, nil
}

func decodeParameters(data json.RawMessage) (Parameters, error) {
	object, err := decodeObject(data, []string{"scenario", "defect"}, []string{"scenario", "defect", "precondition"})
	if err != nil {
		return Parameters{}, err
	}
	scenario, err := decodeString(object["scenario"])
	if err != nil {
		return Parameters{}, err
	}
	defect, err := decodeString(object["defect"])
	if err != nil {
		return Parameters{}, err
	}
	parameters := Parameters{Scenario: scenario, Defect: defect}
	if raw, exists := object["precondition"]; exists {
		precondition, err := decodeString(raw)
		if err != nil {
			return Parameters{}, err
		}
		parameters.Precondition = precondition
	}
	return parameters, nil
}

func decodeObject(data json.RawMessage, required, allowed []string) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("value is not an object")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &object); err != nil {
		return nil, err
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	var unknown []string
	for key := range object {
		if _, exists := allowedSet[key]; !exists {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) != 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("object has unknown member %q", unknown[0])
	}
	for _, key := range required {
		if _, exists := object[key]; !exists {
			return nil, fmt.Errorf("object is missing required member %q", key)
		}
	}
	return object, nil
}

func decodeString(data json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) < 2 || trimmed[0] != '"' {
		return "", errors.New("value is not a string")
	}
	var value string
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return "", err
	}
	return value, nil
}

func decodeInteger(data json.RawMessage) (int, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || (trimmed[0] != '-' && (trimmed[0] < '0' || trimmed[0] > '9')) {
		return 0, errors.New("value is not an integer")
	}
	var value int
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return 0, err
	}
	return value, nil
}

func decodeArray(data json.RawMessage) ([]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, errors.New("value is not an array")
	}
	var values []json.RawMessage
	if err := json.Unmarshal(trimmed, &values); err != nil {
		return nil, err
	}
	return values, nil
}

func decodeStringArray(data json.RawMessage) ([]string, error) {
	values, err := decodeArray(data)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		text, err := decodeString(value)
		if err != nil {
			return nil, err
		}
		result = append(result, text)
	}
	return result, nil
}

func validateCatalog(catalog *Catalog) error {
	if catalog == nil {
		return ErrNilCatalog
	}
	var failures []error
	if catalog.SchemaURI != CatalogSchemaURI || catalog.SchemaVersion != catalogVersion || catalog.Release != CatalogRelease {
		failures = append(failures, fmt.Errorf("%w: catalog header", ErrInvalidCatalog))
	}
	if len(catalog.Faults) == 0 {
		failures = append(failures, fmt.Errorf("%w: faults are empty", ErrInvalidCatalog))
	}
	if len(catalog.Controls) == 0 {
		failures = append(failures, fmt.Errorf("%w: controls are empty", ErrInvalidCatalog))
	}

	faultIDs := make(map[string]struct{}, len(catalog.Faults))
	for _, fault := range catalog.Faults {
		if !faultIDPattern.MatchString(fault.ID) || len(fault.ID) > maximumIDLength || fault.Description == "" {
			failures = append(failures, fmt.Errorf("%w: fault record", ErrInvalidCatalog))
			continue
		}
		if _, duplicate := faultIDs[fault.ID]; duplicate {
			failures = append(failures, fmt.Errorf("%w: duplicate fault", ErrInvalidCatalog))
			continue
		}
		faultIDs[fault.ID] = struct{}{}
	}

	controlIDs := make(map[string]struct{}, len(catalog.Controls))
	for _, control := range catalog.Controls {
		if err := validateControl(control, faultIDs); err != nil {
			failures = append(failures, err)
			continue
		}
		if _, duplicate := controlIDs[control.ID]; duplicate {
			failures = append(failures, fmt.Errorf("%w: duplicate control", ErrInvalidCatalog))
			continue
		}
		controlIDs[control.ID] = struct{}{}
	}
	return errors.Join(failures...)
}

func validateControl(control Control, faultIDs map[string]struct{}) error {
	if !controlIDPattern.MatchString(control.ID) || len(control.ID) > maximumIDLength {
		return fmt.Errorf("%w: control ID", ErrInvalidCatalog)
	}
	if !faultIDPattern.MatchString(control.FaultID) || len(control.FaultID) > maximumIDLength {
		return fmt.Errorf("%w: control fault", ErrInvalidCatalog)
	}
	if _, exists := faultIDs[control.FaultID]; !exists {
		return fmt.Errorf("%w: control names unknown fault", ErrInvalidCatalog)
	}
	if _, valid := validSubjectTypes[control.SubjectType]; !valid {
		return fmt.Errorf("%w: control subject type", ErrInvalidCatalog)
	}
	if len(control.RequirementIDs) != 1 || !requirementIDPattern.MatchString(control.RequirementIDs[0]) {
		return fmt.Errorf("%w: control requirement", ErrInvalidCatalog)
	}
	if !nonemptyUnique(control.NormativeReferences) {
		return fmt.Errorf("%w: control normative references", ErrInvalidCatalog)
	}
	if err := validateInjection(control.Injection); err != nil {
		return err
	}
	if control.ExpectedDetection == "" {
		return fmt.Errorf("%w: control expected detection", ErrInvalidCatalog)
	}
	return nil
}

func validatePlanInjection(plan scenarios.FaultPlan) error {
	if _, valid := validMechanisms[plan.Injection.Mechanism]; !valid {
		return fmt.Errorf("%w: injection mechanism", ErrInvalidPlan)
	}
	if plan.Injection.Target == "" {
		return fmt.Errorf("%w: injection target", ErrInvalidPlan)
	}
	if _, valid := validOperators[plan.Injection.Operator]; !valid {
		return fmt.Errorf("%w: injection operator", ErrInvalidPlan)
	}
	if plan.Injection.Parameters.Scenario == "" || plan.Injection.Parameters.Defect == "" {
		return fmt.Errorf("%w: injection parameters", ErrInvalidPlan)
	}
	return nil
}

func validateInjection(injection Injection) error {
	if _, valid := validMechanisms[injection.Mechanism]; !valid {
		return fmt.Errorf("%w: injection mechanism", ErrInvalidCatalog)
	}
	if injection.Target == "" {
		return fmt.Errorf("%w: injection target", ErrInvalidCatalog)
	}
	if _, valid := validOperators[injection.Operator]; !valid {
		return fmt.Errorf("%w: injection operator", ErrInvalidCatalog)
	}
	if injection.Parameters.Scenario == "" || injection.Parameters.Defect == "" {
		return fmt.Errorf("%w: injection parameters", ErrInvalidCatalog)
	}
	return nil
}

func catalogControl(catalog *Catalog, id string) (Control, bool) {
	for _, control := range catalog.Controls {
		if control.ID == id {
			return control, true
		}
	}
	return Control{}, false
}

func catalogFaultExists(catalog *Catalog, id string) bool {
	for _, fault := range catalog.Faults {
		if fault.ID == id {
			return true
		}
	}
	return false
}

func validPlanAssertionIDs(plan scenarios.FaultPlan) bool {
	if len(plan.ExpectedAssertionIDs) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(plan.ExpectedAssertionIDs))
	for _, id := range plan.ExpectedAssertionIDs {
		value := string(id)
		if !assertionIDPattern.MatchString(value) {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func nonemptyUnique(values []string) bool {
	if len(values) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return false
		}
		if _, exists := seen[value]; exists {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func validateJSONDocument(data []byte) error {
	if !utf8.Valid(data) {
		return errors.New("JSON contains invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON document: %w", err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return errors.New("top-level JSON value must be an object")
	}
	if err := inspectJSONObject(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return errors.New("JSON document contains more than one value")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return validateUnicodeScalars(data)
}

func inspectJSONObject(decoder *json.Decoder) error {
	seen := make(map[string]struct{})
	for {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON object: %w", err)
		}
		if delimiter, ok := token.(json.Delim); ok && delimiter == '}' {
			return nil
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("JSON object member name is not a string")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate JSON object member %q", key)
		}
		seen[key] = struct{}{}
		if err := inspectJSONValue(decoder); err != nil {
			return err
		}
	}
}

func inspectJSONArray(decoder *json.Decoder) error {
	for {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON array: %w", err)
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case ']':
				return nil
			case '{':
				if err := inspectJSONObject(decoder); err != nil {
					return err
				}
			case '[':
				if err := inspectJSONArray(decoder); err != nil {
					return err
				}
			}
		}
	}
}

func inspectJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON value: %w", err)
	}
	if delimiter, ok := token.(json.Delim); ok {
		switch delimiter {
		case '{':
			return inspectJSONObject(decoder)
		case '[':
			return inspectJSONArray(decoder)
		}
	}
	return nil
}

func validateUnicodeScalars(data []byte) error {
	for index := 0; index < len(data); {
		if data[index] != '"' {
			index++
			continue
		}
		index++
		for index < len(data) && data[index] != '"' {
			if data[index] != '\\' {
				_, width := utf8.DecodeRune(data[index:])
				index += width
				continue
			}
			if index+1 >= len(data) || data[index+1] != 'u' {
				index += 2
				continue
			}
			value, valid := parseUnicodeEscape(data, index)
			if !valid {
				return errors.New("JSON contains an invalid Unicode escape")
			}
			switch {
			case value >= 0xd800 && value <= 0xdbff:
				next := index + 6
				low, paired := parseUnicodeEscape(data, next)
				if !paired || low < 0xdc00 || low > 0xdfff {
					return errors.New("JSON contains a lone UTF-16 surrogate")
				}
				index = next + 6
			case value >= 0xdc00 && value <= 0xdfff:
				return errors.New("JSON contains a lone UTF-16 surrogate")
			default:
				index += 6
			}
		}
		if index < len(data) {
			index++
		}
	}
	return nil
}

func parseUnicodeEscape(data []byte, start int) (uint16, bool) {
	if start+6 > len(data) || data[start] != '\\' || data[start+1] != 'u' {
		return 0, false
	}
	var value uint16
	for _, character := range data[start+2 : start+6] {
		value <<= 4
		switch {
		case character >= '0' && character <= '9':
			value += uint16(character - '0')
		case character >= 'a' && character <= 'f':
			value += uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			value += uint16(character-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}
