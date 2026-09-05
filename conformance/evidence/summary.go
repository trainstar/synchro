// Package evidence generates and validates one closed CI summary.
package evidence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/internal/schemavalidator"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	SchemaURI     = "https://synchro.dev/conformance/schemas/ci-summary-v1.schema.json"
	SchemaVersion = 1
)

var requiredGateVariables = []string{
	"BLACKBOX_TEST_COUNT",
	"DETOX_ARGS",
	"GO_TEST_ARGS",
	"GO_TEST_PKGS",
	"GRADLE_TEST_ARGS",
	"KOTLIN_ANDROID_SERIAL",
	"MUTATION_CONTROL_EXPECT",
	"MUTATION_CONTROL_TEST",
	"PGRX_TEST_NAME",
	"RN_ANDROID_DETOX_CONFIG",
	"SUPPORT_CELL_ID",
	"SUPPORT_PLATFORM_VERSION",
	"TESTRESULT_TEST_NAME",
}

var digestedGateVariables = map[string]struct{}{
	"DETOX_ARGS":       {},
	"GO_TEST_ARGS":     {},
	"GRADLE_TEST_ARGS": {},
}

var embeddedCredentialURL = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^/@\s]+@`)

var proofHomes = map[string]string{
	"reference-model":  "scenario",
	"server-black-box": "real-integration",
	"native-e2e":       "scenario",
	"fault-injection":  "adversarial",
	"negative-control": "adversarial",
}

// Input contains terminal CI facts collected by the workflow.
type Input struct {
	Status         string         `json:"status"`
	ArtifactHashes []string       `json:"artifact_hashes"`
	GateVariables  []GateVariable `json:"gate_variables"`
	Obligations    []Obligation   `json:"obligations"`
}

// Summary binds terminal CI facts to one source commit and authored coverage.
type Summary struct {
	SchemaURI      string          `json:"$schema"`
	SchemaVersion  int             `json:"schema_version"`
	SourceCommit   string          `json:"source_commit"`
	Status         string          `json:"status"`
	ArtifactHashes []string        `json:"artifact_hashes"`
	GateVariables  []GateVariable  `json:"gate_variables"`
	Obligations    []Obligation    `json:"obligations"`
	Coverage       []CoverageEntry `json:"coverage"`
}

// GateVariable records a canonical JSON object of gate names and their values.
// Free-form argument values use sha256 digests because they can contain credentials.
type GateVariable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Obligation records one terminal semantic, smoke, or gate result.
type Obligation struct {
	ID             string   `json:"id"`
	Kind           string   `json:"kind"`
	Status         string   `json:"status"`
	Terminal       bool     `json:"terminal"`
	TestCount      int      `json:"test_count"`
	ArtifactHashes []string `json:"artifact_hashes"`
}

// CoverageEntry maps one authored ownership tuple to its executed CI obligation.
type CoverageEntry struct {
	CoverageID        string  `json:"coverage_id"`
	TestID            string  `json:"test_id"`
	RequirementID     string  `json:"requirement_id"`
	ScenarioID        string  `json:"scenario_id"`
	ProofObligationID string  `json:"proof_obligation_id"`
	AssertionID       string  `json:"assertion_id"`
	SupportCellID     *string `json:"support_cell_id"`
	ProofType         string  `json:"proof_type"`
	ProofHome         string  `json:"proof_home"`
}

type supportMatrix struct {
	Cells                 []supportCell `json:"cells"`
	SemanticCorpusCellIDs []string      `json:"semantic_corpus_cell_ids"`
}

type supportCell struct {
	ID     string `json:"id"`
	Policy string `json:"policy"`
}

// DecodeInput rejects unknown fields, duplicate members, and trailing values.
func DecodeInput(data []byte) (Input, error) {
	var input Input
	if err := decodeClosed(data, &input); err != nil {
		return Input{}, fmt.Errorf("decode CI summary input: %w", err)
	}
	return input, nil
}

// DecodeSummary rejects unknown fields, duplicate members, and trailing values.
func DecodeSummary(data []byte) (Summary, error) {
	var summary Summary
	if err := decodeClosed(data, &summary); err != nil {
		return Summary{}, fmt.Errorf("decode CI summary: %w", err)
	}
	return summary, nil
}

// Generate binds collected terminal facts to the current source commit.
func Generate(ctx context.Context, repoRoot string, input Input) (Summary, error) {
	if err := contextError(ctx); err != nil {
		return Summary{}, err
	}
	root, err := repositoryRoot(repoRoot)
	if err != nil {
		return Summary{}, err
	}
	commit, err := repositoryCommit(ctx, root)
	if err != nil {
		return Summary{}, err
	}
	coverage, err := authoredCoverage(ctx, root)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{
		SchemaURI:      SchemaURI,
		SchemaVersion:  SchemaVersion,
		SourceCommit:   commit,
		Status:         input.Status,
		ArtifactHashes: cloneStrings(input.ArtifactHashes),
		GateVariables:  append([]GateVariable(nil), input.GateVariables...),
		Obligations:    cloneObligations(input.Obligations),
		Coverage:       coverage,
	}
	canonicalize(&summary)
	if err := Validate(ctx, root, summary); err != nil {
		return Summary{}, err
	}
	return summary, nil
}

// Validate checks a summary against the current commit and authored obligations.
func Validate(ctx context.Context, repoRoot string, summary Summary) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	root, err := repositoryRoot(repoRoot)
	if err != nil {
		return err
	}
	commit, err := repositoryCommit(ctx, root)
	if err != nil {
		return err
	}
	if summary.SchemaURI != SchemaURI || summary.SchemaVersion != SchemaVersion {
		return errors.New("CI summary schema binding is invalid")
	}
	if summary.SourceCommit != commit {
		return errors.New("CI summary source commit does not match HEAD")
	}
	if summary.Status != "passed" {
		return errors.New("CI summary status is not passed")
	}
	if err := validateHashes(summary.ArtifactHashes, "CI summary"); err != nil {
		return err
	}
	expected, err := expectedObligations(ctx, root)
	if err != nil {
		return err
	}
	if err := validateGateVariables(summary.GateVariables, expected); err != nil {
		return err
	}
	if err := validateObligations(summary.Obligations, expected, summary.ArtifactHashes); err != nil {
		return err
	}
	expectedCoverage, err := authoredCoverage(ctx, root)
	if err != nil {
		return err
	}
	if err := validateCoverage(summary.Coverage, expectedCoverage, expected); err != nil {
		return err
	}
	encoded, err := MarshalStrict(summary)
	if err != nil {
		return fmt.Errorf("encode CI summary for schema validation: %w", err)
	}
	validator := schemavalidator.New(root)
	defer validator.Close()
	if err := validator.ValidateBytes(ctx, "conformance/schemas/ci-summary-v1.schema.json", encoded); err != nil {
		return fmt.Errorf("validate CI summary schema: %w", err)
	}
	return nil
}

// Encode writes deterministic indented JSON with one trailing newline.
func Encode(writer io.Writer, summary Summary) error {
	if writer == nil {
		return errors.New("CI summary writer is nil")
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(summary); err != nil {
		return fmt.Errorf("encode CI summary: %w", err)
	}
	return nil
}

// RequiredGateVariableNames returns the closed gate-variable allowlist.
func RequiredGateVariableNames() []string {
	return cloneStrings(requiredGateVariables)
}

func expectedObligations(ctx context.Context, root string) (map[string]string, error) {
	matrix, err := loadSupportMatrix(root)
	if err != nil {
		return nil, err
	}
	semanticCells := make(map[string]struct{}, len(matrix.SemanticCorpusCellIDs))
	for _, id := range matrix.SemanticCorpusCellIDs {
		if id == "" {
			return nil, errors.New("support matrix has an empty semantic cell")
		}
		if _, duplicate := semanticCells[id]; duplicate {
			return nil, errors.New("support matrix repeats a semantic cell")
		}
		semanticCells[id] = struct{}{}
	}
	allScenarios, err := scenarios.LoadAll(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("load CI summary scenarios: %w", err)
	}
	expected := make(map[string]string)
	for _, scenario := range allScenarios {
		for _, obligation := range scenario.ProofObligations {
			if obligation.ProofType != "native-e2e" || obligation.SupportCellID == nil {
				continue
			}
			if _, required := semanticCells[string(*obligation.SupportCellID)]; !required {
				continue
			}
			id := "semantic/" + string(obligation.ObligationID)
			if _, duplicate := expected[id]; duplicate {
				return nil, fmt.Errorf("CI summary repeats semantic obligation %s", id)
			}
			expected[id] = "semantic"
		}
	}
	for _, cell := range matrix.Cells {
		if cell.Policy == "excluded" {
			continue
		}
		for _, operation := range []string{"connect", "push", "pull", "kill", "resume"} {
			expected["smoke/"+cell.ID+"/"+operation] = "smoke"
		}
	}
	gates, err := phaseGateTargets(root)
	if err != nil {
		return nil, err
	}
	for _, gate := range gates {
		expected["gate/"+gate] = "gate"
	}
	return expected, nil
}

func authoredCoverage(ctx context.Context, root string) ([]CoverageEntry, error) {
	values, err := scenarios.LoadAll(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("load coverage scenarios: %w", err)
	}
	matrix, err := loadSupportMatrix(root)
	if err != nil {
		return nil, err
	}
	semanticCells := make(map[string]struct{}, len(matrix.SemanticCorpusCellIDs))
	for _, id := range matrix.SemanticCorpusCellIDs {
		semanticCells[id] = struct{}{}
	}
	entries := make([]CoverageEntry, 0)
	seen := make(map[string]struct{})
	for _, scenario := range values {
		obligations := make(map[string]scenarios.ProofObligation, len(scenario.ProofObligations))
		for _, obligation := range scenario.ProofObligations {
			obligations[string(obligation.ObligationID)] = obligation
		}
		for _, owner := range scenario.Ownership {
			obligation, found := obligations[string(owner.ProofObligationID)]
			if !found || owner.ScenarioID != scenario.ID || owner.ProofType != obligation.ProofType {
				return nil, errors.New("coverage ownership is not closed")
			}
			if obligation.ProofType == "native-e2e" {
				if obligation.SupportCellID == nil {
					return nil, errors.New("native coverage has no support cell")
				}
				if _, executed := semanticCells[string(*obligation.SupportCellID)]; !executed {
					continue
				}
			}
			home, found := proofHomes[owner.ProofType]
			if !found {
				return nil, fmt.Errorf("coverage proof type %s has no proof home", owner.ProofType)
			}
			testID := coverageTestID(obligation)
			key := strings.Join([]string{string(owner.RequirementID), string(owner.ScenarioID), string(owner.ProofObligationID), string(owner.AssertionID), optionalSupportID(owner.SupportCellID)}, "\x00")
			if _, duplicate := seen[key]; duplicate {
				return nil, errors.New("coverage contains a duplicate proof home")
			}
			seen[key] = struct{}{}
			digest := sha256.Sum256([]byte(key))
			entries = append(entries, CoverageEntry{
				CoverageID:        "COV-" + strings.ToUpper(hex.EncodeToString(digest[:8])),
				TestID:            testID,
				RequirementID:     string(owner.RequirementID),
				ScenarioID:        string(owner.ScenarioID),
				ProofObligationID: string(owner.ProofObligationID),
				AssertionID:       string(owner.AssertionID),
				SupportCellID:     copySupportID(owner.SupportCellID),
				ProofType:         owner.ProofType,
				ProofHome:         home,
			})
		}
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].CoverageID < entries[right].CoverageID })
	if len(entries) == 0 {
		return nil, errors.New("coverage has no authored ownership tuples")
	}
	return entries, nil
}

func coverageTestID(obligation scenarios.ProofObligation) string {
	switch obligation.ProofType {
	case "native-e2e":
		return "semantic/" + string(obligation.ObligationID)
	case "server-black-box", "fault-injection":
		return "gate/test-blackbox"
	default:
		return "gate/test-conformance"
	}
}

func validateCoverage(actual, expected []CoverageEntry, obligations map[string]string) error {
	if len(actual) != len(expected) {
		return errors.New("CI summary coverage count is incomplete")
	}
	expectedByID := make(map[string]CoverageEntry, len(expected))
	for _, entry := range expected {
		expectedByID[entry.CoverageID] = entry
	}
	seen := make(map[string]struct{}, len(actual))
	proofHomesByTuple := make(map[string]string, len(actual))
	for _, entry := range actual {
		wanted, found := expectedByID[entry.CoverageID]
		if !found || !coverageEqual(entry, wanted) {
			return errors.New("CI summary coverage differs from authored ownership")
		}
		if _, found := obligations[entry.TestID]; !found {
			return fmt.Errorf("CI summary coverage references missing test %s", entry.TestID)
		}
		if _, duplicate := seen[entry.CoverageID]; duplicate {
			return errors.New("CI summary coverage ID is duplicated")
		}
		seen[entry.CoverageID] = struct{}{}
		tuple := strings.Join([]string{entry.RequirementID, entry.ScenarioID, entry.ProofObligationID, entry.AssertionID, optionalString(entry.SupportCellID)}, "\x00")
		if home, exists := proofHomesByTuple[tuple]; exists && home != entry.ProofHome {
			return errors.New("CI summary coverage assigns duplicate proof homes")
		}
		proofHomesByTuple[tuple] = entry.ProofHome
	}
	return nil
}

func validateGateVariables(values []GateVariable, obligations map[string]string) error {
	if len(values) != len(requiredGateVariables) {
		return errors.New("CI summary gate-variable set is incomplete")
	}
	wanted := make(map[string]struct{}, len(requiredGateVariables))
	for _, name := range requiredGateVariables {
		wanted[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(values))
	wantedGates := make(map[string]struct{})
	for id, kind := range obligations {
		if kind == "gate" && strings.HasPrefix(id, "gate/") {
			wantedGates[strings.TrimPrefix(id, "gate/")] = struct{}{}
		}
	}
	for _, variable := range values {
		if _, required := wanted[variable.Name]; !required {
			return fmt.Errorf("CI summary has unknown gate variable %s", variable.Name)
		}
		if _, duplicate := seen[variable.Name]; duplicate {
			return fmt.Errorf("CI summary repeats gate variable %s", variable.Name)
		}
		if len(variable.Value) > 4096 || strings.ContainsAny(variable.Value, "\x00\r\n") {
			return fmt.Errorf("CI summary gate variable %s has an unsafe value", variable.Name)
		}
		var byGate map[string]string
		if err := decodeClosed([]byte(variable.Value), &byGate); err != nil || len(byGate) == 0 {
			return fmt.Errorf("CI summary gate variable %s has invalid gate values", variable.Name)
		}
		if len(byGate) != len(wantedGates) {
			return fmt.Errorf("CI summary gate variable %s has an incomplete gate set", variable.Name)
		}
		for gate, value := range byGate {
			if _, required := wantedGates[gate]; !required {
				return fmt.Errorf("CI summary gate variable %s has an unknown gate %s", variable.Name, gate)
			}
			if gate == "" || len(value) > 4096 || strings.ContainsAny(value, "\x00\r\n") {
				return fmt.Errorf("CI summary gate variable %s has an unsafe gate value", variable.Name)
			}
			if embeddedCredentialURL.MatchString(value) {
				return fmt.Errorf("CI summary gate variable %s exposes credentials", variable.Name)
			}
			if _, digested := digestedGateVariables[variable.Name]; digested && (!strings.HasPrefix(value, "sha256:") || !validSHA256(strings.TrimPrefix(value, "sha256:"))) {
				return fmt.Errorf("CI summary gate variable %s exposes a sensitive value", variable.Name)
			}
		}
		seen[variable.Name] = struct{}{}
	}
	return nil
}

func validateObligations(values []Obligation, expected map[string]string, summaryHashes []string) error {
	if len(values) != len(expected) {
		return errors.New("CI summary obligation count is incomplete")
	}
	allowedHashes := make(map[string]struct{}, len(summaryHashes))
	for _, value := range summaryHashes {
		allowedHashes[value] = struct{}{}
	}
	seen := make(map[string]struct{}, len(values))
	for _, obligation := range values {
		kind, found := expected[obligation.ID]
		if !found || obligation.Kind != kind {
			return fmt.Errorf("CI summary has unknown obligation %s", obligation.ID)
		}
		if _, duplicate := seen[obligation.ID]; duplicate {
			return fmt.Errorf("CI summary repeats obligation %s", obligation.ID)
		}
		if obligation.Status != "passed" || !obligation.Terminal || obligation.TestCount < 1 {
			return fmt.Errorf("CI summary obligation %s is not a terminal pass", obligation.ID)
		}
		if err := validateHashes(obligation.ArtifactHashes, "CI summary obligation "+obligation.ID); err != nil {
			return err
		}
		for _, value := range obligation.ArtifactHashes {
			if _, found := allowedHashes[value]; !found {
				return fmt.Errorf("CI summary obligation %s references an artifact hash outside the summary", obligation.ID)
			}
		}
		seen[obligation.ID] = struct{}{}
	}
	return nil
}

func validateHashes(values []string, label string) error {
	if len(values) == 0 {
		return fmt.Errorf("%s has no artifact hashes", label)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !validSHA256(value) {
			return fmt.Errorf("%s has an invalid artifact hash", label)
		}
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("%s repeats an artifact hash", label)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func loadSupportMatrix(root string) (supportMatrix, error) {
	data, err := os.ReadFile(filepath.Join(root, "conformance", "support-matrix.json"))
	if err != nil {
		return supportMatrix{}, fmt.Errorf("read support matrix: %w", err)
	}
	var matrix supportMatrix
	if err := jsonstrict.Decode(data, &matrix); err != nil {
		return supportMatrix{}, fmt.Errorf("decode support matrix: %w", err)
	}
	seen := make(map[string]struct{}, len(matrix.Cells))
	for _, cell := range matrix.Cells {
		if cell.ID == "" || (cell.Policy != "required" && cell.Policy != "tested" && cell.Policy != "excluded") {
			return supportMatrix{}, errors.New("support matrix cell is invalid")
		}
		if _, duplicate := seen[cell.ID]; duplicate {
			return supportMatrix{}, errors.New("support matrix repeats a cell")
		}
		seen[cell.ID] = struct{}{}
	}
	return matrix, nil
}

func decodeClosed(data []byte, destination any) error {
	if err := jsonstrict.ValidateValue(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON contains trailing data")
		}
		return err
	}
	return nil
}

func phaseGateTargets(root string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		return nil, fmt.Errorf("read Makefile: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "phase-5-check:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "phase-5-check:"))
		if len(fields) == 0 {
			return nil, errors.New("phase-5-check has no gate prerequisites")
		}
		if len(fields) != len(uniqueStrings(fields)) {
			return nil, errors.New("phase-5-check repeats a gate prerequisite")
		}
		sort.Strings(fields)
		return fields, nil
	}
	return nil, errors.New("Makefile has no phase-5-check target")
}

func repositoryRoot(value string) (string, error) {
	if value == "" || strings.IndexByte(value, 0) >= 0 {
		return "", errors.New("repository root is invalid")
	}
	root, err := filepath.Abs(value)
	if err != nil {
		return "", errors.New("repository root is invalid")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return "", errors.New("repository root is not a directory")
	}
	return filepath.Clean(root), nil
}

func repositoryCommit(ctx context.Context, root string) (string, error) {
	command := exec.CommandContext(ctx, "git", "rev-parse", "--verify", "HEAD")
	command.Dir = root
	command.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_NO_REPLACE_OBJECTS=1")
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("resolve source commit: %w", err)
	}
	commit := strings.TrimSpace(string(output))
	if len(commit) != 40 || !validHex(commit) {
		return "", errors.New("source commit is invalid")
	}
	return commit, nil
}

func canonicalize(summary *Summary) {
	sort.Strings(summary.ArtifactHashes)
	sort.Slice(summary.GateVariables, func(left, right int) bool {
		return summary.GateVariables[left].Name < summary.GateVariables[right].Name
	})
	for index := range summary.Obligations {
		sort.Strings(summary.Obligations[index].ArtifactHashes)
	}
	sort.Slice(summary.Obligations, func(left, right int) bool { return summary.Obligations[left].ID < summary.Obligations[right].ID })
	sort.Slice(summary.Coverage, func(left, right int) bool {
		return summary.Coverage[left].CoverageID < summary.Coverage[right].CoverageID
	})
}

func cloneObligations(values []Obligation) []Obligation {
	result := make([]Obligation, len(values))
	for index, value := range values {
		result[index] = value
		result[index].ArtifactHashes = cloneStrings(value.ArtifactHashes)
	}
	return result
}

func cloneStrings(values []string) []string {
	return append([]string(nil), values...)
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, found := seen[value]; found {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func validSHA256(value string) bool {
	return len(value) == sha256.Size*2 && validHex(value)
}

func validHex(value string) bool {
	if value == "" || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	return ctx.Err()
}

func optionalSupportID(value *contract.SupportCellID) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

func copySupportID(value *contract.SupportCellID) *string {
	if value == nil {
		return nil
	}
	result := string(*value)
	return &result
}

func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func coverageEqual(left, right CoverageEntry) bool {
	return left.CoverageID == right.CoverageID && left.TestID == right.TestID && left.RequirementID == right.RequirementID && left.ScenarioID == right.ScenarioID && left.ProofObligationID == right.ProofObligationID && left.AssertionID == right.AssertionID && optionalString(left.SupportCellID) == optionalString(right.SupportCellID) && left.ProofType == right.ProofType && left.ProofHome == right.ProofHome
}

// MarshalStrict is used by tests and the CLI to reject trailing input.
func MarshalStrict(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(buffer.Bytes()), nil
}
