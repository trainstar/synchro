package evidence

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateAndValidateCISummary(t *testing.T) {
	root := repositoryForTest(t)
	input := validInput(t, root)
	summary, err := Generate(context.Background(), root, input)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if summary.SchemaURI != SchemaURI || summary.SchemaVersion != SchemaVersion || len(summary.Coverage) == 0 {
		t.Fatalf("generated summary is incomplete: %#v", summary)
	}
	var blackboxValues map[string]string
	if err := json.Unmarshal([]byte(summary.GateVariables[0].Value), &blackboxValues); err != nil {
		t.Fatalf("decode generated gate variable: %v", err)
	}
	if summary.GateVariables[0].Name != "BLACKBOX_TEST_COUNT" || blackboxValues["test-conformance"] != "17" {
		t.Fatalf("generated gate variable = %#v, want exact value", summary.GateVariables[0])
	}
	if err := Validate(context.Background(), root, summary); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	var encoded bytes.Buffer
	if err := Encode(&encoded, summary); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	decoded, err := DecodeSummary(encoded.Bytes())
	if err != nil {
		t.Fatalf("DecodeSummary() error = %v", err)
	}
	if err := Validate(context.Background(), root, decoded); err != nil {
		t.Fatalf("Validate(decoded) error = %v", err)
	}
}

func TestRequiredGateVariablesRemainClosed(t *testing.T) {
	want := []string{
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
	if got := RequiredGateVariableNames(); !equalStrings(got, want) {
		t.Fatalf("RequiredGateVariableNames() = %v, want %v", got, want)
	}
}

func TestCISummaryRejectsGateVariableAndCoverageDrift(t *testing.T) {
	root := repositoryForTest(t)
	input := validInput(t, root)
	input.GateVariables = input.GateVariables[1:]
	if _, err := Generate(context.Background(), root, input); err == nil || !strings.Contains(err.Error(), "gate-variable set") {
		t.Fatalf("missing gate variable error = %v", err)
	}

	input = validInput(t, root)
	input.GateVariables[0].Name = "UNLISTED_VARIABLE"
	if _, err := Generate(context.Background(), root, input); err == nil || !strings.Contains(err.Error(), "unknown gate variable") {
		t.Fatalf("unlisted gate variable error = %v", err)
	}

	input = validInput(t, root)
	input.GateVariables[0] = input.GateVariables[1]
	if _, err := Generate(context.Background(), root, input); err == nil || !strings.Contains(err.Error(), "repeats gate variable") {
		t.Fatalf("duplicate gate variable error = %v", err)
	}

	input = validInput(t, root)
	input.GateVariables[0].Value = "unsafe\nvalue"
	if _, err := Generate(context.Background(), root, input); err == nil || !strings.Contains(err.Error(), "unsafe value") {
		t.Fatalf("unsafe gate variable error = %v", err)
	}

	summary, err := Generate(context.Background(), root, validInput(t, root))
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	summary.Coverage[0].ProofHome = "unit"
	if err := Validate(context.Background(), root, summary); err == nil || !strings.Contains(err.Error(), "coverage differs") {
		t.Fatalf("changed proof home error = %v", err)
	}
}

func TestCISummaryRejectsMissingZeroAndUnknownObligations(t *testing.T) {
	root := repositoryForTest(t)
	input := validInput(t, root)
	input.Obligations = input.Obligations[1:]
	if _, err := Generate(context.Background(), root, input); err == nil || !strings.Contains(err.Error(), "obligation count") {
		t.Fatalf("missing obligation error = %v", err)
	}

	input = validInput(t, root)
	input.Obligations[0].TestCount = 0
	if _, err := Generate(context.Background(), root, input); err == nil || !strings.Contains(err.Error(), "terminal pass") {
		t.Fatalf("zero test count error = %v", err)
	}

	input = validInput(t, root)
	input.Obligations[0].ID = "gate/unknown"
	if _, err := Generate(context.Background(), root, input); err == nil || !strings.Contains(err.Error(), "unknown obligation") {
		t.Fatalf("unknown obligation error = %v", err)
	}

	input = validInput(t, root)
	input.Obligations[0].ArtifactHashes[0] = strings.Repeat("b", 64)
	if _, err := Generate(context.Background(), root, input); err == nil || !strings.Contains(err.Error(), "outside the summary") {
		t.Fatalf("unbound artifact hash error = %v", err)
	}
}

func TestDecodeCISummaryInputRejectsUnknownAndDuplicateFields(t *testing.T) {
	for _, data := range []string{
		`{"status":"passed","artifact_hashes":[],"gate_variables":[],"obligations":[],"unknown":true}`,
		`{"status":"passed","status":"failed","artifact_hashes":[],"gate_variables":[],"obligations":[]}`,
	} {
		if _, err := DecodeInput([]byte(data)); err == nil {
			t.Fatalf("DecodeInput(%s) accepted invalid data", data)
		}
	}
}

func validInput(t *testing.T, root string) Input {
	t.Helper()
	expected, err := expectedObligations(context.Background(), root)
	if err != nil {
		t.Fatalf("expectedObligations() error = %v", err)
	}
	hash := strings.Repeat("a", 64)
	obligations := make([]Obligation, 0, len(expected))
	for id, kind := range expected {
		obligations = append(obligations, Obligation{ID: id, Kind: kind, Status: "passed", Terminal: true, TestCount: 1, ArtifactHashes: []string{hash}})
	}
	gateValues := make(map[string]string)
	for id, kind := range expected {
		if kind == "gate" {
			gateValues[strings.TrimPrefix(id, "gate/")] = ""
		}
	}
	variables := make([]GateVariable, 0, len(requiredGateVariables))
	for _, name := range requiredGateVariables {
		values := make(map[string]string, len(gateValues))
		for gate, value := range gateValues {
			values[gate] = value
		}
		if name == "BLACKBOX_TEST_COUNT" {
			values["test-conformance"] = "17"
		}
		if _, digested := digestedGateVariables[name]; digested {
			for gate := range values {
				values[gate] = "sha256:" + strings.Repeat("a", 64)
			}
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			t.Fatalf("marshal gate values: %v", err)
		}
		variables = append(variables, GateVariable{Name: name, Value: string(encoded)})
	}
	return Input{Status: "passed", ArtifactHashes: []string{hash}, GateVariables: variables, Obligations: obligations}
}

func repositoryForTest(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	return root
}

func equalStrings(left, right []string) bool {
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

func TestSummaryJSONShapeIsClosed(t *testing.T) {
	data, err := json.Marshal(Summary{})
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	if !bytes.Contains(data, []byte(`"coverage"`)) || bytes.Contains(data, []byte(`"receipt"`)) {
		t.Fatalf("summary JSON shape = %s", data)
	}
}
