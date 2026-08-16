package main

import (
	"strings"
	"testing"
)

func TestClassifyTestResult(t *testing.T) {
	const target = "TestRealMutationControlCursorAdvancement"
	tests := []struct {
		name  string
		input string
		want  result
	}{
		{
			name: "target pass",
			input: eventStream(
				`{"Action":"start","Package":"example/integration"}`,
				`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
				`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
				`{"Action":"pass","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
				`{"Action":"pass","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
				`{"Action":"pass","Package":"example/integration"}`,
			),
			want: resultTargetPass,
		},
		{
			name: "target semantic test failure",
			input: eventStream(
				`{"Action":"start","Package":"example/integration"}`,
				`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
				`{"Action":"output","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement","Output":"=== RUN   TestRealMutationControlCursorAdvancement\n"}`,
				`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
				`{"Action":"fail","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
				`{"Action":"output","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement","Output":"--- FAIL: TestRealMutationControlCursorAdvancement (0.01s)\n"}`,
				`{"Action":"fail","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
				`{"Action":"output","Package":"example/integration","Output":"FAIL\n"}`,
				`{"Action":"output","Package":"example/integration","Output":"FAIL\texample/integration\t0.02s\n"}`,
				`{"Action":"fail","Package":"example/integration"}`,
			),
			want: resultTargetSemanticTestFailure,
		},
		{
			name: "package setup failure",
			input: eventStream(
				`{"Action":"start","Package":"example/integration"}`,
				`{"Action":"output","Package":"example/integration"}`,
				`{"Action":"fail","Package":"example/integration"}`,
			),
			want: resultPackageSetupFailure,
		},
		{
			name: "target setup failure before assertion",
			input: eventStream(
				`{"Action":"start","Package":"example/integration"}`,
				`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
				`{"Action":"fail","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
				`{"Action":"fail","Package":"example/integration"}`,
			),
			want: resultPackageSetupFailure,
		},
		{
			name: "skip",
			input: eventStream(
				`{"Action":"start","Package":"example/integration"}`,
				`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
				`{"Action":"skip","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
				`{"Action":"pass","Package":"example/integration"}`,
			),
			want: resultSkip,
		},
		{
			name: "missing test",
			input: eventStream(
				`{"Action":"start","Package":"example/integration"}`,
				`{"Action":"pass","Package":"example/integration"}`,
			),
			want: resultMissingTest,
		},
		{
			name: "malformed output",
			input: eventStream(
				`{"Action":"start","Package":"example/integration"}`,
				`not JSON`,
			),
			want: resultMalformedOutput,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyTestResult(strings.NewReader(test.input), target); got != test.want {
				t.Fatalf("classifyTestResult() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestClassifyTestResultRejectsUnexpectedTests(t *testing.T) {
	input := eventStream(
		`{"Action":"start","Package":"example/integration"}`,
		`{"Action":"run","Package":"example/integration","Test":"TestUnrelated"}`,
	)
	if got := classifyTestResult(strings.NewReader(input), "TestRealMutationControlCursorAdvancement"); got != resultMalformedOutput {
		t.Fatalf("classifyTestResult() = %q, want %q", got, resultMalformedOutput)
	}
}

func TestClassifyTestResultRejectsTestsAfterPackageFinal(t *testing.T) {
	input := eventStream(
		`{"Action":"start","Package":"example/integration"}`,
		`{"Action":"pass","Package":"example/integration"}`,
		`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
	)
	if got := classifyTestResult(strings.NewReader(input), "TestRealMutationControlCursorAdvancement"); got != resultMalformedOutput {
		t.Fatalf("classifyTestResult() = %q, want %q", got, resultMalformedOutput)
	}
}

func TestClassifyTestResultRejectsTargetFinalBeforeAssertionFinal(t *testing.T) {
	input := eventStream(
		`{"Action":"start","Package":"example/integration"}`,
		`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
		`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
		`{"Action":"fail","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
		`{"Action":"fail","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
		`{"Action":"fail","Package":"example/integration"}`,
	)
	if got := classifyTestResult(strings.NewReader(input), "TestRealMutationControlCursorAdvancement"); got != resultMalformedOutput {
		t.Fatalf("classifyTestResult() = %q, want %q", got, resultMalformedOutput)
	}
}

func TestClassifyTestResultDoesNotCountCleanupFailureAsSemanticFailure(t *testing.T) {
	input := eventStream(
		`{"Action":"start","Package":"example/integration"}`,
		`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
		`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
		`{"Action":"fail","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
		`{"Action":"output","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement","Output":"cleanup failed\n"}`,
		`{"Action":"fail","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
		`{"Action":"fail","Package":"example/integration"}`,
	)
	if got := classifyTestResult(strings.NewReader(input), "TestRealMutationControlCursorAdvancement"); got != resultPackageSetupFailure {
		t.Fatalf("classifyTestResult() = %q, want %q", got, resultPackageSetupFailure)
	}
}

func TestClassifyTestResultRejectsForgedPackageSummary(t *testing.T) {
	const target = "TestRealMutationControlCursorAdvancement"
	input := eventStream(
		`{"Action":"start","Package":"example/integration"}`,
		`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
		`{"Action":"run","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
		`{"Action":"fail","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement/assertion"}`,
		`{"Action":"output","Package":"example/integration","Output":"FAIL\texample/other\t0.02s\n"}`,
		`{"Action":"fail","Package":"example/integration","Test":"TestRealMutationControlCursorAdvancement"}`,
		`{"Action":"fail","Package":"example/integration"}`,
	)
	if got := classifyTestResult(strings.NewReader(input), target); got != resultPackageSetupFailure {
		t.Fatalf("classifyTestResult() = %q, want %q", got, resultPackageSetupFailure)
	}
}

func eventStream(events ...string) string {
	return strings.Join(events, "\n") + "\n"
}
