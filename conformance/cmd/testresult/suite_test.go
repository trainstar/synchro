package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const capturedParallelJSON = `{"Time":"2026-08-27T18:23:33.403979-05:00","Action":"start","Package":"example.com/testresultfixture"}
{"Time":"2026-08-27T18:23:33.585133-05:00","Action":"run","Package":"example.com/testresultfixture","Test":"TestParallel"}
{"Time":"2026-08-27T18:23:33.585211-05:00","Action":"output","Package":"example.com/testresultfixture","Test":"TestParallel","Output":"=== RUN   TestParallel\n"}
{"Time":"2026-08-27T18:23:33.585236-05:00","Action":"run","Package":"example.com/testresultfixture","Test":"TestParallel/first"}
{"Time":"2026-08-27T18:23:33.585238-05:00","Action":"output","Package":"example.com/testresultfixture","Test":"TestParallel/first","Output":"=== RUN   TestParallel/first\n"}
{"Time":"2026-08-27T18:23:33.585241-05:00","Action":"output","Package":"example.com/testresultfixture","Test":"TestParallel/first","Output":"=== PAUSE TestParallel/first\n"}
{"Time":"2026-08-27T18:23:33.585242-05:00","Action":"pause","Package":"example.com/testresultfixture","Test":"TestParallel/first"}
{"Time":"2026-08-27T18:23:33.585245-05:00","Action":"run","Package":"example.com/testresultfixture","Test":"TestParallel/second"}
{"Time":"2026-08-27T18:23:33.585247-05:00","Action":"output","Package":"example.com/testresultfixture","Test":"TestParallel/second","Output":"=== RUN   TestParallel/second\n"}
{"Time":"2026-08-27T18:23:33.585248-05:00","Action":"output","Package":"example.com/testresultfixture","Test":"TestParallel/second","Output":"=== PAUSE TestParallel/second\n"}
{"Time":"2026-08-27T18:23:33.58525-05:00","Action":"pause","Package":"example.com/testresultfixture","Test":"TestParallel/second"}
{"Time":"2026-08-27T18:23:33.585252-05:00","Action":"cont","Package":"example.com/testresultfixture","Test":"TestParallel/first"}
{"Time":"2026-08-27T18:23:33.585253-05:00","Action":"output","Package":"example.com/testresultfixture","Test":"TestParallel/first","Output":"=== CONT  TestParallel/first\n"}
{"Time":"2026-08-27T18:23:33.585255-05:00","Action":"cont","Package":"example.com/testresultfixture","Test":"TestParallel/second"}
{"Time":"2026-08-27T18:23:33.585257-05:00","Action":"output","Package":"example.com/testresultfixture","Test":"TestParallel/second","Output":"=== CONT  TestParallel/second\n"}
{"Time":"2026-08-27T18:23:33.585262-05:00","Action":"output","Package":"example.com/testresultfixture","Output":"--- PASS: TestParallel (0.00s)\n"}
{"Time":"2026-08-27T18:23:33.585263-05:00","Action":"pass","Package":"example.com/testresultfixture","Test":"TestParallel/first","Elapsed":0}
{"Time":"2026-08-27T18:23:33.585284-05:00","Action":"pass","Package":"example.com/testresultfixture","Test":"TestParallel/second","Elapsed":0}
{"Time":"2026-08-27T18:23:33.585286-05:00","Action":"pass","Package":"example.com/testresultfixture","Test":"TestParallel","Elapsed":0}
{"Time":"2026-08-27T18:23:33.585288-05:00","Action":"output","Package":"example.com/testresultfixture","Output":"PASS\n"}
{"Time":"2026-08-27T18:23:33.585553-05:00","Action":"output","Package":"example.com/testresultfixture","Output":"ok  \texample.com/testresultfixture\t0.181s\n"}
{"Time":"2026-08-27T18:23:33.587622-05:00","Action":"pass","Package":"example.com/testresultfixture","Elapsed":0.184}`

func TestValidateSuiteResultAcceptsPackageScopedSubtestSummary(t *testing.T) {
	summary, err := validateSuiteResult(strings.NewReader(capturedParallelJSON), false)
	if err != nil || summary.Tests != 3 {
		t.Fatalf("validateSuiteResult() = %#v, %v", summary, err)
	}
}

func TestValidateSuiteResultRejectsUnscopedForeignOutput(t *testing.T) {
	input := eventStream(
		`{"Action":"start","Package":"example/one"}`,
		`{"Action":"output","Package":"example/one","Output":"foreign output\n"}`,
	)
	for _, benchmarks := range []bool{false, true} {
		if _, err := validateSuiteResult(strings.NewReader(input), benchmarks); err == nil {
			t.Fatalf("validateSuiteResult(benchmarks=%t) accepted unscoped foreign output", benchmarks)
		}
	}
}

func TestValidateSuiteResult(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantPackages  int
		wantTests     int
		wantError     bool
		wantErrorText string
	}{
		{
			name: "one passing package",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one"}`,
			),
			wantPackages: 1,
			wantTests:    1,
		},
		{
			name: "interleaved passing packages",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"start","Package":"example/two"}`,
				`{"Action":"run","Package":"example/two","Test":"TestTwo"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/two","Test":"TestTwo"}`,
				`{"Action":"pass","Package":"example/two"}`,
				`{"Action":"pass","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one"}`,
			),
			wantPackages: 2,
			wantTests:    2,
		},
		{
			name: "repeated passing test",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one"}`,
			),
			wantPackages: 1,
			wantTests:    2,
		},
		{
			name: "negative control rejects package without test files beside tested package",
			input: eventStream(
				`{"Action":"start","Package":"example/empty"}`,
				`{"Action":"output","Package":"example/empty","Output":"?\texample/empty\t[no test files]\n"}`,
				`{"Action":"skip","Package":"example/empty"}`,
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one"}`,
			),
			wantError:     true,
			wantErrorText: "contains no test files",
		},
		{
			name:      "empty output",
			wantError: true,
		},
		{
			name: "zero match",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"output","Package":"example/one","Output":"testing: warning: no tests to run\n"}`,
				`{"Action":"pass","Package":"example/one"}`,
			),
			wantError: true,
		},
		{
			name: "only package without test files",
			input: eventStream(
				`{"Action":"start","Package":"example/empty"}`,
				`{"Action":"output","Package":"example/empty","Output":"?\texample/empty\t[no test files]\n"}`,
				`{"Action":"skip","Package":"example/empty"}`,
			),
			wantError:     true,
			wantErrorText: "contains no test files",
		},
		{
			name: "zero matching subtests with passing parent",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"output","Package":"example/one","Test":"TestOne","Output":"testing: warning: no tests to run\n"}`,
				`{"Action":"pass","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one"}`,
			),
			wantError: true,
		},
		{
			name: "zero matching subtests in package summary",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"output","Package":"example/one","Output":"ok  \texample/one\t0.001s [no tests to run]\n"}`,
				`{"Action":"pass","Package":"example/one"}`,
			),
			wantError: true,
		},
		{
			name: "skipped test",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"skip","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"pass","Package":"example/one"}`,
			),
			wantError: true,
		},
		{
			name: "failed test",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"fail","Package":"example/one","Test":"TestOne"}`,
				`{"Action":"fail","Package":"example/one"}`,
			),
			wantError: true,
		},
		{
			name: "unfinished package",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`{"Action":"run","Package":"example/one","Test":"TestOne"}`,
			),
			wantError: true,
		},
		{
			name: "malformed output",
			input: eventStream(
				`{"Action":"start","Package":"example/one"}`,
				`not JSON`,
			),
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			summary, err := validateSuiteResult(strings.NewReader(test.input), false)
			if (err != nil) != test.wantError {
				t.Fatalf("validateSuiteResult() error = %v, wantError %t", err, test.wantError)
			}
			if test.wantErrorText != "" && !strings.Contains(err.Error(), test.wantErrorText) {
				t.Fatalf("validateSuiteResult() error = %v, want %q", err, test.wantErrorText)
			}
			// Benchmark mode keeps every test rule and also needs a benchmark result.
			_, benchmarkErr := validateSuiteResult(strings.NewReader(test.input), true)
			wantBenchmarkText := test.wantErrorText
			if !test.wantError {
				wantBenchmarkText = "zero benchmark results"
			}
			if benchmarkErr == nil || !strings.Contains(benchmarkErr.Error(), wantBenchmarkText) {
				t.Fatalf("validateSuiteResult(benchmarks) error = %v, want %q", benchmarkErr, wantBenchmarkText)
			}
			if test.wantError {
				return
			}
			if summary.Packages != test.wantPackages || summary.Tests != test.wantTests {
				t.Fatalf("validateSuiteResult() = %#v, want packages=%d tests=%d", summary, test.wantPackages, test.wantTests)
			}
		})
	}
}

// capturedBenchmarkJSON is the go test -json output of make test-adapter
// GO_TEST_PKGS=./internal/jsonnumber with -bench '^BenchmarkCanonicalizeTokens$'
// -benchmem -benchtime=1x on Go 1.25. It is lines 2-72 of
// wp48-evidence/logs/primary-benchmark-runner-probe.log.
const capturedBenchmarkJSON = `{"Time":"2026-09-27T05:07:37.000545-05:00","Action":"start","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber"}
{"Time":"2026-09-27T05:07:37.176368-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText"}
{"Time":"2026-09-27T05:07:37.176535-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText\n"}
{"Time":"2026-09-27T05:07:37.176823-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/5.0"}
{"Time":"2026-09-27T05:07:37.176839-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/5.0","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText/5.0\n"}
{"Time":"2026-09-27T05:07:37.176849-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/5.0","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText/5.0 (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.176851-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/5.0","Elapsed":0}
{"Time":"2026-09-27T05:07:37.176855-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/-0.0"}
{"Time":"2026-09-27T05:07:37.176856-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/-0.0","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText/-0.0\n"}
{"Time":"2026-09-27T05:07:37.176857-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/-0.0","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText/-0.0 (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.176864-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/-0.0","Elapsed":0}
{"Time":"2026-09-27T05:07:37.176872-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/0.000001"}
{"Time":"2026-09-27T05:07:37.176873-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/0.000001","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText/0.000001\n"}
{"Time":"2026-09-27T05:07:37.176875-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/0.000001","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText/0.000001 (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.176876-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/0.000001","Elapsed":0}
{"Time":"2026-09-27T05:07:37.176887-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/0.0000015"}
{"Time":"2026-09-27T05:07:37.176893-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/0.0000015","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText/0.0000015\n"}
{"Time":"2026-09-27T05:07:37.176895-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/0.0000015","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText/0.0000015 (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.176896-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/0.0000015","Elapsed":0}
{"Time":"2026-09-27T05:07:37.176898-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/18446744073709552000.0"}
{"Time":"2026-09-27T05:07:37.1769-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/18446744073709552000.0","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText/18446744073709552000.0\n"}
{"Time":"2026-09-27T05:07:37.176902-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/18446744073709552000.0","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText/18446744073709552000.0 (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.176906-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/18446744073709552000.0","Elapsed":0}
{"Time":"2026-09-27T05:07:37.176911-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e20"}
{"Time":"2026-09-27T05:07:37.176912-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e20","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText/1e20\n"}
{"Time":"2026-09-27T05:07:37.176913-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e20","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText/1e20 (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.176915-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e20","Elapsed":0}
{"Time":"2026-09-27T05:07:37.176916-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e-7"}
{"Time":"2026-09-27T05:07:37.176917-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e-7","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText/1e-7\n"}
{"Time":"2026-09-27T05:07:37.176918-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e-7","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText/1e-7 (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.17692-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e-7","Elapsed":0}
{"Time":"2026-09-27T05:07:37.176925-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1.5"}
{"Time":"2026-09-27T05:07:37.176926-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1.5","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText/1.5\n"}
{"Time":"2026-09-27T05:07:37.176929-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1.5","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText/1.5 (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.17693-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1.5","Elapsed":0}
{"Time":"2026-09-27T05:07:37.176931-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e21"}
{"Time":"2026-09-27T05:07:37.176932-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e21","Output":"=== RUN   TestCanonicalWritesSharedFloatWireText/1e21\n"}
{"Time":"2026-09-27T05:07:37.176934-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e21","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText/1e21 (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.176936-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText/1e21","Elapsed":0}
{"Time":"2026-09-27T05:07:37.176938-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText","Output":"--- PASS: TestCanonicalWritesSharedFloatWireText (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.176939-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalWritesSharedFloatWireText","Elapsed":0}
{"Time":"2026-09-27T05:07:37.17694-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalRejectsNonFiniteText"}
{"Time":"2026-09-27T05:07:37.176941-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalRejectsNonFiniteText","Output":"=== RUN   TestCanonicalRejectsNonFiniteText\n"}
{"Time":"2026-09-27T05:07:37.176943-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalRejectsNonFiniteText","Output":"--- PASS: TestCanonicalRejectsNonFiniteText (0.00s)\n"}
{"Time":"2026-09-27T05:07:37.176944-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"TestCanonicalRejectsNonFiniteText","Elapsed":0}
{"Time":"2026-09-27T05:07:37.183569-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Output":"goos: darwin\n"}
{"Time":"2026-09-27T05:07:37.183595-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Output":"goarch: arm64\n"}
{"Time":"2026-09-27T05:07:37.183597-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Output":"pkg: github.com/trainstar/synchro/api/go/internal/jsonnumber\n"}
{"Time":"2026-09-27T05:07:37.183598-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Output":"cpu: Apple M2\n"}
{"Time":"2026-09-27T05:07:37.183603-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens"}
{"Time":"2026-09-27T05:07:37.183605-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens","Output":"=== RUN   BenchmarkCanonicalizeTokens\n"}
{"Time":"2026-09-27T05:07:37.183606-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens","Output":"BenchmarkCanonicalizeTokens\n"}
{"Time":"2026-09-27T05:07:37.185814-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/small/canonical"}
{"Time":"2026-09-27T05:07:37.185835-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/small/canonical","Output":"=== RUN   BenchmarkCanonicalizeTokens/small/canonical\n"}
{"Time":"2026-09-27T05:07:37.185841-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/small/canonical","Output":"BenchmarkCanonicalizeTokens/small/canonical\n"}
{"Time":"2026-09-27T05:07:37.190103-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/small/canonical","Output":"BenchmarkCanonicalizeTokens/small/canonical-8         \t       1\t     40208 ns/op\t  17.41 MB/s\t   10152 B/op\t     543 allocs/op\n"}
{"Time":"2026-09-27T05:07:37.19016-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/small/postgres"}
{"Time":"2026-09-27T05:07:37.190164-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/small/postgres","Output":"=== RUN   BenchmarkCanonicalizeTokens/small/postgres\n"}
{"Time":"2026-09-27T05:07:37.190166-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/small/postgres","Output":"BenchmarkCanonicalizeTokens/small/postgres\n"}
{"Time":"2026-09-27T05:07:37.190777-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/small/postgres","Output":"BenchmarkCanonicalizeTokens/small/postgres-8          \t       1\t     36417 ns/op\t  21.97 MB/s\t   12744 B/op\t     549 allocs/op\n"}
{"Time":"2026-09-27T05:07:37.198494-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/large/canonical"}
{"Time":"2026-09-27T05:07:37.19853-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/large/canonical","Output":"=== RUN   BenchmarkCanonicalizeTokens/large/canonical\n"}
{"Time":"2026-09-27T05:07:37.198595-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/large/canonical","Output":"BenchmarkCanonicalizeTokens/large/canonical\n"}
{"Time":"2026-09-27T05:07:37.300638-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/large/canonical","Output":"BenchmarkCanonicalizeTokens/large/canonical-8         \t       1\t  43416584 ns/op\t  32.05 MB/s\t18299984 B/op\t 1081397 allocs/op\n"}
{"Time":"2026-09-27T05:07:37.303881-05:00","Action":"run","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/large/postgres"}
{"Time":"2026-09-27T05:07:37.30389-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/large/postgres","Output":"=== RUN   BenchmarkCanonicalizeTokens/large/postgres\n"}
{"Time":"2026-09-27T05:07:37.303893-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/large/postgres","Output":"BenchmarkCanonicalizeTokens/large/postgres\n"}
{"Time":"2026-09-27T05:07:37.394695-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Test":"BenchmarkCanonicalizeTokens/large/postgres","Output":"BenchmarkCanonicalizeTokens/large/postgres-8          \t       1\t  45851875 ns/op\t  34.82 MB/s\t20217408 B/op\t 1089576 allocs/op\n"}
{"Time":"2026-09-27T05:07:37.394776-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Output":"PASS\n"}
{"Time":"2026-09-27T05:07:37.395589-05:00","Action":"output","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Output":"ok  \tgithub.com/trainstar/synchro/api/go/internal/jsonnumber\t0.395s\n"}
{"Time":"2026-09-27T05:07:37.395603-05:00","Action":"pass","Package":"github.com/trainstar/synchro/api/go/internal/jsonnumber","Elapsed":0.395}`

func TestValidateSuiteResultNeedsBenchmarkModeForCapturedBenchmarks(t *testing.T) {
	if _, err := validateSuiteResult(strings.NewReader(capturedBenchmarkJSON), false); err == nil ||
		!strings.Contains(err.Error(), "unscoped event") {
		t.Fatalf("validateSuiteResult() error = %v, want the unscoped benchmark metadata failure", err)
	}
	summary, err := validateSuiteResult(strings.NewReader(capturedBenchmarkJSON), true)
	if err != nil || summary != (suiteSummary{Packages: 1, Tests: 11, Benchmarks: 4, Results: 4}) {
		t.Fatalf("validateSuiteResult(benchmarks) = %#v, %v", summary, err)
	}
}

func TestValidateSuiteResultBenchmarks(t *testing.T) {
	benchmarkStream := func(body ...string) string {
		events := []string{
			`{"Action":"start","Package":"example/bench"}`,
			`{"Action":"run","Package":"example/bench","Test":"TestUnit"}`,
			`{"Action":"pass","Package":"example/bench","Test":"TestUnit"}`,
			`{"Action":"output","Package":"example/bench","Output":"goos: linux\n"}`,
			`{"Action":"output","Package":"example/bench","Output":"goarch: amd64\n"}`,
			`{"Action":"output","Package":"example/bench","Output":"pkg: example/bench\n"}`,
			`{"Action":"output","Package":"example/bench","Output":"cpu: Test CPU\n"}`,
			`{"Action":"run","Package":"example/bench","Test":"BenchmarkX"}`,
			`{"Action":"output","Package":"example/bench","Test":"BenchmarkX","Output":"BenchmarkX\n"}`,
			`{"Action":"run","Package":"example/bench","Test":"BenchmarkX/a"}`,
			`{"Action":"output","Package":"example/bench","Test":"BenchmarkX/a","Output":"BenchmarkX/a\n"}`,
		}
		events = append(events, body...)
		return eventStream(append(events,
			`{"Action":"output","Package":"example/bench","Output":"PASS\n"}`,
			`{"Action":"output","Package":"example/bench","Output":"ok  \texample/bench\t0.010s\n"}`,
			`{"Action":"pass","Package":"example/bench"}`,
		)...)
	}
	result := func(test, output string) string {
		scope := ""
		if test != "" {
			scope = `,"Test":"` + test + `"`
		}
		return `{"Action":"output","Package":"example/bench"` + scope + `,"Output":"` + output + `\n"}`
	}
	measuredA := result("BenchmarkX/a", `BenchmarkX/a-8   \t      10\t     105.0 ns/op`)

	summary, err := validateSuiteResult(strings.NewReader(benchmarkStream(
		// -cpu=1,8 with b.Log in b. Go scopes only the first result of a
		// leaf to its run, and test2json can split a result after its name.
		result("BenchmarkX/a", `BenchmarkX/a     \t      10\t     105.0 ns/op\t      16 B/op\t       1 allocs/op`),
		result("", `BenchmarkX/a-8   \t      12\t      98.5 ns/op\t      16 B/op\t       1 allocs/op`),
		`{"Action":"run","Package":"example/bench","Test":"BenchmarkX/b"}`,
		result("BenchmarkX/b", `BenchmarkX/b`),
		`{"Action":"output","Package":"example/bench","Test":"BenchmarkX/b","Output":"BenchmarkX/b     \t"}`,
		result("BenchmarkX/b", `       5\t       210 ns/op`),
		result("BenchmarkX/b", `--- BENCH: BenchmarkX/b`),
		result("BenchmarkX/b", `    x_test.go:12: sized input`),
		`{"Action":"bench","Package":"example/bench","Test":"BenchmarkX/b"}`,
		result("", `BenchmarkX/b-8   \t       6\t       190 ns/op`),
		result("BenchmarkX/b-8", `--- BENCH: BenchmarkX/b-8`),
		result("BenchmarkX/b-8", `    x_test.go:12: sized input`),
		`{"Action":"bench","Package":"example/bench","Test":"BenchmarkX/b-8"}`,
	)), true)
	if err != nil || summary != (suiteSummary{Packages: 1, Tests: 1, Benchmarks: 2, Results: 4}) {
		t.Fatalf("validateSuiteResult() = %#v, %v", summary, err)
	}

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name: "unmeasured leaf beside a measured leaf",
			input: benchmarkStream(measuredA,
				`{"Action":"run","Package":"example/bench","Test":"BenchmarkX/b"}`,
				result("BenchmarkX/b", `BenchmarkX/b`),
			),
			want: "benchmark BenchmarkX/b in package example/bench reported no result",
		},
		{
			name: "zero matching benchmarks",
			input: eventStream(
				`{"Action":"start","Package":"example/bench"}`,
				`{"Action":"run","Package":"example/bench","Test":"TestUnit"}`,
				`{"Action":"pass","Package":"example/bench","Test":"TestUnit"}`,
				`{"Action":"output","Package":"example/bench","Output":"PASS\n"}`,
				`{"Action":"pass","Package":"example/bench"}`,
			),
			want: "zero benchmark results",
		},
		{
			// The package pass isolates the benchmark rule.
			name: "failed repetition reported with the processor suffix",
			input: benchmarkStream(measuredA,
				result("BenchmarkX/a-8", `--- FAIL: BenchmarkX/a-8`),
				result("BenchmarkX/a-8", `    x_test.go:9: bad input`),
				`{"Action":"fail","Package":"example/bench","Test":"BenchmarkX/a-8"}`,
			),
			want: "benchmark BenchmarkX/a in package example/bench failed",
		},
		{
			name: "skipped benchmark",
			input: benchmarkStream(
				result("BenchmarkX/a", `--- SKIP: BenchmarkX/a`),
				`{"Action":"skip","Package":"example/bench","Test":"BenchmarkX/a"}`,
			),
			want: "benchmark BenchmarkX/a in package example/bench skipped",
		},
		{
			name:  "result names another benchmark",
			input: benchmarkStream(result("BenchmarkX/a", `BenchmarkY-8   \t      10\t     105.0 ns/op`)),
			want:  "reported a result for BenchmarkY-8",
		},
		{
			name:  "package result names an inactive benchmark",
			input: benchmarkStream(measuredA, result("", `BenchmarkX-8   \t      10\t     105.0 ns/op`)),
			want:  "reported a result for BenchmarkX-8",
		},
		{
			name:  "processor suffix one",
			input: benchmarkStream(result("BenchmarkX/a", `BenchmarkX/a-1   \t      10\t     105.0 ns/op`)),
			want:  "reported a result for BenchmarkX/a-1",
		},
		{
			name:  "zero iterations",
			input: benchmarkStream(result("BenchmarkX/a", `BenchmarkX/a-8   \t       0\t     105.0 ns/op`)),
			want:  "malformed result",
		},
		{
			name:  "invalid iterations",
			input: benchmarkStream(result("BenchmarkX/a", `BenchmarkX/a-8   \t    many\t     105.0 ns/op`)),
			want:  "malformed result",
		},
		{
			name:  "no metric",
			input: benchmarkStream(result("BenchmarkX/a", `BenchmarkX/a-8   \t      10`)),
			want:  "malformed result",
		},
		{
			name:  "metric without unit separator",
			input: benchmarkStream(result("BenchmarkX/a", `BenchmarkX/a-8   \t      10\t     105.0ns/op`)),
			want:  "malformed result",
		},
		{
			name:  "not a number metric",
			input: benchmarkStream(result("BenchmarkX/a", `BenchmarkX/a-8   \t      10\t       NaN ns/op`)),
			want:  "malformed result",
		},
		{
			name:  "infinite metric",
			input: benchmarkStream(result("BenchmarkX/a", `BenchmarkX/a-8   \t      10\t      +Inf ns/op`)),
			want:  "malformed result",
		},
		{
			name: "incomplete split result",
			input: benchmarkStream(
				`{"Action":"output","Package":"example/bench","Test":"BenchmarkX/a","Output":"BenchmarkX/a-8   \t"}`,
				`{"Action":"run","Package":"example/bench","Test":"BenchmarkX/b"}`,
			),
			want: "incomplete benchmark result",
		},
		{
			name:  "foreign package output",
			input: benchmarkStream(measuredA, result("", `foreign output`)),
			want:  "unscoped event",
		},
		{
			name:  "unknown benchmark metadata",
			input: benchmarkStream(measuredA, result("", `goversion: go1.25`)),
			want:  "unscoped event",
		},
		{
			name:  "metadata after the first benchmark",
			input: benchmarkStream(measuredA, result("", `goos: linux`)),
			want:  "invalid benchmark metadata",
		},
		{
			name: "metadata for another package",
			input: eventStream(
				`{"Action":"start","Package":"example/bench"}`,
				`{"Action":"run","Package":"example/bench","Test":"TestUnit"}`,
				`{"Action":"pass","Package":"example/bench","Test":"TestUnit"}`,
				`{"Action":"output","Package":"example/bench","Output":"pkg: example/other\n"}`,
			),
			want: "invalid benchmark metadata",
		},
		{
			name:  "pass event for a benchmark",
			input: benchmarkStream(measuredA, `{"Action":"pass","Package":"example/bench","Test":"BenchmarkX/a"}`),
			want:  "invalid final event",
		},
		{
			name: "benchmarks without tests",
			input: eventStream(
				`{"Action":"start","Package":"example/bench"}`,
				`{"Action":"run","Package":"example/bench","Test":"BenchmarkX"}`,
				`{"Action":"output","Package":"example/bench","Test":"BenchmarkX","Output":"BenchmarkX-8   \t      10\t     105.0 ns/op\n"}`,
				`{"Action":"output","Package":"example/bench","Output":"PASS\n"}`,
				`{"Action":"pass","Package":"example/bench"}`,
			),
			want: "package example/bench executed zero tests",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := validateSuiteResult(strings.NewReader(test.input), true); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateSuiteResult() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRunSuiteBenchmarksReplaysCapturedOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "benchmark.json")
	if err := os.WriteFile(path, []byte(capturedBenchmarkJSON+"\n"), 0o600); err != nil {
		t.Fatalf("write captured benchmark output: %v", err)
	}
	tests := []struct {
		name     string
		args     []string
		wantCode int
		want     string
	}{
		{"benchmark mode", []string{"-benchmarks", "cat", path}, 0, "11 tests passed and 4 benchmarks reported 4 results in 1 packages"},
		{"default mode", []string{"cat", path}, 1, "unscoped event"},
		{"failed command", []string{"-benchmarks", "sh", "-c", `cat "$1"; exit 3`, "sh", path}, 1, "test command failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runSuite(test.args, &stdout, &stderr)
			if code != test.wantCode || !strings.Contains(stderr.String(), test.want) || stdout.String() != capturedBenchmarkJSON+"\n" {
				t.Fatalf("runSuite() = %d, stderr %q, want %d and %q", code, stderr.String(), test.wantCode, test.want)
			}
		})
	}
}
