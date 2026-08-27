package main

import (
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
	summary, err := validateSuiteResult(strings.NewReader(capturedParallelJSON))
	if err != nil || summary.Tests != 3 {
		t.Fatalf("validateSuiteResult() = %#v, %v", summary, err)
	}
}

func TestValidateSuiteResultRejectsUnscopedForeignOutput(t *testing.T) {
	input := eventStream(
		`{"Action":"start","Package":"example/one"}`,
		`{"Action":"output","Package":"example/one","Output":"foreign output\n"}`,
	)
	if _, err := validateSuiteResult(strings.NewReader(input)); err == nil {
		t.Fatal("validateSuiteResult() accepted unscoped foreign output")
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
			summary, err := validateSuiteResult(strings.NewReader(test.input))
			if (err != nil) != test.wantError {
				t.Fatalf("validateSuiteResult() error = %v, wantError %t", err, test.wantError)
			}
			if test.wantErrorText != "" && !strings.Contains(err.Error(), test.wantErrorText) {
				t.Fatalf("validateSuiteResult() error = %v, want %q", err, test.wantErrorText)
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
