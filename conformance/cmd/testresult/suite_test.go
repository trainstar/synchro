package main

import (
	"strings"
	"testing"
)

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
