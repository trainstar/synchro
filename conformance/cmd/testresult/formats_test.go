package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateJUnitPath(t *testing.T) {
	tests := []struct {
		name      string
		contents  []string
		wantTests int
		wantError bool
	}{
		{
			name: "passing suites",
			contents: []string{
				`<testsuite tests="2" skipped="0" failures="0" errors="0"><testcase/><testcase/></testsuite>`,
				`<testsuite tests="1" skipped="0" failures="0" errors="0"><testcase/></testsuite>`,
			},
			wantTests: 3,
		},
		{
			name:      "zero tests",
			contents:  []string{`<testsuite tests="0" skipped="0" failures="0" errors="0"></testsuite>`},
			wantError: true,
		},
		{
			name:      "skipped test",
			contents:  []string{`<testsuite tests="1" skipped="1" failures="0" errors="0"><testcase><skipped/></testcase></testsuite>`},
			wantError: true,
		},
		{
			name:      "failed test",
			contents:  []string{`<testsuite tests="1" skipped="0" failures="1" errors="0"><testcase><failure/></testcase></testsuite>`},
			wantError: true,
		},
		{
			name:      "test error",
			contents:  []string{`<testsuite tests="1" skipped="0" failures="0" errors="1"><testcase><error/></testcase></testsuite>`},
			wantError: true,
		},
		{
			name:      "summary mismatch",
			contents:  []string{`<testsuite tests="2" skipped="0" failures="0" errors="0"><testcase/></testsuite>`},
			wantError: true,
		},
		{
			name:      "missing count",
			contents:  []string{`<testsuite skipped="0" failures="0" errors="0"></testsuite>`},
			wantError: true,
		},
	}

	for _, test := range tests {
		for _, wrapped := range []bool{false, true} {
			name := test.name
			if wrapped {
				name += " in container"
			}
			t.Run(name, func(t *testing.T) {
				directory := t.TempDir()
				for index, contents := range test.contents {
					if wrapped {
						contents = "<testsuites>" + contents + "</testsuites>"
					}
					path := filepath.Join(directory, string(rune('a'+index))+".xml")
					if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
						t.Fatalf("write JUnit fixture: %v", err)
					}
				}
				summary, err := validateJUnitPath(directory)
				if (err != nil) != test.wantError {
					t.Fatalf("validateJUnitPath() error = %v, wantError %t", err, test.wantError)
				}
				if !test.wantError && summary.Tests != test.wantTests {
					t.Fatalf("validateJUnitPath() tests = %d, want %d", summary.Tests, test.wantTests)
				}
			})
		}
	}
}

func TestValidateJUnitContainer(t *testing.T) {
	const passing = `<testsuite tests="1" skipped="0" failures="0" errors="0"><testcase/></testsuite>`
	tests := []struct {
		name      string
		contents  string
		wantError bool
	}{
		{"multiple suites", `<testsuites tests="2" skipped="0" failures="0" errors="0">` + passing + passing + `</testsuites>`, false},
		{"empty container", `<testsuites/>`, true},
		{"hidden nested container", `<testsuites>` + passing + `<testsuites><testsuite tests="1" skipped="0" failures="1" errors="0"><testcase><failure/></testcase></testsuite></testsuites></testsuites>`, true},
		{"hidden container in suite", `<testsuites><testsuite tests="1" skipped="0" failures="0" errors="0"><testcase/><testsuites><testsuite tests="1" skipped="0" failures="1" errors="0"><testcase><failure/></testcase></testsuite></testsuites></testsuite></testsuites>`, true},
		{"zero child tests", `<testsuites tests="0"><testsuite tests="0" skipped="0" failures="0" errors="0"/></testsuites>`, true},
		{"container test mismatch", `<testsuites tests="2">` + passing + `</testsuites>`, true},
		{"container failure mismatch", `<testsuites failures="1">` + passing + `</testsuites>`, true},
		{"container skipped mismatch", `<testsuites skipped="1">` + passing + `</testsuites>`, true},
		{"container error mismatch", `<testsuites errors="1">` + passing + `</testsuites>`, true},
		{"invalid container count", `<testsuites tests="invalid">` + passing + `</testsuites>`, true},
		{"empty container tests", `<testsuites tests="">` + passing + `</testsuites>`, true},
		{"empty container skipped", `<testsuites skipped="">` + passing + `</testsuites>`, true},
		{"empty container failures", `<testsuites failures="">` + passing + `</testsuites>`, true},
		{"empty container errors", `<testsuites errors="">` + passing + `</testsuites>`, true},
		{"missing child count", `<testsuites><testsuite skipped="0" failures="0" errors="0"><testcase/></testsuite></testsuites>`, true},
		{"direct container case", `<testsuites><testcase/>` + passing + `</testsuites>`, true},
		{"nested suite", `<testsuites><testsuite tests="1" skipped="0" failures="0" errors="0"><testcase/>` + passing + `</testsuite></testsuites>`, true},
		{"invalid root", `<results>` + passing + `</results>`, true},
		{"malformed XML", `<testsuites>` + passing, true},
		{"trailing document", `<testsuites>` + passing + `</testsuites>` + passing, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "results.xml")
			if err := os.WriteFile(path, []byte(test.contents), 0o600); err != nil {
				t.Fatal(err)
			}
			summary, err := validateJUnitPath(path)
			if (err != nil) != test.wantError {
				t.Fatalf("validateJUnitPath() error = %v, wantError %t", err, test.wantError)
			}
			if !test.wantError && summary != (formatSummary{Suites: 2, Tests: 2}) {
				t.Fatalf("unexpected summary: %+v", summary)
			}
		})
	}
}

func TestValidateJUnitLeadingData(t *testing.T) {
	const passing = `<testsuite tests="1" skipped="0" failures="0" errors="0"><testcase/></testsuite>`
	for _, wrapped := range []bool{false, true} {
		for _, prefix := range []string{`<?xml version="1.0"?><!-- report -->` + "\n", "not-xml"} {
			t.Run(fmt.Sprintf("wrapped=%t/prefix=%s", wrapped, prefix), func(t *testing.T) {
				contents := passing
				if wrapped {
					contents = "<testsuites>" + contents + "</testsuites>"
				}
				path := filepath.Join(t.TempDir(), "results.xml")
				if err := os.WriteFile(path, []byte(prefix+contents+"<!-- end -->\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				_, err := validateJUnitPath(path)
				if (err != nil) != (prefix == "not-xml") {
					t.Fatalf("unexpected validation result: %v", err)
				}
			})
		}
	}
}

func TestValidateJUnitPathRejectsMissingResults(t *testing.T) {
	if _, err := validateJUnitPath(t.TempDir()); err == nil {
		t.Fatal("validateJUnitPath() accepted a directory without results")
	}
}

func TestValidateJestResult(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantError bool
	}{
		{
			name:  "passing result",
			input: `{"numFailedTestSuites":0,"numFailedTests":0,"numPassedTestSuites":1,"numPassedTests":2,"numPendingTestSuites":0,"numPendingTests":0,"numRuntimeErrorTestSuites":0,"numTodoTests":0,"numTotalTestSuites":1,"numTotalTests":2,"success":true,"testResults":[{"assertionResults":[{"status":"passed"},{"status":"passed"}],"numFailingTests":0,"numPassingTests":2,"numPendingTests":0,"status":"passed"}]}`,
		},
		{
			name:      "zero tests",
			input:     `{"numFailedTestSuites":0,"numFailedTests":0,"numPassedTestSuites":0,"numPassedTests":0,"numPendingTestSuites":0,"numPendingTests":0,"numRuntimeErrorTestSuites":0,"numTodoTests":0,"numTotalTestSuites":0,"numTotalTests":0,"success":true,"testResults":[]}`,
			wantError: true,
		},
		{
			name:      "skipped test",
			input:     `{"numFailedTestSuites":0,"numFailedTests":0,"numPassedTestSuites":0,"numPassedTests":0,"numPendingTestSuites":1,"numPendingTests":1,"numRuntimeErrorTestSuites":0,"numTodoTests":0,"numTotalTestSuites":1,"numTotalTests":1,"success":true,"testResults":[{"assertionResults":[{"status":"pending"}],"numFailingTests":0,"numPassingTests":0,"numPendingTests":1,"status":"pending"}]}`,
			wantError: true,
		},
		{
			name:      "suite mismatch",
			input:     `{"numFailedTestSuites":0,"numFailedTests":0,"numPassedTestSuites":1,"numPassedTests":2,"numPendingTestSuites":0,"numPendingTests":0,"numRuntimeErrorTestSuites":0,"numTodoTests":0,"numTotalTestSuites":1,"numTotalTests":2,"success":true,"testResults":[{"assertionResults":[{"status":"passed"}],"numFailingTests":0,"numPassingTests":1,"numPendingTests":0,"status":"passed"}]}`,
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateJestResult(strings.NewReader(test.input))
			if (err != nil) != test.wantError {
				t.Fatalf("validateJestResult() error = %v, wantError %t", err, test.wantError)
			}
		})
	}
}

func TestValidateXCResult(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantError bool
	}{
		{
			name:  "passing result",
			input: `{"devicesAndConfigurations":[{"expectedFailures":0,"failedTests":0,"passedTests":2,"skippedTests":0}],"expectedFailures":0,"failedTests":0,"passedTests":2,"result":"Passed","skippedTests":0,"totalTestCount":2}`,
		},
		{
			name:  "passing result with empty runtime warnings",
			input: `{"devicesAndConfigurations":[{"expectedFailures":0,"failedTests":0,"passedTests":2,"skippedTests":0}],"expectedFailures":0,"failedTests":0,"passedTests":2,"result":"Passed","runtimeWarnings":[],"skippedTests":0,"totalTestCount":2}`,
		},
		{
			name:      "runtime warning",
			input:     `{"devicesAndConfigurations":[{"expectedFailures":0,"failedTests":0,"passedTests":2,"skippedTests":0}],"expectedFailures":0,"failedTests":0,"passedTests":2,"result":"Passed","runtimeWarnings":[{"message":"warning"}],"skippedTests":0,"totalTestCount":2}`,
			wantError: true,
		},
		{
			name:      "zero tests",
			input:     `{"devicesAndConfigurations":[{"expectedFailures":0,"failedTests":0,"passedTests":0,"skippedTests":0}],"expectedFailures":0,"failedTests":0,"passedTests":0,"result":"Passed","skippedTests":0,"totalTestCount":0}`,
			wantError: true,
		},
		{
			name:      "skipped test",
			input:     `{"devicesAndConfigurations":[{"expectedFailures":0,"failedTests":0,"passedTests":1,"skippedTests":1}],"expectedFailures":0,"failedTests":0,"passedTests":1,"result":"Passed","skippedTests":1,"totalTestCount":2}`,
			wantError: true,
		},
		{
			name:      "device mismatch",
			input:     `{"devicesAndConfigurations":[{"expectedFailures":0,"failedTests":0,"passedTests":1,"skippedTests":0}],"expectedFailures":0,"failedTests":0,"passedTests":2,"result":"Passed","skippedTests":0,"totalTestCount":2}`,
			wantError: true,
		},
		{
			name:      "unknown field",
			input:     `{"devicesAndConfigurations":[],"expectedFailures":0,"failedTests":0,"passedTests":1,"result":"Passed","skippedTests":0,"totalTestCount":1,"extra":true}`,
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateXCResult(strings.NewReader(test.input))
			if (err != nil) != test.wantError {
				t.Fatalf("validateXCResult() error = %v, wantError %t", err, test.wantError)
			}
		})
	}
}

func TestValidateRustResult(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantTests int
		wantError bool
	}{
		{
			name:      "passing harnesses",
			input:     "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n",
			wantTests: 2,
		},
		{
			name:      "zero tests",
			input:     "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n",
			wantError: true,
		},
		{
			name:      "ignored test",
			input:     "test result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.01s\n",
			wantError: true,
		},
		{
			name:      "filtered test",
			input:     "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 2 filtered out; finished in 0.01s\n",
			wantError: true,
		},
		{
			name:      "missing summary",
			input:     "Finished test profile\n",
			wantError: true,
		},
		{
			name:      "malformed summary",
			input:     "test result: ok\n",
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			summary, err := validateRustResult(strings.NewReader(test.input))
			if (err != nil) != test.wantError {
				t.Fatalf("validateRustResult() error = %v, wantError %t", err, test.wantError)
			}
			if !test.wantError && summary.Tests != test.wantTests {
				t.Fatalf("validateRustResult() tests = %d, want %d", summary.Tests, test.wantTests)
			}
		})
	}
}
