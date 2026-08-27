package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

type suiteSummary struct {
	Packages int
	Tests    int
}

type suitePackageState struct {
	started     bool
	final       string
	noTestFiles bool
	tests       map[string]*suiteTestState
}

type suiteTestState struct {
	running bool
	runs    int
	passes  int
	failed  bool
	skipped bool
}

func runSuite(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("suite", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", "", "working directory for the test command")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	commandArgs := flags.Args()
	if len(commandArgs) == 0 {
		fmt.Fprintln(stderr, "testresult suite requires a test command")
		return 2
	}

	output, commandErr := runTestCommand(commandArgs, *directory, stdout, stderr)
	summary, resultErr := validateSuiteResult(bytes.NewReader(output))
	if commandErr != nil {
		fmt.Fprintf(stderr, "testresult: test command failed: %v\n", commandErr)
	}
	if resultErr != nil {
		fmt.Fprintf(stderr, "testresult: %v\n", resultErr)
	}
	if commandErr != nil || resultErr != nil {
		return 1
	}

	fmt.Fprintf(stderr, "testresult: %d tests passed in %d packages\n", summary.Tests, summary.Packages)
	return 0
}

func runExact(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("exact", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", "", "working directory for the test command")
	target := flags.String("test", "", "exact top-level test name")
	expected := flags.String("expect", string(resultTargetPass), "required exact-test result")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	commandArgs := flags.Args()
	if !validTargetName(*target) || len(commandArgs) == 0 {
		fmt.Fprintln(stderr, "testresult exact requires one target and a test command")
		return 2
	}
	if *expected != string(resultTargetPass) && *expected != string(resultTargetSemanticTestFailure) {
		fmt.Fprintln(stderr, "testresult exact requires a supported expected result")
		return 2
	}

	output, commandErr := runTestCommand(commandArgs, *directory, stdout, stderr)
	result := classifyTestResult(bytes.NewReader(output), *target)
	fmt.Fprintf(stderr, "testresult: %s\n", result)
	if *expected == string(resultTargetPass) {
		if commandErr == nil && result == resultTargetPass {
			return 0
		}
	} else if commandErr != nil && result == resultTargetSemanticTestFailure {
		return 0
	}
	if commandErr != nil {
		fmt.Fprintf(stderr, "testresult: test command failed: %v\n", commandErr)
	}
	return 1
}

func runTestCommand(args []string, directory string, stdout, stderr io.Writer) ([]byte, error) {
	command := exec.Command(args[0], args[1:]...)
	command.Dir = directory
	var output bytes.Buffer
	command.Stdout = io.MultiWriter(stdout, &output)
	command.Stderr = stderr
	err := command.Run()
	return output.Bytes(), err
}

func validateSuiteResult(input io.Reader) (suiteSummary, error) {
	if input == nil {
		return suiteSummary{}, errors.New("test output is missing")
	}

	decoder := json.NewDecoder(input)
	packages := make(map[string]*suitePackageState)
	eventCount := 0
	for {
		var event testEvent
		err := decoder.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return suiteSummary{}, errors.New("test output is malformed")
		}
		eventCount++
		if err := acceptSuiteEvent(packages, event); err != nil {
			return suiteSummary{}, err
		}
	}
	if eventCount == 0 {
		return suiteSummary{}, errors.New("test output is empty")
	}

	summary := suiteSummary{Packages: len(packages)}
	for packageName, state := range packages {
		if !state.started || state.final == "" {
			return suiteSummary{}, fmt.Errorf("package %s did not finish", packageName)
		}
		if state.noTestFiles {
			return suiteSummary{}, fmt.Errorf("package %s contains no test files", packageName)
		}
		if state.final == "fail" {
			return suiteSummary{}, fmt.Errorf("package %s failed", packageName)
		}
		if state.final == "skip" {
			return suiteSummary{}, fmt.Errorf("package %s skipped", packageName)
		}
		if len(state.tests) == 0 {
			return suiteSummary{}, fmt.Errorf("package %s executed zero tests", packageName)
		}
		for testName, test := range state.tests {
			switch {
			case test.skipped:
				return suiteSummary{}, fmt.Errorf("test %s in package %s skipped", testName, packageName)
			case test.failed:
				return suiteSummary{}, fmt.Errorf("test %s in package %s failed", testName, packageName)
			case test.running || test.runs == 0 || test.passes != test.runs:
				return suiteSummary{}, fmt.Errorf("test %s in package %s did not finish", testName, packageName)
			default:
				summary.Tests += test.passes
			}
		}
	}
	if summary.Tests == 0 {
		return suiteSummary{}, errors.New("test command executed zero tests")
	}
	return summary, nil
}

func acceptSuiteEvent(packages map[string]*suitePackageState, event testEvent) error {
	if event.Package == "" || event.Action == "" {
		return errors.New("test output contains an unscoped event")
	}
	state, exists := packages[event.Package]
	if !exists {
		state = &suitePackageState{tests: make(map[string]*suiteTestState)}
		packages[event.Package] = state
	}

	if event.Action == "start" {
		if event.Test != "" || state.started || state.final != "" {
			return fmt.Errorf("package %s has an invalid start event", event.Package)
		}
		state.started = true
		return nil
	}
	if !state.started || state.final != "" {
		return fmt.Errorf("package %s has an event outside its run", event.Package)
	}

	switch event.Action {
	case "run":
		if event.Test == "" {
			return fmt.Errorf("package %s has an unnamed test", event.Package)
		}
		test := state.tests[event.Test]
		if test == nil {
			test = &suiteTestState{}
			state.tests[event.Test] = test
		}
		if test.running {
			return fmt.Errorf("test %s in package %s started before its prior run finished", event.Test, event.Package)
		}
		test.running = true
		test.runs++
	case "pause", "cont", "bench":
		if !suiteTestRunning(state, event.Test) {
			return fmt.Errorf("test %s in package %s has an invalid %s event", event.Test, event.Package, event.Action)
		}
	case "output":
		if event.Test == "" {
			if testName, ok := suiteSubtestSummary(event.Output); ok {
				event.Test = testName
			} else if !isSuitePackageOutput(event.Output, event.Package) {
				return errors.New("test output contains an unscoped event")
			}
		}
		if event.Test != "" && !suiteTestRunning(state, event.Test) {
			return fmt.Errorf("test %s in package %s has output outside its run", event.Test, event.Package)
		}
		if event.Test == "" && bytes.Contains([]byte(event.Output), []byte("[no test files]")) {
			state.noTestFiles = true
		}
	case "pass", "fail", "skip":
		if event.Test == "" {
			for _, test := range state.tests {
				if test.running {
					return fmt.Errorf("package %s finished before its tests", event.Package)
				}
			}
			state.final = event.Action
			return nil
		}
		if !suiteTestRunning(state, event.Test) {
			return fmt.Errorf("test %s in package %s has an invalid final event", event.Test, event.Package)
		}
		test := state.tests[event.Test]
		test.running = false
		switch event.Action {
		case "pass":
			test.passes++
		case "fail":
			test.failed = true
		case "skip":
			test.skipped = true
		}
	default:
		return fmt.Errorf("package %s has unknown action %s", event.Package, event.Action)
	}
	return nil
}

func suiteSubtestSummary(output string) (string, bool) {
	line := strings.TrimSuffix(output, "\n")
	for _, prefix := range []string{"--- PASS: ", "--- FAIL: ", "--- SKIP: "} {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, prefix))
		if len(fields) == 2 && strings.HasPrefix(fields[1], "(") && strings.HasSuffix(fields[1], "s)") {
			return fields[0], true
		}
	}
	return "", false
}

func isSuitePackageOutput(output, packageName string) bool {
	if output == "PASS\n" || output == "FAIL\n" || strings.Contains(output, "testing: warning: no tests to run") || strings.Contains(output, "[no test files]") || strings.Contains(output, "[no tests to run]") {
		return true
	}
	line := strings.TrimSuffix(output, "\n")
	fields := strings.Split(line, "\t")
	if len(fields) != 3 || packageName == "" || fields[1] != packageName {
		return false
	}
	return (fields[0] == "FAIL" || fields[0] == "ok  ") && strings.HasSuffix(fields[2], "s")
}

func suiteTestRunning(state *suitePackageState, testName string) bool {
	test := state.tests[testName]
	return testName != "" && test != nil && test.running
}
