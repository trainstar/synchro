package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type formatSummary struct {
	Suites int
	Tests  int
}

type junitSuite struct {
	XMLName  xml.Name    `xml:"testsuite"`
	Tests    string      `xml:"tests,attr"`
	Skipped  string      `xml:"skipped,attr"`
	Failures string      `xml:"failures,attr"`
	Errors   string      `xml:"errors,attr"`
	Cases    []junitCase `xml:"testcase"`
}

type junitCase struct {
	Skipped  []struct{} `xml:"skipped"`
	Failures []struct{} `xml:"failure"`
	Errors   []struct{} `xml:"error"`
}

type jestResult struct {
	FailedSuites       int         `json:"numFailedTestSuites"`
	FailedTests        int         `json:"numFailedTests"`
	PassedSuites       int         `json:"numPassedTestSuites"`
	PassedTests        int         `json:"numPassedTests"`
	PendingSuites      int         `json:"numPendingTestSuites"`
	PendingTests       int         `json:"numPendingTests"`
	RuntimeErrorSuites int         `json:"numRuntimeErrorTestSuites"`
	TodoTests          int         `json:"numTodoTests"`
	TotalSuites        int         `json:"numTotalTestSuites"`
	TotalTests         int         `json:"numTotalTests"`
	Success            bool        `json:"success"`
	Suites             []jestSuite `json:"testResults"`
}

type jestSuite struct {
	Assertions []jestAssertion `json:"assertionResults"`
	Status     string          `json:"status"`
}

type jestAssertion struct {
	Status string `json:"status"`
}

type xcResultSummary struct {
	Devices          []xcResultDevice  `json:"devicesAndConfigurations"`
	Environment      string            `json:"environmentDescription"`
	ExpectedFailures int               `json:"expectedFailures"`
	FailedTests      int               `json:"failedTests"`
	FinishTime       float64           `json:"finishTime"`
	PassedTests      int               `json:"passedTests"`
	Result           string            `json:"result"`
	SkippedTests     int               `json:"skippedTests"`
	StartTime        float64           `json:"startTime"`
	Statistics       []json.RawMessage `json:"statistics"`
	TestFailures     []json.RawMessage `json:"testFailures"`
	Title            string            `json:"title"`
	TopInsights      []json.RawMessage `json:"topInsights"`
	TotalTestCount   int               `json:"totalTestCount"`
}

type xcResultDevice struct {
	Device            json.RawMessage `json:"device"`
	ExpectedFailures  int             `json:"expectedFailures"`
	FailedTests       int             `json:"failedTests"`
	PassedTests       int             `json:"passedTests"`
	SkippedTests      int             `json:"skippedTests"`
	TestConfiguration json.RawMessage `json:"testPlanConfiguration"`
}

var rustSummaryPattern = regexp.MustCompile(`^test result: (ok|FAILED)\. ([0-9]+) passed; ([0-9]+) failed; ([0-9]+) ignored; ([0-9]+) measured; ([0-9]+) filtered out; finished in .+$`)

func runJUnit(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("junit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("path", "", "directory containing JUnit XML results")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *path == "" || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "testresult junit requires one result path")
		return 2
	}

	summary, err := validateJUnitPath(*path)
	if err != nil {
		fmt.Fprintf(stderr, "testresult: %v\n", err)
		return 1
	}
	fmt.Fprintf(stderr, "testresult: %d tests passed in %d JUnit suites\n", summary.Tests, summary.Suites)
	return 0
}

func validateJUnitPath(path string) (formatSummary, error) {
	var files []string
	err := filepath.WalkDir(path, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ".xml") {
			files = append(files, current)
		}
		return nil
	})
	if err != nil {
		return formatSummary{}, fmt.Errorf("read JUnit results: %w", err)
	}
	if len(files) == 0 {
		return formatSummary{}, errors.New("JUnit results contain no suites")
	}
	sort.Strings(files)

	summary := formatSummary{}
	for _, file := range files {
		suite, err := loadJUnitSuite(file)
		if err != nil {
			return formatSummary{}, err
		}
		tests, err := parseResultCount("tests", suite.Tests)
		if err != nil {
			return formatSummary{}, fmt.Errorf("JUnit suite %s: %w", file, err)
		}
		skipped, err := parseResultCount("skipped", suite.Skipped)
		if err != nil {
			return formatSummary{}, fmt.Errorf("JUnit suite %s: %w", file, err)
		}
		failures, err := parseResultCount("failures", suite.Failures)
		if err != nil {
			return formatSummary{}, fmt.Errorf("JUnit suite %s: %w", file, err)
		}
		errorsCount, err := parseResultCount("errors", suite.Errors)
		if err != nil {
			return formatSummary{}, fmt.Errorf("JUnit suite %s: %w", file, err)
		}
		if tests != len(suite.Cases) {
			return formatSummary{}, fmt.Errorf("JUnit suite %s test count does not match its cases", file)
		}
		caseSkipped, caseFailures, caseErrors := 0, 0, 0
		for _, testCase := range suite.Cases {
			caseSkipped += len(testCase.Skipped)
			caseFailures += len(testCase.Failures)
			caseErrors += len(testCase.Errors)
		}
		if skipped != caseSkipped || failures != caseFailures || errorsCount != caseErrors {
			return formatSummary{}, fmt.Errorf("JUnit suite %s summary does not match its cases", file)
		}
		if skipped != 0 || failures != 0 || errorsCount != 0 {
			return formatSummary{}, fmt.Errorf("JUnit suite %s has skipped or failed tests", file)
		}
		summary.Suites++
		summary.Tests += tests
	}
	if summary.Tests == 0 {
		return formatSummary{}, errors.New("JUnit results contain zero tests")
	}
	return summary, nil
}

func loadJUnitSuite(path string) (junitSuite, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return junitSuite{}, fmt.Errorf("read JUnit suite %s: %w", path, err)
	}
	var suite junitSuite
	if err := xml.Unmarshal(data, &suite); err != nil {
		return junitSuite{}, fmt.Errorf("decode JUnit suite %s: %w", path, err)
	}
	if suite.XMLName.Local != "testsuite" {
		return junitSuite{}, fmt.Errorf("JUnit file %s has an invalid root", path)
	}
	return suite, nil
}

func parseResultCount(name, value string) (int, error) {
	if value == "" {
		return 0, fmt.Errorf("result field %s is missing", name)
	}
	count, err := strconv.Atoi(value)
	if err != nil || count < 0 {
		return 0, fmt.Errorf("result field %s is invalid", name)
	}
	return count, nil
}

func runJest(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("jest", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("path", "", "Jest JSON result path")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *path == "" || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "testresult jest requires one result path")
		return 2
	}

	file, err := os.Open(*path)
	if err != nil {
		fmt.Fprintf(stderr, "testresult: read Jest results: %v\n", err)
		return 1
	}
	defer file.Close()
	result, err := validateJestResult(file)
	if err != nil {
		fmt.Fprintf(stderr, "testresult: %v\n", err)
		return 1
	}
	fmt.Fprintf(stderr, "testresult: %d tests passed in %d Jest suites\n", result.Tests, result.Suites)
	return 0
}

func validateJestResult(input io.Reader) (formatSummary, error) {
	decoder := json.NewDecoder(input)
	var result jestResult
	if err := decoder.Decode(&result); err != nil {
		return formatSummary{}, errors.New("Jest results are malformed")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return formatSummary{}, errors.New("Jest results contain trailing data")
	}
	if result.TotalSuites <= 0 || result.TotalTests <= 0 {
		return formatSummary{}, errors.New("Jest results contain zero tests")
	}
	if !result.Success || result.FailedSuites != 0 || result.FailedTests != 0 || result.PendingSuites != 0 || result.PendingTests != 0 || result.RuntimeErrorSuites != 0 || result.TodoTests != 0 {
		return formatSummary{}, errors.New("Jest results contain skipped or failed tests")
	}
	if len(result.Suites) != result.TotalSuites || result.PassedSuites != result.TotalSuites || result.PassedTests != result.TotalTests {
		return formatSummary{}, errors.New("Jest summary does not match its suites")
	}
	assertionCount := 0
	for _, suite := range result.Suites {
		if suite.Status != "passed" {
			return formatSummary{}, errors.New("Jest suite contains skipped or failed tests")
		}
		for _, assertion := range suite.Assertions {
			if assertion.Status != "passed" {
				return formatSummary{}, errors.New("Jest assertion is not passed")
			}
		}
		assertionCount += len(suite.Assertions)
	}
	if assertionCount != result.TotalTests {
		return formatSummary{}, errors.New("Jest test count does not match its assertions")
	}
	return formatSummary{Suites: result.TotalSuites, Tests: result.TotalTests}, nil
}

func runXCResult(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("xcresult", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("path", "", "xcresult bundle path")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *path == "" || flags.NArg() != 0 {
		fmt.Fprintln(stderr, "testresult xcresult requires one result path")
		return 2
	}

	command := exec.Command("xcrun", "xcresulttool", "get", "test-results", "summary", "--path", *path, "--format", "json")
	output, err := command.Output()
	if err != nil {
		fmt.Fprintf(stderr, "testresult: read xcresult summary: %v\n", err)
		return 1
	}
	summary, err := validateXCResult(bytes.NewReader(output))
	if err != nil {
		fmt.Fprintf(stderr, "testresult: %v\n", err)
		return 1
	}
	fmt.Fprintf(stderr, "testresult: %d tests passed in xcresult\n", summary.Tests)
	return 0
}

func validateXCResult(input io.Reader) (formatSummary, error) {
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	var result xcResultSummary
	if err := decoder.Decode(&result); err != nil {
		return formatSummary{}, errors.New("xcresult summary is malformed")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return formatSummary{}, err
	}
	if result.TotalTestCount <= 0 {
		return formatSummary{}, errors.New("xcresult contains zero tests")
	}
	if result.Result != "Passed" || result.PassedTests != result.TotalTestCount || result.FailedTests != 0 || result.SkippedTests != 0 || result.ExpectedFailures != 0 {
		return formatSummary{}, errors.New("xcresult contains skipped or failed tests")
	}
	if len(result.Devices) == 0 {
		return formatSummary{}, errors.New("xcresult contains no device result")
	}
	devicePassed := 0
	for _, device := range result.Devices {
		if device.FailedTests != 0 || device.SkippedTests != 0 || device.ExpectedFailures != 0 {
			return formatSummary{}, errors.New("xcresult device contains skipped or failed tests")
		}
		devicePassed += device.PassedTests
	}
	if devicePassed != result.TotalTestCount {
		return formatSummary{}, errors.New("xcresult device test count does not match its summary")
	}
	return formatSummary{Suites: len(result.Devices), Tests: result.TotalTestCount}, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("xcresult summary contains trailing data")
	}
	return nil
}

func runRust(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rust", flag.ContinueOnError)
	flags.SetOutput(stderr)
	directory := flags.String("dir", "", "working directory for the test command")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	commandArgs := flags.Args()
	if len(commandArgs) == 0 {
		fmt.Fprintln(stderr, "testresult rust requires a test command")
		return 2
	}

	command := exec.Command(commandArgs[0], commandArgs[1:]...)
	command.Dir = *directory
	var commandStdout, commandStderr bytes.Buffer
	command.Stdout = io.MultiWriter(stdout, &commandStdout)
	command.Stderr = io.MultiWriter(stderr, &commandStderr)
	commandErr := command.Run()
	summary, resultErr := validateRustResult(io.MultiReader(&commandStdout, &commandStderr))
	if commandErr != nil {
		fmt.Fprintf(stderr, "testresult: test command failed: %v\n", commandErr)
	}
	if resultErr != nil {
		fmt.Fprintf(stderr, "testresult: %v\n", resultErr)
	}
	if commandErr != nil || resultErr != nil {
		return 1
	}
	fmt.Fprintf(stderr, "testresult: %d Rust tests passed in %d harnesses\n", summary.Tests, summary.Suites)
	return 0
}

func validateRustResult(input io.Reader) (formatSummary, error) {
	scanner := bufio.NewScanner(input)
	summary := formatSummary{}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "test result:") {
			continue
		}
		matches := rustSummaryPattern.FindStringSubmatch(line)
		if matches == nil {
			return formatSummary{}, errors.New("Rust test summary is malformed")
		}
		counts := make([]int, 5)
		for index := range counts {
			count, err := strconv.Atoi(matches[index+2])
			if err != nil {
				return formatSummary{}, errors.New("Rust test summary is malformed")
			}
			counts[index] = count
		}
		if matches[1] != "ok" || counts[1] != 0 || counts[2] != 0 || counts[3] != 0 || counts[4] != 0 {
			return formatSummary{}, errors.New("Rust test summary contains ignored, filtered, or failed tests")
		}
		summary.Suites++
		summary.Tests += counts[0]
	}
	if err := scanner.Err(); err != nil {
		return formatSummary{}, fmt.Errorf("read Rust test output: %w", err)
	}
	if summary.Suites == 0 {
		return formatSummary{}, errors.New("Rust test output contains no harness summary")
	}
	if summary.Tests == 0 {
		return formatSummary{}, errors.New("Rust test output contains zero tests")
	}
	return summary, nil
}
