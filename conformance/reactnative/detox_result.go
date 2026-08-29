package reactnative

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const maximumDetoxResultBytes = 1 << 20

type detoxJestResult struct {
	Success                bool `json:"success"`
	FailedSuites           int  `json:"numFailedTestSuites"`
	FailedTests            int  `json:"numFailedTests"`
	PassedSuites           int  `json:"numPassedTestSuites"`
	PassedTests            int  `json:"numPassedTests"`
	PendingSuites          int  `json:"numPendingTestSuites"`
	PendingTests           int  `json:"numPendingTests"`
	RuntimeErrorSuites     int  `json:"numRuntimeErrorTestSuites"`
	TodoTests              int  `json:"numTodoTests"`
	TotalSuites            int  `json:"numTotalTestSuites"`
	TotalTests             int  `json:"numTotalTests"`
	TestExecutionSummaries []struct {
		Name       string `json:"name"`
		Status     string `json:"status"`
		Assertions []struct {
			FullName string `json:"fullName"`
			Status   string `json:"status"`
		} `json:"assertionResults"`
	} `json:"testResults"`
}

func validateDetoxWarmConnectResult(path, expectedTestPath, expectedFullName string) error {
	return validateDetoxSingleTestResult(path, expectedTestPath, expectedFullName, "warm-connect")
}

func validateDetoxSteadyPullResult(path, expectedTestPath, expectedFullName string) error {
	return validateDetoxSingleTestResult(path, expectedTestPath, expectedFullName, "steady-pull")
}

func validateDetoxSingleTestResult(path, expectedTestPath, expectedFullName, label string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read Jest result: %w", err)
	}
	if len(data) == 0 || len(data) > maximumDetoxResultBytes || jsonstrict.ValidateValue(data) != nil {
		return fmt.Errorf("Jest result is invalid")
	}
	var result detoxJestResult
	if err := json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("decode Jest result: %w", err)
	}
	if !result.Success || result.TotalSuites != 1 || result.PassedSuites != 1 || result.TotalTests != 1 || result.PassedTests != 1 ||
		result.FailedSuites != 0 || result.FailedTests != 0 || result.PendingSuites != 0 || result.PendingTests != 0 ||
		result.RuntimeErrorSuites != 0 || result.TodoTests != 0 || len(result.TestExecutionSummaries) != 1 ||
		result.TestExecutionSummaries[0].Status != "passed" || filepath.Clean(result.TestExecutionSummaries[0].Name) != filepath.Clean(expectedTestPath) ||
		len(result.TestExecutionSummaries[0].Assertions) != 1 || result.TestExecutionSummaries[0].Assertions[0].Status != "passed" ||
		result.TestExecutionSummaries[0].Assertions[0].FullName != expectedFullName {
		return fmt.Errorf("Jest result does not contain the selected passed %s test", label)
	}
	return nil
}
