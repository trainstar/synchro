package reactnative

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateDetoxWarmConnectResultRequiresSelectedTestIdentity(t *testing.T) {
	expectedPath := filepath.Join(t.TempDir(), "e2e", "warm-connect.test.ts")
	expectedName := "executes the warm-connect coordinator sequence"
	valid := `{"success":true,"numFailedTestSuites":0,"numFailedTests":0,"numPassedTestSuites":1,"numPassedTests":1,"numPendingTestSuites":0,"numPendingTests":0,"numRuntimeErrorTestSuites":0,"numTodoTests":0,"numTotalTestSuites":1,"numTotalTests":1,"testResults":[{"name":"` + expectedPath + `","status":"passed","assertionResults":[{"fullName":"` + expectedName + `","status":"passed"}]}]}`
	writeResult := func(content string) string {
		path := filepath.Join(t.TempDir(), "result.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write Jest result: %v", err)
		}
		return path
	}
	if err := validateDetoxWarmConnectResult(writeResult(valid), expectedPath, expectedName); err != nil {
		t.Fatalf("valid selected Jest result was rejected: %v", err)
	}
	tests := []struct {
		name    string
		content string
	}{
		{"wrong test path", strings.Replace(valid, expectedPath, filepath.Join(filepath.Dir(expectedPath), "other.test.ts"), 1)},
		{"wrong assertion", strings.Replace(valid, expectedName, "different test", 1)},
		{"duplicate member", strings.Replace(valid, `"success":true`, `"success":true,"success":true`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateDetoxWarmConnectResult(writeResult(test.content), expectedPath, expectedName); err == nil {
				t.Fatal("unselected or malformed Jest result was accepted")
			}
		})
	}
}
