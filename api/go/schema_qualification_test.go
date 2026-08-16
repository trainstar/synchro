package synchroapi

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

var synchroFunctionCallPattern = regexp.MustCompile(
	`(?:[[:alnum:]_]+\.)?synchro_(?:contract_info|connect|pull|push|rebuild|schema_manifest|tables|portable_seed_manifest|portable_seed_scope)\s*\(`,
)

func TestProductionSQLQualifiesSynchroFunctions(t *testing.T) {
	moduleRoot := goModuleRoot(t)
	err := filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, call := range synchroFunctionCallPattern.FindAllString(string(source), -1) {
			if !strings.HasPrefix(call, "synchro.synchro_") {
				t.Errorf("%s contains an unqualified Synchro function call %q", path, call)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan production Go files: %v", err)
	}
}

func TestExtensionControlUsesFixedSynchroSchema(t *testing.T) {
	controlPath := filepath.Join(goModuleRoot(t), "..", "..", "extensions", "synchro-pg", "synchro_pg.control")
	control, err := os.ReadFile(controlPath)
	if err != nil {
		t.Fatalf("read extension control file: %v", err)
	}
	if !strings.Contains(string(control), "schema = 'synchro'") {
		t.Fatal("extension control file does not use the fixed synchro schema")
	}
	if strings.Contains(string(control), "schema = 'public'") {
		t.Fatal("extension control file still uses the public schema")
	}
}

func goModuleRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve Go module root")
	}
	return filepath.Dir(filename)
}
