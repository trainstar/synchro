package mutants

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const integrationManifestPath = "conformance/mutants/integration/manifest.json"

type integrationManifest struct {
	SchemaVersion int                 `json:"schema_version"`
	Mutants       []integrationMutant `json:"mutants"`
}

type integrationMutant struct {
	ID               string `json:"id"`
	Patch            string `json:"patch"`
	RequirementID    string `json:"requirement_id"`
	ControlID        string `json:"control_id"`
	TestTarget       string `json:"test_target"`
	AssertionSubtest string `json:"assertion_subtest"`
}

type faultCatalog struct {
	Controls []faultControl `json:"controls"`
}

type faultControl struct {
	ID             string   `json:"id"`
	RequirementIDs []string `json:"requirement_ids"`
}

func TestIntegrationManifest(t *testing.T) {
	root := manifestRepositoryRoot(t)
	manifest := loadIntegrationManifest(t, root)
	if err := validateIntegrationManifest(root, manifest, gitApplyCheck(root)); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationManifestRejectsInvalidBindings(t *testing.T) {
	root := manifestRepositoryRoot(t)
	manifest := loadIntegrationManifest(t, root)

	tests := []struct {
		name   string
		mutate func(*integrationManifest)
		check  func(string) error
		want   string
	}{
		{
			name: "missing patch",
			mutate: func(m *integrationManifest) {
				m.Mutants[0].Patch = "conformance/mutants/integration/issue49-missing.patch"
			},
			want: "missing patch",
		},
		{
			name:   "stale patch",
			mutate: func(*integrationManifest) {},
			check: func(string) error {
				return fmt.Errorf("does not apply")
			},
			want: "stale patch",
		},
		{
			name: "duplicate ID",
			mutate: func(m *integrationManifest) {
				m.Mutants = append(m.Mutants, m.Mutants[0])
			},
			want: "duplicate mutant ID",
		},
		{
			name: "duplicate patch",
			mutate: func(m *integrationManifest) {
				m.Mutants[1].Patch = m.Mutants[0].Patch
			},
			want: "duplicate patch",
		},
		{
			name: "unknown control",
			mutate: func(m *integrationManifest) {
				m.Mutants[0].ControlID = "CTRL-UNKNOWN-001"
			},
			want: "unknown control",
		},
		{
			name: "wrong requirement binding",
			mutate: func(m *integrationManifest) {
				m.Mutants[0].RequirementID = "SYNC-TIME-001"
			},
			want: "wrong requirement binding",
		},
		{
			name: "unsupported test",
			mutate: func(m *integrationManifest) {
				m.Mutants[0].TestTarget = "TestRealUnsupportedMutation"
			},
			want: "unsupported TestReal target",
		},
		{
			name: "absent assertion",
			mutate: func(m *integrationManifest) {
				m.Mutants[0].AssertionSubtest = "assertion#99"
			},
			want: "absent exact t.Run assertion",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneIntegrationManifest(t, manifest)
			test.mutate(&candidate)
			check := test.check
			if check == nil {
				check = gitApplyCheck(root)
			}
			err := validateIntegrationManifest(root, candidate, check)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func validateIntegrationManifest(root string, manifest integrationManifest, checkPatch func(string) error) error {
	var failures []string
	if manifest.SchemaVersion != 1 {
		failures = append(failures, fmt.Sprintf("manifest schema_version = %d, want 1", manifest.SchemaVersion))
	}
	controls, err := loadControls(root)
	if err != nil {
		return err
	}
	supported, err := loadSupportedTestTargets(root)
	if err != nil {
		return err
	}
	assertions, err := loadTestAssertions(root)
	if err != nil {
		return err
	}
	patches, err := integrationPatches(root)
	if err != nil {
		return err
	}

	manifestPatches := make(map[string]struct{}, len(manifest.Mutants))
	ids := make(map[string]struct{}, len(manifest.Mutants))
	for _, mutant := range manifest.Mutants {
		if !strings.HasPrefix(mutant.ID, "issue49-") {
			failures = append(failures, fmt.Sprintf("invalid Issue 49 mutant ID %q", mutant.ID))
		}
		if _, duplicate := ids[mutant.ID]; duplicate {
			failures = append(failures, fmt.Sprintf("duplicate mutant ID %q", mutant.ID))
		}
		ids[mutant.ID] = struct{}{}
		if _, duplicate := manifestPatches[mutant.Patch]; duplicate {
			failures = append(failures, fmt.Sprintf("duplicate patch %q", mutant.Patch))
		}
		manifestPatches[mutant.Patch] = struct{}{}
		if !validRelativePatch(mutant.Patch) || !fileExists(root, mutant.Patch) {
			failures = append(failures, fmt.Sprintf("missing patch %q", mutant.Patch))
		} else if err := checkPatch(mutant.Patch); err != nil {
			failures = append(failures, fmt.Sprintf("stale patch %q: %v", mutant.Patch, err))
		}
		requirements, known := controls[mutant.ControlID]
		if !known {
			failures = append(failures, fmt.Sprintf("unknown control %q", mutant.ControlID))
		} else if len(requirements) != 1 || requirements[0] != mutant.RequirementID {
			failures = append(failures, fmt.Sprintf("wrong requirement binding for control %q: got %q want %q", mutant.ControlID, mutant.RequirementID, strings.Join(requirements, ",")))
		}
		if _, ok := supported[mutant.TestTarget]; !ok {
			failures = append(failures, fmt.Sprintf("unsupported TestReal target %q", mutant.TestTarget))
		} else if !hasAssertion(assertions[mutant.TestTarget], mutant.AssertionSubtest) {
			failures = append(failures, fmt.Sprintf("absent exact t.Run assertion %q for %s", mutant.AssertionSubtest, mutant.TestTarget))
		}
	}
	for patch := range patches {
		if _, present := manifestPatches[patch]; !present {
			failures = append(failures, fmt.Sprintf("missing manifest entry for patch %q", patch))
		}
	}
	for patch := range manifestPatches {
		if _, present := patches[patch]; !present {
			failures = append(failures, fmt.Sprintf("manifest references non-Issue 49 patch %q", patch))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	sort.Strings(failures)
	return fmt.Errorf("integration mutant manifest is invalid:\n%s", strings.Join(failures, "\n"))
}

func loadIntegrationManifest(t testing.TB, root string) integrationManifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, integrationManifestPath))
	if err != nil {
		t.Fatalf("read integration mutant manifest: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest integrationManifest
	if err := decoder.Decode(&manifest); err != nil {
		t.Fatalf("decode integration mutant manifest: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatal("integration mutant manifest contains trailing data")
	}
	return manifest
}

func cloneIntegrationManifest(t testing.TB, manifest integrationManifest) integrationManifest {
	t.Helper()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode integration mutant manifest: %v", err)
	}
	var clone integrationManifest
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatalf("decode cloned integration mutant manifest: %v", err)
	}
	return clone
}

func loadControls(root string) (map[string][]string, error) {
	data, err := os.ReadFile(filepath.Join(root, "conformance/faults/catalog.json"))
	if err != nil {
		return nil, fmt.Errorf("read fault catalog: %w", err)
	}
	var catalog faultCatalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, fmt.Errorf("decode fault catalog: %w", err)
	}
	controls := make(map[string][]string, len(catalog.Controls))
	for _, control := range catalog.Controls {
		controls[control.ID] = control.RequirementIDs
	}
	return controls, nil
}

var testTargetPattern = regexp.MustCompile(`\b(TestReal[A-Za-z0-9_]+)\b`)

func loadSupportedTestTargets(root string) (map[string]struct{}, error) {
	data, err := os.ReadFile(filepath.Join(root, "Makefile"))
	if err != nil {
		return nil, fmt.Errorf("read Makefile: %w", err)
	}
	targets := make(map[string]struct{})
	for _, name := range testTargetPattern.FindAllString(string(data), -1) {
		targets[name] = struct{}{}
	}
	return targets, nil
}

func loadTestAssertions(root string) (map[string]int, error) {
	assertions := make(map[string]int)
	path := filepath.Join(root, "conformance/blackbox/integration")
	err := filepath.WalkDir(path, func(file string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil || !strings.HasPrefix(function.Name.Name, "TestReal") {
				continue
			}
			count := 0
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) < 1 {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Run" {
					return true
				}
				name, ok := call.Args[0].(*ast.BasicLit)
				if !ok || name.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(name.Value)
				if err == nil && value == "assertion" {
					count++
				}
				return true
			})
			assertions[function.Name.Name] = count
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read TestReal assertions: %w", err)
	}
	return assertions, nil
}

func hasAssertion(count int, assertion string) bool {
	if assertion == "assertion" {
		return count > 0
	}
	if !strings.HasPrefix(assertion, "assertion#") {
		return false
	}
	ordinal, err := strconv.Atoi(strings.TrimPrefix(assertion, "assertion#"))
	return err == nil && ordinal > 0 && ordinal < count
}

func integrationPatches(root string) (map[string]struct{}, error) {
	paths, err := filepath.Glob(filepath.Join(root, "conformance/mutants/integration/issue49-*.patch"))
	if err != nil {
		return nil, fmt.Errorf("list Issue 49 patches: %w", err)
	}
	patches := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return nil, err
		}
		patches[filepath.ToSlash(relative)] = struct{}{}
	}
	return patches, nil
}

func validRelativePatch(path string) bool {
	return strings.HasPrefix(path, "conformance/mutants/integration/issue49-") && strings.HasSuffix(path, ".patch") && !filepath.IsAbs(path) && !strings.Contains(path, "..")
}

func fileExists(root, path string) bool {
	info, err := os.Stat(filepath.Join(root, path))
	return err == nil && !info.IsDir()
}

func gitApplyCheck(root string) func(string) error {
	return func(patch string) error {
		command := exec.Command("git", "-C", root, "apply", "--check", filepath.Join(root, patch))
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
		}
		return nil
	}
}

func manifestRepositoryRoot(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration manifest test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}
