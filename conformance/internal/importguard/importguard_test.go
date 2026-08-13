package importguard

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestModulePolicy(t *testing.T) {
	root := tempModule(t, map[string]string{
		"go.mod":   testModuleFile,
		"legal.go": "package legal\n",
	})
	if err := CheckModulePolicy(context.Background(), root); err != nil {
		t.Fatalf("legal module rejected: %v", err)
	}
}

func TestModulePolicyRepositoryBoundary(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if err := Check(context.Background(), Policy{ModuleRoot: root}); err != nil {
		t.Fatalf("repository conformance boundary rejected: %v", err)
	}
}

func TestModulePolicyRejectsMalformedAndUnsafeModules(t *testing.T) {
	tests := []struct {
		name string
		mod  string
	}{
		{name: "wrong path", mod: "module example.com/other\n\ngo 1.25.0\n"},
		{name: "replace", mod: testModuleFile + "replace example.com/x => ./x\n"},
		{name: "exclude", mod: testModuleFile + "exclude example.com/x v1.0.0\n"},
		{name: "toolchain", mod: testModuleFile + "toolchain go1.25.1\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := tempModule(t, map[string]string{"go.mod": test.mod, "legal.go": "package legal\n"})
			if err := CheckModulePolicy(context.Background(), root); err == nil {
				t.Fatal("unsafe module was accepted")
			}
		})
	}
}

func TestModulePolicyRejectsDependencyDrift(t *testing.T) {
	tests := []struct {
		name string
		mod  string
	}{
		{
			name: "missing direct dependency",
			mod:  strings.Replace(testModuleFile, "\tgithub.com/gowebpki/jcs v1.0.1\n", "", 1),
		},
		{
			name: "changed direct version",
			mod:  strings.Replace(testModuleFile, "github.com/gowebpki/jcs v1.0.1", "github.com/gowebpki/jcs v1.0.0", 1),
		},
		{
			name: "unexpected direct dependency",
			mod:  strings.Replace(testModuleFile, ")\n", "\texample.com/extra v1.0.0\n)\n", 1),
		},
		{
			name: "changed Go version",
			mod:  strings.Replace(testModuleFile, "go 1.25.0", "go 1.24.0", 1),
		},
		{
			name: "missing indirect dependency",
			mod:  strings.Replace(testModuleFile, "\tgolang.org/x/text v0.29.0 // indirect\n", "", 1),
		},
		{
			name: "changed indirect version",
			mod:  strings.Replace(testModuleFile, "golang.org/x/text v0.29.0", "golang.org/x/text v0.28.0", 1),
		},
		{
			name: "unexpected indirect dependency",
			mod:  testModuleFile + "require example.com/extra-indirect v1.0.0 // indirect\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := tempModule(t, map[string]string{
				"go.mod":   test.mod,
				"legal.go": "package legal\n",
			})
			if err := CheckModulePolicy(context.Background(), root); err == nil {
				t.Fatal("dependency drift was accepted")
			}
		})
	}
}

func TestResolvedPackageModuleRejectsUnknownModule(t *testing.T) {
	err := validateResolvedPackageModule(listedPackage{
		ImportPath: "example.com/resolved-only/pkg",
		Dir:        filepath.Join(t.TempDir(), "pkg"),
		Module: &listedModule{
			Path:    "example.com/resolved-only",
			Version: "v1.0.0",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "unknown resolved module") {
		t.Fatalf("unknown resolved module error = %v", err)
	}
}

func TestModulePolicyCancellation(t *testing.T) {
	root := tempModule(t, map[string]string{
		"go.mod": "module github.com/trainstar/synchro/conformance\n\ngo 1.25.0\n",
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := CheckModulePolicy(ctx, root); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestModulePolicyCancellationWhileGoRuns(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture requires a Unix host")
	}
	bin := t.TempDir()
	goPath := filepath.Join(bin, "go")
	if err := os.WriteFile(goPath, []byte("#!/bin/sh\nexec sleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	root := tempModule(t, map[string]string{"go.mod": testModuleFile})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := CheckModulePolicy(ctx, root)
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("expected active cancellation, got %v", err)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatalf("active cancellation exceeded bounded wait: %s", time.Since(started))
	}
}

func TestModulePolicyDisablesAmbientWorkspace(t *testing.T) {
	root := tempModule(t, map[string]string{
		"go.mod":  testModuleFile,
		"go.work": "go 1.25.0\n\nuse .\n",
	})
	if err := CheckModulePolicy(context.Background(), root); err != nil {
		t.Fatalf("ambient workspace changed GOWORK=off resolution: %v", err)
	}
}

func TestModulePolicyIgnoresAmbientGoFlags(t *testing.T) {
	t.Setenv("GOFLAGS", "-mod=vendor")
	root := tempModule(t, map[string]string{
		"go.mod":   testModuleFile,
		"legal.go": "package legal\n",
	})
	if err := CheckModulePolicy(context.Background(), root); err != nil {
		t.Fatalf("ambient GOFLAGS changed readonly module resolution: %v", err)
	}
}

func TestParseImportsIncludesAliasesBlanksAndTests(t *testing.T) {
	root := tempModule(t, map[string]string{
		"go.mod":         testModuleFile,
		"source.go":      "package source\nimport ( alias \"example.com/alias\"; _ \"example.com/blank\" )\nvar _ = alias.Name\n",
		"source_test.go": "package source\nimport _ \"example.com/testonly\"\n",
	})
	imports, err := ParseImports(root)
	if err != nil {
		t.Fatal(err)
	}
	var joined string
	for _, paths := range imports {
		joined += strings.Join(paths, ",") + ","
	}
	for _, want := range []string{"example.com/alias", "example.com/blank", "example.com/testonly"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in parsed imports: %q", want, joined)
		}
	}
}

func TestParseImportsRejectsMalformedSource(t *testing.T) {
	root := tempModule(t, map[string]string{
		"broken.go": "package broken\nfunc {",
	})
	if _, err := ParseImports(root); err == nil {
		t.Fatal("malformed source was accepted")
	}
}

func TestCheckRejectsProductionImports(t *testing.T) {
	for _, imported := range []string{
		"github.com/trainstar/synchro/api/go/internal/x",
		"github.com/trainstar/synchro/clients/swift",
		"github.com/trainstar/synchro/extensions/synchro-core",
		"pgrx",
	} {
		t.Run(strings.ReplaceAll(imported, "/", "_"), func(t *testing.T) {
			root := tempModule(t, map[string]string{
				"go.mod": testModuleFile,
				"bad.go": "package bad\nimport _ \"" + imported + "\"\n",
			})
			if err := Check(context.Background(), Policy{ModuleRoot: root}); err == nil {
				t.Fatal("forbidden import was accepted")
			}
		})
	}
}

func TestCheckRejectsAliasBlankAndTestOnlyForbiddenImports(t *testing.T) {
	tests := []struct {
		name string
		file string
		body string
	}{
		{name: "alias", file: "alias.go", body: "package alias\nimport production \"github.com/trainstar/synchro/clients/swift\"\nvar _ = production.Name\n"},
		{name: "blank", file: "blank.go", body: "package blank\nimport _ \"github.com/trainstar/synchro/extensions/synchro-core\"\n"},
		{name: "test-only", file: "testonly_test.go", body: "package testonly\nimport _ \"github.com/trainstar/synchro/api/go\"\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := tempModule(t, map[string]string{
				"go.mod":  testModuleFile,
				test.file: test.body,
			})
			if err := Check(context.Background(), Policy{ModuleRoot: root}); err == nil {
				t.Fatal("forbidden source import was accepted")
			}
		})
	}
}

func TestCheckRejectsProtectedDirectAndTransitiveEdges(t *testing.T) {
	for _, test := range []struct {
		name  string
		files map[string]string
		root  string
	}{
		{name: "direct", files: map[string]string{
			"blackbox/blackbox.go": "package blackbox\n",
			"protected/direct.go":  "package protected\nimport _ \"github.com/trainstar/synchro/conformance/blackbox\"\n",
		}, root: modulePath + "/protected"},
		{name: "transitive", files: map[string]string{
			"blackbox/baseline/base.go": "package baseline\n",
			"shared/shared.go":          "package shared\nimport _ \"github.com/trainstar/synchro/conformance/blackbox/baseline\"\n",
			"protected/transitive.go":   "package protected\nimport _ \"github.com/trainstar/synchro/conformance/shared\"\n",
		}, root: modulePath + "/protected"},
		{name: "inactive-build-tag", files: map[string]string{
			"blackbox/baseline/base.go": "package baseline\n",
			"shared/shared.go":          "package shared\nimport _ \"github.com/trainstar/synchro/conformance/blackbox/baseline\"\n",
			"protected/hidden.go":       "//go:build synchro_never\n\npackage protected\nimport _ \"github.com/trainstar/synchro/conformance/shared\"\n",
		}, root: modulePath + "/protected"},
		{name: "inactive-platform", files: map[string]string{
			"blackbox/baseline/base.go": "package baseline\n",
			"protected/hidden_plan9.go": "package protected\nimport _ \"github.com/trainstar/synchro/conformance/blackbox/baseline\"\n",
		}, root: modulePath + "/protected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.files["go.mod"] = testModuleFile
			root := tempModule(t, test.files)
			policy := Policy{ModuleRoot: root, Protected: []string{test.root}, ForbiddenEdges: []string{modulePath + "/blackbox", modulePath + "/blackbox/baseline"}}
			if err := Check(context.Background(), policy); err == nil {
				t.Fatal("protected forbidden edge was accepted")
			}
		})
	}
}

func TestDefaultPolicyProtectsModelRunnerAndStrictReleasePackages(t *testing.T) {
	for _, packagePath := range []string{
		modulePath + "/modelrunner",
		modulePath + "/execution",
		modulePath + "/evidence",
		modulePath + "/inventory",
		modulePath + "/cmd/synchro-evidence",
	} {
		if !containsExact(defaultProtected, packagePath) {
			t.Fatalf("strict release package %q is not protected", packagePath)
		}
	}
}

func TestDefaultPolicyRejectsProtectedBlackboxEdges(t *testing.T) {
	for _, test := range []struct {
		name  string
		files map[string]string
	}{
		{
			name: "direct blackbox",
			files: map[string]string{
				"blackbox/blackbox.go": "package blackbox\n",
				"protected/direct.go":  "package protected\nimport _ \"github.com/trainstar/synchro/conformance/blackbox\"\n",
			},
		},
		{
			name: "direct baseline",
			files: map[string]string{
				"blackbox/baseline/base.go": "package baseline\n",
				"protected/direct.go":       "package protected\nimport _ \"github.com/trainstar/synchro/conformance/blackbox/baseline\"\n",
			},
		},
		{
			name: "transitive blackbox",
			files: map[string]string{
				"blackbox/blackbox.go":    "package blackbox\n",
				"shared/shared.go":        "package shared\nimport _ \"github.com/trainstar/synchro/conformance/blackbox\"\n",
				"protected/transitive.go": "package protected\nimport _ \"github.com/trainstar/synchro/conformance/shared\"\n",
			},
		},
		{
			name: "transitive baseline",
			files: map[string]string{
				"blackbox/baseline/base.go": "package baseline\n",
				"shared/shared.go":          "package shared\nimport _ \"github.com/trainstar/synchro/conformance/blackbox/baseline\"\n",
				"protected/transitive.go":   "package protected\nimport _ \"github.com/trainstar/synchro/conformance/shared\"\n",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.files["go.mod"] = testModuleFile
			root := tempModule(t, test.files)
			if err := Check(context.Background(), Policy{ModuleRoot: root}); err == nil {
				t.Fatal("default policy accepted a protected blackbox edge")
			}
		})
	}
}

func TestBaselineImportsAreLimitedToDiagnosticIntegration(t *testing.T) {
	baseline := modulePath + "/blackbox/baseline"
	for allowed := range diagnosticBaselineImporters {
		if err := checkBaselineImporters(map[string][]string{
			allowed:  {baseline},
			baseline: nil,
		}); err != nil {
			t.Fatalf("diagnostic integration %q was rejected: %v", allowed, err)
		}
	}

	for _, graph := range []map[string][]string{
		{
			modulePath + "/modelrunner": {baseline},
			baseline:                    nil,
		},
		{
			modulePath + "/evidence": {modulePath + "/shared"},
			modulePath + "/shared":   {baseline},
			baseline:                 nil,
		},
	} {
		if err := checkBaselineImporters(graph); err == nil {
			t.Fatal("non-diagnostic baseline dependency was accepted")
		}
	}
}

func TestCheckRejectsTransitiveProductionPrefix(t *testing.T) {
	root := tempModule(t, map[string]string{
		"go.mod":                 testModuleFile,
		"protected/protected.go": "package protected\nimport _ \"github.com/trainstar/synchro/conformance/legal\"\n",
		"legal/legal.go":         "package legal\nimport _ \"github.com/trainstar/synchro/clients/swift\"\n",
	})
	policy := Policy{
		ModuleRoot: root,
		PackagePatterns: []string{
			"./protected",
		},
		Protected:      []string{modulePath + "/protected"},
		Forbidden:      []string{"example.com/not-forbidden"},
		ForbiddenEdges: []string{"github.com/trainstar/synchro/clients"},
	}
	if err := Check(context.Background(), policy); err == nil || !strings.Contains(err.Error(), "protected package") {
		t.Fatal("transitive forbidden production prefix was accepted")
	}
}

func TestCheckRejectsVendorAndSymlinkedSources(t *testing.T) {
	t.Run("vendor", func(t *testing.T) {
		root := tempModule(t, map[string]string{
			"go.mod":                    testModuleFile,
			"legal.go":                  "package legal\n",
			"vendor/example.com/x/x.go": "package x\n",
		})
		if err := Check(context.Background(), Policy{ModuleRoot: root}); err == nil || !strings.Contains(err.Error(), "vendored source") {
			t.Fatalf("expected vendor rejection, got %v", err)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("symlink fixture requires a Unix host")
		}
		root := tempModule(t, map[string]string{
			"go.mod":   testModuleFile,
			"legal.go": "package legal\n",
		})
		external := t.TempDir()
		if err := os.WriteFile(filepath.Join(external, "outside.go"), []byte("package outside\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(external, filepath.Join(root, "linked")); err != nil {
			t.Fatal(err)
		}
		if err := Check(context.Background(), Policy{ModuleRoot: root}); err == nil || !strings.Contains(err.Error(), "symlinked source") {
			t.Fatalf("expected symlink rejection, got %v", err)
		}
	})
}

func TestResolvedContainmentRejectsSymlinkEscape(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink fixture requires a Unix host")
	}
	cache := t.TempDir()
	external := t.TempDir()
	link := filepath.Join(cache, "escaped-module")
	if err := os.Symlink(external, link); err != nil {
		t.Fatal(err)
	}
	if withinResolved(link, cache) {
		t.Fatal("real-path containment accepted a module-cache symlink escape")
	}
}

func TestDependencyPackageLocationRejectsSymlinkBelowModuleCache(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink fixture requires a Unix host")
	}
	cache := t.TempDir()
	target := filepath.Join(cache, "target")
	if err := os.MkdirAll(filepath.Join(target, "package"), 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(cache, "linked")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	pkg := listedPackage{
		ImportPath: "example.com/dependency",
		Dir:        filepath.Join(link, "package"),
		Module:     &listedModule{Path: "example.com/dependency"},
	}
	err := validatePackageLocation(pkg, t.TempDir(), cache)
	if err == nil {
		t.Fatal("symlinked dependency package path was accepted")
	}
	if !strings.Contains(err.Error(), link) {
		t.Fatalf("dependency path error omitted offending path %q: %v", link, err)
	}
}

func TestCheckAllowsLegalImports(t *testing.T) {
	root := tempModule(t, map[string]string{
		"go.mod":         testModuleFile,
		"legal/legal.go": "package legal\nimport \"encoding/json\"\nvar _ = json.Valid\n",
	})
	if err := Check(context.Background(), Policy{ModuleRoot: root}); err != nil {
		t.Fatalf("legal imports rejected: %v", err)
	}
}

func tempModule(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	if _, hasSum := files["go.sum"]; !hasSum && strings.Contains(files["go.mod"], "github.com/dlclark/regexp2/v2") {
		_, sourceFile, _, ok := runtime.Caller(0)
		if !ok {
			t.Fatal("resolve importguard test source")
		}
		sum, err := os.ReadFile(filepath.Join(filepath.Dir(sourceFile), "..", "..", "go.sum"))
		if err != nil {
			t.Fatal(err)
		}
		files["go.sum"] = string(sum)
	}
	for name, contents := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

const testModuleFile = `module github.com/trainstar/synchro/conformance

go 1.25.0

require (
	github.com/dlclark/regexp2/v2 v2.6.0
	github.com/gowebpki/jcs v1.0.1
	github.com/jackc/pgx/v5 v5.8.0
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)
`
