// Package importguard enforces the standalone conformance module's dependency
// boundary without importing any production implementation packages.
package importguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

const modulePath = "github.com/trainstar/synchro/conformance"

const goVersion = "1.25.0"

var requiredDirectModules = map[string]string{
	"github.com/dlclark/regexp2/v2":            "v2.6.0",
	"github.com/gowebpki/jcs":                  "v1.0.1",
	"github.com/jackc/pgx/v5":                  "v5.8.0",
	"github.com/santhosh-tekuri/jsonschema/v6": "v6.0.2",
}

var requiredIndirectModules = map[string]string{
	"github.com/jackc/pgpassfile":    "v1.0.0",
	"github.com/jackc/pgservicefile": "v0.0.0-20240606120523-5a60cdf6a761",
	"github.com/jackc/puddle/v2":     "v2.2.2",
	"golang.org/x/sync":              "v0.17.0",
	"golang.org/x/text":              "v0.29.0",
}

var defaultForbidden = []string{
	"github.com/trainstar/synchro/api/go",
	"github.com/trainstar/synchro/clients",
	"github.com/trainstar/synchro/extensions",
	"pgrx",
}

var defaultProtected = []string{
	modulePath + "/internal/contract",
	modulePath + "/vectors",
	modulePath + "/scenarios",
	modulePath + "/reference",
	modulePath + "/modelrunner",
	modulePath + "/barriers",
	modulePath + "/faults",
	modulePath + "/observer",
	modulePath + "/execution",
	modulePath + "/evidence",
	modulePath + "/inventory",
	modulePath + "/mutants",
	modulePath + "/cmd/synchro-evidence",
}

var diagnosticBlackboxImporters = map[string]struct{}{
	modulePath + "/blackbox/integration":       {},
	modulePath + "/blackbox/syntheticproof":    {},
	modulePath + "/cmd/synchro-local-postgres": {},
	modulePath + "/cmd/synchro-conformance":    {},
	modulePath + "/evidence":                   {},
	modulePath + "/kotlin":                     {},
	modulePath + "/reactnative":                {},
	modulePath + "/swift":                      {},
}

// Policy describes the packages and dependency edges allowed in a module.
// PackagePatterns are evaluated by go list from ModuleRoot. Empty patterns
// mean ./.... Forbidden applies to package and module paths. Protected and
// ForbiddenEdges describe reachability edges in the resolved package graph.
type Policy struct {
	ModuleRoot      string
	PackagePatterns []string
	Forbidden       []string
	Protected       []string
	ForbiddenEdges  []string
}

// ParseImports parses every Go source file below root, including internal test
// files and external test files. The returned keys are absolute source paths,
// and each value is the source import path exactly as written in the AST.
func ParseImports(root string) (map[string][]string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve source root: %w", err)
	}
	imports := make(map[string][]string)
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			if entry.Name() == "vendor" {
				return fmt.Errorf("vendored source directory is not allowed: %s", path)
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlinked source is not allowed: %s", path)
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", path, parseErr)
		}
		paths := make([]string, 0, len(file.Imports))
		for _, spec := range file.Imports {
			if spec.Path == nil {
				return fmt.Errorf("parse %s: import has no path", path)
			}
			var value string
			if err := json.Unmarshal([]byte(spec.Path.Value), &value); err != nil {
				return fmt.Errorf("parse import in %s: %w", path, err)
			}
			paths = append(paths, value)
		}
		sort.Strings(paths)
		imports[path] = paths
		return nil
	})
	if err != nil {
		return nil, err
	}
	return imports, nil
}

// Check verifies source imports and the resolved dependency graph against the
// supplied policy. Every Go subprocess is run with GOWORK=off.
func Check(ctx context.Context, policy Policy) error {
	if err := CheckModulePolicy(ctx, policy.ModuleRoot); err != nil {
		return err
	}
	root, err := filepath.Abs(policy.ModuleRoot)
	if err != nil {
		return fmt.Errorf("resolve module root: %w", err)
	}
	env, err := goEnv(ctx, root, "GOMODCACHE")
	if err != nil {
		return err
	}
	modCache := env["GOMODCACHE"]
	sourceImports, err := ParseImports(root)
	if err != nil {
		return err
	}
	sourceGraph := make(map[string][]string)
	for file, paths := range sourceImports {
		packagePath, err := sourcePackagePath(root, file)
		if err != nil {
			return err
		}
		for _, imported := range paths {
			if forbiddenPath(imported, policy.forbidden()) {
				return fmt.Errorf("forbidden import %q in %s", imported, file)
			}
		}
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		sourceGraph[packagePath] = append(sourceGraph[packagePath], paths...)
	}
	for packagePath, imports := range sourceGraph {
		sourceGraph[packagePath] = uniqueSorted(imports)
	}
	if err := checkBlackboxImporters(sourceGraph); err != nil {
		return fmt.Errorf("all-source dependency graph: %w", err)
	}
	if err := checkProtectedEdges(sourceGraph, policy.protected(), policy.forbiddenEdges()); err != nil {
		return fmt.Errorf("all-source dependency graph: %w", err)
	}

	patterns := policy.PackagePatterns
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	packages, err := listPackages(ctx, root, patterns)
	if err != nil {
		return err
	}
	graph := make(map[string][]string, len(packages))
	for _, pkg := range packages {
		if forbiddenPath(pkg.ImportPath, policy.forbidden()) {
			return fmt.Errorf("forbidden package %q in resolved graph", pkg.ImportPath)
		}
		if pkg.Module != nil {
			if forbiddenPath(pkg.Module.Path, policy.forbidden()) {
				return fmt.Errorf("forbidden module %q for package %q", pkg.Module.Path, pkg.ImportPath)
			}
			if pkg.Module.Replace != nil {
				if forbiddenPath(pkg.Module.Replace.Path, policy.forbidden()) {
					return fmt.Errorf("forbidden replacement module %q for package %q", pkg.Module.Replace.Path, pkg.ImportPath)
				}
			}
		}
		if err := validateResolvedPackageModule(pkg); err != nil {
			return err
		}
		if err := validatePackageLocation(pkg, root, modCache); err != nil {
			return err
		}
		graph[pkg.ImportPath] = uniqueSorted(pkg.Imports)
	}
	if err := checkProtectedEdges(graph, policy.protected(), policy.forbiddenEdges()); err != nil {
		return err
	}
	return nil
}

func validateResolvedPackageModule(pkg listedPackage) error {
	if pkg.Standard {
		return nil
	}
	if pkg.Module == nil {
		return fmt.Errorf("non-standard package %q has no resolved module", pkg.ImportPath)
	}
	if pkg.Module.Path == modulePath {
		return nil
	}
	expectedVersion, allowed := requiredDirectModules[pkg.Module.Path]
	if !allowed {
		expectedVersion, allowed = requiredIndirectModules[pkg.Module.Path]
	}
	if !allowed {
		return fmt.Errorf("package %q uses unknown resolved module %q", pkg.ImportPath, pkg.Module.Path)
	}
	if pkg.Module.Version != expectedVersion {
		return fmt.Errorf("resolved module %q must use %q, found %q", pkg.Module.Path, expectedVersion, pkg.Module.Version)
	}
	return nil
}

func checkBlackboxImporters(graph map[string][]string) error {
	blackbox := modulePath + "/blackbox"
	for source := range graph {
		if _, allowed := diagnosticBlackboxImporters[source]; allowed {
			continue
		}
		seen := map[string]bool{source: true}
		queue := append([]string(nil), graph[source]...)
		for len(queue) != 0 {
			target := queue[0]
			queue = queue[1:]
			if forbiddenPath(target, []string{blackbox}) {
				return fmt.Errorf("package %q reaches diagnostic blackbox package %q", source, target)
			}
			if seen[target] {
				continue
			}
			seen[target] = true
			queue = append(queue, graph[target]...)
		}
	}
	return nil
}

// CheckModulePolicy checks the standalone module declaration and verifies that
// module resolution is independent of workspace and local replacement state.
func CheckModulePolicy(ctx context.Context, moduleRoot string) error {
	root, err := filepath.Abs(moduleRoot)
	if err != nil {
		return fmt.Errorf("resolve module root: %w", err)
	}
	document, err := parseModFile(ctx, root)
	if err != nil {
		return err
	}
	if document.Module.Path != modulePath {
		return fmt.Errorf("module path %q does not equal %q", document.Module.Path, modulePath)
	}
	if document.Go != goVersion {
		return fmt.Errorf("Go version %q does not equal %q", document.Go, goVersion)
	}
	if len(document.Replace) != 0 {
		return errors.New("go.mod directive \"replace\" is not allowed")
	}
	if len(document.Exclude) != 0 {
		return errors.New("go.mod directive \"exclude\" is not allowed")
	}
	if document.Toolchain != "" {
		return errors.New("go.mod directive \"toolchain\" is not allowed")
	}
	directModules := make(map[string]string)
	indirectModules := make(map[string]string)
	for _, requirement := range document.Require {
		if requirement.Indirect {
			if _, exists := indirectModules[requirement.Path]; exists {
				return fmt.Errorf("indirect module %q is declared more than once", requirement.Path)
			}
			indirectModules[requirement.Path] = requirement.Version
			continue
		}
		if _, exists := directModules[requirement.Path]; exists {
			return fmt.Errorf("direct module %q is declared more than once", requirement.Path)
		}
		directModules[requirement.Path] = requirement.Version
	}
	for path, version := range requiredDirectModules {
		if directModules[path] != version {
			return fmt.Errorf("direct module %q must use %q, found %q", path, version, directModules[path])
		}
		delete(directModules, path)
	}
	if len(directModules) != 0 {
		paths := make([]string, 0, len(directModules))
		for path := range directModules {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		return fmt.Errorf("unexpected direct modules: %s", strings.Join(paths, ", "))
	}
	for path, version := range requiredIndirectModules {
		if indirectModules[path] != version {
			return fmt.Errorf("indirect module %q must use %q, found %q", path, version, indirectModules[path])
		}
		delete(indirectModules, path)
	}
	if len(indirectModules) != 0 {
		paths := make([]string, 0, len(indirectModules))
		for path := range indirectModules {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		return fmt.Errorf("unexpected indirect modules: %s", strings.Join(paths, ", "))
	}

	env, err := goEnv(ctx, root, "GOWORK", "GOMODCACHE")
	if err != nil {
		return err
	}
	if strings.TrimSpace(env["GOWORK"]) != "off" {
		return fmt.Errorf("effective workspace is not disabled: GOWORK=%q", env["GOWORK"])
	}
	modCache := env["GOMODCACHE"]
	modules, err := listModules(ctx, root)
	if err != nil {
		return err
	}
	for _, mod := range modules {
		if mod.Replace != nil {
			return fmt.Errorf("module %q uses a replacement", mod.Path)
		}
		if mod.Path == modulePath {
			continue
		}
		if forbiddenPath(mod.Path, defaultForbidden) {
			return fmt.Errorf("forbidden module %q in resolved module graph", mod.Path)
		}
		if expectedVersion, ok := requiredDirectModules[mod.Path]; ok && mod.Version != expectedVersion {
			return fmt.Errorf("resolved module %q must use %q, found %q", mod.Path, expectedVersion, mod.Version)
		}
		if expectedVersion, ok := requiredIndirectModules[mod.Path]; ok && mod.Version != expectedVersion {
			return fmt.Errorf("resolved module %q must use %q, found %q", mod.Path, expectedVersion, mod.Version)
		}
		location := mod.Dir
		if location == "" && mod.GoMod != "" {
			location = filepath.Dir(mod.GoMod)
		}
		if location == "" || !withinResolved(location, modCache) {
			return fmt.Errorf("module %q resolves outside module cache: %q", mod.Path, location)
		}
	}
	return nil
}

func (p Policy) forbidden() []string {
	if len(p.Forbidden) != 0 {
		return p.Forbidden
	}
	return defaultForbidden
}

func (p Policy) protected() []string {
	if len(p.Protected) != 0 {
		return p.Protected
	}
	return defaultProtected
}

func (p Policy) forbiddenEdges() []string {
	if len(p.ForbiddenEdges) != 0 {
		return p.ForbiddenEdges
	}
	return []string{modulePath + "/blackbox"}
}

type modFile struct {
	Module struct {
		Path string
	}
	Go        string
	Toolchain string
	Exclude   []json.RawMessage
	Replace   []json.RawMessage
	Require   []struct {
		Path     string
		Version  string
		Indirect bool
	}
}

func parseModFile(ctx context.Context, root string) (modFile, error) {
	output, err := runGo(ctx, root, "mod", "edit", "-json")
	if err != nil {
		return modFile{}, fmt.Errorf("parse go.mod: %w", err)
	}
	var document modFile
	if err := json.Unmarshal(output, &document); err != nil {
		return modFile{}, fmt.Errorf("decode go.mod: %w", err)
	}
	return document, nil
}

type listedPackage struct {
	ImportPath   string
	Dir          string
	Standard     bool
	Imports      []string
	TestImports  []string
	XTestImports []string
	Module       *listedModule
}

type listedModule struct {
	Path    string
	Version string
	Dir     string
	GoMod   string
	Replace *listedModule
}

func listPackages(ctx context.Context, root string, patterns []string) ([]listedPackage, error) {
	args := []string{"list", "-mod=readonly", "-deps", "-json", "-test"}
	args = append(args, patterns...)
	output, err := runGo(ctx, root, args...)
	if err != nil {
		return nil, fmt.Errorf("go list dependency graph: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	var packages []listedPackage
	for {
		var pkg listedPackage
		err := decoder.Decode(&pkg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode go list package: %w", err)
		}
		packages = append(packages, pkg)
	}
	return packages, nil
}

func listModules(ctx context.Context, root string) ([]listedModule, error) {
	output, err := runGo(ctx, root, "list", "-mod=readonly", "-m", "-json", "all")
	if err != nil {
		return nil, fmt.Errorf("go list modules: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	var modules []listedModule
	for {
		var mod listedModule
		err := decoder.Decode(&mod)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode go list module: %w", err)
		}
		modules = append(modules, mod)
	}
	return modules, nil
}

func goEnv(ctx context.Context, root string, keys ...string) (map[string]string, error) {
	output, err := runGo(ctx, root, append([]string{"env"}, keys...)...)
	if err != nil {
		return nil, fmt.Errorf("go env: %w", err)
	}
	values := make(map[string]string, len(keys))
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for i, key := range keys {
		if i < len(lines) {
			values[key] = strings.TrimSpace(lines[i])
		}
	}
	return values, nil
}

func runGo(ctx context.Context, root string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = root
	command.Env = appendWithoutKey(os.Environ(), "GOWORK")
	command.Env = appendWithoutKey(command.Env, "GOFLAGS")
	command.Env = append(command.Env, "GOWORK=off", "GOFLAGS=")
	configureCommand(command)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ctx.Err()
		}
		message := strings.TrimSpace(stderr.String())
		if message != "" {
			return nil, fmt.Errorf("%w: %s", err, message)
		}
		return nil, err
	}
	return output, nil
}

func appendWithoutKey(env []string, key string) []string {
	prefix := key + "="
	filtered := make([]string, 0, len(env)+1)
	for _, value := range env {
		if !strings.HasPrefix(value, prefix) {
			filtered = append(filtered, value)
		}
	}
	return filtered
}

func checkProtectedEdges(graph map[string][]string, protected, forbidden []string) error {
	for source := range graph {
		if !matchesAny(source, protected) {
			continue
		}
		if !containsExact(protected, source) && forbiddenPath(source, forbidden) {
			continue
		}
		seen := map[string]bool{source: true}
		queue := append([]string(nil), graph[source]...)
		for len(queue) != 0 {
			target := queue[0]
			queue = queue[1:]
			if forbiddenPath(target, forbidden) {
				return fmt.Errorf("protected package %q reaches forbidden package %q", source, target)
			}
			if seen[target] {
				continue
			}
			seen[target] = true
			queue = append(queue, graph[target]...)
		}
	}
	return nil
}

func containsExact(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func forbiddenPath(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

func matchesAny(path string, prefixes []string) bool {
	return forbiddenPath(path, prefixes)
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func within(path, root string) bool {
	if root == "" {
		return false
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(absRoot, absPath)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func sourcePackagePath(root, file string) (string, error) {
	directory := filepath.Dir(file)
	relative, err := filepath.Rel(root, directory)
	if err != nil {
		return "", fmt.Errorf("resolve source package for %s: %w", file, err)
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("source file escapes module root: %s", file)
	}
	if relative == "." {
		return modulePath, nil
	}
	return modulePath + "/" + filepath.ToSlash(relative), nil
}

func validatePackageLocation(pkg listedPackage, moduleRoot, moduleCache string) error {
	if pkg.Standard {
		return nil
	}
	if pkg.Dir == "" {
		return fmt.Errorf("non-standard package %q has no resolved source directory", pkg.ImportPath)
	}
	if pkg.Module == nil || pkg.Module.Path == modulePath {
		if !withinResolved(pkg.Dir, moduleRoot) {
			return fmt.Errorf("module package %q resolves outside module root: %q", pkg.ImportPath, pkg.Dir)
		}
		return nil
	}
	if err := validateDependencyPackageLocation(pkg.Dir, moduleCache); err != nil {
		return fmt.Errorf("validate dependency package %q: %w", pkg.ImportPath, err)
	}
	return nil
}

func validateDependencyPackageLocation(packageDir, moduleCache string) error {
	if moduleCache == "" {
		return fmt.Errorf("module cache is empty for package path %q", packageDir)
	}
	lexicalPackage, err := filepath.Abs(packageDir)
	if err != nil {
		return fmt.Errorf("resolve package path %q: %w", packageDir, err)
	}
	lexicalCache, err := filepath.Abs(moduleCache)
	if err != nil {
		return fmt.Errorf("resolve module cache path %q: %w", moduleCache, err)
	}
	if !within(lexicalPackage, lexicalCache) {
		return fmt.Errorf("package path %q is outside lexical module cache %q", lexicalPackage, lexicalCache)
	}
	relative, err := filepath.Rel(lexicalCache, lexicalPackage)
	if err != nil {
		return fmt.Errorf("relate package path %q to module cache %q: %w", lexicalPackage, lexicalCache, err)
	}
	current := lexicalCache
	if relative != "." {
		for _, component := range strings.Split(relative, string(filepath.Separator)) {
			current = filepath.Join(current, component)
			info, err := os.Lstat(current)
			if err != nil {
				return fmt.Errorf("inspect package path component %q: %w", current, err)
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("package path %q contains symlink component %q", lexicalPackage, current)
			}
		}
	}
	if !withinResolved(lexicalPackage, lexicalCache) {
		return fmt.Errorf("package path %q resolves outside module cache %q", lexicalPackage, lexicalCache)
	}
	return nil
}

func withinResolved(path, root string) bool {
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	return within(resolvedPath, resolvedRoot)
}
