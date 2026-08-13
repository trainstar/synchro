package contract

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/internal/schemavalidator"
)

const (
	releaseVersion  = "0.3.0"
	protocolVersion = 3
)

type catalogFile struct {
	name       string
	path       string
	schemaPath string
}

var authoredCatalogFiles = []catalogFile{
	{name: "requirements", path: "conformance/requirements.json", schemaPath: "conformance/schemas/requirements-v2.schema.json"},
	{name: "support matrix", path: "conformance/support-matrix.json", schemaPath: "conformance/schemas/support-matrix.schema.json"},
	{name: "fault catalog", path: "conformance/faults/catalog.json", schemaPath: "conformance/schemas/fault-catalog-v1.schema.json"},
	{name: "artifact inventory", path: "conformance/artifacts/inventory.json", schemaPath: "conformance/schemas/artifact-inventory-v1.schema.json"},
	{name: "performance budgets", path: "conformance/performance/budgets.json", schemaPath: "conformance/schemas/performance-budgets-v2.schema.json"},
}

// Load resolves a real repository root, schema-validates every authored
// catalog, strictly decodes it, and validates the cross-catalog contract.
func Load(ctx context.Context, repoRoot string) (*Bundle, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	rootPath, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return nil, err
	}
	defer root.Close()

	sources, err := captureBundleSources(ctx, root)
	if err != nil {
		return nil, err
	}
	return loadCapturedBundle(ctx, rootPath, sources)
}

func captureBundleSources(ctx context.Context, root *os.Root) (bundleSources, error) {
	sources := bundleSources{
		catalogs:   make(map[string][]byte, len(authoredCatalogFiles)),
		schemas:    make(map[string][]byte, len(authoredCatalogFiles)),
		behavioral: make(map[string][]byte, len(behavioralPaths)),
	}
	for _, item := range authoredCatalogFiles {
		if err := checkContext(ctx); err != nil {
			return bundleSources{}, err
		}
		if err := validateRepositoryRelativePath(item.path); err != nil {
			return bundleSources{}, fmt.Errorf("validate %s path: %w", item.name, err)
		}
		if err := validateRepositoryRelativePath(item.schemaPath); err != nil {
			return bundleSources{}, fmt.Errorf("validate %s schema path: %w", item.name, err)
		}
		contents, err := readRepositoryFile(ctx, root, item.path)
		if err != nil {
			return bundleSources{}, fmt.Errorf("capture %s: %w", item.name, err)
		}
		schema, err := readRepositoryFile(ctx, root, item.schemaPath)
		if err != nil {
			return bundleSources{}, fmt.Errorf("capture %s schema: %w", item.name, err)
		}
		sources.catalogs[item.path] = contents
		sources.schemas[item.schemaPath] = schema
	}
	for _, path := range behavioralPaths {
		contents, err := readRepositoryFile(ctx, root, path)
		if err != nil {
			return bundleSources{}, fmt.Errorf("capture behavioral file %q: %w", path, err)
		}
		sources.behavioral[path] = contents
	}
	return sources, nil
}

func loadCapturedBundle(ctx context.Context, root string, sources bundleSources) (*Bundle, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	validator := schemavalidator.New(root)
	defer validator.Close()
	for _, item := range authoredCatalogFiles {
		catalog, exists := sources.catalogs[item.path]
		if !exists {
			return nil, fmt.Errorf("captured %s is missing", item.name)
		}
		schema, exists := sources.schemas[item.schemaPath]
		if !exists {
			return nil, fmt.Errorf("captured %s schema is missing", item.name)
		}
		if err := validator.ValidateCapturedBytes(ctx, item.schemaPath, schema, catalog); err != nil {
			return nil, fmt.Errorf("validate captured %s schema: %w", item.name, err)
		}
		if err := rejectReadinessClaimKeys(catalog); err != nil {
			return nil, fmt.Errorf("validate %s claim keys: %w", item.name, err)
		}
	}

	bundle := &Bundle{sources: cloneBundleSources(sources)}
	if err := jsonstrict.Decode(bundle.sources.catalogs["conformance/requirements.json"], &bundle.Requirements); err != nil {
		return nil, fmt.Errorf("decode requirements: %w", err)
	}
	if err := jsonstrict.Decode(bundle.sources.catalogs["conformance/support-matrix.json"], &bundle.Support); err != nil {
		return nil, fmt.Errorf("decode support matrix: %w", err)
	}
	if err := jsonstrict.Decode(bundle.sources.catalogs["conformance/faults/catalog.json"], &bundle.Faults); err != nil {
		return nil, fmt.Errorf("decode fault catalog: %w", err)
	}
	if err := jsonstrict.Decode(bundle.sources.catalogs["conformance/artifacts/inventory.json"], &bundle.Artifacts); err != nil {
		return nil, fmt.Errorf("decode artifact inventory: %w", err)
	}
	if err := jsonstrict.Decode(bundle.sources.catalogs["conformance/performance/budgets.json"], &bundle.Performance); err != nil {
		return nil, fmt.Errorf("decode performance budgets: %w", err)
	}
	fingerprint, err := typedPerformanceFingerprint(bundle.Performance)
	if err != nil {
		return nil, fmt.Errorf("fingerprint captured performance catalog: %w", err)
	}
	bundle.performanceFingerprint = fingerprint
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := bundle.validate(ctx); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	return bundle, nil
}

func cloneBundleSources(sources bundleSources) bundleSources {
	return bundleSources{
		catalogs:   cloneSourceMap(sources.catalogs),
		schemas:    cloneSourceMap(sources.schemas),
		behavioral: cloneSourceMap(sources.behavioral),
	}
}

func cloneSourceMap(source map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(source))
	for path, data := range source {
		result[path] = append([]byte(nil), data...)
	}
	return result
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}
	return nil
}

func resolveRepositoryRoot(repoRoot string) (string, error) {
	if repoRoot == "" {
		return "", errors.New("repository root is empty")
	}
	absRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		return "", fmt.Errorf("resolve repository root: %w", err)
	}
	realRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return "", fmt.Errorf("resolve real repository root: %w", err)
	}
	info, err := os.Stat(realRoot)
	if err != nil {
		return "", fmt.Errorf("inspect repository root: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("repository root is not a directory: %s", realRoot)
	}
	return filepath.Clean(realRoot), nil
}

func openRepositoryRoot(repoRoot string) (string, *os.Root, error) {
	rootPath, err := resolveRepositoryRoot(repoRoot)
	if err != nil {
		return "", nil, err
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return "", nil, fmt.Errorf("open repository root: %w", err)
	}
	return rootPath, root, nil
}

func readRepositoryFile(ctx context.Context, root *os.Root, relativePath string) ([]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if root == nil {
		return nil, errors.New("repository root is nil")
	}
	if err := validateRepositoryRelativePath(relativePath); err != nil {
		return nil, err
	}
	file, err := root.Open(filepath.FromSlash(relativePath))
	if err != nil {
		return nil, fmt.Errorf("open rooted repository file %q: %w", relativePath, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened repository file %q: %w", relativePath, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("repository-relative path is not a regular file: %q", relativePath)
	}
	data, err := readAllContext(ctx, file)
	if err != nil {
		return nil, fmt.Errorf("read opened repository file %q: %w", relativePath, err)
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if !utf8.Valid(data) {
		return nil, errors.New("file contains invalid UTF-8")
	}
	return data, nil
}

func readAllContext(ctx context.Context, reader io.Reader) ([]byte, error) {
	var output bytes.Buffer
	buffer := make([]byte, 32*1024)
	for {
		if err := checkContext(ctx); err != nil {
			return nil, err
		}
		count, err := reader.Read(buffer)
		if count > 0 {
			_, _ = output.Write(buffer[:count])
		}
		if errors.Is(err, io.EOF) {
			return output.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func validateRepositoryRelativePath(authoredPath string) error {
	if authoredPath == "" {
		return errors.New("repository-relative path is empty")
	}
	if strings.IndexByte(authoredPath, 0) >= 0 {
		return errors.New("repository-relative path contains NUL")
	}
	if strings.Contains(authoredPath, `\`) {
		return fmt.Errorf("repository-relative path contains backslash: %q", authoredPath)
	}
	if filepath.IsAbs(authoredPath) || strings.HasPrefix(authoredPath, "/") {
		return fmt.Errorf("repository-relative path is absolute: %q", authoredPath)
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(authoredPath)))
	if clean != authoredPath || clean == "." || strings.HasPrefix(clean, "../") || clean == ".." {
		return fmt.Errorf("repository-relative path is not canonical: %q", authoredPath)
	}
	for _, component := range strings.Split(authoredPath, "/") {
		if component == "." || component == ".." || component == "" {
			return fmt.Errorf("repository-relative path has dot or empty component: %q", authoredPath)
		}
	}
	return nil
}

func rejectReadinessClaimKeys(data []byte) error {
	var document map[string]any
	if err := jsonstrict.Decode(data, &document); err != nil {
		return err
	}
	return rejectReadinessClaimObject(document, "$")
}

func rejectReadinessClaimObject(object map[string]any, location string) error {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var failures []error
	for _, key := range keys {
		if isReadinessClaimKey(key) {
			failures = append(failures, fmt.Errorf("forbidden readiness claim key %q at %s", key, location))
		}
		if err := rejectReadinessClaimValue(object[key], location+"."+key); err != nil {
			failures = append(failures, err)
		}
	}
	return joinSemanticErrors(failures)
}

func rejectReadinessClaimValue(value any, location string) error {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var failures []error
		for _, key := range keys {
			if isReadinessClaimKey(key) {
				failures = append(failures, fmt.Errorf("forbidden readiness claim key %q at %s", key, location))
			}
			if err := rejectReadinessClaimValue(typed[key], location+"."+key); err != nil {
				failures = append(failures, err)
			}
		}
		return joinSemanticErrors(failures)
	case []any:
		var failures []error
		for index, child := range typed {
			if err := rejectReadinessClaimValue(child, fmt.Sprintf("%s[%d]", location, index)); err != nil {
				failures = append(failures, err)
			}
		}
		return joinSemanticErrors(failures)
	default:
		return nil
	}
}

func isReadinessClaimKey(key string) bool {
	key = strings.ToLower(key)
	key = strings.NewReplacer("_", "-", "--", "-").Replace(key)
	for strings.Contains(key, "--") {
		key = strings.ReplaceAll(key, "--", "-")
	}
	switch key {
	case "covered", "partial", "passed", "ready", "certified", "waived", "accepted-flaky":
		return true
	default:
		return false
	}
}
