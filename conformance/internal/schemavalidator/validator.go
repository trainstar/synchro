// Package schemavalidator validates JSON documents against repository-local
// JSON Schemas without permitting ambient resource retrieval.
package schemavalidator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dlclark/regexp2/v2"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Validator validates instances with schemas contained by a single real
// repository root. Successfully compiled schemas are cached for the lifetime
// of the Validator and are safe to use concurrently.
type Validator struct {
	repoRoot string
	root     *os.Root
	rootErr  error

	cacheMu sync.RWMutex
	cache   map[string]*jsonschema.Schema
}

const (
	draft2020Dialect      = "https://json-schema.org/draft/2020-12/schema"
	validationWorkerLimit = 4
)

var validationWorkerSlots = make(chan struct{}, validationWorkerLimit)

// New constructs a Validator rooted at repoRoot. Root resolution failures are
// reported by ValidateFile or ValidateBytes because New has no error result.
func New(repoRoot string) *Validator {
	v := &Validator{cache: make(map[string]*jsonschema.Schema)}
	if repoRoot == "" {
		v.rootErr = errors.New("repository root is empty")
		return v
	}

	absRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		v.rootErr = fmt.Errorf("resolve repository root: %w", err)
		return v
	}
	realRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		v.rootErr = fmt.Errorf("resolve real repository root: %w", err)
		return v
	}
	root, err := os.OpenRoot(realRoot)
	if err != nil {
		v.rootErr = fmt.Errorf("open real repository root: %w", err)
		return v
	}

	v.repoRoot = filepath.Clean(realRoot)
	v.root = root
	runtime.SetFinalizer(v, func(validator *Validator) {
		_ = validator.Close()
	})
	return v
}

// Close releases the repository root. Callers must not close a Validator while
// another goroutine uses it.
func (v *Validator) Close() error {
	if v == nil {
		return nil
	}
	v.cacheMu.Lock()
	defer v.cacheMu.Unlock()
	if v.root == nil {
		return nil
	}
	runtime.SetFinalizer(v, nil)
	root := v.root
	v.root = nil
	v.rootErr = errors.New("schema validator is closed")
	v.cache = nil
	return root.Close()
}

// ValidateFile validates the JSON document at instancePath against the schema
// at schemaPath. Both paths must resolve to regular files beneath the real
// repository root.
func (v *Validator) ValidateFile(ctx context.Context, schemaPath, instancePath string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	sch, err := v.compiledSchema(ctx, schemaPath)
	if err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}

	path, err := v.resolveRequestedPath(instancePath, "instance")
	if err != nil {
		return err
	}
	instance, err := v.readStrictJSONFile(ctx, path, "instance")
	if err != nil {
		return fmt.Errorf("decode instance %q: %w", instancePath, err)
	}
	return validate(ctx, sch, instance)
}

// ValidateBytes validates data against the repository-local schema at
// schemaPath. A $schema property in data is instance data only and never
// changes which schema is compiled or used.
func (v *Validator) ValidateBytes(ctx context.Context, schemaPath string, data []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	sch, err := v.compiledSchema(ctx, schemaPath)
	if err != nil {
		return err
	}
	instance, err := decodeStrictJSON(ctx, data)
	if err != nil {
		return fmt.Errorf("decode instance: %w", err)
	}
	return validate(ctx, sch, instance)
}

// ValidateCapturedBytes validates captured instance bytes against captured
// root schema bytes. It does not read the root schema from the repository.
func (v *Validator) ValidateCapturedBytes(ctx context.Context, schemaPath string, schemaData, instanceData []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if v == nil {
		return errors.New("schema validator is nil")
	}
	if v.rootErr != nil {
		return v.rootErr
	}
	path, err := v.resolveCapturedSchemaPath(schemaPath)
	if err != nil {
		return err
	}
	schemaDocument, err := decodeStrictJSON(ctx, schemaData)
	if err != nil {
		return fmt.Errorf("decode captured schema %q: %w", schemaPath, err)
	}
	instance, err := decodeStrictJSON(ctx, instanceData)
	if err != nil {
		return fmt.Errorf("decode captured instance: %w", err)
	}
	resourceURL := v.resourceURL(path)
	loader := &repositoryLoader{
		ctx:             ctx,
		validator:       v,
		declared:        make(map[string]struct{}),
		rootResourceURL: resourceURL,
	}
	sch, err := compileSchema(ctx, resourceURL, schemaDocument, loader)
	if err != nil {
		return fmt.Errorf("compile captured schema %q: %w", schemaPath, err)
	}
	return validate(ctx, sch, instance)
}

func validate(ctx context.Context, sch *jsonschema.Schema, instance any) error {
	err := runValidationWorker(ctx, func() error {
		return sch.Validate(instance)
	})
	if err != nil {
		if contextErr := contextError(ctx); contextErr != nil {
			return contextErr
		}
		return fmt.Errorf("validate instance: %w", err)
	}
	return nil
}

func runValidationWorker(ctx context.Context, work func() error) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	select {
	case validationWorkerSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := contextError(ctx); err != nil {
		<-validationWorkerSlots
		return err
	}

	// The buffer lets the worker report after caller cancellation without blocking.
	result := make(chan error, 1)
	go func() {
		defer func() { <-validationWorkerSlots }()
		result <- work()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		if contextErr := contextError(ctx); contextErr != nil {
			return contextErr
		}
		return err
	}
}

func (v *Validator) compiledSchema(ctx context.Context, schemaPath string) (*jsonschema.Schema, error) {
	if v == nil {
		return nil, errors.New("schema validator is nil")
	}
	if v.rootErr != nil {
		return nil, v.rootErr
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	path, err := v.resolveRequestedPath(schemaPath, "schema")
	if err != nil {
		return nil, err
	}
	v.cacheMu.RLock()
	sch := v.cache[path]
	v.cacheMu.RUnlock()
	if sch != nil {
		return sch, nil
	}

	doc, err := v.readStrictJSONFile(ctx, path, "schema")
	if err != nil {
		return nil, fmt.Errorf("decode schema %q: %w", schemaPath, err)
	}
	resourceURL := v.resourceURL(path)
	loader := &repositoryLoader{
		ctx:             ctx,
		validator:       v,
		declared:        make(map[string]struct{}),
		allowLocal:      true,
		rootResourceURL: resourceURL,
	}
	sch, err = compileSchema(ctx, resourceURL, doc, loader)
	if err != nil {
		if contextErr := contextError(ctx); contextErr != nil {
			return nil, contextErr
		}
		return nil, fmt.Errorf("compile schema %q: %w", schemaPath, err)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	v.cacheMu.Lock()
	if cached := v.cache[path]; cached != nil {
		sch = cached
	} else {
		v.cache[path] = sch
	}
	v.cacheMu.Unlock()
	return sch, nil
}

func compileSchema(ctx context.Context, resourceURL string, doc any, loader *repositoryLoader) (*jsonschema.Schema, error) {
	if err := requireRootDialect(doc); err != nil {
		return nil, err
	}
	if err := loader.declareReferences(doc, resourceURL); err != nil {
		return nil, fmt.Errorf("inspect schema references: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	compiler.UseRegexpEngine(compileECMAScriptRegexp)
	compiler.UseLoader(loader)
	if err := compiler.AddResource(resourceURL, doc); err != nil {
		return nil, fmt.Errorf("add schema resource: %w", err)
	}
	sch, err := compiler.Compile(resourceURL)
	if err != nil {
		if contextErr := contextError(ctx); contextErr != nil {
			return nil, contextErr
		}
		return nil, err
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return sch, nil
}

func requireRootDialect(doc any) error {
	object, ok := doc.(map[string]any)
	if !ok {
		return errors.New("root schema must be an object with the Draft 2020-12 dialect")
	}
	dialect, ok := object["$schema"].(string)
	if !ok || dialect != draft2020Dialect {
		return fmt.Errorf("root schema $schema must equal %q", draft2020Dialect)
	}
	return validateDeclaredDialects(doc)
}

func validateDeclaredDialects(value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if declaration, exists := object["$schema"]; exists {
		dialect, ok := declaration.(string)
		if !ok || dialect != draft2020Dialect {
			return fmt.Errorf("schema resource $schema must equal %q", draft2020Dialect)
		}
	}

	for _, keyword := range []string{
		"additionalProperties", "contains", "contentSchema", "else", "if",
		"items", "not", "propertyNames", "then", "unevaluatedItems",
		"unevaluatedProperties",
	} {
		if child, exists := object[keyword]; exists {
			if err := validateDeclaredDialects(child); err != nil {
				return err
			}
		}
	}
	for _, keyword := range []string{"allOf", "anyOf", "oneOf", "prefixItems"} {
		children, _ := object[keyword].([]any)
		for _, child := range children {
			if err := validateDeclaredDialects(child); err != nil {
				return err
			}
		}
	}
	for _, keyword := range []string{
		"$defs", "definitions", "dependentSchemas", "patternProperties", "properties",
	} {
		children, _ := object[keyword].(map[string]any)
		for _, child := range children {
			if err := validateDeclaredDialects(child); err != nil {
				return err
			}
		}
	}
	return nil
}

type repositoryLoader struct {
	ctx             context.Context
	validator       *Validator
	declared        map[string]struct{}
	allowLocal      bool
	rootResourceURL string
}

func (l *repositoryLoader) Load(rawURL string) (any, error) {
	if err := contextError(l.ctx); err != nil {
		return nil, err
	}
	resourceURL, path, err := l.validator.canonicalFileResource(rawURL)
	if err != nil {
		return nil, err
	}
	if _, ok := l.declared[resourceURL]; !ok {
		return nil, fmt.Errorf("repository resource was not explicitly referenced: %s", resourceURL)
	}
	doc, err := l.validator.readStrictJSONFile(l.ctx, path, "referenced schema")
	if err != nil {
		return nil, fmt.Errorf("decode referenced schema %q: %w", resourceURL, err)
	}
	if err := validateDeclaredDialects(doc); err != nil {
		return nil, fmt.Errorf("validate referenced schema dialect %q: %w", resourceURL, err)
	}
	if err := l.declareReferences(doc, resourceURL); err != nil {
		return nil, fmt.Errorf("inspect referenced schema %q: %w", resourceURL, err)
	}
	return doc, nil
}

func (l *repositoryLoader) declareReferences(doc any, retrievalURL string) error {
	base, err := url.Parse(retrievalURL)
	if err != nil {
		return fmt.Errorf("parse schema resource URL: %w", err)
	}
	return l.walkReferences(doc, base)
}

func (l *repositoryLoader) walkReferences(value any, base *url.URL) error {
	if err := contextError(l.ctx); err != nil {
		return err
	}
	switch value := value.(type) {
	case map[string]any:
		objectBase := base
		if rawID, ok := value["$id"].(string); ok {
			if err := rejectTraversal(rawID); err != nil {
				return fmt.Errorf("$id %q: %w", rawID, err)
			}
			id, err := url.Parse(rawID)
			if err != nil {
				return fmt.Errorf("parse $id %q: %w", rawID, err)
			}
			objectBase = base.ResolveReference(id)
		}

		for _, keyword := range []string{"$schema", "$ref", "$dynamicRef", "$recursiveRef"} {
			rawReference, ok := value[keyword].(string)
			if !ok {
				continue
			}
			if err := rejectTraversal(rawReference); err != nil {
				return fmt.Errorf("%s %q: %w", keyword, rawReference, err)
			}
			reference, err := url.Parse(rawReference)
			if err != nil {
				return fmt.Errorf("parse %s %q: %w", keyword, rawReference, err)
			}
			target := objectBase.ResolveReference(reference)
			if strings.EqualFold(target.Scheme, "file") {
				targetResource := *target
				targetResource.Fragment = ""
				targetResource.RawFragment = ""
				resourceURL := targetResource.String()
				if resourceURL != l.rootResourceURL {
					resourceURL, _, err = l.validator.canonicalFileResource(target.String())
					if err != nil {
						return fmt.Errorf("resolve %s %q: %w", keyword, rawReference, err)
					}
				}
				if !l.allowLocal && resourceURL != l.rootResourceURL {
					return fmt.Errorf("captured schema external local reference is forbidden: %s", resourceURL)
				}
				if l.allowLocal {
					l.declared[resourceURL] = struct{}{}
				}
			}
		}

		for _, child := range value {
			if err := l.walkReferences(child, objectBase); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := l.walkReferences(child, base); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *Validator) canonicalFileResource(rawURL string) (string, string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("parse resource URL %q: %w", rawURL, err)
	}
	if !strings.EqualFold(u.Scheme, "file") {
		if strings.EqualFold(u.Scheme, "http") || strings.EqualFold(u.Scheme, "https") {
			return "", "", fmt.Errorf("network schema retrieval is forbidden: %s", rawURL)
		}
		return "", "", fmt.Errorf("unsupported schema resource URI scheme %q", u.Scheme)
	}
	if u.User != nil || u.Host != "" || u.Opaque != "" {
		return "", "", fmt.Errorf("external or opaque file URI is forbidden: %s", rawURL)
	}
	if u.RawQuery != "" {
		return "", "", fmt.Errorf("file URI query is forbidden: %s", rawURL)
	}
	path := pathFromFileURL(u)
	if !filepath.IsAbs(path) {
		return "", "", fmt.Errorf("file URI is not absolute: %s", rawURL)
	}
	path, err = v.relativeRepositoryPath(path, "schema resource")
	if err != nil {
		return "", "", err
	}
	return v.resourceURL(path), path, nil
}

func (v *Validator) resolveRequestedPath(rawPath, kind string) (string, error) {
	path, err := v.requestedAbsolutePath(rawPath, kind)
	if err != nil {
		return "", err
	}
	return v.relativeRepositoryPath(path, kind)
}

func (v *Validator) resolveCapturedSchemaPath(rawPath string) (string, error) {
	path, err := v.requestedAbsolutePath(rawPath, "captured schema")
	if err != nil {
		return "", err
	}
	return v.lexicalRepositoryPath(path, "captured schema")
}

func (v *Validator) requestedAbsolutePath(rawPath, kind string) (string, error) {
	if v == nil {
		return "", errors.New("schema validator is nil")
	}
	if v.rootErr != nil {
		return "", v.rootErr
	}
	if rawPath == "" {
		return "", fmt.Errorf("%s path is empty", kind)
	}
	if strings.IndexByte(rawPath, 0) >= 0 {
		return "", fmt.Errorf("%s path contains NUL", kind)
	}
	if err := rejectTraversal(rawPath); err != nil {
		return "", fmt.Errorf("%s path %q: %w", kind, rawPath, err)
	}

	u, err := url.Parse(rawPath)
	if err != nil {
		return "", fmt.Errorf("parse %s path %q: %w", kind, rawPath, err)
	}
	if u.Fragment != "" || u.RawQuery != "" {
		return "", fmt.Errorf("%s path must not contain a query or fragment: %q", kind, rawPath)
	}

	var path string
	switch {
	case u.Scheme == "":
		if u.Host != "" || u.User != nil || u.Opaque != "" {
			return "", fmt.Errorf("%s path is not a local path: %q", kind, rawPath)
		}
		path = filepath.FromSlash(u.Path)
	case strings.EqualFold(u.Scheme, "file"):
		if u.Host != "" || u.User != nil || u.Opaque != "" {
			return "", fmt.Errorf("%s file URI is external or opaque: %q", kind, rawPath)
		}
		path = pathFromFileURL(u)
		if !filepath.IsAbs(path) {
			return "", fmt.Errorf("%s file URI is not absolute: %q", kind, rawPath)
		}
	case strings.EqualFold(u.Scheme, "http") || strings.EqualFold(u.Scheme, "https"):
		return "", fmt.Errorf("network %s retrieval is forbidden: %q", kind, rawPath)
	default:
		return "", fmt.Errorf("unsupported %s URI scheme %q", kind, u.Scheme)
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(v.repoRoot, path)
	}
	return filepath.Clean(path), nil
}

func (v *Validator) relativeRepositoryPath(path, kind string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve %s path: %w", kind, err)
	}
	realPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return "", fmt.Errorf("resolve real %s path: %w", kind, err)
	}
	return v.lexicalRepositoryPath(realPath, kind)
}

func (v *Validator) lexicalRepositoryPath(path, kind string) (string, error) {
	relative, err := filepath.Rel(v.repoRoot, path)
	if err != nil {
		return "", fmt.Errorf("resolve repository-relative %s path: %w", kind, err)
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%s path escapes repository root: %s", kind, path)
	}
	return filepath.Clean(relative), nil
}

func (v *Validator) resourceURL(relativePath string) string {
	return fileURL(filepath.Join(v.repoRoot, relativePath))
}

func rejectTraversal(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	for _, path := range []string{u.Path, u.Opaque} {
		path = strings.ReplaceAll(path, `\`, "/")
		for _, component := range strings.Split(path, "/") {
			if component == ".." {
				return errors.New("parent-directory traversal is forbidden")
			}
		}
	}
	return nil
}

func fileURL(path string) string {
	slashPath := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	return (&url.URL{Scheme: "file", Path: slashPath}).String()
}

func pathFromFileURL(resourceURL *url.URL) string {
	path := resourceURL.Path
	if runtime.GOOS == "windows" && len(path) >= 3 && path[0] == '/' && path[2] == ':' {
		path = path[1:]
	}
	return filepath.FromSlash(path)
}

func (v *Validator) readStrictJSONFile(ctx context.Context, path, kind string) (any, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if v.root == nil {
		return nil, errors.New("repository root is not open")
	}
	file, err := v.root.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open rooted %s %q: %w", kind, path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect opened %s %q: %w", kind, path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("opened %s is not a regular file: %s", kind, path)
	}

	data, err := readAllContext(ctx, file)
	if err != nil {
		return nil, err
	}
	return decodeStrictJSON(ctx, data)
}

func readAllContext(ctx context.Context, reader io.Reader) ([]byte, error) {
	var output bytes.Buffer
	buffer := make([]byte, 32*1024)
	for {
		if err := contextError(ctx); err != nil {
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

func decodeStrictJSON(ctx context.Context, data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, errors.New("JSON contains invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := decodeJSONValue(ctx, decoder)
	if err != nil {
		return nil, err
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("multiple top-level JSON values")
		}
		return nil, fmt.Errorf("invalid data after top-level JSON value: %w", err)
	}
	return value, nil
}

func decodeJSONValue(ctx context.Context, decoder *json.Decoder) (any, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}

	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			if err := contextError(ctx); err != nil {
				return nil, err
			}
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("JSON object member name is not a string")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, fmt.Errorf("duplicate JSON object member %q", key)
			}
			value, err := decodeJSONValue(ctx, decoder)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		closing, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if closing != json.Delim('}') {
			return nil, errors.New("JSON object is not terminated")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := decodeJSONValue(ctx, decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		closing, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		if closing != json.Delim(']') {
			return nil, errors.New("JSON array is not terminated")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	return ctx.Err()
}

type ecmaRegexp struct {
	regexp *regexp2.Regexp
}

func (r *ecmaRegexp) MatchString(value string) bool {
	// JSON Schema patterns use ECMA-262 UTF-16 semantics. regexp2 consumes Go
	// runes. Current contract patterns constrain ASCII identifiers and paths, so
	// fail closed where rune matching can otherwise accept a different value.
	for _, character := range value {
		if character == '\u2028' || character == '\u2029' || character > '\uFFFF' {
			return false
		}
	}
	matched, err := r.regexp.MatchString(value)
	return err == nil && matched
}

func (r *ecmaRegexp) String() string {
	return r.regexp.String()
}

func compileECMAScriptRegexp(pattern string) (jsonschema.Regexp, error) {
	compiled, err := regexp2.Compile(pattern, regexp2.ECMAScript)
	if err != nil {
		return nil, err
	}
	compiled.MatchTimeout = regexpMatchTimeout
	return &ecmaRegexp{regexp: compiled}, nil
}

// regexpMatchTimeout bounds backtracking for small, authored contract patterns.
const regexpMatchTimeout = 250 * time.Millisecond
