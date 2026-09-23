package schemavalidator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAssertedFormats(t *testing.T) {
	root := schemaRepository(t, map[string]string{
		"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "string",
  "format": "date-time"
}`,
	})
	validator := New(root)
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`"2026-08-09T12:30:00Z"`)); err != nil {
		t.Fatalf("valid date-time rejected: %v", err)
	}
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`"not-a-date-time"`)); err == nil {
		t.Fatal("invalid asserted format was accepted")
	}
}

func TestECMAScriptPatternEngine(t *testing.T) {
	root := schemaRepository(t, map[string]string{
		"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "string",
  "pattern": "^(?=a)a$"
}`,
	})
	validator := New(root)
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`"a"`)); err != nil {
		t.Fatalf("ECMAScript lookahead pattern rejected: %v", err)
	}
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`"b"`)); err == nil {
		t.Fatal("ECMAScript lookahead mismatch was accepted")
	}
}

func TestECMAScriptPatternRejectsRuneSemanticMismatches(t *testing.T) {
	root := schemaRepository(t, map[string]string{
		"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "string",
  "pattern": "^.$"
}`,
	})
	validator := New(root)
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`"A"`)); err != nil {
		t.Fatalf("ASCII pattern value rejected: %v", err)
	}
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "line separator", value: "\u2028"},
		{name: "paragraph separator", value: "\u2029"},
		{name: "astral character", value: "\U0001F600"},
	} {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if err := validator.ValidateBytes(context.Background(), "schema.json", data); err == nil {
				t.Fatalf("pattern accepted rune-semantic mismatch %U", []rune(test.value)[0])
			}
		})
	}
}

func TestBuiltInDraft2020Metaschema(t *testing.T) {
	root := schemaRepository(t, map[string]string{
		"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$ref": "https://json-schema.org/draft/2020-12/schema"
}`,
	})
	validator := New(root)
	instance := []byte(`{"type":"object","properties":{"name":{"type":"string"}}}`)
	if err := validator.ValidateBytes(context.Background(), "schema.json", instance); err != nil {
		t.Fatalf("built-in Draft 2020-12 metaschema was unavailable: %v", err)
	}
}

func TestRequiresExactDraft2020Dialect(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		data   string
	}{
		{name: "missing", schema: `{"type":"integer"}`, data: `1`},
		{name: "Draft 7 prefixItems negative control", schema: `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "array",
  "prefixItems": [false]
}`, data: `[1]`},
		{name: "2019-09", schema: `{"$schema":"https://json-schema.org/draft/2019-09/schema","type":"integer"}`, data: `1`},
		{name: "HTTP alias", schema: `{"$schema":"http://json-schema.org/draft/2020-12/schema","type":"integer"}`, data: `1`},
		{name: "fragment alias", schema: `{"$schema":"https://json-schema.org/draft/2020-12/schema#","type":"integer"}`, data: `1`},
		{name: "unknown", schema: `{"$schema":"https://example.invalid/dialect","type":"integer"}`, data: `1`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := schemaRepository(t, map[string]string{"schema.json": test.schema})
			if err := New(root).ValidateBytes(context.Background(), "schema.json", []byte(test.data)); err == nil {
				t.Fatal("non-exact schema dialect was accepted")
			}
		})
	}
}

func TestRejectsNonDraft2020NestedAndReferencedResources(t *testing.T) {
	t.Run("nested resource", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{
			"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$defs": {
    "legacy": {
      "$id": "legacy",
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "integer"
    }
  }
}`,
		})
		if err := New(root).ValidateBytes(context.Background(), "schema.json", []byte(`1`)); err == nil {
			t.Fatal("nested Draft 7 resource was accepted")
		}
	})

	t.Run("referenced resource", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{
			"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$ref": "legacy.json"
}`,
			"legacy.json": `{
  "$schema": "https://json-schema.org/draft/2019-09/schema",
  "type": "integer"
}`,
		})
		if err := New(root).ValidateBytes(context.Background(), "schema.json", []byte(`1`)); err == nil {
			t.Fatal("referenced 2019-09 resource was accepted")
		}
	})
}

func TestAllowsExplicitRepositoryLocalReferences(t *testing.T) {
	t.Run("relative", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{
			"schemas/root.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {"value": {"$ref": "defs.json#/$defs/value"}},
  "required": ["value"]
}`,
			"schemas/defs.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$defs": {"value": {"type": "integer"}}
}`,
		})
		validator := New(root)
		if err := validator.ValidateBytes(context.Background(), "schemas/root.json", []byte(`{"value":7}`)); err != nil {
			t.Fatalf("relative repository reference rejected: %v", err)
		}
		if err := validator.ValidateBytes(context.Background(), "schemas/root.json", []byte(`{"value":"7"}`)); err == nil {
			t.Fatal("referenced relative schema was not applied")
		}
	})

	t.Run("file URI", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{
			"defs.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$defs": {"value": {"type": "integer"}}
}`,
		})
		rootSchema := fmt.Sprintf(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$ref": %q
}`, fileURL(filepath.Join(root, "defs.json"))+"#/$defs/value")
		writeRepositoryFile(t, root, "root.json", rootSchema)

		validator := New(root)
		if err := validator.ValidateBytes(context.Background(), "root.json", []byte(`11`)); err != nil {
			t.Fatalf("repository-local file URI reference rejected: %v", err)
		}
	})
}

func TestRejectsNonLocalAndUnsafeReferences(t *testing.T) {
	var networkRequests atomic.Int64
	handler := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		networkRequests.Add(1)
		_, _ = writer.Write([]byte(`{"type":"integer"}`))
	})
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)
	httpsServer := httptest.NewTLSServer(handler)
	t.Cleanup(httpsServer.Close)
	originalTransport := http.DefaultTransport
	http.DefaultTransport = httpsServer.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = originalTransport })

	tests := []struct {
		name      string
		reference func(root, external string) string
	}{
		{name: "HTTP", reference: func(_, _ string) string { return httpServer.URL + "/schema.json" }},
		{name: "HTTPS with fragment", reference: func(_, _ string) string { return httpsServer.URL + "/schema.json#/$defs/value" }},
		{name: "unsupported scheme", reference: func(_, _ string) string { return "ftp://example.invalid/schema.json" }},
		{name: "external file URI", reference: func(_, external string) string { return fileURL(external) + "#/$defs/value" }},
		{name: "absolute external path", reference: func(_, external string) string { return external }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			externalRoot := t.TempDir()
			external := filepath.Join(externalRoot, "external.json")
			if err := os.WriteFile(external, []byte(`{"$defs":{"value":{"type":"integer"}}}`), 0o644); err != nil {
				t.Fatal(err)
			}
			root := schemaRepository(t, nil)
			reference := test.reference(root, external)
			writeRepositoryFile(t, root, "root.json", fmt.Sprintf(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$ref": %q
}`, reference))

			if err := New(root).ValidateBytes(context.Background(), "root.json", []byte(`1`)); err == nil {
				t.Fatalf("unsafe reference %q was accepted", reference)
			}
		})
	}
	if count := networkRequests.Load(); count != 0 {
		t.Fatalf("schema validation made %d forbidden network requests", count)
	}
}

func TestRejectsTraversalEvenWhenItStaysInsideRepository(t *testing.T) {
	t.Run("reference", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{
			"defs.json": `{"type":"integer"}`,
			"schemas/root.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$ref": "../defs.json"
}`,
		})
		if err := New(root).ValidateBytes(context.Background(), "schemas/root.json", []byte(`1`)); err == nil {
			t.Fatal("parent-directory traversal was accepted")
		}
	})

	t.Run("base identifier", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{
			"defs.json": `{"type":"integer"}`,
			"schemas/root.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "../",
  "$ref": "defs.json"
}`,
		})
		if err := New(root).ValidateBytes(context.Background(), "schemas/root.json", []byte(`1`)); err == nil {
			t.Fatal("parent-directory traversal through $id was accepted")
		}
	})
}

func TestRejectsReferencedSymlinkEscape(t *testing.T) {
	externalRoot := t.TempDir()
	external := filepath.Join(externalRoot, "external.json")
	if err := os.WriteFile(external, []byte(`{"type":"integer"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	root := schemaRepository(t, map[string]string{
		"root.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$ref": "linked.json"
}`,
	})
	if err := os.Symlink(external, filepath.Join(root, "linked.json")); err != nil {
		t.Fatal(err)
	}
	if err := New(root).ValidateBytes(context.Background(), "root.json", []byte(`1`)); err == nil {
		t.Fatal("schema reference escaping through a symlink was accepted")
	}
}

func TestRootedOpenControls(t *testing.T) {
	t.Run("requested schema symlink escape", func(t *testing.T) {
		externalRoot := t.TempDir()
		external := filepath.Join(externalRoot, "schema.json")
		if err := os.WriteFile(external, []byte(`{"type":"integer"}`), 0o644); err != nil {
			t.Fatal(err)
		}
		root := schemaRepository(t, nil)
		if err := os.Symlink(external, filepath.Join(root, "schema.json")); err != nil {
			t.Fatal(err)
		}
		if err := New(root).ValidateBytes(context.Background(), "schema.json", []byte(`1`)); err == nil {
			t.Fatal("rooted schema open followed an external symlink")
		}
	})

	t.Run("requested instance symlink escape", func(t *testing.T) {
		externalRoot := t.TempDir()
		external := filepath.Join(externalRoot, "instance.json")
		if err := os.WriteFile(external, []byte(`1`), 0o644); err != nil {
			t.Fatal(err)
		}
		root := schemaRepository(t, map[string]string{"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "integer"
}`})
		if err := os.Symlink(external, filepath.Join(root, "instance.json")); err != nil {
			t.Fatal(err)
		}
		if err := New(root).ValidateFile(context.Background(), "schema.json", "instance.json"); err == nil {
			t.Fatal("rooted instance open followed an external symlink")
		}
	})

	t.Run("internal symlinks", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{
			"schemas/target.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "integer"
}`,
			"instances/target.json": `1`,
		})
		if err := os.Symlink("target.json", filepath.Join(root, "schemas", "linked.json")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("target.json", filepath.Join(root, "instances", "linked.json")); err != nil {
			t.Fatal(err)
		}
		if err := New(root).ValidateFile(context.Background(), "schemas/linked.json", "instances/linked.json"); err != nil {
			t.Fatalf("rooted open rejected contained symlinks: %v", err)
		}
	})

	t.Run("non-regular instance", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object"
}`})
		if err := os.Mkdir(filepath.Join(root, "instance"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := New(root).ValidateFile(context.Background(), "schema.json", "instance"); err == nil {
			t.Fatal("rooted open accepted a non-regular instance")
		}
	})
}

func TestRootDescriptorSurvivesRepositoryRename(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "repository")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	writeRepositoryFile(t, root, "schema.json", `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "integer"
}`)
	validator := New(root)

	movedRoot := filepath.Join(base, "moved-repository")
	if err := os.Rename(root, movedRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	writeRepositoryFile(t, root, "schema.json", `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "string"
}`)

	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`1`)); err != nil {
		t.Fatalf("validator lost its descriptor-backed repository root: %v", err)
	}
}

func TestCloseIsIdempotentAndRejectsFurtherValidation(t *testing.T) {
	root := schemaRepository(t, map[string]string{"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "integer"
}`})
	validator := New(root)
	if err := validator.Close(); err != nil {
		t.Fatalf("close validator: %v", err)
	}
	if err := validator.Close(); err != nil {
		t.Fatalf("close validator again: %v", err)
	}
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`1`)); err == nil {
		t.Fatal("closed validator accepted file-backed validation")
	}
	if err := validator.ValidateCapturedBytes(context.Background(), "schema.json", []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"integer"}`), []byte(`1`)); err == nil {
		t.Fatal("closed validator accepted captured validation")
	}
}

func TestRepositoryLoaderRejectsUndeclaredResource(t *testing.T) {
	root := schemaRepository(t, map[string]string{
		"undeclared.json": `{"type":"integer"}`,
	})
	validator := New(root)
	loader := &repositoryLoader{
		ctx:       context.Background(),
		validator: validator,
		declared:  make(map[string]struct{}),
	}
	if _, err := loader.Load(fileURL(filepath.Join(root, "undeclared.json"))); err == nil {
		t.Fatalf("expected undeclared repository resource rejection, got %v", err)
	}
}

func TestRejectsMalformedAndDuplicateJSON(t *testing.T) {
	t.Run("malformed schema", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{"schema.json": `{"type":`})
		if err := New(root).ValidateBytes(context.Background(), "schema.json", []byte(`1`)); err == nil {
			t.Fatal("malformed schema was accepted")
		}
	})

	t.Run("duplicate schema member", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{"schema.json": `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"integer","type":"string"}`})
		if err := New(root).ValidateBytes(context.Background(), "schema.json", []byte(`1`)); err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("expected duplicate schema rejection, got %v", err)
		}
	})

	t.Run("duplicate referenced schema member", func(t *testing.T) {
		root := schemaRepository(t, map[string]string{
			"schema.json": `{"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"defs.json"}`,
			"defs.json":   `{"type":"integer","type":"string"}`,
		})
		if err := New(root).ValidateBytes(context.Background(), "schema.json", []byte(`1`)); err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("expected duplicate referenced schema rejection, got %v", err)
		}
	})

	root := schemaRepository(t, map[string]string{"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object"
}`})
	validator := New(root)
	t.Run("malformed instance", func(t *testing.T) {
		if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`{"value":`)); err == nil {
			t.Fatal("malformed instance was accepted")
		}
	})
	t.Run("duplicate instance member", func(t *testing.T) {
		if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`{"value":1,"value":2}`)); err == nil || !strings.Contains(err.Error(), "duplicate") {
			t.Fatalf("expected duplicate instance rejection, got %v", err)
		}
	})
	t.Run("invalid UTF-8 instance", func(t *testing.T) {
		data := []byte{'{', '"', 'v', 'a', 'l', 'u', 'e', '"', ':', '"', 0xff, '"', '}'}
		if err := validator.ValidateBytes(context.Background(), "schema.json", data); err == nil {
			t.Fatal("invalid UTF-8 instance was accepted")
		}
	})
}

func TestInstanceSchemaCannotSelectValidator(t *testing.T) {
	root := schemaRepository(t, map[string]string{
		"required.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["required_value"]
}`,
		"permissive.json": `true`,
	})
	instance := fmt.Sprintf(`{"$schema":%q}`, fileURL(filepath.Join(root, "permissive.json")))
	if err := New(root).ValidateBytes(context.Background(), "required.json", []byte(instance)); err == nil {
		t.Fatal("instance $schema replaced the explicitly selected schema")
	}
}

func TestContextCancellation(t *testing.T) {
	root := schemaRepository(t, map[string]string{
		"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "integer"
}`,
		"instance.json": `1`,
	})
	validator := New(root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := validator.ValidateBytes(ctx, "schema.json", []byte(`1`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("ValidateBytes did not preserve cancellation: %v", err)
	}
	if err := validator.ValidateFile(ctx, "schema.json", "instance.json"); !errors.Is(err, context.Canceled) {
		t.Fatalf("ValidateFile did not preserve cancellation: %v", err)
	}
}

func TestStrictDecoderPreservesLargeNumber(t *testing.T) {
	value, err := decodeStrictJSON(context.Background(), []byte(`1e400`))
	if err != nil {
		t.Fatalf("strict decoder rejected a valid large number: %v", err)
	}
	number, ok := value.(json.Number)
	if !ok || number.String() != "1e400" {
		t.Fatalf("decoded number = %#v, want json.Number(1e400)", value)
	}

	root := schemaRepository(t, map[string]string{
		"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "number"
}`,
	})
	if err := New(root).ValidateBytes(context.Background(), "schema.json", []byte(`1e400`)); err != nil {
		t.Fatalf("schema validation rejected a valid large number: %v", err)
	}
}

func TestValidateCapturedBytesUsesCapturedSchema(t *testing.T) {
	capturedSchema := []byte(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "integer"
}`)
	root := schemaRepository(t, map[string]string{
		"schema.json": string(capturedSchema),
	})
	validator := New(root)
	writeRepositoryFile(t, root, "schema.json", `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "string"
}`)

	if err := validator.ValidateCapturedBytes(context.Background(), "schema.json", capturedSchema, []byte(`7`)); err != nil {
		t.Fatalf("captured schema bytes were not used: %v", err)
	}
	if err := validator.ValidateCapturedBytes(context.Background(), "schema.json", capturedSchema, []byte(`"seven"`)); err == nil {
		t.Fatal("captured integer schema accepted a string")
	}
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`"seven"`)); err != nil {
		t.Fatalf("disk mutation was not visible to normal validation: %v", err)
	}
}

func TestValidateCapturedBytesReferencePolicy(t *testing.T) {
	root := schemaRepository(t, map[string]string{
		"defs.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "integer"
}`,
	})
	validator := New(root)

	internalSchema := []byte(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$defs": {"value": {"type": "integer"}},
  "$ref": "#/$defs/value"
}`)
	if err := validator.ValidateCapturedBytes(context.Background(), "snapshot/schema.json", internalSchema, []byte(`7`)); err != nil {
		t.Fatalf("captured internal fragment reference rejected: %v", err)
	}

	externalSchema := []byte(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$ref": "defs.json"
}`)
	if err := validator.ValidateCapturedBytes(context.Background(), "schema.json", externalSchema, []byte(`7`)); err == nil {
		t.Fatal("captured schema loaded an external local reference")
	}
}

func TestValidationWorkerLimitAndQueuedCancellation(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAll()

	started := make(chan struct{}, validationWorkerLimit)
	results := make(chan error, validationWorkerLimit)
	var active atomic.Int64
	for range validationWorkerLimit {
		go func() {
			results <- runValidationWorker(context.Background(), func() error {
				active.Add(1)
				started <- struct{}{}
				defer active.Add(-1)
				<-release
				return nil
			})
		}()
	}
	for range validationWorkerLimit {
		<-started
	}
	if got := active.Load(); got != validationWorkerLimit {
		t.Fatalf("active workers = %d, want %d", got, validationWorkerLimit)
	}

	var queuedStarted atomic.Int64
	for range validationWorkerLimit * 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		err := runValidationWorker(ctx, func() error {
			queuedStarted.Add(1)
			return nil
		})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued validation did not preserve cancellation: %v", err)
		}
	}
	if got := queuedStarted.Load(); got != 0 {
		t.Fatalf("%d queued workers started above the limit", got)
	}

	releaseAll()
	for range validationWorkerLimit {
		if err := <-results; err != nil {
			t.Fatalf("bounded worker failed: %v", err)
		}
	}
	if got := active.Load(); got != 0 {
		t.Fatalf("active workers after release = %d", got)
	}
}

func TestCatastrophicPatternCancellationAndBound(t *testing.T) {
	const pattern = `^(a|aa)+$`
	value := strings.Repeat("a", 256) + "!"
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	root := schemaRepository(t, map[string]string{
		"schema.json":   fmt.Sprintf(`{"$schema":%q,"type":"string","pattern":%q}`, draft2020Dialect, pattern),
		"instance.json": string(data),
	})
	validator := New(root)
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`"a"`)); err != nil {
		t.Fatalf("compile catastrophic-pattern control schema: %v", err)
	}

	for _, test := range []struct {
		name     string
		validate func(context.Context) error
	}{
		{name: "ValidateBytes", validate: func(ctx context.Context) error {
			return validator.ValidateBytes(ctx, "schema.json", data)
		}},
		{name: "ValidateFile", validate: func(ctx context.Context) error {
			return validator.ValidateFile(ctx, "schema.json", "instance.json")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			started := time.Now()
			err := test.validate(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected deadline error, got %v", err)
			}
			if elapsed := time.Since(started); elapsed >= time.Second {
				t.Fatalf("canceled validation returned after %s", elapsed)
			}
		})
	}

	compiled, err := compileECMAScriptRegexp(pattern)
	if err != nil {
		t.Fatal(err)
	}
	bounded, ok := compiled.(*ecmaRegexp)
	if !ok || bounded.regexp.MatchTimeout != regexpMatchTimeout {
		t.Fatalf("regexp timeout is not fixed at %s", regexpMatchTimeout)
	}
	started := time.Now()
	if compiled.MatchString(value) {
		t.Fatal("catastrophic pattern unexpectedly matched")
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("bounded regexp returned after %s", elapsed)
	}
}

func TestCompiledSchemaCacheAndConcurrentUse(t *testing.T) {
	root := schemaRepository(t, map[string]string{
		"schema.json": `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "integer"
}`,
	})
	validator := New(root)
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	writeRepositoryFile(t, root, "schema.json", `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "string"
}`)
	if err := validator.ValidateBytes(context.Background(), "schema.json", []byte(`2`)); err != nil {
		t.Fatalf("compiled schema was not retained in the validator cache: %v", err)
	}

	const callers = 24
	var wait sync.WaitGroup
	errorsByCaller := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsByCaller <- validator.ValidateBytes(context.Background(), "schema.json", []byte(`3`))
		}()
	}
	wait.Wait()
	close(errorsByCaller)
	for err := range errorsByCaller {
		if err != nil {
			t.Fatalf("concurrent validation failed: %v", err)
		}
	}
}

func schemaRepository(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for path, contents := range files {
		writeRepositoryFile(t, root, path, contents)
	}
	return root
}

func writeRepositoryFile(t *testing.T, root, path, contents string) {
	t.Helper()
	fullPath := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}
