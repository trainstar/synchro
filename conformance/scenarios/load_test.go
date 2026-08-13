package scenarios

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/internal/schemavalidator"
)

func TestLoadFileStrictAndRoundTrip(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	data := scenarioFixture("SCN-LOAD-001", "Valid scenario")
	root := scenarioRepository(t, map[string][]byte{path: data})

	scenario, err := LoadFile(context.Background(), root, path)
	if err != nil {
		t.Fatalf("load valid scenario: %v", err)
	}
	if scenario.ID != "SCN-LOAD-001" {
		t.Fatalf("scenario ID = %q", scenario.ID)
	}
	if scenario.sourcePath != path || !bytes.Equal(scenario.sourceBytes, data) {
		t.Fatal("loaded scenario did not preserve its exact source")
	}
	for _, target := range []string{"test-conformance", "test-blackbox"} {
		if _, exists := scenario.makeTargets[target]; !exists {
			t.Fatalf("loaded scenario did not capture grouped Make target %q", target)
		}
	}
	if scenario.ProofObligations[0].SupportCellID != nil || scenario.ProofObligations[0].FaultPlanID != nil {
		t.Fatal("required nullable IDs did not decode as nil pointers")
	}
	if scenario.WireExpectations[0].ErrorCode != nil {
		t.Fatal("required nullable error_code did not decode as a nil pointer")
	}

	encoded, err := json.Marshal(scenario)
	if err != nil {
		t.Fatalf("marshal loaded scenario: %v", err)
	}
	var original any
	if err := jsonstrict.Decode(data, &original); err != nil {
		t.Fatal(err)
	}
	var roundTrip any
	if err := jsonstrict.Decode(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTrip, original) {
		t.Fatalf("round-trip document changed\nencoded: %s", encoded)
	}
}

func TestScenarioStructJSONTags(t *testing.T) {
	tests := []struct {
		value any
		tags  []string
	}{
		{Scenario{}, []string{"$schema", "schema_version", "id", "title", "description,omitempty", "requirement_ids", "normative_references", "proof_types", "proof_obligations", "ownership", "model", "barrier_plan", "fault_plans", "replay", "negative_controls", "steps", "wire_expectations", "assertions"}},
		{Operation{}, []string{"contract_operation", "name", "payload"}},
		{ProofObligation{}, []string{"obligation_id", "requirement_ids", "assertion_ids", "proof_type", "support_cell_id", "artifact_inventory_ids", "performance_budget_ids", "required_measurement_ids", "required_vector_set_ids", "make_target", "argv", "fault_plan_id", "control_id"}},
		{Ownership{}, []string{"scenario_id", "requirement_id", "proof_obligation_id", "assertion_id", "proof_type", "support_cell_id"}},
		{ModelSpec{}, []string{"setup", "expected_state"}},
		{ModelExpectation{}, []string{"id", "predicate", "state_facts,omitempty"}},
		{StateFacts{}, []string{"transaction_count,omitempty", "row_count,omitempty", "scope_count,omitempty", "rebuild_count,omitempty", "batch_count,omitempty", "mutation_count,omitempty", "configured_limits,omitempty", "transactions,omitempty", "registry,omitempty", "stream,omitempty", "rows,omitempty", "scopes,omitempty", "poison,omitempty", "rebuilds,omitempty", "clients,omitempty"}},
		{ConfiguredLimitsFact{}, []string{"max_scope_fanout", "max_impact_rows", "pull_maximum", "rebuild_maximum", "compaction_batch_maximum", "backfill_batch_maximum"}},
		{TransactionFact{}, []string{"stream_generation", "commit_lsn", "end_lsn", "registry_generation", "lifecycle", "event_ordinals"}},
		{RegistryFact{}, []string{"current_generation"}},
		{StreamFact{}, []string{"materialized_stream_generation", "materialized_kind", "materialized_commit_lsn", "acknowledged_end_lsn"}},
		{RowFact{}, []string{"table_id", "canonical_wire_json", "version", "checksum"}},
		{ScopeFact{}, []string{"scope_id", "membership_generation", "cardinality", "effect_versions"}},
		{PoisonFact{}, []string{"stream_generation", "commit_lsn", "relation", "reason", "lifecycle"}},
		{RebuildFact{}, []string{"user_id", "client_id", "scope_id", "rebuild_id", "page_limit", "staged_row_count", "page_count", "next_row_ordinal", "has_continuation", "has_final_cursor", "status"}},
		{ClientDurabilityFact{}, []string{"user_id", "client_id", "current_schema,omitempty", "row_count,omitempty", "provenance_count,omitempty", "checkpoint_count,omitempty", "queue_count,omitempty", "outcome_count,omitempty", "sealed_batch_count,omitempty", "rebuild_attempt_count,omitempty", "provenance,omitempty", "checkpoints,omitempty", "queue,omitempty", "outcomes,omitempty"}},
		{SchemaFact{}, []string{"version", "hash"}},
		{ProvenanceFact{}, []string{"table_id", "canonical_wire_json", "scopes", "version"}},
		{CheckpointFact{}, []string{"scope_id", "has_cursor", "has_checksum", "verified"}},
		{QueuedMutationFact{}, []string{"mutation_id", "table_id", "canonical_wire_json", "authored_schema", "operation", "base_version", "client_version", "authored_columns", "local_order", "status"}},
		{FieldFact{}, []string{"field_id", "type", "wire_json"}},
		{MutationOutcomeFact{}, []string{"mutation_id", "state", "reason"}},
		{Predicate{}, []string{"contract_predicate", "name", "payload"}},
		{BarrierPlan{}, []string{"barriers"}},
		{Barrier{}, []string{"id", "name", "release_order", "participants"}},
		{FaultPlan{}, []string{"id", "requirement_id", "fault_id", "control_id", "barrier_id", "expected_assertion_ids", "injection"}},
		{InjectionRecipe{}, []string{"mechanism", "target", "operator", "parameters"}},
		{InjectionParameters{}, []string{"scenario", "defect", "precondition,omitempty"}},
		{ReplaySpec{}, []string{"mode", "seed_required", "barrier_trace_required"}},
		{NegativeControl{}, []string{"control_id", "requirement_id", "fault_id", "subject_artifact_inventory_ids", "detected_by"}},
		{Step{}, []string{"id", "phase", "transport", "description,omitempty", "operation", "expected_outcome"}},
		{ExpectedOutcome{}, []string{"disposition", "error_code,omitempty"}},
		{WireExpectation{}, []string{"step_id", "assertion_id", "contract_case", "http_status", "error_code", "retryable"}},
		{Assertion{}, []string{"id", "requirement_ids", "description", "expectation_ids", "predicate", "oracle", "detects_control_ids"}},
		{Oracle{}, []string{"kind", "expected_source", "observed_source"}},
		{Catalog{}, []string{"schema_version", "scenarios"}},
		{ScenarioEntry{}, []string{"scenario_id", "path", "sha256"}},
		{contract.NormativeReference{}, []string{"path", "anchor"}},
	}
	for _, test := range tests {
		typeOf := reflect.TypeOf(test.value)
		t.Run(typeOf.Name(), func(t *testing.T) {
			if err := validateJSONTags(typeOf, test.tags); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJSONTagCheckRejectsMissingDuplicateAndMismatchedTags(t *testing.T) {
	type missing struct {
		Value string
	}
	type mismatched struct {
		Value string `json:"camelCase"`
	}
	duplicateType := reflect.StructOf([]reflect.StructField{
		{Name: "First", Type: reflect.TypeOf(""), Tag: `json:"same"`},
		{Name: "Second", Type: reflect.TypeOf(""), Tag: `json:"same"`},
	})
	for _, test := range []struct {
		name   string
		typeOf reflect.Type
		tags   []string
	}{
		{"missing", reflect.TypeOf(missing{}), []string{"value"}},
		{"duplicate", duplicateType, []string{"same", "same"}},
		{"mismatched", reflect.TypeOf(mismatched{}), []string{"snake_case"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateJSONTags(test.typeOf, test.tags); err == nil {
				t.Fatal("invalid JSON tags were accepted")
			}
		})
	}
}

func TestLoadAllSortsAndRejectsDuplicateIDsAndZeroFiles(t *testing.T) {
	root := scenarioRepository(t, map[string][]byte{
		"conformance/scenarios/z.json":        scenarioFixture("SCN-SORT-Z-002", "Zeta scenario"),
		"conformance/scenarios/nested/a.json": scenarioFixture("SCN-SORT-A-001", "Alpha scenario"),
	})
	scenarios, err := LoadAll(context.Background(), root)
	if err != nil {
		t.Fatalf("load all scenarios: %v", err)
	}
	if got := []contract.ScenarioID{scenarios[0].ID, scenarios[1].ID}; !reflect.DeepEqual(got, []contract.ScenarioID{"SCN-SORT-A-001", "SCN-SORT-Z-002"}) {
		t.Fatalf("sorted IDs = %v", got)
	}

	duplicateRoot := scenarioRepository(t, map[string][]byte{
		"conformance/scenarios/one.json": scenarioFixture("SCN-DUPLICATE-001", "First scenario"),
		"conformance/scenarios/two.json": scenarioFixture("SCN-DUPLICATE-001", "Other scenario"),
	})
	requireLoadError(t, func() error {
		_, err := LoadAll(context.Background(), duplicateRoot)
		return err
	})

	emptyRoot := scenarioRepository(t, nil)
	requireLoadError(t, func() error {
		_, err := LoadAll(context.Background(), emptyRoot)
		return err
	})
}

func TestLoadRejectsMalformedDuplicateUnknownAndInvalidDocuments(t *testing.T) {
	valid := scenarioFixture("SCN-STRICT-001", "Strict scenario")
	tests := []struct {
		name string
		data []byte
	}{
		{"malformed", valid[:len(valid)-3]},
		{"duplicate key", bytes.Replace(valid, []byte(`"title": "Strict scenario",`), []byte(`"title": "Strict scenario", "title": "Other",`), 1)},
		{"trailing data", append(append([]byte(nil), valid...), []byte(` {}`)...)},
		{"unknown field", bytes.Replace(valid, []byte(`"title": "Strict scenario",`), []byte(`"title": "Strict scenario", "unknown_field": true,`), 1)},
		{"changed schema URI", bytes.Replace(valid, []byte(`https://synchro.dev/conformance/schemas/scenario-v2.schema.json`), []byte(`https://example.test/unknown.schema.json`), 1)},
		{"invalid UTF-8", append(append([]byte(nil), valid[:len(valid)-2]...), 0xff, '}', '\n')},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := "conformance/scenarios/invalid.json"
			root := scenarioRepository(t, map[string][]byte{path: test.data})
			requireLoadError(t, func() error {
				_, err := LoadFile(context.Background(), root, path)
				return err
			})
		})
	}
}

func TestLoadRejectsScenarioFilenameIdentityMismatch(t *testing.T) {
	for _, test := range []struct {
		path string
		id   string
	}{
		{"conformance/scenarios/server/wal-order-001.json", "SCN-WAL-OTHER-001"},
		{"conformance/scenarios/performance/warm-connect-001.json", "SCN-WARM-CONNECT-001"},
	} {
		root := scenarioRepository(t, map[string][]byte{test.path: scenarioFixture(test.id, "Mismatched identity")})
		requireLoadError(t, func() error {
			_, err := LoadFile(context.Background(), root, test.path)
			return err
		})
	}
}

func TestLoadRejectsUnknownOrChangedSchemaDialect(t *testing.T) {
	for _, dialect := range []string{
		"https://json-schema.org/draft/2019-09/schema",
		"https://example.test/schema-dialect",
	} {
		t.Run(filepath.Base(dialect), func(t *testing.T) {
			path := "conformance/scenarios/valid.json"
			root := scenarioRepository(t, map[string][]byte{path: scenarioFixture("SCN-DIALECT-001", "Dialect scenario")})
			schemaPath := filepath.Join(root, filepath.FromSlash(scenarioSchemaPath))
			schema, err := os.ReadFile(schemaPath)
			if err != nil {
				t.Fatal(err)
			}
			schema = bytes.Replace(schema, []byte("https://json-schema.org/draft/2020-12/schema"), []byte(dialect), 1)
			if err := os.WriteFile(schemaPath, schema, 0o644); err != nil {
				t.Fatal(err)
			}
			requireLoadError(t, func() error {
				_, err := LoadFile(context.Background(), root, path)
				return err
			})
		})
	}
}

func TestLoadRejectsReadinessClaimKeysAtAnyDepth(t *testing.T) {
	valid := scenarioFixture("SCN-CLAIM-001", "Claim scenario")
	mutants := [][]byte{
		bytes.Replace(valid, []byte(`{"seed": 1}`), []byte(`{"nested":[{"READY":true}]}`), 1),
		bytes.Replace(valid, []byte(`{"seed": 1}`), []byte(`{"nested":[{"ACCEPTED___FLAKY":true}]}`), 1),
	}
	for index, mutant := range mutants {
		t.Run(fmt.Sprintf("mutant-%d", index), func(t *testing.T) {
			path := "conformance/scenarios/claim.json"
			root := scenarioRepository(t, map[string][]byte{path: mutant})
			requireLoadError(t, func() error {
				_, err := LoadFile(context.Background(), root, path)
				return err
			})
		})
	}
}

func TestLoadRejectsUnsafePathsAndNonregularFiles(t *testing.T) {
	root := scenarioRepository(t, map[string][]byte{
		"conformance/scenarios/valid.json": scenarioFixture("SCN-PATH-001", "Path scenario"),
	})
	for _, path := range []string{
		"",
		"/conformance/scenarios/valid.json",
		`conformance\scenarios\valid.json`,
		"conformance/scenarios/../valid.json",
		"conformance/scenarios/./valid.json",
		"conformance//scenarios/valid.json",
		"conformance/scenarios/valid.json\x00",
		"conformance/other/valid.json",
		"conformance/scenarios/valid.txt",
	} {
		t.Run(fmt.Sprintf("%q", path), func(t *testing.T) {
			requireLoadError(t, func() error {
				_, err := LoadFile(context.Background(), root, path)
				return err
			})
		})
	}
	directoryPath := filepath.Join(root, "conformance", "scenarios", "directory.json")
	if err := os.MkdirAll(directoryPath, 0o755); err != nil {
		t.Fatal(err)
	}
	requireLoadError(t, func() error {
		_, err := LoadFile(context.Background(), root, "conformance/scenarios/directory.json")
		return err
	})
}

func TestLoadAllRejectsEverySymlinkUnderScenarioDirectory(t *testing.T) {
	root := scenarioRepository(t, map[string][]byte{
		"conformance/scenarios/valid.json": scenarioFixture("SCN-LINK-001", "Link scenario"),
	})
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("not JSON"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "conformance", "scenarios", "ignored.txt")); err != nil {
		t.Fatal(err)
	}
	requireLoadError(t, func() error {
		_, err := LoadAll(context.Background(), root)
		return err
	})
}

func TestLoadHonorsCanceledContext(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	root := scenarioRepository(t, map[string][]byte{path: scenarioFixture("SCN-CONTEXT-001", "Context scenario")})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, load := range []func() error{
		func() error { _, err := LoadFile(ctx, root, path); return err },
		func() error { _, err := LoadAll(ctx, root); return err },
	} {
		if err := load(); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled load error = %v", err)
		}
	}
}

func TestGenerateCatalogSortsAndHashesExactBytes(t *testing.T) {
	paths := map[string][]byte{
		"conformance/scenarios/z.json": scenarioFixture("SCN-CATALOG-Z-002", "Zeta catalog"),
		"conformance/scenarios/a.json": scenarioFixture("SCN-CATALOG-A-001", "Alpha catalog"),
	}
	root := scenarioRepository(t, paths)
	loaded, err := LoadAll(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	loaded[0], loaded[1] = loaded[1], loaded[0]
	catalog, err := GenerateCatalog(root, loaded)
	if err != nil {
		t.Fatalf("generate catalog: %v", err)
	}
	if catalog.SchemaVersion != 1 || len(catalog.Scenarios) != 2 {
		t.Fatalf("catalog = %#v", catalog)
	}
	for index, wantID := range []contract.ScenarioID{"SCN-CATALOG-A-001", "SCN-CATALOG-Z-002"} {
		entry := catalog.Scenarios[index]
		if entry.ScenarioID != wantID {
			t.Fatalf("entry %d ID = %q, want %q", index, entry.ScenarioID, wantID)
		}
		digest := sha256.Sum256(paths[entry.Path])
		if entry.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatalf("entry %q hash = %q", entry.ScenarioID, entry.SHA256)
		}
	}

	first, err := CatalogBytes(catalog)
	if err != nil {
		t.Fatal(err)
	}
	second, err := CatalogBytes(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("catalog bytes are not deterministic")
	}
	if !bytes.HasSuffix(first, []byte("\n")) || bytes.HasSuffix(first, []byte("\n\n")) {
		t.Fatalf("catalog does not have one trailing newline: %q", first)
	}
	if !bytes.Contains(first, []byte("\n  \"schema_version\": 1,")) {
		t.Fatalf("catalog is not indented deterministically: %s", first)
	}
}

func TestGenerateCatalogRejectsUnloadedDuplicateAndInvalidSources(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	root := scenarioRepository(t, map[string][]byte{path: scenarioFixture("SCN-GENERATE-001", "Generate scenario")})
	loaded, err := LoadAll(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := GenerateCatalog(root, nil); err == nil {
		t.Fatal("empty catalog was accepted")
	}
	if _, err := GenerateCatalog(root, []Scenario{{ID: "SCN-UNLOADED-001"}}); err == nil {
		t.Fatal("unloaded scenario was accepted")
	}
	if _, err := GenerateCatalog(root, []Scenario{loaded[0], loaded[0]}); err == nil {
		t.Fatal("duplicate scenario ID was accepted")
	}
	duplicatePath := loaded[0]
	duplicatePath.ID = "SCN-GENERATE-002"
	duplicatePath.sourceBytes = bytes.Replace(duplicatePath.sourceBytes, []byte("SCN-GENERATE-001"), []byte("SCN-GENERATE-002"), -1)
	if _, err := GenerateCatalog(root, []Scenario{loaded[0], duplicatePath}); err == nil {
		t.Fatal("duplicate scenario path was accepted")
	}
	invalidPath := loaded[0]
	invalidPath.sourcePath = "conformance/scenarios/../valid.json"
	if _, err := GenerateCatalog(root, []Scenario{invalidPath}); err == nil {
		t.Fatal("invalid loaded path was accepted")
	}
}

func TestGenerateCatalogRejectsSameLengthDriftAndMissingCurrentFile(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	data := scenarioFixture("SCN-DRIFT-001", "First scenario")
	root := scenarioRepository(t, map[string][]byte{path: data})
	loaded, err := LoadAll(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(data, []byte("First scenario"), []byte("Frost scenario"), 1)
	if len(changed) != len(data) {
		t.Fatal("drift mutant changed byte length")
	}
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(path)), changed, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := GenerateCatalog(root, loaded); err == nil {
		t.Fatal("same-length source drift was accepted")
	}
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(path))); err != nil {
		t.Fatal(err)
	}
	if _, err := GenerateCatalog(root, loaded); err == nil {
		t.Fatal("missing current scenario was accepted")
	}
}

func TestCatalogWriteAndCheckDetectExactDrift(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	root := scenarioRepository(t, map[string][]byte{path: scenarioFixture("SCN-CHECK-001", "Check scenario")})
	loaded, err := LoadAll(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := GenerateCatalog(root, loaded)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteCatalog(context.Background(), root, catalog); err != nil {
		t.Fatalf("write catalog: %v", err)
	}
	if err := CheckCatalog(context.Background(), root, catalog); err != nil {
		t.Fatalf("check written catalog: %v", err)
	}
	catalogFile := filepath.Join(root, filepath.FromSlash(catalogPath))
	written, err := os.ReadFile(catalogFile)
	if err != nil {
		t.Fatal(err)
	}
	drifted := append([]byte(nil), written...)
	drifted[0] = '['
	if err := os.WriteFile(catalogFile, drifted, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := CheckCatalog(context.Background(), root, catalog); err == nil {
		t.Fatal("same-length catalog drift was accepted")
	}
	if err := os.Remove(catalogFile); err != nil {
		t.Fatal(err)
	}
	if err := CheckCatalog(context.Background(), root, catalog); err == nil {
		t.Fatal("missing catalog was accepted")
	}
}

func TestGeneratedCatalogWriteAndCheckUseOnePinnedRoot(t *testing.T) {
	root := scenarioRepository(t, map[string][]byte{
		"conformance/scenarios/valid.json": scenarioFixture("SCN-ROOTED-001", "Rooted scenario"),
	})
	if err := WriteGeneratedCatalog(context.Background(), root); err != nil {
		t.Fatalf("write generated catalog: %v", err)
	}
	if err := CheckGeneratedCatalog(context.Background(), root); err != nil {
		t.Fatalf("check generated catalog: %v", err)
	}
}

func TestScenarioSourceRecheckRejectsAddedAndChangedFiles(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	data := scenarioFixture("SCN-RECHECK-001", "Recheck scenario")
	rootPath := scenarioRepository(t, map[string][]byte{path: data})
	loaded, err := LoadAll(context.Background(), rootPath)
	if err != nil {
		t.Fatal(err)
	}
	_, root, err := openRepositoryRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	addedPath := "conformance/scenarios/added.json"
	writeTestFile(t, rootPath, addedPath, scenarioFixture("SCN-RECHECK-002", "Added scenario"))
	if err := recheckScenarioSources(context.Background(), root, loaded); err == nil {
		t.Fatal("added scenario was not detected")
	}
	if err := os.Remove(filepath.Join(rootPath, filepath.FromSlash(addedPath))); err != nil {
		t.Fatal(err)
	}
	changed := bytes.Replace(data, []byte("Recheck scenario"), []byte("Recheck scenaria"), 1)
	if len(changed) != len(data) {
		t.Fatal("source mutant changed byte length")
	}
	if err := os.WriteFile(filepath.Join(rootPath, filepath.FromSlash(path)), changed, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := recheckScenarioSources(context.Background(), root, loaded); err == nil {
		t.Fatal("changed scenario bytes were not detected")
	}
}

func TestFinalSourceRecheckRejectsChangeAfterCatalogComparison(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	data := scenarioFixture("SCN-FINAL-001", "Final scenario")
	rootPath := scenarioRepository(t, map[string][]byte{path: data})
	loaded, err := LoadAll(context.Background(), rootPath)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := GenerateCatalog(rootPath, loaded)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteCatalog(context.Background(), rootPath, catalog); err != nil {
		t.Fatal(err)
	}
	_, root, err := openRepositoryRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := checkCatalogRooted(context.Background(), root, catalog); err != nil {
		t.Fatalf("initial catalog comparison: %v", err)
	}
	changed := bytes.Replace(data, []byte("Final scenario"), []byte("Fatal scenario"), 1)
	if len(changed) != len(data) {
		t.Fatal("final mutant changed byte length")
	}
	if err := os.WriteFile(filepath.Join(rootPath, filepath.FromSlash(path)), changed, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := recheckScenarioSources(context.Background(), root, loaded); err == nil {
		t.Fatal("final source recheck accepted changed bytes")
	}
}

func TestRootedCatalogOperationSurvivesRepositoryPathReplacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("open-directory rename behavior differs on Windows")
	}
	rootPath := scenarioRepository(t, map[string][]byte{
		"conformance/scenarios/valid.json": scenarioFixture("SCN-PINNED-001", "Pinned scenario"),
	})
	realRoot, root, err := openRepositoryRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	validator := schemavalidator.New(realRoot)
	defer validator.Close()
	movedRoot := rootPath + "-moved"
	t.Cleanup(func() { _ = os.RemoveAll(movedRoot) })
	if err := os.Rename(rootPath, movedRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rootPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeGeneratedCatalogRooted(context.Background(), root, validator); err != nil {
		t.Fatalf("write through pinned root: %v", err)
	}
	if err := checkGeneratedCatalogRooted(context.Background(), root, validator); err != nil {
		t.Fatalf("check through pinned root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(movedRoot, filepath.FromSlash(catalogPath))); err != nil {
		t.Fatalf("catalog was not written to pinned tree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rootPath, filepath.FromSlash(catalogPath))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement tree received catalog: %v", err)
	}
}

func TestOpenedFileIdentityAndSymlinkRechecks(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	t.Run("identity", func(t *testing.T) {
		rootPath := scenarioRepository(t, map[string][]byte{path: scenarioFixture("SCN-IDENTITY-001", "Identity scenario")})
		_, root, err := openRepositoryRoot(rootPath)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		file, err := root.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		absolute := filepath.Join(rootPath, filepath.FromSlash(path))
		if err := os.Rename(absolute, absolute+".old"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, scenarioFixture("SCN-IDENTITY-002", "Replacement scenario"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyOpenedFileIdentity(context.Background(), root, path, file); err == nil {
			t.Fatal("opened file identity mismatch was accepted")
		}
	})

	t.Run("symlink", func(t *testing.T) {
		rootPath := scenarioRepository(t, map[string][]byte{path: scenarioFixture("SCN-SYMLINK-001", "Symlink scenario")})
		_, root, err := openRepositoryRoot(rootPath)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		file, err := root.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		absolute := filepath.Join(rootPath, filepath.FromSlash(path))
		target := absolute + ".old"
		if err := os.Rename(absolute, target); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, absolute); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyOpenedFileIdentity(context.Background(), root, path, file); err == nil {
			t.Fatal("opened file symlink replacement was accepted")
		}
	})
}

func TestPinnedConformanceDirectoryPreventsPublicationRedirection(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("open-directory rename behavior differs on Windows")
	}
	rootPath := scenarioRepository(t, map[string][]byte{
		"conformance/scenarios/valid.json": scenarioFixture("SCN-DIRECTORY-001", "Directory scenario"),
	})
	_, root, err := openRepositoryRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	pinned, err := openPinnedDirectory(context.Background(), root, "conformance")
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	original := filepath.Join(rootPath, "conformance-original")
	if err := os.Rename(filepath.Join(rootPath, "conformance"), original); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(rootPath, "conformance"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := verifyPinnedDirectoryIdentity(context.Background(), root, "conformance", pinned); err == nil {
		t.Fatal("replaced visible conformance directory passed identity verification")
	}
	temporary, file, err := createCatalogTemp(context.Background(), pinned)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("pinned catalog\n")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pinned.Rename(temporary, catalogRelativeName); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(original, catalogRelativeName)); err != nil {
		t.Fatalf("pinned publication did not reach original directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rootPath, "conformance", catalogRelativeName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement directory received publication: %v", err)
	}
}

func TestGeneratedPublicationRejectsVisibleDirectoryWithoutCatalog(t *testing.T) {
	path := "conformance/scenarios/valid.json"
	data := scenarioFixture("SCN-VISIBLE-001", "Visible scenario")
	rootPath := scenarioRepository(t, map[string][]byte{path: data})
	loaded, err := LoadAll(context.Background(), rootPath)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := GenerateCatalog(rootPath, loaded)
	if err != nil {
		t.Fatal(err)
	}
	_, root, err := openRepositoryRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := writeCatalogRooted(context.Background(), root, catalog); err != nil {
		t.Fatal(err)
	}
	published := filepath.Join(rootPath, "conformance-published")
	if err := os.Rename(filepath.Join(rootPath, "conformance"), published); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, rootPath, path, data)
	if err := verifyGeneratedCatalogPublication(context.Background(), root, loaded, catalog); err == nil {
		t.Fatal("generated publication accepted a visible directory without its catalog")
	}
}

func TestRepeatedOpenedFileReadDetectsInPlaceByteChange(t *testing.T) {
	rootPath := scenarioRepository(t, map[string][]byte{
		"conformance/scenarios/valid.json": scenarioFixture("SCN-STABLE-001", "Stable scenario"),
	})
	path := "conformance/stable.txt"
	firstBytes := []byte("first stable bytes\n")
	writeTestFile(t, rootPath, path, firstBytes)
	_, root, err := openRepositoryRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	file, err := root.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	first, err := readAllContext(context.Background(), file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, filepath.FromSlash(path)), []byte("other stable byte\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stable, err := rereadOpenedFile(context.Background(), file, first)
	if err != nil {
		t.Fatal(err)
	}
	if stable {
		t.Fatal("repeated opened-file read accepted changed bytes")
	}
}

func TestGenerateCatalogRootedCancelsDuringWork(t *testing.T) {
	rootPath := scenarioRepository(t, map[string][]byte{
		"conformance/scenarios/valid.json": scenarioFixture("SCN-CANCEL-001", "Cancel scenario"),
	})
	loaded, err := LoadAll(context.Background(), rootPath)
	if err != nil {
		t.Fatal(err)
	}
	_, root, err := openRepositoryRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ctx := newCancelAfterContext(3)
	if _, err := generateCatalogRooted(ctx, root, loaded); !errors.Is(err, context.Canceled) {
		t.Fatalf("generation cancellation error = %v", err)
	}
}

type cancelAfterContext struct {
	context.Context
	mu        sync.Mutex
	remaining int
	done      chan struct{}
	canceled  bool
}

func newCancelAfterContext(checks int) *cancelAfterContext {
	return &cancelAfterContext{Context: context.Background(), remaining: checks, done: make(chan struct{})}
}

func (ctx *cancelAfterContext) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *cancelAfterContext) Err() error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.canceled {
		return context.Canceled
	}
	ctx.remaining--
	if ctx.remaining <= 0 {
		ctx.canceled = true
		close(ctx.done)
		return context.Canceled
	}
	return nil
}

func validateJSONTags(typeOf reflect.Type, expected []string) error {
	var exported []reflect.StructField
	for index := 0; index < typeOf.NumField(); index++ {
		field := typeOf.Field(index)
		if field.IsExported() {
			exported = append(exported, field)
		}
	}
	if len(exported) != len(expected) {
		return fmt.Errorf("%s has %d exported fields, want %d", typeOf, len(exported), len(expected))
	}
	seen := make(map[string]string, len(exported))
	for index, field := range exported {
		tag, exists := field.Tag.Lookup("json")
		if !exists || tag == "" {
			return fmt.Errorf("%s.%s has no explicit JSON tag", typeOf, field.Name)
		}
		if tag != expected[index] {
			return fmt.Errorf("%s.%s JSON tag = %q, want %q", typeOf, field.Name, tag, expected[index])
		}
		name := strings.Split(tag, ",")[0]
		if previous, duplicate := seen[name]; duplicate {
			return fmt.Errorf("%s.%s duplicates JSON tag on %s", typeOf, field.Name, previous)
		}
		seen[name] = field.Name
	}
	return nil
}

func requireLoadError(t *testing.T, operation func() error) {
	t.Helper()
	if err := operation(); err == nil {
		t.Fatal("invalid scenario input was accepted")
	}
}

func scenarioRepository(t *testing.T, scenarioFiles map[string][]byte) string {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, root, makefilePath, []byte(testScenarioMakefile))
	writeTestFile(t, root, scenarioSchemaPath, authoredScenarioSchema(t))
	if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(scenarioDirectory)), 0o755); err != nil {
		t.Fatal(err)
	}
	for path, data := range scenarioFiles {
		writeTestFile(t, root, path, data)
	}
	return root
}

const testScenarioMakefile = `test-conformance test-blackbox:
test-swift:
test-kotlin:
test-rn-e2e-ios:
test-rn-e2e-android:
test-conformance-scenarios:
`

func authoredScenarioSchema(t *testing.T) []byte {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate scenario test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(scenarioSchemaPath)))
	if err != nil {
		t.Fatalf("read authored scenario schema: %v", err)
	}
	return data
}

func writeTestFile(t *testing.T, root, path string, data []byte) {
	t.Helper()
	absolute := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(absolute, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func scenarioFixture(id, title string) []byte {
	fixture := `{
  "$schema": "https://synchro.dev/conformance/schemas/scenario-v2.schema.json",
  "schema_version": 2,
  "id": "SCN-FIXTURE-001",
  "title": "Fixture title",
  "description": "Compact valid scenario.",
  "requirement_ids": ["SYNC-TEST-001"],
  "normative_references": [
    {"path": "docs/src/content/docs/spec/04-invariants.mdx", "anchor": "#canonical-time-format"}
  ],
  "proof_types": ["reference-model"],
  "proof_obligations": [
    {
      "obligation_id": "OBL-TEST-001",
      "requirement_ids": ["SYNC-TEST-001"],
      "assertion_ids": ["ASSERT-TEST-001"],
      "proof_type": "reference-model",
      "support_cell_id": null,
      "artifact_inventory_ids": ["ARTDEF-TEST-001"],
      "performance_budget_ids": [],
      "required_measurement_ids": [],
      "required_vector_set_ids": [],
      "make_target": "test-conformance-scenarios",
      "argv": ["make", "test-conformance-scenarios"],
      "fault_plan_id": null,
      "control_id": null
    }
  ],
  "ownership": [
    {
      "scenario_id": "SCN-FIXTURE-001",
      "requirement_id": "SYNC-TEST-001",
      "proof_obligation_id": "OBL-TEST-001",
      "assertion_id": "ASSERT-TEST-001",
      "proof_type": "reference-model",
      "support_cell_id": null
    }
  ],
  "model": {
    "setup": [
      {"contract_operation": "model", "name": "author-state", "payload": {"seed": 1}}
    ],
    "expected_state": [
      {
        "id": "EXPECT-TEST-001",
        "predicate": {
          "contract_predicate": "state-equality",
          "name": "state-unchanged",
          "payload": {}
        }
      }
    ]
  },
  "barrier_plan": {"barriers": []},
  "fault_plans": [],
  "replay": {
    "mode": "deterministic",
    "seed_required": false,
    "barrier_trace_required": false
  },
  "negative_controls": [],
  "steps": [
    {
      "id": "STEP-TEST-001",
      "phase": "exercise",
      "transport": "model",
      "description": "Run the authored model.",
	  "operation": {"contract_operation": "model", "name": "evaluate-state", "payload": {}},
	  "expected_outcome": {"disposition": "success"}
    }
  ],
  "wire_expectations": [
    {
      "step_id": "STEP-TEST-001",
      "assertion_id": "ASSERT-TEST-001",
      "contract_case": "pull_success",
      "http_status": 200,
      "error_code": null,
      "retryable": false
    }
  ],
  "assertions": [
    {
      "id": "ASSERT-TEST-001",
      "requirement_ids": ["SYNC-TEST-001"],
      "description": "The observed state equals the authored state.",
      "expectation_ids": ["EXPECT-TEST-001"],
      "predicate": {
        "contract_predicate": "state-equality",
        "name": "state-unchanged",
        "payload": {}
      },
      "oracle": {
        "kind": "model-state-equality",
        "expected_source": "authored-model",
        "observed_source": "system-under-test"
      },
      "detects_control_ids": []
    }
  ]
}
`
	fixture = strings.ReplaceAll(fixture, "SCN-FIXTURE-001", id)
	fixture = strings.Replace(fixture, "Fixture title", title, 1)
	return []byte(fixture)
}
