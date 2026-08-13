package vectors

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
	"strings"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/contract"
)

func TestLoadValidCatalogAndDefensiveCopies(t *testing.T) {
	root := writeTestRepository(t)
	catalog, err := Load(context.Background(), root)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	ids := catalog.IDs()
	if len(ids) != 1 || ids[0] != "VSET-CANONICAL-001" || !catalog.Has(ids[0]) || catalog.Has("VSET-ABSENT-001") {
		t.Fatalf("catalog IDs or Has() = %v", ids)
	}
	ids[0] = "VSET-CHANGED-001"
	if got := catalog.IDs(); len(got) != 1 || got[0] != "VSET-CANONICAL-001" {
		t.Fatalf("IDs() returned an aliased slice: %v", got)
	}
	first, ok := catalog.Set("VSET-CANONICAL-001")
	if !ok {
		t.Fatal("Set() did not return the loaded set")
	}
	index := -1
	for current, vector := range first.Vectors {
		if vector.Valid && vector.Expected.CanonicalBytesHex != nil {
			index = current
			break
		}
	}
	if index < 0 {
		t.Fatal("loaded set has no valid vector")
	}
	first.Vectors[index].Input[0] ^= 0xff
	*first.Vectors[index].Expected.CanonicalBytesHex = "00"
	second, ok := catalog.Set("VSET-CANONICAL-001")
	if !ok {
		t.Fatal("Set() did not return a second loaded set")
	}
	if bytes.Equal(first.Vectors[index].Input, second.Vectors[index].Input) || *second.Vectors[index].Expected.CanonicalBytesHex == "00" {
		t.Fatal("Set() did not return fully defensive copies")
	}
}

func TestLoadRejectsCatalogSchemaBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"unknown member", func(c map[string]any) { c["unknown"] = true }},
		{"wrong release", func(c map[string]any) { c["release"] = "0.0.0" }},
		{"wrong version", func(c map[string]any) { c["schema_version"] = 2 }},
		{"traversal path", func(c map[string]any) { catalogEntry(c)["path"] = "conformance/vectors/../canonical-v1.json" }},
		{"backslash path", func(c map[string]any) { catalogEntry(c)["path"] = `conformance\\vectors\\canonical-v1.json` }},
		{"malformed hash", func(c map[string]any) { catalogEntry(c)["source_sha256"] = "ABC" }},
		{"wrong languages", func(c map[string]any) { catalogEntry(c)["required_languages"] = []any{"rust", "go", "swift", "kotlin"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := writeTestRepository(t)
			mutateCatalog(t, root, test.mutate)
			if _, err := Load(context.Background(), root); err == nil {
				t.Fatal("Load() succeeded")
			}
		})
	}
}

func TestValidateCatalogEntriesRejectsDuplicateAndUnorderedEntries(t *testing.T) {
	validHash := strings.Repeat("a", 64)
	entry := func(id contract.VectorSetID, path string) catalogVectorEntry {
		return catalogVectorEntry{VectorSetID: id, Path: path, SourceSHA256: validHash, AggregateSHA256: validHash}
	}
	for _, entries := range [][]catalogVectorEntry{
		{entry("VSET-ONE-001", "conformance/vectors/one.json"), entry("VSET-ONE-001", "conformance/vectors/two.json")},
		{entry("VSET-ONE-001", "conformance/vectors/one.json"), entry("VSET-TWO-001", "conformance/vectors/one.json")},
		{entry("VSET-TWO-001", "conformance/vectors/two.json"), entry("VSET-ONE-001", "conformance/vectors/one.json")},
	} {
		if err := validateCatalogEntries(entries); err == nil {
			t.Fatalf("validateCatalogEntries(%v) succeeded", entries)
		}
	}
	tooMany := make([]catalogVectorEntry, maxVectorSets+1)
	for index := range tooMany {
		tooMany[index] = entry(
			contract.VectorSetID(fmt.Sprintf("VSET-LIMIT-%03d", index)),
			fmt.Sprintf("conformance/vectors/limit-%03d.json", index),
		)
	}
	if err := validateCatalogEntries(tooMany); err == nil {
		t.Fatal("validateCatalogEntries() accepted too many vector sets")
	}
}

func TestLoadRejectsVectorDocumentBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"unknown vector member", func(s map[string]any) { s["vectors"].([]any)[0].(map[string]any)["unknown"] = true }},
		{"duplicate vector ID", func(s map[string]any) {
			vectors := s["vectors"].([]any)
			vectors[0].(map[string]any)["vector_id"] = vectors[1].(map[string]any)["vector_id"]
		}},
		{"unordered vector IDs", func(s map[string]any) {
			vectors := s["vectors"].([]any)
			vectors[0], vectors[1] = vectors[1], vectors[0]
		}},
		{"catalog count", func(s map[string]any) { s["vectors"] = s["vectors"].([]any)[:1] }},
		{"aggregate count", func(s map[string]any) { s["aggregate"].(map[string]any)["vector_count"] = 1 }},
		{"aggregate copied hash", func(s map[string]any) {
			s["aggregate"].(map[string]any)["vector_hashes"].([]any)[0].(map[string]any)["expected_bytes_sha256"] = strings.Repeat("0", 64)
		}},
		{"aggregate SHA", func(s map[string]any) { s["aggregate"].(map[string]any)["sha256"] = strings.Repeat("0", 64) }},
		{"unknown aggregate child", func(s map[string]any) {
			s["aggregate"].(map[string]any)["vector_hashes"].([]any)[0].(map[string]any)["unknown"] = true
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := writeTestRepository(t)
			mutateVectorSource(t, root, test.mutate)
			if _, err := Load(context.Background(), root); err == nil {
				t.Fatal("Load() succeeded")
			}
		})
	}

	t.Run("source hash", func(t *testing.T) {
		root := writeTestRepository(t)
		mutateCatalog(t, root, func(c map[string]any) { catalogEntry(c)["source_sha256"] = strings.Repeat("0", 64) })
		if _, err := Load(context.Background(), root); err == nil {
			t.Fatal("Load() succeeded")
		}
	})
}

func TestLoadRejectsUnboundAndSymlinkedFiles(t *testing.T) {
	t.Run("unbound JSON", func(t *testing.T) {
		root := writeTestRepository(t)
		writeFile(t, filepath.Join(root, vectorDirectory, "unbound.json"), []byte("{}"))
		if _, err := Load(context.Background(), root); err == nil {
			t.Fatal("Load() succeeded")
		}
	})
	t.Run("symlinked vector", func(t *testing.T) {
		root := writeTestRepository(t)
		path := filepath.Join(root, "conformance/vectors/canonical-v1.json")
		target := filepath.Join(root, "outside.json")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, target, data)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, path); err != nil {
			t.Fatalf("create required vector symlink: %v", err)
		}
		if _, err := Load(context.Background(), root); err == nil {
			t.Fatal("Load() succeeded")
		}
	})
	t.Run("symlinked parent", func(t *testing.T) {
		root := writeTestRepository(t)
		path := filepath.Join(root, vectorDirectory)
		target := path + "-real"
		if err := os.Rename(path, target); err != nil {
			t.Fatalf("rename required vector directory: %v", err)
		}
		if err := os.Symlink(target, path); err != nil {
			t.Fatalf("create required parent symlink: %v", err)
		}
		if _, err := Load(context.Background(), root); err == nil {
			t.Fatal("Load() succeeded")
		}
	})
}

func TestLoaderTOCTOUBoundaries(t *testing.T) {
	t.Run("changed captured bytes", func(t *testing.T) {
		rootPath := writeTestRepository(t)
		_, root, err := openRepositoryRoot(rootPath)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		path := "conformance/vectors/catalog.json"
		captured, err := readRootedFile(context.Background(), root, path)
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(rootPath, path), append(captured, ' '))
		if err := verifyCapturedSources(context.Background(), root, map[string][]byte{path: captured}); err == nil {
			t.Fatal("verifyCapturedSources() succeeded")
		}
	})
	t.Run("opened file replacement", func(t *testing.T) {
		rootPath := writeTestRepository(t)
		_, root, err := openRepositoryRoot(rootPath)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		path := "conformance/vectors/catalog.json"
		file, err := root.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		replacement := filepath.Join(rootPath, "replacement.json")
		writeFile(t, replacement, []byte("{}"))
		if err := os.Rename(replacement, filepath.Join(rootPath, path)); err != nil {
			t.Fatal(err)
		}
		if err := verifyOpenedFileIdentity(context.Background(), root, path, file); err == nil {
			t.Fatal("verifyOpenedFileIdentity() succeeded")
		}
	})
	t.Run("final enumeration", func(t *testing.T) {
		rootPath := writeTestRepository(t)
		_, root, err := openRepositoryRoot(rootPath)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		expected := []string{"conformance/vectors/catalog.json", "conformance/vectors/canonical-v1.json"}
		if err := requireExactVectorPaths(context.Background(), root, append([]string(nil), expected...)); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(rootPath, vectorDirectory, "added.json"), []byte("{}"))
		if err := requireExactVectorPaths(context.Background(), root, expected); err == nil {
			t.Fatal("requireExactVectorPaths() succeeded")
		}
	})
}

func TestPinnedRootSurvivesVisiblePathReplacement(t *testing.T) {
	parent := t.TempDir()
	visible := filepath.Join(parent, "visible")
	writeFile(t, filepath.Join(visible, "file.json"), []byte("old"))
	_, root, err := openRepositoryRoot(visible)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	replacement := filepath.Join(parent, "replacement")
	writeFile(t, filepath.Join(replacement, "file.json"), []byte("new"))
	if err := os.Rename(visible, filepath.Join(parent, "retired")); err != nil {
		t.Fatalf("rename required visible root: %v", err)
	}
	if err := os.Rename(replacement, visible); err != nil {
		t.Fatalf("replace required visible root: %v", err)
	}
	got, err := readRootedFile(context.Background(), root, "file.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("pinned root read %q, want old", got)
	}
}

func TestLoaderResourceAndContextBounds(t *testing.T) {
	if _, err := readAllContext(context.Background(), bytes.NewReader(make([]byte, maxRepositoryJSONFileSize+1))); err == nil {
		t.Fatal("readAllContext() accepted an oversized file")
	}
	deep := []byte(strings.Repeat("[", maxJSONNestingDepth+1) + strings.Repeat("]", maxJSONNestingDepth+1))
	if err := validateJSONDocument(deep, jsonValidation{}); err == nil {
		t.Fatal("validateJSONDocument() accepted excessive nesting")
	}
	budget := []byte("[" + strings.Repeat("0,", maxJSONValuesAndNames) + "0]")
	if err := validateJSONDocument(budget, jsonValidation{}); err == nil {
		t.Fatal("validateJSONDocument() accepted excessive values")
	}
	rootPath := writeTestRepository(t)
	for index := 0; index < maxVectorDirectoryEntries; index++ {
		writeFile(t, filepath.Join(rootPath, vectorDirectory, fmt.Sprintf("entry-%03d.txt", index)), []byte("x"))
	}
	_, root, err := openRepositoryRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enumerateVectorPaths(context.Background(), root); err == nil {
		_ = root.Close()
		t.Fatal("enumerateVectorPaths() accepted too many directory entries")
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Load(ctx, writeTestRepository(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load() error = %v, want context cancellation", err)
	}
}

func writeTestRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, path := range []string{vectorSchemaPath, vectorCatalogPath, "conformance/vectors/canonical-v1.json"} {
		data, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Fatalf("read trusted %s: %v", path, err)
		}
		writeFile(t, filepath.Join(root, path), data)
	}
	return root
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func mutateCatalog(t *testing.T, root string, mutate func(map[string]any)) {
	t.Helper()
	path := filepath.Join(root, vectorCatalogPath)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	mutate(document)
	data, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, data)
}

func mutateVectorSource(t *testing.T, root string, mutate func(map[string]any)) {
	t.Helper()
	path := filepath.Join(root, "conformance/vectors/canonical-v1.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var source map[string]any
	if err := json.Unmarshal(data, &source); err != nil {
		t.Fatal(err)
	}
	mutate(source)
	data, err = json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, data)
	digest := sha256.Sum256(data)
	mutateCatalog(t, root, func(c map[string]any) { catalogEntry(c)["source_sha256"] = hex.EncodeToString(digest[:]) })
}

func catalogEntry(c map[string]any) map[string]any {
	return c["vector_sets"].([]any)[0].(map[string]any)
}
