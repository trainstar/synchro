package contract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const (
	scenarioSourceDirectory = "conformance/scenarios"
	vectorSourceDirectory   = "conformance/vectors"
	vectorSourceCatalog     = "conformance/vectors/catalog.json"
)

type snapshotScenarioCatalog struct {
	SchemaVersion int                           `json:"schema_version"`
	Scenarios     []snapshotScenarioCatalogItem `json:"scenarios"`
}

type snapshotScenarioCatalogItem struct {
	ScenarioID string `json:"scenario_id"`
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
}

type snapshotVectorCatalog struct {
	SchemaURI     string                      `json:"$schema"`
	SchemaVersion int                         `json:"schema_version"`
	Release       string                      `json:"release"`
	VectorSets    []snapshotVectorCatalogItem `json:"vector_sets"`
}

type snapshotVectorCatalogItem struct {
	VectorSetID       string   `json:"vector_set_id"`
	Path              string   `json:"path"`
	SourceSHA256      string   `json:"source_sha256"`
	VectorCount       int      `json:"vector_count"`
	AggregateSHA256   string   `json:"aggregate_sha256"`
	RequiredLanguages []string `json:"required_languages"`
}

type snapshotSourceBinding struct {
	path   string
	digest string
}

func verifySnapshotSourceClosure(ctx context.Context, root *os.Root, scenarioCatalog, vectorCatalog []byte) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	scenarios, err := decodeSnapshotScenarioCatalog(scenarioCatalog)
	if err != nil {
		return fmt.Errorf("validate scenario source catalog: %w", err)
	}
	vectors, err := decodeSnapshotVectorCatalog(vectorCatalog)
	if err != nil {
		return fmt.Errorf("validate vector source catalog: %w", err)
	}
	scenarioSources, err := snapshotScenarioSources(scenarios)
	if err != nil {
		return err
	}
	vectorSources, err := snapshotVectorSources(vectors)
	if err != nil {
		return err
	}
	if err := verifySnapshotSourceSet(ctx, root, scenarioSourceDirectory, "", scenarioSources); err != nil {
		return err
	}
	if err := verifySnapshotSourceSet(ctx, root, vectorSourceDirectory, vectorSourceCatalog, vectorSources); err != nil {
		return err
	}
	return nil
}

func decodeSnapshotScenarioCatalog(data []byte) (snapshotScenarioCatalog, error) {
	var document snapshotScenarioCatalog
	if err := decodeSnapshotObject(data, &document, []string{"schema_version", "scenarios"}); err != nil {
		return snapshotScenarioCatalog{}, err
	}
	var object map[string]json.RawMessage
	if err := jsonstrict.Decode(data, &object); err != nil {
		return snapshotScenarioCatalog{}, err
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(object["scenarios"], &entries); err != nil {
		return snapshotScenarioCatalog{}, fmt.Errorf("decode scenarios: %w", err)
	}
	for index, entry := range entries {
		var decoded map[string]json.RawMessage
		if err := jsonstrict.Decode(entry, &decoded); err != nil {
			return snapshotScenarioCatalog{}, fmt.Errorf("decode scenario entry %d: %w", index, err)
		}
		if err := requireSnapshotObjectKeys(decoded, []string{"scenario_id", "path", "sha256"}); err != nil {
			return snapshotScenarioCatalog{}, fmt.Errorf("scenario entry %d: %w", index, err)
		}
	}
	if document.SchemaVersion != 1 {
		return snapshotScenarioCatalog{}, fmt.Errorf("scenario catalog schema_version is %d, want 1", document.SchemaVersion)
	}
	if len(document.Scenarios) == 0 {
		return snapshotScenarioCatalog{}, errors.New("scenario catalog must contain at least one source")
	}
	return document, nil
}

func decodeSnapshotVectorCatalog(data []byte) (snapshotVectorCatalog, error) {
	var document snapshotVectorCatalog
	if err := decodeSnapshotObject(data, &document, []string{"$schema", "schema_version", "release", "vector_sets"}); err != nil {
		return snapshotVectorCatalog{}, err
	}
	var object map[string]json.RawMessage
	if err := jsonstrict.Decode(data, &object); err != nil {
		return snapshotVectorCatalog{}, err
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(object["vector_sets"], &entries); err != nil {
		return snapshotVectorCatalog{}, fmt.Errorf("decode vector_sets: %w", err)
	}
	for index, entry := range entries {
		var decoded map[string]json.RawMessage
		if err := jsonstrict.Decode(entry, &decoded); err != nil {
			return snapshotVectorCatalog{}, fmt.Errorf("decode vector-set entry %d: %w", index, err)
		}
		if err := requireSnapshotObjectKeys(decoded, []string{"vector_set_id", "path", "source_sha256", "vector_count", "aggregate_sha256", "required_languages"}); err != nil {
			return snapshotVectorCatalog{}, fmt.Errorf("vector-set entry %d: %w", index, err)
		}
		var languages []string
		if err := json.Unmarshal(decoded["required_languages"], &languages); err != nil {
			return snapshotVectorCatalog{}, fmt.Errorf("vector-set entry %d required_languages: %w", index, err)
		}
		if !equalSnapshotStrings(languages, []string{"go", "rust", "swift", "kotlin"}) {
			return snapshotVectorCatalog{}, fmt.Errorf("vector-set entry %d required_languages is not the locked language list", index)
		}
	}
	if document.SchemaVersion != 1 {
		return snapshotVectorCatalog{}, fmt.Errorf("vector catalog schema_version is %d, want 1", document.SchemaVersion)
	}
	if document.Release != releaseVersion {
		return snapshotVectorCatalog{}, fmt.Errorf("vector catalog release is %q, want %q", document.Release, releaseVersion)
	}
	if len(document.VectorSets) == 0 {
		return snapshotVectorCatalog{}, errors.New("vector catalog must contain at least one source")
	}
	return document, nil
}

func decodeSnapshotObject(data []byte, destination any, required []string) error {
	var object map[string]json.RawMessage
	if err := jsonstrict.Decode(data, &object); err != nil {
		return err
	}
	if err := requireSnapshotObjectKeys(object, required); err != nil {
		return err
	}
	if err := jsonstrict.Decode(data, destination); err != nil {
		return err
	}
	return nil
}

func requireSnapshotObjectKeys(object map[string]json.RawMessage, required []string) error {
	expected := make(map[string]struct{}, len(required))
	for _, key := range required {
		expected[key] = struct{}{}
		if _, exists := object[key]; !exists {
			return fmt.Errorf("object is missing required member %q", key)
		}
	}
	for key := range object {
		if _, exists := expected[key]; !exists {
			return fmt.Errorf("object has unexpected member %q", key)
		}
	}
	return nil
}

func equalSnapshotStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func snapshotScenarioSources(document snapshotScenarioCatalog) ([]snapshotSourceBinding, error) {
	sources := make([]snapshotSourceBinding, 0, len(document.Scenarios))
	seenIDs := make(map[string]struct{}, len(document.Scenarios))
	seenPaths := make(map[string]struct{}, len(document.Scenarios))
	for index, entry := range document.Scenarios {
		if entry.ScenarioID == "" {
			return nil, fmt.Errorf("scenario catalog entry %d has an empty scenario_id", index)
		}
		if _, exists := seenIDs[entry.ScenarioID]; exists {
			return nil, fmt.Errorf("scenario catalog has duplicate scenario_id %q", entry.ScenarioID)
		}
		if err := validateSnapshotSourcePath(entry.Path, scenarioSourceDirectory, "scenario"); err != nil {
			return nil, err
		}
		if _, exists := seenPaths[entry.Path]; exists {
			return nil, fmt.Errorf("scenario catalog has duplicate source path %q", entry.Path)
		}
		if !isLowerSHA256(entry.SHA256) {
			return nil, fmt.Errorf("scenario catalog source %q has an invalid SHA-256", entry.Path)
		}
		seenIDs[entry.ScenarioID] = struct{}{}
		seenPaths[entry.Path] = struct{}{}
		sources = append(sources, snapshotSourceBinding{path: entry.Path, digest: entry.SHA256})
	}
	return sources, nil
}

func snapshotVectorSources(document snapshotVectorCatalog) ([]snapshotSourceBinding, error) {
	sources := make([]snapshotSourceBinding, 0, len(document.VectorSets))
	seenIDs := make(map[string]struct{}, len(document.VectorSets))
	seenPaths := make(map[string]struct{}, len(document.VectorSets))
	for index, entry := range document.VectorSets {
		if entry.VectorSetID == "" {
			return nil, fmt.Errorf("vector catalog entry %d has an empty vector_set_id", index)
		}
		if _, exists := seenIDs[entry.VectorSetID]; exists {
			return nil, fmt.Errorf("vector catalog has duplicate vector_set_id %q", entry.VectorSetID)
		}
		if err := validateSnapshotSourcePath(entry.Path, vectorSourceDirectory, "vector"); err != nil {
			return nil, err
		}
		if entry.Path == vectorSourceCatalog {
			return nil, fmt.Errorf("vector catalog cannot bind itself as a source")
		}
		if _, exists := seenPaths[entry.Path]; exists {
			return nil, fmt.Errorf("vector catalog has duplicate source path %q", entry.Path)
		}
		if !isLowerSHA256(entry.SourceSHA256) {
			return nil, fmt.Errorf("vector catalog source %q has an invalid SHA-256", entry.Path)
		}
		if entry.VectorCount < 1 {
			return nil, fmt.Errorf("vector catalog source %q has an invalid vector count", entry.Path)
		}
		if !isLowerSHA256(entry.AggregateSHA256) {
			return nil, fmt.Errorf("vector catalog source %q has an invalid aggregate SHA-256", entry.Path)
		}
		seenIDs[entry.VectorSetID] = struct{}{}
		seenPaths[entry.Path] = struct{}{}
		sources = append(sources, snapshotSourceBinding{path: entry.Path, digest: entry.SourceSHA256})
	}
	return sources, nil
}

func validateSnapshotSourcePath(path, directory, kind string) error {
	if err := validateRepositoryRelativePath(path); err != nil {
		return fmt.Errorf("%s source path %q: %w", kind, path, err)
	}
	prefix := directory + "/"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, ".json") {
		return fmt.Errorf("%s source path is outside %s or is not JSON: %q", kind, directory, path)
	}
	return nil
}

func verifySnapshotSourceSet(ctx context.Context, root *os.Root, directory, excluded string, expected []snapshotSourceBinding) error {
	if root == nil {
		return errors.New("repository root is nil")
	}
	expectedPaths := make([]string, 0, len(expected))
	expectedBytes := make(map[string][]byte, len(expected))
	for _, source := range expected {
		expectedPaths = append(expectedPaths, source.path)
	}
	sort.Strings(expectedPaths)
	actual, err := enumerateSnapshotSourcePaths(ctx, root, directory, excluded)
	if err != nil {
		return err
	}
	if err := requireExactSnapshotSourcePaths(actual, expectedPaths, directory); err != nil {
		return err
	}
	for _, source := range expected {
		if err := checkContext(ctx); err != nil {
			return err
		}
		data, err := readRepositoryFile(ctx, root, source.path)
		if err != nil {
			return fmt.Errorf("read %s source %q: %w", directory, source.path, err)
		}
		digest := sha256.Sum256(data)
		if hexDigest := fmt.Sprintf("%x", digest); hexDigest != source.digest {
			return fmt.Errorf("%s source %q bytes do not match catalog SHA-256", directory, source.path)
		}
		expectedBytes[source.path] = append([]byte(nil), data...)
	}
	finalPaths, err := enumerateSnapshotSourcePaths(ctx, root, directory, excluded)
	if err != nil {
		return err
	}
	if err := requireExactSnapshotSourcePaths(finalPaths, expectedPaths, directory); err != nil {
		return err
	}
	for _, source := range expected {
		current, err := readRepositoryFile(ctx, root, source.path)
		if err != nil {
			return fmt.Errorf("recheck %s source %q: %w", directory, source.path, err)
		}
		if !bytes.Equal(current, expectedBytes[source.path]) {
			return fmt.Errorf("%s source %q changed during snapshot construction", directory, source.path)
		}
	}
	return nil
}

func enumerateSnapshotSourcePaths(ctx context.Context, root *os.Root, directory, excluded string) ([]string, error) {
	if err := rejectRepositorySymlinkComponents(ctx, root, directory); err != nil {
		return nil, fmt.Errorf("inspect %s source directory: %w", directory, err)
	}
	paths := make([]string, 0)
	err := fs.WalkDir(root.FS(), directory, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		info, err := root.Lstat(filepath.FromSlash(path))
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is forbidden under %s: %q", directory, path)
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular file is forbidden under %s: %q", directory, path)
		}
		if path != excluded && strings.HasSuffix(path, ".json") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("enumerate %s source files: %w", directory, err)
	}
	sort.Strings(paths)
	return paths, nil
}

func requireExactSnapshotSourcePaths(actual, expected []string, directory string) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("%s source set has %d files, want %d", directory, len(actual), len(expected))
	}
	for index := range expected {
		if actual[index] != expected[index] {
			return fmt.Errorf("%s source set differs at path %d: found %q, want %q", directory, index, actual[index], expected[index])
		}
	}
	return nil
}

func rejectRepositorySymlinkComponents(ctx context.Context, root *os.Root, path string) error {
	if root == nil {
		return errors.New("repository root is nil")
	}
	current := ""
	for _, component := range strings.Split(path, "/") {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if current == "" {
			current = component
		} else {
			current += "/" + component
		}
		info, err := root.Lstat(filepath.FromSlash(current))
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("repository path has symlink component %q", current)
		}
	}
	return nil
}
