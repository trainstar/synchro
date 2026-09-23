package scenarios

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/internal/schemavalidator"
)

const (
	catalogPath         = "conformance/catalog.json"
	catalogRelativeName = "catalog.json"
)

type Catalog struct {
	SchemaVersion int             `json:"schema_version"`
	Scenarios     []ScenarioEntry `json:"scenarios"`
}

type ScenarioEntry struct {
	ScenarioID contract.ScenarioID `json:"scenario_id"`
	Path       string              `json:"path"`
	SHA256     string              `json:"sha256"`
}

// GenerateCatalog verifies loaded source files and builds a sorted catalog.
func GenerateCatalog(repoRoot string, scenarios []Scenario) (Catalog, error) {
	return GenerateCatalogContext(context.Background(), repoRoot, scenarios)
}

// GenerateCatalogContext verifies loaded source files with cancellation support.
func GenerateCatalogContext(ctx context.Context, repoRoot string, scenarios []Scenario) (Catalog, error) {
	if err := checkContext(ctx); err != nil {
		return Catalog{}, err
	}
	_, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return Catalog{}, err
	}
	defer root.Close()
	return generateCatalogRooted(ctx, root, scenarios)
}

func generateCatalogRooted(ctx context.Context, root *os.Root, scenarios []Scenario) (Catalog, error) {
	if err := checkContext(ctx); err != nil {
		return Catalog{}, err
	}
	if len(scenarios) == 0 {
		return Catalog{}, errors.New("catalog requires at least one loaded scenario")
	}

	entries := make([]ScenarioEntry, 0, len(scenarios))
	seenIDs := make(map[contract.ScenarioID]string, len(scenarios))
	seenPaths := make(map[string]contract.ScenarioID, len(scenarios))
	for _, scenario := range scenarios {
		if err := checkContext(ctx); err != nil {
			return Catalog{}, err
		}
		if scenario.sourcePath == "" || len(scenario.sourceBytes) == 0 {
			return Catalog{}, fmt.Errorf("scenario %q was not loaded from a source file", scenario.ID)
		}
		if err := validateScenarioPath(scenario.sourcePath); err != nil {
			return Catalog{}, fmt.Errorf("validate loaded scenario %q path: %w", scenario.ID, err)
		}
		if err := validateScenarioIdentity(scenario.sourcePath, scenario.ID); err != nil {
			return Catalog{}, err
		}
		if previousPath, duplicate := seenIDs[scenario.ID]; duplicate {
			return Catalog{}, fmt.Errorf("duplicate scenario ID %q in %q and %q", scenario.ID, previousPath, scenario.sourcePath)
		}
		if previousID, duplicate := seenPaths[scenario.sourcePath]; duplicate {
			return Catalog{}, fmt.Errorf("duplicate scenario path %q for %q and %q", scenario.sourcePath, previousID, scenario.ID)
		}
		var capturedIdentity struct {
			ID contract.ScenarioID `json:"id"`
		}
		if err := jsonstrict.Decode(scenario.sourceBytes, &capturedIdentity); err != nil {
			return Catalog{}, fmt.Errorf("decode captured scenario identity %q: %w", scenario.sourcePath, err)
		}
		if capturedIdentity.ID != scenario.ID {
			return Catalog{}, fmt.Errorf("loaded scenario ID %q does not match captured ID %q", scenario.ID, capturedIdentity.ID)
		}
		current, err := readRootedFile(ctx, root, scenario.sourcePath)
		if err != nil {
			return Catalog{}, fmt.Errorf("verify current scenario %q: %w", scenario.sourcePath, err)
		}
		equal, err := equalBytesContext(ctx, current, scenario.sourceBytes)
		if err != nil {
			return Catalog{}, err
		}
		if !equal {
			return Catalog{}, fmt.Errorf("scenario source changed after loading: %q", scenario.sourcePath)
		}
		digest, err := sha256Context(ctx, current)
		if err != nil {
			return Catalog{}, err
		}
		entries = append(entries, ScenarioEntry{
			ScenarioID: scenario.ID,
			Path:       scenario.sourcePath,
			SHA256:     digest,
		})
		seenIDs[scenario.ID] = scenario.sourcePath
		seenPaths[scenario.sourcePath] = scenario.ID
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].ScenarioID < entries[right].ScenarioID
	})
	if err := checkContext(ctx); err != nil {
		return Catalog{}, err
	}
	return Catalog{SchemaVersion: 1, Scenarios: entries}, nil
}

// WriteGeneratedCatalog loads, verifies, and publishes through one pinned root.
func WriteGeneratedCatalog(ctx context.Context, repoRoot string) error {
	return generatedCatalogOperation(ctx, repoRoot, true)
}

// CheckGeneratedCatalog loads and checks the catalog through one pinned root.
func CheckGeneratedCatalog(ctx context.Context, repoRoot string) error {
	return generatedCatalogOperation(ctx, repoRoot, false)
}

func generatedCatalogOperation(ctx context.Context, repoRoot string, write bool) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	rootPath, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	validator := schemavalidator.New(rootPath)
	defer validator.Close()
	if write {
		return writeGeneratedCatalogRooted(ctx, root, validator)
	}
	return checkGeneratedCatalogRooted(ctx, root, validator)
}

func writeGeneratedCatalogRooted(ctx context.Context, root *os.Root, validator *schemavalidator.Validator) error {
	scenarios, err := loadAllRooted(ctx, root, validator)
	if err != nil {
		return err
	}
	catalog, err := generateCatalogRooted(ctx, root, scenarios)
	if err != nil {
		return err
	}
	if err := recheckScenarioSources(ctx, root, scenarios); err != nil {
		return fmt.Errorf("recheck scenarios before catalog publication: %w", err)
	}
	if err := writeCatalogRooted(ctx, root, catalog); err != nil {
		return err
	}
	return verifyGeneratedCatalogPublication(ctx, root, scenarios, catalog)
}

func verifyGeneratedCatalogPublication(ctx context.Context, root *os.Root, scenarios []Scenario, catalog Catalog) error {
	if err := checkCatalogRooted(ctx, root, catalog); err != nil {
		return fmt.Errorf("check visible catalog after publication: %w", err)
	}
	if err := recheckScenarioSources(ctx, root, scenarios); err != nil {
		return fmt.Errorf("final scenario recheck after catalog publication: %w", err)
	}
	return nil
}

func checkGeneratedCatalogRooted(ctx context.Context, root *os.Root, validator *schemavalidator.Validator) error {
	scenarios, err := loadAllRooted(ctx, root, validator)
	if err != nil {
		return err
	}
	catalog, err := generateCatalogRooted(ctx, root, scenarios)
	if err != nil {
		return err
	}
	if err := checkCatalogRooted(ctx, root, catalog); err != nil {
		return err
	}
	if err := recheckScenarioSources(ctx, root, scenarios); err != nil {
		return fmt.Errorf("recheck scenarios after catalog check: %w", err)
	}
	return nil
}

func recheckScenarioSources(ctx context.Context, root *os.Root, scenarios []Scenario) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	captured := make(map[string][]byte, len(scenarios))
	expectedPaths := make([]string, 0, len(scenarios))
	for _, scenario := range scenarios {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if scenario.sourcePath == "" || len(scenario.sourceBytes) == 0 {
			return fmt.Errorf("scenario %q has no captured source", scenario.ID)
		}
		if _, duplicate := captured[scenario.sourcePath]; duplicate {
			return fmt.Errorf("duplicate captured scenario path %q", scenario.sourcePath)
		}
		captured[scenario.sourcePath] = scenario.sourceBytes
		expectedPaths = append(expectedPaths, scenario.sourcePath)
	}
	sort.Strings(expectedPaths)
	paths, err := enumerateScenarioPaths(ctx, root)
	if err != nil {
		return err
	}
	if err := requireExactScenarioPaths(ctx, paths, expectedPaths); err != nil {
		return err
	}
	for _, path := range expectedPaths {
		if err := checkContext(ctx); err != nil {
			return err
		}
		current, err := readRootedFile(ctx, root, path)
		if err != nil {
			return fmt.Errorf("re-read captured scenario %q: %w", path, err)
		}
		equal, err := equalBytesContext(ctx, current, captured[path])
		if err != nil {
			return err
		}
		if !equal {
			return fmt.Errorf("captured scenario bytes changed: %q", path)
		}
	}
	finalPaths, err := enumerateScenarioPaths(ctx, root)
	if err != nil {
		return err
	}
	return requireExactScenarioPaths(ctx, finalPaths, expectedPaths)
}

func requireExactScenarioPaths(ctx context.Context, actual, expected []string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("scenario source set has %d paths, want %d", len(actual), len(expected))
	}
	for index := range expected {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if actual[index] != expected[index] {
			return fmt.Errorf("scenario source set differs at path %d", index)
		}
	}
	return nil
}

func openPinnedDirectory(ctx context.Context, root *os.Root, path string) (*os.Root, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := validateRepositoryRelativePath(path); err != nil {
		return nil, err
	}
	if err := rejectSymlinkComponents(ctx, root, path); err != nil {
		return nil, err
	}
	directoryRoot, err := root.OpenRoot(path)
	if err != nil {
		return nil, fmt.Errorf("open rooted directory %q: %w", path, err)
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = directoryRoot.Close()
		}
	}()
	if err := verifyPinnedDirectoryIdentity(ctx, root, path, directoryRoot); err != nil {
		return nil, fmt.Errorf("recheck opened directory %q: %w", path, err)
	}
	closeOnError = false
	return directoryRoot, nil
}

func verifyPinnedDirectoryIdentity(ctx context.Context, repositoryRoot *os.Root, path string, pinnedRoot *os.Root) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if repositoryRoot == nil {
		return errors.New("repository root is nil")
	}
	if pinnedRoot == nil {
		return errors.New("pinned directory root is nil")
	}
	if err := validateRepositoryRelativePath(path); err != nil {
		return err
	}
	if err := rejectSymlinkComponents(ctx, repositoryRoot, path); err != nil {
		return err
	}
	pinnedInfo, err := pinnedRoot.Stat(".")
	if err != nil {
		return fmt.Errorf("inspect pinned directory %q: %w", path, err)
	}
	currentInfo, err := repositoryRoot.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect current directory %q: %w", path, err)
	}
	if currentInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("rooted directory became a symlink: %q", path)
	}
	if !pinnedInfo.IsDir() || !currentInfo.IsDir() {
		return fmt.Errorf("rooted path is not a directory: %q", path)
	}
	if !os.SameFile(pinnedInfo, currentInfo) {
		return fmt.Errorf("opened directory identity changed: %q", path)
	}
	return nil
}

// CatalogBytes returns deterministic indented JSON with one trailing newline.
func CatalogBytes(catalog Catalog) ([]byte, error) {
	return catalogBytesContext(context.Background(), catalog)
}

func catalogBytesContext(ctx context.Context, catalog Catalog) ([]byte, error) {
	if err := validateCatalogContext(ctx, catalog); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode scenario catalog: %w", err)
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// WriteCatalog atomically replaces the repository catalog with generated bytes.
func WriteCatalog(ctx context.Context, repoRoot string, catalog Catalog) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	_, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	return writeCatalogRooted(ctx, root, catalog)
}

func writeCatalogRooted(ctx context.Context, root *os.Root, catalog Catalog) error {
	data, err := catalogBytesContext(ctx, catalog)
	if err != nil {
		return err
	}
	conformanceRoot, err := openPinnedDirectory(ctx, root, "conformance")
	if err != nil {
		return fmt.Errorf("inspect catalog directory: %w", err)
	}
	defer conformanceRoot.Close()
	if info, err := conformanceRoot.Lstat(catalogRelativeName); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("scenario catalog path is a symlink")
		}
		if !info.Mode().IsRegular() {
			return errors.New("scenario catalog path is not a regular file")
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("inspect scenario catalog: %w", err)
	}

	temporaryPath, file, err := createCatalogTemp(ctx, conformanceRoot)
	if err != nil {
		return err
	}
	keepTemporary := true
	defer func() {
		_ = file.Close()
		if keepTemporary {
			_ = conformanceRoot.Remove(temporaryPath)
		}
	}()
	if err := writeAllContext(ctx, file, data); err != nil {
		return fmt.Errorf("write temporary scenario catalog: %w", err)
	}
	if err := file.Chmod(0o644); err != nil {
		return fmt.Errorf("set temporary scenario catalog mode: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temporary scenario catalog: %w", err)
	}
	if _, err := verifyOpenedFileIdentity(ctx, conformanceRoot, temporaryPath, file); err != nil {
		return fmt.Errorf("verify temporary scenario catalog: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary scenario catalog: %w", err)
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := conformanceRoot.Rename(temporaryPath, catalogRelativeName); err != nil {
		return fmt.Errorf("replace scenario catalog: %w", err)
	}
	keepTemporary = false
	directory, err := conformanceRoot.Open(".")
	if err != nil {
		return fmt.Errorf("open catalog directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync catalog directory: %w", err)
	}
	current, err := readRootedFile(ctx, conformanceRoot, catalogRelativeName)
	if err != nil {
		return fmt.Errorf("verify published scenario catalog: %w", err)
	}
	equal, err := equalBytesContext(ctx, current, data)
	if err != nil {
		return err
	}
	if !equal {
		return errors.New("published scenario catalog bytes do not match generated catalog")
	}
	if err := verifyPinnedDirectoryIdentity(ctx, root, "conformance", conformanceRoot); err != nil {
		return fmt.Errorf("verify visible catalog directory after publication: %w", err)
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	return nil
}

// CheckCatalog requires the current catalog bytes to equal generated bytes.
func CheckCatalog(ctx context.Context, repoRoot string, catalog Catalog) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	_, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return err
	}
	defer root.Close()
	return checkCatalogRooted(ctx, root, catalog)
}

func checkCatalogRooted(ctx context.Context, root *os.Root, catalog Catalog) error {
	expected, err := catalogBytesContext(ctx, catalog)
	if err != nil {
		return err
	}
	current, err := readRootedFile(ctx, root, catalogPath)
	if err != nil {
		return fmt.Errorf("read current scenario catalog: %w", err)
	}
	equal, err := equalBytesContext(ctx, current, expected)
	if err != nil {
		return err
	}
	if !equal {
		return errors.New("scenario catalog bytes do not match generated catalog")
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	return nil
}

func validateCatalogContext(ctx context.Context, catalog Catalog) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if catalog.SchemaVersion != 1 {
		return fmt.Errorf("scenario catalog schema_version is %d, want 1", catalog.SchemaVersion)
	}
	if len(catalog.Scenarios) == 0 {
		return errors.New("scenario catalog must not be empty")
	}
	var previousID contract.ScenarioID
	seenPaths := make(map[string]struct{}, len(catalog.Scenarios))
	for index, entry := range catalog.Scenarios {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if entry.ScenarioID == "" {
			return fmt.Errorf("scenario catalog entry %d has an empty scenario_id", index)
		}
		if index > 0 && entry.ScenarioID <= previousID {
			return errors.New("scenario catalog entries are not strictly sorted by scenario_id")
		}
		if err := validateScenarioPath(entry.Path); err != nil {
			return fmt.Errorf("validate scenario catalog path %q: %w", entry.Path, err)
		}
		if err := validateScenarioIdentity(entry.Path, entry.ScenarioID); err != nil {
			return err
		}
		if _, duplicate := seenPaths[entry.Path]; duplicate {
			return fmt.Errorf("duplicate scenario catalog path %q", entry.Path)
		}
		digest, err := hex.DecodeString(entry.SHA256)
		if err != nil || len(digest) != sha256.Size || entry.SHA256 != hex.EncodeToString(digest) {
			return fmt.Errorf("scenario catalog entry %q has an invalid SHA-256", entry.ScenarioID)
		}
		seenPaths[entry.Path] = struct{}{}
		previousID = entry.ScenarioID
	}
	return nil
}

func createCatalogTemp(ctx context.Context, root *os.Root) (string, *os.File, error) {
	for attempt := 0; attempt < 16; attempt++ {
		if err := checkContext(ctx); err != nil {
			return "", nil, err
		}
		var random [12]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, fmt.Errorf("select temporary scenario catalog name: %w", err)
		}
		path := ".catalog.json.tmp-" + hex.EncodeToString(random[:])
		file, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			if _, identityErr := verifyOpenedFileIdentity(ctx, root, path, file); identityErr != nil {
				_ = file.Close()
				_ = root.Remove(path)
				return "", nil, fmt.Errorf("verify temporary scenario catalog identity: %w", identityErr)
			}
			return path, file, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", nil, fmt.Errorf("create temporary scenario catalog: %w", err)
		}
	}
	return "", nil, errors.New("could not create a unique temporary scenario catalog")
}

func writeAllContext(ctx context.Context, file *os.File, data []byte) error {
	for len(data) > 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		count, err := file.Write(data)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}

func equalBytesContext(ctx context.Context, left, right []byte) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if len(left) != len(right) {
		return false, nil
	}
	const chunkSize = 32 * 1024
	for offset := 0; offset < len(left); offset += chunkSize {
		if err := checkContext(ctx); err != nil {
			return false, err
		}
		end := min(offset+chunkSize, len(left))
		if !bytes.Equal(left[offset:end], right[offset:end]) {
			return false, nil
		}
	}
	return true, nil
}

func sha256Context(ctx context.Context, data []byte) (string, error) {
	digest := sha256.New()
	const chunkSize = 32 * 1024
	for offset := 0; offset < len(data); offset += chunkSize {
		if err := checkContext(ctx); err != nil {
			return "", err
		}
		end := min(offset+chunkSize, len(data))
		_, _ = digest.Write(data[offset:end])
	}
	if err := checkContext(ctx); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
