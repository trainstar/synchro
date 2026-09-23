package scenarios

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/internal/schemavalidator"
)

// SHA256 returns the exact digest of the loaded source bytes.
func SHA256(scenario Scenario) string {
	if scenario.sourcePath == "" || len(scenario.sourceBytes) == 0 {
		return ""
	}
	encoded, err := json.Marshal(scenario)
	if err != nil {
		return ""
	}
	var authored any
	var current any
	if jsonstrict.Decode(scenario.sourceBytes, &authored) != nil || jsonstrict.Decode(encoded, &current) != nil || !reflect.DeepEqual(authored, current) {
		return ""
	}
	digest := sha256.Sum256(scenario.sourceBytes)
	return hex.EncodeToString(digest[:])
}

// Clone returns an isolated scenario while preserving its source binding.
func Clone(scenario Scenario) (Scenario, error) {
	encoded, err := json.Marshal(scenario)
	if err != nil {
		return Scenario{}, err
	}
	var result Scenario
	if err := jsonstrict.Decode(encoded, &result); err != nil {
		return Scenario{}, err
	}
	result.sourcePath = scenario.sourcePath
	result.sourceBytes = append([]byte(nil), scenario.sourceBytes...)
	result.makeTargets = cloneStringSet(scenario.makeTargets)
	return result, nil
}

const (
	scenarioDirectory  = "conformance/scenarios"
	scenarioSchemaPath = "conformance/schemas/scenario-v2.schema.json"
	makefilePath       = "Makefile"
)

var makeTargetPattern = regexp.MustCompile(`^([A-Za-z0-9_.-]+(?:[ \t]+[A-Za-z0-9_.-]+)*):(?:[ \t]|$)`)

// LoadFile loads one repository-relative scenario through a pinned repository root.
func LoadFile(ctx context.Context, repoRoot, path string) (Scenario, error) {
	if err := checkContext(ctx); err != nil {
		return Scenario{}, err
	}
	rootPath, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return Scenario{}, err
	}
	defer root.Close()
	validator := schemavalidator.New(rootPath)
	defer validator.Close()
	return loadFileRooted(ctx, root, validator, path)
}

// LoadBytes decodes and schema-validates exact supplied scenario bytes.
// Repository files supply only the schema and Makefile target set.
func LoadBytes(ctx context.Context, repoRoot, path string, data []byte) (Scenario, error) {
	return LoadBytesWithSchema(ctx, repoRoot, path, data, nil)
}

// LoadBytesWithSchema decodes exact supplied scenario bytes with an optional
// captured schema. A nil schema selects the repository schema.
func LoadBytesWithSchema(ctx context.Context, repoRoot, path string, data, schema []byte) (Scenario, error) {
	if err := checkContext(ctx); err != nil {
		return Scenario{}, err
	}
	rootPath, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return Scenario{}, err
	}
	defer root.Close()
	if err := validateScenarioPath(path); err != nil {
		return Scenario{}, fmt.Errorf("validate scenario path: %w", err)
	}
	if schema == nil {
		schema, err = readRootedFile(ctx, root, scenarioSchemaPath)
		if err != nil {
			return Scenario{}, fmt.Errorf("capture scenario schema: %w", err)
		}
	} else {
		schema = append([]byte(nil), schema...)
	}
	makeTargets, err := loadMakeTargets(ctx, root)
	if err != nil {
		return Scenario{}, err
	}
	validator := schemavalidator.New(rootPath)
	defer validator.Close()
	return loadCapturedScenario(ctx, validator, schema, path, append([]byte(nil), data...), makeTargets)
}

func loadFileRooted(ctx context.Context, root *os.Root, validator *schemavalidator.Validator, path string) (Scenario, error) {
	if err := validateScenarioPath(path); err != nil {
		return Scenario{}, fmt.Errorf("validate scenario path: %w", err)
	}
	schema, err := readRootedFile(ctx, root, scenarioSchemaPath)
	if err != nil {
		return Scenario{}, fmt.Errorf("capture scenario schema: %w", err)
	}
	data, err := readRootedFile(ctx, root, path)
	if err != nil {
		return Scenario{}, fmt.Errorf("capture scenario %q: %w", path, err)
	}
	makeTargets, err := loadMakeTargets(ctx, root)
	if err != nil {
		return Scenario{}, err
	}
	return loadCapturedScenario(ctx, validator, schema, path, data, makeTargets)
}

// LoadAll loads all scenario JSON files through one pinned repository root.
func LoadAll(ctx context.Context, repoRoot string) ([]Scenario, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	rootPath, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	validator := schemavalidator.New(rootPath)
	defer validator.Close()
	return loadAllRooted(ctx, root, validator)
}

func loadAllRooted(ctx context.Context, root *os.Root, validator *schemavalidator.Validator) ([]Scenario, error) {
	schema, err := readRootedFile(ctx, root, scenarioSchemaPath)
	if err != nil {
		return nil, fmt.Errorf("capture scenario schema: %w", err)
	}
	paths, err := enumerateScenarioPaths(ctx, root)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, errors.New("no scenario JSON files found")
	}
	makeTargets, err := loadMakeTargets(ctx, root)
	if err != nil {
		return nil, err
	}

	scenarios := make([]Scenario, 0, len(paths))
	seenIDs := make(map[string]string, len(paths))
	for _, path := range paths {
		if err := checkContext(ctx); err != nil {
			return nil, err
		}
		data, err := readRootedFile(ctx, root, path)
		if err != nil {
			return nil, fmt.Errorf("capture scenario %q: %w", path, err)
		}
		scenario, err := loadCapturedScenario(ctx, validator, schema, path, data, makeTargets)
		if err != nil {
			return nil, err
		}
		id := string(scenario.ID)
		if previousPath, duplicate := seenIDs[id]; duplicate {
			return nil, fmt.Errorf("duplicate scenario ID %q in %q and %q", id, previousPath, path)
		}
		seenIDs[id] = path
		scenarios = append(scenarios, scenario)
	}
	sort.Slice(scenarios, func(left, right int) bool {
		return scenarios[left].ID < scenarios[right].ID
	})
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	return scenarios, nil
}

func loadCapturedScenario(ctx context.Context, validator *schemavalidator.Validator, schema []byte, path string, data []byte, makeTargets map[string]struct{}) (Scenario, error) {
	if err := checkContext(ctx); err != nil {
		return Scenario{}, err
	}
	if err := rejectReadinessClaimKeys(ctx, data); err != nil {
		return Scenario{}, fmt.Errorf("validate scenario %q claim keys: %w", path, err)
	}
	if err := validator.ValidateCapturedBytes(ctx, scenarioSchemaPath, schema, data); err != nil {
		return Scenario{}, fmt.Errorf("validate scenario %q schema: %w", path, err)
	}
	var scenario Scenario
	if err := jsonstrict.Decode(data, &scenario); err != nil {
		return Scenario{}, fmt.Errorf("decode scenario %q: %w", path, err)
	}
	if err := validateScenarioIdentity(path, scenario.ID); err != nil {
		return Scenario{}, fmt.Errorf("validate scenario %q identity: %w", path, err)
	}
	if err := checkContext(ctx); err != nil {
		return Scenario{}, err
	}
	scenario.sourcePath = path
	scenario.sourceBytes = append([]byte(nil), data...)
	scenario.makeTargets = cloneStringSet(makeTargets)
	return scenario, nil
}

func loadMakeTargets(ctx context.Context, root *os.Root) (map[string]struct{}, error) {
	data, err := readRootedFile(ctx, root, makefilePath)
	if err != nil {
		return nil, fmt.Errorf("capture repository Makefile: %w", err)
	}
	targets := make(map[string]struct{})
	for _, line := range strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n") {
		if err := checkContext(ctx); err != nil {
			return nil, err
		}
		match := makeTargetPattern.FindStringSubmatch(line)
		if len(match) != 2 {
			continue
		}
		for _, target := range strings.Fields(match[1]) {
			targets[target] = struct{}{}
		}
	}
	if len(targets) == 0 {
		return nil, errors.New("repository Makefile defines no targets")
	}
	return targets, nil
}

func cloneStringSet(source map[string]struct{}) map[string]struct{} {
	clone := make(map[string]struct{}, len(source))
	for value := range source {
		clone[value] = struct{}{}
	}
	return clone
}

func enumerateScenarioPaths(ctx context.Context, root *os.Root) ([]string, error) {
	if err := rejectSymlinkComponents(ctx, root, scenarioDirectory); err != nil {
		return nil, fmt.Errorf("inspect scenario directory: %w", err)
	}
	var paths []string
	err := fs.WalkDir(root.FS(), scenarioDirectory, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if walkErr != nil {
			return fmt.Errorf("enumerate %q: %w", path, walkErr)
		}
		info, err := root.Lstat(filepath.FromSlash(path))
		if err != nil {
			return fmt.Errorf("inspect enumerated path %q: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is forbidden under %s: %q", scenarioDirectory, path)
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular file is forbidden under %s: %q", scenarioDirectory, path)
		}
		if strings.HasSuffix(path, ".json") {
			if err := validateScenarioPath(path); err != nil {
				return err
			}
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("enumerate scenarios: %w", err)
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	return paths, nil
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

func validateScenarioPath(path string) error {
	if err := validateRepositoryRelativePath(path); err != nil {
		return err
	}
	if !strings.HasPrefix(path, scenarioDirectory+"/") {
		return fmt.Errorf("scenario path is outside %s: %q", scenarioDirectory, path)
	}
	if !strings.HasSuffix(path, ".json") {
		return fmt.Errorf("scenario path does not end in .json: %q", path)
	}
	return nil
}

func validateScenarioIdentity(path string, id contract.ScenarioID) error {
	relative := strings.TrimPrefix(path, scenarioDirectory+"/")
	directory, name := filepath.ToSlash(filepath.Dir(relative)), filepath.Base(relative)
	if directory != "server" && directory != "performance" {
		return nil
	}
	stem := strings.TrimSuffix(name, filepath.Ext(name))
	prefix := "SCN-"
	if directory == "performance" {
		prefix = "SCN-PERF-"
	}
	wanted := prefix + strings.ToUpper(stem)
	if string(id) != wanted {
		return fmt.Errorf("scenario ID %q does not match filename identity %q", id, wanted)
	}
	return nil
}

func validateRepositoryRelativePath(path string) error {
	if path == "" {
		return errors.New("repository-relative path is empty")
	}
	if strings.IndexByte(path, 0) >= 0 {
		return errors.New("repository-relative path contains NUL")
	}
	if strings.Contains(path, `\`) {
		return fmt.Errorf("repository-relative path contains backslash: %q", path)
	}
	if filepath.IsAbs(path) || strings.HasPrefix(path, "/") {
		return fmt.Errorf("repository-relative path is absolute: %q", path)
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	if clean != path || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("repository-relative path is not canonical: %q", path)
	}
	for _, component := range strings.Split(path, "/") {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("repository-relative path has an empty or dot component: %q", path)
		}
	}
	return nil
}

func readRootedFile(ctx context.Context, root *os.Root, path string) ([]byte, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if root == nil {
		return nil, errors.New("repository root is nil")
	}
	if err := validateRepositoryRelativePath(path); err != nil {
		return nil, err
	}
	if err := rejectSymlinkComponents(ctx, root, path); err != nil {
		return nil, err
	}
	file, err := root.Open(filepath.FromSlash(path))
	if err != nil {
		return nil, fmt.Errorf("open rooted repository file %q: %w", path, err)
	}
	defer file.Close()
	if _, err := verifyOpenedFileIdentity(ctx, root, path, file); err != nil {
		return nil, err
	}
	data, err := readAllContext(ctx, file)
	if err != nil {
		return nil, fmt.Errorf("read rooted repository file %q: %w", path, err)
	}
	stable, err := rereadOpenedFile(ctx, file, data)
	if err != nil {
		return nil, fmt.Errorf("repeat rooted repository file read %q: %w", path, err)
	}
	if _, err := verifyOpenedFileIdentity(ctx, root, path, file); err != nil {
		return nil, fmt.Errorf("verify repository file after repeated read %q: %w", path, err)
	}
	if !stable {
		return nil, fmt.Errorf("repository file bytes changed across repeated reads: %q", path)
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("repository file contains invalid UTF-8: %q", path)
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	return data, nil
}

func rereadOpenedFile(ctx context.Context, file *os.File, first []byte) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("opened regular file cannot seek to its start: %w", err)
	}
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	second, err := readAllContext(ctx, file)
	if err != nil {
		return false, err
	}
	return equalBytesContext(ctx, first, second)
}

func verifyOpenedFileIdentity(ctx context.Context, root *os.Root, path string, file *os.File) (os.FileInfo, error) {
	if err := rejectSymlinkComponents(ctx, root, path); err != nil {
		return nil, fmt.Errorf("recheck opened repository file %q: %w", path, err)
	}
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("inspect rooted repository file %q: %w", path, err)
	}
	currentInfo, err := root.Lstat(filepath.FromSlash(path))
	if err != nil {
		return nil, fmt.Errorf("inspect current rooted repository file %q: %w", path, err)
	}
	if currentInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("repository file became a symlink: %q", path)
	}
	if !openedInfo.Mode().IsRegular() || !currentInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("repository path is not a regular file: %q", path)
	}
	if !os.SameFile(openedInfo, currentInfo) {
		return nil, fmt.Errorf("opened repository file identity changed: %q", path)
	}
	return openedInfo, nil
}

func rejectSymlinkComponents(ctx context.Context, root *os.Root, path string) error {
	var current string
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
			return fmt.Errorf("inspect path component %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("repository path has symlink component %q", current)
		}
	}
	return nil
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
			if contextErr := checkContext(ctx); contextErr != nil {
				return nil, contextErr
			}
			return output.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func rejectReadinessClaimKeys(ctx context.Context, data []byte) error {
	var document any
	if err := jsonstrict.Decode(data, &document); err != nil {
		return err
	}
	return inspectClaimValue(ctx, document, "$")
}

func inspectClaimValue(ctx context.Context, value any, location string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	switch value := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if isReadinessClaimKey(key) {
				return fmt.Errorf("forbidden readiness claim key %q at %s", key, location)
			}
			if err := inspectClaimValue(ctx, value[key], location+"."+key); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range value {
			if err := inspectClaimValue(ctx, child, fmt.Sprintf("%s[%d]", location, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func isReadinessClaimKey(key string) bool {
	key = strings.ReplaceAll(strings.ToLower(key), "_", "-")
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

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context canceled: %w", err)
	}
	return nil
}
