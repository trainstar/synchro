package vectors

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
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/internal/schemavalidator"
)

const (
	vectorDirectory           = "conformance/vectors"
	vectorCatalogPath         = "conformance/vectors/catalog.json"
	vectorSchemaPath          = "conformance/schemas/vector-catalog-v1.schema.json"
	aggregateHashDomain       = "synchro:v3:vector-set-aggregate:v1"
	maxRepositoryJSONFileSize = 8 * 1024 * 1024
	maxVectorSourceBytes      = 64 * 1024 * 1024
	maxVectorSets             = 256
	maxVectorDirectoryEntries = 512
)

var vectorIDPattern = regexp.MustCompile(`^VEC-[A-Z0-9]+(?:-[A-Z0-9]+)*-[0-9]{3}$`)

type catalogDocument struct {
	SchemaURI     string               `json:"$schema"`
	SchemaVersion int                  `json:"schema_version"`
	Release       string               `json:"release"`
	VectorSets    []catalogVectorEntry `json:"vector_sets"`
}

type catalogVectorEntry struct {
	VectorSetID       contract.VectorSetID `json:"vector_set_id"`
	Path              string               `json:"path"`
	SourceSHA256      string               `json:"source_sha256"`
	VectorCount       int                  `json:"vector_count"`
	AggregateSHA256   string               `json:"aggregate_sha256"`
	RequiredLanguages []string             `json:"required_languages"`
}

type aggregateDocument struct {
	Algorithm    string                `json:"algorithm"`
	VectorCount  int                   `json:"vector_count"`
	VectorHashes []aggregateVectorHash `json:"vector_hashes"`
	SHA256       string                `json:"sha256"`
}

type aggregateVectorHash struct {
	VectorID            string  `json:"vector_id"`
	ExpectedBytesSHA256 *string `json:"expected_bytes_sha256"`
	ExpectedSHA256      *string `json:"expected_sha256"`
}

// Load validates the catalog and every bound vector source through one root.
func Load(ctx context.Context, repoRoot string) (Catalog, error) {
	if err := contextError(ctx); err != nil {
		return Catalog{}, err
	}
	rootPath, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return Catalog{}, err
	}
	defer root.Close()
	validator := schemavalidator.New(rootPath)
	defer validator.Close()

	schema, err := readRootedFile(ctx, root, vectorSchemaPath)
	if err != nil {
		return Catalog{}, fmt.Errorf("capture vector catalog schema: %w", err)
	}
	catalogBytes, err := readRootedFile(ctx, root, vectorCatalogPath)
	if err != nil {
		return Catalog{}, fmt.Errorf("capture vector catalog: %w", err)
	}
	if err := validateJSONDocument(schema, jsonValidation{iJSON: true, safeInteger: true}); err != nil {
		return Catalog{}, fmt.Errorf("validate vector catalog schema JSON: %w", err)
	}
	if err := validateJSONDocument(catalogBytes, jsonValidation{iJSON: true, safeInteger: true}); err != nil {
		return Catalog{}, fmt.Errorf("validate vector catalog JSON: %w", err)
	}
	var document catalogDocument
	if err := json.Unmarshal(catalogBytes, &document); err != nil {
		return Catalog{}, fmt.Errorf("decode vector catalog: %w", err)
	}
	if err := validateCatalogEntries(document.VectorSets); err != nil {
		return Catalog{}, err
	}
	if err := validator.ValidateCapturedBytes(ctx, vectorSchemaPath, schema, catalogBytes); err != nil {
		return Catalog{}, fmt.Errorf("validate vector catalog schema: %w", err)
	}

	captured := map[string][]byte{
		vectorSchemaPath:  append([]byte(nil), schema...),
		vectorCatalogPath: append([]byte(nil), catalogBytes...),
	}
	sets := make(map[contract.VectorSetID]VectorSet, len(document.VectorSets))
	expectedPaths := []string{vectorCatalogPath}
	totalSourceBytes := len(schema) + len(catalogBytes)
	for _, entry := range document.VectorSets {
		if err := contextError(ctx); err != nil {
			return Catalog{}, err
		}
		source, err := readRootedFile(ctx, root, entry.Path)
		if err != nil {
			return Catalog{}, fmt.Errorf("capture vector set %q: %w", entry.Path, err)
		}
		if len(source) > maxVectorSourceBytes-totalSourceBytes {
			return Catalog{}, fmt.Errorf("vector sources exceed %d total bytes", maxVectorSourceBytes)
		}
		totalSourceBytes += len(source)
		sourceDigest := sha256.Sum256(source)
		if hex.EncodeToString(sourceDigest[:]) != entry.SourceSHA256 {
			return Catalog{}, fmt.Errorf("vector set %q source SHA-256 does not match catalog", entry.Path)
		}
		set, err := parseVectorSet(entry, source)
		if err != nil {
			return Catalog{}, fmt.Errorf("validate vector set %q: %w", entry.Path, err)
		}
		sets[entry.VectorSetID] = set
		captured[entry.Path] = append([]byte(nil), source...)
		expectedPaths = append(expectedPaths, entry.Path)
	}
	if err := requireExactVectorPaths(ctx, root, expectedPaths); err != nil {
		return Catalog{}, err
	}
	if err := verifyCapturedSources(ctx, root, captured); err != nil {
		return Catalog{}, err
	}
	if err := requireExactVectorPaths(ctx, root, expectedPaths); err != nil {
		return Catalog{}, err
	}
	return Catalog{sets: sets}, nil
}

func validateCatalogEntries(entries []catalogVectorEntry) error {
	if len(entries) == 0 || len(entries) > maxVectorSets {
		return fmt.Errorf("vector catalog set count is outside 1..%d", maxVectorSets)
	}
	seenIDs := make(map[contract.VectorSetID]struct{}, len(entries))
	seenPaths := make(map[string]struct{}, len(entries))
	for index, entry := range entries {
		if index > 0 && entries[index-1].VectorSetID >= entry.VectorSetID {
			return errors.New("vector catalog IDs are not strictly ordered")
		}
		if _, duplicate := seenIDs[entry.VectorSetID]; duplicate {
			return fmt.Errorf("duplicate vector_set_id %q", entry.VectorSetID)
		}
		if _, duplicate := seenPaths[entry.Path]; duplicate {
			return fmt.Errorf("duplicate vector-set path %q", entry.Path)
		}
		seenIDs[entry.VectorSetID] = struct{}{}
		seenPaths[entry.Path] = struct{}{}
		if err := validateVectorPath(entry.Path); err != nil {
			return err
		}
		if !isLowerSHA256(entry.SourceSHA256) || !isLowerSHA256(entry.AggregateSHA256) {
			return fmt.Errorf("vector catalog entry %q has an invalid digest", entry.VectorSetID)
		}
	}
	return nil
}

func parseVectorSet(entry catalogVectorEntry, source []byte) (VectorSet, error) {
	object, err := strictJSONObject(source, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return VectorSet{}, err
	}
	if err := requireObjectKeys(object, []string{"vector_set_id", "vectors", "aggregate"}, nil); err != nil {
		return VectorSet{}, err
	}
	idText, err := decodeRequiredString(object["vector_set_id"], "vector_set_id")
	if err != nil {
		return VectorSet{}, err
	}
	id := contract.VectorSetID(idText)
	if id != entry.VectorSetID {
		return VectorSet{}, errors.New("vector source ID does not match catalog")
	}
	values, err := decodeJSONArray(object["vectors"], jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return VectorSet{}, err
	}
	if len(values) != entry.VectorCount {
		return VectorSet{}, errors.New("vector source count does not match catalog")
	}
	vectors := make([]Vector, 0, len(values))
	seenIDs := make(map[string]struct{}, len(values))
	for index, value := range values {
		vector, err := parseVector(value)
		if err != nil {
			return VectorSet{}, err
		}
		if _, duplicate := seenIDs[vector.ID]; duplicate {
			return VectorSet{}, fmt.Errorf("duplicate vector_id %q", vector.ID)
		}
		if index > 0 && vectors[index-1].ID >= vector.ID {
			return VectorSet{}, errors.New("vector IDs are not strictly ordered")
		}
		seenIDs[vector.ID] = struct{}{}
		vectors = append(vectors, vector)
	}
	aggregate, err := parseAggregate(object["aggregate"], vectors)
	if err != nil {
		return VectorSet{}, err
	}
	if aggregate.SHA256 != entry.AggregateSHA256 {
		return VectorSet{}, errors.New("vector aggregate does not match catalog")
	}
	return VectorSet{
		ID: id, Path: entry.Path, SourceSHA256: entry.SourceSHA256,
		AggregateSHA256: aggregate.SHA256, Vectors: vectors,
		sourceBytes: append([]byte(nil), source...),
	}, nil
}

func parseVector(raw json.RawMessage) (Vector, error) {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return Vector{}, err
	}
	if err := requireObjectKeys(object, []string{"vector_id", "kind", "valid", "input", "expected"}, nil); err != nil {
		return Vector{}, err
	}
	id, err := decodeRequiredString(object["vector_id"], "vector_id")
	if err != nil {
		return Vector{}, err
	}
	if !vectorIDPattern.MatchString(id) {
		return Vector{}, fmt.Errorf("invalid vector_id %q", id)
	}
	kind, err := decodeRequiredString(object["kind"], "kind")
	if err != nil {
		return Vector{}, err
	}
	if !knownVectorKind(kind) {
		return Vector{}, fmt.Errorf("unsupported vector kind %q", kind)
	}
	valid, err := decodeBoolean(object["valid"], "valid")
	if err != nil {
		return Vector{}, err
	}
	if err := validateVectorInputShape(kind, object["input"]); err != nil {
		return Vector{}, fmt.Errorf("vector %q input: %w", id, err)
	}
	expectedObject, err := strictJSONObject(object["expected"], jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return Vector{}, err
	}
	if err := requireObjectKeys(expectedObject, []string{
		"canonical_bytes_hex", "expected_bytes_sha256", "expected_sha256",
	}, nil); err != nil {
		return Vector{}, err
	}
	var expected Expected
	if err := json.Unmarshal(object["expected"], &expected); err != nil {
		return Vector{}, fmt.Errorf("decode expected values: %w", err)
	}
	if err := validateExpected(kind, valid, expected); err != nil {
		return Vector{}, fmt.Errorf("vector %q expected values: %w", id, err)
	}
	return Vector{
		ID: id, Kind: kind, Valid: valid,
		Input: append(json.RawMessage(nil), object["input"]...), Expected: expected,
	}, nil
}

func knownVectorKind(kind string) bool {
	switch kind {
	case "typed_value", "schema_manifest", "row_identity", "row_digest", "scope_digest", "mutation_fingerprint", "batch_fingerprint":
		return true
	default:
		return false
	}
}

func validateVectorInputShape(kind string, raw json.RawMessage) error {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return err
	}
	var required []string
	switch kind {
	case "typed_value":
		required = []string{"field_spec", "raw_json"}
	case "schema_manifest":
		required = []string{"manifest_json"}
	case "row_identity":
		required = []string{"manifest_json", "table_id", "pk_json"}
	case "row_digest":
		required = []string{"manifest_json", "table_id", "pk_json", "row_json", "server_version"}
	case "scope_digest":
		required = []string{"schema_hash", "scope_id", "entries"}
	case "mutation_fingerprint":
		required = []string{"authenticated_user_id", "client_id", "mutation_json"}
	case "batch_fingerprint":
		required = []string{"authenticated_user_id", "batch_json"}
	}
	if err := requireObjectKeys(object, required, nil); err != nil {
		return err
	}
	if kind == "typed_value" {
		fieldSpec, err := strictJSONObject(object["field_spec"], jsonValidation{iJSON: true, safeInteger: true})
		if err != nil {
			return err
		}
		if err := requireObjectKeys(fieldSpec, []string{"type", "nullable"}, []string{"precision", "scale"}); err != nil {
			return err
		}
	}
	if kind == "scope_digest" {
		entries, err := decodeJSONArray(object["entries"], jsonValidation{iJSON: true, safeInteger: true})
		if err != nil {
			return err
		}
		for _, entry := range entries {
			entryObject, err := strictJSONObject(entry, jsonValidation{iJSON: true, safeInteger: true})
			if err != nil {
				return err
			}
			if err := requireObjectKeys(entryObject, []string{"row_identity_hex", "row_digest_hex"}, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateExpected(kind string, valid bool, expected Expected) error {
	if !valid {
		if expected.CanonicalBytesHex != nil || expected.ExpectedBytesSHA256 != nil || expected.ExpectedSHA256 != nil {
			return errors.New("invalid vector has non-null expected output")
		}
		return nil
	}
	if expected.CanonicalBytesHex == nil || expected.ExpectedBytesSHA256 == nil {
		return errors.New("valid vector omits canonical bytes or their hash")
	}
	canonical, err := decodeLowerHex(*expected.CanonicalBytesHex)
	if err != nil {
		return fmt.Errorf("canonical bytes: %w", err)
	}
	if !isLowerSHA256(*expected.ExpectedBytesSHA256) {
		return errors.New("expected_bytes_sha256 is invalid")
	}
	computed := sha256.Sum256(canonical)
	if hex.EncodeToString(computed[:]) != *expected.ExpectedBytesSHA256 {
		return errors.New("canonical bytes do not match expected_bytes_sha256")
	}
	hashKind := kind == "schema_manifest" || kind == "row_digest" || kind == "scope_digest" || kind == "mutation_fingerprint" || kind == "batch_fingerprint"
	if hashKind {
		if expected.ExpectedSHA256 == nil || !isLowerSHA256(*expected.ExpectedSHA256) {
			return errors.New("valid digest vector omits expected_sha256")
		}
		if *expected.ExpectedSHA256 != *expected.ExpectedBytesSHA256 {
			return errors.New("expected hash does not match preimage hash")
		}
	} else if expected.ExpectedSHA256 != nil {
		return errors.New("non-digest vector has expected_sha256")
	}
	return nil
}

func parseAggregate(raw json.RawMessage, vectors []Vector) (aggregateDocument, error) {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return aggregateDocument{}, err
	}
	if err := requireObjectKeys(object, []string{"algorithm", "vector_count", "vector_hashes", "sha256"}, nil); err != nil {
		return aggregateDocument{}, err
	}
	vectorHashes, err := decodeJSONArray(object["vector_hashes"], jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return aggregateDocument{}, err
	}
	for index, rawHash := range vectorHashes {
		hashObject, err := strictJSONObject(rawHash, jsonValidation{iJSON: true, safeInteger: true})
		if err != nil {
			return aggregateDocument{}, fmt.Errorf("aggregate vector_hashes[%d]: %w", index, err)
		}
		if err := requireObjectKeys(hashObject, []string{"vector_id", "expected_bytes_sha256", "expected_sha256"}, nil); err != nil {
			return aggregateDocument{}, fmt.Errorf("aggregate vector_hashes[%d]: %w", index, err)
		}
	}
	var aggregate aggregateDocument
	if err := json.Unmarshal(raw, &aggregate); err != nil {
		return aggregateDocument{}, err
	}
	if aggregate.Algorithm != "sha-256" || aggregate.VectorCount != len(vectors) || len(aggregate.VectorHashes) != len(vectors) {
		return aggregateDocument{}, errors.New("aggregate algorithm or count is invalid")
	}
	for index, vector := range vectors {
		copy := aggregate.VectorHashes[index]
		if copy.VectorID != vector.ID || !equalOptionalString(copy.ExpectedBytesSHA256, vector.Expected.ExpectedBytesSHA256) || !equalOptionalString(copy.ExpectedSHA256, vector.Expected.ExpectedSHA256) {
			return aggregateDocument{}, errors.New("aggregate vector hashes do not copy vector expectations in order")
		}
	}
	preimageObject := struct {
		Algorithm    string                `json:"algorithm"`
		VectorCount  int                   `json:"vector_count"`
		VectorHashes []aggregateVectorHash `json:"vector_hashes"`
	}{aggregate.Algorithm, aggregate.VectorCount, aggregate.VectorHashes}
	encoded, err := json.Marshal(preimageObject)
	if err != nil {
		return aggregateDocument{}, err
	}
	canonical, err := canonicalizeJCS(encoded)
	if err != nil {
		return aggregateDocument{}, err
	}
	preimage := append([]byte(aggregateHashDomain), 0)
	preimage = append(preimage, canonical...)
	computed := sha256.Sum256(preimage)
	if !isLowerSHA256(aggregate.SHA256) || hex.EncodeToString(computed[:]) != aggregate.SHA256 {
		return aggregateDocument{}, errors.New("aggregate SHA-256 mismatch")
	}
	return aggregate, nil
}

func equalOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func validateVectorPath(path string) error {
	if err := validateRepositoryRelativePath(path); err != nil {
		return err
	}
	if !strings.HasPrefix(path, vectorDirectory+"/") || path == vectorCatalogPath || !strings.HasSuffix(path, ".json") {
		return fmt.Errorf("invalid vector-set path %q", path)
	}
	return nil
}

func requireExactVectorPaths(ctx context.Context, root *os.Root, expected []string) error {
	paths, err := enumerateVectorPaths(ctx, root)
	if err != nil {
		return err
	}
	sort.Strings(expected)
	if len(paths) != len(expected) {
		return errors.New("vector directory has an unbound or missing JSON file")
	}
	for index := range paths {
		if paths[index] != expected[index] {
			return errors.New("vector directory has an unbound or missing JSON file")
		}
	}
	return nil
}

func enumerateVectorPaths(ctx context.Context, root *os.Root) ([]string, error) {
	if err := rejectSymlinkComponents(ctx, root, vectorDirectory); err != nil {
		return nil, err
	}
	var paths []string
	entryCount := 0
	err := fs.WalkDir(root.FS(), vectorDirectory, func(path string, entry fs.DirEntry, walkErr error) error {
		if err := contextError(ctx); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		entryCount++
		if entryCount > maxVectorDirectoryEntries {
			return fmt.Errorf("vector directory exceeds %d entries", maxVectorDirectoryEntries)
		}
		info, err := root.Lstat(filepath.FromSlash(path))
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is forbidden under %s: %q", vectorDirectory, path)
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("nonregular file is forbidden under %s: %q", vectorDirectory, path)
		}
		if strings.HasSuffix(path, ".json") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("enumerate vector files: %w", err)
	}
	sort.Strings(paths)
	return paths, nil
}

func verifyCapturedSources(ctx context.Context, root *os.Root, captured map[string][]byte) error {
	paths := make([]string, 0, len(captured))
	for path := range captured {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		current, err := readRootedFile(ctx, root, path)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, captured[path]) {
			return fmt.Errorf("captured vector source changed: %q", path)
		}
	}
	return nil
}

func resolveRepositoryRoot(repoRoot string) (string, error) {
	if repoRoot == "" {
		return "", errors.New("repository root is empty")
	}
	absRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		return "", err
	}
	realRoot, err := filepath.EvalSymlinks(absRoot)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(realRoot)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", errors.New("repository root is not a directory")
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
		return "", nil, err
	}
	return rootPath, root, nil
}

func validateRepositoryRelativePath(path string) error {
	if path == "" || strings.IndexByte(path, 0) >= 0 || strings.Contains(path, `\`) || filepath.IsAbs(path) || strings.HasPrefix(path, "/") {
		return fmt.Errorf("repository path is invalid: %q", path)
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	if clean != path || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("repository path is not canonical: %q", path)
	}
	for _, component := range strings.Split(path, "/") {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("repository path contains a dot component: %q", path)
		}
	}
	return nil
}

func readRootedFile(ctx context.Context, root *os.Root, path string) ([]byte, error) {
	if err := contextError(ctx); err != nil {
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
		return nil, err
	}
	defer file.Close()
	if err := verifyOpenedFileIdentity(ctx, root, path, file); err != nil {
		return nil, err
	}
	data, err := readAllContext(ctx, file)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	second, err := readAllContext(ctx, file)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(data, second) {
		return nil, fmt.Errorf("repository file changed across reads: %q", path)
	}
	if err := verifyOpenedFileIdentity(ctx, root, path, file); err != nil {
		return nil, err
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("repository file contains invalid UTF-8: %q", path)
	}
	return data, nil
}

func verifyOpenedFileIdentity(ctx context.Context, root *os.Root, path string, file *os.File) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := rejectSymlinkComponents(ctx, root, path); err != nil {
		return err
	}
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	current, err := root.Lstat(filepath.FromSlash(path))
	if err != nil {
		return err
	}
	if current.Mode()&os.ModeSymlink != 0 || !opened.Mode().IsRegular() || !current.Mode().IsRegular() || !os.SameFile(opened, current) {
		return fmt.Errorf("repository file identity is invalid: %q", path)
	}
	return nil
}

func rejectSymlinkComponents(ctx context.Context, root *os.Root, path string) error {
	var current string
	for _, component := range strings.Split(path, "/") {
		if err := contextError(ctx); err != nil {
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

func readAllContext(ctx context.Context, reader io.Reader) ([]byte, error) {
	var output bytes.Buffer
	buffer := make([]byte, 32*1024)
	for {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		remaining := maxRepositoryJSONFileSize - output.Len()
		readBuffer := buffer
		if len(readBuffer) > remaining+1 {
			readBuffer = readBuffer[:remaining+1]
		}
		count, err := reader.Read(readBuffer)
		if count > 0 {
			if count > remaining {
				return nil, fmt.Errorf("repository JSON file exceeds %d bytes", maxRepositoryJSONFileSize)
			}
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

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	return ctx.Err()
}

func isLowerSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := decodeLowerHex(value)
	return err == nil
}

func decodeLowerHex(value string) ([]byte, error) {
	if len(value)%2 != 0 {
		return nil, errors.New("hexadecimal value has odd length")
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return nil, errors.New("value is not lowercase hexadecimal")
		}
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}
