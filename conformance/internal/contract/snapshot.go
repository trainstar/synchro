package contract

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

var behavioralPaths = []string{
	"docs/src/content/docs/spec/00-principles.mdx",
	"docs/src/content/docs/spec/01-wire-protocol.mdx",
	"docs/src/content/docs/spec/02-client-contract.mdx",
	"docs/src/content/docs/spec/03-state-machines.mdx",
	"docs/src/content/docs/spec/04-invariants.mdx",
	"docs/src/content/docs/spec/05-schema-evolution.mdx",
	"docs/src/content/docs/architecture/decisions/001-wal-change-stream.mdx",
	"docs/src/content/docs/architecture/decisions/002-mutation-idempotency-and-conflicts.mdx",
	"docs/src/content/docs/architecture/decisions/003-pull-cursor-and-rebuild.mdx",
	"docs/src/content/docs/architecture/decisions/004-membership-schema-and-retention.mdx",
	"docs/src/content/docs/architecture/decisions/005-integrity-authorization-and-seeds.mdx",
}

var schemaPaths = []string{
	"conformance/schemas/requirements-v2.schema.json",
	"conformance/schemas/support-matrix.schema.json",
	"conformance/schemas/scenario-v2.schema.json",
	"conformance/schemas/ci-summary-v1.schema.json",
	"conformance/schemas/rc-candidate-lock-v1.schema.json",
	"conformance/schemas/rc-manifest-v2.schema.json",
	"conformance/schemas/fault-catalog-v1.schema.json",
	"conformance/schemas/artifact-inventory-v1.schema.json",
	"conformance/schemas/performance-budgets-v2.schema.json",
	"conformance/schemas/vector-catalog-v1.schema.json",
}

// BuildSnapshot validates the authored contract and hashes the exact 28
// release-contract bindings.
func BuildSnapshot(ctx context.Context, repoRoot string) (Snapshot, error) {
	if err := checkContext(ctx); err != nil {
		return Snapshot{}, err
	}
	rootPath, root, err := openRepositoryRoot(repoRoot)
	if err != nil {
		return Snapshot{}, err
	}
	defer root.Close()
	captured, err := captureSnapshotFiles(ctx, root)
	if err != nil {
		return Snapshot{}, err
	}
	sources, err := snapshotBundleSources(captured)
	if err != nil {
		return Snapshot{}, err
	}
	if _, err := loadCapturedBundle(ctx, rootPath, sources); err != nil {
		return Snapshot{}, fmt.Errorf("validate captured authored contract before snapshot: %w", err)
	}
	if err := verifySnapshotSourceClosure(ctx, root, captured["conformance/catalog.json"], captured["conformance/vectors/catalog.json"]); err != nil {
		return Snapshot{}, err
	}

	requirements, err := bindCapturedFile(captured, "conformance/requirements.json")
	if err != nil {
		return Snapshot{}, err
	}
	support, err := bindCapturedFile(captured, "conformance/support-matrix.json")
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{
		ReleaseVersion:  releaseVersion,
		ProtocolVersion: protocolVersion,
		Requirements:    requirements,
		SupportMatrix:   support,
		BehavioralFiles: make([]BehavioralBinding, 0, len(behavioralPaths)),
	}
	for index, path := range behavioralPaths {
		if err := checkContext(ctx); err != nil {
			return Snapshot{}, err
		}
		binding, err := bindCapturedFile(captured, path)
		if err != nil {
			return Snapshot{}, err
		}
		behavioral := BehavioralBinding{Path: binding.Path, SHA256: binding.SHA256}
		if index >= 6 {
			status, err := acceptedADRStatus(captured[path])
			if err != nil {
				return Snapshot{}, fmt.Errorf("validate ADR %q status: %w", path, err)
			}
			behavioral.Status = &status
		}
		snapshot.BehavioralFiles = append(snapshot.BehavioralFiles, behavioral)
	}

	if snapshot.VerificationInputs.ScenarioCatalog, err = bindCapturedFile(captured, "conformance/catalog.json"); err != nil {
		return Snapshot{}, err
	}
	if snapshot.VerificationInputs.VectorCatalog, err = bindCapturedFile(captured, "conformance/vectors/catalog.json"); err != nil {
		return Snapshot{}, err
	}
	if snapshot.VerificationInputs.FaultCatalog, err = bindCapturedFile(captured, "conformance/faults/catalog.json"); err != nil {
		return Snapshot{}, err
	}
	if snapshot.VerificationInputs.PerformanceBudgets, err = bindCapturedFile(captured, "conformance/performance/budgets.json"); err != nil {
		return Snapshot{}, err
	}
	if snapshot.VerificationInputs.ArtifactInventory, err = bindCapturedFile(captured, "conformance/artifacts/inventory.json"); err != nil {
		return Snapshot{}, err
	}

	bindings := make([]FileBinding, len(schemaPaths))
	for index, path := range schemaPaths {
		if bindings[index], err = bindCapturedFile(captured, path); err != nil {
			return Snapshot{}, err
		}
	}
	snapshot.SchemaFiles = SchemaFiles{
		Requirements:       bindings[0],
		SupportMatrix:      bindings[1],
		Scenario:           bindings[2],
		CISummary:          bindings[3],
		RCCandidateLock:    bindings[4],
		RCManifest:         bindings[5],
		FaultCatalog:       bindings[6],
		ArtifactInventory:  bindings[7],
		PerformanceBudgets: bindings[8],
		VectorCatalog:      bindings[9],
	}
	if err := snapshot.Validate(); err != nil {
		return Snapshot{}, err
	}
	if err := verifyCapturedSnapshotFiles(ctx, root, captured); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func bindCapturedFile(captured map[string][]byte, path string) (FileBinding, error) {
	data, exists := captured[path]
	if !exists {
		return FileBinding{}, fmt.Errorf("captured snapshot binding %q is missing", path)
	}
	digest := sha256.Sum256(data)
	return FileBinding{Path: path, SHA256: hex.EncodeToString(digest[:])}, nil
}

func captureSnapshotFiles(ctx context.Context, root *os.Root) (map[string][]byte, error) {
	paths := snapshotFilePaths()
	if len(paths) != 28 {
		return nil, fmt.Errorf("snapshot binding path count is %d, want 28", len(paths))
	}
	captured := make(map[string][]byte, len(paths))
	for _, path := range paths {
		if _, exists := captured[path]; exists {
			return nil, fmt.Errorf("snapshot binding path %q is duplicated", path)
		}
		data, err := readRepositoryFile(ctx, root, path)
		if err != nil {
			return nil, fmt.Errorf("capture snapshot binding %q: %w", path, err)
		}
		captured[path] = data
	}
	return captured, nil
}

func snapshotBundleSources(captured map[string][]byte) (bundleSources, error) {
	sources := bundleSources{
		catalogs:   make(map[string][]byte, len(authoredCatalogFiles)),
		schemas:    make(map[string][]byte, len(authoredCatalogFiles)),
		behavioral: make(map[string][]byte, len(behavioralPaths)),
	}
	for _, item := range authoredCatalogFiles {
		catalog, exists := captured[item.path]
		if !exists {
			return bundleSources{}, fmt.Errorf("captured snapshot catalog %q is missing", item.path)
		}
		schema, exists := captured[item.schemaPath]
		if !exists {
			return bundleSources{}, fmt.Errorf("captured snapshot schema %q is missing", item.schemaPath)
		}
		sources.catalogs[item.path] = append([]byte(nil), catalog...)
		sources.schemas[item.schemaPath] = append([]byte(nil), schema...)
	}
	for _, path := range behavioralPaths {
		data, exists := captured[path]
		if !exists {
			return bundleSources{}, fmt.Errorf("captured snapshot behavioral file %q is missing", path)
		}
		sources.behavioral[path] = append([]byte(nil), data...)
	}
	return sources, nil
}

func snapshotFilePaths() []string {
	paths := []string{
		"conformance/requirements.json",
		"conformance/support-matrix.json",
	}
	paths = append(paths, behavioralPaths...)
	paths = append(paths,
		"conformance/catalog.json",
		"conformance/vectors/catalog.json",
		"conformance/faults/catalog.json",
		"conformance/performance/budgets.json",
		"conformance/artifacts/inventory.json",
	)
	paths = append(paths, schemaPaths...)
	return paths
}

func verifyCapturedSnapshotFiles(ctx context.Context, root *os.Root, captured map[string][]byte) error {
	for _, path := range snapshotFilePaths() {
		current, err := readRepositoryFile(ctx, root, path)
		if err != nil {
			return fmt.Errorf("recheck snapshot binding %q: %w", path, err)
		}
		capturedDigest := sha256.Sum256(captured[path])
		currentDigest := sha256.Sum256(current)
		if capturedDigest != currentDigest {
			return fmt.Errorf("snapshot binding %q changed during construction", path)
		}
	}
	return nil
}

// Validate rejects a Snapshot that differs from the frozen candidate contract
// binding surface.
func (s Snapshot) Validate() error {
	var failures []error
	if s.ReleaseVersion != releaseVersion {
		failures = append(failures, fmt.Errorf("snapshot release_version must be %s", releaseVersion))
	}
	if s.ProtocolVersion != protocolVersion {
		failures = append(failures, fmt.Errorf("snapshot protocol_version must be %d", protocolVersion))
	}
	failures = append(failures, validateBinding(s.Requirements, "conformance/requirements.json", "requirements")...)
	failures = append(failures, validateBinding(s.SupportMatrix, "conformance/support-matrix.json", "support_matrix")...)
	if len(s.BehavioralFiles) != len(behavioralPaths) {
		failures = append(failures, fmt.Errorf("snapshot behavioral_files must contain exactly %d files", len(behavioralPaths)))
	}
	for index, binding := range s.BehavioralFiles {
		if index >= len(behavioralPaths) {
			failures = append(failures, fmt.Errorf("snapshot behavioral_files has unexpected extra binding %q", binding.Path))
			continue
		}
		failures = append(failures, validateBehavioralBinding(binding, behavioralPaths[index], index)...)
	}
	for _, expected := range []struct {
		binding FileBinding
		path    string
		name    string
	}{
		{s.VerificationInputs.ScenarioCatalog, "conformance/catalog.json", "verification_inputs.scenario_catalog"},
		{s.VerificationInputs.VectorCatalog, "conformance/vectors/catalog.json", "verification_inputs.vector_catalog"},
		{s.VerificationInputs.FaultCatalog, "conformance/faults/catalog.json", "verification_inputs.fault_catalog"},
		{s.VerificationInputs.PerformanceBudgets, "conformance/performance/budgets.json", "verification_inputs.performance_budgets"},
		{s.VerificationInputs.ArtifactInventory, "conformance/artifacts/inventory.json", "verification_inputs.artifact_inventory"},
		{s.SchemaFiles.Requirements, schemaPaths[0], "schema_files.requirements"},
		{s.SchemaFiles.SupportMatrix, schemaPaths[1], "schema_files.support_matrix"},
		{s.SchemaFiles.Scenario, schemaPaths[2], "schema_files.scenario"},
		{s.SchemaFiles.CISummary, schemaPaths[3], "schema_files.ci_summary"},
		{s.SchemaFiles.RCCandidateLock, schemaPaths[4], "schema_files.rc_candidate_lock"},
		{s.SchemaFiles.RCManifest, schemaPaths[5], "schema_files.rc_manifest"},
		{s.SchemaFiles.FaultCatalog, schemaPaths[6], "schema_files.fault_catalog"},
		{s.SchemaFiles.ArtifactInventory, schemaPaths[7], "schema_files.artifact_inventory"},
		{s.SchemaFiles.PerformanceBudgets, schemaPaths[8], "schema_files.performance_budgets"},
		{s.SchemaFiles.VectorCatalog, schemaPaths[9], "schema_files.vector_catalog"},
	} {
		failures = append(failures, validateBinding(expected.binding, expected.path, expected.name)...)
	}
	return joinSemanticErrors(failures)
}

func validateBinding(binding FileBinding, expectedPath, name string) []error {
	var failures []error
	if binding.Path != expectedPath {
		failures = append(failures, fmt.Errorf("snapshot %s path must be %q, found %q", name, expectedPath, binding.Path))
	}
	if !isLowerSHA256(binding.SHA256) {
		failures = append(failures, fmt.Errorf("snapshot %s SHA-256 is not lowercase hexadecimal", name))
	}
	return failures
}

func validateBehavioralBinding(binding BehavioralBinding, expectedPath string, index int) []error {
	failures := validateBinding(FileBinding{Path: binding.Path, SHA256: binding.SHA256}, expectedPath, "behavioral_files")
	if index < 6 {
		if binding.Status != nil {
			failures = append(failures, fmt.Errorf("specification binding %q must have null status", expectedPath))
		}
	} else if binding.Status == nil || *binding.Status != "Accepted" {
		failures = append(failures, fmt.Errorf("ADR binding %q must have Accepted status", expectedPath))
	}
	return failures
}

func isLowerSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

// CanonicalBytes validates the frozen Snapshot shape, marshals it, and applies
// RFC 8785 JSON Canonicalization Scheme encoding.
func (s Snapshot) CanonicalBytes() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	canonical, err := jcs.Transform(encoded)
	if err != nil {
		return nil, fmt.Errorf("canonicalize snapshot as RFC 8785 JSON: %w", err)
	}
	return canonical, nil
}

// SHA256 returns the SHA-256 of CanonicalBytes.
func (s Snapshot) SHA256() ([32]byte, error) {
	canonical, err := s.CanonicalBytes()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(canonical), nil
}

func acceptedADRStatus(contents []byte) (string, error) {
	text := strings.ReplaceAll(string(contents), "\r\n", "\n")
	lines := strings.Split(text, "\n")
	if len(lines) == 0 || lines[0] != "---" {
		return "", fmt.Errorf("initial YAML frontmatter is missing")
	}
	mapping := make(map[string]string)
	closed := false
	for index := 1; index < len(lines); index++ {
		line := lines[index]
		if line == "---" || line == "..." {
			closed = true
			break
		}
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if len(line) != len(strings.TrimLeft(line, " \t")) {
			return "", fmt.Errorf("frontmatter has a nested or indented mapping")
		}
		key, value, err := parseFrontmatterMapping(line)
		if err != nil {
			return "", err
		}
		if _, exists := mapping[key]; exists {
			return "", fmt.Errorf("frontmatter has duplicate semantic key %q", key)
		}
		mapping[key] = value
	}
	if !closed {
		return "", fmt.Errorf("initial YAML frontmatter is not closed")
	}
	status, exists := mapping["status"]
	if !exists {
		return "", fmt.Errorf("initial YAML frontmatter must contain exactly one status scalar")
	}
	if status != "Accepted" {
		return "", fmt.Errorf("ADR frontmatter status must be exactly Accepted")
	}
	return status, nil
}

func parseFrontmatterMapping(line string) (string, string, error) {
	if strings.HasPrefix(line, "-") || strings.HasPrefix(line, "?") || strings.HasPrefix(line, "&") || strings.HasPrefix(line, "*") || strings.HasPrefix(line, "<<") {
		return "", "", fmt.Errorf("frontmatter uses an unsupported collection, complex key, alias, or merge")
	}
	separator, err := frontmatterColon(line)
	if err != nil {
		return "", "", err
	}
	if separator < 0 {
		return "", "", fmt.Errorf("frontmatter mapping entry has no key separator")
	}
	key, err := parseFrontmatterKey(line[:separator])
	if err != nil {
		return "", "", err
	}
	value, err := parseFrontmatterScalar(line[separator+1:])
	if err != nil {
		return "", "", err
	}
	return key, value, nil
}

func frontmatterColon(line string) (int, error) {
	quote := byte(0)
	escaped := false
	for index := 0; index < len(line); index++ {
		character := line[index]
		if quote != 0 {
			if quote == '"' && escaped {
				escaped = false
				continue
			}
			if quote == '"' && character == '\\' {
				escaped = true
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		if character == '"' || character == '\'' {
			quote = character
			continue
		}
		if character == ':' {
			return index, nil
		}
	}
	if quote != 0 {
		return -1, fmt.Errorf("frontmatter has an unterminated quoted key")
	}
	return -1, nil
}

func parseFrontmatterKey(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.ContainsAny(raw, "[]{}|>&*!#") {
		return "", fmt.Errorf("frontmatter has an unsupported mapping key")
	}
	if raw[0] == '"' || raw[0] == '\'' {
		value, trailing, err := parseFrontmatterQuoted(raw)
		if err != nil || strings.TrimSpace(trailing) != "" {
			return "", fmt.Errorf("frontmatter has an invalid quoted mapping key")
		}
		return value, nil
	}
	if strings.ContainsAny(raw, " \t") {
		return "", fmt.Errorf("frontmatter has an unsupported mapping key")
	}
	return raw, nil
}

func parseFrontmatterScalar(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.HasPrefix(raw, "-") || strings.HasPrefix(raw, "?") || strings.HasPrefix(raw, "[") || strings.HasPrefix(raw, "{") || strings.HasPrefix(raw, "|") || strings.HasPrefix(raw, ">") || strings.HasPrefix(raw, "&") || strings.HasPrefix(raw, "*") || strings.HasPrefix(raw, "!") {
		return "", fmt.Errorf("frontmatter value is not a simple scalar")
	}
	if raw[0] == '"' || raw[0] == '\'' {
		value, trailing, err := parseFrontmatterQuoted(raw)
		if err != nil {
			return "", err
		}
		trailing = strings.TrimSpace(trailing)
		if trailing != "" && !strings.HasPrefix(trailing, "#") {
			return "", fmt.Errorf("frontmatter value is not a simple scalar")
		}
		return value, nil
	}
	if comment := strings.Index(raw, " #"); comment >= 0 {
		raw = strings.TrimSpace(raw[:comment])
	}
	if raw == "" || strings.Contains(raw, ": ") || strings.ContainsAny(raw, "[]{}|>&*!\t\n\r") {
		return "", fmt.Errorf("frontmatter value is not a simple scalar")
	}
	return raw, nil
}

func parseFrontmatterQuoted(raw string) (string, string, error) {
	if raw == "" {
		return "", "", fmt.Errorf("frontmatter quoted scalar is empty")
	}
	quote := raw[0]
	if quote == '"' {
		return parseYAMLDoubleQuoted(raw)
	}
	var output strings.Builder
	for index := 1; index < len(raw); index++ {
		character := raw[index]
		if quote == '"' && character == '\\' {
			if index+1 >= len(raw) {
				return "", "", fmt.Errorf("frontmatter quoted scalar has an invalid escape")
			}
			index++
			output.WriteByte(raw[index])
			continue
		}
		if character == quote {
			if quote == '\'' && index+1 < len(raw) && raw[index+1] == '\'' {
				output.WriteByte('\'')
				index++
				continue
			}
			return output.String(), raw[index+1:], nil
		}
		output.WriteByte(character)
	}
	return "", "", fmt.Errorf("frontmatter quoted scalar is not terminated")
}

func parseYAMLDoubleQuoted(raw string) (string, string, error) {
	if !utf8.ValidString(raw) {
		return "", "", fmt.Errorf("frontmatter quoted scalar has invalid UTF-8")
	}
	var output strings.Builder
	for index := 1; index < len(raw); {
		character := raw[index]
		switch character {
		case '"':
			return output.String(), raw[index+1:], nil
		case '\\':
			decoded, next, err := decodeYAMLDoubleQuotedEscape(raw, index+1)
			if err != nil {
				return "", "", err
			}
			output.WriteRune(decoded)
			index = next
		default:
			runeValue, width := utf8.DecodeRuneInString(raw[index:])
			if runeValue == utf8.RuneError && width == 1 {
				return "", "", fmt.Errorf("frontmatter quoted scalar has invalid UTF-8")
			}
			if runeValue < 0x20 {
				return "", "", fmt.Errorf("frontmatter quoted scalar contains an unescaped control character")
			}
			output.WriteRune(runeValue)
			index += width
		}
	}
	return "", "", fmt.Errorf("frontmatter quoted scalar is not terminated")
}

func decodeYAMLDoubleQuotedEscape(raw string, index int) (rune, int, error) {
	if index >= len(raw) {
		return 0, 0, fmt.Errorf("frontmatter quoted scalar has an invalid escape")
	}
	switch raw[index] {
	case '0':
		return 0, index + 1, nil
	case 'a':
		return '\a', index + 1, nil
	case 'b':
		return '\b', index + 1, nil
	case 't', '\t':
		return '\t', index + 1, nil
	case 'n':
		return '\n', index + 1, nil
	case 'v':
		return '\v', index + 1, nil
	case 'f':
		return '\f', index + 1, nil
	case 'r':
		return '\r', index + 1, nil
	case 'e':
		return 0x1b, index + 1, nil
	case ' ':
		return ' ', index + 1, nil
	case '"':
		return '"', index + 1, nil
	case '/':
		return '/', index + 1, nil
	case '\\':
		return '\\', index + 1, nil
	case 'N':
		return 0x85, index + 1, nil
	case '_':
		return 0xa0, index + 1, nil
	case 'L':
		return 0x2028, index + 1, nil
	case 'P':
		return 0x2029, index + 1, nil
	case 'x':
		return decodeYAMLEscapeHex(raw, index+1, 2)
	case 'u':
		return decodeYAMLEscapeHex(raw, index+1, 4)
	case 'U':
		return decodeYAMLEscapeHex(raw, index+1, 8)
	default:
		return 0, 0, fmt.Errorf("frontmatter quoted scalar has an invalid escape \\%c", raw[index])
	}
}

func decodeYAMLEscapeHex(raw string, start, width int) (rune, int, error) {
	if start+width > len(raw) {
		return 0, 0, fmt.Errorf("frontmatter quoted scalar has a truncated hexadecimal escape")
	}
	value := rune(0)
	for _, character := range raw[start : start+width] {
		value <<= 4
		switch {
		case character >= '0' && character <= '9':
			value += character - '0'
		case character >= 'a' && character <= 'f':
			value += character - 'a' + 10
		case character >= 'A' && character <= 'F':
			value += character - 'A' + 10
		default:
			return 0, 0, fmt.Errorf("frontmatter quoted scalar has an invalid hexadecimal escape")
		}
	}
	if !utf8.ValidRune(value) || (value >= 0xd800 && value <= 0xdfff) {
		return 0, 0, fmt.Errorf("frontmatter quoted scalar has an invalid Unicode escape")
	}
	return value, start + width, nil
}
