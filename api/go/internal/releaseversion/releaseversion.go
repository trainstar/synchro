package releaseversion

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

var semverRE = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

const (
	postgresSQLDir     = "extensions/synchro-pg/sql"
	updateBaselinePath = "extensions/synchro-pg/update-baseline.json"
)

var (
	synchroPodspecVersionRE         = regexp.MustCompile(`(?m)^  s\.version = ".*"$`)
	synchroPodspecSourceTagRE       = regexp.MustCompile(`(?m)^  s\.source = \{ :git => "https://github\.com/trainstar/synchro\.git", :tag => ".*" \}$`)
	reactNativePackageVersionRE     = regexp.MustCompile(`(?m)^  "version": ".*",$`)
	reactNativePodspecTagRE         = regexp.MustCompile(`(?m)^  s\.source\s+= \{ :git => "https://github\.com/trainstar/synchro\.git", :tag => ".*" \}$`)
	reactNativePodspecDependencyRE  = regexp.MustCompile(`(?m)^  s\.dependency "Synchro", ".*"$`)
	reactNativeAndroidVersionRE     = regexp.MustCompile(`(?m)^def defaultSynchroVersion = ".*"$`)
	reactNativeAndroidDependencyRE  = regexp.MustCompile(`(?m)^  implementation "fit\.trainstar:synchro:.*"$`)
	kotlinGradlePropertiesVersionRE = regexp.MustCompile(`(?m)^version=.*$`)
	kotlinCoordinatesRE             = regexp.MustCompile(`(?m)^    coordinates\("fit\.trainstar", "synchro", .*\)$`)
	cargoWorkspaceVersionRE         = regexp.MustCompile(`(?ms)(\[workspace\.package\]\s+version = ")([^"]+)(")`)
	controlVersionRE                = regexp.MustCompile(`(?m)^default_version = '.*'$`)
	distributionReleaseRE           = regexp.MustCompile(`(?m)^  "release": ".*",$`)
	schemaReleaseConstRE            = regexp.MustCompile(`(?m)^    "release": \{ "const": ".*" \},$`)
	conformanceReleaseRE            = regexp.MustCompile(`(?m)^const Version = ".*"$`)
	cargoLockCoreVersionRE          = regexp.MustCompile(`(?m)^name = "synchro-core"\nversion = ".*"$`)
	cargoLockPGVersionRE            = regexp.MustCompile(`(?m)^name = "synchro-pg"\nversion = ".*"$`)
	goConsumerRequireRE             = regexp.MustCompile(`(?m)^require github\.com/trainstar/synchro/api/go v.*$`)
	baseSQLFileRE                   = regexp.MustCompile(`^synchro_pg--(\d+\.\d+\.\d+)\.sql$`)
	updateSQLFileRE                 = regexp.MustCompile(`^synchro_pg--(\d+\.\d+\.\d+)--(\d+\.\d+\.\d+)\.sql$`)
)

// Token patterns find release references inside content that the version tool
// does not otherwise own. Group 1 holds the release version.
var (
	publishedReferenceRE   = regexp.MustCompile("(?:Synchro\\s+`v?|Git tag `v|@trainstar/synchro-react-native@|trainstar-synchro-react-native-|fit\\.trainstar:synchro:|trainstar/synchro\\.git', :tag => 'v|trainstar/synchro\\.git\",\\s+exact: \")(\\d+\\.\\d+\\.\\d+)")
	installSQLReferenceRE  = regexp.MustCompile(`synchro_pg--(\d+\.\d+\.\d+)\.sql`)
	extensionVersionRE     = regexp.MustCompile(`"extension_version":\s*"(\d+\.\d+\.\d+)"`)
	podfileLockReferenceRE = regexp.MustCompile(`(?m)^(?:  - Synchro \(|  - SynchroReactNative \(|    - Synchro \(= )(\d+\.\d+\.\d+)\)`)
)

type fileExpectation struct {
	path     string
	pattern  *regexp.Regexp
	expected string
}

// tokenExpectation requires each pattern match to name the release. Each listed
// path must contain a match. A directory contributes every file with suffix
// that contains a match, and it must contribute at least one file.
type tokenExpectation struct {
	paths   []string
	dir     string
	suffix  string
	pattern *regexp.Regexp
}

func FindRepoRoot(start string) (string, error) {
	current, err := filepath.Abs(start)
	if err != nil {
		return "", fmt.Errorf("resolving start path: %w", err)
	}

	for {
		gitPath := filepath.Join(current, ".git")
		if info, err := os.Stat(gitPath); err == nil {
			if info.IsDir() {
				return current, nil
			}
			if info.Mode().IsRegular() {
				data, err := os.ReadFile(gitPath)
				if err != nil {
					return "", fmt.Errorf("reading Git worktree marker: %w", err)
				}
				if directory, ok := strings.CutPrefix(strings.TrimSpace(string(data)), "gitdir: "); ok && directory != "" {
					return current, nil
				}
			}
		}

		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("could not locate repo root")
		}
		current = parent
	}
}

func Validate(version string) error {
	if !semverRE.MatchString(version) {
		return fmt.Errorf("version %q must match X.Y.Z", version)
	}
	return nil
}

func ReadVersion(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, "VERSION"))
	if err != nil {
		return "", fmt.Errorf("reading VERSION: %w", err)
	}

	version := strings.TrimSpace(string(data))
	if err := Validate(version); err != nil {
		return "", err
	}
	return version, nil
}

func Set(root string, version string) error {
	if err := Validate(version); err != nil {
		return err
	}
	if _, err := postgresInstallSQLRename(root, version); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "VERSION"), []byte(version+"\n"), 0o644); err != nil {
		return fmt.Errorf("writing VERSION: %w", err)
	}
	return Sync(root)
}

func distributionExpectations(root, version string) []fileExpectation {
	return []fileExpectation{
		{
			path:     filepath.Join(root, "Synchro.podspec"),
			pattern:  synchroPodspecVersionRE,
			expected: fmt.Sprintf(`  s.version = "%s"`, version),
		},
		{
			path:     filepath.Join(root, "Synchro.podspec"),
			pattern:  synchroPodspecSourceTagRE,
			expected: `  s.source = { :git => "https://github.com/trainstar/synchro.git", :tag => "v#{s.version}" }`,
		},
		{
			path:     filepath.Join(root, "clients/react-native/package.json"),
			pattern:  reactNativePackageVersionRE,
			expected: fmt.Sprintf(`  "version": "%s",`, version),
		},
		{
			path:     filepath.Join(root, "clients/react-native/SynchroReactNative.podspec"),
			pattern:  reactNativePodspecTagRE,
			expected: `  s.source       = { :git => "https://github.com/trainstar/synchro.git", :tag => "v#{s.version}" }`,
		},
		{
			path:     filepath.Join(root, "clients/react-native/SynchroReactNative.podspec"),
			pattern:  reactNativePodspecDependencyRE,
			expected: `  s.dependency "Synchro", "= #{s.version}"`,
		},
		{
			path:     filepath.Join(root, "clients/react-native/android/build.gradle"),
			pattern:  reactNativeAndroidVersionRE,
			expected: fmt.Sprintf(`def defaultSynchroVersion = "%s"`, version),
		},
		{
			path:     filepath.Join(root, "clients/react-native/android/build.gradle"),
			pattern:  reactNativeAndroidDependencyRE,
			expected: `  implementation "fit.trainstar:synchro:${resolvedSynchroVersion}"`,
		},
		{
			path:     filepath.Join(root, "clients/kotlin/gradle.properties"),
			pattern:  kotlinGradlePropertiesVersionRE,
			expected: fmt.Sprintf(`version=%s`, version),
		},
		{
			path:     filepath.Join(root, "clients/kotlin/synchro/build.gradle.kts"),
			pattern:  kotlinCoordinatesRE,
			expected: `    coordinates("fit.trainstar", "synchro", project.version.toString())`,
		},
		{
			path:     filepath.Join(root, "extensions/Cargo.toml"),
			pattern:  cargoWorkspaceVersionRE,
			expected: version,
		},
		{
			path:     filepath.Join(root, "extensions/synchro-pg/synchro_pg.control"),
			pattern:  controlVersionRE,
			expected: fmt.Sprintf(`default_version = '%s'`, version),
		},
		{
			path:     filepath.Join(root, "conformance/artifacts/inventory.json"),
			pattern:  distributionReleaseRE,
			expected: fmt.Sprintf(`  "release": "%s",`, version),
		},
		{
			path:     filepath.Join(root, "conformance/requirements.json"),
			pattern:  distributionReleaseRE,
			expected: fmt.Sprintf(`  "release": "%s",`, version),
		},
		{
			path:     filepath.Join(root, "conformance/support-matrix.json"),
			pattern:  distributionReleaseRE,
			expected: fmt.Sprintf(`  "release": "%s",`, version),
		},
		{
			path:     filepath.Join(root, "conformance/faults/catalog.json"),
			pattern:  distributionReleaseRE,
			expected: fmt.Sprintf(`  "release": "%s",`, version),
		},
		{
			path:     filepath.Join(root, "conformance/performance/budgets.json"),
			pattern:  distributionReleaseRE,
			expected: fmt.Sprintf(`  "release": "%s",`, version),
		},
		{
			path:     filepath.Join(root, "conformance/vectors/catalog.json"),
			pattern:  distributionReleaseRE,
			expected: fmt.Sprintf(`  "release": "%s",`, version),
		},
		{
			path:     filepath.Join(root, "conformance/schemas/requirements-v2.schema.json"),
			pattern:  schemaReleaseConstRE,
			expected: fmt.Sprintf(`    "release": { "const": "%s" },`, version),
		},
		{
			path:     filepath.Join(root, "conformance/schemas/fault-catalog-v1.schema.json"),
			pattern:  schemaReleaseConstRE,
			expected: fmt.Sprintf(`    "release": { "const": "%s" },`, version),
		},
		{
			path:     filepath.Join(root, "conformance/schemas/performance-budgets-v2.schema.json"),
			pattern:  schemaReleaseConstRE,
			expected: fmt.Sprintf(`    "release": { "const": "%s" },`, version),
		},
		{
			path:     filepath.Join(root, "conformance/schemas/vector-catalog-v1.schema.json"),
			pattern:  schemaReleaseConstRE,
			expected: fmt.Sprintf(`    "release": { "const": "%s" },`, version),
		},
		{
			path:     filepath.Join(root, "conformance/internal/release/release.go"),
			pattern:  conformanceReleaseRE,
			expected: fmt.Sprintf(`const Version = "%s"`, version),
		},
		{
			path:     filepath.Join(root, "extensions/Cargo.lock"),
			pattern:  cargoLockCoreVersionRE,
			expected: fmt.Sprintf("name = \"synchro-core\"\nversion = \"%s\"", version),
		},
		{
			path:     filepath.Join(root, "extensions/Cargo.lock"),
			pattern:  cargoLockPGVersionRE,
			expected: fmt.Sprintf("name = \"synchro-pg\"\nversion = \"%s\"", version),
		},
		{
			path:     filepath.Join(root, "verification/consumers/go/go.mod"),
			pattern:  goConsumerRequireRE,
			expected: fmt.Sprintf(`require github.com/trainstar/synchro/api/go v%s`, version),
		},
	}
}

func tokenExpectations(root string) []tokenExpectation {
	return []tokenExpectation{
		{
			paths: []string{
				filepath.Join(root, "README.md"),
				filepath.Join(root, "clients/react-native/README.md"),
				filepath.Join(root, "docs/src/content/docs/clients/consumption.mdx"),
				filepath.Join(root, "docs/src/content/docs/getting-started/quickstart.mdx"),
				filepath.Join(root, "docs/src/content/docs/getting-started/server-setup.mdx"),
				filepath.Join(root, "docs/src/content/docs/index.mdx"),
			},
			pattern: publishedReferenceRE,
		},
		{
			paths:   []string{filepath.Join(root, "clients/react-native/example/ios/Podfile.lock")},
			pattern: podfileLockReferenceRE,
		},
		{
			dir:     filepath.Join(root, "conformance/mutants/integration"),
			suffix:  ".patch",
			pattern: installSQLReferenceRE,
		},
		{
			dir:     filepath.Join(root, "conformance/scenarios"),
			suffix:  ".json",
			pattern: extensionVersionRE,
		},
	}
}

func Sync(root string) error {
	version, err := ReadVersion(root)
	if err != nil {
		return err
	}
	baseScript, err := postgresInstallSQLRename(root, version)
	if err != nil {
		return err
	}
	for _, replacement := range distributionExpectations(root, version) {
		if err := rewriteFile(replacement.path, replacement.pattern, replacement.expected); err != nil {
			return err
		}
	}
	for _, expectation := range tokenExpectations(root) {
		paths, err := tokenPaths(root, expectation)
		if err != nil {
			return err
		}
		for _, path := range paths {
			if err := rewriteTokens(path, expectation.pattern, version); err != nil {
				return err
			}
		}
	}

	if baseScript != "" {
		target := filepath.Join(root, postgresSQLDir, baseSQLName(version))
		if err := os.Rename(baseScript, target); err != nil {
			return fmt.Errorf("renaming PostgreSQL install SQL to %s: %w", filepath.Base(target), err)
		}
	}

	return nil
}

func Check(root string, expectedTag string) error {
	version, err := ReadVersion(root)
	if err != nil {
		return err
	}

	var failures []string

	for _, expectation := range distributionExpectations(root, version) {
		ok, actual, checkErr := matchesExpectation(expectation.path, expectation.pattern, expectation.expected)
		if checkErr != nil {
			failures = append(failures, checkErr.Error())
			continue
		}
		if !ok {
			failures = append(failures, fmt.Sprintf("%s does not match expected value %q, found %q", relativePath(root, expectation.path), expectation.expected, actual))
		}
	}

	for _, expectation := range tokenExpectations(root) {
		paths, err := tokenPaths(root, expectation)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		for _, path := range paths {
			if err := checkTokens(root, path, expectation.pattern, version); err != nil {
				failures = append(failures, err.Error())
			}
		}
	}

	failures = append(failures, checkPostgresInstallSQL(root, version)...)

	if expectedTag != "" && expectedTag != "v"+version {
		failures = append(failures, fmt.Sprintf("expected release tag %q to match v%s", expectedTag, version))
	}

	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "\n"))
	}

	return nil
}

func rewriteFile(path string, pattern *regexp.Regexp, replacement string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}

	if !pattern.Match(data) {
		return fmt.Errorf("could not find expected pattern in %s", path)
	}

	var updated string
	if pattern == cargoWorkspaceVersionRE {
		updated = pattern.ReplaceAllString(string(data), "${1}"+replacement+"${3}")
	} else {
		updated = pattern.ReplaceAllLiteralString(string(data), replacement)
	}
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}

	return nil
}

func matchesExpectation(path string, pattern *regexp.Regexp, expected string) (bool, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, "", fmt.Errorf("reading %s: %w", path, err)
	}

	match := pattern.FindStringSubmatch(string(data))
	if match == nil {
		return false, "", fmt.Errorf("missing expected pattern in %s", path)
	}

	actual := match[0]
	if pattern == cargoWorkspaceVersionRE {
		actual = match[2]
	}

	return actual == expected, actual, nil
}

func tokenPaths(root string, expectation tokenExpectation) ([]string, error) {
	if expectation.dir == "" {
		return expectation.paths, nil
	}
	var paths []string
	err := filepath.WalkDir(expectation.dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, expectation.suffix) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if expectation.pattern.Match(data) {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scanning %s: %w", relativePath(root, expectation.dir), err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("%s has no release-version reference", relativePath(root, expectation.dir))
	}
	return paths, nil
}

func rewriteTokens(path string, pattern *regexp.Regexp, version string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	matches := pattern.FindAllSubmatchIndex(data, -1)
	if len(matches) == 0 {
		return fmt.Errorf("could not find a release-version reference in %s", path)
	}
	var updated []byte
	last := 0
	for _, match := range matches {
		updated = append(updated, data[last:match[2]]...)
		updated = append(updated, version...)
		last = match[3]
	}
	updated = append(updated, data[last:]...)
	if err := os.WriteFile(path, updated, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

func checkTokens(root string, path string, pattern *regexp.Regexp, version string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading %s: %w", path, err)
	}
	matches := pattern.FindAllSubmatch(data, -1)
	if len(matches) == 0 {
		return fmt.Errorf("missing release-version reference in %s", relativePath(root, path))
	}
	for _, match := range matches {
		if string(match[1]) != version {
			return fmt.Errorf("%s references release %q, expected %q", relativePath(root, path), match[1], version)
		}
	}
	return nil
}

type updateScript struct {
	from string
	to   string
}

type postgresSQLScripts struct {
	baseVersions []string
	updates      []updateScript
	invalid      []string
}

// postgresInstallSQLRename checks that Sync can give the base script the
// version. It returns the base script that Sync renames, or an empty path when
// the base script already has the version.
func postgresInstallSQLRename(root string, version string) (string, error) {
	scripts, err := readPostgresSQLScripts(root)
	if err != nil {
		return "", err
	}
	if len(scripts.baseVersions) != 1 {
		return "", fmt.Errorf("%s must contain exactly one PostgreSQL install SQL file, found %d", postgresSQLDir, len(scripts.baseVersions))
	}
	current := scripts.baseVersions[0]
	if current == version {
		return "", nil
	}
	if compareVersions(version, current) < 0 {
		return "", fmt.Errorf("version %s is lower than the PostgreSQL install SQL version in %s", version, sqlPath(baseSQLName(current)))
	}
	if !slices.Contains(scripts.updates, updateScript{from: current, to: version}) {
		return "", fmt.Errorf("missing PostgreSQL update SQL %s", sqlPath(updateSQLName(current, version)))
	}
	return filepath.Join(root, postgresSQLDir, baseSQLName(current)), nil
}

// checkPostgresInstallSQL requires one base script for version and one chain
// of update scripts from the pinned update baseline to version.
func checkPostgresInstallSQL(root string, version string) []string {
	var failures []string
	baseline, baselineErr := readUpdateBaseline(root)
	if baselineErr != nil {
		failures = append(failures, baselineErr.Error())
	}
	scripts, err := readPostgresSQLScripts(root)
	if err != nil {
		return append(failures, err.Error())
	}
	failures = append(failures, scripts.invalid...)

	switch len(scripts.baseVersions) {
	case 0:
		failures = append(failures, fmt.Sprintf("missing PostgreSQL install SQL %s", sqlPath(baseSQLName(version))))
	case 1:
		if scripts.baseVersions[0] != version {
			failures = append(failures, fmt.Sprintf("PostgreSQL install SQL %s does not match VERSION, expected %s", sqlPath(baseSQLName(scripts.baseVersions[0])), baseSQLName(version)))
		}
	default:
		for _, base := range scripts.baseVersions {
			failures = append(failures, fmt.Sprintf("PostgreSQL install SQL %s is not the only install SQL file", sqlPath(baseSQLName(base))))
		}
	}

	if baselineErr != nil {
		return failures
	}
	return append(failures, checkUpdateChain(scripts.updates, baseline, version)...)
}

// checkUpdateChain requires the update scripts to form exactly one chain from
// baseline to version that uses every update script.
func checkUpdateChain(updates []updateScript, baseline string, version string) []string {
	var failures []string
	switch compareVersions(version, baseline) {
	case -1:
		return []string{fmt.Sprintf("VERSION %s is lower than the update baseline %s in %s", version, baseline, updateBaselinePath)}
	case 0:
		for _, update := range updates {
			failures = append(failures, fmt.Sprintf("PostgreSQL update SQL %s is not allowed when VERSION is the update baseline %s", sqlPath(update.name()), baseline))
		}
		return failures
	}

	reported := make(map[updateScript]bool)
	next := make(map[string]updateScript)
	for _, update := range updates {
		if compareVersions(update.from, update.to) >= 0 {
			failures = append(failures, fmt.Sprintf("PostgreSQL update SQL %s does not go from a lower version to a higher version", sqlPath(update.name())))
			reported[update] = true
			continue
		}
		if previous, exists := next[update.from]; exists {
			failures = append(failures, fmt.Sprintf("PostgreSQL update SQL %s starts at the same version as %s", sqlPath(update.name()), sqlPath(previous.name())))
			reported[update] = true
			continue
		}
		next[update.from] = update
	}

	used := make(map[updateScript]bool)
	for current := baseline; current != version; {
		update, exists := next[current]
		if !exists {
			missing := updateSQLName(current, missingUpdateTarget(next, current, version))
			failures = append(failures, fmt.Sprintf("missing PostgreSQL update SQL %s: the update chain from %s stops at %s", sqlPath(missing), baseline, current))
			break
		}
		used[update] = true
		if compareVersions(update.to, version) > 0 {
			failures = append(failures, fmt.Sprintf("PostgreSQL update SQL %s goes past VERSION %s", sqlPath(update.name()), version))
			break
		}
		current = update.to
	}
	for _, update := range updates {
		if !used[update] && !reported[update] {
			failures = append(failures, fmt.Sprintf("PostgreSQL update SQL %s is not in the update chain from %s to %s", sqlPath(update.name()), baseline, version))
		}
	}
	return failures
}

// missingUpdateTarget returns the target version of the update script that the
// chain needs at current: the next start version of an update script, or version.
func missingUpdateTarget(next map[string]updateScript, current string, version string) string {
	target := version
	for from := range next {
		if compareVersions(current, from) < 0 && compareVersions(from, target) < 0 {
			target = from
		}
	}
	return target
}

// readPostgresSQLScripts returns the base script versions and the update
// scripts in the PostgreSQL SQL directory. It reports each other entry as invalid.
func readPostgresSQLScripts(root string) (postgresSQLScripts, error) {
	entries, err := os.ReadDir(filepath.Join(root, postgresSQLDir))
	if err != nil {
		return postgresSQLScripts{}, fmt.Errorf("reading %s: %w", postgresSQLDir, err)
	}

	var scripts postgresSQLScripts
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type().IsRegular() {
			if match := baseSQLFileRE.FindStringSubmatch(name); match != nil && semverRE.MatchString(match[1]) {
				scripts.baseVersions = append(scripts.baseVersions, match[1])
				continue
			}
			if match := updateSQLFileRE.FindStringSubmatch(name); match != nil && semverRE.MatchString(match[1]) && semverRE.MatchString(match[2]) {
				scripts.updates = append(scripts.updates, updateScript{from: match[1], to: match[2]})
				continue
			}
		}
		scripts.invalid = append(scripts.invalid, fmt.Sprintf("%s is not a regular PostgreSQL install SQL or update SQL file", sqlPath(name)))
	}
	return scripts, nil
}

// readUpdateBaseline returns the version of the update baseline pin. The pin is
// one JSON object with exactly the keys version, artifact_url, and artifact_sha256.
func readUpdateBaseline(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, updateBaselinePath))
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", updateBaselinePath, err)
	}
	fields, err := decodeObjectFields(data)
	if err != nil {
		return "", fmt.Errorf("%s is invalid: %w", updateBaselinePath, err)
	}
	for _, key := range []string{"version", "artifact_url", "artifact_sha256"} {
		if _, exists := fields[key]; !exists {
			return "", fmt.Errorf("%s is invalid: missing key %q", updateBaselinePath, key)
		}
	}
	if len(fields) != 3 {
		return "", fmt.Errorf("%s is invalid: it must contain only version, artifact_url, and artifact_sha256", updateBaselinePath)
	}
	var version string
	if err := json.Unmarshal(fields["version"], &version); err != nil || !semverRE.MatchString(version) {
		return "", fmt.Errorf("%s is invalid: version must be a string that matches X.Y.Z", updateBaselinePath)
	}
	return version, nil
}

// decodeObjectFields decodes one JSON object. It rejects a duplicate key and
// data after the object.
func decodeObjectFields(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return nil, errors.New("content must be one JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("malformed JSON: %w", err)
		}
		key, _ := token.(string)
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("malformed JSON: %w", err)
		}
		fields[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("malformed JSON: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("content must be one JSON object")
	}
	return fields, nil
}

// compareVersions compares two versions that semverRE accepts by numeric
// major, then minor, then patch. semverRE rejects leading zeros, so a longer
// component is a larger number.
func compareVersions(left string, right string) int {
	leftParts := strings.Split(left, ".")
	rightParts := strings.Split(right, ".")
	for index := range leftParts {
		if order := cmp.Or(cmp.Compare(len(leftParts[index]), len(rightParts[index])), strings.Compare(leftParts[index], rightParts[index])); order != 0 {
			return order
		}
	}
	return 0
}

func (update updateScript) name() string {
	return updateSQLName(update.from, update.to)
}

func baseSQLName(version string) string {
	return "synchro_pg--" + version + ".sql"
}

func updateSQLName(from string, to string) string {
	return "synchro_pg--" + from + "--" + to + ".sql"
}

func sqlPath(name string) string {
	return filepath.Join(postgresSQLDir, name)
}

func relativePath(root string, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return rel
}
