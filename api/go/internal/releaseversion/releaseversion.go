package releaseversion

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var semverRE = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

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

	if err := syncPostgresInstallSQL(root, version); err != nil {
		return err
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

	if err := checkPostgresInstallSQL(root, version); err != nil {
		failures = append(failures, err.Error())
	}

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

func syncPostgresInstallSQL(root string, version string) error {
	sqlDir := filepath.Join(root, "extensions", "synchro-pg", "sql")
	baseFiles, upgradeFiles, err := postgresSQLFiles(sqlDir)
	if err != nil {
		return err
	}

	target := filepath.Join(sqlDir, fmt.Sprintf("synchro_pg--%s.sql", version))
	if _, err := os.Stat(target); err == nil {
		return nil
	}

	if len(baseFiles) == 1 && len(upgradeFiles) == 0 {
		if err := os.Rename(baseFiles[0], target); err != nil {
			return fmt.Errorf("renaming PostgreSQL install SQL to %s: %w", filepath.Base(target), err)
		}
		return nil
	}

	return fmt.Errorf("manual PostgreSQL extension version update required: expected %s to exist", relativePath(root, target))
}

func checkPostgresInstallSQL(root string, version string) error {
	sqlDir := filepath.Join(root, "extensions", "synchro-pg", "sql")
	target := filepath.Join(sqlDir, fmt.Sprintf("synchro_pg--%s.sql", version))
	if _, err := os.Stat(target); err == nil {
		return nil
	}

	baseFiles, upgradeFiles, scanErr := postgresSQLFiles(sqlDir)
	if scanErr != nil {
		return scanErr
	}

	if len(baseFiles) == 1 && len(upgradeFiles) == 0 {
		return fmt.Errorf("PostgreSQL install SQL is still versioned as %s, expected %s", filepath.Base(baseFiles[0]), filepath.Base(target))
	}

	return fmt.Errorf("missing PostgreSQL install SQL %s", relativePath(root, target))
}

func postgresSQLFiles(sqlDir string) ([]string, []string, error) {
	entries, err := os.ReadDir(sqlDir)
	if err != nil {
		return nil, nil, fmt.Errorf("reading %s: %w", sqlDir, err)
	}

	var baseFiles []string
	var upgradeFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		fullPath := filepath.Join(sqlDir, name)
		if baseSQLFileRE.MatchString(name) {
			baseFiles = append(baseFiles, fullPath)
			continue
		}

		if strings.HasPrefix(name, "synchro_pg--") && strings.HasSuffix(name, ".sql") {
			upgradeFiles = append(upgradeFiles, fullPath)
		}
	}

	return baseFiles, upgradeFiles, nil
}

func relativePath(root string, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return rel
}
