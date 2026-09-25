package releaseversion

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	valid := []string{"0.1.0", "1.2.3", "10.20.30"}
	for _, version := range valid {
		if err := Validate(version); err != nil {
			t.Fatalf("Validate(%q) returned error: %v", version, err)
		}
	}

	invalid := []string{"v1.2.3", "1.2", "1.2.x", "1.2.3-beta", "01.2.3", "1.02.3", "1.2.03", "00.0.0"}
	for _, version := range invalid {
		if err := Validate(version); err == nil {
			t.Fatalf("Validate(%q) unexpectedly succeeded", version)
		}
	}
}

func TestSetSyncsAndChecksVersionedSurfaces(t *testing.T) {
	t.Parallel()

	root := newFixtureRepo(t)

	if err := Set(root, "1.4.5"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	if err := Check(root, "v1.4.5"); err != nil {
		t.Fatalf("Check returned error after Set: %v", err)
	}

	assertFileContains(t, filepath.Join(root, "VERSION"), "1.4.5")
	assertFileContains(t, filepath.Join(root, "Synchro.podspec"), `s.version = "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "clients/react-native/package.json"), `"version": "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "clients/react-native/SynchroReactNative.podspec"), `:tag => "v#{s.version}"`)
	assertFileContains(t, filepath.Join(root, "clients/react-native/SynchroReactNative.podspec"), `s.dependency "Synchro", "= #{s.version}"`)
	assertFileContains(t, filepath.Join(root, "clients/react-native/android/build.gradle"), `def defaultSynchroVersion = "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "clients/react-native/android/build.gradle"), `implementation "fit.trainstar:synchro:${resolvedSynchroVersion}"`)
	assertFileContains(t, filepath.Join(root, "clients/kotlin/gradle.properties"), `version=1.4.5`)
	assertFileContains(t, filepath.Join(root, "clients/kotlin/synchro/build.gradle.kts"), `coordinates("fit.trainstar", "synchro", project.version.toString())`)
	assertFileContains(t, filepath.Join(root, "extensions/Cargo.toml"), `version = "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "extensions/synchro-pg/synchro_pg.control"), `default_version = '1.4.5'`)
	assertFileContains(t, filepath.Join(root, "conformance/artifacts/inventory.json"), `"release": "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "conformance/requirements.json"), `"release": "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "conformance/requirements.json"), `"protocol_version": 3`)
	assertFileContains(t, filepath.Join(root, "conformance/support-matrix.json"), `"release": "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "conformance/support-matrix.json"), `"schema_version": 1`)

	assertFileContains(t, filepath.Join(root, "conformance/faults/catalog.json"), `"release": "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "conformance/schemas/vector-catalog-v1.schema.json"), `"release": { "const": "1.4.5" }`)
	assertFileContains(t, filepath.Join(root, "conformance/internal/release/release.go"), `const Version = "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "extensions/Cargo.lock"), "name = \"serde\"\nversion = \"1.0.0\"")
	assertFileContains(t, filepath.Join(root, "extensions/Cargo.lock"), "name = \"synchro-core\"\nversion = \"1.4.5\"")
	assertFileContains(t, filepath.Join(root, "extensions/Cargo.lock"), "name = \"synchro-pg\"\nversion = \"1.4.5\"")
	assertFileContains(t, filepath.Join(root, "verification/consumers/go/go.mod"), "require github.com/trainstar/synchro/api/go v1.4.5")
	assertFileContains(t, filepath.Join(root, "clients/react-native/example/ios/Podfile.lock"), "  - SynchroReactNative (1.4.5):\n    - Synchro (= 1.4.5)")
	assertFileContains(t, filepath.Join(root, "conformance/mutants/integration/install.patch"), "synchro_pg--1.4.5.sql")
	assertFileContains(t, filepath.Join(root, "conformance/scenarios/versioned.json"), `"extension_version": "1.4.5"`)
	assertFileContains(t, filepath.Join(root, "conformance/scenarios/unversioned.json"), `"id": "SCN-FIXTURE"`)
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.ReplaceAll(publishedReferenceFixture, "0.1.0", "1.4.5"); string(readme) != want {
		t.Fatalf("README.md release references were not synced exactly:\n%s", readme)
	}

	if _, err := os.Stat(filepath.Join(root, "extensions/synchro-pg/sql/synchro_pg--1.4.5.sql")); err != nil {
		t.Fatalf("expected PostgreSQL install SQL to be renamed: %v", err)
	}
}

func TestCheckFailsOnDrift(t *testing.T) {
	t.Parallel()

	root := newFixtureRepo(t)
	if err := Set(root, "1.4.5"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	path := filepath.Join(root, "clients/kotlin/gradle.properties")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading drift file: %v", err)
	}

	updated := strings.Replace(string(data), "version=1.4.5", "version=9.9.9", 1)
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatalf("writing drift file: %v", err)
	}

	err = Check(root, "")
	if err == nil {
		t.Fatal("Check unexpectedly succeeded with drifted metadata")
	}
	if !strings.Contains(err.Error(), "clients/kotlin/gradle.properties") {
		t.Fatalf("Check error did not mention drifted file: %v", err)
	}
}

func TestCheckRejectsAndSyncRepairsReleaseReferenceDrift(t *testing.T) {
	cases := []struct {
		path string
		old  string
		new  string
	}{
		{"extensions/Cargo.lock", "name = \"synchro-pg\"\nversion = \"1.4.5\"", "name = \"synchro-pg\"\nversion = \"9.9.9\""},
		{"verification/consumers/go/go.mod", "api/go v1.4.5", "api/go v9.9.9"},
		{"conformance/internal/release/release.go", `"1.4.5"`, `"9.9.9"`},
		{"docs/src/content/docs/index.mdx", "exact: \"1.4.5\"", "exact: \"9.9.9\""},
		{"clients/react-native/example/ios/Podfile.lock", "Synchro (= 1.4.5)", "Synchro (= 9.9.9)"},
		{"conformance/mutants/integration/install.patch", "synchro_pg--1.4.5.sql", "synchro_pg--9.9.9.sql"},
		{"conformance/scenarios/versioned.json", `"extension_version": "1.4.5"`, `"extension_version": "9.9.9"`},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			root := newFixtureRepo(t)
			if err := Set(root, "1.4.5"); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, tc.path)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), tc.old) {
				t.Fatalf("%s does not contain %q", tc.path, tc.old)
			}
			writeFixtureFile(t, root, tc.path, strings.Replace(string(data), tc.old, tc.new, 1))
			if err := Check(root, ""); err == nil || !strings.Contains(err.Error(), tc.path) {
				t.Fatalf("Check did not identify drift in %s: %v", tc.path, err)
			}
			if err := Sync(root); err != nil {
				t.Fatal(err)
			}
			if err := Check(root, ""); err != nil {
				t.Fatal(err)
			}
			assertFileContains(t, path, tc.old)
		})
	}
}

func TestCheckRequiresReleaseReferences(t *testing.T) {
	cases := []struct {
		name    string
		remove  []string
		message string
	}{
		{"listed path", []string{"clients/react-native/README.md"}, "missing release-version reference in clients/react-native/README.md"},
		{"directory set", []string{"conformance/mutants/integration/install.patch"}, "conformance/mutants/integration has no release-version reference"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := newFixtureRepo(t)
			if err := Set(root, "1.4.5"); err != nil {
				t.Fatal(err)
			}
			for _, path := range tc.remove {
				writeFixtureFile(t, root, path, "no release reference\n")
			}
			if err := Check(root, ""); err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("Check error = %v, want %q", err, tc.message)
			}
			if err := Sync(root); err == nil {
				t.Fatal("Sync accepted a surface without a release reference")
			}
		})
	}
}

func TestCheckFailsOnTagMismatch(t *testing.T) {
	t.Parallel()

	root := newFixtureRepo(t)
	if err := Set(root, "1.4.5"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	err := Check(root, "v1.4.6")
	if err == nil {
		t.Fatal("Check unexpectedly succeeded with mismatched tag")
	}
	if !strings.Contains(err.Error(), `expected release tag "v1.4.6" to match v1.4.5`) {
		t.Fatalf("unexpected tag mismatch error: %v", err)
	}
}

func TestCheckRejectsDistributionCatalogDrift(t *testing.T) {
	for _, path := range []string{
		"conformance/artifacts/inventory.json",
		"conformance/requirements.json",
		"conformance/support-matrix.json",
		"conformance/faults/catalog.json",
		"conformance/performance/budgets.json",
		"conformance/vectors/catalog.json",
	} {
		t.Run(path, func(t *testing.T) {
			root := newFixtureRepo(t)
			if err := Sync(root); err != nil {
				t.Fatal(err)
			}
			writeFixtureFile(t, root, path, "{\n  \"release\": \"9.9.9\",\n  \"protocol_version\": 3\n}\n")
			if err := Check(root, ""); err == nil || !strings.Contains(err.Error(), path) {
				t.Fatalf("Check did not identify catalog drift: %v", err)
			}
			if err := Sync(root); err != nil {
				t.Fatal(err)
			}
			if err := Check(root, ""); err != nil {
				t.Fatal(err)
			}
			assertFileContains(t, filepath.Join(root, path), `"protocol_version": 3`)
		})
	}
}

func TestFindRepoRootSupportsGitDirectoryAndWorktreeFile(t *testing.T) {
	for _, worktree := range []bool{false, true} {
		root := t.TempDir()
		start := filepath.Join(root, "api", "go")
		mustMkdirAll(t, start)
		if worktree {
			writeFixtureFile(t, root, ".git", "gitdir: /checkout/.git/worktrees/linked\n")
		} else {
			mustMkdirAll(t, filepath.Join(root, ".git"))
		}
		got, err := FindRepoRoot(start)
		if err != nil || got != root {
			t.Fatalf("FindRepoRoot(worktree=%v) = %q, %v", worktree, got, err)
		}
	}
	root := t.TempDir()
	writeFixtureFile(t, root, ".git", "not a Git worktree marker\n")
	if got, err := FindRepoRoot(root); err == nil {
		t.Fatalf("accepted invalid Git marker at %q", got)
	}
}

// publishedReferenceFixture covers each published reference form and one
// PostgreSQL version that the tool must not change.
const publishedReferenceFixture = "Synchro `0.1.0` uses PostgreSQL 18.3.\n" +
	"Git tag `v0.1.0`\n" +
	"npm install @trainstar/synchro-react-native@0.1.0\n" +
	"dist/local-consumer/npm/trainstar-synchro-react-native-0.1.0.tgz\n" +
	"implementation(\"fit.trainstar:synchro:0.1.0\")\n" +
	"pod 'Synchro', :git => 'https://github.com/trainstar/synchro.git', :tag => 'v0.1.0'\n" +
	".package(url: \"https://github.com/trainstar/synchro.git\",\n        exact: \"0.1.0\")\n"

func newFixtureRepo(t *testing.T) string {
	t.Helper()

	root := t.TempDir()
	mustMkdirAll(t, filepath.Join(root, ".git"))
	writeFixtureFile(t, root, "Synchro.podspec", "Pod::Spec.new do |s|\n  s.version = \"0.2.0\"\n  s.source = { :git => \"https://github.com/trainstar/synchro.git\", :tag => \"0.2.0\" }\nend\n")
	writeFixtureFile(t, root, "clients/react-native/package.json", "{\n  \"name\": \"@trainstar/synchro-react-native\",\n  \"version\": \"0.2.0\",\n  \"description\": \"fixture\"\n}\n")
	writeFixtureFile(t, root, "clients/react-native/SynchroReactNative.podspec", "Pod::Spec.new do |s|\n  s.source       = { :git => \"https://github.com/trainstar/synchro.git\", :tag => \"0.2.0\" }\n  s.dependency \"Synchro\", \"~> 0.2\"\nend\n")
	writeFixtureFile(t, root, "clients/react-native/android/build.gradle", "def defaultSynchroVersion = \"0.2.0\"\n\ndependencies {\n  implementation \"fit.trainstar:synchro:0.1.0\"\n}\n")
	writeFixtureFile(t, root, "clients/kotlin/gradle.properties", "version=0.1.0\n")
	writeFixtureFile(t, root, "clients/kotlin/synchro/build.gradle.kts", "mavenPublishing {\n    coordinates(\"fit.trainstar\", \"synchro\", project.findProperty(\"version\")?.toString() ?: \"0.1.0\")\n}\n")
	writeFixtureFile(t, root, "extensions/Cargo.toml", "[workspace]\nresolver = \"2\"\n\n[workspace.package]\nversion = \"0.1.0\"\nedition = \"2021\"\n")
	writeFixtureFile(t, root, "extensions/synchro-pg/synchro_pg.control", "comment = 'fixture'\ndefault_version = '0.1.0'\n")
	writeFixtureFile(t, root, "conformance/artifacts/inventory.json", "{\n  \"release\": \"0.1.0\",\n  \"artifacts\": []\n}\n")
	writeFixtureFile(t, root, "conformance/requirements.json", "{\n  \"release\": \"0.1.0\",\n  \"protocol_version\": 3\n}\n")
	writeFixtureFile(t, root, "conformance/support-matrix.json", "{\n  \"release\": \"0.1.0\",\n  \"schema_version\": 1\n}\n")
	writeFixtureFile(t, root, "conformance/schemas/requirements-v2.schema.json", "{\n  \"properties\": {\n    \"release\": { \"const\": \"0.1.0\" },\n    \"schema_version\": { \"const\": 2 }\n  }\n}\n")
	for _, path := range []string{"conformance/faults/catalog.json", "conformance/performance/budgets.json", "conformance/vectors/catalog.json"} {
		writeFixtureFile(t, root, path, "{\n  \"release\": \"0.1.0\",\n  \"schema_version\": 1\n}\n")
	}
	for _, path := range []string{"conformance/schemas/fault-catalog-v1.schema.json", "conformance/schemas/performance-budgets-v2.schema.json", "conformance/schemas/vector-catalog-v1.schema.json"} {
		writeFixtureFile(t, root, path, "{\n  \"properties\": {\n    \"release\": { \"const\": \"0.1.0\" },\n    \"schema_version\": { \"const\": 1 }\n  }\n}\n")
	}
	writeFixtureFile(t, root, "conformance/internal/release/release.go", "package release\n\nconst Version = \"0.1.0\"\n")
	writeFixtureFile(t, root, "extensions/Cargo.lock", "[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n\n[[package]]\nname = \"synchro-core\"\nversion = \"0.1.0\"\n\n[[package]]\nname = \"synchro-pg\"\nversion = \"0.1.0\"\n")
	writeFixtureFile(t, root, "verification/consumers/go/go.mod", "module example.com/consumer\n\ngo 1.25\n\nrequire github.com/trainstar/synchro/api/go v0.1.0\n")
	for _, path := range []string{
		"README.md",
		"clients/react-native/README.md",
		"docs/src/content/docs/clients/consumption.mdx",
		"docs/src/content/docs/getting-started/quickstart.mdx",
		"docs/src/content/docs/getting-started/server-setup.mdx",
		"docs/src/content/docs/index.mdx",
	} {
		writeFixtureFile(t, root, path, publishedReferenceFixture)
	}
	writeFixtureFile(t, root, "clients/react-native/example/ios/Podfile.lock", "PODS:\n  - Synchro (0.1.0):\n    - SQLCipher\n  - SynchroReactNative (0.1.0):\n    - Synchro (= 0.1.0)\n")
	writeFixtureFile(t, root, "conformance/mutants/integration/install.patch", "+SELECT 'synchro_pg--0.1.0.sql';\n")
	writeFixtureFile(t, root, "conformance/mutants/integration/other.patch", "+SELECT 1;\n")
	writeFixtureFile(t, root, "conformance/scenarios/versioned.json", "{\n  \"extension_version\": \"0.1.0\"\n}\n")
	writeFixtureFile(t, root, "conformance/scenarios/unversioned.json", "{\n  \"id\": \"SCN-FIXTURE\"\n}\n")
	writeFixtureFile(t, root, "extensions/synchro-pg/sql/synchro_pg--0.1.0.sql", "-- install script\n")
	writeFixtureFile(t, root, "VERSION", "0.2.0\n")

	return root
}

func writeFixtureFile(t *testing.T, root string, relativePath string, contents string) {
	t.Helper()

	fullPath := filepath.Join(root, relativePath)
	mustMkdirAll(t, filepath.Dir(fullPath))
	if err := os.WriteFile(fullPath, []byte(contents), 0o644); err != nil {
		t.Fatalf("writing %s: %v", relativePath, err)
	}
}

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()

	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("creating %s: %v", path, err)
	}
}

func assertFileContains(t *testing.T, path string, want string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	if !strings.Contains(string(data), want) {
		t.Fatalf("%s does not contain %q\n%s", path, want, string(data))
	}
}
