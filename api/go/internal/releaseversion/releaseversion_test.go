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

	invalid := []string{"v1.2.3", "1.2", "1.2.x", "1.2.3-beta"}
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
