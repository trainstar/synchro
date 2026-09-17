package releaseversion

import (
	"os"
	"os/exec"
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

func TestCheckRejectsDistributionCatalogDrift(t *testing.T) {
	for _, path := range []string{"conformance/artifacts/inventory.json", "conformance/requirements.json", "conformance/support-matrix.json"} {
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

func TestNextVersionPassesSupportPolicyAndRequirementsSchema(t *testing.T) {
	repoRoot, err := FindRepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	root := newFixtureRepo(t)
	const schemaPath = "conformance/schemas/requirements-v2.schema.json"
	for _, path := range []string{"conformance/requirements.json", "conformance/support-matrix.json", schemaPath} {
		data, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatal(err)
		}
		writeFixtureFile(t, root, path, string(data))
	}
	if err := Set(root, "0.4.0"); err != nil {
		t.Fatal(err)
	}
	if err := Check(root, "v0.4.0"); err != nil {
		t.Fatal(err)
	}
	script := `
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import Ajv2020 from "ajv/dist/2020.js";
import { supportPolicyErrors } from "./scripts/validators/support-policy.mjs";

const root = process.argv[1];
const read = (file) => JSON.parse(fs.readFileSync(path.join(root, file), "utf8"));
const release = fs.readFileSync(path.join(root, "VERSION"), "utf8").trim();
const requirements = read("conformance/requirements.json");
const support = read("conformance/support-matrix.json");
const schema = read("conformance/schemas/requirements-v2.schema.json");
const ajv = new Ajv2020({ allErrors: true, strict: false, validateSchema: true });
const validate = ajv.compile(schema);
assert.equal(validate(requirements), true, JSON.stringify(validate.errors));
assert.deepEqual(supportPolicyErrors(requirements, support, release), []);

const staleRequirements = { ...requirements, release: "0.3.0" };
assert.equal(validate(staleRequirements), false);
assert(validate.errors.some((error) => error.instancePath === "/release" && error.keyword === "const"));
assert(supportPolicyErrors(staleRequirements, support, release).length > 0);
assert(supportPolicyErrors(requirements, { ...support, release: "0.3.0" }, release).length > 0);

for (const file of ["requirements.json", "support-matrix.json"]) {
  const original = JSON.parse(fs.readFileSync(path.join("..", "conformance", file), "utf8"));
  const updated = read(path.join("conformance", file));
  original.release = updated.release;
  assert.deepEqual(updated, original);
}
const originalSchema = JSON.parse(fs.readFileSync("../conformance/schemas/requirements-v2.schema.json", "utf8"));
originalSchema.properties.release.const = release;
assert.deepEqual(schema, originalSchema);
`
	command := exec.Command("node", "--input-type=module", "-e", script, root)
	command.Dir = filepath.Join(repoRoot, "docs")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("next-version support and schema validation failed: %v\n%s", err, output)
	}

	data, err := os.ReadFile(filepath.Join(root, schemaPath))
	if err != nil {
		t.Fatal(err)
	}
	stale := strings.Replace(string(data), `"release": { "const": "0.4.0" }`, `"release": { "const": "0.3.0" }`, 1)
	writeFixtureFile(t, root, schemaPath, stale)
	if err := Check(root, ""); err == nil || !strings.Contains(err.Error(), schemaPath) {
		t.Fatalf("Check did not identify schema release drift: %v", err)
	}
	if err := Sync(root); err != nil {
		t.Fatal(err)
	}
	if err := Check(root, ""); err != nil {
		t.Fatal(err)
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
