package blackbox

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestVerifyPG18BinariesRequiresExactRuntimeVersion(t *testing.T) {
	tests := []struct {
		name      string
		versions  map[string]string
		wantError bool
	}{
		{name: "exact", versions: map[string]string{}},
		{name: "wrong patch", versions: map[string]string{"postgres": "18.2"}, wantError: true},
		{name: "wrong major", versions: map[string]string{"psql": "17.7"}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binDir := writePostgresVersionFixtures(t, test.versions)
			_, version, err := verifyPG18Binaries(binDir)
			if test.wantError {
				if err == nil {
					t.Fatal("PostgreSQL version mismatch was accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("exact PostgreSQL runtime rejected: %v", err)
			}
			if version != postgresqlRuntimeVersion {
				t.Fatalf("PostgreSQL version = %q, want %q", version, postgresqlRuntimeVersion)
			}
		})
	}
}

func TestVerifyAdapterArtifactRejectsTampering(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "synchrod-pg")
	original := []byte("adapter-artifact")
	if err := os.WriteFile(path, original, 0o755); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(original)
	if err := os.WriteFile(path+".sha256", []byte(hex.EncodeToString(digest[:])+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifyAdapterArtifact(path); err != nil {
		t.Fatalf("valid adapter artifact rejected: %v", err)
	}
	if err := os.WriteFile(path, []byte("tampered-adapter"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := verifyAdapterArtifact(path); err == nil {
		t.Fatal("tampered adapter artifact was accepted")
	}
}

func TestLoadEnvironmentRequiresFiveDistinctRoleCredentials(t *testing.T) {
	root := t.TempDir()
	passwordFiles := make(map[string]string)
	for _, role := range []string{"admin", "adapter", "observer", "worker", "operator"} {
		path := filepath.Join(root, role+"-password")
		if err := os.WriteFile(path, []byte(role+"-secret"), 0o600); err != nil {
			t.Fatal(err)
		}
		passwordFiles[role] = path
	}
	jwtPath := filepath.Join(root, "jwt-secret")
	if err := os.WriteFile(jwtPath, []byte("jwt-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	adapterPath := filepath.Join(root, "synchrod-pg")
	adapterData := []byte("adapter")
	if err := os.WriteFile(adapterPath, adapterData, 0o755); err != nil {
		t.Fatal(err)
	}
	adapterDigest := sha256.Sum256(adapterData)
	if err := os.WriteFile(adapterPath+".sha256", []byte(hex.EncodeToString(adapterDigest[:])), 0o644); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"SYNCHRO_CONFORMANCE_PG18_BINDIR":            writePostgresVersionFixtures(t, nil),
		"SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT":     writeExtensionBundleFixture(t),
		"SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT":       adapterPath,
		"SYNCHRO_CONFORMANCE_ADMIN_USER":             "cf_admin",
		"SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE":    passwordFiles["admin"],
		"SYNCHRO_CONFORMANCE_ADAPTER_USER":           "cf_adapter",
		"SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE":  passwordFiles["adapter"],
		"SYNCHRO_CONFORMANCE_OBSERVER_USER":          "cf_observer",
		"SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE": passwordFiles["observer"],
		"SYNCHRO_CONFORMANCE_WORKER_USER":            "cf_worker",
		"SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE":   passwordFiles["worker"],
		"SYNCHRO_CONFORMANCE_OPERATOR_USER":          "cf_operator",
		"SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE": passwordFiles["operator"],
		"SYNCHRO_CONFORMANCE_JWT_SECRET_FILE":        jwtPath,
		"SYNCHRO_CONFORMANCE_INSTALL_LOCK":           filepath.Join(root, "install.lock"),
	}
	lookup := func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
	config, err := loadEnvironment(lookup)
	if err != nil {
		t.Fatalf("valid five-role environment rejected: %v", err)
	}
	if config.Operator.Username != "cf_operator" {
		t.Fatalf("operator username = %q", config.Operator.Username)
	}

	values["SYNCHRO_CONFORMANCE_OPERATOR_USER"] = values["SYNCHRO_CONFORMANCE_ADMIN_USER"]
	if _, err := loadEnvironment(lookup); err == nil || !strings.Contains(err.Error(), "roles must be distinct") {
		t.Fatalf("duplicate role username was accepted: %v", err)
	}
}

func TestLoadEnvironmentRequiresOperatorCredentialVariables(t *testing.T) {
	lookup := func(key string) (string, bool) {
		if key == "SYNCHRO_CONFORMANCE_OPERATOR_USER" || key == "SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE" {
			return "", false
		}
		return "fixture", true
	}
	_, err := loadEnvironment(lookup)
	if err == nil || !strings.Contains(err.Error(), "SYNCHRO_CONFORMANCE_OPERATOR_USER") ||
		!strings.Contains(err.Error(), "SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE") {
		t.Fatalf("missing operator credential variables were not reported: %v", err)
	}
}

func TestLoadLocalEnvironmentRequiresRuntimeMatchingExtensionArtifact(t *testing.T) {
	root := t.TempDir()
	passwordFiles := make(map[string]string)
	for _, role := range []string{"admin", "adapter", "observer", "worker", "operator"} {
		path := filepath.Join(root, role+"-password")
		if err := os.WriteFile(path, []byte(role+"-secret"), 0o600); err != nil {
			t.Fatal(err)
		}
		passwordFiles[role] = path
	}
	jwtPath := filepath.Join(root, "jwt-secret")
	if err := os.WriteFile(jwtPath, []byte("jwt-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	adapterPath := filepath.Join(root, "synchrod-pg")
	adapterData := []byte("adapter")
	if err := os.WriteFile(adapterPath, adapterData, 0o755); err != nil {
		t.Fatal(err)
	}
	adapterDigest := sha256.Sum256(adapterData)
	if err := os.WriteFile(adapterPath+".sha256", []byte(hex.EncodeToString(adapterDigest[:])), 0o644); err != nil {
		t.Fatal(err)
	}
	versions := make(map[string]string)
	for _, program := range []string{"initdb", "pg_ctl", "postgres", "psql", "pg_isready", "pg_config"} {
		versions[program] = "18.6"
	}
	extensionPath := writeExtensionBundleFixture(t)
	manifest := readExtensionManifestFixture(t, extensionPath)
	manifest.PostgreSQLVersion = "18.6"
	writeExtensionManifestFixture(t, extensionPath, manifest)
	values := map[string]string{
		"SYNCHRO_CONFORMANCE_PG18_BINDIR":            writePostgresVersionFixtures(t, versions),
		"SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT":     extensionPath,
		"SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT":       adapterPath,
		"SYNCHRO_CONFORMANCE_ADMIN_USER":             "cf_admin",
		"SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE":    passwordFiles["admin"],
		"SYNCHRO_CONFORMANCE_ADAPTER_USER":           "cf_adapter",
		"SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE":  passwordFiles["adapter"],
		"SYNCHRO_CONFORMANCE_OBSERVER_USER":          "cf_observer",
		"SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE": passwordFiles["observer"],
		"SYNCHRO_CONFORMANCE_WORKER_USER":            "cf_worker",
		"SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE":   passwordFiles["worker"],
		"SYNCHRO_CONFORMANCE_OPERATOR_USER":          "cf_operator",
		"SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE": passwordFiles["operator"],
		"SYNCHRO_CONFORMANCE_JWT_SECRET_FILE":        jwtPath,
		"SYNCHRO_CONFORMANCE_INSTALL_LOCK":           filepath.Join(root, "install.lock"),
	}
	lookup := func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
	if _, err := loadLocalEnvironment(lookup); err != nil {
		t.Fatalf("runtime-matched local environment rejected: %v", err)
	}

	manifest.PostgreSQLVersion = "18.5"
	writeExtensionManifestFixture(t, extensionPath, manifest)
	if _, err := loadLocalEnvironment(lookup); err == nil {
		t.Fatal("local environment accepted an extension artifact for a different runtime")
	}

	manifest.PostgreSQLVersion = "17.7"
	writeExtensionManifestFixture(t, extensionPath, manifest)
	for program := range versions {
		versions[program] = "17.7"
	}
	values["SYNCHRO_CONFORMANCE_PG18_BINDIR"] = writePostgresVersionFixtures(t, versions)
	if _, err := loadLocalEnvironment(lookup); err == nil {
		t.Fatal("local environment accepted a non-PostgreSQL-18 runtime")
	}
}

func TestVerifyExtensionBundleRejectsTamperingAndWrongDestinations(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		if _, err := verifyExtensionBundle(writeExtensionBundleFixture(t)); err != nil {
			t.Fatalf("valid extension bundle rejected: %v", err)
		}
	})

	t.Run("payload tampering", func(t *testing.T) {
		root := writeExtensionBundleFixture(t)
		bundle, err := verifyExtensionBundle(root)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(bundle.files[0].Path)), []byte("tampered"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyExtensionBundle(root); err == nil {
			t.Fatal("tampered extension payload was accepted")
		}
	})

	t.Run("manifest tampering", func(t *testing.T) {
		root := writeExtensionBundleFixture(t)
		path := filepath.Join(root, extensionBundleManifestName)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyExtensionBundle(root); err == nil {
			t.Fatal("manifest with a stale digest was accepted")
		}
	})

	t.Run("digest tampering", func(t *testing.T) {
		root := writeExtensionBundleFixture(t)
		path := filepath.Join(root, extensionBundleManifestName+".sha256")
		if err := os.WriteFile(path, []byte(strings.Repeat("0", 64)+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := verifyExtensionBundle(root); err == nil {
			t.Fatal("wrong manifest digest was accepted")
		}
	})

	t.Run("extra destination", func(t *testing.T) {
		root := writeExtensionBundleFixture(t)
		manifest := readExtensionManifestFixture(t, root)
		manifest.Files = append(manifest.Files, extensionBundleFile{
			Path:        manifest.Files[0].Path,
			Destination: "sharedir/extension/unexpected.sql",
			SHA256:      manifest.Files[0].SHA256,
		})
		writeExtensionManifestFixture(t, root, manifest)
		if _, err := verifyExtensionBundle(root); err == nil {
			t.Fatal("extension bundle with an extra destination was accepted")
		}
	})

	t.Run("wrong PostgreSQL version", func(t *testing.T) {
		root := writeExtensionBundleFixture(t)
		manifest := readExtensionManifestFixture(t, root)
		manifest.PostgreSQLVersion = "18.2"
		writeExtensionManifestFixture(t, root, manifest)
		if _, err := verifyExtensionBundle(root); err == nil {
			t.Fatal("extension bundle for a different PostgreSQL runtime was accepted")
		}
	})

	t.Run("renamed destination", func(t *testing.T) {
		root := writeExtensionBundleFixture(t)
		manifest := readExtensionManifestFixture(t, root)
		manifest.Files[0].Destination = "pkglibdir/synchro_pg_changed.so"
		writeExtensionManifestFixture(t, root, manifest)
		if _, err := verifyExtensionBundle(root); err == nil {
			t.Fatal("extension bundle with a changed destination was accepted")
		}
	})
}

func TestExtensionBundleIdentityRejectsReplacedManifestAndPayload(t *testing.T) {
	t.Run("manifest", func(t *testing.T) {
		root := writeExtensionBundleFixture(t)
		loaded, err := verifyExtensionBundle(root)
		if err != nil {
			t.Fatal(err)
		}
		replaceFileWithSameBytes(t, filepath.Join(root, extensionBundleManifestName))
		current, err := verifyExtensionBundle(root)
		if err != nil {
			t.Fatal(err)
		}
		if sameExtensionBundleIdentity(loaded, current) {
			t.Fatal("replaced manifest retained the loaded bundle identity")
		}
	})

	t.Run("payload", func(t *testing.T) {
		root := writeExtensionBundleFixture(t)
		loaded, err := verifyExtensionBundle(root)
		if err != nil {
			t.Fatal(err)
		}
		replaceFileWithSameBytes(t, filepath.Join(root, filepath.FromSlash(loaded.files[0].Path)))
		current, err := verifyExtensionBundle(root)
		if err != nil {
			t.Fatal(err)
		}
		if sameExtensionBundleIdentity(loaded, current) {
			t.Fatal("replaced payload retained the loaded bundle identity")
		}
	})
}

func writePostgresVersionFixtures(t *testing.T, versions map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for _, program := range []string{"initdb", "pg_ctl", "postgres", "psql", "pg_isready", "pg_config"} {
		version := versions[program]
		if version == "" {
			version = postgresqlRuntimeVersion
		}
		body := "#!/bin/sh\nprintf '%s\\n' '" + program + " (PostgreSQL) " + version + "'\n"
		if err := os.WriteFile(filepath.Join(root, program), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func writeExtensionBundleFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	suffix := "so"
	if runtime.GOOS == "darwin" {
		suffix = "dylib"
	}
	files := []extensionBundleFile{
		{Path: "payload/synchro_pg." + suffix, Destination: "pkglibdir/synchro_pg." + suffix},
		{Path: "payload/synchro_pg.control", Destination: "sharedir/extension/synchro_pg.control"},
		{Path: "payload/synchro_pg--0.3.0.sql", Destination: "sharedir/extension/synchro_pg--0.3.0.sql"},
	}
	for index := range files {
		path := filepath.Join(root, filepath.FromSlash(files[index].Path))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		data := []byte("fixture-" + files[index].Destination)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		files[index].SHA256 = hex.EncodeToString(digest[:])
	}
	writeExtensionManifestFixture(t, root, extensionBundleManifest{
		Format:            extensionBundleManifestFormat,
		PostgreSQLMajor:   18,
		PostgreSQLVersion: postgresqlRuntimeVersion,
		Files:             files,
	})
	return root
}

func readExtensionManifestFixture(t *testing.T, root string) extensionBundleManifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, extensionBundleManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var manifest extensionBundleManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func writeExtensionManifestFixture(t *testing.T, root string, manifest extensionBundleManifest) {
	t.Helper()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, extensionBundleManifestName)
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if err := os.WriteFile(manifestPath+".sha256", []byte(hex.EncodeToString(digest[:])+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func replaceFileWithSameBytes(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mode, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	temporary := path + ".replacement"
	if err := os.WriteFile(temporary, data, mode.Mode().Perm()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}
}
