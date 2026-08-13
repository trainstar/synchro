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
