package blackbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const (
	environmentCommandTimeout = 5 * time.Second
	maximumSecretFileBytes    = int64(64 << 10)
	maximumManifestBytes      = int64(1 << 20)

	extensionBundleManifestName   = "artifact-manifest.json"
	extensionBundleManifestFormat = "synchro-pg18-extension-bundle-v1"
	postgresqlRuntimeVersion      = "18.3"
)

var (
	postgresVersionPattern     = regexp.MustCompile(`(?i)postgresql\)?\s*([0-9]+(?:\.[0-9]+)*)`)
	postgresql18VersionPattern = regexp.MustCompile(`^18\.[0-9]+$`)
	roleNamePattern            = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

// RequiredEnvironmentVariables lists the complete conformance environment contract.
// LoadEnvironment reads no other SYNCHRO_CONFORMANCE_ variable.
var RequiredEnvironmentVariables = []string{
	"SYNCHRO_CONFORMANCE_PG18_BINDIR",
	"SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT",
	"SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT",
	"SYNCHRO_CONFORMANCE_ADMIN_USER",
	"SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE",
	"SYNCHRO_CONFORMANCE_ADAPTER_USER",
	"SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE",
	"SYNCHRO_CONFORMANCE_OBSERVER_USER",
	"SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE",
	"SYNCHRO_CONFORMANCE_WORKER_USER",
	"SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE",
	"SYNCHRO_CONFORMANCE_OPERATOR_USER",
	"SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE",
	"SYNCHRO_CONFORMANCE_JWT_SECRET_FILE",
	"SYNCHRO_CONFORMANCE_INSTALL_LOCK",
}

// RoleCredential identifies one isolated PostgreSQL role.
// The password remains private to the black-box process owner.
type RoleCredential struct {
	Username     string
	PasswordFile string

	password       []byte
	passwordDigest string
}

// EnvironmentConfig contains the complete verified external harness input.
// Its unexported fields prevent unchecked configuration construction.
type EnvironmentConfig struct {
	AttachDatabaseURL string
	PG18BinDir        string
	ExtensionArtifact string
	AdapterArtifact   string
	Admin             RoleCredential
	Adapter           RoleCredential
	Observer          RoleCredential
	Worker            RoleCredential
	Operator          RoleCredential
	JWTSecretFile     string
	InstallationLock  string

	jwtSecret       []byte
	jwtDigest       string
	adapterSHA256   string
	adapterIdentity adapterArtifactIdentity
	postgresVersion string
	extension       extensionBundle
	verified        bool
}

type adapterArtifactIdentity struct {
	path           string
	digestPath     string
	sha256         string
	digestSHA256   string
	executableInfo os.FileInfo
	digestInfo     os.FileInfo
}

type extensionBundle struct {
	root                 string
	rootInfo             os.FileInfo
	manifestInfo         os.FileInfo
	manifestDigestInfo   os.FileInfo
	manifestSHA256       string
	manifestDigestSHA256 string
	files                []extensionBundleFile
}

type extensionBundleManifest struct {
	Format            string                `json:"format"`
	PostgreSQLMajor   int                   `json:"postgresql_major"`
	PostgreSQLVersion string                `json:"postgresql_version"`
	Files             []extensionBundleFile `json:"files"`
}

type extensionBundleFile struct {
	Path        string `json:"path"`
	Destination string `json:"destination"`
	SHA256      string `json:"sha256"`

	sourceInfo os.FileInfo
}

// LoadEnvironment loads and verifies all required harness inputs.
func LoadEnvironment() (EnvironmentConfig, error) {
	return loadEnvironment(os.LookupEnv)
}

// LoadLocalEnvironment verifies a local PostgreSQL 18 runtime and its extension bundle.
func LoadLocalEnvironment() (EnvironmentConfig, error) {
	return loadLocalEnvironment(os.LookupEnv)
}

func loadEnvironment(lookup func(string) (string, bool)) (EnvironmentConfig, error) {
	return loadEnvironmentForPostgreSQLVersion(lookup, postgresqlRuntimeVersion)
}

func loadLocalEnvironment(lookup func(string) (string, bool)) (EnvironmentConfig, error) {
	return loadEnvironmentForPostgreSQLVersion(lookup, "")
}

func loadEnvironmentForPostgreSQLVersion(lookup func(string) (string, bool), requiredVersion string) (EnvironmentConfig, error) {
	if lookup == nil {
		return EnvironmentConfig{}, errors.New("conformance environment lookup is required")
	}
	attachURL, _ := lookup("SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL")
	attachURL = strings.TrimSpace(attachURL)
	// Attach mode owns no cluster lifecycle, so it needs no PostgreSQL
	// binaries, extension artifact, or installation lock on the consumer.
	attachOptional := map[string]bool{
		"SYNCHRO_CONFORMANCE_PG18_BINDIR":        attachURL != "",
		"SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT": attachURL != "",
		"SYNCHRO_CONFORMANCE_INSTALL_LOCK":       attachURL != "",
	}
	values := make(map[string]string, len(RequiredEnvironmentVariables))
	var failures []error
	for _, key := range RequiredEnvironmentVariables {
		value, present := lookup(key)
		if !present || strings.TrimSpace(value) == "" {
			if attachOptional[key] {
				continue
			}
			failures = append(failures, fmt.Errorf("%s is required", key))
			continue
		}
		values[key] = strings.TrimSpace(value)
	}
	if len(failures) != 0 {
		return EnvironmentConfig{}, errors.Join(failures...)
	}

	var pgBinDir, version string
	if value := values["SYNCHRO_CONFORMANCE_PG18_BINDIR"]; value != "" {
		var err error
		pgBinDir, version, err = verifyPG18BinariesForPostgreSQLVersion(value, requiredVersion)
		if err != nil {
			return EnvironmentConfig{}, err
		}
	}
	var extension extensionBundle
	if value := values["SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT"]; value != "" {
		extensionVersion := requiredVersion
		if extensionVersion == "" {
			extensionVersion = version
		}
		if extensionVersion == "" {
			return EnvironmentConfig{}, errors.New("PostgreSQL runtime version is required for the extension artifact")
		}
		var err error
		extension, err = verifyExtensionBundleForPostgreSQLVersion(value, extensionVersion)
		if err != nil {
			return EnvironmentConfig{}, err
		}
		if version == "" {
			version = extensionVersion
		}
	}
	adapterIdentity, err := loadAdapterArtifactIdentity(values["SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT"])
	if err != nil {
		return EnvironmentConfig{}, err
	}
	admin, err := loadRoleCredential(
		values["SYNCHRO_CONFORMANCE_ADMIN_USER"],
		values["SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE"],
		"SYNCHRO_CONFORMANCE_ADMIN",
	)
	if err != nil {
		return EnvironmentConfig{}, err
	}
	adapter, err := loadRoleCredential(
		values["SYNCHRO_CONFORMANCE_ADAPTER_USER"],
		values["SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE"],
		"SYNCHRO_CONFORMANCE_ADAPTER",
	)
	if err != nil {
		return EnvironmentConfig{}, err
	}
	observer, err := loadRoleCredential(
		values["SYNCHRO_CONFORMANCE_OBSERVER_USER"],
		values["SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE"],
		"SYNCHRO_CONFORMANCE_OBSERVER",
	)
	if err != nil {
		return EnvironmentConfig{}, err
	}
	worker, err := loadRoleCredential(
		values["SYNCHRO_CONFORMANCE_WORKER_USER"],
		values["SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE"],
		"SYNCHRO_CONFORMANCE_WORKER",
	)
	if err != nil {
		return EnvironmentConfig{}, err
	}
	operator, err := loadRoleCredential(
		values["SYNCHRO_CONFORMANCE_OPERATOR_USER"],
		values["SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE"],
		"SYNCHRO_CONFORMANCE_OPERATOR",
	)
	if err != nil {
		return EnvironmentConfig{}, err
	}
	roleNames := map[string]struct{}{
		admin.Username:    {},
		adapter.Username:  {},
		observer.Username: {},
		worker.Username:   {},
		operator.Username: {},
	}
	if len(roleNames) != 5 {
		return EnvironmentConfig{}, errors.New("conformance roles must be distinct")
	}
	jwtSecret, jwtPath, jwtDigest, err := loadSecretFile(values["SYNCHRO_CONFORMANCE_JWT_SECRET_FILE"], "SYNCHRO_CONFORMANCE_JWT_SECRET_FILE")
	if err != nil {
		return EnvironmentConfig{}, err
	}
	var installationLock string
	if value := values["SYNCHRO_CONFORMANCE_INSTALL_LOCK"]; value != "" {
		installationLock, err = verifyInstallationLock(value)
		if err != nil {
			return EnvironmentConfig{}, err
		}
	}

	return EnvironmentConfig{
		AttachDatabaseURL: attachURL,
		PG18BinDir:        pgBinDir,
		ExtensionArtifact: extension.root,
		AdapterArtifact:   adapterIdentity.path,
		Admin:             admin,
		Adapter:           adapter,
		Observer:          observer,
		Worker:            worker,
		Operator:          operator,
		JWTSecretFile:     jwtPath,
		InstallationLock:  installationLock,
		jwtSecret:         jwtSecret,
		jwtDigest:         jwtDigest,
		adapterSHA256:     adapterIdentity.sha256,
		adapterIdentity:   adapterIdentity,
		postgresVersion:   version,
		extension:         extension,
		verified:          true,
	}, nil
}

func loadRoleCredential(username, passwordFile, variablePrefix string) (RoleCredential, error) {
	if !roleNamePattern.MatchString(username) {
		return RoleCredential{}, fmt.Errorf("%s_USER is invalid", variablePrefix)
	}
	password, path, digest, err := loadSecretFile(passwordFile, variablePrefix+"_PASSWORD_FILE")
	if err != nil {
		return RoleCredential{}, err
	}
	return RoleCredential{
		Username:       username,
		PasswordFile:   path,
		password:       password,
		passwordDigest: digest,
	}, nil
}

func loadSecretFile(path, variable string) ([]byte, string, string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, "", "", fmt.Errorf("%s path is invalid", variable)
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return nil, "", "", fmt.Errorf("%s is unavailable", variable)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o400 == 0 {
		return nil, "", "", fmt.Errorf("%s must be an owner-readable regular file", variable)
	}
	file, err := os.Open(absolute)
	if err != nil {
		return nil, "", "", fmt.Errorf("%s cannot be read", variable)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maximumSecretFileBytes+1))
	if err != nil || int64(len(data)) > maximumSecretFileBytes {
		return nil, "", "", fmt.Errorf("%s has invalid contents", variable)
	}
	secret := bytes.TrimSpace(data)
	if len(secret) == 0 {
		return nil, "", "", fmt.Errorf("%s must not be empty", variable)
	}
	digest := sha256.Sum256(secret)
	return append([]byte(nil), secret...), absolute, hex.EncodeToString(digest[:]), nil
}

func verifyPG18Binaries(path string) (string, string, error) {
	return verifyPG18BinariesForPostgreSQLVersion(path, postgresqlRuntimeVersion)
}

func verifyPG18BinariesForPostgreSQLVersion(path, requiredVersion string) (string, string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", "", errors.New("SYNCHRO_CONFORMANCE_PG18_BINDIR path is invalid")
	}
	info, err := os.Stat(absolute)
	if err != nil || !info.IsDir() {
		return "", "", errors.New("SYNCHRO_CONFORMANCE_PG18_BINDIR must be a directory")
	}
	programs := []string{"initdb", "pg_ctl", "postgres", "psql", "pg_isready", "pg_config"}
	versions := make(map[string]string, len(programs))
	for _, program := range programs {
		candidate := filepath.Join(absolute, program)
		if err := verifyExecutable(candidate); err != nil {
			return "", "", fmt.Errorf("PostgreSQL binary %s is unavailable", program)
		}
		version, err := postgresBinaryVersionForPostgreSQLVersion(candidate, requiredVersion)
		if err != nil {
			if requiredVersion == "" {
				return "", "", fmt.Errorf("PostgreSQL binary %s is not PostgreSQL 18", program)
			}
			return "", "", fmt.Errorf("PostgreSQL binary %s is not PostgreSQL %s", program, requiredVersion)
		}
		versions[program] = version
	}
	first := versions[programs[0]]
	for _, program := range programs[1:] {
		if versions[program] != first {
			return "", "", errors.New("PostgreSQL binaries do not match")
		}
	}
	return absolute, first, nil
}

func verifyExecutable(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return errors.New("not an executable file")
	}
	return nil
}

func postgresBinaryVersion(path string) (string, error) {
	return postgresBinaryVersionForPostgreSQLVersion(path, postgresqlRuntimeVersion)
}

func postgresBinaryVersionForPostgreSQLVersion(path, requiredVersion string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), environmentCommandTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, path, "--version")
	output, err := command.Output()
	if err != nil || ctx.Err() != nil {
		return "", errors.New("version command failed")
	}
	match := postgresVersionPattern.FindStringSubmatch(string(output))
	if len(match) != 2 {
		return "", errors.New("version output is invalid")
	}
	if requiredVersion != "" && match[1] != requiredVersion {
		return "", fmt.Errorf("PostgreSQL version is not %s", requiredVersion)
	}
	if requiredVersion == "" && !postgresql18VersionPattern.MatchString(match[1]) {
		return "", errors.New("PostgreSQL version is not PostgreSQL 18")
	}
	return match[1], nil
}

func verifyAdapterArtifact(path string) (string, string, error) {
	identity, err := loadAdapterArtifactIdentity(path)
	if err != nil {
		return "", "", err
	}
	return identity.path, identity.sha256, nil
}

func loadAdapterArtifactIdentity(path string) (adapterArtifactIdentity, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return adapterArtifactIdentity{}, errors.New("SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT path is invalid")
	}
	info, err := os.Lstat(absolute)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return adapterArtifactIdentity{}, errors.New("SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT must be an executable regular file")
	}
	digestPath := absolute + ".sha256"
	digestInfo, err := os.Lstat(digestPath)
	if err != nil || digestInfo.Mode()&os.ModeSymlink != 0 || !digestInfo.Mode().IsRegular() {
		return adapterArtifactIdentity{}, errors.New("adapter artifact digest file is required")
	}
	expected, err := readAdapterDigest(digestPath)
	if err != nil {
		return adapterArtifactIdentity{}, err
	}
	digestSHA256, err := fileSHA256(digestPath)
	if err != nil {
		return adapterArtifactIdentity{}, errors.New("adapter artifact digest file cannot be verified")
	}
	actual, err := fileSHA256(absolute)
	if err != nil {
		return adapterArtifactIdentity{}, errors.New("adapter artifact hash cannot be verified")
	}
	if actual != expected {
		return adapterArtifactIdentity{}, errors.New("adapter artifact hash does not match its manifest")
	}
	currentInfo, err := os.Lstat(absolute)
	if err != nil || !os.SameFile(info, currentInfo) {
		return adapterArtifactIdentity{}, errors.New("adapter artifact identity changed during verification")
	}
	currentDigestInfo, err := os.Lstat(digestPath)
	if err != nil || !os.SameFile(digestInfo, currentDigestInfo) {
		return adapterArtifactIdentity{}, errors.New("adapter artifact digest identity changed during verification")
	}
	return adapterArtifactIdentity{
		path:           absolute,
		digestPath:     digestPath,
		sha256:         actual,
		digestSHA256:   digestSHA256,
		executableInfo: currentInfo,
		digestInfo:     currentDigestInfo,
	}, nil
}

func readAdapterDigest(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("adapter artifact digest file is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("adapter artifact digest file cannot be read")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 1025))
	if err != nil || len(data) > 1024 {
		return "", errors.New("adapter artifact digest file is invalid")
	}
	fields := strings.Fields(string(data))
	if len(fields) != 1 || !validSHA256(fields[0]) {
		return "", errors.New("adapter artifact digest file is invalid")
	}
	return fields[0], nil
}

func verifyExtensionBundle(path string) (extensionBundle, error) {
	return verifyExtensionBundleForPostgreSQLVersion(path, postgresqlRuntimeVersion)
}

func verifyExtensionBundleForPostgreSQLVersion(path, requiredVersion string) (extensionBundle, error) {
	root, err := filepath.Abs(path)
	if err != nil {
		return extensionBundle{}, errors.New("SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT path is invalid")
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return extensionBundle{}, errors.New("SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT must be a directory")
	}
	manifestPath := filepath.Join(root, extensionBundleManifestName)
	manifestInfo, err := os.Lstat(manifestPath)
	if err != nil || manifestInfo.Mode()&os.ModeSymlink != 0 || !manifestInfo.Mode().IsRegular() {
		return extensionBundle{}, errors.New("extension artifact manifest is required")
	}
	manifestData, err := readRegularFile(manifestPath, maximumManifestBytes)
	if err != nil {
		return extensionBundle{}, errors.New("extension artifact manifest is required")
	}
	manifestSHA256 := sha256.Sum256(manifestData)
	manifestDigest := hex.EncodeToString(manifestSHA256[:])
	manifestDigestPath := manifestPath + ".sha256"
	manifestDigestInfo, err := os.Lstat(manifestDigestPath)
	if err != nil || manifestDigestInfo.Mode()&os.ModeSymlink != 0 || !manifestDigestInfo.Mode().IsRegular() {
		return extensionBundle{}, errors.New("extension artifact manifest digest is required")
	}
	expectedManifestDigest, err := readArtifactDigest(manifestDigestPath, "extension artifact manifest digest")
	if err != nil || expectedManifestDigest != manifestDigest {
		return extensionBundle{}, errors.New("extension artifact manifest digest does not match")
	}
	manifestDigestSHA256, err := fileSHA256(manifestDigestPath)
	if err != nil {
		return extensionBundle{}, errors.New("extension artifact manifest digest cannot be verified")
	}
	var manifest extensionBundleManifest
	if err := decodeStrictManifest(manifestData, &manifest); err != nil {
		return extensionBundle{}, errors.New("extension artifact manifest is invalid")
	}
	if !postgresql18VersionPattern.MatchString(requiredVersion) || manifest.Format != extensionBundleManifestFormat || manifest.PostgreSQLMajor != 18 || manifest.PostgreSQLVersion != requiredVersion || len(manifest.Files) != 3 {
		return extensionBundle{}, errors.New("extension artifact manifest is invalid")
	}
	files := append([]extensionBundleFile(nil), manifest.Files...)
	sort.Slice(files, func(left, right int) bool {
		return files[left].Destination < files[right].Destination
	})
	seenSource := make(map[string]struct{}, len(files))
	seenDestination := make(map[string]struct{}, len(files))
	expectedDestinations := extensionBundleDestinations()
	for index := range files {
		file := &files[index]
		if !safeBundleRelativePath(file.Path) || !safeBundleDestination(file.Destination) || !validSHA256(file.SHA256) {
			return extensionBundle{}, errors.New("extension artifact manifest is invalid")
		}
		if _, exists := seenSource[file.Path]; exists {
			return extensionBundle{}, errors.New("extension artifact manifest has duplicate files")
		}
		if _, exists := seenDestination[file.Destination]; exists {
			return extensionBundle{}, errors.New("extension artifact manifest has duplicate destinations")
		}
		seenSource[file.Path] = struct{}{}
		seenDestination[file.Destination] = struct{}{}
		if _, expected := expectedDestinations[file.Destination]; !expected {
			return extensionBundle{}, errors.New("extension artifact manifest has an unexpected destination")
		}
		actualPath, err := safeBundleSourcePath(root, file.Path)
		if err != nil {
			return extensionBundle{}, errors.New("extension artifact contains an unsafe file")
		}
		actualDigest, err := fileSHA256(actualPath)
		if err != nil || actualDigest != file.SHA256 {
			return extensionBundle{}, errors.New("extension artifact hash does not match its manifest")
		}
		file.sourceInfo, err = os.Lstat(actualPath)
		if err != nil {
			return extensionBundle{}, errors.New("extension artifact contains an unavailable file")
		}
	}
	if len(seenDestination) != len(expectedDestinations) {
		return extensionBundle{}, errors.New("extension artifact is incomplete")
	}
	return extensionBundle{
		root:                 root,
		rootInfo:             rootInfo,
		manifestInfo:         manifestInfo,
		manifestDigestInfo:   manifestDigestInfo,
		manifestSHA256:       manifestDigest,
		manifestDigestSHA256: manifestDigestSHA256,
		files:                files,
	}, nil
}

func extensionBundleDestinations() map[string]struct{} {
	librarySuffix := "so"
	if runtime.GOOS == "darwin" {
		librarySuffix = "dylib"
	}
	return map[string]struct{}{
		"pkglibdir/synchro_pg." + librarySuffix:    {},
		"sharedir/extension/synchro_pg.control":    {},
		"sharedir/extension/synchro_pg--0.3.0.sql": {},
	}
}

func readArtifactDigest(path, description string) (string, error) {
	data, err := readRegularFile(path, 1024)
	if err != nil {
		return "", fmt.Errorf("%s cannot be read", description)
	}
	fields := strings.Fields(string(data))
	if len(fields) != 1 || !validSHA256(fields[0]) {
		return "", fmt.Errorf("%s is invalid", description)
	}
	return fields[0], nil
}

func sameExtensionBundleIdentity(left, right extensionBundle) bool {
	if left.root != right.root || left.manifestSHA256 != right.manifestSHA256 ||
		left.manifestDigestSHA256 != right.manifestDigestSHA256 ||
		!os.SameFile(left.rootInfo, right.rootInfo) ||
		!os.SameFile(left.manifestInfo, right.manifestInfo) ||
		!os.SameFile(left.manifestDigestInfo, right.manifestDigestInfo) ||
		len(left.files) != len(right.files) {
		return false
	}
	for index := range left.files {
		leftFile := left.files[index]
		rightFile := right.files[index]
		if leftFile.Path != rightFile.Path || leftFile.Destination != rightFile.Destination ||
			leftFile.SHA256 != rightFile.SHA256 || !os.SameFile(leftFile.sourceInfo, rightFile.sourceInfo) {
			return false
		}
	}
	return true
}

func sameAdapterArtifactIdentity(left, right adapterArtifactIdentity) bool {
	return left.path != "" && left.path == right.path &&
		left.digestPath == right.digestPath && left.sha256 == right.sha256 &&
		left.digestSHA256 == right.digestSHA256 &&
		left.executableInfo != nil && right.executableInfo != nil &&
		left.digestInfo != nil && right.digestInfo != nil &&
		os.SameFile(left.executableInfo, right.executableInfo) &&
		os.SameFile(left.digestInfo, right.digestInfo)
}

func verifyEnvironmentArtifactIdentity(environment EnvironmentConfig) error {
	if environment.ExtensionArtifact != "" {
		extension, err := verifyExtensionBundleForPostgreSQLVersion(environment.ExtensionArtifact, environment.postgresVersion)
		if err != nil || !sameExtensionBundleIdentity(environment.extension, extension) {
			return errors.New("candidate extension artifact identity changed after execution")
		}
	}
	adapter, err := loadAdapterArtifactIdentity(environment.AdapterArtifact)
	if err != nil || !sameAdapterArtifactIdentity(environment.adapterIdentity, adapter) {
		return errors.New("candidate adapter artifact identity changed after execution")
	}
	return nil
}

func decodeStrictManifest(data []byte, destination *extensionBundleManifest) error {
	if err := jsonstrict.ValidateValue(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("trailing manifest JSON")
	}
	return nil
}

func readRegularFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errors.New("file is too large")
	}
	return data, nil
}

func safeBundleRelativePath(value string) bool {
	if value == "" || filepath.IsAbs(value) || strings.Contains(value, `\`) {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(value))
	return clean != "." && clean != ".." && !strings.HasPrefix(clean, "../") && clean == value
}

func safeBundleDestination(value string) bool {
	if !safeBundleRelativePath(value) {
		return false
	}
	if strings.HasPrefix(value, "pkglibdir/") {
		return strings.TrimPrefix(value, "pkglibdir/") != ""
	}
	if strings.HasPrefix(value, "sharedir/") {
		return strings.TrimPrefix(value, "sharedir/") != ""
	}
	return false
}

func safeBundleSourcePath(root, relative string) (string, error) {
	if !safeBundleRelativePath(relative) {
		return "", errors.New("unsafe relative path")
	}
	current := root
	for _, component := range strings.Split(filepath.FromSlash(relative), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("unsafe bundle path")
		}
	}
	info, err := os.Lstat(current)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("bundle file is invalid")
	}
	return current, nil
}

func fileSHA256(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("file is not safe")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && value == hex.EncodeToString(decoded)
}

func verifyInstallationLock(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("SYNCHRO_CONFORMANCE_INSTALL_LOCK path is invalid")
	}
	parent := filepath.Dir(absolute)
	parentInfo, err := os.Lstat(parent)
	if err != nil || parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return "", errors.New("SYNCHRO_CONFORMANCE_INSTALL_LOCK parent is invalid")
	}
	if info, err := os.Lstat(absolute); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return "", errors.New("SYNCHRO_CONFORMANCE_INSTALL_LOCK must be a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", errors.New("SYNCHRO_CONFORMANCE_INSTALL_LOCK is unavailable")
	}
	return absolute, nil
}
