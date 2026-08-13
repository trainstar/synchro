package blackbox

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultStartupTimeout  = 45 * time.Second
	defaultShutdownTimeout = 10 * time.Second
	defaultProcessLogBytes = 1 << 20
	maximumProcessLogBytes = 4 << 20
	processPollInterval    = 50 * time.Millisecond
)

//go:embed testdata/schema.sql
var diagnosticSchemaSQL string

//go:embed testdata/register-diagnostic-v2.sql
var diagnosticRegistrationSQL string

var diagnosticSourceTables = []string{
	"cf_global_items",
	"cf_items",
	"cf_documents",
	"cf_document_members",
	"cf_document_notes",
	"cf_schema_queue",
	"cf_decode_trap",
	"cf_late_registration",
}

var diagnosticLegacyInternalTables = []string{
	"sync_registry",
	"sync_changelog",
	"sync_clients",
	"sync_shared_scopes",
	"sync_bucket_edges",
	"sync_rule_failures",
	"sync_schema_manifest",
	"sync_client_checkpoints",
	"sync_runtime_state",
}

// HarnessConfig controls one isolated PostgreSQL and adapter run.
type HarnessConfig struct {
	Environment     EnvironmentConfig
	TempParent      string
	StartupTimeout  time.Duration
	ShutdownTimeout time.Duration
	ProcessLogBytes int
}

// HarnessNames are the nonsecret isolated PostgreSQL object names.
type HarnessNames struct {
	Database        string
	ReplicationSlot string
	Publication     string
}

// Harness owns all resources created for one black-box diagnostic run.
type Harness struct {
	config HarnessConfig
	env    EnvironmentConfig
	names  HarnessNames

	runRoot     string
	dataDir     string
	socketDir   string
	port        int
	adapterPort int
	adapterURL  string
	sourceRole  string

	lock      *installationLock
	installed *installedExtension
	postgres  *ownedProcess
	adapter   *ownedProcess

	databaseCreated    bool
	rolesCreated       bool
	slotCreated        bool
	publicationCreated bool
	sourceReady        bool
	restartCount       int

	closeMu      sync.Mutex
	closeDone    chan struct{}
	closeErr     error
	closeStarted bool
}

// SourceExecutor permits source-table DML through one restricted NOLOGIN role.
type SourceExecutor struct {
	harness *Harness
}

// OperatorExecutor exposes only the fixed administrative controls used by diagnostics.
type OperatorExecutor struct {
	harness *Harness
}

// SourceTransaction owns one source-DML transaction.
type SourceTransaction struct {
	database *sql.DB
	tx       *sql.Tx
	mu       sync.Mutex
	done     bool
}

// Provision creates an isolated PostgreSQL 18 cluster and a loopback adapter.
func Provision(ctx context.Context, config HarnessConfig) (_ *Harness, returnedErr error) {
	if ctx == nil {
		return nil, errors.New("harness context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, err := normalizeHarnessConfig(config)
	if err != nil {
		return nil, err
	}
	names, err := newHarnessNames()
	if err != nil {
		return nil, errors.New("create isolated PostgreSQL names failed")
	}
	harness := &Harness{config: config, env: config.Environment, names: names}
	harness.sourceRole = "synchro_source_" + strings.TrimPrefix(names.Database, "synchro_conformance_")
	lock, err := acquireInstallationLock(ctx, config.Environment.InstallationLock)
	if err != nil {
		return nil, err
	}
	harness.lock = lock
	defer func() {
		if returnedErr == nil {
			return
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		if cleanupErr := harness.Close(cleanupContext); cleanupErr != nil {
			returnedErr = errors.Join(returnedErr, cleanupErr)
		}
	}()

	if err := harness.createRunDirectories(); err != nil {
		return nil, err
	}
	if err := harness.installExtension(ctx); err != nil {
		return nil, err
	}
	if err := harness.initializeCluster(ctx); err != nil {
		return nil, err
	}
	if err := harness.writePostmasterConfiguration(); err != nil {
		return nil, err
	}
	if err := harness.startPostgres(ctx); err != nil {
		return nil, err
	}
	if err := harness.createRolesAndDatabase(ctx); err != nil {
		return nil, err
	}
	if err := harness.installExtensionTopology(ctx); err != nil {
		return nil, err
	}
	if err := harness.applyIndependentSourceSetup(ctx); err != nil {
		return nil, err
	}
	if err := harness.restartPostgres(ctx); err != nil {
		return nil, err
	}
	if err := harness.verifyPostmasterSettings(ctx); err != nil {
		return nil, err
	}
	if err := harness.waitForWorker(ctx); err != nil {
		return nil, err
	}
	if err := harness.grantRunRoles(ctx); err != nil {
		return nil, err
	}
	if err := harness.startAdapter(ctx); err != nil {
		return nil, err
	}
	return harness, nil
}

func normalizeHarnessConfig(config HarnessConfig) (HarnessConfig, error) {
	if !config.Environment.verified || config.Environment.PG18BinDir == "" || config.Environment.extension.root == "" {
		return HarnessConfig{}, errors.New("verified harness environment is required")
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = defaultStartupTimeout
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaultShutdownTimeout
	}
	if config.ProcessLogBytes == 0 {
		config.ProcessLogBytes = defaultProcessLogBytes
	}
	if config.StartupTimeout <= 0 || config.ShutdownTimeout <= 0 || config.ProcessLogBytes < 1 || config.ProcessLogBytes > maximumProcessLogBytes {
		return HarnessConfig{}, errors.New("harness configuration is invalid")
	}
	if config.TempParent != "" {
		parent, err := filepath.Abs(config.TempParent)
		if err != nil {
			return HarnessConfig{}, errors.New("harness temporary parent is invalid")
		}
		info, err := os.Lstat(parent)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return HarnessConfig{}, errors.New("harness temporary parent is invalid")
		}
		config.TempParent = parent
	}
	return config, nil
}

func newHarnessNames() (HarnessNames, error) {
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return HarnessNames{}, err
	}
	value := hex.EncodeToString(nonce[:])
	return HarnessNames{
		Database:        "synchro_conformance_" + value,
		ReplicationSlot: "synchro_cf_" + value,
		Publication:     "synchro_cf_" + value,
	}, nil
}

func (h *Harness) createRunDirectories() error {
	root, err := os.MkdirTemp(h.config.TempParent, "synchro-conformance-")
	if err != nil {
		return errors.New("create isolated harness directory failed")
	}
	h.runRoot = root
	h.dataDir = filepath.Join(root, "postgres")
	h.socketDir = filepath.Join(root, "socket")
	if err := os.Mkdir(h.socketDir, 0o700); err != nil {
		return errors.New("create private PostgreSQL socket directory failed")
	}
	port, err := allocateLoopbackPort()
	if err != nil {
		return errors.New("allocate PostgreSQL loopback port failed")
	}
	adapterPort, err := allocateLoopbackPort()
	if err != nil {
		return errors.New("allocate adapter loopback port failed")
	}
	h.port = port
	h.adapterPort = adapterPort
	h.adapterURL = "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(adapterPort))
	return nil
}

func allocateLoopbackPort() (int, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address.Port < 1 {
		return 0, errors.New("dynamic loopback address is invalid")
	}
	return address.Port, nil
}

func (h *Harness) installExtension(ctx context.Context) error {
	bundle, err := verifyExtensionBundle(h.env.ExtensionArtifact)
	if err != nil {
		return err
	}
	if !sameExtensionBundleIdentity(h.env.extension, bundle) {
		return errors.New("extension bundle identity changed after environment load")
	}
	roots, err := h.extensionDestinationRoots(ctx)
	if err != nil {
		return err
	}
	backupRoot := filepath.Join(h.runRoot, "extension-backups")
	if err := os.Mkdir(backupRoot, 0o700); err != nil {
		return errors.New("create extension backup directory failed")
	}
	installed := &installedExtension{backupRoot: backupRoot}
	h.installed = installed
	for index, file := range bundle.files {
		source, err := safeBundleSourcePath(bundle.root, file.Path)
		if err != nil {
			return errors.New("extension bundle changed during installation")
		}
		sourceInfo, err := os.Lstat(source)
		if err != nil || !os.SameFile(sourceInfo, file.sourceInfo) {
			return errors.New("extension bundle identity changed during installation")
		}
		destination, err := extensionDestination(roots, file.Destination)
		if err != nil {
			return errors.New("extension bundle destination is invalid")
		}
		record := installedExtensionFile{destination: destination, installedDigest: file.SHA256}
		if info, statErr := os.Lstat(destination); statErr == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return errors.New("extension destination is unsafe")
			}
			backup := filepath.Join(backupRoot, fmt.Sprintf("%03d.original", index))
			digest, err := copyVerifiedFile(destination, backup, "", info.Mode().Perm())
			if err != nil {
				return errors.New("back up installed extension file failed")
			}
			record.hadOriginal = true
			record.originalPath = backup
			record.originalDigest = digest
			record.originalMode = info.Mode().Perm()
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return errors.New("inspect extension destination failed")
		}
		installed.files = append(installed.files, record)
		recordIndex := len(installed.files) - 1
		info, err := os.Stat(source)
		if err != nil {
			return errors.New("extension bundle changed during installation")
		}
		if err := installVerifiedExtensionFile(&installed.files[recordIndex], source, info.Mode().Perm(), syncDirectory); err != nil {
			return errors.New("install extension file failed")
		}
	}
	verifiedAfterInstall, err := verifyExtensionBundle(h.env.ExtensionArtifact)
	if err != nil || !sameExtensionBundleIdentity(bundle, verifiedAfterInstall) {
		return errors.New("extension bundle identity changed during installation")
	}
	return nil
}

type extensionDestinationRoots struct {
	pkglibdir string
	sharedir  string
}

func (h *Harness) extensionDestinationRoots(ctx context.Context) (extensionDestinationRoots, error) {
	pkglibdir, err := pgConfigValue(ctx, filepath.Join(h.env.PG18BinDir, "pg_config"), "--pkglibdir")
	if err != nil {
		return extensionDestinationRoots{}, errors.New("resolve PostgreSQL library destination failed")
	}
	sharedir, err := pgConfigValue(ctx, filepath.Join(h.env.PG18BinDir, "pg_config"), "--sharedir")
	if err != nil {
		return extensionDestinationRoots{}, errors.New("resolve PostgreSQL shared destination failed")
	}
	for _, path := range []string{pkglibdir, sharedir} {
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			return extensionDestinationRoots{}, errors.New("PostgreSQL extension destination is invalid")
		}
	}
	return extensionDestinationRoots{pkglibdir: pkglibdir, sharedir: sharedir}, nil
}

func pgConfigValue(ctx context.Context, executable, argument string) (string, error) {
	commandContext, cancel := context.WithTimeout(ctx, environmentCommandTimeout)
	defer cancel()
	command := exec.CommandContext(commandContext, executable, argument)
	output, err := command.Output()
	if err != nil || commandContext.Err() != nil {
		return "", errors.New("pg_config failed")
	}
	value := strings.TrimSpace(string(output))
	if value == "" || !filepath.IsAbs(value) {
		return "", errors.New("pg_config result is invalid")
	}
	return value, nil
}

func extensionDestination(roots extensionDestinationRoots, destination string) (string, error) {
	var base string
	var relative string
	switch {
	case strings.HasPrefix(destination, "pkglibdir/"):
		base = roots.pkglibdir
		relative = strings.TrimPrefix(destination, "pkglibdir/")
	case strings.HasPrefix(destination, "sharedir/"):
		base = roots.sharedir
		relative = strings.TrimPrefix(destination, "sharedir/")
	default:
		return "", errors.New("unknown destination root")
	}
	if relative == "" || !safeBundleRelativePath(relative) {
		return "", errors.New("unsafe extension destination")
	}
	path := filepath.Join(base, filepath.FromSlash(relative))
	if !withinPath(path, base) {
		return "", errors.New("extension destination escapes root")
	}
	parent := filepath.Dir(path)
	info, err := os.Stat(parent)
	if err != nil || !info.IsDir() {
		return "", errors.New("extension destination parent is invalid")
	}
	return path, nil
}

func withinPath(path, root string) bool {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(absRoot, absPath)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func copyVerifiedFile(source, destination, expectedDigest string, mode os.FileMode) (string, error) {
	digest, _, err := copyVerifiedFileWithSync(source, destination, expectedDigest, mode, syncDirectory)
	return digest, err
}

func installVerifiedExtensionFile(record *installedExtensionFile, source string, mode os.FileMode, syncParent func(string) error) error {
	if record == nil || syncParent == nil {
		return errors.New("extension installation record is invalid")
	}
	_, replaced, err := copyVerifiedFileWithSync(source, record.destination, record.installedDigest, mode, syncParent)
	if replaced {
		record.installed = true
	}
	return err
}

func copyVerifiedFileWithSync(source, destination, expectedDigest string, mode os.FileMode, syncParent func(string) error) (string, bool, error) {
	if syncParent == nil {
		return "", false, errors.New("directory synchronization is required")
	}
	info, err := os.Lstat(source)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", false, errors.New("source file is unsafe")
	}
	input, err := os.Open(source)
	if err != nil {
		return "", false, err
	}
	defer input.Close()
	parent := filepath.Dir(destination)
	temporary, err := os.CreateTemp(parent, ".synchro-conformance-")
	if err != nil {
		return "", false, err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode & 0o777); err != nil {
		_ = temporary.Close()
		return "", false, err
	}
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(temporary, hash), input); err != nil {
		_ = temporary.Close()
		return "", false, err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if expectedDigest != "" && digest != expectedDigest {
		_ = temporary.Close()
		return "", false, errors.New("source digest changed")
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return "", false, err
	}
	if err := temporary.Close(); err != nil {
		return "", false, err
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		return "", false, err
	}
	removeTemporary = false
	if actual, err := fileSHA256(destination); err != nil || actual != digest {
		return "", true, errors.New("destination digest changed")
	}
	if err := syncParent(parent); err != nil {
		return "", true, err
	}
	return digest, true, nil
}

type installedExtension struct {
	backupRoot string
	files      []installedExtensionFile
}

type installedExtensionFile struct {
	destination     string
	installedDigest string
	hadOriginal     bool
	originalPath    string
	originalDigest  string
	originalMode    os.FileMode
	installed       bool
}

func (extension *installedExtension) restore() error {
	if extension == nil {
		return nil
	}
	var failures []error
	for index := len(extension.files) - 1; index >= 0; index-- {
		file := extension.files[index]
		if !file.installed {
			continue
		}
		actual, err := fileSHA256(file.destination)
		if err != nil || actual != file.installedDigest {
			failures = append(failures, errors.New("extension restoration refused changed destination"))
			continue
		}
		if file.hadOriginal {
			if _, err := copyVerifiedFile(file.originalPath, file.destination, file.originalDigest, file.originalMode); err != nil {
				failures = append(failures, errors.New("restore extension file failed"))
			}
			continue
		}
		if err := os.Remove(file.destination); err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = append(failures, errors.New("remove installed extension file failed"))
			continue
		}
		if err := syncDirectory(filepath.Dir(file.destination)); err != nil {
			failures = append(failures, errors.New("sync extension restoration failed"))
		}
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (h *Harness) initializeCluster(ctx context.Context) error {
	passwordFile := filepath.Join(h.runRoot, "initdb-password")
	if err := os.WriteFile(passwordFile, h.env.Admin.password, 0o600); err != nil {
		return errors.New("write PostgreSQL initialization credential failed")
	}
	removePasswordFile := true
	defer func() {
		if removePasswordFile {
			_ = os.Remove(passwordFile)
		}
	}()
	arguments := []string{
		"-D", h.dataDir,
		"--username=" + h.env.Admin.Username,
		"--pwfile=" + passwordFile,
		"--auth-local=trust",
		"--auth-host=scram-sha-256",
		"--encoding=UTF8",
		"--no-instructions",
	}
	if err := runBoundedCommand(ctx, filepath.Join(h.env.PG18BinDir, "initdb"), arguments, nil, h.config.ProcessLogBytes, [][]byte{h.env.Admin.password}); err != nil {
		return errors.New("initialize PostgreSQL cluster failed")
	}
	if err := os.Remove(passwordFile); err != nil {
		return errors.New("remove PostgreSQL initialization credential failed")
	}
	removePasswordFile = false
	return nil
}

func (h *Harness) writePostmasterConfiguration() error {
	configuration := strings.Join([]string{
		"listen_addresses = '127.0.0.1'",
		"port = " + strconv.Itoa(h.port),
		"unix_socket_directories = " + quotePostgresLiteral(h.socketDir),
		"wal_level = logical",
		"max_replication_slots = 1",
		"max_wal_senders = 1",
		"shared_preload_libraries = 'synchro_pg'",
		"synchro.auto_start = on",
		"synchro.database = " + quotePostgresLiteral(h.names.Database),
		"synchro.replication_slot = " + quotePostgresLiteral(h.names.ReplicationSlot),
		"synchro.publication_name = " + quotePostgresLiteral(h.names.Publication),
		"fsync = on",
		"synchronous_commit = on",
		"",
	}, "\n")
	file, err := os.OpenFile(filepath.Join(h.dataDir, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return errors.New("open PostgreSQL configuration failed")
	}
	if _, err := file.WriteString("\n# Synchro conformance isolated settings\n" + configuration); err != nil {
		_ = file.Close()
		return errors.New("write PostgreSQL configuration failed")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.New("sync PostgreSQL configuration failed")
	}
	if err := file.Close(); err != nil {
		return errors.New("close PostgreSQL configuration failed")
	}
	return nil
}

func quotePostgresLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func (h *Harness) startPostgres(ctx context.Context) error {
	process, err := startOwnedProcess(
		filepath.Join(h.env.PG18BinDir, "postgres"),
		[]string{"-D", h.dataDir},
		nil,
		h.config.ProcessLogBytes,
		[][]byte{h.env.Admin.password, h.env.Adapter.password, h.env.Observer.password, h.env.jwtSecret},
	)
	if err != nil {
		return errors.New("start PostgreSQL failed")
	}
	h.postgres = process
	if err := h.waitForPostgres(ctx, "postgres"); err != nil {
		return fmt.Errorf("wait for PostgreSQL readiness: %w", err)
	}
	return nil
}

func (h *Harness) waitForPostgres(ctx context.Context, database string) error {
	deadline, cancel := context.WithTimeout(ctx, h.config.StartupTimeout)
	defer cancel()
	return waitUntil(deadline, func(attemptContext context.Context) (bool, error) {
		if h.postgres != nil && h.postgres.Exited() {
			return false, errors.New("PostgreSQL exited before readiness")
		}
		if !h.pgIsReady(attemptContext, database) {
			return false, nil
		}
		databaseHandle, err := h.openDatabase(attemptContext, database, h.env.Admin, false)
		if err != nil {
			return false, nil
		}
		defer databaseHandle.Close()
		if err := databaseHandle.PingContext(attemptContext); err != nil {
			return false, nil
		}
		return true, nil
	})
}

func (h *Harness) pgIsReady(ctx context.Context, database string) bool {
	commandContext, cancel := context.WithTimeout(ctx, environmentCommandTimeout)
	defer cancel()
	command := exec.CommandContext(
		commandContext,
		filepath.Join(h.env.PG18BinDir, "pg_isready"),
		"-h", "127.0.0.1",
		"-p", strconv.Itoa(h.port),
		"-d", database,
		"-U", h.env.Admin.Username,
	)
	return command.Run() == nil && commandContext.Err() == nil
}

func waitUntil(ctx context.Context, condition func(context.Context) (bool, error)) error {
	if ctx == nil {
		return errors.New("wait context is required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return errors.New("bounded readiness wait expired")
		}
		ready, err := condition(ctx)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		timer := time.NewTimer(processPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return errors.New("bounded readiness wait expired")
		case <-timer.C:
		}
	}
}

func (h *Harness) createRolesAndDatabase(ctx context.Context) error {
	database, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return errors.New("connect PostgreSQL administrator failed")
	}
	defer database.Close()
	if err := execRolePassword(ctx, database, "ALTER ROLE "+quoteIdentifier(h.env.Admin.Username)+" WITH LOGIN SUPERUSER PASSWORD $1", h.env.Admin.password); err != nil {
		return errors.New("configure PostgreSQL administrator failed")
	}
	for _, role := range []RoleCredential{h.env.Adapter, h.env.Observer} {
		statement := "CREATE ROLE " + quoteIdentifier(role.Username) + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD $1"
		if err := execRolePassword(ctx, database, statement, role.password); err != nil {
			return errors.New("create isolated PostgreSQL role failed")
		}
	}
	if _, err := database.ExecContext(ctx, "CREATE ROLE "+quoteIdentifier(h.sourceRole)+" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"); err != nil {
		return errors.New("create isolated source role failed")
	}
	h.rolesCreated = true
	statement := "CREATE DATABASE " + quoteIdentifier(h.names.Database) + " OWNER " + quoteIdentifier(h.env.Admin.Username)
	if _, err := database.ExecContext(ctx, statement); err != nil {
		return errors.New("create isolated PostgreSQL database failed")
	}
	h.databaseCreated = true
	return nil
}

func execRolePassword(ctx context.Context, database *sql.DB, statement string, password []byte) error {
	if strings.Count(statement, "$1") != 1 || len(password) == 0 {
		return errors.New("role password statement is invalid")
	}
	statement = strings.Replace(statement, "$1", quotePostgresLiteral(string(password)), 1)
	_, err := database.ExecContext(ctx, statement)
	return err
}

func (h *Harness) installExtensionTopology(ctx context.Context) error {
	database, err := h.openDatabase(ctx, h.names.Database, h.env.Admin, false)
	if err != nil {
		return errors.New("connect isolated PostgreSQL database failed")
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "CREATE EXTENSION synchro_pg"); err != nil {
		return errors.New("install synchro_pg extension failed")
	}
	var slotName string
	if err := database.QueryRowContext(ctx, "SELECT slot_name FROM pg_create_logical_replication_slot($1, 'pgoutput')", h.names.ReplicationSlot).Scan(&slotName); err != nil || slotName != h.names.ReplicationSlot {
		return errors.New("create isolated replication slot failed")
	}
	h.slotCreated = true
	if _, err := database.ExecContext(ctx, "CREATE PUBLICATION "+quoteIdentifier(h.names.Publication)); err != nil {
		return errors.New("create isolated publication failed")
	}
	h.publicationCreated = true
	return nil
}

func (h *Harness) restartPostgres(ctx context.Context) error {
	stopContext, cancel := context.WithTimeout(context.Background(), processCleanupStageTimeout(h.config.ShutdownTimeout))
	defer cancel()
	if err := h.stopPostgres(stopContext); err != nil {
		return err
	}
	if err := h.startPostgres(ctx); err != nil {
		return err
	}
	h.restartCount++
	return nil
}

func (h *Harness) verifyPostmasterSettings(ctx context.Context) error {
	database, err := h.openDatabase(ctx, h.names.Database, h.env.Admin, false)
	if err != nil {
		return errors.New("connect for PostgreSQL setting verification failed")
	}
	defer database.Close()
	expected := map[string]string{
		"listen_addresses":         "127.0.0.1",
		"port":                     strconv.Itoa(h.port),
		"unix_socket_directories":  h.socketDir,
		"wal_level":                "logical",
		"shared_preload_libraries": "synchro_pg",
		"synchro.auto_start":       "on",
		"synchro.database":         h.names.Database,
		"synchro.replication_slot": h.names.ReplicationSlot,
		"synchro.publication_name": h.names.Publication,
		"fsync":                    "on",
		"synchronous_commit":       "on",
	}
	for setting, wanted := range expected {
		var actual string
		if err := database.QueryRowContext(ctx, "SELECT current_setting($1)", setting).Scan(&actual); err != nil || actual != wanted {
			return errors.New("PostgreSQL setting verification failed")
		}
	}
	for _, setting := range []string{"max_replication_slots", "max_wal_senders"} {
		var actual string
		if err := database.QueryRowContext(ctx, "SELECT current_setting($1)", setting).Scan(&actual); err != nil {
			return errors.New("PostgreSQL setting verification failed")
		}
		value, err := strconv.Atoi(actual)
		if err != nil || value < 1 {
			return errors.New("PostgreSQL setting verification failed")
		}
	}
	return nil
}

func (h *Harness) waitForWorker(ctx context.Context) error {
	deadline, cancel := context.WithTimeout(ctx, h.config.StartupTimeout)
	defer cancel()
	err := waitUntil(deadline, func(attemptContext context.Context) (bool, error) {
		database, err := h.openDatabase(attemptContext, h.names.Database, h.env.Admin, false)
		if err != nil {
			return false, nil
		}
		defer database.Close()
		var present bool
		err = database.QueryRowContext(attemptContext, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_catalog.pg_stat_activity
				WHERE datname = $1 AND backend_type = 'synchro WAL consumer'
			)`, h.names.Database).Scan(&present)
		if err != nil {
			return false, nil
		}
		return present, nil
	})
	if err != nil {
		return fmt.Errorf("wait for synchro WAL consumer: %w", err)
	}
	return nil
}

func (h *Harness) applyIndependentSourceSetup(ctx context.Context) error {
	if err := h.executeSourceScript(ctx, "schema.sql", diagnosticSchemaSQL); err != nil {
		return err
	}
	if err := h.executeSourceScript(ctx, "register-diagnostic-v2.sql", diagnosticRegistrationSQL); err != nil {
		return err
	}
	h.sourceReady = true
	return nil
}

func (h *Harness) executeSourceScript(ctx context.Context, name, body string) error {
	path := filepath.Join(h.runRoot, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		return errors.New("write independent source setup failed")
	}
	arguments := []string{
		"-X",
		"-v", "ON_ERROR_STOP=1",
		"-h", h.socketDir,
		"-p", strconv.Itoa(h.port),
		"-U", h.env.Admin.Username,
		"-d", h.names.Database,
		"-f", path,
	}
	if err := runBoundedCommand(ctx, filepath.Join(h.env.PG18BinDir, "psql"), arguments, scrubPostgresEnvironment(os.Environ()), h.config.ProcessLogBytes, nil); err != nil {
		return errors.New("apply independent source setup failed")
	}
	return nil
}

func (h *Harness) grantRunRoles(ctx context.Context) error {
	database, err := h.openDatabase(ctx, h.names.Database, h.env.Admin, false)
	if err != nil {
		return errors.New("connect for isolated role grants failed")
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "REVOKE CONNECT ON DATABASE "+quoteIdentifier(h.names.Database)+" FROM PUBLIC"); err != nil {
		return errors.New("revoke public database access failed")
	}
	if _, err := database.ExecContext(ctx, "REVOKE TEMPORARY ON DATABASE "+quoteIdentifier(h.names.Database)+" FROM PUBLIC"); err != nil {
		return errors.New("revoke public temporary-object access failed")
	}
	if _, err := database.ExecContext(ctx, "REVOKE CREATE ON SCHEMA public FROM PUBLIC"); err != nil {
		return errors.New("revoke public schema creation failed")
	}
	for _, statement := range []string{
		"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC",
		"REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC",
		"REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM PUBLIC",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			return errors.New("revoke public PostgreSQL privileges failed")
		}
	}
	for _, role := range []RoleCredential{h.env.Adapter, h.env.Observer} {
		if _, err := database.ExecContext(ctx, "GRANT CONNECT ON DATABASE "+quoteIdentifier(h.names.Database)+" TO "+quoteIdentifier(role.Username)); err != nil {
			return errors.New("grant isolated database access failed")
		}
		if _, err := database.ExecContext(ctx, "GRANT USAGE ON SCHEMA public TO "+quoteIdentifier(role.Username)); err != nil {
			return errors.New("grant isolated schema access failed")
		}
	}
	if _, err := database.ExecContext(ctx, "GRANT USAGE ON SCHEMA public TO "+quoteIdentifier(h.sourceRole)); err != nil {
		return errors.New("grant source schema access failed")
	}
	for _, table := range diagnosticSourceTables {
		if _, err := database.ExecContext(ctx, "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public."+quoteIdentifier(table)+" TO "+quoteIdentifier(h.sourceRole)); err != nil {
			return errors.New("grant source-table access failed")
		}
		if _, err := database.ExecContext(ctx, "GRANT SELECT ON TABLE public."+quoteIdentifier(table)+" TO "+quoteIdentifier(h.env.Observer.Username)); err != nil {
			return errors.New("grant observer source-table access failed")
		}
	}
	for _, signature := range []string{
		"synchro_contract_info()",
		"synchro_connect(text,jsonb)",
		"synchro_pull(text,jsonb)",
		"synchro_push(text,jsonb)",
		"synchro_rebuild(text,jsonb)",
		"synchro_schema_manifest()",
		"synchro_tables()",
		"synchro_debug(text,text)",
	} {
		if _, err := database.ExecContext(ctx, "ALTER FUNCTION "+signature+" SECURITY DEFINER"); err != nil {
			return errors.New("secure adapter function failed")
		}
		if _, err := database.ExecContext(ctx, "ALTER FUNCTION "+signature+" SET search_path TO pg_catalog, public"); err != nil {
			return errors.New("set adapter function search path failed")
		}
		if _, err := database.ExecContext(ctx, "GRANT EXECUTE ON FUNCTION "+signature+" TO "+quoteIdentifier(h.env.Adapter.Username)); err != nil {
			return errors.New("grant adapter function access failed")
		}
	}
	if err := h.verifyRunRoleSeparation(ctx, database); err != nil {
		return err
	}
	return nil
}

func (h *Harness) verifyRunRoleSeparation(ctx context.Context, database *sql.DB) error {
	for _, role := range []string{h.sourceRole, h.env.Adapter.Username, h.env.Observer.Username} {
		var restricted bool
		if err := database.QueryRowContext(ctx, `
			SELECT NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls
			FROM pg_catalog.pg_roles WHERE rolname = $1`, role).Scan(&restricted); err != nil || !restricted {
			return errors.New("isolated PostgreSQL role is not restricted")
		}
	}
	for _, table := range diagnosticLegacyInternalTables {
		for _, role := range []string{h.sourceRole, h.env.Observer.Username} {
			var directAccess bool
			if err := database.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, 'SELECT,INSERT,UPDATE,DELETE')", role, "public."+table).Scan(&directAccess); err != nil || directAccess {
				return errors.New("isolated role can access extension-internal tables")
			}
		}
	}
	for _, table := range diagnosticSourceTables {
		var sourceWrite, observerRead, observerWrite, adapterAccess bool
		if err := database.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, 'INSERT,UPDATE,DELETE')", h.sourceRole, "public."+table).Scan(&sourceWrite); err != nil || !sourceWrite {
			return errors.New("source role lacks source-table write access")
		}
		if err := database.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, 'SELECT')", h.env.Observer.Username, "public."+table).Scan(&observerRead); err != nil || !observerRead {
			return errors.New("observer role lacks source-table read access")
		}
		if err := database.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, 'INSERT,UPDATE,DELETE')", h.env.Observer.Username, "public."+table).Scan(&observerWrite); err != nil || observerWrite {
			return errors.New("observer role can write source tables")
		}
		if err := database.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, 'SELECT,INSERT,UPDATE,DELETE')", h.env.Adapter.Username, "public."+table).Scan(&adapterAccess); err != nil || adapterAccess {
			return errors.New("adapter role can access source tables directly")
		}
	}
	return nil
}

func (h *Harness) startAdapter(ctx context.Context) error {
	if err := verifyExecutable(h.env.AdapterArtifact); err != nil {
		return errors.New("adapter artifact changed before execution")
	}
	if digest, err := fileSHA256(h.env.AdapterArtifact); err != nil || digest != h.env.adapterSHA256 {
		return errors.New("adapter artifact hash changed before execution")
	}
	databaseURL := h.databaseURL(h.env.Adapter)
	environment := scrubPostgresEnvironment(os.Environ())
	environment = append(environment,
		"DATABASE_URL="+databaseURL,
		"LISTEN_ADDR="+net.JoinHostPort("127.0.0.1", strconv.Itoa(h.adapterPort)),
		"JWT_SECRET="+string(h.env.jwtSecret),
		"LOG_LEVEL=error",
	)
	process, err := startOwnedProcess(
		h.env.AdapterArtifact,
		nil,
		environment,
		h.config.ProcessLogBytes,
		[][]byte{[]byte(databaseURL), h.env.Adapter.password, h.env.jwtSecret},
	)
	if err != nil {
		return errors.New("start adapter failed")
	}
	h.adapter = process
	deadline, cancel := context.WithTimeout(ctx, h.config.StartupTimeout)
	defer cancel()
	lastStatus := 0
	if err := waitUntil(deadline, func(attemptContext context.Context) (bool, error) {
		if h.adapter.Exited() {
			return false, errors.New("adapter exited before readiness")
		}
		request, err := http.NewRequestWithContext(attemptContext, http.MethodGet, h.adapterURL+"/sync/schema", nil)
		if err != nil {
			return false, errors.New("create adapter readiness request failed")
		}
		client := &http.Client{Timeout: environmentCommandTimeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		}}
		response, err := client.Do(request)
		if err != nil {
			return false, nil
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		closeErr := response.Body.Close()
		if closeErr != nil {
			return false, nil
		}
		lastStatus = response.StatusCode
		return response.StatusCode == http.StatusOK, nil
	}); err != nil {
		diagnostic := h.adapter.diagnosticText()
		if lastStatus != 0 {
			if diagnostic != "" {
				return fmt.Errorf("adapter readiness failed with HTTP %d: %s", lastStatus, diagnostic)
			}
			return fmt.Errorf("adapter readiness failed with HTTP %d", lastStatus)
		}
		if diagnostic != "" {
			return fmt.Errorf("adapter readiness failed without an HTTP response: %s", diagnostic)
		}
		return errors.New("adapter readiness failed without an HTTP response")
	}
	return nil
}

func scrubPostgresEnvironment(source []string) []string {
	blocked := map[string]struct{}{
		"DATABASE_URL": {}, "LISTEN_ADDR": {}, "JWT_SECRET": {}, "JWKS_URL": {},
		"JWT_USER_CLAIM": {}, "MIN_CLIENT_VERSION": {}, "LOG_LEVEL": {},
		"PGHOST": {}, "PGPORT": {}, "PGUSER": {}, "PGPASSWORD": {}, "PGDATABASE": {},
	}
	result := make([]string, 0, len(source))
	for _, entry := range source {
		name, _, found := strings.Cut(entry, "=")
		if found {
			if _, blockedEntry := blocked[name]; blockedEntry {
				continue
			}
		}
		result = append(result, entry)
	}
	return result
}

func (h *Harness) databaseURL(role RoleCredential) string {
	return postgresDSN("127.0.0.1", h.port, h.names.Database, role, true)
}

func (h *Harness) openDatabase(ctx context.Context, database string, role RoleCredential, withPassword bool) (*sql.DB, error) {
	if ctx == nil {
		return nil, errors.New("database context is required")
	}
	host := h.socketDir
	if withPassword {
		host = "127.0.0.1"
	}
	databaseHandle, err := sql.Open("pgx", postgresDSN(host, h.port, database, role, withPassword))
	if err != nil {
		return nil, err
	}
	databaseHandle.SetMaxOpenConns(4)
	databaseHandle.SetMaxIdleConns(1)
	return databaseHandle, nil
}

func postgresDSN(host string, port int, database string, role RoleCredential, withPassword bool) string {
	parts := []string{
		"host=" + quoteDSNValue(host),
		"port=" + strconv.Itoa(port),
		"user=" + quoteDSNValue(role.Username),
		"dbname=" + quoteDSNValue(database),
		"sslmode=disable",
	}
	if withPassword {
		parts = append(parts, "password="+quoteDSNValue(string(role.password)))
	}
	return strings.Join(parts, " ")
}

func quoteDSNValue(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "'", `\'`) + "'"
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

// Names returns a copy of the isolated nonsecret PostgreSQL object names.
func (h *Harness) Names() HarnessNames {
	if h == nil {
		return HarnessNames{}
	}
	return h.names
}

// AdapterURL returns the dynamic loopback adapter origin.
func (h *Harness) AdapterURL() string {
	if h == nil {
		return ""
	}
	return h.adapterURL
}

// RestartCount reports the required post-extension PostgreSQL restart count.
func (h *Harness) RestartCount() int {
	if h == nil {
		return 0
	}
	return h.restartCount
}

// FailureDiagnostics returns bounded, sanitized process output for a failed run.
func (h *Harness) FailureDiagnostics() string {
	if h == nil {
		return ""
	}
	var diagnostics []string
	if text := h.postgres.diagnosticText(); text != "" {
		diagnostics = append(diagnostics, "postgres: "+text)
	}
	if text := h.adapter.diagnosticText(); text != "" {
		diagnostics = append(diagnostics, "adapter: "+text)
	}
	return strings.Join(diagnostics, " | ")
}

// Source returns a source-DML-only executor for this isolated run.
func (h *Harness) Source() *SourceExecutor {
	if h == nil || !h.sourceReady {
		return nil
	}
	return &SourceExecutor{harness: h}
}

// Operator returns the fixed administrative controls for this isolated run.
func (h *Harness) Operator() *OperatorExecutor {
	if h == nil || !h.sourceReady {
		return nil
	}
	return &OperatorExecutor{harness: h}
}

// OpenObserver opens a PostgreSQL connection as the read-only observer role.
func (h *Harness) OpenObserver(ctx context.Context) (*sql.DB, error) {
	if h == nil || !h.sourceReady {
		return nil, errors.New("isolated harness is not ready")
	}
	database, err := h.openDatabase(ctx, h.names.Database, h.env.Observer, true)
	if err != nil {
		return nil, errors.New("open observer connection failed")
	}
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return nil, errors.New("connect observer role failed")
	}
	return database, nil
}

// DiagnosticBearerToken signs one short-lived token for the isolated adapter.
func (h *Harness) DiagnosticBearerToken(now time.Time) (string, error) {
	if h == nil || !h.sourceReady || len(h.env.jwtSecret) == 0 {
		return "", errors.New("isolated harness is not ready")
	}
	issued := now.Round(0).UTC()
	return SignHS256(h.env.jwtSecret, Claims{
		"sub": "diagnostic-user",
		"iat": issued.Unix(),
		"exp": issued.Add(time.Hour).Unix(),
	})
}

// ExecContext applies one checked source-table mutation as the restricted source role.
func (executor *SourceExecutor) ExecContext(ctx context.Context, statement string, arguments ...any) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return errors.New("source executor is unavailable")
	}
	if err := validateSourceDML(statement); err != nil {
		return err
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return errors.New("open source mutation connection failed")
	}
	defer database.Close()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin source mutation failed")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+quoteIdentifier(executor.harness.sourceRole)); err != nil {
		return errors.New("activate source role failed")
	}
	if _, err := tx.ExecContext(ctx, statement, arguments...); err != nil {
		return errors.New("source mutation failed")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit source mutation failed")
	}
	return nil
}

// BeginTx begins one checked source-table mutation transaction.
func (executor *SourceExecutor) BeginTx(ctx context.Context) (*SourceTransaction, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return nil, errors.New("source executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return nil, errors.New("open source transaction connection failed")
	}
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		_ = database.Close()
		return nil, errors.New("begin source transaction failed")
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+quoteIdentifier(executor.harness.sourceRole)); err != nil {
		_ = tx.Rollback()
		_ = database.Close()
		return nil, errors.New("activate source transaction role failed")
	}
	return &SourceTransaction{database: database, tx: tx}, nil
}

// CommitInReverseBeginOrder commits the second source transaction before the first.
func (executor *SourceExecutor) CommitInReverseBeginOrder(ctx context.Context, firstStatement string, firstArguments []any, secondStatement string, secondArguments []any) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return errors.New("source executor is unavailable")
	}
	if err := validateSourceDML(firstStatement); err != nil {
		return err
	}
	if err := validateSourceDML(secondStatement); err != nil {
		return err
	}
	prepared := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		transaction, err := executor.BeginTx(ctx)
		if err != nil {
			firstDone <- err
			return
		}
		if _, err := transaction.ExecContext(ctx, firstStatement, firstArguments...); err != nil {
			_ = transaction.Rollback()
			firstDone <- err
			return
		}
		close(prepared)
		select {
		case <-releaseFirst:
		case <-ctx.Done():
			_ = transaction.Rollback()
			firstDone <- ctx.Err()
			return
		}
		firstDone <- transaction.Commit()
	}()
	select {
	case <-prepared:
	case err := <-firstDone:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
	second, err := executor.BeginTx(ctx)
	if err != nil {
		close(releaseFirst)
		<-firstDone
		return err
	}
	if _, err := second.ExecContext(ctx, secondStatement, secondArguments...); err != nil {
		_ = second.Rollback()
		close(releaseFirst)
		<-firstDone
		return err
	}
	if err := second.Commit(); err != nil {
		close(releaseFirst)
		<-firstDone
		return err
	}
	close(releaseFirst)
	select {
	case err := <-firstDone:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ExecContext applies one checked source-table mutation inside the transaction.
func (transaction *SourceTransaction) ExecContext(ctx context.Context, statement string, arguments ...any) (sql.Result, error) {
	if transaction == nil || transaction.tx == nil {
		return nil, errors.New("source transaction is unavailable")
	}
	if err := validateSourceDML(statement); err != nil {
		return nil, err
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.done {
		return nil, errors.New("source transaction is complete")
	}
	result, err := transaction.tx.ExecContext(ctx, statement, arguments...)
	if err != nil {
		return nil, errors.New("source transaction mutation failed")
	}
	return result, nil
}

// Commit commits the source transaction and closes its database handle.
func (transaction *SourceTransaction) Commit() error {
	return transaction.complete(true)
}

// Rollback rolls back the source transaction and closes its database handle.
func (transaction *SourceTransaction) Rollback() error {
	return transaction.complete(false)
}

func (transaction *SourceTransaction) complete(commit bool) error {
	if transaction == nil || transaction.tx == nil || transaction.database == nil {
		return errors.New("source transaction is unavailable")
	}
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.done {
		return errors.New("source transaction is complete")
	}
	transaction.done = true
	var err error
	if commit {
		err = transaction.tx.Commit()
	} else {
		err = transaction.tx.Rollback()
	}
	closeErr := transaction.database.Close()
	if err != nil || closeErr != nil {
		return errors.New("complete source transaction failed")
	}
	return nil
}

// DropHydrationColumn removes the fixed diagnostic column.
func (executor *OperatorExecutor) DropHydrationColumn(ctx context.Context) error {
	return executor.exec(ctx, "ALTER TABLE cf_schema_queue DROP COLUMN legacy_value")
}

// RestoreHydrationColumn restores the fixed diagnostic column.
func (executor *OperatorExecutor) RestoreHydrationColumn(ctx context.Context) error {
	return executor.exec(ctx, "ALTER TABLE cf_schema_queue ADD COLUMN legacy_value TEXT NOT NULL DEFAULT 'restored'")
}

// RegisterSchemaQueue refreshes the fixed schema-queue registration.
func (executor *OperatorExecutor) RegisterSchemaQueue(ctx context.Context) error {
	return executor.exec(ctx, `SELECT synchro_register_table(
        'cf_schema_queue',
        $$SELECT ARRAY['user:' || owner_id] FROM cf_schema_queue WHERE id = $1::uuid$$,
        'id', 'updated_at', 'deleted_at', 'enabled'
    )`)
}

// ConfigureDecodeTrap selects the fixed diagnostic primary key.
func (executor *OperatorExecutor) ConfigureDecodeTrap(ctx context.Context, primaryKey string) error {
	if primaryKey != "id" && primaryKey != "deleted_at" {
		return errors.New("diagnostic decode control is invalid")
	}
	if err := executor.exec(ctx, `SELECT synchro_register_table(
        'cf_decode_trap',
        $$SELECT ARRAY['user:' || owner_id] FROM cf_decode_trap WHERE id = $1::uuid$$,
        $1, 'updated_at', 'deleted_at', 'enabled'
    )`, primaryKey); err != nil {
		return err
	}
	return executor.ReloadRegistry(ctx)
}

// RegisterLateSourceTable registers the fixed late-registration table.
func (executor *OperatorExecutor) RegisterLateSourceTable(ctx context.Context) error {
	return executor.exec(ctx, `SELECT synchro_register_table(
        'cf_late_registration',
        $$SELECT ARRAY['user:' || owner_id] FROM cf_late_registration WHERE id = $1::uuid$$,
        'id', 'updated_at', 'deleted_at', 'enabled'
    )`)
}

// UnregisterLateSourceTable unregisters the fixed late-registration table.
func (executor *OperatorExecutor) UnregisterLateSourceTable(ctx context.Context) error {
	return executor.exec(ctx, "SELECT synchro_unregister_table('cf_late_registration')")
}

// ConfigureCrossScopeTable installs the fixed cross-scope diagnostic registration.
func (executor *OperatorExecutor) ConfigureCrossScopeTable(ctx context.Context) error {
	for _, statement := range []string{
		"SELECT synchro_register_shared_scope('cf:dedup', false)",
		`SELECT synchro_register_table(
            'cf_items',
            $$SELECT ARRAY['cf:dedup', 'user:' || owner_id] FROM cf_items WHERE id = $1::uuid$$,
            'id', 'updated_at', 'deleted_at', 'enabled'
        )`,
	} {
		if err := executor.exec(ctx, statement); err != nil {
			return errors.New("configure cross-scope diagnostic failed")
		}
	}
	return executor.ReloadRegistry(ctx)
}

// RestoreCrossScopeTable restores the fixed single-scope registration.
func (executor *OperatorExecutor) RestoreCrossScopeTable(ctx context.Context) error {
	for _, statement := range []string{
		`SELECT synchro_register_table(
            'cf_items',
            $$SELECT ARRAY['user:' || owner_id] FROM cf_items WHERE id = $1::uuid$$,
            'id', 'updated_at', 'deleted_at', 'enabled'
        )`,
		"SELECT synchro_unregister_shared_scope('cf:dedup')",
	} {
		if err := executor.exec(ctx, statement); err != nil {
			return errors.New("restore cross-scope diagnostic failed")
		}
	}
	return executor.ReloadRegistry(ctx)
}

// ReloadRegistry requests one PostgreSQL configuration reload.
func (executor *OperatorExecutor) ReloadRegistry(ctx context.Context) error {
	return executor.exec(ctx, "SELECT pg_reload_conf()")
}

// CompactPositiveInterval runs compaction with a strict positive near-zero interval.
func (executor *OperatorExecutor) CompactPositiveInterval(ctx context.Context) ([]byte, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return nil, errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return nil, errors.New("open operator connection failed")
	}
	defer database.Close()
	var raw []byte
	if err := database.QueryRowContext(ctx, "SELECT synchro_compact('1 microsecond', 1)").Scan(&raw); err != nil {
		return nil, errors.New("diagnostic compaction control failed")
	}
	return append([]byte(nil), raw...), nil
}

func (executor *OperatorExecutor) exec(ctx context.Context, statement string, arguments ...any) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return errors.New("open operator connection failed")
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, statement, arguments...); err != nil {
		return errors.New("diagnostic operator control failed")
	}
	return nil
}

func validateSourceDML(statement string) error {
	normalized := strings.ToLower(strings.TrimSpace(statement))
	if normalized == "" || strings.Contains(normalized, "sync_") || strings.Contains(normalized, ";") || strings.Contains(normalized, "--") || strings.Contains(normalized, "/*") {
		return errors.New("source mutation statement is invalid")
	}
	fields := strings.Fields(normalized)
	var table string
	switch {
	case strings.HasPrefix(normalized, "insert into cf_"):
		if len(fields) >= 3 {
			table = strings.TrimSuffix(fields[2], "(")
		}
	case strings.HasPrefix(normalized, "update cf_"):
		if len(fields) >= 2 {
			table = strings.TrimSuffix(fields[1], "(")
		}
	case strings.HasPrefix(normalized, "delete from cf_"):
		if len(fields) >= 3 {
			table = strings.TrimSuffix(fields[2], "(")
		}
	}
	allowed := false
	for _, candidate := range diagnosticSourceTables {
		if table == candidate {
			allowed = true
			break
		}
	}
	if !allowed {
		return errors.New("source mutation must target an independent source table")
	}
	return nil
}

// Close stops all owned processes and restores installed extension files.
// It returns every cleanup failure after attempting each reverse-order step.
func (h *Harness) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("cleanup context is required")
	}
	h.closeMu.Lock()
	if h.closeStarted {
		done := h.closeDone
		h.closeMu.Unlock()
		select {
		case <-done:
			h.closeMu.Lock()
			defer h.closeMu.Unlock()
			return h.closeErr
		case <-ctx.Done():
			return errors.New("bounded cleanup wait expired")
		}
	}
	h.closeStarted = true
	h.closeDone = make(chan struct{})
	h.closeMu.Unlock()

	err := h.cleanup(ctx)
	h.closeMu.Lock()
	h.closeErr = err
	close(h.closeDone)
	h.closeMu.Unlock()
	return err
}

func (h *Harness) cleanup(ctx context.Context) error {
	var failures []error
	if err := runCleanupStage(ctx, processCleanupStageTimeout(h.config.ShutdownTimeout), h.stopAdapter); err != nil {
		failures = append(failures, err)
	}
	if err := h.dropRunTopology(ctx); err != nil {
		failures = append(failures, err)
	}
	postmasterStopped := true
	if err := runCleanupStage(ctx, processCleanupStageTimeout(h.config.ShutdownTimeout), h.stopPostgres); err != nil {
		postmasterStopped = false
		failures = append(failures, err)
	}
	if postmasterStopped {
		if err := h.removeCluster(); err != nil {
			failures = append(failures, err)
		}
	} else {
		failures = append(failures, errors.New("remove isolated PostgreSQL cluster refused while postmaster remains"))
	}
	restored := h.installed == nil
	if postmasterStopped {
		if h.installed != nil {
			if err := h.installed.restore(); err != nil {
				failures = append(failures, err)
			} else {
				restored = true
			}
		}
	} else if h.installed != nil {
		failures = append(failures, errors.New("restore extension files refused while postmaster remains"))
	}
	if postmasterStopped && (restored || h.installed == nil) {
		if err := h.removeRunRoot(); err != nil {
			failures = append(failures, err)
		}
	}
	if h.lock != nil {
		if !postmasterStopped || !restored {
			failures = append(failures, errors.New("installation lock retained until PostgreSQL stops and extension restoration succeeds"))
		} else if err := h.lock.Release(); err != nil {
			failures = append(failures, err)
		} else {
			h.lock = nil
		}
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	return nil
}

func (h *Harness) stopAdapter(ctx context.Context) error {
	if h.adapter == nil {
		return nil
	}
	if err := h.adapter.Stop(ctx, h.config.ShutdownTimeout); err != nil {
		return errors.New("stop adapter process failed")
	}
	h.adapter = nil
	return nil
}

func (h *Harness) dropRunTopology(ctx context.Context) error {
	if h.postgres == nil || h.postgres.Exited() {
		if h.databaseCreated || h.rolesCreated || h.slotCreated || h.publicationCreated {
			return errors.New("PostgreSQL stopped before topology cleanup")
		}
		return nil
	}
	var failures []error
	if h.databaseCreated {
		if err := runCleanupStage(ctx, h.config.ShutdownTimeout, h.terminateRunConnections); err != nil {
			failures = append(failures, err)
		}
	}
	if h.publicationCreated {
		if err := runCleanupStage(ctx, h.config.ShutdownTimeout, h.dropPublication); err != nil {
			failures = append(failures, err)
		}
	}
	if h.slotCreated {
		if err := runCleanupStage(ctx, h.config.ShutdownTimeout, h.dropReplicationSlot); err != nil {
			failures = append(failures, err)
		}
	}
	if h.databaseCreated {
		if err := runCleanupStage(ctx, h.config.ShutdownTimeout, h.dropDatabase); err != nil {
			failures = append(failures, err)
		}
	}
	if h.rolesCreated {
		if err := runCleanupStage(ctx, h.config.ShutdownTimeout, h.dropRoles); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	return nil
}

func runCleanupStage(parent context.Context, timeout time.Duration, operation func(context.Context) error) error {
	if parent == nil || timeout <= 0 || operation == nil {
		return errors.New("cleanup stage configuration is invalid")
	}
	stageContext, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()
	return operation(stageContext)
}

func processCleanupStageTimeout(shutdownTimeout time.Duration) time.Duration {
	return 2*shutdownTimeout + time.Second
}

func (h *Harness) terminateRunConnections(ctx context.Context) error {
	database, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return errors.New("connect for run connection termination failed")
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, `
		SELECT pg_terminate_backend(pid)
		FROM pg_catalog.pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid()`, h.names.Database); err != nil {
		return errors.New("terminate run connections failed")
	}
	return nil
}

func (h *Harness) dropPublication(ctx context.Context) error {
	database, err := h.openDatabase(ctx, h.names.Database, h.env.Admin, false)
	if err != nil {
		return errors.New("connect for publication cleanup failed")
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "DROP PUBLICATION IF EXISTS "+quoteIdentifier(h.names.Publication)); err != nil {
		return errors.New("drop isolated publication failed")
	}
	h.publicationCreated = false
	return nil
}

func (h *Harness) dropReplicationSlot(ctx context.Context) error {
	database, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return errors.New("connect for replication slot cleanup failed")
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", h.names.ReplicationSlot); err != nil {
		return errors.New("drop isolated replication slot failed")
	}
	h.slotCreated = false
	return nil
}

func (h *Harness) dropDatabase(ctx context.Context) error {
	database, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return errors.New("connect for database cleanup failed")
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "DROP DATABASE "+quoteIdentifier(h.names.Database)+" WITH (FORCE)"); err != nil {
		return errors.New("drop isolated PostgreSQL database failed")
	}
	h.databaseCreated = false
	return nil
}

func (h *Harness) dropRoles(ctx context.Context) error {
	database, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return errors.New("connect for role cleanup failed")
	}
	defer database.Close()
	var failures []error
	for _, role := range []string{h.env.Observer.Username, h.env.Adapter.Username, h.sourceRole} {
		if _, err := database.ExecContext(ctx, "DROP ROLE IF EXISTS "+quoteIdentifier(role)); err != nil {
			failures = append(failures, errors.New("drop isolated PostgreSQL role failed"))
		}
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	h.rolesCreated = false
	return nil
}

func (h *Harness) stopPostgres(ctx context.Context) error {
	if h.postgres == nil {
		return nil
	}
	if err := h.postgres.Stop(ctx, h.config.ShutdownTimeout); err != nil {
		return errors.New("stop PostgreSQL process failed")
	}
	h.postgres = nil
	return nil
}

func (h *Harness) removeCluster() error {
	if h.dataDir != "" {
		if err := os.RemoveAll(h.dataDir); err != nil {
			return errors.New("remove isolated PostgreSQL cluster failed")
		}
	}
	if h.socketDir != "" {
		if err := os.RemoveAll(h.socketDir); err != nil {
			return errors.New("remove isolated PostgreSQL socket directory failed")
		}
	}
	return nil
}

func (h *Harness) removeRunRoot() error {
	if h.runRoot == "" {
		return nil
	}
	if err := os.RemoveAll(h.runRoot); err != nil {
		return errors.New("remove isolated harness directory failed")
	}
	h.runRoot = ""
	return nil
}

type installationLock struct {
	file     *os.File
	mu       sync.Mutex
	released bool
}

func acquireInstallationLock(ctx context.Context, path string) (*installationLock, error) {
	if ctx == nil {
		return nil, errors.New("installation lock context is required")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.New("open installation lock failed")
	}
	for {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &installationLock{file: file}, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			_ = file.Close()
			return nil, errors.New("acquire installation lock failed")
		}
		timer := time.NewTimer(processPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			_ = file.Close()
			return nil, errors.New("bounded installation lock wait expired")
		case <-timer.C:
		}
	}
}

func (lock *installationLock) Release() error {
	if lock == nil {
		return nil
	}
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.released {
		return nil
	}
	lock.released = true
	unlockErr := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	closeErr := lock.file.Close()
	if unlockErr != nil || closeErr != nil {
		return errors.New("release installation lock failed")
	}
	return nil
}

type boundedLog struct {
	mu         sync.Mutex
	limit      int
	data       []byte
	truncated  bool
	redactions [][]byte
}

func newBoundedLog(limit int, redactions [][]byte) *boundedLog {
	values := make([][]byte, 0, len(redactions))
	for _, value := range redactions {
		if len(value) != 0 {
			values = append(values, append([]byte(nil), value...))
		}
	}
	return &boundedLog{limit: limit, redactions: values}
}

func (log *boundedLog) Write(data []byte) (int, error) {
	if log == nil {
		return len(data), nil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.data) >= log.limit {
		log.truncated = true
		return len(data), nil
	}
	remaining := log.limit - len(log.data)
	if len(data) > remaining {
		log.data = append(log.data, data[:remaining]...)
		log.truncated = true
		return len(data), nil
	}
	log.data = append(log.data, data...)
	return len(data), nil
}

func (log *boundedLog) sanitizedBytes() []byte {
	if log == nil {
		return nil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	result := append([]byte(nil), log.data...)
	for _, value := range log.redactions {
		result = []byte(strings.ReplaceAll(string(result), string(value), "[REDACTED]"))
	}
	return result
}
func runBoundedCommand(ctx context.Context, executable string, arguments, environment []string, logLimit int, redactions [][]byte) error {
	if ctx == nil {
		return errors.New("command context is required")
	}
	log := newBoundedLog(logLimit, redactions)
	command := exec.CommandContext(ctx, executable, arguments...)
	if environment != nil {
		command.Env = environment
	}
	configureProcessGroup(command)
	command.Stdout = log
	command.Stderr = log
	err := command.Run()
	if err != nil || ctx.Err() != nil {
		_ = log.sanitizedBytes()
		return errors.New("bounded command failed")
	}
	return nil
}

type ownedProcess struct {
	command *exec.Cmd
	cancel  context.CancelFunc
	done    chan struct{}

	mu      sync.Mutex
	waitErr error
	stopMu  sync.Mutex
	log     *boundedLog
}

func startOwnedProcess(executable string, arguments, environment []string, logLimit int, redactions [][]byte) (*ownedProcess, error) {
	processContext, cancel := context.WithCancel(context.Background())
	command := exec.CommandContext(processContext, executable, arguments...)
	if environment != nil {
		command.Env = environment
	}
	configureProcessGroup(command)
	log := newBoundedLog(logLimit, redactions)
	command.Stdout = log
	command.Stderr = log
	if err := command.Start(); err != nil {
		cancel()
		return nil, err
	}
	process := &ownedProcess{command: command, cancel: cancel, done: make(chan struct{}), log: log}
	go func() {
		err := command.Wait()
		process.mu.Lock()
		process.waitErr = err
		process.mu.Unlock()
		close(process.done)
	}()
	return process, nil
}

func configureProcessGroup(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func (process *ownedProcess) Exited() bool {
	if process == nil {
		return true
	}
	select {
	case <-process.done:
		return true
	default:
		return false
	}
}

func (process *ownedProcess) diagnosticText() string {
	if process == nil || process.log == nil {
		return ""
	}
	text := strings.TrimSpace(string(process.log.sanitizedBytes()))
	text = strings.ReplaceAll(text, "\r", " ")
	text = strings.ReplaceAll(text, "\n", " ")
	const maximum = 512
	if len(text) > maximum {
		text = text[:maximum]
	}
	return text
}
func (process *ownedProcess) Stop(ctx context.Context, timeout time.Duration) error {
	if process == nil || process.command == nil || process.command.Process == nil {
		return nil
	}
	if ctx == nil || timeout <= 0 {
		return errors.New("process stop configuration is invalid")
	}
	process.stopMu.Lock()
	defer process.stopMu.Unlock()
	pid := process.command.Process.Pid
	if pid < 1 {
		return errors.New("process identifier is invalid")
	}
	if !processGroupAlive(pid) {
		process.cancel()
		return waitForOwnedProcess(ctx, process.done)
	}
	if err := signalProcessGroup(pid, syscall.SIGTERM); err != nil {
		return errors.New("send process group SIGTERM failed")
	}
	if process.waitForGroupExit(ctx, pid, timeout) {
		process.cancel()
		return waitForOwnedProcess(ctx, process.done)
	}
	if err := signalProcessGroup(pid, syscall.SIGKILL); err != nil {
		return errors.New("send process group SIGKILL failed")
	}
	if !process.waitForGroupExit(ctx, pid, timeout) {
		return errors.New("bounded process group SIGKILL wait expired")
	}
	process.cancel()
	return waitForOwnedProcess(ctx, process.done)
}

func (process *ownedProcess) waitForGroupExit(ctx context.Context, pid int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for processGroupAlive(pid) {
		timer := time.NewTimer(processPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return false
		case <-deadline.C:
			if !timer.Stop() {
				<-timer.C
			}
			return false
		case <-timer.C:
		}
	}
	return true
}

func waitForOwnedProcess(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.New("bounded process wait expired")
	}
}

func signalProcessGroup(pid int, signal syscall.Signal) error {
	err := syscall.Kill(-pid, signal)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func processGroupAlive(pid int) bool {
	err := syscall.Kill(-pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
