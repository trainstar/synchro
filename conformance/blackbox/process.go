package blackbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const (
	defaultStartupTimeout                  = 45 * time.Second
	defaultShutdownTimeout                 = 10 * time.Second
	defaultProcessLogBytes                 = 1 << 20
	maximumProcessLogBytes                 = 4 << 20
	processPollInterval                    = 50 * time.Millisecond
	maxWorkerHeartbeatAge                  = 30
	maxWALLagBytes                         = 64 * 1024 * 1024
	maxWALLagSeconds                       = 30
	streamResetOperatorLockKey       int64 = 0x7273_746f
	streamResetOperationKind               = "stream_reset"
	projectionBootstrapOperationKind       = "projection_bootstrap"
)

//go:embed testdata/schema.sql
var diagnosticSchemaSQL string

//go:embed testdata/register-diagnostic.sql
var diagnosticRegistrationSQL string

var diagnosticUUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

var diagnosticSourceTables = []string{
	"cf_global_items",
	"cf_items",
	"cf_documents",
	"cf_document_members",
	"cf_document_access",
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
	"sync_scope_digest_cache",
	"sync_rule_failures",
	"sync_schema_manifest",
	"sync_client_checkpoints",
	"sync_runtime_state",
}

// HarnessConfig controls one isolated PostgreSQL and adapter run.
type HarnessConfig struct {
	Environment                         EnvironmentConfig
	TempParent                          string
	StartupTimeout                      time.Duration
	ShutdownTimeout                     time.Duration
	ProcessLogBytes                     int
	AllowInitialCaptureReadinessFailure bool
	SkipAdapter                         bool
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
	worker      RoleCredential

	lock      *installationLock
	installed *installedExtension
	postgres  *ownedProcess
	adapter   *ownedProcess

	databaseCreated     bool
	rolesCreated        bool
	slotCreated         bool
	publicationCreated  bool
	sourceReady         bool
	restartCount        int
	workerDetachedState bool

	closeMu      sync.Mutex
	closeDone    chan struct{}
	closeErr     error
	closeStarted bool

	databaseMu      sync.Mutex
	databaseHandles []*sql.DB
}

// SourceExecutor permits source-table DML through one restricted NOLOGIN role.
type SourceExecutor struct {
	harness *Harness
}

// OperatorExecutor exposes only the fixed administrative controls used by diagnostics.
type OperatorExecutor struct {
	harness *Harness
}

// UnregisterDefaultSharedScope removes the default shared assignment from native fixtures.
func (executor *OperatorExecutor) UnregisterDefaultSharedScope(ctx context.Context) error {
	if ctx == nil {
		return errors.New("native shared scope context is required")
	}
	if err := ctx.Err(); err != nil {
		return errors.New("native shared scope context expired")
	}
	if err := executor.exec(ctx, "SELECT synchro.synchro_unregister_shared_scope('cf:global')"); err != nil {
		return errors.New("unregister default native shared scope failed")
	}
	if err := ctx.Err(); err != nil {
		return errors.New("native shared scope context expired")
	}
	return nil
}

// SourceTransaction owns one source-DML transaction.
type SourceTransaction struct {
	database *sql.DB
	tx       *sql.Tx
	mu       sync.Mutex
	done     bool
}

// ProjectionBootstrapBarrierControl creates a deterministic post-baseline catch-up window.
type ProjectionBootstrapBarrierControl struct {
	harness      *Harness
	database     *sql.DB
	tx           *sql.Tx
	acquired     chan error
	mu           sync.Mutex
	queued       bool
	lockAcquired bool
	released     bool
}

// PushOverlapControl holds one fixed source row while concurrent adapter pushes reach its lock.
type PushOverlapControl struct {
	harness  *Harness
	database *sql.DB
	tx       *sql.Tx
	mu       sync.Mutex
	done     bool
}

// ClientCheckpointObservation is one bounded checkpoint row without user-owned data.
type ClientCheckpointObservation struct {
	ScopeID            string
	StreamGeneration   string
	PositionKind       string
	CommitLSN          string
	CommitLSNValid     bool
	EventOrdinal       int64
	EventOrdinalValid  bool
	EffectOrdinal      int32
	EffectOrdinalValid bool
	UpdatedAt          string
}

// ItemStateMatchObservation compares one fixed diagnostic item with expected synthetic state.
type ItemStateMatchObservation struct {
	Live           bool
	ValueMatches   bool
	VersionMatches bool
}

// WALRecordObservation is redacted durable state for one fixed diagnostic row.
type WALRecordObservation struct {
	RecordID      string
	CommitLSN     string
	EndLSN        string
	EventOrdinal  int64
	EffectOrdinal int32
	FenceCoverage string
	RowVersion    string
	ReplayCount   int64
}

// WALPipelineObservation is bounded operational evidence for the WAL pipeline.
type WALPipelineObservation struct {
	Records                           []WALRecordObservation
	WorkerRunning                     bool
	BlockingPoison                    bool
	ContiguousAcknowledged            bool
	AcknowledgedEndLSN                string
	AcknowledgementMatchesObservedEnd bool
	SlotConfirmedFlushLSN             string
	SlotMatchesObservedEnd            bool
}

// WALProgressObservation is bounded durable acknowledgement state.
type WALProgressObservation struct {
	AcknowledgedEndLSN    string
	SlotConfirmedFlushLSN string
	SlotMatchesProgress   bool
}

// WALReplayRestartObservation contains bounded state around one worker restart.
type WALReplayRestartObservation struct {
	PriorProgress                     WALProgressObservation
	BeforeRestart                     WALPipelineObservation
	AfterRestart                      WALPipelineObservation
	BeforeStages                      WALRecordStageObservation
	AfterStages                       WALRecordStageObservation
	WorkerExitedBeforeAcknowledgement bool
	WorkerRestarted                   bool
}

// WALProgressOrderObservation is bounded evidence for one persisted ordering violation.
type WALProgressOrderObservation struct {
	PoisonActive              bool
	FailureClass              string
	RelationIDMatchesRegistry bool
	SourceRowPresent          bool
	ProgressCommitAtOrAhead   bool
	RecordMaterialized        bool
	WorkerBlocked             bool
}

// WALPoisonObservation is bounded evidence for one blocking source transaction.
type WALPoisonObservation struct {
	FailureClass              string
	RelationID                string
	RelationIDMatchesRegistry bool
	CommitLSN                 string
	AcknowledgementBlocked    bool
	LaterRecordMaterialized   bool
	LaterFencePending         bool
	WorkerBlocked             bool
	ReadinessBlocked          bool
	PoisonCheckFailed         bool
	WALLagSeconds             float64
}

// WALPoisonRecoveryObservation is bounded repair evidence for one durable
// decoder poison and its materialized source transaction.
type WALPoisonRecoveryObservation struct {
	PoisonCount        int64
	FailureClass       string
	Lifecycle          string
	AttemptCount       int64
	RetryRequested     bool
	Resolved           bool
	SameCommitPosition bool
}

// StreamResetResult identifies one activated candidate stream.
type StreamResetResult struct {
	ResetID                string
	SourceStreamGeneration string
	TargetStreamGeneration string
	OldSlotName            string
	CandidateSlotName      string
}

// ProjectionBootstrapResult identifies one activated projection bootstrap.
type ProjectionBootstrapResult struct {
	BootstrapID            string   `json:"bootstrap_id"`
	RegistryGeneration     int64    `json:"registry_generation"`
	SourceStreamGeneration string   `json:"source_stream_generation"`
	ActiveSlotName         string   `json:"active_slot_name"`
	CandidateSlotName      string   `json:"candidate_slot_name"`
	SchemaVersion          *int64   `json:"schema_version"`
	SchemaHash             *string  `json:"schema_hash"`
	ActivationBarrier      string   `json:"activation_barrier"`
	AffectedScopes         []string `json:"affected_scopes"`
}

type candidateRecoveryPlan struct {
	AbortFunction   string
	CleanupFunction string
	RetiredSlotName string
	Activated       bool
}

type recoveredCandidateOperation struct {
	OperationKind       string
	StreamReset         *StreamResetResult
	ProjectionBootstrap *ProjectionBootstrapResult
}

type streamResetSnapshotMarker struct {
	XID   string `json:"marker_xid"`
	Nonce string `json:"marker_nonce"`
}

// StreamResetObservation is bounded evidence for an activated reset baseline.
type StreamResetObservation struct {
	Lifecycle                 string
	ActiveSlotName            string
	ActiveStreamGeneration    string
	OldSlotAbsent             bool
	CandidateSlotValid        bool
	PoisonCleared             bool
	BaselineRecordPresent     bool
	BaselineProvenanceMatches bool
	BaselineMembershipPresent bool
	FenceCoverage             string
	NoSyntheticEvent          bool
	NoSyntheticEffect         bool
	CheckpointsInvalidated    bool
	ReadinessReady            bool
	ReadinessFailures         string
}

// ProjectionBootstrapObservation is bounded evidence for one Class 3 activation.
type ProjectionBootstrapObservation struct {
	Lifecycle                     string
	StreamUnchanged               bool
	ActiveSlotUnchanged           bool
	CandidateSlotAbsent           bool
	RegistryActive                bool
	ManifestPublished             bool
	HistoricalRecordPresent       bool
	CatchupRecordPresent          bool
	HistoricalMembershipPresent   bool
	CatchupMembershipPresent      bool
	CatchupFenceCoverage          string
	CatchupFenceProvenanceMatches bool
	NoPendingFences               bool
	StageCleared                  bool
}

// ActiveProjectionBootstrapObservation identifies one durable interrupted Class 3 operation.
type ActiveProjectionBootstrapObservation struct {
	BootstrapID          string
	Lifecycle            string
	CandidateSlotName    string
	CandidateSlotPresent bool
}

// ProjectionBootstrapRecoveryObservation is bounded cleanup evidence for one Class 3 operation.
type ProjectionBootstrapRecoveryObservation struct {
	Lifecycle           string
	CandidateSlotAbsent bool
	StageCleared        bool
}

// MembershipEffectObservation is one redacted effect caused by a dependency event.
type MembershipEffectObservation struct {
	TableID       string
	RecordID      string
	BucketID      string
	Operation     int16
	EventOrdinal  int64
	EffectOrdinal int32
}

// CaptureDependencyObservation is bounded evidence for one capture-only source event.
type CaptureDependencyObservation struct {
	RegistrationKind  string
	TableIDAbsent     bool
	ProjectionOwnerID string
	CurrentOwnerID    string
	FenceCoverage     string
	DirectEffectCount int64
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
	harness := &Harness{
		config: config,
		env:    config.Environment,
		names:  names,
		worker: config.Environment.Worker,
	}
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
		if diagnostic := harness.FailureDiagnostics(); diagnostic != "" {
			returnedErr = fmt.Errorf("%w: %s", returnedErr, diagnostic)
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
	if err := harness.writeHBAConfiguration(); err != nil {
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
	if err := harness.verifyWorkerAuthenticationBoundary(ctx); err != nil {
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
	if err := harness.verifyCaptureReadiness(ctx); err != nil && !config.AllowInitialCaptureReadinessFailure {
		return nil, err
	}
	if err := harness.grantRunRoles(ctx); err != nil {
		return nil, err
	}
	if !config.SkipAdapter {
		if err := harness.startAdapter(ctx); err != nil {
			return nil, err
		}
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

func (h *Harness) writeHBAConfiguration() error {
	configuration := workerHBAConfiguration(h.names.Database, h.worker.Username)
	file, err := os.OpenFile(filepath.Join(h.dataDir, "pg_hba.conf"), os.O_TRUNC|os.O_WRONLY, 0)
	if err != nil {
		return errors.New("open PostgreSQL HBA configuration failed")
	}
	if _, err := file.WriteString(configuration); err != nil {
		_ = file.Close()
		return errors.New("write PostgreSQL HBA configuration failed")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.New("sync PostgreSQL HBA configuration failed")
	}
	if err := file.Close(); err != nil {
		return errors.New("close PostgreSQL HBA configuration failed")
	}
	return nil
}

func workerHBAConfiguration(database, worker string) string {
	database = quoteHBAName(database)
	worker = quoteHBAName(worker)
	return strings.Join([]string{
		"# Synchro conformance authentication boundary",
		"local " + database + " " + worker + " scram-sha-256",
		"local all " + worker + " reject",
		"local all all trust",
		"host " + database + " " + worker + " 127.0.0.1/32 scram-sha-256",
		"host all " + worker + " 127.0.0.1/32 reject",
		"host all all 127.0.0.1/32 scram-sha-256",
		"host all all ::1/128 scram-sha-256",
		"",
	}, "\n")
}

func quoteHBAName(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func (h *Harness) writePostmasterConfiguration() error {
	configuration := strings.Join([]string{
		"listen_addresses = '127.0.0.1'",
		"port = " + strconv.Itoa(h.port),
		"unix_socket_directories = " + quotePostgresLiteral(h.socketDir),
		"wal_level = logical",
		"max_replication_slots = 2",
		"max_wal_senders = 1",
		"shared_preload_libraries = 'synchro_pg'",
		"synchro.auto_start = on",
		"synchro.database = " + quotePostgresLiteral(h.names.Database),
		"synchro.replication_slot = " + quotePostgresLiteral(h.names.ReplicationSlot),
		"synchro.publication_name = " + quotePostgresLiteral(h.names.Publication),
		"synchro.worker_login = " + quotePostgresLiteral(h.worker.Username),
		"synchro.max_worker_heartbeat_age_seconds = " + strconv.Itoa(maxWorkerHeartbeatAge),
		"synchro.max_wal_lag_bytes = " + strconv.Itoa(maxWALLagBytes),
		"synchro.max_wal_lag_seconds = " + strconv.Itoa(maxWALLagSeconds),
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
		[][]byte{h.env.Admin.password, h.env.Adapter.password, h.env.Observer.password, h.worker.password, h.env.Operator.password, h.env.jwtSecret},
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
	for _, role := range []RoleCredential{h.env.Adapter, h.env.Observer, h.env.Operator} {
		statement := "CREATE ROLE " + quoteIdentifier(role.Username) + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD $1"
		if err := execRolePassword(ctx, database, statement, role.password); err != nil {
			return errors.New("create isolated PostgreSQL role failed")
		}
	}
	workerStatement := "CREATE ROLE " + quoteIdentifier(h.worker.Username) + " LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD $1"
	if err := execRolePassword(ctx, database, workerStatement, h.worker.password); err != nil {
		return fmt.Errorf("provision isolated worker role failed: %w", err)
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

func (h *Harness) verifyWorkerAuthenticationBoundary(ctx context.Context) error {
	database, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return errors.New("connect for PostgreSQL HBA verification failed")
	}
	defer database.Close()
	var invalidRules int
	if err := database.QueryRowContext(ctx, "SELECT count(*) FROM pg_catalog.pg_hba_file_rules WHERE error IS NOT NULL").Scan(&invalidRules); err != nil || invalidRules != 0 {
		return errors.New("PostgreSQL HBA configuration is invalid")
	}
	var exactWorkerRule bool
	if err := database.QueryRowContext(ctx, `
		SELECT count(*) = 1
		FROM pg_catalog.pg_hba_file_rules
		WHERE type = 'host'
		  AND $1 = ANY(database)
		  AND $2 = ANY(user_name)
		  AND address = '127.0.0.1'
		  AND netmask = '255.255.255.255'
		  AND auth_method = 'scram-sha-256'`, h.names.Database, h.worker.Username).Scan(&exactWorkerRule); err != nil || !exactWorkerRule {
		return errors.New("PostgreSQL worker HBA rule is invalid")
	}

	workerDatabase, err := h.openDatabase(ctx, h.names.Database, h.worker, true)
	if err != nil {
		return errors.New("open authenticated worker connection failed")
	}
	if err := workerDatabase.PingContext(ctx); err != nil {
		_ = workerDatabase.Close()
		return errors.New("external worker credential authentication failed")
	}
	if err := workerDatabase.Close(); err != nil {
		return errors.New("close authenticated worker connection failed")
	}

	wrongCredential := h.worker
	wrongCredential.password = []byte("invalid-conformance-worker-password")
	if pingDatabase(ctx, h, h.names.Database, wrongCredential, true) == nil {
		return errors.New("PostgreSQL accepted an invalid worker credential")
	}
	if pingDatabase(ctx, h, "postgres", h.worker, true) == nil {
		return errors.New("PostgreSQL worker credential reached an unconfigured database")
	}
	if pingDatabase(ctx, h, h.names.Database, h.worker, false) == nil {
		return errors.New("PostgreSQL worker credential bypassed SCRAM authentication")
	}
	return nil
}

func pingDatabase(ctx context.Context, harness *Harness, database string, role RoleCredential, withPassword bool) error {
	handle, err := harness.openDatabase(ctx, database, role, withPassword)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.PingContext(ctx)
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
	if _, err := database.ExecContext(ctx, "GRANT synchro_adapter TO "+quoteIdentifier(h.env.Adapter.Username)); err != nil {
		return errors.New("grant isolated adapter group failed")
	}
	if _, err := database.ExecContext(ctx, "GRANT synchro_worker TO "+quoteIdentifier(h.worker.Username)); err != nil {
		return errors.New("grant isolated worker group failed")
	}
	if _, err := database.ExecContext(ctx, "GRANT synchro_operator TO "+quoteIdentifier(h.env.Operator.Username)); err != nil {
		return errors.New("grant isolated operator group failed")
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
	if h.postgres == nil {
		return errors.New("PostgreSQL process is unavailable")
	}
	if err := h.postgres.StopPostmasterFast(stopContext, h.config.ShutdownTimeout); err != nil {
		return err
	}
	h.postgres = nil
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
		"listen_addresses":                         "127.0.0.1",
		"port":                                     strconv.Itoa(h.port),
		"unix_socket_directories":                  h.socketDir,
		"wal_level":                                "logical",
		"shared_preload_libraries":                 "synchro_pg",
		"synchro.auto_start":                       "on",
		"synchro.database":                         h.names.Database,
		"synchro.replication_slot":                 h.names.ReplicationSlot,
		"synchro.publication_name":                 h.names.Publication,
		"synchro.worker_login":                     h.worker.Username,
		"synchro.max_worker_heartbeat_age_seconds": strconv.Itoa(maxWorkerHeartbeatAge),
		"synchro.max_wal_lag_bytes":                strconv.Itoa(maxWALLagBytes),
		"synchro.max_wal_lag_seconds":              strconv.Itoa(maxWALLagSeconds),
		"fsync":                                    "on",
		"synchronous_commit":                       "on",
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

func (h *Harness) verifyCaptureReadiness(ctx context.Context) error {
	database, err := h.openDatabase(ctx, h.names.Database, h.env.Admin, false)
	if err != nil {
		return errors.New("connect for capture readiness verification failed")
	}
	defer database.Close()
	deadline, cancel := context.WithTimeout(ctx, h.config.StartupTimeout)
	defer cancel()
	var failedNames string
	var queryErr error
	err = waitUntil(deadline, func(attemptContext context.Context) (bool, error) {
		var ready bool
		var failedChecks int
		queryErr = database.QueryRowContext(attemptContext, `
			SELECT (health->>'ready')::boolean,
			       (SELECT count(*) FROM jsonb_each(health->'checks') entry
			        WHERE entry.value->>'state' <> 'ok'),
			       COALESCE((
			           SELECT string_agg(entry.key, ',' ORDER BY entry.key)
			           FROM jsonb_each(health->'checks') entry
			           WHERE entry.value->>'state' <> 'ok'
			       ), '')
			FROM (SELECT synchro.synchro_health_detail() AS health) state`,
		).Scan(&ready, &failedChecks, &failedNames)
		if queryErr != nil {
			return false, nil
		}
		return ready && failedChecks == 0, nil
	})
	if err != nil {
		if queryErr != nil {
			return fmt.Errorf("capture readiness verification failed: %w", queryErr)
		}
		return fmt.Errorf("capture readiness verification failed: %s", failedNames)
	}
	return nil
}

func (h *Harness) applyIndependentSourceSetup(ctx context.Context) error {
	if err := h.executeSourceScript(ctx, "schema.sql", diagnosticSchemaSQL); err != nil {
		return err
	}
	if err := h.grantWorkerReplicationSourceAccess(ctx); err != nil {
		return err
	}
	if err := h.executeSourceScript(ctx, "register-diagnostic.sql", diagnosticRegistrationSQL); err != nil {
		return err
	}
	h.sourceReady = true
	return nil
}

func (h *Harness) grantWorkerReplicationSourceAccess(ctx context.Context) error {
	database, err := h.openDatabase(ctx, h.names.Database, h.env.Admin, false)
	if err != nil {
		return errors.New("connect for worker replication source grants failed")
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "GRANT USAGE ON SCHEMA public TO "+quoteIdentifier(h.worker.Username)); err != nil {
		return errors.New("grant worker replication schema access failed")
	}
	for _, table := range diagnosticSourceTables {
		if _, err := database.ExecContext(
			ctx,
			"GRANT SELECT ON TABLE public."+quoteIdentifier(table)+" TO "+quoteIdentifier(h.worker.Username),
		); err != nil {
			return errors.New("grant worker replication source-table access failed")
		}
	}
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
		return fmt.Errorf("apply independent source setup failed: %w", err)
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
	for _, role := range []RoleCredential{h.env.Adapter, h.env.Observer, h.worker, h.env.Operator} {
		if _, err := database.ExecContext(ctx, "GRANT CONNECT ON DATABASE "+quoteIdentifier(h.names.Database)+" TO "+quoteIdentifier(role.Username)); err != nil {
			return errors.New("grant isolated database access failed")
		}
	}
	if _, err := database.ExecContext(ctx, "GRANT USAGE ON SCHEMA public TO "+quoteIdentifier(h.env.Observer.Username)); err != nil {
		return errors.New("grant isolated observer schema access failed")
	}
	if _, err := database.ExecContext(ctx, "GRANT USAGE ON SCHEMA public TO "+quoteIdentifier(h.sourceRole)); err != nil {
		return errors.New("grant source schema access failed")
	}
	for _, table := range diagnosticSourceTables {
		if _, err := database.ExecContext(ctx, "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public."+quoteIdentifier(table)+" TO "+quoteIdentifier(h.sourceRole)); err != nil {
			return errors.New("grant source-table access failed")
		}
		if _, err := database.ExecContext(ctx,
			"CREATE POLICY synchro_conformance_source ON public."+quoteIdentifier(table)+
				" AS PERMISSIVE FOR ALL TO "+quoteIdentifier(h.sourceRole)+" USING (true) WITH CHECK (true)",
		); err != nil {
			return errors.New("create source-table row security policy failed")
		}
		if _, err := database.ExecContext(ctx, "GRANT SELECT ON TABLE public."+quoteIdentifier(table)+" TO "+quoteIdentifier(h.env.Observer.Username)); err != nil {
			return errors.New("grant observer source-table access failed")
		}
	}
	if err := h.verifyRunRoleSeparation(ctx, database); err != nil {
		return err
	}
	return nil
}

func (h *Harness) verifyRunRoleSeparation(ctx context.Context, database *sql.DB) error {
	for _, role := range []string{h.sourceRole, h.env.Adapter.Username, h.env.Observer.Username, h.env.Operator.Username} {
		var restricted bool
		if err := database.QueryRowContext(ctx, `
			SELECT NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls
			FROM pg_catalog.pg_roles WHERE rolname = $1`, role).Scan(&restricted); err != nil || !restricted {
			return errors.New("isolated PostgreSQL role is not restricted")
		}
	}
	for _, table := range diagnosticLegacyInternalTables {
		for _, role := range []string{h.sourceRole, h.env.Adapter.Username, h.env.Observer.Username, h.env.Operator.Username} {
			var directAccess bool
			if err := database.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, 'SELECT,INSERT,UPDATE,DELETE')", role, "synchro."+table).Scan(&directAccess); err != nil || directAccess {
				return errors.New("isolated role can access extension-internal tables")
			}
		}
	}
	for role, expectedGroup := range map[string]string{
		h.env.Adapter.Username:  "synchro_adapter",
		h.worker.Username:       "synchro_worker",
		h.env.Operator.Username: "synchro_operator",
	} {
		var exactMembership bool
		if err := database.QueryRowContext(ctx, `
			SELECT count(*) = 1 AND COALESCE(bool_and(granted.rolname = $2), false)
			FROM pg_catalog.pg_auth_members membership
			JOIN pg_catalog.pg_roles granted ON granted.oid = membership.roleid
			JOIN pg_catalog.pg_roles member ON member.oid = membership.member
			WHERE member.rolname = $1`, role, expectedGroup).Scan(&exactMembership); err != nil || !exactMembership {
			return errors.New("isolated runtime role membership is invalid")
		}
	}
	var workerBoundary, operatorLogin bool
	if err := database.QueryRowContext(ctx, `
		SELECT rolreplication AND NOT rolinherit AND rolcanlogin
		FROM pg_catalog.pg_roles WHERE rolname = $1`, h.worker.Username).Scan(&workerBoundary); err != nil || !workerBoundary {
		return errors.New("isolated worker replication boundary is invalid")
	}
	if err := database.QueryRowContext(ctx, `
		SELECT rolcanlogin AND NOT rolsuper AND NOT rolreplication AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolbypassrls
		FROM pg_catalog.pg_roles WHERE rolname = $1`, h.env.Operator.Username).Scan(&operatorLogin); err != nil || !operatorLogin {
		return errors.New("isolated operator login is not restricted")
	}
	var adapterDirectGrant, adapterDebug bool
	if err := database.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_proc procedure
			JOIN pg_catalog.pg_namespace namespace ON namespace.oid = procedure.pronamespace
			CROSS JOIN LATERAL pg_catalog.aclexplode(
				COALESCE(procedure.proacl, pg_catalog.acldefault('f', procedure.proowner))
			) acl
			JOIN pg_catalog.pg_roles grantee ON grantee.oid = acl.grantee
			WHERE namespace.nspname = 'synchro' AND grantee.rolname = $1
		)`, h.env.Adapter.Username).Scan(&adapterDirectGrant); err != nil || adapterDirectGrant {
		return errors.New("isolated adapter has a direct function grant")
	}
	if err := database.QueryRowContext(ctx,
		"SELECT has_function_privilege($1, 'synchro.synchro_debug(text,text)', 'EXECUTE')",
		h.env.Adapter.Username,
	).Scan(&adapterDebug); err != nil || adapterDebug {
		return errors.New("isolated adapter can execute operator debug")
	}
	for _, table := range diagnosticSourceTables {
		var sourceWrite, observerRead, observerWrite, adapterAccess, workerRead, workerWrite bool
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
		if err := database.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, 'SELECT')", h.worker.Username, "public."+table).Scan(&workerRead); err != nil || !workerRead {
			return errors.New("worker replication login lacks source-table read access")
		}
		if err := database.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, 'INSERT,UPDATE,DELETE')", h.worker.Username, "public."+table).Scan(&workerWrite); err != nil || workerWrite {
			return errors.New("worker replication login can write source tables")
		}
	}
	for _, table := range diagnosticSourceTables {
		var operatorAccess bool
		if err := database.QueryRowContext(ctx, "SELECT has_table_privilege($1, $2, 'SELECT,INSERT,UPDATE,DELETE')", h.env.Operator.Username, "public."+table).Scan(&operatorAccess); err != nil || operatorAccess {
			return errors.New("operator role can access source tables directly")
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
		"DATABASE_URL": {}, "WORKER_DATABASE_URL": {}, "LISTEN_ADDR": {}, "JWT_SECRET": {}, "JWKS_URL": {},
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
	h.databaseMu.Lock()
	h.databaseHandles = append(h.databaseHandles, databaseHandle)
	h.databaseMu.Unlock()
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

// DatabaseURL returns the administrator connection string for this isolated run.
func (h *Harness) DatabaseURL() string {
	if h == nil || !h.sourceReady {
		return ""
	}
	return h.databaseURL(h.env.Admin)
}

// RestartCount reports the required post-extension PostgreSQL restart count.
func (h *Harness) RestartCount() int {
	if h == nil {
		return 0
	}
	return h.restartCount
}

// RestartPostgres restarts the isolated postmaster for a process-fault test.
func (h *Harness) RestartPostgres(ctx context.Context) error {
	if h == nil || ctx == nil || !h.sourceReady {
		return errors.New("isolated PostgreSQL restart is unavailable")
	}
	return h.restartPostgres(ctx)
}

// FailureDiagnostics returns bounded, sanitized process output for a failed run.
func (h *Harness) FailureDiagnostics() string {
	if h == nil {
		return ""
	}
	var diagnostics []string
	if text := h.postgres.diagnosticTextMatching(
		"synchro WAL",
		"stream reset",
		"projection bootstrap",
		"rebuild staging snapshot failed",
		"background worker",
		"PANIC:",
		"FATAL:",
		"ERROR:",
	); text != "" {
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
	token, err := SignHS256(h.env.jwtSecret, Claims{
		"sub": "diagnostic-user",
		"iat": issued.Unix(),
		"exp": issued.Add(time.Hour).Unix(),
	})
	if err != nil {
		return "", err
	}
	if h.adapter != nil {
		h.adapter.log.addRedaction([]byte(token))
	}
	if h.postgres != nil {
		h.postgres.log.addRedaction([]byte(token))
	}
	return token, nil
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
		return sourceMutationError("source mutation failed", err)
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
		return nil, sourceMutationError("source transaction mutation failed", err)
	}
	return result, nil
}

func sourceMutationError(message string, err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && len(postgresError.Code) == 5 {
		return fmt.Errorf("%s (SQLSTATE %s)", message, postgresError.Code)
	}
	return errors.New(message)
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

// NewProjectionBootstrapBarrier creates a post-baseline barrier control.
func (executor *OperatorExecutor) NewProjectionBootstrapBarrier() (*ProjectionBootstrapBarrierControl, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return nil, errors.New("operator executor is unavailable")
	}
	return &ProjectionBootstrapBarrierControl{harness: executor.harness}, nil
}

// QueueBarrier queues a reset-state lock behind active baseline staging.
func (control *ProjectionBootstrapBarrierControl) QueueBarrier(ctx context.Context) error {
	if control == nil || control.harness == nil {
		return errors.New("projection bootstrap barrier control is unavailable")
	}
	control.mu.Lock()
	if control.queued || control.released {
		control.mu.Unlock()
		return errors.New("projection bootstrap barrier control state is invalid")
	}
	control.mu.Unlock()

	for attempt := 0; attempt < 4; attempt++ {
		database, tx, acquired, acquiredEarly, err := beginProjectionBootstrapBarrierAttempt(ctx, control.harness)
		if err != nil {
			return err
		}
		if !acquiredEarly {
			control.mu.Lock()
			control.database = database
			control.tx = tx
			control.acquired = acquired
			control.queued = true
			control.mu.Unlock()
			return nil
		}

		if err := waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
			var stageQueued bool
			err := database.QueryRowContext(attemptContext, `
				SELECT EXISTS (
					SELECT 1 FROM pg_catalog.pg_locks
					WHERE relation = 'synchro.sync_stream_resets'::regclass
					  AND mode = 'ShareRowExclusiveLock'
					  AND NOT granted
				)`).Scan(&stageQueued)
			if err != nil {
				return false, errors.New("observe queued projection bootstrap stage failed")
			}
			return stageQueued, nil
		}); err != nil {
			_ = tx.Rollback()
			_ = database.Close()
			return errors.New("projection bootstrap stage did not queue behind the barrier")
		}
		if err := tx.Rollback(); err != nil {
			_ = database.Close()
			return errors.New("release early projection bootstrap barrier failed")
		}
		if err := database.Close(); err != nil {
			return errors.New("release early projection bootstrap barrier failed")
		}
	}
	return errors.New("projection bootstrap reset barrier did not queue")
}

func beginProjectionBootstrapBarrierAttempt(
	ctx context.Context,
	harness *Harness,
) (*sql.DB, *sql.Tx, chan error, bool, error) {
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return nil, nil, nil, false, errors.New("open projection bootstrap queued barrier failed")
	}
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		_ = database.Close()
		return nil, nil, nil, false, errors.New("begin projection bootstrap queued barrier failed")
	}
	var backendPID int
	if err := tx.QueryRowContext(ctx, "SELECT pg_catalog.pg_backend_pid()").Scan(&backendPID); err != nil {
		_ = tx.Rollback()
		_ = database.Close()
		return nil, nil, nil, false, errors.New("identify projection bootstrap queued barrier failed")
	}
	acquired := make(chan error, 1)
	go func() {
		_, lockErr := tx.ExecContext(ctx, "LOCK TABLE synchro.sync_stream_resets IN SHARE MODE")
		acquired <- lockErr
	}()
	for {
		select {
		case lockErr := <-acquired:
			if lockErr != nil {
				_ = tx.Rollback()
				_ = database.Close()
				return nil, nil, nil, false, errors.New("queue projection bootstrap barrier failed")
			}
			return database, tx, acquired, true, nil
		default:
		}
		var queued bool
		if err := database.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_catalog.pg_locks
				WHERE pid = $1
				  AND relation = 'synchro.sync_stream_resets'::regclass
				  AND mode = 'ShareLock'
				  AND NOT granted
			)`, backendPID).Scan(&queued); err != nil {
			_ = tx.Rollback()
			_ = database.Close()
			return nil, nil, nil, false, errors.New("observe queued projection bootstrap barrier failed")
		}
		if queued {
			return database, tx, acquired, false, nil
		}
		timer := time.NewTimer(processPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			_ = tx.Rollback()
			_ = database.Close()
			return nil, nil, nil, false, errors.New("queue projection bootstrap barrier timed out")
		case <-timer.C:
		}
	}
}

// WaitForBarrier waits until baseline staging commits and the queued lock is held.
func (control *ProjectionBootstrapBarrierControl) WaitForBarrier(ctx context.Context) error {
	if control == nil {
		return errors.New("projection bootstrap barrier control is unavailable")
	}
	control.mu.Lock()
	if control.lockAcquired {
		control.mu.Unlock()
		return nil
	}
	if !control.queued || control.released {
		control.mu.Unlock()
		return errors.New("projection bootstrap barrier control state is invalid")
	}
	acquired := control.acquired
	control.mu.Unlock()

	select {
	case err := <-acquired:
		if err != nil {
			return errors.New("acquire projection bootstrap queued barrier failed")
		}
		control.mu.Lock()
		control.lockAcquired = true
		control.mu.Unlock()
		return nil
	case <-ctx.Done():
		return errors.New("acquire projection bootstrap queued barrier timed out")
	}
}

// ReleaseBarrier lets the production coordinator emit its activation barrier.
func (control *ProjectionBootstrapBarrierControl) ReleaseBarrier() error {
	if control == nil {
		return errors.New("projection bootstrap barrier control is unavailable")
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.released {
		return nil
	}
	if !control.lockAcquired || control.tx == nil || control.database == nil {
		return errors.New("projection bootstrap barrier control state is invalid")
	}
	control.released = true
	if err := control.tx.Rollback(); err != nil {
		_ = control.database.Close()
		return errors.New("release projection bootstrap queued barrier failed")
	}
	if err := control.database.Close(); err != nil {
		return errors.New("release projection bootstrap queued barrier failed")
	}
	return nil
}

// Close releases any barrier resources that remain after a failed control flow.
func (control *ProjectionBootstrapBarrierControl) Close() error {
	if control == nil {
		return nil
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.released || control.tx == nil {
		return nil
	}
	control.released = true
	var failures []error
	if err := control.tx.Rollback(); err != nil {
		if !errors.Is(err, sql.ErrTxDone) {
			failures = append(failures, errors.New("close projection bootstrap queued barrier failed"))
		}
	}
	if control.database != nil {
		if err := control.database.Close(); err != nil {
			failures = append(failures, errors.New("close projection bootstrap queued barrier failed"))
		}
	}
	return errors.Join(failures...)
}

// WaitForCandidateOperationRelease waits for PostgreSQL to release a terminated coordinator session lock.
func (executor *OperatorExecutor) WaitForCandidateOperationRelease(ctx context.Context) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return errors.New("open candidate operation lock observation failed")
	}
	defer database.Close()
	return waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		tx, err := database.BeginTx(attemptContext, nil)
		if err != nil {
			return false, errors.New("begin candidate operation lock observation failed")
		}
		var acquired bool
		queryErr := tx.QueryRowContext(
			attemptContext,
			"SELECT pg_catalog.pg_try_advisory_xact_lock($1::bigint)",
			int64(0x7273746f),
		).Scan(&acquired)
		rollbackErr := tx.Rollback()
		if queryErr != nil || rollbackErr != nil {
			return false, errors.New("observe candidate operation lock failed")
		}
		return acquired, nil
	})
}

// HoldItemForConcurrentPush holds one live diagnostic item while adapter pushes block.
func (executor *OperatorExecutor) HoldItemForConcurrentPush(ctx context.Context, recordID string) (*PushOverlapControl, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return nil, errors.New("operator executor is unavailable")
	}
	if ctx == nil || !diagnosticUUIDPattern.MatchString(recordID) {
		return nil, errors.New("push overlap control input is invalid")
	}
	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return nil, errors.New("open push overlap control connection failed")
	}
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		_ = database.Close()
		return nil, errors.New("begin push overlap control failed")
	}
	var lockedID string
	if err := tx.QueryRowContext(ctx, `
		SELECT id::text
		FROM public.cf_items
		WHERE id = $1::uuid AND deleted_at IS NULL
		FOR UPDATE`, recordID).Scan(&lockedID); err != nil || lockedID != recordID {
		_ = tx.Rollback()
		_ = database.Close()
		return nil, errors.New("lock push overlap control row failed")
	}
	return &PushOverlapControl{harness: harness, database: database, tx: tx}, nil
}

// WaitForBlockedPushes waits until the fixed number of adapter pushes is lock-blocked.
func (control *PushOverlapControl) WaitForBlockedPushes(ctx context.Context, count int) error {
	if control == nil || ctx == nil || count < 1 || count > 2 {
		return errors.New("push overlap observation input is invalid")
	}
	control.mu.Lock()
	if control.done || control.harness == nil {
		control.mu.Unlock()
		return errors.New("push overlap control is unavailable")
	}
	harness := control.harness
	control.mu.Unlock()
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return errors.New("open push overlap observation connection failed")
	}
	defer database.Close()
	if err := waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		var blocked int
		if err := database.QueryRowContext(attemptContext, `
			SELECT count(*)
			FROM pg_catalog.pg_stat_activity
			WHERE datname = $1
			  AND usename = $2
			  AND backend_type = 'client backend'
			  AND state = 'active'
			  AND wait_event_type = 'Lock'
			  AND query LIKE 'SELECT synchro.synchro_push(%'`,
			harness.names.Database,
			harness.env.Adapter.Username,
		).Scan(&blocked); err != nil {
			return false, nil
		}
		return blocked == count, nil
	}); err != nil {
		return errors.New("concurrent push overlap was not observed")
	}
	return nil
}

// Release releases the fixed source-row lock without changing source data.
func (control *PushOverlapControl) Release() error {
	if control == nil {
		return nil
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.done {
		return nil
	}
	control.done = true
	var failures []error
	if control.tx != nil {
		if err := control.tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			failures = append(failures, errors.New("release push overlap row lock failed"))
		}
	}
	if control.database != nil {
		if err := control.database.Close(); err != nil {
			failures = append(failures, errors.New("close push overlap control connection failed"))
		}
	}
	return errors.Join(failures...)
}

// DropHydrationColumn removes the fixed diagnostic column.
func (executor *OperatorExecutor) DropHydrationColumn(ctx context.Context) error {
	return executor.exec(ctx, "ALTER TABLE public.cf_schema_queue DROP COLUMN legacy_value")
}

// RestoreHydrationColumn restores the fixed diagnostic column.
func (executor *OperatorExecutor) RestoreHydrationColumn(ctx context.Context) error {
	return executor.exec(ctx, "ALTER TABLE public.cf_schema_queue ADD COLUMN legacy_value TEXT NOT NULL DEFAULT 'restored'")
}

// RegisterSchemaQueue refreshes the fixed schema-queue registration.
func (executor *OperatorExecutor) RegisterSchemaQueue(ctx context.Context) error {
	return executor.exec(ctx, `SELECT synchro.synchro_register_table(
        'public.cf_schema_queue',
		'public.cf_schema_queue_membership',
        'single_scope',
        'id', 'updated_at', 'deleted_at', 'enabled'
    )`)
}

// ConfigureDecodeTrap selects the fixed diagnostic primary key.
func (executor *OperatorExecutor) ConfigureDecodeTrap(ctx context.Context, primaryKey string) error {
	if primaryKey != "id" && primaryKey != "deleted_at" {
		return errors.New("diagnostic decode control is invalid")
	}
	if err := executor.exec(ctx, `SELECT synchro.synchro_register_table(
        'public.cf_decode_trap',
		'public.cf_decode_trap_membership',
        'single_scope',
        $1, 'updated_at', 'deleted_at', 'enabled'
    )`, primaryKey); err != nil {
		return err
	}
	return executor.ReloadRegistry(ctx)
}

// InjectRegisteredTruncate commits the fixed unsupported WAL operation.
func (executor *OperatorExecutor) InjectRegisteredTruncate(ctx context.Context) error {
	return executor.exec(ctx, "TRUNCATE TABLE public.cf_items")
}

// InjectDecoderMetadataChange commits one valid source transaction whose
// relation metadata differs from the running decoder cache.
func (executor *OperatorExecutor) InjectDecoderMetadataChange(ctx context.Context, recordID string) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady ||
		ctx == nil || !diagnosticUUIDPattern.MatchString(recordID) {
		return errors.New("decoder metadata control is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return errors.New("open decoder metadata control connection failed")
	}
	defer database.Close()
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin decoder metadata control failed")
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, "ALTER TABLE public.cf_items ALTER COLUMN value TYPE varchar(256)"); err != nil {
		return errors.New("alter decoder metadata control relation failed")
	}
	if _, err := transaction.ExecContext(
		ctx,
		"INSERT INTO public.cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'decode-repair-source')",
		recordID,
	); err != nil {
		return errors.New("insert decoder metadata control row failed")
	}
	if err := transaction.Commit(); err != nil {
		return errors.New("commit decoder metadata control failed")
	}
	return nil
}

// RetryWALPoison requests the production same-position retry path.
func (executor *OperatorExecutor) RetryWALPoison(ctx context.Context) (bool, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady || ctx == nil {
		return false, errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(
		ctx,
		executor.harness.names.Database,
		executor.harness.env.Operator,
		false,
	)
	if err != nil {
		return false, errors.New("open WAL poison retry connection failed")
	}
	defer database.Close()
	var requested bool
	if err := database.QueryRowContext(ctx, "SELECT synchro.synchro_retry_wal_poison()").Scan(&requested); err != nil {
		return false, errors.New("request WAL poison retry failed")
	}
	return requested, nil
}

// AdvanceActiveSlotPastDurableBoundary simulates an unauthorized slot skip.
func (executor *OperatorExecutor) AdvanceActiveSlotPastDurableBoundary(ctx context.Context) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return errors.New("open slot advance control connection failed")
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE public.cf_slot_advance_control (id bigint PRIMARY KEY);
		INSERT INTO public.cf_slot_advance_control (id) VALUES (1)`); err != nil {
		return errors.New("create slot advance control WAL failed")
	}
	return waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		var advanced string
		err := database.QueryRowContext(attemptContext, `
			SELECT advanced.end_lsn::text
			FROM synchro.sync_runtime_state runtime
			CROSS JOIN LATERAL pg_catalog.pg_replication_slot_advance(
				runtime.active_slot_name, pg_catalog.pg_current_wal_lsn()
			) advanced
			WHERE runtime.singleton`).Scan(&advanced)
		if err != nil {
			return false, nil
		}
		return advanced != "", nil
	})
}

// RunStreamReset creates and activates one permanent exported-snapshot slot.
func (executor *OperatorExecutor) RunStreamReset(ctx context.Context) (_ StreamResetResult, returnedErr error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return StreamResetResult{}, errors.New("operator executor is unavailable")
	}
	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return StreamResetResult{}, errors.New("open stream reset connection failed")
	}
	defer database.Close()
	operationLock, err := acquireStreamResetOperationLock(ctx, database)
	if err != nil {
		return StreamResetResult{}, err
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		returnedErr = errors.Join(returnedErr, releaseStreamResetOperationLock(cleanupContext, operationLock))
	}()
	recovered, err := recoverInterruptedCandidateOperation(ctx, database, operationLock)
	if err != nil {
		return StreamResetResult{}, err
	}
	if recovered != nil && recovered.StreamReset != nil {
		return *recovered.StreamReset, nil
	}
	if err := downgradeStreamResetOperationLock(ctx, operationLock); err != nil {
		return StreamResetResult{}, err
	}
	candidateSlot := harness.names.ReplicationSlot + "_reset"
	if len(candidateSlot) > 63 {
		return StreamResetResult{}, errors.New("stream reset candidate name is invalid")
	}

	var preparedRaw []byte
	if err := database.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_prepare_stream_reset($1)",
		candidateSlot,
	).Scan(&preparedRaw); err != nil {
		return StreamResetResult{}, errors.New("prepare stream reset failed")
	}
	var prepared struct {
		ResetID                string `json:"reset_id"`
		TargetStreamGeneration string `json:"target_stream_generation"`
		OldSlotName            string `json:"old_slot_name"`
	}
	if err := json.Unmarshal(preparedRaw, &prepared); err != nil || !diagnosticUUIDPattern.MatchString(prepared.ResetID) || prepared.TargetStreamGeneration == "" || prepared.OldSlotName == "" {
		return StreamResetResult{}, errors.New("prepared stream reset response is invalid")
	}
	var sourceLockDatabase *sql.DB
	var sourceLockConnection *sql.Conn
	var activationTransaction *sql.Tx
	var replicationConnection *pgconn.PgConn
	slotCreationAttempted := false
	activated := false
	defer func() {
		if returnedErr == nil {
			return
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var cleanupErrors []error
		if activationTransaction != nil {
			if err := activationTransaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				cleanupErrors = append(cleanupErrors, errors.New("rollback stream reset activation transaction failed"))
			}
		}
		if replicationConnection != nil {
			if err := replicationConnection.Close(cleanupContext); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("close stream reset replication connection failed"))
			}
		}
		if !activated {
			if _, err := database.ExecContext(cleanupContext, "SELECT synchro.synchro_abort_stream_reset($1::uuid)", prepared.ResetID); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("abort failed stream reset failed"))
			}
			if slotCreationAttempted {
				if err := dropInactiveReplicationSlot(cleanupContext, database, candidateSlot); err != nil {
					cleanupErrors = append(cleanupErrors, errors.New("drop failed stream reset candidate slot failed"))
				}
			}
		}
		if sourceLockConnection != nil {
			if _, err := sourceLockConnection.ExecContext(cleanupContext, "SELECT pg_catalog.pg_advisory_unlock_all()"); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("unlock failed stream reset sources failed"))
			}
			if err := sourceLockConnection.Close(); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("close failed stream reset source lock connection failed"))
			}
		}
		if sourceLockDatabase != nil {
			if err := sourceLockDatabase.Close(); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("close failed stream reset source lock database failed"))
			}
		}
		returnedErr = errors.Join(returnedErr, errors.Join(cleanupErrors...))
	}()
	sourceLockDatabase, err = harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return StreamResetResult{}, errors.New("open stream reset source lock database failed")
	}
	sourceLockDatabase.SetMaxOpenConns(1)
	sourceLockDatabase.SetMaxIdleConns(0)
	sourceLockConnection, err = sourceLockDatabase.Conn(ctx)
	if err != nil {
		return StreamResetResult{}, errors.New("open stream reset source lock connection failed")
	}
	var locked bool
	if err := sourceLockConnection.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_lock_stream_reset_sources($1::uuid)",
		prepared.ResetID,
	).Scan(&locked); err != nil || !locked {
		return StreamResetResult{}, errors.New("lock stream reset sources failed")
	}
	beforeMarker, err := markStreamResetSnapshot(ctx, database, prepared.ResetID, "before")
	if err != nil {
		return StreamResetResult{}, err
	}

	replicationDSN := postgresDSN("127.0.0.1", harness.port, harness.names.Database, harness.worker, true) + " replication=database"
	replicationConnection, err = pgconn.Connect(ctx, replicationDSN)
	if err != nil {
		return StreamResetResult{}, errors.New("open stream reset replication connection failed")
	}
	slotCreationAttempted = true
	consistentPoint, snapshotName, err := createExportedSnapshotSlot(ctx, replicationConnection, candidateSlot)
	if err != nil {
		return StreamResetResult{}, err
	}
	afterMarker, err := markStreamResetSnapshot(ctx, database, prepared.ResetID, "after")
	if err != nil {
		return StreamResetResult{}, err
	}

	snapshotTransaction, err := database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return StreamResetResult{}, errors.New("begin stream reset snapshot transaction failed")
	}
	defer snapshotTransaction.Rollback()
	if _, err := snapshotTransaction.ExecContext(ctx, "SET TRANSACTION SNAPSHOT "+quotePostgresLiteral(snapshotName)); err != nil {
		return StreamResetResult{}, errors.New("import stream reset snapshot failed")
	}
	var stagedRaw []byte
	if err := snapshotTransaction.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_stage_stream_reset($1::uuid, $2, $3, $4, $5, $6::uuid, $7, $8::uuid)",
		prepared.ResetID,
		candidateSlot,
		consistentPoint,
		snapshotName,
		beforeMarker.XID,
		beforeMarker.Nonce,
		afterMarker.XID,
		afterMarker.Nonce,
	).Scan(&stagedRaw); err != nil {
		return StreamResetResult{}, errors.New("stage stream reset baseline failed")
	}
	if len(stagedRaw) == 0 {
		return StreamResetResult{}, errors.New("staged stream reset response is invalid")
	}
	if err := snapshotTransaction.Commit(); err != nil {
		return StreamResetResult{}, errors.New("commit stream reset baseline failed")
	}
	if err := replicationConnection.Close(ctx); err != nil {
		return StreamResetResult{}, errors.New("close stream reset replication connection failed")
	}
	replicationConnection = nil

	activationTransaction, err = database.BeginTx(ctx, nil)
	if err != nil {
		return StreamResetResult{}, errors.New("begin stream reset activation transaction failed")
	}
	var activatedRaw []byte
	if err := activationTransaction.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_activate_stream_reset($1::uuid)",
		prepared.ResetID,
	).Scan(&activatedRaw); err != nil {
		return StreamResetResult{}, errors.New("activate stream reset failed")
	}
	if len(activatedRaw) == 0 {
		return StreamResetResult{}, errors.New("activated stream reset response is invalid")
	}
	if err := activationTransaction.Commit(); err != nil {
		return StreamResetResult{}, errors.New("commit stream reset activation failed")
	}
	activationTransaction = nil
	activated = true
	if _, err := sourceLockConnection.ExecContext(ctx, "SELECT pg_catalog.pg_advisory_unlock_all()"); err != nil {
		return StreamResetResult{}, errors.New("unlock stream reset sources failed")
	}
	if err := sourceLockConnection.Close(); err != nil {
		return StreamResetResult{}, errors.New("close stream reset source lock connection failed")
	}
	sourceLockConnection = nil
	if err := sourceLockDatabase.Close(); err != nil {
		return StreamResetResult{}, errors.New("close stream reset source lock database failed")
	}
	sourceLockDatabase = nil

	if err := dropInactiveReplicationSlot(ctx, database, prepared.OldSlotName); err != nil {
		return StreamResetResult{}, errors.New("retire old stream reset slot failed")
	}
	var cleanupComplete bool
	if err := database.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_complete_stream_reset_cleanup($1::uuid)",
		prepared.ResetID,
	).Scan(&cleanupComplete); err != nil || !cleanupComplete {
		return StreamResetResult{}, errors.New("complete stream reset cleanup failed")
	}

	var result StreamResetResult
	if err := database.QueryRowContext(ctx, `
		SELECT reset_id::text, source_stream_generation, target_stream_generation,
		       old_slot_name::text, candidate_slot_name::text
		FROM synchro.sync_stream_resets
		WHERE reset_id = $1::uuid AND lifecycle = 'cleanup_complete'`, prepared.ResetID).Scan(
		&result.ResetID,
		&result.SourceStreamGeneration,
		&result.TargetStreamGeneration,
		&result.OldSlotName,
		&result.CandidateSlotName,
	); err != nil {
		return StreamResetResult{}, errors.New("read completed stream reset failed")
	}
	return result, nil
}

// RunProjectionBootstrap executes the verified production projection-bootstrap command.
func (executor *OperatorExecutor) RunProjectionBootstrap(ctx context.Context, registryGeneration int64) (ProjectionBootstrapResult, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return ProjectionBootstrapResult{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || registryGeneration <= 0 {
		return ProjectionBootstrapResult{}, errors.New("projection bootstrap input is invalid")
	}
	harness := executor.harness
	artifact, digest, err := verifyAdapterArtifact(harness.env.AdapterArtifact)
	if err != nil || artifact != harness.env.AdapterArtifact || digest != harness.env.adapterSHA256 {
		return ProjectionBootstrapResult{}, errors.New("adapter artifact identity changed before projection bootstrap")
	}
	operatorURL := harness.databaseURL(harness.env.Operator)
	workerURL := harness.databaseURL(harness.worker)
	environment := scrubPostgresEnvironment(os.Environ())
	environment = append(environment,
		"DATABASE_URL="+operatorURL,
		"WORKER_DATABASE_URL="+workerURL,
	)
	stdout, _, err := runProjectionBootstrapProcess(
		ctx,
		artifact,
		[]string{"projection-bootstrap", "--registry-generation", strconv.FormatInt(registryGeneration, 10)},
		environment,
		harness.config.ProcessLogBytes,
		[][]byte{[]byte(operatorURL), []byte(workerURL), harness.env.Operator.password, harness.worker.password},
	)
	if err != nil {
		return ProjectionBootstrapResult{}, err
	}
	return parseProjectionBootstrapResult(stdout, registryGeneration)
}

func parseProjectionBootstrapResult(raw []byte, registryGeneration int64) (ProjectionBootstrapResult, error) {
	if err := jsonstrict.ValidateValue(raw); err != nil {
		return ProjectionBootstrapResult{}, errors.New("projection bootstrap result is invalid")
	}
	var result ProjectionBootstrapResult
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return ProjectionBootstrapResult{}, errors.New("projection bootstrap result is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ProjectionBootstrapResult{}, errors.New("projection bootstrap result is invalid")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 9 {
		return ProjectionBootstrapResult{}, errors.New("projection bootstrap result is invalid")
	}
	for _, field := range []string{
		"bootstrap_id", "registry_generation", "source_stream_generation", "active_slot_name",
		"candidate_slot_name", "schema_version", "schema_hash", "activation_barrier", "affected_scopes",
	} {
		if _, ok := fields[field]; !ok {
			return ProjectionBootstrapResult{}, errors.New("projection bootstrap result is invalid")
		}
	}
	if result.RegistryGeneration != registryGeneration ||
		!diagnosticUUIDPattern.MatchString(result.BootstrapID) ||
		result.SourceStreamGeneration == "" || result.ActiveSlotName == "" ||
		result.CandidateSlotName == "" || result.ActivationBarrier == "" ||
		len(result.AffectedScopes) == 0 || !validProjectionBootstrapSchema(result.SchemaVersion, result.SchemaHash) {
		return ProjectionBootstrapResult{}, errors.New("projection bootstrap result is invalid")
	}
	for _, scope := range result.AffectedScopes {
		if scope == "" {
			return ProjectionBootstrapResult{}, errors.New("projection bootstrap result is invalid")
		}
	}
	return result, nil
}

func runProjectionBootstrapProcess(ctx context.Context, executable string, arguments, environment []string, logLimit int, redactions [][]byte) ([]byte, []byte, error) {
	if ctx == nil {
		return nil, nil, errors.New("command context is required")
	}
	stdoutLog := newBoundedLog(logLimit, redactions)
	stderrLog := newBoundedLog(logLimit, redactions)
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Env = environment
	configureProcessGroup(command)
	command.Stdout = stdoutLog
	command.Stderr = stderrLog
	if err := command.Run(); err != nil || ctx.Err() != nil {
		return nil, nil, boundedCommandFailure(stdoutLog, stderrLog)
	}
	if stdoutLog.isTruncated() {
		return nil, nil, errors.New("projection bootstrap stdout is truncated")
	}
	return stdoutLog.sanitizedBytes(), stderrLog.sanitizedBytes(), nil
}

func boundedCommandFailure(stdout, stderr *boundedLog) error {
	output := strings.TrimSpace(string(stdout.sanitizedBytes()))
	if stderrOutput := strings.TrimSpace(string(stderr.sanitizedBytes())); stderrOutput != "" {
		if output != "" {
			output += " "
		}
		output += stderrOutput
	}
	output = strings.ReplaceAll(output, "\r", " ")
	output = strings.ReplaceAll(output, "\n", " ")
	const maximum = 512
	if len(output) > maximum {
		const segment = maximum / 2
		output = output[:segment] + " ... " + output[len(output)-segment:]
	}
	if output != "" {
		return fmt.Errorf("bounded command failed: %s", output)
	}
	return errors.New("bounded command failed")
}

func acquireStreamResetOperationLock(ctx context.Context, database *sql.DB) (*sql.Conn, error) {
	connection, err := database.Conn(ctx)
	if err != nil {
		return nil, errors.New("open stream reset operation lock connection failed")
	}
	var locked bool
	if err := connection.QueryRowContext(
		ctx,
		"SELECT pg_catalog.pg_try_advisory_lock($1::bigint)",
		streamResetOperatorLockKey,
	).Scan(&locked); err != nil || !locked {
		_ = connection.Close()
		return nil, errors.New("another stream reset operation is active")
	}
	return connection, nil
}

func releaseStreamResetOperationLock(ctx context.Context, connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	var cleanupErrors []error
	if _, err := connection.ExecContext(ctx, "SELECT pg_catalog.pg_advisory_unlock_all()"); err != nil {
		cleanupErrors = append(cleanupErrors, errors.New("unlock stream reset operation failed"))
	}
	if err := connection.Close(); err != nil {
		cleanupErrors = append(cleanupErrors, errors.New("close stream reset operation lock connection failed"))
	}
	return errors.Join(cleanupErrors...)
}

func downgradeStreamResetOperationLock(ctx context.Context, connection *sql.Conn) error {
	if _, err := connection.ExecContext(
		ctx,
		"SELECT pg_catalog.pg_advisory_lock_shared($1::bigint)",
		streamResetOperatorLockKey,
	); err != nil {
		return errors.New("acquire shared stream reset operation lock failed")
	}
	var unlocked bool
	if err := connection.QueryRowContext(
		ctx,
		"SELECT pg_catalog.pg_advisory_unlock($1::bigint)",
		streamResetOperatorLockKey,
	).Scan(&unlocked); err != nil || !unlocked {
		return errors.New("release exclusive stream reset operation lock failed")
	}
	return nil
}

func validProjectionBootstrapSchema(version *int64, hash *string) bool {
	if version == nil || hash == nil {
		return version == nil && hash == nil
	}
	if *version <= 0 || len(*hash) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(*hash)
	return err == nil && *hash == hex.EncodeToString(decoded)
}

func candidateOperationRecoveryPlan(operationKind, lifecycle, oldSlot, candidateSlot string) (candidateRecoveryPlan, error) {
	if oldSlot == "" || candidateSlot == "" || oldSlot == candidateSlot {
		return candidateRecoveryPlan{}, errors.New("interrupted candidate operation slots are invalid")
	}
	plan := candidateRecoveryPlan{RetiredSlotName: candidateSlot}
	switch operationKind {
	case streamResetOperationKind:
		if lifecycle == "catching_up" {
			return candidateRecoveryPlan{}, errors.New("stream reset has an invalid recovery lifecycle")
		}
		plan.AbortFunction = "synchro_abort_stream_reset"
		plan.CleanupFunction = "synchro_complete_stream_reset_cleanup"
		if lifecycle == "activated" {
			plan.RetiredSlotName = oldSlot
		}
	case projectionBootstrapOperationKind:
		plan.AbortFunction = "synchro_abort_projection_bootstrap"
		plan.CleanupFunction = "synchro_complete_projection_bootstrap_cleanup"
	default:
		return candidateRecoveryPlan{}, errors.New("interrupted candidate operation kind is invalid")
	}
	switch lifecycle {
	case "preparing", "baseline_staged", "catching_up":
		return plan, nil
	case "activated":
		plan.Activated = true
		return plan, nil
	default:
		return candidateRecoveryPlan{}, errors.New("interrupted candidate operation lifecycle is invalid")
	}
}

func recoveredProjectionBootstrapResult(reset StreamResetResult, generation, schemaVersion sql.NullInt64, schemaHash, activationBarrier sql.NullString, affectedScopes []string) (ProjectionBootstrapResult, error) {
	if !generation.Valid || generation.Int64 <= 0 ||
		!activationBarrier.Valid || activationBarrier.String == "" ||
		!diagnosticUUIDPattern.MatchString(reset.ResetID) || reset.CandidateSlotName == "" {
		return ProjectionBootstrapResult{}, errors.New("recovered projection bootstrap state is invalid")
	}
	var version *int64
	if schemaVersion.Valid {
		value := schemaVersion.Int64
		version = &value
	}
	var hash *string
	if schemaHash.Valid {
		value := schemaHash.String
		hash = &value
	}
	if !validProjectionBootstrapSchema(version, hash) {
		return ProjectionBootstrapResult{}, errors.New("recovered projection bootstrap schema is invalid")
	}
	return ProjectionBootstrapResult{
		BootstrapID:            reset.ResetID,
		RegistryGeneration:     generation.Int64,
		SourceStreamGeneration: reset.SourceStreamGeneration,
		ActiveSlotName:         reset.OldSlotName,
		CandidateSlotName:      reset.CandidateSlotName,
		SchemaVersion:          version,
		SchemaHash:             hash,
		ActivationBarrier:      activationBarrier.String,
		AffectedScopes:         append([]string(nil), affectedScopes...),
	}, nil
}

func recoverInterruptedCandidateOperation(ctx context.Context, database *sql.DB, operationLock *sql.Conn) (*recoveredCandidateOperation, error) {
	var operationKind string
	var reset StreamResetResult
	var sourceRegistryGeneration int64
	var targetRegistryGeneration sql.NullInt64
	var schemaVersion sql.NullInt64
	var schemaHash sql.NullString
	var activationBarrier sql.NullString
	var affectedScopesJSON string
	var lifecycle string
	err := database.QueryRowContext(ctx, `
		SELECT operation_kind, reset_id::text, source_stream_generation,
		       target_stream_generation, source_registry_generation,
		       target_registry_generation, old_slot_name::text,
		       candidate_slot_name::text, target_schema_version,
		       target_schema_hash, activation_barrier::text,
		       COALESCE(to_jsonb(affected_scopes), '[]'::jsonb)::text,
		       lifecycle
		FROM synchro.sync_stream_resets
		WHERE lifecycle IN ('preparing', 'baseline_staged', 'catching_up', 'activated')
		ORDER BY prepared_at
		LIMIT 1`).Scan(
		&operationKind,
		&reset.ResetID,
		&reset.SourceStreamGeneration,
		&reset.TargetStreamGeneration,
		&sourceRegistryGeneration,
		&targetRegistryGeneration,
		&reset.OldSlotName,
		&reset.CandidateSlotName,
		&schemaVersion,
		&schemaHash,
		&activationBarrier,
		&affectedScopesJSON,
		&lifecycle,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.New("load interrupted candidate operation failed")
	}
	if sourceRegistryGeneration <= 0 {
		return nil, errors.New("interrupted candidate operation source generation is invalid")
	}
	plan, err := candidateOperationRecoveryPlan(
		operationKind,
		lifecycle,
		reset.OldSlotName,
		reset.CandidateSlotName,
	)
	if err != nil {
		return nil, err
	}
	var recoveredProjection *ProjectionBootstrapResult
	if plan.Activated && operationKind == projectionBootstrapOperationKind {
		var affectedScopes []string
		if err := json.Unmarshal([]byte(affectedScopesJSON), &affectedScopes); err != nil {
			return nil, errors.New("recovered projection bootstrap scopes are invalid")
		}
		projection, err := recoveredProjectionBootstrapResult(
			reset,
			targetRegistryGeneration,
			schemaVersion,
			schemaHash,
			activationBarrier,
			affectedScopes,
		)
		if err != nil {
			return nil, err
		}
		recoveredProjection = &projection
	}
	if plan.Activated {
		if err := dropInactiveReplicationSlot(ctx, database, plan.RetiredSlotName); err != nil {
			return nil, errors.New("retire interrupted candidate operation slot failed")
		}
		var cleanupComplete bool
		if err := operationLock.QueryRowContext(
			ctx,
			"SELECT synchro."+plan.CleanupFunction+"($1::uuid)",
			reset.ResetID,
		).Scan(&cleanupComplete); err != nil || !cleanupComplete {
			return nil, errors.New("complete interrupted candidate operation cleanup failed")
		}
		result := &recoveredCandidateOperation{OperationKind: operationKind}
		if operationKind == streamResetOperationKind {
			result.StreamReset = &reset
			return result, nil
		}
		result.ProjectionBootstrap = recoveredProjection
		return result, nil
	}
	var abortedRaw []byte
	if err := operationLock.QueryRowContext(
		ctx,
		"SELECT synchro."+plan.AbortFunction+"($1::uuid)",
		reset.ResetID,
	).Scan(&abortedRaw); err != nil || len(abortedRaw) == 0 {
		return nil, errors.New("abort interrupted candidate operation failed")
	}
	if err := dropInactiveReplicationSlot(ctx, database, plan.RetiredSlotName); err != nil {
		return nil, errors.New("discard interrupted candidate operation slot failed")
	}
	return nil, nil
}

// CreateInterruptedStreamReset creates a stale pre-activation candidate for crash recovery tests.
func (executor *OperatorExecutor) CreateInterruptedStreamReset(ctx context.Context) (StreamResetResult, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return StreamResetResult{}, errors.New("operator executor is unavailable")
	}
	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return StreamResetResult{}, errors.New("open interrupted stream reset connection failed")
	}
	defer database.Close()
	candidateSlot := harness.names.ReplicationSlot + "_reset"
	var preparedRaw []byte
	if err := database.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_prepare_stream_reset($1)",
		candidateSlot,
	).Scan(&preparedRaw); err != nil {
		return StreamResetResult{}, errors.New("prepare interrupted stream reset failed")
	}
	var prepared struct {
		ResetID                string `json:"reset_id"`
		TargetStreamGeneration string `json:"target_stream_generation"`
		OldSlotName            string `json:"old_slot_name"`
	}
	if err := json.Unmarshal(preparedRaw, &prepared); err != nil || !diagnosticUUIDPattern.MatchString(prepared.ResetID) {
		return StreamResetResult{}, errors.New("interrupted stream reset response is invalid")
	}
	replicationDSN := postgresDSN("127.0.0.1", harness.port, harness.names.Database, harness.worker, true) + " replication=database"
	replicationConnection, err := pgconn.Connect(ctx, replicationDSN)
	if err != nil {
		return StreamResetResult{}, errors.New("open interrupted stream reset replication connection failed")
	}
	if _, _, err := createExportedSnapshotSlot(ctx, replicationConnection, candidateSlot); err != nil {
		_ = replicationConnection.Close(ctx)
		return StreamResetResult{}, err
	}
	if err := replicationConnection.Close(ctx); err != nil {
		return StreamResetResult{}, errors.New("close interrupted stream reset replication connection failed")
	}
	return StreamResetResult{
		ResetID:                prepared.ResetID,
		TargetStreamGeneration: prepared.TargetStreamGeneration,
		OldSlotName:            prepared.OldSlotName,
		CandidateSlotName:      candidateSlot,
	}, nil
}

// RecoverInterruptedStreamReset discards one stale pre-activation reset.
func (executor *OperatorExecutor) RecoverInterruptedStreamReset(ctx context.Context) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return errors.New("open interrupted stream reset recovery connection failed")
	}
	defer database.Close()
	operationLock, err := acquireStreamResetOperationLock(ctx, database)
	if err != nil {
		return err
	}
	defer releaseStreamResetOperationLock(context.Background(), operationLock)
	recovered, err := recoverInterruptedCandidateOperation(ctx, database, operationLock)
	if err != nil {
		return err
	}
	if recovered != nil && recovered.StreamReset != nil {
		return errors.New("interrupted stream reset was already activated")
	}
	return nil
}

// ObservePreparingReset reports whether reset preparation exists and its candidate slot is visible.
func (executor *OperatorExecutor) ObservePreparingReset(ctx context.Context) (bool, bool, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return false, false, errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return false, false, errors.New("open reset preparation observation connection failed")
	}
	defer database.Close()
	var preparing bool
	var candidatePresent bool
	err = database.QueryRowContext(ctx, `
		SELECT EXISTS (
		           SELECT 1 FROM synchro.sync_stream_resets
		           WHERE lifecycle = 'preparing'
		       ),
		       EXISTS (
		           SELECT 1
		           FROM synchro.sync_stream_resets reset
		           JOIN pg_catalog.pg_replication_slots slot
		             ON slot.slot_name = reset.candidate_slot_name
		           WHERE reset.lifecycle IN ('preparing', 'baseline_staged', 'catching_up')
		       )`).Scan(&preparing, &candidatePresent)
	if err != nil {
		return false, false, errors.New("read reset preparation observation failed")
	}
	return preparing, candidatePresent, nil
}

// ObserveActiveProjectionBootstrap returns one durable pre-cleanup Class 3 operation.
func (executor *OperatorExecutor) ObserveActiveProjectionBootstrap(ctx context.Context) (ActiveProjectionBootstrapObservation, bool, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady || ctx == nil {
		return ActiveProjectionBootstrapObservation{}, false, errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return ActiveProjectionBootstrapObservation{}, false, errors.New("open projection bootstrap interruption observation failed")
	}
	defer database.Close()
	var observation ActiveProjectionBootstrapObservation
	err = database.QueryRowContext(ctx, `
		SELECT reset.reset_id::text, reset.lifecycle,
		       reset.candidate_slot_name::text,
		       EXISTS (
		           SELECT 1 FROM pg_catalog.pg_replication_slots slot
		           WHERE slot.slot_name = reset.candidate_slot_name
		       )
		FROM synchro.sync_stream_resets reset
		WHERE reset.operation_kind = 'projection_bootstrap'
		  AND reset.lifecycle IN ('preparing', 'baseline_staged', 'catching_up', 'activated')
		ORDER BY reset.prepared_at
		LIMIT 1`).Scan(
		&observation.BootstrapID,
		&observation.Lifecycle,
		&observation.CandidateSlotName,
		&observation.CandidateSlotPresent,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ActiveProjectionBootstrapObservation{}, false, nil
	}
	if err != nil || !diagnosticUUIDPattern.MatchString(observation.BootstrapID) ||
		observation.CandidateSlotName == "" {
		return ActiveProjectionBootstrapObservation{}, false, errors.New("read projection bootstrap interruption observation failed")
	}
	return observation, true, nil
}

// ObserveProjectionBootstrapRecovery returns cleanup evidence for one Class 3 operation.
func (executor *OperatorExecutor) ObserveProjectionBootstrapRecovery(ctx context.Context, bootstrapID string) (ProjectionBootstrapRecoveryObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady || ctx == nil ||
		!diagnosticUUIDPattern.MatchString(bootstrapID) {
		return ProjectionBootstrapRecoveryObservation{}, errors.New("projection bootstrap recovery observation input is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return ProjectionBootstrapRecoveryObservation{}, errors.New("open projection bootstrap recovery observation failed")
	}
	defer database.Close()
	var observation ProjectionBootstrapRecoveryObservation
	err = database.QueryRowContext(ctx, `
		SELECT reset.lifecycle,
		       NOT EXISTS (
		           SELECT 1 FROM pg_catalog.pg_replication_slots slot
		           WHERE slot.slot_name = reset.candidate_slot_name
		       ),
		       NOT EXISTS (
		           SELECT 1 FROM synchro.sync_projection_bootstrap_events
		           WHERE bootstrap_id = reset.reset_id
		           UNION ALL
		           SELECT 1 FROM synchro.sync_projection_bootstrap_transactions
		           WHERE bootstrap_id = reset.reset_id
		           UNION ALL
		           SELECT 1 FROM synchro.sync_stream_reset_scope_digests
		           WHERE reset_id = reset.reset_id
		           UNION ALL
		           SELECT 1 FROM synchro.sync_stream_reset_fence_coverage
		           WHERE reset_id = reset.reset_id
		           UNION ALL
		           SELECT 1 FROM synchro.sync_stream_reset_membership_edges
		           WHERE reset_id = reset.reset_id
		           UNION ALL
		           SELECT 1 FROM synchro.sync_stream_reset_capture_dependency_rows
		           WHERE reset_id = reset.reset_id
		           UNION ALL
		           SELECT 1 FROM synchro.sync_stream_reset_captured_rows
		           WHERE reset_id = reset.reset_id
		           UNION ALL
		           SELECT 1 FROM synchro.sync_stream_reset_row_versions
		           WHERE reset_id = reset.reset_id
		       )
		FROM synchro.sync_stream_resets reset
		WHERE reset.reset_id = $1::uuid
		  AND reset.operation_kind = 'projection_bootstrap'`, bootstrapID).Scan(
		&observation.Lifecycle,
		&observation.CandidateSlotAbsent,
		&observation.StageCleared,
	)
	if err != nil {
		return ProjectionBootstrapRecoveryObservation{}, errors.New("read projection bootstrap recovery observation failed")
	}
	return observation, nil
}

func markStreamResetSnapshot(ctx context.Context, database *sql.DB, resetID, phase string) (streamResetSnapshotMarker, error) {
	var raw []byte
	if err := database.QueryRowContext(
		ctx,
		"SELECT synchro.synchro_mark_stream_reset_snapshot($1::uuid, $2)",
		resetID,
		phase,
	).Scan(&raw); err != nil {
		return streamResetSnapshotMarker{}, errors.New("mark stream reset snapshot failed")
	}
	var marker streamResetSnapshotMarker
	if err := json.Unmarshal(raw, &marker); err != nil || marker.XID == "" || !diagnosticUUIDPattern.MatchString(marker.Nonce) {
		return streamResetSnapshotMarker{}, errors.New("stream reset snapshot marker is invalid")
	}
	return marker, nil
}

func dropInactiveReplicationSlot(ctx context.Context, database *sql.DB, slotName string) error {
	return waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		var activePID sql.NullInt64
		err := database.QueryRowContext(
			attemptContext,
			"SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
			slotName,
		).Scan(&activePID)
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		if err != nil || activePID.Valid {
			return false, nil
		}
		if _, err := database.ExecContext(
			attemptContext,
			"SELECT pg_catalog.pg_drop_replication_slot($1)",
			slotName,
		); err != nil {
			return false, nil
		}
		return true, nil
	})
}

func createExportedSnapshotSlot(ctx context.Context, connection *pgconn.PgConn, slotName string) (string, string, error) {
	results, err := connection.Exec(
		ctx,
		"CREATE_REPLICATION_SLOT "+quoteIdentifier(slotName)+" LOGICAL pgoutput EXPORT_SNAPSHOT",
	).ReadAll()
	if err != nil || len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 4 {
		return "", "", errors.New("create exported-snapshot replication slot failed")
	}
	row := results[0].Rows[0]
	if string(row[0]) != slotName || string(row[1]) == "" || string(row[2]) == "" || string(row[3]) != "pgoutput" {
		return "", "", errors.New("exported-snapshot replication slot response is invalid")
	}
	return string(row[1]), string(row[2]), nil
}

// RegisterLateSourceTable registers the fixed late-registration table.
func (executor *OperatorExecutor) RegisterLateSourceTable(ctx context.Context) error {
	return executor.exec(ctx, `SELECT synchro.synchro_register_table(
        'public.cf_late_registration',
		'public.cf_late_registration_membership',
        'single_scope',
        'id', 'updated_at', 'deleted_at', 'enabled'
    )`)
}

// PendingLateSourceRegistryGeneration returns the validated pending generation for the fixed late table.
func (executor *OperatorExecutor) PendingLateSourceRegistryGeneration(ctx context.Context) (int64, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return 0, errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return 0, errors.New("open pending late registration connection failed")
	}
	defer database.Close()
	var generation int64
	if err := database.QueryRowContext(ctx, `
		SELECT registry.registry_generation
		FROM synchro.sync_registry registry
		JOIN synchro.sync_registry_generations generation
		  ON generation.generation = registry.registry_generation
		WHERE generation.state = 'pending'
		  AND generation.validated
		  AND registry.physical_schema = 'public'
		  AND registry.physical_relation = 'cf_late_registration'
		  AND NOT EXISTS (
		      SELECT 1 FROM synchro.sync_schema_manifest manifest
		      WHERE manifest.registry_generation = registry.registry_generation
		  )
		ORDER BY registry.registry_generation DESC
		LIMIT 1`).Scan(&generation); err != nil || generation <= 0 {
		return 0, errors.New("read pending late registration generation failed")
	}
	return generation, nil
}

// ObserveProjectionBootstrap returns fixed late-registration activation evidence.
func (executor *OperatorExecutor) ObserveProjectionBootstrap(ctx context.Context, bootstrapID, historicalRecordID, catchupRecordID string) (ProjectionBootstrapObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return ProjectionBootstrapObservation{}, errors.New("operator executor is unavailable")
	}
	if !diagnosticUUIDPattern.MatchString(bootstrapID) ||
		!diagnosticUUIDPattern.MatchString(historicalRecordID) ||
		!diagnosticUUIDPattern.MatchString(catchupRecordID) {
		return ProjectionBootstrapObservation{}, errors.New("projection bootstrap observation identity is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return ProjectionBootstrapObservation{}, errors.New("open projection bootstrap observation connection failed")
	}
	defer database.Close()
	var observation ProjectionBootstrapObservation
	if err := database.QueryRowContext(ctx, `
		WITH selected AS (
			SELECT *
			FROM synchro.sync_stream_resets
			WHERE reset_id = $1::uuid AND operation_kind = 'projection_bootstrap'
		), target AS (
			SELECT registry.relation_id, registry.table_name
			FROM selected
			JOIN synchro.sync_registry registry
			  ON registry.registry_generation = selected.target_registry_generation
			WHERE registry.physical_schema = 'public'
			  AND registry.physical_relation = 'cf_late_registration'
		), catchup_fence AS (
			SELECT fence.*
			FROM selected
			JOIN target ON true
			JOIN synchro.sync_write_fences fence
			  ON fence.relation_id = target.relation_id
			 AND fence.new_record_id = $3
			ORDER BY fence.created_at DESC
			LIMIT 1
		)
		SELECT selected.lifecycle,
		       runtime.stream_generation = selected.source_stream_generation,
		       runtime.active_slot_name = selected.old_slot_name,
		       NOT EXISTS (
			       SELECT 1 FROM pg_catalog.pg_replication_slots slot
			       WHERE slot.slot_name = selected.candidate_slot_name
		       ),
		       EXISTS (
			       SELECT 1 FROM synchro.sync_registry_generations generation
			       WHERE generation.generation = selected.target_registry_generation
			         AND generation.state = 'active' AND generation.validated
		       ),
		       EXISTS (
			       SELECT 1 FROM synchro.sync_schema_manifest manifest
			       WHERE manifest.registry_generation = selected.target_registry_generation
		       ),
		       EXISTS (
			       SELECT 1 FROM synchro.sync_captured_rows captured, target
			       WHERE captured.relation_id = target.relation_id
			         AND captured.record_id = $2 AND NOT captured.deleted
		       ),
		       EXISTS (
			       SELECT 1 FROM synchro.sync_captured_rows captured, target
			       WHERE captured.relation_id = target.relation_id
			         AND captured.record_id = $3 AND NOT captured.deleted
		       ),
		       EXISTS (
			       SELECT 1 FROM synchro.sync_bucket_edges edge, target
			       WHERE edge.relation_id = target.relation_id
			         AND edge.record_id = $2 AND edge.bucket_id = 'user:diagnostic-user'
		       ),
		       EXISTS (
			       SELECT 1 FROM synchro.sync_bucket_edges edge, target
			       WHERE edge.relation_id = target.relation_id
			         AND edge.record_id = $3 AND edge.bucket_id = 'user:diagnostic-user'
		       ),
		       COALESCE((SELECT coverage FROM catchup_fence), ''),
		       COALESCE((
			       SELECT reset_id = selected.reset_id
			              AND reset_slot_name = selected.candidate_slot_name
			              AND reset_consistent_point = selected.consistent_point
			              AND commit_lsn IS NOT NULL AND event_ordinal >= 0
			       FROM catchup_fence
		       ), false),
		       NOT EXISTS (
			       SELECT 1 FROM synchro.sync_write_fences fence, target
			       WHERE fence.relation_id = target.relation_id AND fence.coverage = 'pending'
		       ),
		       NOT EXISTS (
			       SELECT 1 FROM synchro.sync_stream_reset_captured_rows staged
			       WHERE staged.reset_id = selected.reset_id
		       )
		FROM selected
		CROSS JOIN synchro.sync_runtime_state runtime
		WHERE runtime.singleton`, bootstrapID, historicalRecordID, catchupRecordID).Scan(
		&observation.Lifecycle,
		&observation.StreamUnchanged,
		&observation.ActiveSlotUnchanged,
		&observation.CandidateSlotAbsent,
		&observation.RegistryActive,
		&observation.ManifestPublished,
		&observation.HistoricalRecordPresent,
		&observation.CatchupRecordPresent,
		&observation.HistoricalMembershipPresent,
		&observation.CatchupMembershipPresent,
		&observation.CatchupFenceCoverage,
		&observation.CatchupFenceProvenanceMatches,
		&observation.NoPendingFences,
		&observation.StageCleared,
	); err != nil {
		return ProjectionBootstrapObservation{}, errors.New("read projection bootstrap observation failed")
	}
	return observation, nil
}

// UnregisterLateSourceTable unregisters the fixed late-registration table.
func (executor *OperatorExecutor) UnregisterLateSourceTable(ctx context.Context) error {
	return executor.exec(ctx, "SELECT synchro.synchro_unregister_table('cf_late_registration')")
}

// ConfigureCrossScopeTable installs the fixed cross-scope diagnostic registration.
func (executor *OperatorExecutor) ConfigureCrossScopeTable(ctx context.Context) error {
	for _, statement := range []string{
		"SELECT synchro.synchro_register_shared_scope('cf:dedup', false)",
		`SELECT synchro.synchro_register_table(
            'public.cf_items',
			'public.cf_items_cross_scope_membership',
            'multi_scope',
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
		`SELECT synchro.synchro_register_table(
            'public.cf_items',
			'public.cf_items_membership',
            'single_scope',
            'id', 'updated_at', 'deleted_at', 'enabled'
        )`,
		"SELECT synchro.synchro_unregister_shared_scope('cf:dedup')",
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

// WALDiagnostics returns bounded operational state without source payloads.
func (executor *OperatorExecutor) WALDiagnostics(ctx context.Context) (string, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return "", errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return "", errors.New("open operator connection failed")
	}
	defer database.Close()
	var diagnostic string
	err = database.QueryRowContext(ctx, `
		SELECT jsonb_build_object(
			'worker_state', COALESCE((SELECT state FROM synchro.sync_wal_worker_state WHERE worker_id = 'synchro_wal_consumer'), 'missing'),
			'worker_backend_present', EXISTS (
				SELECT 1 FROM pg_catalog.pg_stat_activity
				WHERE datname = current_database() AND backend_type = 'synchro WAL consumer'
			),
			'worker_backend_state', (
				SELECT state FROM pg_catalog.pg_stat_activity
				WHERE datname = current_database() AND backend_type = 'synchro WAL consumer'
			),
			'worker_wait_event_type', (
				SELECT wait_event_type FROM pg_catalog.pg_stat_activity
				WHERE datname = current_database() AND backend_type = 'synchro WAL consumer'
			),
			'worker_wait_event', (
				SELECT wait_event FROM pg_catalog.pg_stat_activity
				WHERE datname = current_database() AND backend_type = 'synchro WAL consumer'
			),
			'worker_registry_generation', (SELECT registry_generation FROM synchro.sync_wal_worker_state WHERE worker_id = 'synchro_wal_consumer'),
			'active_registry_generation', (SELECT generation FROM synchro.sync_registry_generations WHERE state = 'active'),
			'pending_registry_generations', (SELECT count(*) FROM synchro.sync_registry_generations WHERE state = 'pending'),
			'poison_class', (SELECT failure_class FROM synchro.sync_wal_poison WHERE lifecycle = 'active'),
			'poison_commit_lsn', (SELECT commit_lsn::text FROM synchro.sync_wal_poison WHERE lifecycle = 'active'),
			'materialized_commit_lsn', (SELECT materialized_commit_lsn::text FROM synchro.sync_wal_progress WHERE singleton),
			'acknowledged_end_lsn', (SELECT acknowledged_end_lsn::text FROM synchro.sync_wal_progress WHERE singleton),
			'active_slot_confirmed_flush_lsn', (
				SELECT slot.confirmed_flush_lsn::text
				FROM pg_catalog.pg_replication_slots slot
				JOIN synchro.sync_runtime_state runtime
				  ON runtime.singleton AND runtime.active_slot_name = slot.slot_name
			),
			'slot_progress_matches', COALESCE((
				SELECT slot.confirmed_flush_lsn = progress.acknowledged_end_lsn
				FROM pg_catalog.pg_replication_slots slot
				JOIN synchro.sync_runtime_state runtime
				  ON runtime.singleton AND runtime.active_slot_name = slot.slot_name
				CROSS JOIN synchro.sync_wal_progress progress
				WHERE progress.singleton
			), false),
			'transaction_count', (SELECT count(*) FROM synchro.sync_wal_transactions),
			'event_count', (SELECT count(*) FROM synchro.sync_wal_events),
			'pending_fence_count', (SELECT count(*) FROM synchro.sync_write_fences WHERE coverage = 'pending')
		)::text`).Scan(&diagnostic)
	if err != nil {
		return "", errors.New("read WAL diagnostics failed")
	}
	return diagnostic, nil
}

// WorkerPeekDiagnostics checks whether the configured replication login can
// decode one bounded batch from the active slot.
func (executor *OperatorExecutor) WorkerPeekDiagnostics(ctx context.Context) (string, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return "", errors.New("operator executor is unavailable")
	}
	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.worker, true)
	if err != nil {
		return "", errors.New("open worker replication diagnostic connection failed")
	}
	defer database.Close()
	var count int64
	err = database.QueryRowContext(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_logical_slot_peek_binary_changes(
			$1, NULL, 1,
			'proto_version', '1',
			'publication_names', $2,
			'messages', 'true'
		)`, harness.names.ReplicationSlot, harness.names.Publication).Scan(&count)
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) {
			return fmt.Sprintf("sqlstate=%s message=%s", postgresError.Code, postgresError.Message), nil
		}
		return "", errors.New("worker replication diagnostic query failed")
	}
	return fmt.Sprintf("message_count=%d", count), nil
}

// CreateWALProgressOrderViolation commits one source row behind persisted progress.
func (executor *OperatorExecutor) CreateWALProgressOrderViolation(ctx context.Context, recordID string) error {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return errors.New("operator executor is unavailable")
	}
	if ctx == nil || !diagnosticUUIDPattern.MatchString(recordID) {
		return errors.New("WAL progress order control identity is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return errors.New("open WAL progress order control connection failed")
	}
	defer database.Close()
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("begin WAL progress order control failed")
	}
	defer tx.Rollback()
	var clean bool
	if err := tx.QueryRowContext(ctx, `
		SELECT NOT EXISTS (
			       SELECT 1 FROM synchro.sync_wal_poison WHERE lifecycle = 'active'
		       )
		   AND NOT EXISTS (
			       SELECT 1 FROM cf_items WHERE id = $1
		       )`, recordID).Scan(&clean); err != nil || !clean {
		return errors.New("WAL progress order control precondition failed")
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+quoteIdentifier(executor.harness.sourceRole)); err != nil {
		return errors.New("activate WAL progress order source role failed")
	}
	if _, err := tx.ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		"mutation-progress-order",
	); err != nil {
		return errors.New("create WAL progress order source row failed")
	}
	if _, err := tx.ExecContext(ctx, "RESET ROLE"); err != nil {
		return errors.New("restore WAL progress order operator role failed")
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE synchro.sync_wal_progress
		SET materialized_commit_lsn = 'FFFFFFFF/FFFFFFFF'::pg_lsn,
		    materialized_end_lsn = 'FFFFFFFF/FFFFFFFF'::pg_lsn,
		    updated_at = now()
		WHERE singleton`); err != nil {
		return errors.New("persist WAL progress order violation failed")
	}
	if err := tx.Commit(); err != nil {
		return errors.New("commit WAL progress order control failed")
	}
	return nil
}

// ObserveWALProgressOrder returns durable ordering-violation handling evidence.
func (executor *OperatorExecutor) ObserveWALProgressOrder(ctx context.Context, recordID string) (WALProgressOrderObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return WALProgressOrderObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || !diagnosticUUIDPattern.MatchString(recordID) {
		return WALProgressOrderObservation{}, errors.New("WAL progress order observation identity is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return WALProgressOrderObservation{}, errors.New("open WAL progress order observation connection failed")
	}
	defer database.Close()
	var observation WALProgressOrderObservation
	err = database.QueryRowContext(ctx, `
		WITH poison AS (
			SELECT commit_lsn, failure_class, relation_id
			FROM synchro.sync_wal_poison
			WHERE lifecycle = 'active'
		)
		SELECT EXISTS (
			       SELECT 1 FROM poison
		       ),
		       COALESCE((
			       SELECT failure_class FROM poison LIMIT 1
		       ), ''),
		       EXISTS (
			       SELECT 1
			       FROM poison
			       JOIN synchro.sync_registry registry
			         ON registry.relation_id::uuid = poison.relation_id
			       JOIN synchro.sync_registry_generations generation
			         ON generation.generation = registry.registry_generation
			       WHERE registry.table_name = 'cf_items'
			         AND generation.state = 'active'
		       ),
		       EXISTS (
			       SELECT 1 FROM cf_items
			       WHERE id = $1
			         AND owner_id = 'diagnostic-user'
			         AND value = 'mutation-progress-order'
		       ),
		       COALESCE((
			       SELECT progress.materialized_commit_lsn >= poison.commit_lsn
			       FROM poison
			       CROSS JOIN synchro.sync_wal_progress progress
			       WHERE progress.singleton
		       ), false),
		       EXISTS (
			       SELECT 1 FROM synchro.sync_changelog
			       WHERE table_name = 'cf_items' AND record_id = $2
		       ),
		       COALESCE((
			       SELECT state = 'blocked' FROM synchro.sync_wal_worker_state
			       WHERE worker_id = 'synchro_wal_consumer'
		       ), false)`, recordID, recordID).Scan(
		&observation.PoisonActive,
		&observation.FailureClass,
		&observation.RelationIDMatchesRegistry,
		&observation.SourceRowPresent,
		&observation.ProgressCommitAtOrAhead,
		&observation.RecordMaterialized,
		&observation.WorkerBlocked,
	)
	if err != nil {
		return WALProgressOrderObservation{}, errors.New("read WAL progress order observation failed")
	}
	return observation, nil
}

// ObserveWALProgress returns the durable replication acknowledgement position.
func (executor *OperatorExecutor) ObserveWALProgress(ctx context.Context) (WALProgressObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return WALProgressObservation{}, errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return WALProgressObservation{}, errors.New("open WAL progress observation connection failed")
	}
	defer database.Close()
	var observation WALProgressObservation
	if err := database.QueryRowContext(ctx, `
		SELECT COALESCE(progress.acknowledged_end_lsn::text, ''),
		       COALESCE(slot.confirmed_flush_lsn::text, ''),
		       COALESCE(progress.acknowledged_end_lsn = slot.confirmed_flush_lsn, false)
		FROM synchro.sync_wal_progress progress
		JOIN synchro.sync_runtime_state runtime ON runtime.singleton
		JOIN pg_catalog.pg_replication_slots slot
		  ON slot.slot_name = runtime.active_slot_name
		WHERE progress.singleton`).Scan(
		&observation.AcknowledgedEndLSN,
		&observation.SlotConfirmedFlushLSN,
		&observation.SlotMatchesProgress,
	); err != nil {
		return WALProgressObservation{}, errors.New("read WAL progress observation failed")
	}
	return observation, nil
}

// CurrentWALWorkerPID returns the unique current WAL consumer process ID.
func (executor *OperatorExecutor) CurrentWALWorkerPID(ctx context.Context) (int, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return 0, errors.New("operator executor is unavailable")
	}
	if ctx == nil {
		return 0, errors.New("WAL worker process context is required")
	}
	if err := ctx.Err(); err != nil {
		return 0, errors.New("WAL worker process context expired")
	}
	observationContext, cancel := context.WithTimeout(ctx, environmentCommandTimeout)
	defer cancel()
	database, err := executor.harness.openDatabase(observationContext, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return 0, errors.New("open WAL worker process observation connection failed")
	}
	defer database.Close()
	var count int
	var pid int
	if err := database.QueryRowContext(observationContext, `
		SELECT count(*), COALESCE(min(pid), 0)
		FROM pg_catalog.pg_stat_activity
		WHERE datname = current_database()
		  AND backend_type = 'synchro WAL consumer'`).Scan(&count, &pid); err != nil {
		return 0, errors.New("read WAL worker process observation failed")
	}
	if count != 1 || pid <= 0 {
		return 0, errors.New("unique WAL worker process is unavailable")
	}
	return pid, nil
}

// RunWALReplayRestartControl forces a worker exit after durable materialization and before acknowledgement.
func (executor *OperatorExecutor) RunWALReplayRestartControl(ctx context.Context, recordID string) (observation WALReplayRestartObservation, returnedErr error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return WALReplayRestartObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || !diagnosticUUIDPattern.MatchString(recordID) {
		return WALReplayRestartObservation{}, errors.New("WAL replay restart identity is invalid")
	}
	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return WALReplayRestartObservation{}, errors.New("open WAL replay restart connection failed")
	}
	defer database.Close()

	observation.PriorProgress, err = executor.ObserveWALProgress(ctx)
	if err != nil || observation.PriorProgress.AcknowledgedEndLSN == "" ||
		observation.PriorProgress.SlotConfirmedFlushLSN == "" ||
		!observation.PriorProgress.SlotMatchesProgress {
		return WALReplayRestartObservation{}, errors.New("WAL replay restart precondition failed")
	}

	var lockTransaction *sql.Tx
	var replicationConnection *pgconn.PgConn
	defer func() {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var cleanupErrors []error
		if replicationConnection != nil {
			if err := replicationConnection.Close(cleanupContext); err != nil {
				cleanupErrors = append(cleanupErrors, errors.New("close WAL replay restart replication connection failed"))
			}
		}
		if lockTransaction != nil {
			if err := lockTransaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				cleanupErrors = append(cleanupErrors, errors.New("rollback WAL replay restart lock failed"))
			}
		}
		returnedErr = errors.Join(returnedErr, errors.Join(cleanupErrors...))
	}()

	lockTransaction, err = database.BeginTx(ctx, nil)
	if err != nil {
		return WALReplayRestartObservation{}, errors.New("begin WAL replay restart lock failed")
	}
	if _, err := lockTransaction.ExecContext(ctx, "LOCK TABLE synchro.sync_wal_transactions IN ACCESS EXCLUSIVE MODE"); err != nil {
		return WALReplayRestartObservation{}, errors.New("lock WAL replay materialization failed")
	}
	if err := (&SourceExecutor{harness: harness}).ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		"restart-before-acknowledgement",
	); err != nil {
		return WALReplayRestartObservation{}, errors.New("commit WAL replay restart source row failed")
	}

	var workerPID int64
	blockedContext, blockedCancel := context.WithTimeout(ctx, 20*time.Second)
	err = waitUntil(blockedContext, func(attemptContext context.Context) (bool, error) {
		err := database.QueryRowContext(attemptContext, `
			SELECT activity.pid
			FROM pg_catalog.pg_stat_activity activity
			JOIN pg_catalog.pg_locks waiting
			  ON waiting.pid = activity.pid AND NOT waiting.granted
			WHERE activity.datname = current_database()
			  AND activity.backend_type = 'synchro WAL consumer'
			  AND waiting.relation = 'synchro.sync_wal_transactions'::regclass
			LIMIT 1`).Scan(&workerPID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return err == nil && workerPID > 0, err
	})
	blockedCancel()
	if err != nil {
		return WALReplayRestartObservation{}, errors.New("wait for blocked WAL materialization failed")
	}

	replicationDSN := postgresDSN("127.0.0.1", harness.port, harness.names.Database, harness.worker, true) + " replication=database"
	replicationConnection, err = pgconn.Connect(ctx, replicationDSN)
	if err != nil {
		return WALReplayRestartObservation{}, errors.New("open WAL replay restart replication connection failed")
	}
	replicationCommand := "START_REPLICATION SLOT " + quoteIdentifier(harness.names.ReplicationSlot) +
		" LOGICAL " + observation.PriorProgress.SlotConfirmedFlushLSN +
		" (proto_version '1', publication_names " + quotePostgresLiteral(harness.names.Publication) + ", messages 'true')"
	// Slot ownership prevents acknowledgement while the worker commits materialization.
	replicationConnection.Exec(ctx, replicationCommand)
	replicationPID := int64(replicationConnection.PID())
	slotContext, slotCancel := context.WithTimeout(ctx, 10*time.Second)
	err = waitUntil(slotContext, func(attemptContext context.Context) (bool, error) {
		var activePID sql.NullInt64
		if err := database.QueryRowContext(attemptContext, `
			SELECT active_pid
			FROM pg_catalog.pg_replication_slots
			WHERE slot_name = $1`, harness.names.ReplicationSlot).Scan(&activePID); err != nil {
			return false, err
		}
		return activePID.Valid && activePID.Int64 == replicationPID, nil
	})
	slotCancel()
	if err != nil {
		return WALReplayRestartObservation{}, errors.New("hold WAL slot before acknowledgement failed")
	}

	if err := lockTransaction.Commit(); err != nil {
		return WALReplayRestartObservation{}, errors.New("release WAL replay materialization failed")
	}
	lockTransaction = nil

	materializedContext, materializedCancel := context.WithTimeout(ctx, 20*time.Second)
	err = waitUntil(materializedContext, func(attemptContext context.Context) (bool, error) {
		pipeline, err := executor.ObserveWALRecords(attemptContext, []string{recordID})
		if err != nil {
			return false, err
		}
		stages, err := executor.ObserveWALRecordStages(attemptContext, "cf_items", []string{recordID})
		if err != nil {
			return false, err
		}
		observation.BeforeRestart = pipeline
		observation.BeforeStages = stages
		return len(pipeline.Records) == 1 &&
			pipeline.Records[0].FenceCoverage == "materialized" &&
			!pipeline.ContiguousAcknowledged &&
			pipeline.AcknowledgedEndLSN == observation.PriorProgress.AcknowledgedEndLSN &&
			pipeline.SlotConfirmedFlushLSN == observation.PriorProgress.SlotConfirmedFlushLSN &&
			stages.PendingFences == 0 && stages.EventCount > 0 && stages.ChangeCount > 0, nil
	})
	materializedCancel()
	if err != nil {
		return WALReplayRestartObservation{}, errors.New("wait for unacknowledged WAL materialization failed")
	}
	workerExitContext, workerExitCancel := context.WithTimeout(ctx, 20*time.Second)
	err = waitUntil(workerExitContext, func(attemptContext context.Context) (bool, error) {
		var workerPresent bool
		if err := database.QueryRowContext(attemptContext, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_catalog.pg_stat_activity
				WHERE datname = current_database()
				  AND backend_type = 'synchro WAL consumer'
				  AND pid = $1
			)`, workerPID).Scan(&workerPresent); err != nil {
			return false, err
		}
		return !workerPresent, nil
	})
	workerExitCancel()
	if err != nil {
		return WALReplayRestartObservation{}, errors.New("wait for WAL worker exit before acknowledgement failed")
	}
	observation.WorkerExitedBeforeAcknowledgement = true

	if err := replicationConnection.Close(ctx); err != nil {
		return WALReplayRestartObservation{}, errors.New("release WAL replay restart slot failed")
	}
	replicationConnection = nil

	workerContext, workerCancel := context.WithTimeout(ctx, 20*time.Second)
	err = waitUntil(workerContext, func(attemptContext context.Context) (bool, error) {
		var replacementPID int64
		err := database.QueryRowContext(attemptContext, `
			SELECT pid
			FROM pg_catalog.pg_stat_activity
			WHERE datname = current_database()
			  AND backend_type = 'synchro WAL consumer'
			  AND pid <> $1`, workerPID).Scan(&replacementPID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		observation.WorkerRestarted = replacementPID > 0
		return observation.WorkerRestarted, nil
	})
	workerCancel()
	if err != nil {
		return WALReplayRestartObservation{}, errors.New("wait for WAL worker restart failed")
	}

	replayedContext, replayedCancel := context.WithTimeout(ctx, 20*time.Second)
	err = waitUntil(replayedContext, func(attemptContext context.Context) (bool, error) {
		pipeline, err := executor.ObserveWALRecords(attemptContext, []string{recordID})
		if err != nil {
			return false, err
		}
		stages, err := executor.ObserveWALRecordStages(attemptContext, "cf_items", []string{recordID})
		if err != nil {
			return false, err
		}
		observation.AfterRestart = pipeline
		observation.AfterStages = stages
		return len(pipeline.Records) == 1 && pipeline.ContiguousAcknowledged &&
			pipeline.AcknowledgementMatchesObservedEnd && pipeline.SlotMatchesObservedEnd, nil
	})
	replayedCancel()
	if err != nil {
		return WALReplayRestartObservation{}, errors.New("wait for WAL replay after worker restart failed")
	}
	return observation, nil
}

// ObserveWALRecords returns redacted replay state for fixed diagnostic row IDs.
func (executor *OperatorExecutor) ObserveWALRecords(ctx context.Context, recordIDs []string) (WALPipelineObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return WALPipelineObservation{}, errors.New("operator executor is unavailable")
	}
	if len(recordIDs) == 0 || len(recordIDs) > 16 {
		return WALPipelineObservation{}, errors.New("WAL observation record IDs are invalid")
	}
	for _, recordID := range recordIDs {
		if !diagnosticUUIDPattern.MatchString(recordID) {
			return WALPipelineObservation{}, errors.New("WAL observation record ID is invalid")
		}
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return WALPipelineObservation{}, errors.New("open operator connection failed")
	}
	defer database.Close()
	rows, err := database.QueryContext(ctx, `
		SELECT c.record_id,
		       c.commit_lsn::text,
		       transaction.end_lsn::text,
		       c.event_ordinal,
		       c.effect_ordinal,
		       fence.coverage,
		       c.row_version::text,
		       transaction.replay_count
		FROM synchro.sync_changelog c
		JOIN synchro.sync_wal_transactions transaction
		  ON transaction.stream_generation = c.stream_generation
		 AND transaction.commit_lsn = c.commit_lsn
		JOIN synchro.sync_wal_events event
		  ON event.stream_generation = c.stream_generation
		 AND event.commit_lsn = c.commit_lsn
		 AND event.event_ordinal = c.event_ordinal
		 AND event.relation_id = c.relation_id
		JOIN synchro.sync_write_fences fence ON fence.fence_id = event.fence_id
		WHERE c.table_name = 'cf_items'
		  AND c.record_id = ANY($1)
		ORDER BY c.commit_lsn, c.event_ordinal, c.effect_ordinal, c.record_id`, recordIDs)
	if err != nil {
		return WALPipelineObservation{}, errors.New("read WAL record observations failed")
	}
	defer rows.Close()
	observation := WALPipelineObservation{}
	for rows.Next() {
		var record WALRecordObservation
		if err := rows.Scan(
			&record.RecordID,
			&record.CommitLSN,
			&record.EndLSN,
			&record.EventOrdinal,
			&record.EffectOrdinal,
			&record.FenceCoverage,
			&record.RowVersion,
			&record.ReplayCount,
		); err != nil {
			return WALPipelineObservation{}, errors.New("scan WAL record observation failed")
		}
		observation.Records = append(observation.Records, record)
	}
	if err := rows.Err(); err != nil {
		return WALPipelineObservation{}, errors.New("read WAL record observations failed")
	}
	err = database.QueryRowContext(ctx, `
		WITH observed AS (
			SELECT max(transaction.end_lsn) AS maximum_end_lsn
			FROM synchro.sync_changelog c
			JOIN synchro.sync_wal_transactions transaction
			  ON transaction.stream_generation = c.stream_generation
			 AND transaction.commit_lsn = c.commit_lsn
			WHERE c.table_name = 'cf_items' AND c.record_id = ANY($1)
		)
		SELECT EXISTS (
				SELECT 1 FROM pg_catalog.pg_stat_activity
				WHERE datname = current_database() AND backend_type = 'synchro WAL consumer'
			),
		       EXISTS (SELECT 1 FROM synchro.sync_wal_poison WHERE lifecycle = 'active'),
			       COALESCE(
				(SELECT progress.acknowledged_end_lsn >= observed.maximum_end_lsn
				 FROM synchro.sync_wal_progress progress, observed
				 WHERE progress.singleton AND observed.maximum_end_lsn IS NOT NULL),
				false
		       ),
		       COALESCE(
				(SELECT progress.acknowledged_end_lsn::text
				 FROM synchro.sync_wal_progress progress
				 WHERE progress.singleton),
				''
		       ),
		       COALESCE(
				(SELECT progress.acknowledged_end_lsn = observed.maximum_end_lsn
				 FROM synchro.sync_wal_progress progress, observed
				 WHERE progress.singleton AND observed.maximum_end_lsn IS NOT NULL),
				false
		       ),
		       COALESCE(
				(SELECT slot.confirmed_flush_lsn::text
				 FROM pg_catalog.pg_replication_slots slot
				 JOIN synchro.sync_runtime_state runtime
				   ON runtime.singleton AND runtime.active_slot_name = slot.slot_name
				 WHERE slot.database = current_database()),
				''
		       ),
		       COALESCE(
				(SELECT slot.confirmed_flush_lsn = observed.maximum_end_lsn
				 FROM pg_catalog.pg_replication_slots slot
				 JOIN synchro.sync_runtime_state runtime
				   ON runtime.singleton AND runtime.active_slot_name = slot.slot_name
				 CROSS JOIN observed
				 WHERE slot.database = current_database()
				   AND observed.maximum_end_lsn IS NOT NULL),
				false
		       )`, recordIDs).Scan(
		&observation.WorkerRunning,
		&observation.BlockingPoison,
		&observation.ContiguousAcknowledged,
		&observation.AcknowledgedEndLSN,
		&observation.AcknowledgementMatchesObservedEnd,
		&observation.SlotConfirmedFlushLSN,
		&observation.SlotMatchesObservedEnd,
	)
	if err != nil {
		return WALPipelineObservation{}, errors.New("read WAL pipeline observation failed")
	}
	return observation, nil
}

// ObserveBlockingPoison returns redacted quarantine and blocking evidence.
func (executor *OperatorExecutor) ObserveBlockingPoison(ctx context.Context, laterRecordID string) (WALPoisonObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return WALPoisonObservation{}, errors.New("operator executor is unavailable")
	}
	if !diagnosticUUIDPattern.MatchString(laterRecordID) {
		return WALPoisonObservation{}, errors.New("WAL poison observation identity is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return WALPoisonObservation{}, errors.New("open WAL poison observation connection failed")
	}
	defer database.Close()
	var observation WALPoisonObservation
	var relationID sql.NullString
	err = database.QueryRowContext(ctx, `
		WITH poison AS (
			SELECT *
			FROM synchro.sync_wal_poison
			WHERE lifecycle = 'active'
		), health AS (
			SELECT synchro.synchro_health_detail() AS value
		)
		SELECT poison.failure_class,
		       poison.relation_id::text,
		       EXISTS (
			       SELECT 1
			       FROM synchro.sync_registry registry
			       JOIN synchro.sync_registry_generations generation
			         ON generation.generation = registry.registry_generation
			       WHERE generation.state = 'active'
			         AND registry.table_name = 'cf_items'
			         AND registry.relation_id = poison.relation_id
		       ),
		       poison.commit_lsn::text,
		       COALESCE(progress.acknowledged_end_lsn < poison.commit_lsn, true),
		       EXISTS (
			       SELECT 1 FROM synchro.sync_changelog
			       WHERE table_name = 'cf_items' AND record_id = $1
		       ),
		       EXISTS (
			       SELECT 1 FROM synchro.sync_write_fences
			       WHERE new_record_id = $1 AND coverage = 'pending'
		       ),
		       worker.state = 'blocked',
		       NOT (health.value->>'ready')::boolean,
		       health.value->'checks'->'poison'->>'state' = 'failed',
		       (health.value->'observations'->>'wal_lag_seconds')::double precision
		FROM poison
		CROSS JOIN synchro.sync_wal_progress progress
		CROSS JOIN synchro.sync_wal_worker_state worker
		CROSS JOIN health
		WHERE progress.singleton AND worker.worker_id = 'synchro_wal_consumer'`, laterRecordID).Scan(
		&observation.FailureClass,
		&relationID,
		&observation.RelationIDMatchesRegistry,
		&observation.CommitLSN,
		&observation.AcknowledgementBlocked,
		&observation.LaterRecordMaterialized,
		&observation.LaterFencePending,
		&observation.WorkerBlocked,
		&observation.ReadinessBlocked,
		&observation.PoisonCheckFailed,
		&observation.WALLagSeconds,
	)
	if err != nil {
		return WALPoisonObservation{}, errors.New("read WAL poison observation failed")
	}
	observation.RelationID = relationID.String
	return observation, nil
}

// ObserveWALPoisonRecovery returns durable same-position repair evidence.
func (executor *OperatorExecutor) ObserveWALPoisonRecovery(ctx context.Context, recordID string) (WALPoisonRecoveryObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return WALPoisonRecoveryObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || !diagnosticUUIDPattern.MatchString(recordID) {
		return WALPoisonRecoveryObservation{}, errors.New("WAL poison recovery observation identity is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return WALPoisonRecoveryObservation{}, errors.New("open WAL poison recovery observation connection failed")
	}
	defer database.Close()

	var observation WALPoisonRecoveryObservation
	err = database.QueryRowContext(ctx, `
		WITH poison AS (
			SELECT commit_lsn, failure_class, lifecycle, attempt_count,
			       retry_requested_at, resolved_at
			FROM synchro.sync_wal_poison
			WHERE failure_class = 'decode_failed'
		), source_effect AS (
			SELECT commit_lsn
			FROM synchro.sync_changelog
			WHERE table_name = 'cf_items' AND record_id = $1
		)
		SELECT (SELECT count(*) FROM poison),
		       poison.failure_class,
		       poison.lifecycle,
		       poison.attempt_count,
		       poison.retry_requested_at IS NOT NULL,
		       poison.resolved_at IS NOT NULL,
		       poison.commit_lsn = source_effect.commit_lsn
		FROM poison, source_effect`, recordID).Scan(
		&observation.PoisonCount,
		&observation.FailureClass,
		&observation.Lifecycle,
		&observation.AttemptCount,
		&observation.RetryRequested,
		&observation.Resolved,
		&observation.SameCommitPosition,
	)
	if err != nil {
		return WALPoisonRecoveryObservation{}, errors.New("read WAL poison recovery observation failed")
	}
	return observation, nil
}

// ObserveStreamReset returns redacted activation and baseline evidence.
func (executor *OperatorExecutor) ObserveStreamReset(ctx context.Context, resetID, tableName, baselineRecordID string) (StreamResetObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return StreamResetObservation{}, errors.New("operator executor is unavailable")
	}
	if !diagnosticUUIDPattern.MatchString(resetID) || !diagnosticUUIDPattern.MatchString(baselineRecordID) {
		return StreamResetObservation{}, errors.New("stream reset observation identity is invalid")
	}
	if tableName != "cf_items" && tableName != "cf_documents" {
		return StreamResetObservation{}, errors.New("stream reset observation table is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return StreamResetObservation{}, errors.New("open stream reset observation connection failed")
	}
	defer database.Close()
	var observation StreamResetObservation
	err = database.QueryRowContext(ctx, `
		WITH selected_reset AS (
			SELECT * FROM synchro.sync_stream_resets WHERE reset_id = $1::uuid
		), selected_registry AS (
			SELECT registry.relation_id
			FROM synchro.sync_registry registry
			JOIN synchro.sync_registry_generations generation
			  ON generation.generation = registry.registry_generation
			WHERE generation.state = 'active' AND registry.table_name = $3
		), health AS (
			SELECT synchro.synchro_health_detail() AS value
		)
		SELECT reset.lifecycle,
		       runtime.active_slot_name::text,
		       runtime.stream_generation,
		       NOT EXISTS (
			       SELECT 1 FROM pg_catalog.pg_replication_slots
			       WHERE slot_name = reset.old_slot_name
		       ),
		       EXISTS (
			       SELECT 1 FROM pg_catalog.pg_replication_slots slot
			       WHERE slot.slot_name = reset.candidate_slot_name
			         AND slot.slot_type = 'logical'
			         AND slot.plugin = 'pgoutput'
			         AND NOT slot.temporary
			         AND slot.invalidation_reason IS NULL
			         AND slot.wal_status IS DISTINCT FROM 'lost'
		       ),
		       NOT EXISTS (SELECT 1 FROM synchro.sync_wal_poison WHERE lifecycle = 'active'),
		       EXISTS (
			       SELECT 1
			       FROM synchro.sync_captured_rows captured
			       JOIN selected_registry registry ON registry.relation_id = captured.relation_id
			       WHERE captured.record_id = $2 AND NOT captured.deleted
		       ),
		       EXISTS (
			       SELECT 1
			       FROM synchro.sync_captured_rows captured
			       JOIN selected_registry registry ON registry.relation_id = captured.relation_id
			       WHERE captured.record_id = $2
			         AND captured.source_reset_id = reset.reset_id
			         AND captured.source_stream_generation = reset.target_stream_generation
			         AND captured.source_commit_lsn IS NULL
			         AND captured.source_event_ordinal IS NULL
		       ),
		       EXISTS (
			       SELECT 1
			       FROM synchro.sync_bucket_edges edge
			       JOIN selected_registry registry ON registry.relation_id = edge.relation_id
			       WHERE edge.record_id = $2 AND edge.bucket_id = 'user:diagnostic-user'
		       ),
		       fence.coverage,
		       NOT EXISTS (
			       SELECT 1 FROM synchro.sync_wal_events event
			       WHERE event.fence_id = fence.fence_id
		       ),
		       NOT EXISTS (
			       SELECT 1 FROM synchro.sync_changelog effect
			       WHERE effect.record_id = $2
		       ),
		       NOT EXISTS (SELECT 1 FROM synchro.sync_client_checkpoints),
		       (health.value->>'ready')::boolean,
		       COALESCE((
		           SELECT string_agg(entry.key, ',' ORDER BY entry.key)
		           FROM jsonb_each(health.value->'checks') entry
		           WHERE entry.value->>'state' <> 'ok'
		       ), '')
		FROM selected_reset reset
		CROSS JOIN synchro.sync_runtime_state runtime
		JOIN synchro.sync_write_fences fence
		  ON fence.new_record_id = $2
		 AND fence.reset_id = reset.reset_id
		CROSS JOIN health
		WHERE runtime.singleton`, resetID, baselineRecordID, tableName).Scan(
		&observation.Lifecycle,
		&observation.ActiveSlotName,
		&observation.ActiveStreamGeneration,
		&observation.OldSlotAbsent,
		&observation.CandidateSlotValid,
		&observation.PoisonCleared,
		&observation.BaselineRecordPresent,
		&observation.BaselineProvenanceMatches,
		&observation.BaselineMembershipPresent,
		&observation.FenceCoverage,
		&observation.NoSyntheticEvent,
		&observation.NoSyntheticEffect,
		&observation.CheckpointsInvalidated,
		&observation.ReadinessReady,
		&observation.ReadinessFailures,
	)
	if err != nil {
		return StreamResetObservation{}, errors.New("read stream reset observation failed")
	}
	return observation, nil
}

// ObserveMembershipBuckets returns current buckets for one fixed diagnostic record.
func (executor *OperatorExecutor) ObserveMembershipBuckets(ctx context.Context, tableName, recordID string) ([]string, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return nil, errors.New("operator executor is unavailable")
	}
	validTable := false
	for _, candidate := range diagnosticSourceTables {
		if tableName == candidate {
			validTable = true
			break
		}
	}
	if !validTable || !diagnosticUUIDPattern.MatchString(recordID) {
		return nil, errors.New("membership observation identity is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return nil, errors.New("open membership observation connection failed")
	}
	defer database.Close()
	rows, err := database.QueryContext(ctx, `
		SELECT bucket_id
		FROM synchro.sync_bucket_edges
		WHERE table_name = $1 AND record_id = $2
		ORDER BY bucket_id`, tableName, recordID)
	if err != nil {
		return nil, errors.New("read membership observation failed")
	}
	defer rows.Close()
	var buckets []string
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, errors.New("scan membership observation failed")
		}
		buckets = append(buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read membership observation failed")
	}
	return buckets, nil
}

// HasClientCheckpoint reports whether a real client has acknowledged any scope.
func (executor *OperatorExecutor) HasClientCheckpoint(ctx context.Context, clientID string) (bool, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady || clientID == "" {
		return false, errors.New("operator executor is unavailable")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return false, errors.New("open checkpoint observation connection failed")
	}
	defer database.Close()
	var present bool
	if err := database.QueryRowContext(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM synchro.sync_client_checkpoints WHERE client_id = $1)",
		clientID,
	).Scan(&present); err != nil {
		return false, errors.New("read checkpoint observation failed")
	}
	return present, nil
}

// ObserveClientCheckpoints returns every bounded checkpoint field for one diagnostic client.
func (executor *OperatorExecutor) ObserveClientCheckpoints(ctx context.Context, clientID string) ([]ClientCheckpointObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return nil, errors.New("operator executor is unavailable")
	}
	if ctx == nil || clientID == "" || len(clientID) > 128 {
		return nil, errors.New("checkpoint observation input is invalid")
	}
	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return nil, errors.New("open checkpoint observation connection failed")
	}
	defer database.Close()
	rows, err := database.QueryContext(ctx, `
		SELECT bucket_id, stream_generation, position_kind,
		       commit_lsn::text, event_ordinal, effect_ordinal,
		       pg_catalog.to_char(
		           updated_at AT TIME ZONE 'UTC',
		           'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
		       )
		FROM synchro.sync_client_checkpoints
		WHERE user_id = 'diagnostic-user' AND client_id = $1
		ORDER BY bucket_id
		LIMIT 17`, clientID)
	if err != nil {
		return nil, errors.New("read checkpoint observations failed")
	}
	defer rows.Close()
	observations := make([]ClientCheckpointObservation, 0, 2)
	for rows.Next() {
		var observation ClientCheckpointObservation
		var commitLSN sql.NullString
		var eventOrdinal sql.NullInt64
		var effectOrdinal sql.NullInt32
		if err := rows.Scan(
			&observation.ScopeID,
			&observation.StreamGeneration,
			&observation.PositionKind,
			&commitLSN,
			&eventOrdinal,
			&effectOrdinal,
			&observation.UpdatedAt,
		); err != nil {
			return nil, errors.New("scan checkpoint observation failed")
		}
		observation.CommitLSN = commitLSN.String
		observation.CommitLSNValid = commitLSN.Valid
		observation.EventOrdinal = eventOrdinal.Int64
		observation.EventOrdinalValid = eventOrdinal.Valid
		observation.EffectOrdinal = effectOrdinal.Int32
		observation.EffectOrdinalValid = effectOrdinal.Valid
		if observation.ScopeID == "" || observation.StreamGeneration == "" || observation.UpdatedAt == "" {
			return nil, errors.New("checkpoint observation is invalid")
		}
		switch observation.PositionKind {
		case "generation_start":
			if observation.CommitLSNValid || observation.EventOrdinalValid || observation.EffectOrdinalValid {
				return nil, errors.New("checkpoint observation is invalid")
			}
		case "effect":
			if !observation.CommitLSNValid || !observation.EventOrdinalValid || !observation.EffectOrdinalValid {
				return nil, errors.New("checkpoint observation is invalid")
			}
		case "transaction_end":
			if !observation.CommitLSNValid || observation.EventOrdinalValid || observation.EffectOrdinalValid {
				return nil, errors.New("checkpoint observation is invalid")
			}
		default:
			return nil, errors.New("checkpoint observation is invalid")
		}
		observations = append(observations, observation)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read checkpoint observations failed")
	}
	if len(observations) > 16 {
		return nil, errors.New("checkpoint observation limit exceeded")
	}
	return observations, nil
}

// ObserveItemStateMatch compares one diagnostic item without returning its source value.
func (executor *OperatorExecutor) ObserveItemStateMatch(ctx context.Context, recordID, expectedValue, expectedVersion string) (ItemStateMatchObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return ItemStateMatchObservation{}, errors.New("operator executor is unavailable")
	}
	if ctx == nil || !diagnosticUUIDPattern.MatchString(recordID) ||
		expectedValue == "" || len(expectedValue) > 128 || !diagnosticUUIDPattern.MatchString(expectedVersion) {
		return ItemStateMatchObservation{}, errors.New("item state observation input is invalid")
	}
	harness := executor.harness
	database, err := harness.openDatabase(ctx, harness.names.Database, harness.env.Admin, false)
	if err != nil {
		return ItemStateMatchObservation{}, errors.New("open item state observation connection failed")
	}
	defer database.Close()
	var observation ItemStateMatchObservation
	if err := database.QueryRowContext(ctx, `
		WITH active_relation AS (
			SELECT registry.relation_id
			FROM synchro.sync_registry registry
			JOIN synchro.sync_registry_generations generation
			  ON generation.generation = registry.registry_generation
			WHERE generation.state = 'active'
			  AND registry.physical_schema = 'public'
			  AND registry.physical_relation = 'cf_items'
		)
		SELECT item.deleted_at IS NULL,
		       item.value = $2,
		       version.row_version::text = $3
		FROM public.cf_items item
		JOIN active_relation relation ON true
		JOIN synchro.sync_row_versions version
		  ON version.relation_id = relation.relation_id
		 AND version.record_id = item.id::text
		WHERE item.id = $1::uuid`, recordID, expectedValue, expectedVersion).Scan(
		&observation.Live,
		&observation.ValueMatches,
		&observation.VersionMatches,
	); err != nil {
		return ItemStateMatchObservation{}, errors.New("read item state observation failed")
	}
	return observation, nil
}

// ObserveDependencyEffects returns bounded effects from the latest fixed dependency-source event.
func (executor *OperatorExecutor) ObserveDependencyEffects(ctx context.Context, sourceRecordID string, recordIDs []string) ([]MembershipEffectObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return nil, errors.New("operator executor is unavailable")
	}
	if !diagnosticUUIDPattern.MatchString(sourceRecordID) || len(recordIDs) == 0 || len(recordIDs) > 16 {
		return nil, errors.New("dependency effect observation identity is invalid")
	}
	for _, recordID := range recordIDs {
		if !diagnosticUUIDPattern.MatchString(recordID) {
			return nil, errors.New("dependency effect observation identity is invalid")
		}
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return nil, errors.New("open dependency effect observation connection failed")
	}
	defer database.Close()
	rows, err := database.QueryContext(ctx, `
		WITH source_event AS (
			SELECT projection.stream_generation, projection.commit_lsn, projection.event_ordinal
			FROM synchro.sync_captured_projections projection
			JOIN synchro.sync_registry registry
			  ON registry.registry_generation = projection.registry_generation
			 AND registry.relation_id = projection.relation_id
			WHERE registry.table_name = 'cf_documents'
			  AND projection.record_id = $1
			  AND projection.image_kind = 'after'
			ORDER BY projection.commit_lsn DESC, projection.event_ordinal DESC
			LIMIT 1
		)
		SELECT identity.table_id::text, effect.record_id, effect.bucket_id,
		       effect.operation, effect.event_ordinal, effect.effect_ordinal
		FROM synchro.sync_changelog effect
		JOIN source_event source
		  ON source.stream_generation = effect.stream_generation
		 AND source.commit_lsn = effect.commit_lsn
		 AND source.event_ordinal = effect.event_ordinal
		JOIN LATERAL (
			SELECT registry.table_id
			FROM synchro.sync_registry registry
			WHERE registry.relation_id = effect.relation_id
			ORDER BY registry.registry_generation DESC
			LIMIT 1
		) identity ON true
		WHERE effect.record_id = ANY($2)
		ORDER BY effect.bucket_id, effect.effect_ordinal`, sourceRecordID, recordIDs)
	if err != nil {
		return nil, errors.New("read dependency effect observation failed")
	}
	defer rows.Close()
	var effects []MembershipEffectObservation
	for rows.Next() {
		var effect MembershipEffectObservation
		if err := rows.Scan(
			&effect.TableID,
			&effect.RecordID,
			&effect.BucketID,
			&effect.Operation,
			&effect.EventOrdinal,
			&effect.EffectOrdinal,
		); err != nil {
			return nil, errors.New("scan dependency effect observation failed")
		}
		effects = append(effects, effect)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read dependency effect observation failed")
	}
	return effects, nil
}

// ObserveCaptureDependency returns bounded projection and fence evidence for one capture-only row.
func (executor *OperatorExecutor) ObserveCaptureDependency(ctx context.Context, sourceRecordID string) (CaptureDependencyObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return CaptureDependencyObservation{}, errors.New("operator executor is unavailable")
	}
	if !diagnosticUUIDPattern.MatchString(sourceRecordID) {
		return CaptureDependencyObservation{}, errors.New("capture dependency observation identity is invalid")
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return CaptureDependencyObservation{}, errors.New("open capture dependency observation connection failed")
	}
	defer database.Close()
	var observation CaptureDependencyObservation
	err = database.QueryRowContext(ctx, `
		WITH source_event AS (
			SELECT projection.stream_generation, projection.commit_lsn,
			       projection.event_ordinal, projection.relation_id,
			       projection.registry_generation, projection.row_data
			FROM synchro.sync_capture_dependency_projections projection
			JOIN synchro.sync_registry registry
			  ON registry.registry_generation = projection.registry_generation
			 AND registry.relation_id = projection.relation_id
			WHERE registry.physical_schema = 'public'
			  AND registry.physical_relation = 'cf_document_access'
			  AND projection.capture_key ->> 'id' = $1
			  AND projection.image_kind = 'after'
			ORDER BY projection.commit_lsn DESC, projection.event_ordinal DESC
			LIMIT 1
		)
		SELECT registry.registration_kind,
		       registry.table_id IS NULL,
		       source.row_data ->> 'owner_id',
		       current.row_data ->> 'owner_id',
		       fence.coverage,
		       (
			       SELECT count(*)
			       FROM synchro.sync_changelog effect
			       WHERE effect.stream_generation = source.stream_generation
			         AND effect.commit_lsn = source.commit_lsn
			         AND effect.event_ordinal = source.event_ordinal
			         AND effect.relation_id = source.relation_id
		       )
		FROM source_event source
		JOIN synchro.sync_registry registry
		  ON registry.registry_generation = source.registry_generation
		 AND registry.relation_id = source.relation_id
		JOIN synchro.sync_wal_events event
		  ON event.stream_generation = source.stream_generation
		 AND event.commit_lsn = source.commit_lsn
		 AND event.event_ordinal = source.event_ordinal
		JOIN synchro.sync_write_fences fence ON fence.fence_id = event.fence_id
		JOIN synchro.sync_capture_dependency_rows current
		  ON current.relation_id = source.relation_id
		 AND current.capture_key ->> 'id' = $1`, sourceRecordID).Scan(
		&observation.RegistrationKind,
		&observation.TableIDAbsent,
		&observation.ProjectionOwnerID,
		&observation.CurrentOwnerID,
		&observation.FenceCoverage,
		&observation.DirectEffectCount,
	)
	if err != nil {
		return CaptureDependencyObservation{}, errors.New("read capture dependency observation failed")
	}
	return observation, nil
}

// ObserveCaptureDependencyEffects returns target effects from the latest capture-only event.
func (executor *OperatorExecutor) ObserveCaptureDependencyEffects(ctx context.Context, sourceRecordID string, recordIDs []string) ([]MembershipEffectObservation, error) {
	if executor == nil || executor.harness == nil || !executor.harness.sourceReady {
		return nil, errors.New("operator executor is unavailable")
	}
	if !diagnosticUUIDPattern.MatchString(sourceRecordID) || len(recordIDs) == 0 || len(recordIDs) > 16 {
		return nil, errors.New("capture dependency effect observation identity is invalid")
	}
	for _, recordID := range recordIDs {
		if !diagnosticUUIDPattern.MatchString(recordID) {
			return nil, errors.New("capture dependency effect observation identity is invalid")
		}
	}
	database, err := executor.harness.openDatabase(ctx, executor.harness.names.Database, executor.harness.env.Admin, false)
	if err != nil {
		return nil, errors.New("open capture dependency effect observation connection failed")
	}
	defer database.Close()
	rows, err := database.QueryContext(ctx, `
		WITH source_event AS (
			SELECT projection.stream_generation, projection.commit_lsn, projection.event_ordinal
			FROM synchro.sync_capture_dependency_projections projection
			JOIN synchro.sync_registry registry
			  ON registry.registry_generation = projection.registry_generation
			 AND registry.relation_id = projection.relation_id
			WHERE registry.physical_schema = 'public'
			  AND registry.physical_relation = 'cf_document_access'
			  AND projection.capture_key ->> 'id' = $1
			  AND projection.image_kind = 'after'
			ORDER BY projection.commit_lsn DESC, projection.event_ordinal DESC
			LIMIT 1
		)
		SELECT identity.table_id::text, effect.record_id, effect.bucket_id,
		       effect.operation, effect.event_ordinal, effect.effect_ordinal
		FROM synchro.sync_changelog effect
		JOIN source_event source
		  ON source.stream_generation = effect.stream_generation
		 AND source.commit_lsn = effect.commit_lsn
		 AND source.event_ordinal = effect.event_ordinal
		JOIN LATERAL (
			SELECT registry.table_id
			FROM synchro.sync_registry registry
			WHERE registry.relation_id = effect.relation_id
			ORDER BY registry.registry_generation DESC
			LIMIT 1
		) identity ON true
		WHERE effect.record_id = ANY($2)
		ORDER BY effect.bucket_id, effect.effect_ordinal`, sourceRecordID, recordIDs)
	if err != nil {
		return nil, errors.New("read capture dependency effects failed")
	}
	defer rows.Close()
	var effects []MembershipEffectObservation
	for rows.Next() {
		var effect MembershipEffectObservation
		if err := rows.Scan(
			&effect.TableID,
			&effect.RecordID,
			&effect.BucketID,
			&effect.Operation,
			&effect.EventOrdinal,
			&effect.EffectOrdinal,
		); err != nil {
			return nil, errors.New("scan capture dependency effects failed")
		}
		effects = append(effects, effect)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("read capture dependency effects failed")
	}
	return effects, nil
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
	if err := database.QueryRowContext(ctx, "SELECT synchro.synchro_compact('1 microsecond', 1)").Scan(&raw); err != nil {
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
	failures := runCleanupLifecycle(
		ctx,
		processCleanupStageTimeout(h.config.ShutdownTimeout),
		h.stopAdapter,
		h.closeDatabaseHandles,
		h.detachWorker,
		h.dropRunTopology,
	)
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
	if h.env.verified {
		if err := verifyEnvironmentArtifactIdentity(h.env); err != nil {
			failures = append(failures, err)
		}
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

func runCleanupLifecycle(parent context.Context, timeout time.Duration, operations ...func(context.Context) error) []error {
	var failures []error
	for _, operation := range operations {
		if err := runCleanupStage(parent, timeout, operation); err != nil {
			failures = append(failures, err)
		}
	}
	return failures
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

func (h *Harness) closeDatabaseHandles(ctx context.Context) error {
	if ctx == nil {
		return errors.New("database handle cleanup context is required")
	}
	h.databaseMu.Lock()
	handles := h.databaseHandles
	h.databaseHandles = nil
	h.databaseMu.Unlock()
	var failures []error
	for _, handle := range handles {
		if err := handle.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close harness database handle failed: %w", err))
		}
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	return nil
}

func (h *Harness) detachWorker(ctx context.Context) error {
	if h.postgres == nil || h.postgres.Exited() || h.workerDetached() {
		return nil
	}
	database, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return errors.New("connect for worker detachment failed")
	}
	if _, err := database.ExecContext(ctx, "ALTER SYSTEM SET synchro.auto_start = 'off'"); err != nil {
		_ = database.Close()
		return fmt.Errorf("disable synchro WAL worker auto-start failed: %w", err)
	}
	if err := database.Close(); err != nil {
		return fmt.Errorf("close worker detachment connection failed: %w", err)
	}
	if err := h.restartPostgres(ctx); err != nil {
		return fmt.Errorf("restart PostgreSQL without synchro WAL worker failed: %w", err)
	}
	h.databaseMu.Lock()
	h.workerDetachedState = true
	h.databaseMu.Unlock()
	return nil
}

func (h *Harness) workerDetached() bool {
	h.databaseMu.Lock()
	defer h.databaseMu.Unlock()
	return h.workerDetachedState
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
	if err := waitUntil(ctx, func(attemptContext context.Context) (bool, error) {
		rows, err := database.QueryContext(attemptContext, `
			SELECT slot_name, active_pid
			FROM pg_catalog.pg_replication_slots
			WHERE database = $1`, h.names.Database)
		if err != nil {
			return false, nil
		}
		var slots []struct {
			name      string
			activePID sql.NullInt64
		}
		for rows.Next() {
			var slot struct {
				name      string
				activePID sql.NullInt64
			}
			if err := rows.Scan(&slot.name, &slot.activePID); err != nil {
				_ = rows.Close()
				return false, nil
			}
			slots = append(slots, slot)
		}
		if err := rows.Close(); err != nil {
			return false, nil
		}
		if len(slots) == 0 {
			return true, nil
		}
		for _, slot := range slots {
			if slot.activePID.Valid {
				_, _ = database.ExecContext(attemptContext, "SELECT pg_terminate_backend($1)", slot.activePID.Int64)
				continue
			}
			if _, err := database.ExecContext(attemptContext, "SELECT pg_drop_replication_slot($1)", slot.name); err != nil {
				return false, nil
			}
		}
		return false, nil
	}); err != nil {
		return errors.New("drop isolated replication slot failed")
	}
	h.slotCreated = false
	return nil
}

func (h *Harness) dropDatabase(ctx context.Context) error {
	database, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return fmt.Errorf("connect for database cleanup failed: %w", err)
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "DROP DATABASE "+quoteIdentifier(h.names.Database)); err != nil {
		if !isActiveDatabaseError(err) {
			return databaseDropError(err)
		}
		// FORCE is a last resort for sessions outside the harness pools that
		// remain after the worker and every harness connection have stopped.
		if _, forceErr := database.ExecContext(ctx, "DROP DATABASE "+quoteIdentifier(h.names.Database)+" WITH (FORCE)"); forceErr != nil {
			return errors.Join(databaseDropError(err), databaseDropError(forceErr))
		}
	}
	h.databaseCreated = false
	return nil
}

func isActiveDatabaseError(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == "55006"
}

func databaseDropError(err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		return fmt.Errorf(
			"drop isolated PostgreSQL database failed: PostgreSQL error (SQLSTATE %s): %s: %w",
			postgresError.Code,
			postgresError.Message,
			err,
		)
	}
	return fmt.Errorf("drop isolated PostgreSQL database failed: %w", err)
}

func (h *Harness) dropRoles(ctx context.Context) error {
	database, err := h.openDatabase(ctx, "postgres", h.env.Admin, false)
	if err != nil {
		return errors.New("connect for role cleanup failed")
	}
	defer database.Close()
	var failures []error
	for _, role := range []string{h.worker.Username, h.env.Operator.Username, h.env.Observer.Username, h.env.Adapter.Username, h.sourceRole} {
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

func (log *boundedLog) addRedaction(value []byte) {
	if log == nil || len(value) == 0 {
		return
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, existing := range log.redactions {
		if bytes.Equal(existing, value) {
			return
		}
	}
	log.redactions = append(log.redactions, append([]byte(nil), value...))
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
		result = bytes.ReplaceAll(result, value, []byte("[REDACTED]"))
	}
	if log.truncated {
		for _, value := range log.redactions {
			result = redactTrailingSecretPrefix(result, value)
		}
	}
	return result
}

func redactTrailingSecretPrefix(data, secret []byte) []byte {
	maximum := len(secret) - 1
	if len(data) < maximum {
		maximum = len(data)
	}
	for size := maximum; size > 0; size-- {
		if bytes.Equal(data[len(data)-size:], secret[:size]) {
			redacted := make([]byte, 0, len(data)-size+len("[REDACTED]"))
			redacted = append(redacted, data[:len(data)-size]...)
			return append(redacted, "[REDACTED]"...)
		}
	}
	return data
}

func (log *boundedLog) isTruncated() bool {
	if log == nil {
		return false
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.truncated
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
		output := strings.TrimSpace(string(log.sanitizedBytes()))
		output = strings.ReplaceAll(output, "\r", " ")
		output = strings.ReplaceAll(output, "\n", " ")
		const maximum = 512
		if len(output) > maximum {
			const segment = maximum / 2
			output = output[:segment] + " ... " + output[len(output)-segment:]
		}
		if output != "" {
			return fmt.Errorf("bounded command failed: %s", output)
		}
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
		text = text[len(text)-maximum:]
	}
	return text
}

func (process *ownedProcess) diagnosticTextMatching(patterns ...string) string {
	if process == nil || process.log == nil {
		return ""
	}
	var matched []string
	for _, line := range strings.Split(string(process.log.sanitizedBytes()), "\n") {
		for _, pattern := range patterns {
			if strings.Contains(line, pattern) {
				matched = append(matched, strings.TrimSpace(line))
				break
			}
		}
	}
	text := strings.Join(matched, " ")
	text = strings.ReplaceAll(text, "\r", " ")
	const maximum = 4096
	if len(text) > maximum {
		text = text[len(text)-maximum:]
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

// StopPostmasterFast requests PostgreSQL fast shutdown without signaling its
// child backends directly. This preserves durable replication-slot state.
func (process *ownedProcess) StopPostmasterFast(ctx context.Context, timeout time.Duration) error {
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
	if err := process.command.Process.Signal(syscall.SIGINT); err != nil {
		return errors.New("request PostgreSQL fast shutdown failed")
	}
	if !process.waitForGroupExit(ctx, pid, timeout) {
		return errors.New("bounded PostgreSQL fast shutdown wait expired")
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
