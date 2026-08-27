package blackbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	nativePortableSeedArtifactID = "ARTDEF-PORTABLE-SEED-001"
	nativePortableSeedFixtureID  = "SEEDFIX-PORTABLE-SHARED-1000-001"
	nativePortableSeedRowCount   = 1000
)

// NativeArtifactConfig configures production portable-seed staging.
type NativeArtifactConfig struct {
	Harness          *Harness
	SeedToolPath     string
	StagingDirectory string
	WaitTimeout      time.Duration
}

// NativeArtifact stages the one closed portable seed through the production tool.
type NativeArtifact struct {
	harness          *Harness
	seedToolPath     string
	stagingDirectory string
	waitTimeout      time.Duration

	mu           sync.Mutex
	closed       bool
	seedPrepared bool
	staged       map[string]*nativeStagedArtifact
}

type nativeStagedArtifact struct {
	userID       string
	clientID     string
	path         string
	sha256       string
	resolvedStep scenarios.StepID
}

type nativePortableSeedPayload struct {
	UserID                 string `json:"user_id"`
	ClientID               string `json:"client_id"`
	PortableSeedArtifactID string `json:"portable_seed_artifact_id"`
	SeedFixtureID          string `json:"seed_fixture_id"`
}

// NewNativeArtifact creates one staging capability for a ready harness.
func NewNativeArtifact(config NativeArtifactConfig) (*NativeArtifact, error) {
	if config.Harness == nil || config.Harness.Source() == nil || config.Harness.Operator() == nil {
		return nil, errors.New("native artifact requires a ready black-box harness")
	}
	tool, err := filepath.Abs(config.SeedToolPath)
	if err != nil || tool == "" {
		return nil, errors.New("native artifact seed tool path is invalid")
	}
	toolInfo, err := os.Lstat(tool)
	if err != nil || toolInfo.Mode()&os.ModeSymlink != 0 || !toolInfo.Mode().IsRegular() || toolInfo.Mode().Perm()&0o111 == 0 {
		return nil, errors.New("native artifact seed tool is unavailable")
	}
	directory, err := filepath.Abs(config.StagingDirectory)
	if err != nil || directory == "" {
		return nil, errors.New("native artifact staging directory is invalid")
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil || directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() || directoryInfo.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("native artifact staging directory must be an existing private directory")
	}
	if config.WaitTimeout == 0 {
		config.WaitTimeout = nativeControllerWaitTimeout
	}
	if config.WaitTimeout <= 0 {
		return nil, errors.New("native artifact wait timeout is invalid")
	}
	return &NativeArtifact{
		harness:          config.Harness,
		seedToolPath:     tool,
		stagingDirectory: filepath.Clean(directory),
		waitTimeout:      config.WaitTimeout,
		staged:           make(map[string]*nativeStagedArtifact),
	}, nil
}

// StageStep generates one finalized seed for the operation target.
func (a *NativeArtifact) StageStep(ctx context.Context, operation scenarios.Operation) (NativeStepObservation, error) {
	if err := a.context(ctx); err != nil {
		return NativeStepObservation{}, err
	}
	if scenarios.OperationKey(operation) != "artifact/install-portable-seed" {
		return NativeStepObservation{}, fmt.Errorf("native artifact stage operation %q is unsupported", scenarios.OperationKey(operation))
	}
	if err := scenarios.ValidateOperation(operation); err != nil {
		return NativeStepObservation{}, fmt.Errorf("native artifact stage operation is invalid: %w", err)
	}
	var payload nativePortableSeedPayload
	if err := jsonstrict.Decode(operation.Payload, &payload); err != nil {
		return NativeStepObservation{}, errors.New("decode native portable seed operation failed")
	}
	if !validNativeIdentity(payload.UserID) || !validNativeIdentity(payload.ClientID) || payload.PortableSeedArtifactID != nativePortableSeedArtifactID || payload.SeedFixtureID != nativePortableSeedFixtureID {
		return NativeStepObservation{}, errors.New("native portable seed operation identity is invalid")
	}
	target := nativeArtifactTarget(payload.UserID, payload.ClientID)
	a.mu.Lock()
	if _, duplicate := a.staged[target]; duplicate {
		a.mu.Unlock()
		return NativeStepObservation{}, errors.New("native portable seed target is already staged")
	}
	prepared := a.seedPrepared
	a.mu.Unlock()
	if !prepared {
		if err := a.preparePortableSeed(ctx); err != nil {
			return NativeStepObservation{}, err
		}
		a.mu.Lock()
		a.seedPrepared = true
		a.mu.Unlock()
	}

	path := filepath.Join(a.stagingDirectory, nativeArtifactFilename(payload.UserID, payload.ClientID))
	if err := requireNativeArtifactPathAbsent(path); err != nil {
		return NativeStepObservation{}, err
	}
	if err := a.runSeedTool(ctx, path); err != nil {
		_ = removeNativeArtifactPath(path, "")
		return NativeStepObservation{}, err
	}
	digest, err := verifyNativeArtifactFile(path)
	if err != nil {
		_ = removeNativeArtifactPath(path, "")
		return NativeStepObservation{}, err
	}
	artifact := &nativeStagedArtifact{userID: payload.UserID, clientID: payload.ClientID, path: path, sha256: digest}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		_ = removeNativeArtifactPath(path, digest)
		return NativeStepObservation{}, errors.New("native artifact is closed")
	}
	if _, duplicate := a.staged[target]; duplicate {
		_ = removeNativeArtifactPath(path, digest)
		return NativeStepObservation{}, errors.New("native portable seed target is already staged")
	}
	a.staged[target] = artifact
	return nativeSuccess(), nil
}

func (a *NativeArtifact) preparePortableSeed(ctx context.Context) error {
	observer, err := a.harness.OpenObserver(ctx)
	if err != nil {
		return fmt.Errorf("open native portable seed source observation: %w", err)
	}
	var existing int
	err = observer.QueryRowContext(ctx, "SELECT count(*) FROM public.cf_global_items").Scan(&existing)
	closeErr := observer.Close()
	if err != nil || closeErr != nil {
		return errors.New("inspect native portable seed source failed")
	}
	if existing != 0 {
		return errors.New("native portable seed source is not empty")
	}

	transaction, err := a.harness.Source().BeginTx(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = transaction.Rollback()
		}
	}()
	recordIDs := make([]string, 0, nativePortableSeedRowCount)
	for ordinal := 1; ordinal <= nativePortableSeedRowCount; ordinal++ {
		identity := fmt.Sprintf("seed-%06d", ordinal)
		canonical, _ := json.Marshal(identity)
		recordID := nativeRuntimeUUID("portable-seed", string(canonical))
		result, err := transaction.ExecContext(ctx,
			"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
			recordID,
			identity,
		)
		if err != nil {
			return fmt.Errorf("insert native portable seed source row: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return errors.New("native portable seed source insert did not affect one row")
		}
		recordIDs = append(recordIDs, recordID)
	}
	if err := transaction.Commit(); err != nil {
		return err
	}
	committed = true
	return a.waitForPortableSeedWAL(ctx, recordIDs)
}

func (a *NativeArtifact) waitForPortableSeedWAL(ctx context.Context, recordIDs []string) error {
	deadline, cancel := context.WithTimeout(ctx, a.waitTimeout)
	defer cancel()
	for start := 0; start < len(recordIDs); start += 16 {
		end := start + 16
		if end > len(recordIDs) {
			end = len(recordIDs)
		}
		batch := recordIDs[start:end]
		for {
			observation, err := a.harness.Operator().ObserveWALRecordsForTable(deadline, "cf_global_items", batch)
			if err == nil && len(observation.Records) == len(batch) && observation.ContiguousAcknowledged && !observation.BlockingPoison {
				break
			}
			if err := waitNativePoll(deadline); err != nil {
				return errors.New("native portable seed rows did not become WAL-materialized")
			}
		}
	}
	return nil
}

func (a *NativeArtifact) runSeedTool(ctx context.Context, outputPath string) error {
	databaseURL := a.harness.databaseURL(a.harness.env.Admin)
	environment := append(scrubPostgresEnvironment(os.Environ()), "DATABASE_URL="+databaseURL)
	err := runBoundedCommand(
		ctx,
		a.seedToolPath,
		[]string{"--output", outputPath},
		environment,
		defaultProcessLogBytes,
		[][]byte{[]byte(databaseURL), a.harness.env.Admin.password},
	)
	if err != nil {
		return fmt.Errorf("generate native portable seed: %w", err)
	}
	return nil
}

// SeedDatabasePath resolves the only staged artifact for an authored client.
func (a *NativeArtifact) SeedDatabasePath(ctx context.Context, userID, clientID string, stepID scenarios.StepID) (string, error) {
	if err := a.context(ctx); err != nil {
		return "", err
	}
	if !validNativeIdentity(userID) || !validNativeIdentity(clientID) || stepID == "" {
		return "", errors.New("native portable seed resolver identity is invalid")
	}
	target := nativeArtifactTarget(userID, clientID)
	a.mu.Lock()
	artifact := a.staged[target]
	if artifact == nil {
		a.mu.Unlock()
		return "", errors.New("native portable seed target is not staged")
	}
	if artifact.resolvedStep != "" && artifact.resolvedStep != stepID {
		a.mu.Unlock()
		return "", errors.New("native portable seed was resolved by a different step")
	}
	artifact.resolvedStep = stepID
	path := artifact.path
	digest := artifact.sha256
	a.mu.Unlock()
	actual, err := verifyNativeArtifactFile(path)
	if err != nil || actual != digest {
		return "", errors.New("native portable seed changed after staging")
	}
	return path, nil
}

// Capture verifies every staged file before closing the artifact-state source.
func (a *NativeArtifact) Capture(ctx context.Context, clientKeys, sources []string) ([]NativeCaptureFacts, error) {
	if err := a.context(ctx); err != nil {
		return nil, err
	}
	if len(sources) != 1 || sources[0] != "artifact-state" {
		return nil, errors.New("native artifact capture supports only one artifact-state source")
	}
	a.mu.Lock()
	artifacts := make([]nativeStagedArtifact, 0, len(a.staged))
	for _, artifact := range a.staged {
		artifacts = append(artifacts, *artifact)
	}
	a.mu.Unlock()
	sort.Slice(artifacts, func(left, right int) bool { return artifacts[left].path < artifacts[right].path })
	for _, artifact := range artifacts {
		digest, err := verifyNativeArtifactFile(artifact.path)
		if err != nil || digest != artifact.sha256 {
			return nil, errors.New("native portable seed changed before artifact capture")
		}
	}
	return []NativeCaptureFacts{{Source: "artifact-state", StateFacts: scenarios.StateFacts{}}}, nil
}

// Close removes only unchanged files created by this artifact capability.
func (a *NativeArtifact) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("native artifact close context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	artifacts := make([]nativeStagedArtifact, 0, len(a.staged))
	for _, artifact := range a.staged {
		artifacts = append(artifacts, *artifact)
	}
	a.mu.Unlock()
	sort.Slice(artifacts, func(left, right int) bool { return artifacts[left].path > artifacts[right].path })
	var failures []error
	for _, artifact := range artifacts {
		if !nativePathWithin(artifact.path, a.stagingDirectory) {
			failures = append(failures, errors.New("native artifact cleanup path escaped its staging directory"))
			continue
		}
		if err := removeNativeArtifactPath(artifact.path, artifact.sha256); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (a *NativeArtifact) context(ctx context.Context) error {
	if a == nil || a.harness == nil {
		return errors.New("native artifact is unavailable")
	}
	if ctx == nil {
		return errors.New("native artifact context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return errors.New("native artifact is closed")
	}
	return nil
}

func nativeArtifactTarget(userID, clientID string) string {
	return userID + "\x00" + clientID
}

func nativeArtifactFilename(userID, clientID string) string {
	digest := sha256.Sum256([]byte("synchro:native-portable-seed:v1\x00" + userID + "\x00" + clientID))
	return "portable-seed-" + hex.EncodeToString(digest[:]) + ".sqlite"
}

func requireNativeArtifactPathAbsent(path string) error {
	for _, candidate := range []string{path, path + "-journal", path + "-wal", path + "-shm"} {
		if _, err := os.Lstat(candidate); err == nil {
			return errors.New("native portable seed output already exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.New("inspect native portable seed output failed")
		}
	}
	return nil
}

func verifyNativeArtifactFile(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 {
		return "", errors.New("native portable seed file is invalid")
	}
	for _, sidecar := range []string{path + "-journal", path + "-wal", path + "-shm"} {
		if _, err := os.Lstat(sidecar); err == nil {
			return "", errors.New("native portable seed has an unexpected SQLite sidecar")
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", errors.New("inspect native portable seed sidecar failed")
		}
	}
	digest, err := fileSHA256(path)
	if err != nil {
		return "", errors.New("hash native portable seed failed")
	}
	return digest, nil
}

func removeNativeArtifactPath(path, expectedDigest string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("native artifact cleanup refused an unsafe path")
	}
	if expectedDigest != "" {
		actual, err := fileSHA256(path)
		if err != nil || actual != expectedDigest {
			return errors.New("native artifact cleanup refused a changed file")
		}
	}
	if err := os.Remove(path); err != nil {
		return errors.New("remove native portable seed failed")
	}
	return syncDirectory(filepath.Dir(path))
}

func nativePathWithin(path, root string) bool {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(absoluteRoot, absolutePath)
	return err == nil && relative != ".." && !filepath.IsAbs(relative) && len(relative) > 0 && relative[:1] != "."
}
