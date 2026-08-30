package swift

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

const MaximumMessageBytes = 1 << 20

// Config identifies the Swift runner and direct platform settings.
// StartSession uses only RunnerPath.
type Config struct {
	RunnerPath                   string
	ApplicationDatabaseDirectory string
	ServerURL                    string
	AuthToken                    func(context.Context, Client) (string, error)
	Platform                     string
	AppVersion                   string
	PullPageSize                 int
	PushBatchSize                int
}

// Request is one Swift runner command.
type Request = runnerCommand

// LocalAction is one direct application write through the Swift runner.
type LocalAction = runnerLocalAction

// RowSelector identifies one application row for Swift runner inspection.
type RowSelector = runnerRowSelector

// Result contains one bounded Swift runner response.
type Result = runnerResult

// Failure contains one validated raw Swift client failure.
type Failure = runnerFailure

// SchemaRef identifies one Swift client schema.
type SchemaRef = schemaRef

// TransportObservation is one immutable accepted transport observation.
type TransportObservation = transportObservation

// TransportObservationSnapshot is one complete transport observation range.
type TransportObservationSnapshot = transportObservationSnapshot

// TransportRequestFacts contains one transport request fact set.
type TransportRequestFacts = transportRequestFacts

// TransportRebuildResponseFacts contains one rebuild response fact set.
type TransportRebuildResponseFacts = transportRebuildResponseFacts

// TransportPullResponseFacts contains one pull response fact set.
type TransportPullResponseFacts = transportPullResponseFacts

// ScopeState is one captured scope state record.
type ScopeState = scopeStateRecord

// ScopeRow is one captured scope row record.
type ScopeRow = scopeRowRecord

// RowMetadata is one captured row metadata record.
type RowMetadata = rowMetadataRecord

// RebuildAttempt is one captured rebuild attempt record.
type RebuildAttempt = rebuildAttemptRecord

// RebuildReceipt is one captured raw rebuild receipt.
type RebuildReceipt = rebuildReceiptRecord

// RetainedMutation is one captured retained mutation.
type RetainedMutation = retainedMutation

// RetainedField is one captured retained mutation field.
type RetainedField = retainedField

// RetainedRejection is one captured retained mutation rejection.
type RetainedRejection = retainedRejection

// WireMutation is one wire mutation in a captured rejection.
type WireMutation = wireMutation

// WireRejection is one wire rejection in a captured rejection.
type WireRejection = wireRejection

// Event is one captured Swift client event.
type Event = eventRecord

// CommandError reports one bounded Swift runner error code.
type CommandError = runnerCommandError

// Session owns one synchro-native-runner child process.
type Session struct {
	mu      sync.Mutex
	closed  bool
	process *runnerProcess
}

// stderrReport reports what the runner wrote to stderr. A bounded protocol
// error code cannot name the underlying failure, so a failing step reads it
// here instead of reporting an opaque code.
func (s *Session) stderrReport() string {
	if s == nil {
		return "session is unavailable"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	report := s.process.stderrContents()
	if report == "" {
		return "runner reported nothing on stderr"
	}
	return report
}

// StartSession starts one existing synchro-native-runner executable.
func StartSession(ctx context.Context, config Config) (*Session, error) {
	if err := requireContext(ctx, "Swift session context is required"); err != nil {
		return nil, err
	}
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	process, err := startRunnerProcess(ctx, normalized.RunnerPath)
	if err != nil {
		return nil, err
	}
	return &Session{process: process}, nil
}

func normalizeConfig(config Config) (Config, error) {
	if config.RunnerPath == "" {
		return Config{}, errors.New("Swift runner path is required")
	}
	path, err := filepath.Abs(config.RunnerPath)
	if err != nil {
		return Config{}, errors.New("Swift runner path is invalid")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return Config{}, errors.New("Swift runner is unavailable")
	}
	config.RunnerPath = filepath.Clean(path)
	return config, nil
}

// Execute sends one strict newline-delimited command and validates its response.
func (s *Session) Execute(ctx context.Context, request Request) (Result, error) {
	if s == nil {
		return Result{}, errors.New("Swift session is nil")
	}
	process, err := s.currentProcess()
	if err != nil {
		return Result{}, err
	}
	return process.send(ctx, request)
}

func (s *Session) currentProcess() (*runnerProcess, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.process == nil {
		return nil, errors.New("Swift session is unavailable")
	}
	return s.process, nil
}

// Checkpoint returns the latest accepted immutable transport checkpoint.
func (s *Session) Checkpoint() uint64 {
	process := s.inspectionProcess()
	if process == nil {
		return 0
	}
	return process.transportCheckpointValue()
}

// ObservationsAfter returns immutable accepted observations after checkpoint.
func (s *Session) ObservationsAfter(checkpoint uint64) ([]TransportObservation, error) {
	process := s.inspectionProcess()
	if process == nil {
		return nil, errors.New("Swift session is unavailable")
	}
	return process.transportObservationsAfter(checkpoint)
}

func (s *Session) inspectionProcess() *runnerProcess {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	process := s.process
	s.mu.Unlock()
	return process
}

// Kill sends SIGKILL to the child and confirms that it exited from that signal.
func (s *Session) Kill(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if err := requireContext(ctx, "Swift kill context is required"); err != nil {
		return err
	}
	process, err := s.currentProcess()
	if err != nil {
		return err
	}
	return process.killSIGKILLContext(ctx)
}

// Close closes the child process and waits for its exit.
func (s *Session) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if err := requireContext(ctx, "Swift session close context is required"); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	process := s.process
	s.mu.Unlock()
	if process == nil {
		return nil
	}
	return process.close(ctx)
}

func requireContext(ctx context.Context, message string) error {
	if ctx == nil {
		return errors.New(message)
	}
	return ctx.Err()
}
