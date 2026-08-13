package execution

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	// ErrInvalidCommandCapability reports a command capability that cannot
	// prove its locked repository source state.
	ErrInvalidCommandCapability = errors.New("evidence command capability is invalid")
	// ErrCommandCapabilityUsed reports a second process launch attempt through
	// one evidence command capability.
	ErrCommandCapabilityUsed = errors.New("evidence command capability was already used")
)

// CommandCapability permits one exact Make invocation for an evidence receipt.
// Its unexported state prevents callers from creating a substitute capability
// for an issuer that the Builder already owns.
type CommandCapability struct {
	state *commandCapabilityState
}

// CommandResult is the process observation used to complete an evidence receipt.
type CommandResult struct {
	Argv                 []string
	ExitCode             int
	StartedAt            time.Time
	CompletedAt          time.Time
	MakeExecutableSHA256 string
	SourceSnapshotSHA256 string
}

// CommandObservation is the authenticated projection of one Make process.
type CommandObservation struct {
	Argv                 []string  `json:"argv"`
	ExitCode             int       `json:"exit_code"`
	StartedAt            time.Time `json:"started_at"`
	CompletedAt          time.Time `json:"completed_at"`
	MakeExecutableSHA256 string    `json:"make_executable_sha256"`
	SourceSnapshotSHA256 string    `json:"source_snapshot_sha256"`
}

type commandObservation struct {
	result CommandResult
}

type commandCapabilityState struct {
	mu           sync.Mutex
	repoRoot     string
	sourceCommit string
	makefile     []byte
	gitPath      string
	makePath     string
	makeSHA256   string
	issuer       *issuerState
	used         bool
	observed     *commandObservation
}

// NewCommandCapability creates a repository-bound capability for one Builder.
// The capability accepts only an issuer that is created with this exact value.
func NewCommandCapability(repoRoot, sourceCommit string, makefile []byte) (CommandCapability, error) {
	root, err := commandRepositoryRoot(repoRoot)
	if err != nil {
		return CommandCapability{}, fmt.Errorf("%w: repository root: %v", ErrInvalidCommandCapability, err)
	}
	if !validCommandCommit(sourceCommit) {
		return CommandCapability{}, fmt.Errorf("%w: source commit", ErrInvalidCommandCapability)
	}
	gitPath, err := commandToolPath("git")
	if err != nil {
		return CommandCapability{}, fmt.Errorf("%w: git executable: %v", ErrInvalidCommandCapability, err)
	}
	makePath, err := commandToolPath("make")
	if err != nil {
		return CommandCapability{}, fmt.Errorf("%w: make executable: %v", ErrInvalidCommandCapability, err)
	}
	makeSHA256, err := commandFileSHA256(makePath)
	if err != nil {
		return CommandCapability{}, fmt.Errorf("%w: make executable digest: %v", ErrInvalidCommandCapability, err)
	}
	state := &commandCapabilityState{
		repoRoot:     root,
		sourceCommit: sourceCommit,
		makefile:     append([]byte(nil), makefile...),
		gitPath:      gitPath,
		makePath:     makePath,
		makeSHA256:   makeSHA256,
	}
	if err := state.verifySource(context.Background()); err != nil {
		return CommandCapability{}, err
	}
	return CommandCapability{state: state}, nil
}

// Execute launches the exact supplied Make argv in the locked repository.
// It records no environment values and discards command output.
func (c CommandCapability) Execute(ctx context.Context, argv []string) (CommandResult, error) {
	if ctx == nil {
		return CommandResult{}, fmt.Errorf("%w: context", ErrInvalidCommandCapability)
	}
	if err := ctx.Err(); err != nil {
		return CommandResult{}, err
	}
	if c.state == nil {
		return CommandResult{}, ErrInvalidCommandCapability
	}
	if len(argv) != 2 || argv[0] != "make" || argv[1] == "" {
		return CommandResult{}, fmt.Errorf("%w: argv", ErrInvalidCommandCapability)
	}

	c.state.mu.Lock()
	if c.state.used {
		c.state.mu.Unlock()
		return CommandResult{}, ErrCommandCapabilityUsed
	}
	if c.state.issuer == nil {
		c.state.mu.Unlock()
		return CommandResult{}, fmt.Errorf("%w: issuer binding", ErrInvalidCommandCapability)
	}
	c.state.used = true
	c.state.mu.Unlock()

	if err := c.state.verifySource(ctx); err != nil {
		return CommandResult{}, err
	}
	snapshot, snapshotSHA256, err := c.state.createSnapshot(ctx)
	if err != nil {
		return CommandResult{}, err
	}
	defer os.RemoveAll(snapshot)
	if err := c.state.verifySource(ctx); err != nil {
		return CommandResult{}, err
	}
	started := time.Now().Round(0).UTC()
	command := exec.CommandContext(ctx, c.state.makePath, "--no-builtin-rules", "--no-builtin-variables", "--makefile", "Makefile", argv[1])
	command.Args[0] = "make"
	command.Dir = snapshot
	command.Env = commandEnvironment()
	command.Stdin = nil
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	runErr := command.Run()
	completed := time.Now().Round(0).UTC()
	result := CommandResult{
		Argv:                 append([]string(nil), argv...),
		StartedAt:            started,
		CompletedAt:          completed,
		MakeExecutableSHA256: c.state.makeSHA256,
		SourceSnapshotSHA256: snapshotSHA256,
	}
	if command.ProcessState != nil {
		result.ExitCode = command.ProcessState.ExitCode()
	}
	if runErr != nil {
		if _, isExit := runErr.(*exec.ExitError); !isExit || result.ExitCode < 0 {
			return CommandResult{}, fmt.Errorf("%w: run make: %v", ErrInvalidCommandCapability, runErr)
		}
	}
	if result.ExitCode < 0 || completed.Before(started) || len(result.Argv) != 2 || result.Argv[0] != "make" || result.Argv[1] != argv[1] {
		return CommandResult{}, fmt.Errorf("%w: process observation", ErrInvalidCommandCapability)
	}
	if err := c.state.verifySource(ctx); err != nil {
		return result, err
	}

	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.issuer == nil || c.state.observed != nil {
		return CommandResult{}, fmt.Errorf("%w: issuer observation", ErrInvalidCommandCapability)
	}
	c.state.observed = &commandObservation{result: cloneCommandResult(result)}
	return result, nil
}

func (c CommandCapability) matchesIssuer(issuer *issuerState) bool {
	if c.state == nil || issuer == nil {
		return false
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	return c.state.issuer == issuer && issuer.command == c.state
}

// MatchesIssuer reports whether this capability is bound to the exact issuer.
func (c CommandCapability) MatchesIssuer(issuer ReceiptIssuer) bool {
	return c.matchesIssuer(issuer.state)
}

func (c CommandCapability) bindIssuer(issuer *issuerState) error {
	if c.state == nil || issuer == nil {
		return ErrInvalidCommandCapability
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.used || c.state.issuer != nil {
		return fmt.Errorf("%w: issuer binding", ErrInvalidCommandCapability)
	}
	c.state.issuer = issuer
	return nil
}

func (c CommandCapability) validates(fields ReceiptFields, issuer *issuerState) bool {
	if c.state == nil || issuer == nil {
		return false
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if c.state.issuer != issuer || c.state.observed == nil {
		return false
	}
	return sameCommandArgv(c.state.observed.result.Argv, fields.Argv) && sameCommandResult(c.state.observed.result, fields.Command)
}

func (s *commandCapabilityState) verifySource(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalidCommandCapability
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := commandRepositoryClean(ctx, s.gitPath, s.repoRoot); err != nil {
		return fmt.Errorf("%w: clean worktree: %v", ErrInvalidCommandCapability, err)
	}
	commit, err := commandRepositoryCommit(ctx, s.gitPath, s.repoRoot)
	if err != nil || commit != s.sourceCommit {
		return fmt.Errorf("%w: source commit", ErrInvalidCommandCapability)
	}
	current, err := commandMakefile(s.repoRoot)
	if err != nil || !sameCommandBytes(current, s.makefile) {
		return fmt.Errorf("%w: Makefile bytes", ErrInvalidCommandCapability)
	}
	committed, err := commandRepositoryBlob(ctx, s.gitPath, s.repoRoot, s.sourceCommit, "Makefile")
	if err != nil || !sameCommandBytes(committed, s.makefile) {
		return fmt.Errorf("%w: Makefile source commit binding", ErrInvalidCommandCapability)
	}
	return nil
}

func (s *commandCapabilityState) createSnapshot(ctx context.Context) (string, string, error) {
	if s == nil || ctx == nil || s.gitPath == "" {
		return "", "", ErrInvalidCommandCapability
	}
	root, err := os.MkdirTemp("", "synchro-evidence-source-")
	if err != nil {
		return "", "", fmt.Errorf("%w: create source snapshot: %v", ErrInvalidCommandCapability, err)
	}
	archive, err := commandRepositoryArchive(ctx, s.gitPath, s.repoRoot, s.sourceCommit)
	if err != nil {
		_ = os.RemoveAll(root)
		return "", "", fmt.Errorf("%w: archive source commit: %v", ErrInvalidCommandCapability, err)
	}
	digest, err := extractCommandSnapshot(root, archive)
	if err != nil {
		_ = os.RemoveAll(root)
		return "", "", fmt.Errorf("%w: extract source snapshot: %v", ErrInvalidCommandCapability, err)
	}
	makefile, err := commandMakefile(root)
	if err != nil || !sameCommandBytes(makefile, s.makefile) {
		_ = os.RemoveAll(root)
		return "", "", fmt.Errorf("%w: snapshot Makefile binding", ErrInvalidCommandCapability)
	}
	return root, digest, nil
}

func commandRepositoryRoot(root string) (string, error) {
	if root == "" || strings.IndexByte(root, 0) >= 0 {
		return "", errors.New("repository root is invalid")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", errors.New("repository root is not a directory")
	}
	return filepath.Clean(resolved), nil
}

func commandRepositoryClean(ctx context.Context, gitPath, root string) error {
	command := exec.CommandContext(ctx, gitPath, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=no", "--ignore-submodules=none")
	command.Dir = root
	command.Stdin = nil
	command.Env = commandGitEnvironment()
	output, err := command.Output()
	if err != nil {
		return err
	}
	if len(output) != 0 {
		return errors.New("repository worktree is dirty")
	}
	return nil
}

func commandRepositoryCommit(ctx context.Context, gitPath, root string) (string, error) {
	command := exec.CommandContext(ctx, gitPath, "rev-parse", "HEAD")
	command.Dir = root
	command.Stdin = nil
	command.Env = commandGitEnvironment()
	output, err := command.Output()
	if err != nil {
		return "", err
	}
	commit := strings.TrimSpace(string(output))
	if !validCommandCommit(commit) {
		return "", errors.New("repository commit is invalid")
	}
	return commit, nil
}

func commandRepositoryBlob(ctx context.Context, gitPath, root, commit, path string) ([]byte, error) {
	if !validCommandCommit(commit) || path != "Makefile" {
		return nil, errors.New("repository blob binding is invalid")
	}
	command := exec.CommandContext(ctx, gitPath, "--no-pager", "show", commit+":"+path)
	command.Dir = root
	command.Stdin = nil
	command.Env = commandGitEnvironment()
	return command.Output()
}

func commandRepositoryArchive(ctx context.Context, gitPath, root, commit string) ([]byte, error) {
	if !validCommandCommit(commit) {
		return nil, errors.New("repository archive binding is invalid")
	}
	command := exec.CommandContext(ctx, gitPath, "archive", "--format=tar", commit)
	command.Dir = root
	command.Stdin = nil
	command.Env = commandGitEnvironment()
	return command.Output()
}

func extractCommandSnapshot(root string, archive []byte) (string, error) {
	reader := tar.NewReader(bytes.NewReader(archive))
	files := make(map[string]string)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		if header.Typeflag == tar.TypeXHeader || header.Typeflag == tar.TypeXGlobalHeader {
			continue
		}
		name := header.Name
		if header.Typeflag == tar.TypeDir {
			name = strings.TrimSuffix(name, "/")
		}
		path := filepath.ToSlash(filepath.Clean(filepath.FromSlash(name)))
		if path != name || path == "." || path == ".." || strings.HasPrefix(path, "../") || filepath.IsAbs(name) {
			return "", errors.New("source archive path is invalid")
		}
		target := filepath.Join(root, filepath.FromSlash(path))
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return "", err
			}
		case tar.TypeReg, tar.TypeRegA:
			if header.Size < 0 {
				return "", errors.New("source archive file size is invalid")
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return "", err
			}
			data, err := io.ReadAll(io.LimitReader(reader, header.Size+1))
			if err != nil || int64(len(data)) != header.Size {
				return "", errors.New("source archive file is incomplete")
			}
			mode := os.FileMode(0o600)
			if header.FileInfo().Mode()&0o111 != 0 {
				mode = 0o700
			}
			if err := os.WriteFile(target, data, mode); err != nil {
				return "", err
			}
			digest := sha256.Sum256(data)
			files[path] = hex.EncodeToString(digest[:])
		default:
			return "", errors.New("source archive contains a nonregular entry")
		}
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	hash := sha256.New()
	hash.Write([]byte("synchro:conformance:source-snapshot:v1"))
	for _, path := range paths {
		hash.Write([]byte{0})
		hash.Write([]byte(path))
		hash.Write([]byte{0})
		hash.Write([]byte(files[path]))
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func commandToolPath(name string) (string, error) {
	if name != "git" && name != "make" {
		return "", errors.New("command tool is not allowed")
	}
	path := filepath.Join("/usr/bin", name)
	if runtime.GOOS == "windows" {
		return "", errors.New("command tool platform is not supported")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return "", errors.New("command tool is not an executable regular file")
	}
	return path, nil
}

func commandFileSHA256(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("command file is not regular")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	current, err := os.Lstat(path)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(info, current) {
		return "", errors.New("command file identity changed")
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

// RunningExecutableSHA256 measures the current process executable.
func RunningExecutableSHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return commandFileSHA256(resolved)
}

func commandEnvironment() []string {
	forbidden := map[string]struct{}{
		"BASH_ENV": {}, "CDPATH": {}, "ENV": {}, "GIT_CONFIG": {},
		"DETOX_ARGS": {}, "GOFLAGS": {}, "GOWORK": {}, "GO_TEST_ARGS": {}, "GO_TEST_PKGS": {},
		"GIT_CONFIG_COUNT": {}, "GIT_CONFIG_GLOBAL": {}, "GIT_CONFIG_NOSYSTEM": {},
		"DYLD_INSERT_LIBRARIES": {}, "DYLD_LIBRARY_PATH": {}, "LD_AUDIT": {}, "LD_LIBRARY_PATH": {}, "LD_PRELOAD": {},
		"GIT_DIR": {}, "GIT_INDEX_FILE": {}, "GIT_OBJECT_DIRECTORY": {}, "GIT_WORK_TREE": {},
		"GNUMAKEFLAGS": {}, "GRADLE_TEST_ARGS": {}, "MAKE": {}, "MAKEFILES": {}, "MAKEFLAGS": {}, "MAKELEVEL": {},
		"MAKEOVERRIDES": {}, "MFLAGS": {}, "SHELL": {},
	}
	environment := make([]string, 0, len(os.Environ())+3)
	for _, value := range os.Environ() {
		name, _, found := strings.Cut(value, "=")
		if !found {
			continue
		}
		if _, rejected := forbidden[name]; rejected || strings.HasPrefix(name, "DYLD_") || strings.HasPrefix(name, "GIT_") || strings.HasPrefix(name, "LD_") {
			continue
		}
		environment = append(environment, value)
	}
	environment = replaceCommandEnvironment(environment, "LANG", "C")
	environment = replaceCommandEnvironment(environment, "LC_ALL", "C")
	environment = replaceCommandEnvironment(environment, "MAKE", "/usr/bin/make")
	environment = replaceCommandEnvironment(environment, "PATH", commandPath())
	environment = replaceCommandEnvironment(environment, "SHELL", "/bin/sh")
	return environment
}

func commandPath() string {
	paths := []string{"/usr/local/go/bin", "/usr/local/cargo/bin", "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"}
	if home, err := os.UserHomeDir(); err == nil && filepath.IsAbs(home) {
		paths = append([]string{filepath.Join(home, ".cargo", "bin")}, paths...)
	}
	return strings.Join(paths, string(os.PathListSeparator))
}

func commandGitEnvironment() []string {
	environment := commandEnvironment()
	return append(environment,
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_NO_REPLACE_OBJECTS=1",
		"GIT_OPTIONAL_LOCKS=0",
		"GIT_TERMINAL_PROMPT=0",
	)
}

func replaceCommandEnvironment(environment []string, name, value string) []string {
	prefix := name + "="
	result := make([]string, 0, len(environment)+1)
	for _, item := range environment {
		if !strings.HasPrefix(item, prefix) {
			result = append(result, item)
		}
	}
	return append(result, prefix+value)
}

func cloneCommandResult(result CommandResult) CommandResult {
	result.Argv = append([]string(nil), result.Argv...)
	return result
}

func sameCommandResult(observed CommandResult, claimed CommandObservation) bool {
	return sameCommandArgv(observed.Argv, claimed.Argv) &&
		observed.ExitCode == claimed.ExitCode &&
		observed.StartedAt.Equal(claimed.StartedAt) &&
		observed.CompletedAt.Equal(claimed.CompletedAt) &&
		observed.MakeExecutableSHA256 == claimed.MakeExecutableSHA256 &&
		observed.SourceSnapshotSHA256 == claimed.SourceSnapshotSHA256
}

// Observation returns an isolated receipt projection of this process result.
func (r CommandResult) Observation() CommandObservation {
	return CommandObservation{
		Argv:                 append([]string(nil), r.Argv...),
		ExitCode:             r.ExitCode,
		StartedAt:            r.StartedAt,
		CompletedAt:          r.CompletedAt,
		MakeExecutableSHA256: r.MakeExecutableSHA256,
		SourceSnapshotSHA256: r.SourceSnapshotSHA256,
	}
}

// SourceSnapshotSHA256 returns the deterministic tree digest for one commit.
func SourceSnapshotSHA256(ctx context.Context, repoRoot, sourceCommit string) (string, error) {
	root, err := commandRepositoryRoot(repoRoot)
	if err != nil || !validCommandCommit(sourceCommit) {
		return "", fmt.Errorf("%w: source snapshot binding", ErrInvalidCommandCapability)
	}
	gitPath, err := commandToolPath("git")
	if err != nil {
		return "", fmt.Errorf("%w: git executable: %v", ErrInvalidCommandCapability, err)
	}
	archive, err := commandRepositoryArchive(ctx, gitPath, root, sourceCommit)
	if err != nil {
		return "", fmt.Errorf("%w: archive source commit: %v", ErrInvalidCommandCapability, err)
	}
	temporary, err := os.MkdirTemp("", "synchro-evidence-source-digest-")
	if err != nil {
		return "", fmt.Errorf("%w: source snapshot digest: %v", ErrInvalidCommandCapability, err)
	}
	defer os.RemoveAll(temporary)
	digest, err := extractCommandSnapshot(temporary, archive)
	if err != nil {
		return "", fmt.Errorf("%w: source snapshot digest: %v", ErrInvalidCommandCapability, err)
	}
	return digest, nil
}

func commandMakefile(root string) ([]byte, error) {
	path := filepath.Join(root, "Makefile")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("Makefile is not a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	current, err := os.Lstat(path)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(info, current) {
		return nil, errors.New("Makefile identity changed")
	}
	return data, nil
}

func validCommandCommit(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func sameCommandArgv(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sameCommandBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
