// synchro-local-postgres exposes the existing black-box PostgreSQL provisioner
// to the Make local target and the black-box harness workflow.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const (
	localStartupTimeout   = 90 * time.Second
	localShutdownTimeout  = 15 * time.Second
	localPollInterval     = 250 * time.Millisecond
	lifecycleMessageBytes = 64 << 10
	lifecycleStateName    = "lifecycle-state.json"
)

var lifecycleRunIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

type lifecycleState struct {
	RunID          string `json:"run_id"`
	ControlAddress string `json:"control_address"`
	Destroyed      bool   `json:"destroyed"`
}

type lifecycleRequest struct {
	Operation string `json:"operation"`
	RunID     string `json:"run_id"`
}

type lifecycleResponse struct {
	RunID             string `json:"run_id"`
	AttachDatabaseURL string `json:"attach_database_url"`
	Destroyed         bool   `json:"destroyed"`
	Error             string `json:"error,omitempty"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "synchro-local-postgres: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if len(args) == 0 {
		return errors.New("command is required")
	}
	switch args[0] {
	case "start":
		return runStart(ctx, args[1:])
	case "prepare":
		return runPrepare(ctx, args[1:])
	case "lifecycle":
		return runLifecycle(ctx, args[1:])
	default:
		return errors.New("unknown command")
	}
}

func runStart(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("start", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	pg18BinDir := flags.String("pg18-bin-dir", "", "PostgreSQL 18 binary directory")
	extensionArtifact := flags.String("extension-artifact", "", "verified PostgreSQL extension bundle")
	adapterArtifact := flags.String("adapter-artifact", "", "verified adapter artifact")
	stateDir := flags.String("state-dir", "", "private state directory")
	tempParent := flags.String("temp-parent", "", "private temporary directory parent")
	urlFile := flags.String("url-file", "", "administrator URL output file")
	attachEnvironmentFile := flags.String("attach-environment-file", "", "attach-mode environment output file")
	listen := flags.String("listen", "127.0.0.1", "PostgreSQL listen address")
	if err := flags.Parse(args); err != nil {
		return errors.New("start flags are invalid")
	}
	if flags.NArg() != 0 || *pg18BinDir == "" || *extensionArtifact == "" || *adapterArtifact == "" || *stateDir == "" || *tempParent == "" || *urlFile == "" || *attachEnvironmentFile == "" {
		return errors.New("start requires --pg18-bin-dir, --extension-artifact, --adapter-artifact, --state-dir, --temp-parent, --url-file, and --attach-environment-file")
	}
	resolvedInstallationLock, err := blackbox.PostgreSQLInstallationLockPath(ctx, *pg18BinDir)
	if err != nil {
		return errors.New("derive local provisioner installation lock failed")
	}
	if err := ensurePrivateDirectory(*stateDir); err != nil {
		return err
	}
	stateRoot, err := filepath.Abs(*stateDir)
	if err != nil {
		return errors.New("local provisioner state directory is invalid")
	}
	executable, err := os.Executable()
	if err != nil {
		return errors.New("resolve local provisioner executable failed")
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		return errors.New("resolve local provisioner executable failed")
	}
	lifecycleCommand, err := localLifecycleCommand(executable, stateRoot)
	if err != nil {
		return err
	}
	credentials, err := createCredentials(*stateDir)
	if err != nil {
		return err
	}
	defer credentials.remove()
	restoreEnvironment := setEnvironment(map[string]string{
		"SYNCHRO_CONFORMANCE_PG18_BINDIR":            *pg18BinDir,
		"SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT":     *extensionArtifact,
		"SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT":       *adapterArtifact,
		"SYNCHRO_CONFORMANCE_ADMIN_USER":             credentials.adminUser,
		"SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE":    credentials.adminPassword,
		"SYNCHRO_CONFORMANCE_ADAPTER_USER":           credentials.adapterUser,
		"SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE":  credentials.adapterPassword,
		"SYNCHRO_CONFORMANCE_OBSERVER_USER":          credentials.observerUser,
		"SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE": credentials.observerPassword,
		"SYNCHRO_CONFORMANCE_WORKER_USER":            credentials.workerUser,
		"SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE":   credentials.workerPassword,
		"SYNCHRO_CONFORMANCE_OPERATOR_USER":          credentials.operatorUser,
		"SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE": credentials.operatorPassword,
		"SYNCHRO_CONFORMANCE_JWT_SECRET_FILE":        credentials.jwtSecret,
		"SYNCHRO_CONFORMANCE_INSTALL_LOCK":           resolvedInstallationLock,
	})
	defer restoreEnvironment()
	environment, err := blackbox.LoadLocalEnvironment()
	if err != nil {
		return fmt.Errorf("load local provisioner environment: %w", err)
	}
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{
		Environment:     environment,
		TempParent:      *tempParent,
		ListenAddress:   strings.TrimSpace(*listen),
		SkipAdapter:     true,
		StartupTimeout:  localStartupTimeout,
		ShutdownTimeout: localShutdownTimeout,
	})
	if err != nil {
		return fmt.Errorf("provision local PostgreSQL: %w", err)
	}
	runID, err := createRunID()
	if err != nil {
		_ = harness.Close(context.Background())
		return err
	}
	listener, err := openLifecycleListener()
	if err != nil {
		_ = harness.Close(context.Background())
		return err
	}
	defer func() {
		_ = listener.Close()
	}()
	if err := writeLifecycleState(stateRoot, lifecycleState{
		RunID:          runID,
		ControlAddress: listener.Addr().String(),
	}); err != nil {
		_ = harness.Close(context.Background())
		return err
	}
	url := harness.DatabaseURL()
	if url == "" {
		_ = harness.Close(context.Background())
		return errors.New("local PostgreSQL administrator URL is unavailable")
	}
	if err := writePrivateFile(*urlFile, []byte(url+"\n")); err != nil {
		_ = harness.Close(context.Background())
		return fmt.Errorf("write local PostgreSQL URL: %w", err)
	}
	attachURL, err := lifecycleAttachDatabaseURL(url)
	if err != nil {
		_ = harness.Close(context.Background())
		return err
	}
	if err := writePrivateFile(*attachEnvironmentFile, []byte(attachEnvironment(attachURL, runID, lifecycleCommand, credentials))); err != nil {
		_ = harness.Close(context.Background())
		return fmt.Errorf("write attach environment: %w", err)
	}
	destroyed, serveErr := serveLifecycle(ctx, listener, stateRoot, runID, harness)
	var closeErr error
	if !destroyed {
		closeContext, cancel := context.WithTimeout(context.Background(), localShutdownTimeout)
		closeErr = harness.Close(closeContext)
		cancel()
		if closeErr == nil {
			closeErr = writeLifecycleState(stateRoot, lifecycleState{
				RunID:     runID,
				Destroyed: true,
			})
		}
	}
	credentials.remove()
	_ = os.Remove(*urlFile)
	_ = os.Remove(*attachEnvironmentFile)
	if serveErr != nil {
		return serveErr
	}
	if closeErr != nil {
		return fmt.Errorf("close local PostgreSQL: %w", closeErr)
	}
	return nil
}

func localLifecycleCommand(executable, stateDir string) ([]string, error) {
	if !filepath.IsAbs(stateDir) {
		return nil, errors.New("lifecycle command state directory must be absolute")
	}
	encoded, err := json.Marshal([]string{executable, "lifecycle", "--state-dir", stateDir})
	if err != nil {
		return nil, errors.New("encode local lifecycle command failed")
	}
	command, err := blackbox.ParseAttachLifecycleCommand(string(encoded))
	if err != nil {
		return nil, fmt.Errorf("local lifecycle command is invalid: %w", err)
	}
	return command, nil
}

// attachEnvironment references credential files through SYNCHRO_ATTACH_DIR,
// so a copied attach bundle works from any consumer directory.
func attachEnvironment(url, runID string, lifecycleCommand []string, credentials localCredentials) string {
	commandJSON, err := json.Marshal(lifecycleCommand)
	if err != nil {
		panic("marshal fixed lifecycle command: " + err.Error())
	}
	return strings.Join([]string{
		environmentAssignment("SYNCHRO_CONFORMANCE_ATTACH_DATABASE_URL", url),
		environmentAssignment("SYNCHRO_CONFORMANCE_ATTACH_RUN_ID", runID),
		environmentAssignment("SYNCHRO_CONFORMANCE_ATTACH_LIFECYCLE_COMMAND", string(commandJSON)),
		environmentAssignment("SYNCHRO_CONFORMANCE_ATTACH_DESTROY_ON_CLOSE", "false"),
		environmentAssignment("SYNCHRO_CONFORMANCE_ADMIN_USER", credentials.adminUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE", credentials.adminPassword),
		environmentAssignment("SYNCHRO_CONFORMANCE_ADAPTER_USER", credentials.adapterUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE", credentials.adapterPassword),
		environmentAssignment("SYNCHRO_CONFORMANCE_OBSERVER_USER", credentials.observerUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE", credentials.observerPassword),
		environmentAssignment("SYNCHRO_CONFORMANCE_WORKER_USER", credentials.workerUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE", credentials.workerPassword),
		environmentAssignment("SYNCHRO_CONFORMANCE_OPERATOR_USER", credentials.operatorUser),
		attachDirAssignment("SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE", credentials.operatorPassword),
		attachDirAssignment("SYNCHRO_CONFORMANCE_JWT_SECRET_FILE", credentials.jwtSecret),
		"",
	}, "\n")
}

func createRunID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", errors.New("generate local provisioner run identity failed")
	}
	return hex.EncodeToString(value[:]), nil
}

func openLifecycleListener() (*net.TCPListener, error) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		return nil, errors.New("listen for local provisioner lifecycle commands failed")
	}
	return listener, nil
}

func lifecycleAttachDatabaseURL(value string) (string, error) {
	config, err := pgconn.ParseConfig(value)
	if err != nil || config.Host == "" || config.Port == 0 || config.Database == "" {
		return "", errors.New("local provisioner attach URL is invalid")
	}
	result := url.URL{
		Scheme:   "postgres",
		Host:     net.JoinHostPort(config.Host, strconv.Itoa(int(config.Port))),
		Path:     "/" + config.Database,
		RawQuery: "sslmode=disable",
	}
	return result.String(), nil
}

func serveLifecycle(ctx context.Context, listener *net.TCPListener, stateDir, runID string, harness *blackbox.Harness) (bool, error) {
	if ctx == nil || listener == nil || harness == nil || !lifecycleRunIDPattern.MatchString(runID) {
		return false, errors.New("local provisioner lifecycle server is invalid")
	}
	for {
		if err := listener.SetDeadline(time.Now().Add(localPollInterval)); err != nil {
			return false, errors.New("set local provisioner lifecycle deadline failed")
		}
		connection, err := listener.AcceptTCP()
		if err != nil {
			if networkError, ok := err.(net.Error); ok && networkError.Timeout() {
				if ctx.Err() != nil {
					return false, nil
				}
				continue
			}
			return false, errors.New("accept local provisioner lifecycle command failed")
		}
		if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			_ = connection.Close()
			return false, errors.New("bound local provisioner lifecycle request failed")
		}
		destroyed, err := handleLifecycleConnection(connection, stateDir, runID, harness)
		_ = connection.Close()
		if err != nil {
			return false, err
		}
		if destroyed {
			return true, nil
		}
	}
}

func handleLifecycleConnection(connection *net.TCPConn, stateDir, runID string, harness *blackbox.Harness) (bool, error) {
	data, err := io.ReadAll(io.LimitReader(connection, lifecycleMessageBytes+1))
	if err != nil || len(data) > lifecycleMessageBytes {
		return false, writeLifecycleWireResponse(connection, lifecycleResponse{Error: "lifecycle request is invalid"})
	}
	var request lifecycleRequest
	if err := decodeStrictJSON(data, &request); err != nil || request.RunID != runID || !lifecycleRunIDPattern.MatchString(request.RunID) {
		return false, writeLifecycleWireResponse(connection, lifecycleResponse{Error: "lifecycle run identity is invalid"})
	}
	operationContext, cancel := context.WithTimeout(context.Background(), localStartupTimeout+localShutdownTimeout)
	defer cancel()
	switch request.Operation {
	case "restart":
		if err := harness.RestartPostgres(operationContext); err != nil {
			if writeErr := writeLifecycleWireResponse(connection, lifecycleResponse{RunID: runID, Error: "lifecycle restart failed"}); writeErr != nil {
				return false, writeErr
			}
			return false, nil
		}
		lifecycleURL, err := lifecycleAttachDatabaseURL(harness.DatabaseURL())
		if err != nil {
			if writeErr := writeLifecycleWireResponse(connection, lifecycleResponse{RunID: runID, Error: "lifecycle restart URL failed"}); writeErr != nil {
				return false, writeErr
			}
			return false, nil
		}
		return false, writeLifecycleWireResponse(connection, lifecycleResponse{
			RunID:             runID,
			AttachDatabaseURL: lifecycleURL,
		})
	case "destroy":
		if err := harness.Close(operationContext); err != nil {
			if writeErr := writeLifecycleWireResponse(connection, lifecycleResponse{RunID: runID, Error: "lifecycle destroy failed"}); writeErr != nil {
				return false, writeErr
			}
			return false, nil
		}
		if err := writeLifecycleState(stateDir, lifecycleState{
			RunID:     runID,
			Destroyed: true,
		}); err != nil {
			_ = writeLifecycleWireResponse(connection, lifecycleResponse{RunID: runID, Error: "lifecycle state update failed"})
			return false, errors.New("persist destroyed lifecycle state failed")
		}
		return true, writeLifecycleWireResponse(connection, lifecycleResponse{RunID: runID, Destroyed: true})
	default:
		return false, writeLifecycleWireResponse(connection, lifecycleResponse{RunID: runID, Error: "lifecycle operation is invalid"})
	}
}

func writeLifecycleWireResponse(connection io.Writer, response lifecycleResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return errors.New("encode local provisioner lifecycle response failed")
	}
	if _, err := connection.Write(append(data, '\n')); err != nil {
		return errors.New("write local provisioner lifecycle response failed")
	}
	return nil
}

func writeLifecycleState(stateDir string, state lifecycleState) error {
	if !validLifecycleState(state) {
		return errors.New("local provisioner lifecycle state is invalid")
	}
	path := filepath.Join(stateDir, lifecycleStateName)
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return errors.New("local provisioner lifecycle state path is unsafe")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("inspect local provisioner lifecycle state failed")
	}
	data, err := json.Marshal(state)
	if err != nil {
		return errors.New("encode local provisioner lifecycle state failed")
	}
	if err := writePrivateFile(path, append(data, '\n')); err != nil {
		return errors.New("write local provisioner lifecycle state failed")
	}
	return nil
}

func readLifecycleState(stateDir string) (lifecycleState, error) {
	directoryInfo, err := os.Lstat(stateDir)
	if err != nil || directoryInfo.Mode()&os.ModeSymlink != 0 || !directoryInfo.IsDir() || directoryInfo.Mode().Perm()&0o077 != 0 {
		return lifecycleState{}, errors.New("local provisioner lifecycle state directory is unsafe")
	}
	path := filepath.Join(stateDir, lifecycleStateName)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return lifecycleState{}, errors.New("local provisioner lifecycle state is unavailable")
	}
	file, err := os.Open(path)
	if err != nil {
		return lifecycleState{}, errors.New("local provisioner lifecycle state is unavailable")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, lifecycleMessageBytes+1))
	if err != nil || len(data) > lifecycleMessageBytes {
		return lifecycleState{}, errors.New("local provisioner lifecycle state is unavailable")
	}
	var state lifecycleState
	if err := decodeStrictJSON(data, &state); err != nil || !validLifecycleState(state) {
		return lifecycleState{}, errors.New("local provisioner lifecycle state is invalid")
	}
	return state, nil
}

func validLifecycleState(state lifecycleState) bool {
	return lifecycleRunIDPattern.MatchString(state.RunID) &&
		((!state.Destroyed && validLifecycleControlAddress(state.ControlAddress)) ||
			(state.Destroyed && state.ControlAddress == ""))
}

func validLifecycleControlAddress(value string) bool {
	host, port, err := net.SplitHostPort(value)
	if err != nil || host != "127.0.0.1" {
		return false
	}
	number, err := strconv.Atoi(port)
	return err == nil && number > 0 && number <= 65535
}

func decodeStrictJSON(data []byte, destination any) error {
	data = bytes.TrimSpace(data)
	if err := jsonstrict.ValidateValue(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination)
}

func runLifecycle(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("lifecycle", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	stateDir := flags.String("state-dir", "", "private lifecycle state directory")
	if err := flags.Parse(args); err != nil {
		return errors.New("lifecycle flags are invalid")
	}
	if *stateDir == "" || flags.NArg() != 2 {
		return errors.New("lifecycle requires --state-dir, an operation, and a run identity")
	}
	operation, runID := flags.Arg(0), flags.Arg(1)
	if (operation != "restart" && operation != "destroy") || !lifecycleRunIDPattern.MatchString(runID) {
		return errors.New("lifecycle operation or run identity is invalid")
	}
	stateRoot, err := filepath.Abs(*stateDir)
	if err != nil {
		return errors.New("lifecycle state directory is invalid")
	}
	state, err := readLifecycleState(stateRoot)
	if err != nil {
		return err
	}
	if state.RunID != runID {
		return errors.New("lifecycle run identity does not match the owned run")
	}
	if state.Destroyed {
		if operation != "destroy" {
			return errors.New("destroyed lifecycle run cannot restart")
		}
		return writeLifecycleCommandResponse(lifecycleResponse{RunID: runID, Destroyed: true})
	}
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "tcp4", state.ControlAddress)
	if err != nil {
		return errors.New("connect to owned lifecycle run failed")
	}
	defer connection.Close()
	operationDeadline := time.Now().Add(localStartupTimeout + localShutdownTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(operationDeadline) {
		operationDeadline = contextDeadline
	}
	if err := connection.SetDeadline(operationDeadline); err != nil {
		return errors.New("bound lifecycle command failed")
	}
	stopCancellation := context.AfterFunc(ctx, func() {
		_ = connection.Close()
	})
	defer stopCancellation()
	request, err := json.Marshal(lifecycleRequest{Operation: operation, RunID: runID})
	if err != nil {
		return errors.New("encode lifecycle command failed")
	}
	if _, err := connection.Write(append(request, '\n')); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("send lifecycle command: %w", contextErr)
		}
		return errors.New("send lifecycle command failed")
	}
	if tcpConnection, ok := connection.(*net.TCPConn); ok {
		_ = tcpConnection.CloseWrite()
	}
	data, err := io.ReadAll(io.LimitReader(connection, lifecycleMessageBytes+1))
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return fmt.Errorf("read lifecycle response: %w", contextErr)
		}
		return errors.New("read lifecycle response failed")
	}
	if len(data) > lifecycleMessageBytes {
		return errors.New("read lifecycle response failed")
	}
	var response lifecycleResponse
	if err := decodeStrictJSON(data, &response); err != nil || response.RunID != runID {
		return errors.New("lifecycle response identity is invalid")
	}
	if response.Error != "" {
		return errors.New(response.Error)
	}
	if operation == "restart" && (response.Destroyed || response.AttachDatabaseURL == "") {
		return errors.New("lifecycle restart response is invalid")
	}
	if operation == "destroy" && (!response.Destroyed || response.AttachDatabaseURL != "") {
		return errors.New("lifecycle destroy response is invalid")
	}
	return writeLifecycleCommandResponse(response)
}

func writeLifecycleCommandResponse(response lifecycleResponse) error {
	response.Error = ""
	data, err := json.Marshal(response)
	if err != nil {
		return errors.New("encode lifecycle response failed")
	}
	if _, err := fmt.Fprintln(os.Stdout, string(data)); err != nil {
		return errors.New("write lifecycle response failed")
	}
	return nil
}

func attachDirAssignment(name, path string) string {
	return name + "=\"${SYNCHRO_ATTACH_DIR}/" + filepath.Base(path) + "\""
}

func environmentAssignment(name, value string) string {
	return name + "='" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

type localCredentials struct {
	adminUser, adminPassword       string
	adapterUser, adapterPassword   string
	observerUser, observerPassword string
	workerUser, workerPassword     string
	operatorUser, operatorPassword string
	jwtSecret                      string
	paths                          []string
}

func createCredentials(stateDir string) (localCredentials, error) {
	credentials := localCredentials{
		adminUser:    "synchro_local_admin",
		adapterUser:  "synchro_local_adapter",
		observerUser: "synchro_local_observer",
		workerUser:   "synchro_local_worker",
		operatorUser: "synchro_local_operator",
	}
	values := []struct {
		path *string
		name string
	}{
		{&credentials.adminPassword, "admin-password"},
		{&credentials.adapterPassword, "adapter-password"},
		{&credentials.observerPassword, "observer-password"},
		{&credentials.workerPassword, "worker-password"},
		{&credentials.operatorPassword, "operator-password"},
		{&credentials.jwtSecret, "jwt-secret"},
	}
	for _, value := range values {
		data := make([]byte, 32)
		if _, err := rand.Read(data); err != nil {
			credentials.remove()
			return localCredentials{}, errors.New("generate local provisioner credential failed")
		}
		path := filepath.Join(stateDir, value.name)
		if err := writePrivateFile(path, []byte(hex.EncodeToString(data))); err != nil {
			credentials.remove()
			return localCredentials{}, errors.New("write local provisioner credential failed")
		}
		*value.path = path
		credentials.paths = append(credentials.paths, path)
	}
	return credentials, nil
}

func (credentials localCredentials) remove() {
	for _, path := range credentials.paths {
		_ = os.Remove(path)
	}
}

func setEnvironment(values map[string]string) func() {
	original := make(map[string]string, len(values))
	present := make(map[string]bool, len(values))
	for key, value := range values {
		original[key], present[key] = os.LookupEnv(key)
		_ = os.Setenv(key, value)
	}
	return func() {
		for key := range values {
			if present[key] {
				_ = os.Setenv(key, original[key])
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}
}

func runPrepare(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("prepare", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	repoRoot := flags.String("repo-root", "", "repository root")
	databaseURL := flags.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection string")
	if err := flags.Parse(args); err != nil {
		return errors.New("prepare flags are invalid")
	}
	if flags.NArg() != 0 || *repoRoot == "" || strings.TrimSpace(*databaseURL) == "" {
		return errors.New("prepare requires --repo-root and DATABASE_URL")
	}
	connectionString := strings.TrimSpace(*databaseURL)
	root, err := filepath.Abs(*repoRoot)
	if err != nil {
		return errors.New("repository root is invalid")
	}
	database, err := sql.Open("pgx", connectionString)
	if err != nil {
		return errors.New("open client integration database failed")
	}
	defer database.Close()
	if err := database.PingContext(ctx); err != nil {
		return errors.New("ping client integration database failed")
	}
	if _, err := database.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS synchro_pg CASCADE"); err != nil {
		return errors.New("ensure synchro_pg extension failed")
	}
	for _, script := range []string{"schema.sql", "register.sql"} {
		if err := executeScript(ctx, database, filepath.Join(root, "extensions", "testdata", script)); err != nil {
			return fmt.Errorf("apply client integration %s: %w", script, err)
		}
	}
	if err := waitFor(ctx, database, func(ctx context.Context, database *sql.DB) (bool, error) {
		var ready bool
		err := database.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1
			FROM synchro.sync_registry_generations generation
			WHERE generation.state = 'active'
			  AND (SELECT count(*)
			       FROM synchro.sync_registry registry
			       WHERE registry.registry_generation = generation.generation
			         AND registry.physical_schema = 'public'
			         AND registry.table_name = ANY(ARRAY[
			             'regions', 'nations', 'suppliers', 'parts', 'part_suppliers',
			             'categories', 'customers', 'orders', 'line_items', 'documents',
			             'document_members', 'document_comments', 'type_zoo'
			         ])) = 13
			  AND EXISTS (SELECT 1 FROM synchro.sync_registry registry WHERE registry.registry_generation = generation.generation AND registry.table_name = 'line_items' AND registry.membership_function_name = 'test_line_items_membership')
			  AND EXISTS (SELECT 1 FROM synchro.sync_registry registry WHERE registry.registry_generation = generation.generation AND registry.table_name = 'document_comments' AND registry.membership_function_name = 'test_document_comments_membership')
		)`).Scan(&ready)
		return ready, err
	}); err != nil {
		return errors.New("client integration registry did not activate")
	}
	if err := executeScript(ctx, database, filepath.Join(root, "extensions", "testdata", "canonical-seed.sql")); err != nil {
		return fmt.Errorf("apply client integration canonical seed: %w", err)
	}
	if err := waitFor(ctx, database, func(ctx context.Context, database *sql.DB) (bool, error) {
		var count int
		err := database.QueryRowContext(ctx, "SELECT count(*) FROM synchro.sync_bucket_edges").Scan(&count)
		return count >= 6, err
	}); err != nil {
		return errors.New("client integration seed rows did not materialize")
	}
	if _, err := database.ExecContext(ctx, "SELECT synchro.synchro_backfill_bucket_edges()"); err != nil {
		return fmt.Errorf("backfill client integration scope edges failed: %w", err)
	}
	return nil
}

func executeScript(ctx context.Context, database *sql.DB, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, err = database.ExecContext(ctx, string(data))
	return err
}

func waitFor(parent context.Context, database *sql.DB, condition func(context.Context, *sql.DB) (bool, error)) error {
	ctx, cancel := context.WithTimeout(parent, localStartupTimeout)
	defer cancel()
	for {
		ready, err := condition(ctx, database)
		if err == nil && ready {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		timer := time.NewTimer(localPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return errors.New("create local provisioner state directory failed")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errors.New("local provisioner state directory must be private")
	}
	return nil
}

func writePrivateFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
