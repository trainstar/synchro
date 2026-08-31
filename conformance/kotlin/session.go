// Package kotlin drives the real Kotlin Android instrumentation session.
package kotlin

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// InstrumentationClassSelector selects the host-controlled Kotlin session test.
	InstrumentationClassSelector = "com.trainstar.synchro.conformance.NativeSessionInstrumentationTest#serveHostControlledSession"

	MaximumMessageBytes = 1 << 20
	maximumADBOutput    = 64 << 10
	adbCommandTimeout   = 2 * time.Minute
	connectTimeout      = 30 * time.Second
	connectStability    = 250 * time.Millisecond
	requestTimeout      = 3 * time.Minute
	cleanupTimeout      = 10 * time.Second
	shutdownGracePeriod = time.Second
)

// Config identifies one Android device and its Kotlin instrumentation artifacts.
// StartSession installs both APKs when both artifact paths are set.
type Config struct {
	ADBPath                  string
	DeviceSerial             string
	ApplicationAPKPath       string
	InstrumentationAPKPath   string
	ApplicationID            string
	InstrumentationComponent string
	ServerURL                string
	AuthToken                AuthTokenResolver
	Platform                 string
	AppVersion               string
	PullPageSize             int
	PushBatchSize            int
	TransportCapacity        int
}

// Session owns one adb-forwarded Kotlin instrumentation process.
type Session struct {
	config Config

	mu        sync.Mutex
	requestMu sync.Mutex
	controlMu sync.Mutex

	connection  net.Conn
	scanner     *bufio.Scanner
	done        chan struct{}
	forwardPort int
	closed      bool
	output      *boundedWriter
	processErr  error

	reverseDevicePort int
	reverseHostPort   int
	stagedSeedNames   []string

	processID                   string
	databaseIdentityFingerprint string
	transportCheckpoint         uint64
	transportObservations       []TransportObservation
}

// StartSession installs configured artifacts and starts one host-controlled session.
func StartSession(ctx context.Context, config Config) (*Session, error) {
	if err := requireContext(ctx, "Kotlin session context is required"); err != nil {
		return nil, err
	}
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	session := &Session{config: normalized}
	if err := session.prepareDevice(ctx); err != nil {
		return nil, err
	}
	socketName, err := newSocketName()
	if err != nil {
		return nil, err
	}
	port, err := session.createForward(ctx, socketName)
	if err != nil {
		return nil, err
	}
	session.forwardPort = port
	if err := session.startInstrumentation(socketName); err != nil {
		session.cleanupFailedStart()
		return nil, err
	}
	if err := session.connect(ctx, port); err != nil {
		session.cleanupFailedStart()
		return nil, err
	}
	return session, nil
}

func normalizeConfig(config Config) (Config, error) {
	if config.ADBPath == "" || !validADBArgument(config.DeviceSerial, 256) || !validADBArgument(config.ApplicationID, 255) || !validADBArgument(config.InstrumentationComponent, 512) {
		return Config{}, errors.New("Kotlin session configuration is incomplete")
	}
	if (config.ApplicationAPKPath == "") != (config.InstrumentationAPKPath == "") {
		return Config{}, errors.New("Kotlin APK configuration is incomplete")
	}
	adbPath, err := exec.LookPath(config.ADBPath)
	if err != nil {
		return Config{}, errors.New("Android adb is unavailable")
	}
	config.ADBPath = adbPath
	if config.ApplicationAPKPath == "" {
		return config, nil
	}
	config.ApplicationAPKPath, err = requireRegularFile(config.ApplicationAPKPath)
	if err != nil {
		return Config{}, errors.New("Kotlin application APK is unavailable")
	}
	config.InstrumentationAPKPath, err = requireRegularFile(config.InstrumentationAPKPath)
	if err != nil {
		return Config{}, errors.New("Kotlin instrumentation APK is unavailable")
	}
	return config, nil
}

func validADBArgument(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && !strings.ContainsAny(value, "\x00\r\n")
}

func requireRegularFile(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(absolute)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("file is unavailable")
	}
	return filepath.Clean(absolute), nil
}

func (s *Session) prepareDevice(ctx context.Context) error {
	state, err := s.adb(ctx, "get-state")
	if err != nil || strings.TrimSpace(state) != "device" {
		return errors.New("Android device is unavailable")
	}
	if s.config.ApplicationAPKPath == "" {
		return nil
	}
	if _, err := s.adb(ctx, "install", "-r", "-t", s.config.ApplicationAPKPath); err != nil {
		return errors.New("install Kotlin application APK failed")
	}
	if _, err := s.adb(ctx, "install", "-r", "-t", s.config.InstrumentationAPKPath); err != nil {
		return errors.New("install Kotlin instrumentation APK failed")
	}
	return nil
}

func newSocketName() (string, error) {
	value := make([]byte, 18)
	if _, err := rand.Read(value); err != nil {
		return "", errors.New("generate Kotlin instrumentation socket name failed")
	}
	return "synchro-" + hex.EncodeToString(value), nil
}

func adbArguments(serial string, arguments ...string) []string {
	result := make([]string, 0, len(arguments)+2)
	result = append(result, "-s", serial)
	return append(result, arguments...)
}

func instrumentationArguments(config Config, socketName string) []string {
	return adbArguments(
		config.DeviceSerial,
		"shell", "am", "instrument", "-w", "-r",
		"-e", "synchro.native.socket", socketName,
		"-e", "class", InstrumentationClassSelector,
		config.InstrumentationComponent,
	)
}

func (s *Session) adb(ctx context.Context, arguments ...string) (string, error) {
	if err := requireContext(ctx, "Android adb context is required"); err != nil {
		return "", err
	}
	commandContext, cancel := context.WithTimeout(ctx, adbCommandTimeout)
	defer cancel()
	command := exec.CommandContext(commandContext, s.config.ADBPath, adbArguments(s.config.DeviceSerial, arguments...)...)
	output := &boundedWriter{maximum: maximumADBOutput}
	command.Stdout = output
	command.Stderr = output
	if err := command.Run(); err != nil {
		if commandContext.Err() != nil {
			return "", commandContext.Err()
		}
		data, _ := output.snapshot()
		return "", &adbCommandError{output: string(data)}
	}
	data, overflowed := output.snapshot()
	if overflowed {
		return "", errors.New("Android adb output exceeded its bound")
	}
	return string(data), nil
}

type adbCommandError struct {
	output string
}

func (e *adbCommandError) Error() string {
	return "Android adb command failed"
}

func adbReverseListenerMissing(err error) bool {
	var failure *adbCommandError
	if !errors.As(err, &failure) {
		return false
	}
	output := strings.ToLower(failure.output)
	return strings.Contains(output, "listener") && strings.Contains(output, "not found")
}

func (s *Session) createForward(ctx context.Context, socketName string) (int, error) {
	output, err := s.adb(ctx, "forward", "tcp:0", "localabstract:"+socketName)
	if err != nil {
		return 0, errors.New("create Android adb forward failed")
	}
	port, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil || !validPort(port) {
		return 0, errors.New("Android adb forward did not return a local port")
	}
	return port, nil
}

func (s *Session) startInstrumentation(socketName string) error {
	command := exec.Command(s.config.ADBPath, instrumentationArguments(s.config, socketName)...)
	output := &boundedWriter{maximum: maximumADBOutput}
	command.Stdout = output
	command.Stderr = output
	if err := command.Start(); err != nil {
		return errors.New("start Kotlin instrumentation failed")
	}
	done := make(chan struct{})
	s.mu.Lock()
	s.done = done
	s.output = output
	s.mu.Unlock()
	go func() {
		err := command.Wait()
		s.mu.Lock()
		s.processErr = err
		s.mu.Unlock()
		close(done)
	}()
	return nil
}

func (s *Session) instrumentationFailure(message string) error {
	s.mu.Lock()
	output := s.output
	processErr := s.processErr
	s.mu.Unlock()
	if output == nil {
		return errors.New(message)
	}
	data, overflowed := output.snapshot()
	if overflowed || len(data) == 0 {
		return errors.New(message)
	}
	const maximumDiagnosticBytes = 2048
	if len(data) > maximumDiagnosticBytes {
		data = data[len(data)-maximumDiagnosticBytes:]
	}
	diagnostic := strings.TrimSpace(strings.ToValidUTF8(string(data), "?"))
	if diagnostic == "" {
		return errors.New(message)
	}
	if processErr != nil {
		return errors.New(message + ": instrumentation exited: " + diagnostic)
	}
	return errors.New(message + ": " + diagnostic)
}

func (s *Session) connect(ctx context.Context, port int) error {
	connectContext, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	for {
		connection, err := (&net.Dialer{Timeout: time.Second}).DialContext(connectContext, "tcp", address)
		if err == nil {
			if !connectionIsStable(connection) {
				_ = connection.Close()
				continue
			}
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				_ = connection.Close()
				return errors.New("Kotlin instrumentation session is closed")
			}
			s.connection = connection
			s.scanner = bufio.NewScanner(connection)
			s.scanner.Buffer(make([]byte, 4096), MaximumMessageBytes+1)
			s.mu.Unlock()
			return nil
		}

		s.mu.Lock()
		done := s.done
		s.mu.Unlock()
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-connectContext.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("Kotlin instrumentation socket did not become ready")
		case <-done:
			timer.Stop()
			return errors.New("Kotlin instrumentation exited before its socket became ready")
		case <-timer.C:
		}
	}
}

func connectionIsStable(connection net.Conn) bool {
	if err := connection.SetReadDeadline(time.Now().Add(connectStability)); err != nil {
		return false
	}
	var probe [1]byte
	read, err := connection.Read(probe[:])
	if clearErr := connection.SetReadDeadline(time.Time{}); clearErr != nil {
		return false
	}
	var networkError net.Error
	return read == 0 && errors.As(err, &networkError) && networkError.Timeout()
}

// ReverseHostPort makes one bounded host adapter port reachable from Android.
func (s *Session) ReverseHostPort(ctx context.Context, devicePort, hostPort int) error {
	if err := requireContext(ctx, "Android reverse context is required"); err != nil {
		return err
	}
	if !validPort(devicePort) || !validPort(hostPort) {
		return errors.New("Android reverse port is invalid")
	}
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("Kotlin instrumentation session is closed")
	}
	if s.reverseDevicePort != 0 {
		matches := s.reverseDevicePort == devicePort && s.reverseHostPort == hostPort
		s.mu.Unlock()
		if matches {
			return nil
		}
		return errors.New("Android host adapter reverse is already configured")
	}
	s.mu.Unlock()
	if _, err := s.adb(ctx, "reverse", "tcp:"+strconv.Itoa(devicePort), "tcp:"+strconv.Itoa(hostPort)); err != nil {
		return errors.New("create Android adb reverse failed")
	}
	s.mu.Lock()
	s.reverseDevicePort = devicePort
	s.reverseHostPort = hostPort
	s.mu.Unlock()
	return nil
}

// StageSeed copies one production seed into application-private storage.
func (s *Session) StageSeed(ctx context.Context, databaseKey, sourcePath string) (string, error) {
	if err := requireContext(ctx, "Android seed context is required"); err != nil {
		return "", err
	}
	if databaseKey == "" || len(databaseKey) > 128 || strings.ContainsRune(databaseKey, '\x00') {
		return "", errors.New("Android seed database key is invalid")
	}
	path, err := requireRegularFile(sourcePath)
	if err != nil {
		return "", errors.New("Android production seed is unavailable")
	}
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return "", errors.New("Kotlin instrumentation session is closed")
	}
	s.mu.Unlock()

	digest := sha256.Sum256([]byte(databaseKey + "\x00" + path))
	name := "synchro-seed-" + hex.EncodeToString(digest[:16]) + ".sqlite"
	remote := "/data/local/tmp/" + name
	if _, err := s.adb(ctx, "push", path, remote); err != nil {
		return "", errors.New("stage Android production seed failed")
	}
	defer s.removeTemporarySeed(remote)
	if _, err := s.adb(ctx, "shell", "run-as", s.config.ApplicationID, "mkdir", "-p", "files"); err != nil {
		return "", errors.New("prepare Android seed storage failed")
	}
	if _, err := s.adb(ctx, "shell", "run-as", s.config.ApplicationID, "cp", remote, "files/"+name); err != nil {
		s.removePrivateSeed(name)
		return "", errors.New("copy Android production seed failed")
	}
	s.mu.Lock()
	s.stagedSeedNames = append(s.stagedSeedNames, name)
	s.mu.Unlock()
	return name, nil
}

func (s *Session) removeTemporarySeed(remote string) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_, _ = s.adb(ctx, "shell", "rm", "-f", remote)
}

func (s *Session) removePrivateSeed(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_, _ = s.adb(ctx, "shell", "run-as", s.config.ApplicationID, "rm", "-f", "files/"+name)
}

func validPort(port int) bool {
	return port >= 1 && port <= 65535
}

// Kill sends SIGKILL to the observed instrumentation PID and confirms its exit.
func (s *Session) Kill(ctx context.Context) error {
	if err := requireContext(ctx, "Android kill context is required"); err != nil {
		return err
	}
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	processID := s.processID
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return errors.New("Kotlin instrumentation session is closed")
	}
	if !validProcessID(processID) {
		return errors.New("Kotlin instrumentation process identity is unavailable")
	}
	if _, err := s.adb(ctx, "shell", "run-as", s.config.ApplicationID, "kill", "-9", processID); err != nil {
		return errors.New("kill Kotlin instrumentation process failed")
	}
	if _, err := s.adb(ctx, "shell", "run-as", s.config.ApplicationID, "kill", "-0", processID); err == nil {
		return errors.New("Kotlin instrumentation process termination is not confirmed")
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errors.New("Kotlin instrumentation process termination is not confirmed")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return s.closeTransport(ctx)
}

func validProcessID(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	return err == nil && parsed > 0
}

// WaitForExit waits until the owned instrumentation process exits.
func (s *Session) WaitForExit(ctx context.Context) error {
	if err := requireContext(ctx, "Kotlin instrumentation wait context is required"); err != nil {
		return err
	}
	s.mu.Lock()
	done := s.done
	s.mu.Unlock()
	if done == nil {
		return errors.New("Kotlin instrumentation process is unavailable")
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Session) closeTransport(ctx context.Context) error {
	s.mu.Lock()
	connection := s.connection
	s.connection = nil
	s.scanner = nil
	port := s.forwardPort
	s.forwardPort = 0
	s.mu.Unlock()
	if connection != nil {
		_ = connection.Close()
	}
	if port == 0 {
		return nil
	}
	if _, err := s.adb(ctx, "forward", "--remove", "tcp:"+strconv.Itoa(port)); err != nil {
		return errors.New("remove Android adb forward failed")
	}
	return nil
}

// Close stops the instrumentation process and removes all adb resources.
func (s *Session) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if err := requireContext(ctx, "Kotlin session close context is required"); err != nil {
		return err
	}
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	done := s.done
	reversePort := s.reverseDevicePort
	s.reverseDevicePort = 0
	s.reverseHostPort = 0
	seedNames := append([]string(nil), s.stagedSeedNames...)
	s.stagedSeedNames = nil
	s.mu.Unlock()

	var failures []error
	if err := s.closeTransport(ctx); err != nil {
		failures = append(failures, err)
	}
	if reversePort != 0 {
		if _, err := s.adb(ctx, "reverse", "--remove", "tcp:"+strconv.Itoa(reversePort)); err != nil && !adbReverseListenerMissing(err) {
			failures = append(failures, errors.New("remove Android adb reverse failed"))
		}
	}
	for _, seedName := range seedNames {
		if _, err := s.adb(ctx, "shell", "run-as", s.config.ApplicationID, "rm", "-f", "files/"+seedName); err != nil {
			failures = append(failures, errors.New("remove Android staged seed failed"))
		}
	}
	if done == nil {
		return errors.Join(failures...)
	}

	timer := time.NewTimer(shutdownGracePeriod)
	defer timer.Stop()
	select {
	case <-done:
		return errors.Join(failures...)
	case <-ctx.Done():
		failures = append(failures, ctx.Err())
		return errors.Join(failures...)
	case <-timer.C:
	}
	if _, err := s.adb(ctx, "shell", "am", "force-stop", s.config.ApplicationID); err != nil {
		failures = append(failures, errors.New("force-stop Kotlin instrumentation failed"))
		return errors.Join(failures...)
	}
	waitContext, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	select {
	case <-done:
	case <-waitContext.Done():
		failures = append(failures, errors.New("Kotlin instrumentation did not stop"))
	}
	return errors.Join(failures...)
}

func (s *Session) cleanupFailedStart() {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_ = s.Close(ctx)
}

func requireContext(ctx context.Context, message string) error {
	if ctx == nil {
		return errors.New(message)
	}
	return ctx.Err()
}

type boundedWriter struct {
	mu         sync.Mutex
	maximum    int
	data       []byte
	overflowed bool
}

func (w *boundedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	available := w.maximum - len(w.data)
	if available < len(data) {
		w.overflowed = true
	}
	if available > 0 {
		if available > len(data) {
			available = len(data)
		}
		w.data = append(w.data, data[:available]...)
	}
	return len(data), nil
}

func (w *boundedWriter) snapshot() ([]byte, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.data...), w.overflowed
}
