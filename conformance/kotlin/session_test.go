package kotlin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testDigest    = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testADBHelper = "SYNCHRO_KOTLIN_ADB_HELPER"
	testADBLog    = "SYNCHRO_KOTLIN_ADB_LOG"
)

func TestMain(m *testing.M) {
	if os.Getenv(testADBHelper) == "1" {
		runADBHelper()
		return
	}
	os.Exit(m.Run())
}

func runADBHelper() {
	arguments := os.Args[1:]
	encoded, _ := json.Marshal(arguments)
	if path := os.Getenv(testADBLog); path != "" {
		file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			os.Exit(2)
		}
		_, _ = file.Write(append(encoded, '\n'))
		_ = file.Close()
	}
	if commandEndsWith(arguments, "get-state") {
		_, _ = os.Stdout.WriteString("device\n")
	}
	if commandEndsWith(arguments, "kill", "-0", "1234") {
		os.Exit(1)
	}
	os.Exit(0)
}

func commandEndsWith(arguments []string, suffix ...string) bool {
	return len(arguments) >= len(suffix) && reflect.DeepEqual(arguments[len(arguments)-len(suffix):], suffix)
}

func TestADBArgumentConstructionIncludesInstrumentationSelector(t *testing.T) {
	config := Config{
		DeviceSerial:             "emulator-5554",
		InstrumentationComponent: "com.trainstar.test/androidx.test.runner.AndroidJUnitRunner",
	}
	want := []string{
		"-s", "emulator-5554", "shell", "am", "instrument", "-w", "-r",
		"-e", "synchro.native.socket", "synchro-socket",
		"-e", "class", InstrumentationClassSelector,
		config.InstrumentationComponent,
	}
	if got := instrumentationArguments(config, "synchro-socket"); !reflect.DeepEqual(got, want) {
		t.Fatalf("instrumentation arguments = %#v", got)
	}
	if got := adbArguments("serial", "forward", "tcp:0", "localabstract:name"); !reflect.DeepEqual(got, []string{"-s", "serial", "forward", "tcp:0", "localabstract:name"}) {
		t.Fatalf("adb arguments = %#v", got)
	}
}

func TestADBReverseListenerMissingRejectsOtherFailures(t *testing.T) {
	if !adbReverseListenerMissing(&adbCommandError{output: "adb: error: listener 'tcp:8091' not found"}) {
		t.Fatal("missing reverse listener was not accepted")
	}
	for _, err := range []error{
		&adbCommandError{output: "adb: error: device not found"},
		&adbCommandError{output: "adb: error: reverse failed"},
		errors.New("context canceled"),
	} {
		if adbReverseListenerMissing(err) {
			t.Fatalf("unexpected absent reverse listener match: %v", err)
		}
	}
}

func TestDecodeResponseRejectsExtendedOrDuplicateEnvelopes(t *testing.T) {
	valid := responseWithObservations("")
	if _, err := DecodeResponse([]byte(valid)); err != nil {
		t.Fatalf("valid Kotlin response failed: %v", err)
	}
	invalid := []string{
		strings.Replace(valid, `,"error_code":null`, "", 1),
		strings.Replace(valid, `,"error_code":null`, `,"error_code":null,"extra":true`, 1),
		strings.Replace(valid, `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
		strings.Replace(valid, `"process_id":"1234"`, `"process_id":"1234","secret":true`, 1),
	}
	for _, data := range invalid {
		if _, err := DecodeResponse([]byte(data)); err == nil {
			t.Fatalf("invalid Kotlin response passed: %s", data)
		}
	}
}

func TestDecodeResponseValidatesPushMutationCount(t *testing.T) {
	observation := `{"sequence":1,"operation_class":"push","status_code":200,"error_code":null,"retryable":false,"duration_nanoseconds":1,"request_facts":{"client_generation":1,"schema_version":1,"schema_hash":"` + testDigest + `","mutation_count":2}}`
	result, err := DecodeResponse([]byte(responseWithObservations(observation)))
	if err != nil {
		t.Fatalf("valid push response failed: %v", err)
	}
	count := result.TransportObservations.Observations[0].RequestFacts.MutationCount
	if count == nil || *count != 2 {
		t.Fatalf("push mutation count = %v", count)
	}
	for _, invalid := range []string{
		strings.Replace(observation, `,"mutation_count":2`, "", 1),
		strings.Replace(observation, `,"error_code":null`, "", 1),
		strings.Replace(observation, `,"retryable":false`, "", 1),
		strings.Replace(observation, `"retryable":false`, `"retryable":true`, 1),
		strings.Replace(observation, `"mutation_count":2`, `"mutation_count":0`, 1),
		strings.Replace(observation, `"mutation_count":2`, `"mutation_count":1001`, 1),
		strings.Replace(observation, `"operation_class":"push"`, `"operation_class":"pull"`, 1),
		strings.Replace(observation, `"mutation_count":2`, `"mutation_count":2,"unknown":true`, 1),
	} {
		if _, err := DecodeResponse([]byte(responseWithObservations(invalid))); err == nil {
			t.Fatalf("invalid push response passed: %s", invalid)
		}
	}
}

func TestDecodeResponseAcceptsLargeAggregateCounts(t *testing.T) {
	response := strings.Replace(
		responseWithObservations(""),
		`"transport_observations"`,
		`"application_row_count":1000,"mutation_ledger_count":1000,"mutation_outcome_count":1000,"sealed_batch_count":1,"rejected_mutation_count":1,"scope_state_count":1,"scope_row_count":1000,"provenance_count":1000,"row_metadata_count":1000,"rebuild_attempt_count":1,"rebuild_receipt_count":10,"transport_observations"`,
		1,
	)
	result, err := DecodeResponse([]byte(response))
	if err != nil {
		t.Fatalf("decode aggregate counts: %v", err)
	}
	if result.ApplicationRowCount == nil || *result.ApplicationRowCount != 1000 || result.ProvenanceCount == nil || *result.ProvenanceCount != 1000 {
		t.Fatalf("aggregate counts were not retained: %+v", result)
	}
}

func TestValidateRequestAcceptsPushBatchSizeForOpenOnly(t *testing.T) {
	open := Request{
		SchemaVersion: 1,
		Operation:     "open",
		DatabaseKey:   "client.sqlite",
		DatabaseMode:  "create",
		ServerURL:     "http://127.0.0.1:8090",
		AuthToken:     "token",
		ClientID:      "client-a",
		PushBatchSize: 1000,
	}
	if err := validateRequest(open); err != nil {
		t.Fatalf("validate open push batch size: %v", err)
	}
	open.PushBatchSize = 1001
	if err := validateRequest(open); err == nil {
		t.Fatal("oversized push batch passed")
	}
	nonOpen := Request{SchemaVersion: 1, Operation: "lifecycle", LifecycleOperation: "stop", PushBatchSize: 1}
	if err := validateRequest(nonOpen); err == nil {
		t.Fatal("push batch size passed outside open")
	}
}

func TestSessionRejectsChangedOrBackwardCheckpoint(t *testing.T) {
	firstObservation := TransportObservation{Sequence: 1, OperationClass: "connect", StatusCode: 200, DurationNanoseconds: 1}
	session := &Session{}
	if err := session.acceptResult(testResult([]TransportObservation{firstObservation})); err != nil {
		t.Fatalf("accept first checkpoint: %v", err)
	}
	if session.Checkpoint() != 1 {
		t.Fatalf("checkpoint = %d", session.Checkpoint())
	}
	observations, err := session.ObservationsAfter(0)
	if err != nil || len(observations) != 1 {
		t.Fatalf("observations after checkpoint: %v, %#v", err, observations)
	}
	observations[0].StatusCode = 500
	stored, _ := session.ObservationsAfter(0)
	if stored[0].StatusCode != 200 {
		t.Fatal("returned observation changed stored checkpoint")
	}
	changed := firstObservation
	changed.StatusCode = 201
	if err := session.acceptResult(testResult([]TransportObservation{changed})); err == nil {
		t.Fatal("changed checkpoint passed")
	}
	if err := session.acceptResult(testResult(nil)); err == nil {
		t.Fatal("backward checkpoint passed")
	}
}

func TestBoundedOutputAndResponseLimit(t *testing.T) {
	writer := &boundedWriter{maximum: 4}
	if count, err := writer.Write([]byte("abcdef")); err != nil || count != 6 {
		t.Fatalf("bounded write = %d, %v", count, err)
	}
	data, overflowed := writer.snapshot()
	if string(data) != "abcd" || !overflowed {
		t.Fatalf("bounded output = %q, %t", data, overflowed)
	}
	oversized := bytes.Repeat([]byte{' '}, MaximumMessageBytes+1)
	if _, err := DecodeResponse(oversized); err == nil {
		t.Fatal("oversized response passed")
	}
}

func TestConnectionStabilityRejectsClosedADBForward(t *testing.T) {
	host, device := net.Pipe()
	if err := device.Close(); err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if connectionIsStable(host) {
		t.Fatal("closed adb forward passed the stability check")
	}
}

func TestConnectionStabilityAcceptsWaitingInstrumentation(t *testing.T) {
	host, device := net.Pipe()
	defer host.Close()
	defer device.Close()
	if !connectionIsStable(host) {
		t.Fatal("waiting instrumentation failed the stability check")
	}
}

func TestInstallSeedReverseKillAndCleanupCommands(t *testing.T) {
	logPath := t.TempDir() + "/adb.jsonl"
	t.Setenv(testADBHelper, "1")
	t.Setenv(testADBLog, logPath)
	applicationAPK := writeFixture(t, "application.apk")
	instrumentationAPK := writeFixture(t, "instrumentation.apk")
	seedPath := writeFixture(t, "seed.sqlite")
	session := &Session{config: Config{
		ADBPath:                  os.Args[0],
		DeviceSerial:             "emulator-5554",
		ApplicationAPKPath:       applicationAPK,
		InstrumentationAPKPath:   instrumentationAPK,
		ApplicationID:            "com.trainstar.synchro.conformance",
		InstrumentationComponent: "component",
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := session.prepareDevice(ctx); err != nil {
		t.Fatalf("prepare device: %v", err)
	}
	if err := session.ReverseHostPort(ctx, 8091, 8091); err != nil {
		t.Fatalf("reverse host port: %v", err)
	}
	seedName, err := session.StageSeed(ctx, "client-db", seedPath)
	if err != nil {
		t.Fatalf("stage seed: %v", err)
	}
	session.mu.Lock()
	session.processID = "1234"
	session.forwardPort = 4321
	session.mu.Unlock()
	if err := session.Kill(ctx); err != nil {
		t.Fatalf("kill process: %v", err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatalf("close session: %v", err)
	}
	commands := readADBCommands(t, logPath)
	for _, suffix := range [][]string{
		{"get-state"},
		{"install", "-r", "-t", applicationAPK},
		{"install", "-r", "-t", instrumentationAPK},
		{"reverse", "tcp:8091", "tcp:8091"},
		{"push", seedPath, "/data/local/tmp/" + seedName},
		{"shell", "run-as", session.config.ApplicationID, "mkdir", "-p", "files"},
		{"shell", "run-as", session.config.ApplicationID, "cp", "/data/local/tmp/" + seedName, "files/" + seedName},
		{"shell", "rm", "-f", "/data/local/tmp/" + seedName},
		{"shell", "run-as", session.config.ApplicationID, "kill", "-9", "1234"},
		{"shell", "run-as", session.config.ApplicationID, "kill", "-0", "1234"},
		{"forward", "--remove", "tcp:4321"},
		{"reverse", "--remove", "tcp:8091"},
		{"shell", "run-as", session.config.ApplicationID, "rm", "-f", "files/" + seedName},
	} {
		if !hasCommandSuffix(commands, suffix) {
			t.Fatalf("missing adb command suffix %#v in %#v", suffix, commands)
		}
	}
}

func responseWithObservations(observations string) string {
	return `{"schema_version":1,"outcome":"passed","result":{"transport_observations":{"observations":[` + observations + `],"overflowed":false,"sequence_checkpoint":` + boolCount(observations) + `},"process_id":"1234","database_identity_fingerprint":"` + testDigest + `"},"error_code":null}`
}

func boolCount(value string) string {
	if value == "" {
		return "0"
	}
	return "1"
}

func testResult(observations []TransportObservation) Result {
	return Result{
		ProcessID:                   "1234",
		DatabaseIdentityFingerprint: testDigest,
		TransportObservations: &TransportObservationSnapshot{
			Observations:       observations,
			SequenceCheckpoint: uint64(len(observations)),
		},
	}
}

func writeFixture(t *testing.T, name string) string {
	t.Helper()
	path := t.TempDir() + "/" + name
	if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func readADBCommands(t *testing.T, path string) [][]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result [][]string
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		var command []string
		if err := json.Unmarshal(line, &command); err != nil {
			t.Fatal(err)
		}
		result = append(result, command)
	}
	return result
}

func hasCommandSuffix(commands [][]string, suffix []string) bool {
	for _, command := range commands {
		if commandEndsWith(command, suffix...) {
			return true
		}
	}
	return false
}
