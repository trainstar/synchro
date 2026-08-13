package baseline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RunnerConfig supplies only the legacy diagnostic transport and source-DML inputs.
type RunnerConfig struct {
	BaseURL      string
	HTTPClient   *http.Client
	BearerToken  string
	Source       SourceOperations
	Operator     OperatorOperations
	Output       OutputPath
	MaxBodyBytes int64
}

// SourceOperations contains the complete source-DML capability used by probes.
type SourceOperations interface {
	ExecContext(context.Context, string, ...any) error
	CommitInReverseBeginOrder(context.Context, string, []any, string, []any) error
}

// OperatorOperations contains the complete administrative capability used by probes.
type OperatorOperations interface {
	DropHydrationColumn(context.Context) error
	RestoreHydrationColumn(context.Context) error
	RegisterSchemaQueue(context.Context) error
	ConfigureDecodeTrap(context.Context, string) error
	RegisterLateSourceTable(context.Context) error
	UnregisterLateSourceTable(context.Context) error
	ConfigureCrossScopeTable(context.Context) error
	RestoreCrossScopeTable(context.Context) error
	ReloadRegistry(context.Context) error
	CompactPositiveInterval(context.Context) ([]byte, error)
}

// Runner executes only fixed protocol 2 endpoint DTOs.
type Runner struct {
	baseURL     *url.URL
	httpClient  *http.Client
	bearerToken string
	source      SourceOperations
	operator    OperatorOperations
	output      OutputPath
	maximumBody int64
	attachments *attachmentStore

	mu       sync.Mutex
	receipts []DiagnosticReceipt
	nextID   uint64
	runMu    sync.Mutex
}

// ProbeRuntime exposes only real HTTP, source DML, and approved operator controls to probes.
type ProbeRuntime struct {
	runner *Runner
}

type exchange struct {
	body      []byte
	status    int
	receipt   DiagnosticReceipt
	transport error
}

type attachmentStore struct {
	output OutputPath
	root   string
	mu     sync.Mutex
}

// NewRunner creates an isolated non-release protocol 2 diagnostic runner.
func NewRunner(config RunnerConfig) (*Runner, error) {
	baseURL, err := validateBaseURL(config.BaseURL)
	if err != nil {
		return nil, err
	}
	if config.HTTPClient == nil || strings.TrimSpace(config.BearerToken) == "" || config.Source == nil || config.Operator == nil {
		return nil, errors.New("baseline runner configuration is incomplete")
	}
	if config.Output.class != nonReleaseClass || config.Output.path == "" {
		return nil, errors.New("typed baseline output is required")
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = maximumDiagnosticBodyBytes
	}
	if config.MaxBodyBytes < 1 || config.MaxBodyBytes > maximumDiagnosticBodyBytes {
		return nil, errors.New("baseline body limit is invalid")
	}
	store, err := newAttachmentStore(config.Output)
	if err != nil {
		return nil, err
	}
	client := *config.HTTPClient
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Runner{
		baseURL:     baseURL,
		httpClient:  &client,
		bearerToken: config.BearerToken,
		source:      config.Source,
		operator:    config.Operator,
		output:      config.Output,
		maximumBody: config.MaxBodyBytes,
		attachments: store,
	}, nil
}

func validateBaseURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("baseline HTTP origin is invalid")
	}
	return parsed, nil
}

func newAttachmentStore(output OutputPath) (*attachmentStore, error) {
	if output.class != nonReleaseClass || output.path == "" {
		return nil, errors.New("baseline attachment output is invalid")
	}
	if err := os.MkdirAll(output.path, 0o700); err != nil {
		return nil, errors.New("create baseline output directory failed")
	}
	info, err := os.Lstat(output.path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("baseline output directory is unsafe")
	}
	root := filepath.Join(output.path, "attachments")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, errors.New("create baseline attachment directory failed")
	}
	info, err = os.Lstat(root)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("baseline attachment directory is unsafe")
	}
	return &attachmentStore{output: output, root: root}, nil
}

// Run executes all ten fixed current diagnostic probes.
func (runner *Runner) Run(ctx context.Context) (Report, error) {
	return runner.RunProbes(ctx, DefaultProbes())
}

// RunProbes executes supplied diagnostic probes without making their output release evidence.
func (runner *Runner) RunProbes(ctx context.Context, probes []Probe) (Report, error) {
	if runner == nil {
		return Report{}, errors.New("baseline runner is required")
	}
	if ctx == nil {
		return Report{}, errors.New("baseline run context is required")
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if len(probes) == 0 {
		return Report{}, errors.New("baseline probes are required")
	}
	runner.runMu.Lock()
	defer runner.runMu.Unlock()
	runtime := &ProbeRuntime{runner: runner}
	report := Report{createdAt: time.Now().UTC(), output: runner.output, probes: make([]ProbeResult, 0, len(probes))}
	seen := make(map[DefectFamily]struct{}, len(probes))
	for _, probe := range probes {
		if probe == nil || probe.Family() == "" {
			return Report{}, errors.New("baseline probe is invalid")
		}
		if _, exists := seen[probe.Family()]; exists {
			return Report{}, errors.New("baseline probe is duplicated")
		}
		seen[probe.Family()] = struct{}{}
		result, err := probe.Run(ctx, runtime)
		report.probes = append(report.probes, result)
		if err != nil {
			report.receipts = runner.copyReceipts()
			return report, err
		}
		if result.Family != probe.Family() || !result.Captured || len(result.ReceiptIDs) == 0 {
			report.receipts = runner.copyReceipts()
			return report, fmt.Errorf("baseline %s probe did not capture its expected divergence", probe.Family())
		}
	}
	report.receipts = runner.copyReceipts()
	if err := report.Validate(); err != nil {
		return report, err
	}
	if err := writeReport(report); err != nil {
		return report, err
	}
	return report, nil
}

func (runner *Runner) copyReceipts() []DiagnosticReceipt {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return append([]DiagnosticReceipt(nil), runner.receipts...)
}

func writeReport(report Report) error {
	encoded, err := json.Marshal(report)
	if err != nil {
		return errors.New("encode baseline report failed")
	}
	path := filepath.Join(report.output.path, reportFileName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.New("publish baseline report failed")
	}
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		return errors.New("write baseline report failed")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.New("sync baseline report failed")
	}
	if err := file.Close(); err != nil {
		return errors.New("close baseline report failed")
	}
	if err := syncBaselineDirectory(report.output.path); err != nil {
		return errors.New("sync baseline output directory failed")
	}
	remove = false
	return nil
}

func syncBaselineDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (store *attachmentStore) put(kind string, data []byte) (Attachment, error) {
	if store == nil || (kind != "raw_http_request" && kind != "raw_http_response") {
		return Attachment{}, errors.New("baseline attachment is invalid")
	}
	digest := sha256.Sum256(data)
	hexDigest := hex.EncodeToString(digest[:])
	name := kind + "-sha256-" + hexDigest + ".bin"
	relative := filepath.ToSlash(filepath.Join("attachments", name))
	path := filepath.Join(store.root, name)
	store.mu.Lock()
	defer store.mu.Unlock()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		actual, verifyErr := verifyAttachment(path, data)
		if verifyErr != nil || actual != hexDigest {
			return Attachment{}, errors.New("baseline attachment changed")
		}
		return Attachment{
			id:       "baseline-" + kind + "-sha256:" + hexDigest,
			kind:     kind,
			path:     store.output,
			relative: relative,
			sha256:   hexDigest,
			size:     int64(len(data)),
		}, nil
	}
	if err != nil {
		return Attachment{}, errors.New("create baseline attachment failed")
	}
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return Attachment{}, errors.New("write baseline attachment failed")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return Attachment{}, errors.New("sync baseline attachment failed")
	}
	if err := file.Close(); err != nil {
		return Attachment{}, errors.New("close baseline attachment failed")
	}
	if err := syncBaselineDirectory(store.root); err != nil {
		return Attachment{}, errors.New("sync baseline attachment directory failed")
	}
	remove = false
	return Attachment{
		id:       "baseline-" + kind + "-sha256:" + hexDigest,
		kind:     kind,
		path:     store.output,
		relative: relative,
		sha256:   hexDigest,
		size:     int64(len(data)),
	}, nil
}

func verifyAttachment(path string, wanted []byte) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() != int64(len(wanted)) {
		return "", errors.New("attachment is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil || !bytes.Equal(data, wanted) {
		return "", errors.New("attachment content changed")
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func (runtime *ProbeRuntime) Connect(ctx context.Context, request ConnectRequest) (ConnectResponse, exchange, error) {
	if err := validateProtocolVersion(request.ProtocolVersion); err != nil || request.ClientID == "" || request.Platform == "" || request.AppVersion == "" || request.KnownScopes == nil {
		return ConnectResponse{}, exchange{}, errors.New("protocol 2 connect request is invalid")
	}
	exchange, err := runtime.runner.execute(ctx, EndpointConnect, request, false)
	if err != nil {
		return ConnectResponse{}, exchange, err
	}
	if exchange.status != http.StatusOK {
		return ConnectResponse{}, exchange, endpointStatusError(EndpointConnect, exchange)
	}
	var response ConnectResponse
	if err := decodeProtocol2JSON(exchange.body, &response); err != nil || response.ProtocolVersion != ProtocolVersion {
		return ConnectResponse{}, exchange, errors.New("protocol 2 connect response is invalid")
	}
	return response, exchange, nil
}

func (runtime *ProbeRuntime) Push(ctx context.Context, request PushRequest) (PushResponse, exchange, error) {
	if err := validateProtocolVersion(request.ProtocolVersion); err != nil || request.ClientID == "" || request.BatchID == "" {
		return PushResponse{}, exchange{}, errors.New("protocol 2 push request is invalid")
	}
	exchange, err := runtime.runner.execute(ctx, EndpointPush, request, false)
	if err != nil {
		return PushResponse{}, exchange, err
	}
	if exchange.status != http.StatusOK {
		return PushResponse{}, exchange, endpointStatusError(EndpointPush, exchange)
	}
	var response PushResponse
	if err := decodeProtocol2JSON(exchange.body, &response); err != nil {
		return PushResponse{}, exchange, errors.New("protocol 2 push response is invalid")
	}
	response.ProtocolVersion = ProtocolVersion
	return response, exchange, nil
}

// PushDropAfterSuccess sends one real push and suppresses its response after upstream completion.
func (runtime *ProbeRuntime) PushDropAfterSuccess(ctx context.Context, request PushRequest) (exchange, exchange, error) {
	if err := validateProtocolVersion(request.ProtocolVersion); err != nil || request.ClientID == "" || request.BatchID == "" {
		return exchange{}, exchange{}, errors.New("protocol 2 push request is invalid")
	}
	exchange, err := runtime.runner.execute(ctx, EndpointPush, request, true)
	if err != nil {
		return exchange, exchange, err
	}
	if exchange.status == 0 {
		return exchange, exchange, errors.New("upstream push did not complete")
	}
	return exchange, exchange, errors.New("diagnostic response was dropped after upstream success")
}

func (runtime *ProbeRuntime) Pull(ctx context.Context, request PullRequest) (PullResponse, exchange, error) {
	if err := validateProtocolVersion(request.ProtocolVersion); err != nil || request.ClientID == "" || request.Scopes == nil || request.Limit < 1 {
		return PullResponse{}, exchange{}, errors.New("protocol 2 pull request is invalid")
	}
	exchange, err := runtime.runner.execute(ctx, EndpointPull, request, false)
	if err != nil {
		return PullResponse{}, exchange, err
	}
	if exchange.status != http.StatusOK {
		return PullResponse{}, exchange, endpointStatusError(EndpointPull, exchange)
	}
	var response PullResponse
	if err := decodeProtocol2JSON(exchange.body, &response); err != nil {
		return PullResponse{}, exchange, errors.New("protocol 2 pull response is invalid")
	}
	response.ProtocolVersion = ProtocolVersion
	return response, exchange, nil
}

func (runtime *ProbeRuntime) Rebuild(ctx context.Context, request RebuildRequest) (RebuildResponse, exchange, error) {
	if err := validateProtocolVersion(request.ProtocolVersion); err != nil || request.ClientID == "" || request.Scope == "" || request.Limit < 1 {
		return RebuildResponse{}, exchange{}, errors.New("protocol 2 rebuild request is invalid")
	}
	exchange, err := runtime.runner.execute(ctx, EndpointRebuild, request, false)
	if err != nil {
		return RebuildResponse{}, exchange, err
	}
	if exchange.status != http.StatusOK {
		return RebuildResponse{}, exchange, endpointStatusError(EndpointRebuild, exchange)
	}
	var response RebuildResponse
	if err := decodeProtocol2JSON(exchange.body, &response); err != nil {
		return RebuildResponse{}, exchange, errors.New("protocol 2 rebuild response is invalid")
	}
	response.ProtocolVersion = ProtocolVersion
	return response, exchange, nil
}

func (runner *Runner) execute(ctx context.Context, endpoint Endpoint, payload any, dropAfterSuccess bool) (exchange, error) {
	if runner == nil || ctx == nil {
		return exchange{}, errors.New("baseline HTTP execution is unavailable")
	}
	if err := validateEndpoint(endpoint); err != nil {
		return exchange{}, err
	}
	body, err := json.Marshal(payload)
	if err != nil || int64(len(body)) > runner.maximumBody {
		return exchange{}, errors.New("encode protocol 2 request failed")
	}
	requestAttachment, err := runner.attachments.put("raw_http_request", body)
	if err != nil {
		return exchange{}, err
	}
	target := *runner.baseURL
	target.Path = "/sync/" + string(endpoint)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return exchange{}, errors.New("create protocol 2 HTTP request failed")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+runner.bearerToken)
	request.Header.Set("X-Synchro-Protocol-Version", "2")
	request.Header.Set("X-Synchro-Diagnostic-Class", string(nonReleaseClass))
	response, err := runner.httpClient.Do(request)
	if err != nil {
		receipt := runner.recordReceipt(endpoint, 0, requestAttachment, nil)
		return exchange{receipt: receipt, transport: errors.New("protocol 2 HTTP transport failed")}, errors.New("protocol 2 HTTP transport failed")
	}
	if response == nil || response.Body == nil {
		receipt := runner.recordReceipt(endpoint, 0, requestAttachment, nil)
		return exchange{receipt: receipt}, errors.New("protocol 2 HTTP response is incomplete")
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, runner.maximumBody+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || int64(len(responseBody)) > runner.maximumBody {
		receipt := runner.recordReceipt(endpoint, response.StatusCode, requestAttachment, nil)
		return exchange{status: response.StatusCode, receipt: receipt}, errors.New("protocol 2 HTTP response is invalid")
	}
	responseAttachment, err := runner.attachments.put("raw_http_response", responseBody)
	if err != nil {
		return exchange{}, err
	}
	receipt := runner.recordReceipt(endpoint, response.StatusCode, requestAttachment, &responseAttachment)
	exchange := exchange{body: append([]byte(nil), responseBody...), status: response.StatusCode, receipt: receipt}
	if dropAfterSuccess {
		exchange.transport = errors.New("diagnostic response was dropped after upstream success")
	}
	return exchange, nil
}

func (runner *Runner) recordReceipt(endpoint Endpoint, status int, request Attachment, response *Attachment) DiagnosticReceipt {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.nextID++
	hash := sha256.New()
	_, _ = hash.Write([]byte("synchro:baseline-receipt:v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(endpoint))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(request.id))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(fmt.Sprintf("%d:%d", status, runner.nextID)))
	if response != nil {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(response.id))
	}
	receipt := DiagnosticReceipt{
		id:       "baseline-receipt-sha256:" + hex.EncodeToString(hash.Sum(nil)),
		endpoint: endpoint,
		status:   status,
		request:  request,
	}
	if response != nil {
		copy := *response
		receipt.response = &copy
	}
	runner.receipts = append(runner.receipts, receipt)
	return receipt
}

func endpointStatusError(endpoint Endpoint, exchange exchange) error {
	var response ErrorResponse
	if decodeProtocol2JSON(exchange.body, &response) == nil && len(response.Error) != 0 {
		return fmt.Errorf("protocol 2 %s returned an error response", endpoint)
	}
	return fmt.Errorf("protocol 2 %s returned an unexpected HTTP status", endpoint)
}

func (runtime *ProbeRuntime) sourceDML(ctx context.Context, statement string, arguments ...any) error {
	if runtime == nil || runtime.runner == nil || !isSourceDML(statement) {
		return errors.New("diagnostic source DML is invalid")
	}
	return runtime.runner.source.ExecContext(ctx, statement, arguments...)
}

func (runtime *ProbeRuntime) dropHydrationColumn(ctx context.Context) error {
	return runtime.runner.operator.DropHydrationColumn(ctx)
}

func (runtime *ProbeRuntime) restoreHydrationColumn(ctx context.Context) error {
	return runtime.runner.operator.RestoreHydrationColumn(ctx)
}

func (runtime *ProbeRuntime) registerSchemaQueue(ctx context.Context) error {
	return runtime.runner.operator.RegisterSchemaQueue(ctx)
}

func (runtime *ProbeRuntime) restoreSchemaQueue(ctx context.Context) error {
	if err := runtime.restoreHydrationColumn(ctx); err != nil {
		return err
	}
	return runtime.registerSchemaQueue(ctx)
}

func (runtime *ProbeRuntime) configureDecodeTrap(ctx context.Context) error {
	return runtime.registerDecodeTrap(ctx, "deleted_at")
}

func (runtime *ProbeRuntime) restoreDecodeTrap(ctx context.Context) error {
	return runtime.registerDecodeTrap(ctx, "id")
}

func (runtime *ProbeRuntime) registerDecodeTrap(ctx context.Context, primaryKey string) error {
	if runtime == nil || runtime.runner == nil {
		return errors.New("diagnostic decode control is invalid")
	}
	return runtime.runner.operator.ConfigureDecodeTrap(ctx, primaryKey)
}

func (runtime *ProbeRuntime) registerLateSourceTable(ctx context.Context) error {
	return runtime.runner.operator.RegisterLateSourceTable(ctx)
}

func (runtime *ProbeRuntime) unregisterLateSourceTable(ctx context.Context) error {
	return runtime.runner.operator.UnregisterLateSourceTable(ctx)
}

func (runtime *ProbeRuntime) configureCrossScopeTable(ctx context.Context) error {
	return runtime.runner.operator.ConfigureCrossScopeTable(ctx)
}

func (runtime *ProbeRuntime) restoreCrossScopeTable(ctx context.Context) error {
	return runtime.runner.operator.RestoreCrossScopeTable(ctx)
}

func (runtime *ProbeRuntime) reloadRegistry(ctx context.Context) error {
	return runtime.runner.operator.ReloadRegistry(ctx)
}

func (runtime *ProbeRuntime) compactWithPositiveInterval(ctx context.Context) (CompactionResult, error) {
	if runtime == nil || runtime.runner == nil || ctx == nil {
		return CompactionResult{}, errors.New("diagnostic compaction control is invalid")
	}
	raw, err := runtime.runner.operator.CompactPositiveInterval(ctx)
	if err != nil {
		return CompactionResult{}, err
	}
	var result CompactionResult
	if err := decodeProtocol2JSON(raw, &result); err != nil {
		return CompactionResult{}, errors.New("diagnostic compaction result is invalid")
	}
	return result, nil
}

func (runtime *ProbeRuntime) commitInReverseBeginOrder(ctx context.Context, firstStatement string, firstArguments []any, secondStatement string, secondArguments []any) error {
	if runtime == nil || runtime.runner == nil || !isSourceDML(firstStatement) || !isSourceDML(secondStatement) {
		return errors.New("diagnostic transaction barrier is invalid")
	}
	return runtime.runner.source.CommitInReverseBeginOrder(ctx, firstStatement, firstArguments, secondStatement, secondArguments)
}

func isSourceDML(statement string) bool {
	normalized := strings.ToLower(strings.TrimSpace(statement))
	if strings.Contains(normalized, "sync_") {
		return false
	}
	return strings.HasPrefix(normalized, "insert into cf_") || strings.HasPrefix(normalized, "update cf_") || strings.HasPrefix(normalized, "delete from cf_")
}
