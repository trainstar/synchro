// Package baseline contains non-release protocol 2 diagnostics.
package baseline

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const ProtocolVersion = 2

const (
	baselineReportFormat       ReportFormat    = "baseline-report-v1"
	nonReleaseClass            DiagnosticClass = "non_release_diagnostic"
	reportFileName                             = "baseline-report-v1.json"
	maximumDiagnosticBodyBytes int64           = 1 << 20
)

// ReportFormat permanently identifies a diagnostic report format.
type ReportFormat string

// DiagnosticClass permanently identifies non-release diagnostic output.
type DiagnosticClass string

// Endpoint is one supported protocol 2 HTTP operation.
type Endpoint string

const (
	EndpointConnect Endpoint = "connect"
	EndpointPush    Endpoint = "push"
	EndpointPull    Endpoint = "pull"
	EndpointRebuild Endpoint = "rebuild"
)

// DefectFamily identifies one known current diagnostic divergence.
type DefectFamily string

const (
	DefectCommitOrder        DefectFamily = "commit_order"
	DefectPullStarvation     DefectFamily = "pull_starvation"
	DefectHydrationFailure   DefectFamily = "hydration_failure"
	DefectDecodeFailure      DefectFamily = "decode_failure"
	DefectRegistryReload     DefectFamily = "registry_reload"
	DefectResponseLoss       DefectFamily = "response_loss"
	DefectForgedRebuild      DefectFamily = "forged_rebuild_cursor"
	DefectSchemaIntent       DefectFamily = "schema_intent"
	DefectCompactionInterval DefectFamily = "compaction_interval"
	DefectOwnershipChange    DefectFamily = "ownership_change"
	DefectChecksumEncoding   DefectFamily = "checksum_encoding"
	DefectCrossScopeDedup    DefectFamily = "cross_scope_dedup"
	DefectRebuildSnapshot    DefectFamily = "rebuild_live_snapshot"
)

// SchemaRef is the protocol 2 schema reference DTO.
type SchemaRef struct {
	Version int64  `json:"version"`
	Hash    string `json:"hash"`
}

// ScopeCursor is the protocol 2 per-scope cursor DTO.
type ScopeCursor struct {
	Cursor *string `json:"cursor"`
}

// ScopeAssignment is the protocol 2 scope assignment DTO.
type ScopeAssignment struct {
	ID     string  `json:"id"`
	Cursor *string `json:"cursor"`
}

// ScopeDelta is the protocol 2 scope-delta DTO.
type ScopeDelta struct {
	Add    []ScopeAssignment `json:"add"`
	Remove []string          `json:"remove"`
}

// SchemaDescriptor is the protocol 2 connect-response schema DTO.
type SchemaDescriptor struct {
	Version int64  `json:"version"`
	Hash    string `json:"hash"`
	Action  string `json:"action"`
}

// ConnectRequest is the explicit protocol 2 connect request DTO.
type ConnectRequest struct {
	ClientID        string                 `json:"client_id"`
	Platform        string                 `json:"platform"`
	AppVersion      string                 `json:"app_version"`
	ProtocolVersion int                    `json:"protocol_version"`
	Schema          SchemaRef              `json:"schema"`
	ScopeSetVersion int64                  `json:"scope_set_version"`
	KnownScopes     map[string]ScopeCursor `json:"known_scopes"`
}

// ConnectResponse is the explicit protocol 2 connect response DTO.
type ConnectResponse struct {
	ServerTime       string           `json:"server_time"`
	ProtocolVersion  int              `json:"protocol_version"`
	ScopeSetVersion  int64            `json:"scope_set_version"`
	Schema           SchemaDescriptor `json:"schema"`
	Scopes           ScopeDelta       `json:"scopes"`
	SchemaDefinition json.RawMessage  `json:"schema_definition,omitempty"`
}

// Mutation is the protocol 2 push mutation DTO.
type Mutation struct {
	MutationID    string          `json:"mutation_id"`
	Table         string          `json:"table"`
	Operation     string          `json:"op"`
	PrimaryKey    json.RawMessage `json:"pk"`
	BaseVersion   *string         `json:"base_version,omitempty"`
	ClientVersion *string         `json:"client_version,omitempty"`
	Columns       json.RawMessage `json:"columns,omitempty"`
}

// PushRequest is the explicit protocol 2 push request DTO.
type PushRequest struct {
	ProtocolVersion int        `json:"protocol_version"`
	ClientID        string     `json:"client_id"`
	BatchID         string     `json:"batch_id"`
	Schema          SchemaRef  `json:"schema"`
	Mutations       []Mutation `json:"mutations"`
}

// MutationOutcome is the protocol 2 push outcome DTO.
type MutationOutcome struct {
	MutationID    string          `json:"mutation_id"`
	Table         string          `json:"table"`
	PrimaryKey    json.RawMessage `json:"pk"`
	Status        string          `json:"status"`
	Code          string          `json:"code,omitempty"`
	Message       *string         `json:"message,omitempty"`
	ServerRow     json.RawMessage `json:"server_row,omitempty"`
	ServerVersion *string         `json:"server_version,omitempty"`
}

// PushResponse is the explicit protocol 2 push response DTO.
// ProtocolVersion is assigned by the fixed endpoint and never negotiated.
type PushResponse struct {
	ProtocolVersion int               `json:"-"`
	ServerTime      string            `json:"server_time"`
	Accepted        []MutationOutcome `json:"accepted"`
	Rejected        []MutationOutcome `json:"rejected"`
}

// PullRequest is the explicit protocol 2 pull request DTO.
type PullRequest struct {
	ProtocolVersion int                    `json:"protocol_version"`
	ClientID        string                 `json:"client_id"`
	Schema          SchemaRef              `json:"schema"`
	ScopeSetVersion int64                  `json:"scope_set_version"`
	Scopes          map[string]ScopeCursor `json:"scopes"`
	Limit           int                    `json:"limit"`
}

// ChangeRecord is the protocol 2 pull change DTO.
type ChangeRecord struct {
	Scope         string          `json:"scope"`
	Table         string          `json:"table"`
	Operation     string          `json:"op"`
	PrimaryKey    json.RawMessage `json:"pk"`
	Row           json.RawMessage `json:"row,omitempty"`
	RowChecksum   *int32          `json:"row_checksum,omitempty"`
	ServerVersion string          `json:"server_version"`
}

// PullResponse is the explicit protocol 2 pull response DTO.
// ProtocolVersion is assigned by the fixed endpoint and never negotiated.
type PullResponse struct {
	ProtocolVersion int               `json:"-"`
	Changes         []ChangeRecord    `json:"changes"`
	ScopeSetVersion int64             `json:"scope_set_version"`
	ScopeCursors    map[string]string `json:"scope_cursors"`
	ScopeUpdates    ScopeDelta        `json:"scope_updates"`
	Rebuild         []string          `json:"rebuild"`
	HasMore         bool              `json:"has_more"`
	Checksums       map[string]string `json:"checksums,omitempty"`
}

// RebuildRequest is the explicit protocol 2 rebuild request DTO.
type RebuildRequest struct {
	ProtocolVersion int     `json:"protocol_version"`
	ClientID        string  `json:"client_id"`
	Scope           string  `json:"scope"`
	Cursor          *string `json:"cursor"`
	Limit           int     `json:"limit"`
}

// RebuildRecord is the protocol 2 rebuild record DTO.
type RebuildRecord struct {
	Table         string          `json:"table"`
	PrimaryKey    json.RawMessage `json:"pk"`
	Row           json.RawMessage `json:"row,omitempty"`
	RowChecksum   *int32          `json:"row_checksum,omitempty"`
	ServerVersion string          `json:"server_version"`
}

// RebuildResponse is the explicit protocol 2 rebuild response DTO.
// ProtocolVersion is assigned by the fixed endpoint and never negotiated.
type RebuildResponse struct {
	ProtocolVersion  int             `json:"-"`
	Scope            string          `json:"scope"`
	Records          []RebuildRecord `json:"records"`
	Cursor           *string         `json:"cursor"`
	HasMore          bool            `json:"has_more"`
	FinalScopeCursor *string         `json:"final_scope_cursor,omitempty"`
	Checksum         *string         `json:"checksum,omitempty"`
}

// ErrorResponse is the explicit protocol 2 error DTO.
// Error remains raw because legacy adapter errors can be scalar or object values.
type ErrorResponse struct {
	Error json.RawMessage `json:"error"`
}

// CompactionResult is the bounded public result from synchro_compact.
type CompactionResult struct {
	DeactivatedClients int64 `json:"deactivated_clients"`
	SafeSequence       int64 `json:"safe_seq"`
	DeletedEntries     int64 `json:"deleted_entries"`
}

// OutputPath identifies a private, non-candidate diagnostic output directory.
type OutputPath struct {
	path  string
	class DiagnosticClass
}

// NewOutputPath creates a permanently typed non-release output path.
func NewOutputPath(path string) (OutputPath, error) {
	if strings.TrimSpace(path) == "" {
		return OutputPath{}, errors.New("baseline output path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return OutputPath{}, errors.New("baseline output path is invalid")
	}
	base := strings.ToLower(filepath.Base(absolute))
	if base != "baseline" && !strings.HasPrefix(base, "baseline-") {
		return OutputPath{}, errors.New("baseline output path must have a baseline name")
	}
	for _, part := range strings.Split(filepath.ToSlash(absolute), "/") {
		lower := strings.ToLower(part)
		if lower == "candidate" || lower == "candidates" || strings.HasPrefix(lower, "candidate-") {
			return OutputPath{}, errors.New("baseline output cannot use a candidate directory")
		}
	}
	for ancestor := filepath.Dir(absolute); ; ancestor = filepath.Dir(ancestor) {
		lockPath := filepath.Join(ancestor, "rc-candidate-lock.json")
		if _, err := os.Lstat(lockPath); err == nil {
			return OutputPath{}, errors.New("baseline output is beneath an RC candidate lock")
		} else if !errors.Is(err, os.ErrNotExist) {
			return OutputPath{}, errors.New("baseline output ancestor cannot be inspected")
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			break
		}
	}
	return OutputPath{path: absolute, class: nonReleaseClass}, nil
}

// Path returns the output path. The value remains classified as non-release.
func (path OutputPath) Path() string {
	return path.path
}

// Class returns the permanent diagnostic classification.
func (path OutputPath) Class() DiagnosticClass {
	return nonReleaseClass
}

// Attachment identifies a private raw diagnostic attachment.
type Attachment struct {
	id       string
	kind     string
	path     OutputPath
	relative string
	sha256   string
	size     int64
}

// ID returns the content-addressed attachment identifier.
func (attachment Attachment) ID() string {
	return attachment.id
}

// Kind returns the fixed attachment kind.
func (attachment Attachment) Kind() string {
	return attachment.kind
}

// SHA256 returns the attachment content digest.
func (attachment Attachment) SHA256() string {
	return attachment.sha256
}

// Size returns the attachment byte count.
func (attachment Attachment) Size() int64 {
	return attachment.size
}

// RelativePath returns the path below the typed diagnostic root.
func (attachment Attachment) RelativePath() string {
	return attachment.relative
}

// DiagnosticReceipt records one classified HTTP exchange without headers or token bytes.
type DiagnosticReceipt struct {
	id       string
	endpoint Endpoint
	status   int
	request  Attachment
	response *Attachment
}

// ID returns the receipt identifier.
func (receipt DiagnosticReceipt) ID() string {
	return receipt.id
}

// Endpoint returns the fixed protocol 2 endpoint.
func (receipt DiagnosticReceipt) Endpoint() Endpoint {
	return receipt.endpoint
}

// Status returns the observed HTTP status.
func (receipt DiagnosticReceipt) Status() int {
	return receipt.status
}

// RequestAttachment returns the private raw request attachment metadata.
func (receipt DiagnosticReceipt) RequestAttachment() Attachment {
	return receipt.request
}

// ResponseAttachment returns the private raw response attachment metadata.
func (receipt DiagnosticReceipt) ResponseAttachment() (Attachment, bool) {
	if receipt.response == nil {
		return Attachment{}, false
	}
	return *receipt.response, true
}

// ProbeResult describes one expected diagnostic divergence.
type ProbeResult struct {
	Family           DefectFamily `json:"family"`
	ExpectedContract string       `json:"expected_contract"`
	Divergence       string       `json:"divergence"`
	Captured         bool         `json:"captured"`
	ReceiptIDs       []string     `json:"receipt_ids"`
}

// Report is permanently a non-release baseline report.
type Report struct {
	createdAt time.Time
	output    OutputPath
	probes    []ProbeResult
	receipts  []DiagnosticReceipt
}

// Format returns baseline-report-v1 for every report.
func (report Report) Format() ReportFormat {
	return baselineReportFormat
}

// Classification returns non_release_diagnostic for every report.
func (report Report) Classification() DiagnosticClass {
	return nonReleaseClass
}

// CreatedAt returns the UTC report creation time.
func (report Report) CreatedAt() time.Time {
	return report.createdAt
}

// Output returns the permanently typed output path.
func (report Report) Output() OutputPath {
	return report.output
}

// Probes returns a copy of diagnostic probe results.
func (report Report) Probes() []ProbeResult {
	result := make([]ProbeResult, len(report.probes))
	for index, probe := range report.probes {
		result[index] = probe
		result[index].ReceiptIDs = append([]string(nil), probe.ReceiptIDs...)
	}
	return result
}

// Receipts returns a copy of classified diagnostic receipts.
func (report Report) Receipts() []DiagnosticReceipt {
	return append([]DiagnosticReceipt(nil), report.receipts...)
}

// Validate verifies non-release report isolation and attachment bindings.
func (report Report) Validate() error {
	if report.createdAt.IsZero() || report.output.class != nonReleaseClass || report.output.path == "" || len(report.probes) == 0 || len(report.receipts) == 0 {
		return errors.New("baseline report is invalid")
	}
	if report.output.Class() != nonReleaseClass {
		return errors.New("baseline report classification is invalid")
	}
	receipts := make(map[string]struct{}, len(report.receipts))
	for _, receipt := range report.receipts {
		if receipt.id == "" || receipt.endpoint == "" || receipt.status < 0 || receipt.request.id == "" || receipt.request.path.class != nonReleaseClass {
			return errors.New("baseline receipt is invalid")
		}
		if _, exists := receipts[receipt.id]; exists {
			return errors.New("baseline receipt is duplicated")
		}
		receipts[receipt.id] = struct{}{}
		if receipt.response != nil && receipt.response.path.class != nonReleaseClass {
			return errors.New("baseline response attachment is invalid")
		}
	}
	for _, probe := range report.probes {
		if probe.Family == "" || probe.ExpectedContract == "" || probe.Divergence == "" || !probe.Captured || len(probe.ReceiptIDs) == 0 {
			return errors.New("baseline probe result is invalid")
		}
		for _, identifier := range probe.ReceiptIDs {
			if _, exists := receipts[identifier]; !exists {
				return errors.New("baseline probe receipt is missing")
			}
		}
	}
	return nil
}

// MarshalJSON emits the permanently typed non-release representation.
func (report Report) MarshalJSON() ([]byte, error) {
	if err := report.Validate(); err != nil {
		return nil, err
	}
	type receiptJSON struct {
		Format         ReportFormat    `json:"format"`
		Classification DiagnosticClass `json:"classification"`
		ReceiptID      string          `json:"receipt_id"`
		Endpoint       Endpoint        `json:"endpoint"`
		Status         int             `json:"status"`
		Request        attachmentJSON  `json:"request"`
		Response       *attachmentJSON `json:"response,omitempty"`
	}
	type reportJSON struct {
		Format         ReportFormat    `json:"format"`
		Classification DiagnosticClass `json:"classification"`
		CreatedAt      time.Time       `json:"created_at"`
		OutputPath     string          `json:"output_path"`
		Probes         []ProbeResult   `json:"probes"`
		Receipts       []receiptJSON   `json:"receipts"`
	}
	receipts := make([]receiptJSON, len(report.receipts))
	for index, receipt := range report.receipts {
		item := receiptJSON{
			Format:         baselineReportFormat,
			Classification: nonReleaseClass,
			ReceiptID:      receipt.id,
			Endpoint:       receipt.endpoint,
			Status:         receipt.status,
			Request:        marshalAttachment(receipt.request),
		}
		if receipt.response != nil {
			response := marshalAttachment(*receipt.response)
			item.Response = &response
		}
		receipts[index] = item
	}
	return json.Marshal(reportJSON{
		Format:         baselineReportFormat,
		Classification: nonReleaseClass,
		CreatedAt:      report.createdAt.UTC(),
		OutputPath:     report.output.path,
		Probes:         report.Probes(),
		Receipts:       receipts,
	})
}

type attachmentJSON struct {
	Format         ReportFormat    `json:"format"`
	Classification DiagnosticClass `json:"classification"`
	ID             string          `json:"id"`
	Kind           string          `json:"kind"`
	Path           string          `json:"path"`
	SHA256         string          `json:"sha256"`
	Size           int64           `json:"size"`
}

func marshalAttachment(attachment Attachment) attachmentJSON {
	return attachmentJSON{
		Format:         baselineReportFormat,
		Classification: nonReleaseClass,
		ID:             attachment.id,
		Kind:           attachment.kind,
		Path:           attachment.relative,
		SHA256:         attachment.sha256,
		Size:           attachment.size,
	}
}

func validateProtocolVersion(value int) error {
	if value != ProtocolVersion {
		return errors.New("baseline request is not protocol version 2")
	}
	return nil
}

func validateEndpoint(endpoint Endpoint) error {
	switch endpoint {
	case EndpointConnect, EndpointPush, EndpointPull, EndpointRebuild:
		return nil
	default:
		return errors.New("baseline endpoint is invalid")
	}
}

func decodeProtocol2JSON(data []byte, destination any) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return errors.New("protocol 2 response is empty")
	}
	var header map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&header); err != nil {
		return errors.New("protocol 2 response is invalid")
	}
	if raw, found := header["protocol_version"]; found {
		var version int
		if err := json.Unmarshal(raw, &version); err != nil || version != ProtocolVersion {
			return errors.New("protocol 2 response has an unsupported protocol version")
		}
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("protocol 2 response does not match its DTO")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("protocol 2 response has trailing data")
	}
	return nil
}
