package blackbox

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultMetadataLimit  = 256
	maximumMetadataLimit  = 4096
	defaultRawBodyLimit   = int64(1 << 20)
	maximumRawBodyLimit   = int64(16 << 20)
	defaultHeaderValues   = 8
	maximumHeaderValues   = 32
	defaultHeaderValueLen = 512
	maximumHeaderValueLen = 4096
)

var (
	// ErrRecorderBound reports metadata or body data outside configured bounds.
	ErrRecorderBound = errors.New("recorder bound exceeded")
	// ErrSensitiveRecording reports token bytes found in a recorded body.
	ErrSensitiveRecording = errors.New("sensitive bytes cannot be recorded")
)

var relevantResponseHeaders = []string{
	"Content-Type",
	"ETag",
	"Retry-After",
	"X-Synchro-Protocol-Version",
}

// RecorderConfig contains closed metadata and private attachment bounds.
type RecorderConfig struct {
	AttachmentRoot      string
	MaxRecords          int
	MaxRawBodyBytes     int64
	MaxHeaderValues     int
	MaxHeaderValueBytes int
}

// RecordedHeader is one bounded response header.
type RecordedHeader struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

// ExchangeMetadata records bounded transport facts without request headers.
type ExchangeMetadata struct {
	RequestClass         string           `json:"request_class"`
	Status               int              `json:"status"`
	Headers              []RecordedHeader `json:"headers"`
	DurationNanoseconds  int64            `json:"duration_nanoseconds"`
	RequestBodySHA256    string           `json:"request_body_sha256"`
	ResponseBodySHA256   string           `json:"response_body_sha256"`
	RequestAttachmentID  string           `json:"request_attachment_id"`
	ResponseAttachmentID string           `json:"response_attachment_id"`
}

// Recorder owns bounded metadata and private raw-body attachments.
type Recorder struct {
	mu      sync.Mutex
	config  RecorderConfig
	root    string
	records []ExchangeMetadata
}

// NewRecorder creates one bounded recorder with a private attachment root.
func NewRecorder(config RecorderConfig) (*Recorder, error) {
	if config.AttachmentRoot == "" {
		return nil, errors.New("recorder attachment root is required")
	}
	if config.MaxRecords == 0 {
		config.MaxRecords = defaultMetadataLimit
	}
	if config.MaxRawBodyBytes == 0 {
		config.MaxRawBodyBytes = defaultRawBodyLimit
	}
	if config.MaxHeaderValues == 0 {
		config.MaxHeaderValues = defaultHeaderValues
	}
	if config.MaxHeaderValueBytes == 0 {
		config.MaxHeaderValueBytes = defaultHeaderValueLen
	}
	if config.MaxRecords < 1 || config.MaxRecords > maximumMetadataLimit ||
		config.MaxRawBodyBytes < 1 || config.MaxRawBodyBytes > maximumRawBodyLimit ||
		config.MaxHeaderValues < 1 || config.MaxHeaderValues > maximumHeaderValues ||
		config.MaxHeaderValueBytes < 1 || config.MaxHeaderValueBytes > maximumHeaderValueLen {
		return nil, fmt.Errorf("%w: invalid recorder configuration", ErrRecorderBound)
	}
	root, err := preparePrivateRoot(config.AttachmentRoot)
	if err != nil {
		return nil, err
	}
	return &Recorder{config: config, root: root}, nil
}

// Snapshot returns isolated metadata from the specified record offset.
func (r *Recorder) Snapshot(offset int) ([]ExchangeMetadata, error) {
	if r == nil {
		return nil, errors.New("recorder is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if offset < 0 || offset > len(r.records) {
		return nil, errors.New("recorder offset is invalid")
	}
	result := make([]ExchangeMetadata, len(r.records)-offset)
	for index := range result {
		result[index] = cloneExchangeMetadata(r.records[offset+index])
	}
	return result, nil
}

// Len returns the current bounded metadata count.
func (r *Recorder) Len() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

// AttachmentPath resolves one content-addressed attachment in the private root.
func (r *Recorder) AttachmentPath(id string) (string, error) {
	if r == nil {
		return "", errors.New("recorder is required")
	}
	digest, err := rawAttachmentDigest(id)
	if err != nil {
		return "", err
	}
	return filepath.Join(r.root, "raw-body-sha256-"+digest+".bin"), nil
}

func (r *Recorder) recordExchange(requestClass string, status int, headers http.Header, duration time.Duration, requestBody, responseBody []byte, sensitive [][]byte) (ExchangeMetadata, error) {
	if r == nil {
		return ExchangeMetadata{}, errors.New("recorder is required")
	}
	if !validRequestClass(requestClass) {
		return ExchangeMetadata{}, errors.New("request class is invalid")
	}
	if status < 0 || status > 599 || duration < 0 {
		return ExchangeMetadata{}, errors.New("exchange metadata is invalid")
	}
	if int64(len(requestBody)) > r.config.MaxRawBodyBytes || int64(len(responseBody)) > r.config.MaxRawBodyBytes {
		return ExchangeMetadata{}, fmt.Errorf("%w: raw body", ErrRecorderBound)
	}
	if containsSensitive(requestBody, sensitive) || containsSensitive(responseBody, sensitive) {
		return ExchangeMetadata{}, ErrSensitiveRecording
	}
	recordedHeaders, err := r.boundedHeaders(headers)
	if err != nil {
		return ExchangeMetadata{}, err
	}
	requestID, requestDigest, err := r.putRawBody(requestBody)
	if err != nil {
		return ExchangeMetadata{}, err
	}
	responseID, responseDigest, err := r.putRawBody(responseBody)
	if err != nil {
		return ExchangeMetadata{}, err
	}
	metadata := ExchangeMetadata{
		RequestClass:         requestClass,
		Status:               status,
		Headers:              recordedHeaders,
		DurationNanoseconds:  duration.Nanoseconds(),
		RequestBodySHA256:    requestDigest,
		ResponseBodySHA256:   responseDigest,
		RequestAttachmentID:  requestID,
		ResponseAttachmentID: responseID,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.records) >= r.config.MaxRecords {
		return ExchangeMetadata{}, fmt.Errorf("%w: metadata count", ErrRecorderBound)
	}
	r.records = append(r.records, cloneExchangeMetadata(metadata))
	return cloneExchangeMetadata(metadata), nil
}

func (r *Recorder) boundedHeaders(headers http.Header) ([]RecordedHeader, error) {
	result := make([]RecordedHeader, 0, len(relevantResponseHeaders))
	for _, name := range relevantResponseHeaders {
		values := headers.Values(name)
		if len(values) == 0 {
			continue
		}
		if len(values) > r.config.MaxHeaderValues {
			return nil, fmt.Errorf("%w: response header values", ErrRecorderBound)
		}
		copyValues := make([]string, len(values))
		for index, value := range values {
			if len(value) > r.config.MaxHeaderValueBytes || strings.ContainsAny(value, "\r\n") {
				return nil, fmt.Errorf("%w: response header value", ErrRecorderBound)
			}
			copyValues[index] = value
		}
		result = append(result, RecordedHeader{Name: http.CanonicalHeaderKey(name), Values: copyValues})
	}
	return result, nil
}

func (r *Recorder) putRawBody(body []byte) (string, string, error) {
	digest := sha256.Sum256(body)
	encoded := hex.EncodeToString(digest[:])
	id := "raw-body-sha256:" + encoded
	path := filepath.Join(r.root, "raw-body-sha256-"+encoded+".bin")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		if verifyErr := verifyExistingRawBody(path, body, r.config.MaxRawBodyBytes); verifyErr != nil {
			return "", "", verifyErr
		}
		return id, encoded, nil
	}
	if err != nil {
		return "", "", fmt.Errorf("create private raw-body attachment: %w", err)
	}
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return "", "", fmt.Errorf("write private raw-body attachment: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return "", "", fmt.Errorf("sync private raw-body attachment: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", "", fmt.Errorf("close private raw-body attachment: %w", err)
	}
	remove = false
	return id, encoded, nil
}

func preparePrivateRoot(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve private attachment root: %w", err)
	}
	info, err := os.Lstat(absolute)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(absolute, 0o700); err != nil {
			return "", fmt.Errorf("create private attachment root: %w", err)
		}
		info, err = os.Lstat(absolute)
	}
	if err != nil {
		return "", fmt.Errorf("inspect private attachment root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return "", errors.New("private attachment root must be a private directory")
	}
	return absolute, nil
}

func verifyExistingRawBody(path string, wanted []byte, limit int64) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect existing raw-body attachment: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > limit {
		return errors.New("existing raw-body attachment is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open existing raw-body attachment: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return fmt.Errorf("read existing raw-body attachment: %w", err)
	}
	if !bytes.Equal(data, wanted) {
		return errors.New("content-addressed raw-body attachment changed")
	}
	return nil
}

func rawAttachmentDigest(id string) (string, error) {
	const prefix = "raw-body-sha256:"
	if !strings.HasPrefix(id, prefix) {
		return "", errors.New("raw-body attachment ID is invalid")
	}
	value := strings.TrimPrefix(id, prefix)
	if len(value) != sha256.Size*2 {
		return "", errors.New("raw-body attachment digest is invalid")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || value != hex.EncodeToString(decoded) {
		return "", errors.New("raw-body attachment digest is invalid")
	}
	return value, nil
}

func containsSensitive(body []byte, sensitive [][]byte) bool {
	for _, value := range sensitive {
		if len(value) != 0 && bytes.Contains(body, value) {
			return true
		}
	}
	return false
}

func validRequestClass(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= 'A' && character <= 'Z':
		case character >= '0' && character <= '9':
		case strings.ContainsRune("._/-", character):
		default:
			return false
		}
	}
	return true
}

func cloneExchangeMetadata(source ExchangeMetadata) ExchangeMetadata {
	result := source
	result.Headers = make([]RecordedHeader, len(source.Headers))
	for index, header := range source.Headers {
		result.Headers[index] = RecordedHeader{Name: header.Name, Values: append([]string(nil), header.Values...)}
	}
	return result
}
