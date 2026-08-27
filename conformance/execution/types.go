// Package execution contains shared raw execution observation types.
package execution

// Result is one closed terminal result.
type Result string

const (
	ResultPassed Result = "passed"
	ResultFailed Result = "failed"
	ResultError  Result = "error"
)

// ArtifactBinding identifies one executed artifact payload.
type ArtifactBinding struct {
	InventoryID string `json:"inventory_id"`
	ArtifactID  string `json:"artifact_id"`
	Role        string `json:"role,omitempty"`
	Path        string `json:"path"`
	MediaType   string `json:"media_type,omitempty"`
	Size        int64  `json:"size_bytes"`
	SizeBytes   int64  `json:"-"`
	SHA256      string `json:"sha256"`
}

// EnvironmentDimension records one allowlisted support-cell dimension.
type EnvironmentDimension struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// VectorResult records one authored vector-set result.
type VectorResult struct {
	VectorSetID        string `json:"vector_set_id"`
	SourceSHA256       string `json:"source_sha256"`
	AggregateSHA256    string `json:"aggregate_sha256"`
	Language           string `json:"language"`
	ArtifactID         string `json:"artifact_id"`
	Outcome            string `json:"outcome"`
	ResultAttachmentID string `json:"result_attachment_id"`
	ExecutedCount      int    `json:"executed_count"`
	PassedCount        int    `json:"passed_count"`
	FailedCount        int    `json:"failed_count"`
}

// Attachment describes one bounded execution artifact.
type Attachment struct {
	ID        string `json:"id"`
	Kind      string `json:"kind"`
	Path      string `json:"path"`
	MediaType string `json:"media_type"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// AttachmentPublisher stores one sanitized execution artifact.
type AttachmentPublisher interface {
	Publish(kind, mediaType string, data []byte) (Attachment, error)
}

// RequestCounts records the finite request classes used by performance proof.
type RequestCounts struct {
	Connect     int `json:"connect"`
	Push        int `json:"push"`
	Pull        int `json:"pull"`
	RebuildPage int `json:"rebuild_page"`
	SchemaFetch int `json:"schema_fetch"`
	Other       int `json:"other"`
}

// PerformanceMeasurement contains request and hop counters for one budget.
type PerformanceMeasurement struct {
	RequestCounts            RequestCounts `json:"request_counts"`
	ReturnedRebuildPageCount int           `json:"returned_rebuild_page_count"`
	OutboundNetworkOrRPCHops int           `json:"outbound_network_or_rpc_hops"`
}

// MetricValue records one numeric observation.
type MetricValue struct {
	MetricID string  `json:"metric_id"`
	Value    float64 `json:"value"`
}
