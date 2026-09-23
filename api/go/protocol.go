package synchroapi

// ConnectRequest is the request body for POST /sync/connect.
type ConnectRequest struct {
	ClientID string `json:"client_id"`
}

// PullRequest is the minimal validated request envelope for pull.
type PullRequest struct {
	ClientID string `json:"client_id"`
}

// PushRequest is the minimal validated request envelope for push.
type PushRequest struct {
	ClientID string `json:"client_id"`
}

// RebuildRequest is the minimal validated request envelope for rebuild.
type RebuildRequest struct {
	ClientID string `json:"client_id"`
	Scope    string `json:"scope"`
}

// Request member lists are the transport-level allowlists for each endpoint.
// Semantic validation remains the responsibility of the extension.
var (
	connectRequestMembers = []string{
		"client_id",
		"client_generation",
		"platform",
		"app_version",
		"protocol_version",
		"schema_reset",
		"schema",
		"scope_set_version",
		"known_scopes",
		"seed_receipts",
	}
	pullRequestMembers = []string{
		"client_id",
		"client_generation",
		"schema",
		"scope_set_version",
		"scopes",
		"limit",
	}
	pushRequestMembers = []string{
		"client_id",
		"client_generation",
		"batch_id",
		"schema",
		"mutations",
	}
	rebuildRequestMembers = []string{
		"client_id",
		"client_generation",
		"schema",
		"scope",
		"rebuild_id",
		"cursor",
		"limit",
	}
)
