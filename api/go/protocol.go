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
