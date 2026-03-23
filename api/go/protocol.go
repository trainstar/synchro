package synchroapi

// ConnectRequestVNext is the request body for POST /sync/connect.
type ConnectRequestVNext struct {
	ClientID string `json:"client_id"`
}

// PullRequestVNext is the minimal validated request envelope for vNext pull.
type PullRequestVNext struct {
	ClientID string `json:"client_id"`
}

// PushRequestVNext is the minimal validated request envelope for vNext push.
type PushRequestVNext struct {
	ClientID string `json:"client_id"`
}

// RebuildRequestVNext is the minimal validated request envelope for vNext rebuild.
type RebuildRequestVNext struct {
	ClientID string `json:"client_id"`
	Scope    string `json:"scope"`
}
