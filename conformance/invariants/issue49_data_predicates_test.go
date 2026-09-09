package invariants

type issue49OpaqueVersionObservation struct {
	ServerVersion     string
	PersistedVersion  string
	ReconciledVersion string
	BaseVersion       string
	ServerGenerated   bool
	Parsed            bool
	Sorted            bool
	Incremented       bool
	TimestampCompared bool
	DerivedFromClient bool
}

func issue49OpaqueVersionValid(observation issue49OpaqueVersionObservation) bool {
	return observation.ServerVersion != "" &&
		observation.PersistedVersion == observation.ServerVersion &&
		observation.ReconciledVersion == observation.ServerVersion &&
		observation.BaseVersion == observation.ServerVersion &&
		observation.ServerGenerated &&
		!observation.Parsed &&
		!observation.Sorted &&
		!observation.Incremented &&
		!observation.TimestampCompared &&
		!observation.DerivedFromClient
}

type issue49StoredMutation struct {
	UserID           string
	ClientID         string
	OriginalBatchID  string
	MutationID       string
	Fingerprint      string
	CanonicalOutcome string
	Executions       int
}

type issue49CrossBatchAttempt struct {
	BatchID              string
	Fingerprint          string
	HTTPStatus           int
	ErrorCode            string
	CanonicalOutcome     string
	MutationExecutions   int
	WholeRequestRejected bool
}

type issue49CrossBatchReplay struct {
	Stored          issue49StoredMutation
	EqualReplay     issue49CrossBatchAttempt
	DifferentReplay issue49CrossBatchAttempt
}

func issue49CrossBatchReplayValid(observation issue49CrossBatchReplay) bool {
	stored := observation.Stored
	if stored.UserID == "" || stored.ClientID == "" || stored.OriginalBatchID == "" || stored.MutationID == "" || stored.Fingerprint == "" || stored.CanonicalOutcome == "" || stored.Executions != 1 {
		return false
	}
	equal := observation.EqualReplay
	if equal.BatchID == "" || equal.BatchID == stored.OriginalBatchID || equal.Fingerprint != stored.Fingerprint || equal.HTTPStatus != 200 || equal.ErrorCode != "" || equal.CanonicalOutcome != stored.CanonicalOutcome || equal.MutationExecutions != 0 || equal.WholeRequestRejected {
		return false
	}
	different := observation.DifferentReplay
	return different.BatchID != "" &&
		different.BatchID != stored.OriginalBatchID &&
		different.BatchID != equal.BatchID &&
		different.Fingerprint != "" &&
		different.Fingerprint != stored.Fingerprint &&
		different.HTTPStatus == 409 &&
		different.ErrorCode == "idempotency_conflict" &&
		different.CanonicalOutcome == "" &&
		different.MutationExecutions == 0 &&
		different.WholeRequestRejected
}
