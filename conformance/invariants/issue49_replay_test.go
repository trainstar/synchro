package invariants

import "testing"

func TestIssue49OpaqueServerVersions(t *testing.T) {
	observation := issue49OpaqueVersionObservation{
		ServerVersion:     "09z.server/version+opaque==",
		PersistedVersion:  "09z.server/version+opaque==",
		ReconciledVersion: "09z.server/version+opaque==",
		BaseVersion:       "09z.server/version+opaque==",
		ServerGenerated:   true,
	}
	mutant := observation
	mutant.PersistedVersion = "9"
	issue49Proof(t, "SYNC-VERSION-001", issue49OpaqueVersionValid(observation), issue49OpaqueVersionValid(mutant))
}

func TestIssue49MutationReplayAcrossBatches(t *testing.T) {
	observation := issue49CrossBatchReplay{
		Stored: issue49StoredMutation{
			UserID:           "user-17",
			ClientID:         "client-9",
			OriginalBatchID:  "01998bdf-1890-7bb6-a38a-d17deeb08ad7",
			MutationID:       "01998bdf-3ba9-7a40-a132-dd18567e7041",
			Fingerprint:      "2c19a0d9e68e14e63f349715d407d4c2925b8e586fa8b7ad02e9673435c3e648",
			CanonicalOutcome: `{"mutation_id":"01998bdf-3ba9-7a40-a132-dd18567e7041","status":"applied","server_version":"opaque-v17"}`,
			Executions:       1,
		},
		EqualReplay: issue49CrossBatchAttempt{
			BatchID:          "01998bdf-663b-70c4-b4fd-7625e31c0e8b",
			Fingerprint:      "2c19a0d9e68e14e63f349715d407d4c2925b8e586fa8b7ad02e9673435c3e648",
			HTTPStatus:       200,
			CanonicalOutcome: `{"mutation_id":"01998bdf-3ba9-7a40-a132-dd18567e7041","status":"applied","server_version":"opaque-v17"}`,
		},
		DifferentReplay: issue49CrossBatchAttempt{
			BatchID:              "01998bdf-898c-75d2-89b0-1e915bfe6acc",
			Fingerprint:          "7a21aa5836d9c976f64693817bfe7b16e128cbe0ce62a2e84d8ac7f8124b4d2f",
			HTTPStatus:           409,
			ErrorCode:            "idempotency_conflict",
			WholeRequestRejected: true,
		},
	}
	mutant := observation
	mutant.EqualReplay.MutationExecutions = 1
	issue49Proof(t, "SYNC-IDEMPOTENCY-002", issue49CrossBatchReplayValid(observation), issue49CrossBatchReplayValid(mutant))
}
