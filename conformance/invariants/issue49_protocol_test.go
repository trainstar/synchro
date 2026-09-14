package invariants

import "testing"

func TestIssue49LegalClientStateTransitions(t *testing.T) {
	states := []string{"uninitialized", "local_ready", "connecting", "schema_applying", "ready", "pushing", "pulling", "rebuilding", "backoff", "error", "stopped"}
	observedEdges := map[string][]string{
		"uninitialized":   {"local_ready", "error", "stopped"},
		"local_ready":     {"connecting", "error", "stopped"},
		"connecting":      {"schema_applying", "ready", "backoff", "error", "stopped"},
		"schema_applying": {"ready", "rebuilding", "error", "stopped"},
		"ready":           {"connecting", "pushing", "pulling", "rebuilding", "error", "stopped"},
		"pushing":         {"pushing", "ready", "pulling", "connecting", "backoff", "error", "stopped"},
		"pulling":         {"pulling", "ready", "rebuilding", "connecting", "backoff", "error", "stopped"},
		"rebuilding":      {"rebuilding", "ready", "connecting", "backoff", "error", "stopped"},
		"backoff":         {"connecting", "pushing", "pulling", "rebuilding", "error", "stopped"},
		"error":           {"local_ready", "stopped"},
		"stopped":         {"local_ready"},
	}
	transitions := make([]issue49LifecycleTransition, 0, len(states)*len(states)+1)
	for _, from := range states {
		for _, to := range states {
			accepted := containsString(observedEdges[from], to)
			before := "durable:" + from
			after := before
			if accepted {
				after = "durable:" + to + ":committed"
			}
			transitions = append(transitions, issue49LifecycleTransition{
				From:              from,
				To:                to,
				Accepted:          accepted,
				ContractError:     !accepted,
				BeforeFingerprint: before,
				AfterFingerprint:  after,
			})
		}
	}
	transitions = append(transitions, issue49LifecycleTransition{
		From:              "ready",
		To:                "unknown",
		ContractError:     true,
		BeforeFingerprint: "durable:ready",
		AfterFingerprint:  "durable:ready",
	})
	mutant := append([]issue49LifecycleTransition(nil), transitions...)
	for index := range mutant {
		if mutant[index].From == "stopped" && mutant[index].To == "pulling" {
			mutant[index].Accepted = true
			mutant[index].ContractError = false
			mutant[index].AfterFingerprint = "durable:pulling:committed"
			break
		}
	}
	issue49Proof(t, "SYNC-STATE-001", issue49LifecycleValid(states, transitions), issue49LifecycleValid(states, mutant))
}

func TestIssue49SemanticVersionPrecedence(t *testing.T) {
	transitions := []issue49VersionTransition{
		{ApplicationVersion: "1.0.0", MinimumVersion: "1.0.0", ProtocolVersion: 3, Accepted: true, StoredVersion: "1.0.0"},
		{ApplicationVersion: "2.0.0", MinimumVersion: "1.999999999999999999999999999.999999999999999999999999999", ProtocolVersion: 3, Accepted: true, StoredVersion: "2.0.0"},
		{ApplicationVersion: "999999999999999999999999999.0.0", MinimumVersion: "2.0.0", ProtocolVersion: 3, Accepted: true, StoredVersion: "999999999999999999999999999.0.0"},
		{ApplicationVersion: "1.0.0-alpha-beta", MinimumVersion: "1.0.0-alpha", ProtocolVersion: 3, Accepted: true, StoredVersion: "1.0.0-alpha-beta"},
		{ApplicationVersion: "1.0.0-alpha.1", MinimumVersion: "1.0.0-alpha.1", ProtocolVersion: 3, Accepted: true, StoredVersion: "1.0.0-alpha.1"},
		{ApplicationVersion: "1.0.0-beta.11", MinimumVersion: "1.0.0-beta.2", ProtocolVersion: 3, Accepted: true, StoredVersion: "1.0.0-beta.11"},
		{ApplicationVersion: "1.0.0+build.7", MinimumVersion: "1.0.0+build.99", ProtocolVersion: 3, Accepted: true, StoredVersion: "1.0.0+build.7"},
		{ApplicationVersion: "1.0.0-alpha", MinimumVersion: "1.0.0", ProtocolVersion: 3},
		{ApplicationVersion: "1.0.0-beta.2", MinimumVersion: "1.0.0-beta.11", ProtocolVersion: 3},
		{ApplicationVersion: "v1.0.0", MinimumVersion: "1.0.0", ProtocolVersion: 3},
		{ApplicationVersion: "01.0.0", MinimumVersion: "1.0.0", ProtocolVersion: 3},
		{ApplicationVersion: "1.0.0-alpha.01", MinimumVersion: "1.0.0", ProtocolVersion: 3},
		{ApplicationVersion: "1.0", MinimumVersion: "1.0.0", ProtocolVersion: 3},
		{ApplicationVersion: "1.0.0+", MinimumVersion: "1.0.0", ProtocolVersion: 3},
		{ApplicationVersion: "3.0.0", MinimumVersion: "2.0.0", ProtocolVersion: 2},
	}
	mutant := append([]issue49VersionTransition(nil), transitions...)
	mutant[8].Accepted = true
	mutant[8].StoredVersion = mutant[8].ApplicationVersion
	issue49Proof(t, "SYNC-CLIENT-VERSION-001", issue49VersionTransitionsValid(transitions), issue49VersionTransitionsValid(mutant))
}

func TestIssue49PortableWireIntegerRange(t *testing.T) {
	observation := issue49PortableIntegerObservation{
		EnvelopeValues: []issue49PortableInteger{
			{Value: -issue49MaxPortableInteger, Sign: "signed", Encoded: "-9007199254740991", Decoded: -issue49MaxPortableInteger},
			{Value: 0, Sign: "nonnegative", Encoded: "0", Decoded: 0},
			{Value: 1, Sign: "positive", Encoded: "1", Decoded: 1},
			{Value: issue49MaxPortableInteger, Sign: "positive", Encoded: "9007199254740991", Decoded: issue49MaxPortableInteger},
		},
		OpaqueDecimalValues: []string{"9007199254740992", "18446744073709551615"},
		CounterBefore:       issue49MaxPortableInteger,
		CounterAfter:        issue49MaxPortableInteger,
		OverflowAttempted:   true,
		OverflowRejected:    true,
		StateUnchanged:      true,
	}
	mutant := observation
	mutant.EnvelopeValues = append(append([]issue49PortableInteger(nil), observation.EnvelopeValues...), issue49PortableInteger{
		Value: issue49MaxPortableInteger + 1, Sign: "positive", Encoded: "9007199254740992", Decoded: issue49MaxPortableInteger + 1,
	})
	issue49Proof(t, "SYNC-PROTOCOL-004", issue49PortableIntegersValid(observation), issue49PortableIntegersValid(mutant))
}

func TestIssue49MutationOutcomeSchemaBinding(t *testing.T) {
	observation := issue49OutcomeSchemaObservation{
		OutcomeID:                "outcome-17",
		AuthoredSchema:           "schema-old",
		ClassificationSchema:     "schema-old",
		ChecksumSchema:           "schema-old",
		ReplaySchema:             "schema-old",
		CurrentSchema:            "schema-current",
		HistoricalManifestLoaded: true,
		HistoricalChecksumValid:  true,
		ProjectionSafe:           false,
		HistoricalAppliedCurrent: false,
		OutcomeInspectable:       true,
		LaterIntentBefore:        []string{"mutation-18", "mutation-19"},
		LaterIntentAfter:         []string{"mutation-18", "mutation-19"},
	}
	mutant := observation
	mutant.ReplaySchema = mutant.CurrentSchema
	issue49Proof(t, "SYNC-OUTCOME-002", issue49OutcomeSchemaValid(observation), issue49OutcomeSchemaValid(mutant))
}

func TestIssue49EffectLevelPullProgress(t *testing.T) {
	position0 := issue49EffectPosition{CommitLSN: 91, EventOrdinal: 4, EffectOrdinal: 0}
	position1 := issue49EffectPosition{CommitLSN: 91, EventOrdinal: 4, EffectOrdinal: 1}
	position2 := issue49EffectPosition{CommitLSN: 91, EventOrdinal: 4, EffectOrdinal: 2}
	observation := issue49EffectProgress{
		PersistedEffects: []issue49PullEffect{
			{ID: "effect-a", Position: position0, WALReplayIdentity: "wal-91-4"},
			{ID: "effect-b", Position: position1, WALReplayIdentity: "wal-91-4"},
			{ID: "effect-c", Position: position2, WALReplayIdentity: "wal-91-4"},
		},
		FirstPage:       []string{"effect-a"},
		FirstCursor:     position0,
		SecondPage:      []string{"effect-b", "effect-c"},
		FinalCursor:     position2,
		ReplayFirstPage: []string{"effect-a"},
	}
	mutant := observation
	mutant.FirstCursor = position1
	issue49Proof(t, "SYNC-PULL-005", issue49EffectProgressValid(observation), issue49EffectProgressValid(mutant))
}

func TestIssue49PostBoundaryChangeDelivery(t *testing.T) {
	observation := issue49RebuildBoundary{
		Boundary: 100,
		SnapshotRecords: []issue49BoundaryChange{
			{ID: "row-before", Kind: "write", Position: 99, IncludedInSnapshot: true},
			{ID: "row-at", Kind: "write", Position: 100, IncludedInSnapshot: true},
		},
		PostBoundary: []issue49BoundaryChange{
			{ID: "row-after-write", Kind: "write", Position: 101, IncrementalDelivered: true},
			{ID: "row-after-delete", Kind: "delete", Position: 102, IncrementalDelivered: true},
			{ID: "row-after-membership", Kind: "membership", Position: 103, IncrementalDelivered: true},
		},
		FinalCursor:     100,
		CursorPresented: true,
	}
	mutant := observation
	mutant.PostBoundary = append([]issue49BoundaryChange(nil), observation.PostBoundary...)
	mutant.PostBoundary[0].IncludedInSnapshot = true
	issue49Proof(t, "SYNC-REBUILD-008", issue49RebuildBoundaryValid(observation), issue49RebuildBoundaryValid(mutant))
}

func TestIssue49AcceptedWriteInvalidatesOlderRebuild(t *testing.T) {
	observation := issue49RebuildEpoch{
		SessionEpoch:           8,
		EpochBeforeWrite:       8,
		EpochAfterAccepted:     9,
		AcceptedFirstExecution: true,
		WriteAndEpochAtomic:    true,
		Responses: []issue49RebuildEpochResponse{
			{RequestKind: "later_page", Epoch: 8, Status: 409, ErrorCode: "rebuild_restart_required"},
			{RequestKind: "stored_replay", Epoch: 8, Status: 409, ErrorCode: "rebuild_restart_required"},
		},
	}
	mutant := observation
	mutant.Responses = append([]issue49RebuildEpochResponse(nil), observation.Responses...)
	mutant.Responses[1].Records = 1
	mutant.Responses[1].Progress = true
	issue49Proof(t, "SYNC-REBUILD-011", issue49RebuildEpochValid(observation), issue49RebuildEpochValid(mutant))
}
