package invariants

import "testing"

func TestIssue49SynchroOwnedRowCRUD(t *testing.T) {
	registered := []string{"notes", "projects"}
	transitions := []issue49CRUDTransition{
		{Table: "notes", Operation: "insert", Canonical: true, AfterExists: true, AfterVersion: "n-v1", DurableOutcome: "accepted", SourceCommitted: true},
		{Table: "notes", Operation: "update", Canonical: true, BeforeExists: true, AfterExists: true, BeforeVersion: "n-v1", AfterVersion: "n-v2", DurableOutcome: "accepted", SourceCommitted: true},
		{Table: "notes", Operation: "delete", Canonical: true, BeforeExists: true, AfterExists: true, AfterDeleted: true, BeforeVersion: "n-v2", AfterVersion: "n-v3", DurableOutcome: "accepted", SourceCommitted: true},
		{Table: "projects", Operation: "insert", Canonical: true, AfterExists: true, AfterVersion: "p-v1", DurableOutcome: "accepted", SourceCommitted: true},
		{Table: "projects", Operation: "update", Canonical: true, BeforeExists: true, AfterExists: true, BeforeVersion: "p-v1", AfterVersion: "p-v2", DurableOutcome: "accepted", SourceCommitted: true},
		{Table: "projects", Operation: "delete", Canonical: true, BeforeExists: true, AfterExists: true, AfterDeleted: true, BeforeVersion: "p-v2", AfterVersion: "p-v3", DurableOutcome: "accepted", SourceCommitted: true},
	}
	mutant := append([]issue49CRUDTransition(nil), transitions[:len(transitions)-1]...)
	issue49Proof(t, "SYNC-CRUD-001", issue49CRUDValid(registered, transitions), issue49CRUDValid(registered, mutant))
}

func TestIssue49MutationOutcomeConservation(t *testing.T) {
	observation := issue49MutationConservation{
		Captured: []string{"m-pending", "m-accepted", "m-rejected", "m-superseded", "m-cancelled", "m-blocked"},
		Queue: []issue49Mutation{
			{ID: "m-pending", State: "pending", Durable: true, Inspectable: true},
			{ID: "m-accepted", State: "accepted", Durable: true, Inspectable: true},
			{ID: "m-rejected", State: "server_rejected", Durable: true, Inspectable: true},
			{ID: "m-superseded", State: "superseded_before_send", Durable: true, Inspectable: true},
			{ID: "m-cancelled", State: "cancelled_before_send", Durable: true, Inspectable: true},
			{ID: "m-blocked", State: "blocked_by_predecessor", PredecessorID: "m-pending", Durable: true, Inspectable: true},
		},
		Outcomes: []issue49MutationOutcome{
			{MutationID: "m-accepted", Kind: "accepted", Durable: true, Inspectable: true},
			{MutationID: "m-rejected", Kind: "server_rejected", Durable: true, Inspectable: true},
			{MutationID: "m-superseded", Kind: "superseded_before_send", Durable: true, Inspectable: true},
			{MutationID: "m-cancelled", Kind: "cancelled_before_send", Durable: true, Inspectable: true},
			{MutationID: "m-blocked", Kind: "blocked_by_predecessor", Durable: true, Inspectable: true},
		},
	}
	mutant := observation
	mutant.Outcomes = append([]issue49MutationOutcome(nil), observation.Outcomes...)
	mutant.Outcomes = mutant.Outcomes[1:]
	issue49Proof(t, "SYNC-MUTATION-001", issue49MutationConservationValid(observation), issue49MutationConservationValid(mutant))
}

func TestIssue49NoDuplicateLocalIntent(t *testing.T) {
	observation := issue49LocalIntent{
		LogicalActions:       1,
		StableMutationIDs:    1,
		DurableQueueEntries:  1,
		LocalRowTransitions:  1,
		RequestReplays:       2,
		ResponseReplays:      2,
		AuthoritativeEchoes:  1,
		EchoCreatedMutations: 0,
		EchoRowTransitions:   0,
	}
	mutant := observation
	mutant.DurableQueueEntries++
	issue49Proof(t, "SYNC-MUTATION-003", issue49LocalIntentValid(observation), issue49LocalIntentValid(mutant))
}

func TestIssue49AtomicVersionCompareAndSwap(t *testing.T) {
	races := issue49CASRaces()
	mutant := issue49CASRaces()
	mutant[0].FenceTransaction = "tx-split-fence"
	issue49Proof(t, "SYNC-CONFLICT-001", issue49AtomicCASValid(races), issue49AtomicCASValid(mutant))
}

func TestIssue49ConcurrentUpdateDeleteConservation(t *testing.T) {
	races := issue49CASRaces()[:2]
	mutant := issue49CASRaces()[:2]
	mutant[0].Attempts[1].Outcome = "applied"
	mutant[0].Attempts[1].SourcePreserved = true
	issue49Proof(t, "SYNC-CONFLICT-002", issue49ConcurrentUpdateDeleteValid(races), issue49ConcurrentUpdateDeleteValid(mutant))
}

func TestIssue49ClientTimeHasNoConflictAuthority(t *testing.T) {
	observation := []issue49TimeOutcome{
		{Case: "equal", BaseState: "row-v8", ClientVersion: "2026-09-09T12:00:00Z", Winner: "update", ServerVersion: "server-v9", SourceUpdated: "2026-09-09T12:00:01Z", WriteOrdinal: 44, Outcome: "accepted", DiagnosticKept: true},
		{Case: "past", BaseState: "row-v8", ClientVersion: "2000-01-01T00:00:00Z", Winner: "update", ServerVersion: "server-v9", SourceUpdated: "2026-09-09T12:00:01Z", WriteOrdinal: 44, Outcome: "accepted", DiagnosticKept: true},
		{Case: "future", BaseState: "row-v8", ClientVersion: "2099-12-31T23:59:59Z", Winner: "update", ServerVersion: "server-v9", SourceUpdated: "2026-09-09T12:00:01Z", WriteOrdinal: 44, Outcome: "accepted", DiagnosticKept: true},
	}
	mutant := append([]issue49TimeOutcome(nil), observation...)
	mutant[2].ServerVersion = "server-v10"
	issue49Proof(t, "SYNC-TIME-002", issue49ClientTimeValid(observation), issue49ClientTimeValid(mutant))
}

func TestIssue49CanonicalOperationVocabulary(t *testing.T) {
	observation := issue49Vocabulary{
		AcceptedPush: []string{"insert", "update", "delete"},
		EmittedPull:  []string{"upsert", "delete"},
		RejectedPush: []string{"create", "upsert", "resurrect", "merge", "patch", "restore"},
	}
	mutant := observation
	mutant.AcceptedPush = append(append([]string(nil), observation.AcceptedPush...), "create")
	issue49Proof(t, "SYNC-VOCAB-001", issue49VocabularyValid(observation), issue49VocabularyValid(mutant))
}

func issue49CASRaces() []issue49CASRace {
	return []issue49CASRace{
		{
			Kind:                  "update-delete-update-wins",
			LockedRow:             true,
			LockedVersionIdentity: true,
			WinnerVersion:         "server-v2",
			SourceTransaction:     "tx-update",
			FenceTransaction:      "tx-update",
			Attempts: []issue49CASAttempt{
				{Operation: "update", BaseVersion: "server-v1", Outcome: "applied", CurrentVersion: "server-v2", SourcePreserved: true, Authoritative: true},
				{Operation: "delete", BaseVersion: "server-v1", Outcome: "conflict", CurrentVersion: "server-v2", Authoritative: true},
			},
		},
		{
			Kind:                  "update-delete-delete-wins",
			LockedRow:             true,
			LockedVersionIdentity: true,
			WinnerVersion:         "server-v3",
			SourceTransaction:     "tx-delete",
			FenceTransaction:      "tx-delete",
			Attempts: []issue49CASAttempt{
				{Operation: "delete", BaseVersion: "server-v2", Outcome: "applied", CurrentVersion: "server-v3", SourcePreserved: true, Authoritative: true},
				{Operation: "update", BaseVersion: "server-v2", Outcome: "conflict", CurrentVersion: "server-v3", Authoritative: true},
			},
		},
		{
			Kind:                  "insert-reservation",
			LockedRow:             true,
			LockedVersionIdentity: true,
			WinnerVersion:         "server-v4",
			SourceTransaction:     "tx-insert",
			FenceTransaction:      "tx-insert",
			InsertReservations:    1,
			Attempts: []issue49CASAttempt{
				{Operation: "insert", Outcome: "applied", CurrentVersion: "server-v4", SourcePreserved: true, Authoritative: true},
				{Operation: "insert", Outcome: "conflict", CurrentVersion: "server-v4", Authoritative: true},
			},
		},
	}
}
