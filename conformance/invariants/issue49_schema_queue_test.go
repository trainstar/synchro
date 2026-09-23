package invariants

import "testing"

func TestIssue49CausallyBoundClass3ProjectionBootstrap(t *testing.T) {
	observation := issue49Class3Bootstrap{
		CandidateSlotPermanent: true,
		CandidateDatabase:      "synchro",
		CandidatePlugin:        "pgoutput",
		ExportedSnapshot:       "candidate-snapshot-17",
		SnapshotImported:       true,
		MainBarrier:            451,
		CandidateBarrier:       451,
		Projections: map[string]bool{
			"source":     true,
			"dependency": true,
			"version":    true,
			"membership": true,
			"integrity":  true,
		},
		CaughtUp:              true,
		ManifestActiveBefore:  false,
		ManifestActiveAfter:   true,
		GenerationsActive:     true,
		ProjectionsActive:     true,
		InvalidationActive:    true,
		ActivationTransaction: "activation-tx-22",
		ActivationTransactions: map[string]string{
			"manifest":     "activation-tx-22",
			"generations":  "activation-tx-22",
			"projections":  "activation-tx-22",
			"invalidation": "activation-tx-22",
		},
		FailureStateUnchanged: true,
	}
	mutant := observation
	mutant.CandidateBarrier++
	issue49Proof(t, "SYNC-SCHEMA-005", issue49Class3BootstrapValid(observation), issue49Class3BootstrapValid(mutant))
}

func TestIssue49CrashSafeClientSchemaMigration(t *testing.T) {
	observation := issue49SchemaMigration{
		JournalPersistedBeforeDDL: true,
		TargetManifestVerified:    true,
		TargetManifestComplete:    true,
		PlanVersion:               1,
		PlanTyped:                 true,
		SourceReferenceBound:      true,
		TargetReferenceBound:      true,
		ActionBound:               true,
		ScopesBound:               true,
		PhaseBound:                true,
		RestartFetchedSchema:      false,
		RestartPhases: []issue49MigrationPhase{
			{Name: "prepare", RestartBefore: true, RestartAfter: true, Committed: true, Executions: 1},
			{Name: "create-target", RestartBefore: true, RestartAfter: true, Committed: true, Executions: 1},
			{Name: "copy-data", RestartBefore: true, RestartAfter: true, Committed: true, Executions: 1},
			{Name: "activate-metadata", RestartBefore: true, RestartAfter: true, Committed: true, Executions: 1},
			{Name: "cleanup", RestartBefore: true, RestartAfter: true, Committed: true, Executions: 1},
		},
		QueueBefore:     []string{"mutation-1", "mutation-2"},
		QueueAfter:      []string{"mutation-1", "mutation-2"},
		LocalDataBefore: []string{"draft-1", "preference-2"},
		LocalDataAfter:  []string{"draft-1", "preference-2"},
		Consistency: map[string]bool{
			"physical":  true,
			"reference": true,
			"body":      true,
			"plan":      true,
			"phase":     true,
		},
		MismatchResults: []issue49MigrationMismatch{
			{Kind: "physical", ExplicitError: true},
			{Kind: "reference", ExplicitError: true},
			{Kind: "body", ExplicitError: true},
			{Kind: "plan", ExplicitError: true},
			{Kind: "phase", ExplicitError: true},
		},
	}
	mutant := observation
	mutant.MismatchResults = append([]issue49MigrationMismatch(nil), observation.MismatchResults...)
	mutant.MismatchResults[0].Completed = true
	issue49Proof(t, "SYNC-SCHEMA-006", issue49SchemaMigrationValid(observation), issue49SchemaMigrationValid(mutant))
}

func TestIssue49SchemaCursorContinuity(t *testing.T) {
	position := issue49EffectPosition{CommitLSN: 712, EventOrdinal: 8, EffectOrdinal: 2}
	observation := []issue49SchemaCursorScope{
		{
			Scope:                "unaffected",
			HistoricalPosition:   position,
			HistoricalToken:      "old-unaffected-token",
			HistoricalSchema:     "schema-4",
			CurrentSchema:        "schema-5",
			CompatibleLineage:    true,
			OtherBindingsCurrent: true,
			ReplacementToken:     "server-new-unaffected-token",
			ReplacementPosition:  position,
			ReplacementSchema:    "schema-5",
			ClientInstalled:      true,
			InstalledAtomically:  true,
		},
		{
			Scope:                "affected",
			HistoricalPosition:   position,
			HistoricalToken:      "old-affected-token",
			HistoricalSchema:     "schema-4",
			CurrentSchema:        "schema-5",
			CompatibleLineage:    true,
			Class3Affected:       true,
			OtherBindingsCurrent: true,
			ReturnedNull:         true,
		},
		{
			Scope:                "stale",
			HistoricalPosition:   position,
			HistoricalToken:      "old-stale-token",
			HistoricalSchema:     "schema-4",
			CurrentSchema:        "schema-5",
			CompatibleLineage:    true,
			OtherBindingsCurrent: false,
			ReturnedNull:         true,
		},
	}
	mutant := append([]issue49SchemaCursorScope(nil), observation...)
	mutant[0].ReplacementToken = mutant[0].HistoricalToken
	issue49Proof(t, "SYNC-SCHEMA-007", issue49SchemaCursorValid(observation), issue49SchemaCursorValid(mutant))
}

func TestIssue49DurableScopeRetentionFloor(t *testing.T) {
	observation := issue49RetentionFloor{
		Scope:            "project:17",
		Floor:            50,
		FloorDurable:     true,
		Lineage:          "retention-lineage-4",
		LineageDurable:   true,
		EffectsRemaining: 0,
		HighWatermark:    80,
		Cursors: []issue49RetentionCursor{
			{Name: "below", Position: 49, OtherBindings: true, Rebuild: true},
			{Name: "at", Position: 50, OtherBindings: true, Eligible: true},
			{Name: "above", Position: 80, OtherBindings: true, Eligible: true},
		},
	}
	mutant := observation
	mutant.Cursors = append([]issue49RetentionCursor(nil), observation.Cursors...)
	mutant.Cursors[1].Eligible = false
	mutant.Cursors[1].Rebuild = true
	issue49Proof(t, "SYNC-RETENTION-001", issue49RetentionFloorValid(observation), issue49RetentionFloorValid(mutant))
}

func TestIssue49PreWireMutationDependencyResolution(t *testing.T) {
	observation := issue49QueueResolution{
		Actions: []issue49QueueAction{
			{ID: "insert", Operation: "insert", State: "cancelled_before_send", LinkedTo: "delete", Inspectable: true},
			{ID: "update", Operation: "update", State: "superseded_before_send", LinkedTo: "insert", Inspectable: true},
			{ID: "delete", Operation: "delete", State: "cancelled_before_send", LinkedTo: "insert", Inspectable: true},
			{ID: "sealed", Operation: "update", State: "sealed", BaseVersion: "server-v1", Sent: true, Inspectable: true},
			{ID: "blocked", Operation: "delete", State: "blocked_by_predecessor", LinkedTo: "sealed", Inspectable: true},
		},
		ServerRowsBefore: 0,
		ServerRowsAfter:  0,
	}
	mutant := observation
	mutant.Actions = append([]issue49QueueAction(nil), observation.Actions...)
	mutant.Actions[4].BaseVersion = "fabricated-version"
	mutant.Actions[4].Sent = true
	mutant.FabricatedBaseSent = true
	issue49Proof(t, "SYNC-QUEUE-004", issue49QueueResolutionValid(observation), issue49QueueResolutionValid(mutant))
}

func TestIssue49ScopeCleanupPreservesPendingIntent(t *testing.T) {
	paths := []string{"assignment", "seed", "rebuild"}
	intents := []string{"insert", "update", "blocked"}
	observation := issue49ScopeCleanup{}
	for _, path := range paths {
		for _, intent := range intents {
			observation.ProtectedRows = append(observation.ProtectedRows, issue49CleanupRow{
				Path:               path,
				IntentKind:         intent,
				Unresolved:         true,
				ApplicationVisible: true,
			})
		}
		observation.UnprotectedRows = append(observation.UnprotectedRows, issue49CleanupRow{Path: path})
	}
	mutant := observation
	mutant.ProtectedRows = append([]issue49CleanupRow(nil), observation.ProtectedRows...)
	mutant.ProtectedRows[0].ApplicationVisible = false
	issue49Proof(t, "SYNC-SCOPE-006", issue49ScopeCleanupValid(observation), issue49ScopeCleanupValid(mutant))
}
