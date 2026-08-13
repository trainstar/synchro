package modelrunner

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

func stateFactsFailure(want scenarios.StateFacts, got reference.StateSnapshot) string {
	for name, check := range map[string]struct {
		want *uint64
		got  int
	}{
		"transaction": {want.TransactionCount, len(got.Stream.Transactions)},
		"row":         {want.RowCount, len(got.Rows)},
		"scope":       {want.ScopeCount, len(got.Scopes)},
		"rebuild":     {want.RebuildCount, len(got.Rebuilds)},
		"batch":       {want.BatchCount, len(got.Batches)},
		"mutation":    {want.MutationCount, len(got.Mutations)},
	} {
		if check.want != nil && *check.want != uint64(check.got) {
			return fmt.Sprintf("%s count is %d, want %d", name, check.got, *check.want)
		}
	}
	if want.Registry != nil && uint64(got.Registry.CurrentGeneration) != want.Registry.CurrentGeneration {
		return fmt.Sprintf("registry generation is %d, want %d", got.Registry.CurrentGeneration, want.Registry.CurrentGeneration)
	}
	if want.ConfiguredLimits != nil {
		limits := got.ConfiguredLimits
		if limits.MaxScopeFanout != want.ConfiguredLimits.MaxScopeFanout || limits.MaxImpactRows != want.ConfiguredLimits.MaxImpactRows || uint64(limits.PullMaximum) != want.ConfiguredLimits.PullMaximum || uint64(limits.RebuildMaximum) != want.ConfiguredLimits.RebuildMaximum || limits.CompactionBatchMaximum != want.ConfiguredLimits.CompactionBatchMaximum || limits.BackfillBatchMaximum != want.ConfiguredLimits.BackfillBatchMaximum {
			return "configured limits do not match the authored facts"
		}
	}
	if want.Stream != nil {
		boundary := got.Stream.Authority.GlobalMaterializationBoundary
		if string(boundary.StreamGeneration) != want.Stream.MaterializedStreamGeneration || string(boundary.Kind) != want.Stream.MaterializedKind || decimal(uint64(boundary.CommitLSN)) != want.Stream.MaterializedCommitLSN || decimal(uint64(got.Stream.Authority.AcknowledgedEndLSN)) != want.Stream.AcknowledgedEndLSN {
			return "stream progress does not match the authored facts"
		}
	}
	if reason := transactionFactsFailure(want.Transactions, got.Stream.Transactions); reason != "" {
		return reason
	}
	if reason := rowFactsFailure(want.Rows, got.Rows); reason != "" {
		return reason
	}
	if reason := scopeFactsFailure(want.Scopes, got.Scopes); reason != "" {
		return reason
	}
	if reason := poisonFactsFailure(want.Poison, got.Stream.Poison); reason != "" {
		return reason
	}
	if reason := rebuildFactsFailure(want.Rebuilds, got.Rebuilds); reason != "" {
		return reason
	}
	if reason := clientFactsFailure(want.Clients, got.ClientLocal); reason != "" {
		return reason
	}
	return ""
}

func transactionFactsFailure(want []scenarios.TransactionFact, got []reference.StreamTransaction) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("transaction facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		transaction := got[index]
		ordinals := make([]uint64, len(transaction.Events))
		for eventIndex, event := range transaction.Events {
			ordinals[eventIndex] = uint64(event.ReplayKey.EventOrdinal)
		}
		if string(transaction.ReplayKey.StreamGeneration) != fact.StreamGeneration || decimal(uint64(transaction.ReplayKey.CommitLSN)) != fact.CommitLSN || decimal(uint64(transaction.EndLSN)) != fact.EndLSN || uint64(transaction.RegistryGeneration) != fact.RegistryGeneration || string(transaction.Lifecycle) != fact.Lifecycle || !reflect.DeepEqual(ordinals, fact.EventOrdinals) {
			return fmt.Sprintf("transaction %d does not match the authored facts", index)
		}
	}
	return ""
}

func rowFactsFailure(want []scenarios.RowFact, got []reference.SnapshotEntry[reference.RowIdentity, reference.AuthoritativeRow]) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("row facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		row := got[index].Value
		if string(row.Identity.TableID) != fact.TableID || row.Identity.CanonicalWireJSON != fact.CanonicalWireJSON || string(row.Version) != fact.Version || hex.EncodeToString(row.Checksum[:]) != fact.Checksum {
			return fmt.Sprintf("row %d does not match the authored facts", index)
		}
	}
	return ""
}

func scopeFactsFailure(want []scenarios.ScopeFact, got []reference.SnapshotEntry[reference.ScopeID, reference.ScopeState]) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("scope facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		scope := got[index]
		versions := make([]string, len(scope.Value.Effects))
		for effectIndex, effect := range scope.Value.Effects {
			versions[effectIndex] = string(effect.Version)
		}
		if string(scope.Key) != fact.ScopeID || uint64(scope.Value.MembershipGeneration) != fact.MembershipGeneration || uint64(scope.Value.Cardinality) != fact.Cardinality || !reflect.DeepEqual(versions, fact.EffectVersions) {
			return fmt.Sprintf("scope %d does not match the authored facts", index)
		}
	}
	return ""
}

func poisonFactsFailure(want []scenarios.PoisonFact, got []reference.PoisonRecord) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("poison facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		record := got[index]
		relation := ""
		if fact.Relation != nil {
			relation = *fact.Relation
		}
		if string(record.Transaction.StreamGeneration) != fact.StreamGeneration || decimal(uint64(record.Transaction.CommitLSN)) != fact.CommitLSN || record.HasRelation != (fact.Relation != nil) || string(record.Relation) != relation || string(record.Reason) != fact.Reason || string(record.Lifecycle) != fact.Lifecycle {
			return fmt.Sprintf("poison record %d does not match the authored facts", index)
		}
	}
	return ""
}

func rebuildFactsFailure(want []scenarios.RebuildFact, got []reference.SnapshotEntry[reference.RebuildKey, reference.RebuildSession]) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("rebuild facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		entry := got[index]
		session := entry.Value
		if string(entry.Key.Client.UserID) != fact.UserID || string(entry.Key.Client.ClientID) != fact.ClientID || string(entry.Key.Scope) != fact.ScopeID || string(entry.Key.Rebuild) != fact.RebuildID || uint64(session.PageLimit) != fact.PageLimit || uint64(len(session.StagedRows)) != fact.StagedRowCount || uint64(len(session.Pages)) != fact.PageCount || session.NextRowOrdinal != fact.NextRowOrdinal || session.HasContinuation != fact.HasContinuation || session.HasFinalCursor != fact.HasFinalCursor || string(session.Status) != fact.Status {
			return fmt.Sprintf("rebuild %d does not match the authored facts", index)
		}
	}
	return ""
}

func clientFactsFailure(want []scenarios.ClientDurabilityFact, got []reference.SnapshotEntry[reference.ClientKey, reference.ClientLocalState]) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("client facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		entry := got[index]
		local := entry.Value
		if string(entry.Key.UserID) != fact.UserID || string(entry.Key.ClientID) != fact.ClientID {
			return fmt.Sprintf("client %d identity does not match the authored facts", index)
		}
		if fact.CurrentSchema != nil && (local.CurrentSchema.Version != fact.CurrentSchema.Version || hex.EncodeToString(local.CurrentSchema.Hash[:]) != fact.CurrentSchema.Hash) {
			return fmt.Sprintf("client %d schema does not match the authored facts", index)
		}
		for name, check := range map[string]struct {
			want *uint64
			got  int
		}{
			"row":             {fact.RowCount, len(local.Rows)},
			"provenance":      {fact.ProvenanceCount, len(local.Provenance)},
			"checkpoint":      {fact.CheckpointCount, len(local.ScopeCheckpoints)},
			"queue":           {fact.QueueCount, len(local.DurableQueue)},
			"outcome":         {fact.OutcomeCount, len(local.Outcomes)},
			"sealed batch":    {fact.SealedBatchCount, len(local.SealedBatches)},
			"rebuild attempt": {fact.RebuildAttemptCount, len(local.RebuildAttempts)},
		} {
			if check.want != nil && *check.want != uint64(check.got) {
				return fmt.Sprintf("client %d %s count is %d, want %d", index, name, check.got, *check.want)
			}
		}
		if reason := provenanceFactsFailure(fact.Provenance, local.Provenance); reason != "" {
			return fmt.Sprintf("client %d %s", index, reason)
		}
		if reason := checkpointFactsFailure(fact.Checkpoints, local.ScopeCheckpoints); reason != "" {
			return fmt.Sprintf("client %d %s", index, reason)
		}
		if reason := queueFactsFailure(fact.Queue, local.DurableQueue); reason != "" {
			return fmt.Sprintf("client %d %s", index, reason)
		}
		if reason := outcomeFactsFailure(fact.Outcomes, local.Outcomes); reason != "" {
			return fmt.Sprintf("client %d %s", index, reason)
		}
	}
	return ""
}

func provenanceFactsFailure(want []scenarios.ProvenanceFact, got []reference.LocalProvenance) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("provenance facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		scopes := make([]string, len(got[index].Scopes))
		for scopeIndex, scope := range got[index].Scopes {
			scopes[scopeIndex] = string(scope)
		}
		if string(got[index].Row.TableID) != fact.TableID || got[index].Row.CanonicalWireJSON != fact.CanonicalWireJSON || string(got[index].Version) != fact.Version || !reflect.DeepEqual(scopes, fact.Scopes) {
			return fmt.Sprintf("provenance %d does not match the authored facts", index)
		}
	}
	return ""
}

func checkpointFactsFailure(want []scenarios.CheckpointFact, got []reference.LocalScopeCheckpoint) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("checkpoint facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		checkpoint := got[index]
		if string(checkpoint.Scope) != fact.ScopeID || checkpoint.HasCursor != fact.HasCursor || checkpoint.HasChecksum != fact.HasChecksum || checkpoint.Verified != fact.Verified {
			return fmt.Sprintf("checkpoint %d does not match the authored facts", index)
		}
	}
	return ""
}

func queueFactsFailure(want []scenarios.QueuedMutationFact, got []reference.QueuedMutation) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("queue facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		mutation := got[index]
		baseVersion := ""
		if fact.BaseVersion != nil {
			baseVersion = *fact.BaseVersion
		}
		columns := make([]scenarios.FieldFact, len(mutation.AuthoredColumns))
		for columnIndex, column := range mutation.AuthoredColumns {
			columns[columnIndex] = scenarios.FieldFact{FieldID: string(column.Field), Type: string(column.Type), WireJSON: column.WireJSON}
		}
		if string(mutation.Mutation) != fact.MutationID || string(mutation.Table) != fact.TableID || mutation.Row.CanonicalWireJSON != fact.CanonicalWireJSON || mutation.AuthoredSchema.Version != fact.AuthoredSchema.Version || hex.EncodeToString(mutation.AuthoredSchema.Hash[:]) != fact.AuthoredSchema.Hash || string(mutation.Operation) != fact.Operation || mutation.HasBaseVersion != (fact.BaseVersion != nil) || string(mutation.BaseVersion) != baseVersion || string(mutation.ClientVersion) != fact.ClientVersion || mutation.LocalOrder != fact.LocalOrder || string(mutation.Status) != fact.Status || !reflect.DeepEqual(columns, fact.AuthoredColumns) {
			return fmt.Sprintf("queued mutation %d does not match the authored facts", index)
		}
	}
	return ""
}

func outcomeFactsFailure(want []scenarios.MutationOutcomeFact, got []reference.MutationOutcome) string {
	if want == nil {
		return ""
	}
	if len(want) != len(got) {
		return fmt.Sprintf("outcome facts contain %d entries, model contains %d", len(want), len(got))
	}
	for index, fact := range want {
		if string(got[index].Mutation) != fact.MutationID || string(got[index].State) != fact.State || string(got[index].Reason) != fact.Reason {
			return fmt.Sprintf("mutation outcome %d does not match the authored facts", index)
		}
	}
	return ""
}

func decimal(value uint64) string {
	return strconv.FormatUint(value, 10)
}
