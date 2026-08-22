package nativeexecution

import (
	"errors"
	"reflect"
	"sort"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func preserveStateFactProjections(source, target []scenarios.ModelExpectation) {
	byID := make(map[scenarios.ExpectationID]*scenarios.StateFacts, len(source))
	for _, expectation := range source {
		if expectation.StateFacts == nil {
			continue
		}
		facts := cloneStateFacts(*expectation.StateFacts)
		byID[expectation.ID] = &facts
	}
	for index := range target {
		if facts, found := byID[target[index].ID]; found {
			copy := cloneStateFacts(*facts)
			target[index].StateFacts = &copy
		}
	}
}

func normalizeStateFacts(source scenarios.StateFacts) (scenarios.StateFacts, error) {
	facts := cloneStateFacts(source)
	if err := normalizeTransactions(facts.Transactions); err != nil {
		return scenarios.StateFacts{}, err
	}
	if err := normalizeRows(facts.Rows); err != nil {
		return scenarios.StateFacts{}, err
	}
	if err := normalizeScopes(facts.Scopes); err != nil {
		return scenarios.StateFacts{}, err
	}
	if err := normalizePoison(facts.Poison); err != nil {
		return scenarios.StateFacts{}, err
	}
	if err := normalizeRebuilds(facts.Rebuilds); err != nil {
		return scenarios.StateFacts{}, err
	}
	if err := normalizeClients(facts.Clients); err != nil {
		return scenarios.StateFacts{}, err
	}
	return facts, nil
}

// NormalizeStateFacts returns the canonical form used for central comparison.
func NormalizeStateFacts(source scenarios.StateFacts) (scenarios.StateFacts, error) {
	return normalizeStateFacts(source)
}

func normalizeTransactions(values []scenarios.TransactionFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		key := values[index].StreamGeneration + "\x00" + values[index].CommitLSN
		if !addUnique(seen, key) {
			return errors.New("transaction fact is duplicated")
		}
		ordinals := make(map[uint64]struct{}, len(values[index].EventOrdinals))
		for _, ordinal := range values[index].EventOrdinals {
			if _, duplicate := ordinals[ordinal]; duplicate {
				return errors.New("transaction event ordinal is duplicated")
			}
			ordinals[ordinal] = struct{}{}
		}
		sort.Slice(values[index].EventOrdinals, func(left, right int) bool {
			return values[index].EventOrdinals[left] < values[index].EventOrdinals[right]
		})
	}
	sort.Slice(values, func(left, right int) bool {
		return transactionKey(values[left]) < transactionKey(values[right])
	})
	return nil
}

func normalizeRows(values []scenarios.RowFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUnique(seen, rowKey(value)) {
			return errors.New("row fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return rowKey(values[left]) < rowKey(values[right]) })
	return nil
}

func normalizeScopes(values []scenarios.ScopeFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		if !addUnique(seen, values[index].ScopeID) {
			return errors.New("scope fact is duplicated")
		}
		effectVersions := make(map[string]struct{}, len(values[index].EffectVersions))
		for _, version := range values[index].EffectVersions {
			if !addUnique(effectVersions, version) {
				return errors.New("scope effect version is duplicated")
			}
		}
		sort.Strings(values[index].EffectVersions)
	}
	sort.Slice(values, func(left, right int) bool { return values[left].ScopeID < values[right].ScopeID })
	return nil
}

func normalizePoison(values []scenarios.PoisonFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUnique(seen, poisonKey(value)) {
			return errors.New("poison fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return poisonKey(values[left]) < poisonKey(values[right]) })
	return nil
}

func normalizeRebuilds(values []scenarios.RebuildFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUnique(seen, rebuildKey(value)) {
			return errors.New("rebuild fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return rebuildKey(values[left]) < rebuildKey(values[right]) })
	return nil
}

func normalizeClients(values []scenarios.ClientDurabilityFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		client := &values[index]
		if !addUnique(seen, clientKey(*client)) {
			return errors.New("client fact is duplicated")
		}
		if err := normalizeProvenance(client.Provenance); err != nil {
			return err
		}
		if err := normalizeCheckpoints(client.Checkpoints); err != nil {
			return err
		}
		if err := normalizeQueue(client.Queue); err != nil {
			return err
		}
		if err := normalizeOutcomes(client.Outcomes); err != nil {
			return err
		}
	}
	sort.Slice(values, func(left, right int) bool { return clientKey(values[left]) < clientKey(values[right]) })
	return nil
}

func normalizeProvenance(values []scenarios.ProvenanceFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		if !addUnique(seen, provenanceKey(values[index])) {
			return errors.New("provenance fact is duplicated")
		}
		scopes := make(map[string]struct{}, len(values[index].Scopes))
		for _, scope := range values[index].Scopes {
			if !addUnique(scopes, scope) {
				return errors.New("provenance scope is duplicated")
			}
		}
		sort.Strings(values[index].Scopes)
	}
	sort.Slice(values, func(left, right int) bool { return provenanceKey(values[left]) < provenanceKey(values[right]) })
	return nil
}

func normalizeCheckpoints(values []scenarios.CheckpointFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUnique(seen, value.ScopeID) {
			return errors.New("checkpoint fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return values[left].ScopeID < values[right].ScopeID })
	return nil
}

func normalizeQueue(values []scenarios.QueuedMutationFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		if !addUnique(seen, values[index].MutationID) {
			return errors.New("queued mutation fact is duplicated")
		}
		columns := make(map[string]struct{}, len(values[index].AuthoredColumns))
		for _, column := range values[index].AuthoredColumns {
			if !addUnique(columns, column.FieldID) {
				return errors.New("queued mutation field fact is duplicated")
			}
		}
		sort.Slice(values[index].AuthoredColumns, func(left, right int) bool {
			return values[index].AuthoredColumns[left].FieldID < values[index].AuthoredColumns[right].FieldID
		})
	}
	sort.Slice(values, func(left, right int) bool {
		if values[left].LocalOrder != values[right].LocalOrder {
			return values[left].LocalOrder < values[right].LocalOrder
		}
		return values[left].MutationID < values[right].MutationID
	})
	return nil
}

func normalizeOutcomes(values []scenarios.MutationOutcomeFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUnique(seen, value.MutationID) {
			return errors.New("mutation outcome fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return values[left].MutationID < values[right].MutationID })
	return nil
}

func stateFactsProjectionEqual(want, got scenarios.StateFacts) bool {
	if !optionalUintEqual(want.TransactionCount, got.TransactionCount) ||
		!optionalUintEqual(want.RowCount, got.RowCount) ||
		!optionalUintEqual(want.ScopeCount, got.ScopeCount) ||
		!optionalUintEqual(want.RebuildCount, got.RebuildCount) ||
		!optionalUintEqual(want.BatchCount, got.BatchCount) ||
		!optionalUintEqual(want.MutationCount, got.MutationCount) {
		return false
	}
	if want.ConfiguredLimits != nil && (got.ConfiguredLimits == nil || *want.ConfiguredLimits != *got.ConfiguredLimits) {
		return false
	}
	if want.Registry != nil && (got.Registry == nil || *want.Registry != *got.Registry) {
		return false
	}
	if want.Stream != nil && (got.Stream == nil || *want.Stream != *got.Stream) {
		return false
	}
	if !projectedListEqual(want.Transactions, got.Transactions) ||
		!projectedListEqual(want.Rows, got.Rows) ||
		!projectedListEqual(want.Scopes, got.Scopes) ||
		!projectedListEqual(want.Poison, got.Poison) ||
		!projectedListEqual(want.Rebuilds, got.Rebuilds) {
		return false
	}
	if want.Clients == nil {
		return true
	}
	if len(want.Clients) != len(got.Clients) {
		return false
	}
	for index := range want.Clients {
		if !clientProjectionEqual(want.Clients[index], got.Clients[index]) {
			return false
		}
	}
	return true
}

func clientProjectionEqual(want, got scenarios.ClientDurabilityFact) bool {
	if want.UserID != got.UserID || want.ClientID != got.ClientID {
		return false
	}
	if want.CurrentSchema != nil && (got.CurrentSchema == nil || *want.CurrentSchema != *got.CurrentSchema) {
		return false
	}
	if !optionalUintEqual(want.RowCount, got.RowCount) ||
		!optionalUintEqual(want.ProvenanceCount, got.ProvenanceCount) ||
		!optionalUintEqual(want.CheckpointCount, got.CheckpointCount) ||
		!optionalUintEqual(want.QueueCount, got.QueueCount) ||
		!optionalUintEqual(want.OutcomeCount, got.OutcomeCount) ||
		!optionalUintEqual(want.SealedBatchCount, got.SealedBatchCount) ||
		!optionalUintEqual(want.RebuildAttemptCount, got.RebuildAttemptCount) {
		return false
	}
	if !projectedListEqual(want.Provenance, got.Provenance) ||
		!checkpointProjectionEqual(want.Checkpoints, got.Checkpoints) ||
		!projectedListEqual(want.Queue, got.Queue) ||
		!projectedListEqual(want.Outcomes, got.Outcomes) {
		return false
	}
	return true
}

func checkpointProjectionEqual(want, got []scenarios.CheckpointFact) bool {
	if want == nil {
		return true
	}
	if len(want) != len(got) {
		return false
	}
	for index := range want {
		left := want[index]
		right := got[index]
		if left.ScopeID != right.ScopeID || left.HasCursor != right.HasCursor || left.HasChecksum != right.HasChecksum || left.Verified != right.Verified {
			return false
		}
		if left.Checksum != nil && (right.Checksum == nil || *left.Checksum != *right.Checksum) {
			return false
		}
	}
	return true
}

func optionalUintEqual(want, got *uint64) bool {
	return want == nil || got != nil && *want == *got
}

func projectedListEqual[T any](want, got []T) bool {
	if want == nil {
		return true
	}
	if len(want) == 0 {
		return len(got) == 0
	}
	return reflect.DeepEqual(want, got)
}

func cloneStateFacts(source scenarios.StateFacts) scenarios.StateFacts {
	result := source
	result.TransactionCount = cloneUint64(source.TransactionCount)
	result.RowCount = cloneUint64(source.RowCount)
	result.ScopeCount = cloneUint64(source.ScopeCount)
	result.RebuildCount = cloneUint64(source.RebuildCount)
	result.BatchCount = cloneUint64(source.BatchCount)
	result.MutationCount = cloneUint64(source.MutationCount)
	if source.ConfiguredLimits != nil {
		value := *source.ConfiguredLimits
		result.ConfiguredLimits = &value
	}
	if source.Registry != nil {
		value := *source.Registry
		result.Registry = &value
	}
	if source.Stream != nil {
		value := *source.Stream
		result.Stream = &value
	}
	result.Transactions = cloneSlice(source.Transactions)
	for index := range result.Transactions {
		result.Transactions[index].EventOrdinals = append([]uint64(nil), source.Transactions[index].EventOrdinals...)
	}
	result.Rows = cloneSlice(source.Rows)
	result.Scopes = cloneSlice(source.Scopes)
	for index := range result.Scopes {
		result.Scopes[index].EffectVersions = append([]string(nil), source.Scopes[index].EffectVersions...)
	}
	result.Poison = cloneSlice(source.Poison)
	for index := range result.Poison {
		result.Poison[index].Relation = cloneString(source.Poison[index].Relation)
	}
	result.Rebuilds = cloneSlice(source.Rebuilds)
	result.Clients = cloneSlice(source.Clients)
	for index := range result.Clients {
		cloneClientStateFact(&result.Clients[index], source.Clients[index])
	}
	return result
}

func cloneClientStateFact(target *scenarios.ClientDurabilityFact, source scenarios.ClientDurabilityFact) {
	if source.CurrentSchema != nil {
		value := *source.CurrentSchema
		target.CurrentSchema = &value
	}
	target.RowCount = cloneUint64(source.RowCount)
	target.ProvenanceCount = cloneUint64(source.ProvenanceCount)
	target.CheckpointCount = cloneUint64(source.CheckpointCount)
	target.QueueCount = cloneUint64(source.QueueCount)
	target.OutcomeCount = cloneUint64(source.OutcomeCount)
	target.SealedBatchCount = cloneUint64(source.SealedBatchCount)
	target.RebuildAttemptCount = cloneUint64(source.RebuildAttemptCount)
	target.Provenance = cloneSlice(source.Provenance)
	for index := range target.Provenance {
		target.Provenance[index].Scopes = append([]string(nil), source.Provenance[index].Scopes...)
	}
	target.Checkpoints = cloneSlice(source.Checkpoints)
	for index := range target.Checkpoints {
		target.Checkpoints[index].Checksum = cloneString(source.Checkpoints[index].Checksum)
	}
	target.Queue = cloneSlice(source.Queue)
	for index := range target.Queue {
		target.Queue[index].BaseVersion = cloneString(source.Queue[index].BaseVersion)
		target.Queue[index].AuthoredColumns = append([]scenarios.FieldFact(nil), source.Queue[index].AuthoredColumns...)
	}
	target.Outcomes = cloneSlice(source.Outcomes)
}

func cloneUint64(source *uint64) *uint64 {
	if source == nil {
		return nil
	}
	value := *source
	return &value
}

func cloneSlice[T any](source []T) []T {
	if source == nil {
		return nil
	}
	result := make([]T, len(source))
	copy(result, source)
	return result
}

func addUnique(values map[string]struct{}, key string) bool {
	if _, duplicate := values[key]; duplicate {
		return false
	}
	values[key] = struct{}{}
	return true
}

func transactionKey(value scenarios.TransactionFact) string {
	return value.StreamGeneration + "\x00" + value.CommitLSN + "\x00" + value.EndLSN
}

func rowKey(value scenarios.RowFact) string {
	return value.TableID + "\x00" + value.CanonicalWireJSON
}

func poisonKey(value scenarios.PoisonFact) string {
	relation := "\x00"
	if value.Relation != nil {
		relation = "\x01" + *value.Relation
	}
	return value.StreamGeneration + "\x00" + value.CommitLSN + "\x00" + relation
}

func rebuildKey(value scenarios.RebuildFact) string {
	return value.UserID + "\x00" + value.ClientID + "\x00" + value.ScopeID + "\x00" + value.RebuildID
}

func clientKey(value scenarios.ClientDurabilityFact) string {
	return value.UserID + "\x00" + value.ClientID
}

func provenanceKey(value scenarios.ProvenanceFact) string {
	return value.TableID + "\x00" + value.CanonicalWireJSON
}
