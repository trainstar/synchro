package scenarios

import (
	"errors"
	"reflect"
	"sort"
)

// NormalizeStateFacts returns the canonical form used for authored projection comparison.
func NormalizeStateFacts(source StateFacts) (StateFacts, error) {
	facts := CloneStateFacts(source)
	if err := normalizeTransactions(facts.Transactions); err != nil {
		return StateFacts{}, err
	}
	if err := normalizeRows(facts.Rows); err != nil {
		return StateFacts{}, err
	}
	if err := normalizeScopes(facts.Scopes); err != nil {
		return StateFacts{}, err
	}
	if err := normalizePoison(facts.Poison); err != nil {
		return StateFacts{}, err
	}
	if err := normalizeRebuilds(facts.Rebuilds); err != nil {
		return StateFacts{}, err
	}
	if err := normalizeClients(facts.Clients); err != nil {
		return StateFacts{}, err
	}
	return facts, nil
}

func normalizeTransactions(values []TransactionFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		key := values[index].StreamGeneration + "\x00" + values[index].CommitLSN
		if !addUniqueStateFact(seen, key) {
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
		return stateTransactionKey(values[left]) < stateTransactionKey(values[right])
	})
	return nil
}

func normalizeRows(values []RowFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUniqueStateFact(seen, stateRowKey(value)) {
			return errors.New("row fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return stateRowKey(values[left]) < stateRowKey(values[right]) })
	return nil
}

func normalizeScopes(values []ScopeFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		if !addUniqueStateFact(seen, values[index].ScopeID) {
			return errors.New("scope fact is duplicated")
		}
		effectVersions := make(map[string]struct{}, len(values[index].EffectVersions))
		for _, version := range values[index].EffectVersions {
			if !addUniqueStateFact(effectVersions, version) {
				return errors.New("scope effect version is duplicated")
			}
		}
		sort.Strings(values[index].EffectVersions)
	}
	sort.Slice(values, func(left, right int) bool { return values[left].ScopeID < values[right].ScopeID })
	return nil
}

func normalizePoison(values []PoisonFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUniqueStateFact(seen, statePoisonKey(value)) {
			return errors.New("poison fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return statePoisonKey(values[left]) < statePoisonKey(values[right]) })
	return nil
}

func normalizeRebuilds(values []RebuildFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUniqueStateFact(seen, stateRebuildKey(value)) {
			return errors.New("rebuild fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return stateRebuildKey(values[left]) < stateRebuildKey(values[right]) })
	return nil
}

func normalizeClients(values []ClientDurabilityFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		client := &values[index]
		if !addUniqueStateFact(seen, stateClientKey(*client)) {
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
	sort.Slice(values, func(left, right int) bool { return stateClientKey(values[left]) < stateClientKey(values[right]) })
	return nil
}

func normalizeProvenance(values []ProvenanceFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		if !addUniqueStateFact(seen, stateProvenanceKey(values[index])) {
			return errors.New("provenance fact is duplicated")
		}
		scopes := make(map[string]struct{}, len(values[index].Scopes))
		for _, scope := range values[index].Scopes {
			if !addUniqueStateFact(scopes, scope) {
				return errors.New("provenance scope is duplicated")
			}
		}
		sort.Strings(values[index].Scopes)
	}
	sort.Slice(values, func(left, right int) bool {
		return stateProvenanceKey(values[left]) < stateProvenanceKey(values[right])
	})
	return nil
}

func normalizeCheckpoints(values []CheckpointFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUniqueStateFact(seen, value.ScopeID) {
			return errors.New("checkpoint fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return values[left].ScopeID < values[right].ScopeID })
	return nil
}

func normalizeQueue(values []QueuedMutationFact) error {
	seen := make(map[string]struct{}, len(values))
	for index := range values {
		if !addUniqueStateFact(seen, values[index].MutationID) {
			return errors.New("queued mutation fact is duplicated")
		}
		columns := make(map[string]struct{}, len(values[index].AuthoredColumns))
		for _, column := range values[index].AuthoredColumns {
			if !addUniqueStateFact(columns, column.FieldID) {
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

func normalizeOutcomes(values []MutationOutcomeFact) error {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !addUniqueStateFact(seen, value.MutationID) {
			return errors.New("mutation outcome fact is duplicated")
		}
	}
	sort.Slice(values, func(left, right int) bool { return values[left].MutationID < values[right].MutationID })
	return nil
}

// StateFactsProjectionEqual compares only the fact families selected by want.
func StateFactsProjectionEqual(want, got StateFacts) bool {
	if !optionalStateUintEqual(want.TransactionCount, got.TransactionCount) ||
		!optionalStateUintEqual(want.RowCount, got.RowCount) ||
		!optionalStateUintEqual(want.ScopeCount, got.ScopeCount) ||
		!optionalStateUintEqual(want.RebuildCount, got.RebuildCount) ||
		!optionalStateUintEqual(want.BatchCount, got.BatchCount) ||
		!optionalStateUintEqual(want.MutationCount, got.MutationCount) {
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
	if !projectedStateListEqual(want.Transactions, got.Transactions) ||
		!projectedStateListEqual(want.Rows, got.Rows) ||
		!projectedStateListEqual(want.Scopes, got.Scopes) ||
		!projectedStateListEqual(want.Poison, got.Poison) ||
		!projectedStateListEqual(want.Rebuilds, got.Rebuilds) {
		return false
	}
	if want.Clients == nil {
		return true
	}
	if len(want.Clients) != len(got.Clients) {
		return false
	}
	for index := range want.Clients {
		if !clientStateProjectionEqual(want.Clients[index], got.Clients[index]) {
			return false
		}
	}
	return true
}

func clientStateProjectionEqual(want, got ClientDurabilityFact) bool {
	if want.UserID != got.UserID || want.ClientID != got.ClientID {
		return false
	}
	if want.CurrentSchema != nil && (got.CurrentSchema == nil || *want.CurrentSchema != *got.CurrentSchema) {
		return false
	}
	if !optionalStateUintEqual(want.RowCount, got.RowCount) ||
		!optionalStateUintEqual(want.ProvenanceCount, got.ProvenanceCount) ||
		!optionalStateUintEqual(want.CheckpointCount, got.CheckpointCount) ||
		!optionalStateUintEqual(want.QueueCount, got.QueueCount) ||
		!optionalStateUintEqual(want.OutcomeCount, got.OutcomeCount) ||
		!optionalStateUintEqual(want.SealedBatchCount, got.SealedBatchCount) ||
		!optionalStateUintEqual(want.RebuildAttemptCount, got.RebuildAttemptCount) {
		return false
	}
	return projectedStateListEqual(want.Provenance, got.Provenance) &&
		checkpointStateProjectionEqual(want.Checkpoints, got.Checkpoints) &&
		projectedStateListEqual(want.Queue, got.Queue) &&
		projectedStateListEqual(want.Outcomes, got.Outcomes)
}

func checkpointStateProjectionEqual(want, got []CheckpointFact) bool {
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

func optionalStateUintEqual(want, got *uint64) bool {
	return want == nil || got != nil && *want == *got
}

func projectedStateListEqual[T any](want, got []T) bool {
	if want == nil {
		return true
	}
	if len(want) == 0 {
		return len(got) == 0
	}
	return reflect.DeepEqual(want, got)
}

// CloneStateFacts preserves omitted and explicit-empty projections.
func CloneStateFacts(source StateFacts) StateFacts {
	result := source
	result.TransactionCount = cloneStateUint64(source.TransactionCount)
	result.RowCount = cloneStateUint64(source.RowCount)
	result.ScopeCount = cloneStateUint64(source.ScopeCount)
	result.RebuildCount = cloneStateUint64(source.RebuildCount)
	result.BatchCount = cloneStateUint64(source.BatchCount)
	result.MutationCount = cloneStateUint64(source.MutationCount)
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
	result.Transactions = cloneStateSlice(source.Transactions)
	for index := range result.Transactions {
		result.Transactions[index].EventOrdinals = append([]uint64(nil), source.Transactions[index].EventOrdinals...)
	}
	result.Rows = cloneStateSlice(source.Rows)
	result.Scopes = cloneStateSlice(source.Scopes)
	for index := range result.Scopes {
		result.Scopes[index].EffectVersions = append([]string(nil), source.Scopes[index].EffectVersions...)
	}
	result.Poison = cloneStateSlice(source.Poison)
	for index := range result.Poison {
		result.Poison[index].Relation = cloneStateString(source.Poison[index].Relation)
	}
	result.Rebuilds = cloneStateSlice(source.Rebuilds)
	result.Clients = cloneStateSlice(source.Clients)
	for index := range result.Clients {
		cloneClientStateFact(&result.Clients[index], source.Clients[index])
	}
	return result
}

func cloneClientStateFact(target *ClientDurabilityFact, source ClientDurabilityFact) {
	if source.CurrentSchema != nil {
		value := *source.CurrentSchema
		target.CurrentSchema = &value
	}
	target.RowCount = cloneStateUint64(source.RowCount)
	target.ProvenanceCount = cloneStateUint64(source.ProvenanceCount)
	target.CheckpointCount = cloneStateUint64(source.CheckpointCount)
	target.QueueCount = cloneStateUint64(source.QueueCount)
	target.OutcomeCount = cloneStateUint64(source.OutcomeCount)
	target.SealedBatchCount = cloneStateUint64(source.SealedBatchCount)
	target.RebuildAttemptCount = cloneStateUint64(source.RebuildAttemptCount)
	target.Provenance = cloneStateSlice(source.Provenance)
	for index := range target.Provenance {
		target.Provenance[index].Scopes = append([]string(nil), source.Provenance[index].Scopes...)
	}
	target.Checkpoints = cloneStateSlice(source.Checkpoints)
	for index := range target.Checkpoints {
		target.Checkpoints[index].Checksum = cloneStateString(source.Checkpoints[index].Checksum)
	}
	target.Queue = cloneStateSlice(source.Queue)
	for index := range target.Queue {
		target.Queue[index].BaseVersion = cloneStateString(source.Queue[index].BaseVersion)
		target.Queue[index].AuthoredColumns = append([]FieldFact(nil), source.Queue[index].AuthoredColumns...)
	}
	target.Outcomes = cloneStateSlice(source.Outcomes)
}

func cloneStateUint64(source *uint64) *uint64 {
	if source == nil {
		return nil
	}
	value := *source
	return &value
}

func cloneStateString(source *string) *string {
	if source == nil {
		return nil
	}
	value := *source
	return &value
}

func cloneStateSlice[T any](source []T) []T {
	if source == nil {
		return nil
	}
	result := make([]T, len(source))
	copy(result, source)
	return result
}

func addUniqueStateFact(values map[string]struct{}, key string) bool {
	if _, duplicate := values[key]; duplicate {
		return false
	}
	values[key] = struct{}{}
	return true
}

func stateTransactionKey(value TransactionFact) string {
	return value.StreamGeneration + "\x00" + value.CommitLSN + "\x00" + value.EndLSN
}

func stateRowKey(value RowFact) string {
	return value.TableID + "\x00" + value.CanonicalWireJSON
}

func statePoisonKey(value PoisonFact) string {
	relation := "\x00"
	if value.Relation != nil {
		relation = "\x01" + *value.Relation
	}
	return value.StreamGeneration + "\x00" + value.CommitLSN + "\x00" + relation
}

func stateRebuildKey(value RebuildFact) string {
	return value.UserID + "\x00" + value.ClientID + "\x00" + value.ScopeID + "\x00" + value.RebuildID
}

func stateClientKey(value ClientDurabilityFact) string {
	return value.UserID + "\x00" + value.ClientID
}

func stateProvenanceKey(value ProvenanceFact) string {
	return value.TableID + "\x00" + value.CanonicalWireJSON
}
