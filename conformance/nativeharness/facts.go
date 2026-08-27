package nativeharness

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/nativeexecution"
	"github.com/trainstar/synchro/conformance/scenarios"
)

// MergeStateFacts merges raw partial facts from independent source classes.
// Equal duplicate facts are idempotent. Conflicting facts fail closed.
func MergeStateFacts(parts []scenarios.StateFacts) (scenarios.StateFacts, error) {
	var result scenarios.StateFacts
	for _, part := range parts {
		normalized, err := scenarios.NormalizeStateFacts(part)
		if err != nil {
			return scenarios.StateFacts{}, fmt.Errorf("normalize raw state facts: %w", err)
		}
		if err := mergeStateFacts(&result, normalized); err != nil {
			return scenarios.StateFacts{}, err
		}
	}
	return scenarios.NormalizeStateFacts(result)
}

func mergeStateFacts(destination *scenarios.StateFacts, source scenarios.StateFacts) error {
	if err := mergeUint64(&destination.TransactionCount, source.TransactionCount, "transaction count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.RowCount, source.RowCount, "row count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.ScopeCount, source.ScopeCount, "scope count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.RebuildCount, source.RebuildCount, "rebuild count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.BatchCount, source.BatchCount, "batch count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.MutationCount, source.MutationCount, "mutation count"); err != nil {
		return err
	}
	if err := mergeValue(&destination.ConfiguredLimits, source.ConfiguredLimits, "configured limits"); err != nil {
		return err
	}
	if err := mergeValue(&destination.Registry, source.Registry, "registry"); err != nil {
		return err
	}
	if err := mergeValue(&destination.Stream, source.Stream, "stream"); err != nil {
		return err
	}

	var err error
	destination.Transactions, err = mergeTransactions(destination.Transactions, source.Transactions)
	if err != nil {
		return err
	}
	destination.Rows, err = mergeRows(destination.Rows, source.Rows)
	if err != nil {
		return err
	}
	destination.Scopes, err = mergeScopes(destination.Scopes, source.Scopes)
	if err != nil {
		return err
	}
	destination.Poison, err = mergePoison(destination.Poison, source.Poison)
	if err != nil {
		return err
	}
	destination.Rebuilds, err = mergeRebuilds(destination.Rebuilds, source.Rebuilds)
	if err != nil {
		return err
	}
	destination.Clients, err = mergeClients(destination.Clients, source.Clients)
	return err
}

func mergeUint64(destination **uint64, source *uint64, name string) error {
	if source == nil {
		return nil
	}
	if *destination == nil {
		value := *source
		*destination = &value
		return nil
	}
	if **destination != *source {
		return fmt.Errorf("conflicting raw %s facts", name)
	}
	return nil
}

func mergeValue[T any](destination **T, source *T, name string) error {
	if source == nil {
		return nil
	}
	if *destination == nil {
		value := *source
		*destination = &value
		return nil
	}
	if !reflect.DeepEqual(**destination, *source) {
		return fmt.Errorf("conflicting raw %s facts", name)
	}
	return nil
}

func mergeTransactions(destination, source []scenarios.TransactionFact) ([]scenarios.TransactionFact, error) {
	result := append([]scenarios.TransactionFact(nil), destination...)
	for _, value := range source {
		key := value.StreamGeneration + "\x00" + value.CommitLSN + "\x00" + value.EndLSN
		index := indexBy(result, func(candidate scenarios.TransactionFact) bool {
			return candidate.StreamGeneration+"\x00"+candidate.CommitLSN+"\x00"+candidate.EndLSN == key
		})
		if index < 0 {
			result = append(result, cloneTransaction(value))
			continue
		}
		if !reflect.DeepEqual(result[index], value) {
			return nil, errors.New("conflicting raw transaction facts")
		}
	}
	return result, nil
}

func mergeRows(destination, source []scenarios.RowFact) ([]scenarios.RowFact, error) {
	result := append([]scenarios.RowFact(nil), destination...)
	for _, value := range source {
		key := value.TableID + "\x00" + value.CanonicalWireJSON
		index := indexBy(result, func(candidate scenarios.RowFact) bool {
			return candidate.TableID+"\x00"+candidate.CanonicalWireJSON == key
		})
		if index < 0 {
			result = append(result, value)
			continue
		}
		if !reflect.DeepEqual(result[index], value) {
			return nil, errors.New("conflicting raw row facts")
		}
	}
	return result, nil
}

func mergeScopes(destination, source []scenarios.ScopeFact) ([]scenarios.ScopeFact, error) {
	result := append([]scenarios.ScopeFact(nil), destination...)
	for _, value := range source {
		index := indexBy(result, func(candidate scenarios.ScopeFact) bool { return candidate.ScopeID == value.ScopeID })
		if index < 0 {
			result = append(result, cloneScope(value))
			continue
		}
		if !reflect.DeepEqual(result[index], value) {
			return nil, errors.New("conflicting raw scope facts")
		}
	}
	return result, nil
}

func mergePoison(destination, source []scenarios.PoisonFact) ([]scenarios.PoisonFact, error) {
	result := append([]scenarios.PoisonFact(nil), destination...)
	for _, value := range source {
		key := poisonFactKey(value)
		index := indexBy(result, func(candidate scenarios.PoisonFact) bool { return poisonFactKey(candidate) == key })
		if index < 0 {
			result = append(result, clonePoison(value))
			continue
		}
		if !reflect.DeepEqual(result[index], value) {
			return nil, errors.New("conflicting raw poison facts")
		}
	}
	return result, nil
}

func mergeRebuilds(destination, source []scenarios.RebuildFact) ([]scenarios.RebuildFact, error) {
	result := append([]scenarios.RebuildFact(nil), destination...)
	for _, value := range source {
		key := value.UserID + "\x00" + value.ClientID + "\x00" + value.ScopeID + "\x00" + value.RebuildID
		index := indexBy(result, func(candidate scenarios.RebuildFact) bool {
			return candidate.UserID+"\x00"+candidate.ClientID+"\x00"+candidate.ScopeID+"\x00"+candidate.RebuildID == key
		})
		if index < 0 {
			result = append(result, value)
			continue
		}
		if !reflect.DeepEqual(result[index], value) {
			return nil, errors.New("conflicting raw rebuild facts")
		}
	}
	return result, nil
}

func mergeClients(destination, source []scenarios.ClientDurabilityFact) ([]scenarios.ClientDurabilityFact, error) {
	result := append([]scenarios.ClientDurabilityFact(nil), destination...)
	for _, value := range source {
		key := value.UserID + "\x00" + value.ClientID
		index := indexBy(result, func(candidate scenarios.ClientDurabilityFact) bool {
			return candidate.UserID+"\x00"+candidate.ClientID == key
		})
		if index < 0 {
			result = append(result, cloneClient(value))
			continue
		}
		if err := mergeClient(&result[index], value); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func mergeClient(destination *scenarios.ClientDurabilityFact, source scenarios.ClientDurabilityFact) error {
	if err := mergeValue(&destination.CurrentSchema, source.CurrentSchema, "client schema"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.RowCount, source.RowCount, "client row count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.ProvenanceCount, source.ProvenanceCount, "client provenance count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.CheckpointCount, source.CheckpointCount, "client checkpoint count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.QueueCount, source.QueueCount, "client queue count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.OutcomeCount, source.OutcomeCount, "client outcome count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.SealedBatchCount, source.SealedBatchCount, "client sealed batch count"); err != nil {
		return err
	}
	if err := mergeUint64(&destination.RebuildAttemptCount, source.RebuildAttemptCount, "client rebuild count"); err != nil {
		return err
	}
	var err error
	destination.Provenance, err = mergeProvenance(destination.Provenance, source.Provenance)
	if err != nil {
		return err
	}
	destination.Checkpoints, err = mergeCheckpoints(destination.Checkpoints, source.Checkpoints)
	if err != nil {
		return err
	}
	destination.Queue, err = mergeQueue(destination.Queue, source.Queue)
	if err != nil {
		return err
	}
	destination.Outcomes, err = mergeOutcomes(destination.Outcomes, source.Outcomes)
	return err
}

func mergeProvenance(destination, source []scenarios.ProvenanceFact) ([]scenarios.ProvenanceFact, error) {
	result := append([]scenarios.ProvenanceFact(nil), destination...)
	for _, value := range source {
		key := value.TableID + "\x00" + value.CanonicalWireJSON
		index := indexBy(result, func(candidate scenarios.ProvenanceFact) bool {
			return candidate.TableID+"\x00"+candidate.CanonicalWireJSON == key
		})
		if index < 0 {
			result = append(result, cloneProvenance(value))
			continue
		}
		if !reflect.DeepEqual(result[index], value) {
			return nil, errors.New("conflicting raw provenance facts")
		}
	}
	return result, nil
}

func mergeCheckpoints(destination, source []scenarios.CheckpointFact) ([]scenarios.CheckpointFact, error) {
	result := append([]scenarios.CheckpointFact(nil), destination...)
	for _, value := range source {
		index := indexBy(result, func(candidate scenarios.CheckpointFact) bool { return candidate.ScopeID == value.ScopeID })
		if index < 0 {
			result = append(result, cloneCheckpoint(value))
			continue
		}
		if !reflect.DeepEqual(result[index], value) {
			return nil, errors.New("conflicting raw checkpoint facts")
		}
	}
	return result, nil
}

func mergeQueue(destination, source []scenarios.QueuedMutationFact) ([]scenarios.QueuedMutationFact, error) {
	result := append([]scenarios.QueuedMutationFact(nil), destination...)
	for _, value := range source {
		index := indexBy(result, func(candidate scenarios.QueuedMutationFact) bool { return candidate.MutationID == value.MutationID })
		if index < 0 {
			result = append(result, cloneQueue(value))
			continue
		}
		if !reflect.DeepEqual(result[index], value) {
			return nil, errors.New("conflicting raw queue facts")
		}
	}
	return result, nil
}

func mergeOutcomes(destination, source []scenarios.MutationOutcomeFact) ([]scenarios.MutationOutcomeFact, error) {
	result := append([]scenarios.MutationOutcomeFact(nil), destination...)
	for _, value := range source {
		index := indexBy(result, func(candidate scenarios.MutationOutcomeFact) bool { return candidate.MutationID == value.MutationID })
		if index < 0 {
			result = append(result, value)
			continue
		}
		if !reflect.DeepEqual(result[index], value) {
			return nil, errors.New("conflicting raw mutation outcome facts")
		}
	}
	return result, nil
}

func indexBy[T any](values []T, predicate func(T) bool) int {
	for index, value := range values {
		if predicate(value) {
			return index
		}
	}
	return -1
}

func transactionFactKey(value scenarios.TransactionFact) string {
	return value.StreamGeneration + "\x00" + value.CommitLSN + "\x00" + value.EndLSN
}

func rowFactKey(value scenarios.RowFact) string {
	return value.TableID + "\x00" + value.CanonicalWireJSON
}

func poisonFactKey(value scenarios.PoisonFact) string {
	relation := ""
	if value.Relation != nil {
		relation = *value.Relation
	}
	return value.StreamGeneration + "\x00" + value.CommitLSN + "\x00" + relation
}

func rebuildFactKey(value scenarios.RebuildFact) string {
	return value.UserID + "\x00" + value.ClientID + "\x00" + value.ScopeID + "\x00" + value.RebuildID
}

func clientFactKey(value scenarios.ClientDurabilityFact) string {
	return value.UserID + "\x00" + value.ClientID
}

func provenanceFactKey(value scenarios.ProvenanceFact) string {
	return value.TableID + "\x00" + value.CanonicalWireJSON
}

func cloneTransaction(value scenarios.TransactionFact) scenarios.TransactionFact {
	value.EventOrdinals = append([]uint64(nil), value.EventOrdinals...)
	return value
}

func cloneScope(value scenarios.ScopeFact) scenarios.ScopeFact {
	value.EffectVersions = append([]string(nil), value.EffectVersions...)
	return value
}

func clonePoison(value scenarios.PoisonFact) scenarios.PoisonFact {
	if value.Relation != nil {
		copy := *value.Relation
		value.Relation = &copy
	}
	return value
}

func cloneProvenance(value scenarios.ProvenanceFact) scenarios.ProvenanceFact {
	value.Scopes = append([]string(nil), value.Scopes...)
	return value
}

func cloneCheckpoint(value scenarios.CheckpointFact) scenarios.CheckpointFact {
	if value.Checksum != nil {
		copy := *value.Checksum
		value.Checksum = &copy
	}
	return value
}

func cloneQueue(value scenarios.QueuedMutationFact) scenarios.QueuedMutationFact {
	if value.BaseVersion != nil {
		copy := *value.BaseVersion
		value.BaseVersion = &copy
	}
	value.AuthoredColumns = append([]scenarios.FieldFact(nil), value.AuthoredColumns...)
	return value
}

func cloneClient(value scenarios.ClientDurabilityFact) scenarios.ClientDurabilityFact {
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var result scenarios.ClientDurabilityFact
	if json.Unmarshal(data, &result) != nil {
		return value
	}
	return result
}

func exactSourceRequest(sources []string) error {
	if len(sources) == 0 {
		return errors.New("native capture sources are empty")
	}
	seen := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if _, known := CaptureSourceClassFor(source); !known {
			return fmt.Errorf("native capture source %q is unsupported", source)
		}
		if _, duplicate := seen[source]; duplicate {
			return fmt.Errorf("native capture source %q is duplicated", source)
		}
		seen[source] = struct{}{}
	}
	return nil
}

func exactSourceClosure(expected []string, actual []CaptureSourceObservation) error {
	if len(expected) != len(actual) {
		return errors.New("capture sources do not close exactly")
	}
	expectedSet := make(map[string]struct{}, len(expected))
	for _, source := range expected {
		if _, duplicate := expectedSet[source]; duplicate {
			return fmt.Errorf("capture requested duplicate source %q", source)
		}
		expectedSet[source] = struct{}{}
	}
	seen := make(map[string]struct{}, len(actual))
	for _, value := range actual {
		if _, known := CaptureSourceClassFor(value.Source); !known {
			return fmt.Errorf("capture returned unsupported source %q", value.Source)
		}
		if _, duplicate := seen[value.Source]; duplicate {
			return fmt.Errorf("capture returned duplicate source %q", value.Source)
		}
		seen[value.Source] = struct{}{}
	}
	for _, source := range expected {
		if _, found := seen[source]; !found {
			return fmt.Errorf("capture omitted source %q", source)
		}
	}
	return nil
}

func exactBudgetClosure(expected []contract.BudgetID, actual []nativeexecution.BudgetObservation) error {
	if len(expected) != len(actual) {
		return errors.New("budget observations do not close exactly")
	}
	expectedSet := make(map[contract.BudgetID]struct{}, len(expected))
	for _, id := range expected {
		if id == "" {
			return errors.New("budget request identity is incomplete")
		}
		if _, duplicate := expectedSet[id]; duplicate {
			return fmt.Errorf("budget request %q is duplicated", id)
		}
		expectedSet[id] = struct{}{}
	}
	seen := make(map[contract.BudgetID]struct{}, len(actual))
	for _, value := range actual {
		if value.BudgetID == "" {
			return errors.New("budget observation identity is incomplete")
		}
		if _, duplicate := seen[value.BudgetID]; duplicate {
			return fmt.Errorf("budget observation %q is duplicated", value.BudgetID)
		}
		seen[value.BudgetID] = struct{}{}
	}
	for _, id := range expected {
		if _, found := seen[id]; !found {
			return fmt.Errorf("budget observation %q is missing", id)
		}
	}
	return nil
}
