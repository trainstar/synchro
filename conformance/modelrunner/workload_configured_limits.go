package modelrunner

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/trainstar/synchro/conformance/reference"
	"github.com/trainstar/synchro/conformance/scenarios"
)

const (
	configuredPushMutationMaximum = uint64(1000)
	configuredBackfillRowCount    = uint64(2)
	configuredNeutralLimit        = uint64(2)
)

type configuredLimitsMaximums struct {
	fanout     uint64
	impact     uint64
	pull       uint64
	rebuild    uint64
	compaction uint64
	backfill   uint64
}

type configuredLimitsContext struct {
	relation             cardinalityRelationContext
	clientKey            reference.ClientKey
	client               reference.ClientState
	schema               reference.SchemaRef
	table                reference.TableManifest
	valueField           reference.FieldManifest
	stream               reference.StreamGeneration
	sampleScope          reference.ScopeID
	affectedScopes       []string
	markerCommitLSN      uint64
	rowCommitLSN         uint64
	rows                 []reference.RowIdentity
	rowEvents            []any
	registryGeneration   uint64
	membershipGeneration uint64
}

// expandConfiguredLimitsWorkload expands all configured-limit strata from one
// authored macro payload. It does not change the supplied snapshot.
func expandConfiguredLimitsWorkload(snapshot reference.StateSnapshot, payload map[string]json.RawMessage) (workloadExpansionPlan, error) {
	maximums, err := configuredMaximums(snapshot, payload)
	if err != nil {
		return workloadExpansionPlan{}, err
	}
	context, err := configuredContext(snapshot)
	if err != nil {
		return workloadExpansionPlan{}, err
	}
	if configuredNeutralLimit > maximums.fanout || configuredNeutralLimit > maximums.impact || configuredNeutralLimit > maximums.backfill {
		return workloadExpansionPlan{}, errors.New("configured limits do not permit the neutral administrative sample value")
	}

	plan := workloadExpansionPlan{Operations: make([]scenarios.Operation, 0, 1100), Samples: make([]workloadSamplePlan, 0, 63)}
	configuredAppendSupport(&plan, "model", "commit-source-transaction", map[string]any{
		"stream_generation": context.stream,
		"commit_lsn":        strconv.FormatUint(context.markerCommitLSN, 10),
		"end_lsn":           strconv.FormatUint(context.markerCommitLSN+1, 10),
		"events":            []any{},
	})
	configuredAppendSupport(&plan, "process", "materialize-source-transaction", map[string]any{
		"stream_generation": context.stream,
		"commit_lsn":        strconv.FormatUint(context.markerCommitLSN, 10),
	})

	supportRegistry := context.registryGeneration + 1
	supportMembership := context.membershipGeneration + 1
	configuredAppendSupportOperation(&plan, configuredStageOperation(
		context,
		supportRegistry,
		supportMembership,
		context.markerCommitLSN,
		configuredNeutralLimit,
		configuredNeutralLimit,
		configuredNeutralLimit,
	))
	configuredAppendSupport(&plan, "model", "activate-registry-membership-generation", map[string]any{
		"registry_generation": supportRegistry,
	})
	configuredAppendSupport(&plan, "model", "commit-source-transaction", map[string]any{
		"stream_generation": context.stream,
		"commit_lsn":        strconv.FormatUint(context.rowCommitLSN, 10),
		"end_lsn":           strconv.FormatUint(context.rowCommitLSN+1, 10),
		"events":            context.rowEvents,
	})
	configuredAppendSupport(&plan, "process", "materialize-source-transaction", map[string]any{
		"stream_generation": context.stream,
		"commit_lsn":        strconv.FormatUint(context.rowCommitLSN, 10),
	})

	validAdministrativeSamples := uint64(0)
	administrativeFamilies := []struct {
		family  WorkloadSampleFamily
		maximum uint64
	}{
		{family: WorkloadSampleFanout, maximum: maximums.fanout},
		{family: WorkloadSampleImpact, maximum: maximums.impact},
		{family: WorkloadSampleBackfill, maximum: maximums.backfill},
	}
	for _, family := range administrativeFamilies {
		boundaries, err := configuredBoundaryValues(family.maximum)
		if err != nil {
			return workloadExpansionPlan{}, err
		}
		for _, boundary := range boundaries {
			for repetition := 0; repetition < 3; repetition++ {
				registryGeneration := supportRegistry + validAdministrativeSamples + 1
				membershipGeneration := supportMembership + validAdministrativeSamples + 1
				fanout := configuredNeutralLimit
				impact := configuredNeutralLimit
				batchSize := configuredNeutralLimit
				switch family.family {
				case WorkloadSampleFanout:
					fanout = boundary.value
				case WorkloadSampleImpact:
					impact = boundary.value
				case WorkloadSampleBackfill:
					batchSize = boundary.value
				}
				operation := configuredStageOperation(context, registryGeneration, membershipGeneration, context.rowCommitLSN, fanout, impact, batchSize)
				expected := configuredSampleExpectation(family.family, boundary.boundary, boundary.value, configuredBackfillRowCount)
				configuredAppendSample(&plan, operation, family.family, boundary.boundary, boundary.value, expected)
				if boundary.boundary == WorkloadBoundaryInvalid {
					continue
				}
				configuredAppendSupport(&plan, "model", "activate-registry-membership-generation", map[string]any{
					"registry_generation": registryGeneration,
				})
				validAdministrativeSamples++
			}
		}
	}

	if err := configuredAppendPullSamples(&plan, context, maximums.pull); err != nil {
		return workloadExpansionPlan{}, err
	}
	if err := configuredAppendRebuildSamples(&plan, context, maximums.rebuild); err != nil {
		return workloadExpansionPlan{}, err
	}
	if err := configuredAppendCompactionSamples(&plan, context, maximums.compaction); err != nil {
		return workloadExpansionPlan{}, err
	}
	if err := configuredAppendPushSamples(&plan, context); err != nil {
		return workloadExpansionPlan{}, err
	}
	configuredAppendSupport(&plan, "process", "restart-client", map[string]any{
		"user_id":   string(context.clientKey.UserID),
		"client_id": string(context.clientKey.ClientID),
	})

	if err := validateConfiguredPlan(plan); err != nil {
		return workloadExpansionPlan{}, err
	}
	return plan, nil
}

func configuredMaximums(snapshot reference.StateSnapshot, payload map[string]json.RawMessage) (configuredLimitsMaximums, error) {
	if err := requireWorkloadProfile(payload, "configured_limits"); err != nil {
		return configuredLimitsMaximums{}, err
	}
	wanted := []struct {
		name  string
		value uint64
	}{
		{name: "max_scope_fanout", value: snapshot.ConfiguredLimits.MaxScopeFanout},
		{name: "max_impact_rows", value: snapshot.ConfiguredLimits.MaxImpactRows},
		{name: "pull_maximum", value: uint64(snapshot.ConfiguredLimits.PullMaximum)},
		{name: "rebuild_maximum", value: uint64(snapshot.ConfiguredLimits.RebuildMaximum)},
		{name: "compaction_batch_maximum", value: snapshot.ConfiguredLimits.CompactionBatchMaximum},
		{name: "backfill_batch_maximum", value: snapshot.ConfiguredLimits.BackfillBatchMaximum},
	}
	for _, limit := range wanted {
		value, err := requiredWorkloadUint64(payload, limit.name)
		if err != nil {
			return configuredLimitsMaximums{}, err
		}
		if value == 0 || value != limit.value {
			return configuredLimitsMaximums{}, fmt.Errorf("workload/prepare %s must equal the installed configured maximum", limit.name)
		}
	}
	return configuredLimitsMaximums{
		fanout:     wanted[0].value,
		impact:     wanted[1].value,
		pull:       wanted[2].value,
		rebuild:    wanted[3].value,
		compaction: wanted[4].value,
		backfill:   wanted[5].value,
	}, nil
}

func configuredContext(snapshot reference.StateSnapshot) (configuredLimitsContext, error) {
	if snapshot.ProtocolVersion != 3 || snapshot.Stream.Authority.ActiveGeneration == "" {
		return configuredLimitsContext{}, errors.New("configured limits require an installed protocol 3 stream")
	}
	if len(snapshot.Rows) != 0 || len(snapshot.Stream.Transactions) != 0 {
		return configuredLimitsContext{}, errors.New("configured limits require the authored empty workload state")
	}
	relation, err := cardinalityRelationInfo(snapshot)
	if err != nil {
		return configuredLimitsContext{}, fmt.Errorf("resolve configured-limit relation: %w", err)
	}
	clientKey, client, err := cardinalityAssignedClient(snapshot)
	if err != nil {
		return configuredLimitsContext{}, fmt.Errorf("resolve configured-limit client: %w", err)
	}
	if _, err := workloadClient(snapshot, string(clientKey.UserID), string(clientKey.ClientID)); err != nil {
		return configuredLimitsContext{}, err
	}
	schema, manifest, err := workloadCurrentSchema(snapshot)
	if err != nil {
		return configuredLimitsContext{}, err
	}
	table, err := workloadTable(manifest, string(relation.Definition.TableID))
	if err != nil {
		return configuredLimitsContext{}, err
	}
	valueField, err := workloadRejectionField(table)
	if err != nil {
		return configuredLimitsContext{}, err
	}

	sampleScope := reference.ScopeID("")
	for _, assignment := range client.ScopeAssignments {
		if assignment.Assigned {
			sampleScope = assignment.Scope
			break
		}
	}
	if sampleScope == "" {
		return configuredLimitsContext{}, errors.New("configured limits require one assigned scope")
	}
	affectedScopes := make([]string, 0, len(snapshot.Scopes))
	membershipGeneration := uint64(0)
	for _, entry := range snapshot.Scopes {
		affectedScopes = append(affectedScopes, string(entry.Key))
		if uint64(entry.Value.MembershipGeneration) > membershipGeneration {
			membershipGeneration = uint64(entry.Value.MembershipGeneration)
		}
	}
	sort.Strings(affectedScopes)
	if len(affectedScopes) == 0 || membershipGeneration == 0 {
		return configuredLimitsContext{}, errors.New("configured limits require authoritative scopes")
	}
	registryGeneration := uint64(snapshot.Registry.CurrentGeneration)
	if registryGeneration == 0 || registryGeneration > workloadMaximumSafeInteger-20 || membershipGeneration > workloadMaximumSafeInteger-20 {
		return configuredLimitsContext{}, errors.New("configured limits cannot allocate administrative generations")
	}
	markerCommitLSN, err := cardinalityNextCommitLSN(snapshot)
	if err != nil {
		return configuredLimitsContext{}, err
	}
	if markerCommitLSN > math.MaxUint64-20 {
		return configuredLimitsContext{}, errors.New("configured limits cannot allocate source transaction positions")
	}
	rowCommitLSN := markerCommitLSN + 10
	rows := make([]reference.RowIdentity, 0, configuredBackfillRowCount)
	events := make([]any, 0, configuredBackfillRowCount)
	for ordinal := uint64(1); ordinal <= configuredBackfillRowCount; ordinal++ {
		row, err := cardinalityNewRow(relation, ordinal, rowCommitLSN)
		if err != nil {
			return configuredLimitsContext{}, err
		}
		rows = append(rows, row.Identity)
		events = append(events, map[string]any{
			"event_ordinal": ordinal,
			"relation":      string(relation.Definition.Relation),
			"operation":     "insert",
			"before":        nil,
			"after":         cardinalityRegisteredImage(row),
		})
	}
	return configuredLimitsContext{
		relation:             relation,
		clientKey:            clientKey,
		client:               client,
		schema:               schema,
		table:                table,
		valueField:           valueField,
		stream:               snapshot.Stream.Authority.ActiveGeneration,
		sampleScope:          sampleScope,
		affectedScopes:       affectedScopes,
		markerCommitLSN:      markerCommitLSN,
		rowCommitLSN:         rowCommitLSN,
		rows:                 rows,
		rowEvents:            events,
		registryGeneration:   registryGeneration,
		membershipGeneration: membershipGeneration,
	}, nil
}

func configuredStageOperation(context configuredLimitsContext, registryGeneration, membershipGeneration, boundaryCommitLSN, fanout, impact, batchSize uint64) scenarios.Operation {
	evaluations := make([]any, 0, len(context.rows))
	for _, row := range context.rows {
		evaluations = append(evaluations, map[string]any{
			"row":    cardinalityRowPayload(row),
			"scopes": []string{string(context.sampleScope)},
		})
	}
	impactFunction := string(context.relation.Definition.DependencyImpactFunction)
	if impactFunction == "" {
		impactFunction = string(context.relation.Definition.MembershipFunction) + ".configured_limit_impact"
	}
	return workloadOperation("model", "stage-registry-membership-generation", map[string]any{
		"registry_generation":   registryGeneration,
		"membership_generation": membershipGeneration,
		"batch_size":            batchSize,
		"activation_boundary": map[string]any{
			"stream_generation": context.stream,
			"kind":              "transaction_end",
			"commit_lsn":        strconv.FormatUint(boundaryCommitLSN, 10),
		},
		"affected_scopes": context.affectedScopes,
		"scope_rules": []any{map[string]any{
			"scope_rule_id":         fmt.Sprintf("configured-limit-rule-%d", registryGeneration),
			"relation":              string(context.relation.Definition.Relation),
			"membership_function":   string(context.relation.Definition.MembershipFunction),
			"positive_fanout_bound": fanout,
			"evaluations":           evaluations,
		}},
		"dependency_impacts": []any{map[string]any{
			"dependency_impact_id": fmt.Sprintf("configured-limit-impact-%d", registryGeneration),
			"relation":             string(context.relation.Definition.Relation),
			"function":             impactFunction,
			"captured_field_ids":   []string{string(context.valueField.ID)},
			"positive_row_bound":   impact,
			"affected_rows":        []any{},
			"requires_rebuild":     false,
		}},
	})
}

type configuredBoundaryValue struct {
	boundary WorkloadSampleBoundary
	value    uint64
}

func configuredBoundaryValues(maximum uint64) ([]configuredBoundaryValue, error) {
	if maximum == 0 || maximum == math.MaxUint64 {
		return nil, errors.New("configured maximum cannot produce all closed boundaries")
	}
	return []configuredBoundaryValue{
		{boundary: WorkloadBoundaryLower, value: 1},
		{boundary: WorkloadBoundaryUpper, value: maximum},
		{boundary: WorkloadBoundaryInvalid, value: maximum + 1},
	}, nil
}

func configuredSampleExpectation(family WorkloadSampleFamily, boundary WorkloadSampleBoundary, value, rowCount uint64) workloadSampleExpectation {
	if boundary == WorkloadBoundaryInvalid {
		switch family {
		case WorkloadSampleFanout, WorkloadSampleImpact, WorkloadSampleBackfill, WorkloadSampleCompaction:
			return workloadSampleExpectation{ErrorCode: "invalid_limit", PreserveState: true}
		case WorkloadSamplePull:
			return workloadSampleExpectation{ResultKind: reference.StepResultKindPull, HTTPStatus: 400, HTTPCode: "invalid_request", PreserveState: true}
		case WorkloadSampleRebuild:
			return workloadSampleExpectation{ResultKind: reference.StepResultKindRebuild, HTTPStatus: 400, HTTPCode: "invalid_request", PreserveState: true}
		case WorkloadSamplePush:
			return workloadSampleExpectation{ResultKind: reference.StepResultKindPush, HTTPStatus: 400, HTTPCode: "invalid_request", PreserveState: true}
		}
	}
	switch family {
	case WorkloadSampleFanout, WorkloadSampleImpact:
		return workloadSampleExpectation{ResultKind: reference.StepResultKindSchema}
	case WorkloadSampleBackfill:
		return workloadSampleExpectation{ResultKind: reference.StepResultKindSchema, BatchCount: (rowCount + value - 1) / value, CheckBatchCount: true}
	case WorkloadSamplePull:
		return workloadSampleExpectation{ResultKind: reference.StepResultKindPull, HTTPStatus: 200}
	case WorkloadSampleRebuild:
		return workloadSampleExpectation{ResultKind: reference.StepResultKindRebuild, HTTPStatus: 200}
	case WorkloadSampleCompaction:
		return workloadSampleExpectation{ResultKind: reference.StepResultKindRetention}
	case WorkloadSamplePush:
		return workloadSampleExpectation{ResultKind: reference.StepResultKindPush, HTTPStatus: 200}
	default:
		return workloadSampleExpectation{}
	}
}

func configuredAppendPullSamples(plan *workloadExpansionPlan, context configuredLimitsContext, maximum uint64) error {
	boundaries, err := configuredBoundaryValues(maximum)
	if err != nil {
		return err
	}
	for _, boundary := range boundaries {
		for repetition := 0; repetition < 3; repetition++ {
			operation := workloadOperation("pull", "request-page", map[string]any{
				"user_id":           string(context.clientKey.UserID),
				"client_id":         string(context.clientKey.ClientID),
				"client_generation": uint64(context.client.CurrentGeneration),
				"schema":            workloadSchemaWire(context.schema),
				"scope_set_version": uint64(context.client.ScopeSetVersion),
				"scopes": []any{map[string]any{
					"scope_id":      string(context.sampleScope),
					"cursor_source": "none",
				}},
				"limit": boundary.value,
			})
			configuredAppendSample(plan, operation, WorkloadSamplePull, boundary.boundary, boundary.value, configuredSampleExpectation(WorkloadSamplePull, boundary.boundary, boundary.value, 0))
		}
	}
	return nil
}

func configuredAppendRebuildSamples(plan *workloadExpansionPlan, context configuredLimitsContext, maximum uint64) error {
	boundaries, err := configuredBoundaryValues(maximum)
	if err != nil {
		return err
	}
	ordinal := uint64(0)
	for _, boundary := range boundaries {
		for repetition := 0; repetition < 3; repetition++ {
			ordinal++
			operation := workloadOperation("rebuild", "request-page", map[string]any{
				"user_id":           string(context.clientKey.UserID),
				"client_id":         string(context.clientKey.ClientID),
				"client_generation": uint64(context.client.CurrentGeneration),
				"schema":            workloadSchemaWire(context.schema),
				"scope_id":          string(context.sampleScope),
				"rebuild_id":        workloadUUID("configured-limit-rebuild", string(context.clientKey.UserID), string(context.clientKey.ClientID), context.schema.Version, ordinal),
				"cursor_source":     "none",
				"limit":             boundary.value,
			})
			configuredAppendSample(plan, operation, WorkloadSampleRebuild, boundary.boundary, boundary.value, configuredSampleExpectation(WorkloadSampleRebuild, boundary.boundary, boundary.value, 0))
		}
	}
	return nil
}

func configuredAppendCompactionSamples(plan *workloadExpansionPlan, context configuredLimitsContext, maximum uint64) error {
	boundaries, err := configuredBoundaryValues(maximum)
	if err != nil {
		return err
	}
	for _, boundary := range boundaries {
		for repetition := 0; repetition < 3; repetition++ {
			operation := workloadOperation("model", "compact-scope", map[string]any{
				"scope_id":   string(context.sampleScope),
				"batch_size": boundary.value,
			})
			configuredAppendSample(plan, operation, WorkloadSampleCompaction, boundary.boundary, boundary.value, configuredSampleExpectation(WorkloadSampleCompaction, boundary.boundary, boundary.value, 0))
		}
	}
	return nil
}

func configuredAppendPushSamples(plan *workloadExpansionPlan, context configuredLimitsContext) error {
	wireMutations := make([]map[string]any, 0, configuredPushMutationMaximum)
	clientVersion := "2026-08-11T00:00:00.000000Z"
	for ordinal := uint64(1); ordinal <= configuredPushMutationMaximum; ordinal++ {
		mutationID := workloadUUID("configured-limit-mutation", string(context.clientKey.UserID), string(context.clientKey.ClientID), context.schema.Version, ordinal)
		primaryKey := fmt.Sprintf("configured-limit-%04d", ordinal)
		value := fmt.Sprintf("v%04d", ordinal)
		configuredAppendSupport(plan, "local", "write", map[string]any{
			"authenticated_user_id": string(context.clientKey.UserID),
			"client_id":             string(context.clientKey.ClientID),
			"mutation_id":           mutationID,
			"table_id":              string(context.table.ID),
			"pk":                    map[string]any{string(context.table.PrimaryKeyFieldID): primaryKey},
			"authored_schema":       workloadSchemaWire(context.schema),
			"operation":             "insert",
			"client_version":        clientVersion,
			"columns": []any{map[string]any{
				"field_id": string(context.valueField.ID),
				"value":    value,
			}},
		})
		wireMutations = append(wireMutations, map[string]any{
			"mutation_id":     mutationID,
			"table":           string(context.table.ID),
			"pk":              map[string]any{string(context.table.PrimaryKeyFieldID): primaryKey},
			"authored_schema": workloadSchemaWire(context.schema),
			"op":              "insert",
			"client_version":  clientVersion,
			"columns":         map[string]any{string(context.valueField.ID): value},
		})
	}
	extraMutation := map[string]any{
		"mutation_id":     workloadUUID("configured-limit-mutation", string(context.clientKey.UserID), string(context.clientKey.ClientID), context.schema.Version, configuredPushMutationMaximum+1),
		"table":           string(context.table.ID),
		"pk":              map[string]any{string(context.table.PrimaryKeyFieldID): "configured-limit-invalid"},
		"authored_schema": workloadSchemaWire(context.schema),
		"op":              "insert",
		"client_version":  clientVersion,
		"columns":         map[string]any{string(context.valueField.ID): "invalid"},
	}
	boundaries, err := configuredBoundaryValues(configuredPushMutationMaximum)
	if err != nil {
		return err
	}
	sampleOrdinal := uint64(0)
	for _, boundary := range boundaries {
		for repetition := 0; repetition < 3; repetition++ {
			sampleOrdinal++
			mutationCount := boundary.value
			mutations := make([]map[string]any, 0, mutationCount)
			if mutationCount <= configuredPushMutationMaximum {
				mutations = append(mutations, wireMutations[:mutationCount]...)
			} else {
				mutations = append(mutations, wireMutations...)
				mutations = append(mutations, extraMutation)
			}
			commitLSN := context.rowCommitLSN + 10 + sampleOrdinal*2
			operation := workloadOperation("push", "submit", map[string]any{
				"authenticated_user_id": string(context.clientKey.UserID),
				"request": map[string]any{
					"client_id":         string(context.clientKey.ClientID),
					"client_generation": uint64(context.client.CurrentGeneration),
					"batch_id":          workloadUUID("configured-limit-batch", string(context.clientKey.UserID), string(context.clientKey.ClientID), context.schema.Version, sampleOrdinal),
					"schema":            workloadSchemaWire(context.schema),
					"mutations":         mutations,
				},
				"delivery":   "apply",
				"commit_lsn": strconv.FormatUint(commitLSN, 10),
				"end_lsn":    strconv.FormatUint(commitLSN+1, 10),
			})
			configuredAppendSample(plan, operation, WorkloadSamplePush, boundary.boundary, boundary.value, configuredSampleExpectation(WorkloadSamplePush, boundary.boundary, boundary.value, 0))
		}
	}
	return nil
}

func configuredAppendSupport(plan *workloadExpansionPlan, contractOperation, name string, payload map[string]any) {
	configuredAppendSupportOperation(plan, workloadOperation(contractOperation, name, payload))
}

func configuredAppendSupportOperation(plan *workloadExpansionPlan, operation scenarios.Operation) {
	plan.Operations = append(plan.Operations, operation)
}

func configuredAppendSample(plan *workloadExpansionPlan, operation scenarios.Operation, family WorkloadSampleFamily, boundary WorkloadSampleBoundary, value uint64, expected workloadSampleExpectation) {
	index := len(plan.Operations)
	plan.Operations = append(plan.Operations, operation)
	plan.Samples = append(plan.Samples, workloadSamplePlan{
		Family:                 family,
		Boundary:               boundary,
		Value:                  value,
		ExpandedOperationIndex: index,
		Expected:               expected,
	})
}

func validateConfiguredPlan(plan workloadExpansionPlan) error {
	if len(plan.Samples) != 63 {
		return fmt.Errorf("configured-limit sample count is %d, want 63", len(plan.Samples))
	}
	for index, operation := range plan.Operations {
		key := scenarios.OperationKey(operation)
		if key == "workload/prepare" {
			return errors.New("configured-limit expansion retained workload/prepare")
		}
		class, found := scenarios.LookupOperationClass(key)
		if !found || class != scenarios.OperationClassReference {
			return fmt.Errorf("configured-limit operation %d is not a closed reference operation", index)
		}
		if err := scenarios.ValidateOperation(operation); err != nil {
			return fmt.Errorf("validate configured-limit operation %d %s: %w", index, key, err)
		}
	}
	seen := make(map[int]struct{}, len(plan.Samples))
	for _, sample := range plan.Samples {
		if sample.Family == "" || sample.Boundary == "" || sample.Value == 0 || sample.ExpandedOperationIndex < 0 || sample.ExpandedOperationIndex >= len(plan.Operations) {
			return errors.New("configured-limit sample has an incomplete target")
		}
		if _, duplicate := seen[sample.ExpandedOperationIndex]; duplicate {
			return errors.New("configured-limit samples share one expanded operation")
		}
		seen[sample.ExpandedOperationIndex] = struct{}{}
	}
	return nil
}
