package reference

// ResolvedStep contains one completed step result from the current scenario run.
type ResolvedStep struct {
	StepID       string
	OperationKey string
	Result       StepResult
}

// ResolvedOperationInput contains data that the model runner resolves before dispatch.
type ResolvedOperationInput struct {
	SourceStep   *ResolvedStep
	PortableSeed *PortableSeedFixture
}

// PortableSeedFixture contains one closed, independently built seed artifact.
type PortableSeedFixture struct {
	FixtureID            string
	ArtifactDefinitionID string
	ArtifactBytes        []byte
	ArtifactSHA256       [32]byte
	ManifestBytes        []byte
	ManifestSHA256       [32]byte
	ExportID             ExportID
	Schema               SchemaRef
	RegistryGeneration   Generation
	StreamGeneration     StreamGeneration
	SnapshotBoundary     StreamPosition
	PortableScopeIDs     []ScopeID
	Scopes               []PortableSeedScopeFixture
	Rows                 []PortableSeedRowFixture
}

// PortableSeedScopeFixture contains one portable scope declaration.
type PortableSeedScopeFixture struct {
	Scope                ScopeID
	MembershipGeneration Generation
	RetentionGeneration  Generation
	Cardinality          Cardinality
	Checksum             Checksum
}

// PortableSeedRowFixture contains one ordered portable seed row.
type PortableSeedRowFixture struct {
	Scope   ScopeID
	Ordinal uint64
	Row     AuthoritativeRow
}

func cloneResolvedOperationInput(source ResolvedOperationInput) ResolvedOperationInput {
	result := ResolvedOperationInput{}
	if source.SourceStep != nil {
		step := *source.SourceStep
		step.Result = cloneStepResult(step.Result)
		result.SourceStep = &step
	}
	if source.PortableSeed != nil {
		fixture := *source.PortableSeed
		fixture.ArtifactBytes = cloneBytes(fixture.ArtifactBytes)
		fixture.ManifestBytes = cloneBytes(fixture.ManifestBytes)
		fixture.PortableScopeIDs = cloneScopeIDs(fixture.PortableScopeIDs)
		fixture.Scopes = append([]PortableSeedScopeFixture(nil), fixture.Scopes...)
		fixture.Rows = make([]PortableSeedRowFixture, len(source.PortableSeed.Rows))
		for index, row := range source.PortableSeed.Rows {
			fixture.Rows[index] = row
			fixture.Rows[index].Row = cloneAuthoritativeRow(row.Row)
		}
		result.PortableSeed = &fixture
	}
	return result
}

func cloneStepResult(source StepResult) StepResult {
	result := source
	if source.HTTP != nil {
		value := *source.HTTP
		value.Body = cloneBytes(value.Body)
		result.HTTP = &value
	}
	if source.Connect != nil {
		value := *source.Connect
		value.AddedScopes = cloneScopeIDs(value.AddedScopes)
		value.RemovedScopes = cloneScopeIDs(value.RemovedScopes)
		value.ScopeCursors = append([]ScopeCursorObservation(nil), value.ScopeCursors...)
		result.Connect = &value
	}
	if source.Local != nil {
		value := *source.Local
		result.Local = &value
	}
	if source.Lifecycle != nil {
		value := *source.Lifecycle
		result.Lifecycle = &value
	}
	if source.Push != nil {
		value := *source.Push
		value.Mutations = append([]MutationObservation(nil), value.Mutations...)
		result.Push = &value
	}
	if source.Pull != nil {
		value := *source.Pull
		value.Changes = append([]PullChangeObservation(nil), value.Changes...)
		value.ScopeCursors = append([]ScopeCursorObservation(nil), value.ScopeCursors...)
		value.AddedScopes = cloneScopeIDs(value.AddedScopes)
		value.RemovedScopes = cloneScopeIDs(value.RemovedScopes)
		value.RebuildScopes = cloneScopeIDs(value.RebuildScopes)
		value.ScopeChecksums = append([]ScopeChecksumObservation(nil), value.ScopeChecksums...)
		result.Pull = &value
	}
	if source.Rebuild != nil {
		value := *source.Rebuild
		value.Records = append([]RebuildRecordObservation(nil), value.Records...)
		result.Rebuild = &value
	}
	if source.WAL != nil {
		value := *source.WAL
		value.AffectedScopes = cloneScopeIDs(value.AffectedScopes)
		result.WAL = &value
	}
	if source.Schema != nil {
		value := *source.Schema
		value.AffectedScopes = cloneScopeIDs(value.AffectedScopes)
		result.Schema = &value
	}
	if source.Retention != nil {
		value := *source.Retention
		result.Retention = &value
	}
	if source.Client != nil {
		value := *source.Client
		result.Client = &value
	}
	return result
}
