package invariants

import (
	"strconv"
	"strings"
	"time"
)

const issue49MaxPortableInteger int64 = 9007199254740991

type issue49CRUDTransition struct {
	Table           string
	Operation       string
	Canonical       bool
	BeforeExists    bool
	AfterExists     bool
	AfterDeleted    bool
	BeforeVersion   string
	AfterVersion    string
	DurableOutcome  string
	SourceCommitted bool
}

func issue49CRUDValid(registeredTables []string, transitions []issue49CRUDTransition) bool {
	if len(registeredTables) == 0 {
		return false
	}
	want := make(map[string]struct{}, len(registeredTables)*3)
	for _, table := range registeredTables {
		if table == "" {
			return false
		}
		if _, duplicate := want[table+"\x00insert"]; duplicate {
			return false
		}
		for _, operation := range []string{"insert", "update", "delete"} {
			want[table+"\x00"+operation] = struct{}{}
		}
	}
	seen := make(map[string]struct{}, len(transitions))
	for _, transition := range transitions {
		key := transition.Table + "\x00" + transition.Operation
		if _, required := want[key]; !required {
			return false
		}
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
		if !transition.Canonical || !transition.SourceCommitted || transition.DurableOutcome != "accepted" || transition.AfterVersion == "" || transition.AfterVersion == transition.BeforeVersion {
			return false
		}
		switch transition.Operation {
		case "insert":
			if transition.BeforeExists || !transition.AfterExists || transition.AfterDeleted || transition.BeforeVersion != "" {
				return false
			}
		case "update":
			if !transition.BeforeExists || !transition.AfterExists || transition.AfterDeleted || transition.BeforeVersion == "" {
				return false
			}
		case "delete":
			if !transition.BeforeExists || !transition.AfterDeleted || transition.BeforeVersion == "" {
				return false
			}
		default:
			return false
		}
	}
	return len(seen) == len(want)
}

type issue49Mutation struct {
	ID            string
	State         string
	PredecessorID string
	Durable       bool
	Inspectable   bool
}

type issue49MutationOutcome struct {
	MutationID  string
	Kind        string
	Durable     bool
	Inspectable bool
}

type issue49MutationConservation struct {
	Captured []string
	Queue    []issue49Mutation
	Outcomes []issue49MutationOutcome
}

func issue49MutationConservationValid(observation issue49MutationConservation) bool {
	allowed := map[string]string{
		"pending":                "",
		"accepted":               "accepted",
		"server_rejected":        "server_rejected",
		"superseded_before_send": "superseded_before_send",
		"cancelled_before_send":  "cancelled_before_send",
		"blocked_by_predecessor": "blocked_by_predecessor",
	}
	captured := make(map[string]struct{}, len(observation.Captured))
	for _, id := range observation.Captured {
		if id == "" {
			return false
		}
		if _, duplicate := captured[id]; duplicate {
			return false
		}
		captured[id] = struct{}{}
	}
	queue := make(map[string]issue49Mutation, len(observation.Queue))
	for _, mutation := range observation.Queue {
		if _, exists := captured[mutation.ID]; !exists || !mutation.Durable || !mutation.Inspectable {
			return false
		}
		if _, duplicate := queue[mutation.ID]; duplicate {
			return false
		}
		if _, exists := allowed[mutation.State]; !exists {
			return false
		}
		if mutation.State == "blocked_by_predecessor" {
			if mutation.PredecessorID == "" {
				return false
			}
		} else if mutation.PredecessorID != "" {
			return false
		}
		queue[mutation.ID] = mutation
	}
	outcomes := make(map[string][]issue49MutationOutcome, len(observation.Outcomes))
	for _, outcome := range observation.Outcomes {
		if _, exists := queue[outcome.MutationID]; !exists || outcome.Kind == "" || !outcome.Durable || !outcome.Inspectable {
			return false
		}
		outcomes[outcome.MutationID] = append(outcomes[outcome.MutationID], outcome)
	}
	for id := range captured {
		mutation, exists := queue[id]
		if !exists {
			return false
		}
		if mutation.PredecessorID != "" {
			if _, exists := queue[mutation.PredecessorID]; !exists || mutation.PredecessorID == mutation.ID {
				return false
			}
		}
		wantOutcome := allowed[mutation.State]
		got := outcomes[id]
		if wantOutcome == "" {
			if len(got) != 0 {
				return false
			}
			continue
		}
		if len(got) != 1 || got[0].Kind != wantOutcome {
			return false
		}
	}
	return true
}

type issue49LocalIntent struct {
	LogicalActions       int
	StableMutationIDs    int
	DurableQueueEntries  int
	LocalRowTransitions  int
	RequestReplays       int
	ResponseReplays      int
	AuthoritativeEchoes  int
	EchoCreatedMutations int
	EchoRowTransitions   int
}

func issue49LocalIntentValid(observation issue49LocalIntent) bool {
	return observation.LogicalActions == 1 &&
		observation.StableMutationIDs == 1 &&
		observation.DurableQueueEntries == 1 &&
		observation.LocalRowTransitions == 1 &&
		observation.RequestReplays > 0 &&
		observation.ResponseReplays > 0 &&
		observation.AuthoritativeEchoes > 0 &&
		observation.EchoCreatedMutations == 0 &&
		observation.EchoRowTransitions == 0
}

type issue49CASAttempt struct {
	Operation       string
	BaseVersion     string
	Outcome         string
	CurrentVersion  string
	SourcePreserved bool
	Authoritative   bool
}

type issue49CASRace struct {
	Kind                  string
	LockedRow             bool
	LockedVersionIdentity bool
	Attempts              []issue49CASAttempt
	WinnerVersion         string
	SourceTransaction     string
	FenceTransaction      string
	InsertReservations    int
}

func issue49AtomicCASValid(races []issue49CASRace) bool {
	if len(races) != 3 {
		return false
	}
	seenKinds := make(map[string]struct{}, len(races))
	for _, race := range races {
		if _, duplicate := seenKinds[race.Kind]; duplicate {
			return false
		}
		seenKinds[race.Kind] = struct{}{}
		if !race.LockedRow || !race.LockedVersionIdentity || len(race.Attempts) != 2 || race.SourceTransaction == "" || race.SourceTransaction != race.FenceTransaction || race.WinnerVersion == "" {
			return false
		}
		applied := 0
		conflicted := 0
		for _, attempt := range race.Attempts {
			if !attempt.Authoritative {
				return false
			}
			switch attempt.Outcome {
			case "applied":
				applied++
				if attempt.CurrentVersion != race.WinnerVersion || !attempt.SourcePreserved {
					return false
				}
			case "conflict":
				conflicted++
				if attempt.CurrentVersion != race.WinnerVersion || attempt.SourcePreserved {
					return false
				}
			default:
				return false
			}
		}
		if applied != 1 || conflicted != 1 {
			return false
		}
		switch race.Kind {
		case "update-delete-update-wins", "update-delete-delete-wins":
			if race.InsertReservations != 0 || race.Attempts[0].BaseVersion == "" || race.Attempts[0].BaseVersion != race.Attempts[1].BaseVersion {
				return false
			}
			operations := map[string]int{}
			for _, attempt := range race.Attempts {
				operations[attempt.Operation]++
			}
			if operations["update"] != 1 || operations["delete"] != 1 {
				return false
			}
		case "insert-reservation":
			if race.InsertReservations != 1 || race.Attempts[0].Operation != "insert" || race.Attempts[1].Operation != "insert" || race.Attempts[0].BaseVersion != "" || race.Attempts[1].BaseVersion != "" {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func issue49ConcurrentUpdateDeleteValid(races []issue49CASRace) bool {
	if len(races) != 2 {
		return false
	}
	for _, race := range races {
		if race.Kind != "update-delete-update-wins" && race.Kind != "update-delete-delete-wins" {
			return false
		}
		if len(race.Attempts) != 2 || race.WinnerVersion == "" {
			return false
		}
		operations := map[string]int{}
		outcomes := map[string]int{}
		for _, attempt := range race.Attempts {
			if !attempt.Authoritative {
				return false
			}
			operations[attempt.Operation]++
			outcomes[attempt.Outcome]++
			if attempt.CurrentVersion != race.WinnerVersion {
				return false
			}
			if attempt.Outcome == "applied" && !attempt.SourcePreserved {
				return false
			}
		}
		if operations["update"] != 1 || operations["delete"] != 1 || outcomes["applied"] != 1 || outcomes["conflict"] != 1 {
			return false
		}
	}
	return true
}

type issue49TimeOutcome struct {
	Case           string
	BaseState      string
	ClientVersion  string
	Winner         string
	ServerVersion  string
	SourceUpdated  string
	WriteOrdinal   int
	Outcome        string
	DiagnosticKept bool
}

func issue49ClientTimeValid(outcomes []issue49TimeOutcome) bool {
	if len(outcomes) != 3 {
		return false
	}
	cases := map[string]struct{}{}
	baseline := outcomes[0]
	for _, outcome := range outcomes {
		if _, err := time.Parse(time.RFC3339Nano, outcome.ClientVersion); err != nil || !strings.HasSuffix(outcome.ClientVersion, "Z") || !outcome.DiagnosticKept {
			return false
		}
		if _, duplicate := cases[outcome.Case]; duplicate {
			return false
		}
		cases[outcome.Case] = struct{}{}
		if outcome.BaseState == "" || outcome.BaseState != baseline.BaseState || outcome.Winner != baseline.Winner || outcome.ServerVersion != baseline.ServerVersion || outcome.SourceUpdated != baseline.SourceUpdated || outcome.WriteOrdinal != baseline.WriteOrdinal || outcome.Outcome != baseline.Outcome {
			return false
		}
	}
	return hasExactKeys(cases, "equal", "past", "future")
}

type issue49Vocabulary struct {
	AcceptedPush         []string
	EmittedPull          []string
	RejectedPush         []string
	ImplicitResurrection bool
}

func issue49VocabularyValid(observation issue49Vocabulary) bool {
	if !sameStringSet(observation.AcceptedPush, []string{"insert", "update", "delete"}) || !sameStringSet(observation.EmittedPull, []string{"upsert", "delete"}) {
		return false
	}
	if observation.ImplicitResurrection || len(observation.RejectedPush) == 0 {
		return false
	}
	accepted := map[string]struct{}{"insert": {}, "update": {}, "delete": {}}
	for _, operation := range observation.RejectedPush {
		if _, canonical := accepted[operation]; canonical || operation == "" {
			return false
		}
	}
	return containsString(observation.RejectedPush, "create") && containsString(observation.RejectedPush, "upsert") && containsString(observation.RejectedPush, "resurrect")
}

type issue49LifecycleTransition struct {
	From              string
	To                string
	Accepted          bool
	ContractError     bool
	RetryableError    bool
	BeforeFingerprint string
	AfterFingerprint  string
}

func issue49LifecycleValid(states []string, transitions []issue49LifecycleTransition) bool {
	wantStates := []string{"uninitialized", "local_ready", "connecting", "schema_applying", "ready", "pushing", "pulling", "rebuilding", "backoff", "error", "stopped"}
	if !sameStringSet(states, wantStates) || len(transitions) != len(states)*len(states)+1 {
		return false
	}
	seen := make(map[string]struct{}, len(transitions))
	unknownSeen := false
	for _, transition := range transitions {
		key := transition.From + "\x00" + transition.To
		if _, duplicate := seen[key]; duplicate {
			return false
		}
		seen[key] = struct{}{}
		fromKnown := containsString(states, transition.From)
		toKnown := containsString(states, transition.To)
		if !fromKnown || !toKnown {
			if unknownSeen || transition.Accepted || !transition.ContractError || transition.RetryableError || transition.BeforeFingerprint != transition.AfterFingerprint {
				return false
			}
			unknownSeen = true
			continue
		}
		allowed := issue49LifecycleEdgeAllowed(transition.From, transition.To)
		if transition.Accepted != allowed {
			return false
		}
		if allowed {
			if transition.ContractError || transition.AfterFingerprint == transition.BeforeFingerprint {
				return false
			}
		} else if !transition.ContractError || transition.RetryableError || transition.AfterFingerprint != transition.BeforeFingerprint {
			return false
		}
	}
	return unknownSeen
}

func issue49LifecycleEdgeAllowed(from, to string) bool {
	switch from {
	case "uninitialized":
		return containsString([]string{"local_ready", "error", "stopped"}, to)
	case "local_ready":
		return containsString([]string{"connecting", "error", "stopped"}, to)
	case "connecting":
		return containsString([]string{"schema_applying", "ready", "backoff", "error", "stopped"}, to)
	case "schema_applying":
		return containsString([]string{"ready", "rebuilding", "error", "stopped"}, to)
	case "ready":
		return containsString([]string{"connecting", "pushing", "pulling", "rebuilding", "error", "stopped"}, to)
	case "pushing":
		return containsString([]string{"pushing", "ready", "pulling", "connecting", "backoff", "error", "stopped"}, to)
	case "pulling":
		return containsString([]string{"pulling", "ready", "rebuilding", "connecting", "backoff", "error", "stopped"}, to)
	case "rebuilding":
		return containsString([]string{"rebuilding", "ready", "connecting", "backoff", "error", "stopped"}, to)
	case "backoff":
		return containsString([]string{"connecting", "pushing", "pulling", "rebuilding", "error", "stopped"}, to)
	case "error":
		return containsString([]string{"local_ready", "stopped"}, to)
	case "stopped":
		return to == "local_ready"
	default:
		return false
	}
}

type issue49VersionTransition struct {
	ApplicationVersion string
	MinimumVersion     string
	ProtocolVersion    int
	Accepted           bool
	StoredVersion      string
}

func issue49VersionTransitionsValid(transitions []issue49VersionTransition) bool {
	if len(transitions) == 0 {
		return false
	}
	for _, transition := range transitions {
		application, applicationOK := parseIssue49SemVer(transition.ApplicationVersion)
		minimum, minimumOK := parseIssue49SemVer(transition.MinimumVersion)
		wantAccepted := applicationOK && minimumOK && transition.ProtocolVersion == 3 && compareIssue49SemVer(application, minimum) >= 0
		if transition.Accepted != wantAccepted {
			return false
		}
		if transition.Accepted {
			if transition.StoredVersion != transition.ApplicationVersion {
				return false
			}
		} else if transition.StoredVersion != "" {
			return false
		}
	}
	return true
}

type issue49SemVer struct {
	major      string
	minor      string
	patch      string
	prerelease []string
}

func parseIssue49SemVer(value string) (issue49SemVer, bool) {
	if value == "" || strings.HasPrefix(value, "v") || strings.HasPrefix(value, "V") {
		return issue49SemVer{}, false
	}
	mainAndBuild := strings.SplitN(value, "+", 2)
	if len(mainAndBuild) == 2 && !issue49IdentifiersValid(mainAndBuild[1], false) {
		return issue49SemVer{}, false
	}
	coreAndPrerelease := strings.SplitN(mainAndBuild[0], "-", 2)
	core := strings.Split(coreAndPrerelease[0], ".")
	if len(core) != 3 || !issue49CoreNumberValid(core[0]) || !issue49CoreNumberValid(core[1]) || !issue49CoreNumberValid(core[2]) {
		return issue49SemVer{}, false
	}
	parsed := issue49SemVer{major: core[0], minor: core[1], patch: core[2]}
	if len(coreAndPrerelease) == 2 {
		if !issue49IdentifiersValid(coreAndPrerelease[1], true) {
			return issue49SemVer{}, false
		}
		parsed.prerelease = strings.Split(coreAndPrerelease[1], ".")
	}
	return parsed, true
}

func issue49CoreNumberValid(value string) bool {
	return issue49Numeric(value) && (value == "0" || value[0] != '0')
}

func issue49IdentifiersValid(value string, rejectNumericLeadingZero bool) bool {
	if value == "" {
		return false
	}
	for _, identifier := range strings.Split(value, ".") {
		if identifier == "" {
			return false
		}
		for _, char := range identifier {
			if !((char >= '0' && char <= '9') || (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || char == '-') {
				return false
			}
		}
		if rejectNumericLeadingZero && issue49Numeric(identifier) && len(identifier) > 1 && identifier[0] == '0' {
			return false
		}
	}
	return true
}

func issue49Numeric(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func compareIssue49SemVer(left, right issue49SemVer) int {
	for _, pair := range [][2]string{{left.major, right.major}, {left.minor, right.minor}, {left.patch, right.patch}} {
		if compared := compareIssue49NumericStrings(pair[0], pair[1]); compared != 0 {
			return compared
		}
	}
	if len(left.prerelease) == 0 && len(right.prerelease) == 0 {
		return 0
	}
	if len(left.prerelease) == 0 {
		return 1
	}
	if len(right.prerelease) == 0 {
		return -1
	}
	limit := len(left.prerelease)
	if len(right.prerelease) < limit {
		limit = len(right.prerelease)
	}
	for index := 0; index < limit; index++ {
		leftIdentifier := left.prerelease[index]
		rightIdentifier := right.prerelease[index]
		leftNumeric := issue49Numeric(leftIdentifier)
		rightNumeric := issue49Numeric(rightIdentifier)
		switch {
		case leftNumeric && rightNumeric:
			if compared := compareIssue49NumericStrings(leftIdentifier, rightIdentifier); compared != 0 {
				return compared
			}
		case leftNumeric:
			return -1
		case rightNumeric:
			return 1
		case leftIdentifier < rightIdentifier:
			return -1
		case leftIdentifier > rightIdentifier:
			return 1
		}
	}
	switch {
	case len(left.prerelease) < len(right.prerelease):
		return -1
	case len(left.prerelease) > len(right.prerelease):
		return 1
	default:
		return 0
	}
}

func compareIssue49NumericStrings(left, right string) int {
	switch {
	case len(left) < len(right):
		return -1
	case len(left) > len(right):
		return 1
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

type issue49PortableInteger struct {
	Value   int64
	Sign    string
	Encoded string
	Decoded int64
}

type issue49PortableIntegerObservation struct {
	EnvelopeValues      []issue49PortableInteger
	OpaqueDecimalValues []string
	CounterBefore       int64
	CounterAfter        int64
	OverflowAttempted   bool
	OverflowRejected    bool
	StateUnchanged      bool
}

func issue49PortableIntegersValid(observation issue49PortableIntegerObservation) bool {
	if len(observation.EnvelopeValues) == 0 || len(observation.OpaqueDecimalValues) == 0 {
		return false
	}
	for _, integer := range observation.EnvelopeValues {
		if integer.Value < -issue49MaxPortableInteger || integer.Value > issue49MaxPortableInteger || integer.Decoded != integer.Value || integer.Encoded != strconv.FormatInt(integer.Value, 10) {
			return false
		}
		switch integer.Sign {
		case "signed":
		case "nonnegative":
			if integer.Value < 0 {
				return false
			}
		case "positive":
			if integer.Value < 1 {
				return false
			}
		default:
			return false
		}
	}
	opaqueBeyondEnvelope := false
	for _, opaque := range observation.OpaqueDecimalValues {
		if opaque == "" || !issue49Numeric(opaque) || (len(opaque) > 1 && opaque[0] == '0') {
			return false
		}
		if compareIssue49NumericStrings(opaque, strconv.FormatInt(issue49MaxPortableInteger, 10)) > 0 {
			opaqueBeyondEnvelope = true
		}
	}
	return observation.CounterBefore == issue49MaxPortableInteger &&
		observation.CounterAfter == observation.CounterBefore &&
		observation.OverflowAttempted && observation.OverflowRejected && observation.StateUnchanged && opaqueBeyondEnvelope
}

type issue49OutcomeSchemaObservation struct {
	OutcomeID                string
	AuthoredSchema           string
	ClassificationSchema     string
	ChecksumSchema           string
	ReplaySchema             string
	CurrentSchema            string
	HistoricalManifestLoaded bool
	HistoricalChecksumValid  bool
	ProjectionSafe           bool
	HistoricalAppliedCurrent bool
	OutcomeInspectable       bool
	LaterIntentBefore        []string
	LaterIntentAfter         []string
}

func issue49OutcomeSchemaValid(observation issue49OutcomeSchemaObservation) bool {
	if observation.OutcomeID == "" || observation.AuthoredSchema == "" || observation.CurrentSchema == "" || observation.AuthoredSchema == observation.CurrentSchema {
		return false
	}
	if observation.ClassificationSchema != observation.AuthoredSchema || observation.ChecksumSchema != observation.AuthoredSchema || observation.ReplaySchema != observation.AuthoredSchema {
		return false
	}
	if !observation.HistoricalManifestLoaded || !observation.HistoricalChecksumValid || !observation.OutcomeInspectable {
		return false
	}
	if !observation.ProjectionSafe && observation.HistoricalAppliedCurrent {
		return false
	}
	return equalStrings(observation.LaterIntentBefore, observation.LaterIntentAfter)
}

type issue49EffectPosition struct {
	CommitLSN     uint64
	EventOrdinal  uint64
	EffectOrdinal uint64
}

type issue49PullEffect struct {
	ID                string
	Position          issue49EffectPosition
	WALReplayIdentity string
}

type issue49EffectProgress struct {
	PersistedEffects []issue49PullEffect
	FirstPage        []string
	FirstCursor      issue49EffectPosition
	SecondPage       []string
	FinalCursor      issue49EffectPosition
	ReplayFirstPage  []string
}

func issue49EffectProgressValid(observation issue49EffectProgress) bool {
	if len(observation.PersistedEffects) != 3 || len(observation.FirstPage) != 1 || len(observation.SecondPage) != 2 || len(observation.ReplayFirstPage) != 1 {
		return false
	}
	wantReplayIdentity := observation.PersistedEffects[0].WALReplayIdentity
	seenOrdinals := make(map[uint64]struct{}, len(observation.PersistedEffects))
	for index, effect := range observation.PersistedEffects {
		if effect.ID == "" || effect.WALReplayIdentity == "" || effect.WALReplayIdentity != wantReplayIdentity || effect.Position.CommitLSN != observation.PersistedEffects[0].Position.CommitLSN || effect.Position.EventOrdinal != observation.PersistedEffects[0].Position.EventOrdinal || effect.Position.EffectOrdinal != uint64(index) {
			return false
		}
		if _, duplicate := seenOrdinals[effect.Position.EffectOrdinal]; duplicate {
			return false
		}
		seenOrdinals[effect.Position.EffectOrdinal] = struct{}{}
	}
	return observation.FirstPage[0] == observation.PersistedEffects[0].ID &&
		observation.ReplayFirstPage[0] == observation.FirstPage[0] &&
		observation.FirstCursor == observation.PersistedEffects[0].Position &&
		observation.SecondPage[0] == observation.PersistedEffects[1].ID &&
		observation.SecondPage[1] == observation.PersistedEffects[2].ID &&
		observation.FinalCursor == observation.PersistedEffects[2].Position
}

type issue49BoundaryChange struct {
	ID                   string
	Kind                 string
	Position             uint64
	IncludedInSnapshot   bool
	IncrementalDelivered bool
}

type issue49RebuildBoundary struct {
	Boundary        uint64
	SnapshotRecords []issue49BoundaryChange
	PostBoundary    []issue49BoundaryChange
	FinalCursor     uint64
	CursorPresented bool
}

func issue49RebuildBoundaryValid(observation issue49RebuildBoundary) bool {
	if observation.Boundary == 0 || observation.FinalCursor != observation.Boundary || !observation.CursorPresented || len(observation.PostBoundary) != 3 {
		return false
	}
	for _, change := range observation.SnapshotRecords {
		if change.ID == "" || change.Position > observation.Boundary || !change.IncludedInSnapshot || change.IncrementalDelivered {
			return false
		}
	}
	kinds := make(map[string]struct{}, len(observation.PostBoundary))
	for _, change := range observation.PostBoundary {
		if change.ID == "" || change.Position <= observation.Boundary || change.IncludedInSnapshot || !change.IncrementalDelivered {
			return false
		}
		kinds[change.Kind] = struct{}{}
	}
	return hasExactKeys(kinds, "write", "delete", "membership")
}

type issue49RebuildEpochResponse struct {
	RequestKind string
	Epoch       uint64
	Status      int
	ErrorCode   string
	Records     int
	Progress    bool
}

type issue49RebuildEpoch struct {
	SessionEpoch           uint64
	EpochBeforeWrite       uint64
	EpochAfterAccepted     uint64
	AcceptedFirstExecution bool
	WriteAndEpochAtomic    bool
	RejectedAdvance        bool
	ExactReplayAdvance     bool
	Responses              []issue49RebuildEpochResponse
}

func issue49RebuildEpochValid(observation issue49RebuildEpoch) bool {
	if observation.SessionEpoch != observation.EpochBeforeWrite || !observation.AcceptedFirstExecution || !observation.WriteAndEpochAtomic || observation.EpochAfterAccepted != observation.EpochBeforeWrite+1 || observation.RejectedAdvance || observation.ExactReplayAdvance || len(observation.Responses) != 2 {
		return false
	}
	kinds := make(map[string]struct{}, len(observation.Responses))
	for _, response := range observation.Responses {
		kinds[response.RequestKind] = struct{}{}
		if response.Epoch != observation.SessionEpoch || response.Status != 409 || response.ErrorCode != "rebuild_restart_required" || response.Records != 0 || response.Progress {
			return false
		}
	}
	return hasExactKeys(kinds, "later_page", "stored_replay")
}

type issue49Class3Bootstrap struct {
	CandidateSlotPermanent bool
	CandidateDatabase      string
	CandidatePlugin        string
	ExportedSnapshot       string
	SnapshotImported       bool
	MainBarrier            uint64
	CandidateBarrier       uint64
	Projections            map[string]bool
	CaughtUp               bool
	ManifestActiveBefore   bool
	ManifestActiveAfter    bool
	GenerationsActive      bool
	ProjectionsActive      bool
	InvalidationActive     bool
	ActivationTransaction  string
	ActivationTransactions map[string]string
	FailureStateUnchanged  bool
}

func issue49Class3BootstrapValid(observation issue49Class3Bootstrap) bool {
	if !observation.CandidateSlotPermanent || observation.CandidateDatabase == "" || observation.CandidatePlugin == "" || observation.ExportedSnapshot == "" || !observation.SnapshotImported || observation.MainBarrier == 0 || observation.MainBarrier != observation.CandidateBarrier || !observation.CaughtUp {
		return false
	}
	if observation.ManifestActiveBefore || !observation.ManifestActiveAfter || !observation.GenerationsActive || !observation.ProjectionsActive || !observation.InvalidationActive || observation.ActivationTransaction == "" || !observation.FailureStateUnchanged {
		return false
	}
	wantActivation := []string{"manifest", "generations", "projections", "invalidation"}
	if len(observation.ActivationTransactions) != len(wantActivation) {
		return false
	}
	for _, component := range wantActivation {
		if observation.ActivationTransactions[component] != observation.ActivationTransaction {
			return false
		}
	}
	want := []string{"source", "dependency", "version", "membership", "integrity"}
	if len(observation.Projections) != len(want) {
		return false
	}
	for _, projection := range want {
		if !observation.Projections[projection] {
			return false
		}
	}
	return true
}

type issue49MigrationPhase struct {
	Name          string
	RestartBefore bool
	RestartAfter  bool
	Committed     bool
	Executions    int
}

type issue49MigrationMismatch struct {
	Kind          string
	Completed     bool
	ExplicitError bool
}

type issue49SchemaMigration struct {
	JournalPersistedBeforeDDL bool
	TargetManifestVerified    bool
	TargetManifestComplete    bool
	PlanVersion               int
	PlanTyped                 bool
	SourceReferenceBound      bool
	TargetReferenceBound      bool
	ActionBound               bool
	ScopesBound               bool
	PhaseBound                bool
	RestartFetchedSchema      bool
	RestartPhases             []issue49MigrationPhase
	QueueBefore               []string
	QueueAfter                []string
	LocalDataBefore           []string
	LocalDataAfter            []string
	Consistency               map[string]bool
	MismatchResults           []issue49MigrationMismatch
}

func issue49SchemaMigrationValid(observation issue49SchemaMigration) bool {
	if !observation.JournalPersistedBeforeDDL || !observation.TargetManifestVerified || !observation.TargetManifestComplete || observation.PlanVersion < 1 || !observation.PlanTyped || !observation.SourceReferenceBound || !observation.TargetReferenceBound || !observation.ActionBound || !observation.ScopesBound || !observation.PhaseBound || observation.RestartFetchedSchema {
		return false
	}
	if !equalStrings(observation.QueueBefore, observation.QueueAfter) || !equalStrings(observation.LocalDataBefore, observation.LocalDataAfter) {
		return false
	}
	wantConsistency := []string{"physical", "reference", "body", "plan", "phase"}
	if len(observation.Consistency) != len(wantConsistency) {
		return false
	}
	for _, check := range wantConsistency {
		if !observation.Consistency[check] {
			return false
		}
	}
	if len(observation.MismatchResults) != len(wantConsistency) {
		return false
	}
	mismatches := make(map[string]struct{}, len(observation.MismatchResults))
	for _, mismatch := range observation.MismatchResults {
		if !containsString(wantConsistency, mismatch.Kind) || mismatch.Completed || !mismatch.ExplicitError {
			return false
		}
		if _, duplicate := mismatches[mismatch.Kind]; duplicate {
			return false
		}
		mismatches[mismatch.Kind] = struct{}{}
	}
	if len(observation.RestartPhases) == 0 {
		return false
	}
	for _, phase := range observation.RestartPhases {
		if phase.Name == "" || !phase.RestartBefore || !phase.RestartAfter || phase.Executions != 1 || !phase.Committed {
			return false
		}
	}
	return true
}

type issue49SchemaCursorScope struct {
	Scope                string
	HistoricalPosition   issue49EffectPosition
	HistoricalToken      string
	HistoricalSchema     string
	CurrentSchema        string
	CompatibleLineage    bool
	Class3Affected       bool
	OtherBindingsCurrent bool
	ReplacementToken     string
	ReplacementPosition  issue49EffectPosition
	ReplacementSchema    string
	ReturnedNull         bool
	ClientInstalled      bool
	InstalledAtomically  bool
}

func issue49SchemaCursorValid(scopes []issue49SchemaCursorScope) bool {
	if len(scopes) != 3 {
		return false
	}
	kinds := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		if scope.Scope == "" || scope.HistoricalToken == "" || scope.HistoricalSchema == "" || scope.CurrentSchema == "" || scope.HistoricalSchema == scope.CurrentSchema {
			return false
		}
		kinds[scope.Scope] = struct{}{}
		eligible := scope.CompatibleLineage && !scope.Class3Affected && scope.OtherBindingsCurrent
		if eligible {
			if scope.ReplacementToken == "" || scope.ReplacementToken == scope.HistoricalToken || scope.ReplacementPosition != scope.HistoricalPosition || scope.ReplacementSchema != scope.CurrentSchema || scope.ReturnedNull || !scope.ClientInstalled || !scope.InstalledAtomically {
				return false
			}
		} else if scope.ReplacementToken != "" || scope.ReplacementPosition != (issue49EffectPosition{}) || scope.ReplacementSchema != "" || !scope.ReturnedNull || scope.ClientInstalled || scope.InstalledAtomically {
			return false
		}
	}
	return hasExactKeys(kinds, "unaffected", "affected", "stale")
}

type issue49RetentionCursor struct {
	Name          string
	Position      uint64
	OtherBindings bool
	Eligible      bool
	Rebuild       bool
}

type issue49RetentionFloor struct {
	Scope            string
	Floor            uint64
	FloorDurable     bool
	Lineage          string
	LineageDurable   bool
	EffectsRemaining int
	HighWatermark    uint64
	Cursors          []issue49RetentionCursor
}

func issue49RetentionFloorValid(observation issue49RetentionFloor) bool {
	if observation.Scope == "" || observation.Floor == 0 || !observation.FloorDurable || observation.Lineage == "" || !observation.LineageDurable || observation.EffectsRemaining != 0 || observation.HighWatermark < observation.Floor || len(observation.Cursors) != 3 {
		return false
	}
	names := make(map[string]struct{}, len(observation.Cursors))
	for _, cursor := range observation.Cursors {
		names[cursor.Name] = struct{}{}
		wantEligible := cursor.OtherBindings && cursor.Position >= observation.Floor
		wantRebuild := cursor.OtherBindings && cursor.Position < observation.Floor
		if cursor.Eligible != wantEligible || cursor.Rebuild != wantRebuild {
			return false
		}
	}
	return hasExactKeys(names, "below", "at", "above")
}

type issue49QueueAction struct {
	ID          string
	Operation   string
	State       string
	LinkedTo    string
	BaseVersion string
	Sent        bool
	Inspectable bool
}

type issue49QueueResolution struct {
	Actions            []issue49QueueAction
	ServerRowsBefore   int
	ServerRowsAfter    int
	FabricatedBaseSent bool
}

func issue49QueueResolutionValid(observation issue49QueueResolution) bool {
	if len(observation.Actions) != 5 || observation.ServerRowsBefore != observation.ServerRowsAfter || observation.FabricatedBaseSent {
		return false
	}
	want := map[string]struct {
		operation string
		state     string
		linked    string
		base      string
		sent      bool
	}{
		"insert":  {operation: "insert", state: "cancelled_before_send", linked: "delete", base: "", sent: false},
		"update":  {operation: "update", state: "superseded_before_send", linked: "insert", base: "", sent: false},
		"delete":  {operation: "delete", state: "cancelled_before_send", linked: "insert", base: "", sent: false},
		"sealed":  {operation: "update", state: "sealed", linked: "", base: "server-v1", sent: true},
		"blocked": {operation: "delete", state: "blocked_by_predecessor", linked: "sealed", base: "", sent: false},
	}
	seen := make(map[string]struct{}, len(observation.Actions))
	for _, action := range observation.Actions {
		expected, exists := want[action.ID]
		if !exists || !action.Inspectable || action.Operation != expected.operation || action.State != expected.state || action.LinkedTo != expected.linked || action.BaseVersion != expected.base || action.Sent != expected.sent {
			return false
		}
		if _, duplicate := seen[action.ID]; duplicate {
			return false
		}
		seen[action.ID] = struct{}{}
	}
	return len(seen) == len(want)
}

type issue49CleanupRow struct {
	Path               string
	IntentKind         string
	Unresolved         bool
	ServerProvenance   bool
	ApplicationVisible bool
	MutationCreated    bool
}

type issue49ScopeCleanup struct {
	ProtectedRows   []issue49CleanupRow
	UnprotectedRows []issue49CleanupRow
}

func issue49ScopeCleanupValid(observation issue49ScopeCleanup) bool {
	if len(observation.ProtectedRows) != 9 || len(observation.UnprotectedRows) != 3 {
		return false
	}
	paths := map[string]int{}
	intents := map[string]int{}
	for _, row := range observation.ProtectedRows {
		if !row.Unresolved || row.ServerProvenance || !row.ApplicationVisible || row.MutationCreated {
			return false
		}
		paths[row.Path]++
		intents[row.IntentKind]++
	}
	for _, row := range observation.UnprotectedRows {
		if row.Unresolved || row.ServerProvenance || row.ApplicationVisible || row.MutationCreated {
			return false
		}
		paths[row.Path]++
	}
	return paths["assignment"] == 4 && paths["seed"] == 4 && paths["rebuild"] == 4 && intents["insert"] == 3 && intents["update"] == 3 && intents["blocked"] == 3
}

func sameStringSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	counts := make(map[string]int, len(want))
	for _, value := range want {
		counts[value]++
	}
	for _, value := range got {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}
	return true
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func hasExactKeys(values map[string]struct{}, want ...string) bool {
	if len(values) != len(want) {
		return false
	}
	for _, value := range want {
		if _, exists := values[value]; !exists {
			return false
		}
	}
	return true
}
