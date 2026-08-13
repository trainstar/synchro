package reference

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/scenarios"
)

type pullRebuildTestClock struct {
	now time.Time
}

func (clock *pullRebuildTestClock) Now() time.Time {
	return clock.now
}

func TestPullEnforcesInstalledMaximum(t *testing.T) {
	tests := []struct {
		name       string
		limit      uint32
		wantStatus int
	}{
		{name: "lower", limit: 1, wantStatus: 200},
		{name: "upper", limit: 1000, wantStatus: 200},
		{name: "invalid", limit: 1001, wantStatus: 400},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA})
			model, _ := newPullRebuildTestModel(t, state)
			installTestIncrementalCursor(t, model, client, scopeA, StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart})
			before := model.Snapshot()

			result := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint}}, test.limit))
			if result.HTTP == nil || result.HTTP.Status != test.wantStatus || result.Pull == nil {
				t.Fatalf("pull limit %d result = %#v, want HTTP %d", test.limit, result, test.wantStatus)
			}
			if test.wantStatus == 400 {
				if !result.HTTP.HasCode || result.HTTP.Code != "invalid_request" || result.HTTP.Retryable || result.Kind != StepResultKindPull {
					t.Fatalf("pull limit %d result = %#v, want typed invalid_request", test.limit, result)
				}
				if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
					t.Fatal("oversized pull changed model state")
				}
			}
		})
	}
}

func TestRebuildEnforcesInstalledMaximum(t *testing.T) {
	tests := []struct {
		name       string
		limit      uint32
		wantStatus int
	}{
		{name: "lower", limit: 1, wantStatus: 200},
		{name: "upper", limit: 1000, wantStatus: 200},
		{name: "invalid", limit: 1001, wantStatus: 400},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state, client, schema, _ := newPullRebuildTestState(t, []ScopeID{scopeA})
			model, _ := newPullRebuildTestModel(t, state)
			before := model.Snapshot()

			result := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceNone, test.limit))
			if result.HTTP == nil || result.HTTP.Status != test.wantStatus || result.Rebuild == nil {
				t.Fatalf("rebuild limit %d result = %#v, want HTTP %d", test.limit, result, test.wantStatus)
			}
			if test.wantStatus == 400 {
				if !result.HTTP.HasCode || result.HTTP.Code != "invalid_request" || result.HTTP.Retryable || result.Kind != StepResultKindRebuild {
					t.Fatalf("rebuild limit %d result = %#v, want typed invalid_request", test.limit, result)
				}
				if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
					t.Fatal("oversized rebuild changed model state")
				}
			}
		})
	}
}

func TestPullResolvesScopedCandidatesBeforeTheGlobalLimit(t *testing.T) {
	state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA, scopeB})
	first := canonicalStringRowIdentity(tableA, fieldA, "row-a")
	collision := first
	collision.TableID = tableB
	firstPosition := testEffectPosition(generation, 2, 1, 1)
	secondPosition := testEffectPosition(generation, 3, 1, 1)
	thirdPosition := testEffectPosition(generation, 4, 1, 1)
	fourthPosition := testEffectPosition(generation, 5, 1, 1)
	addTestPullEffect(&state, scopeA, firstPosition, first, "historic-old", Checksum{0: 2})
	addTestPullEffect(&state, scopeB, secondPosition, first, "historic-scope-b", Checksum{0: 3})
	addTestPullEffect(&state, scopeA, thirdPosition, collision, "typed-collision", Checksum{0: 4})
	addTestPullEffect(&state, scopeA, fourthPosition, first, "historic-new", Checksum{0: 5})
	state.Rows[first] = AuthoritativeRow{Identity: first, Version: "live-row-version", Checksum: Checksum{0: 99}}

	model, _ := newPullRebuildTestModel(t, state)
	start := StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart}
	installTestIncrementalCursor(t, model, client, scopeA, start)
	installTestIncrementalCursor(t, model, client, scopeB, start)

	firstPage := applyTestOperation(t, model, "pull", "request-page", pullRequestPayload{
		UserID:           string(client.UserID),
		ClientID:         string(client.ClientID),
		ClientGeneration: uint64(Generation(1)),
		Schema:           testSchemaPayload(schema),
		ScopeSetVersion:  1,
		Scopes: []pullScopePayload{
			{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint},
			{ScopeID: string(scopeB), CursorSource: tokenSourceLocalCheckpoint},
		},
		Limit: 2,
	})
	if firstPage.HTTP.Status != 200 || firstPage.Pull == nil || !firstPage.Pull.HasMore {
		t.Fatalf("first pull result = %#v, want a nonterminal success", firstPage)
	}
	if len(firstPage.Pull.Changes) != 2 {
		t.Fatalf("first pull changes = %d, want 2", len(firstPage.Pull.Changes))
	}
	if got := firstPage.Pull.Changes[0]; got.Scope != scopeB || got.Row != first || got.Version != "historic-scope-b" {
		t.Fatalf("first candidate = %#v, want scope B historical effect", got)
	}
	if got := firstPage.Pull.Changes[1]; got.Scope != scopeA || got.Row != collision || got.Version != "typed-collision" {
		t.Fatalf("second candidate = %#v, want typed-identity collision", got)
	}
	for _, change := range firstPage.Pull.Changes {
		if change.Version == "historic-old" {
			t.Fatal("raw pre-limit effect was selected instead of the retained greatest scoped candidate")
		}
		if change.Version == "live-row-version" {
			t.Fatal("pull hydrated a historical effect from the live row")
		}
	}
	if firstPage.Pull.ScopeChecksums != nil {
		t.Fatal("nonterminal pull included terminal checksums")
	}

	copyIssuedCursorToLocal(model, client, scopeA)
	copyIssuedCursorToLocal(model, client, scopeB)
	secondPage := applyTestOperation(t, model, "pull", "request-page", pullRequestPayload{
		UserID:           string(client.UserID),
		ClientID:         string(client.ClientID),
		ClientGeneration: 1,
		Schema:           testSchemaPayload(schema),
		ScopeSetVersion:  1,
		Scopes: []pullScopePayload{
			{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint},
			{ScopeID: string(scopeB), CursorSource: tokenSourceLocalCheckpoint},
		},
		Limit: 2,
	})
	if secondPage.HTTP.Status != 200 || secondPage.Pull == nil || secondPage.Pull.HasMore || len(secondPage.Pull.Changes) != 1 {
		t.Fatalf("second pull result = %#v, want a terminal one-change page", secondPage)
	}
	if got := secondPage.Pull.Changes[0]; got.Scope != scopeA || got.Row != first || got.Version != "historic-new" {
		t.Fatalf("terminal scoped candidate = %#v, want the retained scope A effect", got)
	}
	if len(secondPage.Pull.ScopeChecksums) != 2 {
		t.Fatalf("terminal checksum count = %d, want 2 active scopes", len(secondPage.Pull.ScopeChecksums))
	}
}

func TestPullFailureAndAcknowledgementBoundaries(t *testing.T) {
	t.Run("missing projection rolls back the complete page", func(t *testing.T) {
		state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA})
		row := canonicalStringRowIdentity(tableA, fieldA, "missing-projection")
		position := testEffectPosition(generation, 2, 1, 1)
		addTestPullEffect(&state, scopeA, position, row, "missing", Checksum{0: 7})
		delete(state.Projections, state.Scopes[scopeA].Effects[0].CapturedProjection)
		model, _ := newPullRebuildTestModel(t, state)
		installTestIncrementalCursor(t, model, client, scopeA, StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart})
		before := model.Snapshot()

		result := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint}}, 1))
		if result.HTTP.Status != 500 || !result.HTTP.HasCode || result.HTTP.Code != "sync_integrity_failure" {
			t.Fatalf("missing projection result = %#v, want integrity failure", result)
		}
		if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatal("missing projection committed cursor or checkpoint state")
		}
	})

	t.Run("issuance does not acknowledge and later presentation advances monotonically", func(t *testing.T) {
		state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA})
		row := canonicalStringRowIdentity(tableA, fieldA, "acknowledgement")
		addTestPullEffect(&state, scopeA, testEffectPosition(generation, 2, 1, 1), row, "ack-row", Checksum{0: 8})
		model, _ := newPullRebuildTestModel(t, state)
		start := StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart}
		installTestIncrementalCursor(t, model, client, scopeA, start)

		issued := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint}}, 1))
		if issued.HTTP.Status != 200 || issued.Pull == nil {
			t.Fatalf("issuance result = %#v", issued)
		}
		server := model.state.Clients[client]
		checkpoint, found := testServerCheckpoint(server, scopeA)
		if !found || checkpoint.Position != start {
			t.Fatal("issued cursor advanced the durable server checkpoint")
		}

		copyIssuedCursorToLocal(model, client, scopeA)
		acknowledged := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint}}, 1))
		if acknowledged.HTTP.Status != 200 || acknowledged.Pull == nil {
			t.Fatalf("acknowledgement result = %#v", acknowledged)
		}
		checkpoint, found = testServerCheckpoint(model.state.Clients[client], scopeA)
		if !found || checkpoint.Position.Kind != PositionKindTransactionEnd || checkpoint.Position.CommitLSN != 10 {
			t.Fatalf("acknowledged checkpoint = %#v, want terminal boundary", checkpoint)
		}
		copyIssuedCursorToLocal(model, client, scopeA)
		older := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint}}, 1))
		if older.HTTP.Status != 200 {
			t.Fatalf("later acknowledged request = %#v", older)
		}
		checkpointAfter, _ := testServerCheckpoint(model.state.Clients[client], scopeA)
		if checkpointAfter.Position != checkpoint.Position {
			t.Fatal("older valid cursor moved the checkpoint backward")
		}
	})

	t.Run("stale scopes rebuild but forged scopes reject the whole request", func(t *testing.T) {
		state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA, scopeB})
		model, _ := newPullRebuildTestModel(t, state)
		start := StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart}
		installTestIncrementalCursor(t, model, client, scopeA, start)
		installTestIncrementalCursor(t, model, client, scopeB, start)

		clientState := model.state.Clients[client]
		index, _ := findScopeAssignment(clientState.ScopeAssignments, scopeA)
		clientState.ScopeAssignments[index].MembershipGeneration++
		model.state.Clients[client] = clientState
		scopeState := model.state.Scopes[scopeA]
		scopeState.MembershipGeneration++
		model.state.Scopes[scopeA] = scopeState
		stale := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{
			{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint},
			{ScopeID: string(scopeB), CursorSource: tokenSourceLocalCheckpoint},
		}, 1))
		if stale.HTTP.Status != 200 || stale.Pull == nil || !containsScope(stale.Pull.RebuildScopes, scopeA) {
			t.Fatalf("stale cursor result = %#v, want scope-local rebuild", stale)
		}

		before := model.Snapshot()
		forged := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{
			{ScopeID: string(scopeA), CursorSource: tokenSourceForged},
			{ScopeID: string(scopeB), CursorSource: tokenSourceLocalCheckpoint},
		}, 1))
		if forged.HTTP.Status != 400 || !forged.HTTP.HasCode || forged.HTTP.Code != "invalid_request" {
			t.Fatalf("forged cursor result = %#v, want invalid request", forged)
		}
		if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatal("forged cursor processed another scope or changed state")
		}
	})

	t.Run("pending accepted fences block pull and rebuild before state changes", func(t *testing.T) {
		state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA})
		state.Fences[fenceA] = VersionFence{
			ID:             fenceA,
			HasMutationKey: true,
			MutationKey:    MutationKey{Client: client, Mutation: mutationA},
			Coverage:       FenceCoveragePending,
		}
		model, _ := newPullRebuildTestModel(t, state)
		installTestIncrementalCursor(t, model, client, scopeA, StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart})
		before := model.Snapshot()
		pull := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint}}, 1))
		if pull.HTTP.Status != 503 || pull.HTTP.Code != "capture_pending" || !pull.HTTP.Retryable {
			t.Fatalf("pending pull = %#v", pull)
		}
		rebuild := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceNone, 1))
		if rebuild.HTTP.Status != 503 || rebuild.HTTP.Code != "capture_pending" || !rebuild.HTTP.Retryable {
			t.Fatalf("pending rebuild = %#v", rebuild)
		}
		if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatal("capture_pending allocated a page, session, or checkpoint")
		}
	})

	t.Run("terminal checksums include active rebuild scopes", func(t *testing.T) {
		state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA, scopeB})
		model, _ := newPullRebuildTestModel(t, state)
		installTestIncrementalCursor(t, model, client, scopeA, StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart})
		result := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{
			{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint},
			{ScopeID: string(scopeB), CursorSource: tokenSourceNone},
		}, 1))
		if result.HTTP.Status != 200 || result.Pull == nil || result.Pull.HasMore || len(result.Pull.ScopeChecksums) != 2 || !containsScope(result.Pull.RebuildScopes, scopeB) {
			t.Fatalf("terminal pull = %#v, want complete checksums and rebuild precedence", result)
		}
		for _, checksum := range result.Pull.ScopeChecksums {
			if !checksum.HasChecksum {
				t.Fatal("terminal checksum map contains an absent checksum")
			}
		}
	})
}

func TestRebuildUsesImmutablePagesAndDispatchesContinuationFailures(t *testing.T) {
	state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA, scopeB})
	first := canonicalStringRowIdentity(tableA, fieldA, "rebuild-a")
	second := canonicalStringRowIdentity(tableA, fieldA, "rebuild-b")
	third := canonicalStringRowIdentity(tableA, fieldA, "rebuild-c")
	fourth := canonicalStringRowIdentity(tableA, fieldA, "post-boundary")
	firstRow := testAuthoritativeRow(first, "snapshot-a", Checksum{0: 11})
	secondRow := testAuthoritativeRow(second, "snapshot-b", Checksum{0: 12})
	thirdRow := testAuthoritativeRow(third, "snapshot-c", Checksum{0: 13})
	state.Rows[first] = firstRow
	state.Rows[second] = secondRow
	state.Rows[third] = thirdRow
	scopeState := state.Scopes[scopeA]
	scopeState.Membership = []ScopeMembership{{Row: first, Included: true}, {Row: second, Included: true}, {Row: third, Included: true}}
	scopeState.Cardinality = 3
	state.Scopes[scopeA] = scopeState
	local := state.ClientLocal[client]
	local.Rows = []LocalRow{{Identity: canonicalStringRowIdentity(tableB, fieldA, "scope-b-row")}}
	local.LocalOnlyRows = []LocalOnlyRow{{Key: LocalOnlyRowKey{Table: tableA, Row: "local-only"}}}
	local.Provenance = []LocalProvenance{{Row: local.Rows[0].Identity, Scopes: []ScopeID{scopeB}}}
	state.ClientLocal[client] = local
	clientState := state.Clients[client]
	clientState.Checkpoints = []ClientCheckpoint{{Scope: scopeB, Position: StreamPosition{StreamGeneration: generation, Kind: PositionKindGenerationStart}}}
	state.Clients[client] = clientState

	model, _ := newPullRebuildTestModel(t, state)
	begin := applyTestOperation(t, model, "local", "begin-rebuild", localBeginRebuildPayload{
		UserID:           string(client.UserID),
		ClientID:         string(client.ClientID),
		ClientGeneration: 1,
		Schema:           testSchemaPayload(schema),
		ScopeID:          string(scopeA),
		RebuildID:        string(rebuildA),
		Limit:            1,
	})
	if begin.Kind != StepResultKindLocal || begin.Local == nil {
		t.Fatalf("local begin result = %#v", begin)
	}
	if len(model.state.ClientLocal[client].LocalOnlyRows) != 1 || len(model.state.ClientLocal[client].Rows) != 1 {
		t.Fatal("scope A begin changed unrelated scope or local-only data")
	}
	checkpointB, _ := testServerCheckpoint(model.state.Clients[client], scopeB)

	firstPage := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceNone, 1))
	if firstPage.HTTP.Status != 200 || firstPage.Rebuild == nil || !firstPage.Rebuild.HasContinuation || firstPage.Rebuild.HasFinalCursor || len(firstPage.Rebuild.Records) != 1 {
		t.Fatalf("first rebuild page = %#v", firstPage)
	}
	firstReplayBefore := model.Snapshot()
	firstReplay := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceNone, 1))
	if !firstReplay.Rebuild.Replayed || !reflect.DeepEqual(firstReplay.Rebuild.Records, firstPage.Rebuild.Records) {
		t.Fatalf("first replay = %#v, want exact stored page", firstReplay)
	}
	if after := model.Snapshot(); !reflect.DeepEqual(after, firstReplayBefore) {
		t.Fatal("first page replay changed the immutable session")
	}

	live := testAuthoritativeRow(second, "live-after-boundary", Checksum{0: 88})
	model.state.Rows[second] = live
	applyTestOperation(t, model, "local", "apply-rebuild-page", localApplyRebuildPagePayload{
		UserID:             string(client.UserID),
		ClientID:           string(client.ClientID),
		ScopeID:            string(scopeA),
		RebuildID:          string(rebuildA),
		PageOrdinal:        1,
		RequestTokenSource: tokenSourceNone,
	})
	secondPage := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceLocalRebuildContinuation, 1))
	if secondPage.HTTP.Status != 200 || secondPage.Rebuild == nil || len(secondPage.Rebuild.Records) != 1 || secondPage.Rebuild.Records[0].Version != "snapshot-b" {
		t.Fatalf("second rebuild page = %#v, want snapshot row", secondPage)
	}
	secondReplay := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceLocalRebuildContinuation, 1))
	if !secondReplay.Rebuild.Replayed || !reflect.DeepEqual(secondReplay.Rebuild.Records, secondPage.Rebuild.Records) {
		t.Fatalf("intermediate replay = %#v, want exact page", secondReplay)
	}

	applyTestOperation(t, model, "local", "apply-rebuild-page", localApplyRebuildPagePayload{
		UserID:             string(client.UserID),
		ClientID:           string(client.ClientID),
		ScopeID:            string(scopeA),
		RebuildID:          string(rebuildA),
		PageOrdinal:        2,
		RequestTokenSource: tokenSourceLocalRebuildContinuation,
	})
	postBoundary := testEffectPosition(generation, 11, 1, 1)
	stateAfterBoundary := model.state.Scopes[scopeA]
	stateAfterBoundary.Membership = append(stateAfterBoundary.Membership, ScopeMembership{Row: fourth, Included: true})
	model.state.Rows[fourth] = testAuthoritativeRow(fourth, "post-boundary", Checksum{0: 14})
	addTestPullEffectToScope(&stateAfterBoundary, scopeA, postBoundary, fourth, "post-boundary", Checksum{0: 14}, model.state.Projections)
	model.state.Scopes[scopeA] = stateAfterBoundary
	model.state.Stream.Authority.GlobalMaterializationBoundary = StreamPosition{StreamGeneration: generation, Kind: PositionKindTransactionEnd, CommitLSN: 11}

	finalPage := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceLocalRebuildContinuation, 1))
	if finalPage.HTTP.Status != 200 || finalPage.Rebuild == nil || !finalPage.Rebuild.HasFinalCursor || !finalPage.Rebuild.HasChecksum || finalPage.Rebuild.HasContinuation || len(finalPage.Rebuild.Records) != 1 || finalPage.Rebuild.Records[0].Version != "snapshot-c" {
		t.Fatalf("final rebuild page = %#v", finalPage)
	}
	finalReplay := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceLocalRebuildContinuation, 1))
	wantFinalReplay := *finalPage.Rebuild
	wantFinalReplay.Replayed = true
	if !reflect.DeepEqual(finalReplay.Rebuild, &wantFinalReplay) {
		t.Fatalf("final replay = %#v, want exact stored final page", finalReplay)
	}

	applyTestOperation(t, model, "local", "apply-rebuild-page", localApplyRebuildPagePayload{
		UserID:             string(client.UserID),
		ClientID:           string(client.ClientID),
		ScopeID:            string(scopeA),
		RebuildID:          string(rebuildA),
		PageOrdinal:        3,
		RequestTokenSource: tokenSourceLocalRebuildContinuation,
	})
	applyTestOperation(t, model, "local", "finalize-rebuild", localFinalizeRebuildPayload{
		UserID:    string(client.UserID),
		ClientID:  string(client.ClientID),
		ScopeID:   string(scopeA),
		RebuildID: string(rebuildA),
	})
	checkpointAfterB, _ := testServerCheckpoint(model.state.Clients[client], scopeB)
	if checkpointAfterB != checkpointB {
		t.Fatal("rebuild changed an unrelated server checkpoint")
	}

	pull := applyTestOperation(t, model, "pull", "request-page", testPullRequest(client, schema, []pullScopePayload{{ScopeID: string(scopeA), CursorSource: tokenSourceLocalCheckpoint}}, 1))
	if pull.HTTP.Status != 200 || pull.Pull == nil || len(pull.Pull.Changes) != 1 || pull.Pull.Changes[0].Row != fourth {
		t.Fatalf("post-boundary pull = %#v, want isolated incremental effect", pull)
	}
}

func TestRebuildExpiryEpochAndForgeryDispatch(t *testing.T) {
	newSession := func(t *testing.T) (*Model, *pullRebuildTestClock, ClientKey, SchemaRef, StreamGeneration) {
		t.Helper()
		state, client, schema, generation := newPullRebuildTestState(t, []ScopeID{scopeA})
		row := canonicalStringRowIdentity(tableA, fieldA, "session")
		state.Rows[row] = testAuthoritativeRow(row, "session-row", Checksum{0: 31})
		scopeState := state.Scopes[scopeA]
		scopeState.Membership = []ScopeMembership{{Row: row, Included: true}}
		state.Scopes[scopeA] = scopeState
		model, clock := newPullRebuildTestModel(t, state)
		first := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceNone, 1))
		if first.HTTP.Status != 200 || first.Rebuild == nil {
			t.Fatalf("first rebuild result = %#v", first)
		}
		return model, clock, client, schema, generation
	}

	t.Run("fixed twenty-four-hour expiry", func(t *testing.T) {
		model, clock, client, schema, _ := newSession(t)
		clock.now = clock.now.Add(24 * time.Hour)
		result := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceNone, 1))
		if result.HTTP.Status != 409 || result.HTTP.Code != "rebuild_restart_required" || result.Rebuild == nil || len(result.Rebuild.Records) != 0 {
			t.Fatalf("expired rebuild result = %#v", result)
		}
	})

	t.Run("accepted-write epoch invalidates stored pages", func(t *testing.T) {
		model, _, client, schema, _ := newSession(t)
		clientState := model.state.Clients[client]
		clientState.AcceptedWriteEpoch++
		model.state.Clients[client] = clientState
		result := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceNone, 1))
		if result.HTTP.Status != 409 || result.HTTP.Code != "rebuild_restart_required" || result.Rebuild == nil || len(result.Rebuild.Records) != 0 {
			t.Fatalf("epoch-invalidated rebuild result = %#v", result)
		}
	})

	t.Run("forged continuation rejects the whole request", func(t *testing.T) {
		model, _, client, schema, _ := newSession(t)
		before := model.Snapshot()
		result := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceForged, 1))
		if result.HTTP.Status != 400 || result.HTTP.Code != "invalid_request" || result.Rebuild == nil || len(result.Rebuild.Records) != 0 {
			t.Fatalf("forged continuation result = %#v", result)
		}
		if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatal("forged continuation changed session state")
		}
	})
}

func TestLocalRebuildFinalityProtectsDataAndCursorInstallation(t *testing.T) {
	t.Run("checksum failure preserves pending finality and installs no cursor", func(t *testing.T) {
		model, client, _ := prepareFinalLocalRebuild(t, false)
		applyTestOperation(t, model, "local", "apply-rebuild-page", localApplyRebuildPagePayload{
			UserID:             string(client.UserID),
			ClientID:           string(client.ClientID),
			ScopeID:            string(scopeA),
			RebuildID:          string(rebuildA),
			PageOrdinal:        1,
			RequestTokenSource: tokenSourceNone,
		})
		local := model.state.ClientLocal[client]
		attemptIndex := localRebuildAttemptIndex(local.RebuildAttempts, scopeA, rebuildA)
		local.RebuildAttempts[attemptIndex].PendingFinalResult.ScopeChecksum[0]++
		model.state.ClientLocal[client] = local
		before := model.Snapshot()
		applyTestOperationError(t, model, "local", "finalize-rebuild", localFinalizeRebuildPayload{
			UserID:    string(client.UserID),
			ClientID:  string(client.ClientID),
			ScopeID:   string(scopeA),
			RebuildID: string(rebuildA),
		})
		if after := model.Snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatal("checksum failure changed local data or installed a cursor")
		}
		local = model.state.ClientLocal[client]
		checkpoint, found := testLocalCheckpoint(local, scopeA)
		if !found || checkpoint.HasCursor {
			t.Fatal("checksum failure installed a local final cursor")
		}
	})

	t.Run("scope-only pruning preserves overlapping provenance and pending intent", func(t *testing.T) {
		state, client, schema, _ := newPullRebuildTestState(t, []ScopeID{scopeA, scopeB})
		overlap := canonicalStringRowIdentity(tableA, fieldA, "overlap")
		pending := canonicalStringRowIdentity(tableA, fieldA, "pending")
		local := state.ClientLocal[client]
		local.Rows = []LocalRow{{Identity: overlap}, {Identity: pending}}
		local.Provenance = []LocalProvenance{
			{Row: overlap, Scopes: []ScopeID{scopeA, scopeB}},
			{Row: pending, Scopes: []ScopeID{scopeA}},
		}
		local.DurableQueue = []QueuedMutation{{Mutation: mutationA, Row: pending, Status: LocalMutationStatusPending}}
		state.ClientLocal[client] = local
		model, _ := newPullRebuildTestModel(t, state)
		applyTestOperation(t, model, "local", "begin-rebuild", localBeginRebuildPayload{
			UserID:           string(client.UserID),
			ClientID:         string(client.ClientID),
			ClientGeneration: 1,
			Schema:           testSchemaPayload(schema),
			ScopeID:          string(scopeA),
			RebuildID:        string(rebuildA),
			Limit:            1,
		})
		local = model.state.ClientLocal[client]
		if !containsLocalRow(local.Rows, overlap) || !containsLocalRow(local.Rows, pending) || len(local.LocalOnlyRows) != 0 {
			t.Fatal("begin rebuild removed overlapping or pending-intent data")
		}
		if scopes := localProvenanceScopes(local, overlap); !reflect.DeepEqual(scopes, []ScopeID{scopeB}) {
			t.Fatalf("overlap provenance = %#v, want only scope B", scopes)
		}

		final := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceNone, 1))
		if final.HTTP.Status != 200 || final.Rebuild == nil || !final.Rebuild.HasFinalCursor {
			t.Fatalf("empty rebuild final page = %#v", final)
		}
		applyTestOperation(t, model, "local", "apply-rebuild-page", localApplyRebuildPagePayload{
			UserID:             string(client.UserID),
			ClientID:           string(client.ClientID),
			ScopeID:            string(scopeA),
			RebuildID:          string(rebuildA),
			PageOrdinal:        1,
			RequestTokenSource: tokenSourceNone,
		})
		applyTestOperation(t, model, "local", "finalize-rebuild", localFinalizeRebuildPayload{
			UserID:    string(client.UserID),
			ClientID:  string(client.ClientID),
			ScopeID:   string(scopeA),
			RebuildID: string(rebuildA),
		})
		local = model.state.ClientLocal[client]
		if !containsLocalRow(local.Rows, overlap) || !containsLocalRow(local.Rows, pending) {
			t.Fatal("final pruning removed overlapping or pending-intent local data")
		}
		if scopes := localProvenanceScopes(local, overlap); !reflect.DeepEqual(scopes, []ScopeID{scopeB}) {
			t.Fatalf("final overlap provenance = %#v, want scope B", scopes)
		}
	})

	t.Run("process-death boundary has no false final cursor", func(t *testing.T) {
		model, client, _ := prepareFinalLocalRebuild(t, true)
		applyTestOperation(t, model, "local", "apply-rebuild-page", localApplyRebuildPagePayload{
			UserID:             string(client.UserID),
			ClientID:           string(client.ClientID),
			ScopeID:            string(scopeA),
			RebuildID:          string(rebuildA),
			PageOrdinal:        1,
			RequestTokenSource: tokenSourceNone,
		})
		local := model.state.ClientLocal[client]
		attemptIndex := localRebuildAttemptIndex(local.RebuildAttempts, scopeA, rebuildA)
		if attemptIndex < 0 || local.RebuildAttempts[attemptIndex].Phase != LocalRebuildAttemptPhasePendingFinality {
			t.Fatal("final page apply did not persist pending finality")
		}
		checkpoint, found := testLocalCheckpoint(local, scopeA)
		if !found || checkpoint.HasCursor || checkpoint.HasChecksum || checkpoint.Verified {
			t.Fatal("final page apply installed local progress before checksum finality")
		}
	})
}

func newPullRebuildTestState(t *testing.T, scopes []ScopeID) (State, ClientKey, SchemaRef, StreamGeneration) {
	t.Helper()
	schema := schemaRef(1, 1)
	client := ClientKey{UserID: userA, ClientID: clientAID}
	generation := StreamGeneration("reference-test-stream")
	boundary := StreamPosition{StreamGeneration: generation, Kind: PositionKindTransactionEnd, CommitLSN: 10}
	state := State{
		ProtocolVersion:  3,
		ConfiguredLimits: ConfiguredLimits{PullMaximum: 1000, RebuildMaximum: 1000},
		Schemas: map[SchemaRef]SchemaManifest{
			schema: {Class: SchemaClassInitial, CompatibilityFloor: 1},
		},
		CurrentSchema: schema,
		Clients: map[ClientKey]ClientState{
			client: {
				CurrentGeneration:  1,
				Generations:        []ClientGenerationState{{Generation: 1}},
				ScopeSetVersion:    1,
				AcceptedWriteEpoch: 1,
			},
		},
		Rows:        make(map[RowIdentity]AuthoritativeRow),
		Scopes:      make(map[ScopeID]ScopeState),
		Fences:      make(map[FenceID]VersionFence),
		Projections: make(map[ProjectionKey]CapturedProjection),
		Rebuilds:    make(map[RebuildKey]RebuildSession),
		ClientLocal: make(map[ClientKey]ClientLocalState),
		Stream: StreamState{Authority: StreamAuthority{
			ActiveGeneration:              generation,
			GlobalMaterializationBoundary: boundary,
		}},
	}
	assignments := make([]ScopeAssignment, 0, len(scopes))
	localAssignments := make([]LocalScopeAssignment, 0, len(scopes))
	localCheckpoints := make([]LocalScopeCheckpoint, 0, len(scopes))
	for index, scope := range scopes {
		checksum := Checksum{0: byte(index + 1)}
		state.Scopes[scope] = ScopeState{
			Schema:               schema,
			MembershipGeneration: 1,
			RetentionGeneration:  1,
			StreamGeneration:     generation,
			Checksum:             checksum,
			HighWatermark:        boundary,
		}
		assignments = append(assignments, ScopeAssignment{
			Scope:                scope,
			MembershipGeneration: 1,
			RetentionGeneration:  1,
			Assigned:             true,
		})
		localAssignments = append(localAssignments, LocalScopeAssignment{
			Scope:                scope,
			MembershipGeneration: 1,
			RetentionGeneration:  1,
			Assigned:             true,
		})
		localCheckpoints = append(localCheckpoints, LocalScopeCheckpoint{Scope: scope})
	}
	clientState := state.Clients[client]
	clientState.ScopeAssignments = assignments
	state.Clients[client] = clientState
	state.ClientLocal[client] = ClientLocalState{
		ClientGeneration: 1,
		CurrentSchema:    schema,
		ScopeAssignments: localAssignments,
		ScopeCheckpoints: localCheckpoints,
		Lifecycle:        ClientLifecycleState{State: ClientLifecycleRebuilding},
	}
	return state, client, schema, generation
}

func newPullRebuildTestModel(t *testing.T, state State) (*Model, *pullRebuildTestClock) {
	t.Helper()
	clock := &pullRebuildTestClock{now: time.Date(2032, time.January, 2, 3, 4, 5, 0, time.UTC)}
	model, err := New(Config{State: state, Clock: clock, Seed: 600})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return model, clock
}

func installTestIncrementalCursor(t *testing.T, model *Model, client ClientKey, scope ScopeID, position StreamPosition) OpaqueToken {
	t.Helper()
	clientState := model.state.Clients[client]
	assignment, found := scopeAssignmentFor(clientState, scope)
	if !found {
		t.Fatalf("scope %q is not assigned", scope)
	}
	scopeState := model.state.Scopes[scope]
	token := model.authority.Mint(string(TokenKindIncrementalCursor), incrementalCursorBindings(model.state, client, assignment, scopeState, scope, position, referenceNow(model.clock)))
	if token == (OpaqueToken{}) {
		t.Fatal("test cursor mint failed")
	}
	clientState.Checkpoints = append(clientState.Checkpoints, ClientCheckpoint{Scope: scope, Position: position, HasCursor: true, Cursor: token})
	model.state.Clients[client] = clientState
	local := model.state.ClientLocal[client]
	index := localScopeCheckpointIndex(local.ScopeCheckpoints, scope)
	if index < 0 {
		local.ScopeCheckpoints = append(local.ScopeCheckpoints, LocalScopeCheckpoint{Scope: scope, Position: position, HasCursor: true, Cursor: token})
	} else {
		local.ScopeCheckpoints[index] = LocalScopeCheckpoint{Scope: scope, Position: position, HasCursor: true, Cursor: token}
	}
	model.state.ClientLocal[client] = local
	return token
}

func copyIssuedCursorToLocal(model *Model, client ClientKey, scope ScopeID) {
	server := model.state.Clients[client]
	checkpoint, found := testServerCheckpoint(server, scope)
	if !found || !checkpoint.HasCursor {
		return
	}
	local := model.state.ClientLocal[client]
	index := localScopeCheckpointIndex(local.ScopeCheckpoints, scope)
	if index < 0 {
		local.ScopeCheckpoints = append(local.ScopeCheckpoints, LocalScopeCheckpoint{Scope: scope, HasCursor: true, Cursor: checkpoint.Cursor})
	} else {
		local.ScopeCheckpoints[index].HasCursor = true
		local.ScopeCheckpoints[index].Cursor = checkpoint.Cursor
	}
	model.state.ClientLocal[client] = local
}

func testAuthoritativeRow(identity RowIdentity, version RowVersion, checksum Checksum) AuthoritativeRow {
	return AuthoritativeRow{
		Identity:    identity,
		FieldValues: []FieldValue{{Field: fieldA, Type: "string", WireJSON: identity.CanonicalWireJSON}},
		Version:     version,
		Checksum:    checksum,
	}
}

func testEffectPosition(generation StreamGeneration, lsn CommitLSN, event, effect EffectOrdinal) StreamPosition {
	return StreamPosition{StreamGeneration: generation, Kind: PositionKindEffect, CommitLSN: lsn, EventOrdinal: EventOrdinal(event), EffectOrdinal: effect}
}

func addTestPullEffect(state *State, scope ScopeID, position StreamPosition, row RowIdentity, version RowVersion, checksum Checksum) {
	scopeState := state.Scopes[scope]
	addTestPullEffectToScope(&scopeState, scope, position, row, version, checksum, state.Projections)
	state.Scopes[scope] = scopeState
}

func addTestPullEffectToScope(scopeState *ScopeState, scope ScopeID, position StreamPosition, row RowIdentity, version RowVersion, checksum Checksum, projections map[ProjectionKey]CapturedProjection) {
	event := EventReplayKey{Transaction: TransactionReplayKey{StreamGeneration: position.StreamGeneration, CommitLSN: position.CommitLSN}, EventOrdinal: position.EventOrdinal}
	key := ProjectionKey{Relation: relationA, Event: event, Image: ProjectionImageAfter}
	projections[key] = CapturedProjection{
		Event:    event,
		Image:    ProjectionImageAfter,
		Row:      row,
		Fields:   []FieldValue{{Field: fieldA, Type: "string", WireJSON: row.CanonicalWireJSON}},
		Version:  version,
		Checksum: checksum,
	}
	scopeState.Effects = append(scopeState.Effects, ScopeEffect{
		Position:              position,
		Row:                   row,
		SourceEvent:           event,
		Operation:             EffectOperationUpsert,
		Version:               version,
		HasCapturedProjection: true,
		CapturedProjection:    key,
		HasChecksum:           true,
		Checksum:              checksum,
	})
	_ = scope
}

func testSchemaPayload(schema SchemaRef) *schemaReferencePayload {
	version := schema.Version
	hash := hex.EncodeToString(schema.Hash[:])
	return &schemaReferencePayload{Version: &version, Hash: &hash}
}

func testPullRequest(client ClientKey, schema SchemaRef, scopes []pullScopePayload, limit uint32) pullRequestPayload {
	return pullRequestPayload{
		UserID:           string(client.UserID),
		ClientID:         string(client.ClientID),
		ClientGeneration: 1,
		Schema:           testSchemaPayload(schema),
		ScopeSetVersion:  1,
		Scopes:           scopes,
		Limit:            limit,
	}
}

func testRebuildRequest(client ClientKey, schema SchemaRef, scope ScopeID, rebuild RebuildID, source tokenSource, limit uint32) rebuildRequestPayload {
	return rebuildRequestPayload{
		UserID:           string(client.UserID),
		ClientID:         string(client.ClientID),
		ClientGeneration: 1,
		Schema:           testSchemaPayload(schema),
		ScopeID:          string(scope),
		RebuildID:        string(rebuild),
		CursorSource:     source,
		Limit:            limit,
	}
}

func applyTestOperation(t *testing.T, model *Model, contractOperation, name string, payload any) StepResult {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	result, err := model.Apply(context.Background(), scenarios.Operation{
		ContractOperation: contractOperation,
		Name:              name,
		Payload:           encoded,
	})
	if err != nil {
		t.Fatalf("Apply %s/%s returned error: %v", contractOperation, name, err)
	}
	return result
}

func applyTestOperationError(t *testing.T, model *Model, contractOperation, name string, payload any) {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if _, err := model.Apply(context.Background(), scenarios.Operation{ContractOperation: contractOperation, Name: name, Payload: encoded}); err == nil {
		t.Fatalf("Apply %s/%s accepted an invalid local operation", contractOperation, name)
	}
}

func testServerCheckpoint(client ClientState, scope ScopeID) (ClientCheckpoint, bool) {
	for _, checkpoint := range client.Checkpoints {
		if checkpoint.Scope == scope {
			return checkpoint, true
		}
	}
	return ClientCheckpoint{}, false
}

func testLocalCheckpoint(local ClientLocalState, scope ScopeID) (LocalScopeCheckpoint, bool) {
	for _, checkpoint := range local.ScopeCheckpoints {
		if checkpoint.Scope == scope {
			return checkpoint, true
		}
	}
	return LocalScopeCheckpoint{}, false
}

func containsScope(scopes []ScopeID, target ScopeID) bool {
	for _, scope := range scopes {
		if scope == target {
			return true
		}
	}
	return false
}

func containsLocalRow(rows []LocalRow, target RowIdentity) bool {
	for _, row := range rows {
		if row.Identity == target {
			return true
		}
	}
	return false
}

func localProvenanceScopes(local ClientLocalState, target RowIdentity) []ScopeID {
	for _, provenance := range local.Provenance {
		if provenance.Row == target {
			return provenance.Scopes
		}
	}
	return nil
}

func prepareFinalLocalRebuild(t *testing.T, keepChecksum bool) (*Model, ClientKey, SchemaRef) {
	t.Helper()
	state, client, schema, _ := newPullRebuildTestState(t, []ScopeID{scopeA})
	row := canonicalStringRowIdentity(tableA, fieldA, "local-final")
	state.Rows[row] = testAuthoritativeRow(row, "local-final-row", Checksum{0: 41})
	scopeState := state.Scopes[scopeA]
	scopeState.Membership = []ScopeMembership{{Row: row, Included: true}}
	scopeState.Cardinality = 1
	state.Scopes[scopeA] = scopeState
	model, _ := newPullRebuildTestModel(t, state)
	applyTestOperation(t, model, "local", "begin-rebuild", localBeginRebuildPayload{
		UserID:           string(client.UserID),
		ClientID:         string(client.ClientID),
		ClientGeneration: 1,
		Schema:           testSchemaPayload(schema),
		ScopeID:          string(scopeA),
		RebuildID:        string(rebuildA),
		Limit:            1,
	})
	result := applyTestOperation(t, model, "rebuild", "request-page", testRebuildRequest(client, schema, scopeA, rebuildA, tokenSourceNone, 1))
	if result.HTTP.Status != 200 || result.Rebuild == nil || !result.Rebuild.HasFinalCursor || !result.Rebuild.HasChecksum {
		t.Fatalf("prepare final rebuild result = %#v", result)
	}
	if !keepChecksum {
		return model, client, schema
	}
	return model, client, schema
}
