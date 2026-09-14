package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

const phase4ClientVersion = "2032-01-02T03:04:05.000000Z"

const (
	s11TransportBodyLimit = int64(1 << 20)
	s11TransportTimeout   = 30 * time.Second
)

func TestRealS11PushResponseLossReplaysExactCanonicalResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "s11-response-loss-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")

	recordID := "00000000-0000-4000-8b01-000000000001"
	mutationID := "00000000-0000-4000-8b01-000000000002"
	batchID := "00000000-0000-4000-8b01-000000000003"
	payload := phase4PushPayload(client, batchID, []map[string]any{
		phase4InsertMutation(client, table, ownerField, mutationID, recordID, "s11-response-loss"),
	})
	requestBody, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode S-11 push request: %v", err)
	}
	rawClient := &blackbox.Client{
		BaseURL: harness.AdapterURL(),
		HTTP:    &http.Client{Timeout: s11TransportTimeout},
		Tokens: blackbox.TokenProviderFunc(func(context.Context) (string, error) {
			return token, nil
		}),
	}
	request := blackbox.Request{
		Method: http.MethodPost,
		Path:   "/sync/push",
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:  requestBody,
		Class: "phase4/push-response-loss",
	}
	responseLossProxy := startS11ResponseLossProxy(
		t,
		harness.AdapterURL(),
		token,
		requestBody,
		batchID,
		mutationID,
	)
	lostClient := &blackbox.Client{
		BaseURL: responseLossProxy.URL(),
		HTTP:    &http.Client{Timeout: s11TransportTimeout},
		Tokens: blackbox.TokenProviderFunc(func(context.Context) (string, error) {
			return token, nil
		}),
	}

	lostContext, lostCancel := context.WithTimeout(ctx, s11TransportTimeout)
	lost, err := lostClient.Do(lostContext, request)
	lostCancel()
	if err == nil {
		t.Fatal("S-11 response-loss request returned an HTTP response")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("S-11 response-loss request timed out instead of losing its response")
	}
	if lost.Status != 0 || len(lost.Body) != 0 || len(lost.CanonicalBody) != 0 || len(lost.Headers) != 0 {
		t.Fatal("S-11 response-loss request returned a fabricated response")
	}
	lostResponse, err := responseLossProxy.Response()
	if err != nil {
		t.Fatalf("validate S-11 hidden adapter response: %v", err)
	}
	hiddenResponse := decodeRealResponseObject(t, lostResponse.Body)
	hiddenAccepted := requireOutcomeList(t, hiddenResponse, "accepted")
	hiddenRejected := requireOutcomeList(t, hiddenResponse, "rejected")
	if hiddenResponse["batch_id"] != batchID || len(hiddenAccepted) != 1 || len(hiddenRejected) != 0 {
		t.Fatal("S-11 hidden adapter response partition is invalid")
	}
	assertCanonicalPhase4Outcome(t, hiddenAccepted[0], client, table, ownerField, mutationID, recordID, "s11-response-loss", "applied", "")
	hiddenVersion, _ := hiddenAccepted[0]["server_version"].(string)
	committed, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{recordID})
	if err != nil {
		t.Fatalf("observe S-11 hidden durable push state: %v", err)
	}
	if committed.BatchCount != 1 || committed.MutationCount != 1 ||
		committed.SourceRowCount != 1 || committed.AcceptedWriteEpoch != 2 {
		t.Fatal("S-11 hidden push did not commit exactly once")
	}
	stateAfterCommit, err := harness.Operator().ObserveItemStateMatch(ctx, recordID, "s11-response-loss", hiddenVersion)
	if err != nil {
		t.Fatalf("observe S-11 hidden source state: %v", err)
	}
	if !stateAfterCommit.Live || !stateAfterCommit.ValueMatches || !stateAfterCommit.VersionMatches {
		t.Fatal("S-11 hidden push did not commit the expected source state")
	}

	firstReplay, err := rawClient.Do(ctx, request)
	if err != nil {
		t.Fatalf("execute first S-11 replay: %v", err)
	}
	secondReplay, err := rawClient.Do(ctx, request)
	if err != nil {
		t.Fatalf("execute second S-11 replay: %v", err)
	}
	if firstReplay.Status != http.StatusOK || secondReplay.Status != http.StatusOK {
		t.Fatalf("S-11 replay statuses = %d and %d, want 200", firstReplay.Status, secondReplay.Status)
	}
	if err := blackbox.CompareExactReplay(lostResponse, firstReplay); err != nil {
		t.Fatalf("compare S-11 hidden response to replay: %v", err)
	}
	if err := blackbox.CompareExactReplay(firstReplay, secondReplay); err != nil {
		t.Fatalf("compare S-11 exact replays: %v", err)
	}
	if !bytes.Equal(lostResponse.Body, firstReplay.Body) ||
		!bytes.Equal(firstReplay.Body, secondReplay.Body) ||
		!bytes.Equal(lostResponse.Body, lostResponse.CanonicalBody) ||
		!bytes.Equal(firstReplay.Body, firstReplay.CanonicalBody) ||
		!bytes.Equal(secondReplay.Body, secondReplay.CanonicalBody) {
		t.Fatal("S-11 replay did not return exact canonical response bytes")
	}

	response := decodeRealResponseObject(t, firstReplay.Body)
	accepted := requireOutcomeList(t, response, "accepted")
	rejected := requireOutcomeList(t, response, "rejected")
	if response["batch_id"] != batchID || len(accepted) != 1 || len(rejected) != 0 {
		t.Fatal("S-11 replay response partition is invalid")
	}
	assertCanonicalPhase4Outcome(t, accepted[0], client, table, ownerField, mutationID, recordID, "s11-response-loss", "applied", "")
	version, _ := accepted[0]["server_version"].(string)
	if version != hiddenVersion {
		t.Fatal("S-11 replay changed the committed server version")
	}
	changedPayload := phase4PushPayload(client, batchID, []map[string]any{
		phase4InsertMutation(client, table, ownerField, mutationID, recordID, "s11-response-loss-changed"),
	})
	changedBody, err := json.Marshal(changedPayload)
	if err != nil {
		t.Fatalf("encode S-11 changed-fingerprint request: %v", err)
	}
	changedRequest := request
	changedRequest.Body = changedBody
	changed, err := rawClient.Do(ctx, changedRequest)
	if err != nil {
		t.Fatalf("submit S-11 changed-fingerprint request: %v", err)
	}
	if changed.Status != http.StatusConflict {
		t.Fatalf("S-11 changed-fingerprint status = %d, want 409", changed.Status)
	}
	changedResponse := decodeRealResponseObject(t, changed.Body)
	changedError, ok := changedResponse["error"].(map[string]any)
	if !ok || changedError["code"] != "idempotency_conflict" || changedError["retryable"] != false {
		t.Fatal("S-11 changed-fingerprint request did not return idempotency_conflict")
	}
	if _, accepted := changedResponse["accepted"]; accepted {
		t.Fatal("S-11 changed-fingerprint response contains accepted outcomes")
	}
	if _, rejected := changedResponse["rejected"]; rejected {
		t.Fatal("S-11 changed-fingerprint response contains rejected outcomes")
	}
	state, err := harness.Operator().ObserveItemStateMatch(ctx, recordID, "s11-response-loss", version)
	if err != nil {
		t.Fatalf("observe S-11 source state: %v", err)
	}
	if !state.Live || !state.ValueMatches || !state.VersionMatches {
		t.Fatal("S-11 replay or changed-fingerprint request changed source state")
	}
	observation, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{recordID})
	if err != nil {
		t.Fatalf("observe S-11 durable push state: %v", err)
	}
	if observation.BatchCount != 1 || observation.MutationCount != 1 ||
		observation.SourceRowCount != 1 || observation.AcceptedWriteEpoch != 2 {
		t.Fatal("S-11 replay or changed-fingerprint request repeated source work")
	}
}

type s11ResponseLossProxy struct {
	adapterURL  string
	token       string
	requestBody []byte
	batchID     string
	mutationID  string
	listener    net.Listener
	server      *http.Server

	mu       sync.Mutex
	requests int
	response blackbox.Response
	err      error
}

func startS11ResponseLossProxy(
	t *testing.T,
	adapterURL, token string,
	requestBody []byte,
	batchID, mutationID string,
) *s11ResponseLossProxy {
	t.Helper()
	if len(requestBody) == 0 || int64(len(requestBody)) > s11TransportBodyLimit {
		t.Fatal("S-11 response-loss request body is outside the test bound")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for S-11 response-loss proxy: %v", err)
	}
	proxy := &s11ResponseLossProxy{
		adapterURL:  adapterURL,
		token:       token,
		requestBody: append([]byte(nil), requestBody...),
		batchID:     batchID,
		mutationID:  mutationID,
		listener:    listener,
	}
	proxy.server = &http.Server{
		Handler:           http.HandlerFunc(proxy.serveHTTP),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       s11TransportTimeout,
		WriteTimeout:      s11TransportTimeout,
		IdleTimeout:       5 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	go func() {
		if err := proxy.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			proxy.setError(errors.New("S-11 response-loss proxy stopped unexpectedly"))
		}
	}()
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := proxy.server.Shutdown(closeContext); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("close S-11 response-loss proxy: %v", err)
		}
	})
	return proxy
}

func (proxy *s11ResponseLossProxy) URL() string {
	return "http://" + proxy.listener.Addr().String()
}

func (proxy *s11ResponseLossProxy) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	defer proxy.closeWithoutResponse(writer)
	proxy.mu.Lock()
	proxy.requests++
	if proxy.requests != 1 {
		proxy.mu.Unlock()
		proxy.setError(errors.New("S-11 response-loss proxy received more than one request"))
		return
	}
	proxy.mu.Unlock()

	response, err := proxy.forwardAndValidate(request)
	if err != nil {
		proxy.setError(err)
		return
	}
	proxy.mu.Lock()
	proxy.response = response
	proxy.mu.Unlock()
}

func (proxy *s11ResponseLossProxy) forwardAndValidate(request *http.Request) (blackbox.Response, error) {
	if request.Method != http.MethodPost || request.URL.RequestURI() != "/sync/push" ||
		request.Header.Get("Authorization") != "Bearer "+proxy.token ||
		request.Header.Get("Content-Type") != "application/json" ||
		request.ContentLength != int64(len(proxy.requestBody)) {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy received an invalid authenticated push")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, s11TransportBodyLimit+1))
	if err != nil {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy could not read the push")
	}
	if int64(len(body)) > s11TransportBodyLimit || !bytes.Equal(body, proxy.requestBody) {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy received a changed push")
	}
	forwardContext, cancel := context.WithTimeout(request.Context(), s11TransportTimeout)
	defer cancel()
	forwarded, err := http.NewRequestWithContext(
		forwardContext,
		request.Method,
		proxy.adapterURL+request.URL.RequestURI(),
		bytes.NewReader(body),
	)
	if err != nil {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy could not create the adapter request")
	}
	forwarded.Header = request.Header.Clone()
	response, err := (&http.Client{Timeout: s11TransportTimeout}).Do(forwarded)
	if err != nil {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy could not forward the push")
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, s11TransportBodyLimit+1))
	if err != nil {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy could not read the adapter response")
	}
	if int64(len(responseBody)) > s11TransportBodyLimit || response.StatusCode != http.StatusOK {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy received an invalid adapter response")
	}
	canonical, err := blackbox.CanonicalResponseBytes(responseBody)
	if err != nil || !bytes.Equal(canonical, responseBody) {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy received a noncanonical adapter response")
	}
	var envelope struct {
		BatchID  string `json:"batch_id"`
		Accepted []struct {
			MutationID string `json:"mutation_id"`
			Status     string `json:"status"`
		} `json:"accepted"`
		Rejected []json.RawMessage `json:"rejected"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err != nil || envelope.BatchID != proxy.batchID ||
		len(envelope.Accepted) != 1 || envelope.Accepted[0].MutationID != proxy.mutationID ||
		envelope.Accepted[0].Status != "applied" || len(envelope.Rejected) != 0 {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy could not validate the adapter response")
	}
	return blackbox.Response{
		Status:        response.StatusCode,
		Headers:       response.Header.Clone(),
		Body:          append([]byte(nil), responseBody...),
		CanonicalBody: append([]byte(nil), canonical...),
	}, nil
}

func (proxy *s11ResponseLossProxy) closeWithoutResponse(writer http.ResponseWriter) {
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		proxy.setError(errors.New("S-11 response-loss proxy cannot close the downstream connection"))
		return
	}
	connection, _, err := hijacker.Hijack()
	if err != nil {
		proxy.setError(errors.New("S-11 response-loss proxy could not close the downstream connection"))
		return
	}
	_ = connection.Close()
}

func (proxy *s11ResponseLossProxy) setError(err error) {
	if err == nil {
		return
	}
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if proxy.err == nil {
		proxy.err = err
	}
}

func (proxy *s11ResponseLossProxy) Response() (blackbox.Response, error) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if proxy.err != nil {
		return blackbox.Response{}, proxy.err
	}
	if proxy.requests != 1 || proxy.response.Status == 0 {
		return blackbox.Response{}, errors.New("S-11 response-loss proxy did not validate one adapter response")
	}
	return blackbox.Response{
		Status:        proxy.response.Status,
		Headers:       proxy.response.Headers.Clone(),
		Body:          append([]byte(nil), proxy.response.Body...),
		CanonicalBody: append([]byte(nil), proxy.response.CanonicalBody...),
	}, nil
}

func TestRealS11MixedPushOutcomesPreservePartitionOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "s11-mixed-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")

	recordIDs := []string{
		"00000000-0000-4000-8b02-000000000001",
		"00000000-0000-4000-8b02-000000000002",
		"00000000-0000-4000-8b02-000000000003",
		"00000000-0000-4000-8b02-000000000004",
	}
	if err := harness.Source().ExecContext(ctx, `
		INSERT INTO cf_items (id, owner_id, value) VALUES
		($1, $2, $3), ($4, $5, $6)`,
		recordIDs[1], "diagnostic-user", "s11-existing-first",
		recordIDs[3], "diagnostic-user", "s11-existing-second",
	); err != nil {
		t.Fatalf("insert S-11 conflict rows: %v", err)
	}

	mutationIDs := []string{
		"00000000-0000-4000-8b02-000000000011",
		"00000000-0000-4000-8b02-000000000012",
		"00000000-0000-4000-8b02-000000000013",
		"00000000-0000-4000-8b02-000000000014",
	}
	values := []string{"s11-applied-first", "s11-conflict-first", "s11-applied-second", "s11-conflict-second"}
	mutations := make([]map[string]any, 0, len(mutationIDs))
	for index := range mutationIDs {
		mutations = append(mutations, phase4InsertMutation(
			client, table, ownerField, mutationIDs[index], recordIDs[index], values[index],
		))
	}
	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8b02-000000000020",
		mutations,
	))
	if status != http.StatusOK {
		t.Fatalf("S-11 mixed push status = %d, want 200", status)
	}
	accepted := requireOutcomeList(t, response, "accepted")
	rejected := requireOutcomeList(t, response, "rejected")
	acceptedIDs := phase4OutcomeIDs(t, accepted)
	rejectedIDs := phase4OutcomeIDs(t, rejected)
	if !slices.Equal(acceptedIDs, []string{mutationIDs[0], mutationIDs[2]}) ||
		!slices.Equal(rejectedIDs, []string{mutationIDs[1], mutationIDs[3]}) {
		t.Fatalf("S-11 mixed partition order is invalid: accepted=%v rejected=%v", acceptedIDs, rejectedIDs)
	}
	seen := make(map[string]struct{}, len(mutationIDs))
	for _, outcome := range append(append([]map[string]any(nil), accepted...), rejected...) {
		mutationID, _ := outcome["mutation_id"].(string)
		if _, duplicate := seen[mutationID]; duplicate {
			t.Fatal("S-11 mixed push returned one mutation more than once")
		}
		seen[mutationID] = struct{}{}
	}
	if len(seen) != len(mutationIDs) {
		t.Fatal("S-11 mixed push omitted a mutation outcome")
	}
	assertCanonicalPhase4Outcome(t, accepted[0], client, table, ownerField, mutationIDs[0], recordIDs[0], values[0], "applied", "")
	assertCanonicalPhase4Outcome(t, accepted[1], client, table, ownerField, mutationIDs[2], recordIDs[2], values[2], "applied", "")
	assertCanonicalPhase4Outcome(t, rejected[0], client, table, ownerField, mutationIDs[1], recordIDs[1], "s11-existing-first", "conflict", "row_already_exists")
	assertCanonicalPhase4Outcome(t, rejected[1], client, table, ownerField, mutationIDs[3], recordIDs[3], "s11-existing-second", "conflict", "row_already_exists")

	observation, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, recordIDs)
	if err != nil {
		t.Fatalf("observe S-11 mixed durable state: %v", err)
	}
	if observation.BatchCount != 1 || observation.MutationCount != 4 ||
		observation.SourceRowCount != 4 || observation.AcceptedWriteEpoch != 2 {
		t.Fatalf("S-11 mixed durable state is invalid: %#v", observation)
	}
}

func TestRealS12StaleClientCompactionAndReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "s12-retention-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")

	before, err := harness.Operator().ObserveDiagnosticClientGeneration(ctx, client.ID)
	if err != nil {
		t.Fatalf("observe S-12 initial generation: %v", err)
	}
	if !before.Active || before.Generation != client.Generation || before.ScopeHistoryGenerations != 1 {
		t.Fatalf("S-12 initial generation state is invalid: %#v", before)
	}

	prefixRecordIDs := []string{
		"00000000-0000-4000-8b03-000000000021",
		"00000000-0000-4000-8b03-000000000022",
	}
	for index, recordID := range prefixRecordIDs {
		if err := harness.Source().ExecContext(
			ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
			recordID,
			"diagnostic-user",
			fmt.Sprintf("s12-prefix-%d", index+1),
		); err != nil {
			t.Fatalf("insert S-12 prefix row %s: %v", recordID, err)
		}
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", prefixRecordIDs...)

	activeControl := connectRealProtocolClient(t, ctx, harness, token, "s12-active-control-client")
	rebuildID := "00000000-0000-4000-8b03-000000000012"
	status, firstRebuildPage := requestRealRebuildPage(
		t,
		ctx,
		harness,
		token,
		activeControl,
		"user:diagnostic-user",
		rebuildID,
		nil,
		1,
	)
	if status != http.StatusOK {
		t.Fatalf("S-12 rebuild pin status = %d: %#v", status, firstRebuildPage)
	}
	if records := requireRealRebuildRecords(t, firstRebuildPage); len(records) != 1 || firstRebuildPage["has_more"] != true {
		t.Fatalf("S-12 rebuild pin page is invalid: %#v", firstRebuildPage)
	}
	continuation, ok := firstRebuildPage["cursor"].(string)
	if !ok || continuation == "" {
		t.Fatalf("S-12 rebuild pin continuation is invalid: %#v", firstRebuildPage)
	}
	rebuildSession, err := harness.Operator().ObserveRebuildSession(ctx, activeControl.ID, rebuildID)
	if err != nil {
		t.Fatalf("observe S-12 rebuild pin: %v", err)
	}
	if rebuildSession.Expired || rebuildSession.BoundaryPositionKind != "transaction_end" || rebuildSession.StagedRowCount != 2 {
		t.Fatalf("S-12 rebuild pin is invalid: %#v", rebuildSession)
	}
	status, finalRebuildPage := requestRealRebuildPage(
		t,
		ctx,
		harness,
		token,
		activeControl,
		"user:diagnostic-user",
		rebuildID,
		continuation,
		1,
	)
	if status != http.StatusOK {
		t.Fatalf("S-12 final rebuild page status = %d: %#v", status, finalRebuildPage)
	}
	if records := requireRealRebuildRecords(t, finalRebuildPage); len(records) != 1 || finalRebuildPage["has_more"] != false {
		t.Fatalf("S-12 final rebuild page is invalid: %#v", finalRebuildPage)
	}
	finalScopeCursor, ok := finalRebuildPage["final_scope_cursor"].(string)
	if !ok || finalScopeCursor == "" {
		t.Fatalf("S-12 final rebuild cursor is invalid: %#v", finalRebuildPage)
	}
	activeControl.Scopes["user:diagnostic-user"] = map[string]any{"cursor": finalScopeCursor}

	pullWithGlobalRebuild := func(limit int) map[string]any {
		t.Helper()
		pullStatus, response := postSync(
			t,
			ctx,
			harness.AdapterURL(),
			token,
			"/sync/pull",
			realPullPayload(activeControl, activeControl.Scopes, limit),
		)
		if pullStatus != http.StatusOK {
			t.Fatalf("S-12 active control pull status = %d: %#v", pullStatus, response)
		}
		rebuild, ok := response["rebuild"].([]any)
		if !ok || len(rebuild) != 1 || rebuild[0] != "cf:global" {
			t.Fatalf("S-12 active control rebuild set is invalid: %#v", response)
		}
		scopeCursors, ok := response["scope_cursors"].(map[string]any)
		if !ok {
			t.Fatalf("S-12 active control cursors are invalid: %#v", response)
		}
		for scopeID, rawCursor := range scopeCursors {
			cursor, ok := rawCursor.(string)
			if !ok || cursor == "" || scopeID != "user:diagnostic-user" {
				t.Fatalf("S-12 active control cursor is invalid: %#v", scopeCursors)
			}
			activeControl.Scopes[scopeID] = map[string]any{"cursor": cursor}
		}
		return response
	}
	baselineAcknowledgement := pullWithGlobalRebuild(1)
	if changes := requireRealChanges(t, baselineAcknowledgement); len(changes) != 0 || baselineAcknowledgement["has_more"] != false {
		t.Fatalf("S-12 baseline acknowledgement is invalid: %#v", baselineAcknowledgement)
	}
	beforePinCheckpoints := observeCheckpointMap(t, ctx, harness, activeControl.ID)
	if len(beforePinCheckpoints) != 2 || beforePinCheckpoints["cf:global"].PositionKind != "generation_start" {
		t.Fatalf("S-12 baseline checkpoint set is invalid: %#v", beforePinCheckpoints)
	}

	pinnedRecordID := "00000000-0000-4000-8b03-000000000023"
	unrelatedRecordID := "00000000-0000-4000-8b03-000000000024"
	transaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin S-12 post-boundary transaction: %v", err)
	}
	if _, err := transaction.ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		pinnedRecordID,
		"diagnostic-user",
		"s12-pinned-suffix",
	); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("insert S-12 pinned row: %v", err)
	}
	if _, err := transaction.ExecContext(
		ctx,
		"INSERT INTO cf_global_items (id, value) VALUES ($1, $2)",
		unrelatedRecordID,
		"s12-unrelated-scope",
	); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("insert S-12 unrelated row: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit S-12 post-boundary transaction: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", pinnedRecordID)
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", unrelatedRecordID)

	pinnedPage := pullWithGlobalRebuild(1)
	pinnedChanges := requireRealChanges(t, pinnedPage)
	if len(pinnedChanges) != 1 || pinnedPage["has_more"] != false {
		t.Fatalf("S-12 pinned effect page is invalid: %#v", pinnedPage)
	}
	requireRealPullChange(t, pinnedChanges, "user:diagnostic-user", table, pinnedRecordID, "s12-pinned-suffix")
	pinnedAcknowledgement := pullWithGlobalRebuild(1)
	if changes := requireRealChanges(t, pinnedAcknowledgement); len(changes) != 0 || pinnedAcknowledgement["has_more"] != false {
		t.Fatalf("S-12 pinned acknowledgement is invalid: %#v", pinnedAcknowledgement)
	}
	afterPresentationCheckpoints := observeCheckpointMap(t, ctx, harness, activeControl.ID)
	if sameCheckpointPosition(
		afterPresentationCheckpoints["user:diagnostic-user"],
		beforePinCheckpoints["user:diagnostic-user"],
	) {
		t.Fatal("S-12 active control did not acknowledge the pinned user effect")
	}
	if !sameCheckpointPosition(
		afterPresentationCheckpoints["cf:global"],
		beforePinCheckpoints["cf:global"],
	) {
		t.Fatal("S-12 active control advanced the unreconstructed global scope")
	}

	beforeCompaction, err := harness.Operator().ObserveDiagnosticRetentionCompaction(
		ctx,
		activeControl.ID,
		rebuildID,
		prefixRecordIDs,
		pinnedRecordID,
		unrelatedRecordID,
	)
	if err != nil {
		t.Fatalf("observe S-12 state before compaction: %v", err)
	}
	if !beforeCompaction.RebuildPinActive || !beforeCompaction.PinnedAfterBoundary ||
		beforeCompaction.PrefixEffectCount != 2 || beforeCompaction.PinnedEffectCount != 1 ||
		beforeCompaction.UnrelatedEffectCount != 1 || beforeCompaction.PrefixMaximumPosition == "" ||
		beforeCompaction.PinnedPosition == "" || beforeCompaction.UserFloorPosition != "generation_start|||" ||
		beforeCompaction.GlobalFloorPosition != "generation_start|||" {
		t.Fatalf("S-12 state before compaction is invalid: %#v", beforeCompaction)
	}
	if err := harness.Operator().ExpireRetentionClient(ctx, "diagnostic-user", "s12-retention-client"); err != nil {
		t.Fatalf("age S-12 diagnostic client: %v", err)
	}
	compaction, err := harness.Operator().RunDiagnosticRetentionCompaction(ctx)
	if err != nil {
		t.Fatalf("run S-12 compaction: %v", err)
	}
	if compaction.DeactivatedClients != 1 || compaction.DeletedEntries != 2 || compaction.SafeSeq <= 0 {
		t.Fatalf(
			"S-12 compaction result is invalid: result=%#v before=%#v checkpoints=%#v session=%#v",
			compaction,
			beforeCompaction,
			afterPresentationCheckpoints,
			rebuildSession,
		)
	}
	afterCompaction, err := harness.Operator().ObserveDiagnosticRetentionCompaction(
		ctx,
		activeControl.ID,
		rebuildID,
		prefixRecordIDs,
		pinnedRecordID,
		unrelatedRecordID,
	)
	if err != nil {
		t.Fatalf("observe S-12 state after compaction: %v", err)
	}
	if !afterCompaction.RebuildPinActive || !afterCompaction.PinnedAfterBoundary ||
		afterCompaction.PrefixEffectCount != 0 || afterCompaction.PinnedEffectCount != 1 ||
		afterCompaction.UnrelatedEffectCount != 1 || afterCompaction.PrefixMaximumPosition != "" ||
		afterCompaction.PinnedPosition != beforeCompaction.PinnedPosition ||
		afterCompaction.UserFloorPosition != beforeCompaction.PrefixMaximumPosition ||
		afterCompaction.GlobalFloorPosition != beforeCompaction.GlobalFloorPosition {
		t.Fatalf("S-12 scope-local compaction state is invalid: before=%#v after=%#v", beforeCompaction, afterCompaction)
	}
	retired, err := harness.Operator().ObserveDiagnosticClientGeneration(ctx, client.ID)
	if err != nil {
		t.Fatalf("observe S-12 retired generation: %v", err)
	}
	controlState, err := harness.Operator().ObserveDiagnosticClientGeneration(ctx, activeControl.ID)
	if err != nil {
		t.Fatalf("observe S-12 active control: %v", err)
	}
	if retired.Active || retired.Generation != before.Generation || !controlState.Active || controlState.Generation != activeControl.Generation {
		t.Fatalf("S-12 compaction selected the wrong client: retired=%#v control=%#v", retired, controlState)
	}

	staleRecordID := "00000000-0000-4000-8b03-000000000001"
	staleStatus, staleResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8b03-000000000002",
		[]map[string]any{phase4InsertMutation(
			client,
			table,
			ownerField,
			"00000000-0000-4000-8b03-000000000003",
			staleRecordID,
			"s12-stale-generation",
		)},
	))
	assertPhase4ProtocolError(t, staleStatus, staleResponse, http.StatusConflict, "client_generation_expired")
	if staleResponse["error"].(map[string]any)["current_client_generation"] != float64(client.Generation) {
		t.Fatal("S-12 stale push did not identify the retired generation")
	}
	stalePush, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{staleRecordID})
	if err != nil {
		t.Fatalf("observe S-12 stale push state: %v", err)
	}
	if stalePush.BatchCount != 0 || stalePush.MutationCount != 0 || stalePush.SourceRowCount != 0 {
		t.Fatalf("S-12 stale push performed durable work: %#v", stalePush)
	}

	status, reconnected := postSync(t, ctx, harness.AdapterURL(), token, "/sync/connect", map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            client.Schema,
		"scope_set_version": client.ScopeSetVersion,
		"known_scopes":      client.Scopes,
	})
	if status != http.StatusOK {
		t.Fatalf("S-12 reconnect status = %d, want 200", status)
	}
	newGeneration, ok := reconnected["client_generation"].(float64)
	if !ok || int64(newGeneration) != client.Generation+1 {
		t.Fatalf("S-12 reconnect generation is invalid: %#v", reconnected)
	}
	reconnectedScopeVersion, ok := reconnected["scope_set_version"].(float64)
	if !ok || int64(reconnectedScopeVersion) != client.ScopeSetVersion {
		t.Fatal("S-12 reconnect changed the unchanged scope set version")
	}
	schema, ok := reconnected["schema"].(map[string]any)
	schemaVersion, schemaVersionOK := schema["version"].(float64)
	clientSchemaVersion, clientSchemaVersionOK := client.Schema["version"].(int64)
	if !ok || !schemaVersionOK || !clientSchemaVersionOK || schema["action"] != "none" ||
		schema["hash"] != client.Schema["hash"] || int64(schemaVersion) != clientSchemaVersion {
		t.Fatalf("S-12 reconnect schema dispatch is invalid: %#v", schema)
	}
	if _, present := reconnected["schema_definition"]; present {
		t.Fatal("S-12 exact-schema reconnect returned a schema definition")
	}
	scopeDelta, ok := reconnected["scopes"].(map[string]any)
	additions, additionsOK := scopeDelta["add"].([]any)
	removals, removalsOK := scopeDelta["remove"].([]any)
	if !ok || !additionsOK || !removalsOK || len(additions) != 0 || len(removals) != 0 {
		t.Fatalf("S-12 reconnect scope delta is invalid: %#v", scopeDelta)
	}
	cursorUpdates, ok := reconnected["scope_cursor_updates"].(map[string]any)
	if !ok || len(cursorUpdates) != 2 || cursorUpdates["cf:global"] != nil || cursorUpdates["user:diagnostic-user"] != nil {
		t.Fatalf("S-12 reconnect cursor reset is invalid: %#v", cursorUpdates)
	}
	after, err := harness.Operator().ObserveDiagnosticClientGeneration(ctx, client.ID)
	if err != nil {
		t.Fatalf("observe S-12 renewed generation: %v", err)
	}
	if !after.Active || after.Generation != client.Generation+1 || after.ScopeHistoryGenerations != 2 || after.CheckpointCount != 2 {
		t.Fatalf("S-12 renewed generation state is invalid: %#v", after)
	}
}

func TestRealS17InvalidPushShapesDoNoDurableWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "s17-invalid-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")

	recordIDs := make([]string, 0, 7)
	tests := []struct {
		name      string
		batchID   string
		recordID  string
		mutations func(string) []map[string]any
		topLevel  func(map[string]any)
	}{
		{
			name:     "insert with base version",
			batchID:  "00000000-0000-4000-8b04-000000000001",
			recordID: "00000000-0000-4000-8b04-000000000011",
			mutations: func(recordID string) []map[string]any {
				mutation := phase4InsertMutation(client, table, ownerField, "00000000-0000-4000-8b04-000000000021", recordID, "s17-insert-base")
				mutation["base_version"] = "forbidden"
				return []map[string]any{mutation}
			},
		},
		{
			name:     "update without base version",
			batchID:  "00000000-0000-4000-8b04-000000000002",
			recordID: "00000000-0000-4000-8b04-000000000012",
			mutations: func(recordID string) []map[string]any {
				mutation := phase4InsertMutation(client, table, ownerField, "00000000-0000-4000-8b04-000000000022", recordID, "s17-update-base")
				mutation["op"] = "update"
				return []map[string]any{mutation}
			},
		},
		{
			name:     "delete with columns",
			batchID:  "00000000-0000-4000-8b04-000000000003",
			recordID: "00000000-0000-4000-8b04-000000000013",
			mutations: func(recordID string) []map[string]any {
				mutation := phase4InsertMutation(client, table, ownerField, "00000000-0000-4000-8b04-000000000023", recordID, "s17-delete-columns")
				mutation["op"] = "delete"
				mutation["base_version"] = "present"
				return []map[string]any{mutation}
			},
		},
		{
			name:     "duplicate mutation identifiers",
			batchID:  "00000000-0000-4000-8b04-000000000004",
			recordID: "00000000-0000-4000-8b04-000000000014",
			mutations: func(recordID string) []map[string]any {
				mutation := phase4InsertMutation(client, table, ownerField, "00000000-0000-4000-8b04-000000000024", recordID, "s17-duplicate")
				return []map[string]any{mutation, mutation}
			},
		},
		{
			name:     "unknown envelope field",
			batchID:  "00000000-0000-4000-8b04-000000000005",
			recordID: "00000000-0000-4000-8b04-000000000015",
			mutations: func(recordID string) []map[string]any {
				return []map[string]any{phase4InsertMutation(client, table, ownerField, "00000000-0000-4000-8b04-000000000025", recordID, "s17-envelope-field")}
			},
			topLevel: func(payload map[string]any) {
				payload["unexpected"] = true
			},
		},
		{
			name:     "unknown mutation field",
			batchID:  "00000000-0000-4000-8b04-000000000006",
			recordID: "00000000-0000-4000-8b04-000000000016",
			mutations: func(recordID string) []map[string]any {
				mutation := phase4InsertMutation(client, table, ownerField, "00000000-0000-4000-8b04-000000000026", recordID, "s17-mutation-field")
				mutation["unexpected"] = true
				return []map[string]any{mutation}
			},
		},
		{
			name:     "upsert operation",
			batchID:  "00000000-0000-4000-8b04-000000000007",
			recordID: "00000000-0000-4000-8b04-000000000017",
			mutations: func(recordID string) []map[string]any {
				mutation := phase4InsertMutation(client, table, ownerField, "00000000-0000-4000-8b04-000000000027", recordID, "s17-upsert")
				mutation["op"] = "upsert"
				return []map[string]any{mutation}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			recordIDs = append(recordIDs, test.recordID)
			payload := phase4PushPayload(client, test.batchID, test.mutations(test.recordID))
			if test.topLevel != nil {
				test.topLevel(payload)
			}
			status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", payload)
			assertPhase4ProtocolError(t, status, response, http.StatusBadRequest, "invalid_request")
		})
	}

	observation, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, recordIDs)
	if err != nil {
		t.Fatalf("observe S-17 durable state: %v", err)
	}
	if observation.BatchCount != 0 || observation.MutationCount != 0 ||
		observation.SourceRowCount != 0 || observation.AcceptedWriteEpoch != 1 {
		t.Fatalf("S-17 invalid requests performed durable mutation work: %#v", observation)
	}
}

func TestRealS20PushMutationCountBoundsAreAtomic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "s20-bounds-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")

	maximumMutations, maximumRecords := phase4BoundedInsertMutations(client, table, ownerField, 1000, "8b20", "8b21", "s20-max")
	maximumStatus, maximumResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8b20-000000000000",
		maximumMutations,
	))
	if maximumStatus != http.StatusOK {
		t.Fatalf("S-20 exact maximum push status = %d, want 200", maximumStatus)
	}
	accepted := requireOutcomeList(t, maximumResponse, "accepted")
	rejected := requireOutcomeList(t, maximumResponse, "rejected")
	if len(accepted) != 1000 || len(rejected) != 0 {
		t.Fatalf("S-20 exact maximum outcome counts = %d accepted and %d rejected", len(accepted), len(rejected))
	}
	for index, outcome := range accepted {
		if outcome["mutation_id"] != maximumMutations[index]["mutation_id"] || outcome["status"] != "applied" {
			t.Fatalf("S-20 exact maximum outcome %d is invalid", index)
		}
	}
	maximumState, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, maximumRecords)
	if err != nil {
		t.Fatalf("observe S-20 maximum durable state: %v", err)
	}
	if maximumState.BatchCount != 1 || maximumState.MutationCount != 1000 ||
		maximumState.SourceRowCount != 1000 || maximumState.AcceptedWriteEpoch != 2 {
		t.Fatalf("S-20 exact maximum durable state is invalid: %#v", maximumState)
	}

	overMutations, overRecords := phase4BoundedInsertMutations(client, table, ownerField, 1001, "8b30", "8b31", "s20-over")
	overStatus, overResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8b30-000000000000",
		overMutations,
	))
	assertPhase4ProtocolError(t, overStatus, overResponse, http.StatusBadRequest, "invalid_request")
	overState, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, overRecords)
	if err != nil {
		t.Fatalf("observe S-20 over-limit durable state: %v", err)
	}
	if overState.BatchCount != 1 || overState.MutationCount != 1000 ||
		overState.SourceRowCount != 0 || overState.AcceptedWriteEpoch != 2 {
		t.Fatalf("S-20 maximum-plus-one request performed partial work: %#v", overState)
	}
}

func phase4PushPayload(client *realProtocolClient, batchID string, mutations []map[string]any) map[string]any {
	return map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"batch_id":          batchID,
		"schema":            client.Schema,
		"mutations":         mutations,
	}
}

func phase4InsertMutation(client *realProtocolClient, table realProtocolTable, ownerField, mutationID, recordID, value string) map[string]any {
	return map[string]any{
		"mutation_id":     mutationID,
		"table":           table.ID,
		"pk":              map[string]any{table.PrimaryKeyField: recordID},
		"authored_schema": client.Schema,
		"op":              "insert",
		"client_version":  phase4ClientVersion,
		"columns": map[string]any{
			ownerField:       "diagnostic-user",
			table.ValueField: value,
		},
	}
}

func phase4BoundedInsertMutations(client *realProtocolClient, table realProtocolTable, ownerField string, count int, recordGroup, mutationGroup, valuePrefix string) ([]map[string]any, []string) {
	mutations := make([]map[string]any, 0, count)
	recordIDs := make([]string, 0, count)
	for index := 1; index <= count; index++ {
		recordID := fmt.Sprintf("00000000-0000-4000-%s-%012x", recordGroup, index)
		mutationID := fmt.Sprintf("00000000-0000-4000-%s-%012x", mutationGroup, index)
		recordIDs = append(recordIDs, recordID)
		mutations = append(mutations, phase4InsertMutation(
			client,
			table,
			ownerField,
			mutationID,
			recordID,
			fmt.Sprintf("%s-%04d", valuePrefix, index),
		))
	}
	return mutations, recordIDs
}

func loadRealProtocolFieldID(t *testing.T, ctx context.Context, harness *blackbox.Harness, tableName, fieldName string) string {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, harness.AdapterURL()+"/sync/schema", nil)
	if err != nil {
		t.Fatalf("create real schema request: %v", err)
	}
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("request real schema: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("real schema status = %d, want 200", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatalf("read real schema: %v", err)
	}
	var envelope struct {
		Manifest json.RawMessage `json:"manifest"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Manifest) == 0 {
		t.Fatalf("decode real schema envelope: %v", err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(envelope.Manifest, &manifest); err != nil {
		t.Fatalf("decode real schema manifest: %v", err)
	}
	tables, ok := manifest["tables"].([]any)
	if !ok {
		t.Fatal("real schema tables are invalid")
	}
	for _, rawTable := range tables {
		table, ok := rawTable.(map[string]any)
		if !ok || table["name"] != tableName {
			continue
		}
		fields, ok := table["fields"].([]any)
		if !ok {
			t.Fatal("real schema fields are invalid")
		}
		for _, rawField := range fields {
			field, ok := rawField.(map[string]any)
			if !ok || field["name"] != fieldName {
				continue
			}
			fieldID, _ := field["field_id"].(string)
			if !uuidPattern.MatchString(fieldID) {
				t.Fatal("real schema field identity is invalid")
			}
			return fieldID
		}
	}
	t.Fatalf("real schema field %s.%s is missing", tableName, fieldName)
	return ""
}

func decodeRealResponseObject(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("decode real response: %v", err)
	}
	return response
}

func phase4OutcomeIDs(t *testing.T, outcomes []map[string]any) []string {
	t.Helper()
	ids := make([]string, 0, len(outcomes))
	for _, outcome := range outcomes {
		mutationID, ok := outcome["mutation_id"].(string)
		if !ok || !uuidPattern.MatchString(mutationID) {
			t.Fatal("predicate mutation_outcome_identity_shape failed")
		}
		ids = append(ids, mutationID)
	}
	return ids
}

func assertCanonicalPhase4Outcome(t *testing.T, outcome map[string]any, client *realProtocolClient, table realProtocolTable, ownerField, mutationID, recordID, expectedValue, status, code string) {
	t.Helper()
	if outcome["mutation_id"] != mutationID || outcome["table"] != table.ID || outcome["status"] != status {
		t.Fatal("predicate mutation_outcome_identity failed")
	}
	pk, ok := outcome["pk"].(map[string]any)
	if !ok || len(pk) != 1 || pk[table.PrimaryKeyField] != recordID {
		t.Fatal("predicate mutation_outcome_primary_key failed")
	}
	schema, ok := outcome["outcome_schema"].(map[string]any)
	schemaVersion, schemaVersionOK := schema["version"].(float64)
	clientSchemaVersion, clientSchemaVersionOK := client.Schema["version"].(int64)
	if !ok || !schemaVersionOK || !clientSchemaVersionOK || schema["hash"] != client.Schema["hash"] ||
		int64(schemaVersion) != clientSchemaVersion {
		t.Fatal("predicate mutation_outcome_schema failed")
	}
	if code == "" {
		if _, present := outcome["code"]; present {
			t.Fatal("predicate mutation_outcome_code_absent failed")
		}
	} else if outcome["code"] != code {
		t.Fatal("predicate mutation_outcome_code_exact failed")
	}
	row, ok := outcome["server_row"].(map[string]any)
	if !ok || row[ownerField] != "diagnostic-user" || row[table.ValueField] != expectedValue {
		t.Fatal("predicate mutation_outcome_canonical_row failed")
	}
	serverVersion, ok := outcome["server_version"].(string)
	if !ok || !uuidPattern.MatchString(serverVersion) {
		t.Fatal("predicate mutation_outcome_server_version failed")
	}
	checksum, ok := outcome["row_checksum"].(map[string]any)
	if !ok || len(checksum) != 4 || checksum["algorithm"] != "sha256" || checksum["encoding"] != "hex" || checksum["version"] != float64(1) {
		t.Fatal("predicate mutation_outcome_checksum_metadata failed")
	}
	digest, ok := checksum["digest"].(string)
	if !ok || len(digest) != 64 {
		t.Fatal("predicate mutation_outcome_checksum_digest failed")
	}
}

func assertPhase4ProtocolError(t *testing.T, status int, response map[string]any, expectedStatus int, code string) {
	t.Helper()
	if status != expectedStatus || len(response) != 1 {
		t.Fatalf("phase 4 protocol error status = %d, want %d", status, expectedStatus)
	}
	errorBody, ok := response["error"].(map[string]any)
	if !ok || errorBody["code"] != code || errorBody["retryable"] != false {
		t.Fatalf("phase 4 protocol error is invalid: %#v", response)
	}
	if _, accepted := response["accepted"]; accepted {
		t.Fatal("phase 4 request error contains accepted outcomes")
	}
	if _, rejected := response["rejected"]; rejected {
		t.Fatal("phase 4 request error contains rejected outcomes")
	}
}
