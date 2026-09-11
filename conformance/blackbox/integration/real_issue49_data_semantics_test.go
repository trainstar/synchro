package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
)

var issue49MicrosecondTimestamp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$`)

func TestRealIssue49ConnectRejectsFreshReuseAndInvalidEnvelopeValues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-connect-client")

	t.Run("assertion", func(t *testing.T) {
		status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
			"client_id":         client.ID,
			"client_generation": client.Generation,
			"platform":          "conformance",
			"app_version":       "0.3.0+issue49.1",
			"protocol_version":  3,
			"schema":            client.Schema,
			"scope_set_version": client.ScopeSetVersion,
			"known_scopes":      client.Scopes,
		})
		if status != http.StatusOK {
			t.Fatalf("unchanged connect status = %d, want 200: %#v", status, response)
		}
		version, ok := response["scope_set_version"].(float64)
		if !ok || int64(version) != client.ScopeSetVersion {
			t.Fatalf("unchanged assignment advanced scope_set_version: %#v", response)
		}
		delta, ok := response["scopes"].(map[string]any)
		add, addOK := delta["add"].([]any)
		remove, removeOK := delta["remove"].([]any)
		if !ok || !addOK || !removeOK || len(add) != 0 || len(remove) != 0 {
			t.Fatalf("unchanged assignment returned a scope delta: %#v", delta)
		}
	})

	t.Run("assertion", func(t *testing.T) {
		database, err := sql.Open("pgx", harness.DatabaseURL())
		if err != nil {
			t.Fatalf("open changed-scope database: %v", err)
		}
		defer database.Close()
		const addedScope = "cf:issue49-granted"
		if _, err := database.ExecContext(
			ctx,
			"SELECT synchro.synchro_grant_user_scope('diagnostic-user', $1)",
			addedScope,
		); err != nil {
			t.Fatalf("grant changed authoritative scope: %v", err)
		}
		status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
			"client_id":         client.ID,
			"client_generation": client.Generation,
			"platform":          "conformance",
			"app_version":       "0.3.0+issue49.2",
			"protocol_version":  3,
			"schema":            client.Schema,
			"scope_set_version": client.ScopeSetVersion,
			"known_scopes":      client.Scopes,
		})
		if status != http.StatusOK {
			t.Fatalf("changed-scope connect status = %d, want 200: %#v", status, response)
		}
		version, ok := response["scope_set_version"].(float64)
		if !ok || int64(version) != client.ScopeSetVersion+1 {
			t.Fatalf("changed assignment did not advance scope_set_version exactly once: %#v", response)
		}
		delta, ok := response["scopes"].(map[string]any)
		add, addOK := delta["add"].([]any)
		remove, removeOK := delta["remove"].([]any)
		if !ok || !addOK || !removeOK || len(add) != 1 || len(remove) != 0 {
			t.Fatalf("changed assignment returned the wrong scope delta: %#v", delta)
		}
		assignment, ok := add[0].(map[string]any)
		if !ok || assignment["id"] != addedScope {
			t.Fatalf("changed assignment omitted the granted scope: %#v", delta)
		}
	})

	t.Run("assertion", func(t *testing.T) {
		status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
			"client_id":         client.ID,
			"platform":          "conformance",
			"app_version":       "0.3.0",
			"protocol_version":  3,
			"schema":            map[string]any{"version": 0, "hash": ""},
			"scope_set_version": 0,
			"known_scopes":      map[string]any{},
		})
		requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_schema_reference")
	})

	for index, appVersion := range []string{
		"v0.3.0",
		"0.3",
		"0.3.0-",
		"01.2.3",
		"1.02.3",
		"1.2.03",
		"1.2.3-01",
		"1.2.3-alpha..1",
		"1.2.3-+build",
		"1.2.3+",
		"1.2.3+build..1",
		"1.2.3-α",
	} {
		index, appVersion := index, appVersion
		t.Run("assertion", func(t *testing.T) {
			status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
				"client_id":         "issue49-semver-client-" + string(rune('a'+index)),
				"platform":          "conformance",
				"app_version":       appVersion,
				"protocol_version":  3,
				"schema":            map[string]any{"version": 0, "hash": ""},
				"scope_set_version": 0,
				"known_scopes":      map[string]any{},
			})
			requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_request")
		})
	}

	t.Run("assertion", func(t *testing.T) {
		status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
			"client_id":         "issue49-unsafe-integer-client",
			"platform":          "conformance",
			"app_version":       "0.3.0",
			"protocol_version":  int64(9_007_199_254_740_992),
			"schema":            map[string]any{"version": 0, "hash": ""},
			"scope_set_version": 0,
			"known_scopes":      map[string]any{},
		})
		requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_request")
	})

	t.Run("assertion", func(t *testing.T) {
		status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
			"client_id":         "issue49-protocol-version-client",
			"platform":          "conformance",
			"app_version":       "3.0.0-rc.1+build.7",
			"protocol_version":  2,
			"schema":            map[string]any{"version": 0, "hash": ""},
			"scope_set_version": 0,
			"known_scopes":      map[string]any{},
		})
		requireRealProtocolError(t, status, response, http.StatusUpgradeRequired, "upgrade_required")
		errorBody := response["error"].(map[string]any)
		if errorBody["required_protocol_version"] != float64(3) || errorBody["received_protocol_version"] != float64(2) {
			t.Fatalf("protocol integer negotiation was conflated with application SemVer: %#v", response)
		}
	})
}

func TestRealIssue49SemanticVersionPrecedence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)

	t.Run("assertion", func(t *testing.T) {
		adapterURL, stop := issue49StartVersionedAdapter(t, ctx, harness, "1.0.0-beta.11")
		defer stop()
		lowerStatus, _ := issue49PostVersionedConnect(
			t,
			ctx,
			adapterURL,
			token,
			"1.0.0-beta.2",
			"issue49-semver-numeric-lower",
			3,
		)
		higherStatus, higherResponse := issue49PostVersionedConnect(
			t,
			ctx,
			adapterURL,
			token,
			"1.0.0-beta.11",
			"issue49-semver-numeric-higher",
			3,
		)
		if lowerStatus != http.StatusUpgradeRequired {
			t.Fatalf("lower numeric prerelease passed minimum with status %d", lowerStatus)
		}
		if higherStatus != http.StatusOK {
			t.Fatalf("higher numeric prerelease failed minimum with status %d: %#v", higherStatus, higherResponse)
		}
	})

	comparisons := []struct {
		name    string
		lower   string
		higher  string
		minimum string
	}{
		{name: "alpha extension", lower: "1.0.0-alpha", higher: "1.0.0-alpha.1", minimum: "1.0.0-alpha.1"},
		{name: "numeric before nonnumeric", lower: "1.0.0-alpha.1", higher: "1.0.0-alpha.beta", minimum: "1.0.0-alpha.beta"},
		{name: "alpha before beta", lower: "1.0.0-alpha.beta", higher: "1.0.0-beta", minimum: "1.0.0-beta"},
		{name: "beta extension", lower: "1.0.0-beta", higher: "1.0.0-beta.2", minimum: "1.0.0-beta.2"},
		{name: "beta before release candidate", lower: "1.0.0-beta.11", higher: "1.0.0-rc.1", minimum: "1.0.0-rc.1"},
		{name: "prerelease before release", lower: "1.0.0-rc.1", higher: "1.0.0", minimum: "1.0.0"},
		{name: "major version", lower: "1.999.999", higher: "2.0.0", minimum: "2.0.0"},
	}
	for index, comparison := range comparisons {
		index, comparison := index, comparison
		t.Run(comparison.name, func(t *testing.T) {
			adapterURL, stop := issue49StartVersionedAdapter(t, ctx, harness, comparison.minimum)
			defer stop()
			lowerStatus, _ := issue49PostVersionedConnect(
				t,
				ctx,
				adapterURL,
				token,
				comparison.lower,
				fmt.Sprintf("issue49-semver-lower-%02d", index),
				3,
			)
			higherStatus, higherResponse := issue49PostVersionedConnect(
				t,
				ctx,
				adapterURL,
				token,
				comparison.higher,
				fmt.Sprintf("issue49-semver-higher-%02d", index),
				3,
			)
			t.Run("assertion", func(t *testing.T) {
				if lowerStatus != http.StatusUpgradeRequired {
					t.Fatalf("lower SemVer %q passed minimum %q with status %d", comparison.lower, comparison.minimum, lowerStatus)
				}
				if higherStatus != http.StatusOK {
					t.Fatalf("higher SemVer %q failed minimum %q with status %d: %#v", comparison.higher, comparison.minimum, higherStatus, higherResponse)
				}
			})
		})
	}

	t.Run("build metadata has equal precedence", func(t *testing.T) {
		adapterURL, stop := issue49StartVersionedAdapter(t, ctx, harness, "1.0.0+minimum")
		defer stop()
		status, response := issue49PostVersionedConnect(
			t,
			ctx,
			adapterURL,
			token,
			"1.0.0+different-build",
			"issue49-semver-build-metadata",
			3,
		)
		t.Run("assertion", func(t *testing.T) {
			if status != http.StatusOK {
				t.Fatalf("build metadata changed SemVer precedence: status=%d response=%#v", status, response)
			}
		})
	})
}

func TestRealIssue49PortableIntegerBoundariesAndCounterOverflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-integer-client")
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open portable-integer database: %v", err)
	}
	defer database.Close()

	const maximumSafeInteger int64 = 9_007_199_254_740_991
	if _, err := database.ExecContext(ctx, `
		UPDATE synchro.sync_clients
		SET scope_set_version = $1
		WHERE user_id = 'diagnostic-user' AND client_id = $2`, maximumSafeInteger, client.ID); err != nil {
		t.Fatalf("stage maximum safe scope-set version: %v", err)
	}
	status, maximumResponse := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            client.Schema,
		"scope_set_version": maximumSafeInteger,
		"known_scopes":      client.Scopes,
	})
	negativeStatus, negativeResponse := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            client.Schema,
		"scope_set_version": int64(-1),
		"known_scopes":      client.Scopes,
	})
	unsafeStatus, unsafeResponse := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
		"client_id":         client.ID,
		"client_generation": client.Generation,
		"platform":          "conformance",
		"app_version":       "0.3.0",
		"protocol_version":  3,
		"schema":            client.Schema,
		"scope_set_version": maximumSafeInteger + 1,
		"known_scopes":      client.Scopes,
	})
	const overflowScope = "cf:issue49-counter-overflow"
	overflowErr := func() error {
		_, err := database.ExecContext(ctx, "SELECT synchro.synchro_register_shared_scope($1, false)", overflowScope)
		return err
	}()
	var retainedVersion int64
	var overflowScopeCount int64
	if err := database.QueryRowContext(ctx, `
		SELECT client.scope_set_version,
		       (SELECT count(*) FROM synchro.sync_shared_scopes WHERE scope_id = $2)
		FROM synchro.sync_clients client
		WHERE client.user_id = 'diagnostic-user' AND client.client_id = $1`, client.ID, overflowScope).Scan(
		&retainedVersion,
		&overflowScopeCount,
	); err != nil {
		t.Fatalf("observe portable counter overflow: %v", err)
	}

	t.Run("assertion", func(t *testing.T) {
		version, ok := maximumResponse["scope_set_version"].(float64)
		if status != http.StatusOK || !ok || int64(version) != maximumSafeInteger {
			t.Fatalf("maximum safe envelope integer did not round trip exactly: status=%d response=%#v", status, maximumResponse)
		}
		requireRealProtocolError(t, negativeStatus, negativeResponse, http.StatusBadRequest, "invalid_request")
		requireRealProtocolError(t, unsafeStatus, unsafeResponse, http.StatusBadRequest, "invalid_request")
		if overflowErr == nil || retainedVersion != maximumSafeInteger || overflowScopeCount != 0 {
			t.Fatalf("server counter allocated outside the portable range: err=%v version=%d scope_count=%d", overflowErr, retainedVersion, overflowScopeCount)
		}
	})
}

func TestRealIssue49MutationLifecycleVersionsVocabularyAndCrossBatchReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-mutation-client")
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open mutation observation database: %v", err)
	}
	defer database.Close()
	rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8d01-000000000001")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8d01-000000000002")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	recordID := "00000000-0000-4000-8d01-000000000010"
	mutationID := "00000000-0000-4000-8d01-000000000011"

	insert := phase4InsertMutation(client, table, ownerField, mutationID, recordID, "issue49-insert")
	status, inserted := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8d01-000000000012",
		[]map[string]any{insert},
	))
	if status != http.StatusOK {
		t.Fatalf("insert push status = %d, want 200: %#v", status, inserted)
	}
	insertOutcome := issue49RequireAcceptedOutcome(t, inserted, mutationID, "applied")
	insertVersion := issue49RequireOpaqueVersion(t, insertOutcome)
	var insertUpdatedAt time.Time
	if err := database.QueryRowContext(ctx, "SELECT updated_at FROM public.cf_items WHERE id = $1::uuid", recordID).Scan(&insertUpdatedAt); err != nil {
		t.Fatalf("read insert updated_at: %v", err)
	}
	t.Run("assertion", func(t *testing.T) {
		serverTime, ok := inserted["server_time"].(string)
		if !ok || !issue49MicrosecondTimestamp.MatchString(serverTime) {
			t.Fatalf("push server_time is not canonical microsecond UTC: %#v", inserted["server_time"])
		}
	})

	t.Run("assertion", func(t *testing.T) {
		replayStatus, replay := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
			client,
			"00000000-0000-4000-8d01-000000000013",
			[]map[string]any{insert},
		))
		if replayStatus != http.StatusOK {
			t.Fatalf("cross-batch replay status = %d, want 200: %#v", replayStatus, replay)
		}
		replayedOutcome := issue49RequireAcceptedOutcome(t, replay, mutationID, "applied")
		if !reflect.DeepEqual(replayedOutcome, insertOutcome) {
			t.Fatalf("cross-batch replay changed stored outcome: first=%#v replay=%#v", insertOutcome, replayedOutcome)
		}
		observation, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{recordID})
		if err != nil {
			t.Fatalf("observe cross-batch replay: %v", err)
		}
		if observation.SourceRowCount != 1 || observation.AcceptedWriteEpoch != 2 {
			t.Fatalf("equal cross-batch replay repeated source work: %#v", observation)
		}
	})

	t.Run("assertion", func(t *testing.T) {
		atomicRecordID := "00000000-0000-4000-8d01-000000000015"
		atomicMutationID := "00000000-0000-4000-8d01-000000000016"
		before, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{recordID, atomicRecordID})
		if err != nil {
			t.Fatalf("observe cross-batch conflict before request: %v", err)
		}
		changed := phase4InsertMutation(client, table, ownerField, mutationID, recordID, "issue49-changed")
		conflictStatus, conflict := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
			client,
			"00000000-0000-4000-8d01-000000000014",
			[]map[string]any{
				phase4InsertMutation(client, table, ownerField, atomicMutationID, atomicRecordID, "issue49-must-not-commit"),
				changed,
			},
		))
		requireRealProtocolError(t, conflictStatus, conflict, http.StatusConflict, "idempotency_conflict")
		after, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{recordID, atomicRecordID})
		if err != nil {
			t.Fatalf("observe cross-batch conflict after request: %v", err)
		}
		if after != before {
			t.Fatalf("cross-batch identity conflict left partial request work: before=%#v after=%#v", before, after)
		}
		state, err := harness.Operator().ObserveItemStateMatch(ctx, recordID, "issue49-insert", insertVersion)
		if err != nil {
			t.Fatalf("observe changed-fingerprint state: %v", err)
		}
		if !state.Live || !state.ValueMatches || !state.VersionMatches {
			t.Fatalf("changed fingerprint modified authoritative state: %#v", state)
		}
	})

	updateMutationID := "00000000-0000-4000-8d01-000000000021"
	update := issue49Mutation(client, table, updateMutationID, recordID, "update", insertVersion, "1970-01-01T00:00:00.000000Z", map[string]any{
		table.ValueField: "issue49-update",
	})
	status, updated := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8d01-000000000022",
		[]map[string]any{update},
	))
	if status != http.StatusOK {
		t.Fatalf("update push status = %d, want 200: %#v", status, updated)
	}
	updateOutcome := issue49RequireAcceptedOutcome(t, updated, updateMutationID, "applied")
	updateVersion := issue49RequireOpaqueVersion(t, updateOutcome)
	var updateUpdatedAt time.Time
	if err := database.QueryRowContext(ctx, "SELECT updated_at FROM public.cf_items WHERE id = $1::uuid", recordID).Scan(&updateUpdatedAt); err != nil {
		t.Fatalf("read update updated_at: %v", err)
	}

	deleteMutationID := "00000000-0000-4000-8d01-000000000031"
	deleteMutation := issue49Mutation(client, table, deleteMutationID, recordID, "delete", updateVersion, "2099-12-31T23:59:59.999999Z", nil)
	status, deleted := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8d01-000000000032",
		[]map[string]any{deleteMutation},
	))
	if status != http.StatusOK {
		t.Fatalf("delete push status = %d, want 200: %#v", status, deleted)
	}
	deleteOutcome := issue49RequireAcceptedOutcome(t, deleted, deleteMutationID, "applied")
	deleteVersion := issue49RequireOpaqueVersion(t, deleteOutcome)
	var deleteUpdatedAt time.Time
	if err := database.QueryRowContext(ctx, "SELECT updated_at FROM public.cf_items WHERE id = $1::uuid", recordID).Scan(&deleteUpdatedAt); err != nil {
		t.Fatalf("read delete updated_at: %v", err)
	}

	t.Run("assertion", func(t *testing.T) {
		if !updateUpdatedAt.After(insertUpdatedAt) || !deleteUpdatedAt.After(updateUpdatedAt) {
			t.Fatalf("server updated_at did not advance independently: insert=%s update=%s delete=%s", insertUpdatedAt, updateUpdatedAt, deleteUpdatedAt)
		}
		for _, clientTime := range []string{"1970-01-01T00:00:00.000000Z", "2099-12-31T23:59:59.999999Z"} {
			parsed, err := time.Parse(time.RFC3339Nano, clientTime)
			if err != nil {
				t.Fatalf("parse diagnostic client time: %v", err)
			}
			if updateUpdatedAt.Equal(parsed) || deleteUpdatedAt.Equal(parsed) {
				t.Fatalf("client_version selected source updated_at %s", clientTime)
			}
		}
	})

	t.Run("assertion", func(t *testing.T) {
		resurrectionMutationID := "00000000-0000-4000-8d01-000000000033"
		resurrectionStatus, resurrection := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
			client,
			"00000000-0000-4000-8d01-000000000034",
			[]map[string]any{issue49Mutation(
				client,
				table,
				resurrectionMutationID,
				recordID,
				"update",
				deleteVersion,
				phase4ClientVersion,
				map[string]any{table.ValueField: "issue49-implicit-resurrection"},
			)},
		))
		if resurrectionStatus != http.StatusOK {
			t.Fatalf("implicit resurrection status = %d, want semantic rejection: %#v", resurrectionStatus, resurrection)
		}
		accepted := requireOutcomeList(t, resurrection, "accepted")
		rejected := requireOutcomeList(t, resurrection, "rejected")
		if len(accepted) != 0 || len(rejected) != 1 || rejected[0]["mutation_id"] != resurrectionMutationID ||
			rejected[0]["status"] != "conflict" || rejected[0]["code"] != "row_deleted" || rejected[0]["server_version"] != deleteVersion {
			t.Fatalf("implicit resurrection did not return the authoritative tombstone: %#v", resurrection)
		}
		state, err := harness.Operator().ObserveItemStateMatch(ctx, recordID, "issue49-update", deleteVersion)
		if err != nil {
			t.Fatalf("observe implicit resurrection rejection: %v", err)
		}
		if state.Live || !state.ValueMatches || !state.VersionMatches {
			t.Fatalf("implicit resurrection changed the authoritative tombstone: %#v", state)
		}
	})

	t.Run("assertion", func(t *testing.T) {
		versions := []string{insertVersion, updateVersion, deleteVersion}
		if insertVersion == updateVersion || updateVersion == deleteVersion || insertVersion == deleteVersion {
			t.Fatalf("accepted transitions reused server versions: %#v", versions)
		}
		for _, diagnosticTime := range []string{phase4ClientVersion, "1970-01-01T00:00:00.000000Z", "2099-12-31T23:59:59.999999Z"} {
			for _, version := range versions {
				if version == diagnosticTime {
					t.Fatalf("server version came from client_version %q", diagnosticTime)
				}
			}
		}
	})

	waitForRealWALEffects(t, ctx, harness, "cf_items", 3, recordID)
	pulled := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 100)
	changes := requireRealChanges(t, pulled)
	t.Run("assertion", func(t *testing.T) {
		deleteSeen := false
		for _, change := range changes {
			op, ok := change["op"].(string)
			if !ok || op != "upsert" && op != "delete" {
				t.Fatalf("pull emitted forbidden operation: %#v", change)
			}
			if op == "delete" {
				deleteSeen = true
			}
			issue49RequireOpaqueVersion(t, change)
			if _, ok := mutationControlChecksumDigest(change["row_checksum"]); !ok {
				t.Fatalf("pull change has invalid row checksum: %#v", change)
			}
		}
		if !deleteSeen {
			t.Fatalf("pull omitted the accepted delete: %#v", changes)
		}
	})

	for index, operation := range []string{"create", "upsert", "replace"} {
		index, operation := index, operation
		t.Run("assertion", func(t *testing.T) {
			invalid := phase4InsertMutation(
				client,
				table,
				ownerField,
				"00000000-0000-4000-8d01-00000000004"+string(rune('1'+index)),
				"00000000-0000-4000-8d01-00000000005"+string(rune('1'+index)),
				"issue49-invalid-operation",
			)
			invalid["op"] = operation
			invalidStatus, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
				client,
				"00000000-0000-4000-8d01-00000000006"+string(rune('1'+index)),
				[]map[string]any{invalid},
			))
			requireRealProtocolError(t, invalidStatus, response, http.StatusBadRequest, "invalid_request")
		})
	}

	t.Run("assertion", func(t *testing.T) {
		before, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{recordID})
		if err != nil {
			t.Fatalf("observe mutation ledgers before compaction: %v", err)
		}
		if before.BatchCount != 5 || before.MutationCount != 4 || before.SourceRowCount != 1 || before.AcceptedWriteEpoch != 4 {
			t.Fatalf("mutation lifecycle durable state is invalid: %#v", before)
		}
		if err := harness.Operator().ExpireRetentionClient(ctx, "diagnostic-user", client.ID); err != nil {
			t.Fatalf("expire mutation ledger client: %v", err)
		}
		if _, err := harness.Operator().RunDiagnosticRetentionCompaction(ctx); err != nil {
			t.Fatalf("run mutation ledger compaction: %v", err)
		}
		afterCompaction, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{recordID})
		if err != nil {
			t.Fatalf("observe mutation ledgers after compaction: %v", err)
		}
		if afterCompaction.BatchCount != before.BatchCount || afterCompaction.MutationCount != before.MutationCount {
			t.Fatalf("retention compaction deleted idempotency ledgers: before=%#v after=%#v", before, afterCompaction)
		}
		transitionRealSchemaQueue(t, ctx, harness)
		afterSchema, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, []string{recordID})
		if err != nil {
			t.Fatalf("observe mutation ledgers after schema publication: %v", err)
		}
		if afterSchema.BatchCount != before.BatchCount || afterSchema.MutationCount != before.MutationCount {
			t.Fatalf("schema publication deleted idempotency ledgers: before=%#v after=%#v", before, afterSchema)
		}
	})
}

func TestRealIssue49PortableSeedScopeContinuationAndTokenBindings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	globalID := "00000000-0000-4000-8d02-000000000001"
	privateID := "00000000-0000-4000-8d02-000000000002"
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_global_items (id, value) VALUES ($1, $2)", globalID, "issue49-portable"); err != nil {
		t.Fatalf("insert portable seed row: %v", err)
	}
	if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", privateID, "diagnostic-user", "issue49-private"); err != nil {
		t.Fatalf("insert private seed row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_global_items", globalID)
	waitForRealWALRecords(t, ctx, harness, "cf_items", privateID)

	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open portable seed database: %v", err)
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "SELECT synchro.synchro_register_shared_scope('cf:not-portable', false)"); err != nil {
		t.Fatalf("register nonportable control scope: %v", err)
	}

	t.Run("assertion", func(t *testing.T) {
		var raw []byte
		if err := database.QueryRowContext(ctx, "SELECT synchro.synchro_portable_seed_manifest(1)").Scan(&raw); err == nil {
			t.Fatal("portable seed manifest succeeded outside SERIALIZABLE READ ONLY DEFERRABLE")
		}
	})

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire portable seed connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"); err != nil {
		t.Fatalf("begin portable seed transaction: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = connection.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	manifest := issue49QueryJSONObject(t, ctx, connection, "SELECT synchro.synchro_portable_seed_manifest(1)")
	scopes, ok := manifest["portable_scopes"].([]any)
	var scope map[string]any
	t.Run("assertion", func(t *testing.T) {
		if !ok || len(scopes) != 1 {
			t.Fatalf("seed exported a nonportable scope: %#v", scopes)
		}
		scope, ok = scopes[0].(map[string]any)
		if !ok || scope["id"] != "cf:global" {
			t.Fatalf("seed exported a nonportable scope: %#v", scopes)
		}
	})
	pageToken, pageTokenOK := scope["page_token"].(string)
	receipt, receiptOK := scope["continuation"].(string)
	if !pageTokenOK || pageToken == "" || !receiptOK || receipt == "" {
		t.Fatalf("portable seed omitted page or continuation metadata: %#v", scope)
	}

	page := issue49QueryJSONObject(
		t,
		ctx,
		connection,
		"SELECT synchro.synchro_portable_seed_scope($1, $2, $3, $4, $5)",
		"cf:global",
		pageToken,
		receipt,
		int64(0),
		1,
	)
	records, ok := page["records"].([]any)
	if !ok || len(records) != 1 || page["scope"] != "cf:global" || page["has_more"] != false {
		t.Fatalf("portable seed page is invalid: %#v", page)
	}
	record, ok := records[0].(map[string]any)
	if !ok || !issue49JSONObjectContains(record["pk"], globalID) || issue49JSONObjectContains(record["pk"], privateID) {
		t.Fatalf("portable seed page contains the wrong row: %#v", record)
	}
	issue49RequireOpaqueVersion(t, record)
	if _, ok := mutationControlChecksumDigest(record["row_checksum"]); !ok {
		t.Fatalf("portable seed row checksum is invalid: %#v", record)
	}

	t.Run("assertion", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			scopeID string
			token   string
			ordinal int64
			limit   int
		}{
			{name: "scope", scopeID: "cf:not-portable", token: pageToken, ordinal: 0, limit: 1},
			{name: "token", scopeID: "cf:global", token: issue49CorruptToken(pageToken), ordinal: 0, limit: 1},
			{name: "ordinal", scopeID: "cf:global", token: pageToken, ordinal: 1, limit: 1},
			{name: "limit", scopeID: "cf:global", token: pageToken, ordinal: 0, limit: 2},
		} {
			t.Run(test.name, func(t *testing.T) {
				response := issue49QueryJSONObject(
					t,
					ctx,
					connection,
					"SELECT synchro.synchro_portable_seed_scope($1, $2, $3, $4, $5)",
					test.scopeID,
					test.token,
					receipt,
					test.ordinal,
					test.limit,
				)
				issue49RequireJSONProtocolError(t, response, "invalid_request")
			})
		}
	})

	if _, err := connection.ExecContext(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit portable seed transaction: %v", err)
	}
	committed = true

	t.Run("assertion", func(t *testing.T) {
		status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
			"client_id":         "issue49-seeded-client",
			"platform":          "conformance",
			"app_version":       "0.3.0",
			"protocol_version":  3,
			"schema":            map[string]any{"version": 0, "hash": ""},
			"scope_set_version": 0,
			"known_scopes":      map[string]any{},
			"seed_receipts":     map[string]any{"cf:global": receipt},
		})
		if status != http.StatusOK {
			t.Fatalf("seed receipt connect status = %d, want 200: %#v", status, response)
		}
		if cursor := issue49AddedScopeCursor(response, "cf:global"); cursor == "" {
			t.Fatalf("valid seed receipt did not become a client-bound cursor: %#v", response)
		}
		if cursor := issue49AddedScopeCursor(response, "user:diagnostic-user"); cursor != "" {
			t.Fatalf("private scope inferred a seed cursor from row presence: %#v", response)
		}
	})

	t.Run("assertion", func(t *testing.T) {
		status, response := postConnect(t, ctx, harness.AdapterURL(), token, map[string]any{
			"client_id":         "issue49-forged-seed-client",
			"platform":          "conformance",
			"app_version":       "0.3.0",
			"protocol_version":  3,
			"schema":            map[string]any{"version": 0, "hash": ""},
			"scope_set_version": 0,
			"known_scopes":      map[string]any{},
			"seed_receipts":     map[string]any{"cf:global": issue49CorruptToken(receipt)},
		})
		if status != http.StatusOK {
			t.Fatalf("forged seed receipt connect status = %d, want 200: %#v", status, response)
		}
		if cursor := issue49AddedScopeCursor(response, "cf:global"); cursor != "" {
			t.Fatalf("forged seed receipt produced a cursor: %#v", response)
		}
	})
}

func TestRealIssue49ConcurrentUpdateDeletePreservesOneAuthoritativeWinner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	recordID := "00000000-0000-4000-8d04-000000000001"
	if err := harness.Source().ExecContext(
		ctx,
		"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
		recordID,
		"diagnostic-user",
		"issue49-conflict-base",
	); err != nil {
		t.Fatalf("insert conflict base row: %v", err)
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", recordID)

	updateClient := connectRealProtocolClient(t, ctx, harness, token, "issue49-update-client")
	deleteClient := connectRealProtocolClient(t, ctx, harness, token, "issue49-delete-client")
	updateRecords, _ := rebuildRealScope(t, ctx, harness, token, updateClient, "user:diagnostic-user", "00000000-0000-4000-8d04-000000000002")
	deleteRecords, _ := rebuildRealScope(t, ctx, harness, token, deleteClient, "user:diagnostic-user", "00000000-0000-4000-8d04-000000000003")
	updateTable := requireRealTable(t, updateClient, "cf_items")
	deleteTable := requireRealTable(t, deleteClient, "cf_items")
	if updateTable != deleteTable {
		t.Fatal("concurrent clients received different logical table identities")
	}
	baseVersion := requireRebuildRecordVersion(t, updateRecords, updateTable, recordID, "issue49-conflict-base")
	if deleteVersion := requireRebuildRecordVersion(t, deleteRecords, deleteTable, recordID, "issue49-conflict-base"); deleteVersion != baseVersion {
		t.Fatal("concurrent clients received different base versions")
	}

	control, err := harness.Operator().HoldItemForConcurrentPush(ctx, recordID)
	if err != nil {
		t.Fatalf("hold conflict row: %v", err)
	}
	t.Cleanup(func() { _ = control.Release() })
	type result struct {
		operation string
		status    int
		response  map[string]any
		err       error
	}
	results := make(chan result, 2)
	start := func(operation string, client *realProtocolClient, batchID, mutationID, clientVersion string) {
		go func() {
			columns := map[string]any(nil)
			if operation == "update" {
				columns = map[string]any{updateTable.ValueField: "issue49-update-winner"}
			}
			status, response, requestErr := executeSyncRequest(
				ctx,
				harness.AdapterURL(),
				token,
				"/sync/push",
				phase4PushPayload(client, batchID, []map[string]any{issue49Mutation(
					client,
					updateTable,
					mutationID,
					recordID,
					operation,
					baseVersion,
					clientVersion,
					columns,
				)}),
			)
			results <- result{operation: operation, status: status, response: response, err: requestErr}
		}()
	}
	start(
		"update",
		updateClient,
		"00000000-0000-4000-8d04-000000000011",
		"00000000-0000-4000-8d04-000000000012",
		"1970-01-01T00:00:00.000000Z",
	)
	waitContext, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	err = control.WaitForBlockedPushes(waitContext, 1)
	waitCancel()
	if err != nil {
		_ = control.Release()
		t.Fatalf("observe blocked update: %v", err)
	}
	start(
		"delete",
		deleteClient,
		"00000000-0000-4000-8d04-000000000021",
		"00000000-0000-4000-8d04-000000000022",
		"2099-12-31T23:59:59.999999Z",
	)
	waitContext, waitCancel = context.WithTimeout(ctx, 10*time.Second)
	err = control.WaitForBlockedPushes(waitContext, 2)
	waitCancel()
	if err != nil {
		_ = control.Release()
		t.Fatalf("observe blocked delete: %v", err)
	}
	if err := control.Release(); err != nil {
		t.Fatalf("release conflict row: %v", err)
	}

	var accepted []result
	var rejected []result
	for range 2 {
		select {
		case outcome := <-results:
			if outcome.err != nil || outcome.status != http.StatusOK {
				t.Fatalf("concurrent %s request failed: status=%d err=%v response=%#v", outcome.operation, outcome.status, outcome.err, outcome.response)
			}
			if len(requireOutcomeList(t, outcome.response, "accepted")) == 1 {
				accepted = append(accepted, outcome)
			}
			if len(requireOutcomeList(t, outcome.response, "rejected")) == 1 {
				rejected = append(rejected, outcome)
			}
		case <-ctx.Done():
			t.Fatal("concurrent update-delete attempts did not complete")
		}
	}

	t.Run("assertion", func(t *testing.T) {
		if len(accepted) != 1 || len(rejected) != 1 {
			t.Fatalf("concurrent outcomes are not one winner and one loser: accepted=%d rejected=%d", len(accepted), len(rejected))
		}
		winner := requireOutcomeList(t, accepted[0].response, "accepted")[0]
		loser := requireOutcomeList(t, rejected[0].response, "rejected")[0]
		winnerVersion := issue49RequireOpaqueVersion(t, winner)
		if winnerVersion == baseVersion || winnerVersion == "1970-01-01T00:00:00.000000Z" || winnerVersion == "2099-12-31T23:59:59.999999Z" {
			t.Fatalf("winning transition returned an invalid server version: %q", winnerVersion)
		}
		if loser["status"] != "conflict" || loser["code"] != "version_conflict" || loser["server_version"] != winnerVersion {
			t.Fatalf("loser did not receive the current authoritative conflict: %#v", loser)
		}
		expectedValue := "issue49-conflict-base"
		expectedLive := false
		if accepted[0].operation == "update" {
			expectedValue = "issue49-update-winner"
			expectedLive = true
		} else if accepted[0].operation != "delete" {
			t.Fatalf("concurrent winner has an unknown operation: %q", accepted[0].operation)
		}
		state, err := harness.Operator().ObserveItemStateMatch(ctx, recordID, expectedValue, winnerVersion)
		if err != nil {
			t.Fatalf("observe update-delete winner: %v", err)
		}
		if state.Live != expectedLive || !state.ValueMatches || !state.VersionMatches {
			t.Fatalf("winning authoritative transition was not preserved: %#v", state)
		}
	})
}

func TestRealIssue49FirstPushResponseFailureRollsBackEveryDurableEffect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-atomicity-client")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	const mutationCount = 17
	const requestLimit = 1 << 20
	recordIDs := make([]string, 0, mutationCount)
	mutations := make([]map[string]any, 0, mutationCount)
	for index := 1; index <= mutationCount; index++ {
		recordID := fmt.Sprintf("00000000-0000-4000-8d05-%012x", index)
		mutationID := fmt.Sprintf("00000000-0000-4000-8d06-%012x", index)
		recordIDs = append(recordIDs, recordID)
		mutations = append(mutations, phase4InsertMutation(client, table, ownerField, mutationID, recordID, ""))
	}
	payload := phase4PushPayload(client, "00000000-0000-4000-8d05-000000000000", mutations)
	emptyBody, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode empty atomic response-failure request: %v", err)
	}
	available := requestLimit - len(emptyBody) - 1
	if available <= mutationCount {
		t.Fatal("response-failure request has no bounded payload capacity")
	}
	perMutation := available / mutationCount
	remainder := available % mutationCount
	for index, mutation := range mutations {
		length := perMutation
		if index < remainder {
			length++
		}
		columns := mutation["columns"].(map[string]any)
		columns[table.ValueField] = strings.Repeat("x", length)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode bounded response-failure request: %v", err)
	}
	if len(body) != requestLimit-1 {
		t.Fatalf("response-failure request size = %d, want %d", len(body), requestLimit-1)
	}

	status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", payload)
	requireRealProtocolError(t, status, response, http.StatusInternalServerError, "sync_integrity_failure")

	t.Run("assertion", func(t *testing.T) {
		observation, err := harness.Operator().ObserveDiagnosticPush(ctx, client.ID, recordIDs)
		if err != nil {
			t.Fatalf("observe failed first push: %v", err)
		}
		if observation.BatchCount != 0 || observation.MutationCount != 0 || observation.SourceRowCount != 0 || observation.AcceptedWriteEpoch != 1 {
			t.Fatalf("failed first push left partial durable work: %#v", observation)
		}
	})
}

func TestRealIssue49RebuildReplayEpochAndMonotonicCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, token := provisionRealProofHarness(t, ctx)
	client := connectRealProtocolClient(t, ctx, harness, token, "issue49-rebuild-client")
	rebuildRealScope(t, ctx, harness, token, client, "cf:global", "00000000-0000-4000-8d03-000000000001")
	table := requireRealTable(t, client, "cf_items")
	ownerField := loadRealProtocolFieldID(t, ctx, harness, "cf_items", "owner_id")
	preBoundaryIDs := []string{
		"00000000-0000-4000-8d03-000000000011",
		"00000000-0000-4000-8d03-000000000012",
		"00000000-0000-4000-8d03-000000000013",
	}
	for index, recordID := range preBoundaryIDs {
		if err := harness.Source().ExecContext(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", recordID, "diagnostic-user", "issue49-rebuild-"+string(rune('1'+index))); err != nil {
			t.Fatalf("insert rebuild row %s: %v", recordID, err)
		}
	}
	waitForRealWALRecords(t, ctx, harness, "cf_items", preBoundaryIDs...)
	before := observeCheckpointMap(t, ctx, harness, client.ID)
	rebuildID := "00000000-0000-4000-8d03-000000000020"

	requestPage := func(cursor any) (blackbox.Response, map[string]any) {
		t.Helper()
		return issue49RawSync(t, ctx, harness.AdapterURL(), token, "/sync/rebuild", map[string]any{
			"client_id":         client.ID,
			"client_generation": client.Generation,
			"schema":            client.Schema,
			"scope":             "user:diagnostic-user",
			"rebuild_id":        rebuildID,
			"cursor":            cursor,
			"limit":             1,
		})
	}
	first, firstBody := requestPage(nil)
	firstReplay, _ := requestPage(nil)
	firstCursor, ok := firstBody["cursor"].(string)
	if first.Status != http.StatusOK || !ok || firstCursor == "" || firstBody["has_more"] != true {
		t.Fatalf("first rebuild page is invalid: %#v", firstBody)
	}

	postBoundaryInsertID := "00000000-0000-4000-8d03-000000000031"
	postBoundaryDeleteID := preBoundaryIDs[0]
	postBoundaryMembershipID := preBoundaryIDs[1]
	transaction, err := harness.Source().BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin post-boundary transaction: %v", err)
	}
	if _, err := transaction.ExecContext(ctx, "INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)", postBoundaryInsertID, "diagnostic-user", "issue49-post-boundary-insert"); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("insert post-boundary row %s: %v", postBoundaryInsertID, err)
	}
	if _, err := transaction.ExecContext(ctx, "UPDATE cf_items SET deleted_at = clock_timestamp(), updated_at = clock_timestamp() WHERE id = $1", postBoundaryDeleteID); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("delete post-boundary row %s: %v", postBoundaryDeleteID, err)
	}
	if _, err := transaction.ExecContext(ctx, "UPDATE cf_items SET owner_id = $2, updated_at = clock_timestamp() WHERE id = $1", postBoundaryMembershipID, "issue49-other-user"); err != nil {
		_ = transaction.Rollback()
		t.Fatalf("change post-boundary membership for %s: %v", postBoundaryMembershipID, err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit post-boundary transaction: %v", err)
	}
	waitForRealWALEffects(t, ctx, harness, "cf_items", 6, postBoundaryInsertID, postBoundaryDeleteID, postBoundaryMembershipID)

	second, secondBody := requestPage(firstCursor)
	secondReplay, _ := requestPage(firstCursor)
	secondCursor, ok := secondBody["cursor"].(string)
	if second.Status != http.StatusOK || !ok || secondCursor == "" || secondBody["has_more"] != true {
		t.Fatalf("intermediate rebuild page is invalid: %#v", secondBody)
	}

	final, finalBody := requestPage(secondCursor)
	finalReplay, _ := requestPage(secondCursor)
	finalCursor, ok := finalBody["final_scope_cursor"].(string)
	if final.Status != http.StatusOK || !ok || finalCursor == "" || finalBody["has_more"] != false {
		t.Fatalf("final rebuild page is invalid: %#v", finalBody)
	}
	if !t.Run("assertion", func(t *testing.T) {
		issue49RequireCursorBindings(t, ctx, harness, client, finalCursor, "user:diagnostic-user")
		incrementalPayload := issue49DecodeOpaqueToken(t, finalCursor, "ic1")
		rebuildPayload := issue49DecodeRebuildToken(t, firstCursor)
		if !reflect.DeepEqual(incrementalPayload["position"], rebuildPayload["snapshot_boundary"]) {
			t.Fatalf("final rebuild cursor does not preserve the snapshot boundary: incremental=%#v rebuild=%#v", incrementalPayload, rebuildPayload)
		}
	}) {
		return
	}
	rebuildRecords := append(
		append(requireRealRebuildRecords(t, firstBody), requireRealRebuildRecords(t, secondBody)...),
		requireRealRebuildRecords(t, finalBody)...,
	)
	if len(rebuildRecords) != 3 {
		t.Fatalf("rebuild snapshot record count = %d, want 3", len(rebuildRecords))
	}
	if realRebuildRecordsContainID(rebuildRecords, table, postBoundaryInsertID) {
		t.Fatalf("post-boundary row %s entered staged snapshot", postBoundaryInsertID)
	}
	for _, recordID := range preBoundaryIDs {
		if !realRebuildRecordsContainID(rebuildRecords, table, recordID) {
			t.Fatalf("post-boundary source state changed staged row %s", recordID)
		}
	}
	if !t.Run("assertion", func(t *testing.T) {
		assertCheckpointMapsEqual(t, before, observeCheckpointMap(t, ctx, harness, client.ID))
	}) {
		return
	}

	client.Scopes["user:diagnostic-user"] = map[string]any{"cursor": finalCursor}
	presentedRebuildScopes := map[string]any{
		"cf:global":            client.Scopes["cf:global"],
		"user:diagnostic-user": client.Scopes["user:diagnostic-user"],
	}
	var selectedCursor any
	var postBoundaryChanges []map[string]any
	var checkpointAfterSelection map[string]blackbox.ClientCheckpointObservation
	for page := 0; ; page++ {
		pull := pullRealClientWithLimit(t, ctx, harness, token, client, client.Scopes, 1)
		changes := requireRealChanges(t, pull)
		if len(changes) != 1 {
			t.Fatalf("post-boundary page %d contains %d changes, want 1", page+1, len(changes))
		}
		postBoundaryChanges = append(postBoundaryChanges, changes...)
		if page == 0 {
			checkpointAfterSelection = observeCheckpointMap(t, ctx, harness, client.ID)
			if sameCheckpointPosition(before["user:diagnostic-user"], checkpointAfterSelection["user:diagnostic-user"]) {
				t.Fatal("presented final rebuild cursor did not advance the durable checkpoint")
			}
			status, repeated := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, presentedRebuildScopes, 1))
			if status != http.StatusOK || !reflect.DeepEqual(changes, requireRealChanges(t, repeated)) {
				t.Fatalf("repeated presented cursor changed its pull page: status=%d first=%#v repeated=%#v", status, pull, repeated)
			}
			assertCheckpointMapsEqual(t, checkpointAfterSelection, observeCheckpointMap(t, ctx, harness, client.ID))
			selectedCursor = client.Scopes["user:diagnostic-user"]
		}
		hasMore, ok := pull["has_more"].(bool)
		if !ok {
			t.Fatalf("post-boundary page has invalid has_more: %#v", pull)
		}
		if hasMore {
			if _, present := pull["checksums"]; present {
				t.Fatalf("nonterminal pull returned checksums: %#v", pull)
			}
			if page >= 4 {
				t.Fatalf("post-boundary pagination did not terminate: %#v", postBoundaryChanges)
			}
			continue
		}
		checksums, ok := pull["checksums"].(map[string]any)
		if !ok || len(checksums) != 2 || checksums["cf:global"] == nil || checksums["user:diagnostic-user"] == nil {
			t.Fatalf("terminal pull checksum map is not the complete active scope set: %#v", pull["checksums"])
		}
		break
	}
	if !t.Run("assertion", func(t *testing.T) {
		seen := map[string]map[string]any{}
		for _, change := range postBoundaryChanges {
			pk, ok := change["pk"].(map[string]any)
			recordID, recordOK := pk[table.PrimaryKeyField].(string)
			if !ok || !recordOK || seen[recordID] != nil {
				t.Fatalf("post-boundary pagination repeated or malformed a change: %#v", change)
			}
			seen[recordID] = change
			issue49RequireOpaqueVersion(t, change)
		}
		if len(seen) != 3 || seen[postBoundaryInsertID] == nil || seen[postBoundaryDeleteID] == nil || seen[postBoundaryMembershipID] == nil {
			t.Fatalf("post-boundary pagination skipped an insert, delete, or membership effect: %#v", seen)
		}
		if seen[postBoundaryDeleteID]["op"] != "delete" || seen[postBoundaryMembershipID]["op"] != "delete" {
			t.Fatalf("post-boundary delete or membership exit was not a deletion: %#v", seen)
		}
		if seen[postBoundaryMembershipID]["scope"] != "user:diagnostic-user" {
			t.Fatalf("membership exit used the wrong scope: %#v", seen[postBoundaryMembershipID])
		}
	}) {
		return
	}
	checkpointAfterFirstPresentation := observeCheckpointMap(t, ctx, harness, client.ID)
	if sameCheckpointPosition(checkpointAfterSelection["user:diagnostic-user"], checkpointAfterFirstPresentation["user:diagnostic-user"]) {
		t.Fatal("presented post-boundary cursors did not advance the durable checkpoint")
	}
	acknowledgeRealClientCursors(t, ctx, harness, token, client)
	acknowledged := observeCheckpointMap(t, ctx, harness, client.ID)
	if sameCheckpointPosition(checkpointAfterFirstPresentation["user:diagnostic-user"], acknowledged["user:diagnostic-user"]) {
		t.Fatal("later cursor presentation did not advance the durable checkpoint")
	}

	if !t.Run("assertion", func(t *testing.T) {
		oldScopes := map[string]any{
			"cf:global":            client.Scopes["cf:global"],
			"user:diagnostic-user": selectedCursor,
		}
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, oldScopes, 100))
		if status != http.StatusOK {
			t.Fatalf("older valid cursor status = %d, want 200: %#v", status, response)
		}
		assertCheckpointMapsEqual(t, acknowledged, observeCheckpointMap(t, ctx, harness, client.ID))
	}) {
		return
	}

	if !t.Run("assertion", func(t *testing.T) {
		currentUserCursor := client.Scopes["user:diagnostic-user"].(map[string]any)["cursor"].(string)
		for _, test := range []struct {
			name   string
			scopes map[string]any
		}{
			{
				name: "forged",
				scopes: map[string]any{
					"cf:global":            client.Scopes["cf:global"],
					"user:diagnostic-user": map[string]any{"cursor": issue49CorruptToken(currentUserCursor)},
				},
			},
			{
				name: "scope misbound",
				scopes: map[string]any{
					"cf:global":            map[string]any{"cursor": currentUserCursor},
					"user:diagnostic-user": client.Scopes["user:diagnostic-user"],
				},
			},
		} {
			if !t.Run(test.name, func(t *testing.T) {
				status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(client, test.scopes, 100))
				requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_request")
				assertCheckpointMapsEqual(t, acknowledged, observeCheckpointMap(t, ctx, harness, client.ID))
			}) {
				return
			}
		}
	}) {
		return
	}

	if !t.Run("assertion", func(t *testing.T) {
		currentUserCursor := client.Scopes["user:diagnostic-user"].(map[string]any)["cursor"].(string)
		other := connectRealProtocolClient(t, ctx, harness, token, "issue49-cursor-other-client")
		otherScopes := issue49CloneObject(t, other.Scopes)
		otherScopes["user:diagnostic-user"] = map[string]any{"cursor": currentUserCursor}
		status, response := postSync(t, ctx, harness.AdapterURL(), token, "/sync/pull", realPullPayload(other, otherScopes, 100))
		requireRealProtocolError(t, status, response, http.StatusBadRequest, "invalid_request")
	}) {
		return
	}

	epochRebuildID := "00000000-0000-4000-8d03-000000000040"
	status, epochFirst := requestRealRebuildPage(t, ctx, harness, token, client, "user:diagnostic-user", epochRebuildID, nil, 1)
	if status != http.StatusOK {
		t.Fatalf("epoch rebuild first page status = %d: %#v", status, epochFirst)
	}
	epochCursor, ok := epochFirst["cursor"].(string)
	if !ok || epochCursor == "" {
		t.Fatalf("epoch rebuild continuation is invalid: %#v", epochFirst)
	}
	writeID := "00000000-0000-4000-8d03-000000000041"
	writeMutationID := "00000000-0000-4000-8d03-000000000042"
	writeStatus, writeResponse := postSync(t, ctx, harness.AdapterURL(), token, "/sync/push", phase4PushPayload(
		client,
		"00000000-0000-4000-8d03-000000000043",
		[]map[string]any{phase4InsertMutation(client, table, ownerField, writeMutationID, writeID, "issue49-epoch-write")},
	))
	if writeStatus != http.StatusOK {
		t.Fatalf("epoch-invalidating write status = %d: %#v", writeStatus, writeResponse)
	}
	issue49RequireAcceptedOutcome(t, writeResponse, writeMutationID, "applied")

	if !t.Run("assertion", func(t *testing.T) {
		for _, cursor := range []any{nil, epochCursor} {
			replayStatus, replay := requestRealRebuildPage(t, ctx, harness, token, client, "user:diagnostic-user", epochRebuildID, cursor, 1)
			requireRealProtocolError(t, replayStatus, replay, http.StatusConflict, "rebuild_restart_required")
			if _, present := replay["records"]; present {
				t.Fatalf("stale rebuild epoch returned records: %#v", replay)
			}
		}
	}) {
		return
	}

	if !t.Run("assertion", func(t *testing.T) {
		for _, page := range []struct {
			name   string
			first  blackbox.Response
			replay blackbox.Response
		}{
			{"first", first, firstReplay},
			{"intermediate", second, secondReplay},
			{"final", final, finalReplay},
		} {
			issue49RequireExactReplay(t, page.first, page.replay, page.name)
		}
	}) {
		return
	}
}

func TestRealIssue49PublishedSchemaIdentityIsImmutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open schema observation database: %v", err)
	}
	defer database.Close()

	publishedManifest := issue49FetchSchemaManifest(t, ctx, harness.AdapterURL())
	publishedVersion, versionOK := publishedManifest["schema_version"].(float64)
	oldHash, hashOK := publishedManifest["schema_hash"].(string)
	if !versionOK || publishedVersion <= 0 || !hashOK || len(oldHash) != 64 {
		t.Fatalf("published schema identity is invalid: %#v", publishedManifest)
	}
	oldVersion := int64(publishedVersion)
	var oldBody string
	var oldClass string
	var oldFloor int64
	if err := database.QueryRowContext(ctx, `
		SELECT canonical_manifest_body, transition_class, compatibility_floor
		FROM synchro.sync_schema_manifest
		WHERE schema_version = $1 AND schema_hash = $2`, oldVersion, oldHash).Scan(&oldBody, &oldClass, &oldFloor); err != nil {
		t.Fatalf("read published schema manifest: %v", err)
	}
	if oldBody == "" || oldClass == "" || oldFloor <= 0 {
		t.Fatal("published schema omitted immutable identity fields")
	}
	t.Run("assertion", func(t *testing.T) {
		var storedBody map[string]any
		if err := json.Unmarshal([]byte(oldBody), &storedBody); err != nil {
			t.Fatalf("decode canonical manifest identity: %v", err)
		}
		publishedBody := issue49CloneObject(t, publishedManifest)
		delete(publishedBody, "schema_hash")
		if len(storedBody) != 5 || !reflect.DeepEqual(storedBody, publishedBody) {
			t.Fatalf("published manifest and hashed identity differ: stored=%#v published=%#v", storedBody, publishedBody)
		}
		digest := sha256.New()
		_, _ = digest.Write([]byte("synchro:v3:schema-manifest:v1\x00"))
		_, _ = digest.Write([]byte(oldBody))
		if fmt.Sprintf("%x", digest.Sum(nil)) != oldHash || storedBody["schema_version"] != float64(oldVersion) ||
			storedBody["transition_class"] != oldClass || storedBody["compatibility_floor"] != float64(oldFloor) ||
			storedBody["tables"] == nil {
			t.Fatalf("stored manifest hash or complete identity is invalid: hash=%s body=%#v", oldHash, storedBody)
		}
	})

	_, newTable := transitionRealSchemaQueue(t, ctx, harness)
	newVersion, newHash := realSchemaReference(t, newTable.Schema)
	if newVersion <= oldVersion || newHash == oldHash {
		t.Fatalf("schema transition did not publish a new identity: old=%d/%s new=%d/%s", oldVersion, oldHash, newVersion, newHash)
	}
	var retainedBody string
	var retainedClass string
	var retainedFloor int64
	if err := database.QueryRowContext(ctx, `
		SELECT canonical_manifest_body, transition_class, compatibility_floor
		FROM synchro.sync_schema_manifest
		WHERE schema_version = $1 AND schema_hash = $2`, oldVersion, oldHash).Scan(&retainedBody, &retainedClass, &retainedFloor); err != nil {
		t.Fatalf("read historical schema manifest: %v", err)
	}
	t.Run("assertion", func(t *testing.T) {
		if retainedBody != oldBody || retainedClass != oldClass || retainedFloor != oldFloor {
			t.Fatal("schema transition changed a published manifest in place")
		}
	})

	t.Run("assertion", func(t *testing.T) {
		if _, err := database.ExecContext(ctx, `
			UPDATE synchro.sync_schema_manifest
			SET compatibility_floor = compatibility_floor
			WHERE schema_version = $1 AND schema_hash = $2`, oldVersion, oldHash); err == nil {
			t.Fatal("published schema manifest accepted an in-place update")
		}
	})
}

func issue49Mutation(
	client *realProtocolClient,
	table realProtocolTable,
	mutationID, recordID, operation, baseVersion, clientVersion string,
	columns map[string]any,
) map[string]any {
	mutation := map[string]any{
		"mutation_id":     mutationID,
		"table":           table.ID,
		"pk":              map[string]any{table.PrimaryKeyField: recordID},
		"authored_schema": client.Schema,
		"op":              operation,
		"base_version":    baseVersion,
		"client_version":  clientVersion,
	}
	if columns != nil {
		mutation["columns"] = columns
	}
	return mutation
}

func issue49RequireAcceptedOutcome(t *testing.T, response map[string]any, mutationID, status string) map[string]any {
	t.Helper()
	accepted := requireOutcomeList(t, response, "accepted")
	rejected := requireOutcomeList(t, response, "rejected")
	if len(accepted) != 1 || len(rejected) != 0 || accepted[0]["mutation_id"] != mutationID || accepted[0]["status"] != status {
		t.Fatalf("mutation outcome partition is invalid: %#v", response)
	}
	return accepted[0]
}

func issue49RequireOpaqueVersion(t *testing.T, value map[string]any) string {
	t.Helper()
	version, ok := value["server_version"].(string)
	if !ok || !uuidPattern.MatchString(version) {
		t.Fatalf("server version is not an opaque UUID: %#v", value["server_version"])
	}
	return version
}

func issue49QueryJSONObject(t *testing.T, ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, statement string, arguments ...any) map[string]any {
	t.Helper()
	var raw []byte
	if err := queryer.QueryRowContext(ctx, statement, arguments...).Scan(&raw); err != nil {
		t.Fatalf("query JSON object: %v", err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode queried JSON object: %v", err)
	}
	return value
}

func issue49RequireJSONProtocolError(t *testing.T, response map[string]any, code string) {
	t.Helper()
	if len(response) != 1 {
		t.Fatalf("protocol error contains result fields: %#v", response)
	}
	errorBody, ok := response["error"].(map[string]any)
	if !ok || errorBody["code"] != code || errorBody["retryable"] != false {
		t.Fatalf("protocol error is invalid: %#v", response)
	}
}

func issue49JSONObjectContains(value any, expected string) bool {
	object, ok := value.(map[string]any)
	if !ok {
		return false
	}
	for _, item := range object {
		if item == expected {
			return true
		}
	}
	return false
}

func issue49CorruptToken(token string) string {
	if token == "" {
		return "x"
	}
	index := strings.LastIndexByte(token, '.') + 1
	if index >= len(token) {
		index = 0
	}
	replacement := byte('A')
	if token[index] == replacement {
		replacement = 'B'
	}
	return token[:index] + string(replacement) + token[index+1:]
}

func issue49DecodeOpaqueToken(t *testing.T, token, prefix string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != prefix {
		t.Fatalf("opaque token envelope is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode opaque token payload: %v", err)
	}
	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		t.Fatalf("decode opaque token object: %v", err)
	}
	return object
}

func issue49DecodeRebuildToken(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != "v3" || parts[1] != "rebuild" {
		t.Fatalf("rebuild token envelope is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode rebuild token payload: %v", err)
	}
	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		t.Fatalf("decode rebuild token object: %v", err)
	}
	return object
}

func issue49RequireCursorBindings(
	t *testing.T,
	ctx context.Context,
	harness *blackbox.Harness,
	client *realProtocolClient,
	token, scopeID string,
) {
	t.Helper()
	payload := issue49DecodeOpaqueToken(t, token, "ic1")
	database, err := sql.Open("pgx", harness.DatabaseURL())
	if err != nil {
		t.Fatalf("open cursor-binding database: %v", err)
	}
	defer database.Close()
	var streamGeneration string
	var membershipGeneration int64
	var retentionGeneration int64
	if err := database.QueryRowContext(ctx, `
		SELECT state.stream_generation::text,
		       state.membership_generation,
		       state.retention_generation
		FROM synchro.sync_scope_state state
		WHERE state.scope_id = $1`, scopeID).Scan(
		&streamGeneration,
		&membershipGeneration,
		&retentionGeneration,
	); err != nil {
		t.Fatalf("read cursor scope bindings: %v", err)
	}
	issuedAt, issuedOK := payload["issued_at"].(string)
	_, issuedErr := time.Parse(time.RFC3339Nano, issuedAt)
	position, positionOK := payload["position"].(map[string]any)
	if payload["kind"] != "incremental_cursor" || payload["token_version"] != float64(1) ||
		payload["key_id"] == "" || payload["stream_generation"] != streamGeneration ||
		payload["user_binding"] != "diagnostic-user" || payload["client_binding"] != client.ID ||
		payload["client_generation"] != float64(client.Generation) || payload["scope_id"] != scopeID ||
		payload["schema_hash"] != client.Schema["hash"] || payload["membership_generation"] != float64(membershipGeneration) ||
		payload["retention_generation"] != float64(retentionGeneration) || !issuedOK || issuedErr != nil ||
		!positionOK || len(position) == 0 {
		t.Fatalf("incremental cursor omitted or changed a required binding: %#v", payload)
	}
}

func issue49CloneObject(t *testing.T, source map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(source)
	if err != nil {
		t.Fatalf("encode object copy: %v", err)
	}
	var copied map[string]any
	if err := json.Unmarshal(encoded, &copied); err != nil {
		t.Fatalf("decode object copy: %v", err)
	}
	return copied
}

func issue49FetchSchemaManifest(t *testing.T, ctx context.Context, adapterURL string) map[string]any {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, adapterURL+"/sync/schema", nil)
	if err != nil {
		t.Fatalf("create schema identity request: %v", err)
	}
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("request schema identity: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 || response.StatusCode != http.StatusOK {
		t.Fatalf("read schema identity: status=%d size=%d err=%v", response.StatusCode, len(body), err)
	}
	var envelope struct {
		Manifest map[string]any `json:"manifest"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Manifest == nil {
		t.Fatalf("decode schema identity envelope: %v", err)
	}
	return envelope.Manifest
}

func issue49StartVersionedAdapter(t *testing.T, ctx context.Context, harness *blackbox.Harness, minimumVersion string) (string, func()) {
	t.Helper()
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load versioned adapter environment: %v", err)
	}
	secret, err := os.ReadFile(environment.JWTSecretFile)
	if err != nil {
		t.Fatalf("read versioned adapter secret: %v", err)
	}
	secret = []byte(strings.TrimSpace(string(secret)))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve versioned adapter address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release versioned adapter address: %v", err)
	}

	command := exec.CommandContext(ctx, environment.AdapterArtifact)
	command.Env = append(os.Environ(),
		"DATABASE_URL="+harness.DatabaseURL(),
		"JWT_SECRET="+string(secret),
		"LISTEN_ADDR="+address,
		"MIN_CLIENT_VERSION="+minimumVersion,
		"LOG_LEVEL=error",
	)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		t.Fatalf("start versioned adapter: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = command.Process.Signal(os.Interrupt)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = command.Process.Kill()
				<-done
			}
		})
	}
	URL := "http://" + address
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, URL+"/ready", nil)
		if requestErr != nil {
			stop()
			t.Fatalf("create versioned adapter readiness request: %v", requestErr)
		}
		response, requestErr := (&http.Client{Timeout: time.Second}).Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return URL, stop
			}
		}
		select {
		case processErr := <-done:
			once.Do(func() {})
			t.Fatalf("versioned adapter exited before readiness: %v", processErr)
		case <-deadline.C:
			stop()
			t.Fatal("versioned adapter readiness timed out")
		case <-ticker.C:
		case <-ctx.Done():
			stop()
			t.Fatalf("wait for versioned adapter readiness: %v", ctx.Err())
		}
	}
}

func issue49PostVersionedConnect(
	t *testing.T,
	ctx context.Context,
	adapterURL, token, appVersion, clientID string,
	protocolVersion int64,
) (int, map[string]any) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"client_id":         clientID,
		"platform":          "conformance",
		"app_version":       appVersion,
		"protocol_version":  protocolVersion,
		"schema":            map[string]any{"version": 0, "hash": ""},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	})
	if err != nil {
		t.Fatalf("encode versioned connect request: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, adapterURL+"/sync/connect", strings.NewReader(string(payload)))
	if err != nil {
		t.Fatalf("create versioned connect request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Client-Version", appVersion)
	response, err := (&http.Client{Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("send versioned connect request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		t.Fatalf("read versioned connect response: size=%d err=%v", len(body), err)
	}
	var object map[string]any
	if len(body) != 0 {
		if err := json.Unmarshal(body, &object); err != nil {
			t.Fatalf("decode versioned connect response: %v", err)
		}
	}
	return response.StatusCode, object
}

func issue49AddedScopeCursor(response map[string]any, scopeID string) string {
	delta, ok := response["scopes"].(map[string]any)
	if !ok {
		return ""
	}
	additions, ok := delta["add"].([]any)
	if !ok {
		return ""
	}
	for _, raw := range additions {
		assignment, ok := raw.(map[string]any)
		if !ok || assignment["id"] != scopeID {
			continue
		}
		cursor, _ := assignment["cursor"].(string)
		return cursor
	}
	return ""
}

func issue49RawSync(
	t *testing.T,
	ctx context.Context,
	baseURL, token, path string,
	payload map[string]any,
) (blackbox.Response, map[string]any) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encode raw sync request: %v", err)
	}
	response, err := newRealBlackboxClient(baseURL, token).Do(ctx, blackbox.Request{
		Method: http.MethodPost,
		Path:   path,
		Headers: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body:  body,
		Class: "issue49/data-semantics",
	})
	if err != nil {
		t.Fatalf("execute raw sync request: %v", err)
	}
	return response, decodeRealResponseObject(t, response.Body)
}

func issue49RequireExactReplay(t *testing.T, first, replay blackbox.Response, page string) {
	t.Helper()
	if first.Status != http.StatusOK || replay.Status != http.StatusOK {
		t.Fatalf("%s rebuild replay statuses = %d and %d, want 200", page, first.Status, replay.Status)
	}
	if err := blackbox.CompareExactReplay(first, replay); err != nil || !bytes.Equal(first.Body, replay.Body) {
		t.Fatalf("%s rebuild page was not replayed exactly: %v", page, err)
	}
}
