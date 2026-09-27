package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/trainstar/synchro/conformance/blackbox"
	"github.com/trainstar/synchro/conformance/internal/release"
)

func TestRealExtensionUpdateFromBaseline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, baselineVersion := provisionRealUpdateBaselineHarness(t, ctx)
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		t.Fatalf("sign extension update token: %v", err)
	}

	t.Run("assertion", func(t *testing.T) {
		beforeID := "00000000-0000-4000-8c07-000000000001"
		if err := harness.Source().ExecContext(
			ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
			beforeID,
			"diagnostic-user",
			"before-extension-update",
		); err != nil {
			t.Fatalf("insert source row before extension update: %v", err)
		}
		waitForRealWALRecords(t, ctx, harness, "cf_items", beforeID)

		update, err := harness.UpdateExtension(ctx)
		if err != nil {
			t.Fatalf("update extension from baseline: %v", err)
		}
		if update.VersionBeforeUpdate != baselineVersion || update.ReadyBeforeUpdate ||
			update.ExtensionObjectsStateBeforeUpdate == "ok" || update.VersionAfterUpdate != release.Version {
			t.Fatalf(
				"extension update observation is invalid: before=%q ready=%t objects=%q after=%q",
				update.VersionBeforeUpdate,
				update.ReadyBeforeUpdate,
				update.ExtensionObjectsStateBeforeUpdate,
				update.VersionAfterUpdate,
			)
		}

		catalogs, err := harness.ObserveExtensionCatalogs(ctx)
		if err != nil {
			t.Fatalf("observe extension catalogs: %v", err)
		}
		if onlyUpdated, onlyClean := extensionCatalogDifference(catalogs.Updated, catalogs.Clean); len(onlyUpdated) != 0 || len(onlyClean) != 0 {
			t.Fatalf(
				"updated extension objects differ from a clean installation: differences=%d\nonly updated:\n%s\nonly clean:\n%s",
				len(onlyUpdated)+len(onlyClean),
				strings.Join(firstLines(onlyUpdated, 20), "\n"),
				strings.Join(firstLines(onlyClean, 20), "\n"),
			)
		}
		t.Logf("extension catalog snapshot lines: updated=%d clean=%d", len(catalogs.Updated), len(catalogs.Clean))

		afterID := "00000000-0000-4000-8c07-000000000002"
		if err := harness.Source().ExecContext(
			ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, $2, $3)",
			afterID,
			"diagnostic-user",
			"after-extension-update",
		); err != nil {
			t.Fatalf("insert source row after extension update: %v", err)
		}
		waitForRealWALRecords(t, ctx, harness, "cf_items", afterID)

		client := connectRealProtocolClient(t, ctx, harness, token, "extension-update-after")
		records, _ := rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8c07-000000000011")
		table := requireRealTable(t, client, "cf_items")
		requireRebuildRecordVersion(t, records, table, beforeID, "before-extension-update")
		requireRebuildRecordVersion(t, records, table, afterID, "after-extension-update")
	})
}

// TestRealExtensionUpdateRepairsRetainedDecoderPoison proves SYNC-WAL-005 and
// SYNC-WAL-006 with a decoder poison that the update baseline creates. The
// baseline decoder rejects the Relation message that pgoutput resends after a
// valid column type change. The updated extension repairs that same source
// transaction on retry, and it then decodes another valid Relation refresh
// without a new poison or restart.
func TestRealExtensionUpdateRepairsRetainedDecoderPoison(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	harness, baselineVersion := provisionRealUpdateBaselineHarness(t, ctx)
	token, err := harness.DiagnosticBearerToken(time.Now())
	if err != nil {
		t.Fatalf("sign retained-poison update token: %v", err)
	}
	const (
		prefixID  = "00000000-0000-4000-8c07-000000000021"
		poisonID  = "00000000-0000-4000-8c07-000000000022"
		laterID   = "00000000-0000-4000-8c07-000000000023"
		warmID    = "00000000-0000-4000-8c07-000000000024"
		changedID = "00000000-0000-4000-8c07-000000000025"
	)
	values := map[string]string{
		prefixID:  "retained-poison-prefix",
		poisonID:  "retained-poison-source",
		laterID:   "retained-poison-later",
		warmID:    "retained-poison-warm",
		changedID: "retained-poison-refresh",
	}
	insertSourceRow := func(t *testing.T, recordID string) {
		t.Helper()
		if err := harness.Source().ExecContext(
			ctx,
			"INSERT INTO cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', $2)",
			recordID,
			values[recordID],
		); err != nil {
			t.Fatalf("insert retained-poison source row: %v", err)
		}
	}
	// One source transaction changes the value column type and writes a row.
	// pgoutput then resends the cf_items Relation message before that row.
	commitTypeChange := func(t *testing.T, columnType, recordID string) {
		t.Helper()
		transaction, err := openIssue49Admin(t, ctx, harness).BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin value type change: %v", err)
		}
		defer transaction.Rollback()
		if _, err := transaction.ExecContext(ctx, "ALTER TABLE public.cf_items ALTER COLUMN value TYPE "+columnType); err != nil {
			t.Fatalf("change value column type: %v", err)
		}
		if _, err := transaction.ExecContext(
			ctx,
			"INSERT INTO public.cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', $2)",
			recordID,
			values[recordID],
		); err != nil {
			t.Fatalf("insert row after value type change: %v", err)
		}
		if err := transaction.Commit(); err != nil {
			t.Fatalf("commit value type change: %v", err)
		}
	}

	insertSourceRow(t, prefixID)
	waitForRealWALRecords(t, ctx, harness, "cf_items", prefixID)
	prefix, err := harness.Operator().ObserveWALRecords(ctx, []string{prefixID})
	if err != nil || len(prefix.Records) != 1 || !prefix.AcknowledgementMatchesObservedEnd || !prefix.SlotMatchesObservedEnd {
		t.Fatalf("establish exact baseline WAL prefix: observation=%#v err=%v", prefix, err)
	}
	prefixEndLSN := prefix.Records[0].EndLSN
	// The baseline decoder has cached the text column from the prefix row.
	commitTypeChange(t, "varchar(256)", poisonID)
	insertSourceRow(t, laterID)
	before := waitForIssue49Poison(t, ctx, harness, laterID)
	beforeAcknowledgement := observeIssue49BlockedAcknowledgement(t, ctx, openIssue49Admin(t, ctx, harness), before.CommitLSN)

	t.Run("assertion", func(t *testing.T) {
		if before.FailureClass != "decode_failed" || before.CommitLSN == "" ||
			before.RelationID != "" || before.RelationIDMatchesRegistry || !before.AcknowledgementBlocked ||
			before.LaterRecordMaterialized || !before.LaterFencePending || !before.WorkerBlocked ||
			!before.ReadinessBlocked || !before.PoisonCheckFailed {
			t.Fatalf("baseline decoder did not persist a blocking decode poison: %#v", before)
		}
		if !beforeAcknowledgement.SlotMatchesProgress || !beforeAcknowledgement.ProgressBeforePoison ||
			!beforeAcknowledgement.SlotBeforePoison || beforeAcknowledgement.ProgressEndLSN != prefixEndLSN ||
			beforeAcknowledgement.SlotFlushLSN != prefixEndLSN {
			t.Fatalf("baseline slot advanced past the poisoned contiguous prefix: prefix=%s acknowledgement=%#v", prefixEndLSN, beforeAcknowledgement)
		}

		update, err := harness.ApplyExtensionUpdate(ctx)
		if err != nil {
			t.Fatalf("apply extension update over retained poison: %v", err)
		}
		if update.VersionBeforeUpdate != baselineVersion || update.VersionAfterUpdate != release.Version {
			t.Fatalf("retained-poison update versions are invalid: before=%q after=%q", update.VersionBeforeUpdate, update.VersionAfterUpdate)
		}
		afterUpdate := waitForIssue49Poison(t, ctx, harness, laterID)
		afterUpdateAcknowledgement := observeIssue49BlockedAcknowledgement(t, ctx, openIssue49Admin(t, ctx, harness), afterUpdate.CommitLSN)
		if afterUpdate.FailureClass != before.FailureClass || afterUpdate.CommitLSN != before.CommitLSN ||
			afterUpdate.RelationID != before.RelationID || !afterUpdate.AcknowledgementBlocked ||
			afterUpdate.LaterRecordMaterialized || !afterUpdate.LaterFencePending ||
			afterUpdateAcknowledgement != beforeAcknowledgement {
			t.Fatalf("retained poison changed across the extension update: before=%#v after=%#v acknowledgement=%#v/%#v",
				before, afterUpdate, beforeAcknowledgement, afterUpdateAcknowledgement)
		}

		retried, err := harness.Operator().RetryWALPoison(ctx)
		if err != nil || !retried {
			t.Fatalf("request retained poison retry: requested=%t err=%v", retried, err)
		}
		waitForRealWALRecords(t, ctx, harness, "cf_items", poisonID, laterID)
		recovery, err := harness.Operator().ObserveWALPoisonRecovery(ctx, poisonID)
		if err != nil {
			t.Fatalf("observe retained poison recovery: %v", err)
		}
		if recovery.PoisonCount != 1 || recovery.FailureClass != "decode_failed" || recovery.Lifecycle != "repaired" ||
			recovery.AttemptCount != 2 || !recovery.RetryRequested || !recovery.Resolved || !recovery.SameCommitPosition {
			t.Fatalf("retained poison did not repair the same WAL identity: %#v", recovery)
		}
		recovered, err := harness.Operator().ObserveWALRecords(ctx, []string{poisonID, laterID})
		if err != nil || len(recovered.Records) != 2 || recovered.Records[0].RecordID != poisonID ||
			recovered.Records[1].RecordID != laterID || recovered.BlockingPoison || !recovered.ContiguousAcknowledged ||
			!recovered.AcknowledgementMatchesObservedEnd || !recovered.SlotMatchesObservedEnd ||
			recovered.AcknowledgedEndLSN == "" || recovered.AcknowledgedEndLSN != recovered.SlotConfirmedFlushLSN {
			t.Fatalf("logical slot did not acknowledge the exact recovered contiguous end LSN: %#v, %v", recovered, err)
		}
		if err := harness.FinishExtensionUpdate(ctx); err != nil {
			t.Fatalf("finish extension update after poison repair: %v", err)
		}
		if retried, err := harness.Operator().RetryWALPoison(ctx); err != nil || retried {
			t.Fatalf("completed poison entered ordinary retry: requested=%t err=%v", retried, err)
		}

		// A restart loads current catalog metadata, so the repair alone does not
		// prove Relation refresh. The same warm worker must decode another change.
		insertSourceRow(t, warmID)
		waitForRealWALRecords(t, ctx, harness, "cf_items", warmID)
		restarts := harness.RestartCount()
		workerPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
		if err != nil {
			t.Fatalf("observe warm WAL worker: %v", err)
		}
		controller, err := blackbox.NewNativeController(blackbox.NativeControllerConfig{Harness: harness})
		if err != nil {
			t.Fatalf("create retained-poison WAL controller: %v", err)
		}
		resumeWAL, err := controller.PauseWALMaterialization(ctx)
		if err != nil {
			t.Fatalf("pause WAL materialization before the Relation refresh: %v", err)
		}
		walPaused := true
		defer func() {
			if !walPaused {
				return
			}
			cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if err := resumeWAL(cleanupContext); err != nil {
				t.Errorf("resume WAL materialization during cleanup: %v", err)
			}
		}()
		commitTypeChange(t, "text", changedID)
		walPaused = false
		if err := resumeWAL(ctx); err != nil {
			t.Fatalf("resume WAL materialization after the Relation refresh: %v", err)
		}
		waitForRealWALRecords(t, ctx, harness, "cf_items", changedID)
		refreshed, err := harness.Operator().ObserveWALPoisonRecovery(ctx, poisonID)
		if err != nil {
			t.Fatalf("observe poison state after the Relation refresh: %v", err)
		}
		currentPID, err := harness.Operator().CurrentWALWorkerPID(ctx)
		if err != nil || refreshed.PoisonCount != 1 || refreshed.Lifecycle != "repaired" ||
			harness.RestartCount() != restarts || currentPID != workerPID {
			t.Fatalf("valid Relation refresh required a new poison or restart: recovery=%#v restarts=%d/%d worker=%d/%d err=%v",
				refreshed, restarts, harness.RestartCount(), workerPID, currentPID, err)
		}

		client := connectRealProtocolClient(t, ctx, harness, token, "extension-update-retained-poison")
		records, _ := rebuildRealScope(t, ctx, harness, token, client, "user:diagnostic-user", "00000000-0000-4000-8c07-000000000031")
		table := requireRealTable(t, client, "cf_items")
		for _, recordID := range []string{prefixID, poisonID, laterID, warmID, changedID} {
			requireRebuildRecordVersion(t, records, table, recordID, values[recordID])
		}
	})
}

// provisionRealUpdateBaselineHarness provisions an owned instance with the
// update baseline extension. A missing baseline artifact is a setup failure.
func provisionRealUpdateBaselineHarness(t *testing.T, ctx context.Context) (*blackbox.Harness, string) {
	t.Helper()
	if !*provision || !*install {
		t.Fatal("real proof requires --provision --install")
	}
	environment, err := blackbox.LoadEnvironment()
	if err != nil {
		t.Fatalf("load extension update environment: %v", err)
	}
	baselineArtifact := os.Getenv("SYNCHRO_CONFORMANCE_UPDATE_BASELINE_EXTENSION_ARTIFACT")
	if baselineArtifact == "" {
		t.Fatal("extension update baseline artifact is unavailable")
	}
	baselineVersion := readUpdateBaselineVersion(t)
	harness, err := blackbox.Provision(ctx, blackbox.HarnessConfig{
		Environment:                     environment,
		UpdateBaselineExtensionArtifact: baselineArtifact,
		UpdateBaselineExtensionVersion:  baselineVersion,
	})
	if err != nil {
		t.Fatalf("provision extension update baseline harness: %v", err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := harness.Close(closeContext); err != nil {
			t.Errorf("close extension update harness: %v", err)
		}
	})
	return harness, baselineVersion
}

// extensionCatalogDifference returns the lines that only one sorted list has.
// It counts repeated lines.
func extensionCatalogDifference(updated, clean []string) ([]string, []string) {
	var onlyUpdated, onlyClean []string
	updatedIndex, cleanIndex := 0, 0
	for updatedIndex < len(updated) && cleanIndex < len(clean) {
		switch {
		case updated[updatedIndex] == clean[cleanIndex]:
			updatedIndex++
			cleanIndex++
		case updated[updatedIndex] < clean[cleanIndex]:
			onlyUpdated = append(onlyUpdated, updated[updatedIndex])
			updatedIndex++
		default:
			onlyClean = append(onlyClean, clean[cleanIndex])
			cleanIndex++
		}
	}
	onlyUpdated = append(onlyUpdated, updated[updatedIndex:]...)
	onlyClean = append(onlyClean, clean[cleanIndex:]...)
	return onlyUpdated, onlyClean
}

func firstLines(lines []string, limit int) []string {
	if len(lines) > limit {
		return lines[:limit]
	}
	return lines
}

var extensionVersionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

func readUpdateBaselineVersion(t *testing.T) string {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(repoRoot, "extensions", "synchro-pg", "update-baseline.json"))
	if err != nil {
		t.Fatalf("read extension update baseline: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var fields map[string]json.RawMessage
	if err := decoder.Decode(&fields); err != nil || fields == nil {
		t.Fatal("extension update baseline is not one JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatal("extension update baseline contains trailing data")
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "artifact_sha256,artifact_url,version" {
		t.Fatalf("extension update baseline keys = %v, want artifact_sha256, artifact_url, and version", keys)
	}
	var version string
	if err := json.Unmarshal(fields["version"], &version); err != nil || !extensionVersionPattern.MatchString(version) {
		t.Fatal("extension update baseline version is not in X.Y.Z form")
	}
	return version
}

type extensionUpdatePath struct {
	source  string
	target  string
	hasPath bool
}

func readExtensionUpdatePaths(t *testing.T, ctx context.Context, database *sql.DB) []extensionUpdatePath {
	t.Helper()
	rows, err := database.QueryContext(ctx, `
		SELECT source, target, path IS NOT NULL
		FROM pg_catalog.pg_extension_update_paths('synchro_pg')`)
	if err != nil {
		t.Fatalf("observe extension update paths: %v", err)
	}
	defer rows.Close()
	var paths []extensionUpdatePath
	for rows.Next() {
		var path extensionUpdatePath
		if err := rows.Scan(&path.source, &path.target, &path.hasPath); err != nil {
			t.Fatalf("read extension update path: %v", err)
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read extension update paths: %v", err)
	}
	return paths
}

// extensionUpdatePathViolation returns an empty string when the update paths
// form one valid chain from the baseline to the current version.
func extensionUpdatePathViolation(paths []extensionUpdatePath, baseline, current string) string {
	baselineNumber, baselineValid := parseExtensionVersion(baseline)
	currentNumber, currentValid := parseExtensionVersion(current)
	if !baselineValid || !currentValid {
		return fmt.Sprintf("baseline %q or current version %q is not in X.Y.Z form", baseline, current)
	}
	known := make(map[string]struct{})
	reachesCurrent := make(map[string]bool)
	for _, path := range paths {
		known[path.source] = struct{}{}
		known[path.target] = struct{}{}
		if path.target == current && path.hasPath {
			reachesCurrent[path.source] = true
		}
	}
	if current == baseline {
		if len(known) != 0 {
			return fmt.Sprintf("current version %s is the baseline, but %d versions are known", current, len(known))
		}
		return ""
	}
	if _, ok := known[baseline]; !ok {
		return fmt.Sprintf("baseline version %s is not known", baseline)
	}
	if _, ok := known[current]; !ok {
		return fmt.Sprintf("current version %s is not known", current)
	}
	versions := make([]string, 0, len(known))
	for version := range known {
		versions = append(versions, version)
	}
	sort.Strings(versions)
	for _, version := range versions {
		number, valid := parseExtensionVersion(version)
		if !valid {
			return fmt.Sprintf("known version %q is not in X.Y.Z form", version)
		}
		if compareExtensionVersions(number, baselineNumber) < 0 {
			return fmt.Sprintf("known version %s is below baseline %s", version, baseline)
		}
		if compareExtensionVersions(number, currentNumber) > 0 {
			return fmt.Sprintf("known version %s is above current version %s", version, current)
		}
		if compareExtensionVersions(number, currentNumber) < 0 && !reachesCurrent[version] {
			return fmt.Sprintf("known version %s has no update path to current version %s", version, current)
		}
	}
	return ""
}

func parseExtensionVersion(version string) ([3]int, bool) {
	var number [3]int
	if !extensionVersionPattern.MatchString(version) {
		return number, false
	}
	for index, part := range strings.Split(version, ".") {
		value, err := strconv.Atoi(part)
		if err != nil {
			return number, false
		}
		number[index] = value
	}
	return number, true
}

func compareExtensionVersions(left, right [3]int) int {
	for index := range left {
		if left[index] != right[index] {
			if left[index] < right[index] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func TestExtensionUpdatePathViolation(t *testing.T) {
	tests := []struct {
		name      string
		paths     []extensionUpdatePath
		baseline  string
		current   string
		violation bool
	}{
		{
			name: "valid chain",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2", hasPath: true},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
			},
			baseline: "0.3.1",
			current:  "0.3.2",
		},
		{
			name: "known version below baseline",
			paths: []extensionUpdatePath{
				{source: "0.3.0", target: "0.3.1", hasPath: true},
				{source: "0.3.0", target: "0.3.2", hasPath: true},
				{source: "0.3.1", target: "0.3.0", hasPath: false},
				{source: "0.3.1", target: "0.3.2", hasPath: true},
				{source: "0.3.2", target: "0.3.0", hasPath: false},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
			},
			baseline:  "0.3.1",
			current:   "0.3.2",
			violation: true,
		},
		{
			name: "missing path to current version",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2", hasPath: false},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
			},
			baseline:  "0.3.1",
			current:   "0.3.2",
			violation: true,
		},
		{
			name: "known version above current version",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2", hasPath: true},
				{source: "0.3.1", target: "0.3.10", hasPath: true},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
				{source: "0.3.2", target: "0.3.10", hasPath: true},
				{source: "0.3.10", target: "0.3.1", hasPath: false},
				{source: "0.3.10", target: "0.3.2", hasPath: true},
			},
			baseline:  "0.3.1",
			current:   "0.3.2",
			violation: true,
		},
		{
			name:      "no versions after baseline",
			baseline:  "0.3.1",
			current:   "0.3.2",
			violation: true,
		},
		{
			name:     "no versions at baseline",
			baseline: "0.3.1",
			current:  "0.3.1",
		},
		{
			name: "baseline version not known",
			paths: []extensionUpdatePath{
				{source: "0.3.2", target: "0.3.3", hasPath: true},
				{source: "0.3.3", target: "0.3.2", hasPath: false},
			},
			baseline:  "0.3.1",
			current:   "0.3.3",
			violation: true,
		},
		{
			name: "versions known at baseline",
			paths: []extensionUpdatePath{
				{source: "0.3.1", target: "0.3.2", hasPath: true},
				{source: "0.3.2", target: "0.3.1", hasPath: false},
			},
			baseline:  "0.3.1",
			current:   "0.3.1",
			violation: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			violation := extensionUpdatePathViolation(test.paths, test.baseline, test.current)
			if (violation != "") != test.violation {
				t.Fatalf("violation = %q, want violation %t", violation, test.violation)
			}
		})
	}
}
