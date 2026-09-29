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
			update.ExtensionObjectsStateBeforeUpdate == "ok" || update.VersionAfterUpdate != release.Version ||
			!update.WorkerStableBeforeUpdate {
			t.Fatalf(
				"extension update observation is invalid: before=%q ready=%t objects=%q after=%q worker_stable=%t",
				update.VersionBeforeUpdate,
				update.ReadyBeforeUpdate,
				update.ExtensionObjectsStateBeforeUpdate,
				update.VersionAfterUpdate,
				update.WorkerStableBeforeUpdate,
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
