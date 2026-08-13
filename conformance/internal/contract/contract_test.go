package contract

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate contract test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func TestLoadRealAuthoredCatalogs(t *testing.T) {
	bundle, err := Load(context.Background(), repositoryRoot(t))
	if err != nil {
		t.Fatalf("load authored catalogs: %v", err)
	}
	if got := len(bundle.Requirements.Requirements); got != 111 {
		t.Fatalf("requirement count = %d, want 111", got)
	}
	if got := len(bundle.Faults.Controls); got != 111 {
		t.Fatalf("control count = %d, want 111", got)
	}
}

func TestBundleValidateUsesFreshBackgroundContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bundle, err := Load(ctx, repositoryRoot(t))
	if err != nil {
		t.Fatalf("load authored catalogs: %v", err)
	}
	cancel()
	if err := bundle.Validate(); err != nil {
		t.Fatalf("validate loaded bundle after caller context cancellation: %v", err)
	}
}

func TestBuildSnapshotUsesCompleteRealContract(t *testing.T) {
	snapshot, err := BuildSnapshot(context.Background(), repositoryRoot(t))
	if err != nil {
		t.Fatalf("build real contract snapshot: %v", err)
	}
	if got, want := snapshot.SchemaFiles.RCCandidateLock.Path, "conformance/schemas/rc-candidate-lock-v1.schema.json"; got != want {
		t.Fatalf("candidate-lock schema path = %q, want %q", got, want)
	}
	if !isLowerSHA256(snapshot.SchemaFiles.RCCandidateLock.SHA256) {
		t.Fatal("candidate-lock schema binding does not contain a lowercase SHA-256")
	}
}

func TestSnapshotDigestBindsCandidateLockSchemaBytes(t *testing.T) {
	root := completeSnapshotFixture(t)
	before, err := BuildSnapshot(context.Background(), root)
	if err != nil {
		t.Fatalf("build initial fixture snapshot: %v", err)
	}
	beforeDigest, err := before.SHA256()
	if err != nil {
		t.Fatalf("digest initial fixture snapshot: %v", err)
	}
	path := "conformance/schemas/rc-candidate-lock-v1.schema.json"
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		t.Fatalf("read candidate-lock schema fixture: %v", err)
	}
	writeFixtureFile(t, root, path, append(data, '\n'))
	after, err := BuildSnapshot(context.Background(), root)
	if err != nil {
		t.Fatalf("build changed fixture snapshot: %v", err)
	}
	afterDigest, err := after.SHA256()
	if err != nil {
		t.Fatalf("digest changed fixture snapshot: %v", err)
	}
	if beforeDigest == afterDigest {
		t.Fatal("candidate-lock schema byte change reused the prior snapshot digest")
	}
	if before.SchemaFiles.RCCandidateLock.SHA256 == after.SchemaFiles.RCCandidateLock.SHA256 {
		t.Fatal("candidate-lock schema byte change reused the prior file digest")
	}
}

func TestBundleSemanticMutantsFailClosed(t *testing.T) {
	root := repositoryRoot(t)
	newBundle := func(t *testing.T) *Bundle {
		t.Helper()
		bundle, err := Load(context.Background(), root)
		if err != nil {
			t.Fatalf("load baseline bundle: %v", err)
		}
		return bundle
	}

	t.Run("duplicate and missing requirement or control", func(t *testing.T) {
		bundle := newBundle(t)
		bundle.Requirements.Requirements = append(bundle.Requirements.Requirements, bundle.Requirements.Requirements[0])
		requireErrorContains(t, bundle.Validate(), "duplicate requirement ID")

		bundle = newBundle(t)
		bundle.Requirements.Requirements = bundle.Requirements.Requirements[1:]
		requireErrorContains(t, bundle.Validate(), "exactly 111 records")

		bundle = newBundle(t)
		bundle.Faults.Controls = append(bundle.Faults.Controls, bundle.Faults.Controls[0])
		requireErrorContains(t, bundle.Validate(), "duplicate control ID")

		bundle = newBundle(t)
		bundle.Faults.Controls = bundle.Faults.Controls[1:]
		requireErrorContains(t, bundle.Validate(), "controls must contain exactly 111")
	})

	t.Run("invariant references require canonical H3 anchors", func(t *testing.T) {
		bundle := newBundle(t)
		bundle.Requirements.Requirements = append([]Requirement(nil), bundle.Requirements.Requirements...)
		bundle.Requirements.Requirements[0].NormativeReferences = []NormativeReference{{Path: invariantsPath, Anchor: "#purpose"}}
		requireErrorContains(t, bundle.Validate(), "level-three invariant")

		bundle = newBundle(t)
		bundle.Requirements.Requirements = append([]Requirement(nil), bundle.Requirements.Requirements...)
		bundle.Requirements.Requirements[0].NormativeReferences = []NormativeReference{{Path: invariantsPath, Anchor: "#missing-anchor"}}
		requireErrorContains(t, bundle.Validate(), "does not name an ATX")

		bundle = newBundle(t)
		bundle.Requirements.Requirements = append([]Requirement(nil), bundle.Requirements.Requirements...)
		bundle.Requirements.Requirements[0].NormativeReferences = []NormativeReference{{Path: "docs/src/content/docs/spec/../spec/04-invariants.mdx", Anchor: "#canonical-time-format"}}
		requireErrorContains(t, bundle.Validate(), "path is not canonical")

		bundle = newBundle(t)
		bundle.Requirements.Requirements = append([]Requirement(nil), bundle.Requirements.Requirements...)
		bundle.Requirements.Requirements[0].NormativeReferences = []NormativeReference{{Path: "docs/src/content/docs/spec/06-outside.mdx", Anchor: "#outside"}}
		requireErrorContains(t, bundle.Validate(), "not a frozen behavioral file")
	})

	t.Run("normative references cannot escape through symlinks", func(t *testing.T) {
		fixture := completeSnapshotFixture(t)
		target := filepath.Join(t.TempDir(), "outside.mdx")
		if err := os.WriteFile(target, []byte("# Outside\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(fixture, filepath.FromSlash(invariantsPath))
		if err := os.Remove(link); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		_, err := Load(context.Background(), fixture)
		requireErrorContains(t, err, "open rooted repository file")
	})

	t.Run("applicability and locked support tuples", func(t *testing.T) {
		bundle := newBundle(t)
		bundle.Requirements.Requirements = append([]Requirement(nil), bundle.Requirements.Requirements...)
		proofs := append([]string(nil), bundle.Requirements.Requirements[0].RequiredProofTypes...)
		bundle.Requirements.Requirements[0].RequiredProofTypes = removeString(proofs, "server-black-box")
		requireErrorContains(t, bundle.Validate(), "server-black-box applicability mismatch")

		bundle = newBundle(t)
		bundle.Support.Cells = append([]SupportCell(nil), bundle.Support.Cells...)
		bundle.Support.Cells[0].ID = "SUP-PG-999"
		requireErrorContains(t, bundle.Validate(), "unexpected stable ID")

		bundle = newBundle(t)
		bundle.Support.Cells = append([]SupportCell(nil), bundle.Support.Cells...)
		bundle.Support.Cells[4].Policy = "excluded"
		requireErrorContains(t, bundle.Validate(), "locked semantic tuple")
	})

	t.Run("artifact IDs and roles are locked", func(t *testing.T) {
		bundle := newBundle(t)
		bundle.Artifacts.Artifacts = append([]ArtifactInventoryItem(nil), bundle.Artifacts.Artifacts...)
		bundle.Artifacts.Artifacts[0].Role, bundle.Artifacts.Artifacts[1].Role = bundle.Artifacts.Artifacts[1].Role, bundle.Artifacts.Artifacts[0].Role
		requireErrorContains(t, bundle.Validate(), "has role")

		bundle = newBundle(t)
		bundle.Artifacts.Artifacts = append([]ArtifactInventoryItem(nil), bundle.Artifacts.Artifacts...)
		bundle.Artifacts.Artifacts[0].ID = "ARTDEF-PG-EXTENSION-001"
		requireErrorContains(t, bundle.Validate(), "missing ARTDEF-CONFORMANCE-RUNNER-001")
	})

	t.Run("performance snapshot and references reject drift", func(t *testing.T) {
		for _, mutate := range []struct {
			name   string
			mutate func(*Bundle)
			want   string
		}{
			{"metric", func(b *Bundle) { b.Performance.Budgets[0].Metric = "rebuild_pull_http_requests" }, "locked metric"},
			{"comparator", func(b *Bundle) { b.Performance.Budgets[0].Comparator = "lte" }, "locked metric"},
			{"limit", func(b *Bundle) { b.Performance.Budgets[0].Limit = "2" }, "locked metric"},
			{"nested stratum parameter", func(b *Bundle) {
				b.Performance.RequiredMeasurements[8].Strata[0].Parameters = []byte(`{"bound_family":"fanout","boundary":"changed"}`)
			}, "locked v0.3.0 semantic snapshot"},
			{"unknown support", func(b *Bundle) { b.Performance.Budgets[0].SupportCellIDs = []SupportCellID{"SUP-PG-014"} }, "unknown or excluded support"},
			{"unknown artifact", func(b *Bundle) {
				b.Performance.Budgets[0].ArtifactInventoryIDs = []ArtifactInventoryID{"ARTDEF-NOT-REAL-001"}
			}, "unknown artifact"},
			{"duplicate metric", func(b *Bundle) {
				b.Performance.RequiredMeasurements[0].Metrics = append(b.Performance.RequiredMeasurements[0].Metrics, b.Performance.RequiredMeasurements[0].Metrics[0])
			}, "duplicate metric ID"},
			{"duplicate stratum", func(b *Bundle) {
				b.Performance.RequiredMeasurements[0].Strata = append(b.Performance.RequiredMeasurements[0].Strata, b.Performance.RequiredMeasurements[0].Strata[0])
			}, "duplicate stratum ID"},
			{"forbidden nested claim", func(b *Bundle) {
				b.Performance.RequiredMeasurements[0].DataProfile.Parameters = []byte(`{"nested":{"READY":true}}`)
			}, "forbidden readiness claim key"},
		} {
			t.Run(mutate.name, func(t *testing.T) {
				bundle := newBundle(t)
				bundle.Performance.Budgets = append([]PerformanceBudget(nil), bundle.Performance.Budgets...)
				bundle.Performance.RequiredMeasurements = append([]RequiredMeasurement(nil), bundle.Performance.RequiredMeasurements...)
				mutate.mutate(bundle)
				requireErrorContains(t, bundle.Validate(), mutate.want)
			})
		}
	})
}

func TestMarkdownHeadingAnchorsMatchPublishedContract(t *testing.T) {
	for _, test := range []struct {
		name    string
		source  string
		anchors []string
		levels  []int
	}{
		{"inline links", "### [Link Label](https://example.test)\n### ![Image Label](image.png)\n### [Reference Label][ref]\n", []string{"link-label", "image-label", "reference-label"}, []int{3, 3, 3}},
		{"HTML and formatting", "### <span>HTML</span> **Bold** _Code_ ~Gone~ `Token`\n", []string{"html-bold-code-gone-token"}, []int{3}},
		{"escaped punctuation", "### A\\! B\\- C\n", []string{"a-b--c"}, []int{3}},
		{"Unicode punctuation and emoji", "### Café — emoji 😀\n", []string{"café-emoji-"}, []int{3}},
		{"ECMAScript full lowercase Turkish I", "### İ\n", []string{"i̇"}, []int{3}},
		{"whitespace run", "### Many   spaces\n", []string{"many-spaces"}, []int{3}},
		{"fences", "```md\n### ignored\n```\n### shown\n````\n### ignored-again\n```\n### after\n", []string{"shown", "after"}, []int{3, 3}},
		{"four-space non-fence input", "    ### not-a-heading\n   ### selected\n", []string{"selected"}, []int{3}},
		{"global duplicates", "# Repeat\n## Repeat\n### Repeat\n", []string{"repeat", "repeat-1", "repeat-2"}, []int{1, 2, 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			headings := parseMarkdownHeadings(test.source)
			if got := headingAnchors(headings); !equalStrings(got, test.anchors) {
				t.Fatalf("anchors = %#v, want %#v", got, test.anchors)
			}
			for index, heading := range headings {
				if heading.level != test.levels[index] {
					t.Fatalf("heading %d level = %d, want %d", index, heading.level, test.levels[index])
				}
			}
			if got, want := headingAnchorsAtLevel(headings, 3), expectedAnchorsAtLevel(test.anchors, test.levels, 3); !equalStrings(got, want) {
				t.Fatalf("level-three anchors = %#v, want %#v", got, want)
			}
		})
	}
}

func TestECMAScriptLowercaseExpandsU0130(t *testing.T) {
	if got, want := githubSlug("İ"), "i̇"; got != want {
		t.Fatalf("githubSlug(U+0130) = %q, want %q", got, want)
	}
}

func TestBundleValidatesFrozenNormativeReferences(t *testing.T) {
	bundle, err := Load(context.Background(), repositoryRoot(t))
	if err != nil {
		t.Fatalf("load authored contract: %v", err)
	}
	valid := bundle.Requirements.Requirements[0].NormativeReferences
	if err := bundle.ValidateNormativeReferences(valid); err != nil {
		t.Fatalf("validate authored normative reference: %v", err)
	}
	for _, references := range [][]NormativeReference{
		{{Path: "docs/src/content/docs/spec/not-frozen.mdx", Anchor: "#missing"}},
		{{Path: valid[0].Path, Anchor: "#missing"}},
	} {
		if err := bundle.ValidateNormativeReferences(references); err == nil {
			t.Fatal("invalid normative reference was accepted")
		}
	}
}

func TestPerformanceDigestUsesJavaScriptParseAndStringifySemantics(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  string
	}{
		{"zero", `{"n":0}`, `{"n":0}`},
		{"decimal zero", `{"n":0.0}`, `{"n":0}`},
		{"exponent zero", `{"n":0e0}`, `{"n":0}`},
		{"negative zero", `{"n":-0}`, `{"n":0}`},
		{"escaped string", `{"text":"\u0061"}`, `{"text":"a"}`},
		{"object ordering", `{"z":0,"10":"ten","2":"two","01":"leading","a":"a"}`, `{"2":"two","10":"ten","z":0,"01":"leading","a":"a"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := javascriptJSONNormalize([]byte(test.input))
			if err != nil {
				t.Fatalf("normalize %s: %v", test.name, err)
			}
			if string(got) != test.want {
				t.Fatalf("normalized JSON = %s, want %s", got, test.want)
			}
		})
	}
	if _, err := javascriptJSONNormalize([]byte(`{"n":0,"n":0.0}`)); err == nil {
		t.Fatal("normalization accepted duplicate JSON keys")
	}
	if _, err := javascriptJSONNormalize([]byte(`{"text":"\ud800"}`)); err == nil {
		t.Fatal("normalization accepted a lone UTF-16 surrogate")
	}
	bundle, err := Load(context.Background(), repositoryRoot(t))
	if err != nil {
		t.Fatalf("load authored performance catalog: %v", err)
	}
	digest, err := bundle.performanceCatalogDigest()
	if err != nil {
		t.Fatalf("digest authored performance catalog: %v", err)
	}
	if digest != lockedPerformanceDigest {
		t.Fatalf("performance digest = %s, want %s", digest, lockedPerformanceDigest)
	}
	bundle.Performance.Budgets = append([]PerformanceBudget(nil), bundle.Performance.Budgets...)
	bundle.Performance.Budgets[0].Limit = "1.0"
	if err := bundle.Validate(); err != nil {
		t.Fatalf("semantic validation did not normalize an equivalent numeric limit: %v", err)
	}
}

func TestLoadRejectsRawPerformancePropertyOrderDrift(t *testing.T) {
	root := completeSnapshotFixture(t)
	performancePath := filepath.Join(root, "conformance", "performance", "budgets.json")
	data, err := os.ReadFile(performancePath)
	if err != nil {
		t.Fatal(err)
	}
	old := `"id": "BUD-WARM-CONNECT-001",
      "scenario_id": "SCN-PERF-WARM-CONNECT-001",`
	new := `"scenario_id": "SCN-PERF-WARM-CONNECT-001",
      "id": "BUD-WARM-CONNECT-001",`
	mutated := strings.Replace(string(data), old, new, 1)
	if mutated == string(data) {
		t.Fatal("locate first raw performance budget fields")
	}
	writeFixtureFile(t, root, "conformance/performance/budgets.json", []byte(mutated))
	_, err = Load(context.Background(), root)
	requireErrorContains(t, err, "locked v0.3.0 semantic snapshot")
}

func TestSnapshotFixtureIsDeterministicAndRejectsBindingMutants(t *testing.T) {
	root := completeSnapshotFixture(t)
	first, err := BuildSnapshot(context.Background(), root)
	if err != nil {
		t.Fatalf("build fixture snapshot: %v", err)
	}
	firstBytes, err := first.CanonicalBytes()
	if err != nil {
		t.Fatalf("canonicalize fixture snapshot: %v", err)
	}
	firstDigest, err := first.SHA256()
	if err != nil {
		t.Fatalf("digest fixture snapshot: %v", err)
	}
	second, err := BuildSnapshot(context.Background(), root)
	if err != nil {
		t.Fatalf("build repeated fixture snapshot: %v", err)
	}
	secondBytes, err := second.CanonicalBytes()
	if err != nil {
		t.Fatalf("canonicalize repeated fixture snapshot: %v", err)
	}
	secondDigest, err := second.SHA256()
	if err != nil {
		t.Fatalf("digest repeated fixture snapshot: %v", err)
	}
	if string(firstBytes) != string(secondBytes) || firstDigest != secondDigest {
		t.Fatal("equivalent snapshot builds were not deterministic")
	}
	if want := sha256.Sum256(firstBytes); firstDigest != want {
		t.Fatal("Snapshot.SHA256 did not hash CanonicalBytes")
	}
	rawRequirements, err := os.ReadFile(filepath.Join(root, "conformance", "requirements.json"))
	if err != nil {
		t.Fatalf("read raw requirements binding: %v", err)
	}
	rawDigest := sha256.Sum256(rawRequirements)
	if first.Requirements.SHA256 != hex.EncodeToString(rawDigest[:]) {
		t.Fatal("requirements binding did not hash exact raw bytes")
	}
	var topLevel map[string]json.RawMessage
	if err := json.Unmarshal(firstBytes, &topLevel); err != nil {
		t.Fatalf("decode canonical snapshot: %v", err)
	}
	if got, want := mapKeys(topLevel), []string{"behavioral_files", "protocol_version", "release_version", "requirements", "schema_files", "support_matrix", "verification_inputs"}; !equalStrings(got, want) {
		t.Fatalf("snapshot top-level keys = %#v, want %#v", got, want)
	}
	requireJSONKeys(t, topLevel["requirements"], "path", "sha256")
	var behavioral []json.RawMessage
	if err := json.Unmarshal(topLevel["behavioral_files"], &behavioral); err != nil {
		t.Fatalf("decode behavioral bindings: %v", err)
	}
	if len(behavioral) != 11 {
		t.Fatalf("behavioral binding count = %d, want 11", len(behavioral))
	}
	requireJSONKeys(t, behavioral[0], "path", "sha256", "status")
	requireJSONKeys(t, topLevel["verification_inputs"], "artifact_inventory", "fault_catalog", "performance_budgets", "scenario_catalog", "vector_catalog")
	requireJSONKeys(t, topLevel["schema_files"], "artifact_inventory", "evidence", "fault_catalog", "performance_budgets", "rc_candidate_lock", "rc_manifest", "requirements", "scenario", "support_matrix", "vector_catalog")
	if got, want := snapshotBindingPaths(first), expectedSnapshotBindingPaths(); !equalStringSets(got, want) || len(got) != 28 {
		t.Fatalf("snapshot binding paths = %#v, want all 28 %#v", got, want)
	}
	lastIndex := -1
	for _, key := range []string{"behavioral_files", "protocol_version", "release_version", "requirements", "schema_files", "support_matrix", "verification_inputs"} {
		index := strings.Index(string(firstBytes), `"`+key+`":`)
		if index < 0 || index <= lastIndex {
			t.Fatalf("canonical snapshot keys are not alphabetically ordered near %q", key)
		}
		lastIndex = index
	}

	writeFixtureFile(t, root, "conformance/catalog.json", []byte(`{ `))
	if _, err := BuildSnapshot(context.Background(), root); err == nil {
		t.Fatal("BuildSnapshot accepted a changed scenario catalog")
	}

	for _, mutate := range []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{"reorder", func(s *Snapshot) {
			s.BehavioralFiles[0], s.BehavioralFiles[1] = s.BehavioralFiles[1], s.BehavioralFiles[0]
		}},
		{"remove", func(s *Snapshot) { s.BehavioralFiles = s.BehavioralFiles[1:] }},
		{"duplicate", func(s *Snapshot) { s.BehavioralFiles = append(s.BehavioralFiles, s.BehavioralFiles[0]) }},
		{"path substitution", func(s *Snapshot) { s.Requirements.Path = "conformance/other.json" }},
		{"invalid hash", func(s *Snapshot) { s.Requirements.SHA256 = "not-a-sha256" }},
		{"release", func(s *Snapshot) { s.ReleaseVersion = "0.3.1" }},
		{"protocol", func(s *Snapshot) { s.ProtocolVersion = 4 }},
		{"spec status", func(s *Snapshot) { value := "Accepted"; s.BehavioralFiles[0].Status = &value }},
		{"direct ADR status", func(s *Snapshot) { value := "Rejected"; s.BehavioralFiles[6].Status = &value }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			mutated := first
			mutated.BehavioralFiles = append([]BehavioralBinding(nil), first.BehavioralFiles...)
			mutate.mutate(&mutated)
			if err := mutated.Validate(); err == nil {
				t.Fatalf("Snapshot.Validate accepted %s mutation", mutate.name)
			}
		})
	}
}

func TestAcceptedADRStatusRejectsAmbiguousFrontmatter(t *testing.T) {
	for _, test := range []struct {
		name    string
		content string
		valid   bool
	}{
		{"unquoted", "---\nstatus: Accepted\n---\nbody\n", true},
		{"quoted", "---\nstatus: \"Accepted\"\n---\nbody\n", true},
		{"quoted escaped key and value", "---\n\"sta\\u0074us\": \"Acc\\u0065pted\"\n---\n", true},
		{"duplicate quoted key", "---\nstatus: Accepted\n\"sta\\u0074us\": Rejected\n---\n", false},
		{"invalid scalar escape", "---\nstatus: \"Accept\\qed\"\n---\n", false},
		{"invalid key escape", "---\n\"sta\\q tus\": Accepted\n---\n", false},
		{"invalid Unicode escape", "---\nstatus: \"Accept\\uD800ed\"\n---\n", false},
		{"alias", "---\nstatus: *accepted\n---\n", false},
		{"nested status", "---\nmetadata:\n  status: Accepted\n---\n", false},
		{"collection", "---\nstatus: [Accepted]\n---\n", false},
		{"multiline", "---\nstatus: |\n  Accepted\n---\n", false},
		{"malformed key", "---\n\"status: Accepted\n---\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			status, err := acceptedADRStatus([]byte(test.content))
			if test.valid {
				if err != nil || status != "Accepted" {
					t.Fatalf("acceptedADRStatus = %q, %v", status, err)
				}
				return
			}
			if err == nil {
				t.Fatal("acceptedADRStatus accepted malformed frontmatter")
			}
		})
	}
}

func TestCapturedSnapshotRecheckRejectsChangedBinding(t *testing.T) {
	root := completeSnapshotFixture(t)
	_, rootDescriptor, err := openRepositoryRoot(root)
	if err != nil {
		t.Fatalf("open repository root: %v", err)
	}
	t.Cleanup(func() { _ = rootDescriptor.Close() })
	captured, err := captureSnapshotFiles(context.Background(), rootDescriptor)
	if err != nil {
		t.Fatalf("capture snapshot files: %v", err)
	}
	writeFixtureFile(t, root, "conformance/catalog.json", []byte(`{ `))
	err = verifyCapturedSnapshotFiles(context.Background(), rootDescriptor, captured)
	requireErrorContains(t, err, "changed during construction")
}

func TestOpenRepositoryRootPinsDirectoryIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("open-directory rename behavior differs on Windows")
	}
	parent := t.TempDir()
	root := filepath.Join(parent, "repo")
	writeFixtureFile(t, root, "bound.json", []byte(`{"tree":"original"}`))
	_, rootDescriptor, err := openRepositoryRoot(root)
	if err != nil {
		t.Fatalf("open repository root: %v", err)
	}
	t.Cleanup(func() { _ = rootDescriptor.Close() })
	moved := filepath.Join(parent, "original")
	if err := os.Rename(root, moved); err != nil {
		t.Fatalf("rename opened repository root: %v", err)
	}
	writeFixtureFile(t, root, "bound.json", []byte(`{"tree":"replacement"}`))
	data, err := readRepositoryFile(context.Background(), rootDescriptor, "bound.json")
	if err != nil {
		t.Fatalf("read pinned repository root: %v", err)
	}
	if got, want := string(data), `{"tree":"original"}`; got != want {
		t.Fatalf("pinned repository data = %s, want %s", got, want)
	}
}

func TestReadinessClaimKeyNormalization(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
	}{
		{"covered uppercase", `{"COVERED":true}`},
		{"partial uppercase", `{"PARTIAL":true}`},
		{"passed uppercase", `{"PASSED":true}`},
		{"ready nested object", `{"outer":{"READY":true}}`},
		{"certified nested array", `{"outer":[{"CERTIFIED":true}]}`},
		{"waived uppercase", `{"WAIVED":true}`},
		{"accepted flaky underscore", `{"ACCEPTED_FLAKY":true}`},
		{"accepted flaky repeated hyphen", `{"accepted---flaky":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := rejectReadinessClaimKeys([]byte(test.data)); err == nil {
				t.Fatalf("readiness key check accepted %s", test.data)
			}
		})
	}
	for _, data := range []string{
		`{"ready_state":true}`,
		`{"status":"ready"}`,
		`{"accepted-flaky-state":true}`,
		`{"large":1e400}`,
	} {
		if err := rejectReadinessClaimKeys([]byte(data)); err != nil {
			t.Fatalf("readiness key check rejected positive control %s: %v", data, err)
		}
	}
}

func TestSnapshotFixtureRejectsFrontmatterMissingFilesAndSymlinkEscape(t *testing.T) {
	for _, mutate := range []struct {
		name   string
		mutate func(t *testing.T, root string)
	}{
		{"lowercase ADR status", func(t *testing.T, root string) {
			path := filepath.Join(root, filepath.FromSlash(behavioralPaths[6]))
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(strings.Replace(string(data), "status: Accepted", "status: accepted", 1)), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"changed ADR status", func(t *testing.T, root string) {
			path := filepath.Join(root, filepath.FromSlash(behavioralPaths[6]))
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(strings.Replace(string(data), "status: Accepted", "status: Rejected", 1)), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing ADR status", func(t *testing.T, root string) {
			writeFixtureFile(t, root, behavioralPaths[6], []byte("---\ntitle: ADR\n---\n# ADR\n"))
		}},
		{"body-only ADR status", func(t *testing.T, root string) {
			writeFixtureFile(t, root, behavioralPaths[6], []byte("---\ntitle: ADR\n---\nstatus: Accepted\n"))
		}},
		{"missing scenario catalog", func(t *testing.T, root string) {
			if err := os.Remove(filepath.Join(root, "conformance", "catalog.json")); err != nil {
				t.Fatal(err)
			}
		}},
		{"scenario catalog symlink escape", func(t *testing.T, root string) {
			path := filepath.Join(root, "conformance", "catalog.json")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(t.TempDir(), "catalog.json")
			if err := os.WriteFile(target, []byte(`{}`), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
		{"scenario catalog directory", func(t *testing.T, root string) {
			path := filepath.Join(root, "conformance", "catalog.json")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(path, 0o755); err != nil {
				t.Fatal(err)
			}
		}},
		{"scenario source byte change", func(t *testing.T, root string) {
			path := filepath.Join(root, filepath.FromSlash("conformance/scenarios/server/wal-order-001.json"))
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			writeFixtureFile(t, root, "conformance/scenarios/server/wal-order-001.json", append(data, '\n'))
		}},
		{"vector source byte change", func(t *testing.T, root string) {
			path := filepath.Join(root, filepath.FromSlash("conformance/vectors/canonical-v1.json"))
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			writeFixtureFile(t, root, "conformance/vectors/canonical-v1.json", append(data, '\n'))
		}},
		{"scenario source symlink", func(t *testing.T, root string) {
			path := filepath.Join(root, filepath.FromSlash("conformance/scenarios/server/wal-order-001.json"))
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(t.TempDir(), "outside.json"), path); err != nil {
				t.Fatal(err)
			}
		}},
		{"vector source symlink", func(t *testing.T, root string) {
			path := filepath.Join(root, filepath.FromSlash("conformance/vectors/canonical-v1.json"))
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(t.TempDir(), "outside.json"), path); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			root := completeSnapshotFixture(t)
			mutate.mutate(t, root)
			if _, err := BuildSnapshot(context.Background(), root); err == nil {
				t.Fatalf("BuildSnapshot accepted %s", mutate.name)
			}
		})
	}
}

func completeSnapshotFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	paths := []string{
		"conformance/requirements.json",
		"conformance/support-matrix.json",
		"conformance/catalog.json",
		"conformance/vectors/catalog.json",
		"conformance/faults/catalog.json",
		"conformance/artifacts/inventory.json",
		"conformance/performance/budgets.json",
	}
	paths = append(paths, behavioralPaths...)
	paths = append(paths, schemaPaths...)
	for _, path := range paths {
		data, err := os.ReadFile(filepath.Join(repositoryRoot(t), filepath.FromSlash(path)))
		if err != nil {
			t.Fatalf("read fixture source %q: %v", path, err)
		}
		writeFixtureFile(t, root, path, data)
	}
	for _, path := range []string{
		"conformance/scenarios/server/membership-reassignment-001.json",
		"conformance/scenarios/server/pull-divergent-checkpoints-001.json",
		"conformance/scenarios/server/pull-hydration-failure-001.json",
		"conformance/scenarios/server/push-response-loss-001.json",
		"conformance/scenarios/server/rebuild-forged-cursor-001.json",
		"conformance/scenarios/server/registry-reload-001.json",
		"conformance/scenarios/server/retention-reconnect-001.json",
		"conformance/scenarios/server/schema-queued-mutation-001.json",
		"conformance/scenarios/server/wal-decode-failure-001.json",
		"conformance/scenarios/server/wal-order-001.json",
		"conformance/scenarios/performance/configured-bounds-001.json",
		"conformance/scenarios/performance/core-sync-path-001.json",
		"conformance/scenarios/performance/fanout-001.json",
		"conformance/scenarios/performance/multi-scope-provenance-001.json",
		"conformance/scenarios/performance/pending-cycle-001.json",
		"conformance/scenarios/performance/queue-replay-001.json",
		"conformance/scenarios/performance/rebuild-apply-001.json",
		"conformance/scenarios/performance/rebuild-cardinality-001.json",
		"conformance/scenarios/performance/rebuild-requests-001.json",
		"conformance/scenarios/performance/schema-check-001.json",
		"conformance/scenarios/performance/seeded-empty-startup-001.json",
		"conformance/scenarios/performance/shared-private-scopes-001.json",
		"conformance/scenarios/performance/steady-pull-001.json",
		"conformance/scenarios/performance/warm-connect-001.json",
		"conformance/vectors/canonical-v1.json",
	} {
		data, err := os.ReadFile(filepath.Join(repositoryRoot(t), filepath.FromSlash(path)))
		if err != nil {
			t.Fatalf("read fixture source %q: %v", path, err)
		}
		writeFixtureFile(t, root, path, data)
	}
	return root
}

func writeFixtureFile(t *testing.T, root, relativePath string, data []byte) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create fixture directory for %q: %v", relativePath, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write fixture file %q: %v", relativePath, err)
	}
}

func mutateRequirementReferencePath(t *testing.T, root, path string) {
	t.Helper()
	requirementsPath := filepath.Join(root, "conformance", "requirements.json")
	data, err := os.ReadFile(requirementsPath)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	requirements, ok := document["requirements"].([]any)
	if !ok || len(requirements) == 0 {
		t.Fatal("fixture requirements did not decode as a nonempty array")
	}
	requirement, ok := requirements[0].(map[string]any)
	if !ok {
		t.Fatal("fixture first requirement did not decode as an object")
	}
	references, ok := requirement["normative_references"].([]any)
	if !ok || len(references) == 0 {
		t.Fatal("fixture first requirement has no normative reference")
	}
	reference, ok := references[0].(map[string]any)
	if !ok {
		t.Fatal("fixture first normative reference did not decode as an object")
	}
	reference["path"] = path
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, root, "conformance/requirements.json", encoded)
}

func requireErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected validation error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("validation error %q does not contain %q", err, want)
	}
}

func removeString(values []string, target string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != target {
			result = append(result, value)
		}
	}
	return result
}

func headingAnchors(headings []markdownHeading) []string {
	anchors := make([]string, len(headings))
	for index, heading := range headings {
		anchors[index] = heading.anchor
	}
	return anchors
}

func headingAnchorsAtLevel(headings []markdownHeading, level int) []string {
	anchors := make([]string, 0, len(headings))
	for _, heading := range headings {
		if heading.level == level {
			anchors = append(anchors, heading.anchor)
		}
	}
	return anchors
}

func expectedAnchorsAtLevel(anchors []string, levels []int, level int) []string {
	selected := make([]string, 0, len(anchors))
	for index, anchor := range anchors {
		if levels[index] == level {
			selected = append(selected, anchor)
		}
	}
	return selected
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

func mapKeys(values map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func requireJSONKeys(t *testing.T, raw json.RawMessage, expected ...string) {
	t.Helper()
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode JSON object: %v", err)
	}
	if got := mapKeys(value); !equalStrings(got, expected) {
		t.Fatalf("JSON object keys = %#v, want %#v", got, expected)
	}
}

func snapshotBindingPaths(snapshot Snapshot) []string {
	paths := []string{
		snapshot.Requirements.Path,
		snapshot.SupportMatrix.Path,
		snapshot.VerificationInputs.ScenarioCatalog.Path,
		snapshot.VerificationInputs.VectorCatalog.Path,
		snapshot.VerificationInputs.FaultCatalog.Path,
		snapshot.VerificationInputs.PerformanceBudgets.Path,
		snapshot.VerificationInputs.ArtifactInventory.Path,
		snapshot.SchemaFiles.Requirements.Path,
		snapshot.SchemaFiles.SupportMatrix.Path,
		snapshot.SchemaFiles.Scenario.Path,
		snapshot.SchemaFiles.Evidence.Path,
		snapshot.SchemaFiles.RCCandidateLock.Path,
		snapshot.SchemaFiles.RCManifest.Path,
		snapshot.SchemaFiles.FaultCatalog.Path,
		snapshot.SchemaFiles.ArtifactInventory.Path,
		snapshot.SchemaFiles.PerformanceBudgets.Path,
		snapshot.SchemaFiles.VectorCatalog.Path,
	}
	for _, binding := range snapshot.BehavioralFiles {
		paths = append(paths, binding.Path)
	}
	return paths
}

func expectedSnapshotBindingPaths() []string {
	paths := []string{
		"conformance/requirements.json",
		"conformance/support-matrix.json",
		"conformance/catalog.json",
		"conformance/vectors/catalog.json",
		"conformance/faults/catalog.json",
		"conformance/performance/budgets.json",
		"conformance/artifacts/inventory.json",
	}
	paths = append(paths, behavioralPaths...)
	paths = append(paths, schemaPaths...)
	return paths
}

func equalStringSets(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftSet := make(map[string]struct{}, len(left))
	for _, value := range left {
		leftSet[value] = struct{}{}
	}
	if len(leftSet) != len(left) {
		return false
	}
	for _, value := range right {
		if _, exists := leftSet[value]; !exists {
			return false
		}
	}
	return true
}
