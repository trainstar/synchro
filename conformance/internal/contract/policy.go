package contract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

const (
	requirementsSchemaURI = "./schemas/requirements-v2.schema.json"
	supportSchemaURI      = "./schemas/support-matrix.schema.json"
	faultSchemaURI        = "https://synchro.dev/conformance/schemas/fault-catalog-v1.schema.json"
	artifactSchemaURI     = "https://synchro.dev/conformance/schemas/artifact-inventory-v1.schema.json"
	performanceSchemaURI  = "https://synchro.dev/conformance/schemas/performance-budgets-v2.schema.json"

	lockedPerformanceDigest = "1ab4d515558dfd92694527142bb539fe6ad344ee58f18751a3075ada4a41b4aa"
)

var lockedSupportCells = map[SupportCellID]supportTuple{
	"SUP-PG-014":                 {component: "postgresql-server", platform: "postgresql", platformVersion: versionTuple{kind: "exact", value: "14"}, policy: "excluded"},
	"SUP-PG-015":                 {component: "postgresql-server", platform: "postgresql", platformVersion: versionTuple{kind: "exact", value: "15"}, policy: "excluded"},
	"SUP-PG-016":                 {component: "postgresql-server", platform: "postgresql", platformVersion: versionTuple{kind: "exact", value: "16"}, policy: "excluded"},
	"SUP-PG-017":                 {component: "postgresql-server", platform: "postgresql", platformVersion: versionTuple{kind: "exact", value: "17"}, policy: "excluded"},
	"SUP-PG-018":                 {component: "postgresql-server", platform: "postgresql", platformVersion: versionTuple{kind: "exact", value: "18"}, policy: "required"},
	"SUP-IOS-MIN-001":            {component: "swift-client", platform: "ios", platformVersion: versionTuple{kind: "minimum", value: "16"}, policy: "required"},
	"SUP-IOS-CURRENT-001":        {component: "swift-client", platform: "ios", platformVersion: versionTuple{kind: "current-stable"}, policy: "required"},
	"SUP-MACOS-MIN-001":          {component: "swift-client", platform: "macos", platformVersion: versionTuple{kind: "minimum", value: "13"}, policy: "required"},
	"SUP-MACOS-CURRENT-001":      {component: "swift-client", platform: "macos", platformVersion: versionTuple{kind: "current-stable"}, policy: "required"},
	"SUP-ANDROID-MIN-001":        {component: "kotlin-client", platform: "android", platformVersion: versionTuple{kind: "minimum", value: "24"}, policy: "required"},
	"SUP-ANDROID-CURRENT-001":    {component: "kotlin-client", platform: "android", platformVersion: versionTuple{kind: "current-stable"}, policy: "required"},
	"SUP-RN-IOS-MIN-001":         {component: "react-native-client", platform: "ios", platformVersion: versionTuple{kind: "minimum", value: "16"}, runtimeVersion: versionTuple{kind: "series", value: "0.83.x"}, policy: "required"},
	"SUP-RN-IOS-CURRENT-001":     {component: "react-native-client", platform: "ios", platformVersion: versionTuple{kind: "current-stable"}, runtimeVersion: versionTuple{kind: "series", value: "0.83.x"}, policy: "required"},
	"SUP-RN-ANDROID-MIN-001":     {component: "react-native-client", platform: "android", platformVersion: versionTuple{kind: "minimum", value: "24"}, runtimeVersion: versionTuple{kind: "series", value: "0.83.x"}, policy: "required"},
	"SUP-RN-ANDROID-CURRENT-001": {component: "react-native-client", platform: "android", platformVersion: versionTuple{kind: "current-stable"}, runtimeVersion: versionTuple{kind: "series", value: "0.83.x"}, policy: "required"},
}

var lockedArtifactRoles = map[ArtifactInventoryID]string{
	"ARTDEF-CONFORMANCE-RUNNER-001": "conformance-runner",
	"ARTDEF-PG-EXTENSION-001":       "pg-extension",
	"ARTDEF-PG-SQL-001":             "pg-install-sql",
	"ARTDEF-ADAPTER-001":            "adapter",
	"ARTDEF-SEED-TOOL-001":          "seed-tool",
	"ARTDEF-SWIFT-SPM-001":          "swift-spm",
	"ARTDEF-COCOAPODS-001":          "cocoapods",
	"ARTDEF-KOTLIN-MAVEN-001":       "kotlin-maven",
	"ARTDEF-RN-NPM-001":             "react-native-npm",
	"ARTDEF-PORTABLE-SEED-001":      "portable-seed",
}

type versionTuple struct {
	kind  string
	value string
}

type supportTuple struct {
	component       string
	platform        string
	platformVersion versionTuple
	runtimeVersion  versionTuple
	policy          string
}

var lockedBudgetTriples = map[BudgetID]budgetTriple{
	"BUD-WARM-CONNECT-001":             {"warm_connect_http_requests", "eq", "1"},
	"BUD-WARM-CONNECT-PULL-001":        {"warm_connect_pull_http_requests", "eq", "1"},
	"BUD-WARM-CONNECT-PUSH-001":        {"warm_connect_push_http_requests", "eq", "0"},
	"BUD-WARM-CONNECT-REBUILD-001":     {"warm_connect_rebuild_page_http_requests", "eq", "0"},
	"BUD-WARM-CONNECT-SCHEMA-001":      {"warm_connect_schema_fetch_http_requests", "eq", "0"},
	"BUD-WARM-CONNECT-OTHER-001":       {"warm_connect_other_http_requests", "eq", "0"},
	"BUD-STEADY-PULL-001":              {"steady_state_pull_http_requests_per_cycle", "eq", "1"},
	"BUD-STEADY-PULL-NONPULL-001":      {"steady_state_pull_non_pull_http_requests_per_cycle", "eq", "0"},
	"BUD-PENDING-PUSH-001":             {"pending_cycle_push_http_requests", "eq", "1"},
	"BUD-PENDING-PULL-001":             {"pending_cycle_pull_http_requests", "eq", "1"},
	"BUD-PENDING-CYCLE-UNEXPECTED-001": {"pending_cycle_non_push_or_pull_http_requests", "eq", "0"},
	"BUD-REBUILD-CONNECT-001":          {"rebuild_connect_http_requests", "eq", "1"},
	"BUD-REBUILD-PULL-001":             {"rebuild_pull_http_requests", "eq", "1"},
	"BUD-REBUILD-PAGE-001":             {"rebuild_page_request_count_minus_returned_page_count", "eq", "0"},
	"BUD-REBUILD-SCHEMA-FETCH-001":     {"rebuild_schema_fetch_http_requests", "eq", "0"},
	"BUD-REBUILD-UNEXPECTED-001":       {"rebuild_unexpected_http_requests", "eq", "0"},
	"BUD-CORE-SYNC-RPC-001":            {"core_sync_outbound_network_or_rpc_hops", "eq", "0"},
}

type budgetTriple struct {
	metric     string
	comparator string
	limit      string
}

var lockedMeasurementIDs = map[MeasurementID]struct{}{
	"MEAS-FANOUT-001":                 {},
	"MEAS-SHARED-PRIVATE-SCOPES-001":  {},
	"MEAS-REBUILD-CARDINALITY-001":    {},
	"MEAS-SCHEMA-CHECK-001":           {},
	"MEAS-SEEDED-EMPTY-STARTUP-001":   {},
	"MEAS-QUEUE-REPLAY-001":           {},
	"MEAS-REBUILD-APPLY-001":          {},
	"MEAS-MULTI-SCOPE-PROVENANCE-001": {},
	"MEAS-CONFIGURED-BOUNDS-001":      {},
}

// Validate checks the cross-catalog semantic contract with a fresh background
// context. Load uses the internal context-aware form during initial loading.
func (b *Bundle) Validate() error {
	return b.validate(context.Background())
}

func (b *Bundle) validate(ctx context.Context) error {
	if b == nil {
		return fmt.Errorf("contract bundle is nil")
	}
	var failures []error
	if err := checkContext(ctx); err != nil {
		failures = append(failures, err)
	}
	failures = append(failures, validateCatalogHeaders(b)...)
	failures = append(failures, b.validateRequirementsAndControls(ctx)...)
	failures = append(failures, validateSupportMatrix(b.Support)...)
	failures = append(failures, validateArtifactInventory(b.Artifacts)...)
	failures = append(failures, validatePerformanceCatalog(b)...)
	if err := checkContext(ctx); err != nil {
		failures = append(failures, err)
	}
	return joinSemanticErrors(failures)
}

func validateCatalogHeaders(b *Bundle) []error {
	var failures []error
	for _, catalog := range []struct {
		name          string
		schema        string
		actualSchema  string
		schemaVersion int
		expected      int
		release       string
	}{
		{"requirements", requirementsSchemaURI, b.Requirements.SchemaURI, b.Requirements.SchemaVersion, 2, b.Requirements.Release},
		{"support matrix", supportSchemaURI, b.Support.SchemaURI, b.Support.SchemaVersion, 1, b.Support.Release},
		{"fault catalog", faultSchemaURI, b.Faults.SchemaURI, b.Faults.SchemaVersion, 1, b.Faults.Release},
		{"artifact inventory", artifactSchemaURI, b.Artifacts.SchemaURI, b.Artifacts.SchemaVersion, 1, b.Artifacts.Release},
		{"performance budgets", performanceSchemaURI, b.Performance.SchemaURI, b.Performance.SchemaVersion, 2, b.Performance.Release},
	} {
		if catalog.actualSchema != catalog.schema {
			failures = append(failures, fmt.Errorf("%s has unexpected schema URI", catalog.name))
		}
		if catalog.schemaVersion != catalog.expected {
			failures = append(failures, fmt.Errorf("%s has unexpected schema version", catalog.name))
		}
		if catalog.release != releaseVersion {
			failures = append(failures, fmt.Errorf("%s has mixed or unsupported release %q", catalog.name, catalog.release))
		}
	}
	return failures
}

func (b *Bundle) validateRequirementsAndControls(ctx context.Context) []error {
	var failures []error
	if err := checkContext(ctx); err != nil {
		failures = append(failures, err)
	}
	if len(b.sources.behavioral) == 0 {
		failures = append(failures, fmt.Errorf("bundle has no captured behavioral source for normative references"))
		return failures
	}
	if len(b.Requirements.Requirements) != 111 {
		failures = append(failures, fmt.Errorf("requirements must contain exactly 111 records, found %d", len(b.Requirements.Requirements)))
	}
	if len(b.Faults.Controls) != 111 {
		failures = append(failures, fmt.Errorf("controls must contain exactly 111 records, found %d", len(b.Faults.Controls)))
	}

	requirements := make(map[RequirementID]Requirement, len(b.Requirements.Requirements))
	for _, requirement := range b.Requirements.Requirements {
		if _, exists := requirements[requirement.ID]; exists {
			failures = append(failures, fmt.Errorf("duplicate requirement ID %q", requirement.ID))
			continue
		}
		requirements[requirement.ID] = requirement
		if err := validateApplicability(requirement); err != nil {
			failures = append(failures, err)
		}
	}

	anchors, headingFailures := b.invariantHeadings()
	failures = append(failures, headingFailures...)
	requirementReferences := make(map[RequirementID]map[string]struct{}, len(requirements))
	invariantOwners := make(map[string]RequirementID)
	for _, requirement := range b.Requirements.Requirements {
		references := make(map[string]struct{}, len(requirement.NormativeReferences))
		invariantReferenceCount := 0
		for _, reference := range requirement.NormativeReferences {
			key, heading, err := b.resolveNormativeReference(reference)
			if err != nil {
				failures = append(failures, fmt.Errorf("requirement %s normative reference: %w", requirement.ID, err))
				continue
			}
			if _, duplicate := references[key]; duplicate {
				failures = append(failures, fmt.Errorf("requirement %s has duplicate normative reference %q", requirement.ID, key))
			}
			references[key] = struct{}{}
			if reference.Path == invariantsPath && heading.level == 3 {
				invariantReferenceCount++
				if owner, exists := invariantOwners[heading.anchor]; exists {
					failures = append(failures, fmt.Errorf("invariant heading %q is mapped by both %s and %s", heading.anchor, owner, requirement.ID))
				} else {
					invariantOwners[heading.anchor] = requirement.ID
				}
			}
		}
		if invariantReferenceCount != 1 {
			failures = append(failures, fmt.Errorf("requirement %s must map to exactly one level-three invariant, found %d", requirement.ID, invariantReferenceCount))
		}
		requirementReferences[requirement.ID] = references
	}
	for _, heading := range anchors {
		if heading.level != 3 {
			continue
		}
		if _, exists := invariantOwners[heading.anchor]; !exists {
			failures = append(failures, fmt.Errorf("level-three invariant heading %q has no requirement", heading.anchor))
		}
	}

	faultIDs := make(map[FaultID]struct{}, len(b.Faults.Faults))
	for _, fault := range b.Faults.Faults {
		if _, exists := faultIDs[fault.ID]; exists {
			failures = append(failures, fmt.Errorf("duplicate fault ID %q", fault.ID))
			continue
		}
		faultIDs[fault.ID] = struct{}{}
	}
	controlOwners := make(map[RequirementID]ControlID, len(b.Faults.Controls))
	usedFaults := make(map[FaultID]struct{}, len(b.Faults.Controls))
	controlIDs := make(map[ControlID]struct{}, len(b.Faults.Controls))
	for _, control := range b.Faults.Controls {
		if _, exists := controlIDs[control.ID]; exists {
			failures = append(failures, fmt.Errorf("duplicate control ID %q", control.ID))
		} else {
			controlIDs[control.ID] = struct{}{}
		}
		if _, exists := faultIDs[control.FaultID]; !exists {
			failures = append(failures, fmt.Errorf("control %s names unknown fault %s", control.ID, control.FaultID))
		} else {
			usedFaults[control.FaultID] = struct{}{}
		}
		if len(control.RequirementIDs) != 1 {
			failures = append(failures, fmt.Errorf("control %s must own exactly one requirement", control.ID))
			continue
		}
		requirementID := control.RequirementIDs[0]
		if _, exists := requirements[requirementID]; !exists {
			failures = append(failures, fmt.Errorf("control %s owns unknown requirement %s", control.ID, requirementID))
			continue
		}
		if owner, exists := controlOwners[requirementID]; exists {
			failures = append(failures, fmt.Errorf("requirement %s is owned by both controls %s and %s", requirementID, owner, control.ID))
		} else {
			controlOwners[requirementID] = control.ID
		}
		if !stringSetEquals(control.NormativeReferences, requirementReferenceStrings(requirementReferences[requirementID])) {
			failures = append(failures, fmt.Errorf("control %s normative references do not exactly match requirement %s", control.ID, requirementID))
		}
	}
	for requirementID := range requirements {
		if _, exists := controlOwners[requirementID]; !exists {
			failures = append(failures, fmt.Errorf("requirement %s has no control", requirementID))
		}
	}
	for faultID := range faultIDs {
		if _, exists := usedFaults[faultID]; !exists {
			failures = append(failures, fmt.Errorf("fault %s is not used by a control", faultID))
		}
	}
	return failures
}

const invariantsPath = "docs/src/content/docs/spec/04-invariants.mdx"

type markdownHeading struct {
	level  int
	anchor string
}

var (
	inlineImagePattern     = regexp.MustCompile(`!\[([^\]]*)\]\([^)]*\)`)
	inlineLinkPattern      = regexp.MustCompile(`\[([^\]]+)\]\([^)]*\)`)
	referenceLinkPattern   = regexp.MustCompile(`\[([^\]]+)\]\[[^\]]*\]`)
	inlineHTMLPattern      = regexp.MustCompile(`<[^>]+>`)
	markdownFormattingRune = "`*_~"
	markdownEscapes        = "!\"#$%&'()*+,./:;<=>?@[]^_`{|}~-"
)

func (b *Bundle) invariantHeadings() ([]markdownHeading, []error) {
	data, exists := b.sources.behavioral[invariantsPath]
	if !exists {
		return nil, []error{fmt.Errorf("captured invariant headings are missing")}
	}
	headings := parseMarkdownHeadings(string(data))
	count := 0
	for _, heading := range headings {
		if heading.level == 3 {
			count++
		}
	}
	if count != 111 {
		return headings, []error{fmt.Errorf("invariants document must contain exactly 111 level-three headings, found %d", count)}
	}
	return headings, nil
}

func (b *Bundle) resolveNormativeReference(reference NormativeReference) (string, markdownHeading, error) {
	if err := validateRepositoryRelativePath(reference.Path); err != nil {
		return "", markdownHeading{}, err
	}
	if !isFrozenBehavioralPath(reference.Path) {
		return "", markdownHeading{}, fmt.Errorf("path %q is not a frozen behavioral file", reference.Path)
	}
	if !strings.HasPrefix(reference.Anchor, "#") || len(reference.Anchor) == 1 {
		return "", markdownHeading{}, fmt.Errorf("anchor %q is not a heading anchor", reference.Anchor)
	}
	data, exists := b.sources.behavioral[reference.Path]
	if !exists {
		return "", markdownHeading{}, fmt.Errorf("captured behavioral file is missing")
	}
	for _, heading := range parseMarkdownHeadings(string(data)) {
		if "#"+heading.anchor == reference.Anchor {
			return reference.Path + reference.Anchor, heading, nil
		}
	}
	return "", markdownHeading{}, fmt.Errorf("anchor %q does not name an ATX Markdown or MDX heading", reference.Anchor)
}

// ValidateNormativeReferences checks references against the captured frozen
// behavioral files and their published heading anchors.
func (b *Bundle) ValidateNormativeReferences(references []NormativeReference) error {
	if b == nil {
		return errors.New("contract bundle is nil")
	}
	var failures []error
	for index, reference := range references {
		if _, _, err := b.resolveNormativeReference(reference); err != nil {
			failures = append(failures, fmt.Errorf("normative reference %d: %w", index, err))
		}
	}
	return errors.Join(failures...)
}

func isFrozenBehavioralPath(path string) bool {
	for _, frozen := range behavioralPaths {
		if path == frozen {
			return true
		}
	}
	return false
}

func parseMarkdownHeadings(document string) []markdownHeading {
	var headings []markdownHeading
	used := make(map[string]struct{})
	slugCounts := make(map[string]int)
	var fence rune
	for _, line := range strings.Split(strings.ReplaceAll(document, "\r\n", "\n"), "\n") {
		if marker, ok := markdownFenceMarker(line); ok {
			if fence == marker {
				fence = 0
			} else if fence == 0 {
				fence = marker
			}
			continue
		}
		if fence != 0 {
			continue
		}
		level, text, ok := atxHeading(line)
		if !ok {
			continue
		}
		base := githubSlug(text)
		suffix := slugCounts[base]
		anchor := base
		for {
			if _, exists := used[anchor]; !exists {
				break
			}
			suffix++
			anchor = fmt.Sprintf("%s-%d", base, suffix)
		}
		slugCounts[base] = suffix
		used[anchor] = struct{}{}
		headings = append(headings, markdownHeading{level: level, anchor: anchor})
	}
	return headings
}

func markdownFenceMarker(line string) (rune, bool) {
	line, ok := trimLeadingMarkdownWhitespace(line)
	if !ok || len(line) < 3 || (line[0] != '`' && line[0] != '~') {
		return 0, false
	}
	marker := rune(line[0])
	width := 0
	for width < len(line) && line[width] == byte(marker) {
		width++
	}
	return marker, width >= 3
}

func atxHeading(line string) (int, string, bool) {
	line, ok := trimLeadingMarkdownWhitespace(line)
	if !ok {
		return 0, "", false
	}
	level := 0
	for level < len(line) && line[level] == '#' {
		level++
	}
	if level == 0 || level > 6 || (level < len(line) && line[level] != ' ' && line[level] != '\t') {
		return 0, "", false
	}
	text := strings.TrimRightFunc(line[level:], unicode.IsSpace)
	if text == "" {
		return 0, "", false
	}
	return level, trimClosingHeadingHashes(text), true
}

func trimLeadingMarkdownWhitespace(line string) (string, bool) {
	count := 0
	for offset, character := range line {
		if !unicode.IsSpace(character) {
			return line[offset:], count <= 3
		}
		count++
		if count > 3 {
			return "", false
		}
	}
	return "", count <= 3
}

func trimClosingHeadingHashes(value string) string {
	end := len(value)
	for end > 0 && value[end-1] == '#' {
		end--
	}
	if end == len(value) {
		return value
	}
	spaceStart := end
	for spaceStart > 0 && (value[spaceStart-1] == ' ' || value[spaceStart-1] == '\t') {
		spaceStart--
	}
	if spaceStart == end {
		return value
	}
	return value[:spaceStart]
}

func stripInlineMarkdown(value string) string {
	text := inlineImagePattern.ReplaceAllString(value, "$1")
	text = inlineLinkPattern.ReplaceAllString(text, "$1")
	text = referenceLinkPattern.ReplaceAllString(text, "$1")
	text = inlineHTMLPattern.ReplaceAllString(text, "")
	text = strings.Map(func(character rune) rune {
		if strings.ContainsRune(markdownFormattingRune, character) {
			return -1
		}
		return character
	}, text)
	return unescapeMarkdownPunctuation(text)
}

func unescapeMarkdownPunctuation(value string) string {
	runes := []rune(value)
	var output strings.Builder
	for index := 0; index < len(runes); index++ {
		if runes[index] == '\\' && index+1 < len(runes) && strings.ContainsRune(markdownEscapes, runes[index+1]) {
			index++
		}
		output.WriteRune(runes[index])
	}
	return output.String()
}

func githubSlug(value string) string {
	value = lowerECMAScript(strings.TrimSpace(stripInlineMarkdown(value)))
	var filtered strings.Builder
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsLetter(character) || unicode.IsMark(character) || unicode.IsNumber(character) || unicode.Is(unicode.Pc, character) || character == '-' {
			filtered.WriteRune(character)
		}
	}
	var output strings.Builder
	lastWhitespace := false
	for _, character := range filtered.String() {
		if unicode.IsSpace(character) {
			if !lastWhitespace {
				output.WriteByte('-')
				lastWhitespace = true
			}
			continue
		}
		lastWhitespace = false
		output.WriteRune(character)
	}
	return output.String()
}

func lowerECMAScript(value string) string {
	// Go uses simple Unicode case mappings. ECMAScript applies the full mapping
	// for U+0130 before its Unicode property filtering step.
	value = strings.ReplaceAll(value, "\u0130", "i\u0307")
	return strings.ToLower(value)
}

func validateApplicability(requirement Requirement) error {
	proofs := stringSet(requirement.RequiredProofTypes)
	components := stringSet(requirement.ApplicableComponents)
	_, hasPostgres := components["postgresql-server"]
	_, serverBlackBox := proofs["server-black-box"]
	if hasPostgres != serverBlackBox {
		return fmt.Errorf("requirement %s has server-black-box applicability mismatch", requirement.ID)
	}
	hasNative := false
	for _, component := range []string{"swift-client", "kotlin-client", "react-native-client"} {
		if _, exists := components[component]; exists {
			hasNative = true
			break
		}
	}
	_, nativeE2E := proofs["native-e2e"]
	if hasNative != nativeE2E {
		return fmt.Errorf("requirement %s has native-e2e applicability mismatch", requirement.ID)
	}
	return nil
}

func validateSupportMatrix(matrix SupportMatrix) []error {
	var failures []error
	if matrix.CurrentTrackPolicy != (CurrentTrackPolicy{Selector: "current-stable", ResolveAt: "release-candidate-start", RecordExactVersionsIn: "rc-manifest"}) {
		failures = append(failures, fmt.Errorf("support matrix current-track policy does not match the locked policy"))
	}
	if len(matrix.Cells) != len(lockedSupportCells) {
		failures = append(failures, fmt.Errorf("support matrix must contain exactly %d cells, found %d", len(lockedSupportCells), len(matrix.Cells)))
	}
	seenIDs := make(map[SupportCellID]struct{}, len(matrix.Cells))
	seenTuples := make(map[string]SupportCellID, len(matrix.Cells))
	for _, cell := range matrix.Cells {
		if _, exists := seenIDs[cell.ID]; exists {
			failures = append(failures, fmt.Errorf("duplicate support cell ID %s", cell.ID))
		} else {
			seenIDs[cell.ID] = struct{}{}
		}
		tuple := supportCellTuple(cell)
		if previous, exists := seenTuples[tuple]; exists {
			failures = append(failures, fmt.Errorf("support cells %s and %s have duplicate semantic tuples", previous, cell.ID))
		} else {
			seenTuples[tuple] = cell.ID
		}
		expected, known := lockedSupportCells[cell.ID]
		if !known {
			failures = append(failures, fmt.Errorf("support matrix has unexpected stable ID %s", cell.ID))
			continue
		}
		if supportTupleFromCell(cell) != expected {
			failures = append(failures, fmt.Errorf("support cell %s does not match its locked semantic tuple", cell.ID))
		}
	}
	for id := range lockedSupportCells {
		if _, exists := seenIDs[id]; !exists {
			failures = append(failures, fmt.Errorf("support matrix is missing locked cell %s", id))
		}
	}
	return failures
}

func supportTupleFromCell(cell SupportCell) supportTuple {
	return supportTuple{
		component:       cell.Component,
		platform:        cell.Platform,
		platformVersion: selectorTuple(cell.PlatformVersion),
		runtimeVersion:  selectorTuple(cell.RuntimeVersion),
		policy:          cell.Policy,
	}
}

func selectorTuple(selector *VersionSelector) versionTuple {
	if selector == nil {
		return versionTuple{}
	}
	return versionTuple{kind: selector.Kind, value: selector.Value}
}

func supportCellTuple(cell SupportCell) string {
	tuple := supportTupleFromCell(cell)
	return strings.Join([]string{tuple.component, tuple.platform, tuple.platformVersion.kind, tuple.platformVersion.value, tuple.runtimeVersion.kind, tuple.runtimeVersion.value, tuple.policy}, "\x00")
}

func validateArtifactInventory(inventory ArtifactInventory) []error {
	var failures []error
	if len(inventory.Artifacts) != len(lockedArtifactRoles) {
		failures = append(failures, fmt.Errorf("artifact inventory must contain exactly %d artifacts, found %d", len(lockedArtifactRoles), len(inventory.Artifacts)))
	}
	seenIDs := make(map[ArtifactInventoryID]struct{}, len(inventory.Artifacts))
	seenRoles := make(map[string]ArtifactInventoryID, len(inventory.Artifacts))
	for _, artifact := range inventory.Artifacts {
		if _, exists := seenIDs[artifact.ID]; exists {
			failures = append(failures, fmt.Errorf("duplicate artifact inventory ID %s", artifact.ID))
		} else {
			seenIDs[artifact.ID] = struct{}{}
		}
		if prior, exists := seenRoles[artifact.Role]; exists {
			failures = append(failures, fmt.Errorf("artifact roles are duplicated by %s and %s", prior, artifact.ID))
		} else {
			seenRoles[artifact.Role] = artifact.ID
		}
		if strings.TrimSpace(artifact.Name) == "" {
			failures = append(failures, fmt.Errorf("artifact %s has an empty name", artifact.ID))
		}
		expectedRole, known := lockedArtifactRoles[artifact.ID]
		if !known {
			failures = append(failures, fmt.Errorf("artifact inventory has unexpected stable ID %s", artifact.ID))
		} else if artifact.Role != expectedRole {
			failures = append(failures, fmt.Errorf("artifact %s has role %q, want %q", artifact.ID, artifact.Role, expectedRole))
		}
	}
	for id, role := range lockedArtifactRoles {
		if _, exists := seenIDs[id]; !exists {
			failures = append(failures, fmt.Errorf("artifact inventory is missing %s role %s", id, role))
		}
	}
	return failures
}

func validatePerformanceCatalog(bundle *Bundle) []error {
	catalog := bundle.Performance
	support := bundle.Support
	artifacts := bundle.Artifacts
	var failures []error
	if len(catalog.Budgets) != len(lockedBudgetTriples) {
		failures = append(failures, fmt.Errorf("performance catalog must contain exactly %d budgets, found %d", len(lockedBudgetTriples), len(catalog.Budgets)))
	}
	if len(catalog.RequiredMeasurements) != len(lockedMeasurementIDs) {
		failures = append(failures, fmt.Errorf("performance catalog must contain exactly %d measurements, found %d", len(lockedMeasurementIDs), len(catalog.RequiredMeasurements)))
	}
	requiredSupport := make(map[SupportCellID]struct{}, len(support.Cells))
	for _, cell := range support.Cells {
		if cell.Policy == "required" {
			requiredSupport[cell.ID] = struct{}{}
		}
	}
	knownArtifacts := make(map[ArtifactInventoryID]struct{}, len(artifacts.Artifacts))
	for _, artifact := range artifacts.Artifacts {
		knownArtifacts[artifact.ID] = struct{}{}
	}
	seenBudgets := make(map[BudgetID]struct{}, len(catalog.Budgets))
	for _, budget := range catalog.Budgets {
		if _, exists := seenBudgets[budget.ID]; exists {
			failures = append(failures, fmt.Errorf("duplicate performance budget ID %s", budget.ID))
		} else {
			seenBudgets[budget.ID] = struct{}{}
		}
		expected, known := lockedBudgetTriples[budget.ID]
		if !known {
			failures = append(failures, fmt.Errorf("performance catalog has unexpected budget ID %s", budget.ID))
		} else if normalizedLimit, err := javaScriptJSONNumber(budget.Limit); err != nil {
			failures = append(failures, fmt.Errorf("budget %s has invalid numeric limit: %w", budget.ID, err))
		} else if budget.Metric != expected.metric || budget.Comparator != expected.comparator || normalizedLimit != expected.limit {
			failures = append(failures, fmt.Errorf("budget %s does not match its locked metric, comparator, and limit", budget.ID))
		}
		failures = append(failures, validatePerformanceReferences(string(budget.ID), budget.SupportCellIDs, budget.ArtifactInventoryIDs, requiredSupport, knownArtifacts)...)
		if err := validateParameters(budget.DataProfile.Parameters, fmt.Sprintf("budget %s data profile", budget.ID)); err != nil {
			failures = append(failures, err)
		}
	}
	for id := range lockedBudgetTriples {
		if _, exists := seenBudgets[id]; !exists {
			failures = append(failures, fmt.Errorf("performance catalog is missing budget %s", id))
		}
	}

	seenMeasurements := make(map[MeasurementID]struct{}, len(catalog.RequiredMeasurements))
	for _, measurement := range catalog.RequiredMeasurements {
		if _, exists := seenMeasurements[measurement.ID]; exists {
			failures = append(failures, fmt.Errorf("duplicate required measurement ID %s", measurement.ID))
		} else {
			seenMeasurements[measurement.ID] = struct{}{}
		}
		if _, known := lockedMeasurementIDs[measurement.ID]; !known {
			failures = append(failures, fmt.Errorf("performance catalog has unexpected measurement ID %s", measurement.ID))
		}
		failures = append(failures, validatePerformanceReferences(string(measurement.ID), measurement.SupportCellIDs, measurement.ArtifactInventoryIDs, requiredSupport, knownArtifacts)...)
		if err := validateParameters(measurement.DataProfile.Parameters, fmt.Sprintf("measurement %s data profile", measurement.ID)); err != nil {
			failures = append(failures, err)
		}
		metricIDs := make(map[MetricID]struct{}, len(measurement.Metrics))
		for _, metric := range measurement.Metrics {
			if _, exists := metricIDs[metric.ID]; exists {
				failures = append(failures, fmt.Errorf("measurement %s has duplicate metric ID %s", measurement.ID, metric.ID))
			} else {
				metricIDs[metric.ID] = struct{}{}
			}
		}
		stratumIDs := make(map[StratumID]struct{}, len(measurement.Strata))
		for _, stratum := range measurement.Strata {
			if _, exists := stratumIDs[stratum.StratumID]; exists {
				failures = append(failures, fmt.Errorf("measurement %s has duplicate stratum ID %s", measurement.ID, stratum.StratumID))
			} else {
				stratumIDs[stratum.StratumID] = struct{}{}
			}
			if err := validateParameters(stratum.Parameters, fmt.Sprintf("measurement %s stratum %s", measurement.ID, stratum.StratumID)); err != nil {
				failures = append(failures, err)
			}
		}
	}
	for id := range lockedMeasurementIDs {
		if _, exists := seenMeasurements[id]; !exists {
			failures = append(failures, fmt.Errorf("performance catalog is missing measurement %s", id))
		}
	}
	digest, err := bundle.performanceCatalogDigest()
	if err != nil {
		failures = append(failures, fmt.Errorf("canonicalize locked performance snapshot: %w", err))
	} else if digest != lockedPerformanceDigest {
		failures = append(failures, fmt.Errorf("performance catalog does not match the locked v0.3.0 semantic snapshot"))
	}
	return failures
}

func (b *Bundle) performanceCatalogDigest() (string, error) {
	fingerprint, err := typedPerformanceFingerprint(b.Performance)
	if err != nil {
		return "", err
	}
	if fingerprint == b.performanceFingerprint {
		if source, exists := b.sources.catalogs["conformance/performance/budgets.json"]; exists {
			return performanceSourceDigest(source)
		}
	}
	return performanceCatalogDigest(b.Performance)
}

func validatePerformanceReferences(owner string, supportIDs []SupportCellID, artifactIDs []ArtifactInventoryID, requiredSupport map[SupportCellID]struct{}, knownArtifacts map[ArtifactInventoryID]struct{}) []error {
	var failures []error
	for _, supportID := range supportIDs {
		if _, exists := requiredSupport[supportID]; !exists {
			failures = append(failures, fmt.Errorf("%s references unknown or excluded support cell %s", owner, supportID))
		}
	}
	for _, artifactID := range artifactIDs {
		if _, exists := knownArtifacts[artifactID]; !exists {
			failures = append(failures, fmt.Errorf("%s references unknown artifact inventory %s", owner, artifactID))
		}
	}
	return failures
}

func validateParameters(parameters json.RawMessage, owner string) error {
	if len(parameters) == 0 {
		return fmt.Errorf("%s has missing parameters", owner)
	}
	if err := jsonstrict.ValidateValue(parameters); err != nil {
		return fmt.Errorf("%s parameters: %w", owner, err)
	}
	return rejectReadinessClaimKeys(parameters)
}

func performanceCatalogDigest(catalog PerformanceCatalog) (string, error) {
	semantic := struct {
		Budgets              []PerformanceBudget   `json:"budgets"`
		RequiredMeasurements []RequiredMeasurement `json:"required_measurements"`
	}{
		Budgets:              catalog.Budgets,
		RequiredMeasurements: catalog.RequiredMeasurements,
	}
	encoded, err := json.Marshal(semantic)
	if err != nil {
		return "", err
	}
	canonical, err := javascriptJSONNormalize(encoded)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(canonical)), nil
}

func typedPerformanceFingerprint(catalog PerformanceCatalog) ([32]byte, error) {
	encoded, err := json.Marshal(catalog)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func performanceSourceDigest(source []byte) (string, error) {
	value, err := parseJavaScriptJSONDocument(source)
	if err != nil {
		return "", err
	}
	if value.kind != javascriptJSONObject {
		return "", fmt.Errorf("performance source is not a JSON object")
	}
	budgets, exists := javaScriptObjectMember(value, "budgets")
	if !exists {
		return "", fmt.Errorf("performance source has no budgets")
	}
	measurements, exists := javaScriptObjectMember(value, "required_measurements")
	if !exists {
		return "", fmt.Errorf("performance source has no required_measurements")
	}
	projection := javascriptJSONValue{
		kind: javascriptJSONObject,
		object: []javascriptJSONMember{
			{name: "budgets", value: budgets},
			{name: "required_measurements", value: measurements},
		},
	}
	var encoded bytes.Buffer
	if err := appendJavaScriptJSONString(&encoded, projection); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded.Bytes())), nil
}

func javaScriptObjectMember(object javascriptJSONValue, name string) (javascriptJSONValue, bool) {
	for _, member := range object.object {
		if member.name == name {
			return member.value, true
		}
	}
	return javascriptJSONValue{}, false
}

type javascriptJSONValue struct {
	object []javascriptJSONMember
	array  []javascriptJSONValue
	string string
	number float64
	bool   bool
	kind   javascriptJSONKind
}

type javascriptJSONMember struct {
	name  string
	value javascriptJSONValue
}

type javascriptJSONKind uint8

const (
	javascriptJSONNull javascriptJSONKind = iota
	javascriptJSONBool
	javascriptJSONNumber
	javascriptJSONString
	javascriptJSONArray
	javascriptJSONObject
)

// javascriptJSONNormalize models JSON.parse followed by JSON.stringify for the
// JSON forms that authored performance catalogs permit. Object member order is
// retained, except that JavaScript array-index property names sort first.
func javascriptJSONNormalize(data []byte) ([]byte, error) {
	value, err := parseJavaScriptJSONDocument(data)
	if err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := appendJavaScriptJSONString(&output, value); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func parseJavaScriptJSONDocument(data []byte) (javascriptJSONValue, error) {
	if err := jsonstrict.ValidateValue(data); err != nil {
		return javascriptJSONValue{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := parseJavaScriptJSONValue(decoder)
	if err != nil {
		return javascriptJSONValue{}, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return javascriptJSONValue{}, fmt.Errorf("JSON document contains more than one value")
		}
		return javascriptJSONValue{}, fmt.Errorf("read JSON after top-level value: %w", err)
	}
	return value, nil
}

func parseJavaScriptJSONValue(decoder *json.Decoder) (javascriptJSONValue, error) {
	token, err := decoder.Token()
	if err != nil {
		return javascriptJSONValue{}, err
	}
	switch typed := token.(type) {
	case json.Delim:
		switch typed {
		case '{':
			value := javascriptJSONValue{kind: javascriptJSONObject}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return javascriptJSONValue{}, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return javascriptJSONValue{}, fmt.Errorf("JSON object member name is not a string")
				}
				child, err := parseJavaScriptJSONValue(decoder)
				if err != nil {
					return javascriptJSONValue{}, err
				}
				value.object = append(value.object, javascriptJSONMember{name: key, value: child})
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim('}') {
				return javascriptJSONValue{}, fmt.Errorf("JSON object is not terminated")
			}
			return value, nil
		case '[':
			value := javascriptJSONValue{kind: javascriptJSONArray}
			for decoder.More() {
				child, err := parseJavaScriptJSONValue(decoder)
				if err != nil {
					return javascriptJSONValue{}, err
				}
				value.array = append(value.array, child)
			}
			closing, err := decoder.Token()
			if err != nil || closing != json.Delim(']') {
				return javascriptJSONValue{}, fmt.Errorf("JSON array is not terminated")
			}
			return value, nil
		default:
			return javascriptJSONValue{}, fmt.Errorf("unexpected JSON delimiter %q", typed)
		}
	case json.Number:
		number, err := strconv.ParseFloat(typed.String(), 64)
		if err != nil {
			if numberError, ok := err.(*strconv.NumError); !ok || numberError.Err != strconv.ErrRange {
				return javascriptJSONValue{}, fmt.Errorf("parse JSON number %q: %w", typed, err)
			}
		}
		return javascriptJSONValue{kind: javascriptJSONNumber, number: number}, nil
	case string:
		return javascriptJSONValue{kind: javascriptJSONString, string: typed}, nil
	case bool:
		return javascriptJSONValue{kind: javascriptJSONBool, bool: typed}, nil
	case nil:
		return javascriptJSONValue{kind: javascriptJSONNull}, nil
	default:
		return javascriptJSONValue{}, fmt.Errorf("unsupported JSON token %T", token)
	}
}

func appendJavaScriptJSONString(output *bytes.Buffer, value javascriptJSONValue) error {
	switch value.kind {
	case javascriptJSONNull:
		output.WriteString("null")
	case javascriptJSONBool:
		output.WriteString(strconv.FormatBool(value.bool))
	case javascriptJSONNumber:
		output.WriteString(javaScriptNumberString(value.number))
	case javascriptJSONString:
		appendJavaScriptString(output, value.string)
	case javascriptJSONArray:
		output.WriteByte('[')
		for index, child := range value.array {
			if index > 0 {
				output.WriteByte(',')
			}
			if err := appendJavaScriptJSONString(output, child); err != nil {
				return err
			}
		}
		output.WriteByte(']')
	case javascriptJSONObject:
		output.WriteByte('{')
		for index, member := range javaScriptObjectOrder(value.object) {
			if index > 0 {
				output.WriteByte(',')
			}
			appendJavaScriptString(output, member.name)
			output.WriteByte(':')
			if err := appendJavaScriptJSONString(output, member.value); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	default:
		return fmt.Errorf("unsupported JavaScript JSON value kind %d", value.kind)
	}
	return nil
}

func javaScriptObjectOrder(members []javascriptJSONMember) []javascriptJSONMember {
	indices := make([]javascriptJSONMember, 0, len(members))
	others := make([]javascriptJSONMember, 0, len(members))
	for _, member := range members {
		if _, ok := javaScriptArrayIndex(member.name); ok {
			indices = append(indices, member)
		} else {
			others = append(others, member)
		}
	}
	sort.Slice(indices, func(left, right int) bool {
		leftIndex, _ := javaScriptArrayIndex(indices[left].name)
		rightIndex, _ := javaScriptArrayIndex(indices[right].name)
		return leftIndex < rightIndex
	})
	return append(indices, others...)
}

func javaScriptArrayIndex(name string) (uint64, bool) {
	if name == "0" {
		return 0, true
	}
	if name == "" || name[0] == '0' {
		return 0, false
	}
	for _, character := range name {
		if character < '0' || character > '9' {
			return 0, false
		}
	}
	index, err := strconv.ParseUint(name, 10, 32)
	if err != nil || index >= 1<<32-1 {
		return 0, false
	}
	return index, true
}

func appendJavaScriptString(output *bytes.Buffer, value string) {
	output.WriteByte('"')
	for _, character := range value {
		switch character {
		case '"':
			output.WriteString(`\"`)
		case '\\':
			output.WriteString(`\\`)
		case '\b':
			output.WriteString(`\b`)
		case '\f':
			output.WriteString(`\f`)
		case '\n':
			output.WriteString(`\n`)
		case '\r':
			output.WriteString(`\r`)
		case '\t':
			output.WriteString(`\t`)
		default:
			if character < 0x20 {
				output.WriteString(`\u00`)
				output.WriteString(fmt.Sprintf("%02x", character))
			} else {
				output.WriteRune(character)
			}
		}
	}
	output.WriteByte('"')
}

func javaScriptNumberString(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "null"
	}
	if value == 0 {
		return "0"
	}
	abs := math.Abs(value)
	if abs >= 1e-6 && abs < 1e21 {
		return strconv.FormatFloat(value, 'f', -1, 64)
	}
	exponent := strconv.FormatFloat(value, 'e', -1, 64)
	parts := strings.Split(exponent, "e")
	if len(parts) != 2 {
		return exponent
	}
	sign := ""
	digits := parts[1]
	if strings.HasPrefix(digits, "+") || strings.HasPrefix(digits, "-") {
		sign, digits = digits[:1], digits[1:]
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		digits = "0"
	}
	return parts[0] + "e" + sign + digits
}

func javaScriptJSONNumber(value json.Number) (string, error) {
	number, err := strconv.ParseFloat(value.String(), 64)
	if err != nil {
		if numberError, ok := err.(*strconv.NumError); !ok || numberError.Err != strconv.ErrRange {
			return "", err
		}
	}
	return javaScriptNumberString(number), nil
}

func requirementReferenceStrings(references map[string]struct{}) []string {
	result := make([]string, 0, len(references))
	for reference := range references {
		result = append(result, reference)
	}
	sort.Strings(result)
	return result
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func stringSetEquals(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftSet := stringSet(left)
	rightSet := stringSet(right)
	if len(leftSet) != len(rightSet) || len(leftSet) != len(left) {
		return false
	}
	for value := range leftSet {
		if _, exists := rightSet[value]; !exists {
			return false
		}
	}
	return true
}

func joinSemanticErrors(failures []error) error {
	if len(failures) == 0 {
		return nil
	}
	filtered := make([]error, 0, len(failures))
	for _, failure := range failures {
		if failure != nil {
			filtered = append(filtered, failure)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	sort.SliceStable(filtered, func(i, j int) bool { return filtered[i].Error() < filtered[j].Error() })
	return fmt.Errorf("contract semantic validation failed: %w", errors.Join(filtered...))
}
