package invariants

import "testing"

const (
	issue49CanonicalBodyHex = "0000000d000000000000000530302d706b01010000000000000005726f772d31000000000000000930312d737472696e6701010000000000000005636166c3a9000000000000000630322d696e740201ffffffef000000000000000830332d696e74363403010020000000000000000000000000000a30342d646563696d616c040100000000000000062d31322e3334000000000000000830352d666c6f617405013ff8000000000000000000000000000a30362d626f6f6c65616e060101000000000000000b30372d6461746574696d650701000000000000001b323032362d30392d30395431323a33343a35362e3132333435365a000000000000000730382d646174650801000000000000000a323032362d30392d3039000000000000000730392d74696d650901000000000000000f31323a33343a35362e313233343536000000000000000731302d6a736f6e0a0100000000000000177b2261223a312c2262223a5b747275652c6e756c6c5d7d000000000000000831312d62797465730b010000000000000004000102ff000000000000000731322d6e756c6c0100"
	issue49SchemaHash       = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	issue49RowIdentityOne   = "73796e6368726f3a76333a726f772d6964656e746974793a763100000000000000000d74626c2d646f63756d656e7473000000000000000530302d706b01010000000000000005726f772d31"
	issue49RowIdentityTwo   = "73796e6368726f3a76333a726f772d6964656e746974793a763100000000000000000d74626c2d646f63756d656e7473000000000000000530302d706b01010000000000000005726f772d32"
	issue49RowDigestOne     = "ae212d5dd1405851eb8951b5c410f3ae88de37683a396f1a35349d8d936fa367"
	issue49RowDigestTwo     = "0c381ee05ea32edbec28fe607faa0fada4d8498da973f28b0e50b2265dfe852c"
)

func TestIssue49CanonicalTypedRowEncoding(t *testing.T) {
	row := issue49CanonicalTypedRowFixture()
	observation := issue49CanonicalRowObservation{
		Row:             row,
		ExpectedBodyHex: issue49CanonicalBodyHex,
		Implementations: map[string]string{
			"postgresql":   issue49CanonicalBodyHex,
			"swift":        issue49CanonicalBodyHex,
			"kotlin":       issue49CanonicalBodyHex,
			"react-native": issue49CanonicalBodyHex,
		},
		Malformed: issue49MalformedTypedRows(row),
	}
	mutant := observation
	mutant.Malformed = append([]issue49MalformedTypedRow(nil), observation.Malformed...)
	mutant.Malformed[0].Applied = true
	issue49Proof(t, "SYNC-INTEGRITY-003", issue49CanonicalTypedRowsValid(observation), issue49CanonicalTypedRowsValid(mutant))
}

func TestIssue49RowDigestBinding(t *testing.T) {
	row := issue49CanonicalTypedRowFixture()
	records := make([]issue49RowDigestRecord, 0, 4)
	for _, kind := range []string{"push", "pull", "rebuild", "seed"} {
		records = append(records, issue49RowDigestRecord{
			Kind:           kind,
			Row:            cloneIssue49TypedRow(row),
			SchemaHash:     issue49SchemaHash,
			ServerVersion:  "09z.server/version+opaque==",
			Digest:         issue49RowDigestOne,
			Verified:       true,
			Reconciled:     true,
			CursorAdvanced: kind == "pull",
		})
	}
	mutant := append([]issue49RowDigestRecord(nil), records...)
	mutant[1].Row = cloneIssue49TypedRow(mutant[1].Row)
	mutant[1].Row.Fields[1].Value = "cafe"
	issue49Proof(t, "SYNC-INTEGRITY-004", issue49RowDigestBindingsValid(records), issue49RowDigestBindingsValid(mutant))
}

func TestIssue49ScopeDigestBinding(t *testing.T) {
	observation := issue49ScopeDigestObservation{
		SchemaHash:  issue49SchemaHash,
		ScopeID:     "documents:user-17",
		Cardinality: 2,
		Entries: []issue49ScopeDigestEntry{
			{RowIdentity: issue49RowIdentityOne, RowDigest: issue49RowDigestOne},
			{RowIdentity: issue49RowIdentityTwo, RowDigest: issue49RowDigestTwo},
		},
		Digest:   "10cd0e9a639d29dd50e98fe041338617c06433c4eb9d39cbfdb4047918665301",
		Verified: true,
	}
	mutant := observation
	mutant.Cardinality = 3
	issue49Proof(t, "SYNC-INTEGRITY-005", issue49ScopeDigestBindingValid(observation), issue49ScopeDigestBindingValid(mutant))
}

func TestIssue49CompleteTerminalChecksumMaps(t *testing.T) {
	activeBefore := []string{"scope-a", "scope-rebuild", "scope-remove"}
	boundary := issue49EffectPosition{CommitLSN: 451}
	nonterminal := issue49PullChecksumPage{
		HasMore:    true,
		SchemaHash: issue49SchemaHash,
		Boundary:   boundary,
		Accepted:   true,
	}
	terminal := issue49PullChecksumPage{
		SchemaHash:     issue49SchemaHash,
		Boundary:       boundary,
		AddScopes:      []string{"scope-new"},
		RemoveScopes:   []string{"scope-remove"},
		RebuildScopes:  []string{"scope-rebuild"},
		Checksums:      issue49CompleteChecksumMap(boundary),
		Accepted:       true,
		CursorAdvanced: true,
	}
	failures := []issue49ChecksumFailure{
		{Kind: "nonterminal-checksums", Page: cloneIssue49PullChecksumPage(nonterminal)},
		{Kind: "missing", Page: cloneIssue49PullChecksumPage(terminal)},
		{Kind: "extra", Page: cloneIssue49PullChecksumPage(terminal)},
		{Kind: "malformed", Page: cloneIssue49PullChecksumPage(terminal)},
		{Kind: "wrong-bound", Page: cloneIssue49PullChecksumPage(terminal)},
	}
	failures[0].Page.Checksums = map[string]issue49BoundScopeChecksum{"scope-a": issue49ScopeChecksum("scope-a", boundary, issue49RowDigestOne)}
	delete(failures[1].Page.Checksums, "scope-new")
	failures[2].Page.Checksums["scope-extra"] = issue49ScopeChecksum("scope-extra", boundary, issue49RowDigestTwo)
	malformed := failures[3].Page.Checksums["scope-a"]
	malformed.Checksum.Algorithm = "crc32"
	failures[3].Page.Checksums["scope-a"] = malformed
	wrongBound := failures[4].Page.Checksums["scope-a"]
	wrongBound.Boundary.CommitLSN++
	failures[4].Page.Checksums["scope-a"] = wrongBound
	for index := range failures {
		failures[index].Page.Accepted = false
		failures[index].Page.CursorAdvanced = false
	}
	observation := issue49TerminalChecksumObservation{
		ActiveBefore: activeBefore,
		Nonterminal:  nonterminal,
		Terminal:     terminal,
		Failures:     failures,
	}
	mutant := observation
	mutant.Failures = append([]issue49ChecksumFailure(nil), observation.Failures...)
	mutant.Failures[1].Page.CursorAdvanced = true
	issue49Proof(t, "SYNC-INTEGRITY-006", issue49TerminalChecksumMapsValid(observation), issue49TerminalChecksumMapsValid(mutant))
}

func TestIssue49ImmutableSchemaManifest(t *testing.T) {
	body := `{"compatibility_floor":8,"parent_schema":{"hash":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","version":7},"schema_version":8,"tables":[{"composition":"multi_scope","fields":[{"field_id":"fld-id","type":"string"},{"field_id":"fld-title","type":"string"}],"table_id":"tbl-documents"},{"composition":"single_scope","fields":[{"field_id":"fld-count","type":"int"},{"field_id":"fld-id","type":"string"}],"table_id":"tbl-totals"}],"transition_class":"class_3"}`
	manifest := issue49PublishedManifest{
		Version:            8,
		Hash:               "efc8b4c7bdf8adcb35c9a3a3c1f0753aac09f9f4c795f960193cf5fa4e8fbdda",
		Body:               body,
		ParentVersion:      7,
		ParentHash:         "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		TransitionClass:    "class_3",
		CompatibilityFloor: 8,
		Tables: []issue49ManifestTable{
			{TableID: "tbl-documents", Composition: "multi_scope", Fields: []issue49ManifestField{{FieldID: "fld-id", Type: "string"}, {FieldID: "fld-title", Type: "string"}}},
			{TableID: "tbl-totals", Composition: "single_scope", Fields: []issue49ManifestField{{FieldID: "fld-count", Type: "int"}, {FieldID: "fld-id", Type: "string"}}},
		},
	}
	observation := issue49ManifestObservation{
		Published:           manifest,
		Historical:          manifest,
		Served:              manifest,
		LiveCatalogBody:     `{"compatibility_floor":8,"parent_schema":{"hash":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","version":7},"schema_version":8,"tables":[{"composition":"multi_scope","fields":[{"field_id":"fld-id","type":"string"},{"field_id":"fld-title","type":"bytes"}],"table_id":"tbl-documents"},{"composition":"single_scope","fields":[{"field_id":"fld-count","type":"int"},{"field_id":"fld-id","type":"string"}],"table_id":"tbl-totals"}],"transition_class":"class_3"}`,
		PublishedAtomically: true,
	}
	mutant := observation
	mutant.Served.Body = observation.LiveCatalogBody
	issue49Proof(t, "SYNC-SCHEMA-003", issue49ImmutableManifestValid(observation), issue49ImmutableManifestValid(mutant))
}

func issue49CanonicalTypedRowFixture() issue49TypedRow {
	return issue49TypedRow{
		TableID:        "tbl-documents",
		PrimaryFieldID: "00-pk",
		PrimaryValue:   "row-1",
		Manifest: []issue49TypedFieldDefinition{
			{ID: "00-pk", Type: "string", Primary: true},
			{ID: "01-string", Type: "string"},
			{ID: "02-int", Type: "int"},
			{ID: "03-int64", Type: "int64"},
			{ID: "04-decimal", Type: "decimal"},
			{ID: "05-float", Type: "float"},
			{ID: "06-boolean", Type: "boolean"},
			{ID: "07-datetime", Type: "datetime"},
			{ID: "08-date", Type: "date"},
			{ID: "09-time", Type: "time"},
			{ID: "10-json", Type: "json"},
			{ID: "11-bytes", Type: "bytes"},
			{ID: "12-null", Type: "string"},
		},
		Fields: []issue49TypedFieldValue{
			{ID: "07-datetime", Type: "datetime", Value: "2026-09-09T12:34:56.123456Z"},
			{ID: "02-int", Type: "int", Value: "-17"},
			{ID: "11-bytes", Type: "bytes", Value: "AAEC_w"},
			{ID: "00-pk", Type: "string", Value: "row-1"},
			{ID: "05-float", Type: "float", Value: "1.5"},
			{ID: "12-null", Type: "string", Null: true},
			{ID: "03-int64", Type: "int64", Value: "9007199254740992"},
			{ID: "10-json", Type: "json", Value: `{"a":1,"b":[true,null]}`},
			{ID: "06-boolean", Type: "boolean", Value: "true"},
			{ID: "01-string", Type: "string", Value: "café"},
			{ID: "09-time", Type: "time", Value: "12:34:56.123456"},
			{ID: "04-decimal", Type: "decimal", Value: "-12.34"},
			{ID: "08-date", Type: "date", Value: "2026-09-09"},
		},
	}
}

func issue49MalformedTypedRows(row issue49TypedRow) []issue49MalformedTypedRow {
	unknown := cloneIssue49TypedRow(row)
	unknown.Fields = append(unknown.Fields, issue49TypedFieldValue{ID: "99-unknown", Type: "string", Value: "unexpected"})

	duplicate := cloneIssue49TypedRow(row)
	duplicate.Fields = append(duplicate.Fields, duplicate.Fields[0])

	omitted := cloneIssue49TypedRow(row)
	omitted.Fields = omitted.Fields[1:]

	mistyped := cloneIssue49TypedRow(row)
	mistyped.Fields[0].Type = "string"

	alias := cloneIssue49TypedRow(row)
	alias.Manifest[2].Type = "integer"
	alias.Fields[1].Type = "integer"

	alternateCase := cloneIssue49TypedRow(row)
	alternateCase.Manifest[1].Type = "String"
	alternateCase.Fields[9].Type = "String"

	physicalType := cloneIssue49TypedRow(row)
	physicalType.Manifest[1].Type = "text"
	physicalType.Fields[9].Type = "text"

	primaryMismatch := cloneIssue49TypedRow(row)
	primaryMismatch.PrimaryValue = "row-2"

	return []issue49MalformedTypedRow{
		{Kind: "unknown", Row: unknown},
		{Kind: "duplicate", Row: duplicate},
		{Kind: "omitted", Row: omitted},
		{Kind: "mistyped", Row: mistyped},
		{Kind: "alias", Row: alias},
		{Kind: "alternate-case", Row: alternateCase},
		{Kind: "physical-type", Row: physicalType},
		{Kind: "primary-key-mismatch", Row: primaryMismatch},
	}
}

func cloneIssue49TypedRow(source issue49TypedRow) issue49TypedRow {
	clone := source
	clone.Manifest = append([]issue49TypedFieldDefinition(nil), source.Manifest...)
	clone.Fields = append([]issue49TypedFieldValue(nil), source.Fields...)
	return clone
}

func issue49CompleteChecksumMap(boundary issue49EffectPosition) map[string]issue49BoundScopeChecksum {
	return map[string]issue49BoundScopeChecksum{
		"scope-a":       issue49ScopeChecksum("scope-a", boundary, issue49RowDigestOne),
		"scope-new":     issue49ScopeChecksum("scope-new", boundary, issue49RowDigestTwo),
		"scope-rebuild": issue49ScopeChecksum("scope-rebuild", boundary, "10cd0e9a639d29dd50e98fe041338617c06433c4eb9d39cbfdb4047918665301"),
	}
}

func issue49ScopeChecksum(scope string, boundary issue49EffectPosition, digest string) issue49BoundScopeChecksum {
	return issue49BoundScopeChecksum{
		ScopeID:    scope,
		SchemaHash: issue49SchemaHash,
		Boundary:   boundary,
		Checksum: issue49ChecksumObject{
			Algorithm: "sha256",
			Version:   1,
			Encoding:  "hex",
			Digest:    digest,
		},
	}
}

func cloneIssue49PullChecksumPage(source issue49PullChecksumPage) issue49PullChecksumPage {
	clone := source
	clone.AddScopes = append([]string(nil), source.AddScopes...)
	clone.RemoveScopes = append([]string(nil), source.RemoveScopes...)
	clone.RebuildScopes = append([]string(nil), source.RebuildScopes...)
	if source.Checksums != nil {
		clone.Checksums = make(map[string]issue49BoundScopeChecksum, len(source.Checksums))
		for scope, checksum := range source.Checksums {
			clone.Checksums[scope] = checksum
		}
	}
	return clone
}
