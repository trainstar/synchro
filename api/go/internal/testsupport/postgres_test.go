package testsupport

import (
	"regexp"
	"strings"
	"testing"
)

func TestUniqueName(t *testing.T) {
	const prefix = "Long Prefix/With Spaces"
	name := UniqueName(t, prefix)

	if len(name) > 63 {
		t.Fatalf("UniqueName length = %d, want at most 63 bytes", len(name))
	}
	if !regexp.MustCompile(`^[a-z0-9_]+$`).MatchString(name) {
		t.Fatalf("UniqueName = %q, want a lowercase PostgreSQL identifier", name)
	}
	if !strings.Contains(name, "long_prefix_with_spaces") {
		t.Fatalf("UniqueName = %q, want sanitized prefix", name)
	}
	if strings.Contains(name, "Long Prefix") {
		t.Fatalf("UniqueName = %q, negative control matched unsanitized prefix", name)
	}

	other := UniqueName(t, "other")
	if name == other {
		t.Fatal("UniqueName returned the same name for distinct prefixes")
	}
}

func TestQuoteIdentifier(t *testing.T) {
	got := quoteIdentifier(`role"name`)
	if got != `"role""name"` {
		t.Fatalf("quoteIdentifier() = %q, want escaped identifier", got)
	}
	if got == `"role"name"` {
		t.Fatal("quoteIdentifier negative control accepted an unescaped quote")
	}
}

func TestQuoteLiteral(t *testing.T) {
	got := quoteLiteral("owner's role")
	if got != `'owner''s role'` {
		t.Fatalf("quoteLiteral() = %q, want escaped literal", got)
	}
	if got == `'owner's role'` {
		t.Fatal("quoteLiteral negative control accepted an unescaped quote")
	}
}

func TestContainsLibrary(t *testing.T) {
	value := "  pg_stat_statements, synchro_pg , other_library  "
	if !containsLibrary(value, "synchro_pg") {
		t.Fatalf("containsLibrary() = false, want exact library match with whitespace")
	}
	if containsLibrary(value, "synchro") {
		t.Fatal("containsLibrary negative control accepted a partial library name")
	}
	if containsLibrary("other_library", "synchro_pg") {
		t.Fatal("containsLibrary negative control accepted an absent library")
	}
}
