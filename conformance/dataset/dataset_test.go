package dataset

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func planDigest(t *testing.T, plan Plan) [32]byte {
	t.Helper()
	hash := sha256.New()
	for _, group := range [][]Transaction{plan.Initial, plan.History} {
		for _, transaction := range group {
			fmt.Fprintf(hash, "%s|%v|%v|", transaction.Kind, transaction.Grants, transaction.Revokes)
			for _, statement := range transaction.Statements {
				fmt.Fprintf(hash, "%s|%#v|", statement.SQL, statement.Args)
			}
		}
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func TestGenerateReplaysOneSeed(t *testing.T) {
	for _, size := range Sizes {
		first, err := Generate(20260927, size)
		if err != nil {
			t.Fatalf("generate %s: %v", size.Name, err)
		}
		second, err := Generate(20260927, size)
		if err != nil {
			t.Fatalf("regenerate %s: %v", size.Name, err)
		}
		if planDigest(t, first) != planDigest(t, second) {
			t.Fatalf("size %s did not replay its seed", size.Name)
		}
		other, err := Generate(20260928, size)
		if err != nil {
			t.Fatalf("generate other seed %s: %v", size.Name, err)
		}
		if planDigest(t, first) == planDigest(t, other) {
			t.Fatalf("size %s ignored its seed", size.Name)
		}
	}
}

func TestGenerateStaysInsideTheDecoderEnvelope(t *testing.T) {
	size, _ := LookupSize("l")
	plan, err := Generate(7, size)
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range [][]Transaction{plan.Initial, plan.History} {
		for _, transaction := range group {
			if transaction.Records > MaxTransactionRecords || transaction.Bytes > 4<<20 {
				t.Fatalf("%s transaction estimate records=%d bytes=%d exceeds the bound", transaction.Kind, transaction.Records, transaction.Bytes)
			}
			if len(transaction.Statements) == 0 && len(transaction.Grants) == 0 && len(transaction.Revokes) == 0 {
				t.Fatalf("%s transaction is empty", transaction.Kind)
			}
		}
	}
	if plan.Stats.UsersInManyOrgs == 0 || plan.Stats.SharedPrograms == 0 || plan.Stats.RepeatedKeyWrites == 0 {
		t.Fatalf("large plan lacks overlap, sharing, or repeated keys: %+v", plan.Stats)
	}
	for _, kind := range []string{"hard-delete-set", "re-create-set", "join-organization", "leave-organization", "flip-program-visibility"} {
		if plan.Stats.HistoryMix[kind] == 0 {
			t.Fatalf("large plan history has no %s operation: %v", kind, plan.Stats.HistoryMix)
		}
	}
}

func TestGenerateRejectsUnsupportedSize(t *testing.T) {
	size, _ := LookupSize("s")
	size.Users = 1 << 20
	if _, err := Generate(1, size); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("unbounded size was accepted: %v", err)
	}
}

func TestRandomUUIDsAreCanonicalVersionFour(t *testing.T) {
	random := NewRandom(3)
	seen := make(map[string]bool)
	for index := 0; index < 10000; index++ {
		value := random.UUID()
		if !uuidV4.MatchString(value) || seen[value] {
			t.Fatalf("UUID %q is invalid or repeated", value)
		}
		seen[value] = true
	}
}

func TestCompareWireRequiresTheSourceValue(t *testing.T) {
	text := func(value string) *string { return &value }
	for _, test := range []struct {
		name, portable, wire string
		source               *string
		match                bool
	}{
		{"decimal canonical", "decimal", `"102.5"`, text("102.5"), true},
		{"decimal trailing zero", "decimal", `"102.50"`, text("102.5"), false},
		{"decimal as number", "decimal", `102.5`, text("102.5"), false},
		{"int64 above binary64", "int64", `"9007199254740993"`, text("9007199254740993"), true},
		{"int64 rounded", "int64", `"9007199254740992"`, text("9007199254740993"), false},
		{"int quoted", "int", `"5"`, text("5"), false},
		{"float exact", "float", `1e-7`, text("1e-07"), true},
		{"float differs", "float", `1e-7`, text("1.0000001e-07"), false},
		{"json key order", "json", `"{\"a\":1,\"b\":[2]}"`, text(`{"b": [2], "a": 1.0}`), true},
		{"json value differs", "json", `"{\"a\":1}"`, text(`{"a": 2}`), false},
		{"bytes base64url", "bytes", `"AP8"`, text("00ff"), true},
		{"bytes padded", "bytes", `"AP8="`, text("00ff"), false},
		{"null", "string", `null`, nil, true},
		{"null for value", "string", `null`, text(""), false},
		{"value for null", "string", `""`, nil, false},
		{"datetime", "datetime", `"2026-03-02T06:30:00.123456Z"`, text("2026-03-02T06:30:00.123456Z"), true},
	} {
		err := CompareWire(test.portable, json.RawMessage(test.wire), test.source)
		if (err == nil) != test.match {
			t.Fatalf("%s: match=%t err=%v", test.name, test.match, err)
		}
	}
}

func TestAuthoredCheckpointsNameCatalogTables(t *testing.T) {
	for name, checkpoint := range map[string]Checkpoint{"initial": AuthoredInitial, "final": AuthoredFinal} {
		for user, scopes := range checkpoint.Assigned {
			for _, scope := range scopes {
				if _, ok := checkpoint.Rows[scope]; !ok {
					t.Fatalf("%s checkpoint assigns %s scope %s without expected rows", name, user, scope)
				}
			}
		}
		for scope, tables := range checkpoint.Rows {
			for table := range tables {
				if _, ok := LookupTable(table); !ok {
					t.Fatalf("%s checkpoint scope %s names unknown table %s", name, scope, table)
				}
			}
		}
		for _, value := range checkpoint.Values {
			table, ok := LookupTable(value.Table)
			if !ok || !hasColumn(table, value.Column) || !json.Valid([]byte(value.Wire)) {
				t.Fatalf("%s checkpoint value %+v is invalid", name, value)
			}
		}
	}
}

func hasColumn(table Table, name string) bool {
	for _, column := range table.Columns {
		if column.Name == name {
			return true
		}
	}
	return false
}
