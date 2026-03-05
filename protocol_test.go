package synchro

import "testing"

func TestOperation_Constants(t *testing.T) {
	if OpInsert != 1 {
		t.Errorf("OpInsert = %d, want 1", OpInsert)
	}
	if OpUpdate != 2 {
		t.Errorf("OpUpdate = %d, want 2", OpUpdate)
	}
	if OpDelete != 3 {
		t.Errorf("OpDelete = %d, want 3", OpDelete)
	}
}

func TestOperation_String(t *testing.T) {
	tests := []struct {
		op   Operation
		want string
	}{
		{OpInsert, "create"},
		{OpUpdate, "update"},
		{OpDelete, "delete"},
		{Operation(0), "unknown"},
		{Operation(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.op.String(); got != tt.want {
				t.Errorf("Operation(%d).String() = %q, want %q", tt.op, got, tt.want)
			}
		})
	}
}

func TestParseOperation(t *testing.T) {
	tests := []struct {
		input string
		want  Operation
		ok    bool
	}{
		{"create", OpInsert, true},
		{"update", OpUpdate, true},
		{"delete", OpDelete, true},
		{"insert", 0, false},
		{"", 0, false},
		{"CREATE", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := ParseOperation(tt.input)
			if ok != tt.ok {
				t.Errorf("ParseOperation(%q) ok = %v, want %v", tt.input, ok, tt.ok)
			}
			if got != tt.want {
				t.Errorf("ParseOperation(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseOperation_StringRoundTrip(t *testing.T) {
	ops := []Operation{OpInsert, OpUpdate, OpDelete}
	for _, op := range ops {
		s := op.String()
		parsed, ok := ParseOperation(s)
		if !ok {
			t.Errorf("ParseOperation(%q) returned ok=false for valid Operation(%d)", s, op)
			continue
		}
		if parsed != op {
			t.Errorf("round-trip failed: Operation(%d) -> %q -> Operation(%d)", op, s, parsed)
		}
	}
}
