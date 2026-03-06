package synchro

import (
	"testing"
)

func TestExpandSlicePlaceholder_Single(t *testing.T) {
	args := []any{"existing"}
	sql, args, nextIdx := expandSlicePlaceholder([]string{"a"}, 2, args)

	if sql != "($2)" {
		t.Errorf("sql = %q, want %q", sql, "($2)")
	}
	if len(args) != 2 || args[1] != "a" {
		t.Errorf("args = %v, want [existing a]", args)
	}
	if nextIdx != 3 {
		t.Errorf("nextIdx = %d, want 3", nextIdx)
	}
}

func TestExpandSlicePlaceholder_Multiple(t *testing.T) {
	args := []any{"x", "y"}
	sql, args, nextIdx := expandSlicePlaceholder([]string{"a", "b", "c"}, 3, args)

	if sql != "($3, $4, $5)" {
		t.Errorf("sql = %q, want %q", sql, "($3, $4, $5)")
	}
	if len(args) != 5 {
		t.Fatalf("args len = %d, want 5", len(args))
	}
	if args[2] != "a" || args[3] != "b" || args[4] != "c" {
		t.Errorf("args = %v, want [x y a b c]", args)
	}
	if nextIdx != 6 {
		t.Errorf("nextIdx = %d, want 6", nextIdx)
	}
}

func TestExpandSlicePlaceholder_Empty(t *testing.T) {
	args := []any{"keep"}
	sql, args, nextIdx := expandSlicePlaceholder([]string{}, 1, args)

	if sql != "(NULL)" {
		t.Errorf("sql = %q, want %q", sql, "(NULL)")
	}
	if len(args) != 1 || args[0] != "keep" {
		t.Errorf("args = %v, want [keep]", args)
	}
	if nextIdx != 1 {
		t.Errorf("nextIdx = %d, want 1", nextIdx)
	}
}

func TestExpandSlicePlaceholder_StartIdx1(t *testing.T) {
	var args []any
	sql, args, nextIdx := expandSlicePlaceholder([]string{"x", "y"}, 1, args)

	if sql != "($1, $2)" {
		t.Errorf("sql = %q, want %q", sql, "($1, $2)")
	}
	if len(args) != 2 {
		t.Fatalf("args len = %d, want 2", len(args))
	}
	if nextIdx != 3 {
		t.Errorf("nextIdx = %d, want 3", nextIdx)
	}
}
