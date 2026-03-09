package synchro

import (
	"strings"
	"testing"
)

func TestGenerateRLSPolicies_PushDisabledTable(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:  "products",
		PushPolicy: PushPolicyDisabled,
	})

	stmts := GenerateRLSPolicies(r)
	// ENABLE + FORCE + SELECT policy = 3 statements
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want 3", len(stmts))
	}

	for _, s := range stmts {
		if strings.Contains(s, "FOR INSERT") || strings.Contains(s, "FOR UPDATE") || strings.Contains(s, "FOR DELETE") {
			t.Fatalf("push-disabled table should not have write policies: %s", s)
		}
	}
}

func TestGenerateRLSPolicies_OwnerTable_DefaultNoNullRead(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "orders",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})

	stmts := GenerateRLSPolicies(r)
	selectFound := false
	for _, s := range stmts {
		if strings.Contains(s, "FOR SELECT") {
			selectFound = true
			if !strings.Contains(s, `"user_id"::text = current_setting('app.user_id', true)`) {
				t.Errorf("SELECT policy missing owner check: %s", s)
			}
			if strings.Contains(s, "IS NULL OR") {
				t.Errorf("default owner table should not allow NULL-owner reads: %s", s)
			}
		}
	}
	if !selectFound {
		t.Fatal("no SELECT policy found")
	}
}

func TestGenerateRLSPolicies_OwnerTable_GlobalReadOptIn(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:       "categories",
		PushPolicy:      PushPolicyOwnerOnly,
		OwnerColumn:     "user_id",
		AllowGlobalRead: true,
	})

	stmts := GenerateRLSPolicies(r)
	for _, s := range stmts {
		if strings.Contains(s, "FOR SELECT") && !strings.Contains(s, "IS NULL OR") {
			t.Errorf("global-read opt-in table should allow NULL-owner reads: %s", s)
		}
	}
}

func TestGenerateRLSPolicies_ChildTable(t *testing.T) {
	r := NewRegistry()
	r.Register(&TableConfig{
		TableName:   "orders",
		PushPolicy:  PushPolicyOwnerOnly,
		OwnerColumn: "user_id",
	})
	r.Register(&TableConfig{
		TableName:   "order_details",
		PushPolicy:  PushPolicyOwnerOnly,
		ParentTable: "orders",
		ParentFKCol: "order_id",
	})

	stmts := GenerateRLSPolicies(r)
	var child []string
	for _, s := range stmts {
		if strings.Contains(s, `"order_details"`) {
			child = append(child, s)
		}
	}
	// ENABLE + FORCE + SELECT + INSERT + UPDATE + DELETE = 6 statements per child table
	if len(child) != 6 {
		t.Fatalf("got %d child statements, want 6", len(child))
	}
	for _, s := range child {
		if strings.Contains(s, "FOR SELECT") || strings.Contains(s, "FOR INSERT") {
			if !strings.Contains(s, "EXISTS") {
				t.Errorf("child policy should use EXISTS subquery: %s", s)
			}
			if !strings.Contains(s, `"orders"`) {
				t.Errorf("child policy should reference parent table: %s", s)
			}
		}
	}
}
