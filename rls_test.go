package synchro

import (
	"strings"
	"testing"
)

func TestGenerateRLSPolicies_NoOwnerColumn(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{
		TableName: "products",
	})

	stmts := GenerateRLSPolicies(r, "user_id")
	// ENABLE + FORCE + SELECT policy = 3 statements
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want 3", len(stmts))
	}

	for _, s := range stmts {
		if strings.Contains(s, "FOR INSERT") || strings.Contains(s, "FOR UPDATE") || strings.Contains(s, "FOR DELETE") {
			t.Fatalf("reference table should not have write policies: %s", s)
		}
	}
}

func TestGenerateRLSPolicies_OwnerTable_NotNullable(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{
		TableName:      "orders",
		columnNullable: map[string]bool{"user_id": false, "id": false},
	})

	stmts := GenerateRLSPolicies(r, "user_id")
	selectFound := false
	for _, s := range stmts {
		if strings.Contains(s, "FOR SELECT") {
			selectFound = true
			if !strings.Contains(s, `"user_id"::text = current_setting('app.user_id', true)`) {
				t.Errorf("SELECT policy missing owner check: %s", s)
			}
			if strings.Contains(s, "IS NULL OR") {
				t.Errorf("non-nullable owner table should not allow NULL-owner reads: %s", s)
			}
		}
	}
	if !selectFound {
		t.Fatal("no SELECT policy found")
	}
}

func TestGenerateRLSPolicies_OwnerTable_Nullable(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{
		TableName:      "categories",
		columnNullable: map[string]bool{"user_id": true, "id": false},
	})

	stmts := GenerateRLSPolicies(r, "user_id")
	for _, s := range stmts {
		if strings.Contains(s, "FOR SELECT") && !strings.Contains(s, "IS NULL OR") {
			t.Errorf("nullable owner table should allow NULL-owner reads: %s", s)
		}
	}
}

func TestGenerateRLSPolicies_ChildTable(t *testing.T) {
	r := NewRegistry()
	r.RegisterForTest(&TableConfig{
		TableName:      "orders",
		columnNullable: map[string]bool{"user_id": false, "id": false},
	})
	r.RegisterForTest(&TableConfig{
		TableName:   "order_details",
		parentTable: "orders",
		parentFKCol: "order_id",
	})

	stmts := GenerateRLSPolicies(r, "user_id")
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
