package reference

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/trainstar/synchro/conformance/internal/importguard"
)

func TestReferenceImportIsolation(t *testing.T) {
	moduleRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("resolve conformance module root: %v", err)
	}

	policy := importguard.Policy{
		ModuleRoot:      moduleRoot,
		PackagePatterns: []string{"./reference"},
		Protected:       []string{"github.com/trainstar/synchro/conformance/reference"},
	}
	if err := importguard.Check(context.Background(), policy); err != nil {
		t.Fatalf("reference import isolation failed: %v", err)
	}
}
