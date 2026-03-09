package synchro

import (
	"testing"
	"time"
)

func TestNewCompactor_Defaults(t *testing.T) {
	c := NewCompactor(nil)

	if c.staleThreshold != 7*24*time.Hour {
		t.Errorf("staleThreshold = %v, want 7 days", c.staleThreshold)
	}
	if c.batchSize != 10000 {
		t.Errorf("batchSize = %d, want 10000", c.batchSize)
	}
	if c.logger == nil {
		t.Error("logger should not be nil")
	}
}

func TestNewCompactor_CustomConfig(t *testing.T) {
	cfg := &CompactorConfig{
		StaleThreshold: 48 * time.Hour,
		BatchSize:      500,
	}
	c := NewCompactor(cfg)

	if c.staleThreshold != 48*time.Hour {
		t.Errorf("staleThreshold = %v, want 48h", c.staleThreshold)
	}
	if c.batchSize != 500 {
		t.Errorf("batchSize = %d, want 500", c.batchSize)
	}
}

func TestNewCompactor_ZeroValues_UseDefaults(t *testing.T) {
	cfg := &CompactorConfig{
		StaleThreshold: 0,
		BatchSize:      0,
	}
	c := NewCompactor(cfg)

	if c.staleThreshold != 7*24*time.Hour {
		t.Errorf("staleThreshold = %v, want 7 days (default)", c.staleThreshold)
	}
	if c.batchSize != 10000 {
		t.Errorf("batchSize = %d, want 10000 (default)", c.batchSize)
	}
}

func TestCompact_ZeroSafeSeq_IsNoop(t *testing.T) {
	c := NewCompactor(nil)
	// safeSeq=0 should return immediately with 0 deleted, no DB calls needed.
	// We can't call Compact with a nil DB, but we can verify the early return
	// path by passing safeSeq <= 0.
	deleted, err := c.Compact(nil, nil, 0)
	if err != nil {
		t.Fatalf("Compact(0) err = %v", err)
	}
	if deleted != 0 {
		t.Errorf("Compact(0) deleted = %d, want 0", deleted)
	}

	deleted, err = c.Compact(nil, nil, -1)
	if err != nil {
		t.Fatalf("Compact(-1) err = %v", err)
	}
	if deleted != 0 {
		t.Errorf("Compact(-1) deleted = %d, want 0", deleted)
	}
}
