package blackbox

import "testing"

func TestRequireCheckpointAtOrAboveFloorUsesStreamOrder(t *testing.T) {
	for _, test := range []struct{ floor, checkpoint string }{
		{"effect|16/0|3|0", "effect|16/0|3|0"},
		{"generation_start|||", "transaction_end|0/24F6640||"},
		{"effect|16/0|3|0", "transaction_end|16/0||"},
		{"transaction_end|16/0||", "effect|16/1|1|0"},
	} {
		if err := RequireCheckpointAtOrAboveFloor(test.floor, test.checkpoint); err != nil {
			t.Fatalf("resumable checkpoint %q at floor %q was rejected: %v", test.checkpoint, test.floor, err)
		}
	}
	for _, test := range []struct{ floor, checkpoint string }{
		{"effect|16/0|3|0", "effect|16/0|2|9"},
		{"transaction_end|16/0||", "effect|16/0|9|9"},
		{"effect|16/0|1|0", "generation_start|||"},
		{"effect|17/0|1|0", "transaction_end|16/FFFFFFFF||"},
		{"effect|16/0|3|0", ""},
		{"floor|16/0||", "transaction_end|16/0||"},
	} {
		if err := RequireCheckpointAtOrAboveFloor(test.floor, test.checkpoint); err == nil {
			t.Fatalf("checkpoint %q below or unrelated to floor %q was accepted", test.checkpoint, test.floor)
		}
	}
}
