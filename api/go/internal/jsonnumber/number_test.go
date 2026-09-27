package jsonnumber

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
)

type floatWireCase struct {
	Source    string `json:"source"`
	Canonical string `json:"canonical"`
}

func TestCanonicalWritesSharedFloatWireText(t *testing.T) {
	data, err := os.ReadFile("../../../../conformance/protocol/float-wire-boundaries-v1.json")
	if err != nil {
		t.Fatalf("read shared float wire cases: %v", err)
	}
	var document struct {
		Version int             `json:"version"`
		Cases   []floatWireCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &document); err != nil || document.Version != 1 || len(document.Cases) == 0 {
		t.Fatalf("decode shared float wire cases: version=%d cases=%d err=%v", document.Version, len(document.Cases), err)
	}
	for _, testCase := range document.Cases {
		t.Run(testCase.Source, func(t *testing.T) {
			want, err := strconv.ParseFloat(testCase.Source, 64)
			if err != nil {
				t.Fatalf("parse source: %v", err)
			}
			// Canonical text never carries negative zero.
			if want == 0 {
				want = 0
			}
			for _, text := range []string{testCase.Source, testCase.Canonical} {
				value, canonical, err := Canonical(text)
				if err != nil {
					t.Fatalf("Canonical(%q): %v", text, err)
				}
				if canonical != testCase.Canonical {
					t.Fatalf("Canonical(%q) text = %q, want %q", text, canonical, testCase.Canonical)
				}
				if math.Float64bits(value) != math.Float64bits(want) {
					t.Fatalf("Canonical(%q) value bits = %016x, want %016x", text, math.Float64bits(value), math.Float64bits(want))
				}
			}
		})
	}
}

func TestCanonicalRejectsNonFiniteText(t *testing.T) {
	for _, text := range []string{"1e400", "-1e400", "NaN", "Infinity"} {
		if _, _, err := Canonical(text); err == nil {
			t.Fatalf("Canonical(%q) accepted a value outside finite binary64", text)
		}
	}
}

// benchmarkDocument repeats one pull change. Its four float tokens come from floats.
func benchmarkDocument(changes int, floats [4]string) []byte {
	var document bytes.Buffer
	document.WriteString(`{"changes": [`)
	for index := 0; index < changes; index++ {
		if index > 0 {
			document.WriteString(", ")
		}
		fmt.Fprintf(&document,
			`{"row": {"fld_id": "row-%08d", "fld_a": %s, "fld_b": %s, "fld_c": %s, "fld_d": %s, "fld_count": %d, "fld_note": "0.0000001"}, "server_version": "v-%08d"}`,
			index, floats[0], floats[1], floats[2], floats[3], index, index)
	}
	document.WriteString(`], "has_more": false}`)
	return document.Bytes()
}

func BenchmarkCanonicalizeTokens(b *testing.B) {
	canonical := [4]string{"1e-7", "1e+21", "5", "1.5"}
	postgres := [4]string{"0.0000001", "1000000000000000000000", "5.0", "1.50"}
	for _, size := range []struct {
		name    string
		changes int
	}{{"small", 4}, {"large", 8192}} {
		want := benchmarkDocument(size.changes, canonical)
		for _, input := range []struct {
			name   string
			floats [4]string
		}{{"canonical", canonical}, {"postgres", postgres}} {
			document := benchmarkDocument(size.changes, input.floats)
			b.Run(size.name+"/"+input.name, func(b *testing.B) {
				got, err := CanonicalizeTokens(document)
				if err != nil || !bytes.Equal(got, want) {
					b.Fatalf("output differs from the canonical document: err=%v", err)
				}
				b.SetBytes(int64(len(document)))
				for b.Loop() {
					if _, err := CanonicalizeTokens(document); err != nil {
						b.Fatal(err)
					}
				}
				// b.Loop resets custom metrics on its first call.
				b.ReportMetric(float64(len(document)), "input-bytes")
			})
		}
	}
}
