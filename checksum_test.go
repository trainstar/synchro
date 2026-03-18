package synchro

import (
	"encoding/json"
	"hash/crc32"
	"testing"
)

func TestComputeRecordChecksum_Deterministic(t *testing.T) {
	// Same data, different key ordering in JSON string
	json1 := `{"alpha":"one","beta":"two","gamma":"three"}`
	json2 := `{"gamma":"three","alpha":"one","beta":"two"}`

	cs1 := ComputeRecordChecksum(json1)
	cs2 := ComputeRecordChecksum(json2)

	if cs1 != cs2 {
		t.Fatalf("checksums differ for same data with different key order: %d vs %d", cs1, cs2)
	}
}

func TestComputeRecordChecksum_KnownVector(t *testing.T) {
	// json.Marshal sorts keys alphabetically, so {"a":"1","b":"2"} is canonical.
	input := `{"b":"2","a":"1"}`
	canonical := `{"a":"1","b":"2"}`
	expected := crc32.ChecksumIEEE([]byte(canonical))

	got := ComputeRecordChecksum(input)
	if got != expected {
		t.Fatalf("checksum = %d, want %d (canonical: %q)", got, expected, canonical)
	}
}

func TestComputeRecordChecksum_EmptyObject(t *testing.T) {
	cs := ComputeRecordChecksum(`{}`)
	expected := crc32.ChecksumIEEE([]byte(`{}`))
	if cs != expected {
		t.Fatalf("checksum = %d, want %d", cs, expected)
	}
}

func TestComputeRecordChecksum_NestedObjects(t *testing.T) {
	json1 := `{"z":{"b":2,"a":1},"a":"first"}`
	json2 := `{"a":"first","z":{"a":1,"b":2}}`

	cs1 := ComputeRecordChecksum(json1)
	cs2 := ComputeRecordChecksum(json2)

	if cs1 != cs2 {
		t.Fatalf("nested object checksums differ: %d vs %d", cs1, cs2)
	}
}

func TestComputeRecordChecksum_DifferentDataDiffers(t *testing.T) {
	cs1 := ComputeRecordChecksum(`{"name":"Alice"}`)
	cs2 := ComputeRecordChecksum(`{"name":"Bob"}`)

	if cs1 == cs2 {
		t.Fatal("different data should produce different checksums")
	}
}

func TestComputeRecordChecksum_InvalidJSON(t *testing.T) {
	// Invalid JSON should not panic; falls back to hashing raw bytes.
	cs := ComputeRecordChecksum("not json")
	expected := crc32.ChecksumIEEE([]byte("not json"))
	if cs != expected {
		t.Fatalf("invalid JSON checksum = %d, want %d", cs, expected)
	}
}

func TestComputeRecordChecksumFromMap(t *testing.T) {
	data := map[string]any{
		"name": "Alice",
		"age":  float64(30),
	}
	cs, err := ComputeRecordChecksumFromMap(data)
	if err != nil {
		t.Fatalf("ComputeRecordChecksumFromMap: %v", err)
	}

	// Verify matches the string-based path
	b, _ := json.Marshal(data)
	expected := crc32.ChecksumIEEE(b)
	if cs != expected {
		t.Fatalf("map checksum = %d, want %d", cs, expected)
	}
}

func TestComputeRecordChecksum_NullValues(t *testing.T) {
	json1 := `{"name":null,"age":null}`
	json2 := `{"age":null,"name":null}`

	cs1 := ComputeRecordChecksum(json1)
	cs2 := ComputeRecordChecksum(json2)

	if cs1 != cs2 {
		t.Fatalf("null value checksums differ: %d vs %d", cs1, cs2)
	}
}
