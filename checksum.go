package synchro

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
)

// ComputeRecordChecksum computes a deterministic CRC32 checksum for a JSON
// record string. It unmarshals the JSON into a map, then re-marshals with
// json.Marshal which sorts keys alphabetically. This guarantees identical
// checksums regardless of the original key order (e.g., PostgreSQL
// row_to_json column-order vs Go alphabetical order).
//
// Uses the IEEE polynomial (ISO 3309 / ITU-T V.42), matching the Swift and
// Kotlin client CRC32 implementations.
func ComputeRecordChecksum(jsonStr string) uint32 {
	var m map[string]any
	if err := json.Unmarshal([]byte(jsonStr), &m); err != nil {
		// If the input is not valid JSON, hash the raw bytes.
		return crc32.ChecksumIEEE([]byte(jsonStr))
	}
	canonical, err := json.Marshal(m)
	if err != nil {
		return crc32.ChecksumIEEE([]byte(jsonStr))
	}
	return crc32.ChecksumIEEE(canonical)
}

// ComputeRecordChecksumFromMap computes a deterministic CRC32 checksum from
// a map that has already been unmarshaled. Marshals with json.Marshal (sorted
// keys) then computes crc32.ChecksumIEEE.
func ComputeRecordChecksumFromMap(data map[string]any) (uint32, error) {
	canonical, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("marshaling record for checksum: %w", err)
	}
	return crc32.ChecksumIEEE(canonical), nil
}
