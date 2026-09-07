package invariants

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

func decodeRawObject(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object map[string]json.RawMessage
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errors.New("value is not a JSON object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("JSON object has trailing content")
	}
	return object, nil
}

func decodeRawArray(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '[' {
		return nil, errors.New("value is not a JSON array")
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return nil, errors.New("value is not a JSON array")
	}
	return values, nil
}

func decodeJSONString(raw json.RawMessage) (string, bool) {
	var value string
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

func decodePositiveUint64(raw json.RawMessage) (uint64, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var number json.Number
	if decoder.Decode(&number) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return 0, false
	}
	value, err := strconv.ParseUint(string(number), 10, 64)
	return value, err == nil && value != 0
}

func validLowerHexDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func validUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, character := range value {
		switch index {
		case 8, 13, 18, 23:
			if character != '-' {
				return false
			}
		default:
			if character < '0' || character > '9' && character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}

func equalRawJSON(left, right json.RawMessage) bool {
	leftNormalized, leftErr := normalizeRawJSON(left)
	rightNormalized, rightErr := normalizeRawJSON(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftNormalized, rightNormalized)
}

func decodeChecksum(raw json.RawMessage) ([32]byte, bool) {
	object, err := decodeRawObject(raw)
	if err != nil || len(object) != 4 {
		return [32]byte{}, false
	}
	algorithm, algorithmOK := decodeJSONString(object["algorithm"])
	encoding, encodingOK := decodeJSONString(object["encoding"])
	version, versionOK := decodePositiveUint64(object["version"])
	digest, digestOK := decodeJSONString(object["digest"])
	if !algorithmOK || algorithm != "sha256" || !encodingOK || encoding != "hex" || !versionOK || version != 1 || !digestOK || !validLowerHexDigest(digest) {
		return [32]byte{}, false
	}
	decoded, _ := hex.DecodeString(digest)
	var result [32]byte
	copy(result[:], decoded)
	return result, true
}
