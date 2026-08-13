package vectors

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"unicode/utf8"
)

var scopeDigestDomain = []byte("synchro:v3:scope-digest:v1\x00")

// ScopeDigest computes one ordered streaming scope digest.
func ScopeDigest(manifestHash [32]byte, scopeID string, rows []DigestEntry) ([32]byte, error) {
	preimage, err := ScopeDigestPreimage(manifestHash, scopeID, rows)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(preimage), nil
}

// ScopeDigestPreimage returns the exact scope-digest hash input.
func ScopeDigestPreimage(manifestHash [32]byte, scopeID string, rows []DigestEntry) ([]byte, error) {
	if scopeID == "" {
		return nil, errors.New("scope_id is empty")
	}
	if err := validateJSONStringValue(scopeID, false); err != nil {
		return nil, fmt.Errorf("scope_id: %w", err)
	}
	ordered := make([]DigestEntry, len(rows))
	for index, row := range rows {
		if err := validateRowIdentity(row.RowIdentity); err != nil {
			return nil, fmt.Errorf("row identity %d: %w", index, err)
		}
		ordered[index] = DigestEntry{
			RowIdentity: append([]byte(nil), row.RowIdentity...),
			RowDigest:   row.RowDigest,
		}
	}
	sort.Slice(ordered, func(left, right int) bool {
		return bytes.Compare(ordered[left].RowIdentity, ordered[right].RowIdentity) < 0
	})
	for index := 1; index < len(ordered); index++ {
		if bytes.Equal(ordered[index-1].RowIdentity, ordered[index].RowIdentity) {
			return nil, errors.New("scope contains a duplicate row identity")
		}
	}

	preimage := append([]byte(nil), scopeDigestDomain...)
	preimage = append(preimage, manifestHash[:]...)
	var err error
	preimage, err = appendText(preimage, scopeID)
	if err != nil {
		return nil, err
	}
	preimage = appendU64(preimage, uint64(len(ordered)))
	for _, row := range ordered {
		preimage = appendBlob(preimage, row.RowIdentity)
		preimage = append(preimage, row.RowDigest[:]...)
	}
	return preimage, nil
}

func validateRowIdentity(identity []byte) error {
	if len(identity) < len(rowIdentityDomain) || !bytes.Equal(identity[:len(rowIdentityDomain)], rowIdentityDomain) {
		return errors.New("invalid domain")
	}
	position := len(rowIdentityDomain)
	var err error
	position, err = consumeRowIdentityText(identity, position, "table_id")
	if err != nil {
		return err
	}
	position, err = consumeRowIdentityText(identity, position, "primary_key_field_id")
	if err != nil {
		return err
	}
	if len(identity)-position < 2 {
		return errors.New("typed primary key is truncated")
	}
	tag := identity[position]
	position++
	switch tag {
	case typeString:
		if identity[position] != 0x01 {
			return errors.New("typed primary key has invalid presence")
		}
		position++
		position, err = consumeRowIdentityTextPayload(identity, position, "primary key")
		if err != nil {
			return err
		}
	case typeInt:
		if identity[position] != 0x01 {
			return errors.New("typed primary key has invalid presence")
		}
		position++
		if len(identity)-position < 4 {
			return errors.New("int primary key payload is truncated")
		}
		position += 4
	case typeInt64:
		if identity[position] != 0x01 {
			return errors.New("typed primary key has invalid presence")
		}
		position++
		if len(identity)-position < 8 {
			return errors.New("int64 primary key payload is truncated")
		}
		position += 8
	default:
		return errors.New("typed primary key has an invalid tag")
	}
	if position != len(identity) {
		return errors.New("row identity has trailing bytes")
	}
	return nil
}

func consumeRowIdentityText(identity []byte, position int, name string) (int, error) {
	position, payload, err := consumeRowIdentityBlob(identity, position, name)
	if err != nil {
		return 0, err
	}
	if len(payload) == 0 {
		return 0, fmt.Errorf("%s is empty", name)
	}
	if !utf8.Valid(payload) {
		return 0, fmt.Errorf("%s is not valid UTF-8", name)
	}
	return position, nil
}

func consumeRowIdentityTextPayload(identity []byte, position int, name string) (int, error) {
	position, payload, err := consumeRowIdentityBlob(identity, position, name+" payload")
	if err != nil {
		return 0, err
	}
	if !utf8.Valid(payload) {
		return 0, fmt.Errorf("%s payload is not valid UTF-8", name)
	}
	return position, nil
}

func consumeRowIdentityBlob(identity []byte, position int, name string) (int, []byte, error) {
	if len(identity)-position < 8 {
		return 0, nil, fmt.Errorf("%s length is truncated", name)
	}
	length := binary.BigEndian.Uint64(identity[position : position+8])
	position += 8
	if length > uint64(len(identity)-position) {
		return 0, nil, fmt.Errorf("%s length exceeds available bytes", name)
	}
	end := position + int(length)
	return end, identity[position:end], nil
}
