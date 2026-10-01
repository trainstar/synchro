// Package jsonnumber writes JSON number text in the RFC 8785 spelling of its
// finite IEEE 754 binary64 value.
package jsonnumber

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

var errMalformedDocument = errors.New("JSON document is malformed")

// Canonical parses JSON number text as a finite binary64 value. It returns
// that value and its RFC 8785 number text. Negative zero becomes 0.
func Canonical(text string) (float64, string, error) {
	parsed, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return 0, "", errors.New("JSON number is outside finite binary64")
	}
	if parsed == 0 {
		return 0, "0", nil
	}
	encoded, err := json.Marshal(parsed)
	if err != nil {
		return 0, "", fmt.Errorf("canonicalizing JSON number: %w", err)
	}
	return parsed, string(encoded), nil
}

// CanonicalizeTokens replaces each number token whose text differs from
// Canonical. All other bytes stay unchanged, and the input slice is returned
// when no token changes. The input must be exactly one complete UTF-8 JSON
// value. Error text never contains document content.
func CanonicalizeTokens(document []byte) ([]byte, error) {
	// json.Valid owns the grammar and rejects trailing input. In valid JSON,
	// only a number starts with '-' or a digit outside a string.
	if !utf8.Valid(document) || !json.Valid(document) {
		return nil, errMalformedDocument
	}
	var output []byte
	copied := 0
	for index := 0; index < len(document); {
		switch character := document[index]; {
		case character == '"':
			for index++; document[index] != '"'; index++ {
				if document[index] == '\\' {
					index++
				}
			}
			index++
		case character == '-' || '0' <= character && character <= '9':
			start := index
			for index < len(document) && strings.IndexByte("+-.0123456789Ee", document[index]) >= 0 {
				index++
			}
			token := string(document[start:index])
			_, canonical, err := Canonical(token)
			if err != nil {
				return nil, err
			}
			if canonical == token {
				continue
			}
			if output == nil {
				output = make([]byte, 0, len(document))
			}
			output = append(output, document[copied:start]...)
			output = append(output, canonical...)
			copied = index
		default:
			index++
		}
	}
	if output == nil {
		return document, nil
	}
	return append(output, document[copied:]...), nil
}
