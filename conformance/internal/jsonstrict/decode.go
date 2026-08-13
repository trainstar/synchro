package jsonstrict

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"unicode/utf8"
)

// ValidateValue validates data as a single JSON object. Objects at any depth
// must not contain duplicate member names after JSON escape decoding.
func ValidateValue(data []byte) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("JSON contains invalid UTF-8")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON document: %w", err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return fmt.Errorf("top-level JSON value must be an object")
	}
	if err := inspectObject(decoder); err != nil {
		return err
	}

	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("JSON document contains more than one value")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	if err := validateUnicodeScalars(data); err != nil {
		return err
	}
	return nil
}

// Decode strictly validates data before decoding it into dst. Numbers decoded
// into interface values remain json.Number values instead of float64 values.
func Decode(data []byte, dst any) error {
	if err := ValidateValue(data); err != nil {
		return err
	}
	value := reflect.ValueOf(dst)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("decode JSON value: %w", err)
	}
	return nil
}

func inspectObject(decoder *json.Decoder) error {
	seen := make(map[string]struct{})
	for {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON object: %w", err)
		}
		if delimiter, ok := token.(json.Delim); ok && delimiter == '}' {
			return nil
		}
		key, ok := token.(string)
		if !ok {
			return fmt.Errorf("JSON object member name is not a string")
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate JSON object member %q", key)
		}
		seen[key] = struct{}{}
		if err := inspectValue(decoder); err != nil {
			return err
		}
	}
}

func inspectArray(decoder *json.Decoder) error {
	for {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON array: %w", err)
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case ']':
				return nil
			case '{':
				if err := inspectObject(decoder); err != nil {
					return err
				}
			case '[':
				if err := inspectArray(decoder); err != nil {
					return err
				}
			}
		}
	}
}

func inspectValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON value: %w", err)
	}
	if delimiter, ok := token.(json.Delim); ok {
		switch delimiter {
		case '{':
			return inspectObject(decoder)
		case '[':
			return inspectArray(decoder)
		}
	}
	return nil
}

// Go replaces lone UTF-16 surrogates during JSON decoding. Reject them before
// decoding so policy digests and JCS input cannot change silently.
func validateUnicodeScalars(data []byte) error {
	for index := 0; index < len(data); {
		if data[index] != '"' {
			index++
			continue
		}
		index++
		for index < len(data) && data[index] != '"' {
			if data[index] != '\\' {
				_, width := utf8.DecodeRune(data[index:])
				index += width
				continue
			}
			if index+1 >= len(data) || data[index+1] != 'u' {
				index += 2
				continue
			}
			value, ok := parseUnicodeEscape(data, index)
			if !ok {
				return fmt.Errorf("JSON contains an invalid Unicode escape")
			}
			switch {
			case value >= 0xd800 && value <= 0xdbff:
				next := index + 6
				low, paired := parseUnicodeEscape(data, next)
				if !paired || low < 0xdc00 || low > 0xdfff {
					return fmt.Errorf("JSON contains a lone UTF-16 surrogate")
				}
				index = next + 6
			case value >= 0xdc00 && value <= 0xdfff:
				return fmt.Errorf("JSON contains a lone UTF-16 surrogate")
			default:
				index += 6
			}
		}
		if index < len(data) {
			index++
		}
	}
	return nil
}

func parseUnicodeEscape(data []byte, start int) (uint16, bool) {
	if start+6 > len(data) || data[start] != '\\' || data[start+1] != 'u' {
		return 0, false
	}
	var value uint16
	for _, character := range data[start+2 : start+6] {
		value <<= 4
		switch {
		case character >= '0' && character <= '9':
			value += uint16(character - '0')
		case character >= 'a' && character <= 'f':
			value += uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			value += uint16(character-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}
