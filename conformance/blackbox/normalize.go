package blackbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

const dynamicFieldMarker = "<contract-declared-dynamic>"

// NormalizationSpec lists exact JSON Pointer paths declared as dynamic.
type NormalizationSpec struct {
	DynamicFields []string
}

// CanonicalResponseBytes returns deterministic JSON bytes without changing values.
func CanonicalResponseBytes(body []byte) ([]byte, error) {
	value, err := decodeJSON(body)
	if err != nil {
		return nil, err
	}
	return encodeCanonicalJSON(value)
}

// NormalizeResponse replaces only fields named by the closed specification.
func NormalizeResponse(body []byte, spec NormalizationSpec) ([]byte, error) {
	value, err := decodeJSON(body)
	if err != nil {
		return nil, err
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("normalized response must be a JSON object")
	}
	seen := make(map[string]struct{}, len(spec.DynamicFields))
	for _, pointer := range spec.DynamicFields {
		if _, duplicate := seen[pointer]; duplicate {
			return nil, fmt.Errorf("dynamic field %q is duplicated", pointer)
		}
		seen[pointer] = struct{}{}
		segments, err := parseJSONPointer(pointer)
		if err != nil {
			return nil, err
		}
		if err := replaceDynamicField(value, segments); err != nil {
			return nil, fmt.Errorf("normalize dynamic field %q: %w", pointer, err)
		}
	}
	return encodeCanonicalJSON(value)
}

// DecodeStrictResponse rejects duplicate, unknown, and trailing members.
func DecodeStrictResponse(body []byte, destination any) error {
	if destination == nil {
		return errors.New("strict response destination is required")
	}
	value := reflect.ValueOf(destination)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return errors.New("strict response destination must be a non-nil pointer")
	}
	if _, err := decodeJSON(body); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode strict response: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	return nil
}

func decodeJSON(body []byte) (any, error) {
	if !utf8.Valid(body) {
		return nil, errors.New("JSON response contains invalid UTF-8")
	}
	if err := validateUniqueJSONMembers(body); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode JSON response: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	return value, nil
}

func validateUniqueJSONMembers(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := inspectJSONValue(decoder); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func inspectJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON token: %w", err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode JSON member: %w", err)
			}
			name, ok := nameToken.(string)
			if !ok {
				return errors.New("JSON member name is not a string")
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("duplicate JSON member %q", name)
			}
			seen[name] = struct{}{}
			if err := inspectJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("JSON object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := inspectJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("JSON array is not closed")
		}
	default:
		return errors.New("JSON contains an unexpected delimiter")
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("JSON response contains more than one value")
		}
		return fmt.Errorf("decode trailing JSON response: %w", err)
	}
	return nil
}

func encodeCanonicalJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "")
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("encode canonical JSON response: %w", err)
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'}), nil
}

func parseJSONPointer(pointer string) ([]string, error) {
	if pointer == "" || pointer[0] != '/' {
		return nil, errors.New("dynamic field must use a non-root JSON Pointer")
	}
	encoded := strings.Split(pointer[1:], "/")
	segments := make([]string, len(encoded))
	for index, segment := range encoded {
		var builder strings.Builder
		for offset := 0; offset < len(segment); offset++ {
			if segment[offset] != '~' {
				builder.WriteByte(segment[offset])
				continue
			}
			if offset+1 >= len(segment) {
				return nil, errors.New("dynamic field has an invalid JSON Pointer escape")
			}
			offset++
			switch segment[offset] {
			case '0':
				builder.WriteByte('~')
			case '1':
				builder.WriteByte('/')
			default:
				return nil, errors.New("dynamic field has an invalid JSON Pointer escape")
			}
		}
		segments[index] = builder.String()
	}
	return segments, nil
}

func replaceDynamicField(root any, segments []string) error {
	if len(segments) == 0 {
		return errors.New("dynamic field cannot replace the response root")
	}
	current := root
	for index, segment := range segments {
		last := index == len(segments)-1
		switch typed := current.(type) {
		case map[string]any:
			value, found := typed[segment]
			if !found {
				return errors.New("declared dynamic member is absent")
			}
			if last {
				if !isJSONScalar(value) {
					return errors.New("declared dynamic member is not scalar")
				}
				typed[segment] = dynamicFieldMarker
				return nil
			}
			current = value
		case []any:
			position, err := strconv.Atoi(segment)
			if err != nil || position < 0 || position >= len(typed) || strconv.Itoa(position) != segment {
				return errors.New("declared dynamic array position is invalid")
			}
			if last {
				if !isJSONScalar(typed[position]) {
					return errors.New("declared dynamic array value is not scalar")
				}
				typed[position] = dynamicFieldMarker
				return nil
			}
			current = typed[position]
		default:
			return errors.New("declared dynamic path crosses a scalar")
		}
	}
	return errors.New("declared dynamic field is invalid")
}

func isJSONScalar(value any) bool {
	switch value.(type) {
	case nil, bool, string, json.Number:
		return true
	default:
		return false
	}
}
