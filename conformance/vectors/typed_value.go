package vectors

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gowebpki/jcs"
)

const (
	typeString   byte = 0x01
	typeInt      byte = 0x02
	typeInt64    byte = 0x03
	typeDecimal  byte = 0x04
	typeFloat    byte = 0x05
	typeBoolean  byte = 0x06
	typeDatetime byte = 0x07
	typeDate     byte = 0x08
	typeTime     byte = 0x09
	typeJSON     byte = 0x0a
	typeBytes    byte = 0x0b
)

var (
	canonicalInteger  = regexp.MustCompile(`^(?:0|-[1-9][0-9]*|[1-9][0-9]*)$`)
	canonicalDecimal  = regexp.MustCompile(`^(?:0|-?(?:[1-9][0-9]*(?:\.[0-9]*[1-9])?|0\.[0-9]*[1-9]))$`)
	canonicalDate     = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	canonicalTime     = regexp.MustCompile(`^[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}$`)
	canonicalDateTime = regexp.MustCompile(
		`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z$`,
	)
)

// EncodeTypedValue validates a canonical wire value and returns typed bytes.
func EncodeTypedValue(spec FieldSpec, raw json.RawMessage) ([]byte, error) {
	tag, err := validateFieldSpec(spec)
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, errors.New("typed value is empty")
	}
	if bytes.Equal(trimmed, []byte("null")) {
		if !spec.Nullable {
			return nil, errors.New("non-nullable field contains null")
		}
		return []byte{tag, 0x00}, nil
	}
	if err := validateJSONDocument(trimmed, jsonValidation{}); err != nil {
		return nil, fmt.Errorf("validate typed JSON: %w", err)
	}

	payload, err := typedPayload(spec, trimmed)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, len(payload)+2)
	encoded = append(encoded, tag, 0x01)
	encoded = append(encoded, payload...)
	return encoded, nil
}

func validateFieldSpec(spec FieldSpec) (byte, error) {
	var tag byte
	switch spec.Type {
	case "string":
		tag = typeString
	case "int":
		tag = typeInt
	case "int64":
		tag = typeInt64
	case "decimal":
		tag = typeDecimal
	case "float":
		tag = typeFloat
	case "boolean":
		tag = typeBoolean
	case "datetime":
		tag = typeDatetime
	case "date":
		tag = typeDate
	case "time":
		tag = typeTime
	case "json":
		tag = typeJSON
	case "bytes":
		tag = typeBytes
	default:
		return 0, fmt.Errorf("unsupported portable type %q", spec.Type)
	}
	if spec.Type == "decimal" {
		if spec.Precision == nil || spec.Scale == nil {
			return 0, errors.New("decimal field requires precision and scale")
		}
		if *spec.Precision <= 0 || *spec.Scale < 0 || *spec.Scale > *spec.Precision {
			return 0, errors.New("decimal precision or scale is invalid")
		}
	} else if spec.Precision != nil || spec.Scale != nil {
		return 0, errors.New("non-decimal field has precision or scale")
	}
	return tag, nil
}

func typedPayload(spec FieldSpec, raw []byte) ([]byte, error) {
	switch spec.Type {
	case "string":
		value, err := decodeJSONString(raw, false)
		if err != nil {
			return nil, err
		}
		return appendBlob(nil, []byte(value)), nil
	case "int":
		text := string(raw)
		if !canonicalInteger.MatchString(text) {
			return nil, errors.New("int wire value is not canonical")
		}
		value, err := strconv.ParseInt(text, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("int wire value is outside int32: %w", err)
		}
		encoded := make([]byte, 4)
		binary.BigEndian.PutUint32(encoded, uint32(int32(value)))
		return encoded, nil
	case "int64":
		text, err := decodeJSONString(raw, false)
		if err != nil {
			return nil, err
		}
		if !canonicalInteger.MatchString(text) {
			return nil, errors.New("int64 wire value is not canonical")
		}
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("int64 wire value is outside int64: %w", err)
		}
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, uint64(value))
		return encoded, nil
	case "decimal":
		text, err := decodeJSONString(raw, false)
		if err != nil {
			return nil, err
		}
		if !canonicalDecimal.MatchString(text) {
			return nil, errors.New("decimal wire value is not canonical")
		}
		if err := validateDecimalBounds(text, *spec.Precision, *spec.Scale); err != nil {
			return nil, err
		}
		return appendBlob(nil, []byte(text)), nil
	case "float":
		text := string(raw)
		value, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
			return nil, errors.New("float wire value is not finite binary64")
		}
		canonical, err := jcs.NumberToJSON(value)
		if err != nil {
			return nil, fmt.Errorf("canonicalize float wire value: %w", err)
		}
		if canonical != text {
			return nil, errors.New("float wire value is not canonical")
		}
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, math.Float64bits(value))
		return encoded, nil
	case "boolean":
		switch string(raw) {
		case "false":
			return []byte{0x00}, nil
		case "true":
			return []byte{0x01}, nil
		default:
			return nil, errors.New("boolean wire value is not true or false")
		}
	case "datetime":
		value, err := decodeJSONString(raw, false)
		if err != nil {
			return nil, err
		}
		if !canonicalDateTime.MatchString(value) {
			return nil, errors.New("datetime wire value has an invalid format")
		}
		if _, err := time.Parse("2006-01-02T15:04:05.000000Z", value); err != nil {
			return nil, fmt.Errorf("datetime wire value is invalid: %w", err)
		}
		return appendBlob(nil, []byte(value)), nil
	case "date":
		value, err := decodeJSONString(raw, false)
		if err != nil {
			return nil, err
		}
		if !canonicalDate.MatchString(value) {
			return nil, errors.New("date wire value has an invalid format")
		}
		if _, err := time.Parse("2006-01-02", value); err != nil {
			return nil, fmt.Errorf("date wire value is invalid: %w", err)
		}
		return appendBlob(nil, []byte(value)), nil
	case "time":
		value, err := decodeJSONString(raw, false)
		if err != nil {
			return nil, err
		}
		if !canonicalTime.MatchString(value) {
			return nil, errors.New("time wire value has an invalid format")
		}
		if _, err := time.Parse("15:04:05.000000", value); err != nil {
			return nil, fmt.Errorf("time wire value is invalid: %w", err)
		}
		return appendBlob(nil, []byte(value)), nil
	case "json":
		value, err := decodeJSONString(raw, false)
		if err != nil {
			return nil, err
		}
		if err := validateJSONDocument([]byte(value), jsonValidation{iJSON: true, safeInteger: true}); err != nil {
			return nil, fmt.Errorf("validate nested JSON wire value: %w", err)
		}
		canonical, err := canonicalizeJCS([]byte(value))
		if err != nil {
			return nil, fmt.Errorf("validate nested JSON wire value: %w", err)
		}
		if !bytes.Equal(canonical, []byte(value)) {
			return nil, errors.New("nested JSON wire value is not canonical")
		}
		return appendBlob(nil, canonical), nil
	case "bytes":
		value, err := decodeJSONString(raw, false)
		if err != nil {
			return nil, err
		}
		decoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("decode canonical base64url: %w", err)
		}
		if base64.RawURLEncoding.EncodeToString(decoded) != value {
			return nil, errors.New("bytes wire value is not canonical base64url")
		}
		return appendBlob(nil, decoded), nil
	default:
		return nil, fmt.Errorf("unsupported portable type %q", spec.Type)
	}
}

func decodeJSONString(raw []byte, iJSON bool) (string, error) {
	if err := validateJSONDocument(raw, jsonValidation{iJSON: iJSON, safeInteger: true}); err != nil {
		return "", err
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", errors.New("wire value is not a JSON string")
	}
	return value, nil
}

func validateDecimalBounds(value string, precision, scale int) error {
	unsigned := strings.TrimPrefix(value, "-")
	integer, fraction := unsigned, ""
	if point := strings.IndexByte(unsigned, '.'); point >= 0 {
		integer, fraction = unsigned[:point], unsigned[point+1:]
	}
	integerDigits := len(strings.TrimLeft(integer, "0"))
	if integerDigits > precision-scale || len(fraction) > scale || integerDigits+len(fraction) > precision {
		return errors.New("decimal wire value exceeds declared precision or scale")
	}
	return nil
}

func appendU32(output []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(output, encoded[:]...)
}

func appendU64(output []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(output, encoded[:]...)
}

func appendBlob(output, value []byte) []byte {
	output = appendU64(output, uint64(len(value)))
	return append(output, value...)
}

func appendText(output []byte, value string) ([]byte, error) {
	if err := validateJSONStringValue(value, false); err != nil {
		return nil, err
	}
	return appendBlob(output, []byte(value)), nil
}
