package jsonstrict

import (
	"encoding/json"
	"testing"
)

func TestValidateValueAcceptsObjectAndTrailingWhitespace(t *testing.T) {
	if err := ValidateValue([]byte("{\"ok\":true} \n\t")); err != nil {
		t.Fatalf("ValidateValue() error = %v", err)
	}
}

func TestValidateValueAcceptsLargeLexicalNumber(t *testing.T) {
	if err := ValidateValue([]byte(`{"large":1e400}`)); err != nil {
		t.Fatalf("ValidateValue() rejected a valid large number: %v", err)
	}
}

func TestValidateValueRejectsNonObjectsAndMalformedDocuments(t *testing.T) {
	for _, data := range []string{
		"[]",
		"null",
		"{\"key\":}",
		"{\"key\":1",
		"{\"key\":NaN}",
		"{\"key\":Infinity}",
		"{\"key\":-Infinity}",
		"{\"key\":1} {\"other\":2}",
	} {
		if err := ValidateValue([]byte(data)); err == nil {
			t.Errorf("ValidateValue(%q) unexpectedly succeeded", data)
		}
	}
}

func TestValidateValueRejectsDuplicateNamesAtEveryDepth(t *testing.T) {
	for _, data := range []string{
		`{"a":1,"a":2}`,
		`{"":1,"":2}`,
		`{"outer":{"a":1,"a":2}}`,
		`{"items":[{"a":1,"a":2}]}`,
		`{"\u0061":1,"a":2}`,
	} {
		if err := ValidateValue([]byte(data)); err == nil {
			t.Errorf("ValidateValue(%q) unexpectedly succeeded", data)
		}
	}
}

func TestValidateValueRejectsInvalidUTF8InKeysAndValues(t *testing.T) {
	for _, data := range [][]byte{
		{'{', '"', 'k', 0xff, '"', ':', '1', '}'},
		{'{', '"', 'k', '"', ':', '"', 0xff, '"', '}'},
	} {
		if err := ValidateValue(data); err == nil {
			t.Errorf("ValidateValue(%#v) unexpectedly succeeded", data)
		}
	}
}

func TestDecodeRejectsLoneUTF16Surrogates(t *testing.T) {
	for _, data := range []string{
		`{"value":"\ud800"}`,
		`{"value":"\udfff"}`,
		`{"\ud800":"value"}`,
	} {
		var decoded map[string]any
		if err := Decode([]byte(data), &decoded); err == nil {
			t.Fatalf("Decode(%s) accepted a lone UTF-16 surrogate", data)
		}
	}
}

func TestDecodeAcceptsPairedAndEscapedSurrogates(t *testing.T) {
	for _, data := range []string{
		`{"value":"\ud83d\ude00"}`,
		`{"value":"\\ud800"}`,
	} {
		var decoded map[string]any
		if err := Decode([]byte(data), &decoded); err != nil {
			t.Fatalf("Decode(%s): %v", data, err)
		}
	}
}

func TestDecodeUsesJSONNumberForInterfaceValues(t *testing.T) {
	var value map[string]any
	if err := Decode([]byte(`{"integer":1,"decimal":1.0,"large":1e400,"negative_zero":-0,"quoted":"1","nested":[{"value":1.0}]}`), &value); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got := value["integer"].(json.Number).String(); got != "1" {
		t.Errorf("integer = %q, want 1", got)
	}
	if got := value["decimal"].(json.Number).String(); got != "1.0" {
		t.Errorf("decimal = %q, want 1.0", got)
	}
	if got := value["large"].(json.Number).String(); got != "1e400" {
		t.Errorf("large = %q, want 1e400", got)
	}
	if got := value["negative_zero"].(json.Number).String(); got != "-0" {
		t.Errorf("negative_zero = %q, want -0", got)
	}
	if got := value["quoted"].(string); got != "1" {
		t.Errorf("quoted = %q, want string 1", got)
	}
	nested := value["nested"].([]any)[0].(map[string]any)
	if got := nested["value"].(json.Number).String(); got != "1.0" {
		t.Errorf("nested value = %q, want 1.0", got)
	}
}

func TestDecodeRejectsInvalidDestinationsWithoutPanicking(t *testing.T) {
	var nilMap *map[string]any
	for _, dst := range []any{nil, map[string]any{}, nilMap} {
		if err := Decode([]byte(`{"key":1}`), dst); err == nil {
			t.Errorf("Decode() with destination %#v unexpectedly succeeded", dst)
		}
	}
}
