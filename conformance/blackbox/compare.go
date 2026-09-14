package blackbox

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"reflect"
)

var (
	// ErrSemanticMismatch reports a well-formed response with wrong semantics.
	ErrSemanticMismatch = errors.New("response semantics do not match")
	// ErrReplayMismatch reports a replay that changed exact response evidence.
	ErrReplayMismatch = errors.New("replay response does not match")
)

// CompareSemanticJSON compares canonical values after declared normalization.
func CompareSemanticJSON(expected, observed []byte, spec NormalizationSpec) error {
	normalizedExpected, err := NormalizeResponse(expected, spec)
	if err != nil {
		return fmt.Errorf("normalize expected response: %w", err)
	}
	normalizedObserved, err := NormalizeResponse(observed, spec)
	if err != nil {
		return fmt.Errorf("normalize observed response: %w", err)
	}
	if !bytes.Equal(normalizedExpected, normalizedObserved) {
		return ErrSemanticMismatch
	}
	return nil
}

// CompareExactReplay checks raw status, relevant headers, and canonical bytes.
func CompareExactReplay(first, replay Response) error {
	if first.Status != replay.Status {
		return fmt.Errorf("%w: raw status", ErrReplayMismatch)
	}
	if !reflect.DeepEqual(relevantHeaders(first.Headers), relevantHeaders(replay.Headers)) {
		return fmt.Errorf("%w: relevant headers", ErrReplayMismatch)
	}
	firstCanonical, err := responseCanonicalBytes(first)
	if err != nil {
		return fmt.Errorf("%w: first canonical response: %v", ErrReplayMismatch, err)
	}
	replayCanonical, err := responseCanonicalBytes(replay)
	if err != nil {
		return fmt.Errorf("%w: replay canonical response: %v", ErrReplayMismatch, err)
	}
	if !bytes.Equal(firstCanonical, replayCanonical) {
		return fmt.Errorf("%w: canonical response bytes", ErrReplayMismatch)
	}
	return nil
}

func relevantHeaders(headers http.Header) []RecordedHeader {
	result := make([]RecordedHeader, 0, len(relevantResponseHeaders))
	for _, name := range relevantResponseHeaders {
		values := headers.Values(name)
		if len(values) == 0 {
			continue
		}
		result = append(result, RecordedHeader{Name: http.CanonicalHeaderKey(name), Values: append([]string(nil), values...)})
	}
	return result
}

func responseCanonicalBytes(response Response) ([]byte, error) {
	if response.CanonicalBody != nil {
		return append([]byte(nil), response.CanonicalBody...), nil
	}
	if response.Body == nil {
		return nil, nil
	}
	if responseIsJSON(response.Headers, response.Body) {
		return CanonicalResponseBytes(response.Body)
	}
	return append([]byte(nil), response.Body...), nil
}
