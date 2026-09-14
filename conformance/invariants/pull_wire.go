package invariants

import (
	"bytes"
	"encoding/json"
	"errors"
)

type pullWireRequest struct {
	clientID string
	scopes   map[string]*string
}

type pullWireResponse struct {
	hasMore bool
	cursors map[string]string
	changes []pullWireChange
	rebuild map[string]struct{}
	removed map[string]struct{}
}

type pullWireChange struct {
	scope  string
	object map[string]json.RawMessage
}

func parsePullWire(requestBody, responseBody []byte) (pullWireRequest, pullWireResponse, bool) {
	requestObject, err := decodeRawObject(requestBody)
	if err != nil {
		return pullWireRequest{}, pullWireResponse{}, false
	}
	clientID, ok := decodeJSONString(requestObject["client_id"])
	if !ok || clientID == "" {
		return pullWireRequest{}, pullWireResponse{}, false
	}
	rawScopes, err := decodeRawObject(requestObject["scopes"])
	if err != nil {
		return pullWireRequest{}, pullWireResponse{}, false
	}
	scopes := make(map[string]*string, len(rawScopes))
	for scopeID, rawScope := range rawScopes {
		if scopeID == "" {
			return pullWireRequest{}, pullWireResponse{}, false
		}
		scope, err := decodeRawObject(rawScope)
		if err != nil {
			return pullWireRequest{}, pullWireResponse{}, false
		}
		rawCursor, present := scope["cursor"]
		if !present {
			return pullWireRequest{}, pullWireResponse{}, false
		}
		if bytes.Equal(bytes.TrimSpace(rawCursor), []byte("null")) {
			scopes[scopeID] = nil
			continue
		}
		cursor, ok := decodeJSONString(rawCursor)
		if !ok || cursor == "" {
			return pullWireRequest{}, pullWireResponse{}, false
		}
		cursorCopy := cursor
		scopes[scopeID] = &cursorCopy
	}

	responseObject, err := decodeRawObject(responseBody)
	if err != nil {
		return pullWireRequest{}, pullWireResponse{}, false
	}
	var hasMore bool
	if json.Unmarshal(responseObject["has_more"], &hasMore) != nil {
		return pullWireRequest{}, pullWireResponse{}, false
	}
	rawCursors, err := decodeRawObject(responseObject["scope_cursors"])
	if err != nil {
		return pullWireRequest{}, pullWireResponse{}, false
	}
	cursors := make(map[string]string, len(rawCursors))
	for scopeID, rawCursor := range rawCursors {
		cursor, ok := decodeJSONString(rawCursor)
		if !ok {
			return pullWireRequest{}, pullWireResponse{}, false
		}
		cursors[scopeID] = cursor
	}
	changes, err := pullChangeScopes(responseObject["changes"])
	if err != nil {
		return pullWireRequest{}, pullWireResponse{}, false
	}
	rebuild, err := stringArraySet(responseObject["rebuild"])
	if err != nil {
		return pullWireRequest{}, pullWireResponse{}, false
	}
	removed, err := pullRemovedScopeSet(responseObject["scope_updates"])
	if err != nil {
		return pullWireRequest{}, pullWireResponse{}, false
	}
	return pullWireRequest{clientID: clientID, scopes: scopes}, pullWireResponse{
		hasMore: hasMore,
		cursors: cursors,
		changes: changes,
		rebuild: rebuild,
		removed: removed,
	}, true
}

func pullChangeScopes(raw json.RawMessage) ([]pullWireChange, error) {
	changes, err := decodeRawArray(raw)
	if err != nil {
		return nil, err
	}
	parsed := make([]pullWireChange, 0, len(changes))
	for _, rawChange := range changes {
		change, err := decodeRawObject(rawChange)
		if err != nil {
			return nil, err
		}
		scope, ok := decodeJSONString(change["scope"])
		if !ok || scope == "" {
			return nil, errInvalidPullWire
		}
		parsed = append(parsed, pullWireChange{scope: scope, object: change})
	}
	return parsed, nil
}

func stringArraySet(raw json.RawMessage) (map[string]struct{}, error) {
	values, err := decodeRawArray(raw)
	if err != nil {
		return nil, err
	}
	result := make(map[string]struct{}, len(values))
	for _, rawValue := range values {
		value, ok := decodeJSONString(rawValue)
		if !ok || value == "" {
			return nil, errInvalidPullWire
		}
		result[value] = struct{}{}
	}
	return result, nil
}

func pullRemovedScopeSet(raw json.RawMessage) (map[string]struct{}, error) {
	updates, err := decodeRawObject(raw)
	if err != nil {
		return nil, err
	}
	return stringArraySet(updates["remove"])
}

var errInvalidPullWire = errors.New("pull wire value is invalid")
