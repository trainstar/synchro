package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const (
	smokeUserID     = "00000000-0000-4000-8000-000000000111"
	smokeClientID   = "00000000-0000-4000-8000-000000000112"
	smokeBatchID    = "00000000-0000-4000-8000-000000000113"
	smokeMutationID = "00000000-0000-4000-8000-000000000114"
	smokeRowID      = "00000000-0000-4000-8000-000000000115"
	smokeTime       = "2026-09-14T12:00:00.000000Z"
)

type schemaRef struct {
	Version int64  `json:"version"`
	Hash    string `json:"hash"`
}

type scopeCursor struct {
	Cursor *string `json:"cursor"`
}

type clientState struct {
	ClientID        string                 `json:"client_id"`
	Generation      int64                  `json:"client_generation"`
	ScopeSetVersion int64                  `json:"scope_set_version"`
	Schema          schemaRef              `json:"schema"`
	Scopes          map[string]scopeCursor `json:"scopes"`
	TableID         string                 `json:"table_id"`
	PrimaryKeyID    string                 `json:"primary_key_field_id"`
	Fields          map[string]string      `json:"fields"`
}

type storedState struct {
	Client   clientState     `json:"client"`
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
}

type phaseResult struct {
	SchemaVersion int    `json:"schema_version"`
	Phase         string `json:"phase"`
	Status        string `json:"status"`
	AdapterPID    int    `json:"adapter_pid"`
	PushDigest    string `json:"push_digest"`
	ReplayEqual   *bool  `json:"replay_equal,omitempty"`
}

type tableDefinition struct {
	TableID           string `json:"table_id"`
	Name              string `json:"name"`
	PrimaryKeyFieldID string `json:"primary_key_field_id"`
	Fields            []struct {
		FieldID  string `json:"field_id"`
		Name     string `json:"name"`
		Writable bool   `json:"writable"`
	} `json:"fields"`
}

type connectResponse struct {
	ClientGeneration int64 `json:"client_generation"`
	ScopeSetVersion  int64 `json:"scope_set_version"`
	Schema           struct {
		Version int64  `json:"version"`
		Hash    string `json:"hash"`
	} `json:"schema"`
	Scopes struct {
		Add []struct {
			ID     string  `json:"id"`
			Cursor *string `json:"cursor"`
		} `json:"add"`
		Remove []string `json:"remove"`
	} `json:"scopes"`
	ScopeCursorUpdates map[string]*string `json:"scope_cursor_updates"`
	SchemaDefinition   *struct {
		Tables []tableDefinition `json:"tables"`
	} `json:"schema_definition"`
}

type pullResponse struct {
	Changes         []json.RawMessage `json:"changes"`
	ScopeCursors    map[string]string `json:"scope_cursors"`
	Rebuild         []string          `json:"rebuild"`
	HasMore         bool              `json:"has_more"`
	ScopeSetVersion int64             `json:"scope_set_version"`
}

type pullChange struct {
	Scope string                     `json:"scope"`
	Table string                     `json:"table"`
	Op    string                     `json:"op"`
	PK    map[string]json.RawMessage `json:"pk"`
	Row   map[string]json.RawMessage `json:"row"`
}

type rebuildResponse struct {
	Scope            string            `json:"scope"`
	Records          []json.RawMessage `json:"records"`
	Cursor           json.RawMessage   `json:"cursor"`
	HasMore          bool              `json:"has_more"`
	FinalScopeCursor *string           `json:"final_scope_cursor"`
}

type pushResponseSummary struct {
	BatchID  string `json:"batch_id"`
	Accepted []struct {
		MutationID string `json:"mutation_id"`
	} `json:"accepted"`
	Rejected []json.RawMessage `json:"rejected"`
}

func parseInitialConnect(data []byte, clientID string) (clientState, error) {
	var response connectResponse
	if err := json.Unmarshal(data, &response); err != nil {
		return clientState{}, fmt.Errorf("decode connect response: %w", err)
	}
	if response.ClientGeneration < 1 || response.ScopeSetVersion < 0 {
		return clientState{}, errors.New("connect response has invalid generation state")
	}
	if response.Schema.Version < 1 || len(response.Schema.Hash) != 64 {
		return clientState{}, errors.New("connect response has invalid schema reference")
	}
	if response.SchemaDefinition == nil {
		return clientState{}, errors.New("initial connect response has no schema definition")
	}

	tableID, primaryKeyID, fields, err := bindCustomers(response.SchemaDefinition.Tables)
	if err != nil {
		return clientState{}, err
	}
	scopes := make(map[string]scopeCursor, len(response.Scopes.Add))
	for _, assignment := range response.Scopes.Add {
		if assignment.ID == "" {
			return clientState{}, errors.New("connect response has an empty scope")
		}
		scopes[assignment.ID] = scopeCursor{Cursor: assignment.Cursor}
	}
	for scopeID, cursor := range response.ScopeCursorUpdates {
		if _, found := scopes[scopeID]; found {
			scopes[scopeID] = scopeCursor{Cursor: cursor}
		}
	}
	if len(scopes) == 0 {
		return clientState{}, errors.New("connect response assigned no scopes")
	}

	return clientState{
		ClientID:        clientID,
		Generation:      response.ClientGeneration,
		ScopeSetVersion: response.ScopeSetVersion,
		Schema: schemaRef{
			Version: response.Schema.Version,
			Hash:    response.Schema.Hash,
		},
		Scopes:       scopes,
		TableID:      tableID,
		PrimaryKeyID: primaryKeyID,
		Fields:       fields,
	}, nil
}

func bindCustomers(tables []tableDefinition) (string, string, map[string]string, error) {
	for _, table := range tables {
		if table.Name != "customers" {
			continue
		}
		if table.TableID == "" || table.PrimaryKeyFieldID == "" {
			return "", "", nil, errors.New("customers table identity is incomplete")
		}
		fields := make(map[string]string)
		for _, field := range table.Fields {
			if field.Writable && field.Name != "" && field.FieldID != "" {
				fields[field.Name] = field.FieldID
			}
		}
		for _, name := range []string{
			"user_id",
			"name",
			"balance",
			"is_active",
		} {
			if fields[name] == "" {
				return "", "", nil, fmt.Errorf("customers field %q is not writable", name)
			}
		}
		return table.TableID, table.PrimaryKeyFieldID, fields, nil
	}
	return "", "", nil, errors.New("schema definition has no customers table")
}

func freshConnect(clientID string) map[string]any {
	return map[string]any{
		"client_id":        clientID,
		"platform":         "linux",
		"app_version":      "1.0.0",
		"protocol_version": 3,
		"schema": map[string]any{
			"version": 0,
			"hash":    "",
		},
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	}
}

func returningConnect(state clientState) map[string]any {
	return map[string]any{
		"client_id":         state.ClientID,
		"client_generation": state.Generation,
		"platform":          "linux",
		"app_version":       "1.0.0",
		"protocol_version":  3,
		"schema":            state.Schema,
		"scope_set_version": state.ScopeSetVersion,
		"known_scopes":      state.Scopes,
	}
}

func pushPayload(state clientState) map[string]any {
	return map[string]any{
		"client_id":         state.ClientID,
		"client_generation": state.Generation,
		"batch_id":          smokeBatchID,
		"schema":            state.Schema,
		"mutations": []any{
			map[string]any{
				"mutation_id":     smokeMutationID,
				"table":           state.TableID,
				"pk":              map[string]any{state.PrimaryKeyID: smokeRowID},
				"authored_schema": state.Schema,
				"op":              "insert",
				"client_version":  smokeTime,
				"columns": map[string]any{
					state.Fields["user_id"]:   smokeUserID,
					state.Fields["name"]:      "Packaged server consumer",
					state.Fields["balance"]:   "0",
					state.Fields["is_active"]: true,
				},
			},
		},
	}
}

func pullPayload(state clientState) map[string]any {
	return map[string]any{
		"client_id":         state.ClientID,
		"client_generation": state.Generation,
		"schema":            state.Schema,
		"scope_set_version": state.ScopeSetVersion,
		"scopes":            state.Scopes,
		"limit":             100,
	}
}

func rebuildPayload(state clientState, scopeID, rebuildID string, cursor *string) map[string]any {
	return map[string]any{
		"client_id":         state.ClientID,
		"client_generation": state.Generation,
		"schema":            state.Schema,
		"scope":             scopeID,
		"rebuild_id":        rebuildID,
		"cursor":            cursor,
		"limit":             100,
	}
}

func newRequest(ctx context.Context, token, url string, body []byte) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Author"+"ization", "Bear"+"er "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Client-Version", "1.0.0")
	return request, nil
}

func postJSON(ctx context.Context, client *http.Client, token, url string, value any) (int, []byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return 0, nil, err
	}
	return postBytes(ctx, client, token, url, body)
}

func postBytes(ctx context.Context, client *http.Client, token, url string, body []byte) (int, []byte, error) {
	request, err := newRequest(ctx, token, url, body)
	if err != nil {
		return 0, nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, data, nil
}

func requireOK(status int, operation string) error {
	if status != http.StatusOK {
		return fmt.Errorf("%s returned HTTP %d", operation, status)
	}
	return nil
}

func requireAcceptedPush(body []byte) error {
	var response pushResponseSummary
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("decode push response: %w", err)
	}
	if response.BatchID != smokeBatchID || len(response.Accepted) != 1 ||
		response.Accepted[0].MutationID != smokeMutationID || len(response.Rejected) != 0 {
		return errors.New("push did not accept the packaged server mutation")
	}
	return nil
}

func newRebuildID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" +
		encoded[16:20] + "-" + encoded[20:32], nil
}

func bootstrapIncrementalCursors(
	ctx context.Context,
	client *http.Client,
	token string,
	baseURL string,
	state clientState,
) (clientState, error) {
	for scopeID, scope := range state.Scopes {
		if scope.Cursor != nil {
			continue
		}
		rebuildID, err := newRebuildID()
		if err != nil {
			return clientState{}, errors.New("generate rebuild ID failed")
		}
		var continuation *string
		for {
			status, body, err := postJSON(
				ctx,
				client,
				token,
				baseURL+"/sync/rebuild",
				rebuildPayload(state, scopeID, rebuildID, continuation),
			)
			if err != nil {
				return clientState{}, err
			}
			if err := requireOK(status, "rebuild"); err != nil {
				return clientState{}, err
			}
			var response rebuildResponse
			if err := json.Unmarshal(body, &response); err != nil {
				return clientState{}, fmt.Errorf("decode rebuild response: %w", err)
			}
			if response.Scope != scopeID || response.Records == nil || len(response.Cursor) == 0 {
				return clientState{}, errors.New("rebuild response is incomplete")
			}
			if response.HasMore {
				var next string
				if err := json.Unmarshal(response.Cursor, &next); err != nil || next == "" ||
					response.FinalScopeCursor != nil {
					return clientState{}, errors.New("intermediate rebuild response is invalid")
				}
				continuation = &next
				continue
			}
			if !bytes.Equal(bytes.TrimSpace(response.Cursor), []byte("null")) ||
				response.FinalScopeCursor == nil || *response.FinalScopeCursor == "" {
				return clientState{}, errors.New("final rebuild response has no incremental cursor")
			}
			finalCursor := *response.FinalScopeCursor
			state.Scopes[scopeID] = scopeCursor{Cursor: &finalCursor}
			break
		}
	}
	return state, nil
}

func pullUntilCustomerDelivered(
	ctx context.Context,
	client *http.Client,
	token string,
	baseURL string,
	state clientState,
) error {
	pullContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	state.Scopes = maps.Clone(state.Scopes)

	for {
		status, body, err := postJSON(
			pullContext,
			client,
			token,
			baseURL+"/sync/pull",
			pullPayload(state),
		)
		if err != nil {
			return err
		}
		if status == http.StatusServiceUnavailable && capturePending(body) {
			select {
			case <-pullContext.Done():
				return errors.New("pull remained capture_pending")
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if err := requireOK(status, "pull"); err != nil {
			return err
		}

		var response pullResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return fmt.Errorf("decode pull response: %w", err)
		}
		if response.ScopeSetVersion != state.ScopeSetVersion || response.Changes == nil ||
			response.ScopeCursors == nil || response.Rebuild == nil {
			return errors.New("pull response is incomplete")
		}
		if len(response.Rebuild) != 0 {
			return errors.New("incremental pull requested an unexpected rebuild")
		}
		for _, rawChange := range response.Changes {
			delivered, err := isAuthoredCustomerUpsert(rawChange, state)
			if err != nil {
				return err
			}
			if delivered {
				return nil
			}
		}
		if !response.HasMore {
			return errors.New("pull did not deliver the authored customer")
		}
		progressed, err := applyScopeCursorDeltas(state.Scopes, response.ScopeCursors)
		if err != nil {
			return err
		}
		if !progressed {
			return errors.New("paginated pull did not advance a scope cursor")
		}
	}
}

func applyScopeCursorDeltas(scopes map[string]scopeCursor, deltas map[string]string) (bool, error) {
	progressed := false
	for scopeID, cursor := range deltas {
		current, found := scopes[scopeID]
		if !found || cursor == "" {
			return false, errors.New("pull returned an invalid scope cursor")
		}
		if current.Cursor == nil || *current.Cursor != cursor {
			progressed = true
		}
		next := cursor
		scopes[scopeID] = scopeCursor{Cursor: &next}
	}
	return progressed, nil
}

func isAuthoredCustomerUpsert(rawChange json.RawMessage, state clientState) (bool, error) {
	var change pullChange
	if err := json.Unmarshal(rawChange, &change); err != nil {
		return false, fmt.Errorf("decode pull change: %w", err)
	}
	if change.Table != state.TableID {
		return false, nil
	}
	rawPrimaryKey, found := change.PK[state.PrimaryKeyID]
	if !found {
		return false, nil
	}
	var rowID string
	if err := json.Unmarshal(rawPrimaryKey, &rowID); err != nil {
		return false, errors.New("customer primary key is invalid")
	}
	if rowID != smokeRowID {
		return false, nil
	}
	if len(change.PK) != 1 {
		return false, errors.New("authored customer primary key is not exact")
	}
	if _, found := state.Scopes[change.Scope]; !found {
		return false, errors.New("authored customer came from an unassigned scope")
	}
	if change.Op != "upsert" || change.Row == nil {
		return false, errors.New("authored customer was not delivered as an upsert")
	}
	expectedStrings := map[string]string{
		state.PrimaryKeyID:      smokeRowID,
		state.Fields["user_id"]: smokeUserID,
		state.Fields["name"]:    "Packaged server consumer",
		state.Fields["balance"]: "0",
	}
	for fieldID, expected := range expectedStrings {
		var actual string
		if raw, found := change.Row[fieldID]; !found ||
			json.Unmarshal(raw, &actual) != nil || actual != expected {
			return false, errors.New("authored customer string value changed")
		}
	}
	var active bool
	if raw, found := change.Row[state.Fields["is_active"]]; !found ||
		json.Unmarshal(raw, &active) != nil || !active {
		return false, errors.New("authored customer activity value changed")
	}
	return true, nil
}

func capturePending(body []byte) bool {
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	return json.Unmarshal(body, &response) == nil && response.Error.Code == "capture_pending"
}

func bearerToken(secret []byte) string {
	encoding := base64.RawURLEncoding
	header := encoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	now := time.Now().Unix()
	payload, _ := json.Marshal(map[string]any{
		"sub": smokeUserID,
		"iat": now,
		"exp": now + 3600,
	})
	encodedPayload := encoding.EncodeToString(payload)
	input := header + "." + encodedPayload
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(input))
	return input + "." + encoding.EncodeToString(mac.Sum(nil))
}

func pushDigest(request, response []byte) string {
	hash := sha256.New()
	_, _ = hash.Write(request)
	_, _ = hash.Write(response)
	return hex.EncodeToString(hash.Sum(nil))
}

func writeJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func readStoredState(path string) (storedState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return storedState{}, err
	}
	var state storedState
	if err := decodeJSON(data, &state); err != nil {
		return storedState{}, err
	}
	if state.Client.ClientID != smokeClientID || len(state.Request) == 0 || len(state.Response) == 0 {
		return storedState{}, errors.New("stored server smoke state is invalid")
	}
	return state, nil
}

func decodeJSON(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("JSON contains trailing data")
	}
	return nil
}

func run(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("public-server-smoke", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	baseURL := flags.String("url", "", "adapter URL")
	secretFile := flags.String("jwt-secret-file", "", "JWT secret file")
	phase := flags.String("phase", "", "initial or resume")
	stateDir := flags.String("state-dir", "", "persistent smoke state directory")
	adapterPID := flags.Int("adapter-pid", 0, "adapter process ID")
	output := flags.String("output", "", "phase result")
	if err := flags.Parse(args); err != nil {
		return errors.New("server smoke flags are invalid")
	}
	if flags.NArg() != 0 || (*phase != "initial" && *phase != "resume") ||
		*baseURL == "" || *secretFile == "" || *stateDir == "" ||
		*adapterPID < 1 || *output == "" {
		return errors.New("server smoke input is incomplete")
	}
	secret, err := os.ReadFile(*secretFile)
	if err != nil {
		return errors.New("read server smoke JWT secret failed")
	}
	token := bearerToken(bytes.TrimSpace(secret))
	client := &http.Client{Timeout: 20 * time.Second}
	statePath := filepath.Join(*stateDir, "server-state.json")

	var state clientState
	var pushRequest []byte
	var pushResponse []byte
	var replayEqual *bool
	if *phase == "initial" {
		status, body, err := postJSON(ctx, client, token, *baseURL+"/sync/connect", freshConnect(smokeClientID))
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		if err := requireOK(status, "connect"); err != nil {
			return err
		}
		state, err = parseInitialConnect(body, smokeClientID)
		if err != nil {
			return err
		}
		state, err = bootstrapIncrementalCursors(ctx, client, token, *baseURL, state)
		if err != nil {
			return fmt.Errorf("bootstrap scope cursors: %w", err)
		}
		pushRequest, err = json.Marshal(pushPayload(state))
		if err != nil {
			return errors.New("encode push request failed")
		}
		status, pushResponse, err = postBytes(ctx, client, token, *baseURL+"/sync/push", pushRequest)
		if err != nil {
			return fmt.Errorf("push: %w", err)
		}
		if err := requireOK(status, "push"); err != nil {
			return err
		}
		if err := requireAcceptedPush(pushResponse); err != nil {
			return err
		}
		if err := writeJSON(statePath, storedState{
			Client:   state,
			Request:  pushRequest,
			Response: pushResponse,
		}); err != nil {
			return errors.New("persist server smoke state failed")
		}
	} else {
		stored, err := readStoredState(statePath)
		if err != nil {
			return errors.New("read server smoke state failed")
		}
		state = stored.Client
		status, _, err := postJSON(ctx, client, token, *baseURL+"/sync/connect", returningConnect(state))
		if err != nil {
			return fmt.Errorf("reconnect: %w", err)
		}
		if err := requireOK(status, "reconnect"); err != nil {
			return err
		}
		pushRequest = stored.Request
		status, pushResponse, err = postBytes(ctx, client, token, *baseURL+"/sync/push", pushRequest)
		if err != nil {
			return fmt.Errorf("push replay: %w", err)
		}
		if err := requireOK(status, "push replay"); err != nil {
			return err
		}
		if err := requireAcceptedPush(pushResponse); err != nil {
			return err
		}
		equal := bytes.Equal(pushResponse, stored.Response)
		if !equal {
			return errors.New("push replay response changed")
		}
		replayEqual = &equal
	}

	if err := pullUntilCustomerDelivered(ctx, client, token, *baseURL, state); err != nil {
		return err
	}
	return writeJSON(*output, phaseResult{
		SchemaVersion: 1,
		Phase:         *phase,
		Status:        "passed",
		AdapterPID:    *adapterPID,
		PushDigest:    pushDigest(pushRequest, pushResponse),
		ReplayEqual:   replayEqual,
	})
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "public-server-smoke: %v\n", err)
		os.Exit(1)
	}
}
