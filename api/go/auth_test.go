package synchroapi

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/trainstar/synchro/api/go/internal/testsupport"
)

// credential applies one authentication presentation to a request.
type credential func(*http.Request)

type authRejection struct {
	name       string
	credential credential
	status     int
	code       string
}

type authConfiguration struct {
	name string
	// configure returns the Routes configuration. It runs before rotate.
	configure func(t *testing.T) Config
	// rotate runs after Routes returns, before any request.
	rotate func()
	// accept presents a valid credential for identity. Nil means the
	// configuration accepts no credential.
	accept     func(identity string) credential
	rejections func(owner string) []authRejection
}

// TestAuthenticationConfigurationsBindExactIdentity proves each supported
// adapter authentication configuration through the real extension. Expected
// outcomes come from the wire protocol status table and the auth integration
// guide: an accepted credential binds exactly its canonical identity, and a
// rejected credential returns 401 auth_required without durable effects.
func TestAuthenticationConfigurationsBindExactIdentity(t *testing.T) {
	db := testsupport.OpenPostgres(t)
	secret := []byte("auth-configuration-test-secret-0123456789")
	rsaKey := generateRSAKey(t)
	rotatedRSAKey := generateRSAKey(t)
	foreignRSAKey := generateRSAKey(t)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ES256 key: %v", err)
	}
	future := time.Now().Add(time.Hour).Unix()
	past := time.Now().Add(-time.Hour).Unix()

	hs256 := func(claims jwt.MapClaims, key []byte) credential {
		return bearer(signToken(t, jwt.SigningMethodHS256, claims, "", key))
	}
	rs256 := func(claims jwt.MapClaims, kid string, key *rsa.PrivateKey) credential {
		return bearer(signToken(t, jwt.SigningMethodRS256, claims, kid, key))
	}
	unauthorized := func(name string, presented credential) authRejection {
		return authRejection{name: name, credential: presented, status: http.StatusUnauthorized, code: "auth_required"}
	}
	commonHeaderRejections := func(valid credential) []authRejection {
		return []authRejection{
			unauthorized("missing authorization header", func(*http.Request) {}),
			unauthorized("non-bearer scheme", func(r *http.Request) {
				valid(r)
				r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "Bearer ", "Basic ", 1))
			}),
			unauthorized("duplicate authorization headers", func(r *http.Request) {
				valid(r)
				r.Header.Add("Authorization", r.Header.Get("Authorization"))
			}),
			unauthorized("malformed token", bearer("not-a-jwt")),
		}
	}

	configurations := []authConfiguration{
		{
			name: "shared secret with default user claim",
			configure: func(*testing.T) Config {
				return Config{JWTSecret: secret}
			},
			accept: func(identity string) credential {
				return hs256(jwt.MapClaims{"sub": identity, "exp": future}, secret)
			},
			rejections: func(owner string) []authRejection {
				return append(commonHeaderRejections(hs256(jwt.MapClaims{"sub": owner, "exp": future}, secret)),
					unauthorized("wrong secret", hs256(jwt.MapClaims{"sub": owner, "exp": future}, []byte("another-secret-0123456789abcdef"))),
					unauthorized("expired", hs256(jwt.MapClaims{"sub": owner, "exp": past}, secret)),
					unauthorized("not yet valid", hs256(jwt.MapClaims{"sub": owner, "exp": future, "nbf": future}, secret)),
					unauthorized("HS384 algorithm", bearer(signToken(t, jwt.SigningMethodHS384, jwt.MapClaims{"sub": owner, "exp": future}, "", secret))),
					unauthorized("RS256 algorithm", rs256(jwt.MapClaims{"sub": owner, "exp": future}, "", rsaKey)),
					unauthorized("none algorithm", bearer(signToken(t, jwt.SigningMethodNone, jwt.MapClaims{"sub": owner, "exp": future}, "", jwt.UnsafeAllowNoneSignatureType))),
					unauthorized("missing user claim", hs256(jwt.MapClaims{"exp": future}, secret)),
					unauthorized("empty user claim", hs256(jwt.MapClaims{"sub": "", "exp": future}, secret)),
					unauthorized("non-string user claim", hs256(jwt.MapClaims{"sub": 42, "exp": future}, secret)),
				)
			},
		},
		{
			name: "shared secret with configured user claim",
			configure: func(*testing.T) Config {
				return Config{JWTSecret: secret, JWTUserClaim: "uid"}
			},
			accept: func(identity string) credential {
				return hs256(jwt.MapClaims{"sub": "decoy-" + identity, "uid": identity, "exp": future}, secret)
			},
			rejections: func(owner string) []authRejection {
				return []authRejection{
					unauthorized("default claim only", hs256(jwt.MapClaims{"sub": owner, "exp": future}, secret)),
					unauthorized("expired", hs256(jwt.MapClaims{"uid": owner, "exp": past}, secret)),
				}
			},
		},
		{
			name: "key set with RS256",
			configure: func(t *testing.T) Config {
				return jwksConfig(t, newJWKSProvider(t, jwkSet(t, rsaJWK("rsa-current", &rsaKey.PublicKey))))
			},
			accept: func(identity string) credential {
				return rs256(jwt.MapClaims{"sub": identity, "exp": future}, "rsa-current", rsaKey)
			},
			rejections: func(owner string) []authRejection {
				return append(commonHeaderRejections(rs256(jwt.MapClaims{"sub": owner, "exp": future}, "rsa-current", rsaKey)),
					unauthorized("known key ID with foreign signature", rs256(jwt.MapClaims{"sub": owner, "exp": future}, "rsa-current", foreignRSAKey)),
					unauthorized("unknown key ID", rs256(jwt.MapClaims{"sub": owner, "exp": future}, "rsa-unknown", foreignRSAKey)),
					unauthorized("expired", rs256(jwt.MapClaims{"sub": owner, "exp": past}, "rsa-current", rsaKey)),
					unauthorized("not yet valid", rs256(jwt.MapClaims{"sub": owner, "exp": future, "nbf": future}, "rsa-current", rsaKey)),
					unauthorized("HS256 with public key as secret", bearer(signToken(t, jwt.SigningMethodHS256, jwt.MapClaims{"sub": owner, "exp": future}, "rsa-current", publicKeyPEM(t, &rsaKey.PublicKey)))),
					unauthorized("none algorithm", bearer(signToken(t, jwt.SigningMethodNone, jwt.MapClaims{"sub": owner, "exp": future}, "rsa-current", jwt.UnsafeAllowNoneSignatureType))),
					unauthorized("missing user claim", rs256(jwt.MapClaims{"exp": future}, "rsa-current", rsaKey)),
				)
			},
		},
		{
			name: "key set with ES256",
			configure: func(t *testing.T) Config {
				return jwksConfig(t, newJWKSProvider(t, jwkSet(t, ecJWK("ec-current", &ecKey.PublicKey))))
			},
			accept: func(identity string) credential {
				return bearer(signToken(t, jwt.SigningMethodES256, jwt.MapClaims{"sub": identity, "exp": future}, "ec-current", ecKey))
			},
			rejections: func(owner string) []authRejection {
				foreignEC, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
				if err != nil {
					t.Fatalf("generate foreign ES256 key: %v", err)
				}
				return []authRejection{
					unauthorized("known key ID with foreign signature", bearer(signToken(t, jwt.SigningMethodES256, jwt.MapClaims{"sub": owner, "exp": future}, "ec-current", foreignEC))),
					unauthorized("expired", bearer(signToken(t, jwt.SigningMethodES256, jwt.MapClaims{"sub": owner, "exp": past}, "ec-current", ecKey))),
				}
			},
		},
		func() authConfiguration {
			var provider *jwksProvider
			return authConfiguration{
				name: "key set rotation",
				configure: func(t *testing.T) Config {
					provider = newJWKSProvider(t, jwkSet(t, rsaJWK("rsa-retired", &rsaKey.PublicKey)))
					return jwksConfig(t, provider)
				},
				rotate: func() {
					provider.serve(jwkSet(t, rsaJWK("rsa-rotated", &rotatedRSAKey.PublicKey)))
				},
				accept: func(identity string) credential {
					return rs256(jwt.MapClaims{"sub": identity, "exp": future}, "rsa-rotated", rotatedRSAKey)
				},
				rejections: func(owner string) []authRejection {
					return []authRejection{
						unauthorized("retired key", rs256(jwt.MapClaims{"sub": owner, "exp": future}, "rsa-retired", rsaKey)),
					}
				},
			}
		}(),
		{
			name: "key set provider failure",
			configure: func(t *testing.T) Config {
				provider := newJWKSProvider(t, nil)
				return jwksConfig(t, provider)
			},
			rejections: func(owner string) []authRejection {
				return []authRejection{
					unauthorized("valid signature without provider keys", rs256(jwt.MapClaims{"sub": owner, "exp": future}, "rsa-current", rsaKey)),
				}
			},
		},
		{
			name: "trusted upstream resolver",
			configure: func(*testing.T) Config {
				return Config{UserIDResolver: func(r *http.Request) (string, error) {
					switch outcome := r.Header.Get(upstreamOutcomeHeader); outcome {
					case "":
						return "", ErrAuthRequired
					case "empty":
						return "", nil
					case "failure":
						return "", errors.New("upstream identity store failed")
					default:
						return strings.TrimPrefix(outcome, "user="), nil
					}
				}}
			},
			accept: func(identity string) credential {
				return upstream("user=" + identity)
			},
			rejections: func(string) []authRejection {
				return []authRejection{
					unauthorized("unauthenticated upstream request", func(*http.Request) {}),
					unauthorized("empty upstream identity", upstream("empty")),
					// The contract defines no status for an internal resolver failure.
					// This case holds the current middleware boundary.
					{name: "upstream resolver failure", credential: upstream("failure"), status: http.StatusInternalServerError, code: "sync_integrity_failure"},
				}
			},
		},
	}

	type identities struct{ owner, other string }
	users := make(map[string]identities, len(configurations))
	var allUsers []string
	for index, configuration := range configurations {
		pair := identities{
			owner: fmt.Sprintf("auth-%d-owner", index),
			other: fmt.Sprintf("auth-%d-other", index),
		}
		users[configuration.name] = pair
		allUsers = append(allUsers, pair.owner, pair.other)
	}
	fixture := registerAuthRowsTable(t, db, allUsers)

	for _, configuration := range configurations {
		t.Run(configuration.name, func(t *testing.T) {
			pair := users[configuration.name]
			cfg := configuration.configure(t)
			cfg.DB = db
			cfg.MinClientVersion = "1.0.0"
			server := httptest.NewServer(Routes(cfg))
			t.Cleanup(server.Close)
			if configuration.rotate != nil {
				configuration.rotate()
			}

			var ownerClient connectedClient
			ownerClientID := testClientID(t, "auth-owner")
			if configuration.accept != nil {
				ownerClient = requireBoundIdentity(t, db, server, fixture, configuration.accept(pair.owner), pair.owner, ownerClientID).connectedClient
			}

			for _, rejection := range configuration.rejections(pair.owner) {
				t.Run(rejection.name, func(t *testing.T) {
					before := durableAuthState(t, db, fixture, pair.owner, pair.other)
					connectStatus, connectBody := postWithCredential(t, server, "/sync/connect", rejection.credential,
						authConnectRequest(t, server, testClientID(t, "auth-rejected")))
					requireProtocolError(t, "connect", connectStatus, connectBody, rejection.status, rejection.code)
					if configuration.accept != nil {
						pushStatus, pushBody := postWithCredential(t, server, "/sync/push", rejection.credential,
							authPushRequest(t, fixture, ownerClientID, ownerClient, pair.owner))
						requireProtocolError(t, "push", pushStatus, pushBody, rejection.status, rejection.code)
					}
					if after := durableAuthState(t, db, fixture, pair.owner, pair.other); after != before {
						t.Fatalf("rejected credential changed durable state:\nbefore=%s\nafter=%s", before, after)
					}
				})
			}

			if configuration.accept == nil {
				return
			}
			t.Run("other identity", func(t *testing.T) {
				otherCredential := configuration.accept(pair.other)
				otherClient := requireBoundIdentity(t, db, server, fixture, otherCredential, pair.other, testClientID(t, "auth-other"))
				before := durableAuthState(t, db, fixture, pair.owner, pair.other)
				for _, request := range []struct {
					path string
					body map[string]any
				}{
					{path: "/sync/pull", body: authPullRequest(ownerClientID, ownerClient)},
					{path: "/sync/rebuild", body: authRebuildRequest(t, ownerClientID, ownerClient, "user:"+pair.owner)},
					{path: "/sync/push", body: authPushRequest(t, fixture, ownerClientID, ownerClient, pair.other)},
				} {
					status, body := postWithCredential(t, server, request.path, otherCredential, request.body)
					requireProtocolError(t, "owner client "+request.path, status, body, http.StatusUnauthorized, "auth_required")
				}
				status, body := postWithCredential(t, server, "/sync/rebuild", otherCredential,
					authRebuildRequest(t, otherClient.id, otherClient.connectedClient, "user:"+pair.owner))
				requireProtocolError(t, "owner scope rebuild", status, body, http.StatusBadRequest, "invalid_request")
				if after := durableAuthState(t, db, fixture, pair.owner, pair.other); after != before {
					t.Fatalf("other identity changed owner-bound durable state:\nbefore=%s\nafter=%s", before, after)
				}
			})
		})
	}
}

const upstreamOutcomeHeader = "X-Test-Upstream-Outcome"

func upstream(outcome string) credential {
	return func(r *http.Request) { r.Header.Set(upstreamOutcomeHeader, outcome) }
}

func bearer(token string) credential {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

func signToken(t *testing.T, method jwt.SigningMethod, claims jwt.MapClaims, kid string, key any) string {
	t.Helper()
	token := jwt.NewWithClaims(method, claims)
	if kid != "" {
		token.Header["kid"] = kid
	}
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("sign %s token: %v", method.Alg(), err)
	}
	return signed
}

func generateRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return key
}

func publicKeyPEM(t *testing.T, key *rsa.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		t.Fatalf("encode RSA public key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func rsaJWK(kid string, key *rsa.PublicKey) map[string]string {
	return map[string]string{
		"kty": "RSA",
		"kid": kid,
		"alg": "RS256",
		"use": "sig",
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
}

func ecJWK(kid string, key *ecdsa.PublicKey) map[string]string {
	point, err := key.Bytes()
	if err != nil {
		panic(fmt.Sprintf("encode ES256 public key: %v", err))
	}
	return map[string]string{
		"kty": "EC",
		"kid": kid,
		"alg": "ES256",
		"use": "sig",
		"crv": "P-256",
		"x":   base64.RawURLEncoding.EncodeToString(point[1:33]),
		"y":   base64.RawURLEncoding.EncodeToString(point[33:65]),
	}
}

func jwkSet(t *testing.T, keys ...map[string]string) []byte {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatalf("encode JWK set: %v", err)
	}
	return encoded
}

// jwksProvider serves a controlled key set. A nil key set returns HTTP 503.
type jwksProvider struct {
	server *httptest.Server
	keys   atomic.Pointer[[]byte]
}

func newJWKSProvider(t *testing.T, keys []byte) *jwksProvider {
	t.Helper()
	provider := &jwksProvider{}
	provider.serve(keys)
	provider.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		keys := provider.keys.Load()
		if *keys == nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(*keys)
	}))
	t.Cleanup(provider.server.Close)
	return provider
}

func (p *jwksProvider) serve(keys []byte) {
	p.keys.Store(&keys)
}

func jwksConfig(t *testing.T, provider *jwksProvider) Config {
	t.Helper()
	lifecycle, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return Config{JWKSURL: provider.server.URL, JWKSContext: lifecycle}
}

type authRowsFixture struct {
	tableName    string
	tableID      string
	primaryField string
	labelField   string
}

// registerAuthRowsTable registers a table whose membership places each row in
// the private scope of the identity before the first slash of its key. One row
// per identity becomes WAL-materialized before the function returns.
func registerAuthRowsTable(t *testing.T, db *sql.DB, identities []string) authRowsFixture {
	t.Helper()
	tableName := testsupport.UniqueName(t, "auth_rows")
	functionName := testsupport.UniqueName(t, "auth_rows_membership")
	functionIdentity := "public." + quoteTestIdentifier(functionName)
	quotedTable := "public." + quoteTestIdentifier(tableName)

	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, label TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), deleted_at TIMESTAMPTZ)", quotedTable)); err != nil {
		t.Fatalf("create auth rows table: %v", err)
	}
	functionCreated := false
	registered := false
	t.Cleanup(func() {
		if registered {
			if _, err := db.Exec("SELECT synchro.synchro_unregister_table($1)", tableName); err != nil {
				t.Errorf("unregister auth rows table: %v", err)
			}
		}
		// The WAL worker fails on a pending generation whose relation is gone.
		if err := waitForRegistryActivation(db); err != nil {
			t.Errorf("auth rows registry cleanup: %v", err)
			return
		}
		if functionCreated {
			if _, err := db.Exec(fmt.Sprintf("DROP FUNCTION IF EXISTS %s(text)", functionIdentity)); err != nil {
				t.Errorf("drop auth rows membership function: %v", err)
			}
		}
		for _, policy := range []string{"synchro_owner_all", "synchro_worker_select"} {
			if _, err := db.Exec(fmt.Sprintf("DROP POLICY IF EXISTS %s ON %s", quoteTestIdentifier(policy), quotedTable)); err != nil {
				t.Errorf("drop auth rows policy %s: %v", policy, err)
			}
		}
		if _, err := db.Exec("DROP TABLE IF EXISTS " + quotedTable); err != nil {
			t.Errorf("drop auth rows table: %v", err)
		}
	})
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE FUNCTION %[1]s(p_id text)
		RETURNS SETOF text
		LANGUAGE SQL STABLE SECURITY INVOKER
		SET search_path = pg_catalog, synchro
		BEGIN ATOMIC
			SELECT 'user:' || pg_catalog.split_part(p_id, '/', 1) WHERE p_id IS NOT NULL;
		END;
		REVOKE ALL ON FUNCTION %[1]s(text) FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION %[1]s(text) TO synchro_owner, synchro_worker;
		GRANT USAGE ON SCHEMA public TO synchro_owner, synchro_worker;
		GRANT SELECT ON TABLE %[2]s TO synchro_owner, synchro_worker;
		ALTER TABLE %[2]s ENABLE ROW LEVEL SECURITY;
		CREATE POLICY synchro_owner_all ON %[2]s
			AS PERMISSIVE FOR ALL TO synchro_owner USING (true) WITH CHECK (true);
		CREATE POLICY synchro_worker_select ON %[2]s
			AS PERMISSIVE FOR SELECT TO synchro_worker USING (true)
	`, functionIdentity, quotedTable)); err != nil {
		t.Fatalf("create auth rows membership function: %v", err)
	}
	functionCreated = true
	if _, err := db.Exec(
		"SELECT synchro.synchro_register_table($1, $2, 'single_scope', 'id', 'updated_at', 'deleted_at', 'read_only')",
		"public."+tableName,
		functionIdentity,
	); err != nil {
		t.Fatalf("register auth rows table: %v", err)
	}
	registered = true
	if err := waitForRegistryActivation(db); err != nil {
		t.Fatalf("activate auth rows table: %v", err)
	}

	for _, identity := range identities {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, label) VALUES ($1, $2)", quotedTable), authRowID(identity), authRowLabel(identity)); err != nil {
			t.Fatalf("insert auth row: %v", err)
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		var materialized int
		if err := db.QueryRow(
			"SELECT count(*) FROM synchro.sync_bucket_edges WHERE table_name = $1 AND bucket_id = 'user:' || pg_catalog.split_part(record_id, '/', 1)",
			tableName,
		).Scan(&materialized); err != nil {
			t.Fatalf("count materialized auth rows: %v", err)
		}
		if materialized == len(identities) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("materialized auth rows = %d, want %d", materialized, len(identities))
		}
		time.Sleep(25 * time.Millisecond)
	}

	fixture := authRowsFixture{tableName: tableName}
	var manifest struct {
		Manifest struct {
			Tables []struct {
				TableID           string `json:"table_id"`
				Name              string `json:"name"`
				PrimaryKeyFieldID string `json:"primary_key_field_id"`
				Fields            []struct {
					FieldID string `json:"field_id"`
					Name    string `json:"name"`
				} `json:"fields"`
			} `json:"tables"`
		} `json:"manifest"`
	}
	var manifestJSON []byte
	if err := db.QueryRow("SELECT synchro.synchro_schema_manifest()::text").Scan(&manifestJSON); err != nil {
		t.Fatalf("load schema manifest: %v", err)
	}
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		t.Fatalf("decode schema manifest: %v", err)
	}
	for _, table := range manifest.Manifest.Tables {
		if table.Name != tableName {
			continue
		}
		fixture.tableID = table.TableID
		fixture.primaryField = table.PrimaryKeyFieldID
		for _, field := range table.Fields {
			if field.Name == "label" {
				fixture.labelField = field.FieldID
			}
		}
	}
	if fixture.tableID == "" || fixture.primaryField == "" || fixture.labelField == "" {
		t.Fatalf("schema manifest does not describe auth rows table %q", tableName)
	}
	return fixture
}

// waitForRegistryActivation waits until the WAL worker activates every
// registry generation. Activation follows WAL decoding, so it is not immediate.
func waitForRegistryActivation(db *sql.DB) error {
	deadline := time.Now().Add(time.Minute)
	for {
		var pending int
		if err := db.QueryRow("SELECT count(*) FROM synchro.sync_registry_generations WHERE state = 'pending'").Scan(&pending); err != nil {
			return fmt.Errorf("count pending registry generations: %w", err)
		}
		if pending == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d registry generations remain pending", pending)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func authRowID(identity string) string    { return identity + "/row" }
func authRowLabel(identity string) string { return "row of " + identity }

type boundClient struct {
	connectedClient
	id string
}

// requireBoundIdentity connects with an accepted credential and proves that
// the extension received exactly identity: the durable client row, the private
// scope assignment, and the rows returned by a private-scope rebuild.
func requireBoundIdentity(t *testing.T, db *sql.DB, server *httptest.Server, fixture authRowsFixture, presented credential, identity, clientID string) boundClient {
	t.Helper()
	status, body := postWithCredential(t, server, "/sync/connect", presented, authConnectRequest(t, server, clientID))
	if status != http.StatusOK {
		t.Fatalf("accepted connect status = %d, body = %v", status, body)
	}
	var privateScopes []string
	delta, _ := body["scopes"].(map[string]any)
	additions, _ := delta["add"].([]any)
	scopes := make(map[string]any)
	for _, raw := range additions {
		assignment, _ := raw.(map[string]any)
		id, _ := assignment["id"].(string)
		scopes[id] = map[string]any{"cursor": assignment["cursor"]}
		if strings.HasPrefix(id, "user:") {
			privateScopes = append(privateScopes, id)
		}
	}
	if !reflect.DeepEqual(privateScopes, []string{"user:" + identity}) {
		t.Fatalf("accepted connect private scopes = %v, want [user:%s]", privateScopes, identity)
	}
	var boundUsers []string
	rows, err := db.Query("SELECT user_id FROM synchro.sync_clients WHERE client_id = $1 ORDER BY user_id", clientID)
	if err != nil {
		t.Fatalf("load bound client identity: %v", err)
	}
	for rows.Next() {
		var user string
		if err := rows.Scan(&user); err != nil {
			t.Fatalf("scan bound client identity: %v", err)
		}
		boundUsers = append(boundUsers, user)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("read bound client identity: %v", err)
	}
	if !reflect.DeepEqual(boundUsers, []string{identity}) {
		t.Fatalf("durable client identities = %v, want [%s]", boundUsers, identity)
	}

	generation, _ := body["client_generation"].(float64)
	scopeSetVersion, _ := body["scope_set_version"].(float64)
	schema, _ := body["schema"].(map[string]any)
	client := boundClient{id: clientID, connectedClient: connectedClient{
		Generation:      int64(generation),
		Schema:          map[string]any{"version": schema["version"], "hash": schema["hash"]},
		ScopeSetVersion: int64(scopeSetVersion),
		Scopes:          scopes,
	}}
	status, body = postWithCredential(t, server, "/sync/rebuild", presented, authRebuildRequest(t, clientID, client.connectedClient, "user:"+identity))
	if status != http.StatusOK {
		t.Fatalf("private scope rebuild status = %d, body = %v", status, body)
	}
	records, _ := body["records"].([]any)
	var returned []string
	for _, raw := range records {
		record, _ := raw.(map[string]any)
		row, _ := record["row"].(map[string]any)
		if record["table"] != fixture.tableID {
			continue
		}
		returned = append(returned, fmt.Sprintf("%v|%v", row[fixture.primaryField], row[fixture.labelField]))
	}
	sort.Strings(returned)
	want := []string{authRowID(identity) + "|" + authRowLabel(identity)}
	if body["has_more"] != false || !reflect.DeepEqual(returned, want) {
		t.Fatalf("private scope rebuild rows = %v has_more = %v, want %v and false", returned, body["has_more"], want)
	}
	return client
}

func authConnectRequest(t *testing.T, server *httptest.Server, clientID string) map[string]any {
	t.Helper()
	return map[string]any{
		"client_id":         clientID,
		"platform":          "ios",
		"app_version":       "1.0.0",
		"protocol_version":  ExpectedProtocolVersion,
		"schema":            currentSchemaReference(t, server),
		"scope_set_version": 0,
		"known_scopes":      map[string]any{},
	}
}

func authPullRequest(clientID string, client connectedClient) map[string]any {
	return map[string]any{
		"client_id":         clientID,
		"client_generation": client.Generation,
		"schema":            client.Schema,
		"scope_set_version": client.ScopeSetVersion,
		"scopes":            client.Scopes,
		"limit":             100,
	}
}

func authRebuildRequest(t *testing.T, clientID string, client connectedClient, scope string) map[string]any {
	return map[string]any{
		"client_id":         clientID,
		"client_generation": client.Generation,
		"schema":            client.Schema,
		"scope":             scope,
		"rebuild_id":        randomUUID(t),
		"cursor":            nil,
		"limit":             100,
	}
}

// authPushRequest writes a row into the private scope of identity. The
// registered table is read-only, so an executed push records a terminal
// outcome in the durable push ledger.
func authPushRequest(t *testing.T, fixture authRowsFixture, clientID string, client connectedClient, identity string) map[string]any {
	return map[string]any{
		"client_id":         clientID,
		"client_generation": client.Generation,
		"batch_id":          randomUUID(t),
		"schema":            client.Schema,
		"mutations": []map[string]any{{
			"mutation_id":     randomUUID(t),
			"table":           fixture.tableID,
			"pk":              map[string]any{fixture.primaryField: identity + "/pushed"},
			"authored_schema": client.Schema,
			"op":              "insert",
			"client_version":  "2026-09-27T00:00:00.000000Z",
			"columns":         map[string]any{fixture.labelField: "pushed by " + identity},
		}},
	}
}

func randomUUID(t *testing.T) string {
	t.Helper()
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatalf("generate UUID: %v", err)
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16])
}

func postWithCredential(t *testing.T, server *httptest.Server, path string, presented credential, body map[string]any) (int, map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encode %s request: %v", path, err)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("create %s request: %v", path, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Client-Version", "1.0.0")
	presented(request)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("send %s request: %v", path, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s response: %v", path, err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode %s response %q: %v", path, raw, err)
	}
	return response.StatusCode, decoded
}

// requireProtocolError requires the exact wire error envelope: one error
// member with code, a nonempty message, and retryable false.
func requireProtocolError(t *testing.T, operation string, status int, body map[string]any, wantStatus int, wantCode string) {
	t.Helper()
	envelope, _ := body["error"].(map[string]any)
	message, _ := envelope["message"].(string)
	if status != wantStatus || len(body) != 1 || len(envelope) != 3 ||
		envelope["code"] != wantCode || envelope["retryable"] != false || message == "" {
		t.Fatalf("%s response = %d %v, want %d with exactly {code:%s, message, retryable:false}", operation, status, body, wantStatus, wantCode)
	}
}

// durableAuthState returns every durable client, assignment, checkpoint,
// rebuild, and push ledger row of the identities, plus the source table.
func durableAuthState(t *testing.T, db *sql.DB, fixture authRowsFixture, identities ...string) string {
	t.Helper()
	parts := []string{"'source'", fmt.Sprintf(
		"(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY r.id), '[]') FROM public.%s r)", quoteTestIdentifier(fixture.tableName))}
	for _, table := range []string{
		"sync_clients", "sync_client_scope_history", "sync_client_checkpoints", "sync_client_retirements",
		"sync_user_scopes", "sync_rebuild_sessions", "sync_push_batches", "sync_push_mutations",
	} {
		parts = append(parts, quoteTestLiteral(table), fmt.Sprintf(
			"(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY to_jsonb(s)::text), '[]') FROM synchro.%s s WHERE s.user_id = ANY($1))", table))
	}
	var state string
	if err := db.QueryRow("SELECT jsonb_build_object("+strings.Join(parts, ", ")+")::text", identities).Scan(&state); err != nil {
		t.Fatalf("load durable auth state: %v", err)
	}
	return state
}

// TestCompactedClientRenewsThroughConnect proves that a bound client that
// compaction deactivated stays bound. Pull and rebuild return
// client_generation_expired with the current generation, as the wire status
// table requires, and connect renews the generation.
func TestCompactedClientRenewsThroughConnect(t *testing.T) {
	db := testsupport.OpenPostgres(t)
	const owner = "auth-compacted-owner"
	secret := []byte("auth-compacted-test-secret-0123456789")
	fixture := registerAuthRowsTable(t, db, []string{owner})
	server := httptest.NewServer(Routes(Config{DB: db, JWTSecret: secret, MinClientVersion: "1.0.0"}))
	t.Cleanup(server.Close)
	presented := bearer(signToken(t, jwt.SigningMethodHS256, jwt.MapClaims{"sub": owner, "exp": time.Now().Add(time.Hour).Unix()}, "", secret))
	clientID := testClientID(t, "auth-compacted")
	client := requireBoundIdentity(t, db, server, fixture, presented, owner, clientID).connectedClient

	var marked bool
	var deactivated int64
	if err := db.QueryRow("SELECT synchro.synchro_inject_client_retention_expiry($1, $2)", owner, clientID).Scan(&marked); err != nil || !marked {
		t.Fatalf("mark client generation for expiry: marked=%v err=%v", marked, err)
	}
	if err := db.QueryRow("SELECT (synchro.synchro_compact('30 days', 10000)->>'deactivated_clients')::bigint").Scan(&deactivated); err != nil || deactivated < 1 {
		t.Fatalf("compact stale clients: deactivated=%d err=%v", deactivated, err)
	}
	var active bool
	if err := db.QueryRow("SELECT is_active FROM synchro.sync_clients WHERE user_id = $1 AND client_id = $2", owner, clientID).Scan(&active); err != nil || active {
		t.Fatalf("compacted client active=%v err=%v, want a retained inactive binding", active, err)
	}

	before := durableAuthState(t, db, fixture, owner)
	for _, request := range []struct {
		path string
		body map[string]any
	}{
		{path: "/sync/pull", body: authPullRequest(clientID, client)},
		{path: "/sync/rebuild", body: authRebuildRequest(t, clientID, client, "user:"+owner)},
	} {
		status, body := postWithCredential(t, server, request.path, presented, request.body)
		envelope, _ := body["error"].(map[string]any)
		message, _ := envelope["message"].(string)
		if status != http.StatusConflict || len(body) != 1 || len(envelope) != 4 ||
			envelope["code"] != "client_generation_expired" || envelope["retryable"] != false || message == "" ||
			envelope["current_client_generation"] != float64(client.Generation) {
			t.Fatalf("%s response = %d %v, want 409 client_generation_expired with current_client_generation %d",
				request.path, status, body, client.Generation)
		}
	}
	if after := durableAuthState(t, db, fixture, owner); after != before {
		t.Fatalf("generation rejection changed durable state:\nbefore=%s\nafter=%s", before, after)
	}

	renewal := authConnectRequest(t, server, clientID)
	renewal["client_generation"] = client.Generation
	renewal["schema"] = client.Schema
	renewal["scope_set_version"] = client.ScopeSetVersion
	renewal["known_scopes"] = client.Scopes
	status, body := postWithCredential(t, server, "/sync/connect", presented, renewal)
	renewed, _ := body["client_generation"].(float64)
	if status != http.StatusOK || int64(renewed) <= client.Generation {
		t.Fatalf("renewal connect = %d %v, want 200 with a generation above %d", status, body, client.Generation)
	}
	if err := db.QueryRow("SELECT is_active FROM synchro.sync_clients WHERE user_id = $1 AND client_id = $2", owner, clientID).Scan(&active); err != nil || !active {
		t.Fatalf("renewed client active=%v err=%v, want active", active, err)
	}
	client.Generation = int64(renewed)
	status, body = postWithCredential(t, server, "/sync/pull", presented, authPullRequest(clientID, client))
	if status != http.StatusOK {
		t.Fatalf("pull after renewal = %d %v, want 200", status, body)
	}
}
