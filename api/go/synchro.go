// Package synchroapi provides a standalone HTTP adapter for the synchro PostgreSQL
// extension. It is a thin passthrough layer: parse HTTP request, call
// SELECT synchro.synchro_*() via database/sql, forward the JSONB response.
//
// This package has zero dependency on the synchro engine module. Users bring
// their own *sql.DB (pgx/v5 stdlib recommended) and auth integration.
//
// Usage:
//
//	handler := synchroapi.Routes(synchroapi.Config{
//	    DB:               db,
//	    JWTSecret:        []byte("my-secret"),
//	    MinClientVersion: "1.0.0",
//	})
//
// Or, when the main API router already authenticated the request:
//
//	handler := synchroapi.Routes(synchroapi.Config{
//	    DB:             db,
//	    UserIDResolver: synchroapi.RequestContextUserIDResolver,
//	})
//	http.ListenAndServe(":8080", handler)
package synchroapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"
)

const (
	maxReadinessResultBytes = 64

	// DefaultDatabaseQueryTimeout bounds one canonical extension call. It remains
	// below the standalone server write timeout.
	DefaultDatabaseQueryTimeout = 25 * time.Second
)

// Config configures the sync HTTP handler.
type Config struct {
	// DB is the database connection pool (required). Must be connected to a
	// PostgreSQL database with the synchro_pg extension installed.
	DB *sql.DB

	// UserIDResolver resolves the canonical internal user ID from a trusted
	// upstream-authenticated request. Mutually exclusive with JWTSecret and
	// JWKSURL.
	UserIDResolver UserIDResolver

	// JWTSecret is the HS256 shared secret for token validation.
	// Mutually exclusive with JWKSURL and UserIDResolver.
	JWTSecret []byte

	// JWKSURL is the RS256/ES256 JWKS endpoint for token validation.
	// Mutually exclusive with JWTSecret and UserIDResolver.
	JWKSURL string

	// JWKSContext controls the JWKS refresh lifecycle. It is required when
	// JWKSURL is set. Cancel it when the handler is no longer in use.
	JWKSContext context.Context

	// JWTUserClaim is the JWT claim containing the user ID (default: "sub").
	JWTUserClaim string

	// MinClientVersion is the minimum client version allowed (semver).
	// When set, authenticated sync requests must send exactly one
	// X-Client-Version or X-App-Version header at or above this version.
	// Leave empty to skip version checking.
	MinClientVersion string

	// DatabaseQueryTimeout bounds one canonical extension call. A zero value uses
	// DefaultDatabaseQueryTimeout. A negative value is invalid.
	DatabaseQueryTimeout time.Duration
}

// Handler holds the database connection and serves sync endpoints.
type Handler struct {
	db           *sql.DB
	queryTimeout time.Duration
}

// Routes returns an http.Handler with all sync endpoints mounted.
// The returned handler applies version check and one auth mode:
//
// - trusted upstream auth via UserIDResolver
// - JWT validation via JWTSecret or JWKSURL
//
// Endpoints:
//
//	POST /sync/connect  (auth required)
//	POST /sync/pull     (auth required)
//	POST /sync/push     (auth required)
//	POST /sync/rebuild  (auth required)
//	GET  /sync/schema   (no auth)
//	GET  /sync/tables   (no auth)
//	GET  /ready         (no auth)
func Routes(cfg Config) http.Handler {
	if cfg.DB == nil {
		panic("synchroapi: Config.DB is required")
	}
	if cfg.DatabaseQueryTimeout < 0 {
		panic("synchroapi: Config.DatabaseQueryTimeout must not be negative")
	}
	queryTimeout := cfg.DatabaseQueryTimeout
	if queryTimeout == 0 {
		queryTimeout = DefaultDatabaseQueryTimeout
	}
	if queryTimeout <= 0 {
		panic("synchroapi: effective database query timeout must be positive")
	}

	h := &Handler{db: cfg.DB, queryTimeout: queryTimeout}

	mux := http.NewServeMux()

	// Unauthenticated endpoints.
	mux.HandleFunc("/sync/schema", h.serveSchema)
	mux.HandleFunc("/sync/tables", h.serveTables)
	mux.HandleFunc("/ready", h.serveReadiness)

	// Authenticated endpoints wrapped in JWT middleware.
	authMux := http.NewServeMux()
	authMux.HandleFunc("/sync/connect", h.serveConnect)
	authMux.HandleFunc("/sync/pull", h.servePull)
	authMux.HandleFunc("/sync/push", h.servePush)
	authMux.HandleFunc("/sync/rebuild", h.serveRebuild)

	authed := authMiddleware(cfg, versionCheckMiddleware(cfg.MinClientVersion, authMux))
	mux.Handle("/sync/connect", authed)
	mux.Handle("/sync/pull", authed)
	mux.Handle("/sync/push", authed)
	mux.Handle("/sync/rebuild", authed)

	return mux
}

func (h *Handler) serveReadiness(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}

	raw, err := h.queryJSONB(r.Context(), "SELECT synchro.synchro_readiness()")
	if err != nil || !strictReadiness(raw) {
		writeRawJSON(w, http.StatusServiceUnavailable, []byte(`{"ready":false}`))
		return
	}
	writeRawJSON(w, http.StatusOK, []byte(`{"ready":true}`))
}

func strictReadiness(raw []byte) bool {
	if len(raw) == 0 || len(raw) > maxReadinessResultBytes || validateJSONObjectJSON(raw) != nil {
		return false
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(raw, &result); err != nil || len(result) != 1 {
		return false
	}
	encodedReady, ok := result["ready"]
	if !ok {
		return false
	}
	var ready bool
	return json.Unmarshal(encodedReady, &ready) == nil && ready
}
