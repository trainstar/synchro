// Package synchroapi provides a standalone HTTP adapter for the synchro PostgreSQL
// extension. It is a thin passthrough layer: parse HTTP request, call
// SELECT synchro_*() via database/sql, forward the JSONB response.
//
// This package has zero dependency on the synchro engine module. Users bring
// their own *sql.DB (pgx/v5 stdlib recommended) and JWT configuration.
//
// Usage:
//
//	handler := synchroapi.Routes(synchroapi.Config{
//	    DB:               db,
//	    JWTSecret:        []byte("my-secret"),
//	    MinClientVersion: "1.0.0",
//	})
//	http.ListenAndServe(":8080", handler)
package synchroapi

import (
	"database/sql"
	"net/http"
)

// Config configures the sync HTTP handler.
type Config struct {
	// DB is the database connection pool (required). Must be connected to a
	// PostgreSQL database with the synchro_pg extension installed.
	DB *sql.DB

	// JWTSecret is the HS256 shared secret for token validation.
	// Mutually exclusive with JWKSURL.
	JWTSecret []byte

	// JWKSURL is the RS256/ES256 JWKS endpoint for token validation.
	// Mutually exclusive with JWTSecret.
	JWKSURL string

	// JWTUserClaim is the JWT claim containing the user ID (default: "sub").
	JWTUserClaim string

	// MinClientVersion is the minimum client version allowed (semver).
	// When set, requests with X-App-Version below this are rejected with 426.
	// Leave empty to skip version checking.
	MinClientVersion string
}

// Handler holds the database connection and serves sync endpoints.
type Handler struct {
	db *sql.DB
}

// Routes returns an http.Handler with all sync endpoints mounted.
// The returned handler applies version check and JWT auth middleware.
//
// Endpoints:
//
//	POST /sync/register   (auth required)
//	POST /sync/pull       (auth required)
//	POST /sync/push       (auth required)
//	POST /sync/rebuild    (auth required)
//	GET  /sync/schema     (no auth)
//	GET  /sync/tables     (no auth)
//	GET  /sync/debug      (auth required)
func Routes(cfg Config) http.Handler {
	if cfg.DB == nil {
		panic("synchroapi: Config.DB is required")
	}

	h := &Handler{db: cfg.DB}

	mux := http.NewServeMux()

	// Unauthenticated endpoints.
	mux.HandleFunc("/sync/schema", h.serveSchema)
	mux.HandleFunc("/sync/tables", h.serveTables)

	// Authenticated endpoints wrapped in JWT middleware.
	authMux := http.NewServeMux()
	authMux.HandleFunc("/sync/register", h.serveRegister)
	authMux.HandleFunc("/sync/pull", h.servePull)
	authMux.HandleFunc("/sync/push", h.servePush)
	authMux.HandleFunc("/sync/rebuild", h.serveRebuild)
	authMux.HandleFunc("/sync/debug", h.serveDebug)

	authed := jwtMiddleware(cfg, authMux)
	mux.Handle("/sync/register", authed)
	mux.Handle("/sync/pull", authed)
	mux.Handle("/sync/push", authed)
	mux.Handle("/sync/rebuild", authed)
	mux.Handle("/sync/debug", authed)

	// Apply version check as outermost middleware.
	return versionCheckMiddleware(cfg.MinClientVersion, mux)
}
