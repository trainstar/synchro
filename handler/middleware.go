package handler

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"

	"github.com/trainstar/synchro"
)

type contextKey string

const userIDKey contextKey = "synchro.user_id"

// UserIDFromContext extracts the user ID from the request context.
// The consuming application is responsible for setting this via WithUserID.
func UserIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}

// WithUserID returns a new context with the user ID set.
// UUIDs are normalized to lowercase per RFC 4122 / PostgreSQL convention.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, strings.ToLower(userID))
}

// UserIDMiddleware is an http.Handler middleware that extracts the user ID
// from a header and sets it in the request context. The consuming application
// should replace this with their own auth middleware.
func UserIDMiddleware(header string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.Header.Get(header)
		if userID == "" {
			writeError(w, http.StatusUnauthorized, "missing user identity")
			return
		}
		ctx := WithUserID(r.Context(), userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// JWTAuthConfig configures JWT-based authentication middleware.
type JWTAuthConfig struct {
	// Secret is the HS256 shared secret. Mutually exclusive with JWKSURL.
	Secret []byte
	// JWKSURL is the RS256/ES256 JWKS endpoint URL. Mutually exclusive with Secret.
	JWKSURL string
	// UserClaim is the JWT claim containing the user ID (default: "sub").
	UserClaim string
}

// JWTAuthMiddleware validates a Bearer token from the Authorization header,
// extracts the user ID from the configured claim, and sets it in context.
func JWTAuthMiddleware(cfg JWTAuthConfig, next http.Handler) http.Handler {
	if cfg.UserClaim == "" {
		cfg.UserClaim = "sub"
	}

	keyFunc, err := buildKeyFunc(cfg)
	if err != nil {
		// Fail fast at startup: misconfigured JWT is a fatal error.
		panic(fmt.Sprintf("jwt auth: %v", err))
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "missing or malformed authorization header")
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		token, err := jwt.Parse(tokenStr, keyFunc, jwt.WithValidMethods(validMethods(cfg)))
		if err != nil || !token.Valid {
			writeError(w, http.StatusUnauthorized, "invalid token")
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			writeError(w, http.StatusUnauthorized, "invalid token claims")
			return
		}

		userID, _ := claims[cfg.UserClaim].(string)
		if userID == "" {
			writeError(w, http.StatusUnauthorized, "token missing user claim")
			return
		}

		ctx := WithUserID(r.Context(), userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func buildKeyFunc(cfg JWTAuthConfig) (jwt.Keyfunc, error) {
	switch {
	case len(cfg.Secret) > 0:
		return func(token *jwt.Token) (any, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return cfg.Secret, nil
		}, nil
	case cfg.JWKSURL != "":
		jwks, err := keyfunc.NewDefault([]string{cfg.JWKSURL})
		if err != nil {
			return nil, fmt.Errorf("fetching JWKS: %w", err)
		}
		return jwks.KeyfuncCtx(context.Background()), nil
	default:
		return nil, fmt.Errorf("JWTAuthConfig requires either Secret or JWKSURL")
	}
}

func validMethods(cfg JWTAuthConfig) []string {
	if len(cfg.Secret) > 0 {
		return []string{"HS256", "HS384", "HS512"}
	}
	return []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}
}

// RetryAfterMiddleware rejects requests when the check function returns true.
// The consuming app provides the policy (rate limiting, maintenance mode, etc.).
func RetryAfterMiddleware(check func(r *http.Request) (reject bool, status int, retryAfter int), next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reject, status, retryAfter := check(r); reject {
			writeRetryError(w, status, http.StatusText(status), retryAfter)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// VersionCheckMiddleware validates the client version header against the minimum.
func VersionCheckMiddleware(header, minVersion string, next http.Handler) http.Handler {
	if minVersion == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientVersion := r.Header.Get(header)
		if clientVersion != "" {
			if err := synchro.CheckVersion(clientVersion, minVersion); err != nil {
				writeError(w, http.StatusUpgradeRequired, "client upgrade required")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
