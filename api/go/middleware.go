package synchroapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const userIDKey contextKey = "synchroapi.user_id"

// UserIDFromContext extracts the user ID set by the JWT middleware.
func UserIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}

// withUserID returns a new context with the user ID set.
// UUIDs are normalized to lowercase per RFC 4122 / PostgreSQL convention.
func withUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, strings.ToLower(userID))
}

// jwtMiddleware validates a Bearer token and extracts the user ID.
func jwtMiddleware(cfg Config, next http.Handler) http.Handler {
	userClaim := "sub"
	if cfg.JWTUserClaim != "" {
		userClaim = cfg.JWTUserClaim
	}

	keyFunc, err := buildKeyFunc(cfg)
	if err != nil {
		panic(fmt.Sprintf("synchroapi: jwt config: %v", err))
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			writeJSONError(w, http.StatusUnauthorized, "missing or malformed authorization header")
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		token, err := jwt.Parse(tokenStr, keyFunc, jwt.WithValidMethods(validMethods(cfg)))
		if err != nil || !token.Valid {
			writeJSONError(w, http.StatusUnauthorized, "invalid token")
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			writeJSONError(w, http.StatusUnauthorized, "invalid token claims")
			return
		}

		userID, _ := claims[userClaim].(string)
		if userID == "" {
			writeJSONError(w, http.StatusUnauthorized, "token missing user claim")
			return
		}

		ctx := withUserID(r.Context(), userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func buildKeyFunc(cfg Config) (jwt.Keyfunc, error) {
	switch {
	case len(cfg.JWTSecret) > 0:
		return func(token *jwt.Token) (any, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return cfg.JWTSecret, nil
		}, nil
	case cfg.JWKSURL != "":
		jwks, err := keyfunc.NewDefault([]string{cfg.JWKSURL})
		if err != nil {
			return nil, fmt.Errorf("fetching JWKS: %w", err)
		}
		return jwks.KeyfuncCtx(context.Background()), nil
	default:
		return nil, fmt.Errorf("JWTSecret or JWKSURL is required")
	}
}

func validMethods(cfg Config) []string {
	if len(cfg.JWTSecret) > 0 {
		return []string{"HS256", "HS384", "HS512"}
	}
	return []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}
}

// versionCheckMiddleware rejects requests with a client version below the minimum.
func versionCheckMiddleware(minVersion string, next http.Handler) http.Handler {
	if minVersion == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientVersion := r.Header.Get("X-Client-Version")
		if clientVersion != "" {
			if err := checkVersion(clientVersion, minVersion); err != nil {
				writeJSONError(w, http.StatusUpgradeRequired, "client upgrade required")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
