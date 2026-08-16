package synchroapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type contextKey string

const userIDKey contextKey = "synchroapi.user_id"

// ErrAuthRequired indicates the request does not carry an authenticated user.
var ErrAuthRequired = errors.New("auth required")

// UserIDResolver resolves the canonical internal user ID for a request.
// It is intended for trusted upstream auth integration, for example when the
// main API router has already validated WorkOS or another identity provider.
//
// Return ErrAuthRequired when the request is unauthenticated.
// Return any other error only for internal resolver failures.
type UserIDResolver func(*http.Request) (string, error)

// UserIDFromContext extracts the user ID set by the JWT middleware.
func UserIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}

// WithUserID returns a new context with the resolved canonical user ID set.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// RequestContextUserIDResolver resolves a user ID previously stored in the
// request context with WithUserID.
func RequestContextUserIDResolver(r *http.Request) (string, error) {
	userID := UserIDFromContext(r.Context())
	if userID == "" {
		return "", ErrAuthRequired
	}
	return userID, nil
}

func authMiddleware(cfg Config, next http.Handler) http.Handler {
	authModes := 0
	if cfg.UserIDResolver != nil {
		authModes++
	}
	if len(cfg.JWTSecret) > 0 {
		authModes++
	}
	if cfg.JWKSURL != "" {
		authModes++
	}
	if authModes > 1 {
		panic("synchroapi: UserIDResolver, JWTSecret, and JWKSURL are mutually exclusive")
	}

	switch {
	case cfg.UserIDResolver != nil:
		return resolverMiddleware(cfg.UserIDResolver, next)
	case len(cfg.JWTSecret) > 0 || cfg.JWKSURL != "":
		return jwtMiddleware(cfg, next)
	default:
		panic("synchroapi: auth configuration requires UserIDResolver, JWTSecret, or JWKSURL")
	}
}

func resolverMiddleware(resolve UserIDResolver, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, err := resolve(r)
		switch {
		case err == nil && userID == "":
			writeJSONError(w, http.StatusUnauthorized, "missing user identity")
			return
		case err != nil && errors.Is(err, ErrAuthRequired):
			writeJSONError(w, http.StatusUnauthorized, "missing user identity")
			return
		case err != nil:
			writeJSONError(w, http.StatusInternalServerError, "auth resolution failed")
			return
		}

		ctx := WithUserID(r.Context(), userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// jwtMiddleware validates a Bearer token and extracts the user ID.
func jwtMiddleware(cfg Config, next http.Handler) http.Handler {
	userClaim := "sub"
	if cfg.JWTUserClaim != "" {
		userClaim = cfg.JWTUserClaim
	}

	keyFuncForContext, err := buildKeyFunc(cfg)
	if err != nil {
		panic(fmt.Sprintf("synchroapi: jwt config: %v", err))
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorizationValues := r.Header.Values("Authorization")
		if len(authorizationValues) != 1 {
			writeJSONError(w, http.StatusUnauthorized, "missing or malformed authorization header")
			return
		}
		authHeader := authorizationValues[0]
		if !strings.HasPrefix(authHeader, "Bearer ") {
			writeJSONError(w, http.StatusUnauthorized, "missing or malformed authorization header")
			return
		}
		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

		token, err := jwt.Parse(tokenStr, keyFuncForContext(r.Context()), jwt.WithValidMethods(validMethods(cfg)))
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

		ctx := WithUserID(r.Context(), userID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func buildKeyFunc(cfg Config) (func(context.Context) jwt.Keyfunc, error) {
	switch {
	case len(cfg.JWTSecret) > 0:
		return func(context.Context) jwt.Keyfunc {
			return func(token *jwt.Token) (any, error) {
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
				}
				return cfg.JWTSecret, nil
			}
		}, nil
	case cfg.JWKSURL != "":
		if cfg.JWKSContext == nil {
			return nil, fmt.Errorf("JWKSContext is required when JWKSURL is set")
		}
		jwks, err := keyfunc.NewDefaultCtx(cfg.JWKSContext, []string{cfg.JWKSURL})
		if err != nil {
			return nil, fmt.Errorf("fetching JWKS: %w", err)
		}
		return jwks.KeyfuncCtx, nil
	default:
		return nil, fmt.Errorf("JWTSecret or JWKSURL is required")
	}
}

func validMethods(cfg Config) []string {
	if len(cfg.JWTSecret) > 0 {
		return []string{"HS256"}
	}
	return []string{"RS256", "ES256"}
}

// versionCheckMiddleware rejects requests with a client version below the minimum.
func versionCheckMiddleware(minVersion string, next http.Handler) http.Handler {
	if minVersion == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		versions := append([]string(nil), r.Header.Values("X-Client-Version")...)
		versions = append(versions, r.Header.Values("X-App-Version")...)
		if len(versions) != 1 {
			writeJSONError(w, http.StatusUpgradeRequired, "client upgrade required")
			return
		}
		if err := checkVersion(versions[0], minVersion); err != nil {
			writeJSONError(w, http.StatusUpgradeRequired, "client upgrade required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
