package handler

import (
	"context"
	"net/http"

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
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
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
