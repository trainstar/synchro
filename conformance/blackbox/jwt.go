package blackbox

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// Claims is one JSON object used as an HS256 JWT payload.
type Claims map[string]any

// SignHS256 creates an independent HS256 JWT with standard-library code.
func SignHS256(secret []byte, claims Claims) (string, error) {
	if len(secret) == 0 {
		return "", errors.New("HS256 secret is required")
	}
	if claims == nil {
		return "", errors.New("HS256 claims are required")
	}
	header, err := json.Marshal(struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
	}{Algorithm: "HS256", Type: "JWT"})
	if err != nil {
		return "", fmt.Errorf("encode JWT header: %w", err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("encode JWT claims: %w", err)
	}
	encoding := base64.RawURLEncoding
	signingInput := encoding.EncodeToString(header) + "." + encoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(signingInput))
	return signingInput + "." + encoding.EncodeToString(mac.Sum(nil)), nil
}

// HS256TokenProvider retains only the completed token.
type HS256TokenProvider struct {
	token string
}

// NewHS256TokenProvider signs one token and does not retain the secret bytes.
func NewHS256TokenProvider(secret []byte, claims Claims) (*HS256TokenProvider, error) {
	token, err := SignHS256(secret, claims)
	if err != nil {
		return nil, err
	}
	return &HS256TokenProvider{token: token}, nil
}

// Token returns the previously signed token without recording it.
func (p *HS256TokenProvider) Token(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", errors.New("token context is required")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if p == nil || p.token == "" {
		return "", errors.New("signed token is unavailable")
	}
	return p.token, nil
}
