package blackbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultRequestBodyLimit  = int64(1 << 20)
	defaultResponseBodyLimit = int64(1 << 20)
	maximumTokenBytes        = 16 << 10
)

// TokenProvider supplies one bearer token without exposing signing secrets.
type TokenProvider interface {
	Token(context.Context) (string, error)
}

// TokenProviderFunc adapts one function to TokenProvider.
type TokenProviderFunc func(context.Context) (string, error)

// Token returns the token supplied by f.
func (f TokenProviderFunc) Token(ctx context.Context) (string, error) {
	if f == nil {
		return "", errors.New("token provider is required")
	}
	return f(ctx)
}

// Request contains one raw HTTP request and its bounded metadata class.
type Request struct {
	Method  string
	Path    string
	Headers http.Header
	Body    []byte
	Class   string
}

// Response contains raw transport data for immediate strict decoding.
type Response struct {
	Status        int
	Headers       http.Header
	Body          []byte
	CanonicalBody []byte
	Duration      time.Duration
	Metadata      *ExchangeMetadata
}

// Client performs raw HTTP requests without production adapter imports.
type Client struct {
	BaseURL string
	HTTP    *http.Client
	Tokens  TokenProvider

	recorder          *Recorder
	requestBodyLimit  int64
	responseBodyLimit int64
}

// Do executes one bounded raw HTTP request.
func (c *Client) Do(ctx context.Context, request Request) (Response, error) {
	if ctx == nil {
		return Response{}, errors.New("HTTP request context is required")
	}
	if err := ctx.Err(); err != nil {
		return Response{}, err
	}
	if c == nil {
		return Response{}, errors.New("HTTP client is required")
	}
	requestLimit := c.requestBodyLimit
	if requestLimit == 0 {
		requestLimit = defaultRequestBodyLimit
	}
	responseLimit := c.responseBodyLimit
	if responseLimit == 0 {
		responseLimit = defaultResponseBodyLimit
	}
	if int64(len(request.Body)) > requestLimit {
		return Response{}, fmt.Errorf("HTTP request body exceeds %d bytes", requestLimit)
	}
	target, err := resolveRequestURL(c.BaseURL, request.Path)
	if err != nil {
		return Response{}, err
	}
	method := request.Method
	if method == "" {
		method = http.MethodPost
	}
	if !validHTTPMethod(method) || !validRequestClass(request.Class) {
		return Response{}, errors.New("HTTP request metadata is invalid")
	}
	headers, err := safeRequestHeaders(request.Headers)
	if err != nil {
		return Response{}, err
	}
	var token string
	if c.Tokens != nil {
		token, err = c.Tokens.Token(ctx)
		if err != nil {
			return Response{}, fmt.Errorf("get bearer token: %w", err)
		}
		if !validToken(token) {
			return Response{}, errors.New("bearer token is invalid")
		}
		if bytes.Contains(request.Body, []byte(token)) {
			return Response{}, ErrSensitiveRecording
		}
		headers.Set("Authorization", "Bearer "+token)
	}
	if headers.Get("Accept-Encoding") == "" {
		headers.Set("Accept-Encoding", "identity")
	}

	httpRequest, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(request.Body))
	if err != nil {
		return Response{}, fmt.Errorf("create raw HTTP request: %w", err)
	}
	httpRequest.Header = headers
	client := isolatedHTTPClient(c.HTTP)
	started := time.Now()
	httpResponse, err := client.Do(httpRequest)
	duration := time.Since(started)
	if err != nil {
		if recordErr := c.record(request, 0, nil, duration, nil, token); recordErr != nil {
			return Response{}, recordErr
		}
		return Response{}, fmt.Errorf("execute raw HTTP request class %q: %w", request.Class, err)
	}
	if httpResponse == nil || httpResponse.Body == nil {
		return Response{}, errors.New("raw HTTP response is incomplete")
	}
	body, readErr := io.ReadAll(io.LimitReader(httpResponse.Body, responseLimit+1))
	closeErr := httpResponse.Body.Close()
	if readErr != nil {
		return Response{}, fmt.Errorf("read raw HTTP response: %w", readErr)
	}
	if closeErr != nil {
		return Response{}, fmt.Errorf("close raw HTTP response: %w", closeErr)
	}
	if int64(len(body)) > responseLimit {
		return Response{}, fmt.Errorf("raw HTTP response exceeds %d bytes", responseLimit)
	}
	canonical := append([]byte(nil), body...)
	if responseIsJSON(httpResponse.Header, body) {
		canonical, err = CanonicalResponseBytes(body)
		if err != nil {
			if recordErr := c.record(request, httpResponse.StatusCode, httpResponse.Header, duration, body, token); recordErr != nil {
				return Response{}, recordErr
			}
			return Response{}, fmt.Errorf("canonicalize JSON response: %w", err)
		}
	}
	metadata, err := c.recordWithMetadata(request, httpResponse.StatusCode, httpResponse.Header, duration, body, token)
	if err != nil {
		return Response{}, err
	}
	response := Response{
		Status:        httpResponse.StatusCode,
		Headers:       httpResponse.Header.Clone(),
		Body:          append([]byte(nil), body...),
		CanonicalBody: append([]byte(nil), canonical...),
		Duration:      duration,
	}
	if metadata != nil {
		copy := cloneExchangeMetadata(*metadata)
		response.Metadata = &copy
	}
	return response, nil
}

func (c *Client) record(request Request, status int, headers http.Header, duration time.Duration, responseBody []byte, token string) error {
	_, err := c.recordWithMetadata(request, status, headers, duration, responseBody, token)
	return err
}

func (c *Client) recordWithMetadata(request Request, status int, headers http.Header, duration time.Duration, responseBody []byte, token string) (*ExchangeMetadata, error) {
	if c.recorder == nil {
		return nil, nil
	}
	var sensitive [][]byte
	if token != "" {
		sensitive = [][]byte{[]byte(token)}
	}
	metadata, err := c.recorder.recordExchange(request.Class, status, headers, duration, request.Body, responseBody, sensitive)
	if err != nil {
		return nil, fmt.Errorf("record bounded HTTP metadata: %w", err)
	}
	return &metadata, nil
}

func resolveRequestURL(baseURL, requestPath string) (*url.URL, error) {
	base, err := url.Parse(baseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, errors.New("HTTP base URL is invalid")
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, errors.New("HTTP base URL scheme is invalid")
	}
	if base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("HTTP base URL contains disallowed data")
	}
	reference, err := url.ParseRequestURI(requestPath)
	if err != nil || reference.IsAbs() || reference.Host != "" || reference.Fragment != "" || !strings.HasPrefix(reference.Path, "/") {
		return nil, errors.New("HTTP request path is invalid")
	}
	target := base.ResolveReference(reference)
	if target.Scheme != base.Scheme || target.Host != base.Host || target.User != nil {
		return nil, errors.New("HTTP request escaped its base origin")
	}
	return target, nil
}

func safeRequestHeaders(source http.Header) (http.Header, error) {
	result := make(http.Header, len(source)+2)
	for name, values := range source {
		canonical := http.CanonicalHeaderKey(name)
		switch canonical {
		case "Authorization", "Cookie", "Proxy-Authorization", "Set-Cookie", "Host", "Content-Length":
			return nil, fmt.Errorf("request header %q is not permitted", canonical)
		}
		if canonical == "" || len(values) > maximumHeaderValues {
			return nil, errors.New("request headers are invalid")
		}
		for _, value := range values {
			if len(value) > maximumHeaderValueLen || strings.ContainsAny(value, "\r\n") {
				return nil, errors.New("request header value is invalid")
			}
			result.Add(canonical, value)
		}
	}
	return result, nil
}

func isolatedHTTPClient(source *http.Client) *http.Client {
	if source == nil {
		source = http.DefaultClient
	}
	copy := *source
	copy.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &copy
}

func validHTTPMethod(method string) bool {
	if method == "" || len(method) > 16 {
		return false
	}
	for _, character := range method {
		if character < 'A' || character > 'Z' {
			return false
		}
	}
	return true
}

func validToken(token string) bool {
	if token == "" || len(token) > maximumTokenBytes {
		return false
	}
	for _, character := range token {
		if character <= ' ' || character == 0x7f {
			return false
		}
	}
	return true
}

func responseIsJSON(headers http.Header, body []byte) bool {
	if len(bytes.TrimSpace(body)) == 0 {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(headers.Get("Content-Type"))
	if err == nil && (mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")) {
		return true
	}
	trimmed := bytes.TrimSpace(body)
	return len(trimmed) != 0 && (trimmed[0] == '{' || trimmed[0] == '[')
}
