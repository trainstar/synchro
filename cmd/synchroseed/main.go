package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/trainstar/synchro/seeddb"
)

func main() {
	server := flag.String("server", "", "Synchrod server URL (e.g., http://localhost:8080)")
	token := flag.String("token", "", "JWT authentication token")
	jwtSecret := flag.String("jwt-secret", "", "JWT secret to auto-generate a token (alternative to -token)")
	output := flag.String("output", "seed.db", "Output SQLite database file path")
	withData := flag.Bool("with-data", false, "Include snapshot data (registers a temporary client and fetches current server data)")
	flag.Parse()

	if *server == "" {
		fmt.Fprintln(os.Stderr, "error: -server flag is required")
		flag.Usage()
		os.Exit(1)
	}

	authToken := *token
	if authToken == "" && *jwtSecret != "" {
		authToken = signJWT(*jwtSecret)
	}

	cfg := seeddb.Config{
		ServerURL:  *server,
		AuthToken:  authToken,
		OutputPath: *output,
		WithData:   *withData,
	}

	if err := seeddb.Generate(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("seed database written to %s\n", *output)
}

func signJWT(secret string) string {
	header := base64URLEncode([]byte(`{"alg":"HS256","typ":"JWT"}`))
	now := time.Now().Unix()
	payload := base64URLEncode([]byte(fmt.Sprintf(`{"sub":"synchroseed","iat":%d,"exp":%d}`, now, now+3600)))
	signingInput := header + "." + payload
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	sig := base64URLEncode(mac.Sum(nil))
	return signingInput + "." + sig
}

func base64URLEncode(data []byte) string {
	s := base64.StdEncoding.EncodeToString(data)
	s = strings.ReplaceAll(s, "+", "-")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.TrimRight(s, "=")
	return s
}
