package main

import (
	"database/sql"
	"net/http"

	synchroapi "github.com/trainstar/synchro/api/go"
)

func main() {
	var db *sql.DB
	handler := synchroapi.Routes(synchroapi.Config{
		DB: db,
		UserIDResolver: func(*http.Request) (string, error) {
			return "consumer", nil
		},
	})
	_ = handler
}
