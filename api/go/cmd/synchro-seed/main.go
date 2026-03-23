package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	seeddb "github.com/trainstar/synchro/api/go/seeddb"
)

func main() {
	var outputPath string
	var databaseURL string
	var overwrite bool

	flag.StringVar(&databaseURL, "database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection string with synchro_pg installed")
	flag.StringVar(&outputPath, "output", "", "Output path for the generated SQLite seed database")
	flag.BoolVar(&overwrite, "overwrite", false, "Overwrite an existing output database")
	flag.Parse()

	if databaseURL == "" {
		fatalf("database URL is required, pass --database-url or set DATABASE_URL")
	}
	if outputPath == "" {
		fatalf("output path is required, pass --output")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		fatalf("opening postgres database: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		fatalf("pinging postgres database: %v", err)
	}

	err = seeddb.Generate(context.Background(), db, seeddb.GenerateOptions{
		OutputPath: outputPath,
		Overwrite:  overwrite,
	})
	if err != nil {
		if errors.Is(err, seeddb.ErrOutputExists) {
			fatalf("%v, pass --overwrite to replace it", err)
		}
		fatalf("generating seed database: %v", err)
	}

	fmt.Fprintf(os.Stdout, "generated seed database at %s\n", outputPath)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
