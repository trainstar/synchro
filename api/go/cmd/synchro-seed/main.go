package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	seeddb "github.com/trainstar/synchro/api/go/seeddb"
)

func main() {
	if err := run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, env func(string) string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("synchro-seed", flag.ContinueOnError)
	fs.SetOutput(stderr)
	databaseURL := fs.String("database-url", env("DATABASE_URL"), "PostgreSQL connection string with synchro_pg installed")
	outputPath := fs.String("output", "", "Output path for the generated SQLite seed database")
	overwrite := fs.Bool("overwrite", false, "Overwrite an existing output database")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *databaseURL == "" {
		return fmt.Errorf("database URL is required, pass --database-url or set DATABASE_URL")
	}
	if *outputPath == "" {
		return fmt.Errorf("output path is required, pass --output")
	}

	db, err := sql.Open("pgx", *databaseURL)
	if err != nil {
		return fmt.Errorf("opening postgres database: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		return fmt.Errorf("pinging postgres database: %w", err)
	}

	err = seeddb.Generate(context.Background(), db, seeddb.GenerateOptions{
		OutputPath: *outputPath,
		Overwrite:  *overwrite,
	})
	if err != nil {
		if errors.Is(err, seeddb.ErrOutputExists) {
			return fmt.Errorf("%w, pass --overwrite to replace it", err)
		}
		return fmt.Errorf("generating seed database: %w", err)
	}

	fmt.Fprintf(stdout, "generated seed database at %s\n", *outputPath)
	return nil
}
