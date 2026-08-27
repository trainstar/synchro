//go:build ddlidentity

package seeddb

import (
	"context"
	"os"
	"testing"
)

func TestCanonicalClientSeedMatchesSeedDBDDL(t *testing.T) {
	t.Run("assertion", func(t *testing.T) {
		outputPath := os.Getenv("SYNCHRO_DDL_IDENTITY_SEED_PATH")
		if outputPath == "" {
			t.Fatal("SYNCHRO_DDL_IDENTITY_SEED_PATH is required")
		}
		db := testPostgres(t)
		registerSeedTestTable(t, db, "test_seed_ddl_identity")
		if err := Generate(context.Background(), db, GenerateOptions{
			OutputPath: outputPath,
			Overwrite:  false,
		}); err != nil {
			t.Fatalf("generate seed database: %v", err)
		}
	})
}
