package soak

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/trainstar/synchro/conformance/faults"
)

// CatalogIdentity binds a run to one exact validated fault catalog.
type CatalogIdentity struct {
	SchemaURI     string `json:"schema_uri"`
	SchemaVersion int    `json:"schema_version"`
	Release       string `json:"release"`
	SHA256        string `json:"sha256"`
}

func catalogIdentityOf(catalog *faults.Catalog) (CatalogIdentity, error) {
	if catalog == nil {
		return CatalogIdentity{}, ErrCatalogRequired
	}
	encoded, err := json.Marshal(catalog)
	if err != nil {
		return CatalogIdentity{}, fmt.Errorf("encode fault catalog identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return CatalogIdentity{
		SchemaURI:     catalog.SchemaURI,
		SchemaVersion: catalog.SchemaVersion,
		Release:       catalog.Release,
		SHA256:        hex.EncodeToString(digest[:]),
	}, nil
}

func (identity CatalogIdentity) validate() error {
	if identity.SchemaURI == "" || identity.SchemaVersion < 1 || identity.Release == "" || len(identity.SHA256) != sha256.Size*2 {
		return errors.New("catalog identity is incomplete")
	}
	for _, character := range identity.SHA256 {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return errors.New("catalog identity digest is not lowercase hexadecimal")
		}
	}
	return nil
}
