package vectors

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
)

func TestFrozenVectors(t *testing.T) {
	catalog, err := Load(context.Background(), "../..")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	ids := catalog.IDs()
	if len(ids) != 1 || !catalog.Has(ids[0]) {
		t.Fatalf("loaded vector-set IDs = %v", ids)
	}
	set, ok := catalog.Set(ids[0])
	if !ok {
		t.Fatal("Catalog.Set() did not return the loaded set")
	}
	if len(set.Vectors) != 123 {
		t.Fatalf("loaded vector count = %d, want 123", len(set.Vectors))
	}
	for _, vector := range set.Vectors {
		vector := vector
		t.Run(vector.ID, func(t *testing.T) {
			preimage, digest, err := executeVector(vector)
			if !vector.Valid {
				if err == nil {
					t.Fatal("invalid vector unexpectedly succeeded")
				}
				return
			}
			if err != nil {
				t.Fatalf("valid vector failed: %v", err)
			}
			expected, err := decodeLowerHex(*vector.Expected.CanonicalBytesHex)
			if err != nil {
				t.Fatalf("decode authored canonical bytes: %v", err)
			}
			if !bytes.Equal(preimage, expected) {
				t.Errorf("canonical bytes differ\n got: %x\nwant: %x", preimage, expected)
			}
			preimageHash := sha256.Sum256(preimage)
			if got := hex.EncodeToString(preimageHash[:]); got != *vector.Expected.ExpectedBytesSHA256 {
				t.Errorf("canonical bytes SHA-256 = %s, want %s", got, *vector.Expected.ExpectedBytesSHA256)
			}
			if vector.Expected.ExpectedSHA256 != nil {
				if digest == nil {
					t.Fatal("digest vector returned no digest")
				}
				if got := hex.EncodeToString(digest[:]); got != *vector.Expected.ExpectedSHA256 {
					t.Errorf("digest = %s, want %s", got, *vector.Expected.ExpectedSHA256)
				}
			} else if digest != nil {
				t.Fatal("non-digest vector returned a digest")
			}
		})
	}
}

func executeVector(vector Vector) ([]byte, *[32]byte, error) {
	input, err := strictJSONObject(vector.Input, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return nil, nil, err
	}
	switch vector.Kind {
	case "typed_value":
		var spec FieldSpec
		if err := json.Unmarshal(input["field_spec"], &spec); err != nil {
			return nil, nil, err
		}
		rawText, err := decodeJSONString(input["raw_json"], true)
		if err != nil {
			return nil, nil, err
		}
		encoded, err := EncodeTypedValue(spec, json.RawMessage(rawText))
		return encoded, nil, err
	case "schema_manifest":
		manifest, err := parseInputManifest(input)
		if err != nil {
			return nil, nil, err
		}
		preimage, err := ManifestPreimage(manifest)
		digest := manifest.Hash()
		return preimage, &digest, err
	case "row_identity":
		manifest, err := parseInputManifest(input)
		if err != nil {
			return nil, nil, err
		}
		tableID, err := decodeRequiredString(input["table_id"], "table_id")
		if err != nil {
			return nil, nil, err
		}
		pk, err := inputJSONText(input["pk_json"])
		if err != nil {
			return nil, nil, err
		}
		identity, err := RowIdentity(manifest, tableID, pk)
		return identity, nil, err
	case "row_digest":
		manifest, err := parseInputManifest(input)
		if err != nil {
			return nil, nil, err
		}
		tableID, err := decodeRequiredString(input["table_id"], "table_id")
		if err != nil {
			return nil, nil, err
		}
		pk, err := inputJSONText(input["pk_json"])
		if err != nil {
			return nil, nil, err
		}
		rowText, err := decodeJSONString(input["row_json"], true)
		if err != nil {
			return nil, nil, err
		}
		row, err := parseRow(json.RawMessage(rowText), pk)
		if err != nil {
			return nil, nil, err
		}
		serverVersion, err := decodeJSONString(input["server_version"], true)
		if err != nil {
			return nil, nil, err
		}
		preimage, err := RowDigestPreimage(manifest, tableID, row, serverVersion)
		if err != nil {
			return nil, nil, err
		}
		digest, err := RowDigest(manifest, tableID, row, serverVersion)
		return preimage, &digest, err
	case "scope_digest":
		hashText, err := decodeRequiredString(input["schema_hash"], "schema_hash")
		if err != nil {
			return nil, nil, err
		}
		manifestHash, err := decodeLowerSHA256(hashText)
		if err != nil {
			return nil, nil, err
		}
		scopeID, err := decodeJSONString(input["scope_id"], true)
		if err != nil {
			return nil, nil, err
		}
		entries, err := parseDigestEntries(input["entries"])
		if err != nil {
			return nil, nil, err
		}
		preimage, err := ScopeDigestPreimage(manifestHash, scopeID, entries)
		if err != nil {
			return nil, nil, err
		}
		digest, err := ScopeDigest(manifestHash, scopeID, entries)
		return preimage, &digest, err
	case "mutation_fingerprint":
		authenticatedUserID, err := decodeRequiredString(input["authenticated_user_id"], "authenticated_user_id")
		if err != nil {
			return nil, nil, err
		}
		clientID, err := decodeRequiredString(input["client_id"], "client_id")
		if err != nil {
			return nil, nil, err
		}
		mutationJSON, err := inputJSONText(input["mutation_json"])
		if err != nil {
			return nil, nil, err
		}
		mutation, err := ParseNormalizedMutation(authenticatedUserID, clientID, mutationJSON)
		if err != nil {
			return nil, nil, err
		}
		preimage, err := MutationFingerprintPreimage(mutation)
		if err != nil {
			return nil, nil, err
		}
		digest, err := MutationFingerprint(mutation)
		return preimage, &digest, err
	case "batch_fingerprint":
		authenticatedUserID, err := decodeRequiredString(input["authenticated_user_id"], "authenticated_user_id")
		if err != nil {
			return nil, nil, err
		}
		batchJSON, err := inputJSONText(input["batch_json"])
		if err != nil {
			return nil, nil, err
		}
		batch, err := ParseNormalizedBatch(authenticatedUserID, batchJSON)
		if err != nil {
			return nil, nil, err
		}
		preimage, err := BatchFingerprintPreimage(batch)
		if err != nil {
			return nil, nil, err
		}
		digest, err := BatchFingerprint(batch)
		return preimage, &digest, err
	default:
		return nil, nil, fmt.Errorf("unsupported vector kind %q", vector.Kind)
	}
}

func parseInputManifest(input map[string]json.RawMessage) (Manifest, error) {
	manifestJSON, err := inputJSONText(input["manifest_json"])
	if err != nil {
		return Manifest{}, err
	}
	return ParseManifest(manifestJSON)
}

func inputJSONText(raw json.RawMessage) (json.RawMessage, error) {
	value, err := decodeJSONString(raw, true)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(value), nil
}

func parseRow(raw, pk json.RawMessage) (Row, error) {
	object, err := strictJSONObject(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return Row{}, err
	}
	fields := make([]RowField, 0, len(object))
	for fieldID, value := range object {
		fields = append(fields, RowField{FieldID: fieldID, Value: append(json.RawMessage(nil), value...)})
	}
	return Row{PK: append(json.RawMessage(nil), pk...), Fields: fields}, nil
}

func parseDigestEntries(raw json.RawMessage) ([]DigestEntry, error) {
	values, err := decodeJSONArray(raw, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		return nil, err
	}
	entries := make([]DigestEntry, 0, len(values))
	for _, value := range values {
		object, err := strictJSONObject(value, jsonValidation{iJSON: true, safeInteger: true})
		if err != nil {
			return nil, err
		}
		identityText, err := decodeRequiredString(object["row_identity_hex"], "row_identity_hex")
		if err != nil {
			return nil, err
		}
		identity, err := decodeLowerHex(identityText)
		if err != nil {
			return nil, err
		}
		digestText, err := decodeRequiredString(object["row_digest_hex"], "row_digest_hex")
		if err != nil {
			return nil, err
		}
		digest, err := decodeLowerSHA256(digestText)
		if err != nil {
			return nil, err
		}
		entries = append(entries, DigestEntry{RowIdentity: identity, RowDigest: digest})
	}
	return entries, nil
}
