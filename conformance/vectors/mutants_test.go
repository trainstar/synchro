package vectors

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestMutantsFailAuthoredVectors(t *testing.T) {
	set := loadFrozenVectorSet(t)
	t.Run("omit typed-value tags", func(t *testing.T) {
		vector := firstValidVector(t, set, "typed_value")
		preimage := requireAuthoredPreimage(t, vector)
		if len(preimage) < 2 {
			t.Fatal("typed value is too short")
		}
		assertMutantDiffers(t, vector, preimage[1:])
	})
	t.Run("unframed row identity strings collide", func(t *testing.T) {
		left := vectorByID(t, set, "VEC-ROW-IDENTITY-LENGTH-COLLISION-A-001")
		right := vectorByID(t, set, "VEC-ROW-IDENTITY-LENGTH-COLLISION-B-001")
		leftMutant := mutantUnframedRowIdentity(t, left)
		rightMutant := mutantUnframedRowIdentity(t, right)
		if !bytes.Equal(leftMutant, rightMutant) {
			t.Fatal("unframed row identities did not collide")
		}
		assertMutantDiffers(t, left, leftMutant)
		assertMutantDiffers(t, right, rightMutant)
	})
	t.Run("preserve source scope-row order", func(t *testing.T) {
		vector := validUnorderedScopeVector(t, set)
		input := scopeInput(t, vector)
		mutant := mutantScopePreimage(input, false, true, true)
		assertMutantDiffers(t, vector, mutant)
	})
	t.Run("omit scope cardinality", func(t *testing.T) {
		vector := validUnorderedScopeVector(t, set)
		input := scopeInput(t, vector)
		assertMutantDiffers(t, vector, mutantScopePreimage(input, true, false, true))
	})
	t.Run("omit scope ID", func(t *testing.T) {
		vector := validUnorderedScopeVector(t, set)
		input := scopeInput(t, vector)
		assertMutantDiffers(t, vector, mutantScopePreimage(input, true, true, false))
	})
	t.Run("skip RFC 8785 canonicalization", func(t *testing.T) {
		vector := vectorByID(t, set, "VEC-MUTATION-ALL-PORTABLE-VALUES-001")
		assertMutantDiffers(t, vector, mutantMutationPreimageWithoutJCS(t, vector))
	})
	t.Run("replace SHA-256 with a constant", func(t *testing.T) {
		vector := firstDigestVector(t, set)
		preimage := requireAuthoredPreimage(t, vector)
		if vector.Expected.ExpectedSHA256 == nil {
			t.Fatal("digest vector omitted expected digest")
		}
		constant := mutantConstantSHA256(preimage)
		if bytes.Equal(constant[:], mustDecodeLowerHex(t, *vector.Expected.ExpectedSHA256)) {
			t.Fatal("authored digest equals the constant mutant")
		}
	})
}

func mutantConstantSHA256([]byte) [32]byte { return [32]byte{} }

func loadFrozenVectorSet(t *testing.T) VectorSet {
	t.Helper()
	catalog, err := Load(context.Background(), "../..")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	set, ok := catalog.Set("VSET-CANONICAL-001")
	if !ok {
		t.Fatal("frozen vector set is missing")
	}
	return set
}

func vectorByID(t *testing.T, set VectorSet, id string) Vector {
	t.Helper()
	for _, vector := range set.Vectors {
		if vector.ID == id {
			return vector
		}
	}
	t.Fatalf("vector %q is missing", id)
	return Vector{}
}

func firstValidVector(t *testing.T, set VectorSet, kind string) Vector {
	t.Helper()
	for _, vector := range set.Vectors {
		if vector.Kind == kind && vector.Valid {
			return vector
		}
	}
	t.Fatalf("valid %s vector is missing", kind)
	return Vector{}
}

func firstDigestVector(t *testing.T, set VectorSet) Vector {
	t.Helper()
	for _, vector := range set.Vectors {
		if vector.Valid && vector.Expected.ExpectedSHA256 != nil {
			return vector
		}
	}
	t.Fatal("valid digest vector is missing")
	return Vector{}
}

func requireAuthoredPreimage(t *testing.T, vector Vector) []byte {
	t.Helper()
	preimage, _, err := executeVector(vector)
	if err != nil {
		t.Fatalf("execute %s: %v", vector.ID, err)
	}
	expected := expectedPreimage(t, vector)
	if !bytes.Equal(preimage, expected) {
		t.Fatalf("%s did not produce its authored expected result", vector.ID)
	}
	return preimage
}

func assertMutantDiffers(t *testing.T, vector Vector, mutant []byte) {
	t.Helper()
	requireAuthoredPreimage(t, vector)
	if bytes.Equal(mutant, expectedPreimage(t, vector)) {
		t.Fatalf("mutant unexpectedly produced %s's authored expected result", vector.ID)
	}
}

func expectedPreimage(t *testing.T, vector Vector) []byte {
	t.Helper()
	if vector.Expected.CanonicalBytesHex == nil {
		t.Fatalf("%s has no authored expected bytes", vector.ID)
	}
	return mustDecodeLowerHex(t, *vector.Expected.CanonicalBytesHex)
}

func mustDecodeLowerHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := decodeLowerHex(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func mutantUnframedRowIdentity(t *testing.T, vector Vector) []byte {
	t.Helper()
	input, err := strictJSONObject(vector.Input, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := parseInputManifest(input)
	if err != nil {
		t.Fatal(err)
	}
	tableID, err := decodeRequiredString(input["table_id"], "table_id")
	if err != nil {
		t.Fatal(err)
	}
	pk, err := inputJSONText(input["pk_json"])
	if err != nil {
		t.Fatal(err)
	}
	table, err := manifestTableByID(manifest, tableID)
	if err != nil {
		t.Fatal(err)
	}
	field, err := manifestFieldByID(table, table.PrimaryKeyFieldID)
	if err != nil {
		t.Fatal(err)
	}
	encodedPK, err := EncodeTypedValue(field.Spec, pk)
	if err != nil {
		t.Fatal(err)
	}
	mutant := append([]byte(nil), rowIdentityDomain...)
	mutant = append(mutant, table.TableID...)
	mutant = append(mutant, table.PrimaryKeyFieldID...)
	return append(mutant, encodedPK...)
}

type mutantScopeInput struct {
	manifestHash [32]byte
	scopeID      string
	entries      []DigestEntry
}

func validUnorderedScopeVector(t *testing.T, set VectorSet) Vector {
	t.Helper()
	for _, vector := range set.Vectors {
		if !vector.Valid || vector.Kind != "scope_digest" {
			continue
		}
		entries := scopeInput(t, vector).entries
		for index := 1; index < len(entries); index++ {
			if bytes.Compare(entries[index-1].RowIdentity, entries[index].RowIdentity) > 0 {
				return vector
			}
		}
	}
	t.Fatal("an authored scope vector with unordered source rows is missing")
	return Vector{}
}

func scopeInput(t *testing.T, vector Vector) mutantScopeInput {
	t.Helper()
	input, err := strictJSONObject(vector.Input, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		t.Fatal(err)
	}
	hashText, err := decodeRequiredString(input["schema_hash"], "schema_hash")
	if err != nil {
		t.Fatal(err)
	}
	manifestHash, err := decodeLowerSHA256(hashText)
	if err != nil {
		t.Fatal(err)
	}
	scopeID, err := decodeJSONString(input["scope_id"], true)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := parseDigestEntries(input["entries"])
	if err != nil {
		t.Fatal(err)
	}
	return mutantScopeInput{manifestHash: manifestHash, scopeID: scopeID, entries: entries}
}

func mutantScopePreimage(input mutantScopeInput, sortRows, includeCount, includeScopeID bool) []byte {
	entries := append([]DigestEntry(nil), input.entries...)
	if sortRows {
		for left := 0; left < len(entries); left++ {
			for right := left + 1; right < len(entries); right++ {
				if bytes.Compare(entries[right].RowIdentity, entries[left].RowIdentity) < 0 {
					entries[left], entries[right] = entries[right], entries[left]
				}
			}
		}
	}
	preimage := append([]byte(nil), scopeDigestDomain...)
	preimage = append(preimage, input.manifestHash[:]...)
	if includeScopeID {
		preimage, _ = appendText(preimage, input.scopeID)
	}
	if includeCount {
		preimage = appendU64(preimage, uint64(len(entries)))
	}
	for _, entry := range entries {
		preimage = appendBlob(preimage, entry.RowIdentity)
		preimage = append(preimage, entry.RowDigest[:]...)
	}
	return preimage
}

func mutantMutationPreimageWithoutJCS(t *testing.T, vector Vector) []byte {
	t.Helper()
	input, err := strictJSONObject(vector.Input, jsonValidation{iJSON: true, safeInteger: true})
	if err != nil {
		t.Fatal(err)
	}
	userID, err := decodeRequiredString(input["authenticated_user_id"], "authenticated_user_id")
	if err != nil {
		t.Fatal(err)
	}
	clientID, err := decodeRequiredString(input["client_id"], "client_id")
	if err != nil {
		t.Fatal(err)
	}
	mutationJSON, err := inputJSONText(input["mutation_json"])
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := ParseNormalizedMutation(userID, clientID, mutationJSON)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := normalizedMutationValue(mutation)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		t.Fatal(err)
	}
	scope := []any{"mutation-scope-v1", userID, clientID, json.RawMessage(encoded)}
	encoded, err = json.Marshal(scope)
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]byte(nil), mutationFingerprintDomain...), encoded...)
}
