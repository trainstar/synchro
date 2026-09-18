//go:build releasecontract

package releaseversion

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestNextVersionPassesSupportPolicyAndRequirementsSchema(t *testing.T) {
	repoRoot, err := FindRepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	root := newFixtureRepo(t)
	const schemaPath = "conformance/schemas/requirements-v2.schema.json"
	for _, path := range []string{"conformance/requirements.json", "conformance/support-matrix.json", schemaPath} {
		data, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatal(err)
		}
		writeFixtureFile(t, root, path, string(data))
	}
	if err := Set(root, "0.4.0"); err != nil {
		t.Fatal(err)
	}
	if err := Check(root, "v0.4.0"); err != nil {
		t.Fatal(err)
	}
	script := `
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import Ajv2020 from "ajv/dist/2020.js";
import { supportPolicyErrors } from "./scripts/validators/support-policy.mjs";

const root = process.argv[1];
const read = (file) => JSON.parse(fs.readFileSync(path.join(root, file), "utf8"));
const release = fs.readFileSync(path.join(root, "VERSION"), "utf8").trim();
const requirements = read("conformance/requirements.json");
const support = read("conformance/support-matrix.json");
const schema = read("conformance/schemas/requirements-v2.schema.json");
const ajv = new Ajv2020({ allErrors: true, strict: false, validateSchema: true });
const validate = ajv.compile(schema);
assert.equal(validate(requirements), true, JSON.stringify(validate.errors));
assert.deepEqual(supportPolicyErrors(requirements, support, release), []);

const staleRequirements = { ...requirements, release: "0.3.0" };
assert.equal(validate(staleRequirements), false);
assert(validate.errors.some((error) => error.instancePath === "/release" && error.keyword === "const"));
assert(supportPolicyErrors(staleRequirements, support, release).length > 0);
assert(supportPolicyErrors(requirements, { ...support, release: "0.3.0" }, release).length > 0);

for (const file of ["requirements.json", "support-matrix.json"]) {
  const original = JSON.parse(fs.readFileSync(path.join("..", "conformance", file), "utf8"));
  const updated = read(path.join("conformance", file));
  original.release = updated.release;
  assert.deepEqual(updated, original);
}
const originalSchema = JSON.parse(fs.readFileSync("../conformance/schemas/requirements-v2.schema.json", "utf8"));
originalSchema.properties.release.const = release;
assert.deepEqual(schema, originalSchema);
`
	command := exec.Command("node", "--input-type=module", "-e", script, root)
	command.Dir = filepath.Join(repoRoot, "docs")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("next-version support and schema validation failed: %v\n%s", err, output)
	}

	data, err := os.ReadFile(filepath.Join(root, schemaPath))
	if err != nil {
		t.Fatal(err)
	}
	stale := strings.Replace(string(data), `"release": { "const": "0.4.0" }`, `"release": { "const": "0.3.0" }`, 1)
	writeFixtureFile(t, root, schemaPath, stale)
	if err := Check(root, ""); err == nil || !strings.Contains(err.Error(), schemaPath) {
		t.Fatalf("Check did not identify schema release drift: %v", err)
	}
	if err := Sync(root); err != nil {
		t.Fatal(err)
	}
	if err := Check(root, ""); err != nil {
		t.Fatal(err)
	}
}
