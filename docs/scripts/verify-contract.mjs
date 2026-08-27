import { createHash } from "node:crypto";
import { readdir, readFile, realpath, stat } from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

import { runValidatorSelfTests } from "./validator-self-test.mjs";
import {
  artifactInventorySemanticErrors,
  authoredIdErrors,
  duplicateLogicalIdErrors,
  faultCatalogSemanticErrors,
  performanceCatalogSemanticErrors,
  vectorCatalogSemanticErrors,
} from "./validators/catalogs.mjs";
import {
  ciSummarySemanticErrors,
  requiredGateVariableNames,
  validCISummaryFixture,
} from "./validators/ci-summary.mjs";
import { parseMakeTargets } from "./validators/make-targets.mjs";
import { markdownAnchors } from "./validators/markdown.mjs";
import { parseJsonStrict } from "./validators/strict-json.mjs";
import { supportPolicyErrors } from "./validators/support-policy.mjs";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");
const conformanceDir = resolve(repoRoot, "conformance");
const failures = [];

const schemaFiles = {
  requirements: "requirements-v2.schema.json",
  supportMatrix: "support-matrix.schema.json",
  scenario: "scenario-v2.schema.json",
  ciSummary: "ci-summary-v1.schema.json",
  rcCandidateLock: "rc-candidate-lock-v1.schema.json",
  rcManifest: "rc-manifest-v2.schema.json",
  faultCatalog: "fault-catalog-v1.schema.json",
  artifactInventory: "artifact-inventory-v1.schema.json",
  performanceBudgets: "performance-budgets-v2.schema.json",
  vectorCatalog: "vector-catalog-v1.schema.json",
};

function fail(message) {
  failures.push(message);
}

function formatAjvErrors(errors = []) {
  return errors
    .map(
      (error) =>
        `${error.instancePath || "/"}: ${error.message} (${error.schemaPath})`,
    )
    .join("\n    ");
}

async function readJson(path, label) {
  try {
    return parseJsonStrict(await readFile(path));
  } catch (error) {
    fail(`${label} could not be parsed: ${error.message}`);
    return null;
  }
}

function validateInstance(validator, value, label) {
  if (!validator) {
    fail(`${label} could not be validated because its schema did not compile`);
    return false;
  }
  if (validator(value)) return true;
  fail(`${label} is invalid:\n    ${formatAjvErrors(validator.errors)}`);
  return false;
}

function expectInvalid(validator, value, label, matches) {
  if (!validator) {
    fail(`${label} could not run because its schema did not compile`);
    return;
  }
  if (!validator(value)) {
    if (!matches || matches(validator.errors ?? [])) return;
    fail(`${label} failed for the wrong reason:\n    ${formatAjvErrors(validator.errors)}`);
    return;
  }
  fail(`${label} unexpectedly passed`);
}

function recordErrors(errors, label) {
  for (const error of errors) fail(`${label}: ${error}`);
}

function pathIsInside(root, candidate) {
  const value = relative(root, candidate);
  return value === "" || (!value.startsWith(`..${sep}`) && value !== ".." && !isAbsolute(value));
}

async function regularRepositoryFile(path, label) {
  const absolute = resolve(repoRoot, path);
  if (!pathIsInside(repoRoot, absolute)) {
    fail(`${label} escapes the repository`);
    return null;
  }
  try {
    const canonicalRoot = await realpath(repoRoot);
    const canonical = await realpath(absolute);
    if (!pathIsInside(canonicalRoot, canonical)) {
      fail(`${label} resolves outside the repository`);
      return null;
    }
    if (!(await stat(canonical)).isFile()) {
      fail(`${label} is not a regular file`);
      return null;
    }
    return canonical;
  } catch (error) {
    fail(`${label} is unavailable: ${error.message}`);
    return null;
  }
}

async function normativeReferenceErrors(references, label) {
  const errors = [];
  for (const reference of references) {
    const path = await regularRepositoryFile(reference.path, `${label} reference`);
    if (!path) continue;
    const anchors = markdownAnchors(await readFile(path, "utf8"));
    const anchor = reference.anchor.slice(1);
    if (!anchors.has(anchor)) {
      errors.push(`${label} reference ${reference.path}${reference.anchor} does not resolve`);
    }
  }
  return errors;
}

async function jsonFilesBelow(directory) {
  const result = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = resolve(directory, entry.name);
    if (entry.isDirectory()) result.push(...(await jsonFilesBelow(path)));
    else if (entry.isFile() && entry.name.endsWith(".json")) result.push(path);
  }
  return result.sort();
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function scenarioClosureErrors(scenarios, requirements, supportMatrix, makeTargets) {
  const errors = [];
  const requirementById = new Map(
    requirements.requirements.map((requirement) => [requirement.id, requirement]),
  );
  const supportIds = new Set(supportMatrix.cells.map(({ id }) => id));
  const scenarioIds = new Set();

  for (const scenario of scenarios) {
    if (scenarioIds.has(scenario.id)) errors.push(`Scenario ID ${scenario.id} is duplicated`);
    scenarioIds.add(scenario.id);
    const assertions = new Map(scenario.assertions.map((item) => [item.id, item]));
    const obligations = new Map(
      scenario.proof_obligations.map((item) => [item.obligation_id, item]),
    );
    const ownershipTuples = new Set();
    for (const obligation of scenario.proof_obligations) {
      if (!makeTargets.has(obligation.make_target)) {
        errors.push(`${scenario.id} references missing Make target ${obligation.make_target}`);
      }
      if (
        obligation.argv.length !== 2 ||
        obligation.argv[0] !== "make" ||
        obligation.argv[1] !== obligation.make_target
      ) {
        errors.push(`${obligation.obligation_id} has a noncanonical Make command`);
      }
      if (obligation.support_cell_id !== null && !supportIds.has(obligation.support_cell_id)) {
        errors.push(`${obligation.obligation_id} references unknown support cell`);
      }
      for (const requirementId of obligation.requirement_ids) {
        if (!requirementById.has(requirementId)) {
          errors.push(`${obligation.obligation_id} references unknown requirement ${requirementId}`);
        }
      }
      for (const assertionId of obligation.assertion_ids) {
        if (!assertions.has(assertionId)) {
          errors.push(`${obligation.obligation_id} references unknown assertion ${assertionId}`);
        }
      }
    }
    for (const owner of scenario.ownership) {
      const obligation = obligations.get(owner.proof_obligation_id);
      const tuple = [
        owner.requirement_id,
        owner.scenario_id,
        owner.proof_obligation_id,
        owner.assertion_id,
        owner.support_cell_id ?? "",
      ].join("\u0000");
      if (ownershipTuples.has(tuple)) {
        errors.push(`${scenario.id} repeats an ownership tuple`);
      }
      ownershipTuples.add(tuple);
      if (!obligation || owner.scenario_id !== scenario.id) {
        errors.push(`${scenario.id} has ownership outside its obligation set`);
        continue;
      }
      if (
        owner.proof_type !== obligation.proof_type ||
        owner.support_cell_id !== obligation.support_cell_id ||
        !obligation.requirement_ids.includes(owner.requirement_id) ||
        !obligation.assertion_ids.includes(owner.assertion_id)
      ) {
        errors.push(`${scenario.id} has ownership that differs from its obligation`);
      }
    }
  }
  return errors;
}

function checkForbiddenOutcomeKeys(value, label, path = "$") {
  if (Array.isArray(value)) {
    value.forEach((item, index) => checkForbiddenOutcomeKeys(item, label, `${path}[${index}]`));
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [key, member] of Object.entries(value)) {
    const normalized = key.toLowerCase().replace(/_/g, "-");
    if (["covered", "certified", "accepted-flaky", "waived"].includes(normalized)) {
      fail(`${label} contains forbidden readiness key ${path}.${key}`);
    }
    checkForbiddenOutcomeKeys(member, label, `${path}.${key}`);
  }
}

async function loadSchemas(ajv) {
  const schemas = {};
  for (const [name, fileName] of Object.entries(schemaFiles)) {
    const schema = await readJson(
      resolve(conformanceDir, "schemas", fileName),
      `Schema ${fileName}`,
    );
    if (!schema) continue;
    if (!ajv.validateSchema(schema)) {
      fail(`Schema ${fileName} is invalid:\n    ${formatAjvErrors(ajv.errors)}`);
      continue;
    }
    schemas[name] = schema;
    try {
      ajv.addSchema(schema);
    } catch (error) {
      fail(`Schema ${fileName} could not be registered: ${error.message}`);
    }
  }
  const validators = {};
  for (const [name, schema] of Object.entries(schemas)) {
    try {
      validators[name] = ajv.getSchema(schema.$id) ?? ajv.compile(schema);
    } catch (error) {
      fail(`Schema ${schemaFiles[name]} could not compile: ${error.message}`);
    }
  }
  return { schemas, validators };
}

async function loadAuthoredInputs(validators) {
  const files = {
    requirements: "conformance/requirements.json",
    supportMatrix: "conformance/support-matrix.json",
    scenarioCatalog: "conformance/catalog.json",
    vectorCatalog: "conformance/vectors/catalog.json",
    faultCatalog: "conformance/faults/catalog.json",
    performanceBudgets: "conformance/performance/budgets.json",
    artifactInventory: "conformance/artifacts/inventory.json",
  };
  const values = {};
  for (const [name, path] of Object.entries(files)) {
    values[name] = await readJson(resolve(repoRoot, path), path);
    if (values[name]) checkForbiddenOutcomeKeys(values[name], path);
  }
  for (const name of [
    "requirements",
    "supportMatrix",
    "vectorCatalog",
    "faultCatalog",
    "performanceBudgets",
    "artifactInventory",
  ]) {
    if (values[name]) validateInstance(validators[name], values[name], files[name]);
  }
  return values;
}

async function loadScenarios(catalog, validator) {
  if (!catalog || !Array.isArray(catalog.scenarios)) {
    fail("Scenario catalog does not contain a scenarios array");
    return [];
  }
  recordErrors(
    duplicateLogicalIdErrors(catalog.scenarios, "scenario_id", "Scenario catalog"),
    "Scenario catalog",
  );
  recordErrors(
    duplicateLogicalIdErrors(catalog.scenarios, "path", "Scenario catalog paths"),
    "Scenario catalog",
  );
  const listedPaths = new Set();
  const scenarios = [];
  for (const reference of catalog.scenarios) {
    const path = await regularRepositoryFile(reference.path, reference.scenario_id);
    if (!path) continue;
    listedPaths.add(path);
    const bytes = await readFile(path);
    if (sha256(bytes) !== reference.sha256) {
      fail(`${reference.scenario_id} catalog hash does not match ${reference.path}`);
    }
    let scenario;
    try {
      scenario = parseJsonStrict(bytes);
    } catch (error) {
      fail(`${reference.path} could not be parsed: ${error.message}`);
      continue;
    }
    if (scenario.id !== reference.scenario_id) {
      fail(`${reference.path} contains scenario ${scenario.id}, not ${reference.scenario_id}`);
    }
    validateInstance(validator, scenario, reference.path);
    checkForbiddenOutcomeKeys(scenario, reference.path);
    scenarios.push(scenario);
  }
  const actualPaths = await jsonFilesBelow(resolve(conformanceDir, "scenarios"));
  for (const path of actualPaths) {
    if (!listedPaths.has(path)) fail(`Scenario file ${relative(repoRoot, path)} is absent from the catalog`);
  }
  for (const path of listedPaths) {
    if (!actualPaths.includes(path)) fail(`Scenario catalog path ${relative(repoRoot, path)} is outside the scenario corpus`);
  }
  return scenarios;
}

function runCISummaryControls(validator) {
  const valid = validCISummaryFixture();
  if (validateInstance(validator, valid, "Valid generated CI summary self-test")) {
    recordErrors(ciSummarySemanticErrors(valid), "Valid generated CI summary self-test");
  }
  const missingVariable = structuredClone(valid);
  missingVariable.gate_variables.pop();
  expectInvalid(
    validator,
    missingVariable,
    "CI summary missing gate variable control",
    (errors) => errors.some((error) => error.instancePath === "/gate_variables"),
  );
  const wrongHome = structuredClone(valid);
  wrongHome.coverage[0].proof_home = "real-integration";
  expectInvalid(
    validator,
    wrongHome,
    "CI summary wrong proof home control",
    (errors) => errors.some((error) => error.instancePath.endsWith("/proof_home")),
  );
  const duplicateHome = structuredClone(valid);
  duplicateHome.coverage.push({
    ...duplicateHome.coverage[0],
    coverage_id: "COV-FEDCBA9876543210",
  });
  if (validator(duplicateHome)) {
    const errors = ciSummarySemanticErrors(duplicateHome);
    if (!errors.some((error) => error.includes("repeats ownership tuple"))) {
      fail("CI summary duplicate proof-home control unexpectedly passed");
    }
  }
}

async function main() {
  runValidatorSelfTests();
  const ajv = new Ajv2020({ allErrors: true, strict: false, validateSchema: true });
  addFormats(ajv, { formats: ["date-time", "uri"], mode: "full" });
  const { schemas, validators } = await loadSchemas(ajv);
  const values = await loadAuthoredInputs(validators);

  if (values.requirements && values.supportMatrix) {
    recordErrors(
      authoredIdErrors(values.requirements, values.supportMatrix),
      "Authored IDs",
    );
    recordErrors(
      supportPolicyErrors(values.requirements, values.supportMatrix),
      "Support policy",
    );
    for (const requirement of values.requirements.requirements) {
      recordErrors(
        await normativeReferenceErrors(
          requirement.normative_references,
          requirement.id,
        ),
        "Normative references",
      );
    }
  }
  if (values.vectorCatalog) {
    recordErrors(vectorCatalogSemanticErrors(values.vectorCatalog), "Vector catalog");
  }
  if (values.artifactInventory) {
    recordErrors(
      artifactInventorySemanticErrors(values.artifactInventory),
      "Artifact inventory",
    );
  }
  if (values.faultCatalog && values.requirements) {
    recordErrors(
      faultCatalogSemanticErrors(values.faultCatalog, values.requirements),
      "Fault catalog",
    );
  }
  if (values.performanceBudgets && values.supportMatrix && values.artifactInventory) {
    recordErrors(
      performanceCatalogSemanticErrors(
        values.performanceBudgets,
        values.supportMatrix,
        values.artifactInventory,
      ),
      "Performance catalog",
    );
  }

  const makeTargets = parseMakeTargets(await readFile(resolve(repoRoot, "Makefile"), "utf8"));
  const scenarios = await loadScenarios(values.scenarioCatalog, validators.scenario);
  if (values.requirements && values.supportMatrix) {
    recordErrors(
      scenarioClosureErrors(
        scenarios,
        values.requirements,
        values.supportMatrix,
        makeTargets,
      ),
      "Scenario closure",
    );
  }
  for (const scenario of scenarios) {
    recordErrors(
      await normativeReferenceErrors(scenario.normative_references, scenario.id),
      "Scenario normative references",
    );
  }

  runCISummaryControls(validators.ciSummary);
  const schemaGateVariables =
    schemas.ciSummary?.$defs?.gateVariable?.properties?.name?.enum ?? [];
  if (
    JSON.stringify(schemaGateVariables) !==
    JSON.stringify(requiredGateVariableNames)
  ) {
    fail("CI summary schema gate-variable allowlist differs from semantic validation");
  }
  if (schemas.rcManifest?.properties?.evidence !== undefined) {
    fail("RC manifest retains the legacy evidence property");
  }
  if (schemas.rcManifest?.properties?.ci_summary === undefined) {
    fail("RC manifest does not bind one CI summary");
  }
  if (failures.length > 0) {
    console.error(`Contract verification failed with ${failures.length} error(s):`);
    failures.forEach((message) => console.error(`  - ${message}`));
    process.exitCode = 1;
    return;
  }
  console.log(
    "Contract verification passed: schemas, authored inputs, references, support policy, scenarios, CI summary, and proof homes.",
  );
}

await main();
