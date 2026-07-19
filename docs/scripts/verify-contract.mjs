import { readFile, realpath, stat } from "node:fs/promises";
import { createHash } from "node:crypto";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");
const conformanceDir = resolve(repoRoot, "conformance");
const failures = [];
let repositoryMakeTargets = new Set();

const schemaFiles = {
  requirements: "requirements-v2.schema.json",
  supportMatrix: "support-matrix.schema.json",
  scenario: "scenario-v2.schema.json",
  evidence: "evidence-v2.schema.json",
  rcManifest: "rc-manifest-v2.schema.json",
  faultCatalog: "fault-catalog-v1.schema.json",
  artifactInventory: "artifact-inventory-v1.schema.json",
  performanceBudgets: "performance-budgets-v2.schema.json",
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

function parseMakeTargets(source) {
  const targets = new Set();
  for (const line of source.split(/\r?\n/)) {
    const match = line.match(
      /^([A-Za-z0-9_.-]+(?:[ \t]+[A-Za-z0-9_.-]+)*):(?:[ \t]|$)/,
    );
    if (!match) continue;
    for (const target of match[1].split(/[ \t]+/)) targets.add(target);
  }
  return targets;
}

async function readJson(path, label) {
  try {
    return JSON.parse(await readFile(path, "utf8"));
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

function validateInstances(validator, values, label) {
  let valid = true;
  for (const [index, value] of values.entries()) {
    valid = validateInstance(validator, value, `${label}[${index}]`) && valid;
  }
  return valid;
}

function expectInvalid(validator, value, label, matchesExpectedError) {
  if (!validator) {
    fail(`${label} could not run because its schema did not compile`);
  } else if (validator(value)) {
    fail(`${label} unexpectedly passed validation`);
  } else if (
    matchesExpectedError &&
    !matchesExpectedError(validator.errors ?? [])
  ) {
    fail(
      `${label} was rejected for an unexpected reason:\n    ${formatAjvErrors(validator.errors)}`,
    );
  }
}

function recordSemanticErrors(errors, label) {
  for (const error of errors) fail(`${label}: ${error}`);
}

function expectSemanticValid(errors, label) {
  if (errors.length === 0) return;
  fail(`${label} failed semantic validation:\n    ${errors.join("\n    ")}`);
}

function expectSemanticInvalid(errors, label, matchesExpectedError) {
  if (errors.length === 0) {
    fail(`${label} unexpectedly passed semantic validation`);
  } else if (matchesExpectedError && !errors.some(matchesExpectedError)) {
    fail(
      `${label} was rejected for an unexpected semantic reason:\n    ${errors.join("\n    ")}`,
    );
  }
}

function duplicateLogicalIdErrors(items, idKey, collection) {
  const errors = [];
  const seen = new Map();
  for (const [index, item] of items.entries()) {
    const id = item[idKey];
    if (seen.has(id)) {
      errors.push(
        `${collection} contains duplicate logical ID ${JSON.stringify(id)} at indexes ${seen.get(id)} and ${index}`,
      );
    } else {
      seen.set(id, index);
    }
  }
  return errors;
}

function authoredIdErrors(requirements, supportMatrix) {
  const errors = [];
  const seen = new Map();
  const entries = [
    ...requirements.requirements.map(({ id }) => [id, "requirements"]),
    ...supportMatrix.cells.map(({ id }) => [id, "support matrix"]),
  ];

  for (const [id, collection] of entries) {
    if (seen.has(id)) {
      errors.push(
        `Duplicate authored ID ${JSON.stringify(id)} in ${seen.get(id)} and ${collection}`,
      );
    } else {
      seen.set(id, collection);
    }
  }
  return errors;
}

function artifactInventorySemanticErrors(inventory) {
  const errors = [
    ...duplicateLogicalIdErrors(inventory.artifacts, "id", "Artifact inventory"),
  ];
  const roles = new Map();
  for (const artifact of inventory.artifacts) {
    roles.set(artifact.role, (roles.get(artifact.role) ?? 0) + 1);
  }
  for (const role of [
    "pg-extension",
    "pg-install-sql",
    "adapter",
    "seed-tool",
    "swift-spm",
    "cocoapods",
    "kotlin-maven",
    "react-native-npm",
    "portable-seed",
  ]) {
    if ((roles.get(role) ?? 0) !== 1) {
      errors.push(`Artifact inventory requires exactly one ${role} role`);
    }
  }
  return errors;
}

function faultCatalogSemanticErrors(catalog, requirements) {
  const errors = [
    ...duplicateLogicalIdErrors(catalog.faults, "id", "Fault catalog faults"),
    ...duplicateLogicalIdErrors(catalog.controls, "id", "Fault catalog controls"),
  ];
  const faultIds = new Set(catalog.faults.map(({ id }) => id));
  const requirementById = new Map(
    requirements.requirements.map((requirement) => [requirement.id, requirement]),
  );
  const controlCountByRequirement = new Map();
  const usedFaultIds = new Set();
  for (const control of catalog.controls) {
    if (control.requirement_ids.length !== 1) {
      errors.push(
        `${control.id} must be owned by exactly one requirement, found ${control.requirement_ids.length}`,
      );
    }
    if (!faultIds.has(control.fault_id)) {
      errors.push(`${control.id} references unknown fault ${control.fault_id}`);
    } else {
      usedFaultIds.add(control.fault_id);
    }
    const expectedReferences = new Set();
    for (const requirementId of control.requirement_ids) {
      const requirement = requirementById.get(requirementId);
      if (!requirement) {
        errors.push(`${control.id} references unknown requirement ${requirementId}`);
        continue;
      }
      controlCountByRequirement.set(
        requirementId,
        (controlCountByRequirement.get(requirementId) ?? 0) + 1,
      );
      for (const reference of requirement.normative_references) {
        expectedReferences.add(`${reference.path}${reference.anchor}`);
      }
    }
    if (!stringSetsEqual(control.normative_references, [...expectedReferences])) {
      errors.push(
        `${control.id} normative references do not exactly match its requirements`,
      );
    }
  }
  for (const requirement of requirements.requirements) {
    const count = controlCountByRequirement.get(requirement.id) ?? 0;
    if (count !== 1) {
      errors.push(
        `${requirement.id} requires exactly one authored negative control, found ${count}`,
      );
    }
  }
  for (const faultId of faultIds) {
    if (!usedFaultIds.has(faultId)) {
      errors.push(`Fault catalog fault ${faultId} is not used by a control`);
    }
  }
  return errors;
}

function performanceCatalogSemanticErrors(
  catalog,
  supportMatrix,
  artifactInventory,
) {
  const errors = [
    ...duplicateLogicalIdErrors(catalog.budgets, "id", "Performance budgets"),
    ...duplicateLogicalIdErrors(
      catalog.required_measurements,
      "id",
      "Required performance measurements",
    ),
  ];
  const requiredSupportIds = new Set(
    supportMatrix.cells
      .filter(({ policy }) => policy === "required")
      .map(({ id }) => id),
  );
  const inventoryIds = new Set(
    artifactInventory.artifacts.map(({ id }) => id),
  );
  const lockedCatalogDigest =
    "ace9daa85f00ad7307ddc3244f37058555a15b8031ceeef3f0bc787af7158fb4";
  const actualCatalogDigest = createHash("sha256")
    .update(
      JSON.stringify({
        budgets: catalog.budgets,
        required_measurements: catalog.required_measurements,
      }),
    )
    .digest("hex");
  if (actualCatalogDigest !== lockedCatalogDigest) {
    errors.push(
      "Performance budgets and characterization measurements do not match the locked v0.3.0 semantic snapshot",
    );
  }
  const lockedBudgets = new Map([
    ["BUD-WARM-CONNECT-001", ["warm_connect_http_requests", "eq", 1]],
    ["BUD-WARM-CONNECT-NONCONNECT-001", ["warm_connect_non_connect_http_requests", "eq", 0]],
    ["BUD-STEADY-PULL-001", ["steady_state_pull_http_requests_per_cycle", "eq", 1]],
    ["BUD-STEADY-PULL-NONPULL-001", ["steady_state_pull_non_pull_http_requests_per_cycle", "eq", 0]],
    ["BUD-PENDING-PUSH-001", ["pending_cycle_push_http_requests", "eq", 1]],
    ["BUD-PENDING-PULL-001", ["pending_cycle_pull_http_requests", "eq", 1]],
    ["BUD-PENDING-CYCLE-UNEXPECTED-001", ["pending_cycle_non_push_or_pull_http_requests", "eq", 0]],
    ["BUD-REBUILD-CONNECT-001", ["rebuild_connect_http_requests", "eq", 1]],
    ["BUD-REBUILD-PULL-001", ["rebuild_pull_http_requests", "eq", 1]],
    ["BUD-REBUILD-PAGE-001", ["rebuild_page_request_count_minus_returned_page_count", "eq", 0]],
    ["BUD-REBUILD-SCHEMA-FETCH-001", ["rebuild_schema_fetch_http_requests", "eq", 0]],
    ["BUD-REBUILD-UNEXPECTED-001", ["rebuild_unexpected_http_requests", "eq", 0]],
    ["BUD-CORE-SYNC-RPC-001", ["core_sync_outbound_network_or_rpc_hops", "eq", 0]],
  ]);
  const lockedMeasurements = new Set([
    "MEAS-FANOUT-001",
    "MEAS-SHARED-PRIVATE-SCOPES-001",
    "MEAS-REBUILD-CARDINALITY-001",
    "MEAS-SCHEMA-CHECK-001",
    "MEAS-SEEDED-EMPTY-STARTUP-001",
    "MEAS-QUEUE-REPLAY-001",
    "MEAS-REBUILD-APPLY-001",
    "MEAS-MULTI-SCOPE-PROVENANCE-001",
    "MEAS-CONFIGURED-BOUNDS-001",
  ]);
  if (!stringSetsEqual(catalog.budgets.map(({ id }) => id), [...lockedBudgets.keys()])) {
    errors.push("Performance budget IDs do not match the locked v0.3.0 request budgets");
  }
  if (
    !stringSetsEqual(
      catalog.required_measurements.map(({ id }) => id),
      [...lockedMeasurements],
    )
  ) {
    errors.push(
      "Required measurement IDs do not match the locked v0.3.0 characterization set",
    );
  }
  for (const item of [...catalog.budgets, ...catalog.required_measurements]) {
    for (const supportCellId of item.support_cell_ids) {
      if (!requiredSupportIds.has(supportCellId)) {
        errors.push(`${item.id} references unknown or excluded support cell ${supportCellId}`);
      }
    }
    for (const inventoryId of item.artifact_inventory_ids) {
      if (!inventoryIds.has(inventoryId)) {
        errors.push(`${item.id} references unknown artifact inventory ${inventoryId}`);
      }
    }
  }
  for (const budget of catalog.budgets) {
    const locked = lockedBudgets.get(budget.id);
    if (
      locked &&
      (budget.metric !== locked[0] ||
        budget.comparator !== locked[1] ||
        budget.limit !== locked[2])
    ) {
      errors.push(`${budget.id} does not match its locked metric, comparator, and limit`);
    }
  }
  for (const measurement of catalog.required_measurements) {
    errors.push(
      ...duplicateLogicalIdErrors(
        measurement.metrics,
        "id",
        `${measurement.id} metrics`,
      ),
      ...duplicateLogicalIdErrors(
        measurement.strata,
        "stratum_id",
        `${measurement.id} strata`,
      ),
    );
  }
  return errors;
}

const forbiddenReadinessKeys = new Set([
  "covered",
  "partial",
  "passed",
  "ready",
  "certified",
  "waived",
  "accepted-flaky",
]);

function checkForbiddenKeys(value, label, path = "$") {
  if (Array.isArray(value)) {
    value.forEach((item, index) =>
      checkForbiddenKeys(item, label, `${path}[${index}]`),
    );
    return;
  }
  if (value === null || typeof value !== "object") return;

  for (const [key, child] of Object.entries(value)) {
    const normalized = key.toLowerCase().replace(/[_-]+/g, "-");
    if (forbiddenReadinessKeys.has(normalized)) {
      fail(`${label} contains forbidden readiness key ${path}.${key}`);
    }
    checkForbiddenKeys(child, label, `${path}.${key}`);
  }
}

function stripInlineMarkdown(value) {
  let text = value;
  text = text.replace(/!\[([^\]]*)\]\([^)]*\)/g, "$1");
  text = text.replace(/\[([^\]]+)\]\([^)]*\)/g, "$1");
  text = text.replace(/\[([^\]]+)\]\[[^\]]*\]/g, "$1");
  text = text.replace(/<[^>]+>/g, "");
  text = text.replace(/[`*_~]/g, "");
  text = text.replace(/\\([!"#$%&'()*+,./:;<=>?@[\]^_`{|}~-])/g, "$1");
  return text;
}

function githubSlug(value) {
  return stripInlineMarkdown(value)
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{M}\p{N}\p{Pc}\-\s]/gu, "")
    .replace(/\s+/g, "-");
}

function markdownAnchors(source) {
  const anchors = new Set();
  const slugCounts = new Map();
  let fence = null;

  for (const line of source.split(/\r?\n/)) {
    const fenceMatch = line.match(/^\s{0,3}(`{3,}|~{3,})/);
    if (fenceMatch) {
      const marker = fenceMatch[1][0];
      if (fence === marker) fence = null;
      else if (fence === null) fence = marker;
      continue;
    }
    if (fence !== null) continue;

    const heading = line.match(/^\s{0,3}#{1,6}[\t ]+(.+?)\s*$/);
    if (!heading) continue;
    const title = heading[1].replace(/[\t ]+#+[\t ]*$/, "");
    const base = githubSlug(title);
    let suffix = slugCounts.get(base) ?? 0;
    let slug = base;
    while (anchors.has(slug)) {
      suffix += 1;
      slug = `${base}-${suffix}`;
    }
    slugCounts.set(base, suffix);
    anchors.add(slug);
  }

  return anchors;
}

function markdownAnchorsAtLevel(source, targetLevel) {
  const selected = new Set();
  const used = new Set();
  const slugCounts = new Map();
  let fence = null;

  for (const line of source.split(/\r?\n/)) {
    const fenceMatch = line.match(/^\s{0,3}(`{3,}|~{3,})/);
    if (fenceMatch) {
      const marker = fenceMatch[1][0];
      if (fence === marker) fence = null;
      else if (fence === null) fence = marker;
      continue;
    }
    if (fence !== null) continue;

    const heading = line.match(/^\s{0,3}(#{1,6})[\t ]+(.+?)\s*$/);
    if (!heading) continue;
    const title = heading[2].replace(/[\t ]+#+[\t ]*$/, "");
    const base = githubSlug(title);
    let suffix = slugCounts.get(base) ?? 0;
    let slug = base;
    while (used.has(slug)) {
      suffix += 1;
      slug = `${base}-${suffix}`;
    }
    slugCounts.set(base, suffix);
    used.add(slug);
    if (heading[1].length === targetLevel) selected.add(slug);
  }

  return selected;
}

function pathIsInside(root, candidate) {
  const pathFromRoot = relative(root, candidate);
  return (
    pathFromRoot === "" ||
    (!isAbsolute(pathFromRoot) &&
      pathFromRoot !== ".." &&
      !pathFromRoot.startsWith(`..${sep}`))
  );
}

async function resolveNormativeReferenceErrors(references, ownerLabel) {
  const errors = [];
  const rootRealPath = await realpath(repoRoot);
  const anchorCache = new Map();

  for (const reference of references) {
    const context = `${ownerLabel} reference ${reference.path}${reference.anchor}`;
    if (isAbsolute(reference.path)) {
      errors.push(`${context} must use a repository-relative path`);
      continue;
    }

    const resolvedPath = resolve(repoRoot, reference.path);
    if (!pathIsInside(repoRoot, resolvedPath)) {
      errors.push(`${context} resolves outside the repository`);
      continue;
    }

    let fileInfo;
    let resolvedRealPath;
    try {
      [fileInfo, resolvedRealPath] = await Promise.all([
        stat(resolvedPath),
        realpath(resolvedPath),
      ]);
    } catch (error) {
      errors.push(
        `${context} does not resolve to an existing file: ${error.message}`,
      );
      continue;
    }
    if (!fileInfo.isFile()) {
      errors.push(`${context} does not resolve to a file`);
      continue;
    }
    if (!pathIsInside(rootRealPath, resolvedRealPath)) {
      errors.push(`${context} resolves through a link outside the repository`);
      continue;
    }

    if (!anchorCache.has(resolvedRealPath)) {
      const source = await readFile(resolvedRealPath, "utf8");
      anchorCache.set(resolvedRealPath, markdownAnchors(source));
    }
    const anchor = reference.anchor.slice(1);
    if (!anchorCache.get(resolvedRealPath).has(anchor)) {
      errors.push(`${context} references a missing heading anchor`);
    }
  }
  return errors;
}

async function normativeReferenceErrors(requirements) {
  const errors = [];
  for (const requirement of requirements.requirements) {
    errors.push(
      ...(await resolveNormativeReferenceErrors(
        requirement.normative_references,
        requirement.id,
      )),
    );
  }
  return errors;
}

async function scenarioNormativeReferenceErrors(scenario) {
  return resolveNormativeReferenceErrors(
    scenario.normative_references,
    scenario.id,
  );
}

function invariantCoverageErrors(requirements, invariantSource) {
  const errors = [];
  const invariantPath = "docs/src/content/docs/spec/04-invariants.mdx";
  const invariantAnchors = markdownAnchorsAtLevel(invariantSource, 3);
  const invariantAnchorSet = new Set(invariantAnchors);
  const mappings = new Map();

  for (const requirement of requirements.requirements) {
    const invariantReferences = requirement.normative_references.filter(
      ({ path }) => path === invariantPath,
    );
    if (invariantReferences.length !== 1) {
      errors.push(
        `${requirement.id} must reference exactly one level-three invariant, found ${invariantReferences.length}`,
      );
    } else if (!invariantAnchorSet.has(invariantReferences[0].anchor.slice(1))) {
      errors.push(
        `${requirement.id} references non-invariant heading ${invariantReferences[0].anchor}`,
      );
    }
    for (const reference of requirement.normative_references) {
      if (reference.path !== invariantPath) continue;
      const anchor = reference.anchor.slice(1);
      const requirementIds = mappings.get(anchor) ?? [];
      requirementIds.push(requirement.id);
      mappings.set(anchor, requirementIds);
    }
  }

  for (const anchor of invariantAnchors) {
    const requirementIds = mappings.get(anchor) ?? [];
    if (requirementIds.length === 0) {
      errors.push(`Invariant #${anchor} has no release requirement`);
    } else if (requirementIds.length > 1) {
      errors.push(
        `Invariant #${anchor} maps to multiple release requirements: ${requirementIds.join(", ")}`,
      );
    }
  }

  return errors;
}

function selectorKey(selector) {
  return selector ? `${selector.kind}:${selector.value ?? ""}` : "none";
}

function supportDimensionKey(cell) {
  return [
    cell.component,
    cell.platform,
    selectorKey(cell.platform_version),
    selectorKey(cell.runtime_version),
  ].join("|");
}

function supportPolicyKey(cell) {
  return `${supportDimensionKey(cell)}|${cell.policy}`;
}

function supportPolicyErrors(requirements, supportMatrix) {
  const errors = [];
  if (requirements.release !== "0.3.0") {
    errors.push(`Requirements release must be 0.3.0, found ${requirements.release}`);
  }
  if (supportMatrix.release !== "0.3.0") {
    errors.push(`Support matrix release must be 0.3.0, found ${supportMatrix.release}`);
  }

  const dimensions = new Map();
  for (const cell of supportMatrix.cells) {
    const key = supportDimensionKey(cell);
    if (dimensions.has(key)) {
      errors.push(
        `Duplicate semantic support cells ${dimensions.get(key)} and ${cell.id}`,
      );
    } else {
      dimensions.set(key, cell.id);
    }
    if (
      cell.component === "react-native-client" &&
      selectorKey(cell.runtime_version) !== "series:0.83.x"
    ) {
      errors.push(`${cell.id} must use React Native runtime series 0.83.x`);
    }
  }

  const expected = [];
  for (const version of ["14", "15", "16", "17", "18"]) {
    expected.push({
      component: "postgresql-server",
      platform: "postgresql",
      platform_version: { kind: "exact", value: version },
      policy: version === "18" ? "required" : "excluded",
    });
  }
  expected.push(
    ...[
      ["swift-client", "ios", "minimum", "16", null],
      ["swift-client", "ios", "current-stable", null, null],
      ["swift-client", "macos", "minimum", "13", null],
      ["swift-client", "macos", "current-stable", null, null],
      ["kotlin-client", "android", "minimum", "24", null],
      ["kotlin-client", "android", "current-stable", null, null],
      ["react-native-client", "ios", "minimum", "16", "0.83.x"],
      ["react-native-client", "ios", "current-stable", null, "0.83.x"],
      ["react-native-client", "android", "minimum", "24", "0.83.x"],
      ["react-native-client", "android", "current-stable", null, "0.83.x"],
    ].map(([component, platform, kind, value, runtime]) => ({
      component,
      platform,
      platform_version: value === null ? { kind } : { kind, value },
      ...(runtime === null
        ? {}
        : { runtime_version: { kind: "series", value: runtime } }),
      policy: "required",
    })),
  );

  const expectedKeys = new Set(expected.map(supportPolicyKey));
  const actualKeys = new Map(
    supportMatrix.cells.map((cell) => [supportPolicyKey(cell), cell.id]),
  );
  for (const key of expectedKeys) {
    if (!actualKeys.has(key)) errors.push(`Missing required support policy cell ${key}`);
  }
  for (const [key, id] of actualKeys) {
    if (!expectedKeys.has(key)) {
      errors.push(`${id} is outside the locked v0.3.0 support policy`);
    }
  }
  for (const requirement of requirements.requirements) {
    const hasServerProof = requirement.required_proof_types.includes(
      "server-black-box",
    );
    const hasNativeProof = requirement.required_proof_types.includes("native-e2e");
    const hasServerComponent = requirement.applicable_components.includes(
      "postgresql-server",
    );
    const clientComponents = requirement.applicable_components.filter(
      (component) => component !== "postgresql-server",
    );
    if (hasServerProof !== hasServerComponent) {
      errors.push(
        `${requirement.id} server proof and applicable PostgreSQL component must be declared together`,
      );
    }
    if (hasNativeProof !== (clientComponents.length > 0)) {
      errors.push(
        `${requirement.id} native proof and applicable client components must be declared together`,
      );
    }
  }
  return errors;
}

const wireCasePolicy = new Map([
  ["connect_success", [200, null, false, ["connect"]]],
  ["push_success", [200, null, false, ["push"]]],
  ["pull_success", [200, null, false, ["pull"]]],
  ["rebuild_success", [200, null, false, ["rebuild"]]],
  ["invalid_request", [400, "invalid_request", false, ["connect", "push", "pull", "rebuild"]]],
  ["invalid_schema_reference", [400, "invalid_schema_reference", false, ["connect"]]],
  ["auth_required", [401, "auth_required", false, ["connect", "push", "pull", "rebuild"]]],
  ["idempotency_conflict", [409, "idempotency_conflict", false, ["push"]]],
  ["client_retired", [409, "client_retired", false, ["connect", "push", "pull", "rebuild"]]],
  ["client_generation_expired", [409, "client_generation_expired", false, ["push", "pull", "rebuild"]]],
  ["rebuild_restart_required", [409, "rebuild_restart_required", false, ["rebuild"]]],
  ["schema_mismatch", [422, "schema_mismatch", false, ["push", "pull", "rebuild"]]],
  ["upgrade_required", [426, "upgrade_required", false, ["connect", "push", "pull", "rebuild"]]],
  ["retry_later", [429, "retry_later", true, ["connect", "push", "pull", "rebuild"]]],
  ["sync_integrity_failure", [500, "sync_integrity_failure", false, ["connect", "push", "pull", "rebuild"]]],
  ["capture_pending", [503, "capture_pending", true, ["pull", "rebuild"]]],
  ["temporary_unavailable", [503, "temporary_unavailable", true, ["connect", "push", "pull", "rebuild"]]],
]);

function scenarioSemanticErrors(scenario) {
  const errors = [
    ...duplicateLogicalIdErrors(scenario.steps, "id", "Scenario steps"),
    ...duplicateLogicalIdErrors(
      scenario.assertions,
      "id",
      "Scenario assertions",
    ),
    ...duplicateLogicalIdErrors(
      scenario.model.expected_state,
      "id",
      "Scenario model expectations",
    ),
    ...duplicateLogicalIdErrors(
      scenario.barrier_plan.barriers,
      "id",
      "Scenario barriers",
    ),
    ...duplicateLogicalIdErrors(
      scenario.fault_plans,
      "id",
      "Scenario fault plans",
    ),
    ...duplicateLogicalIdErrors(
      scenario.negative_controls,
      "control_id",
      "Scenario negative controls",
    ),
    ...duplicateLogicalIdErrors(
      scenario.proof_obligations,
      "obligation_id",
      "Scenario proof obligations",
    ),
  ];
  const assertionIds = new Set(scenario.assertions.map(({ id }) => id));
  const stepsById = new Map(scenario.steps.map((step) => [step.id, step]));
  const assertionsById = new Map(
    scenario.assertions.map((assertion) => [assertion.id, assertion]),
  );
  const scenarioRequirementIds = new Set(scenario.requirement_ids);
  const expectationIds = new Set(
    scenario.model.expected_state.map(({ id }) => id),
  );
  const barrierIds = new Set(
    scenario.barrier_plan.barriers.map(({ id }) => id),
  );
  errors.push(
    ...duplicateLogicalIdErrors(
      scenario.wire_expectations,
      "step_id",
      "Scenario wire expectations",
    ),
  );
  const httpStepIds = scenario.steps
    .filter(({ transport }) => transport === "http")
    .map(({ id }) => id);
  const wireExpectationStepIds = scenario.wire_expectations.map(
    ({ step_id }) => step_id,
  );
  if (!stringSetsEqual(httpStepIds, wireExpectationStepIds)) {
    errors.push(
      `${scenario.id} HTTP steps do not exactly match wire expectations`,
    );
  }
  for (const wireExpectation of scenario.wire_expectations) {
    const step = stepsById.get(wireExpectation.step_id);
    const assertion = assertionsById.get(wireExpectation.assertion_id);
    if (!step || step.transport !== "http") {
      errors.push(
        `${scenario.id} wire expectation references non-HTTP step ${wireExpectation.step_id}`,
      );
    }
    if (!assertion) {
      errors.push(
        `${scenario.id} wire expectation references unknown assertion ${wireExpectation.assertion_id}`,
      );
    } else if (assertion.oracle.kind !== "wire-contract") {
      errors.push(
        `${wireExpectation.assertion_id} must use the wire-contract oracle for ${wireExpectation.step_id}`,
      );
    }
    const policy = wireCasePolicy.get(wireExpectation.contract_case);
    if (policy) {
      const [httpStatus, errorCode, retryable, contractOperations] = policy;
      if (
        wireExpectation.http_status !== httpStatus ||
        wireExpectation.error_code !== errorCode ||
        wireExpectation.retryable !== retryable
      ) {
        errors.push(
          `${scenario.id} wire expectation ${wireExpectation.contract_case} does not match its canonical status, code, and retryability`,
        );
      }
      if (
        step &&
        !contractOperations.includes(step.operation.contract_operation)
      ) {
        errors.push(
          `${scenario.id} wire expectation ${wireExpectation.contract_case} is invalid for ${step.operation.contract_operation}`,
        );
      }
    }
  }
  for (const step of scenario.steps) {
    const isWireOperation = ["connect", "push", "pull", "rebuild"].includes(
      step.operation.contract_operation,
    );
    if ((step.transport === "http") !== isWireOperation) {
      errors.push(
        `${scenario.id} step ${step.id} transport does not match contract operation ${step.operation.contract_operation}`,
      );
    }
  }
  const proofObligationTypes = scenario.proof_obligations.map(
    ({ proof_type }) => proof_type,
  );
  if (
    !stringSetsEqual(
      scenario.proof_types,
      [...new Set(proofObligationTypes)],
    )
  ) {
    errors.push(`${scenario.id} proof types do not match proof obligations`);
  }
  const obligationRequirementIds = new Set();
  const obligationAssertionIds = new Set();
  for (const obligation of scenario.proof_obligations) {
    if (!repositoryMakeTargets.has(obligation.make_target)) {
      errors.push(
        `${scenario.id} obligation ${obligation.obligation_id} target ${obligation.make_target} is not defined by the repository Makefile`,
      );
    }
    for (const requirementId of obligation.requirement_ids) {
      obligationRequirementIds.add(requirementId);
      if (!scenarioRequirementIds.has(requirementId)) {
        errors.push(
          `${obligation.obligation_id} references requirement ${requirementId} outside scenario ${scenario.id}`,
        );
      }
    }
    const assertedRequirementIds = new Set();
    for (const assertionId of obligation.assertion_ids) {
      obligationAssertionIds.add(assertionId);
      const assertion = assertionsById.get(assertionId);
      if (!assertion) {
        errors.push(
          `${obligation.obligation_id} references unknown assertion ${assertionId}`,
        );
        continue;
      }
      for (const requirementId of assertion.requirement_ids) {
        assertedRequirementIds.add(requirementId);
      }
    }
    if (
      !stringSetsEqual(obligation.requirement_ids, [...assertedRequirementIds])
    ) {
      errors.push(
        `${obligation.obligation_id} requirement IDs do not exactly match its assertions`,
      );
    }
  }
  if (
    !stringSetsEqual(scenario.requirement_ids, [...obligationRequirementIds])
  ) {
    errors.push(
      `${scenario.id} requirement IDs do not exactly match its proof obligations`,
    );
  }
  const referencedExpectationIds = new Set();
  const assertionRequirementIds = new Set();
  const predicateByOracle = new Map([
    ["model-state-equality", "state-equality"],
    ["wire-contract", "wire-outcome"],
    ["state-transition", "state-transition"],
    ["artifact-policy", "artifact-integrity"],
    ["performance-budget", "performance-measurement"],
    ["negative-control-detection", "negative-control-detection"],
  ]);
  for (const assertion of scenario.assertions) {
    if (!obligationAssertionIds.has(assertion.id)) {
      errors.push(
        `${assertion.id} is not bound to any proof obligation in ${scenario.id}`,
      );
    }
    for (const requirementId of assertion.requirement_ids) {
      assertionRequirementIds.add(requirementId);
      if (!scenarioRequirementIds.has(requirementId)) {
        errors.push(
          `${assertion.id} references requirement ${requirementId} outside scenario ${scenario.id}`,
        );
      }
    }
    if (
      assertion.predicate.contract_predicate !==
      predicateByOracle.get(assertion.oracle.kind)
    ) {
      errors.push(
        `${assertion.id} contract predicate does not match oracle ${assertion.oracle.kind}`,
      );
    }
    for (const expectationId of assertion.expectation_ids) {
      referencedExpectationIds.add(expectationId);
      if (!expectationIds.has(expectationId)) {
        errors.push(`${assertion.id} references unknown model expectation ${expectationId}`);
      }
    }
  }
  if (!stringSetsEqual(scenario.requirement_ids, [...assertionRequirementIds])) {
    errors.push(
      `${scenario.id} requirement IDs do not exactly match its assertions`,
    );
  }
  for (const expectationId of expectationIds) {
    if (!referencedExpectationIds.has(expectationId)) {
      errors.push(
        `${scenario.id} model expectation ${expectationId} is not referenced by an assertion`,
      );
    }
  }

  if (scenario.replay.mode === "randomized" && !scenario.replay.seed_required) {
    errors.push(`${scenario.id} randomized replay must require a seed`);
  }
  if (
    scenario.barrier_plan.barriers.length > 0 &&
    !scenario.replay.barrier_trace_required
  ) {
    errors.push(`${scenario.id} authored barriers require a barrier trace`);
  }

  const controlsById = new Map(
    scenario.negative_controls.map((control) => [control.control_id, control]),
  );
  const plansByControlId = new Map();
  for (const faultPlan of scenario.fault_plans) {
    const matchingPlans = plansByControlId.get(faultPlan.control_id) ?? [];
    matchingPlans.push(faultPlan);
    plansByControlId.set(faultPlan.control_id, matchingPlans);
    if (!barrierIds.has(faultPlan.barrier_id)) {
      errors.push(`${faultPlan.id} references unknown barrier ${faultPlan.barrier_id}`);
    }
    for (const assertionId of faultPlan.expected_assertion_ids) {
      if (!assertionIds.has(assertionId)) {
        errors.push(`${faultPlan.id} references unknown assertion ${assertionId}`);
      }
    }
    const control = controlsById.get(faultPlan.control_id);
    if (!control) {
      errors.push(
        `${faultPlan.id} has no matching authored negative control ${faultPlan.control_id}`,
      );
      continue;
    }
    if (
      faultPlan.requirement_id !== control.requirement_id ||
      !scenarioRequirementIds.has(faultPlan.requirement_id)
    ) {
      errors.push(
        `${faultPlan.id} requirement does not match negative control ${control.control_id}`,
      );
    }
    if (control.fault_id !== faultPlan.fault_id) {
      errors.push(
        `${faultPlan.id} fault does not match negative control ${control.control_id}`,
      );
    }
    if (!stringSetsEqual(control.detected_by, faultPlan.expected_assertion_ids)) {
      errors.push(
        `${faultPlan.id} expected assertions do not exactly match negative control ${control.control_id} detected_by`,
      );
    }
  }
  for (const control of scenario.negative_controls) {
    if (!scenarioRequirementIds.has(control.requirement_id)) {
      errors.push(
        `Negative control ${control.control_id} references requirement ${control.requirement_id} outside scenario ${scenario.id}`,
      );
    }
    for (const assertionId of control.detected_by) {
      if (!assertionIds.has(assertionId)) {
        errors.push(
          `Negative control ${control.control_id} detected_by ID ${JSON.stringify(assertionId)} does not name an assertion in scenario ${scenario.id}`,
        );
      }
    }
    const plans = plansByControlId.get(control.control_id) ?? [];
    if (plans.length !== 1) {
      errors.push(
        `Negative control ${control.control_id} must have exactly one authored fault plan, found ${plans.length}`,
      );
    }
    for (const assertionId of control.detected_by) {
      const assertion = scenario.assertions.find(({ id }) => id === assertionId);
      if (!assertion?.detects_control_ids.includes(control.control_id)) {
        errors.push(
          `${assertionId} does not declare detection of ${control.control_id}`,
        );
      } else if (!assertion.requirement_ids.includes(control.requirement_id)) {
        errors.push(
          `${assertionId} does not assert requirement ${control.requirement_id} owned by ${control.control_id}`,
        );
      }
    }
  }
  for (const assertion of scenario.assertions) {
    for (const controlId of assertion.detects_control_ids) {
      const control = controlsById.get(controlId);
      if (!control) {
        errors.push(`${assertion.id} detects unknown control ${controlId}`);
      } else if (!control.detected_by.includes(assertion.id)) {
        errors.push(
          `${assertion.id} detection of ${controlId} is not reciprocally authored`,
        );
      }
    }
  }
  return errors;
}

const proofTargetPolicy = new Map([
  ["reference-model", new Set(["test-conformance"])],
  ["server-black-box", new Set(["test-blackbox"])],
  [
    "native-e2e",
    new Set([
      "test-swift",
      "test-kotlin",
      "test-rn-e2e-ios",
      "test-rn-e2e-android",
    ]),
  ],
  [
    "fault-injection",
    new Set([
      "test-blackbox",
      "test-swift",
      "test-kotlin",
      "test-rn-e2e-ios",
      "test-rn-e2e-android",
    ]),
  ],
  ["negative-control", new Set(["test-conformance"])],
]);

const targetComponentPolicy = new Map([
  ["test-conformance", null],
  ["test-blackbox", "postgresql-server"],
  ["test-swift", "swift-client"],
  ["test-kotlin", "kotlin-client"],
  ["test-rn-e2e-ios", "react-native-client"],
  ["test-rn-e2e-android", "react-native-client"],
]);

const targetPlatformPolicy = new Map([
  ["test-rn-e2e-ios", "ios"],
  ["test-rn-e2e-android", "android"],
]);

const targetRequiredArtifactRoles = new Map([
  ["test-conformance", new Set(["conformance-runner"])],
  ["test-blackbox", new Set(["pg-extension", "adapter"])],
  ["test-swift", new Set(["pg-extension", "adapter", "swift-spm"])],
  ["test-kotlin", new Set(["pg-extension", "adapter", "kotlin-maven"])],
  [
    "test-rn-e2e-ios",
    new Set([
      "pg-extension",
      "adapter",
      "swift-spm",
      "cocoapods",
      "react-native-npm",
    ]),
  ],
  [
    "test-rn-e2e-android",
    new Set(["pg-extension", "adapter", "kotlin-maven", "react-native-npm"]),
  ],
]);

const targetAllowedArtifactRoles = new Map([
  [
    "test-blackbox",
    new Set([
      "pg-extension",
      "pg-install-sql",
      "adapter",
      "seed-tool",
      "portable-seed",
    ]),
  ],
  [
    "test-swift",
    new Set([
      "pg-extension",
      "adapter",
      "seed-tool",
      "swift-spm",
      "cocoapods",
      "portable-seed",
    ]),
  ],
  [
    "test-kotlin",
    new Set([
      "pg-extension",
      "adapter",
      "seed-tool",
      "kotlin-maven",
      "portable-seed",
    ]),
  ],
  [
    "test-rn-e2e-ios",
    new Set([
      "pg-extension",
      "adapter",
      "seed-tool",
      "swift-spm",
      "cocoapods",
      "react-native-npm",
      "portable-seed",
    ]),
  ],
  [
    "test-rn-e2e-android",
    new Set([
      "pg-extension",
      "adapter",
      "seed-tool",
      "kotlin-maven",
      "react-native-npm",
      "portable-seed",
    ]),
  ],
]);

function performanceArtifactIdsForSupportCell(
  performanceItem,
  supportCell,
  inventoryRoles,
) {
  const clientRoles = new Set([
    "swift-spm",
    "cocoapods",
    "kotlin-maven",
    "react-native-npm",
  ]);
  const applicableClientRoles = new Set();
  if (supportCell.component === "swift-client") {
    applicableClientRoles.add("swift-spm");
    applicableClientRoles.add("cocoapods");
  } else if (supportCell.component === "kotlin-client") {
    applicableClientRoles.add("kotlin-maven");
  } else if (supportCell.component === "react-native-client") {
    applicableClientRoles.add("react-native-npm");
    if (supportCell.platform === "ios") {
      applicableClientRoles.add("swift-spm");
      applicableClientRoles.add("cocoapods");
    } else {
      applicableClientRoles.add("kotlin-maven");
    }
  }
  return performanceItem.artifact_inventory_ids.filter((inventoryId) => {
    const role = inventoryRoles.get(inventoryId);
    return !clientRoles.has(role) || applicableClientRoles.has(role);
  });
}

function authoredScenarioBindingErrors(
  scenario,
  requirements,
  supportMatrix,
  artifactInventory,
  faultCatalog,
  performanceBudgets,
) {
  const errors = [];
  const requirementIds = new Set(requirements.requirements.map(({ id }) => id));
  const requirementsById = new Map(
    requirements.requirements.map((requirement) => [requirement.id, requirement]),
  );
  const supportPolicies = new Map(
    supportMatrix.cells.map(({ id, policy }) => [id, policy]),
  );
  const inventoryIds = new Set(
    artifactInventory.artifacts.map(({ id }) => id),
  );
  const inventoryRoles = new Map(
    artifactInventory.artifacts.map(({ id, role }) => [id, role]),
  );
  const supportCells = new Map(
    supportMatrix.cells.map((cell) => [cell.id, cell]),
  );
  const faultIds = new Set(faultCatalog.faults.map(({ id }) => id));
  const controlIds = new Set(faultCatalog.controls.map(({ id }) => id));
  const budgetIds = new Set(performanceBudgets.budgets.map(({ id }) => id));
  const measurementIds = new Set(
    performanceBudgets.required_measurements.map(({ id }) => id),
  );
  for (const id of scenario.requirement_ids) {
    if (!requirementIds.has(id)) errors.push(`${scenario.id} references unknown requirement ${id}`);
  }
  for (const obligation of scenario.proof_obligations) {
    for (const requirementId of obligation.requirement_ids) {
      const requirement = requirementsById.get(requirementId);
      if (!scenario.requirement_ids.includes(requirementId)) {
        errors.push(
          `${scenario.id} obligation ${obligation.obligation_id} claims requirement ${requirementId} outside the scenario`,
        );
      }
      if (
        requirement &&
        obligation.support_cell_id !== null &&
        !requirement.applicable_components.includes(
          supportCells.get(obligation.support_cell_id)?.component,
        )
      ) {
        errors.push(
          `${scenario.id} obligation ${obligation.obligation_id} uses support cell outside requirement ${requirementId} applicability`,
        );
      }
    }
    for (const assertionId of obligation.assertion_ids) {
      const assertion = scenario.assertions.find(({ id }) => id === assertionId);
      for (const requirementId of assertion?.requirement_ids ?? []) {
        if (!obligation.requirement_ids.includes(requirementId)) {
          errors.push(
            `${scenario.id} obligation ${obligation.obligation_id} does not claim assertion ${assertionId} requirement ${requirementId}`,
          );
        }
      }
    }
    if (
      obligation.support_cell_id !== null &&
      supportPolicies.get(obligation.support_cell_id) !== "required"
    ) {
      errors.push(
        `${scenario.id} references unknown or excluded support cell ${obligation.support_cell_id}`,
      );
    }
    for (const id of obligation.artifact_inventory_ids) {
      if (!inventoryIds.has(id)) errors.push(`${scenario.id} references unknown artifact inventory ${id}`);
    }
    if (!proofTargetPolicy.get(obligation.proof_type)?.has(obligation.make_target)) {
      errors.push(
        `${scenario.id} obligation ${obligation.obligation_id} target ${obligation.make_target} cannot prove ${obligation.proof_type}`,
      );
    }
    const expectedComponent = targetComponentPolicy.get(obligation.make_target);
    const supportCell =
      obligation.support_cell_id === null
        ? null
        : supportCells.get(obligation.support_cell_id);
    if (expectedComponent === null && obligation.support_cell_id !== null) {
      errors.push(
        `${scenario.id} obligation ${obligation.obligation_id} target ${obligation.make_target} requires support_cell_id null`,
      );
    } else if (
      expectedComponent !== null &&
      expectedComponent !== undefined &&
      supportCell?.component !== expectedComponent
    ) {
      errors.push(
        `${scenario.id} obligation ${obligation.obligation_id} target ${obligation.make_target} requires a ${expectedComponent} support cell`,
      );
    }
    const expectedPlatform = targetPlatformPolicy.get(obligation.make_target);
    if (expectedPlatform && supportCell?.platform !== expectedPlatform) {
      errors.push(
        `${scenario.id} obligation ${obligation.obligation_id} target ${obligation.make_target} requires platform ${expectedPlatform}`,
      );
    }
    const actualRoles = new Set(
      obligation.artifact_inventory_ids
        .map((id) => inventoryRoles.get(id))
        .filter(Boolean),
    );
    for (const role of targetRequiredArtifactRoles.get(obligation.make_target) ?? []) {
      if (!actualRoles.has(role)) {
        errors.push(
          `${scenario.id} obligation ${obligation.obligation_id} target ${obligation.make_target} requires artifact role ${role}`,
        );
      }
    }
    if (obligation.proof_type === "reference-model") {
      if (!stringSetsEqual([...actualRoles], ["conformance-runner"])) {
        errors.push(
          `${scenario.id} obligation ${obligation.obligation_id} reference-model proof requires only the independent conformance-runner artifact`,
        );
      }
    } else if (obligation.make_target !== "test-conformance") {
      const allowedRoles = targetAllowedArtifactRoles.get(obligation.make_target);
      for (const role of actualRoles) {
        if (!allowedRoles?.has(role)) {
          errors.push(
            `${scenario.id} obligation ${obligation.obligation_id} target ${obligation.make_target} does not permit artifact role ${role}`,
          );
        }
      }
    }
    if (obligation.proof_type === "negative-control") {
      const expectedArtifactIds = new Set(["ARTDEF-CONFORMANCE-RUNNER-001"]);
      for (const control of scenario.negative_controls) {
        if (!obligation.requirement_ids.includes(control.requirement_id)) continue;
        for (const inventoryId of control.subject_artifact_inventory_ids) {
          expectedArtifactIds.add(inventoryId);
        }
      }
      if (
        !stringSetsEqual(
          obligation.artifact_inventory_ids,
          [...expectedArtifactIds],
        )
      ) {
        errors.push(
          `${scenario.id} obligation ${obligation.obligation_id} negative-control artifacts must exactly bind the conformance runner and mutated subjects`,
        );
      }
    }
  }

  const expectedNormativeReferences = new Set();
  for (const requirementId of scenario.requirement_ids) {
    const requirement = requirements.requirements.find(
      ({ id }) => id === requirementId,
    );
    for (const reference of requirement?.normative_references ?? []) {
      expectedNormativeReferences.add(`${reference.path}${reference.anchor}`);
    }
  }
  const scenarioNormativeReferences = scenario.normative_references.map(
    ({ path, anchor }) => `${path}${anchor}`,
  );
  if (
    !stringSetsEqual(
      scenarioNormativeReferences,
      [...expectedNormativeReferences],
    )
  ) {
    errors.push(
      `${scenario.id} normative references do not exactly match its requirements`,
    );
  }
  for (const plan of scenario.fault_plans) {
    if (!faultIds.has(plan.fault_id)) errors.push(`${scenario.id} references unknown fault ${plan.fault_id}`);
    if (!controlIds.has(plan.control_id)) errors.push(`${scenario.id} references unknown control ${plan.control_id}`);
    const catalogControl = faultCatalog.controls.find(
      ({ id }) => id === plan.control_id,
    );
    if (catalogControl && catalogControl.fault_id !== plan.fault_id) {
      errors.push(
        `${scenario.id} fault plan ${plan.id} does not match catalog control fault`,
      );
    }
    if (
      catalogControl &&
      (catalogControl.requirement_ids.length !== 1 ||
        catalogControl.requirement_ids[0] !== plan.requirement_id)
    ) {
      errors.push(
        `${scenario.id} fault plan ${plan.id} requirement does not match catalog control ownership`,
      );
    }
    if (
      catalogControl &&
      !isDeepStrictEqual(catalogControl.injection, plan.injection)
    ) {
      errors.push(
        `${scenario.id} fault plan ${plan.id} injection recipe does not match catalog control`,
      );
    }
  }
  const expectedControlIds = faultCatalog.controls
    .filter((control) =>
      control.requirement_ids.some((id) => scenario.requirement_ids.includes(id)),
    )
    .map(({ id }) => id);
  if (
    scenario.proof_types.includes("negative-control") &&
    !stringSetsEqual(
      scenario.negative_controls.map(({ control_id }) => control_id),
      expectedControlIds,
    )
  ) {
    errors.push(
      `${scenario.id} negative controls do not exactly match its requirements`,
    );
  }
  for (const scenarioControl of scenario.negative_controls) {
    const catalogControl = faultCatalog.controls.find(
      ({ id }) => id === scenarioControl.control_id,
    );
    for (const inventoryId of scenarioControl.subject_artifact_inventory_ids) {
      if (!inventoryIds.has(inventoryId)) {
        errors.push(
          `${scenario.id} control ${scenarioControl.control_id} references unknown subject artifact ${inventoryId}`,
        );
      } else if (inventoryRoles.get(inventoryId) === "conformance-runner") {
        errors.push(
          `${scenario.id} control ${scenarioControl.control_id} cannot use the conformance runner as its mutated subject`,
        );
      }
    }
    if (
      catalogControl &&
      !catalogControl.requirement_ids.every((id) =>
        scenario.requirement_ids.includes(id),
      )
    ) {
      errors.push(
        `${scenario.id} control ${scenarioControl.control_id} belongs to a different requirement`,
      );
    }
    if (
      catalogControl &&
      (catalogControl.requirement_ids.length !== 1 ||
        catalogControl.requirement_ids[0] !== scenarioControl.requirement_id)
    ) {
      errors.push(
        `${scenario.id} control ${scenarioControl.control_id} requirement does not match catalog ownership`,
      );
    }
  }
  for (const id of scenario.performance_budget_ids) {
    if (!budgetIds.has(id)) errors.push(`${scenario.id} references unknown performance budget ${id}`);
  }
  for (const id of scenario.required_measurement_ids) {
    if (!measurementIds.has(id)) {
      errors.push(`${scenario.id} references unknown required measurement ${id}`);
    }
  }
  const catalogBudgetIds = performanceBudgets.budgets
    .filter(({ scenario_id }) => scenario_id === scenario.id)
    .map(({ id }) => id);
  if (!stringSetsEqual(scenario.performance_budget_ids, catalogBudgetIds)) {
    errors.push(
      `${scenario.id} performance budget declarations do not exactly match the authored catalog`,
    );
  }
  const catalogMeasurementIds = performanceBudgets.required_measurements
    .filter(({ scenario_id }) => scenario_id === scenario.id)
    .map(({ id }) => id);
  if (
    !stringSetsEqual(
      scenario.required_measurement_ids,
      catalogMeasurementIds,
    )
  ) {
    errors.push(
      `${scenario.id} required measurement declarations do not exactly match the authored catalog`,
    );
  }
  for (const performanceItem of [
    ...performanceBudgets.budgets.filter(
      ({ scenario_id }) => scenario_id === scenario.id,
    ),
    ...performanceBudgets.required_measurements.filter(
      ({ scenario_id }) => scenario_id === scenario.id,
    ),
  ]) {
    for (const supportCellId of performanceItem.support_cell_ids) {
      const obligations = scenario.proof_obligations.filter(
        ({ support_cell_id }) => support_cell_id === supportCellId,
      );
      if (obligations.length === 0) {
        errors.push(
          `${scenario.id} ${performanceItem.id} has no proof obligation for support cell ${supportCellId}`,
        );
      }
      const expectedArtifactIds = performanceArtifactIdsForSupportCell(
        performanceItem,
        supportCells.get(supportCellId),
        inventoryRoles,
      );
      for (const obligation of obligations) {
        if (
          !stringSetsEqual(
            obligation.artifact_inventory_ids,
            expectedArtifactIds,
          )
        ) {
          errors.push(
            `${scenario.id} ${performanceItem.id} obligation ${obligation.obligation_id} artifacts do not exactly match support cell ${supportCellId}`,
          );
        }
      }
    }
  }
  return errors;
}

function stringSetsEqual(left, right) {
  const leftValues = new Set(left);
  const rightValues = new Set(right);
  if (leftValues.size !== rightValues.size) return false;
  return [...leftValues].every((value) => rightValues.has(value));
}

function observedPerformanceValue(metric, measurement) {
  const counts = measurement.request_counts;
  const sum = (...names) => names.reduce((total, name) => total + counts[name], 0);
  switch (metric) {
    case "warm_connect_http_requests":
    case "rebuild_connect_http_requests":
      return counts.connect;
    case "warm_connect_non_connect_http_requests":
      return sum("push", "pull", "rebuild_page", "schema_fetch", "other");
    case "steady_state_pull_http_requests_per_cycle":
    case "pending_cycle_pull_http_requests":
    case "rebuild_pull_http_requests":
      return counts.pull;
    case "steady_state_pull_non_pull_http_requests_per_cycle":
      return sum("connect", "push", "rebuild_page", "schema_fetch", "other");
    case "pending_cycle_push_http_requests":
      return counts.push;
    case "pending_cycle_non_push_or_pull_http_requests":
      return sum("connect", "rebuild_page", "schema_fetch", "other");
    case "rebuild_page_request_count_minus_returned_page_count":
      return counts.rebuild_page - measurement.returned_rebuild_page_count;
    case "rebuild_schema_fetch_http_requests":
      return counts.schema_fetch;
    case "rebuild_unexpected_http_requests":
      return sum("push", "schema_fetch", "other");
    case "core_sync_outbound_network_or_rpc_hops":
      return measurement.outbound_network_or_rpc_hops;
    default:
      return null;
  }
}

function evidenceScenarioSemanticErrors(
  evidence,
  scenario,
  manifest,
  artifactInventory,
  performanceBudgets,
) {
  const errors = [
    ...duplicateLogicalIdErrors(
      evidence.assertions,
      "assertion_id",
      "Evidence assertions",
    ),
    ...duplicateLogicalIdErrors(evidence.attachments, "id", "Evidence attachments"),
    ...duplicateLogicalIdErrors(
      evidence.attachments,
      "path",
      "Evidence attachment paths",
    ),
    ...duplicateLogicalIdErrors(
      evidence.performance_results,
      "budget_id",
      "Evidence performance results",
    ),
    ...duplicateLogicalIdErrors(
      evidence.required_measurement_results,
      "measurement_id",
      "Evidence required measurement results",
    ),
  ];
  if (evidence.scenario_id !== scenario.id) {
    errors.push(
      `${evidence.evidence_id} names scenario ${evidence.scenario_id}, not ${scenario.id}`,
    );
  }

  const obligation = scenario.proof_obligations.find(
    ({ obligation_id }) => obligation_id === evidence.proof_obligation_id,
  );
  if (!obligation) {
    errors.push(
      `${evidence.evidence_id} references unknown proof obligation ${evidence.proof_obligation_id}`,
    );
  } else {
    if (!stringSetsEqual(evidence.requirement_ids, obligation.requirement_ids)) {
      errors.push(
        `${evidence.evidence_id} requirement IDs do not exactly match obligation ${obligation.obligation_id}`,
      );
    }
    if (evidence.proof_type !== obligation.proof_type) {
      errors.push(
        `${evidence.evidence_id} proof type ${evidence.proof_type} does not match obligation ${obligation.obligation_id}`,
      );
    }
    if (evidence.support_cell_id !== obligation.support_cell_id) {
      errors.push(
        `${evidence.evidence_id} support cell ${evidence.support_cell_id} does not match obligation ${obligation.obligation_id}`,
      );
    }
    if (
      evidence.run.make_target !== obligation.make_target ||
      !isDeepStrictEqual(evidence.run.argv, obligation.argv)
    ) {
      errors.push(
        `${evidence.evidence_id} command does not exactly match obligation ${obligation.obligation_id}`,
      );
    }

    const manifestArtifactsById = new Map(
      manifest.artifacts.map((artifact) => [artifact.id, artifact]),
    );
    const inventoryById = new Map(
      artifactInventory.artifacts.map((artifact) => [artifact.id, artifact]),
    );
    const actualInventoryIds = [];
    for (const artifactId of evidence.artifact_ids) {
      const artifact = manifestArtifactsById.get(artifactId);
      if (artifact) {
        actualInventoryIds.push(artifact.inventory_id);
      } else {
        actualInventoryIds.push(`unresolved:${artifactId}`);
        errors.push(
          `${evidence.evidence_id} artifact ${artifactId} does not resolve through the RC manifest`,
        );
      }
    }
    if (
      !stringSetsEqual(actualInventoryIds, obligation.artifact_inventory_ids)
    ) {
      errors.push(
        `${evidence.evidence_id} resolved artifact inventory IDs do not exactly match obligation ${obligation.obligation_id}`,
      );
    }
    const actualRoles = actualInventoryIds
      .map((id) => inventoryById.get(id)?.role)
      .filter(Boolean);
    const requiredRoles = obligation.artifact_inventory_ids
      .map((id) => inventoryById.get(id)?.role)
      .filter(Boolean);
    if (!stringSetsEqual(actualRoles, requiredRoles)) {
      errors.push(
        `${evidence.evidence_id} resolved artifact roles do not exactly match obligation ${obligation.obligation_id}`,
      );
    }
  }

  const expectedAssertions = new Set(
    obligation?.assertion_ids ?? [],
  );
  const actualAssertions = new Set(
    evidence.assertions.map(({ assertion_id }) => assertion_id),
  );
  for (const assertionId of expectedAssertions) {
    if (!actualAssertions.has(assertionId)) {
      errors.push(
        `${evidence.evidence_id} is missing obligation assertion ${assertionId}`,
      );
    }
  }
  for (const assertionId of actualAssertions) {
    if (!expectedAssertions.has(assertionId)) {
      errors.push(
        `${evidence.evidence_id} contains undeclared assertion ${assertionId}`,
      );
    }
  }

  const startedAt = Date.parse(evidence.run.started_at);
  const completedAt = Date.parse(evidence.run.completed_at);
  if (
    Number.isFinite(startedAt) &&
    Number.isFinite(completedAt) &&
    (completedAt < startedAt ||
      completedAt - startedAt !== evidence.run.duration_ms)
  ) {
    errors.push(
      `${evidence.evidence_id} run duration does not match its start and completion timestamps`,
    );
  }
  if (
    evidence.run.argv.length !== 2 ||
    evidence.run.argv[0] !== "make" ||
    evidence.run.argv[1] !== evidence.run.make_target
  ) {
    errors.push(
      `${evidence.evidence_id} command line is not exactly ["make", make_target]`,
    );
  }
  errors.push(
    ...duplicateLogicalIdErrors(
      evidence.environment,
      "name",
      "Evidence environment dimensions",
    ),
  );
  if (evidence.support_cell_id !== null) {
    const resolvedCell = manifest.resolved_support_cells.find(
      ({ support_cell_id }) => support_cell_id === evidence.support_cell_id,
    );
    if (!resolvedCell) {
      errors.push(
        `${evidence.evidence_id} support cell ${evidence.support_cell_id} is not resolved by the RC manifest`,
      );
    } else {
      const expectedEnvironment = resolvedCell.dimensions.map(
        ({ name, version }) => ({ name, value: version }),
      );
      const environmentBindings = (items) =>
        items.map(({ name, value }) => `${name}\u0000${value}`);
      if (
        !stringSetsEqual(
          environmentBindings(evidence.environment),
          environmentBindings(expectedEnvironment),
        )
      ) {
        errors.push(
          `${evidence.evidence_id} environment does not exactly match resolved support cell ${evidence.support_cell_id}`,
        );
      }
    }
  } else if (evidence.environment.length !== 0) {
    errors.push(
      `${evidence.evidence_id} support-neutral evidence must have an empty environment`,
    );
  }

  const attachmentsById = new Map(
    evidence.attachments.map((attachment) => [attachment.id, attachment]),
  );
  for (const [field, requiredKind] of [
    ["log_attachment_ids", "log"],
    ["trace_attachment_ids", "trace"],
    ["replay_data_attachment_ids", "replay-data"],
    ["barrier_trace_attachment_ids", "barrier-trace"],
  ]) {
    for (const attachmentId of evidence.execution_artifacts[field]) {
      const attachment = attachmentsById.get(attachmentId);
      if (!attachment) {
        errors.push(
          `${evidence.evidence_id} ${field} references missing attachment ${attachmentId}`,
        );
      } else if (attachment.kind !== requiredKind) {
        errors.push(
          `${evidence.evidence_id} ${field} attachment ${attachmentId} has kind ${attachment.kind}, not ${requiredKind}`,
        );
      }
    }
  }
  if (evidence.seed !== evidence.replay.seed) {
    errors.push(`${evidence.evidence_id} replay seed bindings do not match`);
  }
  if (
    (scenario.replay.mode === "randomized" || scenario.replay.seed_required) &&
    evidence.replay.seed === null
  ) {
    errors.push(`${evidence.evidence_id} is missing its required replay seed`);
  }
  const authoredBarrierIds = scenario.barrier_plan.barriers.map(({ id }) => id);
  const replayBarrierIds = evidence.replay.barrier_traces.map(
    ({ barrier_id }) => barrier_id,
  );
  errors.push(
    ...duplicateLogicalIdErrors(
      evidence.replay.barrier_traces,
      "barrier_id",
      "Evidence replay barriers",
    ),
  );
  if (!stringSetsEqual(authoredBarrierIds, replayBarrierIds)) {
    errors.push(
      `${evidence.evidence_id} replay barrier IDs do not exactly match authored barriers`,
    );
  }
  const replayBarrierAttachmentIds = [];
  for (const barrierTrace of evidence.replay.barrier_traces) {
    const attachment = attachmentsById.get(barrierTrace.attachment_id);
    replayBarrierAttachmentIds.push(barrierTrace.attachment_id);
    if (!attachment) {
      errors.push(
        `${evidence.evidence_id} barrier ${barrierTrace.barrier_id} references missing attachment ${barrierTrace.attachment_id}`,
      );
    } else if (attachment.kind !== "barrier-trace") {
      errors.push(
        `${evidence.evidence_id} barrier ${barrierTrace.barrier_id} attachment has kind ${attachment.kind}, not barrier-trace`,
      );
    }
  }
  if (
    !stringSetsEqual(
      replayBarrierAttachmentIds,
      evidence.execution_artifacts.barrier_trace_attachment_ids,
    )
  ) {
    errors.push(
      `${evidence.evidence_id} replay barrier attachments do not match execution artifacts`,
    );
  }
  if (scenario.replay.barrier_trace_required && replayBarrierIds.length === 0) {
    errors.push(`${evidence.evidence_id} is missing its required barrier trace`);
  }
  const expectedBudgets = new Set(scenario.performance_budget_ids);
  const actualBudgets = new Set(
    evidence.performance_results.map(({ budget_id }) => budget_id),
  );
  if (!stringSetsEqual([...expectedBudgets], [...actualBudgets])) {
    errors.push(`${evidence.evidence_id} performance budget results do not match scenario`);
  }
  const expectedMeasurements = new Set(scenario.required_measurement_ids);
  const actualMeasurements = new Set(
    evidence.required_measurement_results.map(
      ({ measurement_id }) => measurement_id,
    ),
  );
  if (!stringSetsEqual([...expectedMeasurements], [...actualMeasurements])) {
    errors.push(
      `${evidence.evidence_id} required measurement results do not match scenario`,
    );
  }
  for (const result of evidence.performance_results) {
    const attachment = attachmentsById.get(result.measurement_attachment_id);
    if (!attachment || attachment.kind !== "performance-measurements") {
      errors.push(
        `${evidence.evidence_id} performance budget ${result.budget_id} lacks its typed performance-measurements attachment`,
      );
    }
    const budget = performanceBudgets.budgets.find(
      ({ id }) => id === result.budget_id,
    );
    if (!budget || budget.scenario_id !== scenario.id) {
      errors.push(
        `${evidence.evidence_id} performance budget ${result.budget_id} is not authored for scenario ${scenario.id}`,
      );
      continue;
    }
    if (
      evidence.support_cell_id === null ||
      !budget.support_cell_ids.includes(evidence.support_cell_id)
    ) {
      errors.push(
        `${evidence.evidence_id} performance budget ${result.budget_id} does not authorize support cell ${evidence.support_cell_id}`,
      );
    }
    const budgetArtifactInventoryIds = new Set(
      budget.artifact_inventory_ids,
    );
    for (const artifactId of evidence.artifact_ids) {
      const inventoryId = manifest.artifacts.find(
        ({ id }) => id === artifactId,
      )?.inventory_id;
      if (
        inventoryId !== undefined &&
        !budgetArtifactInventoryIds.has(inventoryId)
      ) {
        errors.push(
          `${evidence.evidence_id} performance budget ${result.budget_id} does not authorize artifact inventory ${inventoryId}`,
        );
      }
    }
    for (const field of [
      "metric",
      "unit",
      "comparator",
      "limit",
      "data_profile",
      "measurement_method",
    ]) {
      if (!isDeepStrictEqual(result[field], budget[field])) {
        errors.push(
          `${evidence.evidence_id} performance budget ${result.budget_id} changed authored ${field}`,
        );
      }
    }
    const derivedObservedValue = observedPerformanceValue(
      result.metric,
      result.measurement,
    );
    if (derivedObservedValue !== result.observed_value) {
      errors.push(
        `${evidence.evidence_id} performance budget ${result.budget_id} observed value is not derived from its typed measurement`,
      );
    }
    const comparisonPassed =
      result.comparator === "eq"
        ? result.observed_value === result.limit
        : result.comparator === "lte"
          ? result.observed_value <= result.limit
          : result.observed_value >= result.limit;
    if (
      (result.outcome === "passed" && !comparisonPassed) ||
      (result.outcome === "failed" && comparisonPassed)
    ) {
      errors.push(
        `${evidence.evidence_id} performance budget ${result.budget_id} outcome contradicts its observed value`,
      );
    }
  }
  for (const result of evidence.required_measurement_results) {
    const attachment = attachmentsById.get(result.measurement_attachment_id);
    if (!attachment || attachment.kind !== "performance-measurements") {
      errors.push(
        `${evidence.evidence_id} required measurement ${result.measurement_id} lacks its typed performance-measurements attachment`,
      );
    }
    const measurement = performanceBudgets.required_measurements.find(
      ({ id }) => id === result.measurement_id,
    );
    if (!measurement || measurement.scenario_id !== scenario.id) {
      errors.push(
        `${evidence.evidence_id} required measurement ${result.measurement_id} is not authored for scenario ${scenario.id}`,
      );
      continue;
    }
    if (
      evidence.support_cell_id === null ||
      !measurement.support_cell_ids.includes(evidence.support_cell_id)
    ) {
      errors.push(
        `${evidence.evidence_id} required measurement ${result.measurement_id} does not authorize support cell ${evidence.support_cell_id}`,
      );
    }
    const measurementArtifactInventoryIds = new Set(
      measurement.artifact_inventory_ids,
    );
    for (const artifactId of evidence.artifact_ids) {
      const inventoryId = manifest.artifacts.find(
        ({ id }) => id === artifactId,
      )?.inventory_id;
      if (
        inventoryId !== undefined &&
        !measurementArtifactInventoryIds.has(inventoryId)
      ) {
        errors.push(
          `${evidence.evidence_id} required measurement ${result.measurement_id} does not authorize artifact inventory ${inventoryId}`,
        );
      }
    }
    for (const field of ["data_profile", "measurement_method", "metrics"]) {
      if (!isDeepStrictEqual(result[field], measurement[field])) {
        errors.push(
          `${evidence.evidence_id} required measurement ${result.measurement_id} changed authored ${field}`,
        );
      }
    }
    errors.push(
      ...duplicateLogicalIdErrors(
        result.strata,
        "stratum_id",
        `${evidence.evidence_id} ${result.measurement_id} strata`,
      ),
    );
    const resultStrataById = new Map(
      result.strata.map((stratum) => [stratum.stratum_id, stratum]),
    );
    if (
      !stringSetsEqual(
        result.strata.map(({ stratum_id }) => stratum_id),
        measurement.strata.map(({ stratum_id }) => stratum_id),
      )
    ) {
      errors.push(
        `${evidence.evidence_id} required measurement ${result.measurement_id} strata do not match the authored set`,
      );
    }
    for (const authoredStratum of measurement.strata) {
      const resultStratum = resultStrataById.get(authoredStratum.stratum_id);
      if (!resultStratum) continue;
      if (!isDeepStrictEqual(resultStratum.parameters, authoredStratum.parameters)) {
        errors.push(
          `${evidence.evidence_id} required measurement ${result.measurement_id} changed stratum ${authoredStratum.stratum_id} parameters`,
        );
      }
      if (
        resultStratum.sample_count <
        measurement.minimum_sample_count_per_stratum
      ) {
        errors.push(
          `${evidence.evidence_id} required measurement ${result.measurement_id} stratum ${authoredStratum.stratum_id} sample count is below the authored minimum`,
        );
      }
      if (resultStratum.sample_count !== resultStratum.observations.length) {
        errors.push(
          `${evidence.evidence_id} required measurement ${result.measurement_id} stratum ${authoredStratum.stratum_id} sample count does not match observations`,
        );
      }
      errors.push(
        ...duplicateLogicalIdErrors(
          resultStratum.observations,
          "sample_id",
          `${evidence.evidence_id} ${result.measurement_id} ${authoredStratum.stratum_id} observations`,
        ),
      );
      const expectedMetricIds = measurement.metrics.map(({ id }) => id);
      for (const observation of resultStratum.observations) {
        errors.push(
          ...duplicateLogicalIdErrors(
            observation.metric_values,
            "metric_id",
            `${observation.sample_id} metric values`,
          ),
        );
        if (
          !stringSetsEqual(
            observation.metric_values.map(({ metric_id }) => metric_id),
            expectedMetricIds,
          )
        ) {
          errors.push(
            `${evidence.evidence_id} required measurement ${result.measurement_id} observation ${observation.sample_id} metric IDs do not match the authored set`,
          );
        }
      }
    }
  }

  if (evidence.proof_type !== "negative-control") return errors;
  const evidenceControl = evidence.negative_control;
  const scenarioControl = scenario.negative_controls.find(
    ({ control_id }) => control_id === evidenceControl?.control_id,
  );
  if (!evidenceControl || !scenarioControl) {
    errors.push(
      `${evidence.evidence_id} negative-control evidence requires matching scenario and evidence metadata`,
    );
    return errors;
  }
  if (evidenceControl.fault_id !== scenarioControl.fault_id) {
    errors.push(
      `${evidence.evidence_id} negative-control fault ${evidenceControl.fault_id} does not match scenario fault ${scenarioControl.fault_id}`,
    );
  }
  const subjectInventoryIds = [];
  for (const artifactId of evidenceControl.control_subject_artifact_ids) {
    if (!evidence.artifact_ids.includes(artifactId)) {
      errors.push(
        `${evidence.evidence_id} negative-control subject artifact ${artifactId} is not an execution artifact`,
      );
    }
    const inventoryId = manifest.artifacts.find(
      ({ id }) => id === artifactId,
    )?.inventory_id;
    if (inventoryId) subjectInventoryIds.push(inventoryId);
    else {
      errors.push(
        `${evidence.evidence_id} negative-control subject artifact ${artifactId} is absent from the RC manifest`,
      );
    }
  }
  if (
    !stringSetsEqual(
      subjectInventoryIds,
      scenarioControl.subject_artifact_inventory_ids,
    )
  ) {
    errors.push(
      `${evidence.evidence_id} negative-control subject artifacts do not exactly match the authored control`,
    );
  }
  if (!stringSetsEqual(evidenceControl.detected_by, scenarioControl.detected_by)) {
    errors.push(
      `${evidence.evidence_id} negative-control detected_by IDs do not match scenario ${scenario.id}`,
    );
  }
  for (const assertionId of evidenceControl.detected_by) {
    const passed = evidence.assertions.some(
      (assertion) =>
        assertion.assertion_id === assertionId && assertion.outcome === "passed",
    );
    if (!passed) {
      errors.push(
        `${evidence.evidence_id} detected_by assertion ${assertionId} does not have a passed evidence outcome`,
      );
    }
  }
  const attachmentIds = new Set(evidence.attachments.map(({ id }) => id));
  for (const attachmentId of evidenceControl.attachment_ids) {
    const attachment = attachmentsById.get(attachmentId);
    if (!attachmentIds.has(attachmentId)) {
      errors.push(
        `${evidence.evidence_id} negative-control attachment ${attachmentId} does not identify an evidence attachment`,
      );
    } else if (attachment.kind !== "negative-control") {
      errors.push(
        `${evidence.evidence_id} negative-control attachment ${attachmentId} has kind ${attachment.kind}, not negative-control`,
      );
    }
  }
  return errors;
}

function faultExecutionCatalogErrors(evidence, scenario, faultCatalog) {
  const errors = [];
  if (!["fault-injection", "negative-control"].includes(evidence.proof_type)) {
    return errors;
  }
  const metadata = evidence.negative_control;
  const execution = evidence.fault_execution;
  const scenarioPlan = scenario.fault_plans.find(
    ({ id }) => id === execution?.fault_plan_id,
  );
  const scenarioControl = scenario.negative_controls.find(
    ({ control_id }) => control_id === execution?.control_id,
  );
  const catalogControl = faultCatalog.controls.find(
    ({ id }) => id === execution?.control_id,
  );
  const catalogFault = faultCatalog.faults.find(
    ({ id }) => id === execution?.fault_id,
  );
  if (!execution) {
    errors.push(`${evidence.evidence_id} lacks fault execution metadata`);
    return errors;
  }
  if (!scenarioControl) errors.push(`${evidence.evidence_id} control is not authored by scenario`);
  if (!scenarioPlan) errors.push(`${evidence.evidence_id} fault plan is not authored by scenario`);
  if (!catalogControl) errors.push(`${evidence.evidence_id} control is absent from fault catalog`);
  if (!catalogFault) errors.push(`${evidence.evidence_id} fault is absent from fault catalog`);
  for (const [label, value, expected] of [
    ["scenario control fault", scenarioControl?.fault_id, execution.fault_id],
    ["scenario plan fault", scenarioPlan?.fault_id, execution.fault_id],
    ["scenario plan control", scenarioPlan?.control_id, execution.control_id],
    ["catalog control fault", catalogControl?.fault_id, execution.fault_id],
  ]) {
    if (value !== undefined && value !== expected) {
      errors.push(`${evidence.evidence_id} ${label} does not match fault execution`);
    }
  }
  const attachment = evidence.attachments.find(
    ({ id }) => id === execution.fault_plan_attachment_id,
  );
  if (!attachment || attachment.kind !== "fault-plan") {
    errors.push(`${evidence.evidence_id} lacks its typed fault-plan attachment`);
  }
  if (execution.subject_type !== catalogControl?.subject_type) {
    errors.push(
      `${evidence.evidence_id} fault execution subject type does not match catalog control`,
    );
  }
  if (
    scenarioPlan &&
    !stringSetsEqual(
      execution.detected_by,
      scenarioPlan.expected_assertion_ids,
    )
  ) {
    errors.push(
      `${evidence.evidence_id} fault execution detected assertions do not match scenario plan`,
    );
  }
  if (
    scenarioControl &&
    !stringSetsEqual(execution.detected_by, scenarioControl.detected_by)
  ) {
    errors.push(
      `${evidence.evidence_id} fault execution detected assertions do not match scenario control`,
    );
  }
  for (const assertionId of execution.detected_by) {
    const assertion = evidence.assertions.find(
      ({ assertion_id }) => assertion_id === assertionId,
    );
    if (!assertion || assertion.outcome !== "passed") {
      errors.push(
        `${evidence.evidence_id} fault execution assertion ${assertionId} is not passed`,
      );
    }
  }
  if (
    scenarioPlan &&
    !isDeepStrictEqual(execution.injection, scenarioPlan.injection)
  ) {
    errors.push(
      `${evidence.evidence_id} fault execution injection does not match scenario plan`,
    );
  }
  if (
    catalogControl &&
    !isDeepStrictEqual(execution.injection, catalogControl.injection)
  ) {
    errors.push(
      `${evidence.evidence_id} fault execution injection does not match catalog control`,
    );
  }
  if (evidence.proof_type === "negative-control") {
    for (const [label, value, expected] of [
      ["metadata fault", metadata?.fault_id, execution.fault_id],
      ["metadata control", metadata?.control_id, execution.control_id],
      ["metadata plan", metadata?.fault_plan_id, execution.fault_plan_id],
      [
        "metadata fault-plan attachment",
        metadata?.fault_plan_attachment_id,
        execution.fault_plan_attachment_id,
      ],
      ["metadata subject type", metadata?.control_subject_type, execution.subject_type],
      ["metadata subject ID", metadata?.control_subject_id, execution.control_id],
    ]) {
      if (value !== expected) {
        errors.push(`${evidence.evidence_id} ${label} does not match fault execution`);
      }
    }
    if (
      metadata &&
      !stringSetsEqual(metadata.detected_by, execution.detected_by)
    ) {
      errors.push(
        `${evidence.evidence_id} negative-control detected assertions do not match fault execution`,
      );
    }
  }
  return errors;
}

function evidencePromotionEligibilityErrors(evidence) {
  const errors = [];
  if (evidence.run.exit_code !== 0) {
    errors.push(
      `${evidence.evidence_id} is not promotion-eligible because exit_code is ${evidence.run.exit_code}, not 0`,
    );
  }
  for (const assertion of evidence.assertions) {
    if (assertion.outcome !== "passed") {
      errors.push(
        `${evidence.evidence_id} is not promotion-eligible because assertion ${assertion.assertion_id} has outcome ${assertion.outcome}, not passed`,
      );
    }
  }
  for (const result of evidence.performance_results) {
    if (result.outcome !== "passed") {
      errors.push(
        `${evidence.evidence_id} is not promotion-eligible because performance budget ${result.budget_id} has outcome ${result.outcome}, not passed`,
      );
    }
  }
  for (const result of evidence.required_measurement_results) {
    if (result.outcome !== "passed") {
      errors.push(
        `${evidence.evidence_id} is not promotion-eligible because required measurement ${result.measurement_id} has outcome ${result.outcome}, not passed`,
      );
    }
  }
  return errors;
}

function evidenceBundleSemanticErrors(evidenceBundle) {
  const errors = duplicateLogicalIdErrors(
    evidenceBundle,
    "evidence_id",
    "Evidence bundle",
  );
  const evidenceById = new Map();
  const lineageAttempts = new Set();
  const successorCount = new Map();
  const lineagesByExecutionKey = new Map();
  const executionKeysByLineage = new Map();
  const attemptsByLineage = new Map();
  const attachmentPathOwners = new Map();
  for (const evidence of evidenceBundle) {
    for (const attachment of evidence.attachments) {
      const owner = attachmentPathOwners.get(attachment.path);
      if (owner) {
        errors.push(
          `Evidence attachment path ${attachment.path} is shared by ${owner} and ${evidence.evidence_id}`,
        );
      } else {
        attachmentPathOwners.set(attachment.path, evidence.evidence_id);
      }
    }
    if (!evidenceById.has(evidence.evidence_id)) {
      evidenceById.set(evidence.evidence_id, evidence);
    }
    const lineageAttempt = `${evidence.run.execution_lineage_id}|${evidence.run.attempt}`;
    if (lineageAttempts.has(lineageAttempt)) {
      errors.push(
        `Evidence bundle contains duplicate lineage attempt ${lineageAttempt}`,
      );
    }
    lineageAttempts.add(lineageAttempt);
    const executionKey = [
      evidence.candidate_id,
      evidence.scenario_id,
      evidence.proof_obligation_id,
      evidence.support_cell_id ?? "null",
    ].join("|");
    const keyLineages = lineagesByExecutionKey.get(executionKey) ?? new Set();
    keyLineages.add(evidence.run.execution_lineage_id);
    lineagesByExecutionKey.set(executionKey, keyLineages);
    const lineageExecutionKeys =
      executionKeysByLineage.get(evidence.run.execution_lineage_id) ?? new Set();
    lineageExecutionKeys.add(executionKey);
    executionKeysByLineage.set(
      evidence.run.execution_lineage_id,
      lineageExecutionKeys,
    );
    const lineageEvidence =
      attemptsByLineage.get(evidence.run.execution_lineage_id) ?? [];
    lineageEvidence.push(evidence);
    attemptsByLineage.set(evidence.run.execution_lineage_id, lineageEvidence);
    if (evidence.run.previous_evidence_id !== null) {
      successorCount.set(
        evidence.run.previous_evidence_id,
        (successorCount.get(evidence.run.previous_evidence_id) ?? 0) + 1,
      );
    }
  }

  for (const [executionKey, lineages] of lineagesByExecutionKey) {
    if (lineages.size > 1) {
      errors.push(
        `Evidence bundle execution key ${executionKey} has multiple lineages: ${[...lineages].join(", ")}`,
      );
    }
  }
  for (const [lineageId, executionKeys] of executionKeysByLineage) {
    if (executionKeys.size > 1) {
      errors.push(
        `Evidence lineage ${lineageId} is reused across execution keys: ${[...executionKeys].join(", ")}`,
      );
    }
  }
  for (const [lineageId, lineageEvidence] of attemptsByLineage) {
    const attempts = lineageEvidence
      .map(({ run }) => run.attempt)
      .sort((left, right) => left - right);
    for (let index = 0; index < attempts.length; index += 1) {
      const expectedAttempt = index + 1;
      if (attempts[index] !== expectedAttempt) {
        errors.push(
          `Evidence lineage ${lineageId} attempts are not linear from 1, expected ${expectedAttempt} and found ${attempts[index]}`,
        );
        break;
      }
    }
  }

  for (const [evidenceId, count] of successorCount) {
    if (count > 1) {
      errors.push(`${evidenceId} has ${count} rerun successors`);
    }
  }

  for (const evidence of evidenceBundle) {
    if (evidence.run.attempt < 2) continue;
    const previousId = evidence.run.previous_evidence_id;
    if (previousId === evidence.evidence_id) {
      errors.push(
        `${evidence.evidence_id} rerun cannot reference itself as previous evidence`,
      );
      continue;
    }
    const previous = evidenceById.get(previousId);
    if (!previous) {
      errors.push(
        `${evidence.evidence_id} rerun references missing previous evidence ${JSON.stringify(previousId)}`,
      );
      continue;
    }
    if (previous.run.attempt !== evidence.run.attempt - 1) {
      errors.push(
        `${evidence.evidence_id} attempt ${evidence.run.attempt} must reference immediately prior attempt ${evidence.run.attempt - 1}, found attempt ${previous.run.attempt}`,
      );
    }
    const approvedAt = Date.parse(evidence.run.rerun_approval.approved_at);
    if (approvedAt >= Date.parse(evidence.run.started_at)) {
      errors.push(
        `${evidence.evidence_id} rerun approval does not precede rerun start`,
      );
    }
    if (approvedAt <= Date.parse(previous.run.completed_at)) {
      errors.push(
        `${evidence.evidence_id} rerun approval does not follow predecessor completion`,
      );
    }

    const scalarBindings = [
      ["candidate_id", previous.candidate_id, evidence.candidate_id],
      [
        "execution_lineage_id",
        previous.run.execution_lineage_id,
        evidence.run.execution_lineage_id,
      ],
      ["release_version", previous.release_version, evidence.release_version],
      ["protocol_version", previous.protocol_version, evidence.protocol_version],
      [
        "contract_snapshot_sha256",
        previous.contract_snapshot_sha256,
        evidence.contract_snapshot_sha256,
      ],
      ["source_commit", previous.source_commit, evidence.source_commit],
      ["scenario_id", previous.scenario_id, evidence.scenario_id],
      [
        "proof_obligation_id",
        previous.proof_obligation_id,
        evidence.proof_obligation_id,
      ],
      ["support_cell_id", previous.support_cell_id, evidence.support_cell_id],
      ["proof_type", previous.proof_type, evidence.proof_type],
      ["make_target", previous.run.make_target, evidence.run.make_target],
    ];
    for (const [binding, previousValue, currentValue] of scalarBindings) {
      if (previousValue !== currentValue) {
        errors.push(
          `${evidence.evidence_id} rerun changed ${binding} from ${JSON.stringify(previousValue)} to ${JSON.stringify(currentValue)}`,
        );
      }
    }
    if (JSON.stringify(previous.run.argv) !== JSON.stringify(evidence.run.argv)) {
      errors.push(`${evidence.evidence_id} rerun changed argv`);
    }
    if (!isDeepStrictEqual(previous.generator, evidence.generator)) {
      errors.push(`${evidence.evidence_id} rerun changed generator`);
    }
    if (!isDeepStrictEqual(previous.environment, evidence.environment)) {
      errors.push(`${evidence.evidence_id} rerun changed environment`);
    }
    if (
      Date.parse(previous.run.completed_at) >= Date.parse(evidence.run.started_at)
    ) {
      errors.push(
        `${evidence.evidence_id} rerun did not start after predecessor ${previous.evidence_id} completed`,
      );
    }
    for (const [binding, previousValues, currentValues] of [
      ["requirement_ids", previous.requirement_ids, evidence.requirement_ids],
      ["artifact_ids", previous.artifact_ids, evidence.artifact_ids],
    ]) {
      if (!stringSetsEqual(previousValues, currentValues)) {
        errors.push(`${evidence.evidence_id} rerun changed ${binding}`);
      }
    }
    if (evidencePromotionEligibilityErrors(previous).length === 0) {
      errors.push(
        `${evidence.evidence_id} rerun references promotion-eligible prior evidence ${previous.evidence_id}`,
      );
    }
    if (previous.run.exit_code === 0) {
      errors.push(
        `${evidence.evidence_id} rerun prior evidence ${previous.evidence_id} must have a nonzero exit_code`,
      );
    }
    if (previous.run.result !== "error") {
      errors.push(
        `${evidence.evidence_id} rerun prior evidence ${previous.evidence_id} must be an infrastructure error`,
      );
    }
    if (!previous.assertions.some(({ outcome }) => outcome === "error")) {
      errors.push(
        `${evidence.evidence_id} rerun prior evidence ${previous.evidence_id} must contain an error assertion`,
      );
    }
    if (
      previous.assertions.some(({ outcome }) =>
        ["failed", "skipped"].includes(outcome),
      )
    ) {
      errors.push(
        `${evidence.evidence_id} rerun prior evidence ${previous.evidence_id} must contain only infrastructure error outcomes`,
      );
    }
  }
  return errors;
}

function evidenceManifestBindingErrors(evidence, manifest) {
  const errors = [];
  for (const [binding, evidenceValue, manifestValue] of [
    ["candidate_id", evidence.candidate_id, manifest.candidate_id],
    ["release_version", evidence.release_version, manifest.release_version],
    ["protocol_version", evidence.protocol_version, manifest.protocol_version],
    ["source_commit", evidence.source_commit, manifest.source_commit],
    [
      "contract_snapshot_sha256",
      evidence.contract_snapshot_sha256,
      manifest.contract.snapshot_sha256,
    ],
  ]) {
    if (evidenceValue !== manifestValue) {
      errors.push(
        `${evidence.evidence_id} ${binding} ${JSON.stringify(evidenceValue)} does not match RC manifest value ${JSON.stringify(manifestValue)}`,
      );
    }
  }

  const manifestEvidence = manifest.evidence.find(
    ({ evidence_id }) => evidence_id === evidence.evidence_id,
  );
  if (!manifestEvidence) {
    errors.push(
      `${evidence.evidence_id} is not referenced by the RC manifest`,
    );
  } else {
    for (const [binding, manifestValue, evidenceValue] of [
      ["scenario_id", manifestEvidence.scenario_id, evidence.scenario_id],
      [
        "proof_obligation_id",
        manifestEvidence.proof_obligation_id,
        evidence.proof_obligation_id,
      ],
      ["support_cell_id", manifestEvidence.support_cell_id, evidence.support_cell_id],
      ["proof_type", manifestEvidence.proof_type, evidence.proof_type],
    ]) {
      if (manifestValue !== evidenceValue) {
        errors.push(
          `${evidence.evidence_id} ${binding} ${JSON.stringify(evidenceValue)} does not match RC manifest reference ${JSON.stringify(manifestValue)}`,
        );
      }
    }
  }

  if (
    !manifest.scenarios.some(
      ({ scenario_id }) => scenario_id === evidence.scenario_id,
    )
  ) {
    errors.push(
      `${evidence.evidence_id} scenario ${evidence.scenario_id} is absent from the RC manifest`,
    );
  }

  const artifactsById = new Map(
    manifest.artifacts.map((artifact) => [artifact.id, artifact]),
  );
  const artifactIds = new Set(artifactsById.keys());
  for (const artifactId of evidence.artifact_ids) {
    if (!artifactIds.has(artifactId)) {
      errors.push(
        `${evidence.evidence_id} artifact ${artifactId} is not bound by the RC manifest`,
      );
    }
  }
  if (
    evidence.run.rerun_approval !== null &&
    !manifest.trusted_rerun_approvers.includes(
      evidence.run.rerun_approval.approver_identity,
    )
  ) {
    errors.push(
      `${evidence.evidence_id} rerun approver is not trusted by the RC manifest`,
    );
  }
  return errors;
}

function manifestEvidenceClosureErrors(
  manifest,
  evidenceBundle,
  scenarioBundle,
  requirements,
  supportMatrix,
) {
  const errors = [];
  const candidatePathOwners = new Map();
  const recordCandidatePath = (path, owner) => {
    if (candidatePathOwners.has(path)) {
      errors.push(
        `RC candidate file path ${path} is shared by ${candidatePathOwners.get(path)} and ${owner}`,
      );
    } else {
      candidatePathOwners.set(path, owner);
    }
  };
  for (const scenario of manifest.scenarios) {
    recordCandidatePath(scenario.path, `scenario ${scenario.scenario_id}`);
  }
  for (const evidence of manifest.evidence) {
    recordCandidatePath(evidence.path, `evidence ${evidence.evidence_id}`);
  }
  for (const artifact of manifest.artifacts) {
    for (const payload of artifact.payloads) {
      recordCandidatePath(payload.path, `artifact ${artifact.id}`);
    }
  }
  for (const attestation of manifest.attestations) {
    recordCandidatePath(attestation.path, `attestation ${attestation.id}`);
    recordCandidatePath(
      attestation.sigstore_verification.bundle_path,
      `Sigstore bundle ${attestation.id}`,
    );
  }
  for (const evidence of evidenceBundle) {
    for (const attachment of evidence.attachments) {
      recordCandidatePath(
        attachment.path,
        `attachment ${attachment.id} in ${evidence.evidence_id}`,
      );
    }
  }
  const evidenceById = new Map(
    evidenceBundle.map((evidence) => [evidence.evidence_id, evidence]),
  );
  const manifestIds = new Set(
    manifest.evidence.map(({ evidence_id }) => evidence_id),
  );
  const manifestScenarioIds = new Set(
    manifest.scenarios.map(({ scenario_id }) => scenario_id),
  );
  const scenariosById = new Map(
    scenarioBundle.map((scenario) => [scenario.id, scenario]),
  );
  for (const reference of manifest.evidence) {
    if (!manifestScenarioIds.has(reference.scenario_id)) {
      errors.push(
        `RC manifest evidence ${reference.evidence_id} references absent scenario ${reference.scenario_id}`,
      );
    }
  }
  for (const scenarioId of manifestScenarioIds) {
    if (!scenariosById.has(scenarioId)) {
      errors.push(`RC manifest references unloaded scenario ${scenarioId}`);
    }
  }
  for (const scenario of scenarioBundle) {
    if (!manifestScenarioIds.has(scenario.id)) {
      errors.push(`RC manifest omits loaded scenario ${scenario.id}`);
    }
  }
  const requirementIds = new Set(
    requirements.requirements.map(({ id }) => id),
  );
  const authoredProofKeys = new Set();
  for (const scenario of scenarioBundle) {
    for (const obligation of scenario.proof_obligations) {
      for (const requirementId of obligation.requirement_ids) {
        if (!requirementIds.has(requirementId)) {
          errors.push(
            `${scenario.id} obligation ${obligation.obligation_id} references unknown requirement ${requirementId}`,
          );
          continue;
        }
        authoredProofKeys.add(
          `${requirementId}|${obligation.proof_type}|${obligation.support_cell_id ?? "neutral"}`,
        );
      }
    }
  }
  for (const requirement of requirements.requirements) {
    for (const proofType of requirement.required_proof_types) {
      const supportCellIds = requiredSupportCellIdsForProof(
        requirement,
        proofType,
        supportMatrix,
      );
      if (supportCellIds.length === 0) {
        errors.push(
          `RC manifest requirement ${requirement.id} has no applicable support cell for ${proofType}`,
        );
      }
      for (const supportCellId of supportCellIds) {
        if (
          !authoredProofKeys.has(
            `${requirement.id}|${proofType}|${supportCellId ?? "neutral"}`,
          )
        ) {
          errors.push(
            `RC manifest requirement ${requirement.id} lacks authored ${proofType} proof for support cell ${supportCellId ?? "neutral"}`,
          );
        }
      }
    }
  }
  for (const evidence of evidenceBundle) {
    if (!manifestScenarioIds.has(evidence.scenario_id)) {
      errors.push(
        `Evidence ${evidence.evidence_id} references scenario ${evidence.scenario_id} absent from the RC manifest`,
      );
    }
    if (!manifestIds.has(evidence.evidence_id)) {
      errors.push(
        `RC manifest omits loaded evidence ${evidence.evidence_id}`,
      );
    }
  }
  for (const evidenceId of manifestIds) {
    const evidence = evidenceById.get(evidenceId);
    if (!evidence) {
      errors.push(`RC manifest references unloaded evidence ${evidenceId}`);
      continue;
    }
    const reference = manifest.evidence.find(
      ({ evidence_id }) => evidence_id === evidenceId,
    );
    for (const [binding, referenceValue, evidenceValue] of [
      ["scenario_id", reference.scenario_id, evidence.scenario_id],
      [
        "proof_obligation_id",
        reference.proof_obligation_id,
        evidence.proof_obligation_id,
      ],
      ["support_cell_id", reference.support_cell_id, evidence.support_cell_id],
      ["proof_type", reference.proof_type, evidence.proof_type],
    ]) {
      if (referenceValue !== evidenceValue) {
        errors.push(
          `RC manifest evidence ${evidenceId} ${binding} does not match loaded evidence`,
        );
      }
    }
    let previousId = evidence.run.previous_evidence_id;
    const seen = new Set([evidenceId]);
    while (previousId !== null) {
      if (seen.has(previousId)) {
        errors.push(`RC manifest evidence chain contains cycle at ${previousId}`);
        break;
      }
      seen.add(previousId);
      if (!manifestIds.has(previousId)) {
        errors.push(
          `RC manifest omits predecessor evidence ${previousId} required by ${evidenceId}`,
        );
        break;
      }
      const previous = evidenceById.get(previousId);
      if (!previous) {
        errors.push(`RC manifest references unloaded predecessor ${previousId}`);
        break;
      }
      previousId = previous.run.previous_evidence_id;
    }
  }
  const successorIds = new Set(
    evidenceBundle
      .map(({ run }) => run.previous_evidence_id)
      .filter((evidenceId) => evidenceId !== null),
  );
  const terminalProofKeys = new Set();
  for (const scenario of scenarioBundle) {
    if (!manifestScenarioIds.has(scenario.id)) continue;
    for (const obligation of scenario.proof_obligations) {
      const matchingEvidence = evidenceBundle.filter(
        (evidence) =>
          evidence.scenario_id === scenario.id &&
          evidence.proof_obligation_id === obligation.obligation_id &&
          evidence.support_cell_id === obligation.support_cell_id &&
          evidence.proof_type === obligation.proof_type,
      );
      if (matchingEvidence.length === 0) {
        errors.push(
          `RC manifest scenario ${scenario.id} obligation ${obligation.obligation_id} has no evidence lineage`,
        );
        continue;
      }
      const terminalEvidence = matchingEvidence.filter(
        ({ evidence_id }) => !successorIds.has(evidence_id),
      );
      if (terminalEvidence.length !== 1) {
        errors.push(
          `RC manifest scenario ${scenario.id} obligation ${obligation.obligation_id} requires exactly one terminal evidence record, found ${terminalEvidence.length}`,
        );
      } else if (
        evidencePromotionEligibilityErrors(terminalEvidence[0]).length !== 0
      ) {
        errors.push(
          `RC manifest scenario ${scenario.id} obligation ${obligation.obligation_id} has no promotion-eligible terminal evidence`,
        );
      } else if (
        !stringSetsEqual(
          terminalEvidence[0].requirement_ids,
          obligation.requirement_ids,
        )
      ) {
        errors.push(
          `RC manifest scenario ${scenario.id} obligation ${obligation.obligation_id} terminal evidence claims different requirements`,
        );
      } else {
        for (const requirementId of obligation.requirement_ids) {
          terminalProofKeys.add(
            `${requirementId}|${obligation.proof_type}|${obligation.support_cell_id ?? "neutral"}`,
          );
        }
      }
    }
  }
  for (const requirement of requirements.requirements) {
    for (const proofType of requirement.required_proof_types) {
      for (const supportCellId of requiredSupportCellIdsForProof(
        requirement,
        proofType,
        supportMatrix,
      )) {
        if (
          !terminalProofKeys.has(
            `${requirement.id}|${proofType}|${supportCellId ?? "neutral"}`,
          )
        ) {
          errors.push(
            `RC manifest requirement ${requirement.id} lacks terminal ${proofType} evidence for support cell ${supportCellId ?? "neutral"}`,
          );
        }
      }
    }
  }
  return errors;
}

function requiredSupportCellIdsForProof(requirement, proofType, supportMatrix) {
  if (proofType === "reference-model" || proofType === "negative-control") {
    return [null];
  }
  let components;
  if (proofType === "server-black-box") {
    components = new Set(["postgresql-server"]);
  } else if (proofType === "native-e2e") {
    components = new Set(
      requirement.applicable_components.filter(
        (component) => component !== "postgresql-server",
      ),
    );
  } else {
    components = new Set(requirement.applicable_components);
  }
  return supportMatrix.cells
    .filter(
      ({ component, policy }) =>
        policy === "required" && components.has(component),
    )
    .map(({ id }) => id);
}

const requiredResolvedDimensions = new Map([
  ["postgresql-server|postgresql", ["postgresql", "os", "rust", "pgrx"]],
  [
    "swift-client|ios",
    ["ios", "xcode", "apple-sdk", "simulator-runtime", "swift"],
  ],
  ["swift-client|macos", ["macos", "xcode", "apple-sdk", "swift"]],
  [
    "kotlin-client|android",
    [
      "android-api",
      "android-sdk",
      "emulator-image",
      "jdk",
      "kotlin",
      "gradle",
    ],
  ],
  [
    "react-native-client|ios",
    [
      "ios",
      "xcode",
      "apple-sdk",
      "simulator-runtime",
      "node",
      "yarn",
      "react",
      "react-native",
      "swift",
      "cocoapods",
    ],
  ],
  [
    "react-native-client|android",
    [
      "android-api",
      "android-sdk",
      "emulator-image",
      "jdk",
      "gradle",
      "node",
      "yarn",
      "react",
      "react-native",
      "kotlin",
    ],
  ],
]);

function numericVersionSegments(version) {
  if (!/^[0-9]+(?:\.[0-9]+)*$/.test(version)) return null;
  return version.split(".").map(BigInt);
}

function compareNumericVersions(left, right) {
  const length = Math.max(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const leftSegment = left[index] ?? 0n;
    const rightSegment = right[index] ?? 0n;
    if (leftSegment < rightSegment) return -1;
    if (leftSegment > rightSegment) return 1;
  }
  return 0;
}

function resolvedSupportDimensionErrors(resolvedCell, authoredCell) {
  const errors = [];
  const dimensionsByName = new Map();
  for (const [index, dimension] of resolvedCell.dimensions.entries()) {
    if (dimensionsByName.has(dimension.name)) {
      errors.push(
        `${resolvedCell.support_cell_id} contains duplicate dimension name ${JSON.stringify(dimension.name)} at indexes ${dimensionsByName.get(dimension.name).index} and ${index}`,
      );
    } else {
      dimensionsByName.set(dimension.name, { ...dimension, index });
    }
  }

  if (!authoredCell || authoredCell.policy !== "required") return errors;

  const dimensionPolicyKey = `${authoredCell.component}|${authoredCell.platform}`;
  for (const name of requiredResolvedDimensions.get(dimensionPolicyKey) ?? []) {
    if (!dimensionsByName.has(name)) {
      errors.push(
        `${resolvedCell.support_cell_id} is missing required resolved dimension ${JSON.stringify(name)}`,
      );
    }
  }

  const platformDimensionName =
    authoredCell.platform === "android"
      ? "android-api"
      : authoredCell.platform;
  const platformDimension = dimensionsByName.get(platformDimensionName);
  if (authoredCell.platform_version.kind === "minimum" && platformDimension) {
    const resolvedSegments = numericVersionSegments(platformDimension.version);
    const minimumSegments = numericVersionSegments(
      authoredCell.platform_version.value,
    );
    if (!resolvedSegments) {
      errors.push(
        `${resolvedCell.support_cell_id} minimum platform dimension ${platformDimensionName} must use a dot-separated numeric version`,
      );
    } else if (minimumSegments && platformDimensionName === "android-api") {
      if (resolvedSegments.length !== 1) {
        errors.push(
          `${resolvedCell.support_cell_id} android-api must resolve to integer API ${authoredCell.platform_version.value}`,
        );
      } else if (resolvedSegments[0] < minimumSegments[0]) {
        errors.push(
          `${resolvedCell.support_cell_id} android-api version ${platformDimension.version} is below authored minimum API ${authoredCell.platform_version.value}`,
        );
      } else if (resolvedSegments[0] > minimumSegments[0]) {
        errors.push(
          `${resolvedCell.support_cell_id} android-api version ${platformDimension.version} is outside declared minimum API ${authoredCell.platform_version.value}`,
        );
      }
    } else if (minimumSegments) {
      if (resolvedSegments[0] < minimumSegments[0]) {
        errors.push(
          `${resolvedCell.support_cell_id} ${platformDimensionName} version ${platformDimension.version} is below authored minimum ${authoredCell.platform_version.value}.x line`,
        );
      } else if (resolvedSegments[0] > minimumSegments[0]) {
        errors.push(
          `${resolvedCell.support_cell_id} ${platformDimensionName} version ${platformDimension.version} is outside declared minimum line ${authoredCell.platform_version.value}.x`,
        );
      }
    }
  }

  if (authoredCell.component === "postgresql-server" && platformDimension) {
    const postgresSegments = numericVersionSegments(platformDimension.version);
    if (!postgresSegments || postgresSegments[0] !== 18n) {
      errors.push(
        `${resolvedCell.support_cell_id} postgresql dimension must resolve to major version 18`,
      );
    }
  }

  if (authoredCell.component === "react-native-client") {
    const runtimeVersion = dimensionsByName.get("react-native")?.version;
    if (
      runtimeVersion !== undefined &&
      !/^0\.83\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$/.test(runtimeVersion)
    ) {
      errors.push(
        `${resolvedCell.support_cell_id} react-native dimension must resolve within authored series 0.83.x`,
      );
    }
    const reactVersion = dimensionsByName.get("react")?.version;
    const reactSegments =
      reactVersion === undefined ? null : numericVersionSegments(reactVersion);
    if (
      reactVersion !== undefined &&
      (!reactSegments ||
        reactSegments[0] !== 19n ||
        compareNumericVersions(reactSegments, [19n, 2n]) < 0)
    ) {
      errors.push(
        `${resolvedCell.support_cell_id} react dimension must resolve within the supported React 19.2 or later line`,
      );
    }
  }
  return errors;
}

function manifestSemanticErrors(manifest, supportMatrix, artifactInventory) {
  const errors = [];
  const filePathOwners = new Map();
  const recordFilePath = (path, owner) => {
    if (filePathOwners.has(path)) {
      errors.push(
        `Manifest file path ${path} is shared by ${filePathOwners.get(path)} and ${owner}`,
      );
    } else {
      filePathOwners.set(path, owner);
    }
  };
  for (const [items, idKey, collection] of [
    [manifest.scenarios, "scenario_id", "Manifest scenarios"],
    [manifest.evidence, "evidence_id", "Manifest evidence"],
    [
      manifest.resolved_support_cells,
      "support_cell_id",
      "Manifest resolved support cells",
    ],
    [manifest.artifacts, "id", "Manifest artifacts"],
    [manifest.attestations, "id", "Manifest attestations"],
  ]) {
    errors.push(...duplicateLogicalIdErrors(items, idKey, collection));
  }
  for (const scenario of manifest.scenarios) {
    recordFilePath(scenario.path, `scenario ${scenario.scenario_id}`);
  }
  for (const evidence of manifest.evidence) {
    recordFilePath(evidence.path, `evidence ${evidence.evidence_id}`);
  }

  const supportCellsById = new Map(
    supportMatrix.cells.map((cell) => [cell.id, cell]),
  );
  const requiredIds = new Set(
    supportMatrix.cells
      .filter(({ policy }) => policy === "required")
      .map(({ id }) => id),
  );
  if (requiredIds.size !== 11) {
    errors.push(
      `Locked v0.3.0 support policy must contain 11 required cells, found ${requiredIds.size}`,
    );
  }

  const resolvedIds = new Set();
  for (const resolvedCell of manifest.resolved_support_cells) {
    const id = resolvedCell.support_cell_id;
    resolvedIds.add(id);
    const authoredCell = supportCellsById.get(id);
    errors.push(...resolvedSupportDimensionErrors(resolvedCell, authoredCell));
    if (!authoredCell) {
      errors.push(`Manifest contains unknown support cell ${id}`);
    } else if (authoredCell.policy !== "required") {
      errors.push(`Manifest contains excluded support cell ${id}`);
    }
  }
  for (const id of requiredIds) {
    if (!resolvedIds.has(id)) {
      errors.push(`Manifest is missing required support cell ${id}`);
    }
  }

  const requiredInventoryIds = new Set(
    artifactInventory.artifacts.map(({ id }) => id),
  );
  const observedInventoryIds = new Set();
  for (const artifact of manifest.artifacts) {
    for (const payload of artifact.payloads) {
      recordFilePath(payload.path, `artifact ${artifact.id}`);
    }
    if (observedInventoryIds.has(artifact.inventory_id)) {
      errors.push(
        `Manifest contains duplicate artifact inventory binding ${artifact.inventory_id}`,
      );
    }
    observedInventoryIds.add(artifact.inventory_id);
    if (!requiredInventoryIds.has(artifact.inventory_id)) {
      errors.push(`Manifest contains unknown artifact inventory binding ${artifact.inventory_id}`);
    }
  }
  for (const inventoryId of requiredInventoryIds) {
    if (!observedInventoryIds.has(inventoryId)) {
      errors.push(`Manifest is missing required artifact inventory binding ${inventoryId}`);
    }
  }

  const artifactsById = new Map(
    manifest.artifacts.map((artifact) => [artifact.id, artifact]),
  );
  const artifactIds = new Set(artifactsById.keys());
  const attestationsBySubjectAndKind = new Map();
  for (const attestation of manifest.attestations) {
    recordFilePath(attestation.path, `attestation ${attestation.id}`);
    recordFilePath(
      attestation.sigstore_verification.bundle_path,
      `Sigstore bundle for ${attestation.id}`,
    );
    if (!artifactIds.has(attestation.subject_artifact_id)) {
      errors.push(
        `Attestation ${attestation.id} references unknown artifact ${attestation.subject_artifact_id}`,
      );
    }
    const artifact = artifactsById.get(attestation.subject_artifact_id);
    if (artifact) {
      const payloadBindings = artifact.payloads.map(({ path, sha256 }) => ({
        path,
        sha256,
      }));
      if (
        attestation.subject_payloads.length !== payloadBindings.length ||
        !attestation.subject_payloads.every((subject) =>
          payloadBindings.some(
            (payload) =>
              payload.path === subject.path && payload.sha256 === subject.sha256,
          ),
        )
      ) {
        errors.push(
          `Attestation ${attestation.id} subject payloads do not exactly match artifact ${artifact.id}`,
        );
      }
      if (
        attestation.sigstore_verification.signed_attestation_sha256 !==
        attestation.sha256
      ) {
        errors.push(
          `Attestation ${attestation.id} Sigstore bundle does not bind the attestation digest`,
        );
      }
      if (
        attestation.sigstore_verification.signed_subjects.length !==
          payloadBindings.length ||
        !attestation.sigstore_verification.signed_subjects.every((subject) =>
          payloadBindings.some(
            (payload) =>
              payload.path === subject.path && payload.sha256 === subject.sha256,
          ),
        )
      ) {
        errors.push(
          `Attestation ${attestation.id} Sigstore subjects do not exactly match artifact ${artifact.id}`,
        );
      }
    }
    const key = `${attestation.subject_artifact_id}|${attestation.kind}`;
    attestationsBySubjectAndKind.set(
      key,
      (attestationsBySubjectAndKind.get(key) ?? 0) + 1,
    );
  }
  for (const artifactId of artifactIds) {
    for (const kind of ["sbom", "provenance"]) {
      const count = attestationsBySubjectAndKind.get(`${artifactId}|${kind}`) ?? 0;
      if (count !== 1) {
        errors.push(
          `Artifact ${artifactId} requires exactly one ${kind} attestation, found ${count}`,
        );
      }
    }
  }
  return errors;
}

const validInjectionRecipe = {
  mechanism: "wire-fault",
  target: "push.client_version timestamp decoder",
  operator: "replace",
  parameters: {
    scenario:
      "portable datetime mutation with a timestamp offset and missing microseconds",
    defect:
      "accept the non-UTC or noncanonical representation instead of rejecting before mutation state changes",
  },
};

const validScenario = {
  $schema: "https://synchro.dev/conformance/schemas/scenario-v2.schema.json",
  schema_version: 2,
  id: "SCN-TIME-001",
  title: "Reject a noncanonical wire timestamp",
  requirement_ids: ["SYNC-TIME-001"],
  normative_references: [
    {
      path: "docs/src/content/docs/spec/04-invariants.mdx",
      anchor: "#canonical-time-format",
    },
  ],
  proof_types: [
    "server-black-box",
    "native-e2e",
    "fault-injection",
    "negative-control",
  ],
  proof_obligations: [
    {
      obligation_id: "OBL-SERVER-001",
      requirement_ids: ["SYNC-TIME-001"],
      assertion_ids: ["ASSERT-TIME-001"],
      proof_type: "server-black-box",
      support_cell_id: "SUP-PG-018",
      artifact_inventory_ids: [
        "ARTDEF-PG-EXTENSION-001",
        "ARTDEF-ADAPTER-001",
      ],
      make_target: "test-blackbox",
      argv: ["make", "test-blackbox"],
    },
    {
      obligation_id: "OBL-NATIVE-001",
      requirement_ids: ["SYNC-TIME-001"],
      assertion_ids: ["ASSERT-TIME-001"],
      proof_type: "native-e2e",
      support_cell_id: "SUP-RN-IOS-MIN-001",
      artifact_inventory_ids: [
        "ARTDEF-PG-EXTENSION-001",
        "ARTDEF-ADAPTER-001",
        "ARTDEF-SWIFT-SPM-001",
        "ARTDEF-COCOAPODS-001",
        "ARTDEF-RN-NPM-001",
      ],
      make_target: "test-rn-e2e-ios",
      argv: ["make", "test-rn-e2e-ios"],
    },
    {
      obligation_id: "OBL-FAULT-001",
      requirement_ids: ["SYNC-TIME-001"],
      assertion_ids: ["ASSERT-TIME-001"],
      proof_type: "fault-injection",
      support_cell_id: "SUP-PG-018",
      artifact_inventory_ids: [
        "ARTDEF-PG-EXTENSION-001",
        "ARTDEF-ADAPTER-001",
      ],
      make_target: "test-blackbox",
      argv: ["make", "test-blackbox"],
    },
    {
      obligation_id: "OBL-NEGATIVE-001",
      requirement_ids: ["SYNC-TIME-001"],
      assertion_ids: ["ASSERT-TIME-001"],
      proof_type: "negative-control",
      support_cell_id: null,
      artifact_inventory_ids: [
        "ARTDEF-CONFORMANCE-RUNNER-001",
        "ARTDEF-RN-NPM-001",
      ],
      make_target: "test-conformance",
      argv: ["make", "test-conformance"],
    },
  ],
  model: {
    setup: [
      {
        contract_operation: "model",
        name: "install-current-contract",
        payload: {},
      },
    ],
    expected_state: [
      {
        id: "EXPECT-TIME-001",
        predicate: {
          contract_predicate: "state-equality",
          name: "state-unchanged",
          payload: {},
        },
      },
    ],
  },
  barrier_plan: {
    barriers: [
      {
        id: "BAR-TIME-001",
        name: "request-dispatch",
        release_order: 1,
        participants: ["client", "server"],
      },
    ],
  },
  fault_plans: [
    {
      id: "FPL-TIME-001",
      requirement_id: "SYNC-TIME-001",
      fault_id: "FAULT-TIME-001",
      control_id: "CTRL-TIMESTAMP-001",
      barrier_id: "BAR-TIME-001",
      expected_assertion_ids: ["ASSERT-TIME-001"],
      injection: validInjectionRecipe,
    },
  ],
  replay: {
    mode: "deterministic",
    seed_required: false,
    barrier_trace_required: true,
  },
  performance_budget_ids: [],
  required_measurement_ids: [],
  negative_controls: [
    {
      control_id: "CTRL-TIMESTAMP-001",
      requirement_id: "SYNC-TIME-001",
      fault_id: "FAULT-TIME-001",
      subject_artifact_inventory_ids: ["ARTDEF-RN-NPM-001"],
      detected_by: ["ASSERT-TIME-001"],
    },
  ],
  steps: [
    {
      id: "STEP-TIME-001",
      phase: "exercise",
      transport: "http",
      operation: {
        contract_operation: "push",
        name: "send-timestamp",
        payload: { value: "not-a-time" },
      },
    },
  ],
  wire_expectations: [
    {
      step_id: "STEP-TIME-001",
      assertion_id: "ASSERT-TIME-001",
      contract_case: "invalid_request",
      http_status: 400,
      error_code: "invalid_request",
      retryable: false,
    },
  ],
  assertions: [
    {
      id: "ASSERT-TIME-001",
      requirement_ids: ["SYNC-TIME-001"],
      description: "The invalid timestamp is rejected.",
      expectation_ids: ["EXPECT-TIME-001"],
      predicate: {
        contract_predicate: "wire-outcome",
        name: "canonical-wire-outcome",
        payload: {},
      },
      oracle: {
        kind: "wire-contract",
        expected_source: "authored-model",
        observed_source: "system-under-test",
      },
      detects_control_ids: ["CTRL-TIMESTAMP-001"],
    },
  ],
};

const validRequirementSubset = {
  requirements: [
    {
      id: "SYNC-TIME-001",
      required_proof_types: [
        "server-black-box",
        "native-e2e",
        "negative-control",
      ],
      applicable_components: [
        "postgresql-server",
        "react-native-client",
      ],
    },
  ],
};

const validClosureSupportMatrix = {
  cells: [
    {
      id: "SUP-PG-018",
      component: "postgresql-server",
      policy: "required",
    },
    {
      id: "SUP-RN-IOS-MIN-001",
      component: "react-native-client",
      policy: "required",
    },
  ],
};

const validEvidence = {
  $schema: "https://synchro.dev/conformance/schemas/evidence-v2.schema.json",
  schema_version: 2,
  evidence_id: "EVD-NATIVE-002",
  candidate_id: "RC-0.3.0-20260717T120000Z-abcdef0",
  release_version: "0.3.0",
  protocol_version: 3,
  contract_snapshot_sha256: "1".repeat(64),
  support_cell_id: "SUP-RN-IOS-MIN-001",
  scenario_id: "SCN-TIME-001",
  proof_obligation_id: "OBL-NATIVE-001",
  requirement_ids: ["SYNC-TIME-001"],
  proof_type: "native-e2e",
  source_commit: "a".repeat(40),
  generator: {
    name: "contract-self-test",
    version: "1.0.0",
    binary_sha256: "b".repeat(64),
  },
  run: {
    id: "RUN-NATIVE-002",
    execution_lineage_id: "EXEC-NATIVE-001",
    url: "https://ci.example.test/runs/2",
    make_target: "test-rn-e2e-ios",
    argv: ["make", "test-rn-e2e-ios"],
    attempt: 2,
    started_at: "2026-07-17T12:00:00Z",
    completed_at: "2026-07-17T12:01:00Z",
    duration_ms: 60000,
    result: "passed",
    exit_code: 0,
    previous_evidence_id: "EVD-NATIVE-001",
    rerun_cause: "compute-host-failure",
    rerun_diagnosis: "The original host lost network connectivity.",
    corrective_action: "The run moved to a healthy replacement host.",
    rerun_approval: {
      approver_identity: "github:release-manager",
      approved_at: "2026-07-17T11:59:00Z",
      uri: "https://ci.example.test/approvals/2",
    },
  },
  environment: [
    { name: "ios", value: "16.0.999" },
    { name: "xcode", value: "99.0.0+1.1" },
    { name: "apple-sdk", value: "99.0.0+1.1" },
    { name: "simulator-runtime", value: "99.0.0+1.1" },
    { name: "node", value: "99.0.0+1.1" },
    { name: "yarn", value: "99.0.0+1.1" },
    { name: "react", value: "19.2.999" },
    { name: "react-native", value: "0.83.999+1.1" },
    { name: "swift", value: "6.99.0+1.1" },
    { name: "cocoapods", value: "1.99.0+1.1" },
  ],
  assertions: [{ assertion_id: "ASSERT-TIME-001", outcome: "passed" }],
  attachments: [
    {
      id: "ATT-LOG-001",
      kind: "log",
      path: "evidence/run.log",
      media_type: "text/plain",
      sha256: "c".repeat(64),
    },
    {
      id: "ATT-TRACE-001",
      kind: "trace",
      path: "evidence/trace.json",
      media_type: "application/json",
      sha256: "d".repeat(64),
    },
    {
      id: "ATT-REPLAY-001",
      kind: "replay-data",
      path: "evidence/replay.json",
      media_type: "application/json",
      sha256: "e".repeat(64),
    },
    {
      id: "ATT-BARRIER-001",
      kind: "barrier-trace",
      path: "evidence/barrier-trace.json",
      media_type: "application/json",
      sha256: "f".repeat(64),
    },
    {
      id: "ATT-FAULT-001",
      kind: "fault-plan",
      path: "evidence/fault-plan.json",
      media_type: "application/json",
      sha256: "0".repeat(64),
    },
    {
      id: "ATT-NEGATIVE-001",
      kind: "negative-control",
      path: "evidence/negative-control.json",
      media_type: "application/json",
      sha256: "1".repeat(64),
    },
  ],
  execution_artifacts: {
    log_attachment_ids: ["ATT-LOG-001"],
    trace_attachment_ids: ["ATT-TRACE-001"],
    replay_data_attachment_ids: ["ATT-REPLAY-001"],
    barrier_trace_attachment_ids: ["ATT-BARRIER-001"],
  },
  replay: {
    seed: null,
    barrier_traces: [
      {
        barrier_id: "BAR-TIME-001",
        attachment_id: "ATT-BARRIER-001",
      },
    ],
  },
  fault_execution: null,
  performance_results: [],
  required_measurement_results: [],
  artifact_ids: [
    "ART-PG-EXTENSION-001",
    "ART-ADAPTER-001",
    "ART-SWIFT-001",
    "ART-COCOAPODS-001",
    "ART-RN-NPM-001",
  ],
  negative_control: null,
  seed: null,
};

function prefixEvidenceAttachmentPaths(evidence, prefix) {
  for (const attachment of evidence.attachments) {
    const fileName = attachment.path.split("/").at(-1);
    attachment.path = `evidence/${prefix}/${fileName}`;
  }
}
prefixEvidenceAttachmentPaths(validEvidence, "native-attempt-2");

const failedEvidenceAttemptOne = structuredClone(validEvidence);
prefixEvidenceAttachmentPaths(failedEvidenceAttemptOne, "native-attempt-1");
failedEvidenceAttemptOne.evidence_id = "EVD-NATIVE-001";
failedEvidenceAttemptOne.run = {
  id: "RUN-NATIVE-001",
  execution_lineage_id: "EXEC-NATIVE-001",
  url: "https://ci.example.test/runs/1",
  make_target: "test-rn-e2e-ios",
  argv: ["make", "test-rn-e2e-ios"],
  attempt: 1,
  started_at: "2026-07-17T11:55:00Z",
  completed_at: "2026-07-17T11:56:00Z",
  duration_ms: 60000,
  result: "error",
  exit_code: 1,
  previous_evidence_id: null,
  rerun_cause: null,
  rerun_diagnosis: null,
  corrective_action: null,
  rerun_approval: null,
};
failedEvidenceAttemptOne.assertions = [
  { assertion_id: "ASSERT-TIME-001", outcome: "error" },
];

const validNegativeControlEvidence = structuredClone(validEvidence);
prefixEvidenceAttachmentPaths(validNegativeControlEvidence, "negative-control");
validNegativeControlEvidence.evidence_id = "EVD-NEGATIVE-001";
validNegativeControlEvidence.proof_type = "negative-control";
validNegativeControlEvidence.proof_obligation_id = "OBL-NEGATIVE-001";
validNegativeControlEvidence.support_cell_id = null;
validNegativeControlEvidence.run = {
  id: "RUN-NEGATIVE-001",
  execution_lineage_id: "EXEC-NEGATIVE-001",
  url: "https://ci.example.test/runs/negative-control-1",
  make_target: "test-conformance",
  argv: ["make", "test-conformance"],
  attempt: 1,
  started_at: "2026-07-17T12:02:00Z",
  completed_at: "2026-07-17T12:03:00Z",
  duration_ms: 60000,
  result: "passed",
  exit_code: 0,
  previous_evidence_id: null,
  rerun_cause: null,
  rerun_diagnosis: null,
  corrective_action: null,
  rerun_approval: null,
};
validNegativeControlEvidence.artifact_ids = [
  "ART-CONFORMANCE-RUNNER-001",
  "ART-RN-NPM-001",
];
validNegativeControlEvidence.environment = [];
validNegativeControlEvidence.negative_control = {
  fault_id: "FAULT-TIME-001",
  control_id: "CTRL-TIMESTAMP-001",
  fault_plan_id: "FPL-TIME-001",
  fault_plan_attachment_id: "ATT-FAULT-001",
  control_subject_id: "CTRL-TIMESTAMP-001",
  control_subject_type: "synthetic-fault",
  control_subject_artifact_ids: ["ART-RN-NPM-001"],
  detected_by: ["ASSERT-TIME-001"],
  outcome: "detected",
  attachment_ids: ["ATT-NEGATIVE-001"],
};
validNegativeControlEvidence.fault_execution = {
  fault_plan_id: "FPL-TIME-001",
  fault_id: "FAULT-TIME-001",
  control_id: "CTRL-TIMESTAMP-001",
  fault_plan_attachment_id: "ATT-FAULT-001",
  subject_type: "synthetic-fault",
  detected_by: ["ASSERT-TIME-001"],
  injection: validInjectionRecipe,
};

const validFaultInjectionEvidence = structuredClone(
  validNegativeControlEvidence,
);
prefixEvidenceAttachmentPaths(validFaultInjectionEvidence, "fault-injection");
validFaultInjectionEvidence.evidence_id = "EVD-FAULT-001";
validFaultInjectionEvidence.proof_type = "fault-injection";
validFaultInjectionEvidence.proof_obligation_id = "OBL-FAULT-001";
validFaultInjectionEvidence.support_cell_id = "SUP-PG-018";
validFaultInjectionEvidence.artifact_ids = [
  "ART-PG-EXTENSION-001",
  "ART-ADAPTER-001",
];
validFaultInjectionEvidence.environment = [
  { name: "postgresql", value: "18.99" },
  { name: "os", value: "99.0.0+16F6" },
  { name: "rust", value: "1.99.0+1.1" },
  { name: "pgrx", value: "0.99.0+1.1" },
];
validFaultInjectionEvidence.negative_control = null;
validFaultInjectionEvidence.run.id = "RUN-FAULT-001";
validFaultInjectionEvidence.run.execution_lineage_id = "EXEC-FAULT-001";
validFaultInjectionEvidence.run.url = "https://ci.example.test/runs/fault-1";
validFaultInjectionEvidence.run.make_target = "test-blackbox";
validFaultInjectionEvidence.run.argv = ["make", "test-blackbox"];

const validServerEvidence = structuredClone(validNegativeControlEvidence);
prefixEvidenceAttachmentPaths(validServerEvidence, "server-blackbox");
validServerEvidence.evidence_id = "EVD-SERVER-001";
validServerEvidence.proof_type = "server-black-box";
validServerEvidence.proof_obligation_id = "OBL-SERVER-001";
validServerEvidence.support_cell_id = "SUP-PG-018";
validServerEvidence.artifact_ids = [
  "ART-PG-EXTENSION-001",
  "ART-ADAPTER-001",
];
validServerEvidence.environment = structuredClone(
  validFaultInjectionEvidence.environment,
);
validServerEvidence.negative_control = null;
validServerEvidence.fault_execution = null;
validServerEvidence.run.id = "RUN-SERVER-001";
validServerEvidence.run.execution_lineage_id = "EXEC-SERVER-001";
validServerEvidence.run.url = "https://ci.example.test/runs/server-1";
validServerEvidence.run.make_target = "test-blackbox";
validServerEvidence.run.argv = ["make", "test-blackbox"];

const validEvidenceBundle = [
  failedEvidenceAttemptOne,
  validEvidence,
  validServerEvidence,
  validFaultInjectionEvidence,
  validNegativeControlEvidence,
];

const validArtifactBindings = [
  ["ART-CONFORMANCE-RUNNER-001", "ARTDEF-CONFORMANCE-RUNNER-001"],
  ["ART-PG-EXTENSION-001", "ARTDEF-PG-EXTENSION-001"],
  ["ART-PG-SQL-001", "ARTDEF-PG-SQL-001"],
  ["ART-ADAPTER-001", "ARTDEF-ADAPTER-001"],
  ["ART-SEED-TOOL-001", "ARTDEF-SEED-TOOL-001"],
  ["ART-SWIFT-001", "ARTDEF-SWIFT-SPM-001"],
  ["ART-COCOAPODS-001", "ARTDEF-COCOAPODS-001"],
  ["ART-KOTLIN-MAVEN-001", "ARTDEF-KOTLIN-MAVEN-001"],
  ["ART-RN-NPM-001", "ARTDEF-RN-NPM-001"],
  ["ART-PORTABLE-SEED-001", "ARTDEF-PORTABLE-SEED-001"],
];

const validArtifacts = validArtifactBindings.map(
  ([id, inventory_id], index) => ({
    id,
    inventory_id,
    release_version: "0.3.0",
    package_version: "0.3.0",
    payloads: [
      {
        path: `artifacts/${inventory_id.toLowerCase()}.bin`,
        media_type: "application/octet-stream",
        sha256: (index + 1).toString(16).repeat(64),
      },
    ],
  }),
);

const validAttestations = validArtifacts.flatMap((artifact, index) =>
  ["sbom", "provenance"].map((kind, kindIndex) => ({
    id: `ATTST-${kind.toUpperCase()}-${String(index * 2 + kindIndex + 1).padStart(3, "0")}`,
    kind,
    format: kind === "sbom" ? "spdx-json" : "slsa-provenance-v1",
    media_type:
      kind === "sbom"
        ? "application/spdx+json"
        : "application/vnd.in-toto+json",
    subject_artifact_id: artifact.id,
    subject_payloads: artifact.payloads.map(({ path, sha256 }) => ({
      path,
      sha256,
    })),
    path: `attestations/${artifact.id.toLowerCase()}-${kind}.json`,
    sha256: ((index * 2 + kindIndex + 1) % 16).toString(16).repeat(64),
    sigstore_verification: {
      bundle_path: `attestations/${artifact.id.toLowerCase()}-${kind}.sigstore.json`,
      bundle_media_type:
        "application/vnd.dev.sigstore.bundle+json;version=0.3",
      bundle_sha256: ((index * 2 + kindIndex + 2) % 16)
        .toString(16)
        .repeat(64),
      signed_attestation_sha256: ((index * 2 + kindIndex + 1) % 16)
        .toString(16)
        .repeat(64),
      signed_subjects: artifact.payloads.map(({ path, sha256 }) => ({
        path,
        sha256,
      })),
      certificate_issuer: "https://token.actions.githubusercontent.com",
      certificate_identity:
        "https://github.com/trainstar/synchro/.github/workflows/release.yml@refs/tags/v0.3.0",
      verifier: {
        name: "cosign",
        version: "2.4.1",
        binary_sha256: "a".repeat(64),
      },
      verified_at: "2026-07-17T11:50:00Z",
      verification_uri: `https://search.sigstore.dev/?logIndex=${index * 2 + kindIndex + 1}`,
      verified: true,
    },
  })),
);

const validManifest = {
  $schema: "https://synchro.dev/conformance/schemas/rc-manifest-v2.schema.json",
  schema_version: 2,
  candidate_id: "RC-0.3.0-20260717T120000Z-abcdef0",
  release_version: "0.3.0",
  protocol_version: 3,
  source_commit: "a".repeat(40),
  created_at: "2026-07-17T12:00:00Z",
  generator: {
    name: "rc-generator",
    version: "1.0.0",
    binary_sha256: "b".repeat(64),
  },
  trusted_rerun_approvers: ["github:release-manager"],
  contract: {
    snapshot_sha256: "1".repeat(64),
    requirements: {
      path: "conformance/requirements.json",
      sha256: "c".repeat(64),
    },
    support_matrix: {
      path: "conformance/support-matrix.json",
      sha256: "d".repeat(64),
    },
    behavioral_files: [
      ["docs/src/content/docs/spec/00-principles.mdx", null],
      ["docs/src/content/docs/spec/01-wire-protocol.mdx", null],
      ["docs/src/content/docs/spec/02-client-contract.mdx", null],
      ["docs/src/content/docs/spec/03-state-machines.mdx", null],
      ["docs/src/content/docs/spec/04-invariants.mdx", null],
      ["docs/src/content/docs/spec/05-schema-evolution.mdx", null],
      ["docs/src/content/docs/architecture/decisions/001-wal-change-stream.mdx", "Accepted"],
      ["docs/src/content/docs/architecture/decisions/002-mutation-idempotency-and-conflicts.mdx", "Accepted"],
      ["docs/src/content/docs/architecture/decisions/003-pull-cursor-and-rebuild.mdx", "Accepted"],
      ["docs/src/content/docs/architecture/decisions/004-membership-schema-and-retention.mdx", "Accepted"],
      ["docs/src/content/docs/architecture/decisions/005-integrity-authorization-and-seeds.mdx", "Accepted"],
    ].map(([path, status], index) => ({
      path,
      status,
      sha256: index.toString(16).padStart(64, "0"),
    })),
    verification_inputs: {
      scenario_catalog: {
        path: "conformance/catalog.json",
        sha256: "6".repeat(64),
      },
      vector_catalog: {
        path: "conformance/vectors/catalog.json",
        sha256: "7".repeat(64),
      },
      fault_catalog: {
        path: "conformance/faults/catalog.json",
        sha256: "8".repeat(64),
      },
      performance_budgets: {
        path: "conformance/performance/budgets.json",
        sha256: "9".repeat(64),
      },
      artifact_inventory: {
        path: "conformance/artifacts/inventory.json",
        sha256: "a".repeat(64),
      },
    },
    schema_files: {
      requirements: { path: "conformance/schemas/requirements-v2.schema.json", sha256: "1".repeat(64) },
      support_matrix: { path: "conformance/schemas/support-matrix.schema.json", sha256: "2".repeat(64) },
      scenario: { path: "conformance/schemas/scenario-v2.schema.json", sha256: "3".repeat(64) },
      evidence: { path: "conformance/schemas/evidence-v2.schema.json", sha256: "4".repeat(64) },
      rc_manifest: { path: "conformance/schemas/rc-manifest-v2.schema.json", sha256: "5".repeat(64) },
      fault_catalog: { path: "conformance/schemas/fault-catalog-v1.schema.json", sha256: "6".repeat(64) },
      artifact_inventory: { path: "conformance/schemas/artifact-inventory-v1.schema.json", sha256: "7".repeat(64) },
      performance_budgets: { path: "conformance/schemas/performance-budgets-v2.schema.json", sha256: "8".repeat(64) },
    },
  },
  scenarios: [
    {
      scenario_id: "SCN-TIME-001",
      path: "conformance/scenarios/time.json",
      sha256: "e".repeat(64),
    },
  ],
  evidence: [
    {
      evidence_id: "EVD-NATIVE-001",
      scenario_id: "SCN-TIME-001",
      proof_obligation_id: "OBL-NATIVE-001",
      support_cell_id: "SUP-RN-IOS-MIN-001",
      proof_type: "native-e2e",
      path: "evidence/native-attempt-1.json",
      sha256: "e".repeat(64),
    },
    {
      evidence_id: "EVD-NATIVE-002",
      scenario_id: "SCN-TIME-001",
      proof_obligation_id: "OBL-NATIVE-001",
      support_cell_id: "SUP-RN-IOS-MIN-001",
      proof_type: "native-e2e",
      path: "evidence/native.json",
      sha256: "f".repeat(64),
    },
    {
      evidence_id: "EVD-SERVER-001",
      scenario_id: "SCN-TIME-001",
      proof_obligation_id: "OBL-SERVER-001",
      support_cell_id: "SUP-PG-018",
      proof_type: "server-black-box",
      path: "evidence/server.json",
      sha256: "0".repeat(64),
    },
    {
      evidence_id: "EVD-FAULT-001",
      scenario_id: "SCN-TIME-001",
      proof_obligation_id: "OBL-FAULT-001",
      support_cell_id: "SUP-PG-018",
      proof_type: "fault-injection",
      path: "evidence/fault.json",
      sha256: "1".repeat(64),
    },
    {
      evidence_id: "EVD-NEGATIVE-001",
      scenario_id: "SCN-TIME-001",
      proof_obligation_id: "OBL-NEGATIVE-001",
      support_cell_id: null,
      proof_type: "negative-control",
      path: "evidence/negative-control.json",
      sha256: "2".repeat(64),
    },
  ],
  resolved_support_cells: [
    {
      support_cell_id: "SUP-PG-018",
      dimensions: [
        { name: "postgresql", version: "18.99" },
        { name: "os", version: "99.0.0+16F6" },
        { name: "rust", version: "1.99.0+1.1" },
        { name: "pgrx", version: "0.99.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-IOS-MIN-001",
      dimensions: [
        { name: "ios", version: "16.0.999" },
        { name: "xcode", version: "99.0.0+1.1" },
        { name: "apple-sdk", version: "99.0.0+1.1" },
        { name: "simulator-runtime", version: "99.0.0+1.1" },
        { name: "swift", version: "6.99.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-IOS-CURRENT-001",
      dimensions: [
        { name: "ios", version: "99.0.999" },
        { name: "xcode", version: "99.0.0+1.1" },
        { name: "apple-sdk", version: "99.0.0+1.1" },
        { name: "simulator-runtime", version: "99.0.0+1.1" },
        { name: "swift", version: "6.99.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-MACOS-MIN-001",
      dimensions: [
        { name: "macos", version: "13.0.999" },
        { name: "xcode", version: "99.0.0+1.1" },
        { name: "apple-sdk", version: "99.0.0+1.1" },
        { name: "swift", version: "6.99.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-MACOS-CURRENT-001",
      dimensions: [
        { name: "macos", version: "99.0.999" },
        { name: "xcode", version: "99.0.0+1.1" },
        { name: "apple-sdk", version: "99.0.0+1.1" },
        { name: "swift", version: "6.99.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-ANDROID-MIN-001",
      dimensions: [
        { name: "android-api", version: "24" },
        { name: "android-sdk", version: "99.0.0+1.1" },
        { name: "emulator-image", version: "99.0.0+1.1" },
        { name: "jdk", version: "99.0.0+1.1" },
        { name: "kotlin", version: "9.99.0+1.1" },
        { name: "gradle", version: "99.0.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-ANDROID-CURRENT-001",
      dimensions: [
        { name: "android-api", version: "999" },
        { name: "android-sdk", version: "99.0.0+1.1" },
        { name: "emulator-image", version: "99.0.0+1.1" },
        { name: "jdk", version: "99.0.0+1.1" },
        { name: "kotlin", version: "9.99.0+1.1" },
        { name: "gradle", version: "99.0.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-RN-IOS-MIN-001",
      dimensions: [
        { name: "ios", version: "16.0.999" },
        { name: "xcode", version: "99.0.0+1.1" },
        { name: "apple-sdk", version: "99.0.0+1.1" },
        { name: "simulator-runtime", version: "99.0.0+1.1" },
        { name: "node", version: "99.0.0+1.1" },
        { name: "yarn", version: "99.0.0+1.1" },
        { name: "react", version: "19.2.999" },
        { name: "react-native", version: "0.83.999+1.1" },
        { name: "swift", version: "6.99.0+1.1" },
        { name: "cocoapods", version: "1.99.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-RN-IOS-CURRENT-001",
      dimensions: [
        { name: "ios", version: "99.0.999" },
        { name: "xcode", version: "99.0.0+1.1" },
        { name: "apple-sdk", version: "99.0.0+1.1" },
        { name: "simulator-runtime", version: "99.0.0+1.1" },
        { name: "node", version: "99.0.0+1.1" },
        { name: "yarn", version: "99.0.0+1.1" },
        { name: "react", version: "19.2.999" },
        { name: "react-native", version: "0.83.999+1.1" },
        { name: "swift", version: "6.99.0+1.1" },
        { name: "cocoapods", version: "1.99.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-RN-ANDROID-MIN-001",
      dimensions: [
        { name: "android-api", version: "24" },
        { name: "android-sdk", version: "99.0.0+1.1" },
        { name: "emulator-image", version: "99.0.0+1.1" },
        { name: "jdk", version: "99.0.0+1.1" },
        { name: "gradle", version: "99.0.0+1.1" },
        { name: "node", version: "99.0.0+1.1" },
        { name: "yarn", version: "99.0.0+1.1" },
        { name: "react", version: "19.2.999" },
        { name: "react-native", version: "0.83.999+1.1" },
        { name: "kotlin", version: "9.99.0+1.1" },
      ],
    },
    {
      support_cell_id: "SUP-RN-ANDROID-CURRENT-001",
      dimensions: [
        { name: "android-api", version: "999" },
        { name: "android-sdk", version: "99.0.0+1.1" },
        { name: "emulator-image", version: "99.0.0+1.1" },
        { name: "jdk", version: "99.0.0+1.1" },
        { name: "gradle", version: "99.0.0+1.1" },
        { name: "node", version: "99.0.0+1.1" },
        { name: "yarn", version: "99.0.0+1.1" },
        { name: "react", version: "19.2.999" },
        { name: "react-native", version: "0.83.999+1.1" },
        { name: "kotlin", version: "9.99.0+1.1" },
      ],
    },
  ],
  artifacts: validArtifacts,
  attestations: validAttestations,
};

async function main() {
  const ajv = new Ajv2020({ allErrors: true, strict: false, validateSchema: true });
  addFormats(ajv, { formats: ["date-time", "uri"], mode: "full" });
  repositoryMakeTargets = parseMakeTargets(
    await readFile(resolve(repoRoot, "Makefile"), "utf8"),
  );

  const validators = {};
  for (const [name, fileName] of Object.entries(schemaFiles)) {
    const path = resolve(conformanceDir, "schemas", fileName);
    const schema = await readJson(path, `Schema ${fileName}`);
    if (!schema) continue;
    try {
      validators[name] = ajv.compile(schema);
    } catch (error) {
      fail(`Schema ${fileName} failed compilation or meta-validation: ${error.message}`);
    }
  }

  const requirements = await readJson(
    resolve(conformanceDir, "requirements.json"),
    "conformance/requirements.json",
  );
  const supportMatrix = await readJson(
    resolve(conformanceDir, "support-matrix.json"),
    "conformance/support-matrix.json",
  );
  const faultCatalog = await readJson(
    resolve(conformanceDir, "faults/catalog.json"),
    "conformance/faults/catalog.json",
  );
  const artifactInventory = await readJson(
    resolve(conformanceDir, "artifacts/inventory.json"),
    "conformance/artifacts/inventory.json",
  );
  const performanceBudgets = await readJson(
    resolve(conformanceDir, "performance/budgets.json"),
    "conformance/performance/budgets.json",
  );
  const invariantSource = await readFile(
    resolve(repoRoot, "docs/src/content/docs/spec/04-invariants.mdx"),
    "utf8",
  );

  const requirementsValid =
    requirements !== null &&
    validateInstance(
      validators.requirements,
      requirements,
      "conformance/requirements.json",
    );
  const supportMatrixValid =
    supportMatrix !== null &&
    validateInstance(
      validators.supportMatrix,
      supportMatrix,
      "conformance/support-matrix.json",
    );
  const faultCatalogValid =
    faultCatalog !== null &&
    validateInstance(
      validators.faultCatalog,
      faultCatalog,
      "conformance/faults/catalog.json",
    );
  const artifactInventoryValid =
    artifactInventory !== null &&
    validateInstance(
      validators.artifactInventory,
      artifactInventory,
      "conformance/artifacts/inventory.json",
    );
  const performanceBudgetsValid =
    performanceBudgets !== null &&
    validateInstance(
      validators.performanceBudgets,
      performanceBudgets,
      "conformance/performance/budgets.json",
    );

  if (artifactInventoryValid) {
    recordSemanticErrors(
      artifactInventorySemanticErrors(artifactInventory),
      "Artifact inventory semantic validation",
    );
  }
  if (faultCatalogValid && requirementsValid) {
    recordSemanticErrors(
      faultCatalogSemanticErrors(faultCatalog, requirements),
      "Fault catalog semantic validation",
    );

    const faultCatalogWithoutControl = structuredClone(faultCatalog);
    const removedControl = faultCatalogWithoutControl.controls.shift();
    faultCatalogWithoutControl.faults =
      faultCatalogWithoutControl.faults.filter(
        ({ id }) => id !== removedControl.fault_id,
      );
    if (
      validateInstance(
        validators.faultCatalog,
        faultCatalogWithoutControl,
        "Schema-valid missing fault control helper control",
      )
    ) {
      expectSemanticInvalid(
        faultCatalogSemanticErrors(faultCatalogWithoutControl, requirements),
        "Missing requirement negative-control coverage helper control",
        (error) =>
          error.includes(removedControl.requirement_ids[0]) &&
          error.includes("found 0"),
      );
    }

    const faultCatalogWithWrongReference = structuredClone(faultCatalog);
    faultCatalogWithWrongReference.controls[0].normative_references = [
      "docs/src/content/docs/spec/04-invariants.mdx#wrong-invariant",
    ];
    if (
      validateInstance(
        validators.faultCatalog,
        faultCatalogWithWrongReference,
        "Schema-valid misbound fault reference helper control",
      )
    ) {
      expectSemanticInvalid(
        faultCatalogSemanticErrors(
          faultCatalogWithWrongReference,
          requirements,
        ),
        "Misbound fault normative reference helper control",
        (error) => error.includes("normative references do not exactly match"),
      );
    }

    const faultCatalogWithSharedControl = structuredClone(faultCatalog);
    faultCatalogWithSharedControl.controls[0].requirement_ids.push(
      faultCatalogWithSharedControl.controls[1].requirement_ids[0],
    );
    expectInvalid(
      validators.faultCatalog,
      faultCatalogWithSharedControl,
      "Fault control owned by multiple requirements",
      (errors) =>
        errors.some(
          (error) =>
            error.instancePath === "/controls/0/requirement_ids" &&
            error.keyword === "maxItems",
        ),
    );
  }
  if (
    performanceBudgetsValid &&
    supportMatrixValid &&
    artifactInventoryValid
  ) {
    recordSemanticErrors(
      performanceCatalogSemanticErrors(
        performanceBudgets,
        supportMatrix,
        artifactInventory,
      ),
      "Performance catalog semantic validation",
    );

    const performanceWithoutBudget = structuredClone(performanceBudgets);
    performanceWithoutBudget.budgets.shift();
    if (
      validateInstance(
        validators.performanceBudgets,
        performanceWithoutBudget,
        "Schema-valid missing locked performance budget helper control",
      )
    ) {
      expectSemanticInvalid(
        performanceCatalogSemanticErrors(
          performanceWithoutBudget,
          supportMatrix,
          artifactInventory,
        ),
        "Missing locked performance budget helper control",
        (error) => error.includes("budget IDs do not match"),
      );
    }

    const performanceWithWeakenedExactBudget = structuredClone(
      performanceBudgets,
    );
    performanceWithWeakenedExactBudget.budgets[0].comparator = "lte";
    if (
      validateInstance(
        validators.performanceBudgets,
        performanceWithWeakenedExactBudget,
        "Schema-valid weakened exact performance budget helper control",
      )
    ) {
      expectSemanticInvalid(
        performanceCatalogSemanticErrors(
          performanceWithWeakenedExactBudget,
          supportMatrix,
          artifactInventory,
        ),
        "Weakened exact performance budget helper control",
        (error) => error.includes("locked metric, comparator, and limit"),
      );
    }

    for (const [label, mutate] of [
      ["unit", (catalog) => (catalog.budgets[0].unit = "hops")],
      [
        "scenario",
        (catalog) =>
          (catalog.budgets[0].scenario_id = "SCN-PERF-STEADY-PULL-001"),
      ],
      ["support cells", (catalog) => catalog.budgets[0].support_cell_ids.pop()],
      [
        "artifacts",
        (catalog) => catalog.budgets[0].artifact_inventory_ids.pop(),
      ],
      [
        "data profile",
        (catalog) =>
          (catalog.budgets[0].data_profile.parameters.pending_mutation_count = 1),
      ],
      [
        "measurement method",
        (catalog) =>
          (catalog.budgets[0].measurement_method.instrumentation =
            "protocol-trace"),
      ],
      [
        "characterization strata",
        (catalog) =>
          (catalog.required_measurements[0].strata[0].parameters.fanout_tier =
            "substituted"),
      ],
      [
        "characterization metrics",
        (catalog) =>
          (catalog.required_measurements[0].metrics[0].name =
            "substituted_metric"),
      ],
    ]) {
      const mutatedCatalog = structuredClone(performanceBudgets);
      mutate(mutatedCatalog);
      if (
        validateInstance(
          validators.performanceBudgets,
          mutatedCatalog,
          `Schema-valid changed performance ${label} snapshot control`,
        )
      ) {
        expectSemanticInvalid(
          performanceCatalogSemanticErrors(
            mutatedCatalog,
            supportMatrix,
            artifactInventory,
          ),
          `Changed performance ${label} snapshot control`,
          (error) => error.includes("locked v0.3.0 semantic snapshot"),
        );
      }
    }
  }

  if (requirements !== null) {
    checkForbiddenKeys(requirements, "conformance/requirements.json");
  }
  if (supportMatrix !== null) {
    checkForbiddenKeys(supportMatrix, "conformance/support-matrix.json");
  }
  if (requirementsValid && supportMatrixValid) {
    recordSemanticErrors(
      authoredIdErrors(requirements, supportMatrix),
      "Authored ID semantic validation",
    );
    recordSemanticErrors(
      supportPolicyErrors(requirements, supportMatrix),
      "Support policy semantic validation",
    );
    recordSemanticErrors(
      await normativeReferenceErrors(requirements),
      "Normative reference semantic validation",
    );
    recordSemanticErrors(
      invariantCoverageErrors(requirements, invariantSource),
      "Invariant requirement coverage validation",
    );

    const requirementWithoutNegativeControl = structuredClone(requirements);
    requirementWithoutNegativeControl.requirements[0].required_proof_types =
      requirementWithoutNegativeControl.requirements[0].required_proof_types.filter(
        (proofType) => proofType !== "negative-control",
      );
    expectInvalid(
      validators.requirements,
      requirementWithoutNegativeControl,
      "Requirement without mandatory negative-control proof",
      (errors) =>
        errors.some(
          (error) =>
            error.instancePath ===
              "/requirements/0/required_proof_types" &&
            error.keyword === "contains",
        ),
    );

    const requirementWithWrongApplicability = structuredClone(requirements);
    requirementWithWrongApplicability.requirements[0].applicable_components =
      requirementWithWrongApplicability.requirements[0].applicable_components.filter(
        (component) => component !== "postgresql-server",
      );
    expectSemanticInvalid(
      supportPolicyErrors(requirementWithWrongApplicability, supportMatrix),
      "Requirement proof and component applicability mismatch control",
      (error) =>
        error.includes(
          "server proof and applicable PostgreSQL component must be declared together",
        ),
    );

    const duplicateAuthoredId = structuredClone(requirements);
    const duplicateRequirement = structuredClone(
      duplicateAuthoredId.requirements[0],
    );
    duplicateRequirement.title = "Duplicate authored ID control";
    duplicateAuthoredId.requirements.push(duplicateRequirement);
    if (
      validateInstance(
        validators.requirements,
        duplicateAuthoredId,
        "Schema-valid duplicate authored ID helper control",
      )
    ) {
      expectSemanticInvalid(
        authoredIdErrors(duplicateAuthoredId, supportMatrix),
        "Duplicate authored ID helper control",
        (error) => error.includes("Duplicate authored ID"),
      );
    }

    const supportMatrixWithoutPg18 = structuredClone(supportMatrix);
    supportMatrixWithoutPg18.cells = supportMatrixWithoutPg18.cells.filter(
      ({ id }) => id !== "SUP-PG-018",
    );
    if (
      validateInstance(
        validators.supportMatrix,
        supportMatrixWithoutPg18,
        "Schema-valid missing PG18 support policy helper control",
      )
    ) {
      expectSemanticInvalid(
        supportPolicyErrors(requirements, supportMatrixWithoutPg18),
        "Missing PG18 support policy helper control",
        (error) =>
          error.includes("Missing required support policy cell") &&
          error.includes("exact:18"),
      );
    }

    const requirementWithMissingAnchor = structuredClone(requirements);
    requirementWithMissingAnchor.requirements[0].normative_references[0].anchor =
      "#missing-normative-heading";
    if (
      validateInstance(
        validators.requirements,
        requirementWithMissingAnchor,
        "Schema-valid missing normative heading helper control",
      )
    ) {
      expectSemanticInvalid(
        await normativeReferenceErrors(requirementWithMissingAnchor),
        "Missing normative heading helper control",
        (error) => error.includes("references a missing heading anchor"),
      );
    }

    const requirementMappedToSection = structuredClone(requirements);
    requirementMappedToSection.requirements[0].normative_references[0].anchor =
      "#protocol-invariants";
    if (
      validateInstance(
        validators.requirements,
        requirementMappedToSection,
        "Schema-valid non-invariant requirement mapping control",
      )
    ) {
      expectSemanticInvalid(
        invariantCoverageErrors(requirementMappedToSection, invariantSource),
        "Requirement mapped to non-invariant heading control",
        (error) => error.includes("references non-invariant heading"),
      );
    }

    const requirementsWithoutFirstInvariant = structuredClone(requirements);
    requirementsWithoutFirstInvariant.requirements.shift();
    if (
      validateInstance(
        validators.requirements,
        requirementsWithoutFirstInvariant,
        "Schema-valid missing invariant mapping helper control",
      )
    ) {
      expectSemanticInvalid(
        invariantCoverageErrors(requirementsWithoutFirstInvariant, invariantSource),
        "Missing invariant mapping helper control",
        (error) => error.includes("has no release requirement"),
      );
    }

    const requirementsWithDuplicateInvariant = structuredClone(requirements);
    requirementsWithDuplicateInvariant.requirements[1].normative_references =
      structuredClone(
        requirementsWithDuplicateInvariant.requirements[0].normative_references,
      );
    if (
      validateInstance(
        validators.requirements,
        requirementsWithDuplicateInvariant,
        "Schema-valid duplicate invariant mapping helper control",
      )
    ) {
      expectSemanticInvalid(
        invariantCoverageErrors(requirementsWithDuplicateInvariant, invariantSource),
        "Duplicate invariant mapping helper control",
        (error) => error.includes("maps to multiple release requirements"),
      );
    }
  }

  if (
    validateInstance(validators.scenario, validScenario, "Valid scenario self-test")
  ) {
    expectSemanticValid(
      scenarioSemanticErrors(validScenario),
      "Valid scenario semantic self-test",
    );
    expectSemanticValid(
      await scenarioNormativeReferenceErrors(validScenario),
      "Valid scenario normative reference self-test",
    );
    if (
      requirementsValid &&
      supportMatrixValid &&
      artifactInventoryValid &&
      faultCatalogValid &&
      performanceBudgetsValid
    ) {
      expectSemanticValid(
        authoredScenarioBindingErrors(
          validScenario,
          requirements,
          supportMatrix,
          artifactInventory,
          faultCatalog,
          performanceBudgets,
        ),
        "Valid authored scenario catalog binding self-test",
      );
    }
  }

  const scenarioWithObligationOverclaim = structuredClone(validScenario);
  scenarioWithObligationOverclaim.proof_obligations[0].requirement_ids.push(
    "SYNC-TIME-002",
  );
  if (
    validateInstance(
      validators.scenario,
      scenarioWithObligationOverclaim,
      "Schema-valid proof obligation requirement overclaim control",
    )
  ) {
    expectSemanticInvalid(
      scenarioSemanticErrors(scenarioWithObligationOverclaim),
      "Proof obligation requirement overclaim control",
      (error) => error.includes("outside scenario SCN-TIME-001"),
    );
  }

  const scenarioWithUndefinedMakeTarget = structuredClone(validScenario);
  scenarioWithUndefinedMakeTarget.proof_obligations[0].make_target =
    "test-blackbox-missing";
  scenarioWithUndefinedMakeTarget.proof_obligations[0].argv = [
    "make",
    "test-blackbox-missing",
  ];
  expectSemanticInvalid(
    scenarioSemanticErrors(scenarioWithUndefinedMakeTarget),
    "Proof obligation undefined Make target control",
    (error) => error.includes("is not defined by the repository Makefile"),
  );

  const scenarioWithWrongWireStatus = structuredClone(validScenario);
  scenarioWithWrongWireStatus.wire_expectations[0].http_status = 409;
  expectSemanticInvalid(
    scenarioSemanticErrors(scenarioWithWrongWireStatus),
    "Wire expectation with noncanonical status control",
    (error) => error.includes("does not match its canonical status"),
  );

  const scenarioWithoutWireExpectation = structuredClone(validScenario);
  scenarioWithoutWireExpectation.wire_expectations = [];
  expectSemanticInvalid(
    scenarioSemanticErrors(scenarioWithoutWireExpectation),
    "HTTP scenario without wire expectation control",
    (error) => error.includes("HTTP steps do not exactly match wire expectations"),
  );

  const scenarioWithMismatchedPredicateOracle = structuredClone(validScenario);
  scenarioWithMismatchedPredicateOracle.assertions[0].predicate.contract_predicate =
    "state-equality";
  expectSemanticInvalid(
    scenarioSemanticErrors(scenarioWithMismatchedPredicateOracle),
    "Assertion predicate and oracle mismatch control",
    (error) => error.includes("contract predicate does not match oracle"),
  );

  const scenarioWithFreeFormPredicate = structuredClone(validScenario);
  scenarioWithFreeFormPredicate.assertions[0].predicate.name = "always-true";
  expectInvalid(
    validators.scenario,
    scenarioWithFreeFormPredicate,
    "Scenario assertion with a free-form predicate",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/assertions/0/predicate/name" &&
          error.keyword === "enum",
      ),
  );

  const scenarioWithMissingNormativeAnchor = structuredClone(validScenario);
  scenarioWithMissingNormativeAnchor.normative_references[0].anchor =
    "#missing-scenario-heading";
  if (
    validateInstance(
      validators.scenario,
      scenarioWithMissingNormativeAnchor,
      "Schema-valid missing scenario normative heading control",
    )
  ) {
    expectSemanticInvalid(
      await scenarioNormativeReferenceErrors(scenarioWithMissingNormativeAnchor),
      "Missing scenario normative heading control",
      (error) => error.includes("references a missing heading anchor"),
    );
  }
  const scenarioWithoutControl = structuredClone(validScenario);
  scenarioWithoutControl.negative_controls = [];
  expectSemanticInvalid(
    scenarioSemanticErrors(scenarioWithoutControl),
    "Negative-control scenario without an authored control",
    (error) => error.includes("has no matching authored negative control"),
  );

  if (
    requirementsValid &&
    supportMatrixValid &&
    artifactInventoryValid &&
    faultCatalogValid &&
    performanceBudgetsValid
  ) {
    const scenarioWithExcludedSupport = structuredClone(validScenario);
    scenarioWithExcludedSupport.proof_obligations[0].support_cell_id =
      "SUP-PG-017";
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithExcludedSupport,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Scenario with excluded support cell control",
      (error) => error.includes("unknown or excluded support cell"),
    );

    const scenarioWithWrongNativeTarget = structuredClone(validScenario);
    const wrongTargetObligation =
      scenarioWithWrongNativeTarget.proof_obligations.find(
        ({ obligation_id }) => obligation_id === "OBL-NATIVE-001",
      );
    wrongTargetObligation.make_target = "docs-build";
    wrongTargetObligation.argv = ["make", "docs-build"];
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithWrongNativeTarget,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Native proof obligation using a documentation target control",
      (error) => error.includes("cannot prove native-e2e"),
    );

    const scenarioWithDirectServerTarget = structuredClone(validScenario);
    const directServerObligation =
      scenarioWithDirectServerTarget.proof_obligations.find(
        ({ obligation_id }) => obligation_id === "OBL-SERVER-001",
      );
    directServerObligation.make_target = "test-adapter";
    directServerObligation.argv = ["make", "test-adapter"];
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithDirectServerTarget,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Server black-box proof using a direct adapter target control",
      (error) => error.includes("cannot prove server-black-box"),
    );

    const scenarioWithWrongNativeArtifacts = structuredClone(validScenario);
    scenarioWithWrongNativeArtifacts.proof_obligations.find(
      ({ obligation_id }) => obligation_id === "OBL-NATIVE-001",
    ).artifact_inventory_ids = ["ARTDEF-PORTABLE-SEED-001"];
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithWrongNativeArtifacts,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Native proof obligation with irrelevant artifact roles control",
      (error) => error.includes("requires artifact role pg-extension"),
    );

    const scenarioWithIrrelevantNativeArtifact = structuredClone(validScenario);
    scenarioWithIrrelevantNativeArtifact.proof_obligations
      .find(({ obligation_id }) => obligation_id === "OBL-NATIVE-001")
      .artifact_inventory_ids.push("ARTDEF-KOTLIN-MAVEN-001");
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithIrrelevantNativeArtifact,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Native proof obligation with an irrelevant platform artifact control",
      (error) => error.includes("does not permit artifact role kotlin-maven"),
    );

    const scenarioWithoutNegativeSubjectArtifact = structuredClone(validScenario);
    scenarioWithoutNegativeSubjectArtifact.proof_obligations.find(
      ({ obligation_id }) => obligation_id === "OBL-NEGATIVE-001",
    ).artifact_inventory_ids = ["ARTDEF-CONFORMANCE-RUNNER-001"];
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithoutNegativeSubjectArtifact,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Negative-control obligation missing its mutated subject artifact control",
      (error) => error.includes("must exactly bind the conformance runner and mutated subjects"),
    );

    const scenarioUsingRunnerAsMutatedSubject = structuredClone(validScenario);
    scenarioUsingRunnerAsMutatedSubject.negative_controls[0].subject_artifact_inventory_ids =
      ["ARTDEF-CONFORMANCE-RUNNER-001"];
    scenarioUsingRunnerAsMutatedSubject.proof_obligations.find(
      ({ obligation_id }) => obligation_id === "OBL-NEGATIVE-001",
    ).artifact_inventory_ids = ["ARTDEF-CONFORMANCE-RUNNER-001"];
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioUsingRunnerAsMutatedSubject,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Negative control using its conformance runner as the mutated subject control",
      (error) => error.includes("cannot use the conformance runner as its mutated subject"),
    );

    const scenarioWithWrongRequirementReference = structuredClone(validScenario);
    scenarioWithWrongRequirementReference.normative_references = [
      {
        path: "docs/src/content/docs/spec/04-invariants.mdx",
        anchor: "#opaque-cursor-invariant",
      },
    ];
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithWrongRequirementReference,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Scenario with a different requirement normative reference control",
      (error) => error.includes("normative references do not exactly match"),
    );

    const scenarioWithWrongRequirementControl = structuredClone(validScenario);
    const cursorControl = faultCatalog.controls.find(
      ({ id }) => id === "CTRL-CURSOR-001",
    );
    scenarioWithWrongRequirementControl.negative_controls[0].control_id =
      cursorControl.id;
    scenarioWithWrongRequirementControl.negative_controls[0].fault_id =
      cursorControl.fault_id;
    scenarioWithWrongRequirementControl.fault_plans[0].control_id =
      cursorControl.id;
    scenarioWithWrongRequirementControl.fault_plans[0].fault_id =
      cursorControl.fault_id;
    scenarioWithWrongRequirementControl.fault_plans[0].injection =
      structuredClone(cursorControl.injection);
    scenarioWithWrongRequirementControl.assertions[0].detects_control_ids = [
      cursorControl.id,
    ];
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithWrongRequirementControl,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Scenario using a catalog control for another requirement control",
      (error) => error.includes("negative controls do not exactly match"),
    );
  }

  const scenarioWithUnknownDetectedBy = structuredClone(validScenario);
  scenarioWithUnknownDetectedBy.negative_controls[0].detected_by = [
    "ASSERT-UNKNOWN-999",
  ];
  if (
    validateInstance(
      validators.scenario,
      scenarioWithUnknownDetectedBy,
      "Schema-valid unknown detected_by semantic control",
    )
  ) {
    expectSemanticInvalid(
      scenarioSemanticErrors(scenarioWithUnknownDetectedBy),
      "Unknown detected_by semantic control",
      (error) => error.includes("does not name an assertion"),
    );
  }

  const scenarioWithDuplicateAssertionId = structuredClone(validScenario);
  scenarioWithDuplicateAssertionId.assertions.push({
    id: "ASSERT-TIME-001",
    requirement_ids: ["SYNC-TIME-001"],
    description: "A distinct assertion object reuses the logical ID.",
    expectation_ids: ["EXPECT-TIME-001"],
    predicate: {
      contract_predicate: "wire-outcome",
      name: "canonical-wire-outcome",
      payload: {},
    },
    oracle: {
      kind: "model-state-equality",
      expected_source: "authored-model",
      observed_source: "system-under-test",
    },
    detects_control_ids: ["CTRL-TIMESTAMP-001"],
  });
  if (
    validateInstance(
      validators.scenario,
      scenarioWithDuplicateAssertionId,
      "Schema-valid duplicate assertion ID semantic control",
    )
  ) {
    expectSemanticInvalid(
      scenarioSemanticErrors(scenarioWithDuplicateAssertionId),
      "Duplicate assertion ID semantic control",
      (error) => error.includes("Scenario assertions contains duplicate logical ID"),
    );
  }

  const scenarioWithDuplicateStepId = structuredClone(validScenario);
  scenarioWithDuplicateStepId.steps.push({
    id: "STEP-TIME-001",
    phase: "cleanup",
    transport: "local",
    operation: {
      contract_operation: "local",
      name: "reset-clock",
      payload: null,
    },
  });
  if (
    validateInstance(
      validators.scenario,
      scenarioWithDuplicateStepId,
      "Schema-valid duplicate step ID semantic control",
    )
  ) {
    expectSemanticInvalid(
      scenarioSemanticErrors(scenarioWithDuplicateStepId),
      "Duplicate step ID semantic control",
      (error) => error.includes("Scenario steps contains duplicate logical ID"),
    );
  }

  for (const [label, mutate, expectedError] of [
    [
      "model expectation",
      (scenario) => {
        const duplicate = structuredClone(scenario.model.expected_state[0]);
        duplicate.predicate.name = "state-equals-authored-model";
        scenario.model.expected_state.push(duplicate);
      },
      "Scenario model expectations contains duplicate logical ID",
    ],
    [
      "barrier",
      (scenario) => {
        const duplicate = structuredClone(scenario.barrier_plan.barriers[0]);
        duplicate.name = "duplicate-barrier";
        scenario.barrier_plan.barriers.push(duplicate);
      },
      "Scenario barriers contains duplicate logical ID",
    ],
    [
      "fault plan",
      (scenario) => {
        const duplicate = structuredClone(scenario.fault_plans[0]);
        scenario.fault_plans.push(duplicate);
      },
      "Scenario fault plans contains duplicate logical ID",
    ],
    [
      "negative control",
      (scenario) => {
        const duplicate = structuredClone(scenario.negative_controls[0]);
        scenario.negative_controls.push(duplicate);
      },
      "Scenario negative controls contains duplicate logical ID",
    ],
    [
      "proof obligation",
      (scenario) => {
        const duplicate = structuredClone(scenario.proof_obligations[0]);
        duplicate.support_cell_id = "SUP-RN-IOS-CURRENT-001";
        scenario.proof_obligations.push(duplicate);
      },
      "Scenario proof obligations contains duplicate logical ID",
    ],
  ]) {
    const scenario = structuredClone(validScenario);
    mutate(scenario);
    if (
      validateInstance(
        validators.scenario,
        scenario,
        `Schema-valid duplicate ${label} ID control`,
      )
    ) {
      expectSemanticInvalid(
        scenarioSemanticErrors(scenario),
        `Duplicate ${label} ID control`,
        (error) => error.includes(expectedError),
      );
    }
  }

  const scenarioWithUnreferencedExpectation = structuredClone(validScenario);
  scenarioWithUnreferencedExpectation.model.expected_state.push({
    id: "EXPECT-TIME-002",
    predicate: {
      contract_predicate: "state-equality",
      name: "state-equals-authored-model",
      payload: {},
    },
  });
  expectSemanticInvalid(
    scenarioSemanticErrors(scenarioWithUnreferencedExpectation),
    "Unreferenced model expectation control",
    (error) => error.includes("is not referenced by an assertion"),
  );

  const randomizedScenarioWithoutSeedPolicy = structuredClone(validScenario);
  randomizedScenarioWithoutSeedPolicy.replay.mode = "randomized";
  randomizedScenarioWithoutSeedPolicy.replay.seed_required = false;
  expectInvalid(
    validators.scenario,
    randomizedScenarioWithoutSeedPolicy,
    "Randomized scenario without required seed policy",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/replay/seed_required" &&
          error.keyword === "const",
      ),
  );

  const scenarioWithoutRequiredBarrierTrace = structuredClone(validScenario);
  scenarioWithoutRequiredBarrierTrace.replay.barrier_trace_required = false;
  expectSemanticInvalid(
    scenarioSemanticErrors(scenarioWithoutRequiredBarrierTrace),
    "Authored barrier without required trace control",
    (error) => error.includes("authored barriers require a barrier trace"),
  );

  const scenarioWithOpenDetectedSet = structuredClone(validScenario);
  scenarioWithOpenDetectedSet.assertions.push({
    id: "ASSERT-TIME-002",
    requirement_ids: ["SYNC-TIME-001"],
    description: "A second detection assertion.",
    expectation_ids: ["EXPECT-TIME-001"],
    predicate: {
      contract_predicate: "state-equality",
      name: "state-equals-authored-model",
      payload: {},
    },
    oracle: {
      kind: "model-state-equality",
      expected_source: "authored-model",
      observed_source: "system-under-test",
    },
    detects_control_ids: ["CTRL-TIMESTAMP-001"],
  });
  scenarioWithOpenDetectedSet.fault_plans[0].expected_assertion_ids = [
    "ASSERT-TIME-002",
  ];
  expectSemanticInvalid(
    scenarioSemanticErrors(scenarioWithOpenDetectedSet),
    "Fault plan and control detected_by closure control",
    (error) => error.includes("do not exactly match negative control"),
  );

  if (faultCatalogValid && performanceBudgetsValid) {
    const scenarioWithChangedInjection = structuredClone(validScenario);
    scenarioWithChangedInjection.fault_plans[0].injection.parameters.defect =
      "a post-authorship recipe substitution";
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithChangedInjection,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Scenario fault recipe catalog mismatch control",
      (error) => error.includes("injection recipe does not match catalog control"),
    );

    const scenarioWithUndeclaredMeasurement = structuredClone(validScenario);
    scenarioWithUndeclaredMeasurement.required_measurement_ids = [
      "MEAS-FANOUT-001",
    ];
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        scenarioWithUndeclaredMeasurement,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Scenario required measurement exact declaration control",
      (error) =>
        error.includes(
          "required measurement declarations do not exactly match the authored catalog",
        ),
    );
  }

  if (
    validateInstance(validators.evidence, validEvidence, "Valid evidence self-test")
  ) {
    expectSemanticValid(
      evidencePromotionEligibilityErrors(validEvidence),
      "Passing evidence promotion-eligibility self-test",
    );
    expectSemanticValid(
      evidenceScenarioSemanticErrors(
        validEvidence,
        validScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Passing evidence-to-scenario binding self-test",
    );
    expectSemanticValid(
      evidenceManifestBindingErrors(validEvidence, validManifest),
      "Passing evidence-to-manifest binding self-test",
    );
  }

  const wrongProtocolEvidence = structuredClone(validEvidence);
  wrongProtocolEvidence.protocol_version = 2;
  expectInvalid(
    validators.evidence,
    wrongProtocolEvidence,
    "Evidence for a protocol other than version 3",
    (errors) =>
      errors.some((error) => error.instancePath === "/protocol_version"),
  );

  const evidenceWithoutContractSnapshot = structuredClone(validEvidence);
  delete evidenceWithoutContractSnapshot.contract_snapshot_sha256;
  expectInvalid(
    validators.evidence,
    evidenceWithoutContractSnapshot,
    "Evidence without a contract snapshot binding",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "" &&
          error.keyword === "required" &&
          error.params.missingProperty === "contract_snapshot_sha256",
      ),
  );

  const scenarioWithAdditionalAssertion = structuredClone(validScenario);
  scenarioWithAdditionalAssertion.assertions.push({
    id: "ASSERT-TIME-002",
    requirement_ids: ["SYNC-TIME-001"],
    description: "The server state remains unchanged.",
    expectation_ids: ["EXPECT-TIME-001"],
    predicate: {
      contract_predicate: "state-equality",
      name: "state-unchanged",
      payload: {},
    },
    oracle: {
      kind: "model-state-equality",
      expected_source: "authored-model",
      observed_source: "system-under-test",
    },
    detects_control_ids: [],
  });
  scenarioWithAdditionalAssertion.proof_obligations.find(
    ({ obligation_id }) => obligation_id === "OBL-NATIVE-001",
  ).assertion_ids.push("ASSERT-TIME-002");
  expectSemanticInvalid(
    evidenceScenarioSemanticErrors(
      validEvidence,
      scenarioWithAdditionalAssertion,
      validManifest,
      artifactInventory,
      performanceBudgets,
    ),
    "Evidence missing one authored scenario assertion control",
    (error) => error.includes("is missing obligation assertion ASSERT-TIME-002"),
  );

  const evidenceWithMissingTrace = structuredClone(validEvidence);
  evidenceWithMissingTrace.execution_artifacts.trace_attachment_ids = [
    "ATT-MISSING-999",
  ];
  expectSemanticInvalid(
    evidenceScenarioSemanticErrors(
      evidenceWithMissingTrace,
      validScenario,
      validManifest,
      artifactInventory,
      performanceBudgets,
    ),
    "Evidence with an unbound trace attachment control",
    (error) => error.includes("references missing attachment ATT-MISSING-999"),
  );

  const evidenceWithDuplicateAttachmentPath = structuredClone(validEvidence);
  evidenceWithDuplicateAttachmentPath.attachments[1].path =
    evidenceWithDuplicateAttachmentPath.attachments[0].path;
  expectSemanticInvalid(
    evidenceScenarioSemanticErrors(
      evidenceWithDuplicateAttachmentPath,
      validScenario,
      validManifest,
      artifactInventory,
      performanceBudgets,
    ),
    "Evidence duplicate attachment path control",
    (error) =>
      error.includes("Evidence attachment paths contains duplicate logical ID"),
  );

  const evidenceWithTraversalPath = structuredClone(validEvidence);
  evidenceWithTraversalPath.attachments[0].path = "../outside.log";
  expectInvalid(
    validators.evidence,
    evidenceWithTraversalPath,
    "Evidence attachment path traversal",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/attachments/0/path" &&
          error.keyword === "pattern",
      ),
  );

  const evidenceWithUnknownReplayBarrier = structuredClone(validEvidence);
  evidenceWithUnknownReplayBarrier.replay.barrier_traces[0].barrier_id =
    "BAR-UNKNOWN-999";
  expectSemanticInvalid(
    evidenceScenarioSemanticErrors(
      evidenceWithUnknownReplayBarrier,
      validScenario,
      validManifest,
      artifactInventory,
      performanceBudgets,
    ),
    "Evidence with an unknown replay barrier control",
    (error) => error.includes("replay barrier IDs do not exactly match"),
  );

  const evidenceWithWrongBarrierAttachmentKind = structuredClone(validEvidence);
  evidenceWithWrongBarrierAttachmentKind.attachments.find(
    ({ id }) => id === "ATT-BARRIER-001",
  ).kind = "trace";
  expectSemanticInvalid(
    evidenceScenarioSemanticErrors(
      evidenceWithWrongBarrierAttachmentKind,
      validScenario,
      validManifest,
      artifactInventory,
      performanceBudgets,
    ),
    "Evidence with a mistyped replay barrier attachment control",
    (error) => error.includes("not barrier-trace"),
  );

  const evidenceWithWrongDuration = structuredClone(validEvidence);
  evidenceWithWrongDuration.run.duration_ms = 1;
  expectSemanticInvalid(
    evidenceScenarioSemanticErrors(
      evidenceWithWrongDuration,
      validScenario,
      validManifest,
      artifactInventory,
      performanceBudgets,
    ),
    "Evidence with an inconsistent duration control",
    (error) => error.includes("run duration does not match"),
  );

  const evidenceWithSpoofedTargetPrefix = structuredClone(validEvidence);
  evidenceWithSpoofedTargetPrefix.run.argv[1] = "test-rn-spoof";
  expectSemanticInvalid(
    evidenceScenarioSemanticErrors(
      evidenceWithSpoofedTargetPrefix,
      validScenario,
      validManifest,
      artifactInventory,
      performanceBudgets,
    ),
    "Evidence with a Make target prefix spoof control",
    (error) => error.includes("command does not exactly match obligation"),
  );

  const evidenceWithMakeAssignment = structuredClone(validEvidence);
  evidenceWithMakeAssignment.run.argv.push(
    "DETOX_ARGS=--configuration ios.sim.release",
  );
  expectInvalid(
    validators.evidence,
    evidenceWithMakeAssignment,
    "Evidence command with a Make command-line assignment",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/run/argv" && error.keyword === "maxItems",
      ),
  );

  for (const [label, mutate, expectedError] of [
    [
      "unknown proof obligation",
      (evidence) => (evidence.proof_obligation_id = "OBL-UNKNOWN-999"),
      "references unknown proof obligation",
    ],
    [
      "wrong obligation support cell",
      (evidence) =>
        (evidence.support_cell_id = "SUP-RN-IOS-CURRENT-001"),
      "does not match obligation",
    ],
    [
      "wrong obligation command",
      (evidence) => {
        evidence.run.make_target = "test-swift";
        evidence.run.argv = ["make", "test-swift"];
      },
      "command does not exactly match obligation",
    ],
    [
      "wrong obligation artifact role",
      (evidence) => (evidence.artifact_ids = ["ART-SWIFT-001"]),
      "resolved artifact inventory IDs do not exactly match obligation",
    ],
    [
      "wrong resolved environment",
      (evidence) => (evidence.environment[0].value = "17.0.0"),
      "environment does not exactly match resolved support cell",
    ],
  ]) {
    const evidence = structuredClone(validEvidence);
    mutate(evidence);
    if (
      validateInstance(
        validators.evidence,
        evidence,
        `Schema-valid evidence ${label} control`,
      )
    ) {
      expectSemanticInvalid(
        evidenceScenarioSemanticErrors(
          evidence,
          validScenario,
          validManifest,
          artifactInventory,
          performanceBudgets,
        ),
        `Evidence ${label} control`,
        (error) => error.includes(expectedError),
      );
    }
  }
  if (
    validateInstance(
      validators.evidence,
      validNegativeControlEvidence,
      "Valid negative-control evidence self-test",
    )
  ) {
    expectSemanticValid(
      evidenceScenarioSemanticErrors(
        validNegativeControlEvidence,
        validScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Valid negative-control evidence binding self-test",
    );
    if (faultCatalogValid) {
      expectSemanticValid(
        faultExecutionCatalogErrors(
          validNegativeControlEvidence,
          validScenario,
          faultCatalog,
        ),
        "Valid negative-control catalog binding self-test",
      );
      const evidenceWithUnknownCatalogFault = structuredClone(
        validNegativeControlEvidence,
      );
      evidenceWithUnknownCatalogFault.negative_control.fault_id =
        "FAULT-UNKNOWN-999";
      evidenceWithUnknownCatalogFault.fault_execution.fault_id =
        "FAULT-UNKNOWN-999";
      expectSemanticInvalid(
        faultExecutionCatalogErrors(
          evidenceWithUnknownCatalogFault,
          validScenario,
          faultCatalog,
        ),
        "Negative-control evidence with unknown fault catalog ID control",
        (error) => error.includes("fault is absent from fault catalog"),
      );
    }
  }
  if (faultCatalogValid) {
    for (const [label, mutate, expectedError] of [
      [
        "mistyped fault-plan attachment",
        (evidence) => {
          evidence.attachments.find(
            ({ id }) => id === "ATT-FAULT-001",
          ).kind = "report";
        },
        "lacks its typed fault-plan attachment",
      ],
      [
        "wrong subject type",
        (evidence) => (evidence.fault_execution.subject_type = "mutant"),
        "subject type does not match catalog control",
      ],
      [
        "changed detected assertions",
        (evidence) =>
          (evidence.fault_execution.detected_by = ["ASSERT-TIME-002"]),
        "detected assertions do not match scenario plan",
      ],
      [
        "changed injection recipe",
        (evidence) =>
          (evidence.fault_execution.injection.parameters.defect =
            "a post-authorship recipe substitution"),
        "injection does not match scenario plan",
      ],
    ]) {
      const evidence = structuredClone(validFaultInjectionEvidence);
      mutate(evidence);
      if (
        validateInstance(
          validators.evidence,
          evidence,
          `Schema-valid fault execution ${label} control`,
        )
      ) {
        expectSemanticInvalid(
          faultExecutionCatalogErrors(evidence, validScenario, faultCatalog),
          `Fault execution ${label} control`,
          (error) => error.includes(expectedError),
        );
      }
    }
  }
  const negativeEvidenceWithWrongSubjectArtifact = structuredClone(
    validNegativeControlEvidence,
  );
  negativeEvidenceWithWrongSubjectArtifact.negative_control.control_subject_artifact_ids =
    ["ART-CONFORMANCE-RUNNER-001"];
  expectSemanticInvalid(
    evidenceScenarioSemanticErrors(
      negativeEvidenceWithWrongSubjectArtifact,
      validScenario,
      validManifest,
      artifactInventory,
      performanceBudgets,
    ),
    "Negative-control evidence with wrong mutated subject artifact control",
    (error) => error.includes("do not exactly match the authored control"),
  );
  const supportNeutralEvidenceWithEnvironment = structuredClone(
    validNegativeControlEvidence,
  );
  supportNeutralEvidenceWithEnvironment.environment = [
    { name: "os", value: "substituted" },
  ];
  expectSemanticInvalid(
    evidenceScenarioSemanticErrors(
      supportNeutralEvidenceWithEnvironment,
      validScenario,
      validManifest,
      artifactInventory,
      performanceBudgets,
    ),
    "Support-neutral evidence with an execution environment control",
    (error) => error.includes("must have an empty environment"),
  );
  if (
    validateInstance(
      validators.evidence,
      validFaultInjectionEvidence,
      "Valid fault-injection evidence self-test",
    )
  ) {
    expectSemanticValid(
      evidenceScenarioSemanticErrors(
        validFaultInjectionEvidence,
        validScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Valid fault-injection evidence binding self-test",
    );
    if (faultCatalogValid) {
      expectSemanticValid(
        faultExecutionCatalogErrors(
          validFaultInjectionEvidence,
          validScenario,
          faultCatalog,
        ),
        "Valid fault-injection catalog binding self-test",
      );
    }
  }

  if (performanceBudgetsValid) {
    const allCellBudgets = performanceBudgets.budgets.filter(
      ({ scenario_id }) => scenario_id === "SCN-PERF-WARM-CONNECT-001",
    );
    const incompleteAllCellScenario = structuredClone(validScenario);
    incompleteAllCellScenario.id = "SCN-PERF-WARM-CONNECT-001";
    incompleteAllCellScenario.performance_budget_ids = allCellBudgets.map(
      ({ id }) => id,
    );
    expectSemanticInvalid(
      authoredScenarioBindingErrors(
        incompleteAllCellScenario,
        requirements,
        supportMatrix,
        artifactInventory,
        faultCatalog,
        performanceBudgets,
      ),
      "Performance scenario missing required support cells control",
      (error) => error.includes("has no proof obligation for support cell"),
    );

    const budget = performanceBudgets.budgets.find(
      ({ id }) => id === "BUD-CORE-SYNC-RPC-001",
    );
    const performanceScenario = structuredClone(validScenario);
    performanceScenario.id = budget.scenario_id;
    performanceScenario.performance_budget_ids = [budget.id];
    performanceScenario.proof_obligations[0].support_cell_id = "SUP-PG-018";
    performanceScenario.proof_obligations[0].artifact_inventory_ids = [
      "ARTDEF-PG-EXTENSION-001",
      "ARTDEF-ADAPTER-001",
    ];
    const performanceEvidence = structuredClone(validServerEvidence);
    performanceEvidence.evidence_id = "EVD-PERFORMANCE-001";
    performanceEvidence.scenario_id = performanceScenario.id;
    performanceEvidence.support_cell_id = "SUP-PG-018";
    performanceEvidence.artifact_ids = [
      "ART-PG-EXTENSION-001",
      "ART-ADAPTER-001",
    ];
    performanceEvidence.attachments.push({
      id: "ATT-PERFORMANCE-001",
      kind: "performance-measurements",
      path: "evidence/performance.json",
      media_type: "application/json",
      sha256: "2".repeat(64),
    });
    performanceEvidence.performance_results = [
      {
        budget_id: budget.id,
        outcome: "passed",
        measurement_attachment_id: "ATT-PERFORMANCE-001",
        metric: budget.metric,
        unit: budget.unit,
        comparator: budget.comparator,
        limit: budget.limit,
        observed_value: budget.limit,
        measurement: {
          request_counts: {
            connect: 0,
            push: 0,
            pull: 0,
            rebuild_page: 0,
            schema_fetch: 0,
            other: 0,
          },
          returned_rebuild_page_count: 0,
          outbound_network_or_rpc_hops: 0,
        },
        data_profile: budget.data_profile,
        measurement_method: budget.measurement_method,
      },
    ];
    if (
      validateInstance(
        validators.evidence,
        performanceEvidence,
        "Valid performance budget evidence self-test",
      )
    ) {
      expectSemanticValid(
        evidenceScenarioSemanticErrors(
          performanceEvidence,
          performanceScenario,
          validManifest,
          artifactInventory,
          performanceBudgets,
        ),
        "Valid performance budget evidence semantic self-test",
      );
    }

    const duplicatePerformanceResult = structuredClone(performanceEvidence);
    const secondPerformanceResult = structuredClone(
      duplicatePerformanceResult.performance_results[0],
    );
    secondPerformanceResult.outcome = "failed";
    secondPerformanceResult.observed_value += 1;
    duplicatePerformanceResult.performance_results.push(secondPerformanceResult);
    expectSemanticInvalid(
      evidenceScenarioSemanticErrors(
        duplicatePerformanceResult,
        performanceScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Duplicate performance budget result control",
      (error) =>
        error.includes(
          "Evidence performance results contains duplicate logical ID",
        ),
    );

    const mistypedPerformanceAttachment = structuredClone(performanceEvidence);
    mistypedPerformanceAttachment.attachments.find(
      ({ id }) => id === "ATT-PERFORMANCE-001",
    ).kind = "report";
    expectSemanticInvalid(
      evidenceScenarioSemanticErrors(
        mistypedPerformanceAttachment,
        performanceScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Mistyped performance attachment control",
      (error) => error.includes("lacks its typed performance-measurements"),
    );

    const failedPerformanceEvidence = structuredClone(performanceEvidence);
    failedPerformanceEvidence.performance_results[0].outcome = "failed";
    failedPerformanceEvidence.performance_results[0].observed_value =
      budget.limit + 1;
    expectSemanticInvalid(
      evidencePromotionEligibilityErrors(failedPerformanceEvidence),
      "Failed performance budget promotion control",
      (error) => error.includes("has outcome failed, not passed"),
    );
    expectSemanticInvalid(
      evidenceScenarioSemanticErrors(
        failedPerformanceEvidence,
        performanceScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Performance observed value not derived from typed counters control",
      (error) => error.includes("is not derived from its typed measurement"),
    );

    const measurement = performanceBudgets.required_measurements.find(
      ({ id }) => id === "MEAS-FANOUT-001",
    );
    const measurementScenario = structuredClone(validScenario);
    measurementScenario.id = measurement.scenario_id;
    measurementScenario.required_measurement_ids = [measurement.id];
    measurementScenario.proof_obligations[0].support_cell_id = "SUP-PG-018";
    measurementScenario.proof_obligations[0].artifact_inventory_ids = [
      "ARTDEF-PG-EXTENSION-001",
      "ARTDEF-ADAPTER-001",
    ];
    const measurementEvidence = structuredClone(validServerEvidence);
    measurementEvidence.evidence_id = "EVD-MEASUREMENT-001";
    measurementEvidence.scenario_id = measurementScenario.id;
    measurementEvidence.support_cell_id = "SUP-PG-018";
    measurementEvidence.artifact_ids = [
      "ART-PG-EXTENSION-001",
      "ART-ADAPTER-001",
    ];
    measurementEvidence.attachments.push({
      id: "ATT-MEASUREMENT-001",
      kind: "performance-measurements",
      path: "evidence/measurement.json",
      media_type: "application/json",
      sha256: "3".repeat(64),
    });
    measurementEvidence.required_measurement_results = [
      {
        measurement_id: measurement.id,
        outcome: "passed",
        measurement_attachment_id: "ATT-MEASUREMENT-001",
        data_profile: measurement.data_profile,
        measurement_method: measurement.measurement_method,
        metrics: measurement.metrics,
        strata: measurement.strata.map(
          ({ stratum_id, parameters }, stratumIndex) => ({
            stratum_id,
            parameters,
            sample_count: measurement.minimum_sample_count_per_stratum,
            observations: Array.from(
              { length: measurement.minimum_sample_count_per_stratum },
              (_, sampleIndex) => ({
                sample_id: `SAMPLE-${String(stratumIndex + 1).padStart(3, "0")}-${String(sampleIndex + 1).padStart(3, "0")}`,
                metric_values: measurement.metrics.map(({ id }) => ({
                  metric_id: id,
                  value: 1,
                })),
              }),
            ),
          }),
        ),
      },
    ];
    if (
      validateInstance(
        validators.evidence,
        measurementEvidence,
        "Valid required measurement evidence self-test",
      )
    ) {
      expectSemanticValid(
        evidenceScenarioSemanticErrors(
          measurementEvidence,
          measurementScenario,
          validManifest,
          artifactInventory,
          performanceBudgets,
        ),
        "Valid required measurement evidence semantic self-test",
      );
    }

    const changedRequiredMeasurement = structuredClone(measurementEvidence);
    changedRequiredMeasurement.required_measurement_results[0].strata[0].sample_count =
      measurement.minimum_sample_count_per_stratum - 1;
    expectSemanticInvalid(
      evidenceScenarioSemanticErrors(
        changedRequiredMeasurement,
        measurementScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Required measurement minimum sample control",
      (error) => error.includes("sample count is below the authored minimum"),
    );

    const countWithoutObservations = structuredClone(measurementEvidence);
    countWithoutObservations.required_measurement_results[0].strata[0].sample_count +=
      1;
    expectSemanticInvalid(
      evidenceScenarioSemanticErrors(
        countWithoutObservations,
        measurementScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Characterization count without corresponding observations control",
      (error) => error.includes("sample count does not match observations"),
    );

    const observationWithoutMetric = structuredClone(measurementEvidence);
    observationWithoutMetric.required_measurement_results[0].strata[0].observations[0].metric_values.pop();
    expectSemanticInvalid(
      evidenceScenarioSemanticErrors(
        observationWithoutMetric,
        measurementScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Characterization observation missing one metric control",
      (error) => error.includes("metric IDs do not match the authored set"),
    );

    const mistypedMeasurementAttachment = structuredClone(measurementEvidence);
    mistypedMeasurementAttachment.attachments.find(
      ({ id }) => id === "ATT-MEASUREMENT-001",
    ).kind = "report";
    expectSemanticInvalid(
      evidenceScenarioSemanticErrors(
        mistypedMeasurementAttachment,
        measurementScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Mistyped required measurement attachment control",
      (error) => error.includes("lacks its typed performance-measurements"),
    );
  }
  if (
    validateInstance(
      validators.evidence,
      failedEvidenceAttemptOne,
      "Schema-valid failed evidence self-test",
    )
  ) {
    expectSemanticInvalid(
      evidencePromotionEligibilityErrors(failedEvidenceAttemptOne),
      "Failed evidence rejected for promotion self-test",
      (error) => error.includes("exit_code is 1, not 0"),
    );
  }
  if (
    validateInstances(
      validators.evidence,
      [failedEvidenceAttemptOne, validEvidence],
      "Schema-valid evidence rerun bundle self-test",
    )
  ) {
    expectSemanticValid(
      evidenceBundleSemanticErrors([failedEvidenceAttemptOne, validEvidence]),
      "Failed attempt followed by passing rerun semantic self-test",
    );
  }

  const skippedEvidence = structuredClone(failedEvidenceAttemptOne);
  skippedEvidence.evidence_id = "EVD-NATIVE-SKIP-001";
  skippedEvidence.run.id = "RUN-NATIVE-SKIP-001";
  skippedEvidence.run.exit_code = 0;
  skippedEvidence.run.result = "passed";
  skippedEvidence.assertions[0].outcome = "skipped";
  if (
    validateInstance(
      validators.evidence,
      skippedEvidence,
      "Schema-valid skipped evidence self-test",
    )
  ) {
    expectSemanticInvalid(
      evidencePromotionEligibilityErrors(skippedEvidence),
      "Schema-valid skipped evidence rejected for promotion self-test",
      (error) => error.includes("outcome skipped, not passed"),
    );
  }

  const rerunAfterSkippedEvidence = structuredClone(validEvidence);
  rerunAfterSkippedEvidence.run.previous_evidence_id =
    skippedEvidence.evidence_id;
  if (
    validateInstances(
      validators.evidence,
      [skippedEvidence, rerunAfterSkippedEvidence],
      "Schema-valid skipped predecessor rerun bundle control",
    )
  ) {
    expectSemanticInvalid(
      evidenceBundleSemanticErrors([
        skippedEvidence,
        rerunAfterSkippedEvidence,
      ]),
      "Skipped predecessor rerun bundle control",
      (error) => error.includes("must contain only infrastructure error outcomes"),
    );
  }

  const evidenceWithoutSupportCell = structuredClone(validEvidence);
  evidenceWithoutSupportCell.support_cell_id = null;
  expectInvalid(
    validators.evidence,
    evidenceWithoutSupportCell,
    "Native-e2e evidence with a null support_cell_id",
    (errors) =>
      errors.some((error) => error.instancePath === "/support_cell_id"),
  );
  const unlinkedRerun = structuredClone(validEvidence);
  unlinkedRerun.run.previous_evidence_id = null;
  unlinkedRerun.run.rerun_cause = null;
  unlinkedRerun.run.rerun_diagnosis = null;
  unlinkedRerun.run.corrective_action = null;
  unlinkedRerun.run.rerun_approval = null;
  expectInvalid(
    validators.evidence,
    unlinkedRerun,
    "Evidence rerun without required rerun metadata",
    (errors) =>
      [
        "/run/previous_evidence_id",
        "/run/rerun_cause",
        "/run/rerun_diagnosis",
        "/run/corrective_action",
        "/run/rerun_approval",
      ].every((path) => errors.some((error) => error.instancePath === path)),
  );
  const rerunWithUntypedCause = structuredClone(validEvidence);
  rerunWithUntypedCause.run.rerun_cause = "test failed, try again";
  expectInvalid(
    validators.evidence,
    rerunWithUntypedCause,
    "Evidence rerun with a non-infrastructure cause",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/run/rerun_cause" &&
          error.keyword === "enum",
      ),
  );
  const evidenceWithoutCandidate = structuredClone(validEvidence);
  evidenceWithoutCandidate.candidate_id = null;
  expectInvalid(
    validators.evidence,
    evidenceWithoutCandidate,
    "Evidence without an identified candidate",
    (errors) =>
      errors.some((error) => error.instancePath === "/candidate_id"),
  );
  const evidenceWithInvalidUri = structuredClone(validEvidence);
  evidenceWithInvalidUri.run.url = "not a uri";
  expectInvalid(
    validators.evidence,
    evidenceWithInvalidUri,
    "Evidence with an invalid URI",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/run/url" && error.keyword === "format",
      ),
  );
  const evidenceWithInvalidDateTime = structuredClone(validEvidence);
  evidenceWithInvalidDateTime.run.started_at = "not a date-time";
  expectInvalid(
    validators.evidence,
    evidenceWithInvalidDateTime,
    "Evidence with an invalid date-time",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/run/started_at" && error.keyword === "format",
      ),
  );

  const evidenceWithoutNegativeControlMetadata = structuredClone(
    validNegativeControlEvidence,
  );
  delete evidenceWithoutNegativeControlMetadata.negative_control
    .control_subject_id;
  expectInvalid(
    validators.evidence,
    evidenceWithoutNegativeControlMetadata,
    "Negative-control evidence with missing control subject metadata",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/negative_control" &&
          error.keyword === "required" &&
          error.params.missingProperty === "control_subject_id",
      ),
  );

  const evidenceWithoutNegativeControlField = structuredClone(validEvidence);
  delete evidenceWithoutNegativeControlField.negative_control;
  expectInvalid(
    validators.evidence,
    evidenceWithoutNegativeControlField,
    "Evidence without required top-level negative_control metadata",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "" &&
          error.keyword === "required" &&
          error.params.missingProperty === "negative_control",
      ),
  );

  const normalEvidenceRelabeledNegativeControl = structuredClone(validEvidence);
  normalEvidenceRelabeledNegativeControl.proof_type = "negative-control";
  expectInvalid(
    validators.evidence,
    normalEvidenceRelabeledNegativeControl,
    "Normal evidence merely relabeled as negative-control",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/negative_control" &&
          error.keyword === "type",
      ),
  );

  const evidenceWithUndeclaredRequirement = structuredClone(validEvidence);
  evidenceWithUndeclaredRequirement.requirement_ids = ["SYNC-TIME-002"];
  if (
    validateInstance(
      validators.evidence,
      evidenceWithUndeclaredRequirement,
      "Schema-valid undeclared evidence requirement control",
    )
  ) {
    expectSemanticInvalid(
      evidenceScenarioSemanticErrors(
        evidenceWithUndeclaredRequirement,
        validScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Undeclared evidence requirement control",
      (error) => error.includes("requirement IDs do not exactly match obligation"),
    );
  }

  const evidenceWithUndeclaredProofType = structuredClone(validEvidence);
  evidenceWithUndeclaredProofType.proof_type = "server-black-box";
  if (
    validateInstance(
      validators.evidence,
      evidenceWithUndeclaredProofType,
      "Schema-valid undeclared evidence proof type control",
    )
  ) {
    expectSemanticInvalid(
      evidenceScenarioSemanticErrors(
        evidenceWithUndeclaredProofType,
        validScenario,
        validManifest,
        artifactInventory,
        performanceBudgets,
      ),
      "Undeclared evidence proof type control",
      (error) => error.includes("does not match obligation"),
    );
  }

  const negativeControlBindingControls = [
    [
      "wrong fault ID",
      (evidence) => (evidence.negative_control.fault_id = "FAULT-TIME-002"),
      (error) => error.includes("does not match scenario fault"),
    ],
    [
      "mismatched detected_by",
      (evidence) => {
        evidence.negative_control.detected_by = ["ASSERT-TIME-002"];
        evidence.assertions.push({
          assertion_id: "ASSERT-TIME-002",
          outcome: "passed",
        });
      },
      (error) => error.includes("detected_by IDs do not match scenario"),
    ],
    [
      "unpassed detection assertion",
      (evidence) => {
        evidence.run.exit_code = 1;
        evidence.run.result = "failed";
        evidence.assertions[0].outcome = "failed";
      },
      (error) => error.includes("does not have a passed evidence outcome"),
    ],
    [
      "missing attachment binding",
      (evidence) =>
        (evidence.negative_control.attachment_ids = ["ATT-MISSING-999"]),
      (error) => error.includes("does not identify an evidence attachment"),
    ],
    [
      "mistyped attachment binding",
      (evidence) =>
        (evidence.negative_control.attachment_ids = ["ATT-LOG-001"]),
      (error) => error.includes("not negative-control"),
    ],
  ];
  for (const [label, mutate, matchesExpectedError] of
    negativeControlBindingControls) {
    const evidence = structuredClone(validNegativeControlEvidence);
    mutate(evidence);
    if (
      validateInstance(
        validators.evidence,
        evidence,
        `Schema-valid negative-control ${label} helper control`,
      )
    ) {
      expectSemanticInvalid(
        evidenceScenarioSemanticErrors(
          evidence,
          validScenario,
          validManifest,
          artifactInventory,
          performanceBudgets,
        ),
        `Negative-control ${label} helper control`,
        matchesExpectedError,
      );
    }
  }

  const evidenceWithCrossRecordAttachmentPath = structuredClone(
    validServerEvidence,
  );
  evidenceWithCrossRecordAttachmentPath.attachments[0].path =
    validEvidence.attachments[0].path;
  expectSemanticInvalid(
    evidenceBundleSemanticErrors([
      failedEvidenceAttemptOne,
      validEvidence,
      evidenceWithCrossRecordAttachmentPath,
    ]),
    "Cross-evidence attachment path collision control",
    (error) => error.includes("Evidence attachment path") && error.includes("is shared"),
  );

  const duplicateEvidenceId = structuredClone(failedEvidenceAttemptOne);
  duplicateEvidenceId.run.id = "RUN-NATIVE-DUPLICATE-001";
  const duplicateEvidenceBundle = [failedEvidenceAttemptOne, duplicateEvidenceId];
  if (
    validateInstances(
      validators.evidence,
      duplicateEvidenceBundle,
      "Schema-valid duplicate evidence ID bundle control",
    )
  ) {
    expectSemanticInvalid(
      evidenceBundleSemanticErrors(duplicateEvidenceBundle),
      "Duplicate evidence ID bundle control",
      (error) => error.includes("Evidence bundle contains duplicate logical ID"),
    );
  }

  const missingPreviousEvidence = structuredClone(validEvidence);
  missingPreviousEvidence.run.previous_evidence_id = "EVD-NATIVE-099";
  const missingPreviousBundle = [
    failedEvidenceAttemptOne,
    missingPreviousEvidence,
  ];
  if (
    validateInstances(
      validators.evidence,
      missingPreviousBundle,
      "Schema-valid missing previous evidence bundle control",
    )
  ) {
    expectSemanticInvalid(
      evidenceBundleSemanticErrors(missingPreviousBundle),
      "Missing previous evidence bundle control",
      (error) => error.includes("references missing previous evidence"),
    );
  }

  const selfPreviousEvidence = structuredClone(validEvidence);
  selfPreviousEvidence.run.previous_evidence_id = selfPreviousEvidence.evidence_id;
  const selfPreviousBundle = [failedEvidenceAttemptOne, selfPreviousEvidence];
  if (
    validateInstances(
      validators.evidence,
      selfPreviousBundle,
      "Schema-valid self previous evidence bundle control",
    )
  ) {
    expectSemanticInvalid(
      evidenceBundleSemanticErrors(selfPreviousBundle),
      "Self previous evidence bundle control",
      (error) => error.includes("cannot reference itself"),
    );
  }

  const nonImmediateRerun = structuredClone(validEvidence);
  nonImmediateRerun.run.attempt = 3;
  const nonImmediateBundle = [failedEvidenceAttemptOne, nonImmediateRerun];
  if (
    validateInstances(
      validators.evidence,
      nonImmediateBundle,
      "Schema-valid non-immediate rerun bundle control",
    )
  ) {
    expectSemanticInvalid(
      evidenceBundleSemanticErrors(nonImmediateBundle),
      "Non-immediate rerun bundle control",
      (error) => error.includes("must reference immediately prior attempt 2"),
    );
  }

  const overlappingRerun = structuredClone(validEvidence);
  overlappingRerun.run.started_at = failedEvidenceAttemptOne.run.completed_at;
  overlappingRerun.run.completed_at = "2026-07-17T11:57:00Z";
  overlappingRerun.run.duration_ms = 60000;
  expectSemanticInvalid(
    evidenceBundleSemanticErrors([failedEvidenceAttemptOne, overlappingRerun]),
    "Rerun temporal overlap control",
    (error) => error.includes("did not start after predecessor"),
  );

  const rerunApprovedAfterStart = structuredClone(validEvidence);
  rerunApprovedAfterStart.run.rerun_approval.approved_at =
    rerunApprovedAfterStart.run.started_at;
  expectSemanticInvalid(
    evidenceBundleSemanticErrors([
      failedEvidenceAttemptOne,
      rerunApprovedAfterStart,
    ]),
    "Rerun approval after start mutant control",
    (error) => error.includes("approval does not precede rerun start"),
  );

  const duplicateExecutionKeyLineage = structuredClone(
    failedEvidenceAttemptOne,
  );
  duplicateExecutionKeyLineage.evidence_id = "EVD-NATIVE-LINEAGE-001";
  duplicateExecutionKeyLineage.run.id = "RUN-NATIVE-LINEAGE-001";
  duplicateExecutionKeyLineage.run.execution_lineage_id =
    "EXEC-NATIVE-BYPASS-001";
  expectSemanticInvalid(
    evidenceBundleSemanticErrors([
      failedEvidenceAttemptOne,
      duplicateExecutionKeyLineage,
    ]),
    "Parallel lineage retry bypass mutant control",
    (error) => error.includes("has multiple lineages"),
  );

  const reusedLineageAcrossObligations = structuredClone(
    validFaultInjectionEvidence,
  );
  reusedLineageAcrossObligations.run.execution_lineage_id = "EXEC-NATIVE-001";
  expectSemanticInvalid(
    evidenceBundleSemanticErrors([
      failedEvidenceAttemptOne,
      reusedLineageAcrossObligations,
    ]),
    "Lineage reused across proof obligations mutant control",
    (error) => error.includes("is reused across execution keys"),
  );

  const productFailurePredecessor = structuredClone(failedEvidenceAttemptOne);
  productFailurePredecessor.run.result = "failed";
  productFailurePredecessor.assertions[0].outcome = "failed";
  expectSemanticInvalid(
    evidenceBundleSemanticErrors([productFailurePredecessor, validEvidence]),
    "Product failure rerun as infrastructure mutant control",
    (error) => error.includes("must be an infrastructure error"),
  );

  const successfulPreviousEvidence = structuredClone(failedEvidenceAttemptOne);
  successfulPreviousEvidence.run.exit_code = 0;
  successfulPreviousEvidence.run.result = "passed";
  successfulPreviousEvidence.assertions[0].outcome = "passed";
  const successfulPreviousBundle = [successfulPreviousEvidence, validEvidence];
  if (
    validateInstances(
      validators.evidence,
      successfulPreviousBundle,
      "Schema-valid prior successful evidence bundle control",
    )
  ) {
    expectSemanticInvalid(
      evidenceBundleSemanticErrors(successfulPreviousBundle),
      "Prior successful evidence bundle control",
      (error) => error.includes("promotion-eligible prior evidence"),
    );
  }

  const evidenceBindingControls = [
    ["candidate_id", (evidence) => (evidence.candidate_id = "RC-0.3.0-20260717T120000Z-1234567")],
    ["source_commit", (evidence) => (evidence.source_commit = "d".repeat(40))],
    ["scenario_id", (evidence) => (evidence.scenario_id = "SCN-TIME-002")],
    [
      "proof_obligation_id",
      (evidence) => (evidence.proof_obligation_id = "OBL-FAULT-001"),
    ],
    [
      "support_cell_id",
      (evidence) => (evidence.support_cell_id = "SUP-RN-IOS-CURRENT-001"),
    ],
    ["proof_type", (evidence) => (evidence.proof_type = "server-black-box")],
    [
      "contract_snapshot_sha256",
      (evidence) => (evidence.contract_snapshot_sha256 = "9".repeat(64)),
    ],
    [
      "make_target",
      (evidence) => {
        evidence.run.make_target = "test-swift";
        evidence.run.argv = ["make", "test-swift"];
      },
    ],
    [
      "requirement_ids",
      (evidence) => (evidence.requirement_ids = ["SYNC-TIME-002"]),
    ],
    ["artifact_ids", (evidence) => (evidence.artifact_ids = ["ART-SWIFT-002"])],
  ];
  for (const [binding, mutate] of evidenceBindingControls) {
    const driftedEvidence = structuredClone(validEvidence);
    mutate(driftedEvidence);
    const driftedBundle = [failedEvidenceAttemptOne, driftedEvidence];
    if (
      validateInstances(
        validators.evidence,
        driftedBundle,
        `Schema-valid ${binding} rerun drift bundle control`,
      )
    ) {
      expectSemanticInvalid(
        evidenceBundleSemanticErrors(driftedBundle),
        `${binding} rerun drift bundle control`,
        (error) => error.includes(`changed ${binding}`),
      );
    }
  }

  if (
    validateInstance(
      validators.rcManifest,
      validManifest,
      "Valid RC manifest self-test",
    ) &&
    supportMatrixValid &&
    artifactInventoryValid
  ) {
    expectSemanticValid(
      manifestSemanticErrors(validManifest, supportMatrix, artifactInventory),
      "Valid RC manifest semantic self-test",
    );
    expectSemanticValid(
      manifestEvidenceClosureErrors(validManifest, validEvidenceBundle, [
        validScenario,
      ], validRequirementSubset, validClosureSupportMatrix),
      "Valid RC manifest retry-chain closure self-test",
    );
  }

  const bundleWithCrossCollectionPathCollision = structuredClone(
    validEvidenceBundle,
  );
  bundleWithCrossCollectionPathCollision
    .find(({ evidence_id }) => evidence_id === "EVD-NATIVE-002")
    .attachments[0].path = validManifest.artifacts[0].payloads[0].path;
  expectSemanticInvalid(
    manifestEvidenceClosureErrors(
      validManifest,
      bundleWithCrossCollectionPathCollision,
      [validScenario],
      validRequirementSubset,
      validClosureSupportMatrix,
    ),
    "Cross-collection RC candidate path collision control",
    (error) => error.includes("RC candidate file path") && error.includes("is shared"),
  );

  const manifestWithDuplicatePayloadPath = structuredClone(validManifest);
  manifestWithDuplicatePayloadPath.artifacts[1].payloads[0].path =
    manifestWithDuplicatePayloadPath.artifacts[0].payloads[0].path;
  expectSemanticInvalid(
    manifestSemanticErrors(
      manifestWithDuplicatePayloadPath,
      supportMatrix,
      artifactInventory,
    ),
    "Manifest duplicate artifact payload path control",
    (error) => error.includes("Manifest file path") && error.includes("is shared"),
  );

  const manifestWithTraversalPayloadPath = structuredClone(validManifest);
  manifestWithTraversalPayloadPath.artifacts[0].payloads[0].path =
    "../outside.bin";
  expectInvalid(
    validators.rcManifest,
    manifestWithTraversalPayloadPath,
    "Manifest artifact payload path traversal",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/artifacts/0/payloads/0/path" &&
          error.keyword === "pattern",
      ),
  );

  const manifestWithMismatchedAttestationSubject = structuredClone(validManifest);
  manifestWithMismatchedAttestationSubject.attestations[0].subject_payloads[0].sha256 =
    "f".repeat(64);
  expectSemanticInvalid(
    manifestSemanticErrors(
      manifestWithMismatchedAttestationSubject,
      supportMatrix,
      artifactInventory,
    ),
    "Attestation payload digest substitution mutant control",
    (error) => error.includes("subject payloads do not exactly match artifact"),
  );

  const manifestWithUnsignedAttestationDigest = structuredClone(validManifest);
  manifestWithUnsignedAttestationDigest.attestations[0].sha256 = "f".repeat(64);
  expectSemanticInvalid(
    manifestSemanticErrors(
      manifestWithUnsignedAttestationDigest,
      supportMatrix,
      artifactInventory,
    ),
    "Attestation digest outside signed Sigstore binding mutant control",
    (error) => error.includes("does not bind the attestation digest"),
  );

  const manifestWithUnapprovedSigstoreIdentity = structuredClone(validManifest);
  manifestWithUnapprovedSigstoreIdentity.attestations[0].sigstore_verification.certificate_identity =
    "https://github.com/acme/repo/.github/workflows/release.yml@refs/heads/main";
  expectInvalid(
    validators.rcManifest,
    manifestWithUnapprovedSigstoreIdentity,
    "Attestation with an unapproved Sigstore certificate identity",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath ===
            "/attestations/0/sigstore_verification/certificate_identity" &&
          error.keyword === "const",
      ),
  );

  const manifestWithWrongSbomMediaType = structuredClone(validManifest);
  manifestWithWrongSbomMediaType.attestations[0].media_type =
    "application/vnd.in-toto+json";
  expectInvalid(
    validators.rcManifest,
    manifestWithWrongSbomMediaType,
    "SBOM attestation with provenance media type",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/attestations/0/media_type" &&
          error.keyword === "const",
      ),
  );

  const manifestWithoutSigstoreIssuer = structuredClone(validManifest);
  delete manifestWithoutSigstoreIssuer.attestations[0].sigstore_verification
    .certificate_issuer;
  expectInvalid(
    validators.rcManifest,
    manifestWithoutSigstoreIssuer,
    "Attestation without Sigstore certificate issuer",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/attestations/0/sigstore_verification" &&
          error.keyword === "required" &&
          error.params.missingProperty === "certificate_issuer",
      ),
  );

  const manifestWithoutSigstoreDigest = structuredClone(validManifest);
  delete manifestWithoutSigstoreDigest.attestations[0].sigstore_verification
    .bundle_sha256;
  expectInvalid(
    validators.rcManifest,
    manifestWithoutSigstoreDigest,
    "Attestation without content-addressed Sigstore bundle",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/attestations/0/sigstore_verification" &&
          error.keyword === "required" &&
          error.params.missingProperty === "bundle_sha256",
      ),
  );

  const manifestWithoutFailedPredecessor = structuredClone(validManifest);
  manifestWithoutFailedPredecessor.evidence =
    manifestWithoutFailedPredecessor.evidence.filter(
      ({ evidence_id }) => evidence_id !== failedEvidenceAttemptOne.evidence_id,
    );
  expectSemanticInvalid(
    manifestEvidenceClosureErrors(
      manifestWithoutFailedPredecessor,
      validEvidenceBundle,
      [validScenario],
      validRequirementSubset,
      validClosureSupportMatrix,
    ),
    "RC manifest missing failed predecessor evidence control",
    (error) => error.includes("omits predecessor evidence"),
  );

  const manifestWithoutFaultObligation = structuredClone(validManifest);
  manifestWithoutFaultObligation.evidence =
    manifestWithoutFaultObligation.evidence.filter(
      ({ proof_obligation_id }) => proof_obligation_id !== "OBL-FAULT-001",
    );
  const bundleWithoutFaultObligation = validEvidenceBundle.filter(
    ({ proof_obligation_id }) => proof_obligation_id !== "OBL-FAULT-001",
  );
  expectSemanticInvalid(
    manifestEvidenceClosureErrors(
      manifestWithoutFaultObligation,
      bundleWithoutFaultObligation,
      [validScenario],
      validRequirementSubset,
      validClosureSupportMatrix,
    ),
    "RC manifest missing one scenario proof obligation control",
    (error) =>
      error.includes("obligation OBL-FAULT-001 has no evidence lineage"),
  );

  const scenarioWithoutServerProof = structuredClone(validScenario);
  scenarioWithoutServerProof.proof_obligations =
    scenarioWithoutServerProof.proof_obligations.filter(
      ({ proof_type }) => proof_type !== "server-black-box",
    );
  const manifestWithoutGlobalServerProof = structuredClone(validManifest);
  manifestWithoutGlobalServerProof.evidence =
    manifestWithoutGlobalServerProof.evidence.filter(
      ({ proof_obligation_id }) => proof_obligation_id !== "OBL-SERVER-001",
    );
  const bundleWithoutGlobalServerProof = validEvidenceBundle.filter(
    ({ proof_obligation_id }) => proof_obligation_id !== "OBL-SERVER-001",
  );
  expectSemanticInvalid(
    manifestEvidenceClosureErrors(
      manifestWithoutGlobalServerProof,
      bundleWithoutGlobalServerProof,
      [scenarioWithoutServerProof],
      validRequirementSubset,
      validClosureSupportMatrix,
    ),
    "Requirement-global server proof closure control",
    (error) =>
      error.includes("requirement SYNC-TIME-001") &&
      error.includes("lacks authored server-black-box proof"),
  );

  const expandedClosureSupportMatrix = structuredClone(
    validClosureSupportMatrix,
  );
  expandedClosureSupportMatrix.cells.push({
    id: "SUP-RN-IOS-CURRENT-001",
    component: "react-native-client",
    policy: "required",
  });
  expectSemanticInvalid(
    manifestEvidenceClosureErrors(
      validManifest,
      validEvidenceBundle,
      [validScenario],
      validRequirementSubset,
      expandedClosureSupportMatrix,
    ),
    "Requirement proof missing one applicable support cell control",
    (error) =>
      error.includes("requirement SYNC-TIME-001") &&
      error.includes("native-e2e proof for support cell SUP-RN-IOS-CURRENT-001"),
  );

  const manifestWithoutEvidenceScenario = structuredClone(validManifest);
  manifestWithoutEvidenceScenario.scenarios[0].scenario_id = "SCN-TIME-002";
  expectSemanticInvalid(
    manifestEvidenceClosureErrors(
      manifestWithoutEvidenceScenario,
      validEvidenceBundle,
      [validScenario],
      validRequirementSubset,
      validClosureSupportMatrix,
    ),
    "Manifest evidence for an absent scenario mutant control",
    (error) => error.includes("references absent scenario SCN-TIME-001"),
  );

  const manifestWithMismatchedEvidenceObligation = structuredClone(validManifest);
  manifestWithMismatchedEvidenceObligation.evidence[1].proof_obligation_id =
    "OBL-FAULT-001";
  expectSemanticInvalid(
    manifestEvidenceClosureErrors(
      manifestWithMismatchedEvidenceObligation,
      validEvidenceBundle,
      [validScenario],
      validRequirementSubset,
      validClosureSupportMatrix,
    ),
    "Manifest proof obligation reference mismatch mutant control",
    (error) => error.includes("proof_obligation_id does not match loaded evidence"),
  );

  const evidenceWithUnknownArtifact = structuredClone(validEvidence);
  evidenceWithUnknownArtifact.artifact_ids = ["ART-UNKNOWN-001"];
  expectSemanticInvalid(
    evidenceManifestBindingErrors(evidenceWithUnknownArtifact, validManifest),
    "Evidence with an artifact absent from the RC manifest control",
    (error) => error.includes("is not bound by the RC manifest"),
  );

  const manifestWithoutTrustedRerunApprover = structuredClone(validManifest);
  manifestWithoutTrustedRerunApprover.trusted_rerun_approvers = [
    "github:other-release-manager",
  ];
  expectSemanticInvalid(
    evidenceManifestBindingErrors(
      validEvidence,
      manifestWithoutTrustedRerunApprover,
    ),
    "Evidence approved by an untrusted rerun identity control",
    (error) => error.includes("rerun approver is not trusted"),
  );

  const wrongProtocolManifest = structuredClone(validManifest);
  wrongProtocolManifest.protocol_version = 2;
  expectInvalid(
    validators.rcManifest,
    wrongProtocolManifest,
    "RC manifest for a protocol other than version 3",
    (errors) =>
      errors.some((error) => error.instancePath === "/protocol_version"),
  );

  const incompleteContractManifest = structuredClone(validManifest);
  incompleteContractManifest.contract.behavioral_files.pop();
  expectInvalid(
    validators.rcManifest,
    incompleteContractManifest,
    "RC manifest with an incomplete behavioral contract snapshot",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/contract/behavioral_files" &&
          error.keyword === "minItems",
      ),
  );

  const manifestWithoutVectorCatalog = structuredClone(validManifest);
  delete manifestWithoutVectorCatalog.contract.verification_inputs.vector_catalog;
  expectInvalid(
    validators.rcManifest,
    manifestWithoutVectorCatalog,
    "RC manifest without the authored vector catalog binding",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/contract/verification_inputs" &&
          error.keyword === "required" &&
          error.params.missingProperty === "vector_catalog",
      ),
  );

  const nonAcceptedAdrManifest = structuredClone(validManifest);
  nonAcceptedAdrManifest.contract.behavioral_files[6].status = null;
  expectInvalid(
    validators.rcManifest,
    nonAcceptedAdrManifest,
    "RC manifest with a non-Accepted ADR",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath === "/contract/behavioral_files/6/status",
      ),
  );

  for (const [binding, mutate] of [
    ["release_version", (evidence) => (evidence.release_version = "0.3.1")],
    ["protocol_version", (evidence) => (evidence.protocol_version = 4)],
    [
      "contract_snapshot_sha256",
      (evidence) => (evidence.contract_snapshot_sha256 = "9".repeat(64)),
    ],
  ]) {
    const driftedEvidence = structuredClone(validEvidence);
    mutate(driftedEvidence);
    expectSemanticInvalid(
      evidenceManifestBindingErrors(driftedEvidence, validManifest),
      `Evidence-to-manifest ${binding} drift control`,
      (error) => error.includes(binding) && error.includes("does not match"),
    );
  }

  const resolvedDimensionControls = [
    [
      "RN iOS minimum below iOS 16",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-RN-IOS-MIN-001",
        );
        cell.dimensions.find(({ name }) => name === "ios").version = "15.999";
      },
      (error) =>
        error.includes("SUP-RN-IOS-MIN-001 ios version 15.999") &&
        error.includes("below authored minimum 16"),
    ],
    [
      "iOS 17 outside iOS 16 minimum line",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-IOS-MIN-001",
        );
        cell.dimensions.find(({ name }) => name === "ios").version = "17.0.1";
      },
      (error) =>
        error.includes("SUP-IOS-MIN-001 ios version 17.0.1") &&
        error.includes("outside declared minimum line 16.x"),
    ],
    [
      "macOS 14 outside macOS 13 minimum line",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-MACOS-MIN-001",
        );
        cell.dimensions.find(({ name }) => name === "macos").version =
          "14.0.1";
      },
      (error) =>
        error.includes("SUP-MACOS-MIN-001 macos version 14.0.1") &&
        error.includes("outside declared minimum line 13.x"),
    ],
    [
      "Android API 25 outside API 24 minimum",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-ANDROID-MIN-001",
        );
        cell.dimensions.find(({ name }) => name === "android-api").version =
          "25";
      },
      (error) =>
        error.includes("SUP-ANDROID-MIN-001 android-api version 25") &&
        error.includes("outside declared minimum API 24"),
    ],
    [
      "RN runtime outside 0.83.x",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-RN-IOS-MIN-001",
        );
        cell.dimensions.find(({ name }) => name === "react-native").version =
          "0.82.999+1.1";
      },
      (error) =>
        error.includes("SUP-RN-IOS-MIN-001 react-native dimension") &&
        error.includes("authored series 0.83.x"),
    ],
    [
      "RN React version below 19.2",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-RN-IOS-MIN-001",
        );
        cell.dimensions.find(({ name }) => name === "react").version = "18.3.1";
      },
      (error) =>
        error.includes("SUP-RN-IOS-MIN-001 react dimension") &&
        error.includes("React 19.2 or later"),
    ],
    [
      "nonnumeric minimum platform resolution",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-MACOS-MIN-001",
        );
        cell.dimensions.find(({ name }) => name === "macos").version =
          "13.0+1.1";
      },
      (error) =>
        error.includes("SUP-MACOS-MIN-001 minimum platform dimension macos") &&
        error.includes("dot-separated numeric version"),
    ],
    [
      "renamed required platform dimension",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-IOS-MIN-001",
        );
        cell.dimensions.find(({ name }) => name === "ios").name = "iphone-os";
      },
      (error) =>
        error.includes("SUP-IOS-MIN-001 is missing required resolved dimension") &&
        error.includes('"ios"'),
    ],
    [
      "duplicate logical dimension name",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-IOS-MIN-001",
        );
        cell.dimensions.push({ name: "ios", version: "17.999" });
      },
      (error) =>
        error.includes("SUP-IOS-MIN-001 contains duplicate dimension name") &&
        error.includes('"ios"'),
    ],
    [
      "PostgreSQL 17 in PG18 cell",
      (manifest) => {
        const cell = manifest.resolved_support_cells.find(
          ({ support_cell_id }) => support_cell_id === "SUP-PG-018",
        );
        cell.dimensions.find(({ name }) => name === "postgresql").version =
          "17.999";
      },
      (error) =>
        error.includes("SUP-PG-018 postgresql dimension") &&
        error.includes("major version 18"),
    ],
  ];
  for (const [label, mutate, matchesExpectedError] of
    resolvedDimensionControls) {
    const manifest = structuredClone(validManifest);
    mutate(manifest);
    if (
      validateInstance(
        validators.rcManifest,
        manifest,
        `Schema-valid ${label} helper control`,
      ) &&
      supportMatrixValid
    ) {
      expectSemanticInvalid(
        manifestSemanticErrors(manifest, supportMatrix, artifactInventory),
        `${label} helper control`,
        matchesExpectedError,
      );
    }
  }

  const movingVersionManifest = structuredClone(validManifest);
  movingVersionManifest.resolved_support_cells[0].dimensions[0].version =
    "current-stable";
  expectInvalid(
    validators.rcManifest,
    movingVersionManifest,
    "RC manifest with current-stable as an exact version",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath ===
          "/resolved_support_cells/0/dimensions/0/version",
      ),
  );
  const wildcardVersionManifest = structuredClone(validManifest);
  wildcardVersionManifest.artifacts[0].package_version = "0.83.x";
  expectInvalid(
    validators.rcManifest,
    wildcardVersionManifest,
    "RC manifest with 0.83.x as an exact version",
    (errors) =>
      errors.some(
        (error) => error.instancePath === "/artifacts/0/package_version",
      ),
  );

  const embeddedCurrentVersionManifest = structuredClone(validManifest);
  embeddedCurrentVersionManifest.resolved_support_cells[0].dimensions[0].version =
    "current-stable-2026";
  expectInvalid(
    validators.rcManifest,
    embeddedCurrentVersionManifest,
    "RC manifest with current-stable-2026 as an exact version",
    (errors) =>
      errors.some(
        (error) =>
          error.instancePath ===
          "/resolved_support_cells/0/dimensions/0/version",
      ),
  );

  const latestBuildVersionManifest = structuredClone(validManifest);
  latestBuildVersionManifest.artifacts[0].package_version = "latest-build";
  expectInvalid(
    validators.rcManifest,
    latestBuildVersionManifest,
    "RC manifest with latest-build as an exact version",
    (errors) =>
      errors.some((error) => error.instancePath === "/artifacts/0/package_version"),
  );

  for (const [version, supportCellId, dimensionName] of [
    ["preview", "SUP-IOS-CURRENT-001", "ios"],
    ["edge", "SUP-MACOS-CURRENT-001", "macos"],
    ["rolling", "SUP-ANDROID-CURRENT-001", "android-api"],
  ]) {
    const currentTrackManifest = structuredClone(validManifest);
    const cellIndex = currentTrackManifest.resolved_support_cells.findIndex(
      ({ support_cell_id }) => support_cell_id === supportCellId,
    );
    const dimensionIndex = currentTrackManifest.resolved_support_cells[
      cellIndex
    ].dimensions.findIndex(({ name }) => name === dimensionName);
    currentTrackManifest.resolved_support_cells[cellIndex].dimensions[
      dimensionIndex
    ].version = version;
    const expectedPath =
      `/resolved_support_cells/${cellIndex}/dimensions/${dimensionIndex}/version`;
    expectInvalid(
      validators.rcManifest,
      currentTrackManifest,
      `RC manifest current-track dimension with ${version} as an exact version`,
      (errors) =>
        errors.some(
          (error) =>
            error.instancePath === expectedPath &&
            error.keyword === "pattern" &&
            error.schemaPath.endsWith("/exactVersion/pattern"),
        ),
    );
  }

  for (const version of ["1.0-preview", "1.0-edge", "1.0-rolling"]) {
    const prereleaseManifest = structuredClone(validManifest);
    prereleaseManifest.artifacts[0].package_version = version;
    expectInvalid(
      validators.rcManifest,
      prereleaseManifest,
      `RC manifest with embedded prerelease ${version} as an exact version`,
      (errors) =>
        errors.some(
          (error) =>
            error.instancePath === "/artifacts/0/package_version",
        ),
    );
  }

  for (const version of ["1.0+preview", "1.0+edge", "1.0+rolling"]) {
    const mutableBuildManifest = structuredClone(validManifest);
    mutableBuildManifest.artifacts[0].package_version = version;
    expectInvalid(
      validators.rcManifest,
      mutableBuildManifest,
      `RC manifest with mutable build metadata ${version} as an exact version`,
      (errors) =>
        errors.some(
          (error) =>
            error.instancePath === "/artifacts/0/package_version",
        ),
    );
  }

  for (const [label, version] of [
    ["main", "1.0+1MaIn-2build.1"],
    ["master", "1.0+1MASTER.20260717"],
    ["dev", "1.0+1DeV.1"],
    ["canary", "1.0+1build-2CaNaRy-001"],
    ["next", "1.0+1build-2NeXt-001"],
    ["trunk", "1.0+1TrUnK-2build-001"],
  ]) {
    const movingLabelManifest = structuredClone(validManifest);
    movingLabelManifest.artifacts[0].package_version = version;
    expectInvalid(
      validators.rcManifest,
      movingLabelManifest,
      `RC manifest with embedded ${label} moving label as an exact version`,
      (errors) =>
        errors.some((error) => error.instancePath === "/artifacts/0/package_version"),
    );
  }

  for (const collection of [
    "scenarios",
    "evidence",
    "resolved_support_cells",
    "artifacts",
    "attestations",
  ]) {
    const structurallyDuplicatedManifest = structuredClone(validManifest);
    structurallyDuplicatedManifest[collection].push(
      structuredClone(structurallyDuplicatedManifest[collection][0]),
    );
    expectInvalid(
      validators.rcManifest,
      structurallyDuplicatedManifest,
      `RC manifest with structurally duplicate ${collection}`,
      (errors) =>
        errors.some(
          (error) =>
            error.instancePath === `/${collection}` &&
            error.keyword === "uniqueItems",
        ),
    );
  }

  const logicalDuplicateManifestControls = [
    ["scenarios", (entry) => (entry.path = "conformance/scenarios/duplicate.json")],
    ["evidence", (entry) => (entry.path = "evidence/duplicate.json")],
    [
      "resolved_support_cells",
      (entry) => (entry.dimensions = [{ name: "postgresql", version: "18.1" }]),
    ],
    ["artifacts", (entry) => (entry.payloads[0].path = "artifacts/duplicate.zip")],
    [
      "attestations",
      (entry) => (entry.path = "attestations/duplicate.json"),
    ],
  ];
  for (const [collection, mutate] of logicalDuplicateManifestControls) {
    const logicalDuplicateManifest = structuredClone(validManifest);
    const duplicateEntry = structuredClone(logicalDuplicateManifest[collection][0]);
    mutate(duplicateEntry);
    logicalDuplicateManifest[collection].push(duplicateEntry);
    if (
      validateInstance(
        validators.rcManifest,
        logicalDuplicateManifest,
        `Schema-valid duplicate logical ID in manifest ${collection} control`,
      ) &&
      supportMatrixValid
    ) {
      expectSemanticInvalid(
        manifestSemanticErrors(
          logicalDuplicateManifest,
          supportMatrix,
          artifactInventory,
        ),
        `Duplicate logical ID in manifest ${collection} control`,
        (error) => error.includes("contains duplicate logical ID"),
      );
    }
  }

  const manifestMissingPg18 = structuredClone(validManifest);
  manifestMissingPg18.resolved_support_cells[0] = {
    support_cell_id: "SUP-PG-017",
    dimensions: [{ name: "postgresql", version: "17.0" }],
  };
  if (
    validateInstance(
      validators.rcManifest,
      manifestMissingPg18,
      "Schema-valid manifest missing required PG18 cell control",
    ) &&
    supportMatrixValid
  ) {
    const errors = manifestSemanticErrors(
      manifestMissingPg18,
      supportMatrix,
      artifactInventory,
    );
    expectSemanticInvalid(
      errors,
      "Manifest missing required PG18 cell control",
      (error) => error.includes("missing required support cell SUP-PG-018"),
    );
    expectSemanticInvalid(
      errors,
      "Manifest excluded PG17 cell control",
      (error) => error.includes("contains excluded support cell SUP-PG-017"),
    );
  }

  const manifestWithUnknownCell = structuredClone(validManifest);
  manifestWithUnknownCell.resolved_support_cells[0] = {
    support_cell_id: "SUP-UNKNOWN-001",
    dimensions: [{ name: "unknown", version: "1.0.0" }],
  };
  if (
    validateInstance(
      validators.rcManifest,
      manifestWithUnknownCell,
      "Schema-valid manifest unknown support cell control",
    ) &&
    supportMatrixValid
  ) {
    expectSemanticInvalid(
      manifestSemanticErrors(
        manifestWithUnknownCell,
        supportMatrix,
        artifactInventory,
      ),
      "Manifest unknown support cell control",
      (error) => error.includes("contains unknown support cell SUP-UNKNOWN-001"),
    );
  }

  if (failures.length > 0) {
    console.error(`Contract verification failed with ${failures.length} error(s):`);
    failures.forEach((message) => console.error(`  - ${message}`));
    process.exitCode = 1;
    return;
  }

  console.log(
    "Contract verification passed: schemas, authored contracts, references, support policy, semantic helpers, promotion eligibility, evidence bundles, manifests, and negative controls.",
  );
}

main().catch((error) => {
  console.error(`Contract verification failed unexpectedly: ${error.stack ?? error}`);
  process.exitCode = 1;
});
