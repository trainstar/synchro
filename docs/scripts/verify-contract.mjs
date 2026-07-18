import { readFile, realpath, stat } from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../..");
const conformanceDir = resolve(repoRoot, "conformance");
const failures = [];

const schemaFiles = {
  requirements: "requirements.schema.json",
  supportMatrix: "support-matrix.schema.json",
  scenario: "scenario.schema.json",
  evidence: "evidence.schema.json",
  rcManifest: "rc-manifest.schema.json",
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

function pathIsInside(root, candidate) {
  const pathFromRoot = relative(root, candidate);
  return (
    pathFromRoot === "" ||
    (!isAbsolute(pathFromRoot) &&
      pathFromRoot !== ".." &&
      !pathFromRoot.startsWith(`..${sep}`))
  );
}

async function normativeReferenceErrors(requirements) {
  const errors = [];
  const rootRealPath = await realpath(repoRoot);
  const anchorCache = new Map();

  for (const requirement of requirements.requirements) {
    for (const reference of requirement.normative_references) {
      const context = `${requirement.id} reference ${reference.path}${reference.anchor}`;
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
  return errors;
}

function scenarioSemanticErrors(scenario) {
  const errors = [
    ...duplicateLogicalIdErrors(scenario.steps, "id", "Scenario steps"),
    ...duplicateLogicalIdErrors(
      scenario.assertions,
      "id",
      "Scenario assertions",
    ),
  ];
  const assertionIds = new Set(scenario.assertions.map(({ id }) => id));
  for (const assertionId of scenario.negative_control?.detected_by ?? []) {
    if (!assertionIds.has(assertionId)) {
      errors.push(
        `Negative control detected_by ID ${JSON.stringify(assertionId)} does not name an assertion in scenario ${scenario.id}`,
      );
    }
  }
  return errors;
}

function stringSetsEqual(left, right) {
  if (left.length !== right.length) return false;
  const rightValues = new Set(right);
  return left.every((value) => rightValues.has(value));
}

function evidenceScenarioSemanticErrors(evidence, scenario) {
  const errors = [];
  if (evidence.scenario_id !== scenario.id) {
    errors.push(
      `${evidence.evidence_id} names scenario ${evidence.scenario_id}, not ${scenario.id}`,
    );
  }

  const declaredRequirements = new Set(scenario.requirement_ids);
  for (const requirementId of evidence.requirement_ids) {
    if (!declaredRequirements.has(requirementId)) {
      errors.push(
        `${evidence.evidence_id} requirement ${requirementId} is not declared by scenario ${scenario.id}`,
      );
    }
  }
  if (!scenario.proof_types.includes(evidence.proof_type)) {
    errors.push(
      `${evidence.evidence_id} proof type ${evidence.proof_type} is not declared by scenario ${scenario.id}`,
    );
  }

  if (evidence.proof_type !== "negative-control") return errors;
  const evidenceControl = evidence.negative_control;
  const scenarioControl = scenario.negative_control;
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
    if (!attachmentIds.has(attachmentId)) {
      errors.push(
        `${evidence.evidence_id} negative-control attachment ${attachmentId} does not identify an evidence attachment`,
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
  return errors;
}

function evidenceBundleSemanticErrors(evidenceBundle) {
  const errors = duplicateLogicalIdErrors(
    evidenceBundle,
    "evidence_id",
    "Evidence bundle",
  );
  const evidenceById = new Map();
  for (const evidence of evidenceBundle) {
    if (!evidenceById.has(evidence.evidence_id)) {
      evidenceById.set(evidence.evidence_id, evidence);
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

    const scalarBindings = [
      ["candidate_id", previous.candidate_id, evidence.candidate_id],
      ["source_commit", previous.source_commit, evidence.source_commit],
      ["scenario_id", previous.scenario_id, evidence.scenario_id],
      ["support_cell_id", previous.support_cell_id, evidence.support_cell_id],
      ["proof_type", previous.proof_type, evidence.proof_type],
    ];
    for (const [binding, previousValue, currentValue] of scalarBindings) {
      if (previousValue !== currentValue) {
        errors.push(
          `${evidence.evidence_id} rerun changed ${binding} from ${JSON.stringify(previousValue)} to ${JSON.stringify(currentValue)}`,
        );
      }
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
    if (
      !previous.assertions.some(({ outcome }) =>
        ["failed", "error"].includes(outcome),
      )
    ) {
      errors.push(
        `${evidence.evidence_id} rerun prior evidence ${previous.evidence_id} must contain a failed or error assertion`,
      );
    }
    if (previous.assertions.some(({ outcome }) => outcome === "skipped")) {
      errors.push(
        `${evidence.evidence_id} rerun prior evidence ${previous.evidence_id} must not contain skipped assertions`,
      );
    }
  }
  return errors;
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

function manifestSemanticErrors(manifest, supportMatrix) {
  const errors = [];
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
  return errors;
}

const validScenario = {
  $schema: "https://synchro.dev/conformance/schemas/scenario.schema.json",
  schema_version: 1,
  id: "SCN-TIME-001",
  title: "Reject a noncanonical wire timestamp",
  requirement_ids: ["SYNC-TIME-001"],
  proof_types: ["native-e2e", "negative-control"],
  steps: [
    {
      id: "STEP-TIME-001",
      phase: "exercise",
      operation: { name: "send-timestamp", payload: { value: "not-a-time" } },
    },
  ],
  assertions: [
    {
      id: "ASSERT-TIME-001",
      description: "The invalid timestamp is rejected.",
      predicate: { name: "is-rejected", payload: { expected: true } },
    },
  ],
  negative_control: {
    fault_id: "FAULT-TIME-001",
    detected_by: ["ASSERT-TIME-001"],
  },
};

const validEvidence = {
  $schema: "https://synchro.dev/conformance/schemas/evidence.schema.json",
  schema_version: 1,
  evidence_id: "EVD-NATIVE-002",
  candidate_id: "RC-0.3.0-20260717T120000Z-abcdef0",
  support_cell_id: "SUP-RN-IOS-MIN-001",
  scenario_id: "SCN-TIME-001",
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
    url: "https://ci.example.test/runs/2",
    attempt: 2,
    started_at: "2026-07-17T12:00:00Z",
    completed_at: "2026-07-17T12:01:00Z",
    exit_code: 0,
    previous_evidence_id: "EVD-NATIVE-001",
    rerun_reason: "Infrastructure host was replaced.",
    rerun_diagnosis: "The original host lost network connectivity.",
    corrective_change: "The run moved to a healthy replacement host.",
  },
  environment: [{ name: "platform", value: "iOS 16.0" }],
  assertions: [{ assertion_id: "ASSERT-TIME-001", outcome: "passed" }],
  attachments: [
    {
      id: "ATT-LOG-001",
      path: "evidence/run.log",
      media_type: "text/plain",
      sha256: "c".repeat(64),
    },
  ],
  artifact_ids: ["ART-SWIFT-001"],
  negative_control: null,
  seed: null,
};

const failedEvidenceAttemptOne = structuredClone(validEvidence);
failedEvidenceAttemptOne.evidence_id = "EVD-NATIVE-001";
failedEvidenceAttemptOne.run = {
  id: "RUN-NATIVE-001",
  url: "https://ci.example.test/runs/1",
  attempt: 1,
  started_at: "2026-07-17T11:55:00Z",
  completed_at: "2026-07-17T11:56:00Z",
  exit_code: 1,
  previous_evidence_id: null,
  rerun_reason: null,
  rerun_diagnosis: null,
  corrective_change: null,
};
failedEvidenceAttemptOne.assertions = [
  { assertion_id: "ASSERT-TIME-001", outcome: "failed" },
];

const validNegativeControlEvidence = structuredClone(validEvidence);
validNegativeControlEvidence.evidence_id = "EVD-NEGATIVE-001";
validNegativeControlEvidence.proof_type = "negative-control";
validNegativeControlEvidence.run = {
  id: "RUN-NEGATIVE-001",
  url: "https://ci.example.test/runs/negative-control-1",
  attempt: 1,
  started_at: "2026-07-17T12:02:00Z",
  completed_at: "2026-07-17T12:03:00Z",
  exit_code: 0,
  previous_evidence_id: null,
  rerun_reason: null,
  rerun_diagnosis: null,
  corrective_change: null,
};
validNegativeControlEvidence.negative_control = {
  fault_id: "FAULT-TIME-001",
  control_subject_id: "CTRL-TIMESTAMP-001",
  control_subject_type: "synthetic-fault",
  detected_by: ["ASSERT-TIME-001"],
  outcome: "detected",
  attachment_ids: ["ATT-LOG-001"],
};

const validManifest = {
  $schema: "https://synchro.dev/conformance/schemas/rc-manifest.schema.json",
  schema_version: 1,
  candidate_id: "RC-0.3.0-20260717T120000Z-abcdef0",
  release_version: "0.3.0",
  source_commit: "a".repeat(40),
  created_at: "2026-07-17T12:00:00Z",
  generator: {
    name: "rc-generator",
    version: "1.0.0",
    binary_sha256: "b".repeat(64),
  },
  contract: {
    requirements: {
      path: "conformance/requirements.json",
      sha256: "c".repeat(64),
    },
    support_matrix: {
      path: "conformance/support-matrix.json",
      sha256: "d".repeat(64),
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
      evidence_id: "EVD-NATIVE-002",
      proof_type: "native-e2e",
      path: "evidence/native.json",
      sha256: "f".repeat(64),
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
      ],
    },
  ],
  artifacts: [
    {
      id: "ART-SWIFT-001",
      name: "Swift SDK",
      path: "artifacts/synchro.zip",
      version: "0.3.0",
      sha256: "1".repeat(64),
    },
  ],
  attestations: [
    {
      id: "ATTST-PROVENANCE-001",
      type: "provenance",
      path: "attestations/provenance.json",
      sha256: "2".repeat(64),
    },
  ],
};

async function main() {
  const ajv = new Ajv2020({ allErrors: true, strict: false, validateSchema: true });
  addFormats(ajv, { formats: ["date-time", "uri"], mode: "full" });

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
  }

  if (
    validateInstance(validators.scenario, validScenario, "Valid scenario self-test")
  ) {
    expectSemanticValid(
      scenarioSemanticErrors(validScenario),
      "Valid scenario semantic self-test",
    );
  }
  const scenarioWithoutControl = structuredClone(validScenario);
  delete scenarioWithoutControl.negative_control;
  expectInvalid(
    validators.scenario,
    scenarioWithoutControl,
    "Negative-control scenario without negative_control",
    (errors) =>
      errors.some(
        (error) =>
          error.keyword === "required" &&
          error.params.missingProperty === "negative_control",
      ),
  );

  const scenarioWithUnknownDetectedBy = structuredClone(validScenario);
  scenarioWithUnknownDetectedBy.negative_control.detected_by = [
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
    description: "A distinct assertion object reuses the logical ID.",
    predicate: { name: "is-rejected-again", payload: { expected: true } },
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
    operation: { name: "reset-clock", payload: null },
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

  if (
    validateInstance(validators.evidence, validEvidence, "Valid evidence self-test")
  ) {
    expectSemanticValid(
      evidencePromotionEligibilityErrors(validEvidence),
      "Passing evidence promotion-eligibility self-test",
    );
    expectSemanticValid(
      evidenceScenarioSemanticErrors(validEvidence, validScenario),
      "Passing evidence-to-scenario binding self-test",
    );
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
      ),
      "Valid negative-control evidence binding self-test",
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
      (error) => error.includes("must not contain skipped assertions"),
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
  unlinkedRerun.run.rerun_reason = null;
  unlinkedRerun.run.rerun_diagnosis = null;
  unlinkedRerun.run.corrective_change = null;
  expectInvalid(
    validators.evidence,
    unlinkedRerun,
    "Evidence rerun without required rerun metadata",
    (errors) =>
      [
        "/run/previous_evidence_id",
        "/run/rerun_reason",
        "/run/rerun_diagnosis",
        "/run/corrective_change",
      ].every((path) => errors.some((error) => error.instancePath === path)),
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
      ),
      "Undeclared evidence requirement control",
      (error) => error.includes("is not declared by scenario"),
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
      ),
      "Undeclared evidence proof type control",
      (error) => error.includes("proof type server-black-box is not declared"),
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
        evidenceScenarioSemanticErrors(evidence, validScenario),
        `Negative-control ${label} helper control`,
        matchesExpectedError,
      );
    }
  }

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

  const successfulPreviousEvidence = structuredClone(failedEvidenceAttemptOne);
  successfulPreviousEvidence.run.exit_code = 0;
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
      "support_cell_id",
      (evidence) => (evidence.support_cell_id = "SUP-RN-IOS-CURRENT-001"),
    ],
    ["proof_type", (evidence) => (evidence.proof_type = "server-black-box")],
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
    supportMatrixValid
  ) {
    expectSemanticValid(
      manifestSemanticErrors(validManifest, supportMatrix),
      "Valid RC manifest semantic self-test",
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
        manifestSemanticErrors(manifest, supportMatrix),
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
  wildcardVersionManifest.artifacts[0].version = "0.83.x";
  expectInvalid(
    validators.rcManifest,
    wildcardVersionManifest,
    "RC manifest with 0.83.x as an exact version",
    (errors) =>
      errors.some(
        (error) => error.instancePath === "/artifacts/0/version",
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
  latestBuildVersionManifest.artifacts[0].version = "latest-build";
  expectInvalid(
    validators.rcManifest,
    latestBuildVersionManifest,
    "RC manifest with latest-build as an exact version",
    (errors) =>
      errors.some((error) => error.instancePath === "/artifacts/0/version"),
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
    prereleaseManifest.artifacts[0].version = version;
    expectInvalid(
      validators.rcManifest,
      prereleaseManifest,
      `RC manifest with embedded prerelease ${version} as an exact version`,
      (errors) =>
        errors.some(
          (error) =>
            error.instancePath === "/artifacts/0/version" &&
            error.keyword === "pattern" &&
            error.schemaPath.endsWith("/exactVersion/pattern"),
        ),
    );
  }

  for (const version of ["1.0+preview", "1.0+edge", "1.0+rolling"]) {
    const mutableBuildManifest = structuredClone(validManifest);
    mutableBuildManifest.artifacts[0].version = version;
    expectInvalid(
      validators.rcManifest,
      mutableBuildManifest,
      `RC manifest with mutable build metadata ${version} as an exact version`,
      (errors) =>
        errors.some(
          (error) =>
            error.instancePath === "/artifacts/0/version" &&
            error.keyword === "pattern" &&
            error.schemaPath.endsWith("/exactVersion/pattern"),
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
    movingLabelManifest.artifacts[0].version = version;
    expectInvalid(
      validators.rcManifest,
      movingLabelManifest,
      `RC manifest with embedded ${label} moving label as an exact version`,
      (errors) =>
        errors.some((error) => error.instancePath === "/artifacts/0/version"),
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
    ["artifacts", (entry) => (entry.path = "artifacts/duplicate.zip")],
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
        manifestSemanticErrors(logicalDuplicateManifest, supportMatrix),
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
    const errors = manifestSemanticErrors(manifestMissingPg18, supportMatrix);
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
      manifestSemanticErrors(manifestWithUnknownCell, supportMatrix),
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
