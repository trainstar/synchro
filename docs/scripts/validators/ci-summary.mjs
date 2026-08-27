export const requiredGateVariableNames = Object.freeze([
  "BLACKBOX_TEST_COUNT",
  "DETOX_ARGS",
  "GO_TEST_ARGS",
  "GO_TEST_PKGS",
  "GRADLE_TEST_ARGS",
  "KOTLIN_ANDROID_SERIAL",
  "MUTATION_CONTROL_EXPECT",
  "MUTATION_CONTROL_TEST",
  "PGRX_TEST_NAME",
  "RN_ANDROID_DETOX_CONFIG",
  "SUPPORT_CELL_ID",
  "SUPPORT_PLATFORM_VERSION",
  "TESTRESULT_TEST_NAME",
]);

const proofHomes = new Map([
  ["reference-model", "scenario"],
  ["server-black-box", "real-integration"],
  ["native-e2e", "scenario"],
  ["fault-injection", "adversarial"],
  ["negative-control", "adversarial"],
]);

function duplicateValues(values, label) {
  const errors = [];
  const seen = new Set();
  for (const value of values) {
    if (seen.has(value)) errors.push(`${label} repeats ${value}`);
    seen.add(value);
  }
  return errors;
}

export function ciSummarySemanticErrors(summary) {
  const errors = [];
  const variableNames = summary.gate_variables.map(({ name }) => name);
  if (JSON.stringify(variableNames.slice().sort()) !== JSON.stringify(requiredGateVariableNames)) {
    errors.push("CI summary gate variables do not match the closed required set");
  }
  errors.push(...duplicateValues(variableNames, "CI summary gate variables"));

  const summaryHashes = new Set(summary.artifact_hashes);
  const obligationIds = summary.obligations.map(({ id }) => id);
  errors.push(...duplicateValues(obligationIds, "CI summary obligations"));
  for (const obligation of summary.obligations) {
    for (const hash of obligation.artifact_hashes) {
      if (!summaryHashes.has(hash)) {
        errors.push(`${obligation.id} references an artifact hash outside the summary`);
      }
    }
  }

  const obligationSet = new Set(obligationIds);
  const coverageIds = summary.coverage.map(({ coverage_id }) => coverage_id);
  errors.push(...duplicateValues(coverageIds, "CI summary coverage IDs"));
  const tuples = new Set();
  for (const entry of summary.coverage) {
    const tuple = [
      entry.requirement_id,
      entry.scenario_id,
      entry.proof_obligation_id,
      entry.assertion_id,
      entry.support_cell_id ?? "",
    ].join("\u0000");
    if (tuples.has(tuple)) {
      errors.push(`CI summary coverage repeats ownership tuple ${entry.coverage_id}`);
    }
    tuples.add(tuple);
    if (!obligationSet.has(entry.test_id)) {
      errors.push(`${entry.coverage_id} references unknown test ${entry.test_id}`);
    }
    if (proofHomes.get(entry.proof_type) !== entry.proof_home) {
      errors.push(`${entry.coverage_id} assigns the wrong proof home`);
    }
  }
  return errors;
}

export function validCISummaryFixture() {
  return {
    $schema: "https://synchro.dev/conformance/schemas/ci-summary-v1.schema.json",
    schema_version: 1,
    source_commit: "a".repeat(40),
    status: "passed",
    artifact_hashes: ["b".repeat(64)],
    gate_variables: requiredGateVariableNames.map((name) => ({ name, value: "" })),
    obligations: [
      {
        id: "gate/test-conformance",
        kind: "gate",
        status: "passed",
        terminal: true,
        test_count: 1,
        artifact_hashes: ["b".repeat(64)],
      },
    ],
    coverage: [
      {
        coverage_id: "COV-0123456789ABCDEF",
        test_id: "gate/test-conformance",
        requirement_id: "SYNC-TIME-001",
        scenario_id: "SCN-TIME-001",
        proof_obligation_id: "OBL-REFERENCE-001",
        assertion_id: "ASSERT-TIME-001",
        support_cell_id: null,
        proof_type: "reference-model",
        proof_home: "scenario",
      },
    ],
  };
}
