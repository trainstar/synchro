import { duplicateLogicalIdErrors } from "./catalogs.mjs";

function selectorKey(selector) {
  return selector ? `${selector.kind}:${selector.value ?? ""}` : "none";
}

function supportDimensionKey(cell) {
  return [
    cell.component,
    cell.platform,
    selectorKey(cell.platform_version),
    selectorKey(cell.runtime_version),
    cell.extension_architecture ?? "none",
  ].join("|");
}

function supportPolicyKey(cell) {
  return `${supportDimensionKey(cell)}|${cell.policy}`;
}

export function supportPolicyErrors(requirements, supportMatrix) {
  const errors = [
    ...duplicateLogicalIdErrors(
      supportMatrix.cells,
      "id",
      "Support matrix cells",
    ),
  ];
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
  for (const version of ["14", "15", "16", "17"]) {
    expected.push({
      id: `SUP-PG-0${version}`,
      component: "postgresql-server",
      platform: "postgresql",
      platform_version: { kind: "exact", value: version },
      policy: "excluded",
    });
  }
  expected.push(
    {
      id: "SUP-PG-LINUX-X64-001",
      component: "postgresql-server",
      platform: "postgresql",
      platform_version: { kind: "exact", value: "18" },
      extension_architecture: "linux-x64",
      policy: "required",
    },
    {
      id: "SUP-PG-MACOS-ARM64-001",
      component: "postgresql-server",
      platform: "postgresql",
      platform_version: { kind: "exact", value: "18" },
      extension_architecture: "macos-arm64",
      policy: "required",
    },
    ...[
      ["SUP-IOS-MIN-001", "swift-client", "ios", "minimum", "16", null],
      ["SUP-IOS-CURRENT-001", "swift-client", "ios", "current-stable", null, null],
      ["SUP-MACOS-CURRENT-001", "swift-client", "macos", "current-stable", null, null, "tested"],
      ["SUP-ANDROID-MIN-001", "kotlin-client", "android", "minimum", "24", null],
      ["SUP-ANDROID-CURRENT-001", "kotlin-client", "android", "current-stable", null, null],
      ["SUP-RN-IOS-CURRENT-001", "react-native-client", "ios", "current-stable", null, "0.83.x"],
      ["SUP-RN-ANDROID-CURRENT-001", "react-native-client", "android", "current-stable", null, "0.83.x"],
    ].map(([id, component, platform, kind, value, runtime, policy = "required"]) => ({
      id,
      component,
      platform,
      platform_version: value === null ? { kind } : { kind, value },
      ...(runtime === null
        ? {}
        : { runtime_version: { kind: "series", value: runtime } }),
      policy,
    })),
  );

  if (supportMatrix.cells.length !== expected.length) {
    errors.push(
      `Support matrix must contain exactly ${expected.length} cells, found ${supportMatrix.cells.length}`,
    );
  }
  const expectedById = new Map(expected.map((cell) => [cell.id, cell]));
  const actualById = new Map(supportMatrix.cells.map((cell) => [cell.id, cell]));
  for (const [id, expectedCell] of expectedById) {
    const actualCell = actualById.get(id);
    if (!actualCell) {
      errors.push(`Missing required support policy cell ${id}`);
    } else if (supportPolicyKey(actualCell) !== supportPolicyKey(expectedCell)) {
      errors.push(`${id} does not match its locked v0.3.0 support policy tuple`);
    }
  }
  for (const id of actualById.keys()) {
    if (!expectedById.has(id)) {
      errors.push(`${id} is outside the locked v0.3.0 support policy`);
    }
  }
  const semanticCorpusCellIds = [
    "SUP-MACOS-CURRENT-001",
    "SUP-ANDROID-CURRENT-001",
    "SUP-RN-IOS-CURRENT-001",
    "SUP-RN-ANDROID-CURRENT-001",
  ];
  if (
    JSON.stringify(supportMatrix.semantic_corpus_cell_ids) !==
    JSON.stringify(semanticCorpusCellIds)
  ) {
    errors.push(
      "Support matrix semantic corpus cell IDs do not match the locked v0.3.0 set",
    );
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
