import { createHash } from "node:crypto";

export function duplicateLogicalIdErrors(items, idKey, collection) {
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

function stringSetsEqual(left, right) {
  return (
    left.length === right.length &&
    left.every((item) => right.includes(item)) &&
    right.every((item) => left.includes(item))
  );
}

export function vectorCatalogSemanticErrors(catalog) {
  return [
    ...duplicateLogicalIdErrors(
      catalog.vector_sets,
      "vector_set_id",
      "Vector catalog vector sets",
    ),
    ...duplicateLogicalIdErrors(
      catalog.vector_sets,
      "path",
      "Vector catalog vector-set paths",
    ),
  ];
}

export function authoredIdErrors(requirements, supportMatrix) {
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

export function artifactInventorySemanticErrors(inventory) {
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

export function faultCatalogSemanticErrors(catalog, requirements) {
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

export function performanceCatalogSemanticErrors(
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
    "cd29425e0cd55e4e8c27a5c36fb185a253396541f2561a76c52564e158cd6d50";
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
    ["BUD-WARM-CONNECT-PULL-001", ["warm_connect_pull_http_requests", "eq", 1]],
    ["BUD-WARM-CONNECT-PUSH-001", ["warm_connect_push_http_requests", "eq", 0]],
    ["BUD-WARM-CONNECT-REBUILD-001", ["warm_connect_rebuild_page_http_requests", "eq", 0]],
    ["BUD-WARM-CONNECT-SCHEMA-001", ["warm_connect_schema_fetch_http_requests", "eq", 0]],
    ["BUD-WARM-CONNECT-OTHER-001", ["warm_connect_other_http_requests", "eq", 0]],
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
