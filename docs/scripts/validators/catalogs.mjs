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
  const publicationIdentities = new Map();
  for (const artifact of inventory.artifacts) {
    if (artifact.visibility !== "public" || artifact.kind !== "file") continue;
    const identity = `file:${artifact.release_path_template}`;
    if (publicationIdentities.has(identity)) {
      errors.push(
        `Artifact inventory repeats publication identity ${JSON.stringify(identity)} for ${publicationIdentities.get(identity)} and ${artifact.id}`,
      );
    } else {
      publicationIdentities.set(identity, artifact.id);
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
  const usedFaultIds = new Set();
  for (const control of catalog.controls) {
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
