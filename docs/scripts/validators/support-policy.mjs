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

export function supportPolicyErrors(requirements, supportMatrix, releaseVersion) {
  const errors = [
    ...duplicateLogicalIdErrors(
      supportMatrix.cells,
      "id",
      "Support matrix cells",
    ),
  ];
  if (requirements.release !== releaseVersion) {
    errors.push(`Requirements release must be ${releaseVersion}, found ${requirements.release}`);
  }
  if (supportMatrix.release !== releaseVersion) {
    errors.push(`Support matrix release must be ${releaseVersion}, found ${supportMatrix.release}`);
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
  }

  const cellIds = new Set(supportMatrix.cells.map(({ id }) => id));
  errors.push(
    ...duplicateLogicalIdErrors(
      supportMatrix.semantic_corpus_cell_ids.map((id) => ({ id })),
      "id",
      "Support matrix semantic corpus cells",
    ),
  );
  for (const id of supportMatrix.semantic_corpus_cell_ids) {
    if (!cellIds.has(id)) {
      errors.push(`Semantic corpus cell ${id} is not a support matrix cell`);
    }
  }
  return errors;
}
