import { isDeepStrictEqual } from "node:util";
import { readFileSync } from "node:fs";

import {
  artifactInventorySemanticErrors,
  duplicateLogicalIdErrors,
  faultCatalogSemanticErrors,
  performanceCatalogSemanticErrors,
  vectorCatalogSemanticErrors,
} from "./validators/catalogs.mjs";
import { parseMakeTargets } from "./validators/make-targets.mjs";
import {
  markdownAnchors,
  markdownAnchorsAtLevel,
} from "./validators/markdown.mjs";
import { parseJsonStrict } from "./validators/strict-json.mjs";
import { supportPolicyErrors } from "./validators/support-policy.mjs";

function requireSelfTest(condition, message) {
  if (!condition) throw new Error(`validator self-test failed: ${message}`);
}

function requireRejects(operation, pattern, message) {
  try {
    operation();
  } catch (error) {
    requireSelfTest(pattern.test(error.message), message);
    return;
  }
  throw new Error(`validator self-test failed: ${message}`);
}

export function runValidatorSelfTests() {
  const parsed = parseJsonStrict(
    Buffer.from('{"integer":9007199254740991,"members":[true,null,"value"]}'),
  );
  requireSelfTest(
    isDeepStrictEqual(parsed, {
      integer: 9007199254740991,
      members: [true, null, "value"],
    }),
    "strict JSON changed valid input",
  );
  requireRejects(
    () => parseJsonStrict(Buffer.from('{"duplicate":1,"duplicate":2}')),
    /duplicate JSON object member/,
    "strict JSON accepted a duplicate member",
  );
  requireRejects(
    () => parseJsonStrict(Uint8Array.from([0x7b, 0x22, 0xff, 0x22, 0x3a, 0x31, 0x7d])),
    /encoded data was not valid|encoding/,
    "strict JSON accepted invalid UTF-8",
  );

  const targets = parseMakeTargets(
    ".PHONY: alpha beta\nalpha beta: dependency\n\tignored: recipe\nname := value\n",
  );
  requireSelfTest(
    isDeepStrictEqual([...targets].sort(), [".PHONY", "alpha", "beta"]),
    "Make target parsing accepted assignments or recipes",
  );

  const markdown = [
    "# Repeated Heading",
    "### Child <em>`Code`</em>",
    "# Repeated Heading",
    "```markdown",
    "# Hidden Heading",
    "```",
  ].join("\n");
  requireSelfTest(
    isDeepStrictEqual([...markdownAnchors(markdown)], [
      "repeated-heading",
      "child-code",
      "repeated-heading-1",
    ]),
    "Markdown anchor parsing changed heading identity",
  );
  requireSelfTest(
    isDeepStrictEqual([...markdownAnchorsAtLevel(markdown, 3)], ["child-code"]),
    "Markdown level filtering accepted another heading level",
  );
  const fenceCases = parseJsonStrict(
    readFileSync(
      new URL("../../conformance/internal/contract/testdata/markdown-fences.json", import.meta.url),
    ),
  );
  requireSelfTest(fenceCases.length > 0, "Markdown fence cases are missing");
  for (const test of fenceCases) {
    requireSelfTest(
      isDeepStrictEqual([...markdownAnchors(test.source)], test.anchors),
      `Markdown fence boundary failed: ${test.name}`,
    );
  }

  requireSelfTest(
    duplicateLogicalIdErrors(
      [{ id: "ITEM-001" }, { id: "ITEM-001" }],
      "id",
      "Self-test items",
    ).some((error) => error.includes("duplicate logical ID")),
    "catalog validation accepted a duplicate logical ID",
  );
  requireSelfTest(
    artifactInventorySemanticErrors({
      artifacts: [
        {
          id: "ARTDEF-ONE-001",
          visibility: "public",
          kind: "file",
          release_path_template: "artifacts/item-{version}.tgz",
        },
        {
          id: "ARTDEF-TWO-001",
          visibility: "public",
          kind: "file",
          release_path_template: "artifacts/item-{version}.tgz",
        },
      ],
    }).some((error) => error.includes("repeats publication identity")),
    "artifact inventory validation accepted a duplicate public path",
  );
  requireSelfTest(
    vectorCatalogSemanticErrors({
      vector_sets: [
        { vector_set_id: "VEC-001", path: "vectors/shared.json" },
        { vector_set_id: "VEC-002", path: "vectors/shared.json" },
      ],
    }).some((error) => error.includes("vector-set paths")),
    "vector catalog validation accepted a duplicate path",
  );
  requireSelfTest(
    faultCatalogSemanticErrors(
      { faults: [{ id: "FAULT-001" }], controls: [] },
      { requirements: [] },
    ).some((error) => error.includes("is not used by a control")),
    "fault catalog validation accepted an unused fault",
  );
  requireSelfTest(
    performanceCatalogSemanticErrors(
      { budgets: [], required_measurements: [] },
      { cells: [] },
      { artifacts: [] },
    ).some((error) => error.includes("semantic snapshot")),
    "performance catalog validation accepted an unlocked catalog",
  );
  requireSelfTest(
    supportPolicyErrors(
      { release: "test-release", requirements: [] },
      {
        release: "test-release",
        semantic_corpus_cell_ids: [],
        cells: [
          {
            id: "SUP-DUPLICATE",
            component: "swift-client",
            platform: "ios",
            platform_version: { kind: "current-stable" },
            policy: "required",
          },
          {
            id: "SUP-DUPLICATE",
            component: "swift-client",
            platform: "ios",
            platform_version: { kind: "current-stable" },
            policy: "required",
          },
        ],
      },
      "test-release",
    ).some((error) => error.includes("duplicate logical ID")),
    "support policy validation accepted a duplicate cell",
  );
  requireSelfTest(
    supportPolicyErrors(
      { release: "test-release", requirements: [] },
      { release: "other-release", semantic_corpus_cell_ids: [], cells: [] },
      "test-release",
    ).some((error) => error.includes("Support matrix release must be test-release")),
    "support policy validation accepted a mismatched release version",
  );
}
