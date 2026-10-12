import json
from pathlib import Path
import re
import subprocess
import unittest


class PostcssNestedCompatibility(unittest.TestCase):
    def test_upstream_expectations_and_selector_boundaries(self):
        docs = Path(__file__).resolve().parents[1]
        fixture = json.loads(
            (docs / "scripts/fixtures/postcss-nested-6.2.0.json").read_text()
        )
        cases = fixture["upstream_cases"] + fixture["authored_boundaries"]
        script = r"""
const assert = require('node:assert/strict');
const { createRequire } = require('node:module');
const fs = require('node:fs');
const docsRequire = createRequire(process.cwd() + '/package.json');
const coreRequire = createRequire(docsRequire.resolve('@expressive-code/core'));
assert.equal(coreRequire('../package.json').version, '0.44.2');
const nestedEntry = coreRequire.resolve('postcss-nested');
const nestedRequire = createRequire(nestedEntry);
assert.equal(nestedRequire('./package.json').version, '6.2.0');
const parserRequire = createRequire(nestedRequire.resolve('postcss-selector-parser'));
assert.equal(parserRequire('../package.json').version, '7.1.6');
const postcss = nestedRequire('postcss').default;
const nested = nestedRequire(nestedEntry);
const cases = JSON.parse(fs.readFileSync(0, 'utf8'));
console.log(JSON.stringify(cases.map(({ input }) => {
  try {
    const result = postcss([nested()]).process(input, { from: '/test.css' });
    return { css: result.css, warnings: result.warnings().length };
  } catch (error) {
    return { error: error.message };
  }
})));
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=docs,
            input=json.dumps(cases),
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
        )
        results = json.loads(completed.stdout)
        self.assertGreater(len(cases), 0)
        self.assertEqual(len(results), len(cases))
        for case, result in zip(cases, results):
            with self.subTest(case=case["name"]):
                if "error" in case:
                    self.assertIn(case["error"], result.get("error", ""))
                else:
                    self.assertNotIn("error", result)
                    actual = re.sub(r"([:;{}]|\*/|/\*)", r" \1 ", result["css"])
                    expected = re.sub(r"([:;{}]|\*/|/\*)", r" \1 ", case["output"])
                    actual = re.sub(r"\s\s+", " ", actual)
                    expected = re.sub(r"\s\s+", " ", expected)
                    self.assertEqual(
                        re.sub(r" ([;:])", r"\1", actual).strip(),
                        re.sub(r" ([;:])", r"\1", expected).strip(),
                    )
                    self.assertEqual(result["warnings"], 0)
