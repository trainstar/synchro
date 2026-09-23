import { createHash } from 'node:crypto';
import { existsSync, readFileSync, realpathSync } from 'node:fs';
import { dirname, join, resolve, sep } from 'node:path';

type Catalog = {
  schema_version: number;
  scenarios: Array<{
    scenario_id: string;
    path: string;
    sha256: string;
  }>;
};

export function loadScenario(id: string): Record<string, unknown> {
  const root = repositoryRoot();
  const catalog = JSON.parse(
    readFileSync(join(root, 'conformance/catalog.json'), 'utf8')
  ) as Catalog;
  if (catalog.schema_version !== 1) {
    throw new Error('unsupported scenario catalog schema version');
  }
  const entries = catalog.scenarios.filter((candidate) => candidate.scenario_id === id);
  if (entries.length !== 1) {
    throw new Error(`scenario is not present in the catalog: ${id}`);
  }
  const [entry] = entries;

  const scenarioPath = realpathSync(resolve(root, entry.path));
  if (!scenarioPath.startsWith(`${realpathSync(root)}${sep}`)) {
    throw new Error(`scenario path escapes the repository: ${id}`);
  }
  const data = readFileSync(scenarioPath);
  const digest = createHash('sha256').update(data).digest('hex');
  if (digest !== entry.sha256) {
    throw new Error(`scenario digest does not match the catalog: ${id}`);
  }
  const scenario = JSON.parse(data.toString('utf8')) as Record<string, unknown>;
  if (scenario.id !== id) {
    throw new Error(`scenario identity does not match the catalog: ${id}`);
  }
  return scenario;
}

function repositoryRoot(): string {
  let current = resolve(__dirname);
  for (let depth = 0; depth < 8; depth += 1) {
    if (existsSync(join(current, 'conformance/catalog.json'))) {
      return current;
    }
    current = dirname(current);
  }
  throw new Error('repository root was not found');
}
