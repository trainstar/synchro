import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';
import { WAIT_TIMEOUT_MS } from './timeouts';

function isJSONObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requiresProcessRelaunch(command: Record<string, unknown>): boolean {
  const action = command.action;
  if (!isJSONObject(action)) return false;
  const nested = action.action;
  if (!isJSONObject(nested) || nested.actor !== 'client' || nested.command !== 'open') return false;
  const parameters = nested.parameters;
  return isJSONObject(parameters) && parameters.database_mode === 'reuse';
}

async function execute(command: Record<string, unknown>): Promise<string> {
  if (requiresProcessRelaunch(command)) {
    await launchCorpusApp({ newInstance: true, delete: false, launchArgs: { synchroConformance: '1' } });
  }
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);
  const { raw, envelope } = await pollCorpusResult(WAIT_TIMEOUT_MS, 'React Native multi-scope provenance command did not finish');
  if (envelope.outcome !== 'passed') throw new Error(`React Native conformance command failed: ${envelope.error_code}${envelope.error_detail === null ? '' : `: ${envelope.error_detail}`}`);
  return raw;
}

// This scenario drives six clients through the coordinator, so it needs more
// than the shared 120 second budget that the slower simulator cannot meet.
it('executes the multi-scope-provenance coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', 2);
  await launchCorpusApp({ newInstance: true, delete: true, launchArgs: { synchroConformance: '1' } });
  let result = 'null';
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    const next = await exchange(endpoint, token, sequence, result);
    if (next.state === 'complete') { if (sequence !== stageCount) throw new Error('React Native multi-scope provenance coordinator completed early'); return; }
    result = await execute(next.command);
  }
  throw new Error('React Native multi-scope provenance coordinator did not complete');
}));
