import { device } from 'detox';

import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';
import { WAIT_TIMEOUT_MS } from '../src/timeouts';

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
    await device.terminateApp();
    await launchCorpusApp({ newInstance: true, delete: false, launchArgs: { synchroConformance: '1' } });
  }
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);
  const { raw, envelope } = await pollCorpusResult(WAIT_TIMEOUT_MS, 'React Native rebuild-requests command did not finish');
  if (envelope.outcome !== 'passed') throw new Error(`React Native conformance command failed: ${envelope.error_code}${envelope.error_detail === null ? '' : `: ${envelope.error_detail}`}`);
  return raw;
}

it('executes the rebuild-requests coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', 2);
  await launchCorpusApp({ newInstance: true, delete: true, launchArgs: { synchroConformance: '1' } });
  let result = 'null';
  let commands = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    const next = await exchange(endpoint, token, sequence, result);
    if (next.state === 'complete') {
      if (sequence !== stageCount || commands !== stageCount - 1) throw new Error('React Native rebuild-requests coordinator completed at an invalid stage');
      return;
    }
    commands += 1;
    result = await execute(next.command);
  }
  throw new Error('React Native rebuild-requests coordinator did not complete');
}));
