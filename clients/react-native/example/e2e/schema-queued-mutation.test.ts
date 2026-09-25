import { device } from 'detox';

import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';
import { WAIT_TIMEOUT_MS } from './timeouts';

function requiresProcessRelaunch(command: Record<string, unknown>): boolean {
  const manifest = command.action;
  if (typeof manifest !== 'object' || manifest === null || Array.isArray(manifest)) return false;
  const action = (manifest as Record<string, unknown>).action;
  if (typeof action !== 'object' || action === null || Array.isArray(action)) return false;
  const fields = action as Record<string, unknown>;
  if (fields.actor !== 'client' || fields.command !== 'open') return false;
  const parameters = fields.parameters;
  return typeof parameters === 'object' && parameters !== null && !Array.isArray(parameters) && (parameters as Record<string, unknown>).database_mode === 'reuse';
}

async function execute(command: Record<string, unknown>): Promise<string> {
  if (requiresProcessRelaunch(command)) {
    await device.terminateApp();
    await launchCorpusApp({ newInstance: true, delete: false, launchArgs: { synchroConformance: '1' } });
  }
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);
  const { raw, envelope } = await pollCorpusResult(WAIT_TIMEOUT_MS, 'React Native schema-queued-mutation command did not finish');
  if (envelope.outcome !== 'passed') throw new Error(`React Native conformance command failed: ${envelope.error_code}: ${envelope.error_detail}`);
  return raw;
}

it('executes the schema-queued-mutation coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', 2);
  await launchCorpusApp({ newInstance: true, delete: true, launchArgs: { synchroConformance: '1' } });
  let result = 'null';
  let commands = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    const next = await exchange(endpoint, token, sequence, result);
    if (next.state === 'complete') {
      if (sequence !== stageCount || commands !== stageCount - 1) throw new Error(`React Native schema-queued-mutation coordinator completed at sequence ${sequence} after ${commands} commands`);
      return;
    }
    commands += 1;
    result = await execute(next.command);
  }
  throw new Error(`React Native schema-queued-mutation coordinator did not complete after ${stageCount} exchanges`);
}));
