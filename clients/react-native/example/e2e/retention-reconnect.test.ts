import { device } from 'detox';

import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';
import { WAIT_TIMEOUT_MS } from '../src/timeouts';

function isJSONObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

async function executeCommand(command: Record<string, unknown>): Promise<string> {
  if (requiresProcessRelaunch(command)) {
    await device.terminateApp();
    await launchCorpusApp({ newInstance: true, delete: false, launchArgs: { synchroConformance: '1' } });
  }
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);

  const { raw, envelope } = await pollCorpusResult(WAIT_TIMEOUT_MS, 'React Native retention-reconnect command did not finish');
  if (envelope.outcome === 'error') {
    throw new Error(`React Native conformance command failed: ${envelope.error_code}: ${envelope.error_detail}`);
  }
  return raw;
}

function requiresProcessRelaunch(command: Record<string, unknown>): boolean {
  const action = command.action;
  if (!isJSONObject(action)) return false;
  const nested = action.action;
  if (!isJSONObject(nested) || nested.actor !== 'client' || nested.command !== 'open') return false;
  const parameters = nested.parameters;
  return isJSONObject(parameters) && parameters.database_mode === 'reuse';
}

it('executes the retention-reconnect coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', 2);
  await launchCorpusApp({
    newInstance: true,
    delete: true,
    launchArgs: { synchroConformance: '1' },
  });

  let rawResult = 'null';
  let commandCount = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    const response = await exchange(endpoint, token, sequence, rawResult);
    if (response.state === 'complete') {
      if (sequence !== stageCount || commandCount !== stageCount - 1) {
        throw new Error('React Native retention-reconnect coordinator completed at an invalid sequence');
      }
      return;
    }
    commandCount += 1;
    if (commandCount >= stageCount) {
      throw new Error('React Native retention-reconnect coordinator returned too many commands');
    }
    try {
      rawResult = await executeCommand(response.command);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`React Native retention-reconnect command at sequence ${sequence} failed: ${detail}`);
    }
  }
  throw new Error('React Native retention-reconnect coordinator did not complete');
}));
