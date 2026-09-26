import { device } from 'detox';

import {
  coordinatorConfiguration, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';
import { WAIT_TIMEOUT_MS } from '../src/timeouts';

function isJSONObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isRestartCommand(command: Record<string, unknown>): boolean {
  if (!isJSONObject(command.action) || !isJSONObject(command.action.action)) {
    return false;
  }
  const action = command.action.action;
  return (
    action.actor === 'client' &&
    action.command === 'open' &&
    isJSONObject(action.parameters) &&
    action.parameters.database_mode === 'reuse'
  );
}

async function executeCommand(command: Record<string, unknown>): Promise<string> {
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);

  const { raw, envelope } = await pollCorpusResult(WAIT_TIMEOUT_MS, 'React Native conformance command did not finish');
  if (envelope.outcome === 'error') {
    throw new Error(`React Native conformance command failed: ${envelope.error_code}`);
  }
  return raw;
}

it('executes the steady-pull coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  await launchCorpusApp({
    newInstance: true,
    delete: true,
    launchArgs: { synchroConformance: '1' },
  });

  let rawResult = 'null';
  let commandCount = 0;
	for (let sequence = 1; sequence <= 22; sequence += 1) {
    const response = await exchange(endpoint, token, sequence, rawResult);
    if (response.state === 'complete') {
		if (sequence !== 22 || commandCount !== 21) {
        throw new Error('React Native steady-pull coordinator completed at an invalid sequence');
      }
      return;
    }
    commandCount += 1;
		if (commandCount > 21) {
      throw new Error('React Native steady-pull coordinator returned too many commands');
    }
    try {
      if (isRestartCommand(response.command)) {
        await device.terminateApp();
        await launchCorpusApp({
          newInstance: true,
          delete: false,
          launchArgs: { synchroConformance: '1' },
        });
      }
      rawResult = await executeCommand(response.command);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`React Native command at sequence ${sequence} failed: ${detail}`);
    }
  }
  throw new Error('React Native steady-pull coordinator did not complete');
}));
