import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';

function isJSONObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isDeviceRestart(command: Record<string, unknown>): boolean {
  const action = command.action;
  if (!isJSONObject(action)) return false;
  const manifest = action.action;
  return isJSONObject(manifest) && manifest.actor === 'device' && manifest.command === 'restart';
}

async function executeCommand(command: Record<string, unknown>): Promise<string> {
  if (isDeviceRestart(command)) {
    await launchCorpusApp({
      newInstance: true,
      delete: false,
      launchArgs: { synchroConformance: '1' },
    });
    return 'null';
  }
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);

  const { raw, envelope } = await pollCorpusResult(45000, 'React Native conformance command did not finish');
  if (envelope.outcome === 'error') {
    throw new Error(`React Native conformance command failed: ${envelope.error_code}: ${envelope.error_detail}`);
  }
  return raw;
}

it('executes the pending-cycle coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const exchanges = coordinatorCount('EXCHANGE_COUNT', 2);
  await launchCorpusApp({
    newInstance: true,
    delete: true,
    launchArgs: { synchroConformance: '1' },
  });

  let rawResult = 'null';
  let commandCount = 0;
  for (let sequence = 1; sequence <= exchanges; sequence += 1) {
    const response = await exchange(endpoint, token, sequence, rawResult);
    if (response.state === 'complete') {
      if (sequence !== exchanges || commandCount !== exchanges - 1) {
        throw new Error('React Native pending-cycle coordinator completed at an invalid sequence');
      }
      return;
    }
    commandCount += 1;
    if (commandCount >= exchanges) {
      throw new Error('React Native pending-cycle coordinator returned too many commands');
    }
    try {
      rawResult = await executeCommand(response.command);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`React Native command at sequence ${sequence} failed: ${detail}`);
    }
  }
  throw new Error('React Native pending-cycle coordinator did not complete');
}));
