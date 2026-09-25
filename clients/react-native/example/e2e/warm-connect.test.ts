import {
  coordinatorConfiguration, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';
import { WAIT_TIMEOUT_MS } from '../src/timeouts';

async function executeCommand(command: Record<string, unknown>): Promise<string> {
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);

  const { raw, envelope } = await pollCorpusResult(WAIT_TIMEOUT_MS, 'React Native conformance command did not finish');
  if (envelope.outcome === 'error') {
    throw new Error(`React Native conformance command failed: ${envelope.error_code}`);
  }
  return raw;
}

it('executes the warm-connect coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  await launchCorpusApp({
    newInstance: true,
    delete: true,
    launchArgs: { synchroConformance: '1' },
  });

  let rawResult = 'null';
  let commandCount = 0;
  for (let sequence = 1; sequence <= 8; sequence += 1) {
    const response = await exchange(endpoint, token, sequence, rawResult);
    if (response.state === 'complete') {
      if (sequence !== 8 || commandCount !== 7) {
        throw new Error('React Native coordinator completed at an invalid sequence');
      }
      return;
    }
    commandCount += 1;
    if (commandCount > 7) {
      throw new Error('React Native coordinator returned too many commands');
    }
    try {
      rawResult = await executeCommand(response.command);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`React Native command at sequence ${sequence} failed: ${detail}`);
    }
  }
  throw new Error('React Native coordinator did not complete');
}));
