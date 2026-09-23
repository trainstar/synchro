import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';

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

async function executeCommand(command: Record<string, unknown>): Promise<string> {
  if (requiresProcessRelaunch(command)) {
    // A reuse open is the coordinator's process-restart boundary. Keep the
    // database while replacing the application process before opening it.
    await launchCorpusApp({ newInstance: true, delete: false, launchArgs: { synchroConformance: '1' } });
  }
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);
  // The response-loss await retries through in-call backoff on the device,
  // so the command poll matches the runner completion wait.
  const { raw, envelope } = await pollCorpusResult(570000, 'React Native conformance command did not finish');
  if (envelope.outcome === 'error') {
    throw new Error(`React Native conformance command failed: ${envelope.error_code}${envelope.error_detail === null ? '' : `: ${envelope.error_detail}`}`);
  }
  return raw;
}

it('executes the queue-replay coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', 2);
  await launchCorpusApp({ newInstance: true, delete: true, launchArgs: { synchroConformance: '1' } });
  let rawResult = 'null';
  let commandCount = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    const response = await exchange(endpoint, token, sequence, rawResult);
    if (response.state === 'complete') {
      if (sequence !== stageCount || commandCount !== stageCount - 1) {
        throw new Error('React Native queue-replay coordinator completed at an invalid sequence');
      }
      return;
    }
    commandCount += 1;
    if (commandCount >= stageCount) {
      throw new Error('React Native queue-replay coordinator returned too many commands');
    }
    try {
      rawResult = await executeCommand(response.command);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`React Native command at sequence ${sequence} failed: ${detail}`);
    }
  }
  throw new Error('React Native queue-replay coordinator did not complete');
}));
