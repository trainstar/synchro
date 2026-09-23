import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';

function isJSONObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function commandParameters(command: Record<string, unknown>): Record<string, unknown> | null {
  const manifest = command.action;
  if (!isJSONObject(manifest)) return null;
  const action = manifest.action;
  if (!isJSONObject(action)) return null;
  return isJSONObject(action.parameters) ? action.parameters : null;
}

function requiresProcessRelaunch(command: Record<string, unknown>): boolean {
  return commandParameters(command)?.process_relaunch === true;
}

function expectedCommandError(command: Record<string, unknown>): { code: string; detailCode: string } | null {
  const parameters = commandParameters(command);
  const code = parameters?.expected_error_code;
  const detailCode = parameters?.expected_error_detail_code;
  return typeof code === 'string' && typeof detailCode === 'string' ? { code, detailCode } : null;
}

async function executeCommand(command: Record<string, unknown>): Promise<string> {
  if (requiresProcessRelaunch(command)) {
    await launchCorpusApp({
      newInstance: true,
      delete: false,
      launchArgs: { synchroConformance: '1' },
    });
  }
  const expectedError = expectedCommandError(command);
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);

  const { raw, envelope } = await pollCorpusResult(45000, 'React Native conformance command did not finish');
  if (envelope.outcome === 'error') {
    if (
      expectedError !== null &&
      envelope.error_code === expectedError.code &&
      envelope.error_detail?.includes(expectedError.detailCode)
    ) {
      return raw;
    }
    throw new Error(`React Native conformance command failed: ${envelope.error_code}`);
  }
  if (expectedError !== null) {
    throw new Error(`React Native conformance command passed, expected ${expectedError.code}`);
  }
  return raw;
}

it('executes the seeded-empty-startup coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', 1);
  await launchCorpusApp({
    newInstance: true,
    delete: true,
    launchArgs: { synchroConformance: '1' },
  });

  let rawResult = 'null';
  let commandCount = 0;
  for (let sequence = 1; sequence <= stageCount + 1; sequence += 1) {
    const response = await exchange(endpoint, token, sequence, rawResult);
    if (response.state === 'complete') {
      if (sequence !== stageCount + 1 || commandCount !== stageCount) {
        throw new Error('React Native seeded-empty-startup coordinator completed at an invalid sequence');
      }
      return;
    }
    commandCount += 1;
    if (commandCount > stageCount) {
      throw new Error('React Native seeded-empty-startup coordinator returned too many commands');
    }
    try {
      rawResult = await executeCommand(response.command);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`React Native command at sequence ${sequence} failed: ${detail}`);
    }
  }
  throw new Error('React Native seeded-empty-startup coordinator did not complete');
}));
