import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';
import { WAIT_TIMEOUT_MS } from './timeouts';

async function execute(command: Record<string, unknown>): Promise<string> {
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);
  const { raw, envelope } = await pollCorpusResult(WAIT_TIMEOUT_MS, 'React Native schema-check command did not finish');
  if (envelope.outcome === 'error') {
    throw new Error(
      `React Native conformance command failed: ${envelope.error_code}${envelope.error_detail === null ? '' : `: ${envelope.error_detail}`}`
    );
  }
  return raw;
}

it('executes the schema-check coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', 2);
  await launchCorpusApp({
    newInstance: true,
    delete: true,
    launchArgs: { synchroConformance: '1' },
  });
  let result = 'null';
  let commands = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    const next = await exchange(endpoint, token, sequence, result);
    if (next.state === 'complete') {
      if (sequence !== stageCount || commands !== stageCount - 1) {
        throw new Error(
          `React Native schema-check coordinator completed at sequence ${sequence} after ${commands} commands`
        );
      }
      return;
    }
    commands += 1;
    result = await execute(next.command);
  }
  throw new Error(`React Native schema-check coordinator did not complete after ${stageCount} exchanges`);
}));
