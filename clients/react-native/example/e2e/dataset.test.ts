import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';

const DATASET_COMMAND_TIMEOUT_MS = 600000;

async function execute(command: Record<string, unknown>): Promise<string> {
  await submitCorpusCommand(JSON.stringify(command));
  const { raw } = await pollCorpusResult(DATASET_COMMAND_TIMEOUT_MS, 'React Native dataset command did not finish');
  // The host flow reads every envelope, including a failed command.
  return raw;
}

it('executes the dataset coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  // The host flow decides the command count. The bound stops a runaway loop.
  const bound = coordinatorCount('STAGE_COUNT', 2);
  await launchCorpusApp({ newInstance: true, delete: true, launchArgs: { synchroConformance: '1' } });
  let result = 'null';
  for (let sequence = 1; sequence <= bound; sequence += 1) {
    const next = await exchange(endpoint, token, sequence, result);
    if (next.state === 'complete') {
      return;
    }
    result = await execute(next.command);
  }
  throw new Error(`React Native dataset coordinator did not complete within ${bound} exchanges`);
}));
