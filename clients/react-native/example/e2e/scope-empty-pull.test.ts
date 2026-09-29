import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';
import { WAIT_TIMEOUT_MS } from '../src/timeouts';

// The coordinator serves five commands and one completion.
const SCOPE_EMPTY_PULL_EXCHANGE_COUNT = 6;

async function execute(command: Record<string, unknown>): Promise<string> {
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);
  const { raw, envelope } = await pollCorpusResult(WAIT_TIMEOUT_MS, 'React Native scope-empty-pull command did not finish');
  if (envelope.outcome !== 'passed') throw new Error(`React Native conformance command failed: ${envelope.error_code}${envelope.error_detail === null ? '' : `: ${envelope.error_detail}`}`);
  return raw;
}

function errorDetail(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

it('executes the scope-empty-pull coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', SCOPE_EMPTY_PULL_EXCHANGE_COUNT, SCOPE_EMPTY_PULL_EXCHANGE_COUNT);
  await launchCorpusApp({ newInstance: true, delete: true, launchArgs: { synchroConformance: '1' } });
  let result = 'null';
  let commands = 0;
  let stoppedAt = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    stoppedAt = sequence;
    try {
      const next = await exchange(endpoint, token, sequence, result);
      if (next.state === 'complete') {
        if (sequence !== stageCount || commands !== stageCount - 1) throw new Error(`React Native scope-empty-pull coordinator completed at an invalid stage: sequence=${sequence} versus stage_count=${stageCount} commands=${commands}`);
        return;
      }
      commands += 1;
      result = await execute(next.command);
    } catch (error) {
      throw new Error(`React Native scope-empty-pull coordinator stopped at sequence=${stoppedAt} versus stage_count=${stageCount}: ${errorDetail(error)}`);
    }
  }
  throw new Error(`React Native scope-empty-pull coordinator stopped at sequence=${stoppedAt} versus stage_count=${stageCount}: no complete response`);
}));
