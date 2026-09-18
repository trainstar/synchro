import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';

const REBUILD_APPLY_COMMAND_TIMEOUT_MS = 600000;

async function execute(command: Record<string, unknown>): Promise<string> {
  const serialized = JSON.stringify(command);
  await submitCorpusCommand(serialized);
  const { raw, envelope } = await pollCorpusResult(REBUILD_APPLY_COMMAND_TIMEOUT_MS, 'React Native rebuild-apply command did not finish');
  if (envelope.outcome !== 'passed') throw new Error(`React Native conformance command failed: ${envelope.error_code}${envelope.error_detail === null ? '' : `: ${envelope.error_detail}`}`);
  return raw;
}

function errorDetail(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

it('executes the rebuild-apply coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', 2);
  await launchCorpusApp({ newInstance: true, delete: true, launchArgs: { synchroConformance: '1' } });
  let result = 'null';
  let commands = 0;
  let stoppedAt = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    stoppedAt = sequence;
    try {
      const next = await exchange(endpoint, token, sequence, result);
      if (next.state === 'complete') {
        if (sequence !== stageCount || commands !== stageCount - 1) throw new Error(`React Native rebuild-apply coordinator completed at an invalid stage: sequence=${sequence} versus stage_count=${stageCount} commands=${commands}`);
        return;
      }
      commands += 1;
      result = await execute(next.command);
    } catch (error) {
      throw new Error(`React Native rebuild-apply coordinator stopped at sequence=${stoppedAt} versus stage_count=${stageCount}: ${errorDetail(error)}`);
    }
  }
  throw new Error(`React Native rebuild-apply coordinator stopped at sequence=${stoppedAt} versus stage_count=${stageCount}: no complete response`);
}));
