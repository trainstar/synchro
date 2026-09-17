import {
  coordinatorConfiguration, coordinatorCount, exchange, pollCorpusResult,
  launchCorpusApp, runCorpusCommandLoop, submitCorpusCommand,
} from './corpus-harness';

async function execute(command: Record<string, unknown>): Promise<string> {
  const serialized = JSON.stringify(command);
  console.log(`rnmem execute start ${String((command.action as { action?: { actor?: unknown; command?: unknown } } | undefined)?.action?.actor)}/${String((command.action as { action?: { actor?: unknown; command?: unknown } } | undefined)?.action?.command)} bytes=${serialized.length}`);
  await submitCorpusCommand(serialized);
  const { raw, envelope } = await pollCorpusResult(120000, 'React Native rebuild-cardinality command did not finish');
  if (envelope.outcome !== 'passed') throw new Error(`React Native conformance command failed: ${envelope.error_code}${envelope.error_detail === null ? '' : `: ${envelope.error_detail}`}`);
  console.log(`rnmem execute complete result-bytes=${raw.length}`);
  return raw;
}

it('executes the rebuild-cardinality coordinator sequence', () => runCorpusCommandLoop(async () => {
  const { endpoint, token } = coordinatorConfiguration();
  const stageCount = coordinatorCount('STAGE_COUNT', 2);
  await launchCorpusApp({ newInstance: true, delete: true, launchArgs: { synchroConformance: '1' } });
  let result = 'null';
  let commands = 0;
  for (let sequence = 1; sequence <= stageCount; sequence += 1) {
    const next = await exchange(endpoint, token, sequence, result);
    if (next.state === 'complete') {
      if (sequence !== stageCount || commands !== stageCount - 1) throw new Error('React Native rebuild-cardinality coordinator completed at an invalid stage');
      return;
    }
    commands += 1;
    result = await execute(next.command);
  }
  throw new Error('React Native rebuild-cardinality coordinator did not complete');
}));
