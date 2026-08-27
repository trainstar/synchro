export function parseMakeTargets(source) {
  const targets = new Set();
  for (const line of source.split(/\r?\n/)) {
    const match = line.match(
      /^([A-Za-z0-9_.-]+(?:[ \t]+[A-Za-z0-9_.-]+)*):(?:[ \t]|$)/,
    );
    if (!match) continue;
    for (const target of match[1].split(/[ \t]+/)) targets.add(target);
  }
  return targets;
}
