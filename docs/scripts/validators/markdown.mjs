function stripInlineMarkdown(value) {
  let text = value;
  text = text.replace(/!\[([^\]]*)\]\([^)]*\)/g, "$1");
  text = text.replace(/\[([^\]]+)\]\([^)]*\)/g, "$1");
  text = text.replace(/\[([^\]]+)\]\[[^\]]*\]/g, "$1");
  text = text.replace(/<[^>]+>/g, "");
  text = text.replace(/[`*_~]/g, "");
  text = text.replace(/\\([!"#$%&'()*+,./:;<=>?@[\]^_`{|}~-])/g, "$1");
  return text;
}

function githubSlug(value) {
  return stripInlineMarkdown(value)
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{M}\p{N}\p{Pc}\-\s]/gu, "")
    .replace(/\s+/g, "-");
}

function headings(source) {
  const result = [];
  const used = new Set();
  const slugCounts = new Map();
  let fence = null;

  for (const line of source.split(/\r?\n/)) {
    const fenceMatch = line.match(/^\s{0,3}(`{3,}|~{3,})/);
    if (fenceMatch) {
      const marker = fenceMatch[1][0];
      if (fence === marker) fence = null;
      else if (fence === null) fence = marker;
      continue;
    }
    if (fence !== null) continue;

    const heading = line.match(/^\s{0,3}(#{1,6})[\t ]+(.+?)\s*$/);
    if (!heading) continue;
    const title = heading[2].replace(/[\t ]+#+[\t ]*$/, "");
    const base = githubSlug(title);
    let suffix = slugCounts.get(base) ?? 0;
    let slug = base;
    while (used.has(slug)) {
      suffix += 1;
      slug = `${base}-${suffix}`;
    }
    slugCounts.set(base, suffix);
    used.add(slug);
    result.push({ level: heading[1].length, slug });
  }
  return result;
}

export function markdownAnchors(source) {
  return new Set(headings(source).map(({ slug }) => slug));
}

export function markdownAnchorsAtLevel(source, targetLevel) {
  return new Set(
    headings(source)
      .filter(({ level }) => level === targetLevel)
      .map(({ slug }) => slug),
  );
}
