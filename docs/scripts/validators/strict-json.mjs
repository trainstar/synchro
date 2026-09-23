export function parseJsonStrict(bytes) {
  const source = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  let index = 0;

  const skipWhitespace = () => {
    while (/[\u0020\u0009\u000a\u000d]/.test(source[index] ?? "")) {
      index += 1;
    }
  };
  const syntaxError = (message) => {
    throw new SyntaxError(`${message} at character offset ${index}`);
  };

  function consumeString() {
    if (source[index] !== '"') syntaxError("expected JSON string");
    const start = index;
    index += 1;
    while (index < source.length) {
      const character = source[index++];
      if (character === '"') return JSON.parse(source.slice(start, index));
      if (character === "\\") {
        const escaped = source[index++];
        if (escaped === "u") {
          const code = source.slice(index, index + 4);
          if (!/^[0-9a-fA-F]{4}$/.test(code)) {
            syntaxError("invalid JSON Unicode escape");
          }
          index += 4;
        } else if (!['"', "\\", "/", "b", "f", "n", "r", "t"].includes(escaped)) {
          syntaxError("invalid JSON string escape");
        }
      } else if (character <= "\u001f") {
        syntaxError("unescaped JSON control character");
      }
    }
    syntaxError("unterminated JSON string");
  }

  function consumeNumber() {
    const match = source
      .slice(index)
      .match(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/);
    if (!match) syntaxError("invalid JSON number");
    index += match[0].length;
  }

  function consumeLiteral(literal) {
    if (!source.startsWith(literal, index)) {
      syntaxError(`expected JSON literal ${literal}`);
    }
    index += literal.length;
  }

  function parseValue() {
    skipWhitespace();
    const character = source[index];
    if (character === '"') return consumeString();
    if (character === "{") return parseObject();
    if (character === "[") return parseArray();
    if (character === "-" || /[0-9]/.test(character ?? "")) {
      return consumeNumber();
    }
    if (character === "t") return consumeLiteral("true");
    if (character === "f") return consumeLiteral("false");
    if (character === "n") return consumeLiteral("null");
    syntaxError("expected JSON value");
  }

  function parseObject() {
    index += 1;
    skipWhitespace();
    const keys = new Set();
    if (source[index] === "}") {
      index += 1;
      return;
    }
    while (index < source.length) {
      skipWhitespace();
      const key = consumeString();
      if (keys.has(key)) {
        syntaxError(`duplicate JSON object member ${JSON.stringify(key)}`);
      }
      keys.add(key);
      skipWhitespace();
      if (source[index] !== ":") syntaxError("expected JSON object member separator");
      index += 1;
      parseValue();
      skipWhitespace();
      if (source[index] === "}") {
        index += 1;
        return;
      }
      if (source[index] !== ",") syntaxError("expected JSON object separator");
      index += 1;
    }
    syntaxError("unterminated JSON object");
  }

  function parseArray() {
    index += 1;
    skipWhitespace();
    if (source[index] === "]") {
      index += 1;
      return;
    }
    while (index < source.length) {
      parseValue();
      skipWhitespace();
      if (source[index] === "]") {
        index += 1;
        return;
      }
      if (source[index] !== ",") syntaxError("expected JSON array separator");
      index += 1;
    }
    syntaxError("unterminated JSON array");
  }

  parseValue();
  skipWhitespace();
  if (index !== source.length) syntaxError("trailing data after JSON value");
  return JSON.parse(source);
}
