import type { SQLiteBindValue } from './types';

const MIN_INT64 = -(BigInt(1) << BigInt(63));
const MAX_INT64 = (BigInt(1) << BigInt(63)) - BigInt(1);

const BASE64URL_ALPHABET =
  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';

function base64urlValue(character: string): number {
  return BASE64URL_ALPHABET.indexOf(character);
}

/** Validate an unpadded, canonical RFC 4648 base64url string. */
export function isCanonicalBase64Url(value: string): boolean {
  if (!/^[A-Za-z0-9_-]*$/.test(value) || value.length % 4 === 1) {
    return false;
  }

  const remainder = value.length % 4;
  if (remainder === 2) {
    const last = base64urlValue(value[value.length - 1]);
    return (last & 0x0f) === 0;
  }
  if (remainder === 3) {
    const last = base64urlValue(value[value.length - 1]);
    return (last & 0x03) === 0;
  }
  return true;
}

export function isCanonicalInt64(value: string): boolean {
  if (!/^(?:0|-?[1-9][0-9]*)$/.test(value)) {
    return false;
  }

  try {
    const parsed = BigInt(value);
    return parsed >= MIN_INT64 && parsed <= MAX_INT64 && parsed.toString() === value;
  } catch {
    return false;
  }
}

function invalidBindValue(index: number, reason: string): TypeError {
  return new TypeError(`Invalid SQL bind value at index ${index}: ${reason}`);
}

/** Reject malformed tagged values before they cross the native bridge. */
export function assertValidSQLiteBindValue(
  value: unknown,
  index: number
): asserts value is SQLiteBindValue {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') {
    return;
  }

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw invalidBindValue(index, 'number must be finite');
    }
    if (Number.isInteger(value) && Math.abs(value) > Number.MAX_SAFE_INTEGER) {
      throw invalidBindValue(index, 'integer is outside the safe range');
    }
    return;
  }

  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw invalidBindValue(index, 'unsupported value type');
  }

  const keys = Object.keys(value);
  if (keys.length !== 2 || !keys.includes('type')) {
    throw invalidBindValue(index, 'tag must contain exactly type and payload fields');
  }

  const tagged = value as Record<string, unknown>;
  if (tagged.type === 'bytes') {
    if (keys.length !== 2 || !keys.includes('base64') || typeof tagged.base64 !== 'string') {
      throw invalidBindValue(index, 'bytes tag must contain a base64 string');
    }
    if (!isCanonicalBase64Url(tagged.base64)) {
      throw invalidBindValue(index, 'bytes tag must use canonical base64url');
    }
    return;
  }

  if (tagged.type === 'int64') {
    if (keys.length !== 2 || !keys.includes('value') || typeof tagged.value !== 'string') {
      throw invalidBindValue(index, 'int64 tag must contain a signed decimal string');
    }
    if (!isCanonicalInt64(tagged.value)) {
      throw invalidBindValue(index, 'int64 tag must contain a canonical signed int64');
    }
    return;
  }

  throw invalidBindValue(index, 'unknown SQL bind tag');
}

export function assertValidSQLiteBindParams(
  values: readonly SQLiteBindValue[]
): readonly SQLiteBindValue[] {
  values.forEach((value, index) => assertValidSQLiteBindValue(value, index));
  return values;
}
