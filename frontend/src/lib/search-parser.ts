/**
 * The shared search mini-language (§6.5).
 *
 * A filter is a whitespace-separated list of tokens with flat AND semantics:
 *   - a bare token is the series ID regex pattern (at most one)
 *   - `key:value` is a metadata filter (key is an exact string, value is a regex)
 *   - keys/values containing whitespace or reserved characters (`:`, `"`, `\`)
 *     are double-quoted; inside quotes `\"` and `\\` are the escape sequences
 *
 * The same string is what the search box contains and what the `q=` URL
 * parameter carries, so URLs round-trip exactly through the parser.
 */

export interface ParseError {
  message: string;
  /** 0-based character offset into the original input. */
  position: number;
}

export interface SeriesFilter {
  /** Series ID regex pattern ("" when absent). */
  pattern: string;
  /** Metadata key (exact) → value (regex). */
  metadata: Record<string, string>;
}

export interface ParseResult {
  filter: SeriesFilter;
  error?: ParseError;
}

interface TokenChar {
  c: string;
  quoted: boolean;
}

interface Token {
  chars: TokenChar[];
  /** 0-based offset of the first character of the token in the input. */
  start: number;
  /** 0-based offset one past the last character of the token. */
  end: number;
}

function isWhitespace(c: string): boolean {
  return /\s/.test(c);
}

/**
 * Splits the input into tokens. Characters inside double quotes are marked
 * `quoted` so a `:` inside a quoted key/value does not act as a separator.
 * Quotes and their escape sequences are consumed; the resulting characters
 * are the literal token content.
 */
function tokenize(input: string): { tokens: Token[]; error?: ParseError } {
  const tokens: Token[] = [];
  let i = 0;
  while (i < input.length) {
    if (isWhitespace(input[i])) {
      i++;
      continue;
    }
    const chars: TokenChar[] = [];
    const start = i;
    while (i < input.length && !isWhitespace(input[i])) {
      const c = input[i];
      if (c === '"') {
        const quoteStart = i;
        i++;
        let closed = false;
        while (i < input.length) {
          const d = input[i];
          if (d === "\\") {
            const next = input[i + 1];
            if (next === '"' || next === "\\") {
              chars.push({ c: next, quoted: true });
              i += 2;
              continue;
            }
            // A backslash before any other character is kept literally (it may
            // be part of a regex such as `\d+`).
            chars.push({ c: d, quoted: true });
            i++;
            continue;
          }
          if (d === '"') {
            i++;
            closed = true;
            break;
          }
          chars.push({ c: d, quoted: true });
          i++;
        }
        if (!closed) {
          return { tokens, error: { message: "Unbalanced quote", position: quoteStart } };
        }
      } else {
        chars.push({ c, quoted: false });
        i++;
      }
    }
    tokens.push({ chars, start, end: i });
  }
  return { tokens };
}

function charsToString(chars: TokenChar[]): string {
  return chars.map((c) => c.c).join("");
}

function parseToken(token: Token): { key?: string; value?: string; pattern?: string; error?: ParseError } {
  let colonIndex = -1;
  for (let i = 0; i < token.chars.length; i++) {
    if (token.chars[i].c === ":" && !token.chars[i].quoted) {
      colonIndex = i;
      break;
    }
  }
  if (colonIndex === -1) {
    return { pattern: charsToString(token.chars) };
  }
  const keyChars = token.chars.slice(0, colonIndex);
  const valueChars = token.chars.slice(colonIndex + 1);
  const key = charsToString(keyChars);
  const value = charsToString(valueChars);
  if (!key.trim()) {
    return { error: { message: "Empty metadata key", position: token.start } };
  }
  if (value.length === 0) {
    return { error: { message: "Empty metadata value", position: token.start } };
  }
  return { key: key.trim(), value };
}

/**
 * Parses a search string into a {@link SeriesFilter}. On failure the partially
 * parsed filter is returned alongside a structured error (position + message).
 */
export function parseSearch(input: string): ParseResult {
  const filter: SeriesFilter = { pattern: "", metadata: {} };
  if (!input.trim()) {
    return { filter };
  }
  const { tokens, error } = tokenize(input);
  if (error) {
    return { filter, error };
  }
  let patternSeen = false;
  for (const token of tokens) {
    const parsed = parseToken(token);
    if (parsed.error) {
      return { filter, error: parsed.error };
    }
    if (parsed.pattern !== undefined) {
      if (patternSeen) {
        return {
          filter,
          error: {
            message: "Only one series pattern is allowed",
            position: token.start,
          },
        };
      }
      patternSeen = true;
      filter.pattern = parsed.pattern;
    } else if (parsed.key !== undefined && parsed.value !== undefined) {
      filter.metadata[parsed.key] = parsed.value;
    }
  }
  return { filter };
}

function needsQuoting(value: string): boolean {
  return /[\s:"\\]/.test(value);
}

function quoteToken(value: string): string {
  if (!needsQuoting(value)) {
    return value;
  }
  return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

/**
 * Serializes a {@link SeriesFilter} back to the canonical search string.
 * The output parses back to an equivalent filter.
 */
export function stringifyFilter(filter: SeriesFilter): string {
  const parts: string[] = [];
  if (filter.pattern) {
    parts.push(quoteToken(filter.pattern));
  }
  for (const [key, value] of Object.entries(filter.metadata)) {
    parts.push(`${quoteToken(key)}:${quoteToken(value)}`);
  }
  return parts.join(" ");
}

/** True when the string contains at least one meaningful token. */
export function isEmptyFilter(filter: SeriesFilter): boolean {
  return !filter.pattern && Object.keys(filter.metadata).length === 0;
}
