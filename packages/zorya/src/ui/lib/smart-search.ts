// ---------------------------------------------------------------------------
// Smart-search query parser for the runs filter bar.
//
// One free-form input replaces the per-field combobox/input maze. Tokens are
// split on whitespace (with double-quote support for values containing
// spaces) and classified:
//
//   `field:value`  → structured workflow field (name, type, version,
//                    namespace, id) — the parsed value drops onto the
//                    matching `RunListQuery` field 1:1.
//   `key=value`    → metadata filter — collected into `metadata`. Values try
//                    `JSON.parse` first so `dryRun=true` becomes `boolean`,
//                    falling back to the raw string so `userId=u_42` works
//                    without quoting.
//   bare token     → free text. Joined with spaces and exposed as
//                    `freeText`; the caller decides whether to look that up
//                    as an id (then navigate) or apply as a name filter.
//
// `serializeQuery` is the inverse — used when removing a single chip to
// rebuild the input string from the remaining filters.
// ---------------------------------------------------------------------------

export type SearchField = "name" | "type" | "version" | "namespace" | "id" | "source" | "sourceId";

const SEARCH_FIELDS: ReadonlyArray<SearchField> = [
  "name",
  "type",
  "version",
  "namespace",
  "id",
  "source",
  "sourceId",
];

export interface ParsedSearchQuery {
  name?: string;
  type?: string;
  version?: string;
  namespace?: string;
  id?: string;
  /**
   * Trigger source — `"schedule"`, `"manual"`, `"api"`, …. Mapped to
   * `RunListQuery.runSource` on the wire so the indexed column does the
   * filtering.
   */
  source?: string;
  /** Producer id paired with `source` (e.g. scheduleId). */
  sourceId?: string;
  metadata?: Record<string, unknown>;
  /** Bare tokens (no `:` or `=`), joined with spaces. */
  freeText?: string;
}

export function parseSearchQuery(text: string): ParsedSearchQuery {
  const tokens = tokenize(text);
  const out: ParsedSearchQuery = {};
  const metadata: Record<string, unknown> = {};
  const freeWords: string[] = [];

  for (const tok of tokens) {
    const colon = tok.indexOf(":");
    const eq = tok.indexOf("=");
    // `field:value` wins over `key=value` when both delimiters appear and the
    // colon comes first — e.g. `name:foo=bar` reads as a name with a literal
    // `=` in the value.
    if (colon > 0 && (eq < 0 || colon < eq)) {
      const field = tok.slice(0, colon).toLowerCase();
      const value = unquote(tok.slice(colon + 1));
      if (isSearchField(field)) {
        if (value) out[field] = value;
        continue;
      }
    }
    if (eq > 0) {
      const key = tok.slice(0, eq);
      const rawValue = unquote(tok.slice(eq + 1));
      let value: unknown = rawValue;
      try {
        value = JSON.parse(rawValue);
      } catch {
        // Plain-string fallback so `userId=u_42` doesn't require quoting.
      }
      metadata[key] = value;
      continue;
    }
    freeWords.push(tok);
  }

  if (Object.keys(metadata).length > 0) out.metadata = metadata;
  if (freeWords.length > 0) out.freeText = freeWords.join(" ");
  return out;
}

export function serializeQuery(q: ParsedSearchQuery): string {
  const parts: string[] = [];
  for (const f of SEARCH_FIELDS) {
    const v = q[f];
    if (v) parts.push(`${f}:${quoteIfNeeded(v)}`);
  }
  if (q.metadata) {
    for (const [k, v] of Object.entries(q.metadata)) {
      const valStr = typeof v === "string" ? v : JSON.stringify(v);
      parts.push(`${k}=${quoteIfNeeded(valStr)}`);
    }
  }
  if (q.freeText) parts.push(q.freeText);
  return parts.join(" ");
}

/**
 * True when the parsed query has at least one filter clause that would
 * narrow `listWorkflows`. `freeText` alone counts — the caller treats it
 * as a name shortcut.
 */
export function hasAnyFilter(q: ParsedSearchQuery): boolean {
  if (q.name || q.type || q.version || q.namespace || q.id || q.freeText) return true;
  if (q.source || q.sourceId) return true;
  if (q.metadata && Object.keys(q.metadata).length > 0) return true;
  return false;
}

function isSearchField(s: string): s is SearchField {
  return (SEARCH_FIELDS as ReadonlyArray<string>).includes(s);
}

/**
 * Tokenize on whitespace, treating `"..."` as a single token (with `\"` as
 * an escaped quote). Used so `name:"my workflow"` survives the split.
 */
function tokenize(text: string): string[] {
  const out: string[] = [];
  let i = 0;
  while (i < text.length) {
    if (/\s/.test(text[i]!)) {
      i++;
      continue;
    }
    let buf = "";
    while (i < text.length && !/\s/.test(text[i]!)) {
      const ch = text[i]!;
      if (ch === '"') {
        // Consume the quoted segment verbatim into the current token.
        buf += '"';
        i++;
        while (i < text.length && text[i] !== '"') {
          if (text[i] === "\\" && i + 1 < text.length) {
            buf += text[i]! + text[i + 1]!;
            i += 2;
          } else {
            buf += text[i]!;
            i++;
          }
        }
        if (i < text.length) {
          buf += '"';
          i++;
        }
      } else {
        buf += ch;
        i++;
      }
    }
    if (buf) out.push(buf);
  }
  return out;
}

function unquote(s: string): string {
  if (s.length >= 2 && s.startsWith('"') && s.endsWith('"')) {
    return s.slice(1, -1).replace(/\\(.)/g, "$1");
  }
  return s;
}

function quoteIfNeeded(s: string): string {
  if (s === "") return '""';
  if (/[\s"]/.test(s)) return `"${s.replace(/"/g, '\\"')}"`;
  return s;
}
