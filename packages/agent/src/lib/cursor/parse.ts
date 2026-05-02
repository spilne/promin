// ---------------------------------------------------------------------------
// NDJSON line parser for Cursor CLI's stream-json output.
//
// Cursor emits one JSON object per line on stdout. This module:
//   1. accumulates byte chunks across `\n` boundaries
//   2. decodes each completed line as JSON
//   3. classifies into a typed `CursorFrame` (or `unknown` for forward-compat)
//
// Invalid JSON lines are surfaced through an error callback so the caller
// can decide whether to abort or skip — by default `runCursorSession`
// treats them as recoverable and continues.
// ---------------------------------------------------------------------------

import type {
  CursorAssistantFrame,
  CursorFrame,
  CursorResultFrame,
  CursorSystemInitFrame,
  CursorToolCallFrame,
  CursorUserFrame,
} from "./frames.ts";

/** Classify a parsed JSON object into a typed Cursor frame. */
export function classifyFrame(raw: unknown): CursorFrame {
  if (!raw || typeof raw !== "object") {
    return { type: "unknown", raw: { value: raw } as Record<string, unknown> };
  }
  const obj = raw as Record<string, unknown>;
  switch (obj.type) {
    case "system":
      if (obj.subtype === "init") return obj as unknown as CursorSystemInitFrame;
      return { type: "unknown", raw: obj };
    case "user":
      return obj as unknown as CursorUserFrame;
    case "assistant":
      return obj as unknown as CursorAssistantFrame;
    case "tool_call":
      return obj as unknown as CursorToolCallFrame;
    case "result":
      return obj as unknown as CursorResultFrame;
    default:
      return { type: typeof obj.type === "string" ? obj.type : "unknown", raw: obj };
  }
}

export interface NdJsonLineParserOptions {
  /** Called with each raw line that didn't parse as JSON. Default: silent. */
  readonly onParseError?: (line: string, error: unknown) => void;
}

/**
 * Push-style line-buffered NDJSON parser. Feed chunks via `push()`;
 * receive parsed frames via `take()`. `flush()` emits any final
 * partial line as a frame (when the stream ends without a trailing
 * newline). Stateless across instances — one per child process.
 */
export class NdJsonLineParser {
  private buffer = "";
  private readonly frames: CursorFrame[] = [];
  private readonly onParseError?: (line: string, error: unknown) => void;
  private readonly decoder = new TextDecoder();

  constructor(opts: NdJsonLineParserOptions = {}) {
    if (opts.onParseError) this.onParseError = opts.onParseError;
  }

  push(chunk: Uint8Array | string): void {
    this.buffer += typeof chunk === "string" ? chunk : this.decoder.decode(chunk, { stream: true });
    let nl: number;
    while ((nl = this.buffer.indexOf("\n")) >= 0) {
      const line = this.buffer.slice(0, nl);
      this.buffer = this.buffer.slice(nl + 1);
      this.consumeLine(line);
    }
  }

  flush(): void {
    if (this.buffer.length > 0) {
      this.consumeLine(this.buffer);
      this.buffer = "";
    }
  }

  take(): CursorFrame[] {
    if (this.frames.length === 0) return [];
    const out = this.frames.slice();
    this.frames.length = 0;
    return out;
  }

  private consumeLine(line: string): void {
    const trimmed = line.trim();
    if (trimmed.length === 0) return;
    try {
      this.frames.push(classifyFrame(JSON.parse(trimmed)));
    } catch (err) {
      this.onParseError?.(trimmed, err);
    }
  }
}
