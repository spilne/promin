// ---------------------------------------------------------------------------
// Cursor agent NDJSON frame types
//
// Source of truth: https://cursor.com/docs/cli/reference/output-format
//
// One JSON object per line on stdout. We keep the shape permissive (only
// fields the runtime actually consumes are typed) so a downstream Cursor
// CLI bump that adds new fields doesn't break the parser. Unknown frame
// types and unknown subtypes are surfaced as `unknown` and skipped by
// the consumer rather than treated as a parse error.
// ---------------------------------------------------------------------------

export interface CursorTextContent {
  readonly type: "text";
  readonly text: string;
}

export interface CursorAssistantMessage {
  readonly role: "assistant";
  readonly content: ReadonlyArray<CursorTextContent>;
}

export interface CursorUserMessage {
  readonly role: "user";
  readonly content: ReadonlyArray<CursorTextContent>;
}

/** Initial frame: session id, cwd, selected model. */
export interface CursorSystemInitFrame {
  readonly type: "system";
  readonly subtype: "init";
  readonly session_id: string;
  readonly cwd?: string;
  readonly model?: string;
  readonly permissionMode?: string;
  readonly apiKeySource?: string;
}

/** Echo of the user's prompt back to the stream. */
export interface CursorUserFrame {
  readonly type: "user";
  readonly message: CursorUserMessage;
  readonly session_id: string;
}

/**
 * Assistant text. Cursor distinguishes streaming partial deltas from
 * final assistant messages by the presence of `timestamp_ms` (deltas
 * carry it; finals carry `model_call_id` instead). Both paths emit
 * `content: [{ type: "text", text }]` — concatenate to form the answer.
 */
export interface CursorAssistantFrame {
  readonly type: "assistant";
  readonly message: CursorAssistantMessage;
  readonly session_id: string;
  /** Present on partial deltas (when `--stream-partial-output` is on). */
  readonly timestamp_ms?: number;
  /** Present on the final assistant message of a turn. */
  readonly model_call_id?: string;
}

/**
 * Tool-call frames. Cursor emits one `started` and one `completed` per
 * call; the inner shape is keyed by the specific tool (`readToolCall`,
 * `writeToolCall`, `runTerminalCall`, …). We keep the inner permissive.
 */
export interface CursorToolCallFrame {
  readonly type: "tool_call";
  readonly subtype: "started" | "completed";
  readonly call_id: string;
  readonly tool_call: Readonly<Record<string, unknown>>;
  readonly session_id: string;
}

/** Terminal frame for a turn — success or error, with the final text. */
export interface CursorResultFrame {
  readonly type: "result";
  readonly subtype: "success" | "error" | string;
  readonly is_error?: boolean;
  readonly result?: string;
  readonly duration_ms?: number;
  readonly duration_api_ms?: number;
  readonly session_id: string;
  readonly request_id?: string;
}

/** Frame we don't know how to type — preserved verbatim for forward-compat. */
export interface CursorUnknownFrame {
  readonly type: string;
  readonly raw: Readonly<Record<string, unknown>>;
}

export type CursorFrame =
  | CursorSystemInitFrame
  | CursorUserFrame
  | CursorAssistantFrame
  | CursorToolCallFrame
  | CursorResultFrame
  | CursorUnknownFrame;

/**
 * Extract the text contribution of an assistant frame. Cursor packs
 * the assistant's text inside `message.content[].text`; concatenate
 * every text block in the array.
 *
 * Important: even with `--stream-partial-output`, Cursor's assistant
 * frames carry CUMULATIVE text — each new frame's text starts with the
 * previous frame's text plus new tokens. Use `assistantTextDelta` (in
 * `session.ts`'s frame loop) to derive a true incremental delta;
 * `assistantFrameText` returns the cumulative string verbatim.
 */
export function assistantFrameText(frame: CursorAssistantFrame): string {
  let acc = "";
  for (const block of frame.message.content) {
    if (block.type === "text") acc += block.text;
  }
  return acc;
}
