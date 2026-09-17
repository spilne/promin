import { describe, expect, it } from "bun:test";
import { buildCursorArgs, defaultCursorTransport, runCursorSession } from "../session.ts";
import type { CursorChild, CursorEvent, CursorTransport } from "../session.ts";

// ---------------------------------------------------------------------------
// Fake transport — feeds canned NDJSON / stderr to the parser without
// touching child_process. Each test composes the byte stream it wants
// the child to emit, captures the spawn args + env, and observes
// the events the session produces.
// ---------------------------------------------------------------------------

interface FakeChildSpec {
  readonly stdout: ReadonlyArray<string>;
  readonly stderr?: ReadonlyArray<string>;
  /** Defaults to 0. */
  readonly exitCode?: number;
  /** When set, skip emitting the trailing newline so flush() handles it. */
  readonly noTrailingNewline?: boolean;
}

interface CapturedSpawn {
  readonly args: ReadonlyArray<string>;
  readonly env: Readonly<Record<string, string | undefined>> | undefined;
}

function fakeTransport(spec: FakeChildSpec): {
  transport: CursorTransport;
  captured: CapturedSpawn[];
} {
  const captured: CapturedSpawn[] = [];
  const transport: CursorTransport = {
    spawn(args, opts) {
      captured.push({ args, env: opts.env });
      const enc = new TextEncoder();
      const stdout = (async function* () {
        for (let i = 0; i < spec.stdout.length; i++) {
          let chunk = spec.stdout[i]!;
          // Append newline unless the caller explicitly opted out for the
          // last chunk.
          if (!(spec.noTrailingNewline === true && i === spec.stdout.length - 1)) {
            chunk = `${chunk}\n`;
          }
          yield enc.encode(chunk);
        }
      })();
      const stderr = (async function* () {
        for (const chunk of spec.stderr ?? []) {
          yield enc.encode(chunk);
        }
      })();
      const child: CursorChild = {
        stdout,
        stderr,
        exited: Promise.resolve(spec.exitCode ?? 0),
        kill: () => {},
      };
      return child;
    },
  };
  return { transport, captured };
}

describe("buildCursorArgs", () => {
  it("emits the canonical headless flag set", () => {
    expect(buildCursorArgs({ prompt: "hi" })).toEqual([
      "agent",
      "-p",
      "--output-format",
      "stream-json",
      "--stream-partial-output",
      "--force",
      "--trust",
      "hi",
    ]);
  });

  it("opts out of --trust when trust:false", () => {
    expect(buildCursorArgs({ prompt: "x", trust: false })).not.toContain("--trust");
  });

  it("threads optional flags through in the documented positions", () => {
    const args = buildCursorArgs({
      prompt: "do it",
      command: "cursor-agent",
      model: "auto",
      workspace: "/tmp/workspace",
      worktree: true,
      sandbox: "enabled",
      resumeSessionId: "sess-42",
      extraArgs: ["--debug"],
    });
    expect(args[0]).toBe("cursor-agent");
    expect(args).toContain("--sandbox");
    expect(args).toContain("enabled");
    expect(args).toContain("--workspace");
    expect(args).toContain("/tmp/workspace");
    expect(args).toContain("--worktree");
    expect(args).toContain("--model");
    expect(args).toContain("auto");
    expect(args).toContain("--resume");
    expect(args).toContain("sess-42");
    expect(args).toContain("--debug");
    // The prompt is always last — Cursor reads the [prompt] positional.
    expect(args[args.length - 1]).toBe("do it");
  });
});

describe("runCursorSession", () => {
  it("derives true incremental deltas from Cursor's cumulative assistant frames", async () => {
    // Cursor emits CUMULATIVE text on each assistant frame even with
    // --stream-partial-output: frame N starts with frame N-1's text plus
    // new tokens. We prefix-slice to recover real deltas. This test
    // pins that contract — same behaviour as roshan-c/cursor-acp.
    const { transport } = fakeTransport({
      stdout: [
        '{"type":"system","subtype":"init","session_id":"sess-1","model":"auto"}',
        '{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hi"}]},"session_id":"sess-1"}',
        '{"type":"assistant","session_id":"sess-1","timestamp_ms":1,"message":{"role":"assistant","content":[{"type":"text","text":"Hel"}]}}',
        '{"type":"assistant","session_id":"sess-1","timestamp_ms":2,"message":{"role":"assistant","content":[{"type":"text","text":"Hello"}]}}',
        '{"type":"assistant","session_id":"sess-1","timestamp_ms":3,"message":{"role":"assistant","content":[{"type":"text","text":"Hello!"}]}}',
        '{"type":"result","subtype":"success","is_error":false,"result":"Hello!","duration_ms":42,"session_id":"sess-1"}',
      ],
    });

    const { events, result } = runCursorSession({ prompt: "hi" }, transport);
    const observed: CursorEvent[] = [];
    for await (const ev of events) observed.push(ev);
    const r = await result;

    const types = observed.map((e) => e.type);
    expect(types).toContain("session-init");
    const deltas = observed.flatMap((e) => (e.type === "text-delta" ? [e.delta] : []));
    // Cumulative input was "Hel" → "Hello" → "Hello!"; expected deltas
    // are the suffix added at each step: "Hel", "lo", "!".
    expect(deltas).toEqual(["Hel", "lo", "!"]);
    expect(types).toContain("result");

    expect(r.sessionId).toBe("sess-1");
    expect(r.text).toBe("Hello!");
    expect(r.isError).toBe(false);
    expect(r.exitCode).toBe(0);
  });

  it("falls back to the cumulative assistant text when the result frame's `result` is missing", async () => {
    const { transport } = fakeTransport({
      stdout: [
        '{"type":"system","subtype":"init","session_id":"s"}',
        '{"type":"assistant","session_id":"s","timestamp_ms":1,"message":{"role":"assistant","content":[{"type":"text","text":"only "}]}}',
        '{"type":"assistant","session_id":"s","timestamp_ms":2,"message":{"role":"assistant","content":[{"type":"text","text":"only the answer"}]}}',
        '{"type":"result","subtype":"success","is_error":false,"session_id":"s"}',
      ],
    });
    const { events, result } = runCursorSession({ prompt: "x" }, transport);
    for await (const _ of events) {
      // drain
    }
    const r = await result;
    expect(r.text).toBe("only the answer");
  });

  it("captures session_id from any frame, not just the system init", async () => {
    // Defensive: roshan-c/cursor-acp captures session_id from whichever
    // frame carries it first. A shell where the `system init` frame
    // arrives late (or is dropped) should still resolve a session id.
    const { transport } = fakeTransport({
      stdout: [
        '{"type":"assistant","session_id":"recovered-id","timestamp_ms":1,"message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}',
        '{"type":"system","subtype":"init","session_id":"recovered-id"}',
        '{"type":"result","subtype":"success","is_error":false,"result":"hi","session_id":"recovered-id"}',
      ],
    });
    const { events, result } = runCursorSession({ prompt: "x" }, transport);
    for await (const _ of events) {
      // drain
    }
    const r = await result;
    expect(r.sessionId).toBe("recovered-id");
  });

  it("captures stderr verbatim and surfaces stderr events alongside stdout frames", async () => {
    const { transport } = fakeTransport({
      stdout: [
        '{"type":"system","subtype":"init","session_id":"s"}',
        '{"type":"result","subtype":"error","is_error":true,"result":"","session_id":"s"}',
      ],
      stderr: ["boom: ", "no API key\n"],
    });
    const { events, result } = runCursorSession({ prompt: "x" }, transport);
    let stderrText = "";
    for await (const ev of events) if (ev.type === "stderr") stderrText += ev.text;
    const r = await result;
    expect(r.stderr).toBe("boom: no API key\n");
    expect(stderrText).toBe("boom: no API key\n");
    expect(r.isError).toBe(true);
  });

  it("reports isError when the child exits non-zero even without a result frame", async () => {
    const { transport } = fakeTransport({
      stdout: ['{"type":"system","subtype":"init","session_id":"s"}'],
      stderr: ["unexpected crash\n"],
      exitCode: 137,
    });
    const { events, result } = runCursorSession({ prompt: "x" }, transport);
    for await (const _ of events) {
      // drain
    }
    const r = await result;
    expect(r.isError).toBe(true);
    expect(r.exitCode).toBe(137);
    expect(r.stderr).toBe("unexpected crash\n");
  });

  it("pairs tool_call started/completed by call_id and exposes both halves", async () => {
    const { transport } = fakeTransport({
      stdout: [
        '{"type":"system","subtype":"init","session_id":"s"}',
        '{"type":"tool_call","subtype":"started","call_id":"c-1","tool_call":{"readToolCall":{"args":{"path":"x.txt"}}},"session_id":"s"}',
        '{"type":"tool_call","subtype":"completed","call_id":"c-1","tool_call":{"readToolCall":{"args":{"path":"x.txt"},"result":"file contents"}},"session_id":"s"}',
        '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}',
      ],
    });
    const { events, result } = runCursorSession({ prompt: "x" }, transport);
    const startEvents: string[] = [];
    const endEvents: string[] = [];
    for await (const ev of events) {
      if (ev.type === "tool-call-start") startEvents.push(ev.callId);
      if (ev.type === "tool-call-end") endEvents.push(ev.callId);
    }
    const r = await result;
    expect(startEvents).toEqual(["c-1"]);
    expect(endEvents).toEqual(["c-1"]);
    expect(r.toolCalls).toHaveLength(1);
    expect(r.toolCalls[0]!.completed).toBeDefined();
  });

  it("threads env vars and the prompt all the way through to the transport's spawn args", async () => {
    const { transport, captured } = fakeTransport({
      stdout: [
        '{"type":"system","subtype":"init","session_id":"s"}',
        '{"type":"result","subtype":"success","is_error":false,"result":"","session_id":"s"}',
      ],
    });
    const { events, result } = runCursorSession(
      { prompt: "build it", model: "auto", env: { CURSOR_API_KEY: "k-1" } },
      transport,
    );
    for await (const _ of events) {
      // drain
    }
    await result;
    expect(captured).toHaveLength(1);
    expect(captured[0]!.args).toContain("--model");
    expect(captured[0]!.args).toContain("auto");
    expect(captured[0]!.args[captured[0]!.args.length - 1]).toBe("build it");
    expect(captured[0]!.env?.CURSOR_API_KEY).toBe("k-1");
  });

  it("missing Cursor CLI binary surfaces a structured error, not a thrown exception", async () => {
    // Drive defaultCursorTransport against a binary name we know
    // doesn't exist. Bun.spawn throws ENOENT synchronously; the
    // transport must catch it and convert to an error stream so the
    // session pipeline produces a result with isError=true and a
    // human-readable stderr message.
    const { events, result } = runCursorSession(
      { prompt: "x", command: "promin-cursor-cli-that-does-not-exist-zzz-abc" },
      defaultCursorTransport,
    );
    let stderrText = "";
    for await (const ev of events) {
      if (ev.type === "stderr") stderrText += ev.text;
    }
    const r = await result;
    expect(r.isError).toBe(true);
    expect(r.exitCode).toBe(-2);
    expect(r.stderr).toMatch(/Cursor CLI not found on PATH/);
    expect(stderrText).toMatch(/curl https:\/\/cursor\.com\/install/);
    // text falls back to stderr so callers (including the chat UI's
    // streaming finish event) actually surface the install hint.
    expect(r.text).toMatch(/Cursor CLI not found on PATH/);
  });
});
