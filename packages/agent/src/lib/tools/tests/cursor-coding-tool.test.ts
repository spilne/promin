import { describe, expect, it } from "bun:test";
import { createCursorCodingTool } from "../cursor-coding-tool.ts";
import type { CursorChild, CursorTransport } from "../../cursor/session.ts";

function fakeTransport(stdout: ReadonlyArray<string>, exitCode = 0): CursorTransport {
  return {
    spawn() {
      const enc = new TextEncoder();
      async function* outIter() {
        for (const line of stdout) yield enc.encode(`${line}\n`);
      }
      async function* empty() {}
      const child: CursorChild = {
        stdout: outIter(),
        stderr: empty(),
        exited: Promise.resolve(exitCode),
        kill: () => {},
      };
      return child;
    },
  };
}

function fakeTransportWithStderr(
  stdout: ReadonlyArray<string>,
  stderr: ReadonlyArray<string>,
  exitCode: number,
): CursorTransport {
  return {
    spawn() {
      const enc = new TextEncoder();
      async function* outIter() {
        for (const line of stdout) yield enc.encode(`${line}\n`);
      }
      async function* errIter() {
        for (const chunk of stderr) yield enc.encode(chunk);
      }
      const child: CursorChild = {
        stdout: outIter(),
        stderr: errIter(),
        exited: Promise.resolve(exitCode),
        kill: () => {},
      };
      return child;
    },
  };
}

describe("createCursorCodingTool", () => {
  it("returns ok=true with the final answer text on a clean run", async () => {
    const tool = createCursorCodingTool({
      transport: fakeTransport([
        '{"type":"system","subtype":"init","session_id":"s"}',
        '{"type":"assistant","session_id":"s","timestamp_ms":1,"message":{"role":"assistant","content":[{"type":"text","text":"done"}]}}',
        '{"type":"result","subtype":"success","is_error":false,"result":"All set.","session_id":"s"}',
      ]),
    });
    const result = await tool.execute({ prompt: "implement foo" }, {});
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unexpected error");
    expect(result.text).toBe("All set.");
    expect(result.sessionId).toBe("s");
  });

  it("returns ok=false when the result frame flags is_error and surfaces stderr verbatim", async () => {
    const tool = createCursorCodingTool({
      transport: fakeTransportWithStderr(
        [
          '{"type":"system","subtype":"init","session_id":"s"}',
          '{"type":"result","subtype":"error","is_error":true,"result":"refused","session_id":"s"}',
        ],
        ["something went wrong\n"],
        1,
      ),
    });
    const result = await tool.execute({ prompt: "x" }, {});
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected error");
    expect(result.error).toBe("refused");
    expect(result.stderr).toBe("something went wrong\n");
    expect(result.exitCode).toBe(1);
  });

  it("falls back to stderr text when the result frame is absent and exit is non-zero", async () => {
    const tool = createCursorCodingTool({
      transport: fakeTransportWithStderr(
        ['{"type":"system","subtype":"init","session_id":"s"}'],
        ["please run `cursor-agent login`\n"],
        2,
      ),
    });
    const result = await tool.execute({ prompt: "x" }, {});
    expect(result.ok).toBe(false);
    if (result.ok) throw new Error("expected error");
    expect(result.error).toContain("login");
    expect(result.exitCode).toBe(2);
  });

  it("captures Cursor's tool calls with input + output + failed flag", async () => {
    const tool = createCursorCodingTool({
      transport: fakeTransport([
        '{"type":"system","subtype":"init","session_id":"s"}',
        '{"type":"tool_call","subtype":"started","call_id":"c1","tool_call":{"readToolCall":{"args":{"path":"a.txt"}}},"session_id":"s"}',
        '{"type":"tool_call","subtype":"completed","call_id":"c1","tool_call":{"readToolCall":{"args":{"path":"a.txt"},"result":"file contents"}},"session_id":"s"}',
        '{"type":"tool_call","subtype":"started","call_id":"c2","tool_call":{"writeToolCall":{"args":{"path":"b.txt","contents":"x"}}},"session_id":"s"}',
        '{"type":"tool_call","subtype":"completed","call_id":"c2","tool_call":{"writeToolCall":{"args":{"path":"b.txt","contents":"x"},"result":{"error":"permission denied"}}},"session_id":"s"}',
        '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}',
      ]),
    });
    const result = await tool.execute({ prompt: "x" }, {});
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("expected ok");
    expect(result.toolCalls).toHaveLength(2);
    const [r, w] = result.toolCalls;
    expect(r!.id).toBe("c1");
    expect(r!.name).toBe("read");
    expect(r!.input).toEqual({ path: "a.txt" });
    expect(r!.output).toBe("file contents");
    expect(r!.failed).toBe(false);
    expect(w!.id).toBe("c2");
    expect(w!.name).toBe("write");
    expect(w!.failed).toBe(true);
  });
});
