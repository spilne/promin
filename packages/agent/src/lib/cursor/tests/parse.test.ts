import { describe, expect, it } from "bun:test";
import { NdJsonLineParser, classifyFrame } from "../parse.ts";
import type { CursorAssistantFrame, CursorResultFrame, CursorSystemInitFrame } from "../frames.ts";

describe("NdJsonLineParser", () => {
  it("emits one frame per newline-delimited line", () => {
    const p = new NdJsonLineParser();
    p.push('{"type":"system","subtype":"init","session_id":"s1"}\n');
    p.push(
      '{"type":"result","subtype":"success","is_error":false,"result":"hi","session_id":"s1"}\n',
    );
    const frames = p.take();
    expect(frames).toHaveLength(2);
    expect((frames[0] as CursorSystemInitFrame).session_id).toBe("s1");
    expect((frames[1] as CursorResultFrame).result).toBe("hi");
  });

  it("buffers across chunk boundaries that split a line", () => {
    const p = new NdJsonLineParser();
    p.push('{"type":"system","subt');
    expect(p.take()).toHaveLength(0);
    p.push('ype":"init","session_id":"s2"}\n');
    const frames = p.take();
    expect(frames).toHaveLength(1);
    expect((frames[0] as CursorSystemInitFrame).session_id).toBe("s2");
  });

  it("handles multiple lines in a single chunk", () => {
    const p = new NdJsonLineParser();
    const blob =
      '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"a"}]},"session_id":"s","timestamp_ms":1}\n' +
      '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"b"}]},"session_id":"s","timestamp_ms":2}\n';
    p.push(blob);
    expect(p.take()).toHaveLength(2);
  });

  it("flush() emits a final partial line that lacks a trailing newline", () => {
    const p = new NdJsonLineParser();
    p.push('{"type":"system","subtype":"init","session_id":"final"}');
    expect(p.take()).toHaveLength(0);
    p.flush();
    const frames = p.take();
    expect(frames).toHaveLength(1);
    expect((frames[0] as CursorSystemInitFrame).session_id).toBe("final");
  });

  it("forwards parse errors via onParseError, never throws", () => {
    const errs: string[] = [];
    const p = new NdJsonLineParser({ onParseError: (line) => errs.push(line) });
    p.push("not-json\n");
    p.push('{"type":"system","subtype":"init","session_id":"ok"}\n');
    const frames = p.take();
    expect(frames).toHaveLength(1);
    expect(errs).toEqual(["not-json"]);
  });

  it("skips empty / whitespace-only lines silently", () => {
    const p = new NdJsonLineParser();
    p.push("\n   \n");
    expect(p.take()).toHaveLength(0);
  });

  it("decodes UTF-8 byte chunks across multi-byte boundaries", () => {
    // The é is two bytes in UTF-8; split it.
    const text = '{"type":"system","subtype":"init","session_id":"é"}\n';
    const bytes = new TextEncoder().encode(text);
    const split = Math.floor(bytes.length / 2);
    const p = new NdJsonLineParser();
    p.push(bytes.slice(0, split));
    p.push(bytes.slice(split));
    const frames = p.take();
    expect(frames).toHaveLength(1);
    expect((frames[0] as CursorSystemInitFrame).session_id).toBe("é");
  });
});

describe("classifyFrame — discriminator", () => {
  it("maps known types to their typed shape", () => {
    expect(classifyFrame({ type: "system", subtype: "init", session_id: "x" }).type).toBe("system");
    expect(
      classifyFrame({ type: "assistant", message: { role: "assistant", content: [] } }).type,
    ).toBe("assistant");
    expect(classifyFrame({ type: "tool_call", subtype: "started" }).type).toBe("tool_call");
    expect(classifyFrame({ type: "result", subtype: "success" }).type).toBe("result");
  });

  it("preserves the discriminator for unknown frame types and stuffs raw payload", () => {
    const frame = classifyFrame({ type: "future_frame", foo: 1 });
    expect(frame.type).toBe("future_frame");
    if ("raw" in frame) expect(frame.raw.foo).toBe(1);
  });

  it("system frames with unknown subtypes degrade to unknown", () => {
    const frame = classifyFrame({ type: "system", subtype: "shutdown", session_id: "x" });
    expect(frame.type).toBe("unknown");
  });

  it("non-objects round-trip as unknown without crashing", () => {
    const frame = classifyFrame("nope");
    expect(frame.type).toBe("unknown");
  });
});

describe("assistantFrameText", () => {
  it("returns the cumulative text for a single block", async () => {
    const { assistantFrameText } = await import("../frames.ts");
    const frame = {
      type: "assistant",
      session_id: "s",
      timestamp_ms: 1,
      message: { role: "assistant", content: [{ type: "text", text: "Hi " }] },
    } as CursorAssistantFrame;
    expect(assistantFrameText(frame)).toBe("Hi ");
  });

  it("concatenates multiple text blocks", async () => {
    const { assistantFrameText } = await import("../frames.ts");
    expect(
      assistantFrameText({
        type: "assistant",
        session_id: "s",
        message: {
          role: "assistant",
          content: [
            { type: "text", text: "alpha " },
            { type: "text", text: "beta" },
          ],
        },
      } as CursorAssistantFrame),
    ).toBe("alpha beta");
  });
});
