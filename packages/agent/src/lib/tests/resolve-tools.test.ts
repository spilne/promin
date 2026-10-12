import { describe, it, expect } from "bun:test";
import { resolveTools } from "../agent-shared.ts";
import { tool } from "../tool.ts";
import type { ToolRegistry } from "../tool-registry.ts";
import { z } from "zod";

// ---------------------------------------------------------------------------
// resolveTools refuses ambiguous config
// ---------------------------------------------------------------------------

describe("resolveTools — ambiguous config", () => {
  it("throws when both toolRegistry and tools are provided", () => {
    const fakeRegistry = { getTools: () => ({}) } as Parameters<
      typeof resolveTools
    >[0]["toolRegistry"];
    expect(() =>
      resolveTools({
        toolRegistry: fakeRegistry,
        tools: {
          foo: tool({
            name: "foo",
            description: "",
            parameters: z.object({}),
            execute: async () => "ok",
          }),
        },
      }),
    ).toThrow(/cannot specify both/i);
  });

  it("returns toolRegistry tools when only registry is set", () => {
    const fakeRegistry: ToolRegistry = {
      close: () => {},
      getTools: () => ({
        bar: tool({
          name: "bar",
          description: "",
          parameters: z.object({}),
          execute: async () => "r",
        }),
      }),
    };
    const result = resolveTools({ toolRegistry: fakeRegistry });
    expect(Object.keys(result)).toEqual(["bar"]);
  });

  it("returns tools map when only inline tools are set", () => {
    const result = resolveTools({
      tools: {
        baz: tool({
          name: "baz",
          description: "",
          parameters: z.object({}),
          execute: async () => "i",
        }),
      },
    });
    expect(Object.keys(result)).toEqual(["baz"]);
  });

  it("returns empty map when neither is set", () => {
    expect(resolveTools({})).toEqual({});
  });
});
