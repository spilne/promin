import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { tool } from "../tool.ts";
import { buildToolDefs } from "../tool-registry.ts";

describe("buildToolDefs", () => {
  it("uses the map key as the LLM tool name", () => {
    const t = tool({
      name: "myTool",
      description: "Does something",
      parameters: z.object({ x: z.string() }),
      execute: async () => "ok",
    });

    const defs = buildToolDefs({ myTool: t });
    expect(defs[0]?.name).toBe("myTool");
  });

  it("includes description in output", () => {
    const t = tool({
      name: "t",
      description: "A plain description",
      parameters: z.object({}),
      execute: async () => "",
    });

    const [def] = buildToolDefs({ t });
    expect(def?.description).toContain("A plain description");
  });

  it("appends usage to description when present", () => {
    const t = tool({
      name: "t",
      description: "Does X.",
      usage: "Use for case A not case B.",
      parameters: z.object({}),
      execute: async () => "",
    });

    const [def] = buildToolDefs({ t });
    expect(def?.description).toContain("Does X.");
    expect(def?.description).toContain("Use for case A not case B.");
  });

  it("appends examples to description when present", () => {
    const t = tool({
      name: "calc",
      description: "Calculate.",
      examples: [{ input: { expression: "2+2" }, output: "4" }],
      parameters: z.object({ expression: z.string() }),
      execute: async () => "",
    });

    const [def] = buildToolDefs({ calc: t });
    expect(def?.description).toContain("2+2");
    expect(def?.description).toContain("4");
  });

  it("generates correct JSON schema for parameters", () => {
    const t = tool({
      name: "t",
      description: "x",
      parameters: z.object({
        query: z.string(),
        limit: z.number().optional(),
      }),
      execute: async () => "",
    });

    const [def] = buildToolDefs({ t });
    expect(def?.parameters).toMatchObject({
      type: "object",
      properties: {
        query: { type: "string" },
        limit: { type: "number" },
      },
      required: ["query"],
    });
  });

  it("returns one def per tool in the map", () => {
    const make = (name: string) =>
      tool({ name, description: "x", parameters: z.object({}), execute: async () => "" });

    const defs = buildToolDefs({ a: make("a"), b: make("b"), c: make("c") });
    expect(defs).toHaveLength(3);
    expect(defs.map((d) => d.name).sort()).toEqual(["a", "b", "c"]);
  });

  it("returns empty array for empty map", () => {
    expect(buildToolDefs({})).toEqual([]);
  });
});
