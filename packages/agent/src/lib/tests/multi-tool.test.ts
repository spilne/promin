import { describe, it, expect } from "bun:test";
import { z } from "zod";
import { multiTool, command } from "../multi-tool.ts";

describe("multiTool", () => {
  it("routes to the correct command handler", async () => {
    const calls: string[] = [];
    const t = multiTool({
      name: "demo",
      description: "Demo tool",
      commands: {
        ping: command({
          parameters: z.object({ msg: z.string() }),
          execute: async ({ msg }) => {
            calls.push(`ping:${msg}`);
            return "pong";
          },
        }),
        echo: command({
          parameters: z.object({ text: z.string() }),
          execute: async ({ text }) => {
            calls.push(`echo:${text}`);
            return text;
          },
        }),
      },
    });

    await t.execute({ command: "ping", msg: "hello" });
    await t.execute({ command: "echo", text: "world" });

    expect(calls).toEqual(["ping:hello", "echo:world"]);
  });

  it("exposes a flat object schema (type:object) compatible with LLM APIs", () => {
    const t = multiTool({
      name: "ops",
      description: "...",
      commands: {
        read: command({ parameters: z.object({ id: z.string() }), execute: async () => "ok" }),
        write: command({
          parameters: z.object({ id: z.string(), data: z.string() }),
          execute: async () => "ok",
        }),
      },
    });

    // Valid inputs parse at the flat schema level
    expect(() => t.parameters.parse({ command: "read", id: "1" })).not.toThrow();
    expect(() => t.parameters.parse({ command: "write", id: "1", data: "x" })).not.toThrow();

    // Unknown command is rejected by the enum
    expect(() => t.parameters.parse({ command: "delete", id: "1" })).toThrow();

    // Non-command fields are optional in the flat schema (per-command validation in execute)
    expect(() => t.parameters.parse({ command: "write", id: "1" })).not.toThrow();
  });

  it("validates per-command required fields inside execute", async () => {
    const t = multiTool({
      name: "ops",
      description: "...",
      commands: {
        write: command({
          parameters: z.object({ id: z.string(), data: z.string() }),
          execute: async () => "ok",
        }),
      },
    });

    // Missing required field "data" is caught by the per-command schema inside execute
    await expect(t.execute({ command: "write", id: "1" })).rejects.toThrow();
  });

  it("single-command tool still works", async () => {
    const t = multiTool({
      name: "single",
      description: "...",
      commands: {
        go: command({
          parameters: z.object({ n: z.number() }),
          execute: async ({ n }) => n * 2,
        }),
      },
    });

    const result = await t.execute({ command: "go", n: 21 });
    expect(result).toBe(42);
  });

  it("includes command descriptions in the tool description", () => {
    const t = multiTool({
      name: "mem",
      description: "Memory operations",
      commands: {
        search: command({
          description: "Find relevant memories",
          parameters: z.object({ query: z.string() }),
          execute: async () => [],
        }),
        save: command({
          description: "Persist a new memory",
          parameters: z.object({ content: z.string() }),
          execute: async () => "saved",
        }),
      },
    });

    expect(t.description).toContain("Memory operations");
    expect(t.description).toContain("search: Find relevant memories");
    expect(t.description).toContain("save: Persist a new memory");
  });

  it("omits description line when a command has none", () => {
    const t = multiTool({
      name: "x",
      description: "X",
      commands: {
        run: command({ parameters: z.object({}), execute: async () => null }),
      },
    });
    expect(t.description).toContain("  run");
    expect(t.description).not.toContain("  run:");
  });

  it("throws on empty commands", () => {
    expect(() => multiTool({ name: "x", description: "x", commands: {} })).toThrow(
      "commands must not be empty",
    );
  });

  it("forwards requireApproval to the tool", () => {
    const t = multiTool({
      name: "guarded",
      description: "...",
      commands: { go: command({ parameters: z.object({}), execute: async () => null }) },
      requireApproval: true,
    });
    expect(t.requireApproval).toBe(true);
  });

  it("command() helper infers execute input from parameters", () => {
    // This is a TypeScript compile-time assertion — if the types are wrong, the file won't compile.
    const _c = command({
      parameters: z.object({ count: z.number(), label: z.string() }),
      execute: async ({ count, label }) => `${label}: ${count}`,
      //                 ^^^^^  ^^^^^  TypeScript should infer these as number and string
    });
    expect(_c).toBeDefined();
  });
});
