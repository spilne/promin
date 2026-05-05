// ---------------------------------------------------------------------------
// Runtime contract tests for createScopedTool / createElevatedTool.
// Verifies:
//   1. Scoped tool throws when invoked without (namespaceId, resourceId).
//   2. Scoped tool body cannot forge or override scope — it sees what
//      the runtime injected, period.
//   3. Elevated tool throws when audit() is not called.
//   4. Elevated tool succeeds when audit() is called.
//   5. The cross-scope leak is prevented by-construction: the user's
//      execute fn only sees the scope its caller passed.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { z } from "zod";
import { createElevatedTool, createScopedTool } from "../tool.ts";

describe("createScopedTool", () => {
  it("passes (namespaceId, resourceId) to the execute body unchanged", async () => {
    const tool = createScopedTool({
      name: "echoScope",
      description: "Returns the scope it was given.",
      parameters: z.object({}),
      execute: async (_input, ctx) => `${ctx.namespaceId}/${ctx.resourceId}`,
    });

    const out = await tool.execute({}, { scope: { namespaceId: "acme", resourceId: "alice" } });
    expect(out).toBe("acme/alice");
  });

  it("throws when ctx is missing", async () => {
    const tool = createScopedTool({
      name: "needsScope",
      description: "test",
      parameters: z.object({}),
      execute: async () => "ok",
    });

    expect(tool.execute({})).rejects.toThrow(
      /Scoped tool 'needsScope' invoked without complete .*scope/,
    );
  });

  it("throws when ctx.scope is missing", async () => {
    const tool = createScopedTool({
      name: "needsScope2",
      description: "test",
      parameters: z.object({}),
      execute: async () => "ok",
    });

    expect(tool.execute({}, {})).rejects.toThrow(
      /Scoped tool 'needsScope2' invoked without complete/,
    );
  });

  it("throws when namespaceId is empty", async () => {
    const tool = createScopedTool({
      name: "needsNs",
      description: "test",
      parameters: z.object({}),
      execute: async () => "ok",
    });

    expect(tool.execute({}, { scope: { namespaceId: "", resourceId: "alice" } })).rejects.toThrow();
  });

  it("throws when resourceId is missing", async () => {
    const tool = createScopedTool({
      name: "needsRid",
      description: "test",
      parameters: z.object({}),
      execute: async () => "ok",
    });

    expect(tool.execute({}, { scope: { namespaceId: "acme" } })).rejects.toThrow();
  });

  it("forwards optional threadId / agentId to scope ctx", async () => {
    const tool = createScopedTool({
      name: "echoFull",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) =>
        `${ctx.namespaceId}/${ctx.resourceId}/${ctx.threadId ?? "_"}/${ctx.agentId ?? "_"}`,
    });

    const out = await tool.execute(
      {},
      {
        scope: {
          namespaceId: "acme",
          resourceId: "alice",
          threadId: "t-1",
          agentId: "support-bot",
        },
      },
    );
    expect(out).toBe("acme/alice/t-1/support-bot");
  });

  it("body cannot forge scope — only sees what runtime injected", async () => {
    let seen = "";
    const tool = createScopedTool({
      name: "tryForge",
      description: "test",
      parameters: z.object({ pretendNs: z.string(), pretendRid: z.string() }),
      execute: async (input, ctx) => {
        // The user CAN'T inject a different scope — ctx is bound by the
        // factory wrapper. The values it gets here come from the runtime.
        seen = `${ctx.namespaceId}/${ctx.resourceId}`;
        return `${input.pretendNs}/${input.pretendRid}`;
      },
    });

    await tool.execute(
      { pretendNs: "evil", pretendRid: "evil" },
      { scope: { namespaceId: "acme", resourceId: "alice" } },
    );
    expect(seen).toBe("acme/alice");
  });

  it("marks the tool with kind='scoped'", () => {
    const tool = createScopedTool({
      name: "marker",
      description: "test",
      parameters: z.object({}),
      execute: async () => "ok",
    });
    expect(tool.kind).toBe("scoped");
  });
});

describe("createElevatedTool", () => {
  it("succeeds when execute calls ctx.audit()", async () => {
    const tool = createElevatedTool({
      name: "admin:doThing",
      description: "test",
      requires: "admin",
      parameters: z.object({ target: z.string() }),
      execute: async (input, ctx) => {
        ctx.audit({ action: "doThing", target: input.target });
        return `${ctx.namespaceId}->${input.target}`;
      },
    });

    const out = await tool.execute(
      { target: "bob" },
      { scope: { namespaceId: "acme", resourceId: "admin-1" } },
    );
    expect(out).toBe("acme->bob");
  });

  it("throws when execute returns without calling audit()", async () => {
    const tool = createElevatedTool({
      name: "admin:silent",
      description: "test",
      parameters: z.object({}),
      execute: async () => {
        // INTENTIONALLY missing ctx.audit() — runtime must catch this.
        return "ok";
      },
    });

    expect(
      tool.execute({}, { scope: { namespaceId: "acme", resourceId: "admin-1" } }),
    ).rejects.toThrow(/completed without calling ctx\.audit/);
  });

  it("throws when scope is missing", async () => {
    const tool = createElevatedTool({
      name: "admin:noScope",
      description: "test",
      parameters: z.object({}),
      execute: async () => "ok",
    });

    expect(tool.execute({})).rejects.toThrow(/invoked without complete .*scope/);
  });

  it("captures the `requires` capability marker on the tool", () => {
    const tool = createElevatedTool({
      name: "admin:needsCap",
      description: "test",
      requires: "billing",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "noop" });
        return "ok";
      },
    });
    expect(tool.requires).toBe("billing");
    expect(tool.kind).toBe("elevated");
  });

  it("audit can be called multiple times — only the first call clears the requirement", async () => {
    let auditCalls = 0;
    const tool = createElevatedTool({
      name: "admin:multiAudit",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        ctx.audit({ action: "first" });
        ctx.audit({ action: "second" });
        auditCalls = 2;
        return "ok";
      },
    });

    const out = await tool.execute({}, { scope: { namespaceId: "acme", resourceId: "admin-1" } });
    expect(out).toBe("ok");
    expect(auditCalls).toBe(2);
  });
});

describe("scope-leak prevention by-construction", () => {
  it("two scoped tool calls with different scopes never see each other", async () => {
    const observed: Array<string> = [];
    const tool = createScopedTool({
      name: "observer",
      description: "test",
      parameters: z.object({}),
      execute: async (_input, ctx) => {
        observed.push(`${ctx.namespaceId}/${ctx.resourceId}`);
        return "ok";
      },
    });

    await Promise.all([
      tool.execute({}, { scope: { namespaceId: "acme", resourceId: "alice" } }),
      tool.execute({}, { scope: { namespaceId: "globex", resourceId: "bob" } }),
    ]);

    expect(observed.sort()).toEqual(["acme/alice", "globex/bob"]);
  });
});
