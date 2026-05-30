// ---------------------------------------------------------------------------
// `resolveRoleBinding` — inline passes through; ref reads the registry and
// throws loudly on a dangling binding or a missing registry.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { resolveRoleBinding } from "../resolve-role.ts";
import { InMemoryRoleRegistry } from "../in-memory-role-registry.ts";

describe("resolveRoleBinding", () => {
  it("returns the embedded definition for an inline binding (no registry needed)", async () => {
    const def = { systemPrompt: "inline persona", tools: ["bash"] };
    const out = await resolveRoleBinding({ inline: def });
    expect(out).toEqual(def);
  });

  it("resolves a ref against the registry", async () => {
    const roles = new InMemoryRoleRegistry();
    await roles.register({
      id: "git-master",
      definition: { systemPrompt: "you are a git master", tools: ["bash"] },
    });
    const out = await resolveRoleBinding({ ref: { id: "git-master" } }, { roles });
    expect(out.systemPrompt).toBe("you are a git master");
    expect(out.tools).toEqual(["bash"]);
  });

  it("resolves a pinned version", async () => {
    const roles = new InMemoryRoleRegistry();
    await roles.register({
      id: "r",
      version: "v1",
      definition: { systemPrompt: "one", tools: [] },
    });
    await roles.register({
      id: "r",
      version: "v2",
      definition: { systemPrompt: "two", tools: [] },
    });
    const out = await resolveRoleBinding({ ref: { id: "r", version: "v1" } }, { roles });
    expect(out.systemPrompt).toBe("one");
  });

  it("throws when a ref has no registry wired", async () => {
    await expect(resolveRoleBinding({ ref: { id: "git-master" } })).rejects.toThrow(
      /needs a RoleRegistry/,
    );
  });

  it("throws when the referenced role is not found", async () => {
    const roles = new InMemoryRoleRegistry();
    await expect(resolveRoleBinding({ ref: { id: "ghost" } }, { roles })).rejects.toThrow(
      /not found/,
    );
  });
});
