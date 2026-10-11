// ---------------------------------------------------------------------------
// composeAgentInstanceId — the id must be injective over
// (namespaceId, registeredAgentId, ownerId), whatever the parts contain.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { composeAgentInstanceId } from "../types.ts";

describe("composeAgentInstanceId", () => {
  it("keeps plain parts readable", () => {
    expect(
      composeAgentInstanceId({
        namespaceId: "acme",
        registeredAgentId: "writer",
        ownerId: "alice",
      }),
    ).toBe("acme::writer::alice");
  });

  it("triples that differ only in where the separator falls do not collide", () => {
    const a = composeAgentInstanceId({
      namespaceId: "acme::writer",
      registeredAgentId: "alice",
      ownerId: "bob",
    });
    const b = composeAgentInstanceId({
      namespaceId: "acme",
      registeredAgentId: "writer::alice",
      ownerId: "bob",
    });
    const c = composeAgentInstanceId({
      namespaceId: "acme",
      registeredAgentId: "writer",
      ownerId: "alice::bob",
    });
    expect(new Set([a, b, c]).size).toBe(3);
  });

  it("single colons next to the separator do not collide", () => {
    const a = composeAgentInstanceId({ namespaceId: "a:", registeredAgentId: "b", ownerId: "c" });
    const b = composeAgentInstanceId({ namespaceId: "a", registeredAgentId: ":b", ownerId: "c" });
    expect(a).not.toBe(b);
  });

  it("an owner that spells an escape sequence does not collide with an escaped colon", () => {
    const a = composeAgentInstanceId({ namespaceId: "n", registeredAgentId: "w", ownerId: "x:y" });
    const b = composeAgentInstanceId({
      namespaceId: "n",
      registeredAgentId: "w",
      ownerId: "x%3Ay",
    });
    expect(a).not.toBe(b);
  });
});
