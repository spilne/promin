// ---------------------------------------------------------------------------
// Agent invoke-body scope validation — proves cross-scope leak prevention
// is by-construction at the route boundary. Every invoke / stream /
// thread-send / approval body MUST carry namespaceId AND one of
// (resourceId | ownerId); empty body, missing namespaceId, missing scope,
// or conflicting scope all reject before reaching the agent layer.
//
// "Scope" here means (namespaceId, resourceId | ownerId) — the outermost
// boundary the agent layer enforces. Multi-tenant org isolation is a
// future axis above this; today's deployment model is single-operator.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { parseScopeFields } from "../agents.ts";

describe("parseScopeFields — cross-scope leak prevention", () => {
  it("accepts namespaceId + resourceId", () => {
    const result = parseScopeFields({ namespaceId: "acme", resourceId: "doc-42" });
    expect(result).toEqual({ namespaceId: "acme", resourceId: "doc-42" });
  });

  it("accepts namespaceId + ownerId", () => {
    const result = parseScopeFields({ namespaceId: "acme", ownerId: "u-9" });
    expect(result).toEqual({ namespaceId: "acme", ownerId: "u-9" });
  });

  it("rejects when both resourceId and ownerId are set (conflicting_identity)", () => {
    const result = parseScopeFields({
      namespaceId: "acme",
      resourceId: "doc-42",
      ownerId: "u-9",
    });
    expect(result).toEqual({ error: "conflicting_identity" });
  });

  it("rejects when neither resourceId nor ownerId is set (missing_scope_identity)", () => {
    const result = parseScopeFields({ namespaceId: "acme" });
    expect(result).toEqual({ error: "missing_scope_identity" });
  });

  it("rejects empty resourceId AND empty ownerId as missing", () => {
    const result = parseScopeFields({ namespaceId: "acme", resourceId: "", ownerId: "" });
    expect(result).toEqual({ error: "missing_scope_identity" });
  });

  it("treats empty string resourceId as absent — falls back to ownerId", () => {
    const result = parseScopeFields({ namespaceId: "acme", resourceId: "", ownerId: "u-9" });
    expect(result).toEqual({ namespaceId: "acme", ownerId: "u-9" });
  });

  it("treats empty string ownerId as absent — falls back to resourceId", () => {
    const result = parseScopeFields({ namespaceId: "acme", resourceId: "doc-42", ownerId: "" });
    expect(result).toEqual({ namespaceId: "acme", resourceId: "doc-42" });
  });

  it("rejects missing namespaceId", () => {
    const result = parseScopeFields({ resourceId: "doc-42" });
    expect(result).toEqual({ error: "missing_namespaceId" });
  });

  it("rejects empty namespaceId", () => {
    const result = parseScopeFields({ namespaceId: "", resourceId: "doc-42" });
    expect(result).toEqual({ error: "missing_namespaceId" });
  });

  it("rejects non-string namespaceId (covers loosely-typed HTTP bodies)", () => {
    const result = parseScopeFields({
      namespaceId: 42 as unknown as string,
      resourceId: "doc-42",
    });
    expect(result).toEqual({ error: "missing_namespaceId" });
  });

  it("rejects non-string resourceId", () => {
    const result = parseScopeFields({
      namespaceId: "acme",
      resourceId: 42 as unknown as string,
    });
    // Non-string resourceId is treated as absent → falls into the
    // missing-scope branch.
    expect(result).toEqual({ error: "missing_scope_identity" });
  });
});
