// ---------------------------------------------------------------------------
// Pure-helper tests for the network policy primitives — no agent loop,
// no IO. Just the predicate logic that gates discovery + delegation.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { matchesNetworkScope, networksOverlap, peerVisible, type PeerView } from "../types.ts";

const peer = (overrides: Partial<PeerView> = {}): PeerView => ({
  id: "writer",
  namespaceId: "acme",
  networks: ["default"],
  capabilities: ["chat"],
  tags: [],
  ...overrides,
});

describe("matchesNetworkScope", () => {
  it("undefined / false denies everything", () => {
    expect(matchesNetworkScope(undefined, peer())).toBe(false);
    expect(matchesNetworkScope(false, peer())).toBe(false);
  });

  it("true allows everyone", () => {
    expect(matchesNetworkScope(true, peer())).toBe(true);
  });

  it("array sugar acts as id allowlist", () => {
    expect(matchesNetworkScope(["writer"], peer({ id: "writer" }))).toBe(true);
    expect(matchesNetworkScope(["writer"], peer({ id: "reviewer" }))).toBe(false);
  });

  it("excludeIds is a hard deny", () => {
    expect(matchesNetworkScope({ excludeIds: ["writer"] }, peer({ id: "writer" }))).toBe(false);
    // Even when an inclusion matches, exclude wins.
    expect(
      matchesNetworkScope({ ids: ["writer"], excludeIds: ["writer"] }, peer({ id: "writer" })),
    ).toBe(false);
  });

  it("inclusion fields are OR-combined", () => {
    const scope = { capabilities: ["research"], tags: ["safe"] };
    expect(matchesNetworkScope(scope, peer({ capabilities: ["research"] }))).toBe(true);
    expect(matchesNetworkScope(scope, peer({ tags: ["safe"] }))).toBe(true);
    expect(matchesNetworkScope(scope, peer())).toBe(false);
  });

  it("object form with no inclusion fields means everyone (minus excludeIds)", () => {
    expect(matchesNetworkScope({ excludeIds: ["legacy"] }, peer({ id: "writer" }))).toBe(true);
    expect(matchesNetworkScope({}, peer({ id: "writer" }))).toBe(true);
  });
});

describe("networksOverlap", () => {
  it("both undefined → both default to ['default'] → overlap", () => {
    expect(networksOverlap(undefined, undefined)).toBe(true);
  });

  it("one declares networks excluding default → no overlap with undefined peer", () => {
    expect(networksOverlap(["finance"], undefined)).toBe(false);
  });

  it("shared membership → overlap", () => {
    expect(networksOverlap(["finance", "default"], ["finance"])).toBe(true);
  });

  it("disjoint memberships → no overlap", () => {
    expect(networksOverlap(["finance"], ["research"])).toBe(false);
  });

  it("empty array is treated as default", () => {
    expect(networksOverlap([], [])).toBe(true);
    expect(networksOverlap([], ["default"])).toBe(true);
  });
});

describe("peerVisible", () => {
  const callerNamespaceId = "acme";

  it("rejects cross-namespace peers", () => {
    const result = peerVisible({
      callerNamespaceId,
      callerNetworks: undefined,
      callerScope: true,
      peer: peer({ namespaceId: "globex" }),
    });
    expect(result).toBe(false);
  });

  it("rejects when networks don't intersect", () => {
    const result = peerVisible({
      callerNamespaceId,
      callerNetworks: ["finance"],
      callerScope: true,
      peer: peer({ networks: ["research"] }),
    });
    expect(result).toBe(false);
  });

  it("accepts when same namespace + shared network + scope matches", () => {
    const result = peerVisible({
      callerNamespaceId,
      callerNetworks: ["default"],
      callerScope: true,
      peer: peer(),
    });
    expect(result).toBe(true);
  });

  it("default-network peers see each other when both omit networks", () => {
    const result = peerVisible({
      callerNamespaceId,
      callerNetworks: undefined,
      callerScope: true,
      peer: peer({ networks: [] }),
    });
    expect(result).toBe(true);
  });

  it("billing-bot is invisible to default-only callers but visible to bridges", () => {
    const billingPeer = peer({ id: "billing", networks: ["finance"] });
    // Default-only caller can't see it.
    expect(
      peerVisible({
        callerNamespaceId,
        callerNetworks: undefined,
        callerScope: true,
        peer: billingPeer,
      }),
    ).toBe(false);
    // Coordinator that bridges default + finance can.
    expect(
      peerVisible({
        callerNamespaceId,
        callerNetworks: ["default", "finance"],
        callerScope: true,
        peer: billingPeer,
      }),
    ).toBe(true);
  });

  it("scope filter still applies after networks intersect", () => {
    const result = peerVisible({
      callerNamespaceId,
      callerNetworks: ["default"],
      callerScope: { capabilities: ["research"] },
      peer: peer({ capabilities: ["chat"] }),
    });
    expect(result).toBe(false);
  });
});
