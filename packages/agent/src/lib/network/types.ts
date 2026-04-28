// ---------------------------------------------------------------------------
// AgentsNetwork — peer discovery + delegation primitive.
//
// Recipes that opt in via `backend.network` get two built-in tools:
//   findAgent({ capability?, tag?, query? })     — peer directory
//   callAgent({ id, prompt })                    — synchronous delegation
//
// The mechanics:
//   1. Discovery is filtered by (a) caller's namespace, (b) shared network
//      membership, (c) caller's `canDiscover` policy. Cross-namespace is
//      out of scope here — that's `RemoteAgentBackend` territory.
//   2. Delegation propagates the caller's `ownerId` into the callee's
//      instance, so `alice → coordinator → writer` reuses
//      `acme::writer::alice` instead of creating a fresh untracked
//      resourceId.
//   3. A depth counter (AsyncLocalStorage) blocks cycles past `maxDepth`.
//   4. Each callee runs with its OWN tool kit per its recipe — caller's
//      tools never leak.
//
// Two-layer access control
// ------------------------
// Caller-side (`canDiscover`, `canCall`) decides who the caller is willing
// to talk to. Callee-side (`networks`) decides who is willing to talk
// back. Both checks must pass.
//
// A peer P is visible/callable from caller C iff ALL hold:
//   - C.namespaceId === P.namespaceId
//   - networks(C) ∩ networks(P) ≠ ∅   (default network applies when unset)
//   - C's policy matches P (caller-side filter — capability / tag / id)
//
// `networks` defaults to ["default"] for both sides — most agents
// participate in one common network and see each other freely. Sensitive
// recipes opt out by declaring narrower memberships:
//
//     billing-bot.networks = ["finance"]      // invisible to default
//     coordinator-bot.networks = ["default", "finance"]  // bridges
//
// This is callee-controlled — callers can't bypass by lying about their
// own networks; the intersection requires both sides to agree.
// ---------------------------------------------------------------------------

/**
 * Caller-side filter for "which peers may I see / call".
 *
 *   true                                  → everyone in caller's namespace
 *   false (or undefined)                  → nobody — feature disabled
 *   string[]                              → sugar — explicit peer-id allowlist
 *   { ids?, capabilities?, tags?,         → rich form, OR-combined inclusion
 *     excludeIds? }                          plus hard-deny excludeIds
 *
 * For the object form, providing no inclusion fields means "everyone in
 * namespace, minus excludeIds". An inclusion field with an empty array
 * matches nothing — leave it unset to mean "ignore this dimension".
 */
export type NetworkScope = boolean | ReadonlyArray<string> | NetworkScopeObject;

export interface NetworkScopeObject {
  readonly ids?: ReadonlyArray<string>;
  readonly capabilities?: ReadonlyArray<string>;
  readonly tags?: ReadonlyArray<string>;
  readonly excludeIds?: ReadonlyArray<string>;
}

/**
 * Recipe-level network configuration. Set `backend.network` on a
 * `LocalAgentBackend` to opt the recipe into the agents network.
 */
export interface NetworkRecipe {
  /**
   * Networks this agent participates in. The discovery + call check
   * requires the caller and callee to share at least one network.
   * Defaults to `["default"]` when unset — most agents see each other.
   * Declare a narrower set to opt out of the default network.
   */
  readonly networks?: ReadonlyArray<string>;
  /** Caller-side filter: which peers can this agent see in `findAgent`? */
  readonly canDiscover?: NetworkScope;
  /** Caller-side filter: which peers can this agent invoke in `callAgent`? */
  readonly canCall?: NetworkScope;
  /** Cycle / runaway-cost guard. Default 3. */
  readonly maxDepth?: number;
}

export const DEFAULT_NETWORK = "default";
export const DEFAULT_MAX_DEPTH = 3;

// ---------------------------------------------------------------------------
// Predicate helpers
// ---------------------------------------------------------------------------

/** Peer view as the policy checks need it — minimal slice of RegisteredAgent. */
export interface PeerView {
  readonly id: string;
  readonly namespaceId: string;
  readonly networks: ReadonlyArray<string>;
  readonly capabilities: ReadonlyArray<string>;
  readonly tags: ReadonlyArray<string>;
}

/**
 * Test whether `peer` matches a caller-side `scope` filter. Pure — no
 * IO, no namespace check (that's `peerVisible` / done by the caller).
 */
export function matchesNetworkScope(scope: NetworkScope | undefined, peer: PeerView): boolean {
  if (scope === undefined || scope === false) return false;
  if (scope === true) return true;
  if (Array.isArray(scope)) return scope.includes(peer.id);

  const obj = scope as NetworkScopeObject;
  if (obj.excludeIds && obj.excludeIds.includes(peer.id)) return false;

  // No inclusion fields → "everyone, minus excludeIds".
  const hasInclusionFilter =
    (obj.ids && obj.ids.length > 0) ||
    (obj.capabilities && obj.capabilities.length > 0) ||
    (obj.tags && obj.tags.length > 0);
  if (!hasInclusionFilter) return true;

  if (obj.ids && obj.ids.includes(peer.id)) return true;
  if (obj.capabilities && obj.capabilities.some((c) => peer.capabilities.includes(c))) return true;
  if (obj.tags && obj.tags.some((t) => peer.tags.includes(t))) return true;
  return false;
}

/**
 * Two networks intersect when they share at least one membership.
 * Both sides default to `["default"]` when their recipe didn't declare
 * any networks — the common case where every agent sees every other.
 */
export function networksOverlap(
  callerNetworks: ReadonlyArray<string> | undefined,
  peerNetworks: ReadonlyArray<string> | undefined,
): boolean {
  const a = callerNetworks && callerNetworks.length > 0 ? callerNetworks : [DEFAULT_NETWORK];
  const b = peerNetworks && peerNetworks.length > 0 ? peerNetworks : [DEFAULT_NETWORK];
  return a.some((n) => b.includes(n));
}

/**
 * Compose the full visibility check for "can caller see this peer". Used
 * by both `findAgent` (with `canDiscover`) and `callAgent` (with `canCall`).
 */
export function peerVisible(args: {
  readonly callerNamespaceId: string;
  readonly callerNetworks: ReadonlyArray<string> | undefined;
  readonly callerScope: NetworkScope | undefined;
  readonly peer: PeerView;
}): boolean {
  if (args.peer.namespaceId !== args.callerNamespaceId) return false;
  if (!networksOverlap(args.callerNetworks, args.peer.networks)) return false;
  return matchesNetworkScope(args.callerScope, args.peer);
}

/**
 * Structured error thrown by `callAgent` when the cycle / depth guard
 * trips. Surfaces the chain so the caller's LLM can decide what to do.
 */
export class NetworkMaxDepthError extends Error {
  readonly code = "network_max_depth_exceeded";
  constructor(
    readonly maxDepth: number,
    readonly attemptedDepth: number,
    readonly chain: ReadonlyArray<string>,
  ) {
    super(
      `callAgent: cycle / depth guard tripped at depth ${attemptedDepth} (max ${maxDepth}). Chain: ${chain.join(" → ")}`,
    );
  }
}

/**
 * Structured error thrown when the caller's policy denies the call.
 */
export class NetworkPermissionError extends Error {
  readonly code = "network_permission_denied";
  constructor(
    readonly callerId: string,
    readonly peerId: string,
    readonly reason: string,
  ) {
    super(`callAgent: ${callerId} cannot call ${peerId}: ${reason}`);
  }
}
