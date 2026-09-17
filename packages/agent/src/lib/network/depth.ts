// ---------------------------------------------------------------------------
// Cross-agent call context — depth + caller chain, propagated through the
// async tree via AsyncLocalStorage so nested `callAgent` invocations can
// see how deep they are without threading the counter through every API.
//
// Lifetime is one chain of cooperating agent calls:
//   user → A.invoke()
//      └─ A's callAgent("B") opens a frame { depth: 1, chain: ["A"] }
//          └─ B.invoke() runs inside that frame
//              └─ B's callAgent("C") sees depth: 1, opens { depth: 2, chain: ["A", "B"] }
//                  └─ C.invoke() runs inside that frame
//
// The top-level invoke (from the gateway) doesn't run inside a frame —
// `currentCallContext()` returns undefined and depth is treated as 0.
// ---------------------------------------------------------------------------

import { AsyncLocalStorage } from "node:async_hooks";

export interface NetworkCallContext {
  /** How many `callAgent` hops we're inside. 1 = one nested call from the user-triggered turn. */
  readonly depth: number;
  /** Caller chain: the registered agent ids of each frame, oldest first. */
  readonly chain: ReadonlyArray<string>;
}

const STORE = new AsyncLocalStorage<NetworkCallContext>();

export function currentCallContext(): NetworkCallContext | undefined {
  return STORE.getStore();
}

/** Run `fn` inside a fresh call frame. Use when entering a `callAgent`. */
export function runInCallContext<T>(ctx: NetworkCallContext, fn: () => Promise<T>): Promise<T> {
  return STORE.run(ctx, fn);
}

/**
 * Compute the next frame given the current store + the agent we're about
 * to call. Returns the new context object — caller passes it to
 * `runInCallContext`. Pure; no IO.
 */
export function nextCallFrame(callerId: string): NetworkCallContext {
  const cur = currentCallContext();
  return {
    depth: (cur?.depth ?? 0) + 1,
    chain: cur ? [...cur.chain, callerId] : [callerId],
  };
}
