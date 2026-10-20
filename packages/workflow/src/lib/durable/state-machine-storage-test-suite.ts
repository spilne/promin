// ---------------------------------------------------------------------------
// Portable StateMachineStorage conformance suite
//
// Usage:
//   import { stateMachineStorageTestSuite } from "@promin/workflow/testing";
//   stateMachineStorageTestSuite(() => new MyStateMachineStorage());
//
// Every test uses fresh machine ids, so backends that share one database
// across tests need no truncation between them.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import type { StateMachineStorage } from "./state-machine-storage.ts";
import { retryMiddleware, stateMachine } from "./state-machine.ts";

export interface StateMachineStorageTestSuiteOptions {
  /**
   * A second storage instance over the same backend, standing in for
   * another process. Enables the cross-instance lock tests.
   */
  createPeer?: () => StateMachineStorage | Promise<StateMachineStorage>;
}

let seq = 0;
function freshId(label: string): string {
  seq += 1;
  return `sm-conf-${label}-${seq}-${crypto.randomUUID().slice(0, 8)}`;
}

/** Poll until `tryLock` succeeds (the previous lease expired) or give up. */
async function lockAfterExpiry(params: {
  storage: StateMachineStorage;
  id: string;
}): Promise<string | null> {
  for (let i = 0; i < 100; i++) {
    await new Promise((r) => setTimeout(r, 10));
    const token = await params.storage.tryLock({ id: params.id, durationMs: 30_000 });
    if (token !== null) return token;
  }
  return null;
}

async function rejects(fn: () => Promise<unknown>): Promise<unknown> {
  try {
    await fn();
  } catch (err) {
    return err;
  }
  throw new Error("expected the call to reject");
}

/**
 * Run the conformance suite against any `StateMachineStorage`. The factory
 * is called once per test.
 */
export function stateMachineStorageTestSuite(
  factory: () => StateMachineStorage | Promise<StateMachineStorage>,
  options: StateMachineStorageTestSuiteOptions = {},
): void {
  describe("StateMachineStorage conformance", () => {
    describe("create / load", () => {
      it("round-trips every field", async () => {
        const s = await factory();
        const id = freshId("create");
        await s.create({
          id,
          name: "order",
          type: "order-type",
          namespace: "tenant-a",
          initial: "draft",
          context: { items: ["a"], n: 1 },
          version: "v2",
          metadata: { owner: "u1" },
        });

        const m = await s.load(id);
        expect(m).not.toBeNull();
        expect(m!.id).toBe(id);
        expect(m!.name).toBe("order");
        expect(m!.type).toBe("order-type");
        expect(m!.namespace).toBe("tenant-a");
        expect(m!.current).toBe("draft");
        expect(m!.context).toEqual({ items: ["a"], n: 1 });
        expect(m!.version).toBe("v2");
        expect(m!.metadata).toEqual({ owner: "u1" });
        expect(m!.revision).toBe(0);
        expect(m!.createdAt).toBeInstanceOf(Date);
        expect(m!.updatedAt).toBeInstanceOf(Date);
      });

      it("leaves optional fields undefined when not supplied", async () => {
        const s = await factory();
        const id = freshId("minimal");
        await s.create({ id, name: "n", initial: "a", context: {} });
        const m = await s.load(id);
        expect(m!.type).toBeUndefined();
        expect(m!.namespace).toBeUndefined();
        expect(m!.version).toBeUndefined();
        expect(m!.metadata).toBeUndefined();
        expect(await s.loadEvents(id)).toEqual([]);
      });

      it("returns null for an unknown machine", async () => {
        const s = await factory();
        expect(await s.load(freshId("ghost"))).toBeNull();
      });
    });

    describe("transition", () => {
      it("moves the state, replaces the context and appends one event", async () => {
        const s = await factory();
        const id = freshId("tx");
        await s.create({ id, name: "n", initial: "a", context: { n: 0 } });

        await s.transition({
          id,
          from: "a",
          to: "b",
          expectedRevision: 0,
          event: "go",
          context: { n: 1 },
          eventData: { by: "u1" },
          metadata: { reason: "test" },
        });

        const m = await s.load(id);
        expect(m!.current).toBe("b");
        expect(m!.context).toEqual({ n: 1 });
        expect(m!.revision).toBe(1);
        expect(m!.updatedAt.getTime()).toBeGreaterThanOrEqual(m!.createdAt.getTime());

        const events = await s.loadEvents(id);
        expect(events).toHaveLength(1);
        const e = events[0]!;
        expect(typeof e.id).toBe("string");
        expect(e.event).toBe("go");
        expect(e.from).toBe("a");
        expect(e.to).toBe("b");
        expect(e.context).toEqual({ n: 1 });
        expect(e.metadata).toEqual({ reason: "test" });
        expect(e.createdAt).toBeInstanceOf(Date);
        expect(e.eventData).toEqual({ by: "u1" });
      });

      it("rejects when the machine is not in `from` and changes nothing", async () => {
        const s = await factory();
        const id = freshId("cas");
        await s.create({ id, name: "n", initial: "a", context: { n: 0 } });

        await rejects(() =>
          s.transition({
            id,
            from: "b",
            to: "c",
            expectedRevision: 0,
            event: "go",
            context: { n: 9 },
          }),
        );

        const m = await s.load(id);
        expect(m!.current).toBe("a");
        expect(m!.context).toEqual({ n: 0 });
        expect(m!.revision).toBe(0);
        expect(await s.loadEvents(id)).toEqual([]);
      });

      it("rejects a stale revision even when the state matches, and changes nothing", async () => {
        const s = await factory();
        const id = freshId("cas-rev");
        await s.create({ id, name: "n", initial: "a", context: { n: 0 } });
        await s.transition({
          id,
          from: "a",
          to: "a",
          expectedRevision: 0,
          event: "t",
          context: { n: 1 },
        });

        // A writer that loaded the machine before the first tick.
        await rejects(() =>
          s.transition({
            id,
            from: "a",
            to: "a",
            expectedRevision: 0,
            event: "t",
            context: { n: 9 },
          }),
        );
        await rejects(() =>
          s.transition({
            id,
            from: "a",
            to: "a",
            expectedRevision: 2,
            event: "t",
            context: { n: 9 },
          }),
        );

        const m = await s.load(id);
        expect(m!.context).toEqual({ n: 1 });
        expect(m!.revision).toBe(1);
        expect(await s.loadEvents(id)).toHaveLength(1);
      });

      it("rejects for an unknown machine", async () => {
        const s = await factory();
        await rejects(() =>
          s.transition({
            id: freshId("missing"),
            from: "a",
            to: "b",
            expectedRevision: 0,
            event: "go",
            context: {},
          }),
        );
      });

      it("concurrent transitions from the same state: exactly one wins", async () => {
        const s = await factory();
        const id = freshId("race");
        await s.create({ id, name: "n", initial: "a", context: {} });

        const results = await Promise.allSettled(
          ["b", "c", "d", "e", "f"].map((to) =>
            s.transition({
              id,
              from: "a",
              to,
              expectedRevision: 0,
              event: `to-${to}`,
              context: { to },
            }),
          ),
        );
        const winners = results.filter((r) => r.status === "fulfilled");
        expect(winners).toHaveLength(1);

        const m = await s.load(id);
        const events = await s.loadEvents(id);
        expect(events).toHaveLength(1);
        expect(events[0]!.to).toBe(m!.current);
        expect(m!.context).toEqual({ to: m!.current });
        expect(m!.revision).toBe(1);
      });

      it("concurrent self-loop transitions at one revision: exactly one wins, none is lost", async () => {
        const s = await factory();
        const id = freshId("race-self");
        await s.create({ id, name: "n", initial: "a", context: { n: 0 } });

        // Every writer read revision 0 and computes its own next context.
        const writers = [1, 2, 3, 4, 5, 6];
        const results = await Promise.allSettled(
          writers.map((n) =>
            s.transition({
              id,
              from: "a",
              to: "a",
              expectedRevision: 0,
              event: "tick",
              context: { n },
            }),
          ),
        );
        const winners = writers.filter((_, i) => results[i]!.status === "fulfilled");
        expect(winners).toHaveLength(1);

        const m = await s.load(id);
        expect(m!.current).toBe("a");
        expect(m!.revision).toBe(1);
        expect(m!.context).toEqual({ n: winners[0] });
        const events = await s.loadEvents(id);
        expect(events.map((e) => e.context)).toEqual([{ n: winners[0] }]);
      });

      it("concurrent read-modify-write increments lose no update when losers retry", async () => {
        const s = await factory();
        const id = freshId("rmw");
        await s.create({ id, name: "n", initial: "a", context: { n: 0 } });

        const increment = async () => {
          for (;;) {
            const m = (await s.load(id))!;
            const n = (m.context as { n: number }).n;
            try {
              await s.transition({
                id,
                from: "a",
                to: "a",
                expectedRevision: m.revision,
                event: "inc",
                context: { n: n + 1 },
              });
              return;
            } catch {
              // Lost the compare-and-set: reload and try again.
            }
          }
        };
        await Promise.all(Array.from({ length: 8 }, increment));

        const m = await s.load(id);
        expect(m!.context).toEqual({ n: 8 });
        expect(m!.revision).toBe(8);
        expect(await s.loadEvents(id)).toHaveLength(8);
      });

      it("a self-loop transition is recorded like any other", async () => {
        const s = await factory();
        const id = freshId("self");
        await s.create({ id, name: "n", initial: "a", context: { n: 0 } });
        await s.transition({
          id,
          from: "a",
          to: "a",
          expectedRevision: 0,
          event: "tick",
          context: { n: 1 },
        });
        await s.transition({
          id,
          from: "a",
          to: "a",
          expectedRevision: 1,
          event: "tick",
          context: { n: 2 },
        });
        expect((await s.load(id))!.context).toEqual({ n: 2 });
        expect((await s.load(id))!.revision).toBe(2);
        expect((await s.loadEvents(id)).map((e) => e.context)).toEqual([{ n: 1 }, { n: 2 }]);
      });
    });

    describe("loadEvents", () => {
      it("returns events in transition order with limit / offset", async () => {
        const s = await factory();
        const id = freshId("events");
        await s.create({ id, name: "n", initial: "s0", context: {} });
        for (let i = 0; i < 5; i++) {
          await s.transition({
            id,
            from: `s${i}`,
            to: `s${i + 1}`,
            expectedRevision: i,
            event: `e${i}`,
            context: {},
          });
        }

        expect((await s.loadEvents(id)).map((e) => e.event)).toEqual([
          "e0",
          "e1",
          "e2",
          "e3",
          "e4",
        ]);
        expect((await s.loadEvents(id, { limit: 2 })).map((e) => e.event)).toEqual(["e0", "e1"]);
        expect((await s.loadEvents(id, { offset: 3 })).map((e) => e.event)).toEqual(["e3", "e4"]);
        expect((await s.loadEvents(id, { limit: 2, offset: 1 })).map((e) => e.event)).toEqual([
          "e1",
          "e2",
        ]);
      });

      it("keeps each machine's history separate", async () => {
        const s = await factory();
        const a = freshId("hist-a");
        const b = freshId("hist-b");
        await s.create({ id: a, name: "n", initial: "x", context: {} });
        await s.create({ id: b, name: "n", initial: "x", context: {} });
        await s.transition({
          id: a,
          from: "x",
          to: "y",
          expectedRevision: 0,
          event: "only-a",
          context: {},
        });

        expect((await s.loadEvents(a)).map((e) => e.event)).toEqual(["only-a"]);
        expect(await s.loadEvents(b)).toEqual([]);
        expect(await s.loadEvents(freshId("none"))).toEqual([]);
      });
    });

    describe("locks", () => {
      it("tryLock is exclusive per id until released, with a fresh token each time", async () => {
        const s = await factory();
        const a = freshId("lock-a");
        const b = freshId("lock-b");
        const first = await s.tryLock({ id: a, durationMs: 30_000 });
        expect(typeof first).toBe("string");
        expect(await s.tryLock({ id: a, durationMs: 30_000 })).toBeNull();
        const tokenB = await s.tryLock({ id: b, durationMs: 30_000 });
        expect(tokenB).not.toBeNull();

        await s.releaseLock({ id: a, token: first! });
        const second = await s.tryLock({ id: a, durationMs: 30_000 });
        expect(second).not.toBeNull();
        expect(second).not.toBe(first);
        await s.releaseLock({ id: a, token: second! });
        await s.releaseLock({ id: b, token: tokenB! });
      });

      it("releasing an unheld lock is a no-op", async () => {
        const s = await factory();
        await s.releaseLock({ id: freshId("never-locked"), token: "no-such-token" });
      });

      it("release and extend with the wrong token leave the lock alone", async () => {
        const s = await factory();
        const id = freshId("lock-wrong-token");
        const token = await s.tryLock({ id, durationMs: 30_000 });
        expect(token).not.toBeNull();

        await s.releaseLock({ id, token: "someone-else" });
        expect(await s.extendLock({ id, token: "someone-else", durationMs: 30_000 })).toBe(false);
        expect(await s.tryLock({ id, durationMs: 30_000 })).toBeNull();

        await s.releaseLock({ id, token: token! });
        expect(await s.extendLock({ id, token: token!, durationMs: 30_000 })).toBe(false);
      });

      it("an expired lock can be taken again", async () => {
        const s = await factory();
        const id = freshId("lock-expiry");
        expect(await s.tryLock({ id, durationMs: 1 })).not.toBeNull();
        const reacquired = await lockAfterExpiry({ storage: s, id });
        expect(reacquired).not.toBeNull();
        await s.releaseLock({ id, token: reacquired! });
      });

      it("extendLock keeps the lock held past its original expiry", async () => {
        const s = await factory();
        const id = freshId("lock-extend");
        const token = await s.tryLock({ id, durationMs: 100 });
        expect(token).not.toBeNull();
        expect(await s.extendLock({ id, token: token!, durationMs: 30_000 })).toBe(true);

        await new Promise((r) => setTimeout(r, 200));
        expect(await s.tryLock({ id, durationMs: 30_000 })).toBeNull();
        await s.releaseLock({ id, token: token! });
      });

      it("a holder whose lock expired and was taken over can neither extend nor release it", async () => {
        const s = await factory();
        const id = freshId("lock-stale");
        const stale = await s.tryLock({ id, durationMs: 1 });
        expect(stale).not.toBeNull();
        const current = await lockAfterExpiry({ storage: s, id });
        expect(current).not.toBeNull();

        expect(await s.extendLock({ id, token: stale!, durationMs: 30_000 })).toBe(false);
        await s.releaseLock({ id, token: stale! });
        expect(await s.tryLock({ id, durationMs: 30_000 })).toBeNull();

        expect(await s.extendLock({ id, token: current!, durationMs: 30_000 })).toBe(true);
        await s.releaseLock({ id, token: current! });
        expect(await s.tryLock({ id, durationMs: 30_000 })).not.toBeNull();
      });

      if (options.createPeer) {
        const createPeer = options.createPeer;

        it("a lock excludes other instances and only its token releases it", async () => {
          const s = await factory();
          const peer = await createPeer();
          const id = freshId("lock-peer");

          const token = await s.tryLock({ id, durationMs: 30_000 });
          expect(token).not.toBeNull();
          expect(await peer.tryLock({ id, durationMs: 30_000 })).toBeNull();

          // The peer never held the lock, so its release must not free it.
          await peer.releaseLock({ id, token: "peer-guess" });
          expect(await peer.tryLock({ id, durationMs: 30_000 })).toBeNull();

          // The token, not the instance, identifies the holder.
          expect(await peer.extendLock({ id, token: token!, durationMs: 30_000 })).toBe(true);
          await peer.releaseLock({ id, token: token! });
          const peerToken = await peer.tryLock({ id, durationMs: 30_000 });
          expect(peerToken).not.toBeNull();
          await peer.releaseLock({ id, token: peerToken! });
        });

        it("concurrent tryLock across instances admits exactly one", async () => {
          const instances = await Promise.all(
            Array.from({ length: 5 }, (_, i) => (i === 0 ? factory() : createPeer())),
          );
          const id = freshId("lock-race");
          const tokens = await Promise.all(
            instances.map((i) => i.tryLock({ id, durationMs: 30_000 })),
          );
          const winners = tokens.filter((t) => t !== null);
          expect(winners).toHaveLength(1);
          await instances[0]!.releaseLock({ id, token: winners[0]! });
        });
      }
    });

    describe("with the state machine runtime", () => {
      it("a retried onEnter does not repeat the saved transition", async () => {
        const storage = await factory();
        let enters = 0;
        const m = stateMachine<any>({ name: freshId("rt-name"), storage })
          .use(retryMiddleware({ maxRetries: 2, baseDelayMs: 1 }))
          .state("a")
          .state("b", {
            terminal: true,
            onEnter: () => {
              enters++;
              if (enters === 1) throw new Error("hook flake");
            },
          })
          .on("go", { from: "a", to: "b" })
          .initial("a")
          .build();

        const id = freshId("rt");
        await m.start({ id, context: {} });
        await m.send({ id, event: "go" });

        expect((await m.getState(id))?.current).toBe("b");
        expect(enters).toBe(2);
        expect((await m.getHistory(id)).map((e) => e.event)).toEqual(["go"]);
        // The lock was released after the send.
        const token = await storage.tryLock({ id, durationMs: 30_000 });
        expect(token).not.toBeNull();
        await storage.releaseLock({ id, token: token! });
      });

      it("sends advance the revision and maxTransitions counts it without loading history", async () => {
        const storage = await factory();
        let historyLoads = 0;
        const counted: StateMachineStorage = {
          create: (p) => storage.create(p),
          load: (id) => storage.load(id),
          transition: (p) => storage.transition(p),
          loadEvents: (id, p) => {
            historyLoads++;
            return storage.loadEvents(id, p);
          },
          tryLock: (p) => storage.tryLock(p),
          releaseLock: (p) => storage.releaseLock(p),
          extendLock: (p) => storage.extendLock(p),
        };
        const m = stateMachine<any>({
          name: freshId("rt-limit"),
          storage: counted,
          limits: { maxTransitions: 3 },
        })
          .state("a")
          .on("tick", { from: "a", to: "a", action: (c: { n: number }) => ({ n: c.n + 1 }) })
          .initial("a")
          .build();

        const id = freshId("rt-limit");
        await m.start({ id, context: { n: 0 } });
        for (let i = 0; i < 3; i++) await m.send({ id, event: "tick" });
        await expect(m.send({ id, event: "tick" })).rejects.toThrow("max transitions");

        expect(historyLoads).toBe(0);
        const state = await storage.load(id);
        expect(state!.revision).toBe(3);
        expect(state!.context).toEqual({ n: 3 });
      });
    });
  });
}
