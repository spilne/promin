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
  /**
   * Whether `transition({ eventData })` round-trips through `loadEvents`.
   * Default `true`.
   */
  persistsEventData?: boolean;
}

let seq = 0;
function freshId(label: string): string {
  seq += 1;
  return `sm-conf-${label}-${seq}-${crypto.randomUUID().slice(0, 8)}`;
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
  const persistsEventData = options.persistsEventData ?? true;

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
          event: "go",
          context: { n: 1 },
          eventData: { by: "u1" },
          metadata: { reason: "test" },
        });

        const m = await s.load(id);
        expect(m!.current).toBe("b");
        expect(m!.context).toEqual({ n: 1 });
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
        if (persistsEventData) expect(e.eventData).toEqual({ by: "u1" });
      });

      it("rejects when the machine is not in `from` and changes nothing", async () => {
        const s = await factory();
        const id = freshId("cas");
        await s.create({ id, name: "n", initial: "a", context: { n: 0 } });

        await rejects(() =>
          s.transition({ id, from: "b", to: "c", event: "go", context: { n: 9 } }),
        );

        const m = await s.load(id);
        expect(m!.current).toBe("a");
        expect(m!.context).toEqual({ n: 0 });
        expect(await s.loadEvents(id)).toEqual([]);
      });

      it("rejects for an unknown machine", async () => {
        const s = await factory();
        await rejects(() =>
          s.transition({ id: freshId("missing"), from: "a", to: "b", event: "go", context: {} }),
        );
      });

      it("concurrent transitions from the same state: exactly one wins", async () => {
        const s = await factory();
        const id = freshId("race");
        await s.create({ id, name: "n", initial: "a", context: {} });

        const results = await Promise.allSettled(
          ["b", "c", "d", "e", "f"].map((to) =>
            s.transition({ id, from: "a", to, event: `to-${to}`, context: { to } }),
          ),
        );
        const winners = results.filter((r) => r.status === "fulfilled");
        expect(winners).toHaveLength(1);

        const m = await s.load(id);
        const events = await s.loadEvents(id);
        expect(events).toHaveLength(1);
        expect(events[0]!.to).toBe(m!.current);
        expect(m!.context).toEqual({ to: m!.current });
      });

      it("a self-loop transition is recorded like any other", async () => {
        const s = await factory();
        const id = freshId("self");
        await s.create({ id, name: "n", initial: "a", context: { n: 0 } });
        await s.transition({ id, from: "a", to: "a", event: "tick", context: { n: 1 } });
        await s.transition({ id, from: "a", to: "a", event: "tick", context: { n: 2 } });
        expect((await s.load(id))!.context).toEqual({ n: 2 });
        expect((await s.loadEvents(id)).map((e) => e.context)).toEqual([{ n: 1 }, { n: 2 }]);
      });
    });

    describe("loadEvents", () => {
      it("returns events in transition order with limit / offset", async () => {
        const s = await factory();
        const id = freshId("events");
        await s.create({ id, name: "n", initial: "s0", context: {} });
        for (let i = 0; i < 5; i++) {
          await s.transition({ id, from: `s${i}`, to: `s${i + 1}`, event: `e${i}`, context: {} });
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
        await s.transition({ id: a, from: "x", to: "y", event: "only-a", context: {} });

        expect((await s.loadEvents(a)).map((e) => e.event)).toEqual(["only-a"]);
        expect(await s.loadEvents(b)).toEqual([]);
        expect(await s.loadEvents(freshId("none"))).toEqual([]);
      });
    });

    describe("locks", () => {
      it("tryLock is exclusive per id until released", async () => {
        const s = await factory();
        const a = freshId("lock-a");
        const b = freshId("lock-b");
        expect(await s.tryLock(a, 30_000)).toBe(true);
        expect(await s.tryLock(a, 30_000)).toBe(false);
        expect(await s.tryLock(b, 30_000)).toBe(true);

        await s.releaseLock(a);
        expect(await s.tryLock(a, 30_000)).toBe(true);
        await s.releaseLock(a);
        await s.releaseLock(b);
      });

      it("releasing an unheld lock is a no-op", async () => {
        const s = await factory();
        await s.releaseLock(freshId("never-locked"));
      });

      it("an expired lock can be taken again", async () => {
        const s = await factory();
        const id = freshId("lock-expiry");
        expect(await s.tryLock(id, 1)).toBe(true);
        let reacquired = false;
        for (let i = 0; i < 100 && !reacquired; i++) {
          await new Promise((r) => setTimeout(r, 10));
          reacquired = await s.tryLock(id, 30_000);
        }
        expect(reacquired).toBe(true);
        await s.releaseLock(id);
      });

      if (options.createPeer) {
        const createPeer = options.createPeer;

        it("a lock excludes other instances and only its holder releases it", async () => {
          const s = await factory();
          const peer = await createPeer();
          const id = freshId("lock-peer");

          expect(await s.tryLock(id, 30_000)).toBe(true);
          expect(await peer.tryLock(id, 30_000)).toBe(false);

          // The peer never held the lock, so its release must not free it.
          await peer.releaseLock(id);
          expect(await peer.tryLock(id, 30_000)).toBe(false);

          await s.releaseLock(id);
          expect(await peer.tryLock(id, 30_000)).toBe(true);
          await peer.releaseLock(id);
        });

        it("concurrent tryLock across instances admits exactly one", async () => {
          const instances = await Promise.all(
            Array.from({ length: 5 }, (_, i) => (i === 0 ? factory() : createPeer())),
          );
          const id = freshId("lock-race");
          const results = await Promise.all(instances.map((i) => i.tryLock(id, 30_000)));
          expect(results.filter(Boolean)).toHaveLength(1);
          await instances[results.indexOf(true)]!.releaseLock(id);
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
        expect(await storage.tryLock(id, 30_000)).toBe(true);
        await storage.releaseLock(id);
      });
    });
  });
}
