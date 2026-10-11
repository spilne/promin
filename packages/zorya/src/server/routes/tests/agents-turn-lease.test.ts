// ---------------------------------------------------------------------------
// Turn-gate lease lifecycle on the thread routes: the lease is released
// when opening the thread or starting the stream throws, when the stream
// finishes, and when the request is refused; it is extended on the gateway
// clock while a streamed turn runs; approve takes the lease too.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { bootAgents, req } from "./agent-gateway-harness.ts";

const body = { task: "hi", namespaceId: "ns", resourceId: "alice" };
const key = { namespaceId: "ns", threadId: "t1" };

async function drain(res: Response): Promise<string> {
  return await res.text();
}

async function waitFor(cond: () => Promise<boolean>): Promise<boolean> {
  for (let i = 0; i < 200; i++) {
    if (await cond()) return true;
    await new Promise((r) => setTimeout(r, 0));
  }
  return false;
}

describe("turn lease — release on failure", () => {
  it("a thread() that throws after the lease is taken releases it (no lingering 409)", async () => {
    let broken = true;
    const { server, leaseStore } = await bootAgents({
      turnGate: true,
      wrapAgent: (agent) => ({
        withScope(scope: unknown) {
          const scoped = (agent as { withScope(s: unknown): object }).withScope(scope);
          return new Proxy(scoped, {
            get(target, prop, receiver) {
              if (prop === "thread" && broken) {
                return async () => {
                  throw new Error("store down");
                };
              }
              const v = Reflect.get(target, prop, receiver);
              return typeof v === "function" ? v.bind(target) : v;
            },
          });
        },
      }),
    });
    for (const path of [
      "/api/agents/support/threads/t1/stream",
      "/api/agents/support/threads/t1",
    ]) {
      const res = await server.handle(req("POST", path, body));
      expect([path, res.status]).toEqual([path, 500]);
      expect(await leaseStore.get(key)).toBeNull();
    }
    broken = false;
    const ok = await server.handle(req("POST", "/api/agents/support/threads/t1", body));
    expect(ok.status).toBe(200);
  });

  it("a refused open (foreign thread) releases the lease", async () => {
    const { server, leaseStore } = await bootAgents({ turnGate: true });
    expect((await server.handle(req("POST", "/api/agents/support/threads/t1", body))).status).toBe(
      200,
    );
    const bob = await server.handle(
      req("POST", "/api/agents/support/threads/t1/stream", { ...body, resourceId: "bob" }),
    );
    expect(bob.status).toBe(404);
    expect(await leaseStore.get(key)).toBeNull();
  });
});

describe("turn lease — streaming", () => {
  it("is released when the stream finishes", async () => {
    const { server, leaseStore } = await bootAgents({ turnGate: true, reply: () => "streamed" });
    const res = await server.handle(req("POST", "/api/agents/support/threads/t1/stream", body));
    expect(res.status).toBe(200);
    const text = await drain(res);
    expect(text).toContain("event: finish");
    expect(await waitFor(async () => (await leaseStore.get(key)) === null)).toBe(true);
    expect((await server.handle(req("POST", "/api/agents/support/threads/t1", body))).status).toBe(
      200,
    );
  });

  it("extends the lease on the gateway clock while a streamed turn is open", async () => {
    let release!: () => void;
    const hold = new Promise<void>((r) => (release = r));
    const { server, leaseStore, clock } = await bootAgents({
      turnGate: true,
      wrapAgent: (agent) => ({
        withScope(scope: unknown) {
          const scoped = (agent as { withScope(s: unknown): object }).withScope(scope);
          return new Proxy(scoped, {
            get(target, prop, receiver) {
              if (prop === "thread") {
                return async (...args: unknown[]) => {
                  const thread = await (
                    target as { thread(...a: unknown[]): Promise<object> }
                  ).thread(...args);
                  return new Proxy(thread, {
                    get(t, p, r) {
                      if (p === "stream") {
                        return (...a: unknown[]) => {
                          const out = (
                            t as { stream(...x: unknown[]): Record<string, unknown> }
                          ).stream(...a);
                          const text = hold.then(() => out.text as Promise<string>);
                          return {
                            ...out,
                            text,
                            finishReason: hold.then(() => out.finishReason),
                            usage: hold.then(() => out.usage),
                            fullStream: (async function* () {
                              await hold;
                              yield* out.fullStream as AsyncIterable<unknown>;
                            })(),
                          };
                        };
                      }
                      const v = Reflect.get(t, p, r);
                      return typeof v === "function" ? v.bind(t) : v;
                    },
                  });
                };
              }
              const v = Reflect.get(target, prop, receiver);
              return typeof v === "function" ? v.bind(target) : v;
            },
          });
        },
      }),
    });
    const res = await server.handle(req("POST", "/api/agents/support/threads/t1/stream", body));
    expect(res.status).toBe(200);
    const reading = drain(res);
    const first = (await leaseStore.get(key))!;
    expect(first).not.toBeNull();
    // Well past the 60s TTL: the extension timer keeps it alive.
    for (let i = 0; i < 6; i++) {
      clock.advance(30_000);
      await new Promise((r) => setTimeout(r, 0));
    }
    const held = (await leaseStore.get(key))!;
    expect(held.leaseId).toBe(first.leaseId);
    expect(held.expiresAt).toBeGreaterThan(clock.currentTimeMs());
    const blocked = await server.handle(req("POST", "/api/agents/support/threads/t1", body));
    expect(blocked.status).toBe(409);

    release();
    expect(await reading).toContain("event: finish");
    expect(await waitFor(async () => (await leaseStore.get(key)) === null)).toBe(true);
    // The extension timer is gone with the lease.
    expect(clock.pendingCount()).toBe(0);
  });

  it("approve is gated by the same lease", async () => {
    const { server, leaseStore } = await bootAgents({ turnGate: true });
    expect((await server.handle(req("POST", "/api/agents/support/threads/t1", body))).status).toBe(
      200,
    );
    await leaseStore.acquire({ key, ownerId: "elsewhere", ttlMs: 60_000 });
    const res = await server.handle(
      req("POST", "/api/agents/support/threads/t1/approve", {
        ...body,
        toolCallId: "c1",
        approved: true,
      }),
    );
    expect(res.status).toBe(409);
  });
});
