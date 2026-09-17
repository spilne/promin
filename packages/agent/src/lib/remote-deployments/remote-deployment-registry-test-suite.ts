// ---------------------------------------------------------------------------
// Conformance suite for `RemoteDeploymentRegistry`. Every implementation
// (in-memory, future Sqlite/Postgres) must pass.
//
// Tests use a closure-driven clock so we can fast-forward across the
// TTL window without sleeping.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import type { RemoteDeploymentRegistry } from "./types.ts";

export interface RemoteDeploymentRegistryFactory {
  /** Build a fresh registry with `getNow()` as the time source. */
  (getNow: () => number): RemoteDeploymentRegistry | Promise<RemoteDeploymentRegistry>;
}

export function remoteDeploymentRegistryTestSuite(factory: RemoteDeploymentRegistryFactory) {
  async function make(): Promise<{
    registry: RemoteDeploymentRegistry;
    advance: (ms: number) => void;
    now: () => number;
  }> {
    let nowMs = 1_700_000_000_000;
    const getNow = () => nowMs;
    const registry = await factory(getNow);
    return {
      registry,
      advance: (ms) => {
        nowMs += ms;
      },
      now: getNow,
    };
  }

  describe("RemoteDeploymentRegistry conformance", () => {
    describe("register", () => {
      it("assigns a fresh deploymentId and stamps registeredAt + lastHeartbeat", async () => {
        const { registry, now } = await make();
        const r = await registry.register({
          endpoint: "https://server-b",
          agents: ["claude-bot", "analyst-bot"],
        });
        expect(r.deploymentId).toBeTruthy();
        expect(r.endpoint).toBe("https://server-b");
        expect(r.agents).toEqual(["claude-bot", "analyst-bot"]);
        expect(r.registeredAt).toBe(now());
        expect(r.lastHeartbeat).toBe(now());
        expect(r.ttlMs).toBeGreaterThan(0);
      });

      it("returns distinct ids for two register calls with the same endpoint", async () => {
        const { registry } = await make();
        const a = await registry.register({ endpoint: "https://x", agents: ["a"] });
        const b = await registry.register({ endpoint: "https://x", agents: ["a"] });
        expect(a.deploymentId).not.toBe(b.deploymentId);
      });

      it("preserves auth when provided", async () => {
        const { registry } = await make();
        const r = await registry.register({
          endpoint: "https://x",
          agents: ["a"],
          auth: { kind: "bearer", token: "tok" },
        });
        expect(r.auth?.token).toBe("tok");
      });

      it("uses caller-supplied ttlMs when set", async () => {
        const { registry } = await make();
        const r = await registry.register({ endpoint: "https://x", agents: ["a"], ttlMs: 5_000 });
        expect(r.ttlMs).toBe(5_000);
      });
    });

    describe("heartbeat", () => {
      it("extends lastHeartbeat to current time", async () => {
        const { registry, advance, now } = await make();
        const r = await registry.register({ endpoint: "https://x", agents: ["a"] });
        const initial = r.lastHeartbeat;
        advance(15_000);
        const updated = await registry.heartbeat(r.deploymentId);
        expect(updated).not.toBeNull();
        expect(updated?.lastHeartbeat).toBe(now());
        expect(updated?.lastHeartbeat).toBeGreaterThan(initial);
      });

      it("returns null for an unknown deploymentId", async () => {
        const { registry } = await make();
        const out = await registry.heartbeat("00000000-0000-0000-0000-000000000000");
        expect(out).toBeNull();
      });
    });

    describe("unregister", () => {
      it("removes the row and returns it", async () => {
        const { registry } = await make();
        const r = await registry.register({ endpoint: "https://x", agents: ["a"] });
        const removed = await registry.unregister(r.deploymentId);
        expect(removed?.deploymentId).toBe(r.deploymentId);
        expect(await registry.get(r.deploymentId)).toBeNull();
      });

      it("returns null when called on an unknown id", async () => {
        const { registry } = await make();
        expect(await registry.unregister("missing")).toBeNull();
      });
    });

    describe("list", () => {
      it("returns all registered deployments", async () => {
        const { registry } = await make();
        await registry.register({ endpoint: "https://a", agents: ["x"] });
        await registry.register({ endpoint: "https://b", agents: ["y"] });
        const all = await registry.list();
        expect(all.map((d) => d.endpoint).sort()).toEqual(["https://a", "https://b"]);
      });

      it("returns empty array when no deployments", async () => {
        const { registry } = await make();
        expect(await registry.list()).toEqual([]);
      });
    });

    describe("expireStale", () => {
      it("removes rows whose TTL has passed", async () => {
        const { registry, advance, now } = await make();
        const r = await registry.register({
          endpoint: "https://x",
          agents: ["a"],
          ttlMs: 10_000,
        });
        advance(15_000);
        const expired = await registry.expireStale({ now: now() });
        expect(expired.map((d) => d.deploymentId)).toEqual([r.deploymentId]);
        expect(await registry.list()).toEqual([]);
      });

      it("keeps rows whose TTL is in the future", async () => {
        const { registry, advance, now } = await make();
        const r = await registry.register({
          endpoint: "https://x",
          agents: ["a"],
          ttlMs: 60_000,
        });
        advance(30_000);
        const expired = await registry.expireStale({ now: now() });
        expect(expired).toEqual([]);
        expect(await registry.get(r.deploymentId)).not.toBeNull();
      });

      it("heartbeat resets the expiry window", async () => {
        const { registry, advance, now } = await make();
        const r = await registry.register({
          endpoint: "https://x",
          agents: ["a"],
          ttlMs: 10_000,
        });
        advance(8_000);
        await registry.heartbeat(r.deploymentId);
        advance(8_000); // total 16s, but heartbeat at 8s, so 8s since last heartbeat
        const expired = await registry.expireStale({ now: now() });
        expect(expired).toEqual([]);
      });

      it("is idempotent — running twice doesn't double-report", async () => {
        const { registry, advance, now } = await make();
        await registry.register({ endpoint: "https://x", agents: ["a"], ttlMs: 1_000 });
        advance(5_000);
        const first = await registry.expireStale({ now: now() });
        expect(first).toHaveLength(1);
        const second = await registry.expireStale({ now: now() });
        expect(second).toHaveLength(0);
      });
    });
  });
}
