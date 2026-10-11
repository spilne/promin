// ---------------------------------------------------------------------------
// Portable NamespaceRegistry conformance suite. Every implementation
// (in-memory, SQLite, Postgres) must pass.
//
// The factory receives a `FakeWallClock`; a registry that stamps
// `createdAt` / `updatedAt` from an injected clock should use it so the
// suite can check the timestamps exactly. A registry that stamps from the
// database server's clock (Postgres `NOW()`) passes `{ serverClock: true }`,
// and the suite checks ordering only.
//
// Usage:
//   import { namespaceRegistryTestSuite } from "@promin/workflow/testing";
//   namespaceRegistryTestSuite({
//     factory: ({ clock }) => new InMemoryNamespaceRegistry({ clock }),
//   });
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "../shared/wall-clock.ts";
import {
  InvalidNamespaceError,
  NamespaceExistsError,
  NamespaceNotFoundError,
  type NamespaceCapabilities,
  type NamespaceRegistry,
} from "./namespace-registry.ts";

export interface NamespaceRegistrySuiteFactoryParams {
  /** Time source for `createdAt` / `updatedAt`. */
  readonly clock: FakeWallClock;
}

export interface NamespaceRegistryTestSuiteOptions {
  /** Called once per test; must return an empty registry. */
  readonly factory: (
    params: NamespaceRegistrySuiteFactoryParams,
  ) => NamespaceRegistry | Promise<NamespaceRegistry>;
  /** The registry stamps times with the database server's clock, not `clock`. */
  readonly serverClock?: boolean;
}

const START_MS = Date.UTC(2026, 0, 1);

export function namespaceRegistryTestSuite(options: NamespaceRegistryTestSuiteOptions): void {
  async function make(): Promise<{ registry: NamespaceRegistry; clock: FakeWallClock }> {
    const clock = FakeWallClock.create(START_MS);
    return { registry: await options.factory({ clock }), clock };
  }

  async function expectInvalid(params: {
    promise: Promise<unknown>;
    field: InvalidNamespaceError["field"];
  }): Promise<void> {
    const err = await params.promise.then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(InvalidNamespaceError);
    expect((err as InvalidNamespaceError).field).toBe(params.field);
  }

  describe("NamespaceRegistry conformance", () => {
    it("starts empty", async () => {
      const { registry } = await make();
      expect(await registry.list()).toEqual([]);
    });

    it("creates and gets a namespace", async () => {
      const { registry } = await make();
      const created = await registry.create({
        id: "acme",
        displayName: "Acme",
        description: "Production tenant",
        capabilities: { ai: { enabled: true, maxTokensPerTurn: 8192, models: ["m-1"] } },
        metadata: { owner: "ops", tags: ["a", 1, null], nested: { deep: true } },
      });
      expect(created.id).toBe("acme");
      expect(created.status).toBe("active");
      expect(created.createdAt).toBeInstanceOf(Date);
      expect(created.updatedAt.getTime()).toBe(created.createdAt.getTime());

      const got = await registry.get("acme");
      expect(got).toEqual(created);
      expect(got?.displayName).toBe("Acme");
      expect(got?.description).toBe("Production tenant");
      expect(got?.capabilities).toEqual({
        ai: { enabled: true, maxTokensPerTurn: 8192, models: ["m-1"] },
      });
      expect(got?.metadata).toEqual({ owner: "ops", tags: ["a", 1, null], nested: { deep: true } });
    });

    it("defaults displayName to the id, description to null, policy and metadata to {}", async () => {
      const { registry } = await make();
      const created = await registry.create({ id: "bare", displayName: "   " });
      expect(created).toMatchObject({
        id: "bare",
        displayName: "bare",
        description: null,
        capabilities: {},
        metadata: {},
      });
      expect(await registry.get("bare")).toEqual(created);
    });

    it("stamps createdAt from the clock", async () => {
      if (options.serverClock) return;
      const { registry } = await make();
      const created = await registry.create({ id: "acme" });
      expect(created.createdAt.getTime()).toBe(START_MS);
    });

    it("trims displayName", async () => {
      const { registry } = await make();
      const created = await registry.create({ id: "acme", displayName: "  Acme Corp  " });
      expect(created.displayName).toBe("Acme Corp");
    });

    it("get returns null for unknown and malformed ids", async () => {
      const { registry } = await make();
      expect(await registry.get("missing")).toBeNull();
      expect(await registry.get("not a namespace")).toBeNull();
      expect(await registry.get("")).toBeNull();
    });

    it("rejects malformed ids", async () => {
      const { registry } = await make();
      for (const id of ["", " acme", "acme ", "-acme", "a/b", "a b", "é", "x".repeat(129)]) {
        await expectInvalid({ promise: registry.create({ id }), field: "id" });
      }
      expect(await registry.list()).toEqual([]);
    });

    it("accepts the full id alphabet", async () => {
      const { registry } = await make();
      for (const id of ["a", "0", "Acme_2.prod:eu-west-1", "x".repeat(128)]) {
        await registry.create({ id });
      }
      expect((await registry.list()).length).toBe(4);
    });

    it("rejects malformed capability policy", async () => {
      const { registry } = await make();
      const bad: unknown[] = [
        { ai: { maxTokensPerTurn: "lots" } },
        { ai: { maxTokensPerTurn: 0 } },
        { ai: { maxTokensPerTurn: 1.5 } },
        { ai: { models: [""] } },
        { ai: { models: "m-1" } },
        { tools: { shell: "yes" } },
        { workflows: 5 },
        { billing: { plan: "pro" } },
        { ai: { maxTokens: 10 } },
        [],
      ];
      for (const capabilities of bad) {
        await expectInvalid({
          promise: registry.create({
            id: "acme",
            capabilities: capabilities as NamespaceCapabilities,
          }),
          field: "capabilities",
        });
      }
      expect(await registry.get("acme")).toBeNull();
    });

    it("rejects metadata JSON cannot carry", async () => {
      const { registry } = await make();
      for (const metadata of [
        { n: Number.NaN },
        { n: Number.POSITIVE_INFINITY },
        { f: () => 1 },
        { d: new Date(0) },
        { big: 1n },
      ]) {
        await expectInvalid({
          promise: registry.create({ id: "acme", metadata: metadata as Record<string, unknown> }),
          field: "metadata",
        });
      }
    });

    it("rejects oversized labels", async () => {
      const { registry } = await make();
      await expectInvalid({
        promise: registry.create({ id: "acme", displayName: "x".repeat(129) }),
        field: "displayName",
      });
      await expectInvalid({
        promise: registry.create({ id: "acme", description: "x".repeat(1025) }),
        field: "description",
      });
    });

    it("rejects a duplicate id with NamespaceExistsError", async () => {
      const { registry } = await make();
      await registry.create({ id: "acme", displayName: "First" });
      const err = await registry.create({ id: "acme", displayName: "Second" }).catch((e) => e);
      expect(err).toBeInstanceOf(NamespaceExistsError);
      expect((err as NamespaceExistsError).namespaceId).toBe("acme");
      expect((await registry.get("acme"))?.displayName).toBe("First");
    });

    it("ids are case-sensitive", async () => {
      const { registry } = await make();
      await registry.create({ id: "acme" });
      await registry.create({ id: "ACME" });
      expect((await registry.list()).map((ns) => ns.id)).toEqual(["ACME", "acme"]);
    });

    it("of concurrent creates of one id, exactly one succeeds", async () => {
      const { registry } = await make();
      const results = await Promise.allSettled(
        Array.from({ length: 8 }, (_, i) =>
          registry.create({ id: "race", displayName: `writer-${i}` }),
        ),
      );
      const ok = results.filter((r) => r.status === "fulfilled");
      const failed = results.filter((r) => r.status === "rejected");
      expect(ok.length).toBe(1);
      for (const r of failed) {
        expect((r as PromiseRejectedResult).reason).toBeInstanceOf(NamespaceExistsError);
      }
      const winner = (ok[0] as PromiseFulfilledResult<{ displayName: string }>).value;
      expect((await registry.get("race"))?.displayName).toBe(winner.displayName);
    });

    it("lists by id and filters by status", async () => {
      const { registry } = await make();
      await registry.create({ id: "zeta", displayName: "A first by label" });
      await registry.create({ id: "alpha" });
      await registry.create({ id: "old" });
      await registry.update({ id: "old", patch: { status: "archived" } });

      expect((await registry.list()).map((ns) => ns.id)).toEqual(["alpha", "old", "zeta"]);
      expect((await registry.list({ status: "active" })).map((ns) => ns.id)).toEqual([
        "alpha",
        "zeta",
      ]);
      expect((await registry.list({ status: "archived" })).map((ns) => ns.id)).toEqual(["old"]);
    });

    it("updates the patched fields and keeps the rest", async () => {
      const { registry, clock } = await make();
      const created = await registry.create({
        id: "acme",
        displayName: "Acme",
        description: "Tenant",
        capabilities: { ai: { enabled: true } },
        metadata: { region: "us" },
      });
      clock.advance(1_000);

      const renamed = await registry.update({ id: "acme", patch: { displayName: "Acme Corp" } });
      expect(renamed.displayName).toBe("Acme Corp");
      expect(renamed.description).toBe("Tenant");
      expect(renamed.capabilities).toEqual({ ai: { enabled: true } });
      expect(renamed.metadata).toEqual({ region: "us" });
      expect(renamed.createdAt.getTime()).toBe(created.createdAt.getTime());
      expect(renamed.updatedAt.getTime()).toBeGreaterThanOrEqual(created.updatedAt.getTime());
      if (!options.serverClock) expect(renamed.updatedAt.getTime()).toBe(START_MS + 1_000);

      const replaced = await registry.update({
        id: "acme",
        patch: {
          description: null,
          capabilities: { workflows: { maxConcurrentRuns: 5 } },
          metadata: { region: "ca" },
        },
      });
      expect(replaced.displayName).toBe("Acme Corp");
      expect(replaced.description).toBeNull();
      expect(replaced.capabilities).toEqual({ workflows: { maxConcurrentRuns: 5 } });
      expect(replaced.metadata).toEqual({ region: "ca" });
      expect(await registry.get("acme")).toEqual(replaced);
    });

    it("archives and reactivates", async () => {
      const { registry } = await make();
      await registry.create({ id: "acme" });
      expect((await registry.update({ id: "acme", patch: { status: "archived" } })).status).toBe(
        "archived",
      );
      expect((await registry.get("acme"))?.status).toBe("archived");
      expect((await registry.update({ id: "acme", patch: { status: "active" } })).status).toBe(
        "active",
      );
    });

    it("a blank displayName patch resets the label to the id", async () => {
      const { registry } = await make();
      await registry.create({ id: "acme", displayName: "Acme" });
      expect((await registry.update({ id: "acme", patch: { displayName: " " } })).displayName).toBe(
        "acme",
      );
    });

    it("update of an unknown namespace throws NamespaceNotFoundError", async () => {
      const { registry } = await make();
      const err = await registry
        .update({ id: "missing", patch: { displayName: "x" } })
        .catch((e) => e);
      expect(err).toBeInstanceOf(NamespaceNotFoundError);
      expect((err as NamespaceNotFoundError).namespaceId).toBe("missing");
      expect(await registry.get("missing")).toBeNull();
    });

    it("rejects malformed patches without changing the row", async () => {
      const { registry } = await make();
      const created = await registry.create({
        id: "acme",
        capabilities: { ai: { enabled: true } },
      });
      await expectInvalid({
        promise: registry.update({ id: "acme", patch: { status: "deleted" as never } }),
        field: "status",
      });
      await expectInvalid({
        promise: registry.update({
          id: "acme",
          patch: { capabilities: { ai: { enabled: "yes" } } as never },
        }),
        field: "capabilities",
      });
      expect(await registry.get("acme")).toEqual(created);
    });

    it("rejects an unknown status filter", async () => {
      const { registry } = await make();
      await expectInvalid({
        promise: registry.list({ status: "deleted" as never }),
        field: "status",
      });
    });

    it("concurrent updates of different fields both land", async () => {
      const { registry } = await make();
      await registry.create({ id: "acme" });
      await Promise.all([
        registry.update({ id: "acme", patch: { displayName: "Acme" } }),
        registry.update({ id: "acme", patch: { metadata: { region: "eu" } } }),
        registry.update({ id: "acme", patch: { capabilities: { tools: { shell: false } } } }),
        registry.update({ id: "acme", patch: { description: "Tenant" } }),
      ]);
      const got = await registry.get("acme");
      expect(got?.displayName).toBe("Acme");
      expect(got?.metadata).toEqual({ region: "eu" });
      expect(got?.capabilities).toEqual({ tools: { shell: false } });
      expect(got?.description).toBe("Tenant");
    });

    it("deletes a namespace; the id can be reused", async () => {
      const { registry } = await make();
      await registry.create({ id: "acme", displayName: "Old" });
      expect(await registry.delete("acme")).toBe(true);
      expect(await registry.get("acme")).toBeNull();
      expect(await registry.delete("acme")).toBe(false);
      expect(await registry.delete("not a namespace")).toBe(false);
      const again = await registry.create({ id: "acme" });
      expect(again.displayName).toBe("acme");
    });

    it("returned records are copies", async () => {
      const { registry } = await make();
      const created = await registry.create({ id: "acme", metadata: { list: [1] } });
      (created.metadata.list as number[]).push(2);
      const got = await registry.get("acme");
      expect(got?.metadata).toEqual({ list: [1] });
    });
  });
}
