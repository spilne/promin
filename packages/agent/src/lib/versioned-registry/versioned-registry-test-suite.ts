// ---------------------------------------------------------------------------
// Conformance suite for `VersionedRegistry`. Every backend runs it, and it
// runs the shared semantics once per kind (agent, role, skill, DAG,
// fragment), then each kind's own normalisation and filters.
//
// Usage:
//   import { versionedRegistryTestSuite } from "@promin/agent/testing";
//   versionedRegistryTestSuite({
//     name: "SqliteVersionedRegistry",
//     factory: ({ kind, clock }) => new SqliteVersionedRegistry({ db, kind, clock }),
//   });
//
// The factory is called once per test and must return an empty registry.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock, type WallClock } from "@promin/workflow";
import { DagValidationError, type DagFilter, type DagRecipe } from "../dag/types.ts";
import type { FragmentFilter, FragmentRecipe } from "../fragments/types.ts";
import type { AgentFilter, AgentRecipe, AgentRecipeInput } from "../registry/types.ts";
import type { RoleFilter, RoleRecipe, RoleRecipeInput } from "../role/types.ts";
import type { SkillFilter, SkillRecipe, SkillRecipeInput } from "../skills/types.ts";
import { AGENT_KIND, DAG_KIND, FRAGMENT_KIND, ROLE_KIND, SKILL_KIND } from "./kinds.ts";
import {
  isRegistryConflict,
  isRegistryInputError,
  isRegistryNotFound,
  type RegistryKind,
  type VersionedRegistry,
} from "./types.ts";

/** Builds an empty registry of `kind` whose timestamps come from `clock`. */
export interface VersionedRegistryFactory {
  <TBody, TFilter, TInput>(params: {
    readonly kind: RegistryKind<TBody, TFilter, TInput>;
    readonly clock: WallClock;
  }):
    | VersionedRegistry<TBody, TFilter, TInput>
    | Promise<VersionedRegistry<TBody, TFilter, TInput>>;
}

/** How a kind's conformance run builds bodies and filters. */
export interface VersionedRegistryKindFixture<TBody, TFilter, TInput> {
  readonly kind: RegistryKind<TBody, TFilter, TInput>;
  /** A valid input body; distinct `n` give distinct bodies. */
  readonly sample: (n: number) => TInput;
  /** A body `filter` matches, and a filter `sample(n)` never matches. */
  readonly filtering?: { readonly matching: (n: number) => TInput; readonly filter: TFilter };
}

const START_MS = 1_700_000_000_000;

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
  } catch (err) {
    return err;
  }
  throw new Error("expected the call to reject");
}

function sharedSemantics<TBody, TFilter, TInput>(params: {
  readonly factory: VersionedRegistryFactory;
  readonly fixture: VersionedRegistryKindFixture<TBody, TFilter, TInput>;
}): void {
  const { factory, fixture } = params;
  const { kind, sample } = fixture;
  async function make(): Promise<{
    registry: VersionedRegistry<TBody, TFilter, TInput>;
    clock: FakeWallClock;
  }> {
    const clock = FakeWallClock.create(START_MS);
    return { registry: await factory({ kind, clock }), clock };
  }

  describe(`${kind.name}: versions`, () => {
    it("the first version of an id is v1, active, enabled, trusted and clock-stamped", async () => {
      const { registry } = await make();
      const entry = await registry.register({ id: "a", ...sample(1) });
      expect(entry).toMatchObject({
        id: "a",
        version: "v1",
        active: true,
        enabled: true,
        trust: "trusted",
        createdAt: START_MS,
      });
      expect(entry.source).toBeUndefined();
      expect(await registry.get({ id: "a" })).toEqual(entry);
    });

    it("a new body becomes the next version as a draft; the active one stays", async () => {
      const { registry, clock } = await make();
      await registry.register({ id: "a", ...sample(1) });
      clock.advance(10);
      const v2 = await registry.register({ id: "a", ...sample(2) });
      expect(v2).toMatchObject({ version: "v2", active: false, createdAt: START_MS + 10 });
      expect((await registry.get({ id: "a" }))?.version).toBe("v1");
      expect((await registry.get({ id: "a", version: "v2" }))?.version).toBe("v2");
    });

    it("register({ promote: true }) makes the new version active", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      const v2 = await registry.register({ id: "a", ...sample(2), promote: true });
      expect(v2.active).toBe(true);
      expect((await registry.get({ id: "a" }))?.version).toBe("v2");
      expect((await registry.get({ id: "a", version: "v1" }))?.active).toBe(false);
    });

    it("registering a body that is already stored reuses its version", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", ...sample(2), promote: true });
      const again = await registry.register({ id: "a", ...sample(2) });
      expect(again.version).toBe("v2");
      const back = await registry.register({ id: "a", ...sample(1), promote: true });
      expect(back.version).toBe("v1");
      expect(back.active).toBe(true);
      expect((await registry.versions({ id: "a" })).map((v) => v.version)).toEqual(["v1", "v2"]);
    });

    it("an explicit version is immutable: same body is a no-op, another body conflicts", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", version: "release-1", ...sample(1) });
      const same = await registry.register({ id: "a", version: "release-1", ...sample(1) });
      expect(same.version).toBe("release-1");
      const err = await rejection(
        registry.register({ id: "a", version: "release-1", ...sample(2) }),
      );
      expect(isRegistryConflict(err) && err.code).toBe("version_conflict");
      const versions = await registry.versions({ id: "a" });
      expect(versions).toHaveLength(1);
      expect(versions[0]).toEqual(same);
    });

    it("minted labels skip versions taken by explicit labels", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", version: "v2", ...sample(1) });
      const next = await registry.register({ id: "a", ...sample(2) });
      expect(next.version).toBe("v3");
    });

    it("rejects malformed version labels", async () => {
      const { registry } = await make();
      for (const version of ["", "has space", 'quote"', "@v1", "a".repeat(65)]) {
        expect(
          isRegistryInputError(
            await rejection(registry.register({ id: "a", version, ...sample(1) })),
          ),
        ).toBe(true);
      }
      expect(await registry.versions({ id: "a" })).toEqual([]);
    });

    it("returned entries are copies: mutating one leaves the store alone", async () => {
      const { registry } = await make();
      const entry = await registry.register({ id: "a", ...sample(1) });
      const before = JSON.stringify(await registry.get({ id: "a" }));
      (entry as { version: string }).version = "mutated";
      expect(JSON.stringify(await registry.get({ id: "a" }))).toBe(before);
    });

    it("versions() lists every version oldest first and flags the active one", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", ...sample(2) });
      await registry.register({ id: "a", ...sample(3) });
      await registry.promote({ id: "a", version: "v2" });
      const versions = await registry.versions({ id: "a" });
      expect(versions.map((v) => [v.version, v.active])).toEqual([
        ["v1", false],
        ["v2", true],
        ["v3", false],
      ]);
      expect(await registry.versions({ id: "unknown" })).toEqual([]);
      expect(await registry.get({ id: "unknown" })).toBeNull();
      expect(await registry.get({ id: "a", version: "v9" })).toBeNull();
    });

    it("records source and trust of new versions", async () => {
      const { registry } = await make();
      const scanned = await registry.register({
        id: "a",
        ...sample(1),
        source: "file",
        trust: "needs-review",
      });
      expect(scanned).toMatchObject({ source: "file", trust: "needs-review" });
      expect(await registry.get({ id: "a" })).toMatchObject({
        source: "file",
        trust: "needs-review",
      });
    });
  });

  describe(`${kind.name}: compare-and-set`, () => {
    it("expectedActive: null creates only a new id", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1), expectedActive: null });
      const err = await rejection(
        registry.register({ id: "a", ...sample(2), expectedActive: null }),
      );
      expect(isRegistryConflict(err) && err.code).toBe("active_mismatch");
      expect(await registry.versions({ id: "a" })).toHaveLength(1);
    });

    it("a stale expectedActive writes nothing; the current one applies", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", ...sample(2), promote: true });
      const err = await rejection(
        registry.register({ id: "a", ...sample(3), expectedActive: "v1", promote: true }),
      );
      expect(isRegistryConflict(err)).toBe(true);
      if (isRegistryConflict(err)) expect(err.actual).toBe("v2");
      expect(await registry.versions({ id: "a" })).toHaveLength(2);
      const ok = await registry.register({
        id: "a",
        ...sample(3),
        expectedActive: "v2",
        promote: true,
      });
      expect(ok).toMatchObject({ version: "v3", active: true });
    });

    it("promote moves the active pointer, guarded by expectedActive", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", ...sample(2) });
      expect(
        isRegistryConflict(
          await rejection(registry.promote({ id: "a", version: "v2", expectedActive: "v2" })),
        ),
      ).toBe(true);
      expect((await registry.get({ id: "a" }))?.version).toBe("v1");
      const promoted = await registry.promote({ id: "a", version: "v2", expectedActive: "v1" });
      expect(promoted).toMatchObject({ version: "v2", active: true });
      expect((await registry.get({ id: "a" }))?.version).toBe("v2");
    });

    it("promote of an unknown version or id is not found", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      expect(
        isRegistryNotFound(await rejection(registry.promote({ id: "a", version: "v7" }))),
      ).toBe(true);
      expect(
        isRegistryNotFound(await rejection(registry.promote({ id: "b", version: "v1" }))),
      ).toBe(true);
    });

    it("concurrent promotes from the same read: exactly one wins", async () => {
      const { registry } = await make();
      for (let n = 1; n <= 4; n++) await registry.register({ id: "a", ...sample(n) });
      const results = await Promise.allSettled(
        ["v2", "v3", "v4"].map((version) =>
          registry.promote({ id: "a", version, expectedActive: "v1" }),
        ),
      );
      const won = results.filter((r) => r.status === "fulfilled");
      expect(won).toHaveLength(1);
      const lost = results.filter((r) => r.status === "rejected");
      for (const r of lost) expect(isRegistryConflict(r.reason)).toBe(true);
    });

    it("concurrent registers of distinct bodies mint distinct versions", async () => {
      const { registry } = await make();
      const entries = await Promise.all(
        [1, 2, 3, 4, 5, 6].map((n) => registry.register({ id: "a", ...sample(n) })),
      );
      const labels = entries.map((e) => e.version).sort();
      expect(new Set(labels).size).toBe(6);
      expect((await registry.versions({ id: "a" })).map((v) => v.version).sort()).toEqual(labels);
      expect((await registry.versions({ id: "a" })).filter((v) => v.active)).toHaveLength(1);
    });

    it("concurrent registers of one body store a single version", async () => {
      const { registry } = await make();
      const entries = await Promise.all(
        [1, 2, 3, 4].map(() => registry.register({ id: "a", ...sample(1) })),
      );
      expect(new Set(entries.map((e) => e.version))).toEqual(new Set(["v1"]));
      expect(await registry.versions({ id: "a" })).toHaveLength(1);
    });
  });

  describe(`${kind.name}: enabled and trust`, () => {
    it("enabled is per id: it survives new versions and promotes", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      const off = await registry.setEnabled({ id: "a", enabled: false });
      expect(off).toMatchObject({ version: "v1", enabled: false });
      await registry.register({ id: "a", ...sample(2), promote: true });
      expect((await registry.get({ id: "a" }))?.enabled).toBe(false);
      expect((await registry.get({ id: "a", version: "v1" }))?.enabled).toBe(false);
      await registry.setEnabled({ id: "a", enabled: true, expectedActive: "v2" });
      expect((await registry.get({ id: "a" }))?.enabled).toBe(true);
      expect(
        isRegistryConflict(
          await rejection(registry.setEnabled({ id: "a", enabled: false, expectedActive: "v1" })),
        ),
      ).toBe(true);
      expect(
        isRegistryNotFound(await rejection(registry.setEnabled({ id: "b", enabled: false }))),
      ).toBe(true);
    });

    it("register({ enabled }) sets the switch with the write; without it the switch stays", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1), enabled: false });
      expect((await registry.get({ id: "a" }))?.enabled).toBe(false);
      await registry.register({ id: "a", ...sample(2), promote: true });
      expect((await registry.get({ id: "a" }))?.enabled).toBe(false);
      // A stored body with a switch change writes only the switch.
      const on = await registry.register({ id: "a", ...sample(2), enabled: true });
      expect(on).toMatchObject({ version: "v2", enabled: true });
      expect(await registry.versions({ id: "a" })).toHaveLength(2);
    });

    it("trust is per version", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1), trust: "needs-review" });
      await registry.register({ id: "a", ...sample(2), trust: "needs-review" });
      const reviewed = await registry.setTrust({ id: "a", version: "v2", trust: "trusted" });
      expect(reviewed).toMatchObject({ version: "v2", trust: "trusted" });
      expect((await registry.get({ id: "a", version: "v1" }))?.trust).toBe("needs-review");
      // Re-registering a stored body keeps its review state.
      expect(
        (await registry.register({ id: "a", ...sample(2), trust: "needs-review" })).trust,
      ).toBe("trusted");
      expect(
        isRegistryNotFound(
          await rejection(registry.setTrust({ id: "a", version: "v9", trust: "trusted" })),
        ),
      ).toBe(true);
    });
  });

  describe(`${kind.name}: unregister`, () => {
    it("removing the active version falls back to the previously active one, not the latest draft", async () => {
      const { registry } = await make();
      // The clock never advances: every version shares createdAt.
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", ...sample(2), promote: true });
      await registry.register({ id: "a", ...sample(3), trust: "needs-review" }); // a draft
      expect(await registry.unregister({ id: "a", version: "v2" })).toBe(true);
      expect(await registry.get({ id: "a" })).toMatchObject({ version: "v1", trust: "trusted" });
      // No earlier active version left: the latest live one.
      await registry.unregister({ id: "a", version: "v1" });
      expect((await registry.get({ id: "a" }))?.version).toBe("v3");
      await registry.unregister({ id: "a", version: "v3" });
      expect(await registry.get({ id: "a" })).toBeNull();
      expect(await registry.versions({ id: "a" })).toEqual([]);
    });

    it("unregister({ promote }) names the replacement of the active version, atomically", async () => {
      const { registry } = await make();
      for (let n = 1; n <= 3; n++) await registry.register({ id: "a", ...sample(n) });
      await registry.promote({ id: "a", version: "v2" });
      expect(
        isRegistryNotFound(
          await rejection(registry.unregister({ id: "a", version: "v2", promote: "v9" })),
        ),
      ).toBe(true);
      expect((await registry.get({ id: "a" }))?.version).toBe("v2"); // nothing removed
      await registry.unregister({ id: "a", version: "v2", promote: "v3", expectedActive: "v2" });
      expect((await registry.get({ id: "a" }))?.version).toBe("v3");
      expect((await registry.versions({ id: "a" })).map((v) => v.version)).toEqual(["v1", "v3"]);
    });

    it("removing a draft keeps the active version", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", ...sample(2) });
      await registry.unregister({ id: "a", version: "v2" });
      expect((await registry.get({ id: "a" }))?.version).toBe("v1");
    });

    it("without a version removes the id; unknown ids report false", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", ...sample(2) });
      await registry.register({ id: "b", ...sample(1) });
      expect(await registry.unregister({ id: "a" })).toBe(true);
      expect(await registry.versions({ id: "a" })).toEqual([]);
      expect((await registry.get({ id: "b" }))?.version).toBe("v1");
      expect(await registry.unregister({ id: "a" })).toBe(false);
      expect(await registry.unregister({ id: "b", version: "v9" })).toBe(false);
    });

    it("a re-created id never reuses a label: new bodies continue the sequence", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1), enabled: false });
      await registry.register({ id: "a", ...sample(2) });
      await registry.unregister({ id: "a" });
      expect(await registry.get({ id: "a" })).toBeNull();
      const fresh = await registry.register({ id: "a", ...sample(3), expectedActive: null });
      expect(fresh).toMatchObject({ version: "v3", active: true, enabled: true });
      // The old body comes back under its old label, so a pin `a@v1` keeps its text.
      const restored = await registry.register({ id: "a", ...sample(1) });
      expect(restored).toMatchObject({ version: "v1", active: false });
    });

    it("a removed label is never bound to another body", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", version: "rel", ...sample(2) });
      await registry.unregister({ id: "a", version: "rel" });
      expect(await registry.get({ id: "a", version: "rel" })).toBeNull();
      const err = await rejection(registry.register({ id: "a", version: "rel", ...sample(3) }));
      expect(isRegistryConflict(err) && err.code).toBe("version_conflict");
      // The same body restores it.
      const back = await registry.register({ id: "a", version: "rel", ...sample(2) });
      expect(back).toMatchObject({ version: "rel", active: false });
      // Minted labels skip removed ones too.
      await registry.unregister({ id: "a", version: "rel" });
      await registry.unregister({ id: "a" });
      const next = await registry.register({ id: "a", ...sample(4) });
      expect(next.version).toBe("v3");
    });

    it("expectedActive guards unregister", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", ...sample(2), promote: true });
      expect(
        isRegistryConflict(await rejection(registry.unregister({ id: "a", expectedActive: "v1" }))),
      ).toBe(true);
      expect(await registry.versions({ id: "a" })).toHaveLength(2);
      expect(await registry.unregister({ id: "a", expectedActive: "v2" })).toBe(true);
    });
  });

  describe(`${kind.name}: list`, () => {
    it("answers each id's active version, in id order", async () => {
      const { registry } = await make();
      await registry.register({ id: "b", ...sample(1) });
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "a", ...sample(2) });
      await registry.register({ id: "c", ...sample(1) });
      await registry.register({ id: "c", ...sample(2), promote: true });
      const page = await registry.list();
      expect(page.items.map((e) => [e.id, e.version])).toEqual([
        ["a", "v1"],
        ["b", "v1"],
        ["c", "v2"],
      ]);
      expect(page.nextCursor).toBeNull();
    });

    it("pages by cursor without duplicates or gaps when every row shares a timestamp", async () => {
      const { registry } = await make();
      const ids = ["k1", "k2", "k3", "k4", "k5", "k6", "k7"];
      for (const id of [...ids].reverse()) await registry.register({ id, ...sample(1) });
      const seen: string[] = [];
      let cursor: string | undefined;
      let pages = 0;
      for (;;) {
        const page = await registry.list({ limit: 3, ...(cursor !== undefined && { cursor }) });
        seen.push(...page.items.map((e) => e.id));
        pages += 1;
        if (page.nextCursor === null) break;
        cursor = page.nextCursor;
      }
      expect(seen).toEqual(ids);
      expect(pages).toBe(3);
    });

    it("a page exactly at the end reports no next cursor", async () => {
      const { registry } = await make();
      for (const id of ["a", "b"]) await registry.register({ id, ...sample(1) });
      const page = await registry.list({ limit: 2 });
      expect(page.items).toHaveLength(2);
      expect(page.nextCursor).toBeNull();
    });

    it("an empty registry lists nothing", async () => {
      const { registry } = await make();
      expect(await registry.list({ limit: 5 })).toEqual({ items: [], nextCursor: null });
    });

    it("rejects a page size that is not a positive integer", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      for (const limit of [0, -1, 1.5]) {
        expect(isRegistryInputError(await rejection(registry.list({ limit })))).toBe(true);
      }
    });

    it("filters on the kill switch and on the active version's source", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1), source: "file" });
      await registry.register({ id: "b", ...sample(1), enabled: false });
      await registry.register({ id: "c", ...sample(1), source: "file" });
      await registry.register({ id: "c", ...sample(2), promote: true }); // operator's now
      const ids = async (params: { enabled?: boolean; source?: string }) =>
        (await registry.list({ ...params, limit: 2 })).items.map((e) => e.id);
      expect(await ids({ enabled: true })).toEqual(["a", "c"]);
      expect(await ids({ enabled: false })).toEqual(["b"]);
      expect(await ids({ source: "file" })).toEqual(["a"]);
    });

    it("omits ids whose versions were all removed", async () => {
      const { registry } = await make();
      await registry.register({ id: "a", ...sample(1) });
      await registry.register({ id: "b", ...sample(1) });
      await registry.unregister({ id: "a" });
      expect((await registry.list()).items.map((e) => e.id)).toEqual(["b"]);
    });

    if (fixture.filtering) {
      const { matching, filter } = fixture.filtering;

      it("applies the filter before the limit", async () => {
        const { registry } = await make();
        for (const id of ["a", "b", "c", "d"]) await registry.register({ id, ...sample(1) });
        await registry.register({ id: "e", ...matching(1) });
        await registry.register({ id: "f", ...matching(2) });
        const first = await registry.list({ filter, limit: 1 });
        expect(first.items.map((e) => e.id)).toEqual(["e"]);
        expect(first.nextCursor).not.toBeNull();
        const second = await registry.list({ filter, limit: 1, cursor: first.nextCursor! });
        expect(second.items.map((e) => e.id)).toEqual(["f"]);
        expect(second.nextCursor).toBeNull();
      });

      it("filters on the active version, not on drafts", async () => {
        const { registry } = await make();
        await registry.register({ id: "a", ...matching(1) });
        await registry.register({ id: "a", ...sample(1) });
        expect((await registry.list({ filter })).items.map((e) => e.id)).toEqual(["a"]);
        await registry.promote({ id: "a", version: "v2" });
        expect((await registry.list({ filter })).items).toEqual([]);
      });
    }
  });
}

const LOCAL_BACKEND = {
  type: "local" as const,
  model: { provider: "mock", id: "m" },
  role: { inline: { systemPrompt: "You help.", tools: [] } },
};

const AGENT_FIXTURE: VersionedRegistryKindFixture<AgentRecipe, AgentFilter, AgentRecipeInput> = {
  kind: AGENT_KIND,
  sample: (n) => ({
    backend: { ...LOCAL_BACKEND, model: { provider: "mock", id: `model-${n}` } },
  }),
  filtering: {
    matching: (n) => ({
      backend: { ...LOCAL_BACKEND, model: { provider: "mock", id: `model-${n}` } },
      metadata: { tags: ["beta"], capabilities: ["chat"] },
    }),
    filter: { tag: "beta", capability: "chat" },
  },
};

const ROLE_FIXTURE: VersionedRegistryKindFixture<RoleRecipe, RoleFilter, RoleRecipeInput> = {
  kind: ROLE_KIND,
  sample: (n) => ({ definition: { systemPrompt: `Persona ${n}`, tools: [] } }),
  filtering: {
    matching: (n) => ({
      definition: { systemPrompt: `Persona ${n}`, tools: ["search"], capabilities: ["web"] },
      metadata: { tags: ["research"] },
    }),
    filter: { capability: "web", tag: "research" },
  },
};

const SKILL_FIXTURE: VersionedRegistryKindFixture<SkillRecipe, SkillFilter, SkillRecipeInput> = {
  kind: SKILL_KIND,
  sample: (n) => ({ description: `Skill ${n}`, body: `# Body ${n}` }),
  filtering: {
    matching: (n) => ({
      description: `Skill ${n}`,
      body: `# Body ${n}`,
      metadata: { tags: ["review"], capabilities: ["code"] },
    }),
    filter: { tag: "review", capability: "code" },
  },
};

function dagBody(n: number, tags?: ReadonlyArray<string>): DagRecipe {
  return {
    nodes: [
      { id: "a", agentId: `agent-${n}`, inputs: { task: { kind: "initial", path: "task" } } },
      { id: "b", agentId: "x", inputs: { task: { kind: "node", nodeId: "a", path: "" } } },
    ],
    edges: [{ from: "a", to: "b" }],
    entry: ["a"],
    terminals: ["b"],
    ...(tags !== undefined && { metadata: { tags } }),
  };
}

const DAG_FIXTURE: VersionedRegistryKindFixture<DagRecipe, DagFilter, DagRecipe> = {
  kind: DAG_KIND,
  sample: (n) => dagBody(n),
  filtering: { matching: (n) => dagBody(n, ["nightly"]), filter: { tag: "nightly" } },
};

const FRAGMENT_FIXTURE: VersionedRegistryKindFixture<
  FragmentRecipe,
  FragmentFilter,
  FragmentRecipe
> = {
  kind: FRAGMENT_KIND,
  sample: (n) => ({ content: `## Block ${n}` }),
};

function kindSpecifics(factory: VersionedRegistryFactory): void {
  const clock = (): FakeWallClock => FakeWallClock.create(START_MS);

  describe("agent: normalisation and filters", () => {
    it("fills metadata defaults", async () => {
      const r = await factory({ kind: AGENT_KIND, clock: clock() });
      const entry = await r.register({ id: "a", ...AGENT_FIXTURE.sample(1) });
      expect(entry.metadata).toEqual({ description: null, capabilities: [], tags: [] });
    });

    it("filters by backend type and keeps Designer drafts apart", async () => {
      const r = await factory({ kind: AGENT_KIND, clock: clock() });
      await r.register({ id: "local", ...AGENT_FIXTURE.sample(1) });
      await r.register({
        id: "remote",
        backend: { type: "remote", endpoint: "https://b.example", remoteAgentId: "x" },
      });
      await r.register({ id: "__draft__abc", ...AGENT_FIXTURE.sample(2) });
      const ids = async (filter: AgentFilter) => (await r.list({ filter })).items.map((e) => e.id);
      expect(await ids({ backendType: "remote" })).toEqual(["remote"]);
      expect(await ids({ drafts: "exclude" })).toEqual(["local", "remote"]);
      expect(await ids({ drafts: "only" })).toEqual(["__draft__abc"]);
    });
  });

  describe("skill: normalisation", () => {
    it("falls back to description for whenToUse and drops unknown metadata", async () => {
      const r = await factory({ kind: SKILL_KIND, clock: clock() });
      const input: SkillRecipeInput = {
        description: "Debug",
        body: "# Steps",
        metadata: { tags: ["x"] },
      };
      const smuggled = { ...input, metadata: { ...input.metadata, trust: "trusted" } };
      const entry = await r.register({ id: "a", ...smuggled, trust: "needs-review" });
      expect(entry.whenToUse).toBe("Debug");
      expect(entry.metadata).toEqual({ capabilities: [], tags: ["x"] });
      expect(entry.trust).toBe("needs-review");
    });

    it("rejects an empty body", async () => {
      const r = await factory({ kind: SKILL_KIND, clock: clock() });
      expect(
        isRegistryInputError(await rejection(r.register({ id: "a", description: "d", body: "" }))),
      ).toBe(true);
    });
  });

  describe("dag: validation", () => {
    it("rejects an invalid graph before it is stored", async () => {
      const r = await factory({ kind: DAG_KIND, clock: clock() });
      const err = await rejection(
        r.register({
          id: "bad",
          nodes: [{ id: "a", agentId: "x", inputs: {} }],
          edges: [{ from: "a", to: "ghost" }],
          entry: ["a"],
          terminals: ["a"],
        }),
      );
      expect(err).toBeInstanceOf(DagValidationError);
      expect(await r.get({ id: "bad" })).toBeNull();
    });
  });

  describe("fragment: validation", () => {
    it("rejects ids with the version separator and empty content", async () => {
      const r = await factory({ kind: FRAGMENT_KIND, clock: clock() });
      expect(isRegistryInputError(await rejection(r.register({ id: "a@b", content: "x" })))).toBe(
        true,
      );
      expect(isRegistryInputError(await rejection(r.register({ id: "a", content: "" })))).toBe(
        true,
      );
    });
  });
}

/**
 * Run the shared `VersionedRegistry` semantics for one kind defined outside
 * this package (eval suites, say) against a backend.
 */
export function versionedRegistryKindTestSuite<TBody, TFilter, TInput>(params: {
  readonly name: string;
  readonly factory: VersionedRegistryFactory;
  readonly fixture: VersionedRegistryKindFixture<TBody, TFilter, TInput>;
}): void {
  describe(`${params.name} conformance`, () => {
    sharedSemantics({ factory: params.factory, fixture: params.fixture });
  });
}

/** Run the `VersionedRegistry` conformance suite against a backend. */
export function versionedRegistryTestSuite(params: {
  readonly name: string;
  readonly factory: VersionedRegistryFactory;
}): void {
  const { factory } = params;
  describe(`${params.name} conformance`, () => {
    sharedSemantics({ factory, fixture: AGENT_FIXTURE });
    sharedSemantics({ factory, fixture: ROLE_FIXTURE });
    sharedSemantics({ factory, fixture: SKILL_FIXTURE });
    sharedSemantics({ factory, fixture: DAG_FIXTURE });
    sharedSemantics({ factory, fixture: FRAGMENT_FIXTURE });
    kindSpecifics(factory);
  });
}
