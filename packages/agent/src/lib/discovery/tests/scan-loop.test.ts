// ---------------------------------------------------------------------------
// `startScanLoop` — the polling loop the agent, skill and fragment scanners
// share: a failing timer tick is reported and polling continues; it never
// becomes an unhandled rejection.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakeWallClock } from "@promin/workflow";
import { startScanLoop } from "../scan-loop.ts";
import { startSkillScanLoop } from "../skill-scanner.ts";
import { InMemorySkillRegistry } from "../../versioned-registry/in-memory-registries.ts";
import type { RegisteredSkill } from "../../skills/types.ts";

/** Yield to the event loop until `done()` holds (bounded by iterations, not time). */
async function until(done: () => boolean): Promise<void> {
  for (let i = 0; i < 100 && !done(); i++) await new Promise((r) => setTimeout(r, 0));
}

describe("startScanLoop", () => {
  it("reports a failing timer tick to onError and keeps polling", async () => {
    const clock = FakeWallClock.create(0);
    const errors: unknown[] = [];
    let runs = 0;
    const loop = startScanLoop({
      clock,
      intervalMs: 1_000,
      onError: (err) => errors.push(err),
      run: async () => {
        runs += 1;
        if (runs === 1) throw new Error("registry unavailable");
        return runs;
      },
    });
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandled);
    try {
      clock.advance(1_000);
      await until(() => errors.length > 0);
      expect(errors).toHaveLength(1);
      expect((errors[0] as Error).message).toBe("registry unavailable");

      clock.advance(1_000);
      await until(() => runs === 2);
      expect(runs).toBe(2);
      expect(errors).toHaveLength(1);
      expect(unhandled).toEqual([]);
    } finally {
      process.off("unhandledRejection", onUnhandled);
      loop.stop();
    }
  });

  it("a direct tick() rejects with the scan's error", async () => {
    const loop = startScanLoop({
      clock: FakeWallClock.create(0),
      run: async () => {
        throw new Error("boom");
      },
    });
    await expect(loop.tick()).rejects.toThrow("boom");
    loop.stop();
  });

  it("a scanner loop whose registry fails reports the tick instead of crashing", async () => {
    class FailingSkillRegistry extends InMemorySkillRegistry {
      override async versions(): Promise<RegisteredSkill[]> {
        throw new Error("db down");
      }
    }
    const root = join(tmpdir(), `scan-loop-failing-${process.pid}-${Date.now()}`);
    await mkdir(root, { recursive: true });
    await writeFile(
      join(root, "debug.md"),
      "---\nname: debug\ndescription: Debug carefully.\n---\n\n# Steps\n",
    );
    const clock = FakeWallClock.create(0);
    const errors: unknown[] = [];
    const loop = startSkillScanLoop({
      registry: new FailingSkillRegistry(),
      root,
      intervalMs: 1_000,
      clock,
      onError: (err) => errors.push(err),
    });
    try {
      clock.advance(1_000);
      await until(() => errors.length > 0);
      expect((errors[0] as Error).message).toBe("db down");
    } finally {
      loop.stop();
      await rm(root, { recursive: true, force: true });
    }
  });
});
