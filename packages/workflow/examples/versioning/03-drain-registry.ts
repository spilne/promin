// ---------------------------------------------------------------------------
// Example 03 — Drain policy via WorkflowVersionRegistry (3+ versions)
//
// `previousVersions` works for 2-3 versions but gets unwieldy when you have
// many coexisting versions. `createWorkflowVersionRegistry()` is a catalog
// of (name, version) definitions that the runner consults on resume —
// automatic version resolution, optional auto-deregister when a version
// drains.
//
// Run: bun run packages/workflow/examples/versioning/03-drain-registry.ts
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import {
  workflow,
  InMemoryWorkflowStorage,
  createWorkflowVersionRegistry,
  createWorkflowRunner,
} from "@promin/workflow";

async function main(): Promise<void> {
  const storage = new InMemoryWorkflowStorage();

  // Each version is a full workflow definition, built and kept around.
  const v1 = workflow<{ x: number }>({ name: "job", version: "1" })
    .step("run", ({ input }) => Pipeline.succeed(`v1-${input.x}`))
    .build();

  const v2 = workflow<{ x: number }>({ name: "job", version: "2" })
    .step("run", ({ input }) => Pipeline.succeed(`v2-${input.x}`))
    .build();

  const v3 = workflow<{ x: number }>({ name: "job", version: "3" })
    .step("run", ({ input }) => Pipeline.succeed(`v3-${input.x}`))
    .build();

  // Registry carries the (name, version) catalog. The runner holds storage
  // and delegates version resolution to the registry on resume.
  // `onDrained` fires when a version's in-flight count hits zero.
  // `autoDeregister: true` removes drained versions (except the latest).
  const registry = createWorkflowVersionRegistry({
    autoDeregister: true,
    onDrained: (_name, version) => {
      console.log(`version "${version}" has drained`);
    },
  });
  registry.register(v1);
  registry.register(v2);
  registry.register(v3);

  const runner = createWorkflowRunner({ storage, registry });

  // Start workflows under v1 and v2 directly to seed the storage.
  await runner.run({ workflow: v1, workflowId: "j-1", input: { x: 10 } });
  await runner.run({ workflow: v2, workflowId: "j-2", input: { x: 20 } });

  // New workflows go to the latest (v3) — registry resolves that automatically.
  const j3Result = (await runner.run({
    name: "job",
    workflowId: "j-3",
    input: { x: 30 },
  })) as string;
  console.log("fresh workflow result:", j3Result);

  // Resuming an existing workflow — registry resolves the stored version
  // and delegates to the matching definition.
  const j1Resumed = (await runner.run({
    name: "job",
    workflowId: "j-1",
    input: { x: 10 },
  })) as string;
  console.log("resumed j-1 (v1) result:", j1Resumed);

  // Ops call — returns per-version counts for monitoring drain progress.
  const counts = await registry.countByVersion({ name: "job", storage });
  for (const [version, c] of counts) {
    console.log(`v${version}: running=${c.running} completed=${c.completed} failed=${c.failed}`);
  }

  // Takeaway: registry is the right scaling answer once you have 3+
  // coexisting versions or want automated drain monitoring.
}

if (import.meta.main) {
  await main();
}
