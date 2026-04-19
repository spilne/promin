// ---------------------------------------------------------------------------
// Example 03 — Drain policy via WorkflowVersionRegistry (3+ versions)
//
// `previousVersions` works for 2-3 versions but gets unwieldy when you have
// many coexisting versions. `WorkflowVersionRegistry.for(name)` is the
// fluent builder for a scoped registry — one entry point, automatic version
// resolution on resume, optional auto-deregister when a version drains.
//
// Run: bun run packages/workflow/examples/versioning/03-drain-registry.ts
// ---------------------------------------------------------------------------

import { Pipeline } from "@promin/core";
import { workflow, InMemoryWorkflowStorage, WorkflowVersionRegistry } from "@promin/workflow";

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

  // Scoped registry: all calls are pinned to the "job" workflow name.
  // `onDrained` fires when a version's in-flight count hits zero.
  // `autoDeregister: true` removes drained versions (except the latest).
  const registry = WorkflowVersionRegistry.for("job", {
    storage,
    autoDeregister: true,
    onDrained: (_name, version) => {
      console.log(`version "${version}" has drained`);
    },
  })
    .register(v1)
    .register(v2)
    .register(v3);

  // Start workflows under v1 and v2 directly to seed the storage.
  await v1.bind(storage).run({ workflowId: "j-1", input: { x: 10 } });
  await v2.bind(storage).run({ workflowId: "j-2", input: { x: 20 } });

  // New workflows go to the latest (v3) — registry resolves that automatically.
  const j3Result = await registry.run<string>({ workflowId: "j-3", input: { x: 30 } });
  console.log("fresh workflow result:", j3Result);

  // Resuming an existing workflow — registry resolves the stored version
  // and delegates to the matching definition.
  const j1Resumed = await registry.run<string>({ workflowId: "j-1", input: { x: 10 } });
  console.log("resumed j-1 (v1) result:", j1Resumed);

  // Ops call — returns per-version counts for monitoring drain progress.
  const counts = await registry.countByVersion({ storage });
  for (const [version, c] of counts) {
    console.log(`v${version}: running=${c.running} completed=${c.completed} failed=${c.failed}`);
  }

  // Takeaway: registry is the right scaling answer once you have 3+
  // coexisting versions or want automated drain monitoring.
}

if (import.meta.main) {
  await main();
}
