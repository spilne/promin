// ---------------------------------------------------------------------------
// versioned-greeter — three versions of the same workflow registered side
// by side so the Deployments page has something to show.
//
// v1 — single-step greeting.
// v2 — two-step (greeting + farewell).
// v3 — three-step (greeting + farewell + audit timestamp).
//
// All three are registered into the demo's `WorkflowVersionRegistry` at
// startup. Promoting / rolling back among them shifts which version new
// `trigger("versioned-greeter", input)` calls dispatch on.
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";

export interface GreeterInput {
  name?: string;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((r) => setTimeout(r, ms));
}

function delay(minMs: number, maxMs: number): number {
  return minMs + Math.floor(Math.random() * (maxMs - minMs));
}

// All three versions live inside the `versions` array (not as bare named
// exports) — the workflow folder scanner only recognises Workflow-shaped
// objects, so wrapping them dodges the duplicate-name warning that fires
// when the same `name: "versioned-greeter"` shows up multiple times.
const v1 = workflow<GreeterInput>({
  name: "versioned-greeter",
  type: "demo",
  version: "1",
})
  .stepAsync("greet", async ({ input }) => {
    await sleep(delay(200, 600));
    return { message: `Hello, ${input.name ?? "world"}!` };
  })
  .build();

const v2 = workflow<GreeterInput>({
  name: "versioned-greeter",
  type: "demo",
  version: "2",
  // Drain rather than strict so an in-flight v1 run can finish on the
  // v1 definition during a promote → v2 transition.
  onVersionMismatch: "drain",
  previousVersions: [v1],
})
  .stepAsync("greet", async ({ input }) => {
    await sleep(delay(200, 600));
    return { message: `Hello, ${input.name ?? "world"}!` };
  })
  .stepAsync("farewell", async ({ prev }) => {
    await sleep(delay(150, 400));
    return {
      ...prev,
      farewell: `Bye, ${prev.message.replace("Hello, ", "").replace("!", "")}.`,
    };
  })
  .build();

const v3 = workflow<GreeterInput>({
  name: "versioned-greeter",
  type: "demo",
  version: "3",
  onVersionMismatch: "drain",
  previousVersions: [v1, v2],
})
  .stepAsync("greet", async ({ input }) => {
    await sleep(delay(200, 600));
    return { message: `Hello, ${input.name ?? "world"}!` };
  })
  .stepAsync("farewell", async ({ prev }) => {
    await sleep(delay(150, 400));
    return {
      ...prev,
      farewell: `Bye, ${prev.message.replace("Hello, ", "").replace("!", "")}.`,
    };
  })
  .stepAsync("audit", async ({ prev }) => {
    await sleep(delay(100, 300));
    return { ...prev, auditedAt: new Date().toISOString() };
  })
  .build();

/** Every version of versioned-greeter, latest last — for the registry. */
export const versionedGreeterVersions = [v1, v2, v3];

// Default export — picked up by the folder scanner as the "current"
// definition. Using v3 means unversioned `trigger("versioned-greeter")`
// calls (before any promote) fall onto the latest-registered version.
export default v3;
