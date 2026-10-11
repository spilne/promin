// ---------------------------------------------------------------------------
// Compile-time fixtures: typed workflow definitions (typed input, output and
// errors, child workflows) are accepted where a worker or the client takes
// a list of definitions, without casts.
//
// Not imported anywhere at runtime. Pure type surface.
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";
import { ZoryaWorker, type ZoryaWorkerConfig } from "./worker.ts";
import type { ZoryaClient } from "./zorya-client.ts";

declare const client: ZoryaClient;

const greet = workflow<{ name?: string }>({ name: "greet" })
  .step("greet", async ({ input }) => ({ message: `Hello, ${input.name ?? "world"}!` }))
  .build();
// Typed errors: `Workflow<{ cents: number }, number, GuardError>`.
const charge = workflow<{ cents: number }>({ name: "charge" })
  .guard("positive", (input) => input.cents >= 0)
  .step("charge", ({ prev }) => prev.cents)
  .build();
const parent = workflow<{ id: string }>({ name: "parent" })
  .subworkflow("child", greet, { input: () => ({}), workflowId: (p) => `${p.id}-child` })
  .build();

export const _config: ZoryaWorkerConfig = { client, workflows: [greet, charge, parent] };
export const _advertised: Promise<void> = client.advertise({
  workerId: "w",
  workflows: [greet, charge, parent],
});
export const _built: ZoryaWorker = ZoryaWorker.builder()
  .client(client)
  .workflow(greet)
  .workflows(charge, parent)
  .build();
