import { workflow } from "@promin/workflow";

export interface DistributedJobInput {
  readonly id?: number;
  readonly delayMs?: number;
}

const pause = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A small, deterministic workload for demonstrating queue distribution.
 * Replace the steps with application work; the worker topology is unchanged.
 */
export const distributedJob = workflow<DistributedJobInput>({
  name: "distributed-job",
  type: "docker-example",
  version: "1",
})
  .step("execute", async ({ input }) => {
    const delayMs = Math.max(0, Math.min(input.delayMs ?? 50, 5_000));
    await pause(delayMs);
    return {
      id: input.id ?? null,
      worker: process.env.WORKER_ID ?? "unknown-worker",
      delayMs,
    };
  })
  .step("finalize", async ({ prev }) => ({ ...prev, finalized: true }))
  .build();
