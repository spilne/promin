import {
  completeSignal,
  isJournaledSuspendStorage,
  type Workflow,
  type WorkflowRunner,
  type WorkflowStorage,
} from "@promin/workflow";
import type { Logger } from "../../src/index.ts";

export interface ApprovalAutoSignalerDeps {
  readonly storage: WorkflowStorage;
  readonly runner: WorkflowRunner;
  readonly workflowsByName: Readonly<Record<string, Workflow<unknown, unknown>>>;
  readonly logger?: Logger;
  readonly intervalMs?: number;
}

export function startApprovalAutoSignaler({
  storage,
  runner,
  workflowsByName,
  logger,
  intervalMs = 5_000,
}: ApprovalAutoSignalerDeps): () => void {
  const delivered = new Set<string>();
  const handle = setInterval(async () => {
    const runs = await storage.listWorkflows({
      name: "approval-flow",
      status: "suspended",
      limit: 50,
    });
    for (const r of runs) {
      if (delivered.has(r.workflowId)) continue;
      const waiting = Object.values(r.steps).some(
        (s) => s.stepName === "review" && s.status === "waiting_for_signal",
      );
      if (!waiting) continue;

      const approved = Math.random() < 0.5;
      const payload = { approved, by: "auto-signaler" };
      await storage.deliverSignal(r.workflowId, "approval", payload);
      if (isJournaledSuspendStorage(storage)) {
        await completeSignal({
          storage,
          workflowId: r.workflowId,
          stepName: "review",
          signalName: "approval",
          value: payload,
        });
      }
      delivered.add(r.workflowId);
      runner
        .run({
          workflow: workflowsByName["approval-flow"]!,
          workflowId: r.workflowId,
          input: r.input,
        })
        .catch((err) => {
          logger?.warn("[zorya] approval auto-signaler resume failed:", err);
        });
    }
  }, intervalMs);

  return () => clearInterval(handle);
}
