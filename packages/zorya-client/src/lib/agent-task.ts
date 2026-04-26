// ---------------------------------------------------------------------------
// runAgentTask — execute an agentAction on a ZoryaWorker with streaming
// wired automatically.
//
// The "AgentWorker" pattern: a worker hosts an agent task, the agent's
// SessionEventBus is registered on `worker.streams[workflowId]` for the
// duration, and dashboard SSE clients receive token deltas + tool events
// via the WS relay built in promin-yxxk + promin-o8dj.
//
// Implemented as a free function rather than a new class because:
//   - The wiring is three lines (bus + register + run + cleanup).
//   - Callers usually have one ZoryaWorker, and adding a wrapper class
//     forces them to construct two parallel objects.
//   - Future fancier agent-side features (auto-discovery, batch
//     dispatch) can compose on top without disturbing the helper.
//
// Usage:
//
//   const result = await runAgentTask(worker, {
//     name: "research",
//     llm: anthropic("claude-sonnet-4-6"),
//     tools: { search, readUrl },
//     systemPrompt: "...",
//   }, {
//     workflowId: "research-1",
//     input: { task: "Summarize the latest paper on X" },
//   });
//
// While `result` resolves, any dashboard tab open on
// `/api/runs/research-1/agent-stream` receives the agent's events live.
// ---------------------------------------------------------------------------

import { agentAction, SessionEventBus } from "@promin/agent";
import type { AgentActionConfig, AgentInput, AgentResult } from "@promin/agent";
import type { ZoryaWorker } from "./worker.ts";

export interface RunAgentTaskParams {
  /** Stable workflow id for the agent run. Doubles as the SSE stream key. */
  workflowId: string;
  /** Task + optional message history fed into agentAction. */
  input: AgentInput;
  /**
   * Bring-your-own bus when the caller wants additional in-process
   * subscribers (e.g. a CLI logger). The helper still registers it on
   * `worker.streams` so the dashboard sees the same events.
   */
  bus?: SessionEventBus;
}

/**
 * Run an agent task on a worker with end-to-end streaming wired.
 *
 * Pairs the agent's SessionEventBus with `worker.streams[workflowId]` so
 * the worker control socket can forward events to dashboard SSE clients
 * (via the AgentStreamHub on the server). On completion (success or
 * failure) the registration is removed so streaming naturally ends.
 *
 * Storage I/O routes through the worker's runner (which is bound to the
 * ZoryaClient's `RemoteWorkflowStorage`), so journaled activities + run
 * history land on the central server like any other workflow.
 */
export async function runAgentTask<TOutput = unknown>(
  worker: ZoryaWorker,
  // biome-ignore lint/suspicious/noExplicitAny: TOutput is inferred from outputSchema if given
  config: AgentActionConfig<any>,
  params: RunAgentTaskParams,
): Promise<AgentResult & { output?: TOutput }> {
  const bus = params.bus ?? new SessionEventBus();
  // The two `agentAction` overloads disambiguate on outputSchema's
  // presence; spreading `config` makes TS pick neither. Cast to the
  // implementation signature — runtime behavior is identical.
  // biome-ignore lint/suspicious/noExplicitAny: see comment above
  const wf = (agentAction as (cfg: AgentActionConfig<any>) => ReturnType<typeof agentAction>)({
    ...config,
    bus,
  });
  // Register before the run so the dashboard catches turn.start. The
  // subscribe shape `(observer) => unsubscribe` matches the contract the
  // worker's control-socket command handler expects — it'll fan every
  // emitted event out as a WS frame on the active streamId.
  const unregister = worker.registerStream(params.workflowId, (observer) =>
    bus.subscribe(observer),
  );
  try {
    const result = (await worker.runner.run({
      workflow: wf,
      workflowId: params.workflowId,
      input: params.input,
    })) as AgentResult & { output?: TOutput };
    return result;
  } finally {
    unregister();
  }
}
