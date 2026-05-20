// ---------------------------------------------------------------------------
// liveScored — decorate an Agent so it scores its own production runs.
//
// An explicit delegating wrapper (not a Proxy). Only `invoke` / `stream` and
// thread `send` / `stream` get the scoring hook; the rest pass through and
// `withScope` re-wraps. After a run resolves it samples, and on a hit runs
// the scorers off-thread against `toEvalOutput(run)`, pushing a `LiveScore`
// to the sink.
//
// Three load-bearing constraints: the response is never blocked; a throwing
// scorer is swallowed via `onError`; a sampling miss skips `toEvalOutput`.
// ---------------------------------------------------------------------------

import type { Agent, AgentInvokeOpts, AgentRunOutput, AgentThread } from "@promin/agent";
import { SystemClock } from "@promin/core";
import { toEvalOutput } from "../targets/to-eval-output.ts";
import type { Score } from "../types.ts";
import type { LiveScore, LiveScoringConfig } from "./types.ts";

/** Wrap an `Agent` so every sampled run is scored off-thread. */
export function liveScored<Input, Output>(
  agent: Agent<Input, Output>,
  config: LiveScoringConfig,
): Agent<Input, Output> {
  const clock = config.clock ?? SystemClock;
  const rate = config.sampling?.rate ?? 1;
  const agentId = config.agentId ?? "agent";
  const onError = config.onError ?? noop;

  const sample = (runId: string): boolean =>
    config.decide !== undefined ? config.decide(runId) : Math.random() < rate;

  // Fire-and-forget: score a run off-thread. Never throws into the caller;
  // a sampling miss skips `toEvalOutput` entirely.
  const scoreRun = (
    run: AgentRunOutput<Output>,
    startedAt: number,
    input: Input,
    opts: AgentInvokeOpts | undefined,
  ): void => {
    const runId =
      opts?.runId ?? `live-${clock.currentTimeMs()}-${Math.random().toString(36).slice(2, 10)}`;
    if (!sample(runId)) return;
    void (async (): Promise<void> => {
      // Wait for the run to settle without surfacing its rejection here.
      await Promise.allSettled([run.messages]);
      const output = await toEvalOutput(run, { latencyMs: clock.currentTimeMs() - startedAt });
      const scores: Score[] = [];
      for (const scorer of config.scorers) {
        try {
          scores.push(await scorer.score({ input, output }));
        } catch (err) {
          onError(err);
        }
      }
      const liveScore: LiveScore = {
        runId,
        agentId,
        scoredAt: clock.currentTimeMs(),
        scores,
        ...(opts?.namespaceId !== undefined && { namespaceId: opts.namespaceId }),
        ...(opts?.resourceId !== undefined && { resourceId: opts.resourceId }),
      };
      await config.sink.record(liveScore);
    })().catch(onError);
  };

  const wrapThread = (inner: AgentThread<Input, Output>): AgentThread<Input, Output> => {
    const thread: AgentThread<Input, Output> = {
      id: inner.id,
      resourceId: inner.resourceId,
      isNew: inner.isNew,
      async send(input, opts) {
        const startedAt = clock.currentTimeMs();
        const run = await inner.send(input, opts);
        scoreRun(run, startedAt, input, opts);
        return run;
      },
      stream(input, opts) {
        const startedAt = clock.currentTimeMs();
        const run = inner.stream(input, opts);
        scoreRun(run, startedAt, input, opts);
        return run;
      },
      messages: (range) => inner.messages(range),
      workingMemory: () => inner.workingMemory(),
      setWorkingMemory: (markdown) => inner.setWorkingMemory(markdown),
      metadata: () => inner.metadata(),
      setMetadata: (metadata) => inner.setMetadata(metadata),
      title: () => inner.title(),
      setTitle: (title) => inner.setTitle(title),
      setArchived: (archivedAt) => inner.setArchived(archivedAt),
      delete: () => inner.delete(),
    };
    if (inner.resume !== undefined) {
      const innerResume = inner.resume.bind(inner);
      thread.resume = (callId, decision, opts) => innerResume(callId, decision, opts);
    }
    if (inner.resumeStream !== undefined) {
      const innerResumeStream = inner.resumeStream.bind(inner);
      thread.resumeStream = (callId, decision, opts) => innerResumeStream(callId, decision, opts);
    }
    return thread;
  };

  return {
    async invoke(input, opts) {
      const startedAt = clock.currentTimeMs();
      const run = await agent.invoke(input, opts);
      scoreRun(run, startedAt, input, opts);
      return run;
    },
    stream(input, opts) {
      const startedAt = clock.currentTimeMs();
      const run = agent.stream(input, opts);
      scoreRun(run, startedAt, input, opts);
      return run;
    },
    async thread(threadId, opts) {
      return wrapThread(await agent.thread(threadId, opts));
    },
    listThreads: (params) => agent.listThreads(params),
    compactThread: (threadId, opts) => agent.compactThread(threadId, opts),
    distillThread: (threadId, opts) => agent.distillThread(threadId, opts),
    withScope: (scope) => liveScored(agent.withScope(scope), config),
  };
}

function noop(): void {
  // Default `onError` — a scoring fault is swallowed, never reaching the run.
}
