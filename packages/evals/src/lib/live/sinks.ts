// ---------------------------------------------------------------------------
// LiveScoreSink implementations.
//
//   metricsLiveSink  — emits each score to an AgentMetrics histogram.
//                      Storage-free, prod-ready: live scores become an
//                      `eval.score` distribution to scrape / alert on.
//   inMemoryLiveSink — collects scores in memory for tests + the demo.
// ---------------------------------------------------------------------------

import type { AgentMetrics } from "@promin/agent";
import type { LiveScore, LiveScoreSink } from "./types.ts";

/** A sink that records each score into an `AgentMetrics` histogram. */
export function metricsLiveSink(metrics: AgentMetrics): LiveScoreSink {
  return {
    record(score) {
      const histogram = metrics.histogram("eval.score");
      for (const entry of score.scores) {
        histogram.observe(entry.value, { scorerId: entry.scorerId, agentId: score.agentId });
      }
    },
  };
}

/** A sink that collects scores in memory — for tests and the dashboard demo. */
export class InMemoryLiveScoreSink implements LiveScoreSink {
  readonly scores: LiveScore[] = [];
  private readonly waiters: Array<{ count: number; resolve: () => void }> = [];

  record(score: LiveScore): void {
    this.scores.push(score);
    for (let i = this.waiters.length - 1; i >= 0; i -= 1) {
      const waiter = this.waiters[i];
      if (waiter !== undefined && this.scores.length >= waiter.count) {
        waiter.resolve();
        this.waiters.splice(i, 1);
      }
    }
  }

  /** Resolve once at least `count` scores have been recorded. */
  waitForCount(count: number): Promise<void> {
    if (this.scores.length >= count) return Promise.resolve();
    return new Promise((resolve) => {
      this.waiters.push({ count, resolve });
    });
  }
}

/** Build a fresh in-memory sink. */
export function inMemoryLiveSink(): InMemoryLiveScoreSink {
  return new InMemoryLiveScoreSink();
}
