// ---------------------------------------------------------------------------
// research workflow — multi-activity .journaled() step demo.
//
// Inside the generator body each `yield* ctx.activity(...)` is recorded in
// the activity journal. On crash + resume, completed activities replay
// from the journal instead of re-running, so a worker that died
// mid-research picks up exactly where it left off without re-fetching
// already-processed sources. `ctx.sleep` is journaled too — a sleeping
// run survives a worker restart.
//
// Mirrors the inline demo in `examples/split/worker.ts` but lives in the
// scanned workflows folder so the all-in-one demo also exposes a
// journaled workflow for inspection.
// ---------------------------------------------------------------------------

import { workflow } from "@promin/workflow";

export interface ResearchInput {
  topic?: string;
  /**
   * How many sources to analyse. Default 3. Higher values produce more
   * `analyze-N` activity entries in the journal so the run-detail Step
   * tab's attempt + journal sections have something to look at.
   */
  sourceCount?: number;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export const researchWorkflow = workflow<ResearchInput>({
  name: "research",
  type: "platform",
})
  .journaled("research", function* (ctx, input) {
    const topic = input.topic ?? "workflows";
    const sourceCount = input.sourceCount ?? 3;

    const sources = yield* ctx.activity("fetch-sources", async () => {
      await sleep(400 + Math.floor(Math.random() * 600));
      return Array.from({ length: sourceCount }, (_, i) => ({
        id: i + 1,
        url: `https://example.com/${topic}/source-${i + 1}`,
      }));
    });

    // Per-source analysis — each becomes its own activity entry in the
    // journal. On replay each one short-circuits to its recorded result.
    const summaries: Array<{ id: number; words: number; relevance: number }> = [];
    for (const src of sources) {
      const summary = yield* ctx.activity(`analyze-${src.id}`, async () => {
        await sleep(300 + Math.floor(Math.random() * 700));
        return {
          id: src.id,
          words: 50 + Math.floor(Math.random() * 200),
          relevance: Number(Math.random().toFixed(2)),
        };
      });
      summaries.push(summary);
    }

    // Cooldown — journaled, so a worker restart during this window resumes
    // the sleep at the original wakeAt rather than starting a new one.
    yield* ctx.sleep(500);

    const report = yield* ctx.activity("compose-report", async () => {
      await sleep(400);
      return {
        topic,
        totalWords: summaries.reduce((s, x) => s + x.words, 0),
        sourceCount: summaries.length,
        avgRelevance: Number(
          (summaries.reduce((s, x) => s + x.relevance, 0) / summaries.length).toFixed(2),
        ),
      };
    });

    return report;
  })
  .build();
