// ---------------------------------------------------------------------------
// buildRetrieverContext: per-binding timeouts on the clock, cancellation of a
// timed-out search, and a retriever that throws before returning a promise.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { FakeWallClock } from "@promin/workflow";
import { buildRetrieverContext } from "../retriever-bindings.ts";
import { InMemoryRetriever } from "../../rag/in-memory-retriever.ts";
import type { RetrieveRequest, RetrieveResult, Retriever } from "../../rag/types.ts";

describe("buildRetrieverContext", () => {
  it("a retriever that throws synchronously fails its binding and leaves no timer", async () => {
    const clock = FakeWallClock.create(0);
    const throwing: Retriever = {
      retrieve: (): Promise<RetrieveResult[]> => {
        throw new Error("bad config");
      },
    };

    const context = await buildRetrieverContext({
      retrievers: { broken: { retriever: throwing, mode: "context" } },
      query: "q",
      clock,
    });

    expect(context.failures).toEqual([
      { bindingId: "broken", error: "bad config", timedOut: false },
    ]);
    expect(clock.pendingCount()).toBe(0);
  });

  it("a timed-out search is cancelled through its signal", async () => {
    const clock = FakeWallClock.create(0);
    let seen: AbortSignal | undefined;
    const hanging: Retriever = {
      retrieve: (request: RetrieveRequest) => {
        seen = request.signal;
        return new Promise<RetrieveResult[]>(() => {});
      },
    };

    const pending = buildRetrieverContext({
      retrievers: { slow: { retriever: hanging, mode: "context", timeoutMs: 500 } },
      query: "q",
      clock,
    });
    for (let i = 0; i < 100 && clock.pendingCount() === 0; i++) {
      await new Promise((r) => setTimeout(r, 0));
    }
    clock.advance(500);
    const context = await pending;

    expect(context.failures[0]?.timedOut).toBe(true);
    expect(seen?.aborted).toBe(true);
  });

  it("the in-memory retriever stops on an aborted signal", async () => {
    const retriever = new InMemoryRetriever({ documents: [{ id: "d", text: "hello world" }] });
    const controller = new AbortController();
    controller.abort(new Error("cancelled"));
    await expect(retriever.retrieve({ query: "hello", signal: controller.signal })).rejects.toThrow(
      "cancelled",
    );
  });
});
