import { describe, it, expect } from "bun:test";
import { OffsetTracker } from "../offset-tracker.ts";

describe("OffsetTracker — parallel-safe commit ordering", () => {
  it("sequential completions — commit advances with each ack", () => {
    const tracker = new OffsetTracker();

    tracker.complete(0, 0);
    tracker.complete(0, 1);
    tracker.complete(0, 2);

    const committable = tracker.committable();
    expect(committable.get(0)).toBe(3); // commit offset = next to read
  });

  it("out-of-order completions — commit waits for the gap to fill", () => {
    const tracker = new OffsetTracker();

    // Process offsets 0, 2, 3 — but offset 1 is still in-flight
    tracker.complete(0, 0);
    tracker.complete(0, 2);
    tracker.complete(0, 3);

    const first = tracker.committable();
    expect(first.get(0)).toBe(1); // only offset 0 is contiguous → commit 1

    // Now offset 1 completes
    tracker.complete(0, 1);

    const second = tracker.committable();
    expect(second.get(0)).toBe(4); // 0,1,2,3 all done → commit 4
  });

  it("nothing committable when first offset is still pending", () => {
    const tracker = new OffsetTracker();
    tracker.setFrontier(0, 0); // consumer starts at offset 0

    tracker.complete(0, 1); // offset 0 not done yet
    tracker.complete(0, 2);

    const committable = tracker.committable();
    expect(committable.size).toBe(0); // can't commit — offset 0 is the gap
  });

  it("tracks partitions independently", () => {
    const tracker = new OffsetTracker();

    tracker.complete(0, 0);
    tracker.complete(0, 1);
    tracker.complete(1, 0);

    const committable = tracker.committable();
    expect(committable.get(0)).toBe(2); // partition 0: offsets 0,1
    expect(committable.get(1)).toBe(1); // partition 1: offset 0
  });

  it("committable is consumed — calling twice returns empty", () => {
    const tracker = new OffsetTracker();

    tracker.complete(0, 0);
    tracker.complete(0, 1);

    const first = tracker.committable();
    expect(first.get(0)).toBe(2);

    const second = tracker.committable();
    expect(second.size).toBe(0); // already consumed
  });

  it("new completions after commit are tracked from the new frontier", () => {
    const tracker = new OffsetTracker();

    tracker.complete(0, 0);
    tracker.complete(0, 1);
    tracker.committable(); // advances frontier to 2

    tracker.complete(0, 2);
    tracker.complete(0, 3);

    const committable = tracker.committable();
    expect(committable.get(0)).toBe(4);
  });

  it("pendingCount shows messages behind a gap", () => {
    const tracker = new OffsetTracker();

    tracker.complete(0, 0);
    tracker.complete(0, 2); // gap at 1
    tracker.complete(0, 3);
    tracker.complete(0, 4);

    tracker.committable(); // consumes offset 0, leaves 2,3,4 pending

    expect(tracker.pendingCount()).toBe(3); // 2, 3, 4 waiting behind gap at 1
  });

  it("simulates parallel processing — 5 workers, out-of-order completion", () => {
    const tracker = new OffsetTracker();

    // 5 messages dispatched to parallel workers
    // They complete in random order: 3, 0, 4 first
    tracker.complete(0, 3);
    tracker.complete(0, 0);
    tracker.complete(0, 4);

    let committable = tracker.committable();
    expect(committable.get(0)).toBe(1); // only offset 0 is contiguous

    // Worker B finishes offset 1
    tracker.complete(0, 1);

    committable = tracker.committable();
    expect(committable.get(0)).toBe(2); // 0,1 done, 2 still missing → commit 2

    // Worker D finishes offset 2 — fills the gap
    tracker.complete(0, 2);

    committable = tracker.committable();
    expect(committable.get(0)).toBe(5); // 2,3,4 all done → commit 5
  });

  it("realistic batch: 10 messages, middle one is slow", () => {
    const tracker = new OffsetTracker();

    // Fast messages complete immediately
    for (let i = 0; i < 10; i++) {
      if (i !== 5) tracker.complete(0, i); // offset 5 is slow
    }

    let committable = tracker.committable();
    expect(committable.get(0)).toBe(5); // 0-4 contiguous, 5 is the gap

    // Slow message finally completes
    tracker.complete(0, 5);

    committable = tracker.committable();
    expect(committable.get(0)).toBe(10); // 5-9 now contiguous from frontier
  });
});

describe("OffsetTracker — observe() frontier seeding", () => {
  it("commits from a non-zero starting offset (no stall on resume)", () => {
    const tracker = new OffsetTracker();
    // Consumer resumes from committed offset 5000 — broker delivers 5000 first.
    tracker.observe(0, 5000);
    tracker.complete(0, 5000);
    expect(tracker.committable().get(0)).toBe(5001);
  });

  it("seeds from the LOWEST observed offset regardless of arrival order", () => {
    const tracker = new OffsetTracker();
    // Out-of-order arrival into the pipe (e.g. after a parallel map): 102 first.
    tracker.observe(0, 102);
    tracker.observe(0, 100);
    tracker.observe(0, 101);
    tracker.complete(0, 100);
    tracker.complete(0, 101);
    tracker.complete(0, 102);
    expect(tracker.committable().get(0)).toBe(103);
  });

  it("self-heals on a rewind (rebalance/seek redelivers below the frontier)", () => {
    const tracker = new OffsetTracker();
    for (let o = 100; o <= 104; o++) {
      tracker.observe(0, o);
      tracker.complete(0, o);
    }
    expect(tracker.committable().get(0)).toBe(105);

    // Partition reassigned; broker redelivers from the actually-committed
    // offset 102 (below our frontier). observe() lowers it so commits resume.
    tracker.observe(0, 102);
    tracker.complete(0, 102);
    tracker.complete(0, 103);
    tracker.complete(0, 104);
    expect(tracker.committable().get(0)).toBe(105);
  });
});
