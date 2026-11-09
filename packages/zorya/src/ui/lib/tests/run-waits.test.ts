import { describe, it, expect } from "bun:test";
import { waitChip } from "../run-waits.ts";
import type { RunWaitDto } from "../../../server/api-types.ts";

const wait = (extra: Partial<RunWaitDto> & Pick<RunWaitDto, "kind">): RunWaitDto => ({
  stepName: "charge",
  since: "2026-01-01T00:00:00.000Z",
  ...extra,
});

describe("waitChip", () => {
  it("names the step, the attempt and the holder of a running step", () => {
    expect(waitChip(wait({ kind: "activity", attempt: 2, holder: "worker-1" }))).toEqual({
      label: "running: charge #2 on worker-1",
      tone: "info",
    });
  });

  it("flags a retry backoff, and keeps parked waits neutral", () => {
    expect(waitChip(wait({ kind: "retry-backoff", attempt: 1 }))).toEqual({
      label: "retry backoff: charge #1",
      tone: "warning",
    });
    expect(waitChip(wait({ kind: "sleep", attempt: 3 }))).toEqual({
      label: "sleeping: charge",
      tone: "neutral",
    });
  });
});
