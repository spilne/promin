import { describe, expect, it } from "bun:test";
import type { WorkflowStorage } from "@promin/workflow";
import {
  validateWorkflowRetentionConfig,
  WorkflowRetentionCleaner,
} from "../workflow-retention.ts";

const logger = {
  log: () => {},
  warn: () => {},
  error: () => {},
};

describe("WorkflowRetentionCleaner", () => {
  it("validates retention settings", () => {
    expect(() => validateWorkflowRetentionConfig({ maxAgeDays: 0 })).toThrow(
      "maxAgeDays must be a positive finite number",
    );
    expect(() => validateWorkflowRetentionConfig({ maxAgeDays: 1, batchSize: 1.5 })).toThrow(
      "batchSize must be a positive integer",
    );
    expect(() => validateWorkflowRetentionConfig({ maxAgeDays: 7, intervalMs: 100 })).not.toThrow();
  });

  it("runs an immediate bounded sweep and stops periodic work", async () => {
    const calls: Array<{ olderThanMs: number; limit: number }> = [];
    const storage = {
      purgeCompleted: async (params: { olderThanMs: number; limit: number }) => {
        calls.push(params);
        return 1;
      },
    } as unknown as WorkflowStorage;
    const cleaner = new WorkflowRetentionCleaner(
      storage,
      { maxAgeDays: 3, intervalMs: 5, batchSize: 7 },
      logger,
    );

    cleaner.start();
    await Bun.sleep(1);
    cleaner.stop();
    const countAfterStop = calls.length;
    await Bun.sleep(10);

    expect(calls.length).toBe(countAfterStop);
    expect(calls[0]).toEqual({ olderThanMs: 3 * 24 * 60 * 60 * 1000, limit: 7 });
  });
});
