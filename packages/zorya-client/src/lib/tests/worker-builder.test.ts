import { describe, expect, it } from "bun:test";
import { FakeWallClock, workflow } from "@promin/workflow";
import { ZoryaWorker, ZoryaWorkerBuilder, type ZoryaWorkerConfig } from "../worker.ts";
import { ZoryaClient } from "../zorya-client.ts";

const client = new ZoryaClient({
  url: "http://zorya.test",
  fetch: () => Promise.reject(new Error("no network in this test")),
});

const greet = workflow<{ name: string }>({ name: "greet" })
  .step("greet", ({ input }) => `hello ${input.name}`)
  .build();
const count = workflow<number>({ name: "count" })
  .step("inc", async ({ input }) => input + 1)
  .build();
const audit = workflow<null>({ name: "audit" })
  .step("noop", () => null)
  .build();

describe("ZoryaWorker.builder()", () => {
  it("returns a ZoryaWorkerBuilder", () => {
    expect(ZoryaWorker.builder()).toBeInstanceOf(ZoryaWorkerBuilder);
  });

  it("requires a client", () => {
    expect(() => ZoryaWorker.builder().workflow(greet).build()).toThrow("client() is required");
  });

  it("requires at least one workflow", () => {
    expect(() => ZoryaWorker.builder().client(client).build()).toThrow(
      "at least one workflow() is required",
    );
    expect(() => ZoryaWorker.builder().client(client).workflows().buildConfig()).toThrow(
      "at least one workflow() is required",
    );
  });

  it("accumulates workflow() and workflows() calls in order", () => {
    const config = ZoryaWorker.builder()
      .client(client)
      .workflow(greet)
      .workflows(count, audit)
      .workflow(greet)
      .buildConfig();
    expect(config.workflows).toEqual([greet, count, audit, greet]);
  });

  it("sets every config field", () => {
    const clock = FakeWallClock.create(0);
    const sampleInput = (name: string) => ({ for: name });
    const onError = () => {};
    const expected: Required<ZoryaWorkerConfig> = {
      client,
      workflows: [greet],
      workerId: "w-1",
      capabilities: ["gpu"],
      concurrency: 3,
      heartbeatIntervalMs: 2_000,
      pollWorkflowStarts: { intervalMs: 100, jitter: 0, claimHeartbeatMs: 1_000 },
      lockedRetryDelayMs: 250,
      lock: { durationMs: 60_000, heartbeatIntervalMs: 10_000 },
      clock,
      sampleInput,
      version: "2.1.0",
      labels: { region: "eu" },
      namespaces: ["tenant-a"],
      metadata: { team: "core" },
      controlSocket: { reconnectDelayMs: 10 },
      onError,
    };

    const config = ZoryaWorker.builder()
      .client(expected.client)
      .workflow(greet)
      .workerId(expected.workerId)
      .capabilities(expected.capabilities)
      .concurrency(expected.concurrency)
      .heartbeatIntervalMs(expected.heartbeatIntervalMs)
      .pollWorkflowStarts(expected.pollWorkflowStarts)
      .lockedRetryDelayMs(expected.lockedRetryDelayMs)
      .lock(expected.lock)
      .clock(expected.clock)
      .sampleInput(expected.sampleInput)
      .version(expected.version)
      .labels(expected.labels)
      .namespaces(expected.namespaces)
      .metadata(expected.metadata)
      .controlSocket(expected.controlSocket)
      .onError(expected.onError)
      .buildConfig();

    expect(config).toEqual(expected);
    // Every field of the config has a setter.
    expect(Object.keys(config).sort()).toEqual(Object.keys(expected).sort());
  });

  it("keeps the last value of a repeated setter", () => {
    const config = ZoryaWorker.builder()
      .client(client)
      .workflow(greet)
      .pollWorkflowStarts({ intervalMs: 1 })
      .pollWorkflowStarts(false)
      .controlSocket(true)
      .controlSocket(false)
      .buildConfig();
    expect(config.pollWorkflowStarts).toBe(false);
    expect(config.controlSocket).toBe(false);
  });

  it("builds a worker from the config", () => {
    const worker = ZoryaWorker.builder()
      .client(client)
      .workflows(greet, count)
      .workerId("built")
      .controlSocket(false)
      .pollWorkflowStarts(false)
      .clock(FakeWallClock.create(0))
      .build();
    expect(worker).toBeInstanceOf(ZoryaWorker);
    expect(worker.workerId).toBe("built");
    expect(worker.client).toBe(client);
    expect(worker.control).toBeUndefined();
  });
});
