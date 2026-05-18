// ---------------------------------------------------------------------------
// RegistryBackedWorkersProvider — maps WorkerRegistry rows to WorkerDto.
// Pins the retired-worker mapping: a gracefully-stopped worker surfaces as
// its own `retired` status, distinct from a crashed worker's `offline`.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { InMemoryWorkerRegistry } from "@promin/workflow";
import { RegistryBackedWorkersProvider } from "../../server/routes/workers.ts";

describe("RegistryBackedWorkersProvider", () => {
  it("maps a fresh active worker to status=online", async () => {
    const reg = new InMemoryWorkerRegistry();
    await reg.register({ workerId: "w-1", capabilities: [], concurrency: 1 });

    const [dto] = await new RegistryBackedWorkersProvider(reg).listWorkers();
    expect(dto?.status).toBe("online");
    expect(dto?.retiredAt).toBeUndefined();
  });

  it("maps a retired worker to status=retired with a retiredAt timestamp", async () => {
    const reg = new InMemoryWorkerRegistry();
    await reg.register({ workerId: "w-1", capabilities: [], concurrency: 1 });
    await reg.deregister("w-1"); // graceful stop → retired

    const [dto] = await new RegistryBackedWorkersProvider(reg).listWorkers();
    expect(dto?.status).toBe("retired");
    expect(typeof dto?.retiredAt).toBe("string");
  });

  it("a stale-heartbeat active worker is offline, not retired", async () => {
    const reg = new InMemoryWorkerRegistry();
    await reg.register({ workerId: "w-1", capabilities: [], concurrency: 1 });

    // offlineAfterMs=0 → any heartbeat age reads as stale.
    const [dto] = await new RegistryBackedWorkersProvider(reg, 0).listWorkers();
    expect(dto?.status).toBe("offline");
    expect(dto?.retiredAt).toBeUndefined();
  });
});
