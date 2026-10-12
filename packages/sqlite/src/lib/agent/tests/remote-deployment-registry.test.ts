import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { remoteDeploymentRegistryTestSuite } from "@promin/agent/testing";
import { FakeWallClock } from "@promin/workflow";
import { SqliteRemoteDeploymentRegistry } from "../remote-deployment-registry.ts";

remoteDeploymentRegistryTestSuite(() => {
  const clock = FakeWallClock.create(1_700_000_000_000);
  return {
    registry: new SqliteRemoteDeploymentRegistry({ db: new Database(":memory:"), clock }),
    advance: (ms) => clock.advance(ms),
    now: () => clock.currentTimeMs(),
  };
});

describe("SqliteRemoteDeploymentRegistry — persistence", () => {
  it("a registration survives re-opening the registry, auth included", async () => {
    const db = new Database(":memory:");
    const clock = FakeWallClock.create(1_000);
    const first = new SqliteRemoteDeploymentRegistry({ db, clock });
    const registered = await first.register({
      endpoint: "https://b.example",
      agents: ["billing"],
      auth: { kind: "bearer", token: "tok" },
      ttlMs: 30_000,
    });
    const reopened = new SqliteRemoteDeploymentRegistry({ db, clock });
    expect(await reopened.get(registered.deploymentId)).toEqual(registered);
    expect(await reopened.list()).toEqual([registered]);
  });
});
