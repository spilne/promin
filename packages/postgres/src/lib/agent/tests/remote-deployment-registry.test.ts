// ---------------------------------------------------------------------------
// PostgresRemoteDeploymentRegistry — the @promin/agent conformance suite
// against a real Postgres. Expiry runs on the database clock, so the suite
// moves time by shifting the stored stamps back instead of advancing a
// fake clock.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { remoteDeploymentRegistryTestSuite } from "@promin/agent/testing";
import { migrate } from "../../migrate.ts";
import { PostgresTestContainer } from "../../test-utils.ts";
import { PostgresRemoteDeploymentRegistry } from "../remote-deployment-registry.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE remote_deployment`;
});

remoteDeploymentRegistryTestSuite(() => ({
  registry: new PostgresRemoteDeploymentRegistry({ db: pg.db }),
  advance: async (ms) => {
    await pg.sql`
      UPDATE remote_deployment
      SET registered_at = registered_at - ${ms}, last_heartbeat = last_heartbeat - ${ms}`;
  },
}));

describe("PostgresRemoteDeploymentRegistry — replicas", () => {
  it("two replicas sweeping at once each report a stale deployment at most once", async () => {
    const a = new PostgresRemoteDeploymentRegistry({ db: pg.db });
    const b = new PostgresRemoteDeploymentRegistry({ db: pg.db });
    for (let i = 0; i < 5; i++) {
      await a.register({ endpoint: `https://r${i}.example`, agents: ["x"], ttlMs: 1_000 });
    }
    await pg.sql`UPDATE remote_deployment SET last_heartbeat = last_heartbeat - 5000`;
    const [fromA, fromB] = await Promise.all([a.expireStale(), b.expireStale()]);
    const ids = [...fromA, ...fromB].map((d) => d.deploymentId);
    expect(ids).toHaveLength(5);
    expect(new Set(ids).size).toBe(5);
  });
});
