// ---------------------------------------------------------------------------
// PostgresNamespaceRegistry — the @promin/workflow conformance suite against
// a real Postgres container (testcontainers). The registry stamps times with
// the server's NOW(), so the suite checks timestamp ordering only.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { sql } from "drizzle-orm";
import { namespaceRegistryTestSuite } from "@promin/workflow/testing";
import { migrate } from "../migrate.ts";
import { PostgresNamespaceRegistry } from "../postgres-namespace-registry.ts";
import { postgresDescribe } from "../test-utils.ts";

postgresDescribe("PostgresNamespaceRegistry", { migrate }, (pg) => {
  namespaceRegistryTestSuite({
    factory: async () => {
      await pg.db.execute(sql`TRUNCATE TABLE wf_namespace_registry`);
      return new PostgresNamespaceRegistry({ db: pg.db });
    },
    serverClock: true,
  });

  describe("Postgres-specific", () => {
    it("rows persist across registry instances against the same db", async () => {
      await pg.db.execute(sql`TRUNCATE TABLE wf_namespace_registry`);
      const r1 = new PostgresNamespaceRegistry({ db: pg.db });
      await r1.create({
        id: "acme",
        displayName: "Acme",
        capabilities: { workflows: { maxConcurrentRuns: 3 } },
      });

      const r2 = new PostgresNamespaceRegistry({ db: pg.db });
      const got = await r2.get("acme");
      expect(got?.displayName).toBe("Acme");
      expect(got?.capabilities.workflows?.maxConcurrentRuns).toBe(3);
    });

    it("the CHECK constraint refuses a status the registry does not know", async () => {
      const err = await (async () =>
        pg.db.execute(
          sql`INSERT INTO wf_namespace_registry (id, display_name, status) VALUES ('x', 'x', 'deleted')`,
        ))().then(
        () => undefined,
        (e: unknown) => e,
      );
      expect(err).toBeDefined();
    });
  });
});
