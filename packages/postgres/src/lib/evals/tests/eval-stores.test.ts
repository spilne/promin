// ---------------------------------------------------------------------------
// Postgres eval stores — run the @promin/evals storage conformance suites
// and the VersionedRegistry conformance for the eval kinds against a real
// Postgres container.
// ---------------------------------------------------------------------------

import { afterEach, beforeAll, describe, expect, it } from "bun:test";
import { versionedRegistryKindTestSuite } from "@promin/agent/testing";
import {
  EVAL_GATE_FIXTURE,
  EVAL_SUITE_FIXTURE,
  evalDatasetStoreTestSuite,
  evalRunStoreTestSuite,
} from "@promin/evals/testing";
import { PostgresVersionedRegistry } from "../../agent/versioned-registry.ts";
import { migrate } from "../../migrate.ts";
import { PostgresTestContainer } from "../../test-utils.ts";
import { PostgresEvalDatasetStore } from "../eval-dataset-store.ts";
import { PostgresEvalSuiteRegistry } from "../eval-registries.ts";
import { PostgresEvalRunStore } from "../eval-run-store.ts";

const pg = new PostgresTestContainer();

beforeAll(async () => {
  await pg.start();
  await migrate(pg.db);
}, 60_000);

afterEach(async () => {
  await pg.sql`TRUNCATE eval_run_case, eval_run`;
  await pg.sql`TRUNCATE eval_dataset`;
  await pg.sql`TRUNCATE registry_head, registry_version`;
});

evalRunStoreTestSuite(() => new PostgresEvalRunStore({ db: pg.db }));
evalDatasetStoreTestSuite(() => new PostgresEvalDatasetStore({ db: pg.db }));
versionedRegistryKindTestSuite({
  name: "PostgresVersionedRegistry (eval suites)",
  factory: ({ kind, clock }) => new PostgresVersionedRegistry({ db: pg.db, kind, clock }),
  fixture: EVAL_SUITE_FIXTURE,
});
versionedRegistryKindTestSuite({
  name: "PostgresVersionedRegistry (eval gates)",
  factory: ({ kind, clock }) => new PostgresVersionedRegistry({ db: pg.db, kind, clock }),
  fixture: EVAL_GATE_FIXTURE,
});

describe("PostgresEvalSuiteRegistry", () => {
  it("lists suites written for an agent through the facet index", async () => {
    const suites = new PostgresEvalSuiteRegistry({ db: pg.db });
    const dataset = { kind: "inline" as const, id: "ds", cases: [{ id: "1", input: "a" }] };
    await suites.register({
      id: "for-bot",
      dataset,
      scorers: [{ kind: "exactMatch" }],
      metadata: { agentIds: ["bot"] },
    });
    await suites.register({ id: "other", dataset, scorers: [{ kind: "exactMatch" }] });
    const page = await suites.list({ filter: { agentId: "bot" } });
    expect(page.items.map((s) => s.id)).toEqual(["for-bot"]);
  });
});
