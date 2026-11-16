import { FakeWallClock } from "@promin/workflow";
import { postgresDescribe } from "@promin/postgres/testing";
import { execRaw } from "@spilne/perfect-postgres";
import { sql } from "drizzle-orm";
import { securityStoreV2TestSuite } from "../../security-module/tests/security-store-v2-test-suite.ts";
import { migrateSecurityStore } from "../migrate.ts";
import { PostgresSecurityStore } from "../security-store.ts";

postgresDescribe(
  "PostgresSecurityStore",
  { migrate: migrateSecurityStore, timeout: 120_000 },
  (pg) => {
    async function reset(): Promise<void> {
      await execRaw(
        pg.db,
        sql`TRUNCATE zsec_break_glass_grant, zsec_denial_bucket, zsec_audit_outbox,
          zsec_audit_record, zsec_session, zsec_service_account_credential,
          zsec_knowledge_audience_grant, zsec_access_binding, zsec_group_membership,
          zsec_provider_identity, zsec_service_account, zsec_access_group,
          zsec_user, zsec_subject CASCADE`,
      );
    }

    securityStoreV2TestSuite("v2 conformance", {
      create: async () => {
        await reset();
        return new PostgresSecurityStore({ db: pg.db });
      },
      createClocked: async () => {
        await reset();
        const clock = FakeWallClock.create(1_000);
        return {
          store: new PostgresSecurityStore({ db: pg.db, clock }),
          currentTimeMs: () => clock.currentTimeMs(),
          advance: (ms: number) => clock.advance(ms),
        };
      },
    });
  },
);
