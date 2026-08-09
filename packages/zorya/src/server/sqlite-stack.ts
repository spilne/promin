import { mkdirSync } from "node:fs";
import path from "node:path";
import { Database } from "bun:sqlite";
import { createWorkflowRunner } from "@promin/workflow";
import {
  type SqliteDatabase,
  SqliteAgentInstanceRegistry,
  SqliteAgentRegistry,
  SqliteDagRegistry,
  SqliteFragmentStore,
  SqliteMemoryStore,
  SqliteKnowledgeBaseStore,
  SqliteNamespaceRegistry,
  SqliteSchedulerStorage,
  SqliteSecretsStorage,
  SqliteSkillRegistry,
  SqliteWorkflowAdvertisementRegistry,
  SqliteWorkflowStartQueue,
  SqliteWorkflowStorage,
} from "@promin/sqlite";

export interface SqliteZoryaStackConfig {
  /**
   * SQLite database path. Defaults to `./target/zorya.db`.
   * Use `":memory:"` for an ephemeral stack.
   */
  readonly path?: string;
  /**
   * Passphrase for the SQLite secrets vault.
   * Production hosts should provide their own stable secret.
   */
  readonly secretsPassphrase?: string;
}

/**
 * Create the common single-process SQLite Zorya stack.
 *
 * This is a convenience for local apps, demos, tests, and small self-hosted
 * deployments. It creates one Bun SQLite connection, enables WAL/foreign keys,
 * and returns matching workflow, scheduler, namespace, agent, skill, fragment,
 * DAG, memory, secrets, worker-start, and advertisement stores plus a workflow
 * runner bound to the workflow storage.
 *
 * Advanced hosts can still construct each store manually, or swap selected
 * stores after creation, as the demo does for Postgres-backed agent state.
 *
 * @example
 * ```ts
 * const stack = createSqliteZoryaStack({ path: "./target/zorya.db" });
 * const workflows = new LocalWorkflows({
 *   storage: stack.storage,
 *   runner: stack.runner,
 *   definitions,
 * });
 * ```
 */
export function createSqliteZoryaStack(config: SqliteZoryaStackConfig = {}) {
  const dbPath = config.path ?? "./target/zorya.db";
  if (dbPath !== ":memory:") {
    mkdirSync(path.dirname(dbPath), { recursive: true });
  }

  const db = new Database(dbPath);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA foreign_keys = ON");
  const sqliteDb = db as unknown as SqliteDatabase;

  const storage = SqliteWorkflowStorage.make({ db: sqliteDb });
  return {
    dbPath,
    db,
    storage,
    runner: createWorkflowRunner({ storage }),
    namespaceRegistry: SqliteNamespaceRegistry.make({ db: sqliteDb }),
    schedulerStorage: SqliteSchedulerStorage.make({ db: sqliteDb }),
    agentRegistry: SqliteAgentRegistry.make({ db: sqliteDb }),
    skillRegistry: SqliteSkillRegistry.make({ db: sqliteDb }),
    fragmentStore: SqliteFragmentStore.make({ db: sqliteDb }),
    dagRegistry: SqliteDagRegistry.make({ db: sqliteDb }),
    instanceRegistry: SqliteAgentInstanceRegistry.make({ db: sqliteDb }),
    memoryStore: SqliteMemoryStore.make({ db: sqliteDb }),
    knowledgeBaseStore: SqliteKnowledgeBaseStore.make({ db: sqliteDb }),
    secretsStorage: SqliteSecretsStorage.make({
      db: sqliteDb,
      passphrase: config.secretsPassphrase ?? "demo-only-passphrase-change-in-prod",
    }),
    workflowStarts: SqliteWorkflowStartQueue.make({ db: sqliteDb }),
    advertisements: SqliteWorkflowAdvertisementRegistry.make({ db: sqliteDb }),
  };
}
