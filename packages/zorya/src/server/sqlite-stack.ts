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
  SqliteNamespaceRegistry,
  SqliteSchedulerStorage,
  SqliteSecretsStorage,
  SqliteSkillRegistry,
  SqliteWorkflowAdvertisementRegistry,
  SqliteWorkflowStartQueue,
  SqliteWorkflowStorage,
} from "@promin/sqlite";

export interface SqliteZoryaStackConfig {
  readonly path?: string;
  readonly secretsPassphrase?: string;
}

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
    secretsStorage: SqliteSecretsStorage.make({
      db: sqliteDb,
      passphrase: config.secretsPassphrase ?? "demo-only-passphrase-change-in-prod",
    }),
    workflowStarts: SqliteWorkflowStartQueue.make({ db: sqliteDb }),
    advertisements: SqliteWorkflowAdvertisementRegistry.make({ db: sqliteDb }),
  };
}
