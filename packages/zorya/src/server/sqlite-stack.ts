// ---------------------------------------------------------------------------
// createSqliteZoryaStack — every store a single-process Zorya needs, on one
// Bun SQLite database.
// ---------------------------------------------------------------------------

import { mkdirSync } from "node:fs";
import path from "node:path";
import { Database } from "bun:sqlite";
import {
  SystemWallClock,
  createWorkflowRunner,
  type WallClock,
  type WorkflowRunner,
} from "@promin/workflow";
import {
  type SqliteDatabase,
  SqliteAgentInstanceRegistry,
  SqliteAgentRegistry,
  SqliteDagRegistry,
  SqliteFragmentStore,
  SqliteKnowledgeBaseStore,
  SqliteMemoryStore,
  SqliteNamespaceRegistry,
  SqliteRoleRegistry,
  SqliteSchedulerStorage,
  SqliteSecretsStorage,
  SqliteSkillRegistry,
  SqliteWorkerRegistry,
  SqliteWorkflowAdvertisementRegistry,
  SqliteWorkflowSchemaStore,
  SqliteWorkflowStartQueue,
  SqliteWorkflowStorage,
} from "@promin/sqlite";

export interface SqliteZoryaStackConfig {
  /**
   * SQLite database file. Its parent directory is created when missing.
   * `":memory:"` gives an ephemeral stack.
   */
  readonly path: string;
  /**
   * Passphrase the secrets vault derives its encryption key from. Without
   * one the stack has no `secretsStorage`. Use a stable secret: secrets
   * written under one passphrase cannot be read under another.
   */
  readonly secretsPassphrase?: string;
  /** Time source for every store and the runner. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

/** What `createSqliteZoryaStack` returns. Every store shares `db`. */
export interface SqliteZoryaStack {
  readonly db: Database;
  /** Workflow runs, steps, signals and locks. */
  readonly storage: SqliteWorkflowStorage;
  /** A runner over `storage`. */
  readonly runner: WorkflowRunner;
  /** Schedules and the scheduler's leader lock (`ZoryaScheduler`). */
  readonly schedulerStorage: SqliteSchedulerStorage;
  /** The start queue remote workers claim from (`QueuedWorkflows`). */
  readonly workflowStarts: SqliteWorkflowStartQueue;
  /** What remote workers advertise (`QueuedWorkflows`). */
  readonly advertisements: SqliteWorkflowAdvertisementRegistry;
  /** Remote workers' registrations and heartbeats (`QueuedWorkflows`). */
  readonly workerRegistry: SqliteWorkerRegistry;
  /** The namespace registry (`ZoryaServerConfig.namespaces`). */
  readonly namespaceRegistry: SqliteNamespaceRegistry;
  readonly agentRegistry: SqliteAgentRegistry;
  readonly instanceRegistry: SqliteAgentInstanceRegistry;
  readonly roleRegistry: SqliteRoleRegistry;
  readonly skillRegistry: SqliteSkillRegistry;
  readonly fragmentStore: SqliteFragmentStore;
  readonly knowledgeBaseStore: SqliteKnowledgeBaseStore;
  readonly memoryStore: SqliteMemoryStore;
  readonly dagRegistry: SqliteDagRegistry;
  /** Drafts and published versions of builder workflows (`ZoryaWorkflowBuilder`). */
  readonly workflowSchemaStore: SqliteWorkflowSchemaStore;
  /** Present when `secretsPassphrase` was given. */
  readonly secretsStorage?: SqliteSecretsStorage;
}

/**
 * Open (or create) one SQLite database with WAL and foreign keys on, and
 * build the workflow, scheduler, worker-protocol, namespace, agent and secrets stores
 * on it, plus a runner over the workflow storage. For local apps, demos,
 * tests and small single-process deployments; larger ones build each store
 * themselves (or replace some of these, e.g. agent state on Postgres).
 *
 * ```ts
 * import { createSqliteZoryaStack } from "@promin/zorya/sqlite";
 *
 * const stack = createSqliteZoryaStack({
 *   path: "./data/zorya.db",
 *   secretsPassphrase: process.env.ZORYA_SECRETS_PASSPHRASE,
 * });
 * const workflows = new LocalWorkflows({
 *   storage: stack.storage,
 *   runner: stack.runner,
 *   definitions,
 * });
 * ```
 */
export function createSqliteZoryaStack(
  config: SqliteZoryaStackConfig & { readonly secretsPassphrase: string },
): SqliteZoryaStack & { readonly secretsStorage: SqliteSecretsStorage };
export function createSqliteZoryaStack(config: SqliteZoryaStackConfig): SqliteZoryaStack;
export function createSqliteZoryaStack(config: SqliteZoryaStackConfig): SqliteZoryaStack {
  if (config.path !== ":memory:") mkdirSync(path.dirname(config.path), { recursive: true });
  const clock = config.clock ?? SystemWallClock;
  const now = () => clock.currentTimeMs();

  const db = new Database(config.path);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA foreign_keys = ON");
  const sqlite: SqliteDatabase = db;

  const storage = SqliteWorkflowStorage.make({ db: sqlite, clock });
  return {
    db,
    storage,
    runner: createWorkflowRunner({ storage, clock }),
    schedulerStorage: SqliteSchedulerStorage.make({ db: sqlite, clock }),
    workflowStarts: SqliteWorkflowStartQueue.make({ db: sqlite, clock }),
    advertisements: SqliteWorkflowAdvertisementRegistry.make({ db: sqlite, clock }),
    workerRegistry: SqliteWorkerRegistry.make({ db: sqlite, clock }),
    namespaceRegistry: SqliteNamespaceRegistry.make({ db: sqlite, clock }),
    agentRegistry: SqliteAgentRegistry.make({ db: sqlite, now }),
    instanceRegistry: SqliteAgentInstanceRegistry.make({ db: sqlite, now }),
    roleRegistry: SqliteRoleRegistry.make({ db: sqlite, now }),
    skillRegistry: SqliteSkillRegistry.make({ db: sqlite, now }),
    fragmentStore: SqliteFragmentStore.make({ db: sqlite, now }),
    knowledgeBaseStore: SqliteKnowledgeBaseStore.make({ db: sqlite }),
    memoryStore: SqliteMemoryStore.make({ db: sqlite, now }),
    dagRegistry: SqliteDagRegistry.make({ db: sqlite, now }),
    workflowSchemaStore: SqliteWorkflowSchemaStore.make({ db: sqlite }),
    ...(config.secretsPassphrase !== undefined && {
      secretsStorage: SqliteSecretsStorage.make({
        db: sqlite,
        passphrase: config.secretsPassphrase,
        now,
      }),
    }),
  };
}
