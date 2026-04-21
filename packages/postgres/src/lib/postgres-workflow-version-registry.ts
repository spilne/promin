// ---------------------------------------------------------------------------
// PostgresWorkflowVersionRegistry — shared workflow definition store
//
// Stores serialized WorkflowDAG entries in Postgres so multiple coordinator
// processes can resolve definitions without having them in-process. Workers
// still hold the real step handlers locally — only the DAG structure (step
// names, dependencies, kinds, needs, priorities) is persisted here.
//
// On resolve(), reconstructs a stub Workflow with placeholder execute
// functions. Coordinators use this for DAG-only orchestration (dispatch
// mode); they never call the stub execute bodies.
// ---------------------------------------------------------------------------

import { eq } from "drizzle-orm";
import { Pipeline, LosslessJsonCodec, type TaggedError } from "@promin/core";
import type {
  Workflow,
  WorkflowDAG,
  IdempotencyConfig,
  IWorkflowVersionRegistry,
} from "@promin/workflow";
import type { DrizzleDb } from "./drizzle-db.ts";
import { workflowRegistry } from "./schema.ts";

export class PostgresWorkflowVersionRegistry implements IWorkflowVersionRegistry {
  constructor(private readonly db: DrizzleDb) {}

  async register(definition: Workflow<unknown, unknown>): Promise<void> {
    const { name, version } = definition;
    if (!version) {
      throw new Error(
        `PostgresWorkflowVersionRegistry.register: workflow "${name}" must have a version. ` +
          `Set it via \`workflow({ name, version: "1" })\`.`,
      );
    }
    await this.db
      .insert(workflowRegistry)
      .values({
        name,
        version,
        dagJson: definition.dag as unknown as Record<string, unknown>,
        idempotency: definition.idempotency
          ? (definition.idempotency as unknown as Record<string, unknown>)
          : null,
      })
      .onConflictDoUpdate({
        target: [workflowRegistry.name, workflowRegistry.version],
        set: {
          dagJson: definition.dag as unknown as Record<string, unknown>,
          idempotency: definition.idempotency
            ? (definition.idempotency as unknown as Record<string, unknown>)
            : null,
        },
      });
  }

  async resolve(name: string, version?: string): Promise<Workflow<unknown, unknown> | undefined> {
    if (version) {
      const rows = await this.db
        .select()
        .from(workflowRegistry)
        .where(eq(workflowRegistry.name, name) && (eq(workflowRegistry.version, version) as any))
        .limit(1);
      const row = rows[0];
      if (!row) return undefined;
      return rowToWorkflow(row);
    }

    // No version — pick latest by registered_at DESC
    const rows = await this.db
      .select()
      .from(workflowRegistry)
      .where(eq(workflowRegistry.name, name))
      .orderBy(workflowRegistry.registeredAt)
      .limit(100);
    if (rows.length === 0) return undefined;
    return rowToWorkflow(rows[rows.length - 1]!);
  }

  async versions(name: string): Promise<readonly string[]> {
    const rows = await this.db
      .select({ version: workflowRegistry.version })
      .from(workflowRegistry)
      .where(eq(workflowRegistry.name, name))
      .orderBy(workflowRegistry.registeredAt);
    return rows.map((r) => r.version);
  }

  async latest(name: string): Promise<string | undefined> {
    const rows = await this.db
      .select({ version: workflowRegistry.version })
      .from(workflowRegistry)
      .where(eq(workflowRegistry.name, name))
      .orderBy(workflowRegistry.registeredAt)
      .limit(100);
    return rows.length > 0 ? rows[rows.length - 1]!.version : undefined;
  }

  async names(): Promise<readonly string[]> {
    const rows = await this.db
      .selectDistinct({ name: workflowRegistry.name })
      .from(workflowRegistry)
      .orderBy(workflowRegistry.name);
    return rows.map((r) => r.name);
  }

  async deregister(name: string, version: string): Promise<void> {
    await this.db
      .delete(workflowRegistry)
      .where(eq(workflowRegistry.name, name) && (eq(workflowRegistry.version, version) as any));
  }
}

// ---------------------------------------------------------------------------
// Reconstruct a stub Workflow from a registry row
// ---------------------------------------------------------------------------

function rowToWorkflow(row: {
  name: string;
  version: string;
  dagJson: unknown;
  idempotency: unknown;
}): Workflow<unknown, unknown> {
  const dag = row.dagJson as WorkflowDAG;
  const idempotency = row.idempotency as IdempotencyConfig | undefined | null;

  // Build stub StepDefinitions from the DAG — execute bodies throw because
  // in dispatch mode the coordinator never calls them; workers execute steps
  // locally using their own step-handler registries.
  const steps = dag.steps.map((s) => ({
    name: s.name,
    dependsOn: [...s.dependsOn],
    kind: s.kind,
    codec: LosslessJsonCodec,
    needs: s.needs,
    priority: s.priority,
    execute: () =>
      Pipeline.fail({
        _tag: "StepError",
        message:
          `Step "${s.name}" of workflow "${dag.name}" cannot be executed via a registry-fetched stub. ` +
          `This workflow was loaded from PostgresWorkflowVersionRegistry (dispatch-only mode). ` +
          `Workers run steps locally using their own step-handler registries.`,
      } as TaggedError),
  }));

  return {
    name: dag.name,
    version: row.version,
    dag,
    idempotency: idempotency ?? undefined,
    _definition: {
      steps,
      onVersionMismatch: "strict",
    },
  } as unknown as Workflow<unknown, unknown>;
}
