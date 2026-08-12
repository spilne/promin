import type { ActivityFactory, ActivityRegistry } from "./activity-registry.ts";
import type { JsonSchema, StepSchema } from "./workflow-schema.ts";

export type WorkflowStepTemplateKind = StepSchema["type"];

export interface WorkflowStepCatalogEntry {
  readonly id: string;
  readonly title: string;
  readonly description?: string;
  readonly category?: string;
  readonly tags?: ReadonlyArray<string>;
  readonly capabilities?: ReadonlyArray<string>;
  readonly kind?: WorkflowStepTemplateKind;
  readonly configSchema?: JsonSchema;
  readonly inputSchema?: JsonSchema;
  readonly outputSchema?: JsonSchema;
  readonly defaultConfig?: Readonly<Record<string, unknown>>;
  readonly icon?: string;
}

export interface WorkflowStepDefinition extends WorkflowStepCatalogEntry {
  readonly activity: ActivityFactory;
}

export interface WorkflowStepCatalog extends ActivityRegistry {
  register(definition: WorkflowStepDefinition): void;
  get(id: string): WorkflowStepCatalogEntry | null;
  entries(): WorkflowStepCatalogEntry[];
}

export class MapWorkflowStepCatalog implements WorkflowStepCatalog {
  private readonly definitions = new Map<string, WorkflowStepDefinition>();

  constructor(definitions: ReadonlyArray<WorkflowStepDefinition> = []) {
    for (const definition of definitions) this.register(definition);
  }

  register(definition: WorkflowStepDefinition): void {
    if (!definition.id.trim()) throw new Error("WorkflowStepCatalog: id is required");
    if (!definition.title.trim()) throw new Error("WorkflowStepCatalog: title is required");
    this.definitions.set(definition.id, cloneDefinition(definition));
  }

  get(id: string): WorkflowStepCatalogEntry | null {
    const definition = this.definitions.get(id);
    return definition ? toEntry(definition) : null;
  }

  entries(): WorkflowStepCatalogEntry[] {
    return [...this.definitions.values()]
      .map(toEntry)
      .sort(
        (a, b) => (a.category ?? "").localeCompare(b.category ?? "") || a.id.localeCompare(b.id),
      );
  }

  list(): string[] {
    return this.entries().map((entry) => entry.id);
  }

  has(ref: string): boolean {
    return this.definitions.has(ref);
  }

  resolve(ref: string, config?: Record<string, unknown>): ReturnType<ActivityRegistry["resolve"]> {
    const definition = this.definitions.get(ref);
    if (!definition) {
      throw new Error(
        `Workflow step "${ref}" not found in catalog. Available: ${this.list().join(", ")}`,
      );
    }
    return definition.activity({
      ...(definition.defaultConfig ?? {}),
      ...(config ?? {}),
    });
  }
}

export function createWorkflowStepCatalog(
  definitions: ReadonlyArray<WorkflowStepDefinition> = [],
): MapWorkflowStepCatalog {
  return new MapWorkflowStepCatalog(definitions);
}

function toEntry(definition: WorkflowStepDefinition): WorkflowStepCatalogEntry {
  const { activity: _activity, ...entry } = definition;
  return structuredClone(entry);
}

function cloneDefinition(definition: WorkflowStepDefinition): WorkflowStepDefinition {
  return {
    ...structuredClone(toEntry(definition)),
    activity: definition.activity,
  };
}
