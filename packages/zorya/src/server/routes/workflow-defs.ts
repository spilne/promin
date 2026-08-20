// ---------------------------------------------------------------------------
// Workflow definitions — expose registered workflows as DTOs so the UI can
// render a "Workflows" directory page and synthesise trigger forms from
// sample inputs.
// ---------------------------------------------------------------------------

import type {
  IWorkflowVersionRegistry,
  JsonSchema,
  Workflow,
  WorkflowSchema,
} from "@promin/workflow";
import { json, jsonError } from "../router.ts";
import type { WorkflowAdvertisementRegistry } from "../workflow-advertisements.ts";
import type { ZoryaWorkflowBuilder } from "../services/workflow-builder.ts";

export interface WorkflowStepDefDto {
  name: string;
  kind: string;
  dependsOn: readonly string[];
}

export interface WorkflowDefDto {
  name: string;
  type?: string;
  /** Primary version (the one the dashboard renders the DAG for). */
  version?: string;
  /**
   * Every distinct version known for this workflow — primary plus any
   * other versions advertised by connected workers. Used by the trigger
   * modal to let users pick which version to run.
   */
  versions?: readonly string[];
  steps: WorkflowStepDefDto[];
  /** Example input shape, used to auto-build trigger forms. Optional. */
  sampleInput?: unknown;
}

export interface WorkflowDefsResponse {
  workflows: WorkflowDefDto[];
}

export interface WorkflowDefsDeps {
  workflows?: Readonly<Record<string, Workflow<unknown, unknown>>>;
  /**
   * Returns a sample input for a given workflow name. Used by the UI to
   * pre-fill trigger forms with plausible defaults so users don't have to
   * start from raw JSON. Returning `undefined` means "no sample".
   */
  sampleInput?: (workflowName: string) => unknown;
  /**
   * Remote advertisements from connected workers. Merged into the response
   * alongside any statically-configured workflows. When both a static and
   * an advertised entry exist for the same name, the static one wins.
   */
  advertisements?: WorkflowAdvertisementRegistry;
  /**
   * Published authored workflows live in the version registry. Merge them
   * into the same definitions directory so trigger/detail pages don't need
   * a separate path for builder-created workflows.
   */
  versionRegistry?: IWorkflowVersionRegistry;
  /** Optional authored-workflow source used to infer samples for builder-published workflows. */
  workflowBuilder?: ZoryaWorkflowBuilder;
}

function toDto(name: string, wf: Workflow<unknown, unknown>, sample?: unknown): WorkflowDefDto {
  return {
    name,
    type: wf._definition.type,
    version: wf.version,
    steps: wf.dag.steps.map((s) => ({ name: s.name, kind: s.kind, dependsOn: s.dependsOn })),
    sampleInput: sample,
  };
}

export function listWorkflowDefs(deps: WorkflowDefsDeps) {
  return async (): Promise<Response> => {
    const byName = new Map<string, WorkflowDefDto>();
    for (const [name, wf] of Object.entries(deps.workflows ?? {})) {
      byName.set(name, toDto(name, wf, deps.sampleInput?.(name)));
    }
    // Merge in remote advertisements — static wins on conflict (embedded
    // workflow definitions are authoritative over worker hearsay).
    if (deps.advertisements) {
      const distinct = await deps.advertisements.distinct();
      for (const adv of distinct) {
        if (byName.has(adv.name)) continue;
        byName.set(adv.name, {
          name: adv.name,
          version: adv.version,
          steps: adv.steps.map((s) => ({
            name: s.name,
            kind: s.kind,
            dependsOn: [...s.dependsOn],
          })),
          sampleInput: adv.sampleInput,
        });
      }
      // Annotate every dto with the full set of advertised versions seen
      // for that name across all connected workers. Lets the trigger
      // modal offer a version dropdown.
      const versionsByName = new Map<string, Set<string>>();
      for (const adv of distinct) {
        if (!adv.version) continue;
        let set = versionsByName.get(adv.name);
        if (!set) {
          set = new Set();
          versionsByName.set(adv.name, set);
        }
        set.add(adv.version);
      }
      for (const dto of byName.values()) {
        const set = versionsByName.get(dto.name) ?? new Set<string>();
        if (dto.version) set.add(dto.version);
        if (set.size > 0) dto.versions = [...set].sort();
      }
    }

    if (deps.versionRegistry) {
      for (const name of await deps.versionRegistry.names()) {
        const versions = await deps.versionRegistry.versions(name);
        const existing = byName.get(name);
        if (existing) {
          annotateVersions(existing, versions);
          continue;
        }
        const wf = await resolveRegisteredWorkflow(deps.versionRegistry, name);
        if (!wf) continue;
        const dto = toDto(name, wf, await resolveSampleInput(name, wf.version, deps));
        annotateVersions(dto, versions);
        byName.set(name, dto);
      }
    }

    const dtos = [...byName.values()].sort((a, b) => a.name.localeCompare(b.name));
    const response: WorkflowDefsResponse = { workflows: dtos };
    return json(200, response);
  };
}

export function getWorkflowDef(deps: WorkflowDefsDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const name = params.name;
    if (!name) return jsonError(400, "missing_name");
    const wf = deps.workflows?.[name];
    if (wf) return json(200, toDto(name, wf, deps.sampleInput?.(name)));

    if (deps.versionRegistry) {
      const registered = await resolveRegisteredWorkflow(deps.versionRegistry, name);
      if (registered) {
        const dto = toDto(
          name,
          registered,
          await resolveSampleInput(name, registered.version, deps),
        );
        const versions = await deps.versionRegistry.versions(name);
        annotateVersions(dto, versions);
        return json(200, dto);
      }
    }

    // Fall back to remote advertisements.
    if (deps.advertisements) {
      const distinct = await deps.advertisements.distinct();
      const adv = distinct.find((a) => a.name === name);
      if (adv) {
        return json(200, {
          name: adv.name,
          version: adv.version,
          steps: adv.steps.map((s) => ({
            name: s.name,
            kind: s.kind,
            dependsOn: [...s.dependsOn],
          })),
          sampleInput: adv.sampleInput,
        });
      }
    }
    return jsonError(404, "not_found");
  };
}

async function resolveRegisteredWorkflow(
  registry: IWorkflowVersionRegistry,
  name: string,
): Promise<Workflow<unknown, unknown> | undefined> {
  const active = await registry.findActive?.(name);
  if (active) return registry.resolve(name, active.version);
  const latest = await registry.latest(name);
  return latest ? registry.resolve(name, latest) : undefined;
}

async function resolveSampleInput(
  name: string,
  version: string | undefined,
  deps: WorkflowDefsDeps,
): Promise<unknown> {
  const configured = deps.sampleInput?.(name);
  if (configured !== undefined) return configured;
  if (!deps.workflowBuilder) return undefined;
  const record = await deps.workflowBuilder.get(name, version);
  if (!record) return undefined;
  return inferSampleInput(record.schema, deps.workflowBuilder);
}

function inferSampleInput(schema: WorkflowSchema, builder: ZoryaWorkflowBuilder): unknown {
  if (schema.inputSchema) return sampleFromSchema(schema.inputSchema);
  const catalog = new Map(builder.steps().map((entry) => [entry.id, entry]));
  for (const step of schema.steps) {
    if (!("dependsOn" in step) || step.dependsOn.length > 0 || !("activityRef" in step)) continue;
    const entry = catalog.get(step.activityRef);
    if (entry?.inputSchema) return sampleFromSchema(entry.inputSchema);
  }
  return undefined;
}

function sampleFromSchema(schema: JsonSchema | undefined): unknown {
  if (!schema) return undefined;
  if (schema.default !== undefined) return schema.default;
  if (schema.enum?.length) return schema.enum[0];
  if (schema.type === "string") return "hello workflow";
  if (schema.type === "number" || schema.type === "integer") return 1;
  if (schema.type === "boolean") return true;
  if (schema.type === "array") return [sampleFromSchema(schema.items)];
  if (schema.type === "object") {
    const out: Record<string, unknown> = {};
    for (const [name, property] of Object.entries(schema.properties ?? {})) {
      out[name] = sampleFromSchema(property);
    }
    return out;
  }
  return undefined;
}

function annotateVersions(dto: WorkflowDefDto, versions: readonly string[]): void {
  const set = new Set([...(dto.versions ?? []), ...versions]);
  if (dto.version) set.add(dto.version);
  if (set.size > 0) dto.versions = [...set].sort();
}
