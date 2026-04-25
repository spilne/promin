// ---------------------------------------------------------------------------
// CoordinatedTriggerService — server-side trigger that hands off to the
// embedded WorkflowCoordinator instead of enqueueing to a workflow-start
// queue.
//
// Used when ZoryaServer is configured with `coordination: { enabled: true }`.
// On a trigger request the service:
//   1. Looks up the advertised DAG for the requested workflow name+version.
//   2. Builds a stub Workflow (no executable step bodies — the coordinator
//      delegates step execution to step-mode workers via the StepQueue).
//   3. Calls `coordinator.submit(...)`. The coordinator pre-creates the
//      storage row and enqueues ready steps; from there it's the worker
//      fleet's job to claim and execute.
//
// Falls back to a clear error when the workflow isn't advertised — without
// a DAG the coordinator can't enqueue anything.
// ---------------------------------------------------------------------------

import type { WorkflowStorage, WorkflowCoordinator, WorkflowDAG } from "@promin/workflow";
import { buildStubWorkflow } from "@promin/workflow";
import type { RunTrigger } from "../routes/runs.ts";
import type {
  AdvertisedWorkflow,
  WorkflowAdvertisementRegistry,
} from "../workflow-advertisements.ts";

export interface CoordinatedTriggerServiceDeps {
  storage: WorkflowStorage;
  coordinator: WorkflowCoordinator;
  /**
   * Worker advertisements — the source of truth for which workflows the
   * coordinator can run. Required: without an advertised DAG there's
   * nothing to dispatch.
   */
  advertisements: WorkflowAdvertisementRegistry;
}

export class CoordinatedTriggerService {
  constructor(private readonly deps: CoordinatedTriggerServiceDeps) {}

  readonly trigger: RunTrigger = async (name, input, options) => {
    const workflowId = options?.workflowId ?? crypto.randomUUID();
    const version = options?.version ?? (await this.resolveDefaultVersion(name));

    const advertised = await this.findAdvertised(name, version);
    if (!advertised) {
      throw new Error(
        `No advertised workflow "${name}"${version ? ` version "${version}"` : ""}. ` +
          `Coordinator-driven dispatch needs at least one connected worker advertising the DAG.`,
      );
    }

    const dag: WorkflowDAG = {
      name: advertised.name,
      steps: advertised.steps.map((s) => ({
        name: s.name,
        kind: s.kind,
        dependsOn: s.dependsOn,
        needs: s.needs,
        priority: s.priority,
      })),
    };
    const stub = buildStubWorkflow(dag, advertised.name, version);

    // The coordinator's `submit` pre-creates the workflow row itself; we
    // don't `storage.createWorkflow` here. Metadata threading goes through
    // the workflow definition's metadata via buildStubWorkflow → submit.
    await this.deps.coordinator.submit({ workflow: stub, workflowId, input });

    return { workflowId };
  };

  private async findAdvertised(
    name: string,
    version?: string,
  ): Promise<AdvertisedWorkflow | undefined> {
    const all = await this.deps.advertisements.distinct();
    if (version !== undefined) {
      return all.find((a) => a.name === name && a.version === version);
    }
    // No version requested — prefer an unversioned advertisement if one
    // exists, else fall back to the highest-versioned entry.
    const matches = all.filter((a) => a.name === name);
    const unversioned = matches.find((a) => !a.version);
    if (unversioned) return unversioned;
    return matches.sort((a, b) => (a.version ?? "").localeCompare(b.version ?? "")).pop();
  }

  private async resolveDefaultVersion(name: string): Promise<string | undefined> {
    const distinct = await this.deps.advertisements.distinct();
    const versions = distinct
      .filter((a) => a.name === name && !!a.version)
      .map((a) => a.version as string);
    if (versions.length === 0) return undefined;
    versions.sort();
    return versions[versions.length - 1];
  }
}
