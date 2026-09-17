// ---------------------------------------------------------------------------
// ZoryaClient — connects a worker process / admin tool to a remote Zorya
// server. Wraps RemoteWorkflowStorage + RemoteStepQueue + RemoteWorkerRegistry
// behind a single ergonomic object, and knows how to advertise workflow
// definitions to the server on connect.
// ---------------------------------------------------------------------------

import type {
  Workflow,
  WorkflowStorage,
  StepQueue,
  WorkerRegistry,
  WorkflowHandle,
  WorkflowRunner,
} from "@promin/workflow";
import { createWorkflowRunner } from "@promin/workflow";
import {
  RemoteStepQueue,
  RemoteWorkerRegistry,
  RemoteWorkflowStorage,
  type FetchLike,
} from "@promin/workflow-remote";

export interface ZoryaClientConfig {
  /**
   * Base URL of the Zorya server, e.g. "https://zorya.internal:4100".
   * Client appends "/rpc/storage", "/rpc/worker", and "/api/advertisements"
   * as needed.
   */
  url: string;
  /** Bearer token for the server's auth layer. Optional. */
  apiKey?: string;
  /** Custom fetch (for tests or in-process wiring). Defaults to globalThis.fetch. */
  fetch?: FetchLike;
}

export class ZoryaClient {
  readonly url: string;
  /**
   * Bearer token used for HTTP-RPC and the persistent WS upgrade. Public
   * so peers (e.g. `WorkerControlSocket`) can authenticate against the
   * same key without re-passing it through every layer.
   */
  readonly apiKey?: string;
  readonly storage: WorkflowStorage;
  readonly stepQueue: StepQueue;
  readonly workerRegistry: WorkerRegistry;
  private readonly fetch: FetchLike;
  private readonly headers: Record<string, string>;
  // Built lazily on first start() / startByName() call. The runner is just
  // a thin wrapper around storage for handle construction — no orchestration
  // happens client-side; the server's worker fleet runs the workflow.
  private _runner?: WorkflowRunner;

  constructor(config: ZoryaClientConfig) {
    const base = config.url.replace(/\/$/, "");
    this.url = base;
    this.apiKey = config.apiKey;
    this.fetch = config.fetch ?? ((req) => globalThis.fetch(req));
    this.headers = {
      "content-type": "application/json",
      ...(config.apiKey ? { authorization: `Bearer ${config.apiKey}` } : {}),
    };
    this.storage = new RemoteWorkflowStorage({
      url: `${base}/rpc/storage`,
      fetch: this.fetch,
      headers: this.headers,
    });
    this.stepQueue = new RemoteStepQueue({
      url: `${base}/rpc/worker`,
      fetch: this.fetch,
      headers: this.headers,
    });
    this.workerRegistry = new RemoteWorkerRegistry({
      url: `${base}/rpc/worker`,
      fetch: this.fetch,
      headers: this.headers,
    });
  }

  /**
   * Register this worker's workflow definitions with the server so they show
   * up in the dashboard's Workflows page. Idempotent — re-calling replaces
   * the prior advertisement for the same workerId.
   */
  async advertise(
    workerId: string,
    workflows: ReadonlyArray<Workflow<unknown, unknown>>,
    sampleInput?: (workflowName: string) => unknown,
  ): Promise<void> {
    // Advertise the primary definition for each workflow plus every
    // previousVersions entry the worker can still serve. Without this the
    // dashboard only sees the active version and the trigger modal's
    // version dropdown is missing the old ones.
    const entries: Array<{
      name: string;
      version?: string;
      steps: Array<{
        name: string;
        kind: string;
        dependsOn: string[];
        needs?: string[];
        priority?: number;
      }>;
      sampleInput?: unknown;
    }> = [];
    const seen = new Set<string>();
    const add = (wf: Workflow<unknown, unknown>) => {
      const key = `${wf.name}@${wf.version ?? ""}`;
      if (seen.has(key)) return;
      seen.add(key);
      entries.push({
        name: wf.name,
        version: wf.version,
        // Forward `needs` / `priority` so the server can build a faithful
        // stub workflow when running coordinator-driven step dispatch —
        // without these the coordinator can't route dispatched tasks to
        // capability-matched workers.
        steps: wf.dag.steps.map((s) => ({
          name: s.name,
          kind: s.kind,
          dependsOn: [...s.dependsOn],
          needs: s.needs ? [...s.needs] : undefined,
          priority: s.priority,
        })),
        sampleInput: sampleInput?.(wf.name),
      });
    };
    for (const wf of workflows) {
      add(wf);
      for (const prev of wf._definition.previousVersions ?? []) add(prev);
    }
    const body = { workerId, workflows: entries };
    const req = new Request(`${this.url}/api/advertisements`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify(body),
    });
    const res = await this.fetch(req);
    if (!res.ok) {
      throw new Error(`ZoryaClient.advertise failed: ${res.status} ${await res.text()}`);
    }
  }

  /** Clear this worker's advertisement on shutdown. */
  async unadvertise(workerId: string): Promise<void> {
    const req = new Request(`${this.url}/api/advertisements/${encodeURIComponent(workerId)}`, {
      method: "DELETE",
      headers: this.headers,
    });
    const res = await this.fetch(req);
    if (!res.ok && res.status !== 404) {
      throw new Error(`ZoryaClient.unadvertise failed: ${res.status}`);
    }
  }

  private get runner(): WorkflowRunner {
    if (!this._runner) {
      this._runner = createWorkflowRunner({ storage: this.storage });
    }
    return this._runner;
  }

  /**
   * Trigger a workflow run by name and return a typed `WorkflowHandle`.
   *
   * Prefer this overload when the caller has the `Workflow<I, O>` definition
   * imported — TypeScript checks `params.input` against the workflow's
   * declared `Input` and threads the `Output` generic into `handle.result()`.
   *
   * Routing on the server is by `name` (and optionally `version`); the
   * imported definition is used purely for type information, so it doesn't
   * have to be the exact instance the worker is running, only structurally
   * compatible.
   */
  async start<Input, Output>(
    workflow: Workflow<Input, Output>,
    params: {
      input: Input;
      workflowId?: string;
      namespace?: string;
      version?: string;
      metadata?: Record<string, unknown>;
    },
  ): Promise<WorkflowHandle<Output>> {
    const { workflowId } = await this._postTrigger(workflow.name, {
      input: params.input,
      workflowId: params.workflowId,
      namespace: params.namespace,
      version: params.version ?? workflow.version,
      metadata: params.metadata,
    });
    return this.runner.handle<Output>(workflowId);
  }

  /**
   * Trigger a workflow run by name without compile-time type info. Returns
   * an untyped handle (`WorkflowHandle<unknown>`). Used by the dashboard's
   * trigger modal and other dynamic-dispatch callers that don't have the
   * workflow definition on hand.
   */
  async startByName(
    name: string,
    params: {
      input?: unknown;
      workflowId?: string;
      namespace?: string;
      version?: string;
      metadata?: Record<string, unknown>;
    } = {},
  ): Promise<WorkflowHandle<unknown>> {
    const { workflowId } = await this._postTrigger(name, params);
    return this.runner.handle<unknown>(workflowId);
  }

  /**
   * @deprecated Prefer {@link start} (typed) or {@link startByName} (untyped)
   * — both return a `WorkflowHandle` with `result()`, `signal()`, `cancel()`,
   * and `events()` methods. This method is kept for back-compat with callers
   * that only need the workflowId for fire-and-forget triggers.
   */
  async triggerWorkflow(
    name: string,
    body: {
      input?: unknown;
      workflowId?: string;
      namespace?: string;
      version?: string;
      metadata?: Record<string, unknown>;
    } = {},
  ): Promise<{ workflowId: string }> {
    return this._postTrigger(name, body);
  }

  private async _postTrigger(
    name: string,
    body: {
      input?: unknown;
      workflowId?: string;
      namespace?: string;
      version?: string;
      metadata?: Record<string, unknown>;
    },
  ): Promise<{ workflowId: string }> {
    const req = new Request(`${this.url}/api/runs/trigger/${encodeURIComponent(name)}`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify(body),
    });
    const res = await this.fetch(req);
    const text = await res.text();
    if (!res.ok) throw new Error(`triggerWorkflow failed: ${res.status} ${text}`);
    return JSON.parse(text) as { workflowId: string };
  }

  /**
   * Claim pending workflow-start requests the worker can serve. Pass
   * `workflowSpecs` listing the (name, versions) tuples the worker
   * advertises; the server only hands back starts whose name matches and
   * whose pinned version (if any) is in the worker's set.
   */
  async claimWorkflowStarts(params: {
    workflowSpecs: ReadonlyArray<{ name: string; versions: readonly string[] }>;
    workerId?: string;
    limit?: number;
  }): Promise<ReadonlyArray<WorkflowStartClaim>> {
    const req = new Request(`${this.url}/api/worker-protocol/claim-starts`, {
      method: "POST",
      headers: this.headers,
      body: JSON.stringify({
        workflowSpecs: params.workflowSpecs.map((s) => ({
          name: s.name,
          versions: [...s.versions],
        })),
        workerId: params.workerId,
        limit: params.limit ?? 10,
      }),
    });
    const res = await this.fetch(req);
    const text = await res.text();
    if (!res.ok) throw new Error(`claimWorkflowStarts failed: ${res.status} ${text}`);
    const body = JSON.parse(text) as { starts?: WorkflowStartClaim[] };
    return body.starts ?? [];
  }

  /** Mark a claimed workflow-start as done. */
  async completeWorkflowStart(id: string): Promise<void> {
    const req = new Request(
      `${this.url}/api/worker-protocol/complete-start/${encodeURIComponent(id)}`,
      { method: "POST", headers: this.headers },
    );
    const res = await this.fetch(req);
    if (!res.ok) throw new Error(`completeWorkflowStart failed: ${res.status}`);
  }
}

export interface WorkflowStartClaim {
  readonly id: string;
  readonly workflowId: string;
  readonly workflowName: string;
  readonly input: unknown;
  readonly metadata?: Record<string, unknown>;
  readonly version?: string;
  readonly enqueuedAt: number;
}
