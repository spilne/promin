// ---------------------------------------------------------------------------
// ZoryaClient — connects a worker process / admin tool to a remote Zorya
// server. Wraps RemoteWorkflowStorage + RemoteStepQueue + RemoteWorkerRegistry
// behind a single ergonomic object, and knows how to advertise workflow
// definitions to the server on connect.
// ---------------------------------------------------------------------------

import type { Workflow, WorkflowStorage, StepQueue, WorkerRegistry } from "@promin/workflow";
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
  readonly storage: WorkflowStorage;
  readonly stepQueue: StepQueue;
  readonly workerRegistry: WorkerRegistry;
  private readonly fetch: FetchLike;
  private readonly headers: Record<string, string>;

  constructor(config: ZoryaClientConfig) {
    const base = config.url.replace(/\/$/, "");
    this.url = base;
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
    const body = {
      workerId,
      workflows: workflows.map((wf) => ({
        name: wf.name,
        version: wf.version,
        steps: wf.dag.steps.map((s) => ({
          name: s.name,
          kind: s.kind,
          dependsOn: [...s.dependsOn],
        })),
        sampleInput: sampleInput?.(wf.name),
      })),
    };
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

  /** Trigger a workflow run through the server's trigger endpoint. */
  async triggerWorkflow(
    name: string,
    body: { input?: unknown; workflowId?: string; namespace?: string } = {},
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
