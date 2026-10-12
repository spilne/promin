// ---------------------------------------------------------------------------
// /api/evals through `ZoryaServer.handle`: suites as a versioned resource,
// runs against agent recipe versions (a draft included) followed to the
// end by polling, run comparison, and the promotion gate the agent routes
// enforce (409 `eval_gate_failed`, the operator override).
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import {
  InMemoryAgentRegistry,
  InMemoryRoleRegistry,
  resolveLocalAgent,
  type Message,
} from "@promin/agent";
import {
  InMemoryEvalRunStore,
  InMemoryEvalSuiteRegistry,
  type RunDiff,
  type StoredEvalRun,
} from "@promin/evals";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents, ZoryaEvals } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";
import type { ScopeAuthorizer } from "../agent-gateway-kit.ts";
import type { EvalGateFailedResponse } from "../evals.ts";

const SUITE = {
  id: "upper",
  description: "Answers in upper case",
  dataset: {
    kind: "inline",
    id: "letters",
    cases: ["a", "b"].map((letter) => ({
      id: letter,
      input: letter,
      expected: letter.toUpperCase(),
    })),
  },
  scorers: [{ kind: "exactMatch" }],
  metadata: { agentIds: ["bot"] },
};

function backend(prompt: string) {
  return {
    type: "local" as const,
    model: { provider: "test", id: "m" },
    role: { inline: { systemPrompt: prompt, tools: [] as string[] } },
  };
}

function lastUser(messages: ReadonlyArray<Message>): string {
  const last = messages.findLast((m) => m.role === "user");
  return typeof last?.content === "string" ? last.content : "";
}

async function boot(opts: { authorizeScope?: ScopeAuthorizer } = {}) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
    signalScanIntervalMs: 0,
  });
  const registry = new InMemoryAgentRegistry();
  await registry.register({ id: "bot", backend: backend("v1: shout") }); // active v1
  await registry.register({ id: "bot", backend: backend("v2: whisper") }); // draft v2
  const agents = new ZoryaAgents({
    registry,
    roles: new InMemoryRoleRegistry(),
    resolve: (recipe) =>
      resolveLocalAgent(recipe, {
        runner,
        tools: {},
        // v1 answers right, v2 wrong: the suite tells the versions apart.
        llm: () => ({
          chat: async (params) => {
            const task = lastUser(params.messages);
            return {
              content: recipe.version === "v1" ? task.toUpperCase() : `not ${task}`,
              finishReason: "stop",
            };
          },
        }),
      }),
    ...(opts.authorizeScope !== undefined && { authorizeScope: opts.authorizeScope }),
  });
  const evals = new ZoryaEvals({
    suites: new InMemoryEvalSuiteRegistry(),
    runs: new InMemoryEvalRunStore(),
    workflows,
    agents,
  });
  const server = new ZoryaServer({ insecure: true, workflows, agents, evals });
  const send = async (params: {
    method: string;
    path: string;
    body?: unknown;
    headers?: Record<string, string>;
  }): Promise<Response> =>
    server.handle(
      new Request(`http://zorya${params.path}`, {
        method: params.method,
        headers: { "content-type": "application/json", ...params.headers },
        ...(params.body !== undefined && { body: JSON.stringify(params.body) }),
      }),
    );
  return { server, send, evals, registry, agents };
}

type Harness = Awaited<ReturnType<typeof boot>>;

async function runToEnd(params: { h: Harness; agentVersion: string }): Promise<StoredEvalRun> {
  const res = await params.h.send({
    method: "POST",
    path: "/api/evals/runs",
    body: {
      suiteId: "upper",
      agentId: "bot",
      agentVersion: params.agentVersion,
      namespaceId: "acme",
    },
  });
  expect(res.status).toBe(202);
  const { runId } = (await res.json()) as { runId: string };
  for (let i = 0; i < 5_000; i++) {
    const run = (await (
      await params.h.send({ method: "GET", path: `/api/evals/runs/${runId}` })
    ).json()) as StoredEvalRun;
    if (run.status !== "running") return run;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("run never ended");
}

async function createSuite(h: Harness): Promise<void> {
  const res = await h.send({ method: "POST", path: "/api/evals/suites", body: SUITE });
  expect(res.status).toBe(201);
}

describe("/api/evals — suites", () => {
  it("creates, versions and promotes a suite", async () => {
    const h = await boot();
    await createSuite(h);
    expect((await h.send({ method: "POST", path: "/api/evals/suites", body: SUITE })).status).toBe(
      409,
    );
    const patched = await h.send({
      method: "PATCH",
      path: "/api/evals/suites/upper",
      headers: { "if-match": 'W/"v1"' },
      body: { samplesPerCase: 2, scorers: SUITE.scorers, publish: false },
    });
    expect(patched.status).toBe(200);
    expect(patched.headers.get("etag")).toBe('W/"v2"');
    const versions = (await (
      await h.send({ method: "GET", path: "/api/evals/suites/upper/versions" })
    ).json()) as { versions: Array<{ version: string; active: boolean }> };
    expect(versions.versions.map((v) => [v.version, v.active])).toEqual([
      ["v1", true],
      ["v2", false],
    ]);
    const listed = (await (
      await h.send({ method: "GET", path: "/api/evals/suites?agentId=bot" })
    ).json()) as { suites: Array<{ id: string }> };
    expect(listed.suites.map((s) => s.id)).toEqual(["upper"]);
  });

  it("refuses a jsonl dataset and a malformed suite", async () => {
    const h = await boot();
    const jsonl = await h.send({
      method: "POST",
      path: "/api/evals/suites",
      body: { ...SUITE, dataset: { kind: "jsonl", path: "/etc/passwd" } },
    });
    expect(jsonl.status).toBe(400);
    expect(((await jsonl.json()) as { error: string }).error).toBe("unsupported_dataset");
    const bad = await h.send({
      method: "POST",
      path: "/api/evals/suites",
      body: { ...SUITE, scorers: [{ kind: "nope" }] },
    });
    expect(bad.status).toBe(400);
  });
});

describe("/api/evals — runs", () => {
  it("runs a suite against the active version and a draft, and compares them", async () => {
    const h = await boot();
    await createSuite(h);
    const good = await runToEnd({ h, agentVersion: "v1" });
    const draft = await runToEnd({ h, agentVersion: "v2" });
    expect(good.status).toBe("completed");
    expect(good.totals?.passRate).toBe(1);
    expect(good.caseResults.map((c) => c.caseId)).toEqual(["a", "b"]);
    expect(draft.totals?.passRate).toBe(0);
    expect(draft.targetVersion).toBe("v2");

    const listed = (await (
      await h.send({ method: "GET", path: "/api/evals/runs?agentId=bot&agentVersion=v2" })
    ).json()) as { runs: Array<{ runId: string }> };
    expect(listed.runs.map((r) => r.runId)).toEqual([draft.runId]);

    const diff = (await (
      await h.send({
        method: "GET",
        path: `/api/evals/compare?baseline=${good.runId}&candidate=${draft.runId}`,
      })
    ).json()) as RunDiff;
    expect(diff.passRateDelta).toBe(-1);
    expect(diff.regressions).toEqual(["a", "b"]);
  });

  it("answers 404 for an unknown suite, agent or run and 400 without a namespace", async () => {
    const h = await boot();
    await createSuite(h);
    const post = (body: unknown) => h.send({ method: "POST", path: "/api/evals/runs", body });
    expect((await post({ suiteId: "ghost", agentId: "bot", namespaceId: "acme" })).status).toBe(
      404,
    );
    expect((await post({ suiteId: "upper", agentId: "ghost", namespaceId: "acme" })).status).toBe(
      404,
    );
    expect((await post({ suiteId: "upper", agentId: "bot" })).status).toBe(400);
    expect((await h.send({ method: "GET", path: "/api/evals/runs/nope" })).status).toBe(404);
  });

  it("checks the caller's scope for the agent before a run starts", async () => {
    const h = await boot({ authorizeScope: ({ scope }) => scope.kind === "operator" });
    await createSuite(h);
    const res = await h.send({
      method: "POST",
      path: "/api/evals/runs",
      body: { suiteId: "upper", agentId: "bot", namespaceId: "acme" },
    });
    expect(res.status).toBe(403);
  });
});

describe("agent promotion gated on evals", () => {
  async function gated(opts: { authorizeScope?: ScopeAuthorizer } = {}) {
    const h = await boot(opts);
    await createSuite(h);
    const put = await h.send({
      method: "PUT",
      path: "/api/evals/gates/bot",
      body: { suiteId: "upper", minScore: 0.9 },
    });
    expect(put.status).toBe(200);
    return h;
  }
  const promote = (h: Harness, body: Record<string, unknown>) =>
    h.send({ method: "POST", path: "/api/agents/bot/promote", body });

  it("refuses a version with no passing run, with the latest result", async () => {
    const h = await gated();
    const unevaluated = await promote(h, { version: "v2" });
    expect(unevaluated.status).toBe(409);
    const none = (await unevaluated.json()) as EvalGateFailedResponse;
    expect(none.error).toBe("eval_gate_failed");
    expect(none.latest).toBeNull();
    expect(none.gate.suiteId).toBe("upper");

    const failing = await runToEnd({ h, agentVersion: "v2" });
    const refused = await promote(h, { version: "v2" });
    expect(refused.status).toBe(409);
    const body = (await refused.json()) as EvalGateFailedResponse;
    expect(body.latest?.runId).toBe(failing.runId);
    expect(body.score).toBe(0);
    expect((await h.registry.get({ id: "bot" }))?.version).toBe("v1");
  });

  it("promotes a version once the suite passed on it", async () => {
    const h = await gated();
    // Roll forward to v2 by override, then back to v1 only after v1 passed.
    expect((await promote(h, { version: "v2", overrideEvalGate: true })).status).toBe(200);
    expect((await promote(h, { version: "v1" })).status).toBe(409);
    await runToEnd({ h, agentVersion: "v1" });
    const res = await promote(h, { version: "v1" });
    expect(res.status).toBe(200);
    expect((await h.registry.get({ id: "bot" }))?.version).toBe("v1");
    const verdict = (await (
      await h.send({ method: "GET", path: "/api/evals/gates/bot?version=v1" })
    ).json()) as { verdict: { passed: boolean; score: number } };
    expect(verdict.verdict).toMatchObject({ passed: true, score: 1 });
  });

  it("lets only an operator override, loosen or remove the gate, or edit its suite", async () => {
    let operator = true;
    const h = await gated({
      authorizeScope: ({ scope }) => scope.kind !== "operator" || operator,
    });
    operator = false;
    const res = await promote(h, { version: "v2", overrideEvalGate: true });
    expect(res.status).toBe(403);
    expect((await promote(h, { version: "v2", overrideEvalGate: "yes" })).status).toBe(400);
    // Neither can the gate be removed or loosened, nor its suite weakened.
    expect((await h.send({ method: "DELETE", path: "/api/evals/gates/bot" })).status).toBe(403);
    expect(
      (
        await h.send({
          method: "PUT",
          path: "/api/evals/gates/bot",
          body: { suiteId: "upper", minScore: 0 },
        })
      ).status,
    ).toBe(403);
    const suiteWrites = [
      { method: "POST", path: "/api/evals/suites", body: { ...SUITE, id: "other" } },
      {
        method: "PATCH",
        path: "/api/evals/suites/upper",
        headers: { "if-match": 'W/"v1"' },
        body: { scorers: [{ kind: "regexMatch", pattern: "." }] },
      },
      { method: "POST", path: "/api/evals/suites/upper/promote", body: { version: "v1" } },
      { method: "DELETE", path: "/api/evals/suites/upper" },
    ];
    for (const write of suiteWrites) expect((await h.send(write)).status).toBe(403);
    expect((await promote(h, { version: "v2" })).status).toBe(409);
    expect((await h.evals.gates.get({ id: "bot" }))?.minScore).toBe(0.9);
  });

  it("checks runs against the scope they ran in", async () => {
    let allowed = "acme";
    const h = await gated({
      authorizeScope: ({ scope }) => scope.kind === "operator" || scope.namespaceId === allowed,
    });
    const run = await runToEnd({ h, agentVersion: "v1" });
    expect(run.namespaceId).toBe("acme");
    expect(run.metadata?.resourceId).toBe(`eval-${run.runId}`);
    allowed = "other";
    const get = await h.send({ method: "GET", path: `/api/evals/runs/${run.runId}` });
    expect(get.status).toBe(404);
    const compare = await h.send({
      method: "GET",
      path: `/api/evals/compare?baseline=${run.runId}&candidate=${run.runId}`,
    });
    expect(compare.status).toBe(404);
    const cancel = await h.send({ method: "POST", path: `/api/evals/runs/${run.runId}/cancel` });
    expect(cancel.status).toBe(404);
    expect((await h.send({ method: "GET", path: "/api/evals/runs" })).status).toBe(400);
    const listed = (await (
      await h.send({ method: "GET", path: "/api/evals/runs?namespaceId=other" })
    ).json()) as { runs: unknown[] };
    expect(listed.runs).toEqual([]);
    expect((await h.send({ method: "GET", path: "/api/evals/runs?namespaceId=acme" })).status).toBe(
      403,
    );
  });

  it("refuses a publishing edit (unevaluated) but takes a draft", async () => {
    const h = await gated();
    const edit = (publish: boolean) =>
      h.send({
        method: "PATCH",
        path: "/api/agents/bot",
        headers: { "if-match": 'W/"v1"' },
        body: { backend: backend("v3"), publish },
      });
    expect((await edit(true)).status).toBe(409);
    const draft = await edit(false);
    expect(draft.status).toBe(200);
    expect((await h.registry.get({ id: "bot" }))?.version).toBe("v1");
  });

  it("gates removing the active version in favour of another", async () => {
    const h = await gated();
    const res = await h.send({
      method: "DELETE",
      path: "/api/agents/bot?version=v1&promote=v2",
      headers: { "if-match": 'W/"v1"' },
    });
    expect(res.status).toBe(409);
    expect((await h.registry.get({ id: "bot" }))?.version).toBe("v1");
  });

  it("a run of a never-promoted (weakened) suite draft does not satisfy the gate", async () => {
    const h = await gated();
    const draft = await h.send({
      method: "PATCH",
      path: "/api/evals/suites/upper",
      headers: { "if-match": 'W/"v1"' },
      body: { publish: false, scorers: [{ kind: "regexMatch", pattern: "." }] },
    });
    expect(draft.status).toBe(200);
    const res = await h.send({
      method: "POST",
      path: "/api/evals/runs",
      body: {
        suiteId: "upper",
        suiteVersion: "v2",
        agentId: "bot",
        agentVersion: "v2",
        namespaceId: "acme",
      },
    });
    expect(res.status).toBe(202);
    const { runId } = (await res.json()) as { runId: string };
    for (let i = 0; i < 5_000; i++) {
      if ((await h.evals.getRun(runId))?.status !== "running") break;
      await new Promise((r) => setTimeout(r, 0));
    }
    expect((await h.evals.getRun(runId))?.totals?.passRate).toBe(1);
    expect((await promote(h, { version: "v2" })).status).toBe(409);
    // Once that suite version is the active one, its runs count.
    await h.evals.suites.promote({ id: "upper", version: "v2" });
    expect((await promote(h, { version: "v2" })).status).toBe(200);
  });

  it("records the suite's active version on an unpinned gate, for display", async () => {
    const h = await gated();
    const gate = (await (await h.send({ method: "GET", path: "/api/evals/gates/bot" })).json()) as {
      gate: { suiteVersionAtSet?: string; suiteVersion?: string };
    };
    expect(gate.gate).toMatchObject({ suiteVersionAtSet: "v1" });
    expect(gate.gate.suiteVersion).toBeUndefined();
  });

  it("gates re-creating a deleted agent id; an override creates it", async () => {
    const h = await gated();
    expect(
      (
        await h.send({
          method: "DELETE",
          path: "/api/agents/bot",
          headers: { "if-match": 'W/"v1"' },
        })
      ).status,
    ).toBe(204);
    const create = (body: Record<string, unknown>) =>
      h.send({
        method: "POST",
        path: "/api/agents",
        body: { id: "bot", backend: backend("v3"), ...body },
      });
    const refused = await create({});
    expect(refused.status).toBe(409);
    expect(((await refused.json()) as { error: string }).error).toBe("eval_gate_failed");
    expect(await h.registry.get({ id: "bot" })).toBeNull();
    expect((await create({ overrideEvalGate: true })).status).toBe(201);
  });

  it("gates a clone into a gated id", async () => {
    const h = await gated();
    await h.send({
      method: "PUT",
      path: "/api/evals/gates/copy",
      body: { suiteId: "upper", minScore: 1 },
    });
    const res = await h.send({
      method: "POST",
      path: "/api/agents/bot/clone",
      body: { targetId: "copy" },
    });
    expect(res.status).toBe(409);
  });

  it("extract-role keeps a gated agent's rebound version as a draft", async () => {
    const h = await gated();
    const res = await h.send({
      method: "POST",
      path: "/api/agents/bot/extract-role",
      body: { roleId: "shouter" },
    });
    expect(res.status).toBe(201);
    const body = (await res.json()) as { published: boolean; agent: { version: string } };
    expect(body.published).toBe(false);
    expect((await h.registry.get({ id: "bot" }))?.version).toBe("v1");
  });

  it("installs its gates as the agents' auto-promote check (file scans, remote re-syncs)", async () => {
    const h = await gated();
    expect(await h.agents.canAutoPromote?.({ id: "bot" })).toBe(false);
    expect(await h.agents.canAutoPromote?.({ id: "ungated" })).toBe(true);
  });

  it("lets a version through once the gate is removed", async () => {
    const h = await gated();
    expect((await h.send({ method: "DELETE", path: "/api/evals/gates/bot" })).status).toBe(204);
    expect((await promote(h, { version: "v2" })).status).toBe(200);
  });
});
