// ---------------------------------------------------------------------------
// Webhook ingress route — covers signature verification, replay dedup,
// scope validation, and dispatch into a registered agent. Uses an
// InMemoryAgentRegistry + a stub resolve callback so the test doesn't
// spin up the full LocalAgent stack.
// ---------------------------------------------------------------------------

import { describe, expect, it } from "bun:test";
import { createHmac } from "node:crypto";
import { InMemoryAgentRegistry } from "@promin/agent";
import type { RegisteredAgent } from "@promin/agent";
import { InMemoryWorkflowStorage, createWorkflowRunner } from "@promin/workflow";
import { LocalWorkflows, ZoryaAgents } from "../../../index.ts";
import { ZoryaServer } from "../../server.ts";

const SECRET = "test-webhook-secret";

function sign(secret: string, body: string): string {
  return `sha256=${createHmac("sha256", secret).update(body).digest("hex")}`;
}

interface BootOpts {
  /** When set, lets the test capture every dispatched task. */
  onInvoke?: (task: string) => void;
}

async function bootGateway(opts: BootOpts = {}) {
  const storage = new InMemoryWorkflowStorage();
  const runner = createWorkflowRunner({ storage });
  const registry = new InMemoryAgentRegistry();
  await registry.register({
    id: "echo-bot",
    backend: {
      type: "local",
      model: { provider: "anthropic", id: "claude-sonnet-4-6" },
      systemPrompt: "Helpful",
      tools: [],
    },
  });
  const workflows = new LocalWorkflows({
    storage,
    runner,
    definitions: {},
    sleepScanIntervalMs: 0,
  });
  // Stub agent — webhook test cares about whether the agent was invoked
  // with the right task, not what the LLM does. The stub captures the
  // task and resolves with empty text immediately.
  const stubAgent = {
    invoke: async (input: { task: string }) => {
      opts.onInvoke?.(input.task);
      return { text: Promise.resolve("ok") };
    },
    withScope: () => stubAgent,
  };
  const agents = new ZoryaAgents({
    registry,
    resolve: () => stubAgent as never,
  });
  const server = new ZoryaServer({
    workflows,
    agents,
    webhooks: {
      sources: {
        github: { secret: SECRET, signatureHeader: "X-Hub-Signature-256" },
      },
    },
  });
  return { server };
}

describe("webhook ingress — POST /webhooks/:source/:agentId", () => {
  it("accepts a signed payload and dispatches to the agent", async () => {
    const dispatched: string[] = [];
    const { server } = await bootGateway({ onInvoke: (task) => dispatched.push(task) });
    const body = JSON.stringify({ event: "push", repo: "promin" });
    const res = await server.handle(
      new Request("http://test/webhooks/github/echo-bot?namespaceId=acme&resourceId=alice", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Hub-Signature-256": sign(SECRET, body),
          "X-Delivery-Id": "delivery-1",
        },
        body,
      }),
    );
    expect(res.status).toBe(200);
    const respBody = (await res.json()) as { accepted: boolean; source: string; agentId: string };
    expect(respBody.accepted).toBe(true);
    expect(respBody.source).toBe("github");
    expect(respBody.agentId).toBe("echo-bot");
    expect(dispatched).toHaveLength(1);
    expect(dispatched[0]).toContain("Incoming webhook from github");
    expect(dispatched[0]).toContain('"repo":"promin"');
  });

  it("rejects 401 on missing signature header", async () => {
    const { server } = await bootGateway();
    const body = JSON.stringify({ event: "push" });
    const res = await server.handle(
      new Request("http://test/webhooks/github/echo-bot?namespaceId=acme&resourceId=alice", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
      }),
    );
    expect(res.status).toBe(401);
    const respBody = (await res.json()) as { error: string };
    expect(respBody.error).toBe("missing_signature");
  });

  it("rejects 401 on signature mismatch (wrong secret)", async () => {
    const { server } = await bootGateway();
    const body = JSON.stringify({ event: "push" });
    const res = await server.handle(
      new Request("http://test/webhooks/github/echo-bot?namespaceId=acme&resourceId=alice", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Hub-Signature-256": sign("wrong-secret", body),
        },
        body,
      }),
    );
    expect(res.status).toBe(401);
    const respBody = (await res.json()) as { error: string };
    expect(respBody.error).toBe("signature_mismatch");
  });

  it("rejects 404 unknown source", async () => {
    const { server } = await bootGateway();
    const body = JSON.stringify({});
    const res = await server.handle(
      new Request("http://test/webhooks/stripe/echo-bot?namespaceId=acme&resourceId=alice", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Hub-Signature-256": sign(SECRET, body),
        },
        body,
      }),
    );
    expect(res.status).toBe(404);
  });

  it("rejects 404 when agent isn't registered", async () => {
    const { server } = await bootGateway();
    const body = JSON.stringify({});
    const res = await server.handle(
      new Request("http://test/webhooks/github/never?namespaceId=acme&resourceId=alice", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Hub-Signature-256": sign(SECRET, body),
        },
        body,
      }),
    );
    expect(res.status).toBe(404);
    const respBody = (await res.json()) as { error: string };
    expect(respBody.error).toBe("agent_not_found");
  });

  it("rejects 400 missing namespaceId", async () => {
    const { server } = await bootGateway();
    const body = JSON.stringify({});
    const res = await server.handle(
      new Request("http://test/webhooks/github/echo-bot", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Hub-Signature-256": sign(SECRET, body),
        },
        body,
      }),
    );
    expect(res.status).toBe(400);
  });

  it("rejects 422 on non-JSON body", async () => {
    const { server } = await bootGateway();
    const body = "not-json";
    const res = await server.handle(
      new Request("http://test/webhooks/github/echo-bot?namespaceId=acme&resourceId=alice", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "X-Hub-Signature-256": sign(SECRET, body),
        },
        body,
      }),
    );
    expect(res.status).toBe(422);
  });

  it("idempotency — same X-Delivery-Id is accepted as replay (no double dispatch)", async () => {
    const dispatched: string[] = [];
    const { server } = await bootGateway({ onInvoke: (task) => dispatched.push(task) });
    const body = JSON.stringify({ event: "push" });
    const headers = {
      "content-type": "application/json",
      "X-Hub-Signature-256": sign(SECRET, body),
      "X-Delivery-Id": "stable-id-42",
    };
    const url = "http://test/webhooks/github/echo-bot?namespaceId=acme&resourceId=alice";

    const first = await server.handle(new Request(url, { method: "POST", headers, body }));
    expect(first.status).toBe(200);
    const firstBody = (await first.json()) as { accepted: boolean; replay?: boolean };
    expect(firstBody.replay).toBeUndefined();
    expect(dispatched).toHaveLength(1);

    const second = await server.handle(new Request(url, { method: "POST", headers, body }));
    expect(second.status).toBe(200);
    const secondBody = (await second.json()) as { accepted: boolean; replay?: boolean };
    expect(secondBody.replay).toBe(true);
    // No second dispatch.
    expect(dispatched).toHaveLength(1);
  });

  it("when no webhooks config, route is not mounted (404)", async () => {
    const storage = new InMemoryWorkflowStorage();
    const runner = createWorkflowRunner({ storage });
    const workflows = new LocalWorkflows({
      storage,
      runner,
      definitions: {},
      sleepScanIntervalMs: 0,
    });
    const server = new ZoryaServer({ workflows }); // no webhooks
    const res = await server.handle(
      new Request("http://test/webhooks/github/x?namespaceId=acme&resourceId=alice", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
      }),
    );
    expect(res.status).toBe(404);
  });
});
