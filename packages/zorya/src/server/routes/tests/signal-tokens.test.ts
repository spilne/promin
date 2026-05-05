// ---------------------------------------------------------------------------
// Signal token routes — happy path + bearer auth + idempotency + late-call.
//
// Hits the three handlers directly (mintSignalToken, completeSignalToken,
// listSignalTokensForRun) against an InMemoryWorkflowStorage. Doesn't
// stand up a full ZoryaServer — the auth-skip behaviour for the public
// completion route is covered separately in the server-level routing test.
// ---------------------------------------------------------------------------

import { beforeEach, describe, expect, it } from "bun:test";
import { InMemoryWorkflowStorage } from "@promin/workflow";
import {
  completeSignalToken,
  listSignalTokensForRun,
  mintSignalToken,
  type MintTokenResponse,
  type SignalTokenDto,
} from "../signal-tokens.ts";

const PUBLIC_BASE = "https://zorya.test";

async function setup(): Promise<{ storage: InMemoryWorkflowStorage; workflowId: string }> {
  const storage = new InMemoryWorkflowStorage();
  const workflowId = "wf-signal-token-test";
  await storage.createWorkflow({
    workflowId,
    workflowName: "approve-doc",
    input: { docId: "d-1" },
  });
  return { storage, workflowId };
}

async function mint(
  storage: InMemoryWorkflowStorage,
  workflowId: string,
  signalName: string,
  body: { tags?: string[]; expiresInMs: number; idempotencyKey?: string },
): Promise<{ status: number; body: MintTokenResponse }> {
  const handler = mintSignalToken({ storage, publicBaseUrl: PUBLIC_BASE });
  const res = await handler(
    new Request(`http://x/api/runs/${workflowId}/signals/${signalName}/token`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
    { id: workflowId, name: signalName },
  );
  return { status: res.status, body: (await res.json()) as MintTokenResponse };
}

async function complete(
  storage: InMemoryWorkflowStorage,
  tokenId: string,
  bearer: string | null,
  value: unknown,
): Promise<{ status: number; body: any }> {
  const handler = completeSignalToken({ storage });
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (bearer) headers.authorization = `Bearer ${bearer}`;
  const res = await handler(
    new Request(`http://x/api/signal-tokens/${tokenId}/complete`, {
      method: "POST",
      headers,
      body: JSON.stringify({ value }),
    }),
    { tokenId },
  );
  return { status: res.status, body: await res.json() };
}

describe("signal-tokens routes", () => {
  let storage: InMemoryWorkflowStorage;
  let workflowId: string;

  beforeEach(async () => {
    ({ storage, workflowId } = await setup());
  });

  describe("mint → complete (happy path)", () => {
    it("issues a token with bearer + URL, then completes via deliverSignal", async () => {
      const minted = await mint(storage, workflowId, "approve", {
        expiresInMs: 60_000,
        tags: ["approval"],
      });
      expect(minted.status).toBe(201);
      expect(minted.body.url).toBe(
        `${PUBLIC_BASE}/api/signal-tokens/${minted.body.tokenId}/complete`,
      );
      expect(minted.body.bearer.length).toBeGreaterThan(0);
      expect(minted.body.isCached).toBe(false);

      const completed = await complete(storage, minted.body.tokenId, minted.body.bearer, {
        approved: true,
      });
      expect(completed.status).toBe(201);
      expect(completed.body).toEqual({ ok: true, value: { approved: true } });

      // Signal was delivered through the existing mechanic.
      const signals = await storage.loadSignals(workflowId);
      expect(signals).toHaveLength(1);
      expect(signals[0]!).toMatchObject({
        signalName: "approve",
        payload: { approved: true },
      });
    });

    it("omits the URL when no publicBaseUrl is configured", async () => {
      const handler = mintSignalToken({ storage });
      const res = await handler(
        new Request(`http://x/api/runs/${workflowId}/signals/approve/token`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ expiresInMs: 60_000 }),
        }),
        { id: workflowId, name: "approve" },
      );
      const body = (await res.json()) as MintTokenResponse;
      expect(body.url).toBeNull();
    });
  });

  describe("bearer auth", () => {
    it("rejects missing bearer with 401", async () => {
      const minted = await mint(storage, workflowId, "approve", { expiresInMs: 60_000 });
      const completed = await complete(storage, minted.body.tokenId, null, "x");
      expect(completed.status).toBe(401);
      expect(completed.body.error).toBe("missing_bearer");
    });

    it("rejects wrong bearer with 401", async () => {
      const minted = await mint(storage, workflowId, "approve", { expiresInMs: 60_000 });
      const completed = await complete(storage, minted.body.tokenId, "wrong-bearer", "x");
      expect(completed.status).toBe(401);
      expect(completed.body.error).toBe("invalid_bearer");
    });

    it("rejects unknown tokenId with 404", async () => {
      const completed = await complete(storage, "unknown-token", "any-bearer", "x");
      expect(completed.status).toBe(404);
      expect(completed.body.error).toBe("token_not_found");
    });
  });

  describe("expiry + idempotent re-submission", () => {
    it("returns 408 when the token has expired before completion", async () => {
      const minted = await mint(storage, workflowId, "approve", { expiresInMs: 1 });
      // Advance past expiry.
      await new Promise((r) => setTimeout(r, 5));
      const completed = await complete(storage, minted.body.tokenId, minted.body.bearer, "x");
      expect(completed.status).toBe(408);
      expect(completed.body.error).toBe("token_expired");
    });

    it("returns 200 on bearer re-submission with the original value", async () => {
      const minted = await mint(storage, workflowId, "approve", { expiresInMs: 60_000 });
      const first = await complete(storage, minted.body.tokenId, minted.body.bearer, {
        approved: true,
      });
      expect(first.status).toBe(201);
      const second = await complete(storage, minted.body.tokenId, minted.body.bearer, {
        approved: false, // ignored — original wins
      });
      expect(second.status).toBe(200);
      expect(second.body.value).toEqual({ approved: true });
      expect(second.body.alreadyCompleted).toBe(true);

      // Signal is still delivered exactly once.
      const signals = await storage.loadSignals(workflowId);
      expect(signals).toHaveLength(1);
    });
  });

  describe("idempotency-key replay across mint", () => {
    it("re-issues the same (tokenId, bearer) when the same key is reused", async () => {
      const first = await mint(storage, workflowId, "approve", {
        expiresInMs: 60_000,
        idempotencyKey: "doc-42",
      });
      expect(first.status).toBe(201);
      expect(first.body.isCached).toBe(false);

      const second = await mint(storage, workflowId, "approve", {
        expiresInMs: 60_000,
        idempotencyKey: "doc-42",
      });
      expect(second.status).toBe(200);
      expect(second.body.isCached).toBe(true);
      expect(second.body.tokenId).toBe(first.body.tokenId);
      expect(second.body.bearer).toBe(first.body.bearer);

      // The cached bearer still works for completion.
      const completed = await complete(storage, second.body.tokenId, second.body.bearer, "ok");
      expect(completed.status).toBe(201);
    });

    it("treats different idempotency keys as separate tokens", async () => {
      const a = await mint(storage, workflowId, "approve", {
        expiresInMs: 60_000,
        idempotencyKey: "doc-A",
      });
      const b = await mint(storage, workflowId, "approve", {
        expiresInMs: 60_000,
        idempotencyKey: "doc-B",
      });
      expect(a.body.tokenId).not.toBe(b.body.tokenId);
      expect(a.body.bearer).not.toBe(b.body.bearer);
    });
  });

  describe("validation", () => {
    it("rejects mint without expiresInMs (400)", async () => {
      const handler = mintSignalToken({ storage, publicBaseUrl: PUBLIC_BASE });
      const res = await handler(
        new Request(`http://x/api/runs/${workflowId}/signals/approve/token`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({}),
        }),
        { id: workflowId, name: "approve" },
      );
      expect(res.status).toBe(400);
    });

    it("rejects mint against a non-existent workflow (404)", async () => {
      const handler = mintSignalToken({ storage, publicBaseUrl: PUBLIC_BASE });
      const res = await handler(
        new Request(`http://x/api/runs/missing/signals/approve/token`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ expiresInMs: 60_000 }),
        }),
        { id: "missing", name: "approve" },
      );
      expect(res.status).toBe(404);
    });

    it("rejects complete without `value` in body (400)", async () => {
      const minted = await mint(storage, workflowId, "approve", { expiresInMs: 60_000 });
      const handler = completeSignalToken({ storage });
      const res = await handler(
        new Request(`http://x/api/signal-tokens/${minted.body.tokenId}/complete`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${minted.body.bearer}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({}),
        }),
        { tokenId: minted.body.tokenId },
      );
      expect(res.status).toBe(400);
    });
  });

  describe("listSignalTokensForRun", () => {
    it("returns the workflow's tokens (without the bearer field)", async () => {
      await mint(storage, workflowId, "approve", {
        expiresInMs: 60_000,
        tags: ["approval"],
      });
      await mint(storage, workflowId, "review", { expiresInMs: 60_000, tags: ["review"] });

      const handler = listSignalTokensForRun({ storage });
      const res = await handler(new Request(`http://x/api/runs/${workflowId}/signal-tokens`), {
        id: workflowId,
      });
      const body = (await res.json()) as { tokens: SignalTokenDto[] };
      expect(body.tokens).toHaveLength(2);
      // Both signals show up; ordering by createdAt DESC may tie at ms
      // resolution so we assert membership rather than position.
      const names = body.tokens.map((t) => t.signalName).sort();
      expect(names).toEqual(["approve", "review"]);
      // Bearer must NOT leak through the dashboard payload.
      for (const t of body.tokens) {
        expect((t as unknown as Record<string, unknown>).bearer).toBeUndefined();
      }
    });
  });
});
