// ---------------------------------------------------------------------------
// Draft recipe endpoints — "test a recipe edit before committing it".
//
// A draft is an ordinary RegisteredAgent registered under a reserved
// `__draft__` id prefix. The Designer POSTs the in-progress backend here,
// chats the draft through the standard /api/agents/:id gateway endpoints,
// and DELETEs it when the test modal closes — so experiments never land
// in permanent registry history.
//
// Routes:
//   POST   /api/agents/_draft        — register a draft, returns the recipe
//   DELETE /api/agents/_draft/:id    — delete a draft (id must be a draft)
//
// `createDraft` also opportunistically sweeps drafts older than the TTL,
// so a browser that closes mid-session leaves no permanent orphan — no
// background loop required.
// ---------------------------------------------------------------------------

import type { AgentRegistry, RegisteredAgent } from "@promin/agent";
import { json, jsonError, readJson } from "../router.ts";

/** Reserved id prefix marking a recipe as an ephemeral Designer draft. */
export const DRAFT_PREFIX = "__draft__";

/** How long an orphaned draft survives before the opportunistic sweep reaps it. */
const DRAFT_TTL_MS = 60 * 60 * 1000; // 1h

/** True when `id` is a draft recipe id. */
export function isDraftId(id: string): boolean {
  return id.startsWith(DRAFT_PREFIX);
}

export interface AgentDraftsDeps {
  readonly registry: AgentRegistry;
}

interface CreateDraftRequest {
  readonly backend?: unknown;
  readonly metadata?: unknown;
  /** The recipe being drafted from — only used to make the draft id legible. */
  readonly sourceId?: unknown;
}

const asMessage = (err: unknown): string => (err instanceof Error ? err.message : String(err));

export function createDraft(deps: AgentDraftsDeps) {
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateDraftRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (typeof body.backend !== "object" || body.backend === null) {
      return jsonError(400, "missing_backend");
    }

    // Opportunistic TTL sweep — reap drafts orphaned by a closed browser
    // before adding another. Best-effort; never fails the create.
    await sweepStaleDrafts(deps.registry);

    const sourceId =
      typeof body.sourceId === "string" && body.sourceId.length > 0 ? body.sourceId : "agent";
    const id = `${DRAFT_PREFIX}${Date.now().toString(36)}__${sourceId}`;
    const metadata =
      typeof body.metadata === "object" && body.metadata !== null
        ? (body.metadata as Partial<RegisteredAgent["metadata"]>)
        : undefined;
    try {
      const recipe = await deps.registry.register({
        id,
        backend: body.backend as RegisteredAgent["backend"],
        ...(metadata !== undefined && { metadata }),
      });
      return json(201, { recipe });
    } catch (err) {
      return jsonError(500, "draft_create_failed", asMessage(err));
    }
  };
}

export function deleteDraft(deps: AgentDraftsDeps) {
  return async (_req: Request, params: Record<string, string>): Promise<Response> => {
    const id = params.id;
    if (!id) return jsonError(400, "missing_id");
    if (!isDraftId(id)) {
      return jsonError(
        400,
        "not_a_draft",
        "Only `__draft__`-prefixed draft recipes can be deleted here.",
      );
    }
    try {
      await deps.registry.unregister(id);
      return new Response(null, { status: 204 });
    } catch (err) {
      return jsonError(500, "draft_delete_failed", asMessage(err));
    }
  };
}

async function sweepStaleDrafts(registry: AgentRegistry): Promise<void> {
  try {
    const cutoff = Date.now() - DRAFT_TTL_MS;
    for (const recipe of await registry.list({})) {
      if (isDraftId(recipe.id) && recipe.createdAt < cutoff) {
        await registry.unregister(recipe.id);
      }
    }
  } catch {
    // Cleanup is best-effort — a hiccup here must not block a draft create.
  }
}
