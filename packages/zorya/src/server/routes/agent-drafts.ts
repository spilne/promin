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
// `createDraft` also opportunistically sweeps drafts older than the TTL
// (at most once a minute), so a browser that closes mid-session leaves no
// permanent orphan — no background loop required.
// ---------------------------------------------------------------------------

import type { AgentRegistry } from "@promin/agent";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import { json, jsonError, readJson } from "../router.ts";
import {
  parseBackend,
  parseMetadataPatch,
  redactRecipe,
  restoreRedactedSecrets,
  type AgentMetadataPatch,
} from "./agent-gateway-kit.ts";

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
  /** Time source for draft ids and the stale-draft TTL cutoff. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

interface CreateDraftRequest {
  readonly backend?: unknown;
  readonly metadata?: unknown;
  /** The recipe being drafted from — only used to make the draft id legible. */
  readonly sourceId?: unknown;
}

/** Shortest gap between two opportunistic draft sweeps. */
const SWEEP_MIN_INTERVAL_MS = 60 * 1000;

/** Characters kept from `sourceId` when it is folded into a draft id. */
const SOURCE_ID_UNSAFE = /[^A-Za-z0-9_.-]/g;

export function createDraft(deps: AgentDraftsDeps) {
  const clock = deps.clock ?? SystemWallClock;
  let lastSweepMs = Number.NEGATIVE_INFINITY;
  return async (req: Request): Promise<Response> => {
    const body = await readJson<CreateDraftRequest>(req);
    if (!body) return jsonError(400, "missing_body");
    if (body.backend === undefined || body.backend === null) {
      return jsonError(400, "missing_backend");
    }
    const parsed = parseBackend(body.backend);
    if ("error" in parsed) return jsonError(400, parsed.error, parsed.message);
    let metadata: AgentMetadataPatch | undefined;
    if (body.metadata !== undefined && body.metadata !== null) {
      const patch = parseMetadataPatch(body.metadata);
      if ("error" in patch) return jsonError(400, patch.error, patch.message);
      metadata = patch.metadata;
    }

    const rawSourceId = typeof body.sourceId === "string" ? body.sourceId : "";
    // A draft of a remote agent round-trips the redacted token; restore the
    // source recipe's stored one so the draft can actually call out.
    const source = rawSourceId.length > 0 ? await deps.registry.get(rawSourceId) : null;
    const backend = restoreRedactedSecrets({
      next: parsed.backend,
      ...(source !== null && { existing: source.backend }),
    });
    if ("error" in backend) return jsonError(400, backend.error);

    // Opportunistic TTL sweep — reap drafts orphaned by a closed browser.
    // Throttled so a burst of creates doesn't list the registry each time.
    const now = clock.currentTimeMs();
    if (now - lastSweepMs >= SWEEP_MIN_INTERVAL_MS) {
      lastSweepMs = now;
      await sweepStaleDrafts({ registry: deps.registry, clock });
    }

    const sourceId = rawSourceId.replace(SOURCE_ID_UNSAFE, "").slice(0, 64) || "agent";
    const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 12);
    const id = `${DRAFT_PREFIX}${now.toString(36)}${suffix}__${sourceId}`;
    const recipe = await deps.registry.register({
      id,
      backend,
      ...(metadata !== undefined && { metadata }),
    });
    return json(201, { recipe: redactRecipe(recipe) });
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
    await deps.registry.unregister(id);
    return new Response(null, { status: 204 });
  };
}

async function sweepStaleDrafts(params: {
  registry: AgentRegistry;
  clock: WallClock;
}): Promise<void> {
  const { registry, clock } = params;
  try {
    const cutoff = clock.currentTimeMs() - DRAFT_TTL_MS;
    for (const recipe of await registry.list({})) {
      if (isDraftId(recipe.id) && recipe.createdAt < cutoff) {
        await registry.unregister(recipe.id);
      }
    }
  } catch {
    // Cleanup is best-effort — a hiccup here must not block a draft create.
  }
}
