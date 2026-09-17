// ---------------------------------------------------------------------------
// `wipeAgentInstance` — operator-grade delete that cascades through both
// the instance registry and the memory store.
//
// The instance registry only knows the (registeredAgentId, namespaceId,
// ownerId) tuple plus its metadata. The actual chat state — working
// memory, facts, episodes, threads, messages — lives in `MemoryStore`
// under `resourceId = instance.id`. Either side alone leaves an
// inconsistent system: deleting the row but not the memory leaves
// orphaned resource state; deleting the memory but not the row leaves a
// stale row that will resurrect the resource on the next invocation.
//
// This helper does both, in the right order:
//   1. Drop all threads under (namespace, instanceId) — cascades to
//      messages + thread-scope facts + thread-scope episodes per the
//      memory store contract.
//   2. Wipe resource-scope state (working memory, facts, episodes).
//   3. Delete the instance registry row.
//
// Returns counts so callers can surface what got wiped (operator UX).
// ---------------------------------------------------------------------------

import type { AgentInstanceRegistry } from "./types.ts";
import type { MemoryStore } from "../memory/types.ts";

export interface WipeAgentInstanceResult {
  readonly instanceId: string;
  readonly threadsDeleted: number;
  readonly factsDeleted: number;
  readonly episodesDeleted: number;
}

export async function wipeAgentInstance(deps: {
  readonly registry: AgentInstanceRegistry;
  readonly memory: MemoryStore;
  readonly instanceId: string;
}): Promise<WipeAgentInstanceResult> {
  const instance = await deps.registry.get(deps.instanceId);
  if (!instance) {
    return {
      instanceId: deps.instanceId,
      threadsDeleted: 0,
      factsDeleted: 0,
      episodesDeleted: 0,
    };
  }

  const key = { namespaceId: instance.namespaceId, resourceId: instance.id };

  // 1. Drop threads. The memory store cascades thread deletes to messages
  //    + thread-scope facts/episodes, so we don't need to walk those by hand.
  const threads = await deps.memory.listThreads({
    namespaceId: instance.namespaceId,
    resourceId: instance.id,
    limit: 100_000,
  });
  for (const t of threads) {
    // listThreads is filtered by resourceId so t.resourceId always equals
    // instance.id here, but the type still allows null — pass instance.id
    // to keep the call site total.
    await deps.memory.deleteThread({
      namespaceId: t.namespaceId,
      resourceId: instance.id,
      threadId: t.threadId,
    });
  }

  // 2. Resource-scope working memory + facts + episodes. We clear working
  //    memory by upserting null; facts and episodes are listed-then-deleted
  //    so the store's per-row invariants stay intact.
  await deps.memory.upsertResource(key, {
    workingMemory: null,
    staticRules: null,
  });

  const facts = await deps.memory.listResourceFacts(key);
  for (const f of facts) {
    await deps.memory.deleteResourceFact(key, f.id);
  }

  const episodes = await deps.memory.listResourceEpisodes(key);
  for (const e of episodes) {
    await deps.memory.deleteResourceEpisode(key, e.id);
  }

  // 3. Drop the registry row last so a partial wipe leaves the row in
  //    place — operator can retry the cascade.
  await deps.registry.delete(deps.instanceId);

  return {
    instanceId: deps.instanceId,
    threadsDeleted: threads.length,
    factsDeleted: facts.length,
    episodesDeleted: episodes.length,
  };
}
