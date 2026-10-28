// ---------------------------------------------------------------------------
// Key layout of RedisWorkflowStorage
// ---------------------------------------------------------------------------
//
// Every key of one workflow carries the hash tag `{wf:<workflowId>}`, so on
// Redis Cluster all of them hash to one slot and a script can touch any of
// them atomically:
//
//   <prefix>:{wf:<id>}                       workflow hash
//   <prefix>:{wf:<id>}:steps:<run>           step rows of a run
//   <prefix>:{wf:<id>}:tasks:<run>:<step>    map-task rows of a step
//   <prefix>:{wf:<id>}:lock                  lock hash (lockedBy, token)
//   <prefix>:{wf:<id>}:fence                 fence-token counter
//   <prefix>:{wf:<id>}:child-intents         children created under its fence
//   <prefix>:{wf:<id>}:journal:...           activity journal
//   ... signals, runs, attempts, signal tokens, streams
//
// The cross-workflow indexes (status / name / parent / namespace sets, the
// ordering and completion sorted sets, the sleep schedule, the distinct-value
// sets) share the tag `{idx}`, so they live together in one other slot:
//
//   <prefix>:{idx}:status:<status>   ...
//
// No script touches both slots. A write changes the workflow's own keys in
// one script, bumping the hash's `iv` (index version) when an indexed field
// changes; a second script then applies the change to the `{idx}` keys,
// skipped when the index already holds that version or a newer one. The
// workflow hash is authoritative: the index can lag it after a crash
// between the two scripts, and readers repair a lagging entry when they
// notice one.
//
// Two lookups keyed by something other than the workflow id stay untagged
// single keys (each command touches only that key): the idempotency-key
// index and the signal-token reverse lookup.
//
// A workflow id containing `}` still keeps all its keys in one slot (the
// tag ends early, identically for every key of that workflow). A prefix
// containing `{` would move the tag into the prefix, pinning every key of
// the prefix to one slot — still correct, just without the spread.
// ---------------------------------------------------------------------------

/** Tag of every cross-workflow index key. */
export const INDEX_HASH_TAG = "{idx}";

export class RedisWorkflowKeys {
  constructor(readonly prefix: string) {}

  // -- Per-workflow keys ({wf:<id>}) ---------------------------------------

  /** The workflow hash; every other per-workflow key starts with it. */
  wf(id: string): string {
    return `${this.prefix}:{wf:${id}}`;
  }

  steps(id: string, run: number): string {
    return `${this.wf(id)}:steps:${run}`;
  }

  tasks(id: string, run: number, stepName: string): string {
    return `${this.wf(id)}:tasks:${run}:${stepName}`;
  }

  signals(id: string): string {
    return `${this.wf(id)}:signals`;
  }

  /** Hash `tokenId → SignalTokenRecord JSON`. */
  signalTokens(id: string): string {
    return `${this.wf(id)}:signal_tokens`;
  }

  /** Hash `idempotencyKey → tokenId`, backing `createSignalToken` dedup. */
  signalTokenIdempotency(id: string): string {
    return `${this.wf(id)}:signal_token_idempotency`;
  }

  runs(id: string): string {
    return `${this.wf(id)}:runs`;
  }

  attempts(id: string): string {
    return `${this.wf(id)}:attempts`;
  }

  lock(id: string): string {
    return `${this.wf(id)}:lock`;
  }

  /**
   * Hash `childId → nonce` of the children this workflow created under its
   * fence: a fenced child create commits here (see `createWorkflow`).
   */
  childIntents(id: string): string {
    return `${this.wf(id)}:child-intents`;
  }

  /** Per-workflow fence-token counter. */
  fence(id: string): string {
    return `${this.wf(id)}:fence`;
  }

  /** Set of stream ids the workflow appended to (for purge). */
  streamIds(id: string): string {
    return `${this.wf(id)}:stream-ids`;
  }

  stream(id: string, streamId: string): string {
    return `${this.wf(id)}:streams:${streamId}`;
  }

  /** Base of the journal keys; scripts append `<step>:idx` and friends. */
  journalBase(id: string): string {
    return `${this.wf(id)}:journal:`;
  }

  /** Set of step names with journal entries. */
  journalSteps(id: string): string {
    return `${this.wf(id)}:journal:steps`;
  }

  /** Per-step sorted set of `${idx}|${branchPath}` members, scored by index. */
  journalIdx(id: string, stepName: string): string {
    return `${this.journalBase(id)}${stepName}:idx`;
  }

  /** Per-entry hash; the key segment encodes `${activityIndex}|${branchPath}`. */
  journalEntry(id: string, stepName: string, activityIndex: number, branchPath: string): string {
    return `${this.journalBase(id)}${stepName}:entry:${activityIndex}|${branchPath}`;
  }

  /** Per-step hash `signalName → ${activityIndex}|${branchPath}` of pending signal waits. */
  journalSignalIdx(id: string, stepName: string): string {
    return `${this.journalBase(id)}${stepName}:signal-idx`;
  }

  // -- Cross-workflow index keys ({idx}) ------------------------------------

  private idx(suffix: string): string {
    return `${this.prefix}:${INDEX_HASH_TAG}:${suffix}`;
  }

  /** Hash `workflowId → iv` the index last applied. */
  get indexVersions(): string {
    return this.idx("ver");
  }

  /** Hash `workflowId → IndexRecord JSON` (filter and sort fields). */
  get indexRecords(): string {
    return this.idx("rec");
  }

  /** Sorted set of every workflow, scored by createdAt ms. */
  get byCreated(): string {
    return this.idx("by-created");
  }

  /** Sorted set of started workflows, scored by startedAt ms. */
  get byStarted(): string {
    return this.idx("by-started");
  }

  /** Sorted set of finished workflows, scored by completedAt ms (drives purge). */
  get byCompleted(): string {
    return this.idx("by-completed");
  }

  status(status: string): string {
    return this.idx(`status:${status}`);
  }

  name(name: string): string {
    return this.idx(`name:${name}`);
  }

  namespace(ns: string): string {
    return this.idx(`ns:${ns}`);
  }

  /** Set of workflow ids created with `parentWorkflowId = parentId`. */
  children(parentId: string): string {
    return this.idx(`children:${parentId}`);
  }

  /** Sorted set across workflows: score = wakeAt ms, member = `{wid}::{step}::{idx}|{path}`. */
  get sleeps(): string {
    return this.idx("sleeps");
  }

  get distinctNames(): string {
    return this.idx("distinct:names");
  }
  get distinctTypes(): string {
    return this.idx("distinct:types");
  }
  get distinctNamespaces(): string {
    return this.idx("distinct:namespaces");
  }
  distinctNamespaceNames(ns: string): string {
    return this.idx(`distinct:names:ns:${ns}`);
  }
  distinctNamespaceTypes(ns: string): string {
    return this.idx(`distinct:types:ns:${ns}`);
  }

  // -- Untagged single-key lookups ------------------------------------------

  /** `(namespace, workflowName, idempotencyKey) → workflowId`, expiring with the key. */
  workflowIdempotency(namespace: string | undefined, workflowName: string, key: string): string {
    return `${this.prefix}:wf-idempotency:${namespace ?? ""}:${workflowName}:${key}`;
  }

  /** `tokenId → workflowId`, so a token resolves by id alone. */
  signalTokenLookup(tokenId: string): string {
    return `${this.prefix}:signal_token:${tokenId}`;
  }
}

/** Escape glob metacharacters so `s` matches literally in `SCAN MATCH`. */
export function escapeGlob(s: string): string {
  return s.replace(/[*?[\]\\]/g, (c) => `\\${c}`);
}
