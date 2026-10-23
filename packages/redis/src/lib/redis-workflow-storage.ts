// ---------------------------------------------------------------------------
// RedisWorkflowStorage — Redis-backed WorkflowStorage, StepAttemptStorage,
// and ActivityJournalStorage.
// ---------------------------------------------------------------------------
//
// Multi-key Lua scripts (terminal transitions, fresh runs) touch keys
// derived from the prefix, so the storage targets a standalone Redis (or a
// cluster where the prefix pins every key to one slot).
// ---------------------------------------------------------------------------

import type {
  WorkflowStorage,
  StepAttemptStorage,
  CompensationLedgerStorage,
  StepCompensationOutcome,
  ActivityJournalStorage,
  JournalEntry,
  JournalExit,
  JournalSlot,
  CompletePendingResult,
  FenceGuard,
  WorkflowOrderBy,
  SignalTokenRecord,
  StreamChunk,
  WorkflowWakeup,
  OrphanedRun,
} from "@promin/workflow";
import type {
  WorkflowState,
  WorkflowStatusSnapshot,
  WorkflowStatus,
  WorkflowRunSummary,
  StepState,
  StepTaskState,
  SignalState,
  StepAttemptRecord,
  RunSource,
} from "@promin/workflow";
import {
  CANCELLED_ERROR,
  CANCELLED_ERROR_TAG,
  FenceTokenMismatchError,
  workflowMetadataMatches,
  WORKFLOW_STATUSES,
  isTerminalWorkflowStatus,
  encodeRunSource,
  decodeRunSource,
  withoutCompensationLedger,
} from "@promin/workflow";
import type { RedisStoreClient } from "./redis-client.ts";
import { SystemWallClock, type WallClock } from "@promin/workflow";

export interface RedisWorkflowStorageConfig {
  redis: RedisStoreClient;
  prefix?: string;
  namespace?: string | null;
  instanceId?: string;
  retention?: {
    completedTtlMs?: number;
    maxRunsPerWorkflow?: number;
  };
  /**
   * Time source for client-side timestamps (everything the client
   * serializes into the Redis payload before `HSET`: createdAt, startedAt,
   * completedAt, deliveredAt, purge cutoffs, fresh-run archive stamps).
   * Default: `SystemWallClock`. Pass a `FakeWallClock` for deterministic tests.
   */
  clock?: WallClock;
}

// -- Lua scripts ----------------------------------------------------------

// Lock is stored as a hash with { lockedBy, token } fields + a PEXPIRE TTL.
// Token is minted from a global INCR counter so each holder's stamp is
// strictly greater than any prior one — mutating writes carry it back
// through every fenced script (`fencedLua`) and a mismatch rejects the stale writer.
//
// TRY_LOCK
// KEYS: [lockKey, counterKey]
// ARGV: [instanceId, lockDurationMs]
// Returns: [acquired (0/1), token (string, empty on miss)]
const TRY_LOCK_LUA = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0, ''}
end
local token = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], 'lockedBy', ARGV[1], 'token', token)
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return {1, tostring(token)}
`;

// PTTL of a lock key (the client surface has no PTTL command).
// KEYS: [lockKey]
const PTTL_LUA = `return redis.call('PTTL', KEYS[1])`;

// RELEASE_LOCK: honor the fence token when provided, else fall back to
// the instanceId check (matches InMemory/Postgres semantics during the
// migration window where some callers don't yet pass guards).
// KEYS: [lockKey]
// ARGV: [instanceId, fenceToken|'']
const RELEASE_LOCK_LUA = `
if ARGV[2] ~= '' then
  if redis.call('HGET', KEYS[1], 'token') == ARGV[2] then
    return redis.call('DEL', KEYS[1])
  end
  return 0
end
if redis.call('HGET', KEYS[1], 'lockedBy') == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

// HEARTBEAT: same fence-or-instanceId semantics as RELEASE_LOCK.
// KEYS: [lockKey]
// ARGV: [instanceId, lockDurationMs, fenceToken|'']
const HEARTBEAT_LUA = `
if ARGV[3] ~= '' then
  if redis.call('HGET', KEYS[1], 'token') == ARGV[3] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
  end
  return 0
end
if redis.call('HGET', KEYS[1], 'lockedBy') == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`;

// Journal: append a COMPLETED activity entry. Idempotent on (wid, step, idx).
// KEYS: [entryHash, idxZset, stepsSet]
// ARGV: [idx, activityName, exitJson, createdAt, stepName, branchPath, payloadHash|'']
const APPEND_ENTRY_LUA = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1],
  'activityName', ARGV[2],
  'stepType', 'activity',
  'phase', 'completed',
  'branchPath', ARGV[6],
  'exit', ARGV[3],
  'createdAt', ARGV[4])
if ARGV[7] ~= '' then
  redis.call('HSET', KEYS[1], 'payloadHash', ARGV[7])
end
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[1] .. '|' .. ARGV[6])
redis.call('SADD', KEYS[3], ARGV[5])
return 1
`;

// Journal: append a PENDING entry (sleep or signal). Idempotent on (wid, step, idx, branch).
// KEYS: [entryHash, idxZset, stepsSet, sleepsZset (global), signalIdxHash]
// ARGV: [idx, activityName, stepType, wakeAtMs|'', createdAt, stepName, sleepsMember|'', signalName|'', branchPath, payloadHash|'']
const APPEND_PENDING_LUA = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1],
  'activityName', ARGV[2],
  'stepType', ARGV[3],
  'phase', 'pending',
  'wakeAt', ARGV[4],
  'branchPath', ARGV[9],
  'createdAt', ARGV[5])
if ARGV[10] ~= '' then
  redis.call('HSET', KEYS[1], 'payloadHash', ARGV[10])
end
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[1] .. '|' .. ARGV[9])
redis.call('SADD', KEYS[3], ARGV[6])
if ARGV[3] == 'sleep' and ARGV[7] ~= '' then
  redis.call('ZADD', KEYS[4], ARGV[4], ARGV[7])
elseif ARGV[3] == 'signal' and ARGV[8] ~= '' then
  redis.call('HSET', KEYS[5], ARGV[8], ARGV[1] .. '|' .. ARGV[9])
end
return 1
`;

// Journal: transition pending -> completed atomically. First writer wins: a
// call on an already-completed (or missing) entry changes nothing.
// KEYS: [entryHash, sleepsZset (global), signalIdxHash]
// ARGV: [exitJson, sleepsMember|'', signalName|'']
// Returns {1, ''} when this call completed the entry, else {0, storedExitJson|''}.
const COMPLETE_PENDING_LUA = `
local phase = redis.call('HGET', KEYS[1], 'phase')
if phase ~= 'pending' then
  return {0, redis.call('HGET', KEYS[1], 'exit') or ''}
end
local stepType = redis.call('HGET', KEYS[1], 'stepType')
redis.call('HSET', KEYS[1], 'phase', 'completed', 'exit', ARGV[1])
if stepType == 'sleep' and ARGV[2] ~= '' then
  redis.call('ZREM', KEYS[2], ARGV[2])
elseif stepType == 'signal' and ARGV[3] ~= '' then
  redis.call('HDEL', KEYS[3], ARGV[3])
end
return {1, ''}
`;

// Journal: delete entries of one step and their index memberships.
// KEYS: [idxZset, sleepsZset (global), signalIdxHash, entryHash_1 .. entryHash_n]
// ARGV: per entry: idxMember, sleepsMember, legacyIdxMember|''
const DISCARD_ENTRIES_LUA = `
for s = 1, #KEYS - 3 do
  local entry = KEYS[3 + s]
  local a = (s - 1) * 3
  local stepType = redis.call('HGET', entry, 'stepType')
  local name = redis.call('HGET', entry, 'activityName')
  redis.call('DEL', entry)
  redis.call('ZREM', KEYS[1], ARGV[a + 1])
  if ARGV[a + 3] ~= '' then redis.call('ZREM', KEYS[1], ARGV[a + 3]) end
  if stepType == 'sleep' then redis.call('ZREM', KEYS[2], ARGV[a + 2]) end
  if stepType == 'signal' and name then
    if redis.call('HGET', KEYS[3], name) == ARGV[a + 1] then redis.call('HDEL', KEYS[3], name) end
  end
end
return 1
`;

// Apply a list of writes computed client-side, in order, as one script.
// ARGV: [opsJson] — a JSON array of ops, each an array of strings:
//   ["ABSENT", key]      — stop with {0} (nothing written) when key exists;
//                          must precede every write.
//   ["STATUS", wfKey, workflowId, statusIdxPrefix, to, from, field, value, ...]
//                        — when the current status is one of the comma-
//                          separated \`from\` statuses (or \`from\` is '*'), set
//                          status = to plus the fields, and move the id
//                          between the status index sets.
//   [command, key, ...]  — any other Redis command, run as given.
// Returns {1, <reply of the last op>}.
const WRITE_OPS_LUA = `
local ops = cjson.decode(ARGV[1])
local last = 1
for _, op in ipairs(ops) do
  local name = op[1]
  if name == 'ABSENT' then
    if redis.call('EXISTS', op[2]) == 1 then return {0} end
  elseif name == 'STATUS' then
    local cur = redis.call('HGET', op[2], 'status')
    local allowed = false
    if cur then
      if op[6] == '*' then
        allowed = true
      else
        for s in string.gmatch(op[6], '[^,]+') do
          if s == cur then allowed = true end
        end
      end
    end
    if allowed then
      redis.call('HSET', op[2], 'status', op[5])
      for i = 7, #op, 2 do redis.call('HSET', op[2], op[i], op[i + 1]) end
      if cur ~= op[5] then
        redis.call('SREM', op[4] .. cur, op[3])
        redis.call('SADD', op[4] .. op[5], op[3])
      end
    end
  else
    last = redis.call(unpack(op))
  end
end
return {1, last}
`;

/** Error-reply prefix a fenced script returns when the fence rejects it. */
const FENCE_REJECTED = "PROMIN_FENCE_REJECTED";

/**
 * Put the fence check in front of a script, so the check and the script's
 * writes run as one atomic script. The fenced script takes the run's lock
 * key as KEYS[1] and the fence token as ARGV[1] ('' for an unfenced call),
 * then the original keys and args, which the body reads as `K` / `A`. A
 * missing lock key (released, or expired through its TTL) or another
 * token rejects the call before anything is written.
 */
function fencedLua(script: string): string {
  return `
local K = {}
for i = 2, #KEYS do K[i - 1] = KEYS[i] end
local A = {}
for i = 2, #ARGV do A[i - 1] = ARGV[i] end
if ARGV[1] ~= '' then
  local held = redis.call('HGET', KEYS[1], 'token')
  if held ~= ARGV[1] then
    return redis.error_reply('${FENCE_REJECTED} ' .. (held or ''))
  end
end
${script.replaceAll("KEYS", "K").replaceAll("ARGV", "A")}`;
}

// Status transition guarded by the current status — the read and the write
// happen in one script, so a concurrent cancel can't be overwritten by a
// late completion (or vice versa).
// KEYS: [wfKey, toStatusIdx, completedZset, fromStatusIdx_1 .. fromStatusIdx_n]
// ARGV: [workflowId, toStatus, nowIso, nowMs, nFields, field_1, value_1, ...,
//        fromStatus_1 .. fromStatus_n]
// Returns the previous status, or false when the workflow is missing or
// its status isn't one of the allowed `from` statuses.
const TRANSITION_STATUS_LUA = `
local status = redis.call('HGET', KEYS[1], 'status')
if not status then return false end
local nFields = tonumber(ARGV[5])
local fromStart = 6 + nFields * 2
local fromKey = nil
for i = fromStart, #ARGV do
  if ARGV[i] == status then fromKey = KEYS[4 + i - fromStart] end
end
if not fromKey then return false end
redis.call('HSET', KEYS[1], 'status', ARGV[2], 'updatedAt', ARGV[3])
for i = 6, fromStart - 1, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
if fromKey ~= KEYS[2] then
  redis.call('SREM', fromKey, ARGV[1])
  redis.call('SADD', KEYS[2], ARGV[1])
end
if KEYS[3] ~= '' and ARGV[4] ~= '' then
  redis.call('ZADD', KEYS[3], ARGV[4], ARGV[1])
end
return status
`;

// Compare-and-set one hash field. Callers read the field, compute the new
// value client-side (keeping JSON fidelity — no cjson round-trip), and
// retry when another writer got in between.
// KEYS: [hash]
// ARGV: [field, expectMissing ('1'|'0'), expected, newValue, requireField|'',
//        extraField_1, extraValue_1, ...]
// Returns 1 on write, 0 on a lost race, -1 when `requireField` is absent.
const HASH_FIELD_CAS_LUA = `
if ARGV[5] ~= '' and redis.call('HEXISTS', KEYS[1], ARGV[5]) == 0 then return -1 end
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if ARGV[2] == '1' then
  if cur then return 0 end
elseif cur ~= ARGV[3] then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
for i = 6, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
return 1
`;

// Create a signal token, deduplicated on (workflowId, idempotencyKey).
// KEYS: [tokensHash, lookupKey, idempotencyHash]
// ARGV: [tokenId, recordJson, workflowId, idempotencyKey|'']
// Returns {1, existingTokenId} on a dedup hit, {0, tokenId} on insert.
const CREATE_SIGNAL_TOKEN_LUA = `
if ARGV[4] ~= '' then
  local existing = redis.call('HGET', KEYS[3], ARGV[4])
  if existing then return {1, existing} end
  redis.call('HSET', KEYS[3], ARGV[4], ARGV[1])
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('SET', KEYS[2], ARGV[3])
return {0, ARGV[1]}
`;

// Start a fresh run: archive the current run, bump the run counter, reset
// status, and drop everything the new run must not replay from — the
// activity journal (every step, plus its members in the global sleeps
// zset) and the delivered signals — all in one atomic script.
// KEYS: [wfKey, runsKey, signalsKey, journalStepsKey, sleepsZset, pendingIdx, completedZset]
// ARGV: [workflowId, expectedRun, summaryJson, maxRuns, nowIso,
//        journalKeyBase ("<prefix>:<id>:journal:"), statusIdxBase ("<prefix>:idx:status:")]
// Returns the new run, or -1 when the run moved on since the caller read it.
const START_FRESH_RUN_LUA = `
local run = redis.call('HGET', KEYS[1], 'run')
if run ~= ARGV[2] then return -1 end
local old = redis.call('HGET', KEYS[1], 'status')
redis.call('RPUSH', KEYS[2], ARGV[3])
redis.call('LTRIM', KEYS[2], -tonumber(ARGV[4]), -1)
local newRun = tonumber(run) + 1
redis.call('HSET', KEYS[1], 'run', tostring(newRun), 'status', 'pending', 'updatedAt', ARGV[5])
redis.call('HDEL', KEYS[1], 'result', 'error', 'errorTag', 'tripwire', 'startedAt', 'completedAt')
redis.call('PERSIST', KEYS[1])
redis.call('PERSIST', KEYS[2])
if old and old ~= 'pending' then
  redis.call('SREM', ARGV[7] .. old, ARGV[1])
  redis.call('SADD', KEYS[6], ARGV[1])
end
redis.call('ZREM', KEYS[7], ARGV[1])
local steps = redis.call('SMEMBERS', KEYS[4])
for _, step in ipairs(steps) do
  local base = ARGV[6] .. step
  local members = redis.call('ZRANGE', base .. ':idx', 0, -1)
  for _, m in ipairs(members) do
    if not string.find(m, '|', 1, true) then m = m .. '|' end
    redis.call('DEL', base .. ':entry:' .. m)
    redis.call('ZREM', KEYS[5], ARGV[1] .. '::' .. step .. '::' .. m)
  end
  redis.call('DEL', base .. ':idx', base .. ':signal-idx')
end
redis.call('DEL', KEYS[4], KEYS[3])
return newRun
`;

// Fenced variants of every script a lock holder writes through.
const FENCED_TRANSITION_STATUS_LUA = fencedLua(TRANSITION_STATUS_LUA);
const FENCED_HASH_FIELD_CAS_LUA = fencedLua(HASH_FIELD_CAS_LUA);
const FENCED_START_FRESH_RUN_LUA = fencedLua(START_FRESH_RUN_LUA);
const FENCED_APPEND_ENTRY_LUA = fencedLua(APPEND_ENTRY_LUA);
const FENCED_APPEND_PENDING_LUA = fencedLua(APPEND_PENDING_LUA);
const FENCED_COMPLETE_PENDING_LUA = fencedLua(COMPLETE_PENDING_LUA);
const FENCED_DISCARD_ENTRIES_LUA = fencedLua(DISCARD_ENTRIES_LUA);
const FENCED_WRITE_OPS_LUA = fencedLua(WRITE_OPS_LUA);

/** Statuses a terminal transition may leave. */
const NON_TERMINAL_STATUSES = WORKFLOW_STATUSES.filter((st) => !isTerminalWorkflowStatus(st));
/** Statuses `cancelWorkflow` may leave. */
const CANCELLABLE_STATUSES: readonly WorkflowStatus[] = ["pending", "running", "suspended"];
/** Bound on compare-and-set retries under contention. */
const MAX_CAS_ATTEMPTS = 100;

/**
 * The error a rejected fenced write throws. `current` is the lock's token
 * at rejection time; null when the lock key is gone — released, or expired
 * through its TTL.
 */
function fenceMismatch(params: {
  workflowId: string;
  provided: string;
  current: string | null;
}): FenceTokenMismatchError {
  const { workflowId, provided, current } = params;
  return new FenceTokenMismatchError({
    workflowId,
    expected: current ?? "(no lock)",
    provided,
    message:
      current === null
        ? `Fenced write for "${workflowId}" rejected — no active lock (released or expired)`
        : `Fenced write for "${workflowId}" rejected — token mismatch (expected "${current}", got "${provided}")`,
  });
}

export class RedisWorkflowStorage
  implements WorkflowStorage, StepAttemptStorage, CompensationLedgerStorage, ActivityJournalStorage
{
  private readonly redis: RedisStoreClient;
  private readonly prefix: string;
  private readonly namespace: string | null;
  private readonly instanceId: string;
  private readonly completedTtlMs?: number;
  private readonly maxRunsPerWorkflow: number;
  private readonly clock: WallClock;

  constructor(config: RedisWorkflowStorageConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "wf";
    this.namespace = config.namespace ?? null;
    this.instanceId = config.instanceId ?? crypto.randomUUID();
    this.completedTtlMs = config.retention?.completedTtlMs;
    this.maxRunsPerWorkflow = config.retention?.maxRunsPerWorkflow ?? 5;
    this.clock = config.clock ?? SystemWallClock;
  }

  // -- Key helpers ----------------------------------------------------------

  private wfKey(id: string): string {
    return `${this.prefix}:${id}`;
  }

  private stepsKey(id: string, run: number): string {
    return `${this.prefix}:${id}:steps:${run}`;
  }

  private tasksKey(id: string, run: number, stepName: string): string {
    return `${this.prefix}:${id}:tasks:${run}:${stepName}`;
  }

  private signalsKey(id: string): string {
    return `${this.prefix}:${id}:signals`;
  }

  /** `(namespace, workflowName, idempotencyKey) → workflowId` index with TTL matching the run's idempotency expiry. */
  private workflowIdempotencyKeyIndex(
    namespace: string | undefined,
    workflowName: string,
    idempotencyKey: string,
  ): string {
    return `${this.prefix}:wf-idempotency:${namespace ?? ""}:${workflowName}:${idempotencyKey}`;
  }

  /** Hash mapping `${tokenId}` → JSON SignalTokenRecord (stored at the workflow scope). */
  private signalTokensKey(workflowId: string): string {
    return `${this.prefix}:${workflowId}:signal_tokens`;
  }

  /** Reverse index from tokenId → workflowId, so the public completion route can look up by tokenId alone. */
  private signalTokenLookupKey(tokenId: string): string {
    return `${this.prefix}:signal_token:${tokenId}`;
  }

  /** Per-workflow set of (idempotencyKey → tokenId), backing dedup for `createSignalToken`. */
  private signalTokenIdempotencyKey(workflowId: string): string {
    return `${this.prefix}:${workflowId}:signal_token_idempotency`;
  }

  private runsKey(id: string): string {
    return `${this.prefix}:${id}:runs`;
  }

  private attemptsKey(id: string): string {
    return `${this.prefix}:${id}:attempts`;
  }

  private lockKey(id: string): string {
    return `${this.prefix}:lock:${id}`;
  }

  /** Global monotonic counter key for fence tokens. One per prefix. */
  private get fenceCounterKey(): string {
    return `${this.prefix}:lock-fence-counter`;
  }

  private statusIndexKey(status: string): string {
    return `${this.prefix}:idx:status:${status}`;
  }

  private nameIndexKey(name: string): string {
    return `${this.prefix}:idx:name:${name}`;
  }

  /** Set of workflow ids created with `parentWorkflowId = parentId`. */
  private childrenIndexKey(parentId: string): string {
    return `${this.prefix}:idx:children:${parentId}`;
  }

  /** Set of stream ids a workflow has appended to (for purge). */
  private streamIdsKey(workflowId: string): string {
    return `${this.prefix}:${workflowId}:stream-ids`;
  }

  private get completedIndexKey(): string {
    return `${this.prefix}:idx:completed`;
  }

  // Distinct-value indexes — maintained on createWorkflow so the dashboard
  // dropdowns see every value ever observed, not just rows still in cache.
  // Per-namespace variants are populated only when the workflow row carries
  // a namespace; the global ("all") variant is always populated.
  private get distinctNamesKey(): string {
    return `${this.prefix}:idx:distinct:names`;
  }
  private get distinctTypesKey(): string {
    return `${this.prefix}:idx:distinct:types`;
  }
  private get distinctNamespacesKey(): string {
    return `${this.prefix}:idx:distinct:namespaces`;
  }
  private distinctNamespaceNamesKey(ns: string): string {
    return `${this.prefix}:idx:distinct:names:ns:${ns}`;
  }
  private distinctNamespaceTypesKey(ns: string): string {
    return `${this.prefix}:idx:distinct:types:ns:${ns}`;
  }

  // -- Journal key helpers --------------------------------------------------

  /** Per-workflow set of step names that have journal entries (for purge/ttl). */
  private journalStepsKey(id: string): string {
    return `${this.prefix}:${id}:journal:steps`;
  }

  /** Per-step sorted set of activity indices, for ordered loadJournal. */
  private journalIdxKey(id: string, stepName: string): string {
    return `${this.prefix}:${id}:journal:${stepName}:idx`;
  }

  /**
   * Per-entry hash: activityName, stepType, phase, exit, wakeAt, createdAt.
   * The entry key encodes (activityIndex, branchPath) — branchPath is `""`
   * for everything pre-`ctx.parallel` (and for sleep/signal yields), so
   * existing keys still resolve unchanged.
   */
  private journalEntryKey(
    id: string,
    stepName: string,
    activityIndex: number,
    branchPath: string,
  ): string {
    // Encoded as `${idx}|${path}` inside the key segment. `|` isn't used
    // elsewhere in the key schema, so it's a safe separator.
    return `${this.prefix}:${id}:journal:${stepName}:entry:${activityIndex}|${branchPath}`;
  }

  /** Per-step hash {signalName → `${activityIndex}|${branchPath}`} for O(1) findPendingSignal. */
  private journalSignalIdxKey(id: string, stepName: string): string {
    return `${this.prefix}:${id}:journal:${stepName}:signal-idx`;
  }

  /** Global sorted set across workflows: score=wakeAt_ms, member="{wid}::{step}::{idx}|{path}". */
  private get sleepsKey(): string {
    return `${this.prefix}:sleeps`;
  }

  private sleepsMember(
    id: string,
    stepName: string,
    activityIndex: number,
    branchPath: string,
  ): string {
    return `${id}::${stepName}::${activityIndex}|${branchPath}`;
  }

  private parseSleepsMember(member: string): {
    workflowId: string;
    stepName: string;
    activityIndex: number;
    branchPath: string;
  } | null {
    // Parse from the right: last "::" separates idx|branchPath; next-to-last
    // separates step. Tolerates "::" inside workflowId but not step name.
    const lastSep = member.lastIndexOf("::");
    if (lastSep < 0) return null;
    const idxAndPath = member.slice(lastSep + 2);
    const rest = member.slice(0, lastSep);
    const midSep = rest.lastIndexOf("::");
    if (midSep < 0) return null;
    const stepName = rest.slice(midSep + 2);
    const workflowId = rest.slice(0, midSep);
    const pipe = idxAndPath.indexOf("|");
    const idxStr = pipe === -1 ? idxAndPath : idxAndPath.slice(0, pipe);
    const branchPath = pipe === -1 ? "" : idxAndPath.slice(pipe + 1);
    const activityIndex = Number(idxStr);
    if (!Number.isFinite(activityIndex)) return null;
    return { workflowId, stepName, activityIndex, branchPath };
  }

  // -- Serialization helpers ------------------------------------------------

  private resolveNamespace(workflowNamespace?: string): string | undefined {
    return workflowNamespace ?? this.namespace ?? undefined;
  }

  private serializeDate(d: Date): string {
    return d.toISOString();
  }

  private parseDate(s: string): Date {
    return new Date(s);
  }

  /** Build a WorkflowState from the raw workflow hash + steps + tasks. */
  private async assembleWorkflow(raw: Record<string, string>): Promise<WorkflowState> {
    const id = raw.id;
    const run = Number(raw.run);

    // Load steps for current run
    const stepsRaw = await this.redis.hgetall(this.stepsKey(id, run));
    const steps: Record<string, StepState> = {};

    for (const [stepName, json] of Object.entries(stepsRaw)) {
      const step = this.parseStepState(json);

      // Load tasks if step is a map step
      if (step.stepType === "map") {
        const tasksRaw = await this.redis.hgetall(this.tasksKey(id, run, stepName));
        if (tasksRaw && Object.keys(tasksRaw).length > 0) {
          const tasks: StepTaskState[] = [];
          for (const taskJson of Object.values(tasksRaw)) {
            tasks.push(this.parseTaskState(taskJson));
          }
          tasks.sort((a, b) => a.taskIndex - b.taskIndex);
          steps[stepName] = { ...step, tasks };
          continue;
        }
      }

      steps[stepName] = step;
    }

    return {
      workflowId: id,
      workflowName: raw.workflowName,
      workflowType: raw.workflowType || undefined,
      parentWorkflowId: raw.parentWorkflowId || undefined,
      namespace: raw.namespace || undefined,
      status: raw.status as WorkflowStatus,
      version: raw.version || undefined,
      run,
      input: JSON.parse(raw.input),
      result: raw.result ? JSON.parse(raw.result) : undefined,
      error: raw.error || undefined,
      errorTag: raw.errorTag || undefined,
      tripwire: raw.tripwire ? JSON.parse(raw.tripwire) : undefined,
      runSource: raw.runSource ? decodeRunSource(Number(raw.runSource)) : undefined,
      runSourceId: raw.runSourceId || undefined,
      metadata: raw.metadata ? JSON.parse(raw.metadata) : undefined,
      steps,
      createdAt: this.parseDate(raw.createdAt),
      startedAt: raw.startedAt ? this.parseDate(raw.startedAt) : undefined,
      updatedAt: this.parseDate(raw.updatedAt),
      completedAt: raw.completedAt ? this.parseDate(raw.completedAt) : undefined,
    };
  }

  private parseStepState(json: string): StepState {
    const s = JSON.parse(json);
    return {
      ...s,
      startedAt: s.startedAt ? new Date(s.startedAt) : undefined,
      completedAt: s.completedAt ? new Date(s.completedAt) : undefined,
      wakeAt: s.wakeAt ? new Date(s.wakeAt) : undefined,
      signalTimeoutAt: s.signalTimeoutAt ? new Date(s.signalTimeoutAt) : undefined,
      compensatedAt: s.compensatedAt ? new Date(s.compensatedAt) : undefined,
    };
  }

  private parseTaskState(json: string): StepTaskState {
    const t = JSON.parse(json);
    return {
      ...t,
      startedAt: t.startedAt ? new Date(t.startedAt) : undefined,
      completedAt: t.completedAt ? new Date(t.completedAt) : undefined,
    };
  }

  private serializeStepState(step: StepState): string {
    return JSON.stringify({
      ...step,
      // Strip tasks — stored separately
      tasks: undefined,
      startedAt: step.startedAt ? this.serializeDate(step.startedAt) : undefined,
      completedAt: step.completedAt ? this.serializeDate(step.completedAt) : undefined,
      wakeAt: step.wakeAt ? this.serializeDate(step.wakeAt) : undefined,
      signalTimeoutAt: step.signalTimeoutAt ? this.serializeDate(step.signalTimeoutAt) : undefined,
      compensatedAt: step.compensatedAt ? this.serializeDate(step.compensatedAt) : undefined,
    });
  }

  // -- Workflow CRUD --------------------------------------------------------

  async createWorkflow(
    params: {
      workflowId: string;
      workflowName: string;
      input: unknown;
      workflowType?: string;
      parentWorkflowId?: string;
      namespace?: string;
      metadata?: Record<string, unknown>;
      version?: string;
      runSource?: RunSource;
      runSourceId?: string;
      idempotencyKey?: string;
      idempotencyExpiresAt?: Date;
    },
    guard?: FenceGuard,
  ): Promise<{ created: true } | { created: false; existing: WorkflowState }> {
    if (guard?.fenceToken && params.parentWorkflowId === undefined) {
      throw new Error("createWorkflow: a fenced create needs parentWorkflowId");
    }
    // Idempotency-key path: check the index first. SET-NX below claims it
    // atomically — concurrent creates serialize, the loser falls through
    // to attach to the winning row.
    if (params.idempotencyKey) {
      const ns = this.resolveNamespace(params.namespace);
      const idxKey = this.workflowIdempotencyKeyIndex(
        ns,
        params.workflowName,
        params.idempotencyKey,
      );
      const cachedId = await this.redis.get(idxKey);
      if (cachedId) {
        const raw = await this.redis.hgetall(this.wfKey(cachedId));
        if (raw?.idempotencyExpiresAt) {
          const expiresAt = new Date(raw.idempotencyExpiresAt);
          if (expiresAt.getTime() > this.clock.now().getTime()) {
            const existing = await this.loadWorkflow(cachedId);
            if (existing) return { created: false, existing };
          }
        }
      }
    }

    const existingRaw = await this.redis.hgetall(this.wfKey(params.workflowId));
    if (existingRaw && existingRaw.id) {
      const existing = await this.loadWorkflow(params.workflowId);
      return { created: false, existing: existing! };
    }

    const now = this.serializeDate(this.clock.now());
    const ns = this.resolveNamespace(params.namespace);

    const fields: Record<string, string> = {
      id: params.workflowId,
      workflowName: params.workflowName,
      status: "pending",
      run: "1",
      input: JSON.stringify(params.input),
      createdAt: now,
      updatedAt: now,
    };
    if (params.workflowType) fields.workflowType = params.workflowType;
    if (params.parentWorkflowId) fields.parentWorkflowId = params.parentWorkflowId;
    if (ns) fields.namespace = ns;
    if (params.metadata) fields.metadata = JSON.stringify(params.metadata);
    if (params.version) fields.version = params.version;
    if (params.runSource !== undefined) {
      fields.runSource = String(encodeRunSource(params.runSource));
    }
    if (params.runSourceId) fields.runSourceId = params.runSourceId;
    if (params.idempotencyKey) fields.idempotencyKey = params.idempotencyKey;
    if (params.idempotencyExpiresAt) {
      fields.idempotencyExpiresAt = this.serializeDate(params.idempotencyExpiresAt);
    }

    // The row and its index entries land in one script that first checks
    // the row is still absent (a concurrent create wins cleanly) and, for
    // a fenced child create, that the parent's lock is still ours.
    const { applied } = await this.writeOps({
      workflowId: params.parentWorkflowId ?? params.workflowId,
      guard,
      ops: [
        ["ABSENT", this.wfKey(params.workflowId)],
        ["HSET", this.wfKey(params.workflowId), ...Object.entries(fields).flat()],
        ["SADD", this.statusIndexKey("pending"), params.workflowId],
        ["SADD", this.nameIndexKey(params.workflowName), params.workflowId],
      ],
    });
    if (!applied) {
      const existing = await this.loadWorkflow(params.workflowId);
      // Gone again (purged in between): try the create once more.
      return existing ? { created: false, existing } : this.createWorkflow(params, guard);
    }

    // Atomic claim of the idempotency index. SET NX with PX expires the
    // index entry exactly at the run's idempotency_expires_at — concurrent
    // creates that race here lose the SET NX and back out below.
    if (params.idempotencyKey && params.idempotencyExpiresAt) {
      const idxKey = this.workflowIdempotencyKeyIndex(
        ns,
        params.workflowName,
        params.idempotencyKey,
      );
      const ttlMs = params.idempotencyExpiresAt.getTime() - this.clock.now().getTime();
      if (ttlMs > 0) {
        const won = await this.redis.set(idxKey, params.workflowId, "NX", "PX", ttlMs);
        if (!won) {
          // Lost the race — undo the workflow row and resolve to the winner.
          await this.redis.del(this.wfKey(params.workflowId));
          await this.redis.srem(this.statusIndexKey("pending"), params.workflowId);
          await this.redis.srem(this.nameIndexKey(params.workflowName), params.workflowId);
          const winnerId = await this.redis.get(idxKey);
          if (winnerId) {
            const existing = await this.loadWorkflow(winnerId);
            if (existing) return { created: false, existing };
          }
        }
      }
    }

    if (params.parentWorkflowId) {
      await this.redis.sadd(this.childrenIndexKey(params.parentWorkflowId), params.workflowId);
    }

    // Distinct-value indexes — record every name/type/namespace ever seen
    // so the dashboard dropdowns stay correct even after rows are purged.
    await this.redis.sadd(this.distinctNamesKey, params.workflowName);
    if (params.workflowType) await this.redis.sadd(this.distinctTypesKey, params.workflowType);
    if (ns) {
      await this.redis.sadd(this.distinctNamespacesKey, ns);
      await this.redis.sadd(this.distinctNamespaceNamesKey(ns), params.workflowName);
      if (params.workflowType) {
        await this.redis.sadd(this.distinctNamespaceTypesKey(ns), params.workflowType);
      }
    }
    return { created: true };
  }

  async findWorkflowByIdempotencyKey(params: {
    workflowName: string;
    namespace?: string;
    idempotencyKey: string;
    now: Date;
  }): Promise<{ workflowId: string } | null> {
    const ns = this.resolveNamespace(params.namespace);
    const idxKey = this.workflowIdempotencyKeyIndex(ns, params.workflowName, params.idempotencyKey);
    const cachedId = await this.redis.get(idxKey);
    if (!cachedId) return null;
    // Defense-in-depth: confirm the workflow's stored expiry is unexpired
    // before returning. The index has its own PEXPIREAT, but a clock skew
    // between Redis and runner could surface a "live" index entry past
    // the row's expiry.
    const raw = await this.redis.hgetall(this.wfKey(cachedId));
    if (!raw?.idempotencyExpiresAt) return null;
    const expiresAt = new Date(raw.idempotencyExpiresAt);
    if (expiresAt.getTime() <= params.now.getTime()) return null;
    return { workflowId: cachedId };
  }

  async distinctWorkflowNames(params?: { namespace?: string }): Promise<string[]> {
    const ns = params?.namespace ?? this.namespace;
    const key = ns ? this.distinctNamespaceNamesKey(ns) : this.distinctNamesKey;
    const members = await this.redis.smembers(key);
    return members.sort();
  }

  async distinctWorkflowTypes(params?: { namespace?: string }): Promise<string[]> {
    const ns = params?.namespace ?? this.namespace;
    const key = ns ? this.distinctNamespaceTypesKey(ns) : this.distinctTypesKey;
    const members = await this.redis.smembers(key);
    return members.sort();
  }

  async distinctNamespaces(): Promise<string[]> {
    const members = await this.redis.smembers(this.distinctNamespacesKey);
    return members.sort();
  }

  async loadWorkflowStatus(workflowId: string): Promise<WorkflowStatusSnapshot | null> {
    const [id, status, error, errorTag] = ((await this.redis.eval(
      `return redis.call('HMGET', KEYS[1], 'id', 'status', 'error', 'errorTag')`,
      1,
      this.wfKey(workflowId),
    )) ?? []) as Array<string | null | false>;
    if (!id || !status) return null;
    return {
      status: status as WorkflowStatus,
      ...(error ? { error } : {}),
      ...(errorTag ? { errorTag } : {}),
    };
  }

  async loadWorkflow(workflowId: string): Promise<WorkflowState | null> {
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return null;
    return this.assembleWorkflow(raw);
  }

  async listWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    version?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    runSource?: RunSource;
    runSourceId?: string;
    metadata?: Record<string, unknown>;
    limit?: number;
    offset?: number;
    orderBy?: WorkflowOrderBy;
    orderDir?: "asc" | "desc";
  }): Promise<WorkflowState[]> {
    // We need the full filtered set in memory before sorting + paginating;
    // streaming-with-early-exit doesn't compose with order-by.
    const all: WorkflowState[] = [];
    for (const raw of await this.filteredWorkflowHashes(params)) {
      all.push(await this.assembleWorkflow(raw));
    }

    const orderBy = params?.orderBy ?? "startedAt";
    const orderDir = params?.orderDir ?? "desc";
    all.sort(makeWorkflowStateComparator(orderBy, orderDir));

    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? all.length;
    return all.slice(offset, offset + limit);
  }

  /** Count over the same filters as `listWorkflows`, without assembling steps. */
  async countWorkflows(params?: {
    status?: WorkflowStatus;
    name?: string;
    version?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    runSource?: RunSource;
    runSourceId?: string;
    metadata?: Record<string, unknown>;
  }): Promise<number> {
    return (await this.filteredWorkflowHashes(params)).length;
  }

  /**
   * Workflow hashes matching the list filters: candidates come from the
   * status / name / parent index sets, the remaining filters are applied
   * to each hash.
   */
  private async filteredWorkflowHashes(params?: {
    status?: WorkflowStatus;
    name?: string;
    version?: string;
    type?: string;
    parentId?: string;
    namespace?: string;
    runSource?: RunSource;
    runSourceId?: string;
    metadata?: Record<string, unknown>;
  }): Promise<Record<string, string>[]> {
    // Collect candidate ID sets based on filters
    const indexKeys: string[] = [];

    if (params?.status) {
      indexKeys.push(this.statusIndexKey(params.status));
    }
    if (params?.name) {
      indexKeys.push(this.nameIndexKey(params.name));
    }
    if (params?.parentId) {
      indexKeys.push(this.childrenIndexKey(params.parentId));
    }

    let candidateIds: string[];

    if (indexKeys.length > 1) {
      candidateIds = await this.redis.sinter(...indexKeys);
    } else if (indexKeys.length === 1) {
      candidateIds = await this.redis.smembers(indexKeys[0]);
    } else {
      // No index filters — scan every status set.
      const idSets = await Promise.all(
        WORKFLOW_STATUSES.map((s) => this.redis.smembers(this.statusIndexKey(s))),
      );
      candidateIds = [...new Set(idSets.flat())];
    }

    const ns = params?.namespace ?? this.namespace;
    const metadataFilter = params?.metadata;

    // Load + apply filters not covered by indexes (namespace, version, type,
    // parentId, run source, metadata).
    const matched: Record<string, string>[] = [];
    for (const id of candidateIds) {
      const raw = await this.redis.hgetall(this.wfKey(id));
      if (!raw || !raw.id) continue;
      if (ns && raw.namespace !== ns) continue;
      if (params?.version !== undefined && (raw.version || undefined) !== params.version) continue;
      if (params?.type && raw.workflowType !== params.type) continue;
      if (params?.parentId && raw.parentWorkflowId !== params.parentId) continue;
      if (
        params?.runSource !== undefined &&
        raw.runSource !== String(encodeRunSource(params.runSource))
      ) {
        continue;
      }
      if (params?.runSourceId !== undefined && raw.runSourceId !== params.runSourceId) continue;
      if (metadataFilter) {
        const metadata = raw.metadata ? JSON.parse(raw.metadata) : undefined;
        if (!workflowMetadataMatches(metadata, metadataFilter)) continue;
      }
      matched.push(raw);
    }
    return matched;
  }

  async cancelWorkflow(
    workflowId: string,
    options?: { cascade?: boolean },
    guard?: FenceGuard,
  ): Promise<void> {
    await this.transitionStatus({
      workflowId,
      guard,
      to: "failed",
      from: CANCELLABLE_STATUSES,
      fields: { error: CANCELLED_ERROR, errorTag: CANCELLED_ERROR_TAG },
    });

    if (options?.cascade) {
      const children = await this.redis.smembers(this.childrenIndexKey(workflowId));
      for (const childId of children) {
        await this.cancelWorkflow(childId, { cascade: true });
      }
    }
  }

  /**
   * Atomically move a workflow to a terminal status when its current
   * status is one of `from`. Returns the previous status, or null when the
   * workflow is missing or its status isn't one of `from`. Fenced by
   * `guard` in the same script.
   */
  private async transitionStatus(params: {
    workflowId: string;
    guard?: FenceGuard;
    to: WorkflowStatus;
    from: readonly WorkflowStatus[];
    fields: Record<string, string>;
  }): Promise<WorkflowStatus | null> {
    const now = this.clock.now();
    const nowIso = this.serializeDate(now);
    const fieldArgs = Object.entries({ completedAt: nowIso, ...params.fields }).flat();
    const result = await this.evalFenced({
      script: FENCED_TRANSITION_STATUS_LUA,
      workflowId: params.workflowId,
      guard: params.guard,
      keys: [
        this.wfKey(params.workflowId),
        this.statusIndexKey(params.to),
        this.completedIndexKey,
        ...params.from.map((st) => this.statusIndexKey(st)),
      ],
      args: [
        params.workflowId,
        params.to,
        nowIso,
        String(now.getTime()),
        String(fieldArgs.length / 2),
        ...fieldArgs,
        ...params.from,
      ],
    });
    return typeof result === "string" ? (result as WorkflowStatus) : null;
  }

  // -- Step results ---------------------------------------------------------

  /** Op moving a `pending` run to `running` on its first step write. */
  private markRunningOp(workflowId: string, nowIso: string): string[] {
    return this.statusOp({
      workflowId,
      to: "running",
      from: ["pending"],
      fields: { startedAt: nowIso, updatedAt: nowIso },
    });
  }

  /** Completed step row for `saveStepResult` / `batchSaveStepResults`. */
  private completedStep(params: {
    run: number;
    existing: Partial<StepState>;
    record: {
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    };
    now: Date;
  }): StepState {
    const { run, existing, record, now } = params;
    return {
      stepName: record.stepName,
      run,
      status: "completed",
      dependsOn: existing.dependsOn ?? [],
      stepType: existing.stepType ?? "single",
      result: record.result,
      // `record.metadata` wins when provided; otherwise preserve whatever
      // was already on the step (e.g. metadata written at execute time).
      metadata: record.metadata ?? existing.metadata,
      startedAt: record.startedAt,
      completedAt: now,
      durationMs: record.durationMs,
      attempt: ((existing.attempt as number) ?? 0) + 1,
    };
  }

  async saveStepResult(
    params: {
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    await this.batchSaveStepResults([params], guard);
  }

  async batchSaveStepResults(
    records: ReadonlyArray<{
      workflowId: string;
      stepName: string;
      result: unknown;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    }>,
    guard?: FenceGuard,
  ): Promise<void> {
    // Reads (the run number and the existing step rows, to keep dependsOn /
    // stepType / attempt) happen up front; each workflow's writes — the
    // pending → running move, the step rows and `updatedAt` — then land in
    // one fenced script. A batch is atomic per workflow.
    const byWf = new Map<string, Array<(typeof records)[number]>>();
    for (const r of records) {
      const bucket = byWf.get(r.workflowId);
      if (bucket) bucket.push(r);
      else byWf.set(r.workflowId, [r]);
    }

    const now = this.clock.now();
    const nowIso = this.serializeDate(now);

    for (const [wfId, rs] of byWf) {
      const raw = await this.redis.hgetall(this.wfKey(wfId));
      if (!raw || !raw.id) continue;
      const run = Number(raw.run);
      const stepsHashKey = this.stepsKey(wfId, run);
      const existingAll = await this.redis.hgetall(stepsHashKey);

      const ops: string[][] = [this.markRunningOp(wfId, nowIso)];
      for (const r of rs) {
        const existingJson = existingAll?.[r.stepName];
        const existing: Partial<StepState> = existingJson ? JSON.parse(existingJson) : {};
        const step = this.completedStep({ run, existing, record: r, now });
        ops.push(["HSET", stepsHashKey, r.stepName, this.serializeStepState(step)]);
      }
      ops.push(["HSET", this.wfKey(wfId), "updatedAt", nowIso]);
      await this.writeOps({ workflowId: wfId, guard, ops });
    }
  }

  async saveStepFailure(
    params: {
      workflowId: string;
      stepName: string;
      error: string;
      errorTag?: string;
      durationMs: number;
      startedAt: Date;
      metadata?: Record<string, unknown>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    const raw = await this.redis.hgetall(this.wfKey(params.workflowId));
    if (!raw || !raw.id) return;

    const run = Number(raw.run);
    const now = this.clock.now();
    const nowIso = this.serializeDate(now);
    const stepsHashKey = this.stepsKey(params.workflowId, run);

    const existingJson = await this.redis.hget(stepsHashKey, params.stepName);
    const existing: Partial<StepState> = existingJson ? JSON.parse(existingJson) : {};

    const step: StepState = {
      stepName: params.stepName,
      run,
      status: "failed",
      dependsOn: existing.dependsOn ?? [],
      stepType: existing.stepType ?? "single",
      error: params.error,
      ...(params.errorTag !== undefined && { errorTag: params.errorTag }),
      metadata: params.metadata ?? existing.metadata,
      startedAt: params.startedAt,
      completedAt: now,
      durationMs: params.durationMs,
      attempt: ((existing.attempt as number) ?? 0) + 1,
    };

    await this.writeOps({
      workflowId: params.workflowId,
      guard,
      ops: [
        this.markRunningOp(params.workflowId, nowIso),
        ["HSET", stepsHashKey, params.stepName, this.serializeStepState(step)],
        ["HSET", this.wfKey(params.workflowId), "updatedAt", nowIso],
      ],
    });
  }

  // -- Task results ---------------------------------------------------------

  async saveTaskResult(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      result: unknown;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    await this.saveTask({
      workflowId: params.workflowId,
      stepName: params.stepName,
      taskIndex: params.taskIndex,
      outcome: { status: "completed", result: params.result },
      guard,
    });
  }

  async saveTaskFailure(
    params: {
      workflowId: string;
      stepName: string;
      taskIndex: number;
      error: string;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    await this.saveTask({
      workflowId: params.workflowId,
      stepName: params.stepName,
      taskIndex: params.taskIndex,
      outcome: { status: "failed", error: params.error },
      guard,
    });
  }

  /**
   * Write one map task row with `outcome`, creating the parent step row if
   * it doesn't exist yet, in one fenced script.
   */
  private async saveTask(params: {
    workflowId: string;
    stepName: string;
    taskIndex: number;
    outcome: { status: "completed"; result: unknown } | { status: "failed"; error: string };
    guard?: FenceGuard;
  }): Promise<void> {
    const raw = await this.redis.hgetall(this.wfKey(params.workflowId));
    if (!raw || !raw.id) return;

    const run = Number(raw.run);
    const now = this.clock.now();
    const tasksHashKey = this.tasksKey(params.workflowId, run, params.stepName);

    const taskField = String(params.taskIndex);
    const existingTaskJson = await this.redis.hget(tasksHashKey, taskField);
    const prev: Partial<StepTaskState> = existingTaskJson ? JSON.parse(existingTaskJson) : {};

    const task: StepTaskState = {
      taskIndex: params.taskIndex,
      ...params.outcome,
      startedAt: prev.startedAt ? new Date(prev.startedAt as unknown as string) : now,
      completedAt: now,
      attempt: ((prev.attempt as number) ?? 0) + 1,
    };
    const parentStep: StepState = {
      stepName: params.stepName,
      run,
      status: "running",
      dependsOn: [],
      stepType: "map",
      attempt: 1,
    };

    await this.writeOps({
      workflowId: params.workflowId,
      guard: params.guard,
      ops: [
        [
          "HSET",
          tasksHashKey,
          taskField,
          JSON.stringify({
            ...task,
            startedAt: task.startedAt ? this.serializeDate(task.startedAt) : undefined,
            completedAt: task.completedAt ? this.serializeDate(task.completedAt) : undefined,
          }),
        ],
        // Ensure the parent step row exists.
        [
          "HSETNX",
          this.stepsKey(params.workflowId, run),
          params.stepName,
          this.serializeStepState(parentStep),
        ],
        ["HSET", this.wfKey(params.workflowId), "updatedAt", this.serializeDate(now)],
      ],
    });
  }

  // -- Workflow completion --------------------------------------------------

  async completeWorkflow(workflowId: string, result: unknown, guard?: FenceGuard): Promise<void> {
    await this.finishWorkflow({
      workflowId,
      guard,
      to: "completed",
      fields: { result: JSON.stringify(result) },
    });
  }

  async failWorkflow(
    workflowId: string,
    error: string,
    guard?: FenceGuard,
    details?: { readonly errorTag?: string },
  ): Promise<void> {
    await this.finishWorkflow({
      workflowId,
      guard,
      to: "failed",
      fields: { error, ...(details?.errorTag !== undefined && { errorTag: details.errorTag }) },
    });
  }

  async tripwireWorkflow(workflowId: string, reason: unknown, guard?: FenceGuard): Promise<void> {
    await this.finishWorkflow({
      workflowId,
      guard,
      to: "tripwire",
      fields: { tripwire: JSON.stringify(reason) },
    });
  }

  /** Terminal transition from any non-terminal status, then retention TTLs. */
  private async finishWorkflow(params: {
    workflowId: string;
    guard?: FenceGuard;
    to: WorkflowStatus;
    fields: Record<string, string>;
  }): Promise<void> {
    const previous = await this.transitionStatus({ ...params, from: NON_TERMINAL_STATUSES });
    if (previous === null || !this.completedTtlMs) return;
    const run = await this.redis.hget(this.wfKey(params.workflowId), "run");
    if (run) await this.applyTtl(params.workflowId, Number(run));
  }

  private async applyTtl(workflowId: string, run: number): Promise<void> {
    if (!this.completedTtlMs) return;
    const ttl = this.completedTtlMs;

    // Apply TTL to workflow hash and sub-keys
    await this.redis.pexpire(this.wfKey(workflowId), ttl);
    await this.redis.pexpire(this.stepsKey(workflowId, run), ttl);
    await this.redis.pexpire(this.signalsKey(workflowId), ttl);
    await this.redis.pexpire(this.runsKey(workflowId), ttl);
    await this.redis.pexpire(this.attemptsKey(workflowId), ttl);

    // TTL task hashes for current run steps
    const stepNames = await this.redis.hkeys(this.stepsKey(workflowId, run));
    for (const stepName of stepNames) {
      await this.redis.pexpire(this.tasksKey(workflowId, run, stepName), ttl);
    }

    // TTL journal keys. The global sleeps zset is shared across workflows
    // and is NOT expired; purgeCompleted + completePendingEntry clean up
    // this workflow's members.
    const journalSteps = await this.redis.smembers(this.journalStepsKey(workflowId));
    if (journalSteps.length > 0) {
      await this.redis.pexpire(this.journalStepsKey(workflowId), ttl);
      for (const stepName of journalSteps) {
        await this.redis.pexpire(this.journalIdxKey(workflowId, stepName), ttl);
        await this.redis.pexpire(this.journalSignalIdxKey(workflowId, stepName), ttl);
        const members = await this.redis.zrangebyscore(
          this.journalIdxKey(workflowId, stepName),
          "-inf",
          "+inf",
        );
        for (const member of members) {
          const { activityIndex, branchPath } = parseJournalMember(member);
          await this.redis.pexpire(
            this.journalEntryKey(workflowId, stepName, activityIndex, branchPath),
            ttl,
          );
        }
      }
    }
  }

  // -- Suspend / Signal -----------------------------------------------------

  async suspendWorkflow(
    workflowId: string,
    stepName: string,
    stepUpdate: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void> {
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return;

    const run = Number(raw.run);
    const now = this.clock.now();

    // Load existing step
    const existingJson = await this.redis.hget(this.stepsKey(workflowId, run), stepName);
    const existing: Partial<StepState> = existingJson ? JSON.parse(existingJson) : {};

    const step = {
      stepName,
      run,
      dependsOn: existing.dependsOn ?? [],
      stepType: existing.stepType ?? "single",
      attempt: existing.attempt ?? 1,
      startedAt: existing.startedAt ?? this.serializeDate(now),
      ...stepUpdate,
    };

    // Serialize dates in stepUpdate
    const serialized: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(step)) {
      if (v instanceof Date) {
        serialized[k] = this.serializeDate(v);
      } else {
        serialized[k] = v;
      }
    }

    // The step row, the status and its index move land in one fenced script.
    await this.writeOps({
      workflowId,
      guard,
      ops: [
        ["HSET", this.stepsKey(workflowId, run), stepName, JSON.stringify(serialized)],
        this.statusOp({
          workflowId,
          to: "suspended",
          from: "*",
          fields: { updatedAt: this.serializeDate(now) },
        }),
      ],
    });
  }

  async deliverSignal(workflowId: string, signalName: string, payload: unknown): Promise<void> {
    const signal: SignalState = {
      signalName,
      payload,
      deliveredAt: this.clock.now(),
    };
    await this.redis.hset(
      this.signalsKey(workflowId),
      signalName,
      JSON.stringify({
        ...signal,
        deliveredAt: this.serializeDate(signal.deliveredAt),
      }),
    );
  }

  async loadSignals(workflowId: string): Promise<SignalState[]> {
    const raw = await this.redis.hgetall(this.signalsKey(workflowId));
    if (!raw || Object.keys(raw).length === 0) return [];

    return Object.values(raw).map((json) => {
      const s = JSON.parse(json);
      return {
        signalName: s.signalName,
        payload: s.payload,
        deliveredAt: new Date(s.deliveredAt),
      };
    });
  }

  async setWorkflowMetadata(
    workflowId: string,
    patch: Record<string, unknown>,
    guard?: FenceGuard,
  ): Promise<void> {
    // Metadata is one JSON string field on the workflow hash. Merge
    // client-side, then compare-and-set against the value we read, retrying
    // when a concurrent patch landed first — so no patch is lost. The fence
    // is checked in the same script as each compare-and-set.
    const key = this.wfKey(workflowId);
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      const raw = await this.redis.hget(key, "metadata");
      const merged: Record<string, unknown> = raw ? JSON.parse(raw) : {};
      for (const [k, v] of Object.entries(patch)) {
        if (v === null) delete merged[k];
        else merged[k] = v;
      }
      const written = await this.evalFenced({
        script: FENCED_HASH_FIELD_CAS_LUA,
        workflowId,
        guard,
        keys: [key],
        args: [
          "metadata",
          raw === null ? "1" : "0",
          raw ?? "",
          JSON.stringify(merged),
          "id",
          "updatedAt",
          this.serializeDate(this.clock.now()),
        ],
      });
      if (written !== 0) return; // 1 = written, -1 = no such workflow
    }
    throw new Error(`setWorkflowMetadata: gave up on "${workflowId}" after contention`);
  }

  // ---------------------------------------------------------------------------
  // Signal tokens — public-bearer authorization for deliverSignal
  // ---------------------------------------------------------------------------

  async createSignalToken(params: {
    tokenId: string;
    workflowId: string;
    signalName: string;
    bearer: string;
    tags: ReadonlyArray<string>;
    idempotencyKey?: string | null;
    expiresAt: Date;
  }): Promise<{ record: SignalTokenRecord; isCached: boolean }> {
    const record: SignalTokenRecord = {
      tokenId: params.tokenId,
      workflowId: params.workflowId,
      signalName: params.signalName,
      bearer: params.bearer,
      tags: [...params.tags],
      idempotencyKey: params.idempotencyKey ?? null,
      expiresAt: params.expiresAt,
      completedAt: null,
      completedValue: null,
      createdAt: this.clock.now(),
    };
    // Dedup check and insert in one script: two concurrent creates with the
    // same idempotency key resolve to one token.
    const [cached, tokenId] = (await this.redis.eval(
      CREATE_SIGNAL_TOKEN_LUA,
      3,
      this.signalTokensKey(params.workflowId),
      this.signalTokenLookupKey(params.tokenId),
      this.signalTokenIdempotencyKey(params.workflowId),
      params.tokenId,
      serializeSignalToken(record),
      params.workflowId,
      params.idempotencyKey ?? "",
    )) as [number, string];
    if (cached === 1) {
      const existing = await this.findSignalTokenById(tokenId);
      if (!existing) throw new Error(`createSignalToken: deduplicated token ${tokenId} missing`);
      return { record: existing, isCached: true };
    }
    return { record, isCached: false };
  }

  async findSignalTokenById(tokenId: string): Promise<SignalTokenRecord | null> {
    const workflowId = await this.redis.get(this.signalTokenLookupKey(tokenId));
    if (!workflowId) return null;
    const raw = await this.redis.hget(this.signalTokensKey(workflowId), tokenId);
    return raw ? deserializeSignalToken(raw) : null;
  }

  async markSignalTokenCompleted(params: {
    tokenId: string;
    value: unknown;
    now: Date;
  }): Promise<
    | { outcome: "delivered"; record: SignalTokenRecord }
    | { outcome: "already_completed"; record: SignalTokenRecord }
  > {
    // Compare-and-set against the pending record we read: exactly one
    // concurrent completer swaps it, every other one re-reads and sees the
    // completed record.
    const workflowId = await this.redis.get(this.signalTokenLookupKey(params.tokenId));
    if (!workflowId) throw new Error(`signal token ${params.tokenId} not found`);
    const hashKey = this.signalTokensKey(workflowId);
    for (let attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      const raw = await this.redis.hget(hashKey, params.tokenId);
      if (!raw) throw new Error(`signal token ${params.tokenId} not found`);
      const current = deserializeSignalToken(raw);
      if (current.completedAt !== null) {
        return { outcome: "already_completed", record: current };
      }
      const updated: SignalTokenRecord = {
        ...current,
        completedAt: params.now,
        completedValue: params.value,
      };
      const written = await this.redis.eval(
        HASH_FIELD_CAS_LUA,
        1,
        hashKey,
        params.tokenId,
        "0",
        raw,
        serializeSignalToken(updated),
        "",
      );
      if (written === 1) return { outcome: "delivered", record: updated };
    }
    throw new Error(`markSignalTokenCompleted: gave up on ${params.tokenId} after contention`);
  }

  async listSignalTokensForWorkflow(workflowId: string): Promise<ReadonlyArray<SignalTokenRecord>> {
    const raw = await this.redis.hgetall(this.signalTokensKey(workflowId));
    if (!raw || Object.keys(raw).length === 0) return [];
    return Object.values(raw)
      .map(deserializeSignalToken)
      .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime());
  }

  // ---------------------------------------------------------------------------
  // Streams — append-only chunks per (workflow, stream) via Redis lists.
  // ---------------------------------------------------------------------------

  private streamKey(workflowId: string, streamId: string): string {
    return `${this.prefix}:${workflowId}:streams:${streamId}`;
  }

  async appendStreamChunk(
    params: {
      workflowId: string;
      streamId: string;
      payload: unknown;
      appendedBy: "workflow" | "external";
    },
    guard?: FenceGuard,
  ): Promise<{ chunkIndex: number }> {
    const key = this.streamKey(params.workflowId, params.streamId);
    const { last } = await this.writeOps({
      workflowId: params.workflowId,
      guard,
      ops: [
        ["SADD", this.streamIdsKey(params.workflowId), params.streamId],
        [
          "RPUSH",
          key,
          JSON.stringify({
            payload: params.payload,
            appendedBy: params.appendedBy,
            appendedAt: this.serializeDate(this.clock.now()),
          }),
        ],
      ],
    });
    // RPUSH returns the new list length; chunkIndex is length - 1.
    return { chunkIndex: Number(last) - 1 };
  }

  async readStreamChunks(params: {
    workflowId: string;
    streamId: string;
    since?: number;
    limit?: number;
  }): Promise<ReadonlyArray<StreamChunk>> {
    const key = this.streamKey(params.workflowId, params.streamId);
    const start = params.since !== undefined ? params.since + 1 : 0;
    const stop = params.limit !== undefined ? start + params.limit - 1 : -1;
    const items = await this.redis.lrange(key, start, stop);
    return items.map((raw: string, i: number) => {
      const parsed = JSON.parse(raw);
      return {
        chunkIndex: start + i,
        payload: parsed.payload,
        appendedBy: parsed.appendedBy as "workflow" | "external",
        appendedAt: new Date(parsed.appendedAt),
      };
    });
  }

  // -- Locking --------------------------------------------------------------

  async tryLock(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ acquired: boolean; token?: string }> {
    // Atomic via Lua: EXISTS → INCR (monotonic fence counter) → HSET lock
    // hash → PEXPIRE. Redis serializes script execution, so no interleave.
    const result = (await this.redis.eval(
      TRY_LOCK_LUA,
      2,
      this.lockKey(workflowId),
      this.fenceCounterKey,
      this.instanceId,
      lockDurationMs.toString(),
    )) as [number, string];
    const [acquired, token] = result;
    if (acquired !== 1) return { acquired: false };
    return { acquired: true, token };
  }

  async tryLockAndLoad(
    workflowId: string,
    lockDurationMs: number,
  ): Promise<{ locked: boolean; token?: string; state: WorkflowState | null }> {
    // Sequenced — a Lua script could do this in one round trip, but
    // `loadWorkflow` reads from several keys (wf hash, steps hash,
    // signals hash, per-run history) that don't fit neatly in a single
    // script without reimplementing the deserialization server-side.
    // The real win — collapsing two HTTP round-trips to one — is
    // already captured at the workflow-remote RPC layer (one POST
    // carries the whole tryLockAndLoad call).
    const { acquired, token } = await this.tryLock(workflowId, lockDurationMs);
    const state = await this.loadWorkflow(workflowId);
    return { locked: acquired, token, state };
  }

  async releaseLock(workflowId: string, guard?: FenceGuard): Promise<void> {
    await this.redis.eval(
      RELEASE_LOCK_LUA,
      1,
      this.lockKey(workflowId),
      this.instanceId,
      guard?.fenceToken ?? "",
    );
  }

  async heartbeat(workflowId: string, lockDurationMs: number, guard?: FenceGuard): Promise<void> {
    const extended = await this.redis.eval(
      HEARTBEAT_LUA,
      1,
      this.lockKey(workflowId),
      this.instanceId,
      lockDurationMs.toString(),
      guard?.fenceToken ?? "",
    );
    // A token holder whose lock is gone (released, or expired through its
    // TTL) or re-taken learns it lost the run. The read is for the error only.
    if (guard?.fenceToken && Number(extended) !== 1) {
      const current = await this.redis.hget(this.lockKey(workflowId), "token");
      throw fenceMismatch({ workflowId, provided: guard.fenceToken, current });
    }
  }

  /**
   * Run a `fencedLua` script for `workflowId`'s lock: the fence check and
   * the script's writes are one atomic script. Without a token in `guard`
   * the script runs unfenced. A rejected fence becomes
   * `FenceTokenMismatchError`.
   */
  private async evalFenced(params: {
    script: string;
    workflowId: string;
    guard?: FenceGuard;
    keys: readonly string[];
    args: readonly string[];
  }): Promise<unknown> {
    const token = params.guard?.fenceToken ?? "";
    try {
      return await this.redis.eval(
        params.script,
        params.keys.length + 1,
        this.lockKey(params.workflowId),
        ...params.keys,
        token,
        ...params.args,
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const at = message.indexOf(FENCE_REJECTED);
      if (at === -1) throw err;
      const current = message.slice(at + FENCE_REJECTED.length).trim();
      throw fenceMismatch({
        workflowId: params.workflowId,
        provided: token,
        current: current === "" ? null : current,
      });
    }
  }

  /** Run `ops` (see `WRITE_OPS_LUA`) as one fenced script. False when an ABSENT op stopped it. */
  private async writeOps(params: {
    workflowId: string;
    guard?: FenceGuard;
    ops: ReadonlyArray<readonly string[]>;
  }): Promise<{ applied: boolean; last: unknown }> {
    const reply = (await this.evalFenced({
      script: FENCED_WRITE_OPS_LUA,
      workflowId: params.workflowId,
      guard: params.guard,
      keys: [],
      args: [JSON.stringify(params.ops)],
    })) as [number, unknown?];
    return { applied: Number(reply[0]) === 1, last: reply[1] };
  }

  /** `STATUS` op of `WRITE_OPS_LUA`. */
  private statusOp(params: {
    workflowId: string;
    to: WorkflowStatus;
    from: readonly WorkflowStatus[] | "*";
    fields: Record<string, string>;
  }): string[] {
    return [
      "STATUS",
      this.wfKey(params.workflowId),
      params.workflowId,
      `${this.prefix}:idx:status:`,
      params.to,
      params.from === "*" ? "*" : params.from.join(","),
      ...Object.entries(params.fields).flat(),
    ];
  }

  // -- Scanner / recovery queries -------------------------------------------
  //
  // Candidates come from the status index sets (ids only), sorted and cut at
  // the keyset cursor client-side; each candidate's workflow hash and
  // current-run steps hash are then read in parallel chunks, stopping as
  // soon as `limit` rows match. Full workflow assembly (tasks, history) is
  // never loaded.

  /**
   * Walk the ids in the given status index sets in ascending order after
   * `afterWorkflowId`, resolving `pick` for each in parallel chunks, and
   * return the first `limit` non-undefined results in id order.
   */
  private async scanStatusIndex<T>(params: {
    statuses: readonly WorkflowStatus[];
    limit: number;
    afterWorkflowId?: string;
    pick: (workflowId: string) => Promise<T | undefined>;
  }): Promise<T[]> {
    const limit = Math.max(0, Math.trunc(params.limit));
    if (limit === 0) return [];
    const sets = await Promise.all(
      params.statuses.map((s) => this.redis.smembers(this.statusIndexKey(s))),
    );
    const after = params.afterWorkflowId;
    const ids = [...new Set(sets.flat())]
      .filter((id) => after === undefined || id > after)
      .sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

    const out: T[] = [];
    const chunkSize = Math.max(limit, 32);
    for (let i = 0; i < ids.length && out.length < limit; i += chunkSize) {
      const picked = await Promise.all(ids.slice(i, i + chunkSize).map((id) => params.pick(id)));
      for (const row of picked) {
        if (row === undefined) continue;
        out.push(row);
        if (out.length >= limit) break;
      }
    }
    return out;
  }

  /** Namespace scoping for the scanners — the same rule `listWorkflows` applies. */
  private inScannerNamespace(raw: Record<string, string>): boolean {
    return !this.namespace || raw.namespace === this.namespace;
  }

  /** Workflow hash + current-run steps of a still-suspended run, sorted by step name. */
  private async loadSuspendedSteps(
    workflowId: string,
  ): Promise<{ raw: Record<string, string>; steps: StepState[] } | undefined> {
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    // The status index can briefly lag the hash; the hash is authoritative.
    if (!raw || !raw.id || raw.status !== "suspended") return undefined;
    if (!this.inScannerNamespace(raw)) return undefined;
    const stepsRaw = await this.redis.hgetall(this.stepsKey(workflowId, Number(raw.run)));
    const steps = Object.values(stepsRaw ?? {})
      .map((json) => this.parseStepState(json))
      .sort((a, b) => (a.stepName < b.stepName ? -1 : a.stepName > b.stepName ? 1 : 0));
    return { raw, steps };
  }

  private toWakeup(params: {
    raw: Record<string, string>;
    stepName: string;
    reason: WorkflowWakeup["reason"];
    signalName?: string;
    signalPayload?: unknown;
  }): WorkflowWakeup {
    const { raw } = params;
    return {
      workflowId: raw.id,
      workflowName: raw.workflowName,
      ...(raw.version ? { version: raw.version } : {}),
      input: JSON.parse(raw.input),
      stepName: params.stepName,
      reason: params.reason,
      ...(params.signalName !== undefined ? { signalName: params.signalName } : {}),
      ...(params.reason === "signal" ? { signalPayload: params.signalPayload } : {}),
    };
  }

  async listDueTimers(params: {
    now: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]> {
    const nowMs = params.now.getTime();
    const due = (at: Date | undefined): boolean => at !== undefined && at.getTime() <= nowMs;
    return this.scanStatusIndex<WorkflowWakeup>({
      statuses: ["suspended"],
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: async (workflowId) => {
        const loaded = await this.loadSuspendedSteps(workflowId);
        if (!loaded) return undefined;
        for (const step of loaded.steps) {
          if (step.status === "sleeping" && due(step.wakeAt)) {
            return this.toWakeup({ raw: loaded.raw, stepName: step.stepName, reason: "sleep" });
          }
          if (step.status === "waiting_for_signal" && due(step.signalTimeoutAt)) {
            return this.toWakeup({
              raw: loaded.raw,
              stepName: step.stepName,
              reason: "signal-timeout",
              signalName: step.signalName,
            });
          }
        }
        return undefined;
      },
    });
  }

  async listSignalWakeups(params: {
    limit: number;
    afterWorkflowId?: string;
  }): Promise<WorkflowWakeup[]> {
    return this.scanStatusIndex<WorkflowWakeup>({
      statuses: ["suspended"],
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: async (workflowId) => {
        // Signals first: most suspended runs have none delivered, and that
        // check is one HGETALL on a usually-missing key.
        const signals = await this.redis.hgetall(this.signalsKey(workflowId));
        if (!signals || Object.keys(signals).length === 0) return undefined;
        const loaded = await this.loadSuspendedSteps(workflowId);
        if (!loaded) return undefined;
        for (const step of loaded.steps) {
          if (step.status !== "waiting_for_signal" || step.signalName === undefined) continue;
          const json = signals[step.signalName];
          if (json === undefined) continue;
          return this.toWakeup({
            raw: loaded.raw,
            stepName: step.stepName,
            reason: "signal",
            signalName: step.signalName,
            signalPayload: JSON.parse(json).payload,
          });
        }
        return undefined;
      },
    });
  }

  /**
   * Locks are keys with a server-side TTL, so a lock that expired is
   * already gone; a live lock counts as expired at `now` when its
   * remaining TTL, measured from this client's clock, ends by then.
   */
  async listOrphanedRuns(params: {
    now: Date;
    updatedBefore: Date;
    limit: number;
    afterWorkflowId?: string;
  }): Promise<OrphanedRun[]> {
    const nowMs = params.now.getTime();
    const beforeMs = params.updatedBefore.getTime();
    return this.scanStatusIndex<OrphanedRun>({
      statuses: ["pending", "running", "compensating"],
      limit: params.limit,
      afterWorkflowId: params.afterWorkflowId,
      pick: async (workflowId) => {
        const [raw, pttl] = await Promise.all([
          this.redis.hgetall(this.wfKey(workflowId)),
          this.redis.eval(PTTL_LUA, 1, this.lockKey(workflowId)) as Promise<number>,
        ]);
        if (!raw || !raw.id) return undefined;
        if (raw.status !== "pending" && raw.status !== "running" && raw.status !== "compensating") {
          return undefined;
        }
        if (!this.inScannerNamespace(raw)) return undefined;
        if (this.parseDate(raw.updatedAt).getTime() >= beforeMs) return undefined;
        // PTTL: -2 = no key, -1 = no expiry (treat as held).
        const ttl = Number(pttl);
        if (ttl === -1) return undefined;
        if (ttl >= 0 && this.clock.currentTimeMs() + ttl > nowMs) return undefined;
        return {
          workflowId: raw.id,
          workflowName: raw.workflowName,
          ...(raw.version ? { version: raw.version } : {}),
          status: raw.status,
          input: JSON.parse(raw.input),
          ...(raw.metadata ? { metadata: JSON.parse(raw.metadata) } : {}),
        };
      },
    });
  }

  // -- Run history ----------------------------------------------------------

  async startFreshRun(workflowId: string, guard?: FenceGuard): Promise<number> {
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) throw new Error(`Workflow ${workflowId} not found`);

    const currentRun = Number(raw.run);

    // Archive current run — load steps for current run
    const stepsRaw = await this.redis.hgetall(this.stepsKey(workflowId, currentRun));
    const steps: Record<string, StepState> = {};
    for (const [stepName, json] of Object.entries(stepsRaw)) {
      steps[stepName] = this.parseStepState(json);
    }

    const summary: WorkflowRunSummary = {
      run: currentRun,
      version: raw.version || undefined,
      status: raw.status as WorkflowStatus,
      result: raw.result ? JSON.parse(raw.result) : undefined,
      error: raw.error || undefined,
      tripwire: raw.tripwire ? JSON.parse(raw.tripwire) : undefined,
      steps,
      createdAt: this.parseDate(raw.createdAt),
      startedAt: raw.startedAt ? this.parseDate(raw.startedAt) : undefined,
      completedAt: raw.completedAt ? this.parseDate(raw.completedAt) : undefined,
    };

    const summaryJson = JSON.stringify(summary, (_, v) => {
      if (v instanceof Date) return v.toISOString();
      return v;
    });

    // Archive + run bump + journal and signal cleanup in one script, so the
    // new run can never observe the previous run's journal or signals.
    const newRun = (await this.evalFenced({
      script: FENCED_START_FRESH_RUN_LUA,
      workflowId,
      guard,
      keys: [
        this.wfKey(workflowId),
        this.runsKey(workflowId),
        this.signalsKey(workflowId),
        this.journalStepsKey(workflowId),
        this.sleepsKey,
        this.statusIndexKey("pending"),
        this.completedIndexKey,
      ],
      args: [
        workflowId,
        String(currentRun),
        summaryJson,
        String(this.maxRunsPerWorkflow),
        this.serializeDate(this.clock.now()),
        `${this.prefix}:${workflowId}:journal:`,
        `${this.prefix}:idx:status:`,
      ],
    })) as number;
    if (newRun === -1) {
      // A concurrent fresh run moved the counter between our read and the
      // script — start over against the new run.
      return this.startFreshRun(workflowId, guard);
    }
    return newRun;
  }

  async loadRunHistory(
    workflowId: string,
    params?: { limit?: number; offset?: number },
  ): Promise<WorkflowRunSummary[]> {
    const raw = await this.redis.hgetall(this.wfKey(workflowId));
    if (!raw || !raw.id) return [];

    // Load current run as a summary
    const currentRun = Number(raw.run);
    const stepsRaw = await this.redis.hgetall(this.stepsKey(workflowId, currentRun));
    const currentSteps: Record<string, StepState> = {};
    for (const [stepName, json] of Object.entries(stepsRaw)) {
      currentSteps[stepName] = this.parseStepState(json);
    }

    const currentSummary: WorkflowRunSummary = {
      run: currentRun,
      version: raw.version || undefined,
      status: raw.status as WorkflowStatus,
      result: raw.result ? JSON.parse(raw.result) : undefined,
      error: raw.error || undefined,
      tripwire: raw.tripwire ? JSON.parse(raw.tripwire) : undefined,
      steps: currentSteps,
      createdAt: this.parseDate(raw.createdAt),
      startedAt: raw.startedAt ? this.parseDate(raw.startedAt) : undefined,
      completedAt: raw.completedAt ? this.parseDate(raw.completedAt) : undefined,
    };

    // Load archived runs via eval+LRANGE
    const archivedRaw = (await this.redis.eval(
      `return redis.call('LRANGE', KEYS[1], 0, -1)`,
      1,
      this.runsKey(workflowId),
    )) as string[] | null;

    const archived: WorkflowRunSummary[] = (archivedRaw ?? []).map((json) => {
      const r = JSON.parse(json);
      // Parse dates in steps
      const steps: Record<string, StepState> = {};
      if (r.steps) {
        for (const [name, step] of Object.entries(r.steps)) {
          const s = step as Record<string, unknown>;
          steps[name] = {
            ...s,
            startedAt: s.startedAt ? new Date(s.startedAt as string) : undefined,
            completedAt: s.completedAt ? new Date(s.completedAt as string) : undefined,
            wakeAt: s.wakeAt ? new Date(s.wakeAt as string) : undefined,
            signalTimeoutAt: s.signalTimeoutAt ? new Date(s.signalTimeoutAt as string) : undefined,
            compensatedAt: s.compensatedAt ? new Date(s.compensatedAt as string) : undefined,
          } as StepState;
        }
      }
      return {
        run: r.run,
        version: r.version || undefined,
        status: r.status,
        result: r.result,
        error: r.error || undefined,
        steps,
        createdAt: new Date(r.createdAt),
        startedAt: r.startedAt ? new Date(r.startedAt) : undefined,
        completedAt: r.completedAt ? new Date(r.completedAt) : undefined,
      };
    });

    const runs = [currentSummary, ...archived];
    runs.sort((a, b) => b.run - a.run);

    const offset = params?.offset ?? 0;
    const limit = params?.limit ?? runs.length;
    return runs.slice(offset, offset + limit);
  }

  // -- Purge ----------------------------------------------------------------

  async purgeCompleted(
    params: { olderThanMs: number; limit: number } | { from: Date; to: Date; limit: number },
  ): Promise<number> {
    let minScore: number;
    let maxScore: number;

    if ("olderThanMs" in params) {
      minScore = 0;
      maxScore = this.clock.currentTimeMs() - params.olderThanMs;
    } else {
      minScore = params.from.getTime();
      maxScore = params.to.getTime();
    }

    // Find candidate workflow IDs from the completed sorted set
    const ids = await this.redis.zrangebyscore(
      this.completedIndexKey,
      minScore,
      maxScore,
      "LIMIT",
      0,
      params.limit,
    );

    let deleted = 0;

    for (const id of ids) {
      // Load workflow to get run count and status for cleanup
      const raw = await this.redis.hgetall(this.wfKey(id));
      if (!raw || !raw.id) {
        // Already gone — just clean up index
        await this.redis.zrem(this.completedIndexKey, id);
        continue;
      }

      const status = raw.status as WorkflowStatus;
      if (!isTerminalWorkflowStatus(status)) continue;

      const run = Number(raw.run);

      // Collect all keys to delete
      const keysToDelete = [
        this.wfKey(id),
        this.stepsKey(id, run),
        this.signalsKey(id),
        this.runsKey(id),
        this.attemptsKey(id),
        this.signalTokensKey(id),
        this.signalTokenIdempotencyKey(id),
        this.streamIdsKey(id),
        this.childrenIndexKey(id),
      ];

      // Signal tokens: drop the tokenId → workflowId reverse lookups too.
      const tokenIds = await this.redis.hkeys(this.signalTokensKey(id));
      for (const tokenId of tokenIds) keysToDelete.push(this.signalTokenLookupKey(tokenId));

      // Streams appended through this storage are tracked per workflow.
      const streamIds = await this.redis.smembers(this.streamIdsKey(id));
      for (const streamId of streamIds) keysToDelete.push(this.streamKey(id, streamId));

      // Delete task hashes for current run
      const stepNames = await this.redis.hkeys(this.stepsKey(id, run));
      for (const stepName of stepNames) {
        keysToDelete.push(this.tasksKey(id, run, stepName));
      }

      // Delete archived run step/task keys
      const archivedRaw = (await this.redis.eval(
        `return redis.call('LRANGE', KEYS[1], 0, -1)`,
        1,
        this.runsKey(id),
      )) as string[] | null;

      if (archivedRaw) {
        for (const json of archivedRaw) {
          const r = JSON.parse(json);
          const archivedRun = r.run as number;
          keysToDelete.push(this.stepsKey(id, archivedRun));
          if (r.steps) {
            for (const stepName of Object.keys(r.steps)) {
              keysToDelete.push(this.tasksKey(id, archivedRun, stepName));
            }
          }
        }
      }

      // Cascade journal: step list, per-step idx zset + signal-idx hash +
      // entry hashes, plus pending members left in the global sleeps zset.
      const journalStepNames = await this.redis.smembers(this.journalStepsKey(id));
      for (const stepName of journalStepNames) {
        const members = await this.redis.zrangebyscore(
          this.journalIdxKey(id, stepName),
          "-inf",
          "+inf",
        );
        for (const member of members) {
          const { activityIndex, branchPath } = parseJournalMember(member);
          keysToDelete.push(this.journalEntryKey(id, stepName, activityIndex, branchPath));
          // Defensive: ZREM is a no-op if not present.
          await this.redis.zrem(
            this.sleepsKey,
            this.sleepsMember(id, stepName, activityIndex, branchPath),
          );
        }
        keysToDelete.push(this.journalIdxKey(id, stepName));
        keysToDelete.push(this.journalSignalIdxKey(id, stepName));
      }
      if (journalStepNames.length > 0) {
        keysToDelete.push(this.journalStepsKey(id));
      }

      // Delete all keys
      if (keysToDelete.length > 0) {
        await this.redis.del(...keysToDelete);
      }

      // Remove from indexes
      await this.redis.srem(this.statusIndexKey(status), id);
      if (raw.workflowName) {
        await this.redis.srem(this.nameIndexKey(raw.workflowName), id);
      }
      if (raw.parentWorkflowId) {
        await this.redis.srem(this.childrenIndexKey(raw.parentWorkflowId), id);
      }
      await this.redis.zrem(this.completedIndexKey, id);

      deleted++;
    }

    return deleted;
  }

  // -- StepAttemptStorage ---------------------------------------------------

  async saveStepAttempt(record: StepAttemptRecord, guard?: FenceGuard): Promise<void> {
    await this.writeOps({
      workflowId: record.workflowId,
      guard,
      ops: [
        [
          "RPUSH",
          this.attemptsKey(record.workflowId),
          JSON.stringify({
            ...record,
            startedAt: this.serializeDate(record.startedAt),
            completedAt: this.serializeDate(record.completedAt),
          }),
        ],
      ],
    });
  }

  async loadStepAttempts(workflowId: string, stepName?: string): Promise<StepAttemptRecord[]> {
    const raw = (await this.redis.eval(
      `return redis.call('LRANGE', KEYS[1], 0, -1)`,
      1,
      this.attemptsKey(workflowId),
    )) as string[] | null;

    const items = (raw ?? []).map((json) => {
      const r = JSON.parse(json);
      return {
        ...r,
        startedAt: new Date(r.startedAt),
        completedAt: new Date(r.completedAt),
      } as StepAttemptRecord;
    });

    return stepName ? items.filter((a) => a.stepName === stepName) : items;
  }

  // -- CompensationLedgerStorage --------------------------------------------

  async beginCompensation(
    params: { readonly workflowId: string; readonly error: string; readonly errorTag?: string },
    guard?: FenceGuard,
  ): Promise<boolean> {
    const { workflowId } = params;
    const nowIso = this.serializeDate(this.clock.now());
    // The move and the read-back run in one script: the reply is the status
    // the run has after it.
    const { last } = await this.writeOps({
      workflowId,
      guard,
      ops: [
        this.statusOp({
          workflowId,
          to: "compensating",
          from: CANCELLABLE_STATUSES,
          fields: { error: params.error, errorTag: params.errorTag ?? "", updatedAt: nowIso },
        }),
        ["HGET", this.wfKey(workflowId), "status"],
      ],
    });
    return last === "compensating";
  }

  async saveStepCompensation(
    params: {
      readonly workflowId: string;
      readonly stepName: string;
      readonly status: StepCompensationOutcome;
      readonly error?: string;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    const { workflowId } = params;
    const run = await this.redis.hget(this.wfKey(workflowId), "run");
    if (!run) return;
    const stepsHashKey = this.stepsKey(workflowId, Number(run));
    const existingJson = await this.redis.hget(stepsHashKey, params.stepName);
    if (!existingJson) return;
    const now = this.clock.now();
    const step: StepState = {
      ...withoutCompensationLedger(this.parseStepState(existingJson)),
      compensationStatus: params.status,
      ...(params.error !== undefined && { compensationError: params.error }),
      compensatedAt: now,
    };
    await this.writeOps({
      workflowId,
      guard,
      ops: [
        ["HSET", stepsHashKey, params.stepName, this.serializeStepState(step)],
        ["HSET", this.wfKey(workflowId), "updatedAt", this.serializeDate(now)],
      ],
    });
  }

  // -- ActivityJournalStorage -----------------------------------------------

  async loadJournal(workflowId: string, stepName: string): Promise<JournalEntry[]> {
    const members = await this.redis.zrangebyscore(
      this.journalIdxKey(workflowId, stepName),
      "-inf",
      "+inf",
    );
    if (members.length === 0) return [];

    const entries = await Promise.all(
      members.map(async (member) => {
        const { activityIndex, branchPath } = parseJournalMember(member);
        const hash = await this.redis.hgetall(
          this.journalEntryKey(workflowId, stepName, activityIndex, branchPath),
        );
        if (!hash || Object.keys(hash).length === 0) return null;
        return this.parseJournalEntry(activityIndex, branchPath, hash);
      }),
    );
    return entries.filter((e): e is JournalEntry => e !== null);
  }

  async appendEntry(
    params: {
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath?: string;
      activityName: string;
      payloadHash?: string;
      exit: NonNullable<JournalEntry["exit"]>;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    const branchPath = params.branchPath ?? "";
    const entryKey = this.journalEntryKey(
      params.workflowId,
      params.stepName,
      params.activityIndex,
      branchPath,
    );
    const idxKey = this.journalIdxKey(params.workflowId, params.stepName);
    const stepsKey = this.journalStepsKey(params.workflowId);
    const createdAt = this.serializeDate(this.clock.now());
    await this.evalFenced({
      script: FENCED_APPEND_ENTRY_LUA,
      workflowId: params.workflowId,
      guard,
      keys: [entryKey, idxKey, stepsKey],
      args: [
        String(params.activityIndex),
        params.activityName,
        JSON.stringify(params.exit),
        createdAt,
        params.stepName,
        branchPath,
        params.payloadHash ?? "",
      ],
    });
  }

  // -- ActivityJournalStorage: pending entries -------------------------------

  async appendPendingEntry(
    params: {
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath?: string;
      activityName: string;
      payloadHash?: string;
      stepType: "sleep" | "signal" | "activity" | "compensation" | "child";
      wakeAt?: Date;
    },
    guard?: FenceGuard,
  ): Promise<void> {
    const branchPath = params.branchPath ?? "";
    const entryKey = this.journalEntryKey(
      params.workflowId,
      params.stepName,
      params.activityIndex,
      branchPath,
    );
    const idxKey = this.journalIdxKey(params.workflowId, params.stepName);
    const stepsKey = this.journalStepsKey(params.workflowId);
    const signalIdxKey = this.journalSignalIdxKey(params.workflowId, params.stepName);
    // Persist wakeAt for signals too: it is the signal's timeout deadline,
    // and replay must read the recorded one rather than recompute it.
    const wakeAtMs = params.wakeAt ? String(params.wakeAt.getTime()) : "";
    // Only register in the global sleeps zset when we have a wakeAt — a sleep
    // entry without one can't be scanned anyway.
    const sleepsMember =
      params.stepType === "sleep" && wakeAtMs
        ? this.sleepsMember(params.workflowId, params.stepName, params.activityIndex, branchPath)
        : "";
    const signalName = params.stepType === "signal" ? params.activityName : "";

    await this.evalFenced({
      script: FENCED_APPEND_PENDING_LUA,
      workflowId: params.workflowId,
      guard,
      keys: [entryKey, idxKey, stepsKey, this.sleepsKey, signalIdxKey],
      args: [
        String(params.activityIndex),
        params.activityName,
        params.stepType,
        wakeAtMs,
        this.serializeDate(this.clock.now()),
        params.stepName,
        sleepsMember,
        signalName,
        branchPath,
        params.payloadHash ?? "",
      ],
    });
  }

  async completePendingEntry(
    params: {
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath?: string;
      exit: JournalExit;
    },
    guard?: FenceGuard,
  ): Promise<CompletePendingResult> {
    const branchPath = params.branchPath ?? "";
    const entryKey = this.journalEntryKey(
      params.workflowId,
      params.stepName,
      params.activityIndex,
      branchPath,
    );
    const signalIdxKey = this.journalSignalIdxKey(params.workflowId, params.stepName);
    // Load current entry to learn the signal name (if any) so the Lua script
    // can remove it from the signal-idx hash. stepType and activityName never
    // change after the pending write; the Lua phase check makes the
    // transition itself atomic, so concurrent completers (a signal delivery
    // racing the body's timeout write) get exactly one winner.
    const current = await this.redis.hgetall(entryKey);
    const stepType = current?.stepType;
    const sleepsMember =
      stepType === "sleep"
        ? this.sleepsMember(params.workflowId, params.stepName, params.activityIndex, branchPath)
        : "";
    const signalName = stepType === "signal" ? (current?.activityName ?? "") : "";

    const [won, storedExit] = (await this.evalFenced({
      script: FENCED_COMPLETE_PENDING_LUA,
      workflowId: params.workflowId,
      guard,
      keys: [entryKey, this.sleepsKey, signalIdxKey],
      args: [JSON.stringify(params.exit), sleepsMember, signalName],
    })) as [number, string];
    if (Number(won) === 1) return { completed: true, exit: params.exit };
    return {
      completed: false,
      exit: storedExit ? (JSON.parse(storedExit) as JournalExit) : undefined,
    };
  }

  async discardJournalEntries(
    params: {
      workflowId: string;
      stepName: string;
      slots: readonly JournalSlot[];
    },
    guard?: FenceGuard,
  ): Promise<void> {
    const idxKey = this.journalIdxKey(params.workflowId, params.stepName);
    const signalIdxKey = this.journalSignalIdxKey(params.workflowId, params.stepName);
    // Every slot goes in one fenced script.
    await this.evalFenced({
      script: FENCED_DISCARD_ENTRIES_LUA,
      workflowId: params.workflowId,
      guard,
      keys: [
        idxKey,
        this.sleepsKey,
        signalIdxKey,
        ...params.slots.map((slot) =>
          this.journalEntryKey(
            params.workflowId,
            params.stepName,
            slot.activityIndex,
            slot.branchPath,
          ),
        ),
      ],
      args: params.slots.flatMap((slot) => [
        `${slot.activityIndex}|${slot.branchPath}`,
        this.sleepsMember(params.workflowId, params.stepName, slot.activityIndex, slot.branchPath),
        // Index members written before branch paths existed are the bare index.
        slot.branchPath === "" ? String(slot.activityIndex) : "",
      ]),
    });
  }

  async findDueSleeps(params: { now: Date; limit: number }): Promise<
    Array<{
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath: string;
      wakeAt: Date;
    }>
  > {
    const raw = await this.redis.zrangebyscore(
      this.sleepsKey,
      "-inf",
      params.now.getTime(),
      "WITHSCORES",
      "LIMIT",
      0,
      params.limit,
    );
    const due: Array<{
      workflowId: string;
      stepName: string;
      activityIndex: number;
      branchPath: string;
      wakeAt: Date;
    }> = [];
    for (let i = 0; i < raw.length; i += 2) {
      const member = raw[i]!;
      const score = Number(raw[i + 1]);
      const parsed = this.parseSleepsMember(member);
      if (!parsed) continue;
      due.push({ ...parsed, wakeAt: new Date(score) });
    }
    return due;
  }

  async findPendingSignal(params: {
    workflowId: string;
    stepName: string;
    signalName: string;
  }): Promise<JournalEntry | null> {
    const composite = await this.redis.hget(
      this.journalSignalIdxKey(params.workflowId, params.stepName),
      params.signalName,
    );
    if (composite == null) return null;
    const { activityIndex, branchPath } = parseJournalMember(composite);
    const hash = await this.redis.hgetall(
      this.journalEntryKey(params.workflowId, params.stepName, activityIndex, branchPath),
    );
    if (!hash || Object.keys(hash).length === 0) return null;
    const entry = this.parseJournalEntry(activityIndex, branchPath, hash);
    if (entry.phase !== "pending") return null;
    return entry;
  }

  // -- Journal helpers ------------------------------------------------------

  private parseJournalEntry(
    activityIndex: number,
    branchPath: string,
    hash: Record<string, string>,
  ): JournalEntry {
    const stepType = hash.stepType as JournalEntry["stepType"];
    const phase = hash.phase as JournalEntry["phase"];
    const exit = hash.exit ? (JSON.parse(hash.exit) as JournalEntry["exit"]) : undefined;
    const wakeAt = hash.wakeAt ? new Date(Number(hash.wakeAt)) : undefined;
    return {
      activityIndex,
      branchPath: hash.branchPath ?? branchPath,
      activityName: hash.activityName!,
      stepType,
      phase,
      payloadHash: hash.payloadHash,
      exit,
      wakeAt,
      createdAt: this.parseDate(hash.createdAt!),
    };
  }
}

/**
 * Comparator factory for sortable `listWorkflows` columns. NULL values
 * always sort last so still-running rows (no `startedAt` / `completedAt` /
 * `duration`) don't push real data off the first page in either direction.
 */
function makeWorkflowStateComparator(
  orderBy: WorkflowOrderBy,
  dir: "asc" | "desc",
): (a: WorkflowState, b: WorkflowState) => number {
  const sign = dir === "asc" ? 1 : -1;
  return (a, b) => {
    const av = workflowStateSortKey(a, orderBy);
    const bv = workflowStateSortKey(b, orderBy);
    if (av === undefined && bv === undefined) return 0;
    if (av === undefined) return 1;
    if (bv === undefined) return -1;
    if (av < bv) return -1 * sign;
    if (av > bv) return 1 * sign;
    return 0;
  };
}

function workflowStateSortKey(
  wf: WorkflowState,
  orderBy: WorkflowOrderBy,
): number | string | undefined {
  switch (orderBy) {
    case "createdAt":
      return wf.createdAt.getTime();
    case "startedAt":
      return wf.startedAt?.getTime();
    case "completedAt":
      return wf.completedAt?.getTime();
    case "duration":
      return wf.completedAt ? wf.completedAt.getTime() - wf.createdAt.getTime() : undefined;
    case "status":
      return wf.status;
    case "name":
      return wf.workflowName;
  }
}

/**
 * Parse a composite journal member `${idx}|${branchPath}`. Pre-plif rows
 * have just `${idx}` with no pipe — we tolerate that for backward compat
 * so old workflows keep loading cleanly.
 */
function parseJournalMember(member: string): { activityIndex: number; branchPath: string } {
  const pipe = member.indexOf("|");
  if (pipe === -1) return { activityIndex: Number(member), branchPath: "" };
  return {
    activityIndex: Number(member.slice(0, pipe)),
    branchPath: member.slice(pipe + 1),
  };
}

function serializeSignalToken(t: SignalTokenRecord): string {
  return JSON.stringify({
    tokenId: t.tokenId,
    workflowId: t.workflowId,
    signalName: t.signalName,
    bearer: t.bearer,
    tags: t.tags,
    idempotencyKey: t.idempotencyKey,
    expiresAt: t.expiresAt.toISOString(),
    completedAt: t.completedAt ? t.completedAt.toISOString() : null,
    completedValue: t.completedValue,
    createdAt: t.createdAt.toISOString(),
  });
}

function deserializeSignalToken(raw: string): SignalTokenRecord {
  const parsed = JSON.parse(raw);
  return {
    tokenId: parsed.tokenId,
    workflowId: parsed.workflowId,
    signalName: parsed.signalName,
    bearer: parsed.bearer,
    tags: parsed.tags ?? [],
    idempotencyKey: parsed.idempotencyKey ?? null,
    expiresAt: new Date(parsed.expiresAt),
    completedAt: parsed.completedAt ? new Date(parsed.completedAt) : null,
    completedValue: parsed.completedValue,
    createdAt: new Date(parsed.createdAt),
  };
}
