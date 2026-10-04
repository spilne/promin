// ---------------------------------------------------------------------------
// Lua scripts of RedisWorkflowStorage
// ---------------------------------------------------------------------------
//
// Each script touches one hash slot (see `redis-workflow-keys.ts`): either
// one workflow's `{wf:<id>}` keys or the shared `{idx}` index keys, never
// both. Keys a script reaches are passed in KEYS; the few scripts that walk
// a workflow's variable key set (steps of a run, journal entries) build those
// names inside Lua from a base passed in ARGV that carries the workflow's
// hash tag, so the derived keys sit in the same slot as KEYS.
// ---------------------------------------------------------------------------

/**
 * Fields of the workflow hash the cross-workflow index is built from, in
 * the order the `snapshot` Lua helper returns them. `iv` is the index
 * version: bumped by every write that changes one of the other fields.
 */
export const INDEX_FIELDS = [
  "id",
  "workflowName",
  "workflowType",
  "namespace",
  "version",
  "parentWorkflowId",
  "runSource",
  "runSourceId",
  "status",
  "createdAt",
  "startedAt",
  "completedAt",
  "iv",
] as const;

const SNAPSHOT_FN = `
local function snapshot(k)
  return redis.call('HMGET', k, ${INDEX_FIELDS.map((f) => `'${f}'`).join(", ")})
end
`;

// A journal index member is `${idx}|${branchPath}`, the suffix of the
// entry's key after `:entry:`.
// Load a workflow hash, its current run's step rows and the task rows of
// every step that has them. Returns {} for a missing workflow.
const LOAD_RUN_FN = `
local function loadRun(wfKey, base)
  local wf = redis.call('HGETALL', wfKey)
  if #wf == 0 then return {} end
  local run = redis.call('HGET', wfKey, 'run')
  local steps = redis.call('HGETALL', base .. ':steps:' .. run)
  local out = {wf, steps}
  for i = 1, #steps, 2 do
    local tasks = redis.call('HGETALL', base .. ':tasks:' .. run .. ':' .. steps[i])
    if #tasks > 0 then
      out[#out + 1] = steps[i]
      out[#out + 1] = tasks
    end
  end
  return out
end
`;

// Lock is a hash { lockedBy, token } with a PEXPIRE TTL. Tokens come from a
// per-workflow counter, seeded from the server clock in microseconds the
// first time (and again after a purge dropped it), so a token is never
// reused for a workflow id — not even by a workflow re-created under the
// same id. INCR's reply is read back with GET: a Lua number would print
// large values in exponent form.
const TRY_LOCK_FN = `
local function tryLock(lockKey, fenceKey, instanceId, ms)
  if redis.call('EXISTS', lockKey) == 1 then return {0, ''} end
  if redis.call('EXISTS', fenceKey) == 0 then
    local t = redis.call('TIME')
    redis.call('SET', fenceKey, t[1] .. string.format('%06d', tonumber(t[2])))
  end
  redis.call('INCR', fenceKey)
  local token = redis.call('GET', fenceKey)
  redis.call('HSET', lockKey, 'lockedBy', instanceId, 'token', token)
  redis.call('PEXPIRE', lockKey, ms)
  return {1, token}
end
`;

// Put a retention TTL on a finished run's keys.
const EXPIRE_RUN_FN = `
local function expireRun(wfKey, base, run, ttl)
  redis.call('PEXPIRE', wfKey, ttl)
  local stepsKey = base .. ':steps:' .. run
  for _, name in ipairs(redis.call('HKEYS', stepsKey)) do
    redis.call('PEXPIRE', base .. ':tasks:' .. run .. ':' .. name, ttl)
  end
  redis.call('PEXPIRE', stepsKey, ttl)
  redis.call('PEXPIRE', base .. ':signals', ttl)
  redis.call('PEXPIRE', base .. ':runs', ttl)
  redis.call('PEXPIRE', base .. ':attempts', ttl)
  redis.call('PEXPIRE', base .. ':child-intents', ttl)
  local journalSteps = base .. ':journal:steps'
  for _, step in ipairs(redis.call('SMEMBERS', journalSteps)) do
    local jb = base .. ':journal:' .. step
    for _, m in ipairs(redis.call('ZRANGE', jb .. ':idx', 0, -1)) do
      redis.call('PEXPIRE', jb .. ':entry:' .. m, ttl)
    end
    redis.call('PEXPIRE', jb .. ':idx', ttl)
    redis.call('PEXPIRE', jb .. ':signal-idx', ttl)
  end
  redis.call('PEXPIRE', journalSteps, ttl)
end
`;

// KEYS: [lockKey, fenceKey]
// ARGV: [instanceId, lockDurationMs]
// Returns: [acquired (0/1), token ('' on miss)]
export const TRY_LOCK_LUA = `
${TRY_LOCK_FN}
return tryLock(KEYS[1], KEYS[2], ARGV[1], ARGV[2])
`;

// Lock and load in one script: the state is the one the lock was taken on.
// KEYS: [lockKey, fenceKey, wfKey]
// ARGV: [instanceId, lockDurationMs, wfKey (base)]
// Returns: [acquired, token, loadRun reply]
export const TRY_LOCK_AND_LOAD_LUA = `
${TRY_LOCK_FN}
${LOAD_RUN_FN}
local lock = tryLock(KEYS[1], KEYS[2], ARGV[1], ARGV[2])
return {lock[1], lock[2], loadRun(KEYS[3], ARGV[3])}
`;

// KEYS: [wfKey]   ARGV: [wfKey (base)]
export const LOAD_RUN_LUA = `
${LOAD_RUN_FN}
return loadRun(KEYS[1], ARGV[1])
`;

// PTTL of a lock key (the client surface has no PTTL command).
// KEYS: [lockKey]
export const PTTL_LUA = `return redis.call('PTTL', KEYS[1])`;

// HMGET (the client surface has no HMGET command).
// KEYS: [hash]   ARGV: fields
export const HMGET_LUA = `return redis.call('HMGET', KEYS[1], unpack(ARGV))`;

// LRANGE 0 -1 of one list.
// KEYS: [list]
export const LRANGE_ALL_LUA = `return redis.call('LRANGE', KEYS[1], 0, -1)`;

// RELEASE_LOCK: honor the fence token when provided, else fall back to
// the instanceId check.
// KEYS: [lockKey]
// ARGV: [instanceId, fenceToken|'']
export const RELEASE_LOCK_LUA = `
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
export const HEARTBEAT_LUA = `
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

// The reads a read-modify-write of step or task rows starts from.
// KEYS: [wfKey]
// ARGV: [wfKey (base), taskStep|'', taskField|'', stepName_1 .. stepName_n]
// Returns {} for a missing workflow, else
// {run, taskJson|false, stepJson_1|false .. stepJson_n|false}.
export const READ_FOR_WRITE_LUA = `
local run = redis.call('HGET', KEYS[1], 'run')
if not run then return {} end
local out = {run, false}
if ARGV[2] ~= '' then
  out[2] = redis.call('HGET', ARGV[1] .. ':tasks:' .. run .. ':' .. ARGV[2], ARGV[3])
end
for i = 4, #ARGV do
  out[#out + 1] = redis.call('HGET', ARGV[1] .. ':steps:' .. run, ARGV[i])
end
return out
`;

// Journal: append a COMPLETED activity entry. Idempotent on (wid, step, idx).
// KEYS: [entryHash, idxZset, stepsSet]
// ARGV: [idx, activityName, exitJson, createdAt, stepName, branchPath, payloadHash|'']
export const APPEND_ENTRY_LUA = `
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

// Journal: append a PENDING entry (sleep or signal). Idempotent on (wid,
// step, idx, branch). The sleep schedule is a cross-workflow key, so the
// caller adds the sleep to it after this script, from the reply.
// KEYS: [entryHash, idxZset, stepsSet, signalIdxHash]
// ARGV: [idx, activityName, stepType, wakeAtMs|'', createdAt, stepName, signalName|'', branchPath, payloadHash|'']
// Returns {inserted (0/1), phase, wakeAtMs|''} of the stored entry.
export const APPEND_PENDING_LUA = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0, redis.call('HGET', KEYS[1], 'phase') or '', redis.call('HGET', KEYS[1], 'wakeAt') or ''}
end
redis.call('HSET', KEYS[1],
  'activityName', ARGV[2],
  'stepType', ARGV[3],
  'phase', 'pending',
  'wakeAt', ARGV[4],
  'branchPath', ARGV[8],
  'createdAt', ARGV[5])
if ARGV[9] ~= '' then
  redis.call('HSET', KEYS[1], 'payloadHash', ARGV[9])
end
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[1] .. '|' .. ARGV[8])
redis.call('SADD', KEYS[3], ARGV[6])
if ARGV[3] == 'signal' and ARGV[7] ~= '' then
  redis.call('HSET', KEYS[4], ARGV[7], ARGV[1] .. '|' .. ARGV[8])
end
return {1, 'pending', ARGV[4]}
`;

// Journal: transition pending -> completed atomically. First writer wins: a
// call on an already-completed (or missing) entry changes nothing.
// KEYS: [entryHash, signalIdxHash]
// ARGV: [exitJson]
// Returns {1, '', stepType} when this call completed the entry, else
// {0, storedExitJson|'', stepType|''}.
export const COMPLETE_PENDING_LUA = `
local phase = redis.call('HGET', KEYS[1], 'phase')
local stepType = redis.call('HGET', KEYS[1], 'stepType') or ''
if phase ~= 'pending' then
  return {0, redis.call('HGET', KEYS[1], 'exit') or '', stepType}
end
redis.call('HSET', KEYS[1], 'phase', 'completed', 'exit', ARGV[1])
if stepType == 'signal' then
  local name = redis.call('HGET', KEYS[1], 'activityName')
  if name then redis.call('HDEL', KEYS[2], name) end
end
return {1, '', stepType}
`;

// Journal: delete entries of one step and their index memberships.
// KEYS: [idxZset, signalIdxHash, entryHash_1 .. entryHash_n]
// ARGV: per entry: idxMember
export const DISCARD_ENTRIES_LUA = `
for s = 1, #KEYS - 2 do
  local entry = KEYS[2 + s]
  local stepType = redis.call('HGET', entry, 'stepType')
  local name = redis.call('HGET', entry, 'activityName')
  redis.call('DEL', entry)
  redis.call('ZREM', KEYS[1], ARGV[s])
  if stepType == 'signal' and name then
    if redis.call('HGET', KEYS[2], name) == ARGV[s] then redis.call('HDEL', KEYS[2], name) end
  end
end
return 1
`;

// Apply a list of writes computed client-side, in order, as one script.
// Keys are referenced by their position in KEYS.
// ARGV: [opsJson] — a JSON array of ops:
//   ["ABSENT", k]            — stop with {0} (nothing written) when KEYS[k]
//                              exists; must precede every write.
//   ["HEQ", k, field, value] — stop with {0} unless HGET KEYS[k] field equals
//                              value; must precede every write.
//   ["STATUS", k, to, from, field, value, ...]
//                            — when the workflow hash KEYS[k] has one of the
//                              comma-separated \`from\` statuses (or \`from\` is
//                              '*'), set status = to plus the fields; a status
//                              change bumps the index version.
//   ["INDEXED", k]           — report KEYS[k]'s index fields (a new row).
//   [command, k, ...]        — any other single-key command on KEYS[k].
// Returns {1, <reply of the last command>, snapshot|false}: the snapshot of
// a workflow hash whose index fields changed.
export const WRITE_OPS_LUA = `
${SNAPSHOT_FN}
local ops = cjson.decode(ARGV[1])
local last = 1
local changed = false
for _, op in ipairs(ops) do
  local name = op[1]
  local key = KEYS[op[2]]
  if name == 'ABSENT' then
    if redis.call('EXISTS', key) == 1 then return {0} end
  elseif name == 'HEQ' then
    if redis.call('HGET', key, op[3]) ~= op[4] then return {0} end
  elseif name == 'STATUS' then
    local cur = redis.call('HGET', key, 'status')
    local allowed = false
    if cur then
      if op[4] == '*' then
        allowed = true
      else
        for s in string.gmatch(op[4], '[^,]+') do
          if s == cur then allowed = true end
        end
      end
    end
    if allowed then
      redis.call('HSET', key, 'status', op[3])
      for i = 5, #op, 2 do redis.call('HSET', key, op[i], op[i + 1]) end
      if cur ~= op[3] then
        redis.call('HINCRBY', key, 'iv', 1)
        changed = key
      end
    end
  elseif name == 'INDEXED' then
    changed = key
  else
    local args = {name, key}
    for i = 3, #op do args[#args + 1] = op[i] end
    last = redis.call(unpack(args))
  end
end
if changed then return {1, last, snapshot(changed)} end
return {1, last, false}
`;

// Checkpoint a settled step: its row, its attempt rows, the pending →
// running move and `updatedAt`, then read back the run's status — one
// script. The client builds the row from the existing row it expects (none
// on the first try); a different existing row stops the script with that
// row, for the client to rebuild from and retry. The row is sent without
// its `run` field, which the script prepends from the workflow hash.
// KEYS: [wfKey, attemptsKey]
// ARGV: [wfKey (base), stepName, expectsRow ('1'|'0'), expectedRowJson, rowJson,
//        nowIso, attemptJson_1 .. attemptJson_n]
// Returns {-1} for a missing workflow (nothing written), {0, existingRow|false}
// when the step row is not the expected one, else
// {1, status, error, errorTag, snapshot|false}.
export const CHECKPOINT_STEP_LUA = `
${SNAPSHOT_FN}
local h = redis.call('HMGET', KEYS[1], 'id', 'run', 'status')
if not h[1] then return {-1} end
local stepsKey = ARGV[1] .. ':steps:' .. h[2]
local existing = redis.call('HGET', stepsKey, ARGV[2])
if ARGV[3] == '0' then
  if existing then return {0, existing} end
elseif existing ~= ARGV[4] then
  return {0, existing}
end
redis.call('HSET', stepsKey, ARGV[2], '{"run":' .. h[2] .. ',' .. string.sub(ARGV[5], 2))
for i = 7, #ARGV do redis.call('RPUSH', KEYS[2], ARGV[i]) end
local snap = false
if h[3] == 'pending' then
  redis.call('HSET', KEYS[1], 'status', 'running', 'startedAt', ARGV[6])
  redis.call('HINCRBY', KEYS[1], 'iv', 1)
  snap = snapshot(KEYS[1])
end
redis.call('HSET', KEYS[1], 'updatedAt', ARGV[6])
local s = redis.call('HMGET', KEYS[1], 'status', 'error', 'errorTag')
return {1, s[1], s[2] or '', s[3] or '', snap}
`;

/** Error-reply prefix a fenced script returns when the fence rejects it. */
export const FENCE_REJECTED = "PROMIN_FENCE_REJECTED";

/**
 * Put the fence check in front of a script, so the check and the script's
 * writes run as one atomic script. The fenced script takes the run's lock
 * key as KEYS[1] and the fence token as ARGV[1] ('' for an unfenced call),
 * then the original keys and args, which the body reads as `K` / `A`. A
 * missing lock key (released, or expired through its TTL) or another
 * token rejects the call before anything is written.
 */
export function fencedLua(script: string): string {
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
${script.replace(/\bKEYS\b/g, "K").replace(/\bARGV\b/g, "A")}`;
}

// Status transition guarded by the current status — the read and the write
// happen in one script, so a concurrent cancel can't be overwritten by a
// late completion (or vice versa). With a TTL, the finished run's keys get
// it in the same script.
// KEYS: [wfKey]
// ARGV: [toStatus, nowIso, ttlMs|'', wfKey (base), nFields, field_1, value_1, ...,
//        fromStatus_1 .. fromStatus_n]
// Returns {previousStatus, snapshot}, or false when the workflow is missing
// or its status isn't one of the allowed `from` statuses.
export const TRANSITION_STATUS_LUA = `
${SNAPSHOT_FN}
${EXPIRE_RUN_FN}
local status = redis.call('HGET', KEYS[1], 'status')
if not status then return false end
local nFields = tonumber(ARGV[5])
local fromStart = 6 + nFields * 2
local allowed = false
for i = fromStart, #ARGV do
  if ARGV[i] == status then allowed = true end
end
if not allowed then return false end
redis.call('HSET', KEYS[1], 'status', ARGV[1], 'updatedAt', ARGV[2])
for i = 6, fromStart - 1, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call('HINCRBY', KEYS[1], 'iv', 1)
if ARGV[3] ~= '' then
  expireRun(KEYS[1], ARGV[4], redis.call('HGET', KEYS[1], 'run'), ARGV[3])
end
return {status, snapshot(KEYS[1])}
`;

// Compare-and-set one hash field. Callers read the field, compute the new
// value client-side (keeping JSON fidelity — no cjson round-trip), and
// retry when another writer got in between.
// KEYS: [hash]
// ARGV: [field, expectMissing ('1'|'0'), expected, newValue, requireField|'',
//        extraField_1, extraValue_1, ...]
// Returns 1 on write, 0 on a lost race, -1 when `requireField` is absent.
export const HASH_FIELD_CAS_LUA = `
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

// Create a signal token, deduplicated on (workflowId, idempotencyKey). The
// tokenId → workflowId lookup is its own key; the caller writes it next.
// KEYS: [tokensHash, idempotencyHash]
// ARGV: [tokenId, recordJson, idempotencyKey|'']
// Returns {1, existingTokenId} on a dedup hit, {0, tokenId} on insert.
export const CREATE_SIGNAL_TOKEN_LUA = `
if ARGV[3] ~= '' then
  local existing = redis.call('HGET', KEYS[2], ARGV[3])
  if existing then return {1, existing} end
  redis.call('HSET', KEYS[2], ARGV[3], ARGV[1])
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return {0, ARGV[1]}
`;

// Start a fresh run: archive the current run, bump the run counter, reset
// status, and drop everything the new run must not replay from — the
// activity journal (every step) and the delivered signals — all in one
// atomic script. The dropped sleeps are returned for the caller to take off
// the cross-workflow sleep schedule.
// KEYS: [wfKey, runsKey, signalsKey, journalStepsKey, attemptsKey]
// ARGV: [expectedRun, summaryJson, maxRuns, nowIso, journalBase]
// Returns {newRun, {"<step>::<idx>|<path>" ...}, snapshot}, or {-1} when
// the run moved on since the caller read it.
export const START_FRESH_RUN_LUA = `
${SNAPSHOT_FN}
local run = redis.call('HGET', KEYS[1], 'run')
if run ~= ARGV[1] then return {-1} end
redis.call('RPUSH', KEYS[2], ARGV[2])
redis.call('LTRIM', KEYS[2], -tonumber(ARGV[3]), -1)
local newRun = tonumber(run) + 1
redis.call('HSET', KEYS[1], 'run', tostring(newRun), 'status', 'pending', 'updatedAt', ARGV[4])
redis.call('HDEL', KEYS[1], 'result', 'error', 'errorTag', 'tripwire', 'startedAt', 'completedAt')
redis.call('HINCRBY', KEYS[1], 'iv', 1)
redis.call('PERSIST', KEYS[1])
redis.call('PERSIST', KEYS[2])
redis.call('PERSIST', KEYS[5])
local sleeps = {}
for _, step in ipairs(redis.call('SMEMBERS', KEYS[4])) do
  local jb = ARGV[5] .. step
  for _, m in ipairs(redis.call('ZRANGE', jb .. ':idx', 0, -1)) do
    local suffix = m
    redis.call('DEL', jb .. ':entry:' .. suffix)
    sleeps[#sleeps + 1] = step .. '::' .. suffix
  end
  redis.call('DEL', jb .. ':idx', jb .. ':signal-idx')
end
redis.call('DEL', KEYS[4], KEYS[3])
return {newRun, sleeps, snapshot(KEYS[1])}
`;

// Reset step rows of the current run (WorkflowRunner.resume): delete the
// listed steps' rows, task rows and journal, swap in the kept steps'
// ledger-free rows (compare-and-set against what the caller read), and move
// a terminal run back to `running`.
// KEYS: [wfKey, journalStepsKey]
// ARGV: [wfKey (base), journalBase, expectedRun, nowIso, n, stepName_1 .. stepName_n,
//        then per kept step with a ledger: field, expectedJson, newJson]
// Returns {-1} for a missing workflow, {0} when the run or a kept step row
// changed since the caller read it, else {1, {"<step>::<idx>|<path>" ...}, snapshot|false}.
export const RESET_STEPS_LUA = `
${SNAPSHOT_FN}
local run = redis.call('HGET', KEYS[1], 'run')
if not run then return {-1} end
if run ~= ARGV[3] then return {0} end
local base = ARGV[1]
local stepsKey = base .. ':steps:' .. run
local n = tonumber(ARGV[5])
local casStart = 6 + n
for i = casStart, #ARGV, 3 do
  if redis.call('HGET', stepsKey, ARGV[i]) ~= ARGV[i + 1] then return {0} end
end
local sleeps = {}
for i = 6, 5 + n do
  local name = ARGV[i]
  redis.call('HDEL', stepsKey, name)
  redis.call('DEL', base .. ':tasks:' .. run .. ':' .. name)
  local jb = ARGV[2] .. name
  for _, m in ipairs(redis.call('ZRANGE', jb .. ':idx', 0, -1)) do
    local suffix = m
    redis.call('DEL', jb .. ':entry:' .. suffix)
    sleeps[#sleeps + 1] = name .. '::' .. suffix
  end
  redis.call('DEL', jb .. ':idx', jb .. ':signal-idx')
  redis.call('SREM', KEYS[2], name)
end
for i = casStart, #ARGV, 3 do
  redis.call('HSET', stepsKey, ARGV[i], ARGV[i + 2])
end
local snap = false
local status = redis.call('HGET', KEYS[1], 'status')
redis.call('HSET', KEYS[1], 'updatedAt', ARGV[4])
if status == 'completed' or status == 'failed' or status == 'tripwire' then
  redis.call('HSET', KEYS[1], 'status', 'running')
  redis.call('HDEL', KEYS[1], 'result', 'error', 'errorTag', 'tripwire', 'completedAt')
  redis.call('HINCRBY', KEYS[1], 'iv', 1)
  snap = snapshot(KEYS[1])
end
-- The run continues: lift a retention TTL set when it finished.
redis.call('PERSIST', KEYS[1])
for _, name in ipairs(redis.call('HKEYS', stepsKey)) do
  redis.call('PERSIST', base .. ':tasks:' .. run .. ':' .. name)
end
redis.call('PERSIST', stepsKey)
redis.call('PERSIST', base .. ':signals')
redis.call('PERSIST', base .. ':runs')
redis.call('PERSIST', base .. ':attempts')
redis.call('PERSIST', base .. ':child-intents')
return {1, sleeps, snap}
`;

// Delete every key of one finished workflow.
// KEYS: [wfKey]
// ARGV: [wfKey (base), terminalStatusesCsv]
// Returns {0} when the workflow is gone, {-1} when it isn't terminal, else
// {1, status, workflowName, parentWorkflowId, namespace,
//  {tokenId ...}, {"<step>::<idx>|<path>" ...}}.
export const PURGE_WORKFLOW_LUA = `
local h = redis.call('HMGET', KEYS[1], 'id', 'status', 'run', 'workflowName', 'parentWorkflowId', 'namespace')
if not h[1] then return {0} end
local terminal = false
for s in string.gmatch(ARGV[2], '[^,]+') do
  if s == h[2] then terminal = true end
end
if not terminal then return {-1} end
local b = ARGV[1]
for r = 1, tonumber(h[3]) do
  local stepsKey = b .. ':steps:' .. r
  for _, name in ipairs(redis.call('HKEYS', stepsKey)) do
    redis.call('DEL', b .. ':tasks:' .. r .. ':' .. name)
  end
  redis.call('DEL', stepsKey)
end
local tokenIds = redis.call('HKEYS', b .. ':signal_tokens')
for _, sid in ipairs(redis.call('SMEMBERS', b .. ':stream-ids')) do
  redis.call('DEL', b .. ':streams:' .. sid)
end
local sleeps = {}
local journalSteps = b .. ':journal:steps'
for _, step in ipairs(redis.call('SMEMBERS', journalSteps)) do
  local jb = b .. ':journal:' .. step
  for _, m in ipairs(redis.call('ZRANGE', jb .. ':idx', 0, -1)) do
    local suffix = m
    redis.call('DEL', jb .. ':entry:' .. suffix)
    sleeps[#sleeps + 1] = step .. '::' .. suffix
  end
  redis.call('DEL', jb .. ':idx', jb .. ':signal-idx')
end
redis.call('DEL', KEYS[1], journalSteps, b .. ':signals', b .. ':runs', b .. ':attempts',
  b .. ':signal_tokens', b .. ':signal_token_idempotency', b .. ':stream-ids', b .. ':fence',
  b .. ':child-intents')
return {1, h[2], h[4] or '', h[5] or '', h[6] or '', tokenIds, sleeps}
`;

// -- Cross-workflow index ({idx} slot) -------------------------------------

// Apply one workflow's index fields at index version `iv`; a no-op when the
// index already holds `iv` or a newer version, so out-of-order or repeated
// syncs converge on the latest write.
// KEYS: [versionsHash, recordsHash, byCreated, byStarted, byCompleted,
//        statusSet_1 .. statusSet_S, addSet_1 .. addSet_m]
// ARGV: [workflowId, iv, recordJson, status, createdMs, startedMs|'', completedMs|'',
//        S, status_1 .. status_S, addMember_1 .. addMember_m]
// Returns 1 when applied, 0 when skipped.
export const SYNC_INDEX_LUA = `
local id = ARGV[1]
local cur = redis.call('HGET', KEYS[1], id)
if cur and tonumber(cur) >= tonumber(ARGV[2]) then return 0 end
redis.call('HSET', KEYS[1], id, ARGV[2])
redis.call('HSET', KEYS[2], id, ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[5], id)
if ARGV[6] ~= '' then redis.call('ZADD', KEYS[4], ARGV[6], id) else redis.call('ZREM', KEYS[4], id) end
if ARGV[7] ~= '' then redis.call('ZADD', KEYS[5], ARGV[7], id) else redis.call('ZREM', KEYS[5], id) end
local S = tonumber(ARGV[8])
for i = 1, S do
  if ARGV[8 + i] == ARGV[4] then redis.call('SADD', KEYS[5 + i], id) else redis.call('SREM', KEYS[5 + i], id) end
end
for i = 6 + S, #KEYS do
  redis.call('SADD', KEYS[i], ARGV[i + 3])
end
return 1
`;

// Drop one workflow from the index.
// KEYS: [versionsHash, recordsHash, byCreated, byStarted, byCompleted,
//        memberSet_1 .. memberSet_m, ownedKey_1 .. ownedKey_d]
// ARGV: [workflowId, d] — the last d keys are deleted outright (the
//        workflow's own children set).
export const UNINDEX_LUA = `
local id = ARGV[1]
local d = tonumber(ARGV[2])
redis.call('HDEL', KEYS[1], id)
redis.call('HDEL', KEYS[2], id)
for i = 3, 5 do redis.call('ZREM', KEYS[i], id) end
for i = 6, #KEYS - d do redis.call('SREM', KEYS[i], id) end
for i = #KEYS - d + 1, #KEYS do redis.call('DEL', KEYS[i]) end
return 1
`;

// One page of an ordering sorted set, followed by the members missing from
// it (NULL sort values sort last) in creation order.
// KEYS: [orderZset, byCreated]
// ARGV: [offset, limit, desc ('1'|'0')]
export const INDEX_PAGE_LUA = `
local off = tonumber(ARGV[1])
local lim = tonumber(ARGV[2])
local n = redis.call('ZCARD', KEYS[1])
local out = {}
if off < n and lim > 0 then
  if ARGV[3] == '1' then
    out = redis.call('ZREVRANGE', KEYS[1], off, off + lim - 1)
  else
    out = redis.call('ZRANGE', KEYS[1], off, off + lim - 1)
  end
end
if #out < lim and KEYS[1] ~= KEYS[2] then
  local skip = off - n
  if skip < 0 then skip = 0 end
  for _, id in ipairs(redis.call('ZRANGE', KEYS[2], 0, -1)) do
    if #out >= lim then break end
    if not redis.call('ZSCORE', KEYS[1], id) then
      if skip > 0 then skip = skip - 1 else out[#out + 1] = id end
    end
  end
end
return out
`;

// Cardinality of the intersection of KEYS (sets).
export const INTER_CARD_LUA = `return #redis.call('SINTER', unpack(KEYS))`;
