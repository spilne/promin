// ---------------------------------------------------------------------------
// RedisStepQueue — Redis-backed StepQueue for distributed step dispatch
//
// Every key of a queue starts with `{<prefix>}`, so on Redis Cluster the
// whole queue sits in one slot: a claim merges the heads of the claim lanes and
// moves tasks, running sets and concurrency sets in one script, so there is
// no per-task key worth spreading. Queues with different prefixes land in
// different slots.
//
// Key structure (base = `{<prefix>}`):
//   <base>:task:<id>                               → Hash (task fields)
//   <base>:pending                                 → Sorted Set of all pending
//                                                    tasks, priority-scored
//                                                    (metrics; lane backfill)
//   <base>:running                                 → Set of task IDs
//   <base>:done                                    → Sorted Set of terminal
//                                                    task IDs scored by
//                                                    completion time (ms);
//                                                    drives purge()
//   <base>:counter                                 → Incr for task ID gen
//   <base>:claimseq                                → Incr for claim tokens
//   <base>:active:<wf>::<step>                     → taskId of the active
//                                                    (pending/running) task
//                                                    — drives idempotent
//                                                    enqueue
//   <base>:conc:<len(scope)>:<scope>:<key>         → Set of running task IDs
//                                                    sharing a (concurrency
//                                                    scope, key) — SCARD is
//                                                    the running count that
//                                                    claim checks against
//                                                    the task's limit
//
//   <base>:lane:p, :lane:c:<sfx>, :lane:f:<sfx>:<key>, :lane:n:<sfx>
//                                                  → Sorted Sets, one per
//                                                    claim lane (plain tasks;
//                                                    one concurrency key;
//                                                    one fairness key of a
//                                                    group; a group's tasks
//                                                    without one). Every
//                                                    pending task is in
//                                                    `pending` and in exactly
//                                                    one lane.
//   <base>:ready, <base>:rmem                      → lexicographic Sorted Set
//                                                    of the lanes a claim may
//                                                    take from (priority,
//                                                    fairness rank, age of the
//                                                    lane's head) and a Hash of
//                                                    each lane's member. Lanes
//                                                    held back by a full key
//                                                    or group are absent.
//   <base>:gready:<sfx>, <base>:gmem:<sfx>         → the same per group, over
//                                                    its fairness-key lanes
//   <base>:gtimers                                 → Sorted Set of groups
//                                                    parked until their rate
//                                                    window frees a slot
//   <base>:lanes:v2                                → marker: queues written
//                                                    before the lanes are
//                                                    indexed by the first claim
//   <base>:grp:wf:<workflowId>                     → Set of that run's pending
//                                                    grouped task IDs
//                                                    (`throttled()`)
//   <base>:grp:run:<len(name)>:<name>              → Set of the group's
//                                                    running task IDs
//   <base>:grp:starts:<len(name)>:<name>           → Sorted Set of task IDs
//                                                    scored by claim time
//                                                    (ms): the rate window
//   <base>:grp:frun:<len(name)>:<name>:<key>       → Set of running task IDs
//                                                    of a fairness key
//
// A lease store that fences `requeueStuck` must keep its keys in the same
// slot: construct it with `prefix: "{<prefix>}"`.
// ---------------------------------------------------------------------------

import { SystemWallClock, type WallClock } from "@promin/workflow";
import {
  DEFAULT_MAX_DELIVERIES,
  type StepQueue,
  type StepQueueClaimParams,
  type StepQueueCompleteParams,
  type StepQueueFailParams,
  type StepQueueHeartbeatParams,
  type StepQueueEnqueueParams,
  type StepQueueRequeueParams,
  type StepQueueRequeueResult,
  type StepTask,
  type StepTaskRecord,
  type StepTaskStatus,
  type ThrottledTask,
} from "@promin/workflow/distributed";
import { StaleLeaseError, type LeaderLease } from "@promin/workflow/scheduler";
import { deadLetterError, percentileCont } from "@promin/workflow/storage-kit";
import type { RedisStoreClient } from "./redis-client.ts";
import type { RedisLeaderLeaseStore } from "./redis-leader-lease-store.ts";
import { hashTagOf, storeKeyBase } from "./redis-key-tags.ts";

// -- Lua scripts -------------------------------------------------------------

/**
 * Lua helper shared by the scripts below: the running-set key for a task's
 * (concurrency scope, concurrency key), or nil when the task isn't keyed.
 * The scope is length-prefixed so `a:b` + `c` can't collide with `a` + `b:c`.
 */
const CONC_KEY_LUA = `
local function conc_key(prefix, scope, key)
  if not scope or scope == '' or not key or key == '' then return nil end
  return prefix .. ':conc:' .. #scope .. ':' .. scope .. ':' .. key
end
`;

/**
 * Lua helpers for step groups (named rate-limit / concurrency groups, see
 * \`step-group-limits.ts\`): the keys of a group's running set, start window
 * and fairness-key running sets, and the admission verdict. A start is a
 * claim: a member of the starts zset scored by claim time in ms.
 */
const GROUP_LUA = `
local function grp_run_key(prefix, name) return prefix .. ':grp:run:' .. #name .. ':' .. name end
local function grp_starts_key(prefix, name) return prefix .. ':grp:starts:' .. #name .. ':' .. name end
local function grp_fair_key(prefix, name, key)
  return prefix .. ':grp:frun:' .. #name .. ':' .. name .. ':' .. key
end

-- nil when the group has room, else 'concurrency' or 'rate' and, for 'rate',
-- the ms the window frees a slot. With mutate, expired starts are trimmed.
local function grp_verdict(prefix, name, max_c, max_s, win, now_ms, mutate)
  if max_c and max_c ~= '' and redis.call('SCARD', grp_run_key(prefix, name)) >= tonumber(max_c) then
    return 'concurrency', nil
  end
  if max_s and max_s ~= '' and win and win ~= '' then
    local starts = grp_starts_key(prefix, name)
    local cutoff = now_ms - tonumber(win)
    if mutate then redis.call('ZREMRANGEBYSCORE', starts, '-inf', cutoff) end
    local live = redis.call('ZRANGEBYSCORE', starts, '(' .. cutoff, '+inf', 'WITHSCORES')
    local n = #live / 2
    if n >= tonumber(max_s) then
      -- The start whose expiry frees a slot: n - max_s entries from the oldest.
      local freeing = tonumber(live[(n - tonumber(max_s)) * 2 + 2])
      return 'rate', freeing + tonumber(win)
    end
  end
  return nil, nil
end
`;

/**
 * Lua helpers for the claim lanes. A pending task sits in exactly one lane
 * zset (score = priority * 1e12 + 1e12 - seq, so highest first):
 *
 *   P            <base>:lane:p                      tasks with no group and no
 *                                                   limited concurrency key
 *   C:<sfx>      <base>:lane:c:<sfx>                one (scope, key) with a limit;
 *                                                   sfx = <len(scope)>:<scope>:<key>
 *   G:<sfx>      a step group, sfx = <len(name)>:<name>, made of one child
 *                lane per fairness key: <base>:lane:f:<sfx>:<key>, plus
 *                <base>:lane:n:<sfx> for tasks without one
 *
 * `<base>:ready` is a lexicographic zset (all scores 0) of the lanes a claim
 * may take from, one member per lane:
 * `<priority><rank><seq>|<lane>` with fixed-width digits, so member order is
 * claim order: priority (high first), fairness rank (the fairness key's
 * running count, 0 elsewhere), then age. The member describes the lane's
 * head task. A lane is in `ready` only when it has a pending task and is not
 * held back: a full concurrency key, or a group out of slots or starts, is
 * absent, so a claim never reads the backlog behind it. A group's own
 * `<base>:gready:<sfx>` indexes its child lanes the same way, and its node
 * stands in `ready` with its best child's member.
 *
 * Whatever changes a lane or what holds it back calls `after_add`,
 * `after_free` or `after_admit` on the task concerned. A group held back by
 * its rate window is parked in `<base>:gtimers` (score = when a slot frees)
 * and re-checked by the next claim.
 *
 * Every script sets PFX (the `{<prefix>}` base) and NOW_MS first.
 */
const LANES_LUA = `
local PFX = nil
local NOW_MS = 0

local function ready_key() return PFX .. ':ready' end
local function ready_mem() return PFX .. ':rmem' end
local function timers_key() return PFX .. ':gtimers' end

local function prefix_of(priority, rank, id)
  local p = tonumber(priority) or 5
  if p > 1073741823 then p = 1073741823 elseif p < -1073741823 then p = -1073741823 end
  return string.format('%010d%010d%012d', 1073741823 - p, rank, tonumber(id))
end

local function child_zkey(suffix, child)
  if child == 'N' then return PFX .. ':lane:n:' .. suffix end
  return PFX .. ':lane:f:' .. suffix .. ':' .. string.sub(child, 3)
end

local function set_nonempty(v) return v and v ~= '' end

-- The lane of a task, from its (immutable) concurrency and group fields:
-- f.c_scope, f.c_key, f.c_limit, f.g.name, f.g.fair.
local function desc_of(f)
  if set_nonempty(f.g.name) then
    local suffix = #f.g.name .. ':' .. f.g.name
    local child = set_nonempty(f.g.fair) and ('F:' .. f.g.fair) or 'N'
    return { kind = 'g', suffix = suffix, child = child, zkey = child_zkey(suffix, child), node = 'G:' .. suffix }
  end
  if set_nonempty(f.c_scope) and set_nonempty(f.c_key) and set_nonempty(f.c_limit) then
    local suffix = #f.c_scope .. ':' .. f.c_scope .. ':' .. f.c_key
    return { kind = 'c', suffix = suffix, zkey = PFX .. ':lane:c:' .. suffix, node = 'C:' .. suffix }
  end
  return { kind = 'p', zkey = PFX .. ':lane:p', node = 'P' }
end

local function conc_desc_of(f)
  if set_nonempty(f.c_scope) and set_nonempty(f.c_key) and set_nonempty(f.c_limit) then
    local suffix = #f.c_scope .. ':' .. f.c_scope .. ':' .. f.c_key
    return { kind = 'c', suffix = suffix, zkey = PFX .. ':lane:c:' .. suffix, node = 'C:' .. suffix }
  end
  return nil
end

local function ix_set(index, mem, ref, member)
  local old = redis.call('HGET', mem, ref)
  if old == member then return end
  if old then redis.call('ZREM', index, old) end
  redis.call('ZADD', index, 0, member)
  redis.call('HSET', mem, ref, member)
end

local function ix_clear(index, mem, ref)
  local old = redis.call('HGET', mem, ref)
  if old then
    redis.call('ZREM', index, old)
    redis.call('HDEL', mem, ref)
  end
end

local function head_of(zkey) return redis.call('ZREVRANGE', zkey, 0, 0)[1] end

local function refresh_plain()
  local head = head_of(PFX .. ':lane:p')
  if not head then ix_clear(ready_key(), ready_mem(), 'P') return end
  local prio = redis.call('HGET', PFX .. ':task:' .. head, 'priority')
  ix_set(ready_key(), ready_mem(), 'P', prefix_of(prio, 0, head) .. '|P')
end

local function refresh_conc(d)
  local head = head_of(d.zkey)
  if not head then ix_clear(ready_key(), ready_mem(), d.node) return end
  local h = redis.call('HMGET', PFX .. ':task:' .. head, 'priority', 'concurrencyLimit')
  if redis.call('SCARD', PFX .. ':conc:' .. d.suffix) >= tonumber(h[2]) then
    ix_clear(ready_key(), ready_mem(), d.node)
    return
  end
  ix_set(ready_key(), ready_mem(), d.node, prefix_of(h[1], 0, head) .. '|' .. d.node)
end

local function refresh_child(d)
  local gready = PFX .. ':gready:' .. d.suffix
  local gmem = PFX .. ':gmem:' .. d.suffix
  local head = head_of(d.zkey)
  if not head then ix_clear(gready, gmem, d.child) return end
  local prio = redis.call('HGET', PFX .. ':task:' .. head, 'priority')
  local rank = 0
  if d.child ~= 'N' then
    rank = redis.call('SCARD', PFX .. ':grp:frun:' .. d.suffix .. ':' .. string.sub(d.child, 3))
  end
  ix_set(gready, gmem, d.child, prefix_of(prio, rank, head) .. '|' .. d.child)
end

local function refresh_group(suffix)
  local node = 'G:' .. suffix
  local best = redis.call('ZRANGEBYLEX', PFX .. ':gready:' .. suffix, '-', '+', 'LIMIT', 0, 1)[1]
  if not best then
    ix_clear(ready_key(), ready_mem(), node)
    redis.call('ZREM', timers_key(), suffix)
    return
  end
  local head = string.format('%d', tonumber(string.sub(best, 21, 32)))
  local h = redis.call('HMGET', PFX .. ':task:' .. head, 'groupName', 'groupMaxConcurrent',
    'groupMaxStarts', 'groupWindowMs')
  local reason, retry = grp_verdict(PFX, h[1], h[2], h[3], h[4], NOW_MS, false)
  if reason then
    ix_clear(ready_key(), ready_mem(), node)
    if reason == 'rate' then
      redis.call('ZADD', timers_key(), retry, suffix)
    else
      redis.call('ZREM', timers_key(), suffix)
    end
    return
  end
  redis.call('ZREM', timers_key(), suffix)
  ix_set(ready_key(), ready_mem(), node, string.sub(best, 1, 32) .. '|' .. node)
end

local function refresh(d)
  if d.kind == 'p' then refresh_plain()
  elseif d.kind == 'c' then refresh_conc(d)
  else
    refresh_child(d)
    refresh_group(d.suffix)
  end
end

-- A task joined its lane: only a new head changes what the lane offers.
local function after_add(f, id)
  local d = desc_of(f)
  if head_of(d.zkey) == id then refresh(d) end
end

-- A task left a running set: its group slot, fairness-key slot and concurrency slot are free.
local function after_free(f)
  local d = desc_of(f)
  if d.kind == 'g' then
    refresh_child(d)
    refresh_group(d.suffix)
  end
  local c = conc_desc_of(f)
  if c then refresh_conc(c) end
end

-- A task left its lane for a running set.
local function after_admit(f)
  local d = desc_of(f)
  refresh(d)
  if d.kind ~= 'c' then
    local c = conc_desc_of(f)
    if c then refresh_conc(c) end
  end
end

-- Re-check the groups whose rate window has been parked until now.
local function wake_timers()
  for _, suffix in ipairs(redis.call('ZRANGEBYSCORE', timers_key(), '-inf', NOW_MS)) do
    redis.call('ZREM', timers_key(), suffix)
    refresh_group(suffix)
  end
end
`;

/**
 * Atomically enqueue a task with idempotency on (workflow_id, step_name).
 * If an active task exists for the same pair (active key present), return
 * its id. Otherwise: INCR counter, HSET task hash (including needs as
 * JSON), ZADD to the pending set and to the task's claim lane (priority +
 * FIFO), SET active key to the new task id.
 *
 * KEYS: [active_key, pending_key, counter_key, ready_key]
 * ARGV: [task_key_prefix, workflowId, stepName, priority, inputJson,
 *        depsJson, needsJson, createdAt, version, namespace,
 *        metadataJson, concurrencyKey, concurrencyScope, concurrencyLimit,
 *        attempt, dependsOnJson, timeoutMs, run, groupName, groupMaxConcurrent,
 *        groupMaxStarts, groupWindowMs, fairnessKey, base, now_ms] — optional values are '' when unset
 */
const ENQUEUE_LUA =
  CONC_KEY_LUA +
  GROUP_LUA +
  LANES_LUA +
  `
local active_key = KEYS[1]
local pending_key = KEYS[2]
local counter_key = KEYS[3]
PFX = ARGV[24]
NOW_MS = tonumber(ARGV[25])

local existing = redis.call('GET', active_key)
if existing then return existing end

local seq = redis.call('INCR', counter_key)
local id = tostring(seq)
local task_key = ARGV[1] .. id

local fields = {
  'id', id,
  'workflowId', ARGV[2],
  'stepName', ARGV[3],
  'priority', ARGV[4],
  'input', ARGV[5],
  'deps', ARGV[6],
  'dependsOn', ARGV[16],
  'needs', ARGV[7],
  'attempt', ARGV[15],
  'run', ARGV[18],
  'deliveries', '0',
  'status', 'pending',
  'createdAt', ARGV[8],
}
local optional = {
  {9, 'version'}, {10, 'namespace'}, {11, 'metadata'},
  {12, 'concurrencyKey'}, {13, 'concurrencyScope'}, {14, 'concurrencyLimit'},
  {17, 'timeoutMs'},
  {19, 'groupName'}, {20, 'groupMaxConcurrent'}, {21, 'groupMaxStarts'}, {22, 'groupWindowMs'},
  {23, 'fairnessKey'},
}
for _, o in ipairs(optional) do
  if ARGV[o[1]] ~= '' then
    table.insert(fields, o[2])
    table.insert(fields, ARGV[o[1]])
  end
end
redis.call('HSET', task_key, unpack(fields))

local priority = tonumber(ARGV[4])
local score = priority * 1e12 + (1e12 - seq)
redis.call('ZADD', pending_key, score, id)
local f = {
  c_key = ARGV[12], c_scope = ARGV[13], c_limit = ARGV[14],
  g = { name = ARGV[19], fair = ARGV[23] },
}
redis.call('ZADD', desc_of(f).zkey, score, id)
if ARGV[19] ~= '' then
  redis.call('SADD', PFX .. ':grp:wf:' .. ARGV[2], id)
end
redis.call('SET', active_key, id)
after_add(f, id)

return id
`;

/**
 * Claim up to `limit` pending tasks the worker can run: needs ⊆
 * capabilities, step name in `step_names` (when given), version in
 * `versions` or unset (when given), the (concurrency scope, key) under its
 * limit and the task's step group (if any) with room. Every claimed task
 * joins its running sets inside the same script, so the caps hold within one
 * call and across concurrent claimers.
 *
 * Tasks are read from the claim lanes (see LANES_LUA), merged through the
 * `ready` index in claim order: priority, fairness rank (a keyed task of a
 * group ranks at its fairness key's running count, so keys interleave
 * round-robin), age. Full concurrency keys and groups are not in `ready`, so
 * a throttled backlog is never read and fairness is exact however deep the
 * queues are. The one thing that still costs per task is routing: a lane
 * whose head this worker can't run is walked past it, at most `scan_budget`
 * tasks per call, so a backlog of other workers' tasks can't hold Redis.
 *
 * Queues written before the lanes existed are indexed once, by the first
 * claim (marker key `lanes:v2`).
 *
 * KEYS: [pending_key, running_key, claimseq_key, ready_key]
 * ARGV: [worker_id, now, limit, prefix, caps_json, scan_budget,
 *        step_names_json ('' = any), versions_json ('' = any), now_ms]
 */
const CLAIM_LUA =
  CONC_KEY_LUA +
  GROUP_LUA +
  LANES_LUA +
  `
local pending_key = KEYS[1]
local running_key = KEYS[2]
local claimseq_key = KEYS[3]
local worker_id = ARGV[1]
local now = ARGV[2]
local limit = tonumber(ARGV[3])
local prefix = ARGV[4]
local caps = cjson.decode(ARGV[5])
local scan_budget = tonumber(ARGV[6])
PFX = prefix
NOW_MS = tonumber(ARGV[9])
local now_ms = NOW_MS

local caps_set = {}
for _, c in ipairs(caps) do caps_set[c] = true end
local step_set = nil
if ARGV[7] ~= '' then
  step_set = {}
  for _, s in ipairs(cjson.decode(ARGV[7])) do step_set[s] = true end
end
local version_set = nil
if ARGV[8] ~= '' then
  version_set = {}
  for _, v in ipairs(cjson.decode(ARGV[8])) do version_set[v] = true end
end

local page = limit * 4
if page < 16 then page = 16 end

local function fields_of(task_key)
  local h = redis.call('HMGET', task_key, 'needs', 'concurrencyKey', 'concurrencyScope',
    'concurrencyLimit', 'stepName', 'version', 'priority', 'groupName', 'groupMaxConcurrent',
    'groupMaxStarts', 'groupWindowMs', 'fairnessKey', 'workflowId')
  return {
    needs = h[1], c_key = h[2], c_scope = h[3], c_limit = h[4], step_name = h[5], version = h[6],
    priority = tonumber(h[7]) or 5,
    g = { name = h[8], max_c = h[9], max_s = h[10], win = h[11], fair = h[12], workflow_id = h[13] },
  }
end

-- First claim on a queue that predates the lanes: index every pending task.
if redis.call('EXISTS', PFX .. ':lanes:v2') == 0 then
  local seen = {}
  for _, id in ipairs(redis.call('ZRANGE', pending_key, 0, -1, 'WITHSCORES') ) do
    seen[#seen + 1] = id
  end
  for i = 1, #seen, 2 do
    local id, score = seen[i], seen[i + 1]
    local task_key = PFX .. ':task:' .. id
    if redis.call('HGET', task_key, 'status') == 'pending' then
      local f = fields_of(task_key)
      redis.call('ZADD', desc_of(f).zkey, score, id)
    end
  end
  local refreshed = {}
  for i = 1, #seen, 2 do
    local task_key = PFX .. ':task:' .. seen[i]
    if redis.call('HGET', task_key, 'status') == 'pending' then
      local d = desc_of(fields_of(task_key))
      if not refreshed[d.zkey] then
        refreshed[d.zkey] = true
        refresh(d)
      end
    end
  end
  redis.call('DEL', PFX .. ':grp:pending')
  redis.call('SET', PFX .. ':lanes:v2', '1')
end

wake_timers()

local results = {}
local examined = 0
-- Per call: lanes with nothing left to take, and how many leading tasks of a
-- lane this worker can't run (they stay unroutable, so they are not re-read).
local dead = {}
local skip_off = {}

local function routable(f)
  local needs_json, step_name, version = f.needs, f.step_name, f.version
  if step_set and not step_set[step_name] then return false end
  if version_set and version and version ~= '' and not version_set[version] then return false end
  if needs_json and needs_json ~= '' and needs_json ~= '[]' then
    for _, n in ipairs(cjson.decode(needs_json)) do
      if not caps_set[n] then return false end
    end
  end
  return true
end

-- Whether the task's own caps have room right now.
local function has_room(f)
  local conc = conc_key(PFX, f.c_scope, f.c_key)
  if conc and f.c_limit and redis.call('SCARD', conc) >= tonumber(f.c_limit) then return false end
  if set_nonempty(f.g.name) then
    if grp_verdict(PFX, f.g.name, f.g.max_c, f.g.max_s, f.g.win, now_ms, true) then return false end
  end
  return true
end

-- Hand the task to the worker: leave pending and its lane, join every running set.
local function admit(id, f)
  local task_key = PFX .. ':task:' .. id
  local conc = conc_key(PFX, f.c_scope, f.c_key)
  local g = f.g
  local claim_token = worker_id .. ':' .. id .. ':' .. redis.call('INCR', claimseq_key)
  redis.call('ZREM', pending_key, id)
  redis.call('ZREM', desc_of(f).zkey, id)
  redis.call('SADD', running_key, id)
  if conc then redis.call('SADD', conc, id) end
  if g.name and g.name ~= '' then
    redis.call('SREM', PFX .. ':grp:wf:' .. g.workflow_id, id)
    redis.call('SADD', grp_run_key(PFX, g.name), id)
    if g.fair and g.fair ~= '' then redis.call('SADD', grp_fair_key(PFX, g.name, g.fair), id) end
    if g.max_s and g.max_s ~= '' and g.win and g.win ~= '' then
      local starts = grp_starts_key(PFX, g.name)
      redis.call('ZADD', starts, now_ms, id)
      redis.call('PEXPIRE', starts, tonumber(g.win) * 2)
    end
  end
  redis.call('HINCRBY', task_key, 'deliveries', 1)
  redis.call('HSET', task_key, 'status', 'running', 'claimedBy', worker_id, 'claimedAt', now,
    'claimToken', claim_token, 'heartbeatAt', '')
  table.insert(results, redis.call('HGETALL', task_key))
  after_admit(f)
end

-- First task of a lane this worker can take now, from its leading unroutable
-- run on. Returns id, fields; nil when the lane has none (or the budget ran out).
local function peek_lane(zkey)
  if dead[zkey] then return nil end
  local pos = skip_off[zkey] or 0
  local leading = true
  while examined < scan_budget do
    local ids = redis.call('ZREVRANGE', zkey, pos, pos + page - 1)
    if #ids == 0 then
      dead[zkey] = true
      return nil
    end
    for _, id in ipairs(ids) do
      if examined >= scan_budget then return nil end
      examined = examined + 1
      local f = fields_of(PFX .. ':task:' .. id)
      if routable(f) then
        leading = false
        if has_room(f) then return id, f end
      elseif leading then
        skip_off[zkey] = pos + 1
      end
      pos = pos + 1
    end
  end
  return nil
end

local function rank_of(suffix, child)
  if child == 'N' then return 0 end
  return redis.call('SCARD', PFX .. ':grp:frun:' .. suffix .. ':' .. string.sub(child, 3))
end

-- Best task of a group: the lowest candidate among its child lanes, which
-- are read in index order until none can beat the best found.
local function peek_group(suffix)
  local node = 'G:' .. suffix
  if dead[node] then return nil end
  local gready = PFX .. ':gready:' .. suffix
  local best = nil
  local lo = '-'
  local exhausted = false
  while true do
    local entries = redis.call('ZRANGEBYLEX', gready, lo, '+', 'LIMIT', 0, 16)
    if #entries == 0 then
      exhausted = true
      break
    end
    local stop = false
    for _, e in ipairs(entries) do
      if best and best.pfx < string.sub(e, 1, 32) then
        stop = true
        break
      end
      local child = string.sub(e, 34)
      local id, f = peek_lane(child_zkey(suffix, child))
      if id then
        local p = prefix_of(f.priority, rank_of(suffix, child), id)
        if not best or p < best.pfx then best = { id = id, f = f, pfx = p } end
      end
      if examined >= scan_budget then
        stop = true
        break
      end
    end
    if stop then break end
    lo = '(' .. entries[#entries]
  end
  if not best and exhausted then dead[node] = true end
  return best
end

local function peek_node(ref)
  if ref == 'P' then
    local id, f = peek_lane(PFX .. ':lane:p')
    if id then return { id = id, f = f, pfx = prefix_of(f.priority, 0, id) } end
    return nil
  end
  local suffix = string.sub(ref, 3)
  if string.sub(ref, 1, 2) == 'C:' then
    local id, f = peek_lane(PFX .. ':lane:c:' .. suffix)
    if id then return { id = id, f = f, pfx = prefix_of(f.priority, 0, id) } end
    return nil
  end
  return peek_group(suffix)
end

-- The next task to claim: the lowest candidate among the ready lanes.
local function select_next()
  local best = nil
  local lo = '-'
  while true do
    local entries = redis.call('ZRANGEBYLEX', ready_key(), lo, '+', 'LIMIT', 0, 16)
    if #entries == 0 then break end
    local stop = false
    for _, e in ipairs(entries) do
      if best and best.pfx < string.sub(e, 1, 32) then
        stop = true
        break
      end
      local cand = peek_node(string.sub(e, 34))
      if cand and (not best or cand.pfx < best.pfx) then best = cand end
      if examined >= scan_budget then
        stop = true
        break
      end
    end
    if stop then break end
    lo = '(' .. entries[#entries]
  end
  return best
end

while #results < limit and examined < scan_budget do
  local c = select_next()
  if not c then break end
  admit(c.id, c.f)
end

return results
`;

/**
 * Take a running task off the running set and its concurrency slot, then
 * either put it back on the pending zset or — when it has used up
 * `max_deliveries` — dead-letter it (mark it failed, clear its active key,
 * index it as done). No-op unless the task is still running under
 * `claim_token` (empty token skips that check), so a racing complete/fail
 * wins.
 *
 * `mode` = 'requeue' (requeueStuck) or 'release' (release(): never
 * dead-letters, and gives the delivery back).
 *
 * Fenced when ARGV[9] is non-empty: nothing is written, and -1 returned,
 * unless the lease epoch key (KEYS[6]) still holds ARGV[9].
 *
 * A grouped task also leaves its group's running sets; going back to pending
 * it gives its start back (a dead-lettered task keeps it). Either way the
 * lanes its slots held back are re-checked, and a task going back to pending
 * rejoins its lane.
 *
 * KEYS: [task_key, running_key, pending_key, done_key, ready_key, lease_epoch_key?]
 * ARGV: [id, prefix, claim_token, mode, max_deliveries, dead_letter_error,
 *        completed_at_iso, completed_at_ms, lease_epoch|'']
 * Returns -1 (stale lease), 0 (not running under the token), 1 (back to
 * pending), 2 (dead-lettered).
 */
const REQUEUE_LUA =
  CONC_KEY_LUA +
  GROUP_LUA +
  LANES_LUA +
  `
local task_key = KEYS[1]
local running_key = KEYS[2]
local pending_key = KEYS[3]
local done_key = KEYS[4]
local id = ARGV[1]
local prefix = ARGV[2]
local claim_token = ARGV[3]
local mode = ARGV[4]
PFX = prefix
NOW_MS = tonumber(ARGV[8])

if ARGV[9] and ARGV[9] ~= '' and redis.call('GET', KEYS[6]) ~= ARGV[9] then return -1 end

local f = redis.call('HMGET', task_key, 'status', 'claimToken', 'priority', 'concurrencyKey',
  'concurrencyScope', 'deliveries', 'workflowId', 'stepName', 'groupName', 'fairnessKey',
  'concurrencyLimit')
if f[1] ~= 'running' then return 0 end
if claim_token ~= '' and f[2] ~= claim_token then return 0 end
local lf = { c_key = f[4], c_scope = f[5], c_limit = f[11], g = { name = f[9], fair = f[10] } }

redis.call('SREM', running_key, id)
local conc = conc_key(prefix, f[5], f[4])
if conc then redis.call('SREM', conc, id) end
local group = f[9]
if group and group ~= '' then
  redis.call('SREM', grp_run_key(prefix, group), id)
  if f[10] and f[10] ~= '' then redis.call('SREM', grp_fair_key(prefix, group, f[10]), id) end
end

local deliveries = tonumber(f[6]) or 0
if mode == 'requeue' and deliveries >= tonumber(ARGV[5]) then
  redis.call('HSET', task_key,
    'status', 'failed',
    'error', ARGV[6],
    'completedAt', ARGV[7],
    'claimToken', '',
    'heartbeatAt', '')
  redis.call('ZADD', done_key, tonumber(ARGV[8]), id)
  after_free(lf)
  return 2
end

if mode == 'release' and deliveries > 0 then
  redis.call('HINCRBY', task_key, 'deliveries', -1)
end
redis.call('HSET', task_key,
  'status', 'pending',
  'claimedBy', '',
  'claimedAt', '',
  'claimToken', '',
  'heartbeatAt', '')
local priority = tonumber(f[3]) or 5
local score = priority * 1e12 + (1e12 - tonumber(id))
redis.call('ZADD', pending_key, score, id)
redis.call('ZADD', desc_of(lf).zkey, score, id)
if group and group ~= '' then
  redis.call('ZREM', grp_starts_key(prefix, group), id)
  redis.call('SADD', prefix .. ':grp:wf:' .. f[7], id)
end
after_free(lf)
after_add(lf, id)
return 1
`;

/**
 * Settle a running task as completed or failed. The claim check and the
 * write are one script, so a stale claim can never settle the task.
 *
 * KEYS: [task_key, running_key, done_key]
 * ARGV: [id, claim_token, status, value_field, value, duration_ms,
 *        completed_at_iso, prefix, completed_at_ms, ...extra field/value
 *        pairs] — an extra pair with an empty value is not written
 */
const SETTLE_LUA =
  CONC_KEY_LUA +
  GROUP_LUA +
  LANES_LUA +
  `
local task_key = KEYS[1]
local running_key = KEYS[2]
local done_key = KEYS[3]
local id = ARGV[1]
local claim_token = ARGV[2]
local prefix = ARGV[8]
PFX = prefix
NOW_MS = tonumber(ARGV[9])

local f = redis.call('HMGET', task_key, 'status', 'claimToken', 'workflowId', 'stepName',
  'concurrencyScope', 'concurrencyKey', 'groupName', 'fairnessKey', 'concurrencyLimit')
if f[1] ~= 'running' then return 0 end
if claim_token ~= '' and f[2] ~= claim_token then return 0 end

redis.call('HSET', task_key,
  'status', ARGV[3],
  ARGV[4], ARGV[5],
  'durationMs', ARGV[6],
  'completedAt', ARGV[7])
for i = 10, #ARGV - 1, 2 do
  if ARGV[i + 1] ~= '' then redis.call('HSET', task_key, ARGV[i], ARGV[i + 1]) end
end
redis.call('SREM', running_key, id)
-- The task keeps its (workflowId, stepName) active slot until consumed.
local conc = conc_key(prefix, f[5], f[6])
if conc then redis.call('SREM', conc, id) end
-- A grouped task frees its group slot; its start stays in the rate window.
if f[7] and f[7] ~= '' then
  redis.call('SREM', grp_run_key(prefix, f[7]), id)
  if f[8] and f[8] ~= '' then redis.call('SREM', grp_fair_key(prefix, f[7], f[8]), id) end
end
redis.call('ZADD', done_key, tonumber(ARGV[9]), id)
after_free({ c_scope = f[5], c_key = f[6], c_limit = f[9], g = { name = f[7], fair = f[8] } })
return 1
`;

const HEARTBEAT_LUA = `
local task_key = KEYS[1]
local claim_token = ARGV[1]
local heartbeat_at = ARGV[2]
-- ARGV[3] = '1' when the heartbeat carries details; ARGV[4] is their JSON
-- ('' clears them). Without them the stored details stay.
local set_details = ARGV[3]
local details = ARGV[4]

local status = redis.call('HGET', task_key, 'status')
local current_token = redis.call('HGET', task_key, 'claimToken')
if status ~= 'running' then return 0 end
if claim_token ~= '' and current_token ~= claim_token then return 0 end

redis.call('HSET', task_key, 'heartbeatAt', heartbeat_at)
if set_details == '1' then redis.call('HSET', task_key, 'heartbeatDetails', details) end
return 1
`;

/**
 * Delete up to `batch` terminal tasks completed before `cutoff_ms`.
 *
 * KEYS: [done_key]
 * ARGV: [prefix, cutoff_ms (exclusive), batch]
 * Returns the number of tasks deleted.
 */
const PURGE_LUA = `
local done_key = KEYS[1]
local prefix = ARGV[1]
local ids = redis.call('ZRANGEBYSCORE', done_key, '-inf', '(' .. ARGV[2], 'LIMIT', 0, tonumber(ARGV[3]))
local purged = 0
for _, id in ipairs(ids) do
  local task_key = prefix .. ':task:' .. id
  local status = redis.call('HGET', task_key, 'status')
  -- A task that left the terminal states isn't ours to delete; it is
  -- re-indexed when it settles again.
  if status == 'completed' or status == 'failed' or not status then
    if status then
      purged = purged + 1
      local f = redis.call('HMGET', task_key, 'workflowId', 'stepName')
      local active_key = prefix .. ':active:' .. tostring(f[1]) .. '::' .. tostring(f[2])
      if redis.call('GET', active_key) == id then redis.call('DEL', active_key) end
    end
    redis.call('DEL', task_key)
  end
  redis.call('ZREM', done_key, id)
end
return {purged, #ids}
`;

/**
 * The grouped pending tasks of one run that their group holds back right now:
 * flat `[id, reason, retryAtMs | '']` triples.
 *
 * KEYS: [grp_wf_key]
 * ARGV: [prefix, now_ms]
 */
const THROTTLED_LUA =
  GROUP_LUA +
  `
local prefix = ARGV[1]
local now_ms = tonumber(ARGV[2])
local out = {}
for _, id in ipairs(redis.call('SMEMBERS', KEYS[1])) do
  local h = redis.call('HMGET', prefix .. ':task:' .. id, 'status', 'groupName',
    'groupMaxConcurrent', 'groupMaxStarts', 'groupWindowMs')
  if h[1] == 'pending' and h[2] and h[2] ~= '' then
    local reason, retry = grp_verdict(prefix, h[2], h[3], h[4], h[5], now_ms, false)
    if reason then
      table.insert(out, id)
      table.insert(out, reason)
      table.insert(out, retry and string.format('%.0f', retry) or '')
    end
  end
end
return out
`;

/**
 * Consume a settled task: stamp `consumedAt` and free its active slot.
 *
 * KEYS: [task_key]
 * ARGV: [id, prefix, consumed_at_iso]
 * Returns 1 when consumed, 0 when missing, unsettled or already consumed.
 */
const CONSUME_LUA = `
local task_key = KEYS[1]
local id = ARGV[1]
local f = redis.call('HMGET', task_key, 'status', 'consumedAt', 'workflowId', 'stepName')
if f[1] ~= 'completed' and f[1] ~= 'failed' then return 0 end
if f[2] and f[2] ~= '' then return 0 end
redis.call('HSET', task_key, 'consumedAt', ARGV[3])
local active_key = ARGV[2] .. ':active:' .. f[3] .. '::' .. f[4]
if redis.call('GET', active_key) == id then redis.call('DEL', active_key) end
return 1
`;

// -- Implementation ----------------------------------------------------------

export interface RedisStepQueueConfig {
  redis: RedisStoreClient;
  /** Key prefix for all queue keys. Default: "sq". */
  prefix?: string;
  /**
   * Upper bound on pending entries one `claim()` examines while skipping
   * tasks the worker can't take (unmet `needs`, other steps or versions).
   * Full concurrency keys and step groups are not scanned at all: they are
   * out of the ready index until they have room. Default: 1000.
   */
  claimScanLimit?: number;
  /**
   * Deliveries after which `requeueStuck` dead-letters a task instead of
   * requeueing it. Default: `DEFAULT_MAX_DELIVERIES` (10).
   */
  maxDeliveries?: number;
  /** Time source for client-side timestamps. Default: `SystemWallClock`. */
  clock?: WallClock;
  /**
   * Lease store whose epoch keys `requeueStuck({ lease })` fences against:
   * every requeue script checks the lease's epoch before writing, and the
   * sweep rejects with `StaleLeaseError` once it has moved on. Must live on
   * the same Redis as the queue, with its keys in the queue's slot:
   * construct it with `prefix: "{<queue prefix>}"` (the constructor throws
   * otherwise). Without it the `lease` param is ignored.
   */
  leaseStore?: RedisLeaderLeaseStore;
}

const PURGE_BATCH = 500;
/** Ids per batch of task reads in `metrics()`. */
const METRICS_BATCH = 500;

export class RedisStepQueue implements StepQueue {
  private readonly redis: RedisStoreClient;
  private readonly prefix: string;
  /** `{<prefix>}`: the start of every key, and the queue's slot. */
  private readonly base: string;
  private readonly claimScanLimit: number;
  private readonly maxDeliveries: number;
  private readonly clock: WallClock;
  private readonly leaseStore: RedisLeaderLeaseStore | undefined;

  constructor(config: RedisStepQueueConfig) {
    this.redis = config.redis;
    this.prefix = config.prefix ?? "sq";
    this.base = storeKeyBase(this.prefix);
    this.claimScanLimit = config.claimScanLimit ?? 1000;
    this.maxDeliveries = config.maxDeliveries ?? DEFAULT_MAX_DELIVERIES;
    this.clock = config.clock ?? SystemWallClock;
    this.leaseStore = config.leaseStore;
    if (this.leaseStore) {
      const epoch = this.leaseStore.keysFor("lease").epoch;
      if (hashTagOf(epoch) !== hashTagOf(this.base)) {
        throw new Error(
          `RedisStepQueue: the lease store's keys (${epoch}) must share the queue's ` +
            `hash tag ${this.base}: construct it with prefix "${this.base}"`,
        );
      }
    }
  }

  // -- Key helpers -----------------------------------------------------------

  private taskKey(id: string): string {
    return `${this.base}:task:${id}`;
  }

  private get pendingKey(): string {
    return `${this.base}:pending`;
  }

  private get runningKey(): string {
    return `${this.base}:running`;
  }

  private get readyKey(): string {
    return `${this.base}:ready`;
  }

  private get doneKey(): string {
    return `${this.base}:done`;
  }

  /**
   * Active-task key for idempotent enqueue. Workflow ids are globally
   * unique, so (workflow_id, step_name) is the key.
   */
  private activeKey(workflowId: string, stepName: string): string {
    return `${this.base}:active:${workflowId}::${stepName}`;
  }

  // -- StepQueue interface ---------------------------------------------------

  async enqueue(params: StepQueueEnqueueParams): Promise<string> {
    const priority = params.priority ?? 5;
    const needs = params.needs ?? [];
    // One Lua round-trip: check active, maybe insert, atomic.
    const id = (await this.redis.eval(
      ENQUEUE_LUA,
      4,
      this.activeKey(params.workflowId, params.stepName),
      this.pendingKey,
      `${this.base}:counter`,
      this.readyKey,
      `${this.base}:task:`,
      params.workflowId,
      params.stepName,
      String(priority),
      JSON.stringify(params.input),
      JSON.stringify(params.deps ?? {}),
      JSON.stringify(needs),
      this.clock.now().toISOString(),
      params.version ?? "",
      params.namespace ?? "",
      params.metadata !== undefined ? JSON.stringify(params.metadata) : "",
      params.concurrencyKey ?? "",
      params.concurrencyScope ?? "",
      params.concurrencyLimit !== undefined ? String(params.concurrencyLimit) : "",
      String(params.attempt ?? 1),
      JSON.stringify(params.dependsOn ?? []),
      params.timeoutMs !== undefined ? String(params.timeoutMs) : "",
      String(params.run ?? 1),
      params.groupName ?? "",
      params.groupMaxConcurrent !== undefined ? String(params.groupMaxConcurrent) : "",
      params.groupMaxStarts !== undefined ? String(params.groupMaxStarts) : "",
      params.groupWindowMs !== undefined ? String(params.groupWindowMs) : "",
      params.fairnessKey ?? "",
      this.base,
      String(this.clock.currentTimeMs()),
    )) as string;
    return String(id);
  }

  async claim(params: StepQueueClaimParams): Promise<StepTask[]> {
    const limit = Math.max(1, Math.floor(params.limit));
    if (params.stepNames !== undefined && params.stepNames.length === 0) return [];

    const raw = (await this.redis.eval(
      CLAIM_LUA,
      4,
      this.pendingKey,
      this.runningKey,
      `${this.base}:claimseq`,
      this.readyKey,
      params.workerId,
      this.clock.now().toISOString(),
      String(limit),
      this.base,
      JSON.stringify(params.capabilities ?? []),
      String(Math.max(limit * 4, this.claimScanLimit)),
      params.stepNames !== undefined ? JSON.stringify(params.stepNames) : "",
      params.versions !== undefined ? JSON.stringify(params.versions) : "",
      String(this.clock.currentTimeMs()),
    )) as string[][];

    const claimed: StepTask[] = [];
    for (const arr of raw) {
      const t = this.parseRecord(hashArrayToMap(arr));
      if (t) claimed.push(toTask(t));
    }
    return claimed;
  }

  async release(params: { taskId: string; claimToken: string }): Promise<boolean> {
    if (params.claimToken === "") return false;
    const code = await this.requeue({
      id: params.taskId,
      claimToken: params.claimToken,
      mode: "release",
    });
    return code === 1;
  }

  async get(taskId: string): Promise<StepTaskRecord | undefined> {
    const raw = await this.redis.hgetall(this.taskKey(taskId));
    if (!raw || Object.keys(raw).length === 0) return undefined;
    return this.parseRecord(raw);
  }

  async complete(params: StepQueueCompleteParams): Promise<boolean> {
    return this.settle({
      taskId: params.taskId,
      claimToken: params.claimToken,
      status: "completed",
      field: "result",
      value: JSON.stringify(params.result),
      durationMs: params.durationMs,
      extra: [
        "stepMetadata",
        params.stepMetadata !== undefined ? JSON.stringify(params.stepMetadata) : "",
      ],
    });
  }

  async fail(params: StepQueueFailParams): Promise<boolean> {
    return this.settle({
      taskId: params.taskId,
      claimToken: params.claimToken,
      status: "failed",
      field: "error",
      value: params.error,
      durationMs: params.durationMs,
      extra: [
        "errorTag",
        params.errorTag ?? "",
        "stepMetadata",
        params.stepMetadata !== undefined ? JSON.stringify(params.stepMetadata) : "",
      ],
    });
  }

  async heartbeat(params: StepQueueHeartbeatParams): Promise<boolean> {
    const ok = await this.redis.eval(
      HEARTBEAT_LUA,
      1,
      this.taskKey(params.taskId),
      params.claimToken ?? "",
      this.clock.now().toISOString(),
      params.details !== undefined ? "1" : "",
      params.details != null ? JSON.stringify(params.details) : "",
    );
    return ok === 1;
  }

  /**
   * With `lease` and a configured `leaseStore`, the sweep checks the lease
   * up front (so a stale lease rejects even when nothing is stuck) and again
   * inside every per-task requeue script. Tasks are requeued one script at a
   * time, so a takeover mid-sweep stops it there: what was requeued while
   * the lease was current stays requeued, and nothing is written after.
   */
  async requeueStuck(params: StepQueueRequeueParams): Promise<StepQueueRequeueResult> {
    const lease = this.leaseStore ? params.lease : undefined;
    if (lease) await this.assertLeaseCurrent(lease);
    const runningIds = await this.redis.smembers(this.runningKey);
    let requeued = 0;
    let deadLettered = 0;

    for (const id of runningIds) {
      const raw = await this.redis.hgetall(this.taskKey(id));
      if (!raw || raw.status !== "running") continue;

      if (params.mode === "worker") {
        if (raw.claimedBy !== params.workerId) continue;
      } else {
        const lastActivity = raw.heartbeatAt || raw.claimedAt;
        if (!lastActivity) continue;
        const idleMs = this.clock.currentTimeMs() - new Date(lastActivity).getTime();
        if (idleMs <= params.olderThanMs) continue;
      }

      // Token-guarded, so a task completed between the read above and the
      // requeue stays completed.
      const code = await this.requeue({
        id,
        claimToken: raw.claimToken ?? "",
        mode: "requeue",
        lease,
      });
      if (code === -1) {
        // The epoch never goes back, so this re-read throws with the
        // current epoch.
        await this.assertLeaseCurrent(lease!);
        throw new StaleLeaseError({ lease: lease!, currentEpoch: null });
      }
      if (code === 1) requeued++;
      else if (code === 2) deadLettered++;
    }

    return { requeued, deadLettered };
  }

  async throttled(params: { workflowId: string }): Promise<ThrottledTask[]> {
    const flat = (await this.redis.eval(
      THROTTLED_LUA,
      1,
      `${this.base}:grp:wf:${params.workflowId}`,
      this.base,
      String(this.clock.currentTimeMs()),
    )) as string[];
    const out: ThrottledTask[] = [];
    for (let i = 0; i < flat.length; i += 3) {
      const record = await this.get(String(flat[i]));
      if (record === undefined || record.groupName === undefined) continue;
      const retryAt = flat[i + 2];
      out.push({
        taskId: record.id,
        workflowId: record.workflowId,
        run: record.run,
        stepName: record.stepName,
        attempt: record.attempt,
        groupName: record.groupName,
        reason: flat[i + 1] === "rate" ? "rate" : "concurrency",
        since: record.createdAt,
        ...(retryAt ? { retryAt: new Date(Number(retryAt)) } : {}),
      });
    }
    return out.sort((a, b) => Number(a.taskId) - Number(b.taskId));
  }

  async consume(params: { taskId: string }): Promise<boolean> {
    const ok = await this.redis.eval(
      CONSUME_LUA,
      1,
      this.taskKey(params.taskId),
      params.taskId,
      this.base,
      this.clock.now().toISOString(),
    );
    return Number(ok) === 1;
  }

  async consumeSettled(params: {
    workflowId: string;
    stepNames: readonly string[];
  }): Promise<number> {
    let consumed = 0;
    for (const stepName of params.stepNames) {
      const id = await this.redis.get(this.activeKey(params.workflowId, stepName));
      if (id && (await this.consume({ taskId: String(id) }))) consumed++;
    }
    return consumed;
  }

  /**
   * Delete terminal tasks completed before `completedBefore`. Only tasks
   * indexed in `{prefix}:done` — every task settled by this version — are
   * found.
   */
  async purge(params: { completedBefore: Date }): Promise<number> {
    let purged = 0;
    for (;;) {
      const [n, scanned] = (await this.redis.eval(
        PURGE_LUA,
        1,
        this.doneKey,
        this.base,
        String(params.completedBefore.getTime()),
        String(PURGE_BATCH),
      )) as [number, number];
      purged += Number(n);
      if (Number(scanned) < PURGE_BATCH) return purged;
    }
  }

  async metrics(params: { since: Date; until?: Date }): Promise<{
    pending: number;
    running: number;
    completed: number;
    failed: number;
    avgWaitMs: number;
    avgExecMs: number;
    p95ExecMs: number;
  }> {
    // Redis has no native time-range index over task hashes, so we read
    // every task the pending, running and done sets hold and filter
    // in-process. Acceptable for the modest backlog sizes Redis is
    // typically used with (and `purge()` keeps it bounded); for millions of
    // retained rows, use Postgres.
    const sinceMs = params.since.getTime();
    const untilMs = (params.until ?? this.clock.now()).getTime();
    const inWindow = (raw: string | undefined): boolean => {
      if (!raw) return false;
      const t = new Date(raw).getTime();
      return t >= sinceMs && t <= untilMs;
    };

    let pending = 0;
    let running = 0;
    let completed = 0;
    let failed = 0;
    let waitSum = 0;
    let waitN = 0;
    let execSum = 0;
    const execTimes: number[] = [];

    const [pendingIds, runningIds, doneIds] = await Promise.all([
      this.redis.zrangebyscore(this.pendingKey, "-inf", "+inf"),
      this.redis.smembers(this.runningKey),
      this.redis.zrangebyscore(this.doneKey, "-inf", "+inf"),
    ]);
    const ids = [...new Set([...pendingIds, ...runningIds, ...doneIds])];
    const hashes: Array<Record<string, string>> = [];
    for (let i = 0; i < ids.length; i += METRICS_BATCH) {
      const batch = ids.slice(i, i + METRICS_BATCH);
      hashes.push(...(await Promise.all(batch.map((id) => this.redis.hgetall(this.taskKey(id))))));
    }
    for (const h of hashes) {
      if (!h) continue;
      const status = h.status;
      if (status === "pending" && inWindow(h.createdAt)) {
        pending++;
      } else if (status === "running" && inWindow(h.claimedAt)) {
        running++;
      } else if ((status === "completed" || status === "failed") && inWindow(h.completedAt)) {
        if (status === "completed") completed++;
        else failed++;
        if (h.createdAt && h.claimedAt) {
          waitSum += new Date(h.claimedAt).getTime() - new Date(h.createdAt).getTime();
          waitN++;
        }
        if (h.durationMs) {
          const d = Number(h.durationMs);
          if (Number.isFinite(d)) {
            execSum += d;
            execTimes.push(d);
          }
        }
      }
    }

    const terminalN = execTimes.length;
    return {
      pending,
      running,
      completed,
      failed,
      avgWaitMs: waitN > 0 ? waitSum / waitN : 0,
      avgExecMs: terminalN > 0 ? execSum / terminalN : 0,
      p95ExecMs: terminalN > 0 ? percentileCont({ values: execTimes, p: 0.95 }) : 0,
    };
  }

  // -- Internal helpers ------------------------------------------------------

  private async settle(params: {
    taskId: string;
    claimToken: string | undefined;
    status: "completed" | "failed";
    field: "result" | "error";
    value: string;
    durationMs: number;
    /** Extra `field, value` pairs; an empty value is not written. */
    extra: readonly string[];
  }): Promise<boolean> {
    const now = this.clock.now();
    const ok = await this.redis.eval(
      SETTLE_LUA,
      3,
      this.taskKey(params.taskId),
      this.runningKey,
      this.doneKey,
      params.taskId,
      params.claimToken ?? "",
      params.status,
      params.field,
      params.value,
      String(params.durationMs),
      now.toISOString(),
      this.base,
      String(now.getTime()),
      ...params.extra,
    );
    return ok === 1;
  }

  /** Throw `StaleLeaseError` unless the lease's epoch key still holds its epoch. */
  private async assertLeaseCurrent(lease: LeaderLease): Promise<void> {
    const raw = await this.redis.get(this.leaseStore!.keysFor(lease.key).epoch);
    const current = raw === null || raw === undefined ? null : Number(raw);
    if (current !== lease.epoch) throw new StaleLeaseError({ lease, currentEpoch: current });
  }

  /**
   * Run REQUEUE_LUA; returns -1 (stale `lease`), 0 (no-op), 1 (pending
   * again), 2 (dead-lettered).
   */
  private async requeue(params: {
    id: string;
    claimToken: string;
    mode: "requeue" | "release";
    lease?: LeaderLease;
  }): Promise<number> {
    const now = this.clock.now();
    const leaseKeys = params.lease ? [this.leaseStore!.keysFor(params.lease.key).epoch] : [];
    const code = await this.redis.eval(
      REQUEUE_LUA,
      5 + leaseKeys.length,
      this.taskKey(params.id),
      this.runningKey,
      this.pendingKey,
      this.doneKey,
      this.readyKey,
      ...leaseKeys,
      params.id,
      this.base,
      params.claimToken,
      params.mode,
      String(this.maxDeliveries),
      deadLetterError(this.maxDeliveries),
      now.toISOString(),
      String(now.getTime()),
      params.lease ? String(params.lease.epoch) : "",
    );
    return Number(code);
  }

  private parseRecord(map: Record<string, string>): StepTaskRecord | undefined {
    if (!map.id) return undefined;
    // Optional fields are only set when present so a missing value
    // round-trips as `undefined`, not the empty string the Lua scripts use
    // to clear a field.
    const opt = (k: string): string | undefined => (map[k] ? map[k] : undefined);
    const json = (k: string): unknown => (map[k] ? JSON.parse(map[k]!) : undefined);
    const date = (k: string): Date | undefined => (map[k] ? new Date(map[k]!) : undefined);
    const record: { -readonly [K in keyof StepTaskRecord]: StepTaskRecord[K] } = {
      id: map.id,
      workflowId: map.workflowId ?? "",
      stepName: map.stepName ?? "",
      needs: map.needs ? (JSON.parse(map.needs) as string[]) : [],
      priority: parseInt(map.priority ?? "5", 10),
      input: map.input ? JSON.parse(map.input) : {},
      deps: map.deps ? JSON.parse(map.deps) : {},
      dependsOn: map.dependsOn ? (JSON.parse(map.dependsOn) as string[]) : [],
      attempt: parseInt(map.attempt || "1", 10),
      run: parseInt(map.run || "1", 10),
      deliveries: parseInt(map.deliveries || "0", 10),
      status: (map.status ?? "pending") as StepTaskStatus,
      createdAt: new Date(map.createdAt ?? this.clock.currentTimeMs()),
    };
    if (opt("version")) record.version = map.version;
    if (opt("claimToken")) record.claimToken = map.claimToken;
    if (opt("metadata")) record.metadata = json("metadata") as Record<string, unknown>;
    if (opt("concurrencyKey")) record.concurrencyKey = map.concurrencyKey;
    if (opt("concurrencyScope")) record.concurrencyScope = map.concurrencyScope;
    if (opt("concurrencyLimit")) record.concurrencyLimit = Number(map.concurrencyLimit);
    if (opt("timeoutMs")) record.timeoutMs = Number(map.timeoutMs);
    if (opt("groupName")) record.groupName = map.groupName;
    if (opt("groupMaxConcurrent")) record.groupMaxConcurrent = Number(map.groupMaxConcurrent);
    if (opt("groupMaxStarts")) record.groupMaxStarts = Number(map.groupMaxStarts);
    if (opt("groupWindowMs")) record.groupWindowMs = Number(map.groupWindowMs);
    if (opt("fairnessKey")) record.fairnessKey = map.fairnessKey;
    if (opt("claimedBy")) record.claimedBy = map.claimedBy;
    if (opt("claimedAt")) record.claimedAt = date("claimedAt");
    if (opt("heartbeatAt")) record.heartbeatAt = date("heartbeatAt");
    if (opt("heartbeatDetails")) record.lastHeartbeatDetails = json("heartbeatDetails");
    if (opt("completedAt")) record.completedAt = date("completedAt");
    if (opt("result")) record.result = json("result");
    if (opt("error")) record.error = map.error;
    if (opt("errorTag")) record.errorTag = map.errorTag;
    if (opt("stepMetadata")) record.stepMetadata = json("stepMetadata") as Record<string, unknown>;
    if (opt("durationMs")) record.durationMs = Number(map.durationMs);
    if (opt("consumedAt")) record.consumedAt = date("consumedAt");
    return record;
  }
}

function hashArrayToMap(arr: string[]): Record<string, string> {
  const map: Record<string, string> = {};
  for (let i = 0; i < arr.length; i += 2) map[arr[i]!] = arr[i + 1]!;
  return map;
}

/** The claim-facing subset of a record. */
function toTask(r: StepTaskRecord): StepTask {
  const {
    claimedBy: _by,
    claimedAt: _at,
    heartbeatAt: _hb,
    completedAt: _done,
    result: _res,
    error: _err,
    errorTag: _tag,
    stepMetadata: _meta,
    durationMs: _dur,
    consumedAt: _consumed,
    ...task
  } = r;
  return task;
}
