// ---------------------------------------------------------------------------
// recipe-memory-form — pure form-state ⇄ recipe mapping for the agent
// edit-drawer's "Memory & compaction" section.
//
// autoCompact / autoDistill are tri-state on the recipe:
//   undefined → inherit the host default
//   false     → explicitly disabled for this recipe
//   object    → recipe-level config
// The form models that as a `mode` of "inherit" | "off" | "on".
//
// Numeric fields are kept as strings so "blank" (inherit / unset) stays
// distinct from "0" — `parsePositiveInt` maps blank → undefined, and the
// builders omit undefined fields rather than writing 0.
//
// The recipe shapes are loose-typed mirrors of @promin/agent's
// AutoCompactRecipe / AutoDistillRecipe / ContextBudgetRecipe — the UI
// stays decoupled from the agent package (same boundary pattern as
// trace-graph.ts).
// ---------------------------------------------------------------------------

export type CompactionMode = "inherit" | "off" | "on";
export type RunMode = "background" | "blocking";

export interface AutoCompactRecipe {
  messageThreshold?: number;
  tokenThreshold?: number;
  contextLimit?: number;
  compressAt?: number;
  keepRecent?: number;
  mode?: RunMode;
}
export interface AutoDistillRecipe {
  messageThreshold?: number;
  tokenThreshold?: number;
  intervalMs?: number;
  force?: boolean;
  mode?: RunMode;
}
export interface ContextBudgetRecipe {
  maxMessageTokens: number;
  maxEpisodeTokens?: number;
}

export interface AutoCompactForm {
  mode: CompactionMode;
  messageThreshold: string;
  tokenThreshold: string;
  contextLimit: string;
  compressAt: string;
  keepRecent: string;
  runMode: RunMode;
}
export interface AutoDistillForm {
  mode: CompactionMode;
  messageThreshold: string;
  tokenThreshold: string;
  intervalMs: string;
  force: boolean;
  runMode: RunMode;
}
export interface ContextBudgetForm {
  maxMessageTokens: string;
  maxEpisodeTokens: string;
}

const numStr = (n: number | undefined): string => (n === undefined ? "" : String(n));

/** Parse a blank-or-positive-integer field. Blank / invalid → undefined. */
export function parsePositiveInt(raw: string): number | undefined {
  const t = raw.trim();
  if (!t) return undefined;
  const n = Number.parseInt(t, 10);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}

// --- autoCompact ---------------------------------------------------------

export function initAutoCompact(v: AutoCompactRecipe | false | undefined): AutoCompactForm {
  const base: AutoCompactForm = {
    mode: "inherit",
    messageThreshold: "",
    tokenThreshold: "",
    contextLimit: "",
    compressAt: "",
    keepRecent: "",
    runMode: "background",
  };
  if (v === undefined) return base;
  if (v === false) return { ...base, mode: "off" };
  return {
    mode: "on",
    messageThreshold: numStr(v.messageThreshold),
    tokenThreshold: numStr(v.tokenThreshold),
    contextLimit: numStr(v.contextLimit),
    compressAt: numStr(v.compressAt),
    keepRecent: numStr(v.keepRecent),
    runMode: v.mode ?? "background",
  };
}

export function buildAutoCompact(f: AutoCompactForm): AutoCompactRecipe | false | undefined {
  if (f.mode === "inherit") return undefined;
  if (f.mode === "off") return false;
  const out: AutoCompactRecipe = { mode: f.runMode };
  const fields = [
    ["messageThreshold", f.messageThreshold],
    ["tokenThreshold", f.tokenThreshold],
    ["contextLimit", f.contextLimit],
    ["compressAt", f.compressAt],
    ["keepRecent", f.keepRecent],
  ] as const;
  for (const [key, raw] of fields) {
    const n = parsePositiveInt(raw);
    if (n !== undefined) out[key] = n;
  }
  return out;
}

// --- autoDistill ---------------------------------------------------------

export function initAutoDistill(v: AutoDistillRecipe | false | undefined): AutoDistillForm {
  const base: AutoDistillForm = {
    mode: "inherit",
    messageThreshold: "",
    tokenThreshold: "",
    intervalMs: "",
    force: false,
    runMode: "background",
  };
  if (v === undefined) return base;
  if (v === false) return { ...base, mode: "off" };
  return {
    mode: "on",
    messageThreshold: numStr(v.messageThreshold),
    tokenThreshold: numStr(v.tokenThreshold),
    intervalMs: numStr(v.intervalMs),
    force: v.force ?? false,
    runMode: v.mode ?? "background",
  };
}

export function buildAutoDistill(f: AutoDistillForm): AutoDistillRecipe | false | undefined {
  if (f.mode === "inherit") return undefined;
  if (f.mode === "off") return false;
  const out: AutoDistillRecipe = { mode: f.runMode };
  const fields = [
    ["messageThreshold", f.messageThreshold],
    ["tokenThreshold", f.tokenThreshold],
    ["intervalMs", f.intervalMs],
  ] as const;
  for (const [key, raw] of fields) {
    const n = parsePositiveInt(raw);
    if (n !== undefined) out[key] = n;
  }
  if (f.force) out.force = true;
  return out;
}

// --- contextBudget -------------------------------------------------------

export function initContextBudget(v: ContextBudgetRecipe | undefined): ContextBudgetForm {
  return {
    maxMessageTokens: numStr(v?.maxMessageTokens),
    maxEpisodeTokens: numStr(v?.maxEpisodeTokens),
  };
}

export function buildContextBudget(f: ContextBudgetForm): ContextBudgetRecipe | undefined {
  // maxMessageTokens is the required field — without it there's no
  // budget to set, so the whole block stays unset (inherit host default).
  const max = parsePositiveInt(f.maxMessageTokens);
  if (max === undefined) return undefined;
  const episode = parsePositiveInt(f.maxEpisodeTokens);
  return {
    maxMessageTokens: max,
    ...(episode !== undefined ? { maxEpisodeTokens: episode } : {}),
  };
}
