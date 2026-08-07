// ---------------------------------------------------------------------------
// AgentVersionsModal — list every persisted version of a recipe and
// render a side-by-side diff between any two. Read-only viewer; publish
// happens from the Edit drawer ("Publish new version…").
//
// Diff rendering:
//   - model: provider/id change shown explicitly above the columns
//   - systemPrompt: line-by-line LCS diff with green/red gutter
//   - tools: set diff (added / removed / unchanged badges)
//   - metadata: shallow field-by-field; arrays compared as sets
//
// LCS is naive O(n*m) — fine for typical system prompts (<a few hundred
// lines). If a deployment writes book-length prompts, swap in a faster
// implementation.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { formatRelative } from "../../lib/format.ts";
import { inlineRoleDefinition } from "../../lib/role.ts";
import type { RegisteredAgent } from "../../../server/routes/agents.ts";

interface Props {
  agentId: string;
  initialVersion?: string;
  onClose: () => void;
  /**
   * When true, render only the picker + diff body (no backdrop, no
   * positioning, no own header). The host owns the chrome — e.g. a tab
   * inside the agent's ⚙ Manage drawer.
   */
  embed?: boolean;
}

export function AgentVersionsModal({ agentId, initialVersion, onClose, embed }: Props) {
  const { data, loading, error } = useFetch(() => api.listAgentVersions(agentId), [agentId], 0);
  const versions = useMemo(() => {
    const list = data?.versions ?? [];
    // Newest-first: registries return ascending; reverse here so the
    // most-recently-published version sits at the top.
    return [...list].sort((a, b) => b.createdAt - a.createdAt);
  }, [data]);

  const [leftVersion, setLeftVersion] = useState<string | null>(null);
  const [rightVersion, setRightVersion] = useState<string | null>(null);

  // Default selection: newest on the right, previous on the left so the
  // diff reads "what changed in the latest publish".
  useEffect(() => {
    if (versions.length === 0 || rightVersion !== null) return;
    const newest = versions[0]!;
    const previous = versions[1] ?? null;
    setRightVersion(initialVersion ?? newest.version);
    setLeftVersion(previous ? previous.version : newest.version);
  }, [versions, initialVersion, rightVersion]);

  useEffect(() => {
    if (embed) return; // host drawer owns close
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose, embed]);

  const left = versions.find((v) => v.version === leftVersion) ?? null;
  const right = versions.find((v) => v.version === rightVersion) ?? null;

  return (
    <>
      {!embed && (
        <div
          class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in"
          onClick={onClose}
          aria-hidden
        />
      )}
      <div
        class={
          embed
            ? "flex flex-col"
            : "fixed inset-x-4 top-8 bottom-8 mx-auto max-w-6xl bg-base-100 rounded-lg shadow-2xl z-40 flex flex-col anim-drawer-in"
        }
        {...(embed ? {} : { role: "dialog", "aria-label": "Agent versions" })}
      >
        {!embed && (
          <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
            <div class="min-w-0">
              <div class="text-xs text-base-content/50 uppercase tracking-wider">Versions</div>
              <div class="font-mono text-sm">{agentId}</div>
              <div class="text-[10px] text-base-content/40 mt-0.5">
                {versions.length} version{versions.length === 1 ? "" : "s"} · pick two to compare
              </div>
            </div>
            <button class="btn btn-sm btn-ghost" onClick={onClose} title="Close (Esc)">
              ✕
            </button>
          </header>
        )}

        {embed && (
          <div class="text-[10px] text-base-content/40 mb-3">
            {versions.length} version{versions.length === 1 ? "" : "s"} · pick two to compare
          </div>
        )}

        {error && <div class="alert alert-error m-4 text-xs">{error.message}</div>}

        {loading && !data ? (
          <div class="p-8 text-center text-base-content/50 text-sm">Loading…</div>
        ) : versions.length === 0 ? (
          <div class="p-8 text-center text-base-content/50 text-sm">No versions found.</div>
        ) : (
          <div class="flex-1 grid grid-cols-2 gap-2 p-4 overflow-hidden">
            <div class="flex flex-col min-h-0">
              <VersionPicker
                label="Left (older)"
                versions={versions}
                value={leftVersion}
                onChange={setLeftVersion}
              />
              {left && <VersionMeta agent={left} />}
            </div>
            <div class="flex flex-col min-h-0">
              <VersionPicker
                label="Right (newer)"
                versions={versions}
                value={rightVersion}
                onChange={setRightVersion}
              />
              {right && <VersionMeta agent={right} />}
            </div>
            {left && right && (
              <div class="col-span-2 overflow-y-auto border-t border-base-300 pt-3 space-y-4">
                <ModelDiff left={left} right={right} />
                <SystemPromptDiff left={left} right={right} />
                <ToolsDiff left={left} right={right} />
                <MetadataDiff left={left} right={right} />
              </div>
            )}
          </div>
        )}
      </div>
    </>
  );
}

function VersionPicker({
  label,
  versions,
  value,
  onChange,
}: {
  label: string;
  versions: RegisteredAgent[];
  value: string | null;
  onChange: (v: string) => void;
}) {
  return (
    <label class="form-control mb-2">
      <span class="text-[10px] text-base-content/50 uppercase tracking-wider mb-1">{label}</span>
      <select
        class="select select-bordered select-sm font-mono"
        value={value ?? ""}
        onChange={(e) => onChange((e.target as HTMLSelectElement).value)}
      >
        {versions.map((v) => (
          <option value={v.version}>
            {v.version} · {formatRelative(new Date(v.createdAt).toISOString())}
          </option>
        ))}
      </select>
    </label>
  );
}

function VersionMeta({ agent }: { agent: RegisteredAgent }) {
  return (
    <div class="text-[10px] text-base-content/50 font-mono space-y-0.5">
      <div>created: {new Date(agent.createdAt).toISOString()}</div>
      <div>updated: {new Date(agent.updatedAt).toISOString()}</div>
    </div>
  );
}

function ModelDiff({ left, right }: { left: RegisteredAgent; right: RegisteredAgent }) {
  if (left.backend.type !== "local" || right.backend.type !== "local") return null;
  const leftKey = `${left.backend.model.provider}::${left.backend.model.id}`;
  const rightKey = `${right.backend.model.provider}::${right.backend.model.id}`;
  const changed = leftKey !== rightKey;
  return (
    <section>
      <h3 class="text-xs uppercase tracking-wider text-base-content/60 mb-1">Model</h3>
      <div class="grid grid-cols-2 gap-2 text-xs font-mono">
        <div class={`p-2 rounded ${changed ? "bg-error/10" : "bg-base-200"}`}>{leftKey}</div>
        <div class={`p-2 rounded ${changed ? "bg-success/10" : "bg-base-200"}`}>{rightKey}</div>
      </div>
    </section>
  );
}

function SystemPromptDiff({ left, right }: { left: RegisteredAgent; right: RegisteredAgent }) {
  // systemPrompt can be plain-string or layered `{ base, layers }` (kx26).
  // Diff the `base` only; layer diffing is future polish.
  const extractPrompt = (a: typeof left): string => {
    if (a.backend.type !== "local") return "";
    const sp = inlineRoleDefinition(a.backend.role)?.systemPrompt ?? null;
    if (sp === null) return "";
    return typeof sp === "string" ? sp : sp.base;
  };
  const leftPrompt = extractPrompt(left);
  const rightPrompt = extractPrompt(right);
  if (leftPrompt === rightPrompt) {
    return (
      <section>
        <h3 class="text-xs uppercase tracking-wider text-base-content/60 mb-1">System prompt</h3>
        <div class="text-[10px] text-base-content/40">unchanged</div>
      </section>
    );
  }
  const lines = lcsLineDiff(leftPrompt, rightPrompt);
  return (
    <section>
      <h3 class="text-xs uppercase tracking-wider text-base-content/60 mb-1">System prompt</h3>
      <div class="grid grid-cols-2 gap-2 text-[11px] font-mono leading-snug">
        <pre class="bg-base-200 rounded p-2 overflow-auto whitespace-pre-wrap">
          {lines.map((d) => (
            <div class={d.kind === "del" ? "bg-error/10 text-error" : ""}>
              {d.kind === "add" ? " " : d.text || " "}
            </div>
          ))}
        </pre>
        <pre class="bg-base-200 rounded p-2 overflow-auto whitespace-pre-wrap">
          {lines.map((d) => (
            <div class={d.kind === "add" ? "bg-success/10 text-success" : ""}>
              {d.kind === "del" ? " " : d.text || " "}
            </div>
          ))}
        </pre>
      </div>
    </section>
  );
}

function ToolsDiff({ left, right }: { left: RegisteredAgent; right: RegisteredAgent }) {
  const leftTools =
    left.backend.type === "local"
      ? new Set(inlineRoleDefinition(left.backend.role)?.tools ?? [])
      : new Set<string>();
  const rightTools =
    right.backend.type === "local"
      ? new Set(inlineRoleDefinition(right.backend.role)?.tools ?? [])
      : new Set<string>();
  const added = [...rightTools].filter((t) => !leftTools.has(t)).sort();
  const removed = [...leftTools].filter((t) => !rightTools.has(t)).sort();
  const unchanged = [...rightTools].filter((t) => leftTools.has(t)).sort();
  if (added.length === 0 && removed.length === 0) {
    return (
      <section>
        <h3 class="text-xs uppercase tracking-wider text-base-content/60 mb-1">Tools</h3>
        <div class="text-[10px] text-base-content/40">unchanged ({unchanged.length})</div>
      </section>
    );
  }
  return (
    <section>
      <h3 class="text-xs uppercase tracking-wider text-base-content/60 mb-1">Tools</h3>
      <div class="flex flex-wrap gap-1">
        {removed.map((t) => (
          <span class="badge badge-sm badge-error gap-1">
            <span>−</span>
            {t}
          </span>
        ))}
        {added.map((t) => (
          <span class="badge badge-sm badge-success gap-1">
            <span>+</span>
            {t}
          </span>
        ))}
        {unchanged.map((t) => (
          <span class="badge badge-sm badge-ghost font-mono opacity-60">{t}</span>
        ))}
      </div>
    </section>
  );
}

function MetadataDiff({ left, right }: { left: RegisteredAgent; right: RegisteredAgent }) {
  const fields: Array<keyof RegisteredAgent["metadata"]> = [
    "description",
    "capabilities",
    "tags",
    "enabled",
  ];
  const rows = fields
    .map((f) => {
      const lv = left.metadata[f];
      const rv = right.metadata[f];
      const changed = JSON.stringify(lv) !== JSON.stringify(rv);
      return { f, lv, rv, changed };
    })
    .filter((r) => r.changed);
  if (rows.length === 0) {
    return (
      <section>
        <h3 class="text-xs uppercase tracking-wider text-base-content/60 mb-1">Metadata</h3>
        <div class="text-[10px] text-base-content/40">unchanged</div>
      </section>
    );
  }
  return (
    <section>
      <h3 class="text-xs uppercase tracking-wider text-base-content/60 mb-1">Metadata</h3>
      <table class="table table-xs">
        <thead>
          <tr class="text-[10px] uppercase tracking-wider text-base-content/50">
            <th>Field</th>
            <th>Left</th>
            <th>Right</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr>
              <td class="font-mono text-xs">{r.f}</td>
              <td class="font-mono text-xs bg-error/5">{formatVal(r.lv)}</td>
              <td class="font-mono text-xs bg-success/5">{formatVal(r.rv)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}

function formatVal(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (Array.isArray(v)) return v.join(", ") || "[]";
  return String(v);
}

// LCS line-diff: returns the merged sequence of
//   { kind: "ctx" | "add" | "del", text }
// where the relative ordering is preserved on both sides. Naive
// O(n*m) — fine for prompt-sized inputs.
export interface DiffLine {
  kind: "ctx" | "add" | "del";
  text: string;
}

export function lcsLineDiff(left: string, right: string): DiffLine[] {
  const a = left.split("\n");
  const b = right.split("\n");
  const n = a.length;
  const m = b.length;
  const dp: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i]![j] = a[i] === b[j] ? dp[i + 1]![j + 1]! + 1 : Math.max(dp[i + 1]![j]!, dp[i]![j + 1]!);
    }
  }
  const out: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) {
      out.push({ kind: "ctx", text: a[i]! });
      i += 1;
      j += 1;
    } else if (dp[i + 1]![j]! >= dp[i]![j + 1]!) {
      out.push({ kind: "del", text: a[i]! });
      i += 1;
    } else {
      out.push({ kind: "add", text: b[j]! });
      j += 1;
    }
  }
  while (i < n) {
    out.push({ kind: "del", text: a[i]! });
    i += 1;
  }
  while (j < m) {
    out.push({ kind: "add", text: b[j]! });
    j += 1;
  }
  return out;
}
