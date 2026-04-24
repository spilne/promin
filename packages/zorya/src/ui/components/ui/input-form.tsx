// ---------------------------------------------------------------------------
// InputForm — renders form fields from a sample object so users can tweak
// a workflow input without hand-writing JSON. Walks the sample one level
// deep; nested objects / arrays fall back to a JSON textarea for that key.
// ---------------------------------------------------------------------------

import { useMemo, useState } from "preact/hooks";

type FieldKind = "string" | "number" | "boolean" | "json";

interface FieldDef {
  key: string;
  kind: FieldKind;
  /** Initial stringified value (for JSON fields) or raw primitive (otherwise). */
  initial: string;
}

interface InputFormProps {
  sample: Record<string, unknown>;
  onChange: (value: Record<string, unknown>, error: string | undefined) => void;
}

function fieldsFromSample(sample: Record<string, unknown>): FieldDef[] {
  return Object.entries(sample).map(([key, value]) => {
    if (typeof value === "string") return { key, kind: "string", initial: value };
    if (typeof value === "number") return { key, kind: "number", initial: String(value) };
    if (typeof value === "boolean") return { key, kind: "boolean", initial: String(value) };
    // Arrays, nested objects, null → JSON textarea
    return { key, kind: "json", initial: JSON.stringify(value, null, 2) };
  });
}

function parseField(
  kind: FieldKind,
  raw: string,
): { ok: true; value: unknown } | { ok: false; error: string } {
  if (kind === "string") return { ok: true, value: raw };
  if (kind === "number") {
    if (raw.trim() === "") return { ok: true, value: undefined };
    const n = Number(raw);
    if (!Number.isFinite(n)) return { ok: false, error: "not a number" };
    return { ok: true, value: n };
  }
  if (kind === "boolean") return { ok: true, value: raw === "true" };
  if (kind === "json") {
    if (raw.trim() === "") return { ok: true, value: undefined };
    try {
      return { ok: true, value: JSON.parse(raw) };
    } catch {
      return { ok: false, error: "invalid JSON" };
    }
  }
  return { ok: false, error: "unknown field kind" };
}

export function InputForm({ sample, onChange }: InputFormProps) {
  const fields = useMemo(() => fieldsFromSample(sample), [sample]);
  const [values, setValues] = useState<Record<string, string>>(() =>
    Object.fromEntries(fields.map((f) => [f.key, f.initial])),
  );

  // Recompute + propagate whenever any value changes.
  const handle = (key: string, raw: string) => {
    const next = { ...values, [key]: raw };
    setValues(next);
    const out: Record<string, unknown> = {};
    let error: string | undefined;
    for (const f of fields) {
      const p = parseField(f.kind, next[f.key]!);
      if (!p.ok) {
        error = error ?? `${f.key}: ${p.error}`;
        continue;
      }
      if (p.value !== undefined) out[f.key] = p.value;
    }
    onChange(out, error);
  };

  return (
    <div class="space-y-2">
      {fields.map((f) => (
        <label class="form-control">
          <div class="label pb-0.5">
            <span class="label-text text-sm font-mono">{f.key}</span>
            <span class="label-text-alt text-xs text-base-content/40">{f.kind}</span>
          </div>
          {renderField(f, values[f.key] ?? "", (v) => handle(f.key, v))}
        </label>
      ))}
    </div>
  );
}

function renderField(f: FieldDef, value: string, onInput: (v: string) => void) {
  if (f.kind === "boolean") {
    return (
      <select
        class="select select-bordered select-sm w-full"
        value={value}
        onChange={(e) => onInput((e.target as HTMLSelectElement).value)}
      >
        <option value="true">true</option>
        <option value="false">false</option>
      </select>
    );
  }
  if (f.kind === "json") {
    return (
      <textarea
        class="textarea textarea-bordered textarea-sm w-full font-mono"
        rows={4}
        value={value}
        onInput={(e) => onInput((e.target as HTMLTextAreaElement).value)}
      />
    );
  }
  if (f.kind === "number") {
    return (
      <input
        type="number"
        class="input input-bordered input-sm w-full font-mono"
        value={value}
        onInput={(e) => onInput((e.target as HTMLInputElement).value)}
      />
    );
  }
  return (
    <input
      type="text"
      class="input input-bordered input-sm w-full"
      value={value}
      onInput={(e) => onInput((e.target as HTMLInputElement).value)}
    />
  );
}
