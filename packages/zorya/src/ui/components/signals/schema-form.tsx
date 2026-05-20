// ---------------------------------------------------------------------------
// SchemaForm — render a Preact form from a JSON Schema (the subset the
// `s.*` builder in @promin/workflow produces).
//
// Used in two places:
//   - SignalList's inline Deliver editor (per /signals row)
//   - SharedSignalPage (the public bearer-authed share link target)
//
// Both call sites pass `schema` from the SignalDto / describe response.
// When schema is undefined, the parent falls back to the generic JSON
// textarea editor — this component only handles the cases the builder
// understands. For anything richer (anyOf, mixed types, const), the form
// gracefully falls back to a JSON textarea for that subfield so authors
// don't lose the ability to deliver.
// ---------------------------------------------------------------------------

import { useState } from "preact/hooks";

interface SchemaFormProps {
  /** JSON Schema (loosely typed — we walk it dynamically). */
  readonly schema: unknown;
  /** Controlled value. Use `{}` for object-rooted schemas, undefined otherwise. */
  readonly value: unknown;
  readonly onChange: (next: unknown) => void;
  /** Optional validation error to render at the top. */
  readonly error?: string | null;
}

export function SchemaForm({ schema, value, onChange, error }: SchemaFormProps) {
  return (
    <div class="space-y-3">
      {error && <div class="alert alert-error text-xs">{error}</div>}
      {renderField({ schema, value, onChange, path: "", label: null, required: true })}
    </div>
  );
}

interface RenderArgs {
  schema: unknown;
  value: unknown;
  onChange: (next: unknown) => void;
  path: string;
  label: string | null;
  required: boolean;
}

function renderField(args: RenderArgs): preact.JSX.Element {
  const schema = args.schema as Record<string, unknown> | undefined;
  if (!schema || typeof schema !== "object") return renderJsonFallback(args);

  // Empty schema (s.unknown()) — JSON textarea.
  if (!("type" in schema) && !("anyOf" in schema) && !("const" in schema)) {
    return renderJsonFallback(args);
  }
  if ("anyOf" in schema || "const" in schema) {
    return renderJsonFallback(args);
  }

  const description = typeof schema.description === "string" ? schema.description : undefined;
  const labelText = args.label ?? description ?? args.path;

  switch (schema.type) {
    case "boolean":
      return (
        <label class="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            class="checkbox checkbox-sm"
            checked={args.value === true}
            onInput={(e) => args.onChange((e.target as HTMLInputElement).checked)}
          />
          <span>{labelText}</span>
          {!args.required && <span class="text-[10px] text-base-content/40">(optional)</span>}
        </label>
      );

    case "string": {
      const enumValues = Array.isArray(schema.enum) ? (schema.enum as string[]) : undefined;
      const current = typeof args.value === "string" ? args.value : "";
      return (
        <Field labelText={labelText} description={description} required={args.required}>
          {enumValues ? (
            <select
              class="select select-sm select-bordered w-full"
              value={current}
              onChange={(e) => args.onChange((e.target as HTMLSelectElement).value)}
            >
              <option value="">— choose —</option>
              {enumValues.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          ) : (
            <input
              type="text"
              class="input input-sm input-bordered w-full"
              value={current}
              onInput={(e) =>
                args.onChange(stringFieldNext((e.target as HTMLInputElement).value, args.required))
              }
            />
          )}
        </Field>
      );
    }

    case "number":
    case "integer": {
      const current = typeof args.value === "number" ? String(args.value) : "";
      return (
        <Field labelText={labelText} description={description} required={args.required}>
          <input
            type="number"
            class="input input-sm input-bordered w-full"
            step={schema.type === "integer" ? 1 : "any"}
            value={current}
            onInput={(e) => {
              const raw = (e.target as HTMLInputElement).value;
              if (raw === "") {
                args.onChange(args.required ? 0 : undefined);
                return;
              }
              const n = Number(raw);
              args.onChange(Number.isFinite(n) ? n : raw);
            }}
          />
        </Field>
      );
    }

    case "null":
      args.onChange(null);
      return <div class="text-xs text-base-content/40">null (fixed)</div>;

    case "array":
      return renderArrayField(args, schema);

    case "object":
      return renderObjectField(args, schema);

    default:
      return renderJsonFallback(args);
  }
}

function renderObjectField(args: RenderArgs, schema: Record<string, unknown>): preact.JSX.Element {
  const properties = (schema.properties as Record<string, unknown>) ?? {};
  const requiredArr = Array.isArray(schema.required) ? (schema.required as string[]) : [];
  const current =
    args.value && typeof args.value === "object" && !Array.isArray(args.value)
      ? (args.value as Record<string, unknown>)
      : {};
  const keys = Object.keys(properties);

  const inner = (
    <div class={args.path === "" ? "space-y-2" : "space-y-2 pl-3 border-l border-base-300"}>
      {keys.map((key) => {
        const childSchema = properties[key] as Record<string, unknown>;
        const childDescription =
          typeof childSchema.description === "string" ? childSchema.description : undefined;
        return (
          <div key={key}>
            {renderField({
              schema: childSchema,
              value: current[key],
              onChange: (next: unknown) => {
                const copy = { ...current };
                if (next === undefined) delete copy[key];
                else copy[key] = next;
                args.onChange(copy);
              },
              path: args.path === "" ? key : `${args.path}.${key}`,
              label: childDescription ?? key,
              required: requiredArr.includes(key),
            })}
          </div>
        );
      })}
    </div>
  );

  if (args.path === "") return inner;
  return (
    <Field
      labelText={args.label ?? args.path}
      description={typeof schema.description === "string" ? schema.description : undefined}
      required={args.required}
    >
      {inner}
    </Field>
  );
}

function renderArrayField(args: RenderArgs, schema: Record<string, unknown>): preact.JSX.Element {
  const itemSchema = schema.items;
  const current = Array.isArray(args.value) ? args.value : [];
  return (
    <Field
      labelText={args.label ?? args.path}
      description={typeof schema.description === "string" ? schema.description : undefined}
      required={args.required}
    >
      <div class="space-y-2">
        {current.map((item, i) => (
          <div key={i} class="flex gap-2 items-start">
            <div class="flex-1">
              {renderField({
                schema: itemSchema,
                value: item,
                onChange: (next: unknown) => {
                  const copy = [...current];
                  copy[i] = next;
                  args.onChange(copy);
                },
                path: `${args.path}[${i}]`,
                label: null,
                required: true,
              })}
            </div>
            <button
              type="button"
              class="btn btn-xs btn-ghost text-error"
              onClick={() => {
                const copy = [...current];
                copy.splice(i, 1);
                args.onChange(copy);
              }}
            >
              ✕
            </button>
          </div>
        ))}
        <button
          type="button"
          class="btn btn-xs btn-ghost"
          onClick={() => args.onChange([...current, defaultFor(itemSchema)])}
        >
          + Add item
        </button>
      </div>
    </Field>
  );
}

/**
 * Fallback for shapes the structured renderer doesn't cover (anyOf,
 * const, unknown empty schema, unrecognized types). Renders a JSON
 * textarea so the operator can still deliver something.
 */
function renderJsonFallback(args: RenderArgs): preact.JSX.Element {
  return <JsonFallbackField args={args} />;
}

function JsonFallbackField({ args }: { args: RenderArgs }): preact.JSX.Element {
  const [draft, setDraft] = useState(() =>
    args.value === undefined ? "" : JSON.stringify(args.value, null, 2),
  );
  const [parseError, setParseError] = useState<string | null>(null);

  function commit(raw: string): void {
    if (raw.trim() === "") {
      setParseError(null);
      args.onChange(args.required ? null : undefined);
      return;
    }
    try {
      const parsed: unknown = JSON.parse(raw);
      setParseError(null);
      args.onChange(parsed);
    } catch (err) {
      setParseError(err instanceof Error ? err.message : String(err));
    }
  }

  return (
    <Field
      labelText={args.label ?? args.path}
      description="JSON — schema for this field can't be rendered structurally"
      required={args.required}
    >
      <textarea
        class="textarea textarea-bordered textarea-sm w-full font-mono text-xs"
        rows={3}
        value={draft}
        onInput={(e) => {
          const next = (e.target as HTMLTextAreaElement).value;
          setDraft(next);
          commit(next);
        }}
        placeholder="{}"
      />
      {parseError && <div class="text-xs text-error mt-1">Invalid JSON: {parseError}</div>}
    </Field>
  );
}

function Field(props: {
  labelText: string;
  description?: string;
  required: boolean;
  children: preact.ComponentChildren;
}): preact.JSX.Element {
  return (
    <label class="block space-y-1">
      <span class="text-xs text-base-content/60">
        {props.labelText}
        {!props.required && <span class="text-[10px] text-base-content/40 ml-1">(optional)</span>}
      </span>
      {props.children}
    </label>
  );
}

/**
 * Blank value for "Add item" — picks a sensible empty for the item
 * schema's type. Strings default to "", numbers to 0, booleans to false,
 * objects to {}, arrays to []. Unknown shapes default to null.
 */
function defaultFor(schema: unknown): unknown {
  const s = schema as Record<string, unknown> | undefined;
  if (!s || typeof s !== "object") return null;
  switch (s.type) {
    case "string":
      return "";
    case "number":
    case "integer":
      return 0;
    case "boolean":
      return false;
    case "null":
      return null;
    case "array":
      return [];
    case "object":
      return {};
    default:
      return null;
  }
}

/**
 * String-field onChange handler — required strings always emit a string
 * (blank → ""), optional ones drop to undefined on blank so the field
 * doesn't show up in the delivered payload at all.
 */
function stringFieldNext(raw: string, required: boolean): string | undefined {
  if (raw === "" && !required) return undefined;
  return raw;
}
