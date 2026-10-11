// ---------------------------------------------------------------------------
// BuilderInspector — edits the selected node: the Input node's JSON Schema
// (with a template inferred from what the root step reads), or a step's
// name, label, wiring and kind-specific fields (activity + config form,
// branch condition and arms, parallel branches, map source, sleep time,
// signal name, timeouts and retries). Every change is reported as the next
// schema; the page owns state.
// ---------------------------------------------------------------------------

import type { ComponentChildren } from "preact";
import { useEffect, useMemo, useState } from "preact/hooks";
import type {
  ActivityDescriptor,
  ActivityRefSchema,
  JsonSchema,
  StepSchema,
  StepSchemaOptions,
  WorkflowSchema,
} from "@promin/workflow";
import {
  INPUT_NODE_ID,
  isPredicate,
  replaceStep,
  stepDependencies,
  wouldCreateCycle,
} from "../../lib/workflow-builder-graph.ts";
import {
  configFields,
  parseJsonInput,
  type ConfigField,
} from "../../lib/workflow-builder-input.ts";
import { schemaTypeLabel, type DraftIssue } from "../../lib/workflow-builder-validation.ts";

export function BuilderInspector(props: {
  schema: WorkflowSchema;
  selected: string | null;
  activities: ReadonlyArray<ActivityDescriptor>;
  issues: ReadonlyArray<DraftIssue>;
  inputLabel: string;
  inputTemplate: JsonSchema;
  onChange: (schema: WorkflowSchema) => void;
  onRename: (params: { from: string; to: string }) => string | undefined;
  onRemove: (name: string) => void;
}) {
  const { schema, selected } = props;
  if (selected === INPUT_NODE_ID) {
    return (
      <Panel title="Input" subtitle={props.inputLabel}>
        <InputSchemaEditor
          value={schema.inputSchema}
          template={props.inputTemplate}
          issues={props.issues.filter((i) => i.input)}
          onChange={(inputSchema) => {
            const { inputSchema: _old, ...rest } = schema;
            props.onChange(inputSchema ? { ...rest, inputSchema } : rest);
          }}
        />
      </Panel>
    );
  }
  const step = schema.steps.find((s) => s.name === selected);
  if (!step) {
    return (
      <Panel title="Inspector">
        <p class="text-xs text-base-content/55">
          Select a node to edit it, or the Input node to declare the workflow's input schema.
        </p>
      </Panel>
    );
  }
  return (
    <StepInspector
      key={step.name}
      {...props}
      step={step}
      issues={props.issues.filter((i) => i.step === step.name && !i.input)}
    />
  );
}

function StepInspector(props: {
  schema: WorkflowSchema;
  step: StepSchema;
  activities: ReadonlyArray<ActivityDescriptor>;
  issues: ReadonlyArray<DraftIssue>;
  onChange: (schema: WorkflowSchema) => void;
  onRename: (params: { from: string; to: string }) => string | undefined;
  onRemove: (name: string) => void;
}) {
  const { schema, step, activities } = props;
  const [name, setName] = useState(step.name);
  const [renameError, setRenameError] = useState<string | undefined>();
  useEffect(() => setName(step.name), [step.name]);
  const update = (next: StepSchema) => props.onChange(replaceStep({ schema, step: next }));
  const label = schema.ui?.[step.name]?.label ?? "";
  const plain = activities.filter((a) => !isPredicate(a));
  const predicates = activities.filter(isPredicate);

  const commitName = () => {
    if (name === step.name) return;
    const error = props.onRename({ from: step.name, to: name });
    setRenameError(error);
    if (error) setName(step.name);
  };

  return (
    <Panel title={step.name} subtitle={step.type}>
      <Field label="Name">
        <input
          class="input input-bordered input-sm font-mono"
          value={name}
          onInput={(e) => setName((e.target as HTMLInputElement).value)}
          onBlur={commitName}
          onKeyDown={(e) => {
            if (e.key === "Enter") commitName();
          }}
        />
        {renameError && <span class="mt-1 text-[11px] text-error">{renameError}</span>}
      </Field>
      <Field label="Label (canvas only)">
        <input
          class="input input-bordered input-sm"
          value={label}
          placeholder={step.name}
          onInput={(e) => {
            const text = (e.target as HTMLInputElement).value;
            const position = schema.ui?.[step.name] ?? { x: 0, y: 0 };
            const { label: _old, ...rest } = position;
            props.onChange({
              ...schema,
              ui: { ...schema.ui, [step.name]: text ? { ...rest, label: text } : rest },
            });
          }}
        />
      </Field>

      {step.type === "map" ? (
        <Field label="Maps over">
          <select
            class="select select-bordered select-sm font-mono"
            value={step.arrayFrom}
            onChange={(e) => update({ ...step, arrayFrom: (e.target as HTMLSelectElement).value })}
          >
            <option value="">— pick a step —</option>
            {schema.steps
              .filter(
                (s) =>
                  s.name !== step.name &&
                  !wouldCreateCycle({ schema, from: s.name, to: step.name }),
              )
              .map((s) => (
                <option key={s.name} value={s.name}>
                  {s.name}
                </option>
              ))}
          </select>
        </Field>
      ) : (
        <Dependencies schema={schema} step={step} onChange={update} />
      )}

      {(step.type === "step" || step.type === "map") && (
        <ActivityEditor
          label="Activity"
          value={{ activityRef: step.activityRef, ...(step.config && { config: step.config }) }}
          options={plain.length > 0 ? plain : activities}
          activities={activities}
          onChange={(ref) => update({ ...step, ...ref, config: ref.config })}
        />
      )}
      {step.type === "map" && (
        <NumberField
          label="Concurrency"
          value={step.options?.concurrency}
          min={1}
          onChange={(concurrency) =>
            update({
              ...step,
              options: withValue({ obj: step.options, key: "concurrency", value: concurrency }),
            })
          }
        />
      )}

      {step.type === "sleep" && (
        <NumberField
          label="Sleep (ms)"
          value={step.ms}
          min={0}
          onChange={(ms) => update({ ...step, ms: ms ?? 0 })}
        />
      )}

      {(step.type === "signal" || step.type === "approval") && (
        <>
          <Field label="Signal name">
            <input
              class="input input-bordered input-sm font-mono"
              value={step.signalName ?? ""}
              placeholder={step.type === "approval" ? `${step.name}-approved` : ""}
              onInput={(e) => {
                const value = (e.target as HTMLInputElement).value;
                if (step.type === "signal") update({ ...step, signalName: value });
                else {
                  const { signalName: _old, ...rest } = step;
                  update(value ? { ...rest, signalName: value } : rest);
                }
              }}
            />
          </Field>
          <NumberField
            label="Timeout (ms, empty = wait forever)"
            value={step.timeoutMs}
            min={0}
            onChange={(timeoutMs) => {
              const { timeoutMs: _old, ...rest } = step;
              update(timeoutMs === undefined ? rest : { ...rest, timeoutMs });
            }}
          />
        </>
      )}

      {step.type === "branch" && (
        <>
          <Field label="Condition (boolean activity)">
            <select
              class="select select-bordered select-sm font-mono"
              value={step.conditionRef}
              onChange={(e) =>
                update({ ...step, conditionRef: (e.target as HTMLSelectElement).value })
              }
            >
              <option value="">— pick a condition —</option>
              {(predicates.length > 0 ? predicates : activities).map((a) => (
                <option key={a.id} value={a.id}>
                  {a.id}
                </option>
              ))}
            </select>
          </Field>
          <ActivityEditor
            label="If true"
            value={step.ifTrue}
            options={plain}
            activities={activities}
            onChange={(ifTrue) => update({ ...step, ifTrue })}
          />
          <ActivityEditor
            label="If false"
            value={step.ifFalse}
            options={plain}
            activities={activities}
            onChange={(ifFalse) => update({ ...step, ifFalse })}
          />
        </>
      )}

      {step.type === "parallel" && (
        <ParallelBranches
          branches={step.branches}
          options={plain}
          activities={activities}
          onChange={(branches) => update({ ...step, branches })}
        />
      )}

      {(step.type === "step" || step.type === "branch" || step.type === "parallel") && (
        <StepOptionsEditor
          value={step.options}
          onChange={(options) => {
            const { options: _old, ...rest } = step;
            update((options ? { ...rest, options } : rest) as StepSchema);
          }}
        />
      )}

      <IssueList issues={props.issues} />
      <button
        type="button"
        class="btn btn-sm btn-error btn-outline w-full"
        onClick={() => props.onRemove(step.name)}
      >
        Delete node
      </button>
    </Panel>
  );
}

function Dependencies(props: {
  schema: WorkflowSchema;
  step: Exclude<StepSchema, { type: "map" }>;
  onChange: (step: StepSchema) => void;
}) {
  const { schema, step } = props;
  const others = schema.steps.filter((s) => s.name !== step.name);
  return (
    <Field label="Waits for">
      {step.dependsOn.length === 0 && (
        <div class="text-[11px] text-success">Reads the workflow input (root step).</div>
      )}
      <div class="max-h-36 space-y-1 overflow-auto rounded border border-base-content/10 p-2">
        {others.length === 0 && <div class="text-[11px] text-base-content/45">No other steps.</div>}
        {others.map((s) => {
          const checked = stepDependencies(step).includes(s.name);
          const cyclic = !checked && wouldCreateCycle({ schema, from: s.name, to: step.name });
          return (
            <label
              key={s.name}
              class={`flex items-center gap-2 text-xs ${cyclic ? "opacity-40" : "cursor-pointer"}`}
              title={cyclic ? "Would create a cycle" : ""}
            >
              <input
                type="checkbox"
                class="checkbox checkbox-xs"
                checked={checked}
                disabled={cyclic}
                onChange={(e) => {
                  const on = (e.target as HTMLInputElement).checked;
                  props.onChange({
                    ...step,
                    dependsOn: on
                      ? [...step.dependsOn, s.name]
                      : step.dependsOn.filter((d) => d !== s.name),
                  });
                }}
              />
              <span class="font-mono">{s.name}</span>
            </label>
          );
        })}
      </div>
    </Field>
  );
}

function ActivityEditor(props: {
  label: string;
  value: ActivityRefSchema;
  options: ReadonlyArray<ActivityDescriptor>;
  activities: ReadonlyArray<ActivityDescriptor>;
  onChange: (value: ActivityRefSchema) => void;
}) {
  const { value } = props;
  const descriptor = props.activities.find((a) => a.id === value.activityRef);
  const known = props.options.some((a) => a.id === value.activityRef);
  return (
    <div class="space-y-2 rounded border border-base-content/10 p-2">
      <Field label={props.label}>
        <select
          class="select select-bordered select-sm font-mono"
          value={value.activityRef}
          onChange={(e) => {
            const id = (e.target as HTMLSelectElement).value;
            const next = props.activities.find((a) => a.id === id);
            props.onChange({
              activityRef: id,
              ...(next?.defaultConfig && { config: { ...next.defaultConfig } }),
            });
          }}
        >
          {!known && <option value={value.activityRef}>{value.activityRef || "— pick —"}</option>}
          {props.options.map((a) => (
            <option key={a.id} value={a.id}>
              {a.id}
            </option>
          ))}
        </select>
        {descriptor?.description && (
          <span class="mt-1 text-[11px] leading-snug text-base-content/50">
            {descriptor.description}
          </span>
        )}
        {descriptor && (
          <span class="mt-1 text-[11px] text-base-content/45">
            {schemaTypeLabel(descriptor.inputSchema)} → {schemaTypeLabel(descriptor.outputSchema)}
          </span>
        )}
      </Field>
      <ConfigEditor
        key={value.activityRef}
        value={value.config ?? {}}
        schema={descriptor?.configSchema}
        onChange={(config) => {
          const { config: _old, ...rest } = value;
          props.onChange(Object.keys(config).length > 0 ? { ...rest, config } : rest);
        }}
      />
    </div>
  );
}

function ConfigEditor(props: {
  value: Record<string, unknown>;
  schema?: JsonSchema;
  onChange: (config: Record<string, unknown>) => void;
}) {
  const fields = useMemo(() => configFields(props.schema), [props.schema]);
  const json = JSON.stringify(props.value, null, 2);
  const [draft, setDraft] = useState(json);
  const [error, setError] = useState<string | undefined>();
  useEffect(() => {
    setDraft(json);
    setError(undefined);
  }, [json]);
  const set = (field: ConfigField, value: unknown) => {
    const { [field.name]: _old, ...rest } = props.value;
    props.onChange(value === undefined || value === "" ? rest : { ...rest, [field.name]: value });
  };
  return (
    <div class="space-y-2">
      {fields.map((field) => (
        <ConfigFieldControl
          key={field.name}
          field={field}
          value={props.value[field.name] ?? field.schema.default}
          onChange={(v) => set(field, v)}
        />
      ))}
      <details class="text-xs">
        <summary class="cursor-pointer text-base-content/55">Config JSON</summary>
        <textarea
          class="textarea textarea-bordered mt-1 w-full min-h-20 font-mono text-[11px]"
          spellcheck={false}
          value={draft}
          onInput={(e) => setDraft((e.target as HTMLTextAreaElement).value)}
        />
        <div class="mt-1 flex items-center justify-between gap-2">
          <span class="text-[11px] text-error">{error ?? ""}</span>
          <button
            type="button"
            class="btn btn-xs btn-outline"
            onClick={() => {
              const parsed = parseJsonInput(draft);
              if (!parsed.ok) return setError(parsed.error);
              const value = parsed.value ?? {};
              if (value === null || typeof value !== "object" || Array.isArray(value)) {
                return setError("Config must be a JSON object.");
              }
              setError(undefined);
              props.onChange(value as Record<string, unknown>);
            }}
          >
            Apply
          </button>
        </div>
      </details>
    </div>
  );
}

function ConfigFieldControl(props: {
  field: ConfigField;
  value: unknown;
  onChange: (value: unknown) => void;
}) {
  const { field, value } = props;
  const label = `${field.name}${field.required ? " *" : ""}`;
  switch (field.control) {
    case "select":
      return (
        <Field label={label} hint={field.schema.description}>
          <select
            class="select select-bordered select-sm font-mono"
            value={JSON.stringify(value ?? null)}
            onChange={(e) => props.onChange(JSON.parse((e.target as HTMLSelectElement).value))}
          >
            <option value="null">— unset —</option>
            {(field.schema.enum ?? []).map((option) => (
              <option key={JSON.stringify(option)} value={JSON.stringify(option)}>
                {String(option)}
              </option>
            ))}
          </select>
        </Field>
      );
    case "toggle":
      return (
        <label class="flex cursor-pointer items-center justify-between gap-3 text-xs">
          <span class="font-mono">{label}</span>
          <input
            type="checkbox"
            class="toggle toggle-sm"
            checked={value === true}
            onChange={(e) => props.onChange((e.target as HTMLInputElement).checked)}
          />
        </label>
      );
    case "number":
      return (
        <NumberField
          label={label}
          value={typeof value === "number" ? value : undefined}
          integer={field.schema.type === "integer"}
          onChange={props.onChange}
        />
      );
    case "text":
      return (
        <Field label={label} hint={field.schema.description}>
          <input
            class="input input-bordered input-sm font-mono"
            value={typeof value === "string" ? value : ""}
            onInput={(e) => props.onChange((e.target as HTMLInputElement).value)}
          />
        </Field>
      );
  }
}

function ParallelBranches(props: {
  branches: Record<string, ActivityRefSchema>;
  options: ReadonlyArray<ActivityDescriptor>;
  activities: ReadonlyArray<ActivityDescriptor>;
  onChange: (branches: Record<string, ActivityRefSchema>) => void;
}) {
  const entries = Object.entries(props.branches);
  const [newName, setNewName] = useState("");
  const add = () => {
    const name = newName.trim();
    if (!name || props.branches[name]) return;
    props.onChange({
      ...props.branches,
      [name]: { activityRef: props.options[0]?.id ?? props.activities[0]?.id ?? "" },
    });
    setNewName("");
  };
  return (
    <div class="space-y-2">
      <div class="text-[10px] uppercase tracking-wider text-base-content/50">Branches</div>
      {entries.map(([name, branch]) => (
        <div key={name} class="space-y-1">
          <div class="flex items-center justify-between">
            <span class="font-mono text-xs">{name}</span>
            <button
              type="button"
              class="btn btn-xs btn-ghost text-error"
              onClick={() => {
                const { [name]: _removed, ...rest } = props.branches;
                props.onChange(rest);
              }}
            >
              Remove
            </button>
          </div>
          <ActivityEditor
            label={`${name} activity`}
            value={branch}
            options={props.options}
            activities={props.activities}
            onChange={(next) => props.onChange({ ...props.branches, [name]: next })}
          />
        </div>
      ))}
      <div class="join w-full">
        <input
          class="input input-bordered input-sm join-item flex-1 font-mono"
          placeholder="branch name"
          value={newName}
          onInput={(e) => setNewName((e.target as HTMLInputElement).value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") add();
          }}
        />
        <button type="button" class="btn btn-sm join-item" onClick={add}>
          Add
        </button>
      </div>
    </div>
  );
}

function StepOptionsEditor(props: {
  value: StepSchemaOptions | undefined;
  onChange: (value: StepSchemaOptions | undefined) => void;
}) {
  const value = props.value ?? {};
  const emit = (next: StepSchemaOptions) => {
    const retry = next.retry && Object.keys(next.retry).length > 0 ? next.retry : undefined;
    const cleaned: StepSchemaOptions = {
      ...(next.timeoutMs !== undefined && { timeoutMs: next.timeoutMs }),
      ...(retry && { retry }),
    };
    props.onChange(Object.keys(cleaned).length > 0 ? cleaned : undefined);
  };
  return (
    <details class="rounded border border-base-content/10 p-2 text-xs">
      <summary class="cursor-pointer text-base-content/60">Timeout and retries</summary>
      <div class="mt-2 space-y-2">
        <NumberField
          label="Timeout (ms)"
          value={value.timeoutMs}
          min={0}
          onChange={(timeoutMs) =>
            emit(withValue({ obj: value, key: "timeoutMs", value: timeoutMs }) ?? {})
          }
        />
        <NumberField
          label="Max retries"
          value={value.retry?.maxRetries}
          min={0}
          onChange={(maxRetries) =>
            emit({
              ...value,
              retry: withValue({ obj: value.retry, key: "maxRetries", value: maxRetries }),
            })
          }
        />
        <NumberField
          label="Retry base delay (ms)"
          value={value.retry?.baseDelayMs}
          min={0}
          onChange={(baseDelayMs) =>
            emit({
              ...value,
              retry: withValue({ obj: value.retry, key: "baseDelayMs", value: baseDelayMs }),
            })
          }
        />
      </div>
    </details>
  );
}

function InputSchemaEditor(props: {
  value: JsonSchema | undefined;
  template: JsonSchema;
  issues: ReadonlyArray<DraftIssue>;
  onChange: (value: JsonSchema | undefined) => void;
}) {
  const text = (schema: JsonSchema | undefined) => (schema ? JSON.stringify(schema, null, 2) : "");
  const [draft, setDraft] = useState(text(props.value));
  const [error, setError] = useState<string | undefined>();
  useEffect(() => {
    setDraft(text(props.value));
    setError(undefined);
  }, [JSON.stringify(props.value)]);
  const apply = () => {
    const parsed = parseJsonInput(draft);
    if (!parsed.ok) return setError(parsed.error);
    const value = parsed.value;
    if (
      value !== undefined &&
      (value === null || typeof value !== "object" || Array.isArray(value))
    ) {
      return setError('A JSON Schema is an object, e.g. { "type": "string" }.');
    }
    setError(undefined);
    props.onChange(value as JsonSchema | undefined);
  };
  return (
    <>
      <p class="text-[11px] leading-snug text-base-content/55">
        {props.value
          ? "Declared input schema. Trigger forms are pre-filled from it."
          : "No schema declared: the input type is inferred from what the root step reads. Declare one to document the input and pre-fill trigger forms."}
      </p>
      <Field label="Workflow input schema (JSON Schema)">
        <textarea
          class="textarea textarea-bordered min-h-48 font-mono text-[11px] leading-relaxed"
          spellcheck={false}
          placeholder={JSON.stringify(props.template, null, 2)}
          value={draft}
          onInput={(e) => setDraft((e.target as HTMLTextAreaElement).value)}
        />
      </Field>
      {error && <div class="text-[11px] text-error">{error}</div>}
      <div class="flex flex-wrap justify-end gap-2">
        <button
          type="button"
          class="btn btn-xs btn-ghost"
          onClick={() => {
            setDraft("");
            props.onChange(undefined);
          }}
        >
          Clear
        </button>
        <button
          type="button"
          class="btn btn-xs btn-outline"
          title="Start from what the root step reads"
          onClick={() => setDraft(JSON.stringify(props.template, null, 2))}
        >
          Use template
        </button>
        <button type="button" class="btn btn-xs btn-primary" onClick={apply}>
          Apply
        </button>
      </div>
      <IssueList issues={props.issues} />
    </>
  );
}

function IssueList(props: { issues: ReadonlyArray<DraftIssue> }) {
  if (props.issues.length === 0) return null;
  return (
    <ul class="space-y-1 rounded border border-base-content/10 bg-base-200/50 p-2 text-[11px]">
      {props.issues.map((issue, i) => (
        <li key={i} class={issue.severity === "error" ? "text-error" : "text-warning"}>
          {issue.message}
        </li>
      ))}
    </ul>
  );
}

function Panel(props: { title: string; subtitle?: string; children: ComponentChildren }) {
  return (
    <div class="card bg-base-100 shadow">
      <div class="border-b border-base-300 px-3 py-2">
        <div class="text-xs uppercase tracking-wider text-base-content/50">Inspector</div>
        <div class="mt-0.5 truncate font-mono text-sm">{props.title}</div>
        {props.subtitle && <div class="text-[11px] text-base-content/50">{props.subtitle}</div>}
      </div>
      <div class="max-h-[70vh] space-y-3 overflow-auto p-3">{props.children}</div>
    </div>
  );
}

function Field(props: { label: string; hint?: string; children: ComponentChildren }) {
  return (
    <label class="form-control">
      <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
        {props.label}
      </span>
      {props.children}
      {props.hint && <span class="mt-1 text-[11px] text-base-content/45">{props.hint}</span>}
    </label>
  );
}

function NumberField(props: {
  label: string;
  value: number | undefined;
  min?: number;
  integer?: boolean;
  onChange: (value: number | undefined) => void;
}) {
  return (
    <Field label={props.label}>
      <input
        type="number"
        class="input input-bordered input-sm font-mono"
        step={props.integer === false ? "any" : "1"}
        min={props.min}
        value={props.value ?? ""}
        onInput={(e) => {
          const raw = (e.target as HTMLInputElement).value.trim();
          const n = Number(raw);
          props.onChange(raw === "" || !Number.isFinite(n) ? undefined : n);
        }}
      />
    </Field>
  );
}

/** `obj` with `key` set to `value`, or removed when `value` is undefined; undefined when empty. */
function withValue<T extends object, K extends keyof T>(params: {
  obj: T | undefined;
  key: K;
  value: T[K] | undefined;
}): T | undefined {
  const next = { ...params.obj } as T;
  if (params.value === undefined) delete next[params.key];
  else next[params.key] = params.value;
  return Object.keys(next).length > 0 ? next : undefined;
}
