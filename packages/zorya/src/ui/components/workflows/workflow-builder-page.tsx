import type { JsonSchema, WorkflowSchema } from "@promin/workflow";
import { useEffect, useMemo, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import type {
  AuthoredWorkflowDto,
  WorkflowStepCatalogResponse,
} from "../../../server/routes/workflow-builder.ts";
import { EmptyState } from "../ui/empty-state.tsx";
import { Page, PageHeader } from "../ui/page.tsx";

type CatalogStep = WorkflowStepCatalogResponse["steps"][number];
type BuilderStep = WorkflowSchema["steps"][number];
type EditableStep = Extract<BuilderStep, { activityRef: string; dependsOn: readonly string[] }>;
type BuilderMode = "canvas" | "json";

const SAMPLE_SCHEMA: WorkflowSchema = {
  version: 1,
  name: "authored-uppercase",
  steps: [
    {
      type: "step",
      name: "upper",
      dependsOn: [],
      activityRef: "transform.uppercase",
    },
  ],
  ui: {
    upper: { x: 80, y: 80, label: "Uppercase" },
  },
};

interface WorkflowBuilderPageProps {
  onOpenWorkflow: (name: string) => void;
  onOpenRun: (id: string) => void;
}

export function WorkflowBuilderPage({ onOpenWorkflow, onOpenRun }: WorkflowBuilderPageProps) {
  const [namespace] = useNamespace();
  const {
    data: catalog,
    loading: catalogLoading,
    error: catalogError,
    refresh: refreshCatalog,
  } = useFetch<WorkflowStepCatalogResponse>(() => api.listWorkflowStepCatalog(), [], 0);
  const {
    data: authored,
    loading: authoredLoading,
    error: authoredError,
    refresh: refreshAuthored,
  } = useFetch(() => api.listAuthoredWorkflows(), [], 0);
  const [schema, setSchema] = useState<WorkflowSchema>(() => structuredClone(SAMPLE_SCHEMA));
  const [schemaJson, setSchemaJson] = useState(() => JSON.stringify(SAMPLE_SCHEMA, null, 2));
  const [version, setVersion] = useState("v1");
  const [selectedStep, setSelectedStep] = useState("upper");
  const [mode, setMode] = useState<BuilderMode>("canvas");
  const [selected, setSelected] = useState<AuthoredWorkflowDto | null>(null);
  const [busy, setBusy] = useState<"save" | "publish" | "run" | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [testInputJson, setTestInputJson] = useState("{}");

  const steps = catalog?.steps ?? [];
  const workflows = authored?.workflows ?? [];
  const configured = catalogLoading || steps.length > 0 || workflows.length > 0;
  const stepById = useMemo(() => new Map(steps.map((step) => [step.id, step])), [steps]);
  const issues = useMemo(() => validateDraft(schema, stepById), [schema, stepById]);
  const activeStep = schema.steps.find((step) => step.name === selectedStep);

  function updateSchema(
    next: WorkflowSchema,
    options: { preserveSelectedWorkflow?: boolean } = {},
  ): void {
    setSchema(next);
    setSchemaJson(JSON.stringify(next, null, 2));
    setError(null);
    setMessage(null);
    if (!options.preserveSelectedWorkflow) setSelected(null);
    if (!next.steps.some((step) => step.name === selectedStep)) {
      setSelectedStep(next.steps[0]?.name ?? "");
    }
  }

  async function save(): Promise<AuthoredWorkflowDto | null> {
    setBusy("save");
    setError(null);
    setMessage(null);
    try {
      const res = await api.saveAuthoredWorkflow({ schema, version: version.trim() || "v1" });
      setSelected(res.workflow);
      setMessage(`Saved ${res.workflow.name}@${res.workflow.version}`);
      refreshAuthored();
      return res.workflow;
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return null;
    } finally {
      setBusy(null);
    }
  }

  async function publish(): Promise<AuthoredWorkflowDto | null> {
    setBusy("publish");
    setError(null);
    setMessage(null);
    try {
      const saved = await api.saveAuthoredWorkflow({ schema, version: version.trim() || "v1" });
      const res = await api.publishAuthoredWorkflow(saved.workflow.name, {
        version: saved.workflow.version,
        promote: true,
      });
      setSelected(res.workflow);
      setMessage(`Published ${res.workflow.name}@${res.workflow.version}`);
      refreshAuthored();
      return res.workflow;
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      return null;
    } finally {
      setBusy(null);
    }
  }

  async function testRun(): Promise<void> {
    setBusy("run");
    setError(null);
    setMessage(null);
    try {
      const published = selected?.status === "published" ? selected : await publish();
      if (!published) return;
      const input = testInputJson.trim() ? JSON.parse(testInputJson) : {};
      const res = await api.triggerWorkflow(published.name, {
        input,
        version: published.version,
        ...(namespace ? { namespace } : {}),
      });
      setMessage(`Triggered test run ${res.workflowId}`);
      onOpenRun(res.workflowId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  function loadWorkflow(workflow: AuthoredWorkflowDto): void {
    setVersion(workflow.version);
    updateSchema(structuredClone(workflow.schema), { preserveSelectedWorkflow: true });
    setSelected(workflow);
    setSelectedStep(workflow.schema.steps[0]?.name ?? "");
  }

  function addStep(entry: CatalogStep): void {
    const baseName = toStepName(entry.id);
    const name = uniqueStepName(schema, baseName);
    const prev = schema.steps.at(-1)?.name;
    const nextStep: BuilderStep = {
      type: entry.kind ?? "step",
      name,
      dependsOn: prev ? [prev] : [],
      activityRef: entry.id,
      ...(entry.defaultConfig ? { config: entry.defaultConfig } : {}),
    } as BuilderStep;
    updateSchema({
      ...schema,
      steps: [...schema.steps, nextStep],
      ui: {
        ...(schema.ui ?? {}),
        [name]: { x: 80 + schema.steps.length * 180, y: 120, label: entry.title },
      },
    });
    setSelectedStep(name);
  }

  function updateWorkflowName(name: string): void {
    updateSchema({ ...schema, name });
  }

  function updateStep(name: string, patch: Partial<EditableStep> & { name?: string }): void {
    const old = schema.steps.find((step) => step.name === name);
    if (!old || !isEditableStep(old)) return;
    const nextName = patch.name?.trim() || old.name;
    const renamed = nextName !== old.name;
    const steps = schema.steps.map((step) => {
      if (step.name === old.name) return { ...old, ...patch, name: nextName } as BuilderStep;
      if (!renamed || !("dependsOn" in step)) return step;
      return {
        ...step,
        dependsOn: step.dependsOn.map((dep) => (dep === old.name ? nextName : dep)),
      } as BuilderStep;
    });
    const ui = { ...(schema.ui ?? {}) };
    if (renamed) {
      ui[nextName] = ui[old.name] ?? { x: 80, y: 120 };
      delete ui[old.name];
      setSelectedStep(nextName);
    }
    updateSchema({ ...schema, steps, ui });
  }

  function removeStep(name: string): void {
    const steps = schema.steps
      .filter((step) => step.name !== name)
      .map((step) =>
        "dependsOn" in step
          ? ({ ...step, dependsOn: step.dependsOn.filter((d) => d !== name) } as BuilderStep)
          : step,
      );
    const ui = { ...(schema.ui ?? {}) };
    delete ui[name];
    updateSchema({ ...schema, steps, ui });
  }

  function applyJson(): void {
    try {
      const parsed = JSON.parse(schemaJson) as WorkflowSchema;
      updateSchema(parsed);
      setSelectedStep(parsed.steps[0]?.name ?? "");
      setMode("canvas");
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <Page>
      <PageHeader
        title="Workflow Builder"
        eyebrow="Step catalog"
        description="Compose authored workflow definitions from reusable step templates, validate the wiring, publish a version, and test-run it."
        meta={
          <span>
            <span class="font-mono">{steps.length}</span> steps ·{" "}
            <span class="font-mono">{workflows.length}</span> authored
          </span>
        }
        actions={
          <button
            class="btn btn-sm btn-ghost gap-1"
            onClick={() => {
              refreshCatalog();
              refreshAuthored();
            }}
          >
            <span aria-hidden="true">↻</span>
            Refresh
          </button>
        }
      />

      {(catalogError || authoredError) && (
        <div class="alert alert-error text-sm">
          {catalogError?.message ?? authoredError?.message}
        </div>
      )}

      {!configured && (
        <div class="card bg-base-100 border border-base-content/10 p-4">
          <EmptyState
            message="Workflow builder is not configured."
            hint="Attach a ZoryaWorkflowBuilder with a step catalog to enable authored workflow publishing."
          />
        </div>
      )}

      <div class="grid grid-cols-1 gap-4 xl:grid-cols-[340px_1fr_340px]">
        <StepCatalogPanel loading={catalogLoading && !catalog} steps={steps} onAdd={addStep} />

        <section class="card bg-base-100/95 border border-base-content/10 shadow overflow-hidden">
          <div class="flex flex-col gap-3 border-b border-base-content/10 p-4 lg:flex-row lg:items-end lg:justify-between">
            <div class="grid flex-1 grid-cols-1 gap-2 md:grid-cols-[1fr_10rem]">
              <label class="form-control">
                <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
                  Workflow
                </span>
                <input
                  class="input input-bordered input-sm font-mono"
                  value={schema.name}
                  onInput={(e) => updateWorkflowName((e.target as HTMLInputElement).value)}
                />
              </label>
              <label class="form-control">
                <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
                  Version
                </span>
                <input
                  class="input input-bordered input-sm font-mono"
                  value={version}
                  onInput={(e) => setVersion((e.target as HTMLInputElement).value)}
                />
              </label>
            </div>
            <div class="join">
              <button
                class={`btn join-item btn-sm ${mode === "canvas" ? "btn-primary" : "btn-ghost"}`}
                onClick={() => setMode("canvas")}
              >
                Canvas
              </button>
              <button
                class={`btn join-item btn-sm ${mode === "json" ? "btn-primary" : "btn-ghost"}`}
                onClick={() => setMode("json")}
              >
                JSON
              </button>
            </div>
          </div>

          {mode === "canvas" ? (
            <div class="grid grid-cols-1 gap-0 lg:grid-cols-[1fr_280px]">
              <div class="min-h-[34rem] border-b border-base-content/10 bg-base-200/45 p-4 lg:border-b-0 lg:border-r">
                <WorkflowCanvas
                  schema={schema}
                  stepById={stepById}
                  selected={selectedStep}
                  onSelect={setSelectedStep}
                />
              </div>
              <StepInspector
                step={activeStep}
                schema={schema}
                catalog={steps}
                issues={issues}
                onUpdate={updateStep}
                onRemove={removeStep}
              />
            </div>
          ) : (
            <div class="space-y-3 p-4">
              <textarea
                class="textarea textarea-bordered min-h-[34rem] w-full resize-y font-mono text-xs leading-relaxed"
                spellcheck={false}
                value={schemaJson}
                onInput={(e) => {
                  setSchemaJson((e.target as HTMLTextAreaElement).value);
                  setError(null);
                  setMessage(null);
                }}
              />
              <div class="flex justify-end">
                <button class="btn btn-sm btn-outline" onClick={applyJson}>
                  Apply JSON
                </button>
              </div>
            </div>
          )}

          <div class="border-t border-base-content/10 p-4">
            {issues.length > 0 && (
              <div class="mb-3 alert alert-warning text-xs">
                <div>
                  {issues.map((issue) => (
                    <div>{issue}</div>
                  ))}
                </div>
              </div>
            )}
            {error && <div class="mb-3 alert alert-error text-xs">{error}</div>}
            {message && <div class="mb-3 alert alert-success text-xs">{message}</div>}
            <label class="form-control mb-3">
              <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
                Test Input JSON
              </span>
              <textarea
                class="textarea textarea-bordered min-h-20 font-mono text-xs"
                value={testInputJson}
                onInput={(e) => setTestInputJson((e.target as HTMLTextAreaElement).value)}
              />
            </label>
            <div class="flex flex-wrap justify-end gap-2">
              <button
                class="btn btn-sm btn-ghost"
                onClick={() => {
                  setSchema(structuredClone(SAMPLE_SCHEMA));
                  setSchemaJson(JSON.stringify(SAMPLE_SCHEMA, null, 2));
                  setVersion("v1");
                  setSelected(null);
                  setSelectedStep("upper");
                  setError(null);
                  setMessage(null);
                }}
              >
                New
              </button>
              <button class="btn btn-sm btn-outline" disabled={busy !== null} onClick={save}>
                {busy === "save" ? "Saving..." : "Save Draft"}
              </button>
              <button class="btn btn-sm btn-primary" disabled={busy !== null} onClick={publish}>
                {busy === "publish" ? "Publishing..." : "Publish"}
              </button>
              <button class="btn btn-sm btn-secondary" disabled={busy !== null} onClick={testRun}>
                {busy === "run" ? "Running..." : "Test Run"}
              </button>
            </div>
          </div>
        </section>

        <AuthoredPanel
          workflows={workflows}
          loading={authoredLoading && !authored}
          selected={selected}
          busy={busy !== null}
          onLoad={loadWorkflow}
          onOpenWorkflow={onOpenWorkflow}
          onPublish={async (workflow) => {
            loadWorkflow(workflow);
            setBusy("publish");
            setError(null);
            try {
              const res = await api.publishAuthoredWorkflow(workflow.name, {
                version: workflow.version,
                promote: true,
              });
              setSelected(res.workflow);
              setMessage(`Published ${res.workflow.name}@${res.workflow.version}`);
              refreshAuthored();
            } catch (e) {
              setError(e instanceof Error ? e.message : String(e));
            } finally {
              setBusy(null);
            }
          }}
        />
      </div>
    </Page>
  );
}

function StepCatalogPanel({
  loading,
  steps,
  onAdd,
}: {
  loading: boolean;
  steps: CatalogStep[];
  onAdd: (step: CatalogStep) => void;
}) {
  return (
    <section class="card bg-base-100/95 border border-base-content/10 shadow overflow-hidden">
      <div class="border-b border-base-content/10 px-4 py-3">
        <div class="text-xs uppercase tracking-wider text-base-content/55">Step Library</div>
      </div>
      <div class="divide-y divide-base-300">
        {loading ? (
          <div class="p-4 text-sm text-base-content/45">Loading...</div>
        ) : steps.length === 0 ? (
          <div class="p-4">
            <EmptyState message="No step templates registered." />
          </div>
        ) : (
          steps.map((step) => (
            <button
              class="w-full p-4 text-left hover:bg-base-200"
              onClick={() => onAdd(step)}
              title="Add this step to the workflow"
            >
              <div class="flex items-start justify-between gap-2">
                <div class="min-w-0">
                  <div class="font-medium leading-tight">{step.title}</div>
                  <div class="mt-1 font-mono text-[11px] text-base-content/45 truncate">
                    {step.id}
                  </div>
                </div>
                <span class="badge badge-sm badge-outline">
                  {step.category ?? step.kind ?? "step"}
                </span>
              </div>
              {step.description && (
                <div class="mt-2 text-xs leading-relaxed text-base-content/60">
                  {step.description}
                </div>
              )}
            </button>
          ))
        )}
      </div>
    </section>
  );
}

function WorkflowCanvas({
  schema,
  stepById,
  selected,
  onSelect,
}: {
  schema: WorkflowSchema;
  stepById: Map<string, CatalogStep>;
  selected: string;
  onSelect: (name: string) => void;
}) {
  const nodes = schema.steps.map((step, index) => {
    const pos = schema.ui?.[step.name] ?? { x: 80 + index * 180, y: 120 };
    return { step, x: pos.x, y: pos.y };
  });
  const bounds = nodes.reduce(
    (acc, node) => ({
      width: Math.max(acc.width, node.x + 180),
      height: Math.max(acc.height, node.y + 120),
    }),
    { width: 720, height: 360 },
  );

  return (
    <svg
      class="h-[34rem] w-full rounded border border-base-content/10 bg-base-100"
      viewBox={`0 0 ${bounds.width} ${bounds.height}`}
      role="img"
    >
      <defs>
        <marker id="wf-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto">
          <path d="M0,0 L8,4 L0,8 Z" class="fill-base-content/35" />
        </marker>
      </defs>
      {nodes.flatMap((node) =>
        "dependsOn" in node.step
          ? node.step.dependsOn.map((dep) => {
              const from = nodes.find((candidate) => candidate.step.name === dep);
              if (!from) return null;
              return (
                <line
                  x1={from.x + 148}
                  y1={from.y + 38}
                  x2={node.x}
                  y2={node.y + 38}
                  stroke="currentColor"
                  class="text-base-content/30"
                  stroke-width="2"
                  marker-end="url(#wf-arrow)"
                />
              );
            })
          : [],
      )}
      {nodes.map((node) => {
        const entry = "activityRef" in node.step ? stepById.get(node.step.activityRef) : undefined;
        const active = selected === node.step.name;
        return (
          <g
            class="cursor-pointer"
            transform={`translate(${node.x}, ${node.y})`}
            onClick={() => onSelect(node.step.name)}
          >
            <rect
              width="148"
              height="76"
              rx="6"
              class={
                active ? "fill-primary/15 stroke-primary" : "fill-base-100 stroke-base-content/20"
              }
              stroke-width={active ? 2 : 1}
            />
            <text x="12" y="24" class="fill-current text-[12px] font-semibold">
              {node.step.name}
            </text>
            <text x="12" y="44" class="fill-current text-[10px] opacity-60">
              {entry?.title ??
                ("activityRef" in node.step ? node.step.activityRef : node.step.type)}
            </text>
            <text x="12" y="62" class="fill-current text-[9px] opacity-45">
              {node.step.type}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

function StepInspector({
  step,
  schema,
  catalog,
  issues,
  onUpdate,
  onRemove,
}: {
  step: BuilderStep | undefined;
  schema: WorkflowSchema;
  catalog: CatalogStep[];
  issues: string[];
  onUpdate: (name: string, patch: Partial<EditableStep> & { name?: string }) => void;
  onRemove: (name: string) => void;
}) {
  if (!step) {
    return <div class="p-4 text-sm text-base-content/45">Select a node.</div>;
  }
  const editable = isEditableStep(step);
  const catalogEntry = editable
    ? catalog.find((entry) => entry.id === step.activityRef)
    : undefined;
  return (
    <aside class="space-y-3 p-4">
      <div>
        <div class="text-xs uppercase tracking-wider text-base-content/55">Node</div>
        <div class="mt-1 font-mono text-sm">{step.name}</div>
      </div>
      {editable ? (
        <>
          <label class="form-control">
            <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">Name</span>
            <input
              class="input input-bordered input-sm font-mono"
              value={step.name}
              onInput={(e) => onUpdate(step.name, { name: (e.target as HTMLInputElement).value })}
            />
          </label>
          <label class="form-control">
            <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
              Activity
            </span>
            <select
              class="select select-bordered select-sm font-mono"
              value={step.activityRef}
              onChange={(e) =>
                onUpdate(step.name, {
                  activityRef: (e.target as HTMLSelectElement).value,
                })
              }
            >
              {catalog.map((entry) => (
                <option value={entry.id}>{entry.id}</option>
              ))}
            </select>
          </label>
          <div>
            <div class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
              Dependencies
            </div>
            <div class="max-h-36 space-y-1 overflow-auto rounded border border-base-content/10 p-2">
              {schema.steps
                .filter((candidate) => candidate.name !== step.name)
                .map((candidate) => (
                  <label class="flex cursor-pointer items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      class="checkbox checkbox-xs"
                      checked={step.dependsOn.includes(candidate.name)}
                      onChange={(e) => {
                        const checked = (e.target as HTMLInputElement).checked;
                        const next = checked
                          ? [...step.dependsOn, candidate.name]
                          : step.dependsOn.filter((dep) => dep !== candidate.name);
                        onUpdate(step.name, { dependsOn: next });
                      }}
                    />
                    <span class="font-mono">{candidate.name}</span>
                  </label>
                ))}
            </div>
          </div>
          <ConfigEditor
            stepName={step.name}
            value={step.config ?? {}}
            schema={catalogEntry?.configSchema}
            onApply={(config) => onUpdate(step.name, { config })}
          />
          <button
            class="btn btn-sm btn-error btn-outline w-full"
            onClick={() => onRemove(step.name)}
          >
            Delete Node
          </button>
        </>
      ) : (
        <div class="text-xs text-base-content/55">
          This step kind can be edited from JSON until the canvas supports its full control surface.
        </div>
      )}
      <div class="rounded border border-base-content/10 bg-base-200/50 p-3">
        <div class="mb-2 text-[10px] uppercase tracking-wider text-base-content/50">Validation</div>
        {issues.length === 0 ? (
          <div class="text-xs text-success">Ready to save and publish.</div>
        ) : (
          <ul class="space-y-1 text-xs text-warning">
            {issues.map((issue) => (
              <li>{issue}</li>
            ))}
          </ul>
        )}
      </div>
    </aside>
  );
}

function ConfigEditor({
  stepName,
  value,
  schema,
  onApply,
}: {
  stepName: string;
  value: Record<string, unknown>;
  schema?: JsonSchema;
  onApply: (config: Record<string, unknown>) => void;
}) {
  const valueJson = JSON.stringify(value, null, 2);
  const [draft, setDraft] = useState(() => JSON.stringify(value, null, 2));
  const [error, setError] = useState<string | null>(null);
  const fields = useMemo(() => configFields(schema), [schema]);

  useEffect(() => {
    setDraft(valueJson);
    setError(null);
  }, [stepName, valueJson]);

  function setField(name: string, next: unknown): void {
    onApply({ ...value, [name]: next });
  }

  return (
    <div class="space-y-3">
      {fields.length > 0 && (
        <div class="space-y-2 rounded border border-base-content/10 p-3">
          <div class="text-[10px] uppercase tracking-wider text-base-content/50">Config Fields</div>
          {fields.map((field) => (
            <ConfigField
              field={field}
              value={value[field.name] ?? field.defaultValue}
              onChange={(next) => setField(field.name, next)}
            />
          ))}
        </div>
      )}
      <label class="form-control">
        <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
          Config JSON
        </span>
        <textarea
          class="textarea textarea-bordered min-h-28 font-mono text-xs"
          value={draft}
          onInput={(e) => {
            setDraft((e.target as HTMLTextAreaElement).value);
            setError(null);
          }}
        />
        <div class="mt-2 flex items-center justify-between gap-2">
          <span class="text-[11px] text-error">{error ?? ""}</span>
          <button
            type="button"
            class="btn btn-xs btn-outline"
            onClick={() => {
              try {
                const parsed = JSON.parse(draft || "{}");
                if (parsed === null || Array.isArray(parsed) || typeof parsed !== "object") {
                  setError("Config must be an object.");
                  return;
                }
                onApply(parsed as Record<string, unknown>);
                setError(null);
              } catch (e) {
                setError(e instanceof Error ? e.message : String(e));
              }
            }}
          >
            Apply
          </button>
        </div>
      </label>
    </div>
  );
}

interface ConfigFieldModel {
  readonly name: string;
  readonly schema: JsonSchema;
  readonly required: boolean;
  readonly defaultValue: unknown;
}

function ConfigField({
  field,
  value,
  onChange,
}: {
  field: ConfigFieldModel;
  value: unknown;
  onChange: (value: unknown) => void;
}) {
  const label = (
    <span class="mb-1 flex items-center justify-between gap-2 text-[10px] uppercase tracking-wider text-base-content/50">
      <span>{field.name}</span>
      {field.required && <span class="text-error">*</span>}
    </span>
  );
  if (field.schema.enum?.length) {
    return (
      <label class="form-control">
        {label}
        <select
          class="select select-bordered select-sm font-mono"
          value={String(value ?? "")}
          onChange={(e) => onChange((e.target as HTMLSelectElement).value)}
        >
          <option value="">Unset</option>
          {field.schema.enum.map((option) => (
            <option value={String(option)}>{String(option)}</option>
          ))}
        </select>
      </label>
    );
  }
  if (field.schema.type === "boolean") {
    return (
      <label class="flex cursor-pointer items-center justify-between gap-3 text-xs">
        <span class="font-mono">{field.name}</span>
        <input
          type="checkbox"
          class="toggle toggle-sm"
          checked={Boolean(value)}
          onChange={(e) => onChange((e.target as HTMLInputElement).checked)}
        />
      </label>
    );
  }
  if (field.schema.type === "number" || field.schema.type === "integer") {
    return (
      <label class="form-control">
        {label}
        <input
          class="input input-bordered input-sm font-mono"
          type="number"
          step={field.schema.type === "integer" ? "1" : "any"}
          value={value === undefined || value === null ? "" : String(value)}
          onInput={(e) => {
            const raw = (e.target as HTMLInputElement).value;
            onChange(raw.trim() ? Number(raw) : undefined);
          }}
        />
      </label>
    );
  }
  return (
    <label class="form-control">
      {label}
      <input
        class="input input-bordered input-sm font-mono"
        value={value === undefined || value === null ? "" : String(value)}
        onInput={(e) => onChange((e.target as HTMLInputElement).value)}
      />
      {field.schema.description && (
        <span class="mt-1 text-[11px] leading-snug text-base-content/45">
          {field.schema.description}
        </span>
      )}
    </label>
  );
}

function configFields(schema: JsonSchema | undefined): ConfigFieldModel[] {
  if (!schema || schema.type !== "object" || !schema.properties) return [];
  const required = new Set(schema.required ?? []);
  return Object.entries(schema.properties)
    .filter(
      ([, property]) =>
        property.enum?.length ||
        property.type === "string" ||
        property.type === "number" ||
        property.type === "integer" ||
        property.type === "boolean",
    )
    .map(([name, property]) => ({
      name,
      schema: property,
      required: required.has(name),
      defaultValue: property.default,
    }));
}

function AuthoredPanel({
  workflows,
  loading,
  selected,
  busy,
  onLoad,
  onOpenWorkflow,
  onPublish,
}: {
  workflows: AuthoredWorkflowDto[];
  loading: boolean;
  selected: AuthoredWorkflowDto | null;
  busy: boolean;
  onLoad: (workflow: AuthoredWorkflowDto) => void;
  onOpenWorkflow: (name: string) => void;
  onPublish: (workflow: AuthoredWorkflowDto) => void;
}) {
  return (
    <section class="card bg-base-100/95 border border-base-content/10 shadow overflow-hidden">
      <div class="border-b border-base-content/10 px-4 py-3">
        <div class="text-xs uppercase tracking-wider text-base-content/55">Authored</div>
      </div>
      <div class="divide-y divide-base-300">
        {loading ? (
          <div class="p-4 text-sm text-base-content/45">Loading...</div>
        ) : workflows.length === 0 ? (
          <div class="p-4">
            <EmptyState message="No authored workflows yet." />
          </div>
        ) : (
          workflows.map((workflow) => (
            <div
              class={`p-4 ${
                selected?.name === workflow.name && selected?.version === workflow.version
                  ? "bg-base-200"
                  : ""
              }`}
            >
              <button class="w-full text-left" onClick={() => onLoad(workflow)}>
                <div class="flex items-start justify-between gap-2">
                  <div class="min-w-0">
                    <div class="font-mono text-sm truncate">{workflow.name}</div>
                    <div class="mt-1 text-[11px] text-base-content/45">
                      {workflow.version} · {new Date(workflow.updatedAt).toLocaleString()}
                    </div>
                  </div>
                  <span
                    class={`badge badge-sm ${
                      workflow.status === "published" ? "badge-success" : "badge-outline"
                    }`}
                  >
                    {workflow.status}
                  </span>
                </div>
              </button>
              <div class="mt-3 flex justify-end gap-2">
                {workflow.status === "published" && (
                  <button
                    class="btn btn-xs btn-ghost"
                    onClick={() => onOpenWorkflow(workflow.name)}
                  >
                    View
                  </button>
                )}
                <button
                  class="btn btn-xs btn-outline"
                  disabled={busy}
                  onClick={() => onPublish(workflow)}
                >
                  Publish
                </button>
              </div>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function validateDraft(schema: WorkflowSchema, stepById: Map<string, CatalogStep>): string[] {
  const issues: string[] = [];
  if (!schema.name.trim()) issues.push("Workflow name is required.");
  const names = new Set<string>();
  for (const step of schema.steps) {
    if (names.has(step.name)) issues.push(`Duplicate step name: ${step.name}`);
    names.add(step.name);
    if ("activityRef" in step && !stepById.has(step.activityRef)) {
      issues.push(`Unknown activityRef on ${step.name}: ${step.activityRef}`);
    }
  }
  for (const step of schema.steps) {
    if (!("dependsOn" in step)) continue;
    for (const dep of step.dependsOn) {
      if (!names.has(dep)) issues.push(`${step.name} depends on missing step ${dep}`);
    }
  }
  return issues;
}

function isEditableStep(step: BuilderStep): step is EditableStep {
  return "activityRef" in step && "dependsOn" in step;
}

function toStepName(id: string): string {
  const last = id.split(".").at(-1) ?? id;
  return last.replace(/[^a-zA-Z0-9_]/g, "_") || "step";
}

function uniqueStepName(schema: WorkflowSchema, base: string): string {
  const names = new Set(schema.steps.map((step) => step.name));
  let name = base;
  let idx = 2;
  while (names.has(name)) {
    name = `${base}${idx}`;
    idx += 1;
  }
  return name;
}
