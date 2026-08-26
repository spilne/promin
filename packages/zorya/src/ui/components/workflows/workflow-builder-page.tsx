import type { JsonSchema, WorkflowSchema } from "@promin/workflow";
import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { confirm, toast } from "../../lib/dialogs.ts";
import type {
  AuthoredWorkflowDto,
  WorkflowStepCatalogResponse,
} from "../../../server/routes/workflow-builder.ts";
import { EmptyState } from "../ui/empty-state.tsx";
import { Page, PageHeader } from "../ui/page.tsx";

type CatalogStep = WorkflowStepCatalogResponse["steps"][number];
type BuilderStep = WorkflowSchema["steps"][number];
type DependableStep = Extract<BuilderStep, { dependsOn: string[] }>;
type EditableStep = Extract<BuilderStep, { activityRef: string; dependsOn: readonly string[] }>;
type BuilderMode = "canvas" | "json";
type BuilderView = "list" | "editor";

const INPUT_NODE_ID = "__workflow_input__";

const DEFAULT_INPUT_SCHEMA_TEMPLATE: JsonSchema = {
  type: "object",
  properties: {
    text: { type: "string" },
  },
  required: ["text"],
};

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
  initialWorkflowName?: string;
  initialWorkflowVersion?: string;
  onBuilderRouteChange: (name?: string, version?: string) => void;
  onOpenWorkflow: (name: string) => void;
  onOpenRun: (id: string) => void;
}

export function WorkflowBuilderPage({
  initialWorkflowName,
  initialWorkflowVersion,
  onBuilderRouteChange,
  onOpenWorkflow,
  onOpenRun,
}: WorkflowBuilderPageProps) {
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
  const [view, setView] = useState<BuilderView>("list");
  const [selected, setSelected] = useState<AuthoredWorkflowDto | null>(null);
  const [connectingFrom, setConnectingFrom] = useState<string | null>(null);
  const [inspectorOpen, setInspectorOpen] = useState(false);
  const [busy, setBusy] = useState<"save" | "publish" | "run" | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [testInputJson, setTestInputJson] = useState(() =>
    JSON.stringify("hello workflow", null, 2),
  );
  const [testInputTouched, setTestInputTouched] = useState(false);

  const steps = catalog?.steps ?? [];
  const workflows = authored?.workflows ?? [];
  const configured = catalogLoading || steps.length > 0 || workflows.length > 0;
  const stepById = useMemo(() => new Map(steps.map((step) => [step.id, step])), [steps]);
  const issues = useMemo(() => validateDraft(schema, stepById), [schema, stepById]);
  const activeStep =
    selectedStep === INPUT_NODE_ID
      ? undefined
      : schema.steps.find((step) => step.name === selectedStep);
  const suggestedTestInput = useMemo(() => inferTestInput(schema, stepById), [schema, stepById]);
  const suggestedTestInputJson = useMemo(
    () => JSON.stringify(suggestedTestInput, null, 2),
    [suggestedTestInput],
  );
  const rootInputLabel = useMemo(() => describeWorkflowInput(schema, stepById), [schema, stepById]);
  const rootInputSchema = useMemo(() => firstRootInputSchema(schema, stepById), [schema, stepById]);

  useEffect(() => {
    if (!testInputTouched) setTestInputJson(suggestedTestInputJson);
  }, [suggestedTestInputJson, testInputTouched]);

  useEffect(() => {
    if (!initialWorkflowName || !authored?.workflows) return;
    const target =
      authored.workflows.find(
        (workflow) =>
          workflow.name === initialWorkflowName &&
          (!initialWorkflowVersion || workflow.version === initialWorkflowVersion),
      ) ?? authored.workflows.find((workflow) => workflow.name === initialWorkflowName);
    if (!target) return;
    if (
      selected?.name === target.name &&
      selected.version === target.version &&
      view === "editor"
    ) {
      return;
    }
    loadWorkflow(target, { updateRoute: false });
  }, [authored, initialWorkflowName, initialWorkflowVersion]);

  function updateSchema(
    next: WorkflowSchema,
    options: { preserveSelectedWorkflow?: boolean } = {},
  ): void {
    setSchema(next);
    setSchemaJson(JSON.stringify(next, null, 2));
    setError(null);
    setMessage(null);
    if (!options.preserveSelectedWorkflow) setSelected(null);
    if (selectedStep !== INPUT_NODE_ID && !next.steps.some((step) => step.name === selectedStep)) {
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
    if (issues.length > 0) {
      setError("Resolve validation issues before running a test.");
      return;
    }
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

  function loadWorkflow(
    workflow: AuthoredWorkflowDto,
    options: { updateRoute?: boolean } = {},
  ): void {
    setVersion(workflow.version);
    updateSchema(structuredClone(workflow.schema), { preserveSelectedWorkflow: true });
    setSelected(workflow);
    setSelectedStep(workflow.schema.steps[0]?.name ?? "");
    setView("editor");
    setInspectorOpen(false);
    setTestInputTouched(false);
    if (options.updateRoute !== false) onBuilderRouteChange(workflow.name, workflow.version);
  }

  function startNewWorkflow(): void {
    setSchema(structuredClone(SAMPLE_SCHEMA));
    setSchemaJson(JSON.stringify(SAMPLE_SCHEMA, null, 2));
    setVersion("v1");
    setSelected(null);
    setSelectedStep("upper");
    setInspectorOpen(false);
    setView("editor");
    onBuilderRouteChange();
    setTestInputTouched(false);
    setError(null);
    setMessage(null);
  }

  async function deleteAuthored(workflow: AuthoredWorkflowDto): Promise<void> {
    const ok = await confirm({
      title: `Delete ${workflow.name}@${workflow.version}?`,
      message:
        workflow.status === "published"
          ? "This deletes the authored workflow and unpublishes the matching version."
          : "This deletes the authored workflow draft.",
      confirmLabel: "Delete",
      variant: "danger",
    });
    if (!ok) return;
    setBusy("save");
    setError(null);
    try {
      await api.deleteAuthoredWorkflow(workflow.name, workflow.version);
      if (selected?.name === workflow.name && selected.version === workflow.version) {
        setSelected(null);
      }
      refreshAuthored();
      toast(`Deleted ${workflow.name}@${workflow.version}`, { variant: "success" });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(null);
    }
  }

  function addStep(entry: CatalogStep): void {
    const baseName = toStepName(entry.id);
    const name = uniqueStepName(schema, baseName);
    const nextStep =
      entry.kind === "branch"
        ? ({
            type: "branch",
            name,
            dependsOn: [],
            conditionRef: String(entry.defaultConfig?.conditionRef ?? "predicate.long"),
            ifTrue: {
              activityRef: String(entry.defaultConfig?.ifTrueActivityRef ?? "transform.uppercase"),
            },
            ifFalse: {
              activityRef: String(entry.defaultConfig?.ifFalseActivityRef ?? "transform.identity"),
            },
          } as BuilderStep)
        : entry.kind === "parallel"
          ? ({
              type: "parallel",
              name,
              dependsOn: [],
              branches: {
                upper: { activityRef: "transform.uppercase" },
                length: { activityRef: "text.length" },
              },
            } as BuilderStep)
          : ({
              type: entry.kind ?? "step",
              name,
              dependsOn: [],
              activityRef: entry.id,
              ...(entry.defaultConfig ? { config: entry.defaultConfig } : {}),
            } as BuilderStep);
    updateSchema({
      ...schema,
      steps: [...schema.steps, nextStep],
      ui: {
        ...(schema.ui ?? {}),
        [name]: {
          x: 80 + (schema.steps.length % 4) * 240,
          y: 120 + Math.floor(schema.steps.length / 4) * 150,
          label: entry.title,
        },
      },
    });
    setSelectedStep(name);
  }

  function updateWorkflowName(name: string): void {
    updateSchema({ ...schema, name });
  }

  function updateWorkflowInputSchema(inputSchema: JsonSchema | undefined): void {
    if (inputSchema === undefined) {
      const { inputSchema: _inputSchema, ...withoutInputSchema } = schema;
      updateSchema(withoutInputSchema);
      return;
    }
    updateSchema({ ...schema, inputSchema });
  }

  function updateStep(name: string, patch: Partial<BuilderStep> & { name?: string }): void {
    const old = schema.steps.find((step) => step.name === name);
    if (!old) return;
    const nextName = patch.name?.trim() || old.name;
    const renamed = nextName !== old.name;
    const steps = schema.steps.map((step) => {
      if (step.name === old.name) return { ...old, ...patch, name: nextName } as BuilderStep;
      if (!renamed || !hasDependsOn(step)) return step;
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
        hasDependsOn(step)
          ? ({ ...step, dependsOn: step.dependsOn.filter((d) => d !== name) } as BuilderStep)
          : step,
      );
    const ui = { ...(schema.ui ?? {}) };
    delete ui[name];
    updateSchema({ ...schema, steps, ui });
  }

  function moveStep(name: string, x: number, y: number): void {
    updateSchema({
      ...schema,
      ui: {
        ...(schema.ui ?? {}),
        [name]: { ...(schema.ui?.[name] ?? {}), x: Math.max(0, x), y: Math.max(0, y) },
      },
    });
    setSelectedStep(name);
  }

  function connectStep(from: string, to: string): void {
    if (from === to) return;
    const target = schema.steps.find((step) => step.name === to);
    if (!target || !hasDependsOn(target)) return;
    if (from === INPUT_NODE_ID) {
      updateStepDependsOn(to, []);
      setConnectingFrom(null);
      setSelectedStep(to);
      return;
    }
    if (target.dependsOn.includes(from)) return;
    if (wouldCreateCycle(schema, from, to)) {
      setError(`Cannot connect ${from} to ${to}: that would create a dependency cycle.`);
      setConnectingFrom(null);
      return;
    }
    updateStepDependsOn(to, [...target.dependsOn, from]);
    setConnectingFrom(null);
    setSelectedStep(to);
  }

  function disconnectStep(from: string, to: string): void {
    const target = schema.steps.find((step) => step.name === to);
    if (!target || !hasDependsOn(target)) return;
    updateStepDependsOn(
      to,
      target.dependsOn.filter((dep) => dep !== from),
    );
  }

  function updateStepDependsOn(name: string, dependsOn: string[]): void {
    updateSchema({
      ...schema,
      steps: schema.steps.map((step) =>
        step.name === name && hasDependsOn(step) ? ({ ...step, dependsOn } as BuilderStep) : step,
      ),
    });
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

      {view === "list" ? (
        <div class="space-y-4">
          <section class="card bg-base-100/95 border border-base-content/10 shadow">
            <div class="flex flex-col gap-3 p-4 md:flex-row md:items-center md:justify-between">
              <div>
                <div class="text-xs uppercase tracking-wider text-base-content/55">Workflows</div>
                <div class="mt-1 text-sm text-base-content/65">
                  Start from an authored workflow or create a new one.
                </div>
              </div>
              <button class="btn btn-sm btn-primary" onClick={startNewWorkflow}>
                New Workflow
              </button>
            </div>
          </section>
          <AuthoredPanel
            workflows={workflows}
            loading={authoredLoading && !authored}
            selected={selected}
            busy={busy !== null}
            onLoad={loadWorkflow}
            onOpenWorkflow={onOpenWorkflow}
            onDelete={deleteAuthored}
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
      ) : (
        <div class="space-y-4">
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
                <button class="btn join-item btn-sm btn-ghost" onClick={() => setView("list")}>
                  Workflows
                </button>
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
                <button
                  class="btn join-item btn-sm btn-ghost"
                  onClick={() => setInspectorOpen(true)}
                >
                  Inspector
                </button>
              </div>
            </div>

            {mode === "canvas" ? (
              <div class="bg-base-200/45 p-4">
                <WorkflowCanvas
                  schema={schema}
                  stepById={stepById}
                  inputLabel={rootInputLabel}
                  selected={selectedStep}
                  connectingFrom={connectingFrom}
                  onSelect={setSelectedStep}
                  onInspect={(name) => {
                    setSelectedStep(name);
                    setInspectorOpen(true);
                  }}
                  onMove={moveStep}
                  onDelete={removeStep}
                  onConnectStart={(name) =>
                    setConnectingFrom(connectingFrom === name ? null : name)
                  }
                  onConnectEnd={connectStep}
                  onDisconnect={disconnectStep}
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
              <div class="grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
                <label class="form-control">
                  <span class="mb-1 flex flex-wrap items-center justify-between gap-2">
                    <span class="text-[10px] uppercase tracking-wider text-base-content/50">
                      Test Input JSON
                    </span>
                    <span class="font-mono text-[11px] text-base-content/45">{rootInputLabel}</span>
                  </span>
                  <textarea
                    class="textarea textarea-bordered min-h-20 font-mono text-xs"
                    value={testInputJson}
                    onInput={(e) => {
                      setTestInputTouched(true);
                      setTestInputJson((e.target as HTMLTextAreaElement).value);
                    }}
                  />
                </label>
                <div class="flex flex-wrap justify-end gap-2">
                  <button
                    class="btn btn-sm btn-ghost"
                    onClick={() => {
                      setTestInputTouched(false);
                      setTestInputJson(suggestedTestInputJson);
                    }}
                  >
                    Use Sample
                  </button>
                  <button
                    class="btn btn-sm btn-ghost"
                    onClick={() => {
                      startNewWorkflow();
                    }}
                  >
                    New
                  </button>
                  <button class="btn btn-sm btn-outline" disabled={busy !== null} onClick={save}>
                    {busy === "save" ? "Saving..." : "Save Draft"}
                  </button>
                  <button
                    class="btn btn-sm btn-primary"
                    disabled={busy !== null || issues.length > 0}
                    onClick={publish}
                    title={issues.length > 0 ? "Resolve validation issues before publishing" : ""}
                  >
                    {busy === "publish" ? "Publishing..." : "Publish"}
                  </button>
                  <button
                    class="btn btn-sm btn-secondary"
                    disabled={busy !== null || issues.length > 0}
                    onClick={testRun}
                    title={issues.length > 0 ? "Resolve validation issues before testing" : ""}
                  >
                    {busy === "run" ? "Running..." : "Test Run"}
                  </button>
                </div>
              </div>
            </div>
          </section>

          <div class="grid grid-cols-1 gap-4">
            <StepCatalogPanel loading={catalogLoading && !catalog} steps={steps} onAdd={addStep} />
          </div>
        </div>
      )}
      {inspectorOpen && view === "editor" && (
        <InspectorDrawer
          step={activeStep}
          schema={schema}
          catalog={steps}
          issues={issues}
          inputSelected={selectedStep === INPUT_NODE_ID}
          inputSchema={schema.inputSchema}
          onClose={() => setInspectorOpen(false)}
          onUpdate={updateStep}
          onUpdateInputSchema={updateWorkflowInputSchema}
          inputSchemaTemplate={rootInputSchema}
          onRemove={(name) => {
            removeStep(name);
            setInspectorOpen(false);
          }}
        />
      )}
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
  const [query, setQuery] = useState("");
  const visibleSteps = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return steps;
    return steps.filter((step) =>
      [step.id, step.title, step.description, step.category, ...(step.tags ?? [])]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(needle)),
    );
  }, [query, steps]);
  const groupedSteps = useMemo(() => {
    const groups = new Map<string, CatalogStep[]>();
    for (const step of visibleSteps) {
      const category = step.category ?? step.kind ?? "Other";
      groups.set(category, [...(groups.get(category) ?? []), step]);
    }
    return [...groups.entries()].sort(([a], [b]) => a.localeCompare(b));
  }, [visibleSteps]);

  return (
    <section class="card bg-base-100/95 border border-base-content/10 shadow overflow-hidden">
      <div class="flex flex-col gap-3 border-b border-base-content/10 px-4 py-3 md:flex-row md:items-end md:justify-between">
        <div>
          <div class="text-xs uppercase tracking-wider text-base-content/55">Step Registry</div>
          <div class="mt-1 text-xs text-base-content/45">
            {visibleSteps.length} of {steps.length} available
          </div>
        </div>
        <label class="form-control md:w-72">
          <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">Search</span>
          <input
            class="input input-bordered input-sm"
            value={query}
            onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
          />
        </label>
      </div>
      <div class="max-h-[42rem] overflow-auto p-3">
        {loading ? (
          <div class="p-4 text-sm text-base-content/45">Loading...</div>
        ) : visibleSteps.length === 0 ? (
          <div class="p-4">
            <EmptyState message="No step templates registered." />
          </div>
        ) : (
          <div class="space-y-3">
            {groupedSteps.map(([category, categorySteps]) => (
              <details class="rounded border border-base-content/10 bg-base-100" open>
                <summary class="cursor-pointer px-3 py-2 text-xs font-medium uppercase tracking-wider text-base-content/55">
                  {category} <span class="font-mono opacity-45">{categorySteps.length}</span>
                </summary>
                <div class="grid grid-cols-1 gap-2 border-t border-base-content/10 p-3 md:grid-cols-2 2xl:grid-cols-3">
                  {categorySteps.map((step) => (
                    <button
                      class="min-h-28 rounded border border-base-content/10 bg-base-100 p-3 text-left hover:border-primary/40 hover:bg-base-200"
                      onClick={() => onAdd(step)}
                      title="Add this step to the workflow"
                    >
                      <div class="flex items-start justify-between gap-2">
                        <div class="min-w-0">
                          <div class="text-sm font-medium leading-tight">{step.title}</div>
                          <div class="mt-1 font-mono text-[11px] text-base-content/45 truncate">
                            {step.id}
                          </div>
                        </div>
                        <span class="badge badge-sm badge-outline shrink-0">
                          {step.kind ?? "step"}
                        </span>
                      </div>
                      {step.description && (
                        <div class="mt-2 line-clamp-2 text-xs leading-relaxed text-base-content/60">
                          {step.description}
                        </div>
                      )}
                      {step.capabilities && step.capabilities.length > 0 && (
                        <div class="mt-2 flex flex-wrap gap-1">
                          {step.capabilities.map((capability) => (
                            <span class="badge badge-xs badge-ghost">{capability}</span>
                          ))}
                        </div>
                      )}
                    </button>
                  ))}
                </div>
              </details>
            ))}
          </div>
        )}
      </div>
    </section>
  );
}

function WorkflowCanvas({
  schema,
  stepById,
  inputLabel,
  selected,
  connectingFrom,
  onSelect,
  onInspect,
  onMove,
  onDelete,
  onConnectStart,
  onConnectEnd,
  onDisconnect,
}: {
  schema: WorkflowSchema;
  stepById: Map<string, CatalogStep>;
  inputLabel: string;
  selected: string;
  connectingFrom: string | null;
  onSelect: (name: string) => void;
  onInspect: (name: string) => void;
  onMove: (name: string, x: number, y: number) => void;
  onDelete: (name: string) => void;
  onConnectStart: (name: string) => void;
  onConnectEnd: (from: string, to: string) => void;
  onDisconnect: (from: string, to: string) => void;
}) {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [drag, setDrag] = useState<{ name: string; dx: number; dy: number } | null>(null);
  const [connectionDrag, setConnectionDrag] = useState<{
    from: string;
    startX: number;
    startY: number;
    x: number;
    y: number;
    moved: boolean;
  } | null>(null);
  const suppressNextConnectClick = useRef(false);
  const nodes = schema.steps.map((step, index) => {
    const pos = schema.ui?.[step.name] ?? { x: 80 + index * 180, y: 120 };
    return { step, x: pos.x, y: pos.y };
  });
  const minNodeX = nodes.reduce((min, node) => Math.min(min, node.x), 280);
  const minNodeY = nodes.reduce((min, node) => Math.min(min, node.y), 120);
  const inputNode = {
    x: minNodeX - 240,
    y: Math.max(40, minNodeY),
  };
  const right = nodes.reduce(
    (max, node) => Math.max(max, node.x + 220),
    Math.max(900, inputNode.x + 200),
  );
  const bottom = nodes.reduce((max, node) => Math.max(max, node.y + 140), 440);
  const viewBox = {
    x: Math.min(0, inputNode.x - 24),
    y: 0,
    width: right - Math.min(0, inputNode.x - 24),
    height: Math.max(bottom, inputNode.y + 120),
  };

  function pointFor(e: MouseEvent): { x: number; y: number } | null {
    const svg = svgRef.current;
    const matrix = svg?.getScreenCTM();
    if (!svg || !matrix) return null;
    const point = svg.createSVGPoint();
    point.x = e.clientX;
    point.y = e.clientY;
    const transformed = point.matrixTransform(matrix.inverse());
    return { x: transformed.x, y: transformed.y };
  }

  function beginDrag(e: MouseEvent, name: string, x: number, y: number): void {
    if (e.button !== 0) return;
    const point = pointFor(e);
    if (!point) return;
    setDrag({ name, dx: point.x - x, dy: point.y - y });
    onSelect(name);
  }

  function moveDrag(e: MouseEvent): void {
    const point = pointFor(e);
    if (!point) return;
    if (connectionDrag) {
      const moved =
        connectionDrag.moved ||
        Math.abs(point.x - connectionDrag.startX) > 4 ||
        Math.abs(point.y - connectionDrag.startY) > 4;
      suppressNextConnectClick.current = suppressNextConnectClick.current || moved;
      setConnectionDrag({ ...connectionDrag, x: point.x, y: point.y, moved });
      return;
    }
    if (!drag) return;
    onMove(drag.name, Math.round(point.x - drag.dx), Math.round(point.y - drag.dy));
  }

  function beginConnectionDrag(e: MouseEvent, name: string, x: number, y: number): void {
    if (e.button !== 0) return;
    e.stopPropagation();
    const point = pointFor(e) ?? { x, y };
    suppressNextConnectClick.current = false;
    setConnectionDrag({ from: name, startX: point.x, startY: point.y, x, y, moved: false });
    onConnectStart(name);
  }

  function endConnectionDrag(to?: string): void {
    const current = connectionDrag;
    setConnectionDrag(null);
    if (!current) return;
    if (to) {
      onConnectEnd(current.from, to);
      return;
    }
    if (current.moved) onConnectStart(current.from);
  }

  return (
    <div class="space-y-2">
      <div class="flex flex-wrap items-center justify-between gap-2 text-xs text-base-content/55">
        <div>
          {connectingFrom ? (
            <span>
              Connecting from{" "}
              <span class="font-mono text-base-content">
                {connectingFrom === INPUT_NODE_ID ? "Input" : connectingFrom}
              </span>
            </span>
          ) : (
            <span>Drag nodes. Use right handles to connect, left handles to receive.</span>
          )}
        </div>
        {connectingFrom && (
          <button class="btn btn-xs btn-ghost" onClick={() => onConnectStart(connectingFrom)}>
            Cancel Connect
          </button>
        )}
      </div>
      <svg
        ref={svgRef}
        class="h-[28rem] w-full rounded border border-base-content/10 bg-base-100 lg:h-[42rem]"
        viewBox={`${viewBox.x} ${viewBox.y} ${viewBox.width} ${viewBox.height}`}
        role="img"
        onMouseMove={moveDrag}
        onMouseUp={() => {
          setDrag(null);
          endConnectionDrag();
        }}
        onMouseLeave={() => {
          setDrag(null);
          endConnectionDrag();
        }}
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
                const sourceX = from.x + 180;
                const sourceY = from.y + 44;
                const targetX = node.x;
                const targetY = node.y + 44;
                return (
                  <g>
                    <path
                      d={edgePath(sourceX, sourceY, targetX, targetY)}
                      fill="none"
                      stroke="currentColor"
                      class="text-base-content/30"
                      stroke-width="2"
                      marker-end="url(#wf-arrow)"
                    />
                    <g
                      class="cursor-pointer"
                      transform={`translate(${(from.x + 180 + node.x) / 2}, ${
                        (from.y + node.y) / 2 + 44
                      })`}
                      onMouseDown={(e) => e.stopPropagation()}
                      onClick={(e) => {
                        e.stopPropagation();
                        onDisconnect(dep, node.step.name);
                      }}
                    >
                      <circle r="9" class="fill-base-100 stroke-base-content/20" />
                      <text
                        x="0"
                        y="4"
                        text-anchor="middle"
                        class="fill-current text-[12px] opacity-70"
                      >
                        x
                      </text>
                    </g>
                  </g>
                );
              })
            : [],
        )}
        {nodes
          .filter((node) => hasDependsOn(node.step) && node.step.dependsOn.length === 0)
          .map((node) => (
            <path
              d={edgePath(inputNode.x + 160, inputNode.y + 36, node.x, node.y + 44)}
              fill="none"
              stroke="currentColor"
              class="text-success/55"
              stroke-width="2"
              marker-end="url(#wf-arrow)"
            />
          ))}
        <g
          class="cursor-pointer"
          transform={`translate(${inputNode.x}, ${inputNode.y})`}
          onClick={() => onSelect(INPUT_NODE_ID)}
          onDblClick={(e) => {
            e.stopPropagation();
            onInspect(INPUT_NODE_ID);
          }}
        >
          <rect
            width="160"
            height="72"
            rx="6"
            class={
              selected === INPUT_NODE_ID
                ? "fill-success/20 stroke-success"
                : "fill-success/10 stroke-success/60"
            }
            stroke-width={selected === INPUT_NODE_ID ? 2 : 1.5}
          />
          <text x="14" y="28" class="select-none fill-current text-[13px] font-semibold">
            Input
          </text>
          <text x="14" y="50" class="select-none fill-current text-[10px] opacity-55">
            {inputLabel}
          </text>
          <g
            class={`cursor-pointer ${
              (connectionDrag?.from ?? connectingFrom) === INPUT_NODE_ID
                ? "text-primary"
                : "text-success"
            }`}
            transform="translate(160, 36)"
            onMouseDown={(e) =>
              beginConnectionDrag(e, INPUT_NODE_ID, inputNode.x + 160, inputNode.y + 36)
            }
            onClick={(e) => {
              e.stopPropagation();
              if (suppressNextConnectClick.current) {
                suppressNextConnectClick.current = false;
                return;
              }
              onConnectStart(INPUT_NODE_ID);
            }}
          >
            <circle r="8" class="fill-base-100 stroke-current" stroke-width="2" />
            <circle r="3" class="fill-current" />
          </g>
        </g>
        {nodes.map((node) => {
          const entry =
            "activityRef" in node.step ? stepById.get(node.step.activityRef) : undefined;
          const active = selected === node.step.name;
          const activeConnectionFrom = connectionDrag?.from ?? connectingFrom;
          const canReceive =
            activeConnectionFrom !== null &&
            activeConnectionFrom !== node.step.name &&
            hasDependsOn(node.step) &&
            (activeConnectionFrom === INPUT_NODE_ID
              ? node.step.dependsOn.length > 0
              : !node.step.dependsOn.includes(activeConnectionFrom));
          return (
            <g transform={`translate(${node.x}, ${node.y})`}>
              <g
                class="cursor-move"
                onMouseDown={(e) => beginDrag(e, node.step.name, node.x, node.y)}
                onClick={() => onSelect(node.step.name)}
                onDblClick={(e) => {
                  e.stopPropagation();
                  onInspect(node.step.name);
                }}
              >
                <rect
                  width="180"
                  height="88"
                  rx="6"
                  class={
                    active
                      ? "fill-primary/15 stroke-primary"
                      : "fill-base-100 stroke-base-content/20"
                  }
                  stroke-width={active ? 2 : 1}
                />
                <text x="14" y="28" class="select-none fill-current text-[13px] font-semibold">
                  {node.step.name}
                </text>
                <text x="14" y="52" class="select-none fill-current text-[11px] opacity-60">
                  {entry?.title ??
                    ("activityRef" in node.step ? node.step.activityRef : node.step.type)}
                </text>
                <text x="14" y="72" class="select-none fill-current text-[10px] opacity-45">
                  {node.step.type}
                </text>
              </g>

              <g
                class={`cursor-pointer ${canReceive ? "text-primary" : "text-base-content/45"}`}
                transform="translate(0, 44)"
                onMouseDown={(e) => e.stopPropagation()}
                onMouseUp={(e) => {
                  if (!connectionDrag) return;
                  e.stopPropagation();
                  endConnectionDrag(canReceive ? node.step.name : undefined);
                }}
                onClick={(e) => {
                  e.stopPropagation();
                  if (connectingFrom) onConnectEnd(connectingFrom, node.step.name);
                  else onSelect(node.step.name);
                }}
              >
                <circle r="8" class="fill-base-100 stroke-current" stroke-width="2" />
              </g>

              <g
                class={`cursor-pointer ${
                  activeConnectionFrom === node.step.name ? "text-primary" : "text-base-content/45"
                }`}
                transform="translate(180, 44)"
                onMouseDown={(e) =>
                  beginConnectionDrag(e, node.step.name, node.x + 180, node.y + 44)
                }
                onClick={(e) => {
                  e.stopPropagation();
                  if (suppressNextConnectClick.current) {
                    suppressNextConnectClick.current = false;
                    return;
                  }
                  onConnectStart(node.step.name);
                }}
              >
                <circle r="8" class="fill-base-100 stroke-current" stroke-width="2" />
                <circle r="3" class="fill-current" />
              </g>

              <g
                class="cursor-pointer text-error"
                transform="translate(166, 14)"
                onMouseDown={(e) => e.stopPropagation()}
                onClick={(e) => {
                  e.stopPropagation();
                  onDelete(node.step.name);
                }}
              >
                <circle r="10" class="fill-base-100 stroke-current" />
                <text x="0" y="4" text-anchor="middle" class="select-none fill-current text-[12px]">
                  x
                </text>
              </g>
            </g>
          );
        })}
        {connectionDrag && (
          <line
            x1={connectionDrag.startX}
            y1={connectionDrag.startY}
            x2={connectionDrag.x}
            y2={connectionDrag.y}
            stroke="currentColor"
            class="pointer-events-none text-primary"
            stroke-width="2"
            stroke-dasharray="6 4"
            marker-end="url(#wf-arrow)"
          />
        )}
      </svg>
    </div>
  );
}

function edgePath(sourceX: number, sourceY: number, targetX: number, targetY: number): string {
  const dx = Math.max(80, Math.abs(targetX - sourceX) * 0.5);
  return `M ${sourceX} ${sourceY} C ${sourceX + dx} ${sourceY}, ${targetX - dx} ${targetY}, ${targetX} ${targetY}`;
}

function InspectorDrawer({
  step,
  schema,
  catalog,
  issues,
  inputSelected,
  inputSchema,
  inputSchemaTemplate,
  onClose,
  onUpdate,
  onUpdateInputSchema,
  onRemove,
}: {
  step: BuilderStep | undefined;
  schema: WorkflowSchema;
  catalog: CatalogStep[];
  issues: string[];
  inputSelected: boolean;
  inputSchema: JsonSchema | undefined;
  inputSchemaTemplate: JsonSchema | undefined;
  onClose: () => void;
  onUpdate: (name: string, patch: Partial<BuilderStep> & { name?: string }) => void;
  onUpdateInputSchema: (schema: JsonSchema | undefined) => void;
  onRemove: (name: string) => void;
}) {
  return (
    <div class="fixed inset-0 z-40 flex justify-end bg-black/20" onClick={onClose}>
      <aside
        class="h-full w-full max-w-[28rem] overflow-auto border-l border-base-content/10 bg-base-100 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div class="sticky top-0 z-10 flex items-center justify-between border-b border-base-content/10 bg-base-100 px-4 py-3">
          <div>
            <div class="text-xs uppercase tracking-wider text-base-content/55">Inspector</div>
            <div class="mt-1 text-xs text-base-content/45">Double-click a node to inspect it.</div>
          </div>
          <button class="btn btn-sm btn-ghost" onClick={onClose}>
            Close
          </button>
        </div>
        <StepInspector
          step={step}
          schema={schema}
          catalog={catalog}
          issues={issues}
          inputSelected={inputSelected}
          inputSchema={inputSchema}
          inputSchemaTemplate={inputSchemaTemplate}
          onUpdate={onUpdate}
          onUpdateInputSchema={onUpdateInputSchema}
          onRemove={onRemove}
        />
      </aside>
    </div>
  );
}

function StepInspector({
  step,
  schema,
  catalog,
  issues,
  inputSelected,
  inputSchema,
  inputSchemaTemplate,
  onUpdate,
  onUpdateInputSchema,
  onRemove,
}: {
  step: BuilderStep | undefined;
  schema: WorkflowSchema;
  catalog: CatalogStep[];
  issues: string[];
  inputSelected: boolean;
  inputSchema: JsonSchema | undefined;
  inputSchemaTemplate: JsonSchema | undefined;
  onUpdate: (name: string, patch: Partial<BuilderStep> & { name?: string }) => void;
  onUpdateInputSchema: (schema: JsonSchema | undefined) => void;
  onRemove: (name: string) => void;
}) {
  if (inputSelected) {
    return (
      <InputInspector
        inputSchema={inputSchema}
        inputSchemaTemplate={inputSchemaTemplate}
        issues={issues}
        onUpdateInputSchema={onUpdateInputSchema}
      />
    );
  }
  if (!step) {
    return <div class="p-4 text-sm text-base-content/45">Select a node.</div>;
  }
  const editableActivity = isEditableStep(step);
  const catalogEntry = editableActivity
    ? catalog.find((entry) => entry.id === step.activityRef)
    : undefined;
  const activityOptions = catalog.filter((entry) => entry.category !== "Control");
  const predicateOptions = catalog.filter(
    (entry) => entry.outputSchema?.type === "boolean" || entry.category === "Predicate",
  );
  return (
    <aside class="space-y-3 p-4">
      <div>
        <div class="text-xs uppercase tracking-wider text-base-content/55">Node</div>
        <div class="mt-1 font-mono text-sm">{step.name}</div>
      </div>
      {editableActivity ? (
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
          <DependencyEditor step={step} schema={schema} onUpdate={onUpdate} />
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
      ) : step.type === "branch" ? (
        <>
          <label class="form-control">
            <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">Name</span>
            <input
              class="input input-bordered input-sm font-mono"
              value={step.name}
              onInput={(e) => onUpdate(step.name, { name: (e.target as HTMLInputElement).value })}
            />
          </label>
          <DependencyEditor step={step} schema={schema} onUpdate={onUpdate} />
          <label class="form-control">
            <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
              Predicate
            </span>
            <select
              class="select select-bordered select-sm font-mono"
              value={step.conditionRef}
              onChange={(e) =>
                onUpdate(step.name, {
                  conditionRef: (e.target as HTMLSelectElement).value,
                })
              }
            >
              {predicateOptions.map((entry) => (
                <option value={entry.id}>{entry.id}</option>
              ))}
            </select>
          </label>
          <label class="form-control">
            <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
              True Activity
            </span>
            <select
              class="select select-bordered select-sm font-mono"
              value={step.ifTrue.activityRef}
              onChange={(e) =>
                onUpdate(step.name, {
                  ifTrue: { ...step.ifTrue, activityRef: (e.target as HTMLSelectElement).value },
                })
              }
            >
              {activityOptions.map((entry) => (
                <option value={entry.id}>{entry.id}</option>
              ))}
            </select>
          </label>
          <label class="form-control">
            <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
              False Activity
            </span>
            <select
              class="select select-bordered select-sm font-mono"
              value={step.ifFalse.activityRef}
              onChange={(e) =>
                onUpdate(step.name, {
                  ifFalse: { ...step.ifFalse, activityRef: (e.target as HTMLSelectElement).value },
                })
              }
            >
              {activityOptions.map((entry) => (
                <option value={entry.id}>{entry.id}</option>
              ))}
            </select>
          </label>
          <button
            class="btn btn-sm btn-error btn-outline w-full"
            onClick={() => onRemove(step.name)}
          >
            Delete Node
          </button>
        </>
      ) : step.type === "parallel" ? (
        <>
          <label class="form-control">
            <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">Name</span>
            <input
              class="input input-bordered input-sm font-mono"
              value={step.name}
              onInput={(e) => onUpdate(step.name, { name: (e.target as HTMLInputElement).value })}
            />
          </label>
          <DependencyEditor step={step} schema={schema} onUpdate={onUpdate} />
          <div class="space-y-2">
            <div class="text-[10px] uppercase tracking-wider text-base-content/50">
              Branch Activities
            </div>
            {Object.entries(step.branches).map(([branchName, branch]) => (
              <label class="form-control">
                <span class="mb-1 font-mono text-[11px] text-base-content/50">{branchName}</span>
                <select
                  class="select select-bordered select-sm font-mono"
                  value={branch.activityRef}
                  onChange={(e) =>
                    onUpdate(step.name, {
                      branches: {
                        ...step.branches,
                        [branchName]: {
                          ...branch,
                          activityRef: (e.target as HTMLSelectElement).value,
                        },
                      },
                    })
                  }
                >
                  {activityOptions.map((entry) => (
                    <option value={entry.id}>{entry.id}</option>
                  ))}
                </select>
              </label>
            ))}
          </div>
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

function InputInspector({
  inputSchema,
  inputSchemaTemplate,
  issues,
  onUpdateInputSchema,
}: {
  inputSchema: JsonSchema | undefined;
  inputSchemaTemplate: JsonSchema | undefined;
  issues: string[];
  onUpdateInputSchema: (schema: JsonSchema | undefined) => void;
}) {
  const template = inputSchemaTemplate ?? DEFAULT_INPUT_SCHEMA_TEMPLATE;
  const schemaTemplate = JSON.stringify(template, null, 2);
  const [json, setJson] = useState(() => JSON.stringify(inputSchema ?? template, null, 2));
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setJson(JSON.stringify(inputSchema ?? template, null, 2));
    setError(null);
  }, [inputSchema, schemaTemplate]);

  function apply(): void {
    try {
      const parsed = JSON.parse(json) as JsonSchema;
      onUpdateInputSchema(parsed);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <aside class="space-y-3 p-4">
      <div>
        <div class="text-xs uppercase tracking-wider text-base-content/55">Node</div>
        <div class="mt-1 font-mono text-sm">Input</div>
        <div class="mt-1 text-xs text-base-content/55">
          {schemaTypeLabel(inputSchema ?? template)}
        </div>
      </div>
      <label class="form-control">
        <span class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">
          Workflow Input Schema
        </span>
        <textarea
          class="textarea textarea-bordered min-h-52 font-mono text-xs leading-relaxed"
          spellcheck={false}
          placeholder={schemaTemplate}
          value={json}
          onInput={(e) => {
            setJson((e.target as HTMLTextAreaElement).value);
            setError(null);
          }}
        />
      </label>
      {error && <div class="alert alert-error text-xs">{error}</div>}
      <div class="flex flex-wrap justify-end gap-2">
        <button class="btn btn-sm btn-ghost" onClick={() => onUpdateInputSchema(undefined)}>
          Clear
        </button>
        <button class="btn btn-sm btn-outline" onClick={() => setJson(schemaTemplate)}>
          Use Template
        </button>
        <button class="btn btn-sm btn-primary" onClick={apply}>
          Apply Schema
        </button>
      </div>
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

function DependencyEditor({
  step,
  schema,
  onUpdate,
}: {
  step: DependableStep;
  schema: WorkflowSchema;
  onUpdate: (name: string, patch: Partial<BuilderStep> & { name?: string }) => void;
}) {
  const candidates = schema.steps.filter((candidate) => candidate.name !== step.name);
  return (
    <div class="space-y-2">
      <div class="rounded border border-base-content/10 bg-base-200/40 p-3">
        <div class="mb-1 text-[10px] uppercase tracking-wider text-base-content/50">Inputs</div>
        {step.dependsOn.length === 0 ? (
          <div class="text-xs text-base-content/55">Workflow input</div>
        ) : (
          <div class="flex flex-wrap gap-1">
            {step.dependsOn.map((dep) => (
              <span class="badge badge-sm badge-outline font-mono">{dep}</span>
            ))}
          </div>
        )}
      </div>
      <details class="rounded border border-base-content/10">
        <summary class="cursor-pointer px-3 py-2 text-xs text-base-content/60">
          Advanced wiring
        </summary>
        <div class="max-h-36 space-y-1 overflow-auto border-t border-base-content/10 p-2">
          {candidates.length === 0 ? (
            <div class="text-xs text-base-content/45">No other nodes.</div>
          ) : (
            candidates.map((candidate) => {
              const checked = step.dependsOn.includes(candidate.name);
              const cyclic = !checked && wouldCreateCycle(schema, candidate.name, step.name);
              return (
                <label
                  class={`flex cursor-pointer items-center gap-2 text-xs ${
                    cyclic ? "opacity-45" : ""
                  }`}
                >
                  <input
                    type="checkbox"
                    class="checkbox checkbox-xs"
                    checked={checked}
                    disabled={cyclic}
                    onChange={(e) => {
                      const nextChecked = (e.target as HTMLInputElement).checked;
                      const next = nextChecked
                        ? [...step.dependsOn, candidate.name]
                        : step.dependsOn.filter((dep) => dep !== candidate.name);
                      onUpdate(step.name, { dependsOn: next });
                    }}
                  />
                  <span class="font-mono">{candidate.name}</span>
                </label>
              );
            })
          )}
        </div>
      </details>
    </div>
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
  onDelete,
  onPublish,
}: {
  workflows: AuthoredWorkflowDto[];
  loading: boolean;
  selected: AuthoredWorkflowDto | null;
  busy: boolean;
  onLoad: (workflow: AuthoredWorkflowDto) => void;
  onOpenWorkflow: (name: string) => void;
  onDelete: (workflow: AuthoredWorkflowDto) => void;
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
                  class="btn btn-xs btn-error btn-outline"
                  disabled={busy}
                  onClick={() => onDelete(workflow)}
                >
                  Delete
                </button>
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
    if (
      schema.inputSchema &&
      hasDependsOn(step) &&
      step.dependsOn.length === 0 &&
      "activityRef" in step
    ) {
      const expected = stepById.get(step.activityRef)?.inputSchema;
      if (expected && !jsonSchemaTypeCompatible(schema.inputSchema, expected)) {
        issues.push(
          `Workflow input schema (${schemaTypeLabel(schema.inputSchema)}) does not match root step ${step.name} input (${schemaTypeLabel(expected)}).`,
        );
      }
    }
    if (step.type === "branch") {
      if (!stepById.has(step.conditionRef)) {
        issues.push(`Unknown predicate on ${step.name}: ${step.conditionRef}`);
      }
      if (!stepById.has(step.ifTrue.activityRef)) {
        issues.push(`Unknown true activity on ${step.name}: ${step.ifTrue.activityRef}`);
      }
      if (!stepById.has(step.ifFalse.activityRef)) {
        issues.push(`Unknown false activity on ${step.name}: ${step.ifFalse.activityRef}`);
      }
    }
    if (step.type === "parallel") {
      const branchNames = Object.keys(step.branches);
      if (branchNames.length === 0) issues.push(`Parallel node ${step.name} needs a branch.`);
      for (const [branchName, branch] of Object.entries(step.branches)) {
        if (!stepById.has(branch.activityRef)) {
          issues.push(
            `Unknown parallel branch activity on ${step.name}.${branchName}: ${branch.activityRef}`,
          );
        }
      }
    }
  }
  for (const step of schema.steps) {
    if (!("dependsOn" in step)) continue;
    for (const dep of step.dependsOn) {
      if (!names.has(dep)) issues.push(`${step.name} depends on missing step ${dep}`);
    }
  }
  if (hasDependencyCycle(schema)) issues.push("Workflow graph contains a dependency cycle.");
  return issues;
}

function inferTestInput(schema: WorkflowSchema, stepById: Map<string, CatalogStep>): unknown {
  if (schema.inputSchema) return sampleFromSchema(schema.inputSchema);
  const rootSchema = firstRootInputSchema(schema, stepById);
  if (rootSchema) return sampleFromSchema(rootSchema);
  return { text: "hello workflow" };
}

function describeWorkflowInput(schema: WorkflowSchema, stepById: Map<string, CatalogStep>): string {
  if (schema.inputSchema) return `workflow: ${schemaTypeLabel(schema.inputSchema)}`;
  const root = firstRootInput(schema, stepById);
  if (!root) return "input: inferred sample";
  return `${root.step.name}: ${schemaTypeLabel(root.entry.inputSchema)}`;
}

function firstRootInputSchema(
  schema: WorkflowSchema,
  stepById: Map<string, CatalogStep>,
): JsonSchema | undefined {
  return firstRootInput(schema, stepById)?.entry.inputSchema;
}

function firstRootInput(
  schema: WorkflowSchema,
  stepById: Map<string, CatalogStep>,
): { step: DependableStep; entry: CatalogStep & { inputSchema: JsonSchema } } | undefined {
  for (const step of schema.steps) {
    if (!hasDependsOn(step) || step.dependsOn.length > 0 || !("activityRef" in step)) continue;
    const entry = stepById.get(step.activityRef);
    if (entry?.inputSchema) {
      return { step, entry: entry as CatalogStep & { inputSchema: JsonSchema } };
    }
  }
  return undefined;
}

function sampleFromSchema(schema: JsonSchema | undefined): unknown {
  if (!schema) return { text: "hello workflow" };
  if (schema.default !== undefined) return schema.default;
  if (schema.enum?.length) return schema.enum[0];
  if (schema.type === "string") return "hello workflow";
  if (schema.type === "number" || schema.type === "integer") return 1;
  if (schema.type === "boolean") return true;
  if (schema.type === "array") return [sampleFromSchema(schema.items)];
  if (schema.type === "object") {
    const out: Record<string, unknown> = {};
    for (const [name, property] of Object.entries(schema.properties ?? {})) {
      out[name] = sampleFromSchema(property);
    }
    return out;
  }
  return { text: "hello workflow" };
}

function schemaTypeLabel(schema: JsonSchema | undefined): string {
  if (!schema) return "unknown";
  if (schema.type === "object" && schema.properties) {
    const keys = Object.keys(schema.properties);
    return keys.length > 0 ? `object { ${keys.join(", ")} }` : "object";
  }
  if (schema.type === "array") return "array";
  return schema.type ?? "value";
}

function jsonSchemaTypeCompatible(actual: JsonSchema, expected: JsonSchema): boolean {
  if (!actual.type || !expected.type) return true;
  if (actual.type === expected.type) return true;
  return false;
}

function isEditableStep(step: BuilderStep): step is EditableStep {
  return "activityRef" in step && "dependsOn" in step;
}

function hasDependsOn(step: BuilderStep): step is DependableStep {
  return "dependsOn" in step;
}

function wouldCreateCycle(schema: WorkflowSchema, from: string, to: string): boolean {
  const steps = schema.steps.map((step) =>
    step.name === to && hasDependsOn(step)
      ? ({ ...step, dependsOn: [...new Set([...step.dependsOn, from])] } as BuilderStep)
      : step,
  );
  return hasDependencyCycle({ ...schema, steps });
}

function hasDependencyCycle(schema: WorkflowSchema): boolean {
  const depsByName = new Map(
    schema.steps.map((step) => [step.name, hasDependsOn(step) ? step.dependsOn : []]),
  );
  const visiting = new Set<string>();
  const visited = new Set<string>();

  function visit(name: string): boolean {
    if (visited.has(name)) return false;
    if (visiting.has(name)) return true;
    visiting.add(name);
    for (const dep of depsByName.get(name) ?? []) {
      if (depsByName.has(dep) && visit(dep)) return true;
    }
    visiting.delete(name);
    visited.add(name);
    return false;
  }

  return [...depsByName.keys()].some((name) => visit(name));
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
