import { useMemo, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import type {
  AuthoredWorkflowDto,
  WorkflowStepCatalogResponse,
} from "../../../server/routes/workflow-builder.ts";
import { EmptyState } from "../ui/empty-state.tsx";
import { Page, PageHeader } from "../ui/page.tsx";

const SAMPLE_SCHEMA = {
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
}

export function WorkflowBuilderPage({ onOpenWorkflow }: WorkflowBuilderPageProps) {
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
  const [version, setVersion] = useState("v1");
  const [schemaJson, setSchemaJson] = useState(() => JSON.stringify(SAMPLE_SCHEMA, null, 2));
  const [selected, setSelected] = useState<AuthoredWorkflowDto | null>(null);
  const [busy, setBusy] = useState<"save" | "publish" | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const steps = catalog?.steps ?? [];
  const workflows = authored?.workflows ?? [];
  const configured = catalogLoading || steps.length > 0 || workflows.length > 0;

  const parsedName = useMemo(() => {
    try {
      const parsed = JSON.parse(schemaJson) as { name?: unknown };
      return typeof parsed.name === "string" ? parsed.name : "";
    } catch {
      return "";
    }
  }, [schemaJson]);

  async function save(): Promise<AuthoredWorkflowDto | null> {
    setBusy("save");
    setError(null);
    setMessage(null);
    try {
      const schema = JSON.parse(schemaJson);
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

  async function publish(): Promise<void> {
    setBusy("publish");
    setError(null);
    setMessage(null);
    try {
      const saved = await save();
      if (!saved) return;
      setBusy("publish");
      const res = await api.publishAuthoredWorkflow(saved.name, {
        version: saved.version,
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
  }

  function loadWorkflow(workflow: AuthoredWorkflowDto): void {
    setSelected(workflow);
    setVersion(workflow.version);
    setSchemaJson(JSON.stringify(workflow.schema, null, 2));
    setError(null);
    setMessage(null);
  }

  function insertStep(entry: WorkflowStepCatalogResponse["steps"][number]): void {
    const stepName = toStepName(entry.id);
    try {
      const schema = JSON.parse(schemaJson) as {
        steps?: unknown;
        ui?: Record<string, unknown>;
      };
      const existingSteps = Array.isArray(schema.steps) ? schema.steps : [];
      const names = new Set(
        existingSteps
          .map((step) =>
            typeof step === "object" && step !== null && "name" in step
              ? String((step as { name?: unknown }).name)
              : "",
          )
          .filter(Boolean),
      );
      let name = stepName;
      let idx = 2;
      while (names.has(name)) {
        name = `${stepName}${idx}`;
        idx += 1;
      }
      schema.steps = [
        ...existingSteps,
        {
          type: entry.kind ?? "step",
          name,
          dependsOn: existingSteps.length > 0 ? [lastStepName(existingSteps)] : [],
          activityRef: entry.id,
          ...(entry.defaultConfig ? { config: entry.defaultConfig } : {}),
        },
      ];
      schema.ui = {
        ...(schema.ui ?? {}),
        [name]: { x: 80 + existingSteps.length * 180, y: 80, label: entry.title },
      };
      setSchemaJson(JSON.stringify(schema, null, 2));
    } catch {
      setError("Fix the workflow JSON before inserting a step.");
    }
  }

  return (
    <Page>
      <PageHeader
        title="Workflow Builder"
        eyebrow="Step catalog"
        description="Compose authored workflow definitions from registered step templates, then publish them into the workflow version registry."
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

      <div class="grid grid-cols-1 gap-4 xl:grid-cols-[360px_1fr_360px]">
        <section class="card bg-base-100/95 border border-base-content/10 shadow overflow-hidden">
          <div class="border-b border-base-content/10 px-4 py-3">
            <div class="text-xs uppercase tracking-wider text-base-content/55">Step Library</div>
          </div>
          <div class="divide-y divide-base-300">
            {catalogLoading && !catalog ? (
              <div class="p-4 text-sm text-base-content/45">Loading...</div>
            ) : steps.length === 0 ? (
              <div class="p-4">
                <EmptyState message="No step templates registered." />
              </div>
            ) : (
              steps.map((step) => (
                <button
                  class="w-full p-4 text-left hover:bg-base-200"
                  onClick={() => insertStep(step)}
                  title="Insert this step into the workflow schema"
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
                  {step.capabilities && step.capabilities.length > 0 && (
                    <div class="mt-2 flex flex-wrap gap-1">
                      {step.capabilities.map((capability) => (
                        <span class="badge badge-xs badge-ghost">{capability}</span>
                      ))}
                    </div>
                  )}
                </button>
              ))
            )}
          </div>
        </section>

        <section class="card bg-base-100/95 border border-base-content/10 shadow p-4 space-y-3">
          <div class="flex flex-col gap-2 md:flex-row md:items-end md:justify-between">
            <div>
              <div class="text-xs uppercase tracking-wider text-base-content/55">Definition</div>
              <div class="mt-1 font-mono text-sm">
                {parsedName || "unnamed"}@{version || "v1"}
              </div>
            </div>
            <label class="form-control w-full md:w-40">
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

          {error && <div class="alert alert-error text-xs">{error}</div>}
          {message && <div class="alert alert-success text-xs">{message}</div>}

          <div class="flex flex-wrap justify-end gap-2">
            <button
              class="btn btn-sm btn-ghost"
              onClick={() => {
                setSchemaJson(JSON.stringify(SAMPLE_SCHEMA, null, 2));
                setVersion("v1");
                setSelected(null);
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
          </div>
        </section>

        <section class="card bg-base-100/95 border border-base-content/10 shadow overflow-hidden">
          <div class="border-b border-base-content/10 px-4 py-3">
            <div class="text-xs uppercase tracking-wider text-base-content/55">Authored</div>
          </div>
          <div class="divide-y divide-base-300">
            {authoredLoading && !authored ? (
              <div class="p-4 text-sm text-base-content/45">Loading...</div>
            ) : workflows.length === 0 ? (
              <div class="p-4">
                <EmptyState message="No authored workflows yet." />
              </div>
            ) : (
              workflows.map((workflow) => (
                <div class={`p-4 ${selected === workflow ? "bg-base-200" : ""}`}>
                  <button class="w-full text-left" onClick={() => loadWorkflow(workflow)}>
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
                      disabled={busy !== null}
                      onClick={async () => {
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
                    >
                      Publish
                    </button>
                  </div>
                </div>
              ))
            )}
          </div>
        </section>
      </div>
    </Page>
  );
}

function toStepName(id: string): string {
  const last = id.split(".").at(-1) ?? id;
  return last.replace(/[^a-zA-Z0-9_]/g, "_") || "step";
}

function lastStepName(steps: unknown[]): string {
  for (let i = steps.length - 1; i >= 0; i -= 1) {
    const step = steps[i];
    if (typeof step === "object" && step !== null && "name" in step) {
      const name = (step as { name?: unknown }).name;
      if (typeof name === "string" && name) return name;
    }
  }
  return "";
}
