// ---------------------------------------------------------------------------
// BuilderEditorPage — edit one builder workflow (`#/builder/<name>`).
//
// The page holds the draft schema; the canvas, inspector, palette and JSON
// view all edit it through the pure functions in lib/workflow-builder-*.
// Live validation marks problems on their nodes; the server re-validates
// and compiles on save.
//
//   Save      PUT the draft with the revision it was loaded at. Someone
//             else's newer save answers 409: the page offers to reload
//             theirs or overwrite it — nothing is merged silently.
//   Publish   save if needed, then publish the draft as the next version
//             (promoted, so new runs start with it).
//   Test run  publish if the draft has unpublished changes, then start a
//             run through the normal trigger route and open it.
//
// The workflow is identified by the route; renaming is "Save as", which
// creates a new workflow and leaves this one (and its runs) in place.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import type { ActivityDescriptor, WorkflowSchema } from "@promin/workflow";
import type { ApiIssue } from "../../../server/api-types.ts";
import type { BuilderWorkflow } from "../../../server/routes/workflow-builder.ts";
import { api, ApiError } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { useNamespace } from "../../hooks/use-namespace.ts";
import { confirm, prompt, toast } from "../../lib/dialogs.ts";
import {
  INPUT_NODE_ID,
  addStep,
  autoLayout,
  connect,
  disconnect,
  moveStep,
  removeStep,
  renameStep,
  withLayout,
} from "../../lib/workflow-builder-graph.ts";
import {
  describeWorkflowInput,
  inputSchemaTemplate,
  parseJsonInput,
  suggestedTestInput,
} from "../../lib/workflow-builder-input.ts";
import {
  WORKFLOW_NAME_PATTERN,
  canSave,
  validateDraft,
  type DraftIssue,
} from "../../lib/workflow-builder-validation.ts";
import { Page } from "../ui/page.tsx";
import { BuilderCanvas, type PaletteDrop } from "./builder-canvas.tsx";
import { BuilderInspector } from "./builder-inspector.tsx";
import { errorMessage } from "./builder-list-page.tsx";
import { BuilderPalette } from "./builder-palette.tsx";

type Busy = "save" | "publish" | "run" | "delete" | null;

export function BuilderEditorPage(props: {
  name: string;
  onBack: () => void;
  onOpenRun: (id: string) => void;
  onOpenWorkflow: (name: string) => void;
  onOpenBuilder: (name: string) => void;
}) {
  const { name } = props;
  const [namespace] = useNamespace();
  const { data: activityData } = useFetch(() => api.listBuilderActivities(), [], 0);
  const activities = activityData?.activities ?? [];
  const activityById = useMemo(
    () => new Map<string, ActivityDescriptor>(activities.map((a) => [a.id, a])),
    [activities],
  );

  const [server, setServer] = useState<BuilderWorkflow | undefined>();
  const [loadError, setLoadError] = useState<string | undefined>();
  const [draft, setDraft] = useState<WorkflowSchema | undefined>();
  const [dirty, setDirty] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [mode, setMode] = useState<"canvas" | "json">("canvas");
  const [jsonText, setJsonText] = useState("");
  const [jsonError, setJsonError] = useState<string | undefined>();
  const [busy, setBusy] = useState<Busy>(null);
  const [serverIssues, setServerIssues] = useState<ReadonlyArray<ApiIssue>>([]);
  const [conflict, setConflict] = useState<number | null | undefined>(undefined);
  const [testInput, setTestInput] = useState("");
  const [testInputTouched, setTestInputTouched] = useState(false);

  const adopt = (workflow: BuilderWorkflow) => {
    setServer(workflow);
    setDraft(withLayout(workflow.schema));
    setDirty(false);
    setConflict(undefined);
    setServerIssues([]);
  };

  const load = async () => {
    try {
      adopt(await api.getBuilderWorkflow(name));
      setLoadError(undefined);
    } catch (err) {
      setLoadError(errorMessage(err));
    }
  };

  useEffect(() => {
    setServer(undefined);
    setDraft(undefined);
    setSelected(null);
    void load();
  }, [name]);

  // Warn before leaving the page with unsaved edits.
  useEffect(() => {
    if (!dirty) return;
    const onLeave = (e: BeforeUnloadEvent) => e.preventDefault();
    window.addEventListener("beforeunload", onLeave);
    return () => window.removeEventListener("beforeunload", onLeave);
  }, [dirty]);

  const issues = useMemo<DraftIssue[]>(
    () => (draft ? validateDraft({ schema: draft, activities: activityById }) : []),
    [draft, activityById],
  );
  const inputLabel = useMemo(
    () => (draft ? describeWorkflowInput({ schema: draft, activities: activityById }) : ""),
    [draft, activityById],
  );
  const inputTemplate = useMemo(
    () => (draft ? inputSchemaTemplate({ schema: draft, activities: activityById }) : {}),
    [draft, activityById],
  );
  const suggested = useMemo(
    () =>
      JSON.stringify(
        draft ? suggestedTestInput({ schema: draft, activities: activityById }) : {},
        null,
        2,
      ),
    [draft, activityById],
  );
  useEffect(() => {
    if (!testInputTouched) setTestInput(suggested);
  }, [suggested, testInputTouched]);

  if (loadError) {
    return (
      <Page>
        <BackBar name={name} onBack={props.onBack} />
        <div class="alert alert-error text-sm">{loadError}</div>
      </Page>
    );
  }
  if (!server || !draft) {
    return (
      <Page>
        <BackBar name={name} onBack={props.onBack} />
        <div class="text-sm text-base-content/50">Loading…</div>
      </Page>
    );
  }

  const edit = (next: WorkflowSchema) => {
    setDraft(next);
    setDirty(true);
    setServerIssues([]);
  };
  const editOr = (result: { ok: true; schema: WorkflowSchema } | { ok: false; error: string }) => {
    if (result.ok) edit(result.schema);
    else toast(result.error, { variant: "warning" });
  };

  const handleError = async (err: unknown): Promise<void> => {
    if (err instanceof ApiError && err.code === "revision_conflict") {
      const latest = await api.getBuilderWorkflow(name).catch(() => undefined);
      setConflict(latest?.revision ?? null);
      return;
    }
    if (err instanceof ApiError && err.issues && err.issues.length > 0) {
      setServerIssues(err.issues);
      return;
    }
    toast(errorMessage(err), { variant: "error" });
  };

  /** Save the draft; resolves to the saved workflow, or undefined when refused. */
  const save = async (revision = server.revision): Promise<BuilderWorkflow | undefined> => {
    const saved = await api.saveBuilderWorkflow(name, { schema: draft, revision });
    setServer(saved);
    setDirty(false);
    setConflict(undefined);
    setServerIssues([]);
    return saved;
  };

  const run = async (kind: Exclude<Busy, null>, fn: () => Promise<void>) => {
    setBusy(kind);
    try {
      await fn();
    } catch (err) {
      await handleError(err);
    } finally {
      setBusy(null);
    }
  };

  const publish = async (): Promise<string> => {
    const current = dirty ? await save() : server;
    const result = await api.publishBuilderWorkflow(name, { revision: current!.revision });
    setServer(result.workflow);
    toast(
      result.created
        ? `Published version ${result.version}`
        : `No changes since version ${result.version}`,
      { variant: result.created ? "success" : "info" },
    );
    return result.version;
  };

  const testRun = async () => {
    const parsed = parseJsonInput(testInput);
    if (!parsed.ok) {
      toast(`Test input is not valid JSON: ${parsed.error}`, { variant: "error" });
      return;
    }
    const version =
      dirty || server.unpublishedChanges
        ? await publish()
        : (server.activeVersion ?? server.versions.at(-1)?.version);
    const res = await api.triggerWorkflow(name, {
      input: parsed.value,
      ...(version !== undefined && { version }),
      ...(namespace ? { namespace } : {}),
    });
    props.onOpenRun(res.workflowId);
  };

  const saveAs = async () => {
    const next = await prompt({
      title: "Save as a new workflow",
      label: "Name",
      initial: `${name}-copy`,
      confirmLabel: "Create",
    });
    if (!next) return;
    if (!WORKFLOW_NAME_PATTERN.test(next)) {
      toast("Use 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit.", {
        variant: "error",
      });
      return;
    }
    await run("save", async () => {
      await api.createBuilderWorkflow({ ...draft, name: next });
      toast(`Created "${next}"; "${name}" is unchanged`, { variant: "success" });
      props.onOpenBuilder(next);
    });
  };

  const remove = async () => {
    const ok = await confirm({
      title: `Delete workflow "${name}"`,
      message:
        "This deletes the draft and every published version. It is refused while runs of it have not ended.",
      confirmLabel: "Delete",
      variant: "danger",
    });
    if (!ok) return;
    await run("delete", async () => {
      await api.deleteBuilderWorkflow(name);
      setDirty(false);
      toast(`Deleted "${name}"`, { variant: "success" });
      props.onBack();
    });
  };

  const onPaletteAdd = (item: PaletteDrop, at?: { x: number; y: number }) => {
    const activity = item.activityId ? activityById.get(item.activityId) : undefined;
    const result = addStep({
      schema: draft,
      kind: item.kind,
      activities,
      ...(activity && { activity }),
      ...(at && { position: at }),
    });
    // A new node lands wired after the selected one, ready to run.
    const linked =
      selected && selected !== INPUT_NODE_ID
        ? connect({ schema: result.schema, from: selected, to: result.name })
        : { ok: true as const, schema: result.schema };
    edit(linked.ok ? linked.schema : result.schema);
    setSelected(result.name);
  };

  const errors = issues.filter((i) => i.severity === "error");
  const published = server.versions.length > 0;

  return (
    <Page>
      <div class="flex flex-wrap items-center gap-3">
        <BackBar name={name} onBack={props.onBack} />
        {server.activeVersion && (
          <span class="badge badge-sm badge-success badge-outline font-mono">
            active v{server.activeVersion}
          </span>
        )}
        {!published && <span class="badge badge-sm badge-ghost">never published</span>}
        {published && server.unpublishedChanges && (
          <span class="badge badge-sm badge-warning badge-outline">unpublished changes</span>
        )}
        {dirty && <span class="badge badge-sm badge-info badge-outline">unsaved</span>}
        <div class="flex-1" />
        <div class="join">
          <button
            type="button"
            class={`btn btn-sm join-item ${mode === "canvas" ? "btn-active" : ""}`}
            onClick={() => setMode("canvas")}
          >
            Canvas
          </button>
          <button
            type="button"
            class={`btn btn-sm join-item ${mode === "json" ? "btn-active" : ""}`}
            onClick={() => {
              setJsonText(JSON.stringify(draft, null, 2));
              setJsonError(undefined);
              setMode("json");
            }}
          >
            JSON
          </button>
        </div>
        <button
          type="button"
          class="btn btn-sm btn-outline"
          disabled={busy !== null || !dirty || !canSave(issues)}
          title={!canSave(issues) ? "Fix the errors first" : ""}
          onClick={() => run("save", async () => void (await save()))}
        >
          {busy === "save" ? "Saving…" : "Save"}
        </button>
        <button
          type="button"
          class="btn btn-sm btn-primary"
          disabled={busy !== null || !canSave(issues)}
          onClick={() => run("publish", async () => void (await publish()))}
        >
          {busy === "publish" ? "Publishing…" : "Publish"}
        </button>
        <div class="dropdown dropdown-end">
          <button type="button" tabIndex={0} class="btn btn-sm btn-ghost">
            ⋯
          </button>
          <ul
            tabIndex={0}
            class="dropdown-content menu menu-sm z-10 w-48 rounded-box bg-base-100 p-2 shadow"
          >
            <li>
              <button type="button" onClick={saveAs}>
                Save as…
              </button>
            </li>
            <li>
              <button type="button" onClick={() => edit(autoLayout(draft))}>
                Tidy layout
              </button>
            </li>
            {published && (
              <li>
                <button type="button" onClick={() => props.onOpenWorkflow(name)}>
                  Open workflow page
                </button>
              </li>
            )}
            <li>
              <button type="button" class="text-error" onClick={remove}>
                Delete
              </button>
            </li>
          </ul>
        </div>
      </div>

      {server.loadError && (
        <div class="alert alert-warning text-xs">
          Some published versions could not be loaded: {server.loadError}
        </div>
      )}
      {conflict !== undefined && (
        <div class="alert alert-warning text-xs flex flex-wrap items-center justify-between gap-2">
          <span>
            Someone else saved this workflow
            {conflict !== null ? ` (now revision ${conflict})` : ""} since you opened it. Your edits
            were not saved.
          </span>
          <span class="flex gap-2">
            <button type="button" class="btn btn-xs" onClick={() => void load()}>
              Reload theirs
            </button>
            {conflict !== null && (
              <button
                type="button"
                class="btn btn-xs btn-warning"
                onClick={() => run("save", async () => void (await save(conflict)))}
              >
                Overwrite with mine
              </button>
            )}
          </span>
        </div>
      )}

      {mode === "json" ? (
        <div class="card bg-base-100 shadow p-3 space-y-2">
          <textarea
            class="textarea textarea-bordered min-h-[32rem] w-full font-mono text-xs leading-relaxed"
            spellcheck={false}
            value={jsonText}
            onInput={(e) => {
              setJsonText((e.target as HTMLTextAreaElement).value);
              setJsonError(undefined);
            }}
          />
          <div class="flex items-center justify-between gap-2">
            <span class="text-xs text-error">{jsonError ?? ""}</span>
            <button
              type="button"
              class="btn btn-sm btn-outline"
              onClick={() => {
                const parsed = parseJsonInput(jsonText);
                if (!parsed.ok) return setJsonError(parsed.error);
                const value = parsed.value as Partial<WorkflowSchema> | undefined;
                if (!value || typeof value !== "object" || !Array.isArray(value.steps)) {
                  return setJsonError("Expected a WorkflowSchema object with a steps array.");
                }
                if (value.name !== name) {
                  return setJsonError(`"name" must stay "${name}"; use Save as to rename.`);
                }
                edit(withLayout(value as WorkflowSchema));
                setMode("canvas");
              }}
            >
              Apply JSON
            </button>
          </div>
        </div>
      ) : (
        <div class="grid grid-cols-1 items-start gap-4 xl:grid-cols-[14rem_minmax(0,1fr)_20rem]">
          <BuilderPalette activities={activities} onAdd={(item) => onPaletteAdd(item)} />
          <BuilderCanvas
            schema={draft}
            activities={activityById}
            inputLabel={inputLabel}
            issues={issues}
            selected={selected}
            onSelect={setSelected}
            onMove={({ name: step, x, y }) => {
              setDraft((d) => (d ? moveStep({ schema: d, name: step, x, y }) : d));
              setDirty(true);
            }}
            onConnect={(edge) => {
              editOr(connect({ schema: draft, ...edge }));
              setSelected(edge.to);
            }}
            onDisconnect={(edge) => editOr(disconnect({ schema: draft, ...edge }))}
            onDrop={({ item, at }) => onPaletteAdd(item, at)}
            onDelete={(step) => {
              edit(removeStep({ schema: draft, name: step }));
              setSelected(null);
            }}
          />
          <BuilderInspector
            schema={draft}
            selected={selected}
            activities={activities}
            issues={issues}
            inputLabel={inputLabel}
            inputTemplate={inputTemplate}
            onChange={edit}
            onRename={({ from, to }) => {
              const result = renameStep({ schema: draft, from, to });
              if (!result.ok) return result.error;
              edit(result.schema);
              setSelected(to.trim());
              return undefined;
            }}
            onRemove={(step) => {
              edit(removeStep({ schema: draft, name: step }));
              setSelected(null);
            }}
          />
        </div>
      )}

      <div class="grid grid-cols-1 gap-4 lg:grid-cols-[minmax(0,1fr)_24rem]">
        <div class="card bg-base-100 shadow">
          <div class="border-b border-base-300 px-4 py-2 text-xs uppercase tracking-wider text-base-content/50">
            Problems{" "}
            <span class="badge badge-sm badge-ghost">{issues.length + serverIssues.length}</span>
          </div>
          <ul class="divide-y divide-base-300 text-xs">
            {issues.length === 0 && serverIssues.length === 0 && (
              <li class="px-4 py-3 text-success">No problems found.</li>
            )}
            {serverIssues.map((issue, i) => (
              <li key={`s${i}`} class="px-4 py-2 text-error">
                <span class="font-mono opacity-60">server · {issue.path}</span> {issue.message}
              </li>
            ))}
            {issues.map((issue, i) => (
              <li
                key={i}
                class={`px-4 py-2 cursor-pointer hover:bg-base-200 ${issue.severity === "error" ? "text-error" : "text-warning"}`}
                onClick={() => setSelected(issue.input ? INPUT_NODE_ID : (issue.step ?? null))}
              >
                {issue.step && <span class="font-mono opacity-70">{issue.step} · </span>}
                {issue.message}
              </li>
            ))}
          </ul>
        </div>

        <div class="card bg-base-100 shadow p-4 space-y-2">
          <div class="flex items-center justify-between">
            <span class="text-xs uppercase tracking-wider text-base-content/50">Test run</span>
            <span class="font-mono text-[11px] text-base-content/45">{inputLabel}</span>
          </div>
          <textarea
            class="textarea textarea-bordered min-h-24 font-mono text-xs"
            spellcheck={false}
            value={testInput}
            onInput={(e) => {
              setTestInputTouched(true);
              setTestInput((e.target as HTMLTextAreaElement).value);
            }}
          />
          <div class="flex justify-end gap-2">
            <button
              type="button"
              class="btn btn-sm btn-ghost"
              onClick={() => {
                setTestInputTouched(false);
                setTestInput(suggested);
              }}
            >
              Use sample
            </button>
            <button
              type="button"
              class="btn btn-sm btn-secondary"
              disabled={busy !== null || errors.length > 0}
              title={
                dirty || server.unpublishedChanges
                  ? "Publishes the draft as a new version first"
                  : "Runs the active version"
              }
              onClick={() => run("run", testRun)}
            >
              {busy === "run" ? "Starting…" : "Run"}
            </button>
          </div>
        </div>
      </div>
    </Page>
  );
}

function BackBar(props: { name: string; onBack: () => void }) {
  return (
    <div class="flex items-center gap-3">
      <button type="button" class="btn btn-sm btn-ghost" onClick={props.onBack}>
        ← Builder
      </button>
      <div class="text-base-content/40">/</div>
      <div class="font-semibold text-lg font-mono">{props.name}</div>
    </div>
  );
}
