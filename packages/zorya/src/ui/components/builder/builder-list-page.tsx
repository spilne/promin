// ---------------------------------------------------------------------------
// BuilderListPage — the workflows authored in the visual builder: their
// published and active versions, unpublished changes, and load problems.
// New workflows start from a one-step template on the server's first
// activity. When the server has no builder configured the API answers 404
// and the page says so.
// ---------------------------------------------------------------------------

import { api, ApiError } from "../../api/client.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { confirm, prompt, toast } from "../../lib/dialogs.ts";
import { formatRelative } from "../../lib/format.ts";
import { starterSchema } from "../../lib/workflow-builder-graph.ts";
import { WORKFLOW_NAME_PATTERN } from "../../lib/workflow-builder-validation.ts";
import { EmptyState } from "../ui/empty-state.tsx";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";

export function BuilderListPage(props: {
  onOpen: (name: string) => void;
  onOpenRuns: (name: string) => void;
}) {
  const { data, loading, error, refresh } = useFetch(() => api.listBuilderWorkflows(), [], 0);
  const notConfigured = error instanceof ApiError && error.status === 404;
  const workflows = data?.workflows ?? [];

  const create = async () => {
    const name = await prompt({
      title: "New workflow",
      label: "Name (letters, digits, '.', '_' or '-')",
      placeholder: "my-workflow",
      confirmLabel: "Create",
    });
    if (!name) return;
    if (!WORKFLOW_NAME_PATTERN.test(name)) {
      toast("Use 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit.", {
        variant: "error",
      });
      return;
    }
    try {
      const { activities } = await api.listBuilderActivities();
      await api.createBuilderWorkflow(starterSchema({ name, activities }));
      props.onOpen(name);
    } catch (err) {
      toast(errorMessage(err), { variant: "error" });
    }
  };

  const remove = async (name: string) => {
    const ok = await confirm({
      title: `Delete workflow "${name}"`,
      message:
        "This deletes the draft and every published version, and the workflow can no longer be triggered. Runs that have ended stay in history.",
      confirmLabel: "Delete",
      variant: "danger",
    });
    if (!ok) return;
    try {
      await api.deleteBuilderWorkflow(name);
      toast(`Deleted "${name}"`, { variant: "success" });
      refresh();
    } catch (err) {
      toast(errorMessage(err), { variant: "error" });
    }
  };

  return (
    <Page>
      <div class="flex items-start justify-between gap-4">
        <div>
          <h2 class="text-xl font-semibold">Workflow builder</h2>
          <p class="text-xs text-base-content/50 max-w-2xl">
            Compose workflows from the server's activities on a canvas, publish them as versions,
            and run them like any other workflow. Each publish is a new version on the workflow's
            Versions panel; runs finish on the version they started with.
          </p>
        </div>
        <div class="flex gap-2">
          <button type="button" class="btn btn-sm btn-ghost" onClick={refresh} title="Refresh">
            ↻
          </button>
          {!notConfigured && (
            <button type="button" class="btn btn-sm btn-primary" onClick={create}>
              New workflow
            </button>
          )}
        </div>
      </div>

      {notConfigured ? (
        <div class="card bg-base-100 shadow">
          <EmptyState
            message="The workflow builder is not configured on this server."
            hint="Pass a ZoryaWorkflowBuilder (with an activity catalog) to ZoryaServer.builder().workflowBuilder(…)."
          />
        </div>
      ) : (
        <>
          {error && <div class="alert alert-error text-xs">{error.message}</div>}
          <div class="card bg-base-100 shadow overflow-x-auto">
            <table class="table table-sm">
              <thead>
                <tr>
                  <th>Workflow</th>
                  <th>Steps</th>
                  <th>Latest</th>
                  <th>Active</th>
                  <th>Status</th>
                  <th>Edited</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {loading && !data ? (
                  <SkeletonRows rows={3} cols={7} />
                ) : workflows.length === 0 ? (
                  <tr>
                    <td colSpan={7}>
                      <EmptyState
                        message="No builder workflows yet."
                        hint="Create one, add steps from the palette, then publish it."
                      />
                    </td>
                  </tr>
                ) : (
                  workflows.map((w) => (
                    <tr
                      key={w.name}
                      class="hover cursor-pointer"
                      onClick={() => props.onOpen(w.name)}
                    >
                      <td class="font-mono text-xs">{w.name}</td>
                      <td>{w.stepCount}</td>
                      <td class="font-mono text-xs">
                        {w.latestVersion ? `v${w.latestVersion}` : "—"}
                      </td>
                      <td class="font-mono text-xs">
                        {w.activeVersion ? `v${w.activeVersion}` : "—"}
                      </td>
                      <td class="space-x-1">
                        {!w.latestVersion ? (
                          <span class="badge badge-sm badge-ghost">draft</span>
                        ) : w.unpublishedChanges ? (
                          <span class="badge badge-sm badge-warning badge-outline">
                            unpublished changes
                          </span>
                        ) : (
                          <span class="badge badge-sm badge-success badge-outline">published</span>
                        )}
                        {w.loadError && (
                          <span class="badge badge-sm badge-error" title={w.loadError}>
                            load error
                          </span>
                        )}
                      </td>
                      <td class="text-xs text-base-content/60">
                        {formatRelative(new Date(w.updatedAt).toISOString())}
                      </td>
                      <td class="text-right whitespace-nowrap" onClick={(e) => e.stopPropagation()}>
                        {w.latestVersion && (
                          <button
                            type="button"
                            class="btn btn-xs btn-ghost"
                            onClick={() => props.onOpenRuns(w.name)}
                          >
                            Runs
                          </button>
                        )}
                        <button
                          type="button"
                          class="btn btn-xs btn-ghost text-error"
                          onClick={() => remove(w.name)}
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </Page>
  );
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
