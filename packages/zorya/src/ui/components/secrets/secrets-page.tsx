// ---------------------------------------------------------------------------
// SecretsPage — operator UI for managing scoped secrets (h1st Phase 3).
//
// Three tabs (Global / Namespace / Resource). Each tab:
//   - List of stored keys at that scope (values never shown)
//   - Form to add a new key+value
//   - Delete button per row
//
// Scope-cascade visualization (showing inherited keys from parent scopes
// in the Namespace / Resource tabs) is a future polish — current cut is
// the minimum viable surface that drives the HTTP CRUD routes.
// ---------------------------------------------------------------------------

import { useEffect, useMemo, useState } from "preact/hooks";
import { confirm } from "../../lib/dialogs.ts";
import { useFetch } from "../../hooks/use-fetch.ts";
import { secretsApi, type SecretScopeWire } from "../../api/client.ts";
import { Page } from "../ui/page.tsx";
import { SkeletonRows } from "../ui/skeleton.tsx";

type ScopeKind = "global" | "namespace" | "resource";

export function SecretsPage() {
  const [tab, setTab] = useState<ScopeKind>("global");
  const [namespaceId, setNamespaceId] = useState("");
  const [resourceId, setResourceId] = useState("");

  const scope: SecretScopeWire = useMemo(() => {
    if (tab === "global") return { kind: "global" };
    if (tab === "namespace") {
      return { kind: "namespace", namespaceId: namespaceId || "" };
    }
    return { kind: "resource", namespaceId: namespaceId || "", resourceId: resourceId || "" };
  }, [tab, namespaceId, resourceId]);

  return (
    <Page>
      <div>
        <h2 class="text-xl font-semibold">Secrets</h2>
        <p class="text-xs text-base-content/50">
          API keys, tokens, and credentials. Values are encrypted at rest. Never readable through
          the API — only key names are returned.
        </p>
      </div>

      <div role="tablist" class="tabs tabs-boxed w-fit">
        <button
          role="tab"
          class={`tab ${tab === "global" ? "tab-active" : ""}`}
          onClick={() => setTab("global")}
        >
          Global
        </button>
        <button
          role="tab"
          class={`tab ${tab === "namespace" ? "tab-active" : ""}`}
          onClick={() => setTab("namespace")}
        >
          Namespace
        </button>
        <button
          role="tab"
          class={`tab ${tab === "resource" ? "tab-active" : ""}`}
          onClick={() => setTab("resource")}
        >
          Resource
        </button>
      </div>

      {tab === "namespace" && (
        <div class="flex gap-2 max-w-md">
          <label class="form-control w-full">
            <span class="text-xs text-base-content/60 mb-1">Namespace</span>
            <input
              class="input input-bordered input-sm font-mono"
              placeholder="e.g. acme"
              value={namespaceId}
              onInput={(e) => setNamespaceId((e.target as HTMLInputElement).value)}
            />
          </label>
        </div>
      )}

      {tab === "resource" && (
        <div class="grid grid-cols-2 gap-2 max-w-2xl">
          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1">Namespace</span>
            <input
              class="input input-bordered input-sm font-mono"
              placeholder="e.g. acme"
              value={namespaceId}
              onInput={(e) => setNamespaceId((e.target as HTMLInputElement).value)}
            />
          </label>
          <label class="form-control">
            <span class="text-xs text-base-content/60 mb-1">Resource</span>
            <input
              class="input input-bordered input-sm font-mono"
              placeholder="e.g. alice"
              value={resourceId}
              onInput={(e) => setResourceId((e.target as HTMLInputElement).value)}
            />
          </label>
        </div>
      )}

      {scopeReady(tab, scope) ? <ScopePanel scope={scope} /> : <ScopeUnready tab={tab} />}
    </Page>
  );
}

function scopeReady(tab: ScopeKind, scope: SecretScopeWire): boolean {
  if (tab === "global") return true;
  if (scope.kind === "namespace") return scope.namespaceId.length > 0;
  if (scope.kind === "resource") {
    return scope.namespaceId.length > 0 && scope.resourceId.length > 0;
  }
  return false;
}

function ScopeUnready({ tab }: { tab: ScopeKind }) {
  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body py-8 text-center text-base-content/50">
        {tab === "namespace" && "Enter a namespace id to view or add secrets at this scope."}
        {tab === "resource" && "Enter both namespace and resource ids to view or add secrets."}
      </div>
    </div>
  );
}

interface ScopePanelProps {
  scope: SecretScopeWire;
}

function ScopePanel({ scope }: ScopePanelProps) {
  // Refetch when the scope value changes — useFetch's deps array.
  const scopeKey = scopeCacheKey(scope);
  const { data, loading, error, refresh } = useFetch(
    () => secretsApi.list(scope),
    [scopeKey],
    0, // no auto-refresh — operator-driven
  );
  const [newKey, setNewKey] = useState("");
  const [newValue, setNewValue] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [showValue, setShowValue] = useState(false);

  // Reset form when scope changes.
  useEffect(() => {
    setNewKey("");
    setNewValue("");
    setSubmitError(null);
  }, [scopeKey]);

  const onSubmit = async (e: Event) => {
    e.preventDefault();
    if (!newKey.trim() || !newValue) return;
    setSubmitting(true);
    setSubmitError(null);
    try {
      await secretsApi.create({ scope, key: newKey.trim(), value: newValue });
      setNewKey("");
      setNewValue("");
      refresh();
    } catch (err) {
      setSubmitError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  };

  const onDelete = async (key: string) => {
    if (
      !(await confirm({
        title: `Delete "${key}"`,
        message: "Values cannot be recovered.",
        variant: "danger",
        confirmLabel: "Delete",
      }))
    )
      return;
    try {
      await secretsApi.delete(scope, key);
      refresh();
    } catch (err) {
      setSubmitError(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <div class="space-y-4">
      <div class="card bg-base-100 shadow">
        <div class="card-body p-4 space-y-3">
          <h3 class="text-sm font-semibold">Add a secret</h3>
          <form class="flex flex-wrap items-end gap-2" onSubmit={onSubmit}>
            <label class="form-control flex-1 min-w-48">
              <span class="text-xs text-base-content/60 mb-1">Key</span>
              <input
                class="input input-bordered input-sm font-mono"
                placeholder="anthropic_api_key"
                value={newKey}
                onInput={(e) => setNewKey((e.target as HTMLInputElement).value)}
                pattern="[A-Za-z][A-Za-z0-9_\-.]{0,127}"
                required
              />
            </label>
            <label class="form-control flex-1 min-w-64">
              <span class="text-xs text-base-content/60 mb-1">Value</span>
              <input
                class="input input-bordered input-sm font-mono"
                type={showValue ? "text" : "password"}
                placeholder="sk-ant-..."
                value={newValue}
                onInput={(e) => setNewValue((e.target as HTMLInputElement).value)}
                required
              />
            </label>
            <button
              type="button"
              class="btn btn-sm btn-ghost"
              onClick={() => setShowValue((v) => !v)}
              tabIndex={-1}
            >
              {showValue ? "Hide" : "Show"}
            </button>
            <button
              type="submit"
              class="btn btn-sm btn-primary"
              disabled={submitting || !newKey.trim() || !newValue}
            >
              {submitting ? "Storing…" : "Store"}
            </button>
          </form>
          {submitError && <div class="alert alert-error text-xs">{submitError}</div>}
        </div>
      </div>

      <div class="card bg-base-100 shadow overflow-hidden">
        <div class="overflow-x-auto">
          <table class="table">
            <thead>
              <tr class="bg-base-200 text-xs uppercase tracking-wider text-base-content/50">
                <th>Key</th>
                <th class="text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading && !data ? (
                <SkeletonRows rows={3} cols={2} />
              ) : error ? (
                <tr>
                  <td colSpan={2} class="text-center text-error py-8">
                    {error.message}
                  </td>
                </tr>
              ) : data && data.keys.length === 0 ? (
                <tr>
                  <td colSpan={2} class="text-center text-base-content/50 py-8">
                    No secrets stored at this scope.
                  </td>
                </tr>
              ) : (
                (data?.keys ?? []).map((key) => (
                  <tr class="hover:bg-base-200">
                    <td class="font-mono text-sm">{key}</td>
                    <td class="text-right">
                      <button class="btn btn-xs btn-ghost text-error" onClick={() => onDelete(key)}>
                        Delete
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function scopeCacheKey(scope: SecretScopeWire): string {
  if (scope.kind === "global") return "global";
  if (scope.kind === "namespace") return `namespace:${scope.namespaceId}`;
  return `resource:${scope.namespaceId}:${scope.resourceId}`;
}
