// ---------------------------------------------------------------------------
// `applyDiscovered` — reconcile what a folder scan found into a
// `VersionedRegistry`, the same way for every kind:
//
//   - A file whose body matches the last version the scanner wrote (or the
//     active version) is left alone: an unchanged file costs one read, no
//     write — and never undoes an operator's promote.
//   - A changed body is registered as a version with `source: "file"` (an
//     earlier version with the same body is reused). It becomes active when
//     the active version is the file's own (or there is none); when an
//     operator made another version active, it waits as a draft.
//   - Whether an entry is file-managed is stored, not tracked per process:
//     its active version's `source` is `"file"` (`list({ source: "file" })`).
//   - `sync` removes entries whose file is gone — but only versions the
//     scanner wrote (`source: "file"`). Operator-authored versions are never
//     deleted, and nothing is deleted at all when the scan could not read
//     every file (`complete: false`): a file that failed to import is not a
//     file that was removed.
// ---------------------------------------------------------------------------

import { sameBody } from "../versioned-registry/store-helpers.ts";
import {
  isRegistryConflict,
  isRegistryInputError,
  listAllVersioned,
  type RegisterVersionInput,
  type Versioned,
  type VersionedRegistry,
  type VersionTrust,
} from "../versioned-registry/types.ts";

/** `source` of every version a scanner writes. */
export const FILE_SOURCE = "file";

export interface ApplyDiscoveredParams<TBody, TFilter, TInput> {
  readonly registry: VersionedRegistry<TBody, TFilter, TInput>;
  readonly discovered: ReadonlyArray<RegisterVersionInput<TInput>>;
  /** Remove entries whose file is gone (scanner-written versions only). Default `false`. */
  readonly sync?: boolean;
  /**
   * Whether the scan read every file. `false` skips `sync` deletes for this
   * pass. Default `true`.
   */
  readonly complete?: boolean;
  /** Only touch ids with this prefix. */
  readonly idPrefix?: string;
  /** Review state of versions this pass creates. Default `"trusted"`. */
  readonly trust?: VersionTrust;
  /** Called before a changed entry is registered (e.g. to warn about missing env). */
  readonly beforeRegister?: (input: RegisterVersionInput<TInput>) => void;
  /** Called with the active entry when the scanned body is already stored. */
  readonly onUnchanged?: (entry: Versioned<TBody>) => Promise<void>;
}

export interface ApplyDiscoveredResult {
  /** Every id the scan found (changed or not) — the file-managed set. */
  readonly upserted: string[];
  /** Ids that did not exist before this pass. */
  readonly added: string[];
  /** Ids whose file changed this pass (a new file version was stored). */
  readonly changed: string[];
  /** Ids removed by `sync`. */
  readonly deleted: string[];
  /** Entries that could not be applied (e.g. a pinned version whose body changed). */
  readonly warnings: string[];
}

export async function applyDiscovered<TBody, TFilter, TInput>(
  params: ApplyDiscoveredParams<TBody, TFilter, TInput>,
): Promise<ApplyDiscoveredResult> {
  const { registry, idPrefix } = params;
  const inScope = (id: string): boolean => idPrefix === undefined || id.startsWith(idPrefix);
  const upserted: string[] = [];
  const added: string[] = [];
  const changed: string[] = [];
  const deleted: string[] = [];
  const warnings: string[] = [];

  for (const input of params.discovered) {
    if (!inScope(input.id)) continue;
    const versions = await registry.versions({ id: input.id });
    const active = versions.find((v) => v.active) ?? null;
    const lastFile = versions.findLast((v) => v.source === FILE_SOURCE) ?? null;
    try {
      // Unchanged since the scanner last wrote it — or already what is
      // active (an operator published the same text).
      const unchanged = [lastFile, active].find(
        (v) => v !== null && sameBody({ kind: registry.kind, entry: v, input }),
      );
      if (unchanged) {
        upserted.push(input.id);
        await params.onUnchanged?.(unchanged);
        continue;
      }
      params.beforeRegister?.(input);
      // A file edit replaces what a file published. An operator's choice of
      // active version (one they authored or promoted) stays active; the
      // file's new text waits as a draft.
      const promote = active === null || active.source === FILE_SOURCE;
      const written = await registry.register({
        ...input,
        // Registry state is the scanner's to set, never the file's.
        expectedActive: undefined,
        enabled: undefined,
        source: FILE_SOURCE,
        trust: params.trust ?? "trusted",
        promote,
      });
      if (!promote) {
        warnings.push(
          `${registry.kind.name} "${input.id}": the file changed; ${written.version} waits as a ` +
            `draft because an operator made ${active!.version} active`,
        );
      }
    } catch (err) {
      // One bad file (a changed body under a pinned version, an invalid
      // version label) is reported; it does not stop the pass.
      if (!isRegistryConflict(err) && !isRegistryInputError(err)) throw err;
      warnings.push(`${registry.kind.name} "${input.id}": ${err.message}`);
      upserted.push(input.id);
      continue;
    }
    upserted.push(input.id);
    changed.push(input.id);
    if (!active) added.push(input.id);
  }

  if (params.sync && params.complete !== false) {
    const discovered = new Set(params.discovered.map((d) => d.id));
    for (const entry of await listAllVersioned({ registry })) {
      if (!inScope(entry.id) || discovered.has(entry.id)) continue;
      const fileVersions = (await registry.versions({ id: entry.id })).filter(
        (v) => v.source === FILE_SOURCE,
      );
      for (const version of fileVersions) {
        await registry.unregister({ id: entry.id, version: version.version });
      }
      if (fileVersions.length > 0) deleted.push(entry.id);
    }
  }

  return { upserted, added, changed, deleted, warnings };
}
