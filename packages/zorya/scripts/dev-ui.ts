// ---------------------------------------------------------------------------
// dev-ui.ts — shared UI build + watch utilities for Zorya dev scripts.
//
// Used by:
//   scripts/dev.ts                       — `bun run zorya` (server-only)
//   examples/start-all.ts                — `bun run demo` (server + worker)
//
// Both scripts need the same dev loop:
//   1. Build packages/zorya/src/ui/** → packages/zorya/dist/public/
//   2. Write a `.reload-timestamp` sentinel under dist/public/
//   3. Watch src/ui and rebuild + refresh the sentinel on change
//
// The dashboard's HTML carries a poll-reload client (injected only when
// ZORYA_DEV_RELOAD=1 is set during build) that fetches the sentinel
// every second; on change it `location.reload()`s.
// ---------------------------------------------------------------------------

import { $ } from "bun";
import path from "node:path";
import { existsSync, watch } from "node:fs";
import { writeFile } from "node:fs/promises";

const pkgRoot = path.resolve(import.meta.dir, "..");

export const ZORYA_DEV_PATHS = {
  pkgRoot,
  indexHtml: path.join(pkgRoot, "dist", "public", "index.html"),
  reloadFile: path.join(pkgRoot, "dist", "public", ".reload-timestamp"),
  uiSrc: path.join(pkgRoot, "src", "ui"),
  buildScript: path.join(pkgRoot, "scripts", "build-ui.ts"),
  demoScript: path.join(pkgRoot, "examples", "demo.ts"),
  workerScript: path.join(pkgRoot, "examples", "worker.ts"),
  userAppScript: path.join(pkgRoot, "examples", "user-app.ts"),
} as const;

/**
 * Build packages/zorya/src/ui → dist/public, then refresh the
 * dev-reload sentinel. ZORYA_DEV_RELOAD=1 makes build-ui.ts inject
 * the polling client into the rendered index.html.
 */
export async function buildUi(): Promise<void> {
  await $`bun ${ZORYA_DEV_PATHS.buildScript}`
    .cwd(ZORYA_DEV_PATHS.pkgRoot)
    .env({ ...process.env, ZORYA_DEV_RELOAD: "1" });
  // Sentinel must be written AFTER build — the build clears dist/public.
  await writeFile(ZORYA_DEV_PATHS.reloadFile, String(Date.now()));
}

/**
 * Build the UI if needed (or always when forceRebuild is true). When
 * the build is skipped, still refreshes the sentinel so the polling
 * client has a stable baseline.
 */
export async function ensureUiBuilt(
  opts: {
    forceRebuild?: boolean;
    log?: (line: string) => void;
  } = {},
): Promise<void> {
  const log = opts.log ?? ((s) => console.log(s));
  if (opts.forceRebuild || !existsSync(ZORYA_DEV_PATHS.indexHtml)) {
    log("[zorya] building UI…");
    await buildUi();
    log("[zorya] UI ready at dist/public/");
  } else {
    log("[zorya] UI cached at dist/public (pass --rebuild to force)");
    await writeFile(ZORYA_DEV_PATHS.reloadFile, String(Date.now()));
  }
}

/**
 * Watch packages/zorya/src/ui recursively. Saves are debounced so a
 * flurry (editor formatter, multi-file rename) collapses into one
 * rebuild. Concurrent rebuilds are serialised via a busy/queued pair —
 * an edit during an in-flight build queues exactly one follow-up.
 */
export function watchUi(
  opts: {
    onRebuildStart?: (filename: string) => void;
    onRebuildDone?: () => void;
    onRebuildError?: (err: Error) => void;
    debounceMs?: number;
  } = {},
): { stop(): void } {
  const debounceMs = opts.debounceMs ?? 150;
  let pending: ReturnType<typeof setTimeout> | undefined;
  let busy = false;
  let queued = false;

  const watcher = watch(ZORYA_DEV_PATHS.uiSrc, { recursive: true }, (_evt, filename) => {
    if (!filename) return;
    if (pending) clearTimeout(pending);
    pending = setTimeout(async () => {
      pending = undefined;
      if (busy) {
        queued = true;
        return;
      }
      busy = true;
      try {
        do {
          queued = false;
          opts.onRebuildStart?.(filename.toString());
          await buildUi();
          opts.onRebuildDone?.();
        } while (queued);
      } catch (err) {
        opts.onRebuildError?.(err as Error);
      } finally {
        busy = false;
      }
    }, debounceMs);
  });

  return {
    stop() {
      if (pending) clearTimeout(pending);
      watcher.close();
    },
  };
}
