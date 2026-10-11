// ---------------------------------------------------------------------------
// build-ui — bundles the Preact dashboard into dist/public.
//
// Output:
//   dist/public/
//     index.html
//     app.js       — bundled Preact app
//     app.css      — Tailwind + DaisyUI + custom styles
//
// The bundle is written to a fresh sibling directory first and swapped into
// dist/public only once every step succeeded, so a failed build leaves the
// previous dashboard in place. Workspace packages resolve through their
// `@promin/source` export condition (their `src/`), so no package needs to
// be built first.
// ---------------------------------------------------------------------------

import { $ } from "bun";
import { chmod, mkdir, mkdtemp, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";

// Injected into dist/public/index.html only when ZORYA_DEV_RELOAD=1 (set by
// scripts/dev.ts). Polls a tiny sentinel file every second; on change,
// reloads the page. Polling beats SSE/WebSocket here because the static
// file server already serves dist/public so we get the endpoint for free —
// no extra route, no CORS, no cleanup on shutdown.
const DEV_RELOAD_SCRIPT = `<script>
(function () {
  var last = null;
  setInterval(function () {
    fetch('/.reload-timestamp', { cache: 'no-store' })
      .then(function (r) { return r.ok ? r.text() : null; })
      .then(function (t) {
        if (t == null) return;
        if (last !== null && t !== last) {
          console.log('[zorya] hot reload');
          location.reload();
        }
        last = t;
      })
      .catch(function () { /* server restart in flight — ignore */ });
  }, 1000);
})();
</script>`;

const pkgRoot = path.resolve(import.meta.dir, "..");
const distDir = path.join(pkgRoot, "dist");
const publicDir = path.join(distDir, "public");

async function main() {
  console.log("[zorya] building UI…");

  await mkdir(distDir, { recursive: true });
  const outDir = await mkdtemp(path.join(distDir, ".public-build-"));
  try {
    await buildInto(outDir);
    await swapInto({ staged: outDir, target: publicDir });
  } catch (err) {
    await rm(outDir, { recursive: true, force: true });
    throw err;
  }
  console.log(`[zorya] done → ${path.relative(process.cwd(), publicDir)}`);
}

/** Replace `target` with `staged`; the old directory is removed only after the rename. */
async function swapInto(params: { staged: string; target: string }): Promise<void> {
  const old = `${params.target}.old-${process.pid}`;
  await rm(old, { recursive: true, force: true });
  await rename(params.target, old).catch((err: NodeJS.ErrnoException) => {
    if (err.code !== "ENOENT") throw err;
  });
  await rename(params.staged, params.target);
  // mkdtemp creates the directory owner-only; the served tree should not be.
  await chmod(params.target, 0o755);
  await rm(old, { recursive: true, force: true });
}

async function buildInto(outDir: string): Promise<void> {
  // 1. Bundle JS (Preact app).
  const jsResult = await Bun.build({
    entrypoints: [path.join(pkgRoot, "src/ui/main.tsx")],
    outdir: outDir,
    target: "browser",
    format: "esm",
    minify: true,
    naming: {
      entry: "app.js",
    },
    // `bun --conditions` does not reach Bun.build; workspace packages
    // resolve to their sources through this condition.
    conditions: ["@promin/source"],
    define: {
      "process.env.NODE_ENV": '"production"',
    },
  });
  if (!jsResult.success) {
    for (const log of jsResult.logs) console.error(log);
    throw new Error("UI JS bundle failed");
  }
  const jsBytes = jsResult.outputs[0]?.size ?? 0;
  console.log(`[zorya] app.js  ${(jsBytes / 1024).toFixed(1)}kb`);

  // 2. Tailwind CSS (runs tailwindcss CLI).
  const cssIn = path.join(pkgRoot, "src/ui/styles.css");
  const cssOut = path.join(outDir, "app.css");
  await $`bunx tailwindcss -c ${path.join(pkgRoot, "tailwind.config.ts")} -i ${cssIn} -o ${cssOut} --minify`
    .cwd(pkgRoot)
    .quiet();
  const cssStat = await Bun.file(cssOut).size;
  console.log(`[zorya] app.css ${(cssStat / 1024).toFixed(1)}kb`);

  // 3. Read + (optionally) inject the reload client into index.html.
  let html = await Bun.file(path.join(pkgRoot, "src/ui/index.html")).text();
  if (process.env.ZORYA_DEV_RELOAD === "1") {
    if (html.includes("</body>")) {
      html = html.replace("</body>", `${DEV_RELOAD_SCRIPT}\n  </body>`);
    } else {
      // No </body> tag (unusual) — append the script at the end and rely
      // on the browser's quirks-mode tolerance. Logged so the operator
      // notices.
      console.warn("[zorya] index.html has no </body>; reload script appended at EOF");
      html += `\n${DEV_RELOAD_SCRIPT}\n`;
    }
  }
  await writeFile(path.join(outDir, "index.html"), html);
  console.log(
    `[zorya] index.html${process.env.ZORYA_DEV_RELOAD === "1" ? " (with hot-reload client)" : ""}`,
  );
}

await main();
