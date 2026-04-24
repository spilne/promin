// ---------------------------------------------------------------------------
// build-ui — bundles the Preact dashboard into dist/public.
//
// Output:
//   dist/public/
//     index.html
//     app.js       — bundled Preact app
//     app.css      — Tailwind + DaisyUI + custom styles
// ---------------------------------------------------------------------------

import { $ } from "bun";
import { copyFile, mkdir, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";

const pkgRoot = path.resolve(import.meta.dir, "..");
const outDir = path.join(pkgRoot, "dist", "public");

async function main() {
  console.log("[zorya] building UI…");

  if (existsSync(outDir)) await rm(outDir, { recursive: true, force: true });
  await mkdir(outDir, { recursive: true });

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

  // 3. Copy index.html.
  await copyFile(path.join(pkgRoot, "src/ui/index.html"), path.join(outDir, "index.html"));
  console.log(`[zorya] index.html`);

  console.log(`[zorya] done → ${path.relative(process.cwd(), outDir)}`);
}

await main();
