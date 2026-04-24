// ---------------------------------------------------------------------------
// zorya dev — builds the UI and runs the demo server in one command.
//
// Usage:
//   bun run packages/zorya/scripts/dev.ts
//   PORT=4200 bun run packages/zorya/scripts/dev.ts
//   bun run packages/zorya/scripts/dev.ts --rebuild
//
// Or from repo root:
//   bun run zorya
// ---------------------------------------------------------------------------

import { $ } from "bun";
import path from "node:path";
import { existsSync } from "node:fs";

const pkgRoot = path.resolve(import.meta.dir, "..");
const indexHtml = path.join(pkgRoot, "dist", "public", "index.html");
const forceRebuild = process.argv.includes("--rebuild");

if (forceRebuild || !existsSync(indexHtml)) {
  await $`bun ${path.join(pkgRoot, "scripts", "build-ui.ts")}`.cwd(pkgRoot);
} else {
  console.log("[zorya] UI cached at dist/public (pass --rebuild to force)");
}

console.log("[zorya] starting demo server…");
await import(path.join(pkgRoot, "examples", "demo.ts"));
