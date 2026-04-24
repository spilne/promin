// ---------------------------------------------------------------------------
// zorya:server — builds the UI if needed, then boots the standalone
// server example. Pairs with `bun run zorya:worker` in a second terminal.
//
// Env:
//   ZORYA_DB=./zorya.db    persistent sqlite (default: :memory:)
//   PORT=4200              override port (default 4100)
// ---------------------------------------------------------------------------

import { $ } from "bun";
import path from "node:path";
import { existsSync } from "node:fs";

const pkgRoot = path.resolve(import.meta.dir, "..");
const indexHtml = path.join(pkgRoot, "dist", "public", "index.html");

if (!existsSync(indexHtml) || process.argv.includes("--rebuild")) {
  await $`bun ${path.join(pkgRoot, "scripts", "build-ui.ts")}`.cwd(pkgRoot);
} else {
  console.log("[zorya] UI cached at dist/public (pass --rebuild to force)");
}

console.log("[zorya] starting server (no workflow code)…");
await import(path.join(pkgRoot, "examples", "split", "server.ts"));
