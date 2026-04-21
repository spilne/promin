// ---------------------------------------------------------------------------
// One-shot migrator — runs Drizzle migrations then exits. Every other
// service `depends_on: { migrator: { condition: service_completed_successfully } }`.
// ---------------------------------------------------------------------------

import { runMigrations } from "./shared.ts";

await runMigrations();
console.log("[migrator] done");
