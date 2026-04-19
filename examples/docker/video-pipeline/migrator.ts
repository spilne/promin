// ---------------------------------------------------------------------------
// One-shot: run DB migrations and exit. docker-compose's `migrator` service
// depends on postgres being healthy and the coordinator/workers depend on
// migrator having exited successfully. That gives an ordered: postgres →
// schema ready → everything else.
// ---------------------------------------------------------------------------

import { runMigrations } from "./shared.ts";

await runMigrations();
console.log("[migrator] migrations complete — exiting 0");
