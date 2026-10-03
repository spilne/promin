import { Database } from "bun:sqlite";
import { journalReplayTestSuite } from "@promin/workflow/testing";
import { SqliteWorkflowStorage } from "../sqlite-workflow-storage.ts";

// Journal replay conformance: branch paths written by ctx.parallel (current
// and legacy grammar) round-trip through the SQLite journal table.
journalReplayTestSuite(() => SqliteWorkflowStorage.make({ db: new Database(":memory:") }));
