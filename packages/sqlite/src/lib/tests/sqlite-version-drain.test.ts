import { Database } from "bun:sqlite";
import { versionDrainTestSuite } from "@promin/workflow/testing";
import { SqliteWorkflowStorage } from "../sqlite-workflow-storage.ts";

versionDrainTestSuite(() => SqliteWorkflowStorage.make({ db: new Database(":memory:") }));
