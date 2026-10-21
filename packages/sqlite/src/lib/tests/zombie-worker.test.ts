import { Database } from "bun:sqlite";
import { zombieWorkerTestSuite } from "@promin/workflow/testing";
import type { WorkflowStorage } from "@promin/workflow";
import { SqliteWorkflowStorage } from "../sqlite-workflow-storage.ts";

// Two storage instances on one database: two workers. The stalled
// holder's lease runs out when its lock row's expiry moves into the past.
const dbs = new WeakMap<WorkflowStorage, Database>();

zombieWorkerTestSuite({
  createStorage: () => {
    const db = new Database(":memory:");
    const storage = SqliteWorkflowStorage.make({ db });
    dbs.set(storage, db);
    return storage;
  },
  createPeer: (storage) => SqliteWorkflowStorage.make({ db: dbs.get(storage)! }),
  expireLock: async ({ storage, workflowId }) => {
    dbs
      .get(storage)!
      .run("UPDATE promin_wf_locks SET expires_at = 0 WHERE workflow_id = ?", [workflowId]);
  },
});
