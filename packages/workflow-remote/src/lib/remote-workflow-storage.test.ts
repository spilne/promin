// ---------------------------------------------------------------------------
// RemoteWorkflowStorage conformance — loopback HTTP storage suite.
//
// Stands up a `createWorkflowStorageHandler` in front of an in-memory
// storage, plugs a `RemoteWorkflowStorage` at it via an in-process fetch
// (no TCP), and runs the portable suite. If every CRUD, lock, signal,
// suspend, fresh-run, and list assertion passes over the RPC, the transport
// is faithful enough for real HTTP deployments to swap in without semantic
// surprises.
// ---------------------------------------------------------------------------

import { InMemoryWorkflowStorage } from "@promin/workflow";
import { storageTestSuite } from "@promin/workflow/testing";
import { RemoteWorkflowStorage } from "./remote-workflow-storage.ts";
import { createWorkflowStorageHandler } from "./storage-http-handler.ts";

storageTestSuite(() => {
  // Fresh in-memory backend per test — matches how the suite's factory is
  // re-invoked on every describe-block entry. The handler closes over this
  // backend, the remote closes over the handler, and nothing leaks across.
  const backing = new InMemoryWorkflowStorage();
  const handler = createWorkflowStorageHandler(backing);
  return new RemoteWorkflowStorage({
    url: "http://test.local/storage",
    fetch: handler,
  });
});
