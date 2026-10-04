import { InMemoryWorkflowStorage } from "@promin/workflow";
import { journalReplayTestSuite } from "@promin/workflow/testing";
import { RemoteWorkflowStorage } from "../remote-workflow-storage.ts";
import { createWorkflowStorageHandler } from "../storage-http-handler.ts";

// Journal replay conformance over the RPC: completion winners, tagged signal
// exits and tagged failures survive the wire codec.
journalReplayTestSuite(
  () =>
    new RemoteWorkflowStorage({
      url: "http://test.local/storage",
      fetch: createWorkflowStorageHandler(new InMemoryWorkflowStorage()),
    }),
);
