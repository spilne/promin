import { InMemoryWorkflowStorage } from "./in-memory-storage.ts";
import { storageTestSuite } from "./storage-test-suite.ts";

// Run the portable test suite against InMemoryWorkflowStorage
storageTestSuite(() => new InMemoryWorkflowStorage(), {
  hasJournal: true,
  hasJournaledSuspend: true,
});
