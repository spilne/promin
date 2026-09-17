import { memoryStoreTestSuite } from "../memory-store-test-suite.ts";
import { InMemoryMemoryStore } from "../in-memory-memory-store.ts";

memoryStoreTestSuite(() => new InMemoryMemoryStore());
