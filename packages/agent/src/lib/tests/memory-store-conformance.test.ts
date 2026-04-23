import { InMemoryMemoryStore } from "../memory-store.ts";
import { memoryStoreTestSuite } from "../../testing.ts";

memoryStoreTestSuite(() => new InMemoryMemoryStore());
