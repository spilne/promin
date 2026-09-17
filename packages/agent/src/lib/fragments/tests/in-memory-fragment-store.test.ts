import { fragmentStoreTestSuite } from "../fragment-store-test-suite.ts";
import { InMemoryFragmentStore } from "../in-memory-fragment-store.ts";

fragmentStoreTestSuite(() => new InMemoryFragmentStore());
