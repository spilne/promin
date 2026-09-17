import { evalDatasetStoreTestSuite, evalRunStoreTestSuite } from "../../../testing.ts";
import { InMemoryEvalDatasetStore } from "../in-memory-dataset-store.ts";
import { InMemoryEvalRunStore } from "../in-memory-run-store.ts";

evalRunStoreTestSuite(() => new InMemoryEvalRunStore());
evalDatasetStoreTestSuite(() => new InMemoryEvalDatasetStore());
