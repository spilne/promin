import { InMemoryStateMachineStorage } from "../state-machine-storage.ts";
import { stateMachineStorageTestSuite } from "../state-machine-storage-test-suite.ts";

stateMachineStorageTestSuite(() => new InMemoryStateMachineStorage());
