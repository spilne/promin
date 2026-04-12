import { InMemoryStepQueue } from "./in-memory-step-queue.ts";
import { stepQueueTestSuite } from "./step-queue-test-suite.ts";

stepQueueTestSuite(() => new InMemoryStepQueue());
