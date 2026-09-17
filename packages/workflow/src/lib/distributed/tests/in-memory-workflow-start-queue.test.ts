import { InMemoryWorkflowStartQueue } from "../workflow-start-queue.ts";
import { workflowStartQueueTestSuite } from "../workflow-start-queue-test-suite.ts";

workflowStartQueueTestSuite(() => new InMemoryWorkflowStartQueue());
