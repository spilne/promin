import { InMemoryWorkflowAdvertisementRegistry } from "../workflow-advertisements.ts";
import { workflowAdvertisementRegistryTestSuite } from "../workflow-advertisements-test-suite.ts";

workflowAdvertisementRegistryTestSuite(() => new InMemoryWorkflowAdvertisementRegistry());
