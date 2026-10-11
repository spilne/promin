import { InMemoryWorkflowSchemaStore } from "../workflow-schema-store.ts";
import { workflowSchemaStoreTestSuite } from "../workflow-schema-store-test-suite.ts";

workflowSchemaStoreTestSuite(() => new InMemoryWorkflowSchemaStore());
