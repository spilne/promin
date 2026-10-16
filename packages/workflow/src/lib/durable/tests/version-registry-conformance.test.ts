import { createWorkflowVersionRegistry } from "../workflow-version-registry.ts";
import { InMemoryWorkflowStorage } from "../in-memory-storage.ts";
import { versionDrainTestSuite, versionRegistryTestSuite } from "../version-registry-test-suite.ts";

versionRegistryTestSuite(() => createWorkflowVersionRegistry());
versionDrainTestSuite(() => new InMemoryWorkflowStorage());
