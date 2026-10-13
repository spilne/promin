import { createWorkflowVersionRegistry } from "../workflow-version-registry.ts";
import { versionRegistryTestSuite } from "../version-registry-test-suite.ts";

versionRegistryTestSuite(() => createWorkflowVersionRegistry());
