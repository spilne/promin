import { InMemoryRemoteDeploymentRegistry } from "../in-memory-remote-deployment-registry.ts";
import { remoteDeploymentRegistryTestSuite } from "../remote-deployment-registry-test-suite.ts";

remoteDeploymentRegistryTestSuite(
  (getNow) => new InMemoryRemoteDeploymentRegistry({ now: getNow }),
);
