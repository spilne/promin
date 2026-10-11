import { InMemoryRemoteDeploymentRegistry } from "../in-memory-remote-deployment-registry.ts";
import { remoteDeploymentRegistryTestSuite } from "../remote-deployment-registry-test-suite.ts";

remoteDeploymentRegistryTestSuite((clock) => new InMemoryRemoteDeploymentRegistry({ clock }));
