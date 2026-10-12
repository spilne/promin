import { FakeWallClock } from "@promin/workflow";
import { InMemoryRemoteDeploymentRegistry } from "../in-memory-remote-deployment-registry.ts";
import { remoteDeploymentRegistryTestSuite } from "../remote-deployment-registry-test-suite.ts";

remoteDeploymentRegistryTestSuite(() => {
  const clock = FakeWallClock.create(1_700_000_000_000);
  return {
    registry: new InMemoryRemoteDeploymentRegistry({ clock }),
    advance: (ms) => clock.advance(ms),
    now: () => clock.currentTimeMs(),
  };
});
