import { InMemoryStepQueue } from "../in-memory-step-queue.ts";
import { stepQueueTestSuite } from "../step-queue-test-suite.ts";
import { InMemoryLeaderLeases } from "../../scheduler/leader-lease.ts";

stepQueueTestSuite(({ maxDeliveries }) => new InMemoryStepQueue({ maxDeliveries }), {
  leaseFenced: () => {
    const leases = new InMemoryLeaderLeases();
    return { queue: new InMemoryStepQueue({ leaderLeases: leases }), leases };
  },
});
