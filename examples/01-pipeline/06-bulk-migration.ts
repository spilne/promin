/**
 * Migrate 100K users from a legacy API to a new database.
 * Rate-limit legacy API to 20 concurrent, track progress, retry failures.
 */

import { Pipeline, StreamPipeline, PipelineSemaphore, PipelineRef } from "@promin/core";

const legacyApiLimit = PipelineSemaphore.make(20);

async function migrateUsers(userIds: string[]) {
  const progress = PipelineRef.make({ migrated: 0, failed: 0 });

  await StreamPipeline.fromIterable(userIds)
    .parAsyncMap(50, (id) =>
      Pipeline.fn(() => legacyApi.getUser(id))
        .withPermit(legacyApiLimit)
        .retry({ maxRetries: 3, jitter: true })
        .runPromise(),
    )
    .tapAsync(async (user) => {
      await newDb.upsert(user);
      await progress.updateAsync((p) => ({ ...p, migrated: p.migrated + 1 }));
    })
    .grouped(100)
    .tapAsync(async () => {
      const p = await progress.getAsync();
      console.log(`Migrated: ${p.migrated}`);
    })
    .drain();

  return progress.getAsync();
}

// Stubs
const legacyApi = { getUser: async (_id: string) => ({ id: _id, name: "User" }) };
const newDb = { upsert: async (_user: unknown) => {} };

export { migrateUsers };
