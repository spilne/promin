/**
 * Data pipeline patterns — migration, validation, ETL
 *
 * Shows patterns combining Pipeline, StreamPipeline, and primitives:
 * - Rate-limited bulk migration with progress
 * - Parallel validation with accumulated errors
 * - Supervised long-running consumer
 */

import {
  Pipeline,
  StreamPipeline,
  PipelineSemaphore,
  PipelineRef,
  PipelineQueue,
} from "@promin/core";
import { Data } from "effect";

// ---------------------------------------------------------------------------
// Rate-limited migration with progress tracking
// ---------------------------------------------------------------------------

async function rateLimitedMigration(
  userIds: string[],
  legacyApi: { getUser: (id: string) => Promise<unknown> },
  newDb: { upsert: (user: unknown) => Promise<void> },
) {
  const rateLimit = PipelineSemaphore.make(20); // legacy API limit
  const progress = PipelineRef.make({ migrated: 0, failed: 0, total: userIds.length });

  await StreamPipeline.fromIterable(userIds)
    .parAsyncMap(50, async (id) => {
      return Pipeline.fn(() => legacyApi.getUser(id))
        .withPermit(rateLimit)
        .retry({ maxRetries: 3, jitter: true })
        .runPromise();
    })
    .tapAsync(async (user) => {
      await newDb.upsert(user);
      await progress.updateAsync((p) => ({ ...p, migrated: p.migrated + 1 }));
    })
    .grouped(100)
    .tapAsync(async () => {
      const p = await progress.getAsync();
      console.log(`${p.migrated}/${p.total} migrated`);
    })
    .drain();
}

// ---------------------------------------------------------------------------
// Parallel validation — collect ALL errors, don't stop at first
// ---------------------------------------------------------------------------

class ValidationError extends Data.TaggedError("ValidationError")<{
  readonly field: string;
  readonly message: string;
}> {}

async function parallelValidation(input: { title: string; tags: string[]; thumbnailUrl: string }) {
  const validateTitle = (title: string) =>
    title.length > 0 && title.length <= 100
      ? Pipeline.succeed(title)
      : Pipeline.fail(new ValidationError({ field: "title", message: "Must be 1-100 chars" }));

  const validateTags = (tags: string[]) =>
    tags.length <= 30
      ? Pipeline.succeed(tags)
      : Pipeline.fail(new ValidationError({ field: "tags", message: "Max 30 tags" }));

  const validateThumbnail = (url: string) =>
    Pipeline.fn(() => fetch(url, { method: "HEAD" }))
      .filter({
        predicate: (res) => res.ok,
        orFail: () => new ValidationError({ field: "thumbnail", message: "Invalid URL" }),
      })
      .map(() => url);

  // All three run in parallel; ALL errors are accumulated
  const [title, tags, thumbnail] = await Pipeline.validate(
    validateTitle(input.title),
    validateTags(input.tags),
    validateThumbnail(input.thumbnailUrl),
  ).runPromise();

  return { title, tags, thumbnail };
}

// ---------------------------------------------------------------------------
// Supervised consumer — auto-restart on failure
// ---------------------------------------------------------------------------

async function supervisedConsumer(queue: ReturnType<typeof PipelineQueue.make>) {
  await Pipeline.fn(async () => {
    await queue
      .toStream()
      .parAsyncMap(10, (event) => processEvent(event))
      .groupWithin(100, 2_000)
      .tapAsync((batch) => writeBatch(batch))
      .drain();
  })
    .supervised({ restart: "on-failure", maxRestarts: 100, intervalMs: 2_000 })
    .runPromise();
}

// ---------------------------------------------------------------------------
// Stubs
// ---------------------------------------------------------------------------

async function processEvent(_event: unknown) {
  return _event;
}
async function writeBatch(_batch: unknown[]) {}

export { rateLimitedMigration, parallelValidation, supervisedConsumer };
