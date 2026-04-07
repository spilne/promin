/**
 * Stream 100K events from a webhook queue.
 * Enrich in parallel, batch into groups of 500, bulk-insert to DB.
 * Backpressure ensures we don't overwhelm the database.
 */

import { PipelineQueue } from "@promin/core";

interface WebhookEvent {
  id: string;
  channelId: string;
  type: string;
  payload: unknown;
}

interface EnrichedEvent extends WebhookEvent {
  channelName: string;
}

const webhookQueue = PipelineQueue.make<WebhookEvent>(1000);

// Producer: HTTP handler pushes to queue (backpressure if full)
async function handleWebhook(event: WebhookEvent) {
  await webhookQueue.offerAsync(event);
}

// Consumer: stream → enrich → batch → insert
async function startConsumer() {
  await webhookQueue
    .toStream()
    .dedupe()
    .parAsyncMap(10, async (event) => {
      const channel = await fetchChannel(event.channelId);
      return { ...event, channelName: channel.name } as EnrichedEvent;
    })
    .filter((e) => e.type !== "ping")
    .groupWithin(500, 2_000)
    .tapAsync((batch) => db.bulkInsert("events", batch))
    .drain();
}

// Stubs
const fetchChannel = async (_id: string) => ({ name: "my-channel" });
const db = { bulkInsert: async (_table: string, _rows: unknown[]) => {} };

export { handleWebhook, startConsumer };
