// Drizzle DB type (re-export for convenience)
export type { DrizzleDb } from "../lib/drizzle-db.ts";

// Types
export {
  type PgmqMessage,
  fifoMessage,
  type PgmqRecord,
  type ReadMode,
  ReadMode as ReadModes,
  type AckMode,
} from "./types.ts";

// High-level typed queue (Streamable + Sinkable + Acknowledgeable)
export { PgmqQueue, type PgmqQueueConfig } from "./pgmq-queue.ts";

// Low-level SQL functions
export {
  createQueue,
  createUnloggedQueue,
  createPartitionedQueue,
  dropQueue,
  listQueues,
  send,
  sendBatch,
  read,
  pop,
  deleteMessage,
  deleteBatch,
  archive,
  archiveBatch,
  purgeQueue,
  setVt,
  metrics,
  enableNotify,
  disableNotify,
  createFifoIndex,
} from "./pgmq.ts";
