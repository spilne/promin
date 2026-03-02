// ---------------------------------------------------------------------------
// pgmq types — mirrors pgmq SQL API
// ---------------------------------------------------------------------------

/** A message to send to a pgmq queue. */
export interface PgmqMessage<T> {
  readonly data: T;
  readonly delay?: number;
  readonly headers?: Record<string, string>;
}

/** Create a FIFO-ordered message with a group key. */
export function fifoMessage<T>(params: { data: T; group: string; delay?: number }): PgmqMessage<T> {
  return {
    data: params.data,
    delay: params.delay,
    headers: { "x-pgmq-group": params.group },
  };
}

/** A raw record returned by pgmq read functions. */
export interface PgmqRecord<T = unknown> {
  readonly msgId: number;
  readonly readCt: number;
  readonly enqueuedAt: Date;
  readonly vt: Date;
  readonly message: T;
  readonly headers?: Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// ReadMode — how to consume messages from a queue
// ---------------------------------------------------------------------------

export type ReadMode =
  | ReadMode.Standard
  | ReadMode.Poll
  | ReadMode.Grouped
  | ReadMode.GroupedPoll
  | ReadMode.GroupedRoundRobin
  | ReadMode.GroupedRoundRobinPoll;

export namespace ReadMode {
  export interface Standard {
    readonly _tag: "standard";
    readonly vt: number;
    readonly qty: number;
  }

  export interface Poll {
    readonly _tag: "poll";
    readonly vt: number;
    readonly qty: number;
    readonly maxPollSeconds?: number;
    readonly pollIntervalMs?: number;
  }

  export interface Grouped {
    readonly _tag: "grouped";
    readonly vt: number;
    readonly qty: number;
  }

  export interface GroupedPoll {
    readonly _tag: "grouped-poll";
    readonly vt: number;
    readonly qty: number;
    readonly maxPollSeconds?: number;
    readonly pollIntervalMs?: number;
  }

  export interface GroupedRoundRobin {
    readonly _tag: "grouped-round-robin";
    readonly vt: number;
    readonly qty: number;
  }

  export interface GroupedRoundRobinPoll {
    readonly _tag: "grouped-round-robin-poll";
    readonly vt: number;
    readonly qty: number;
    readonly maxPollSeconds?: number;
    readonly pollIntervalMs?: number;
  }

  export const standard = (params: { vt: number; qty: number }): Standard => ({
    _tag: "standard",
    ...params,
  });

  export const poll = (params: {
    vt: number;
    qty: number;
    maxPollSeconds?: number;
    pollIntervalMs?: number;
  }): Poll => ({ _tag: "poll", ...params });

  export const grouped = (params: { vt: number; qty: number }): Grouped => ({
    _tag: "grouped",
    ...params,
  });

  export const groupedPoll = (params: {
    vt: number;
    qty: number;
    maxPollSeconds?: number;
    pollIntervalMs?: number;
  }): GroupedPoll => ({ _tag: "grouped-poll", ...params });

  export const groupedRoundRobin = (params: { vt: number; qty: number }): GroupedRoundRobin => ({
    _tag: "grouped-round-robin",
    ...params,
  });

  export const groupedRoundRobinPoll = (params: {
    vt: number;
    qty: number;
    maxPollSeconds?: number;
    pollIntervalMs?: number;
  }): GroupedRoundRobinPoll => ({ _tag: "grouped-round-robin-poll", ...params });
}

/** How to acknowledge processed messages. */
export type AckMode = "delete" | "archive";
