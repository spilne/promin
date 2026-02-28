// ---------------------------------------------------------------------------
// Streamable<T> — "I can produce a stream of T"
// Base capability. Anything that emits data implements this.
// ---------------------------------------------------------------------------

import type { StreamPipeline } from "../stream-pipeline.ts";
import type { Codec } from "./codec.ts";

export interface Streamable<T> {
  subscribe(params?: { group?: string }): StreamPipeline<T, never>;
  codec: Codec<T>;
}

export function isStreamable<T>(value: unknown): value is Streamable<T> {
  return (
    value !== null &&
    typeof value === "object" &&
    "subscribe" in value &&
    typeof (value as any).subscribe === "function" &&
    "codec" in value
  );
}

// ---------------------------------------------------------------------------
// Sinkable<T> — "I can consume values of T"
// ---------------------------------------------------------------------------

export interface Sinkable<T> {
  publish(value: T): Promise<void>;
  codec: Codec<T>;
}

export function isSinkable<T>(value: unknown): value is Sinkable<T> {
  return (
    value !== null &&
    typeof value === "object" &&
    "publish" in value &&
    typeof (value as any).publish === "function" &&
    "codec" in value
  );
}

// ---------------------------------------------------------------------------
// KeyedSinkable<T> — "I can route by key"
// ---------------------------------------------------------------------------

export interface KeyedSinkable<T> extends Sinkable<T> {
  publish(value: T, params?: { key: string }): Promise<void>;
}

export function isKeyedSinkable<T>(value: unknown): value is KeyedSinkable<T> {
  return isSinkable(value);
}

// ---------------------------------------------------------------------------
// Partitionable<T> — "I have partitions"
// ---------------------------------------------------------------------------

export interface Partitionable<T> extends Streamable<T> {
  partitions: number;
  subscribe(params?: { group?: string; partitions?: number[] }): StreamPipeline<T, never>;
}

export function isPartitionable<T>(value: unknown): value is Partitionable<T> {
  return (
    isStreamable(value) && "partitions" in value && typeof (value as any).partitions === "number"
  );
}

// ---------------------------------------------------------------------------
// Replayable<T> — "I can seek to a point in time"
// ---------------------------------------------------------------------------

export type Offset =
  | { type: "earliest" }
  | { type: "latest" }
  | { type: "timestamp"; value: number }
  | { type: "specific"; value: string };

export interface Replayable<T> extends Streamable<T> {
  subscribeFrom(params: { offset: Offset; group?: string }): StreamPipeline<T, never>;
}

export function isReplayable<T>(value: unknown): value is Replayable<T> {
  return (
    isStreamable(value) &&
    "subscribeFrom" in value &&
    typeof (value as any).subscribeFrom === "function"
  );
}

// ---------------------------------------------------------------------------
// Acknowledgeable<T> — "I support manual ack/nack"
// ---------------------------------------------------------------------------

export interface Envelope<T> {
  readonly value: T;
  ack(): Promise<void>;
  nack(): Promise<void>;
  readonly metadata: Record<string, unknown>;
}

export interface Acknowledgeable<T> extends Streamable<T> {
  subscribeAck(params?: { group?: string }): StreamPipeline<Envelope<T>, never>;
}

export function isAcknowledgeable<T>(value: unknown): value is Acknowledgeable<T> {
  return (
    isStreamable(value) &&
    "subscribeAck" in value &&
    typeof (value as any).subscribeAck === "function"
  );
}

// ---------------------------------------------------------------------------
// Checkpointable<T> — "I can save and restore consumption position"
// ---------------------------------------------------------------------------

export interface Checkpointable<T> extends Streamable<T> {
  commitOffset(params: { group: string; offset: string }): Promise<void>;
  getCommittedOffset(params: { group: string }): Promise<string | null>;
}

export function isCheckpointable<T>(value: unknown): value is Checkpointable<T> {
  return (
    isStreamable(value) &&
    "commitOffset" in value &&
    typeof (value as any).commitOffset === "function" &&
    "getCommittedOffset" in value &&
    typeof (value as any).getCommittedOffset === "function"
  );
}
