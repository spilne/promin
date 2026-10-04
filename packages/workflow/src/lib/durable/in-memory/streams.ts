// ---------------------------------------------------------------------------
// InMemoryStreams — append-only chunks per (workflow, stream).
// ---------------------------------------------------------------------------

import type { ReadStreamChunksParams, StreamChunk } from "../storage/stream-store.ts";
import type { WallClock } from "../../shared/wall-clock.ts";

function streamKey(params: { workflowId: string; streamId: string }): string {
  return `${params.workflowId}::${params.streamId}`;
}

export class InMemoryStreams {
  /** Chunks keyed by `streamKey`, in `chunkIndex` order. */
  private readonly chunks = new Map<string, StreamChunk[]>();

  constructor(private readonly clock: WallClock) {}

  /** Append one chunk; its index is the stream's length before the append. */
  append(params: {
    workflowId: string;
    streamId: string;
    payload: unknown;
    appendedBy: "workflow" | "external";
  }): { chunkIndex: number } {
    const key = streamKey(params);
    const existing = this.chunks.get(key) ?? [];
    const chunkIndex = existing.length;
    existing.push({
      chunkIndex,
      payload: params.payload,
      appendedBy: params.appendedBy,
      appendedAt: this.clock.now(),
    });
    this.chunks.set(key, existing);
    return { chunkIndex };
  }

  read(params: ReadStreamChunksParams): StreamChunk[] {
    const all = this.chunks.get(streamKey(params)) ?? [];
    const { since } = params;
    const filtered = since !== undefined ? all.filter((c) => c.chunkIndex > since) : all;
    return params.limit !== undefined ? filtered.slice(0, params.limit) : filtered;
  }

  /** Drop every stream of one workflow (purge). */
  deleteWorkflow(workflowId: string): void {
    const prefix = `${workflowId}::`;
    for (const key of this.chunks.keys()) {
      if (key.startsWith(prefix)) this.chunks.delete(key);
    }
  }

  clear(): void {
    this.chunks.clear();
  }
}
