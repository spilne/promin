// ---------------------------------------------------------------------------
// AgentToolCatalogHistory — persistence loop over a live `AgentToolCatalog`.
//
// The catalog itself is ephemeral (every `listAll()` walks live sources).
// This wraps a catalog + a `ToolHistoryStore`: `snapshot()` lists the
// catalog once and upserts every entry; `start()` runs that on an
// interval so the store accumulates a durable audit trail.
//
// v0 is the snapshot-loop strategy — simple, but a tool that appears and
// vanishes entirely between two snapshots is missed. Event-driven
// hooks on the file registry / MCP reconnect can come later if transient
// drift becomes a concrete problem.
// ---------------------------------------------------------------------------

import { payloadHash, SystemClock, type Clock, type TimerHandle } from "@promin/core";
import type { AgentToolCatalog, ToolCatalogEntry } from "../tool-catalog.ts";
import type { ToolHistoryStore, ToolObservation } from "./types.ts";

export interface AgentToolCatalogHistoryConfig {
  /** The live catalog to snapshot. */
  readonly catalog: AgentToolCatalog;
  /** Where snapshots are persisted. */
  readonly store: ToolHistoryStore;
  /** Time source for the interval loop. Default: `SystemClock`. */
  readonly clock?: Clock;
}

/**
 * Maps a live catalog entry to a `ToolObservation` — the schema hash is
 * the SHA-256 of the canonical JSON Schema, so a parameter-shape change
 * flips the hash and lands as a new history row.
 */
export function toObservation(entry: ToolCatalogEntry): ToolObservation {
  return {
    name: entry.name,
    sourceKind: entry.source.kind,
    sourceDetail:
      entry.source.kind === "file"
        ? (entry.source.path ?? "")
        : entry.source.kind === "mcp"
          ? entry.source.server
          : "",
    schemaHash: payloadHash(entry.parameters),
    description: entry.description,
  };
}

export class AgentToolCatalogHistory {
  private readonly catalog: AgentToolCatalog;
  private readonly store: ToolHistoryStore;
  private readonly clock: Clock;
  private timer: TimerHandle | null = null;

  constructor(config: AgentToolCatalogHistoryConfig) {
    this.catalog = config.catalog;
    this.store = config.store;
    this.clock = config.clock ?? SystemClock;
  }

  /** List the live catalog once and persist every entry. */
  async snapshot(): Promise<void> {
    const entries = await this.catalog.listAll();
    await this.store.recordSnapshot(entries.map(toObservation));
  }

  /**
   * Begin periodic snapshots every `intervalMs`. Idempotent — a second
   * call while already running is a no-op. Does not take an immediate
   * snapshot; call `snapshot()` first if a baseline is wanted.
   */
  start(intervalMs: number): void {
    if (this.timer) return;
    this.timer = this.clock.setInterval(() => {
      void this.snapshot();
    }, intervalMs);
  }

  /** Stop the periodic loop. Safe to call when not running. */
  stop(): void {
    this.timer?.clear();
    this.timer = null;
  }
}
