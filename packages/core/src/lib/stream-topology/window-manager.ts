// ---------------------------------------------------------------------------
// WindowManager — manages windowed state for keyed aggregation
// ---------------------------------------------------------------------------

import type { TimeWindow, WindowType, AggregateSpec } from "./types.ts";

interface WindowEntry<S> {
  window: TimeWindow;
  state: S;
  lastActivity: number;
}

/** Manages windows for a single key, flushing completed windows. */
export class WindowManager<S, T, U> {
  private windows = new Map<string, WindowEntry<S>>();

  constructor(
    private readonly windowType: WindowType,
    private readonly spec: AggregateSpec<S, T, U>,
  ) {}

  /** Add an item to the appropriate window(s). Returns any completed windows to emit. */
  add(key: string, value: T, eventTimeMs: number): U[] {
    const emitted: U[] = [];

    switch (this.windowType.type) {
      case "tumbling": {
        const { windowMs } = this.windowType;
        const windowStart = Math.floor(eventTimeMs / windowMs) * windowMs;
        const windowKey = `${key}:${windowStart}`;
        const entry = this.getOrCreate(windowKey, windowStart, windowStart + windowMs);
        entry.state = this.spec.add(entry.state, value);
        entry.lastActivity = eventTimeMs;
        break;
      }

      case "sliding": {
        const { windowMs, slideMs } = this.windowType;
        // Item belongs to all windows that contain this event time
        const earliestStart = Math.floor((eventTimeMs - windowMs) / slideMs + 1) * slideMs;
        for (let start = earliestStart; start <= eventTimeMs; start += slideMs) {
          if (start < 0) continue;
          const windowKey = `${key}:${start}`;
          const entry = this.getOrCreate(windowKey, start, start + windowMs);
          entry.state = this.spec.add(entry.state, value);
          entry.lastActivity = eventTimeMs;
        }
        break;
      }

      case "session": {
        // Find or create a session for this key
        const sessionKey = `${key}:session`;
        const existing = this.windows.get(sessionKey);

        if (existing && eventTimeMs - existing.lastActivity <= this.windowType.gapMs) {
          // Extend existing session
          existing.state = this.spec.add(existing.state, value);
          existing.window = { start: existing.window.start, end: eventTimeMs };
          existing.lastActivity = eventTimeMs;
        } else {
          // Close old session if exists
          if (existing) {
            emitted.push(this.spec.emit(key, existing.window, existing.state));
            this.windows.delete(sessionKey);
          }
          // Start new session
          const entry: WindowEntry<S> = {
            window: { start: eventTimeMs, end: eventTimeMs },
            state: this.spec.add(this.spec.init(), value),
            lastActivity: eventTimeMs,
          };
          this.windows.set(sessionKey, entry);
        }
        break;
      }
    }

    return emitted;
  }

  /** Flush all windows that have closed (their end time <= watermark). */
  flush(key: string, watermarkMs: number): U[] {
    const emitted: U[] = [];

    for (const [windowKey, entry] of this.windows) {
      if (!windowKey.startsWith(`${key}:`)) continue;

      const shouldFlush =
        this.windowType.type === "session"
          ? watermarkMs - entry.lastActivity > this.windowType.gapMs
          : entry.window.end <= watermarkMs;

      if (shouldFlush) {
        emitted.push(this.spec.emit(key, entry.window, entry.state));
        this.windows.delete(windowKey);
      }
    }

    return emitted;
  }

  /** Flush all remaining windows (e.g., on shutdown). */
  flushAll(): U[] {
    const emitted: U[] = [];
    for (const [windowKey, entry] of this.windows) {
      const key = windowKey.split(":")[0]!;
      emitted.push(this.spec.emit(key, entry.window, entry.state));
    }
    this.windows.clear();
    return emitted;
  }

  /** Snapshot current window state for checkpointing. */
  snapshot(): Array<{ windowKey: string; entry: WindowEntry<S> }> {
    return [...this.windows.entries()].map(([windowKey, entry]) => ({ windowKey, entry }));
  }

  /** Restore window state from a checkpoint. */
  restore(snapshots: Array<{ windowKey: string; entry: WindowEntry<S> }>): void {
    this.windows.clear();
    for (const { windowKey, entry } of snapshots) {
      this.windows.set(windowKey, entry);
    }
  }

  private getOrCreate(windowKey: string, start: number, end: number): WindowEntry<S> {
    let entry = this.windows.get(windowKey);
    if (!entry) {
      entry = {
        window: { start, end },
        state: this.spec.init(),
        lastActivity: start,
      };
      this.windows.set(windowKey, entry);
    }
    return entry;
  }
}
