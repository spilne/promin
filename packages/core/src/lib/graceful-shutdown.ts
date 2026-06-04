// ---------------------------------------------------------------------------
// GracefulShutdown — one signal + a teardown registry for clean shutdown
//
// One AbortController drives everything: stream resources interrupt on
// `signal` (e.g. `pipeline.interruptOn(shutdown.signal)`), and non-stream
// resources (Kafka producers, DB/Redis/HTTP clients) register a teardown via
// `onShutdown`. `run()` aborts the signal and awaits every registered
// teardown, so in-flight work flushes before the process exits.
//
// Resources register their own teardown at construction, so the SIGTERM
// handler stays a one-liner and never has to enumerate every resource:
//
//   const shutdown = createGracefulShutdown();
//   producer = makeProducer({ shutdown });            // self-registers stop()
//   topic.subscribeAck().interruptOn(shutdown.signal)  // interrupts on shutdown
//     .through(commitBatchWithin(...)).drain();
//   process.on("SIGTERM", async () => { await shutdown.run(); process.exit(0); });
// ---------------------------------------------------------------------------

export interface GracefulShutdown {
  /** Aborts when shutdown begins — wire into streams via `interruptOn`. */
  readonly signal: AbortSignal;
  /** Register a teardown to run (and be awaited) on shutdown. */
  onShutdown(close: () => Promise<void>): void;
  /** True once `run()` has started — guards double-shutdown. */
  readonly isShuttingDown: boolean;
  /** Abort the signal and await all registered teardowns. Idempotent. */
  run(): Promise<void>;
}

export function createGracefulShutdown(): GracefulShutdown {
  const controller = new AbortController();
  const closers: Array<() => Promise<void>> = [];
  let shuttingDown = false;
  let ran: Promise<void> | undefined;

  return {
    signal: controller.signal,
    onShutdown(close) {
      closers.push(close);
    },
    get isShuttingDown() {
      return shuttingDown;
    },
    run() {
      // Idempotent — repeated SIGTERM/SIGINT collapse onto the first run.
      if (ran) return ran;
      shuttingDown = true;
      controller.abort();
      // Wrap each closer so a *synchronous* throw becomes a rejected promise
      // (caught by allSettled) rather than escaping before `ran` is set —
      // which would break idempotency and skip the remaining closers.
      ran = Promise.allSettled(closers.map((close) => Promise.resolve().then(close))).then(
        () => undefined,
      );
      return ran;
    },
  };
}
