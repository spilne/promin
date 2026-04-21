/**
 * Turns any AsyncIterable into a multicast hub.
 *
 * The source is consumed eagerly the moment broadcast() is called, buffering
 * all items so late subscribers still receive the full sequence from the start.
 *
 * Usage:
 *   const hub = broadcast(session.stream("hello"));
 *   for await (const chunk of hub.subscribe()) { ... } // display
 *   for await (const chunk of hub.subscribe()) { ... } // log to DB
 */
export function broadcast<T>(source: AsyncIterable<T>): { subscribe(): AsyncIterable<T> } {
  const buffer: T[] = [];
  let sourceDone = false;
  let sourceError: unknown = undefined;
  const waiters = new Set<() => void>();

  function notifyAll() {
    for (const w of waiters) w();
    waiters.clear();
  }

  (async () => {
    try {
      for await (const item of source) {
        buffer.push(item);
        notifyAll();
      }
    } catch (err) {
      sourceError = err;
    } finally {
      sourceDone = true;
      notifyAll();
    }
  })();

  return {
    subscribe(): AsyncIterable<T> {
      return {
        [Symbol.asyncIterator]() {
          let pos = 0;
          return {
            async next(): Promise<IteratorResult<T>> {
              while (true) {
                if (pos < buffer.length) return { value: buffer[pos++]!, done: false };
                if (sourceDone) {
                  if (sourceError !== undefined) throw sourceError;
                  return { value: undefined as never, done: true };
                }
                await new Promise<void>((resolve) => waiters.add(resolve));
              }
            },
            return(): Promise<IteratorResult<T>> {
              return Promise.resolve({ value: undefined as never, done: true });
            },
          };
        },
      };
    },
  };
}
