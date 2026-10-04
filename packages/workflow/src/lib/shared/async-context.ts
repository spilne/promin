// ---------------------------------------------------------------------------
// AsyncContext — an AsyncLocalStorage resolved on first use.
//
// The package root must load in runtimes without Node built-ins (browsers,
// bundlers, edge workers), so nothing reachable from it may statically
// import `node:async_hooks`. The journaled-step engine still needs async
// context propagation, so the store is looked up lazily: a global
// `AsyncLocalStorage` (edge runtimes expose one) or
// `process.getBuiltinModule("node:async_hooks")` (Node >= 20.16, Bun, Deno).
//
// Without either, reads see no store and `run` throws: defining workflows
// and running plain steps works everywhere, journaled step bodies need a
// runtime with async context.
// ---------------------------------------------------------------------------

interface AsyncLocalStorageLike<T> {
  run<R>(store: T, fn: () => R): R;
  exit<R>(fn: () => R): R;
  getStore(): T | undefined;
}

type AsyncLocalStorageCtor = new <T>() => AsyncLocalStorageLike<T>;

interface RuntimeGlobals {
  readonly AsyncLocalStorage?: AsyncLocalStorageCtor;
  readonly process?: { readonly getBuiltinModule?: (id: string) => unknown };
}

let resolvedCtor: AsyncLocalStorageCtor | null | undefined;

function asyncLocalStorageCtor(): AsyncLocalStorageCtor | null {
  if (resolvedCtor !== undefined) return resolvedCtor;
  const g = globalThis as RuntimeGlobals;
  if (typeof g.AsyncLocalStorage === "function") {
    resolvedCtor = g.AsyncLocalStorage;
    return resolvedCtor;
  }
  const mod = g.process?.getBuiltinModule?.("node:async_hooks") as
    | { readonly AsyncLocalStorage?: AsyncLocalStorageCtor }
    | undefined;
  resolvedCtor = typeof mod?.AsyncLocalStorage === "function" ? mod.AsyncLocalStorage : null;
  return resolvedCtor;
}

/** An `AsyncLocalStorage<T>` whose backing store is created on first use. */
export class AsyncContext<T> {
  private storage: AsyncLocalStorageLike<T> | null | undefined;

  private resolve(): AsyncLocalStorageLike<T> | null {
    if (this.storage === undefined) {
      const Ctor = asyncLocalStorageCtor();
      this.storage = Ctor === null ? null : new Ctor<T>();
    }
    return this.storage;
  }

  /** Run `fn` with `store` as the current value for its async continuation. */
  run<R>(store: T, fn: () => R): R {
    const storage = this.resolve();
    if (storage === null) {
      throw new Error(
        "[@promin/workflow] journaled steps need AsyncLocalStorage (Node >= 20.16, Bun, Deno " +
          "or a runtime exposing globalThis.AsyncLocalStorage); none is available here",
      );
    }
    return storage.run(store, fn);
  }

  /** Run `fn` outside any store. */
  exit<R>(fn: () => R): R {
    const storage = this.resolve();
    return storage === null ? fn() : storage.exit(fn);
  }

  /** The current store, or `undefined` outside `run`. */
  getStore(): T | undefined {
    return this.resolve()?.getStore();
  }
}
