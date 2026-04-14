// ---------------------------------------------------------------------------
// StateMachine — builder + runtime for durable event-driven state machines
// ---------------------------------------------------------------------------

import type {
  ContextOf,
  EventData,
  EventName,
  MachineSnapshot,
  SendParams,
  StrictSchemas,
  TransitionEvent,
  TransitionTo,
} from "./state-machine-types.ts";
import { transitionTo } from "./state-machine-types.ts";
import { type StateMachineStorage, InMemoryStateMachineStorage } from "./state-machine-storage.ts";
import { type Clock, SystemClock, type RetryPolicy, type SchemaParser } from "@promin/core";

// ---------------------------------------------------------------------------
// Internal config types
// ---------------------------------------------------------------------------

interface StateConfig {
  name: string;
  terminal: boolean;
  onEnter?: (context: unknown, eventData?: unknown) => void | Promise<void>;
  onExit?: (context: unknown, eventData?: unknown) => void | Promise<void>;
  timeout?: TimeoutConfig;
}

interface TimeoutConfig {
  ms: number;
  target: string;
  guard?: (context: unknown) => boolean | Promise<boolean>;
}

/** Special event name written to history when a state's timeout fires. */
export const TIMEOUT_EVENT = "__timeout__";

interface TransitionConfig {
  event: string;
  from: string | string[];
  to?: string; // undefined = conditional (action decides)
  guard?: (context: unknown, eventData: unknown) => boolean | Promise<boolean>;
  action?: (
    context: unknown,
    eventData: unknown,
    transition: TransitionHelper,
  ) => unknown | Promise<unknown>;
  retry?: RetryPolicy<unknown>;
  onError?: string; // target state on action failure (after retries exhausted)
}

type TransitionHelper = <Target extends string>(
  target: Target,
  context: unknown,
) => TransitionTo<any, any>;

/** Context passed to middleware — describes the transition about to happen. */
export interface TransitionContext {
  readonly machineId: string;
  readonly machineName: string;
  readonly event: string;
  readonly from: string;
  to: string;
  context: unknown;
  readonly eventData?: unknown;
  readonly metadata?: unknown;
}

/** Middleware function — call next() to proceed, or throw to abort. */
export type MachineMiddleware = (
  ctx: TransitionContext,
  next: () => Promise<void>,
) => Promise<void> | void;

/** Compose multiple middleware into one. */
export function composeMachineMiddleware(...middlewares: MachineMiddleware[]): MachineMiddleware {
  return (ctx, next) => {
    const dispatch = (i: number): Promise<void> => {
      if (i >= middlewares.length) return Promise.resolve(next());
      return Promise.resolve(middlewares[i]!(ctx, () => dispatch(i + 1)));
    };
    return dispatch(0);
  };
}

/** Reusable retry middleware — wraps every transition with retry logic. */
export function retryMiddleware(policy: RetryPolicy<unknown>): MachineMiddleware {
  return async (_ctx, next) => {
    await executeWithRetryReturn(next, policy);
  };
}

async function executeWithRetryReturn<T>(
  fn: () => T | Promise<T>,
  policy: RetryPolicy<unknown>,
): Promise<T> {
  const maxRetries = policy.maxRetries ?? 3;
  const baseDelay = policy.baseDelayMs ?? 250;
  const maxDelay = policy.maxDelayMs ?? Infinity;
  const jitter = policy.jitter ?? false;
  const timeBudget = policy.timeBudgetMs ?? Infinity;
  const start = Date.now();

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= maxRetries) throw err;
      if (policy.when && !policy.when(err)) throw err;
      if (Date.now() - start >= timeBudget) throw err;

      let delay = Math.min(baseDelay * 2 ** attempt, maxDelay);
      if (jitter) delay *= 0.75 + Math.random() * 0.5;
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw new Error("unreachable");
}

/** Safety limits to prevent infinite loops and runaway machines. */
export interface MachineLimits {
  /** Max total transitions over the machine's lifetime. Default: unlimited. */
  maxTransitions?: number;
  /** Max transitions per second. Default: unlimited. */
  maxTransitionsPerSecond?: number;
}

/** Thrown when `.strict()` validation rejects the `data` payload of a `.send()`. */
export class EventDataValidationError extends Error {
  readonly _tag = "EventDataValidationError";
  constructor(
    readonly event: string,
    override readonly cause: unknown,
  ) {
    super(`Event "${event}" payload failed schema validation`);
  }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

export class StateMachineBuilder<S, Events = void> {
  private states = new Map<string, StateConfig>();
  private transitions: TransitionConfig[] = [];
  private middlewares: MachineMiddleware[] = [];
  private initialState?: string;
  private strictSchemas?: Record<string, SchemaParser<unknown>>;

  constructor(
    private readonly name: string,
    private readonly storage: StateMachineStorage,
    private readonly version?: string,
    private readonly limits?: MachineLimits,
    private readonly type?: string,
    private readonly namespace?: string,
    private readonly clock?: Clock,
    private readonly autoScheduleTimeouts?: boolean,
  ) {}

  /** Add middleware that wraps every transition. Composable — called in order. */
  use(middleware: MachineMiddleware): this {
    this.middlewares.push(middleware);
    return this;
  }

  state(
    name: string & keyof S,
    options?: {
      terminal?: boolean;
      onEnter?: (context: any, eventData?: any) => void | Promise<void>;
      onExit?: (context: any, eventData?: any) => void | Promise<void>;
      /**
       * Auto-transition to `target` after `ms` if no event arrives. Optional
       * `guard` lets the timeout decide whether to actually fire. Cancelled on
       * any explicit transition out of this state.
       */
      timeout?: {
        ms: number;
        target: string & keyof S;
        guard?: (context: any) => boolean | Promise<boolean>;
      };
    },
  ): this {
    this.states.set(name as string, {
      name: name as string,
      terminal: options?.terminal ?? false,
      onEnter: options?.onEnter,
      onExit: options?.onExit,
      timeout: options?.timeout
        ? { ms: options.timeout.ms, target: options.timeout.target, guard: options.timeout.guard }
        : undefined,
    });
    return this;
  }

  on<K extends EventName<Events>>(
    event: K,
    config: {
      from: (string & keyof S) | (string & keyof S)[];
      to?: string & keyof S;
      guard?: (
        context: any,
        event: [Events] extends [void] ? any : EventData<Events, K & keyof Events>,
      ) => boolean | Promise<boolean>;
      action?: (
        context: any,
        event: [Events] extends [void] ? any : EventData<Events, K & keyof Events>,
        transition: TransitionHelper,
      ) => any | Promise<any>;
      retry?: RetryPolicy<unknown>;
      onError?: string & keyof S;
    },
  ): this {
    this.transitions.push({
      event: event as string,
      from: config.from,
      to: config.to,
      guard: config.guard as TransitionConfig["guard"],
      action: config.action as TransitionConfig["action"],
      retry: config.retry,
      onError: config.onError,
    });
    return this;
  }

  initial(state: string & keyof S): this {
    this.initialState = state as string;
    return this;
  }

  /**
   * Enable runtime validation of `send()` data using SchemaParsers.
   *
   * Pass a per-event schema map. Each schema validates the `data` field of its
   * matching event before the transition runs. Validation failures throw
   * `EventDataValidationError` (the transition does not execute).
   *
   * Re-calling `.strict()` merges with the existing schema map, so per-event
   * stricter overrides can be layered on top of a base map.
   */
  strict(schemas: StrictSchemas<Events>): this {
    this.strictSchemas = {
      ...(this.strictSchemas ?? {}),
      ...(schemas as Record<string, SchemaParser<unknown>>),
    };
    return this;
  }

  build(): StateMachineInstance<S, Events> {
    if (!this.initialState) throw new Error("Initial state not set");
    if (this.states.size === 0) throw new Error("No states registered");

    // Notify storage of terminal states (for TTL-based cleanup)
    if ("registerTerminalStates" in this.storage) {
      const terminals = [...this.states.values()].filter((s) => s.terminal).map((s) => s.name);
      (this.storage as any).registerTerminalStates(terminals);
    }
    return new StateMachineInstance<S, Events>(
      this.name,
      this.storage,
      this.states,
      this.transitions,
      this.initialState,
      this.version,
      this.limits,
      this.middlewares.length > 0 ? composeMachineMiddleware(...this.middlewares) : undefined,
      this.type,
      this.namespace,
      this.strictSchemas,
      this.clock ?? SystemClock,
      this.autoScheduleTimeouts ?? true,
    );
  }
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

export class StateMachineInstance<S, Events = void> {
  private recentSendTimestamps: number[] = [];
  private timers = new Map<string, ReturnType<typeof setTimeout>>();

  constructor(
    private readonly name: string,
    private readonly storage: StateMachineStorage,
    private readonly states: Map<string, StateConfig>,
    private readonly transitions: TransitionConfig[],
    private readonly initialState: string,
    private readonly version?: string,
    private readonly limits?: MachineLimits,
    private readonly middleware?: MachineMiddleware,
    private readonly type?: string,
    private readonly namespace?: string,
    private readonly strictSchemas?: Record<string, SchemaParser<unknown>>,
    private readonly clock: Clock = SystemClock,
    private readonly autoScheduleTimeouts: boolean = true,
  ) {}

  async start(params: {
    id: string;
    context: ContextOf<S, keyof S & string>;
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    const existing = await this.storage.load(params.id);
    if (existing) throw new Error(`Machine ${params.id} already exists`);

    await this.storage.create({
      id: params.id,
      name: this.name,
      type: this.type,
      namespace: this.namespace,
      initial: this.initialState,
      context: params.context,
      version: this.version,
      metadata: params.metadata,
    });

    this.scheduleTimeoutIfAny(params.id, this.initialState);
  }

  async send(params: SendParams<Events>): Promise<void> {
    // Type-erase the union — runtime treats data uniformly.
    const { id, event, metadata } = params as {
      id: string;
      event: string;
      metadata?: unknown;
    };
    let data = (params as { data?: unknown }).data;

    // Validate data via .strict() schema if registered for this event.
    if (this.strictSchemas && event in this.strictSchemas) {
      const parser = this.strictSchemas[event]!;
      const result = parser.safeParse(data);
      if (!result.success) {
        throw new EventDataValidationError(event, result.error);
      }
      data = result.data;
    }

    const locked = await this.storage.tryLock(id, 30_000);
    if (!locked) throw new Error(`Machine ${id} is locked`);

    // Cancel any pending timeout — this transition supersedes it.
    this.cancelTimeout(id);

    try {
      const machine = await this.storage.load(id);
      if (!machine) throw new Error(`Machine ${id} not found`);

      // Check limits
      if (this.limits?.maxTransitions) {
        const events = await this.storage.loadEvents(id);
        if (events.length >= this.limits.maxTransitions) {
          throw new Error(
            `Machine ${id} exceeded max transitions limit (${this.limits.maxTransitions})`,
          );
        }
      }

      if (this.limits?.maxTransitionsPerSecond) {
        const now = Date.now();
        this.recentSendTimestamps = this.recentSendTimestamps.filter((t) => t > now - 1000);
        if (this.recentSendTimestamps.length >= this.limits.maxTransitionsPerSecond) {
          throw new Error(
            `Machine ${id} exceeded rate limit (${this.limits.maxTransitionsPerSecond}/sec)`,
          );
        }
        this.recentSendTimestamps.push(now);
      }

      // Find matching transition
      const transition = this.transitions.find((t) => {
        const froms = Array.isArray(t.from) ? t.from : [t.from];
        return t.event === event && froms.includes(machine.current);
      });
      if (!transition) {
        throw new Error(`No transition for event "${event}" from state "${machine.current}"`);
      }

      // Check guard
      if (transition.guard) {
        const allowed = await transition.guard(machine.context, data);
        if (!allowed) throw new Error(`Guard rejected event "${event}"`);
      }

      // Execute action — compute target state and new context
      let targetState: string;
      let newContext: unknown;

      try {
        if (transition.action) {
          const helper = <Target extends string>(target: Target, ctx: unknown) =>
            transitionTo<any, any>(target, ctx);

          const executeAction = () => transition.action!(machine.context, data, helper);
          const result = transition.retry
            ? await executeWithRetryReturn(executeAction, transition.retry)
            : await executeAction();

          if (
            result &&
            typeof result === "object" &&
            "_tag" in result &&
            result._tag === "TransitionTo"
          ) {
            const tt = result as TransitionTo<any, any>;
            targetState = tt.target as string;
            newContext = tt.context;
          } else if (transition.to) {
            targetState = transition.to;
            newContext = result;
          } else {
            throw new Error(
              `Transition for "${event}" has no target state and action didn't return TransitionTo`,
            );
          }
        } else if (transition.to) {
          targetState = transition.to;
          newContext = machine.context;
        } else {
          throw new Error(`Transition for "${event}" has no action and no target state`);
        }
      } catch (err) {
        if (transition.onError && this.states.has(transition.onError)) {
          // Route to error state instead of throwing
          targetState = transition.onError;
          newContext = {
            ...(machine.context as any),
            error: err instanceof Error ? err.message : String(err),
          };
        } else {
          throw err;
        }
      }

      // Validate target state exists
      if (!this.states.has(targetState)) {
        throw new Error(`Target state "${targetState}" is not registered`);
      }

      // Build transition context for middleware
      const txCtx: TransitionContext = {
        machineId: id,
        machineName: this.name,
        event,
        from: machine.current,
        to: targetState,
        context: newContext,
        eventData: data,
        metadata,
      };

      // Core transition: onExit → persist → onEnter
      const executeTransition = async () => {
        const currentStateConfig = this.states.get(txCtx.from);
        if (currentStateConfig?.onExit) {
          await currentStateConfig.onExit(machine.context, data);
        }

        await this.storage.transition({
          id,
          from: txCtx.from,
          to: txCtx.to,
          event,
          context: txCtx.context,
          eventData: txCtx.eventData,
          metadata: txCtx.metadata,
        });

        const targetStateConfig = this.states.get(txCtx.to);
        if (targetStateConfig?.onEnter) {
          await targetStateConfig.onEnter(txCtx.context, data);
        }
      };

      // Wrap with middleware if present
      if (this.middleware) {
        await this.middleware(txCtx, executeTransition);
      } else {
        await executeTransition();
      }

      // Schedule a fresh timeout for the new state if it has one.
      this.scheduleTimeoutIfAny(id, txCtx.to);
    } finally {
      await this.storage.releaseLock(id);
    }
  }

  // ---------------------------------------------------------------------------
  // Timed auto-transitions
  // ---------------------------------------------------------------------------

  /**
   * Manually check whether the machine `id` has a due timeout and fire it if so.
   * Returns `true` if a transition fired. Useful for FakeClock-driven tests and
   * future distributed scanners that need to drive timeouts externally.
   */
  async checkTimeouts(id: string): Promise<boolean> {
    const machine = await this.storage.load(id);
    if (!machine) return false;
    const cfg = this.states.get(machine.current)?.timeout;
    if (!cfg) return false;
    const dueAt = machine.updatedAt.getTime() + cfg.ms;
    if (this.clock.currentTimeMs() < dueAt) return false;
    return await this.fireTimeout(id, machine.current);
  }

  /** Cancel all pending in-process timers — call before discarding the instance. */
  cancelAllTimeouts(): void {
    for (const handle of this.timers.values()) clearTimeout(handle);
    this.timers.clear();
  }

  private scheduleTimeoutIfAny(id: string, stateName: string): void {
    if (!this.autoScheduleTimeouts) return;
    const cfg = this.states.get(stateName)?.timeout;
    if (!cfg) return;
    this.cancelTimeout(id);
    const handle = setTimeout(() => {
      this.timers.delete(id);
      // Errors are swallowed — timeout firing must not crash the host process.
      // Callers that need failure visibility should use checkTimeouts() directly.
      this.fireTimeout(id, stateName).catch(() => {});
    }, cfg.ms);
    // Don't keep the event loop alive for pending state-machine timeouts.
    if (typeof (handle as { unref?: () => void }).unref === "function") {
      (handle as { unref: () => void }).unref();
    }
    this.timers.set(id, handle);
  }

  private cancelTimeout(id: string): void {
    const handle = this.timers.get(id);
    if (handle) {
      clearTimeout(handle);
      this.timers.delete(id);
    }
  }

  /** Acquire lock, verify state hasn't changed, run guard, transition, reschedule. */
  private async fireTimeout(id: string, fromState: string): Promise<boolean> {
    const cfg = this.states.get(fromState)?.timeout;
    if (!cfg) return false;

    const locked = await this.storage.tryLock(id, 30_000);
    if (!locked) return false;
    try {
      const machine = await this.storage.load(id);
      if (!machine) return false;
      // State may have moved while we waited — bail rather than misfiring.
      if (machine.current !== fromState) return false;

      if (cfg.guard) {
        const allowed = await cfg.guard(machine.context);
        if (!allowed) return false;
      }

      if (!this.states.has(cfg.target)) {
        throw new Error(`Timeout target state "${cfg.target}" is not registered`);
      }

      const txCtx: TransitionContext = {
        machineId: id,
        machineName: this.name,
        event: TIMEOUT_EVENT,
        from: fromState,
        to: cfg.target,
        context: machine.context,
        metadata: undefined,
      };

      const executeTransition = async () => {
        const fromConfig = this.states.get(fromState);
        if (fromConfig?.onExit) await fromConfig.onExit(machine.context, undefined);

        await this.storage.transition({
          id,
          from: fromState,
          to: cfg.target,
          event: TIMEOUT_EVENT,
          context: machine.context,
        });

        const toConfig = this.states.get(cfg.target);
        if (toConfig?.onEnter) await toConfig.onEnter(machine.context, undefined);
      };

      if (this.middleware) {
        await this.middleware(txCtx, executeTransition);
      } else {
        await executeTransition();
      }

      this.scheduleTimeoutIfAny(id, cfg.target);
      return true;
    } finally {
      await this.storage.releaseLock(id);
    }
  }

  async getState(id: string): Promise<MachineSnapshot<S> | null> {
    const machine = await this.storage.load(id);
    if (!machine) return null;
    return { current: machine.current, context: machine.context } as MachineSnapshot<S>;
  }

  async getSnapshot(
    id: string,
  ): Promise<(MachineSnapshot<S> & { name: string; version?: string }) | null> {
    const machine = await this.storage.load(id);
    if (!machine) return null;
    return {
      current: machine.current,
      context: machine.context,
      name: machine.name,
      version: machine.version,
    } as MachineSnapshot<S> & { name: string; version?: string };
  }

  async restore(params: {
    id: string;
    snapshot: { current: string; context: unknown; name?: string; version?: string };
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    const existing = await this.storage.load(params.id);
    if (existing) throw new Error(`Machine ${params.id} already exists`);

    await this.storage.create({
      id: params.id,
      name: params.snapshot.name ?? this.name,
      initial: params.snapshot.current,
      context: params.snapshot.context,
      version: params.snapshot.version ?? this.version,
      metadata: params.metadata,
    });
  }

  async getHistory(
    id: string,
    params?: { limit?: number; offset?: number },
  ): Promise<TransitionEvent[]> {
    return this.storage.loadEvents(id, params);
  }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function stateMachine<S, Events = void>(params: {
  name: string;
  storage: StateMachineStorage;
  version?: string;
  limits?: MachineLimits;
  type?: string;
  namespace?: string;
  /** Time source for due-timeout calculations. Default: SystemClock. */
  clock?: Clock;
  /**
   * Whether to schedule in-process setTimeout for state timeouts. Default: true.
   * Set false for distributed mode where an external scanner drives `checkTimeouts()`.
   */
  autoScheduleTimeouts?: boolean;
}): StateMachineBuilder<S, Events> {
  return new StateMachineBuilder<S, Events>(
    params.name,
    params.storage,
    params.version,
    params.limits,
    params.type,
    params.namespace,
    params.clock,
    params.autoScheduleTimeouts,
  );
}

// ---------------------------------------------------------------------------
// machine() — quick in-memory shortcut (mirrors flow() for workflows)
// ---------------------------------------------------------------------------

/** Live single-instance handle returned by `machine().run()`. */
export class MachineHandle<S, Events = void> {
  constructor(
    readonly id: string,
    private readonly instance: StateMachineInstance<S, Events>,
  ) {}

  /** Send an event. `id` is fixed to this handle's instance and omitted from params. */
  send(params: Omit<SendParams<Events>, "id">): Promise<void> {
    return this.instance.send({ ...(params as object), id: this.id } as SendParams<Events>);
  }

  getState(): Promise<MachineSnapshot<S> | null> {
    return this.instance.getState(this.id);
  }

  getSnapshot(): Promise<(MachineSnapshot<S> & { name: string; version?: string }) | null> {
    return this.instance.getSnapshot(this.id);
  }

  getHistory(params?: { limit?: number; offset?: number }): Promise<TransitionEvent[]> {
    return this.instance.getHistory(this.id, params);
  }
}

/**
 * Builder returned by `machine()`. Same as StateMachineBuilder but with `.run()`
 * for one-shot prototyping — auto-builds, auto-starts, auto-generates id.
 */
export class QuickMachineBuilder<S, Events = void> extends StateMachineBuilder<S, Events> {
  private readonly defaultContext: ContextOf<S, keyof S & string>;

  constructor(params: {
    name: string;
    storage: StateMachineStorage;
    initial: string & keyof S;
    context: ContextOf<S, keyof S & string>;
    version?: string;
    limits?: MachineLimits;
  }) {
    super(params.name, params.storage, params.version, params.limits);
    this.defaultContext = params.context;
    this.initial(params.initial);
  }

  /** Build, start, and return a single-instance handle. Auto-generates id if omitted. */
  async run(params?: {
    id?: string;
    context?: ContextOf<S, keyof S & string>;
  }): Promise<MachineHandle<S, Events>> {
    const inst = this.build();
    const id = params?.id ?? `m-${crypto.randomUUID()}`;
    const ctx = params?.context ?? this.defaultContext;
    await inst.start({ id, context: ctx });
    return new MachineHandle<S, Events>(id, inst);
  }
}

/**
 * Quick in-memory state machine — zero setup for prototyping and tests.
 *
 * ```typescript
 * const m = await machine<Light>({ initial: "red", context: { count: 0 } })
 *   .state("red").state("green").state("yellow")
 *   .on("next", { from: "red", to: "green", action: (c) => ({ count: c.count + 1 }) })
 *   .on("next", { from: "green", to: "yellow", action: (c) => ({ count: c.count + 1 }) })
 *   .on("next", { from: "yellow", to: "red", action: (c) => ({ count: c.count + 1 }) })
 *   .run();
 *
 * await m.send({ event: "next" });
 * ```
 */
export function machine<S, Events = void>(params: {
  initial: string & keyof S;
  context?: ContextOf<S, keyof S & string>;
  name?: string;
  version?: string;
  limits?: MachineLimits;
}): QuickMachineBuilder<S, Events> {
  return new QuickMachineBuilder<S, Events>({
    name: params.name ?? "machine",
    storage: new InMemoryStateMachineStorage(),
    initial: params.initial,
    context: params.context ?? ({} as ContextOf<S, keyof S & string>),
    version: params.version,
    limits: params.limits,
  });
}

// ---------------------------------------------------------------------------
// pureStateMachine() — sync-only, deterministic decision engine
// ---------------------------------------------------------------------------
//
// Same runtime as stateMachine(), narrower compile-time signatures: guards and
// actions cannot return Promises. Use when the machine is a pure decision layer
// over an event log (deterministic replay, no I/O).
// ---------------------------------------------------------------------------

/**
 * Type alias narrowing `StateMachineBuilder` to sync-only guards/actions.
 *
 * Shares the same runtime — only compile-time signatures differ. Async
 * guards/actions become a compile error since `Promise<T>` does not match
 * the narrowed `T` return type.
 */
export interface PureStateMachineBuilder<S, Events = void> {
  use(middleware: MachineMiddleware): this;
  state(
    name: string & keyof S,
    options?: {
      terminal?: boolean;
      onEnter?: (context: any, eventData?: any) => void;
      onExit?: (context: any, eventData?: any) => void;
    },
  ): this;
  on<K extends EventName<Events>>(
    event: K,
    config: {
      from: (string & keyof S) | (string & keyof S)[];
      to?: string & keyof S;
      guard?: (
        context: any,
        event: [Events] extends [void] ? any : EventData<Events, K & keyof Events>,
      ) => boolean;
      action?: (
        context: any,
        event: [Events] extends [void] ? any : EventData<Events, K & keyof Events>,
        transition: TransitionHelper,
      ) => any;
      retry?: RetryPolicy<unknown>;
      onError?: string & keyof S;
    },
  ): this;
  initial(state: string & keyof S): this;
  strict(schemas: StrictSchemas<Events>): this;
  build(): StateMachineInstance<S, Events>;
}

/**
 * Sync-only state machine — same runtime as `stateMachine()` but guards/actions
 * cannot return Promises. Enables deterministic replay and pure decision logic.
 */
export function pureStateMachine<S, Events = void>(params: {
  name: string;
  storage: StateMachineStorage;
  version?: string;
  limits?: MachineLimits;
  type?: string;
  namespace?: string;
  clock?: Clock;
  autoScheduleTimeouts?: boolean;
}): PureStateMachineBuilder<S, Events> {
  return stateMachine<S, Events>(params) as unknown as PureStateMachineBuilder<S, Events>;
}
