// ---------------------------------------------------------------------------
// StateMachine — builder + runtime for durable event-driven state machines
// ---------------------------------------------------------------------------

import type {
  ContextOf,
  MachineSnapshot,
  TransitionEvent,
  TransitionTo,
} from "./state-machine-types.ts";
import { transitionTo } from "./state-machine-types.ts";
import type { StateMachineStorage } from "./state-machine-storage.ts";

// ---------------------------------------------------------------------------
// Internal config types
// ---------------------------------------------------------------------------

interface StateConfig {
  name: string;
  terminal: boolean;
  onEnter?: (context: unknown) => void | Promise<void>;
  onExit?: (context: unknown) => void | Promise<void>;
}

interface TransitionConfig {
  event: string;
  from: string | string[];
  to?: string; // undefined = conditional (action decides)
  guard?: (context: unknown) => boolean | Promise<boolean>;
  action?: (context: unknown, transition: TransitionHelper) => unknown | Promise<unknown>;
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
    let index = -1;
    const dispatch = (i: number): Promise<void> => {
      if (i <= index) return Promise.reject(new Error("next() called multiple times"));
      index = i;
      if (i >= middlewares.length) return Promise.resolve(next());
      return Promise.resolve(middlewares[i]!(ctx, () => dispatch(i + 1)));
    };
    return dispatch(0);
  };
}

/** Safety limits to prevent infinite loops and runaway machines. */
export interface MachineLimits {
  /** Max total transitions over the machine's lifetime. Default: unlimited. */
  maxTransitions?: number;
  /** Max transitions per second. Default: unlimited. */
  maxTransitionsPerSecond?: number;
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

export class StateMachineBuilder<S> {
  private states = new Map<string, StateConfig>();
  private transitions: TransitionConfig[] = [];
  private middlewares: MachineMiddleware[] = [];
  private initialState?: string;

  constructor(
    private readonly name: string,
    private readonly storage: StateMachineStorage,
    private readonly version?: string,
    private readonly limits?: MachineLimits,
    private readonly type?: string,
    private readonly namespace?: string,
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
      onEnter?: (context: any) => void | Promise<void>;
      onExit?: (context: any) => void | Promise<void>;
    },
  ): this {
    this.states.set(name as string, {
      name: name as string,
      terminal: options?.terminal ?? false,
      onEnter: options?.onEnter,
      onExit: options?.onExit,
    });
    return this;
  }

  on(
    event: string,
    config: {
      from: (string & keyof S) | (string & keyof S)[];
      to?: string & keyof S;
      guard?: (context: any) => boolean | Promise<boolean>;
      action?: (context: any, transition: any) => any | Promise<any>;
    },
  ): this {
    this.transitions.push({
      event,
      from: config.from,
      to: config.to,
      guard: config.guard,
      action: config.action,
    });
    return this;
  }

  initial(state: string & keyof S): this {
    this.initialState = state as string;
    return this;
  }

  build(): StateMachineInstance<S> {
    if (!this.initialState) throw new Error("Initial state not set");
    if (this.states.size === 0) throw new Error("No states registered");
    return new StateMachineInstance<S>(
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
    );
  }
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

export class StateMachineInstance<S> {
  private recentSendTimestamps: number[] = [];

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
  }

  async send(params: { id: string; event: string; metadata?: unknown }): Promise<void> {
    const locked = await this.storage.tryLock(params.id, 30_000);
    if (!locked) throw new Error(`Machine ${params.id} is locked`);

    try {
      const machine = await this.storage.load(params.id);
      if (!machine) throw new Error(`Machine ${params.id} not found`);

      // Check limits
      if (this.limits?.maxTransitions) {
        const events = await this.storage.loadEvents(params.id);
        if (events.length >= this.limits.maxTransitions) {
          throw new Error(
            `Machine ${params.id} exceeded max transitions limit (${this.limits.maxTransitions})`,
          );
        }
      }

      if (this.limits?.maxTransitionsPerSecond) {
        const now = Date.now();
        this.recentSendTimestamps = this.recentSendTimestamps.filter((t) => t > now - 1000);
        if (this.recentSendTimestamps.length >= this.limits.maxTransitionsPerSecond) {
          throw new Error(
            `Machine ${params.id} exceeded rate limit (${this.limits.maxTransitionsPerSecond}/sec)`,
          );
        }
        this.recentSendTimestamps.push(now);
      }

      // Find matching transition
      const transition = this.transitions.find((t) => {
        const froms = Array.isArray(t.from) ? t.from : [t.from];
        return t.event === params.event && froms.includes(machine.current);
      });
      if (!transition) {
        throw new Error(
          `No transition for event "${params.event}" from state "${machine.current}"`,
        );
      }

      // Check guard
      if (transition.guard) {
        const allowed = await transition.guard(machine.context);
        if (!allowed) throw new Error(`Guard rejected event "${params.event}"`);
      }

      // Execute action — compute target state and new context
      let targetState: string;
      let newContext: unknown;

      if (transition.action) {
        const helper = <Target extends string>(target: Target, ctx: unknown) =>
          transitionTo<any, any>(target, ctx);

        const result = await transition.action(machine.context, helper);

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
            `Transition for "${params.event}" has no target state and action didn't return TransitionTo`,
          );
        }
      } else if (transition.to) {
        targetState = transition.to;
        newContext = machine.context;
      } else {
        throw new Error(`Transition for "${params.event}" has no action and no target state`);
      }

      // Validate target state exists
      if (!this.states.has(targetState)) {
        throw new Error(`Target state "${targetState}" is not registered`);
      }

      // Build transition context for middleware
      const txCtx: TransitionContext = {
        machineId: params.id,
        machineName: this.name,
        event: params.event,
        from: machine.current,
        to: targetState,
        context: newContext,
        metadata: params.metadata,
      };

      // Core transition: onExit → persist → onEnter
      const executeTransition = async () => {
        const currentStateConfig = this.states.get(txCtx.from);
        if (currentStateConfig?.onExit) {
          await currentStateConfig.onExit(machine.context);
        }

        await this.storage.transition({
          id: params.id,
          from: txCtx.from,
          to: txCtx.to,
          event: params.event,
          context: txCtx.context,
          metadata: txCtx.metadata,
        });

        const targetStateConfig = this.states.get(txCtx.to);
        if (targetStateConfig?.onEnter) {
          await targetStateConfig.onEnter(txCtx.context);
        }
      };

      // Wrap with middleware if present
      if (this.middleware) {
        await this.middleware(txCtx, executeTransition);
      } else {
        await executeTransition();
      }
    } finally {
      await this.storage.releaseLock(params.id);
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

export function stateMachine<S>(params: {
  name: string;
  storage: StateMachineStorage;
  version?: string;
  limits?: MachineLimits;
  type?: string;
  namespace?: string;
}): StateMachineBuilder<S> {
  return new StateMachineBuilder<S>(
    params.name,
    params.storage,
    params.version,
    params.limits,
    params.type,
    params.namespace,
  );
}
