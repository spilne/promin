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

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

export class StateMachineBuilder<S> {
  private states = new Map<string, StateConfig>();
  private transitions: TransitionConfig[] = [];
  private initialState?: string;

  constructor(
    private readonly name: string,
    private readonly storage: StateMachineStorage,
    private readonly version?: string,
  ) {}

  state(name: string & keyof S, options?: { terminal?: boolean }): this {
    this.states.set(name as string, {
      name: name as string,
      terminal: options?.terminal ?? false,
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
    );
  }
}

// ---------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------

export class StateMachineInstance<S> {
  constructor(
    private readonly name: string,
    private readonly storage: StateMachineStorage,
    private readonly states: Map<string, StateConfig>,
    private readonly transitions: TransitionConfig[],
    private readonly initialState: string,
    private readonly version?: string,
  ) {}

  async start(params: { id: string; context: ContextOf<S, keyof S & string> }): Promise<void> {
    const existing = await this.storage.load(params.id);
    if (existing) throw new Error(`Machine ${params.id} already exists`);

    await this.storage.create({
      id: params.id,
      name: this.name,
      initial: this.initialState,
      context: params.context,
      version: this.version,
    });
  }

  async send(params: { id: string; event: string; metadata?: unknown }): Promise<void> {
    const locked = await this.storage.tryLock(params.id, 30_000);
    if (!locked) throw new Error(`Machine ${params.id} is locked`);

    try {
      const machine = await this.storage.load(params.id);
      if (!machine) throw new Error(`Machine ${params.id} not found`);

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

      // Execute action
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

      // Persist transition
      await this.storage.transition({
        id: params.id,
        from: machine.current,
        to: targetState,
        event: params.event,
        context: newContext,
        metadata: params.metadata,
      });
    } finally {
      await this.storage.releaseLock(params.id);
    }
  }

  async getState(id: string): Promise<MachineSnapshot<S> | null> {
    const machine = await this.storage.load(id);
    if (!machine) return null;
    return { current: machine.current, context: machine.context } as MachineSnapshot<S>;
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
}): StateMachineBuilder<S> {
  return new StateMachineBuilder<S>(params.name, params.storage, params.version);
}
