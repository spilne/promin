import { createHash } from "node:crypto";
import { SystemWallClock, type WallClock } from "@promin/workflow";
import type {
  AuthorizationDecision,
  Principal,
  PrincipalRef,
  SecurityContext,
  ZoryaSecurity,
} from "./security/index.ts";

export type ChannelConversationKind = "direct" | "shared";
export type ChannelSenderKind = "user" | "guest" | "bot";

/** Provider-neutral event reported by a trusted channel adapter. */
export interface ChannelInboundEvent {
  readonly eventId: string;
  readonly conversation: {
    readonly id: string;
    readonly kind: ChannelConversationKind;
  };
  readonly sender: {
    readonly id: string;
    readonly kind: ChannelSenderKind;
  };
  readonly target: {
    readonly namespaceId: string;
    readonly agentId: string;
  };
  readonly message: {
    readonly text: string;
  };
  readonly occurredAt?: string;
}

export interface ChannelIdentityKey {
  readonly channelId: string;
  readonly externalUserId: string;
}

export type ChannelEventCompletionStatus =
  | "unlinked"
  | "refused_sender"
  | "refused_identity"
  | "permission_denied"
  | "security_failed"
  | "audit_failed"
  | "invoke_failed"
  | "invoked";

/** Durable identity-link and delivery ledger implemented by the deployment store. */
export interface ChannelStateStore {
  resolveLinkedUser(key: ChannelIdentityKey): Promise<PrincipalRef | undefined>;
  /** Atomically returns false when this channel/event pair was already claimed. */
  claimEvent(input: {
    readonly channelId: string;
    readonly eventId: string;
    readonly correlationId: string;
    readonly receivedAt: number;
  }): Promise<boolean>;
  /** Completes one claimed event without accepting provider payloads or errors. */
  completeEvent(input: {
    readonly channelId: string;
    readonly eventId: string;
    readonly status: ChannelEventCompletionStatus;
    readonly completedAt: number;
    readonly runId?: string;
  }): Promise<void>;
}

export type ChannelKnowledgeAudience =
  | { readonly kind: "effective-user" }
  | { readonly kind: "org-wide-only" };

export interface ChannelTurnInvocation {
  readonly channelId: string;
  readonly eventId: string;
  readonly correlationId: string;
  readonly namespaceId: string;
  readonly agentId: string;
  readonly threadId: string;
  readonly message: string;
  readonly securityContext: SecurityContext;
  readonly knowledgeAudience: ChannelKnowledgeAudience;
}

export interface ChannelTurnInvoker {
  invoke(input: ChannelTurnInvocation): Promise<{ readonly runId: string }>;
}

export type ChannelRuntimeResult =
  | { readonly kind: "duplicate"; readonly correlationId: string }
  | { readonly kind: "unlinked"; readonly correlationId: string }
  | {
      readonly kind: "refused";
      readonly correlationId: string;
      readonly reason: "sender_not_allowed" | "identity_unavailable" | "permission_denied";
    }
  | {
      readonly kind: "failed";
      readonly correlationId: string;
      readonly reason: "security_failed" | "audit_failed" | "invoke_failed";
    }
  | {
      readonly kind: "accepted";
      readonly correlationId: string;
      readonly threadId: string;
      readonly runId: string;
    };

export interface ZoryaChannelRuntime {
  handle(event: ChannelInboundEvent): Promise<ChannelRuntimeResult>;
}

export interface CoreChannelRuntimeConfig {
  readonly channelId: string;
  readonly actor: Principal;
  readonly store: ChannelStateStore;
  readonly security: Pick<ZoryaSecurity, "resolveIdentity" | "authorize" | "recordAudit">;
  readonly invoker: ChannelTurnInvoker;
  readonly clock?: WallClock;
}

/**
 * Provider-neutral, fail-closed channel admission state machine. Provider SDKs,
 * rendering and transport acknowledgements stay in channel adapters.
 */
export class CoreChannelRuntime implements ZoryaChannelRuntime {
  private readonly channelId: string;
  private readonly actor: Principal;
  private readonly store: ChannelStateStore;
  private readonly security: CoreChannelRuntimeConfig["security"];
  private readonly invoker: ChannelTurnInvoker;
  private readonly clock: WallClock;

  constructor(config: CoreChannelRuntimeConfig) {
    this.channelId = requiredText(config.channelId, "channel id", 128);
    if (config.actor.kind !== "service-account" || !config.actor.canActFor) {
      throw new TypeError("Channel runtime actor must be an act-for service account");
    }
    this.actor = snapshotPrincipal(config.actor);
    this.store = config.store;
    this.security = config.security;
    this.invoker = config.invoker;
    this.clock = config.clock ?? SystemWallClock;
  }

  async handle(input: ChannelInboundEvent): Promise<ChannelRuntimeResult> {
    const event = normalizeEvent(input);
    const correlationId = channelCorrelationId(this.channelId, event.eventId);
    const claimed = await this.store.claimEvent({
      channelId: this.channelId,
      eventId: event.eventId,
      correlationId,
      receivedAt: this.clock.currentTimeMs(),
    });
    if (!claimed) return { kind: "duplicate", correlationId };

    if (event.sender.kind !== "user") {
      await this.complete(event.eventId, "refused_sender");
      return { kind: "refused", correlationId, reason: "sender_not_allowed" };
    }

    let linked: PrincipalRef | undefined;
    try {
      linked = await this.store.resolveLinkedUser({
        channelId: this.channelId,
        externalUserId: event.sender.id,
      });
    } catch {
      await this.complete(event.eventId, "security_failed");
      return { kind: "failed", correlationId, reason: "security_failed" };
    }
    if (!linked) {
      await this.complete(event.eventId, "unlinked");
      return { kind: "unlinked", correlationId };
    }
    if (linked.kind !== "user" || !validText(linked.id, 512)) {
      await this.complete(event.eventId, "refused_identity");
      return { kind: "refused", correlationId, reason: "identity_unavailable" };
    }

    let resolved: SecurityContext | undefined;
    try {
      resolved = await this.security.resolveIdentity({ actor: this.actor, asserted: linked });
    } catch {
      await this.complete(event.eventId, "security_failed");
      return { kind: "failed", correlationId, reason: "security_failed" };
    }
    if (
      !resolved ||
      resolved.actor.kind !== this.actor.kind ||
      resolved.actor.id !== this.actor.id ||
      resolved.effective.kind !== "user" ||
      resolved.effective.id !== linked.id
    ) {
      await this.complete(event.eventId, "refused_identity");
      return { kind: "refused", correlationId, reason: "identity_unavailable" };
    }
    const context = snapshotSecurityContext(resolved);

    let decision: AuthorizationDecision;
    try {
      decision = await this.security.authorize({
        context,
        permission: { resource: "agent", action: "invoke" },
        resource: {
          kind: "agent",
          namespaceId: event.target.namespaceId,
          id: event.target.agentId,
        },
      });
    } catch {
      await this.complete(event.eventId, "security_failed");
      return { kind: "failed", correlationId, reason: "security_failed" };
    }
    if (!(await this.auditAuthorization({ event, context, decision, correlationId }))) {
      await this.complete(event.eventId, "audit_failed");
      return { kind: "failed", correlationId, reason: "audit_failed" };
    }
    if (!decision.allowed) {
      await this.complete(event.eventId, "permission_denied");
      return { kind: "refused", correlationId, reason: "permission_denied" };
    }

    const threadId = channelThreadId({
      channelId: this.channelId,
      conversationId: event.conversation.id,
      userId: linked.id,
    });
    let result: { readonly runId: string };
    try {
      result = await this.invoker.invoke({
        channelId: this.channelId,
        eventId: event.eventId,
        correlationId,
        namespaceId: event.target.namespaceId,
        agentId: event.target.agentId,
        threadId,
        message: event.message.text,
        securityContext: context,
        knowledgeAudience:
          event.conversation.kind === "shared"
            ? { kind: "org-wide-only" }
            : { kind: "effective-user" },
      });
    } catch {
      await this.complete(event.eventId, "invoke_failed");
      return { kind: "failed", correlationId, reason: "invoke_failed" };
    }
    let runId: string;
    try {
      runId = requiredText(result.runId, "channel invocation run id", 512);
    } catch {
      await this.complete(event.eventId, "invoke_failed");
      return { kind: "failed", correlationId, reason: "invoke_failed" };
    }
    await this.complete(event.eventId, "invoked", runId);
    return { kind: "accepted", correlationId, threadId, runId };
  }

  private async auditAuthorization(params: {
    readonly event: NormalizedChannelEvent;
    readonly context: SecurityContext;
    readonly decision: AuthorizationDecision;
    readonly correlationId: string;
  }): Promise<boolean> {
    try {
      await this.security.recordAudit({
        event: "channel.event.authorization",
        occurredAt: new Date(this.clock.currentTimeMs()).toISOString(),
        context: params.context,
        namespaceId: params.event.target.namespaceId,
        resource: {
          kind: "agent",
          namespaceId: params.event.target.namespaceId,
          id: params.event.target.agentId,
        },
        permission: { resource: "agent", action: "invoke" },
        decision: params.decision,
        metadata: {
          channelId: this.channelId,
          correlationId: params.correlationId,
          conversationKind: params.event.conversation.kind,
        },
      });
      return true;
    } catch {
      return false;
    }
  }

  private async complete(
    eventId: string,
    status: ChannelEventCompletionStatus,
    runId?: string,
  ): Promise<void> {
    await this.store.completeEvent({
      channelId: this.channelId,
      eventId,
      status,
      completedAt: this.clock.currentTimeMs(),
      ...(runId !== undefined && { runId }),
    });
  }
}

export interface InMemoryChannelEventRecord {
  readonly channelId: string;
  readonly eventId: string;
  readonly correlationId: string;
  readonly receivedAt: number;
  readonly status?: ChannelEventCompletionStatus;
  readonly completedAt?: number;
  readonly runId?: string;
}

/** Test/development store; production hosts should use a shared durable implementation. */
export class InMemoryChannelStateStore implements ChannelStateStore {
  private readonly links = new Map<string, PrincipalRef>();
  private readonly events = new Map<string, InMemoryChannelEventRecord>();

  linkIdentity(key: ChannelIdentityKey, user: PrincipalRef): void {
    if (user.kind !== "user") throw new TypeError("Channel identities may link only to users");
    this.links.set(identityKey(normalizeIdentityKey(key)), Object.freeze({ ...user }));
  }

  unlinkIdentity(key: ChannelIdentityKey): void {
    this.links.delete(identityKey(normalizeIdentityKey(key)));
  }

  async resolveLinkedUser(key: ChannelIdentityKey): Promise<PrincipalRef | undefined> {
    const linked = this.links.get(identityKey(normalizeIdentityKey(key)));
    return linked && Object.freeze({ ...linked });
  }

  async claimEvent(input: {
    readonly channelId: string;
    readonly eventId: string;
    readonly correlationId: string;
    readonly receivedAt: number;
  }): Promise<boolean> {
    const channelId = requiredText(input.channelId, "channel id", 128);
    const eventId = requiredText(input.eventId, "channel event id", 512);
    const key = eventKey(channelId, eventId);
    if (this.events.has(key)) return false;
    const receivedAt = safeTimestamp(input.receivedAt, "channel event receivedAt");
    this.events.set(
      key,
      Object.freeze({
        channelId,
        eventId,
        correlationId: requiredText(input.correlationId, "channel correlation id", 128),
        receivedAt,
      }),
    );
    return true;
  }

  async completeEvent(input: {
    readonly channelId: string;
    readonly eventId: string;
    readonly status: ChannelEventCompletionStatus;
    readonly completedAt: number;
    readonly runId?: string;
  }): Promise<void> {
    const channelId = requiredText(input.channelId, "channel id", 128);
    const eventId = requiredText(input.eventId, "channel event id", 512);
    const key = eventKey(channelId, eventId);
    const current = this.events.get(key);
    if (!current) throw new Error("Cannot complete an unclaimed channel event");
    if (current.status !== undefined) throw new Error("Channel event is already complete");
    if (!COMPLETION_STATUSES.has(input.status)) {
      throw new TypeError("Invalid channel event completion status");
    }
    if (input.status === "invoked" && input.runId === undefined) {
      throw new TypeError("An invoked channel event requires a run id");
    }
    if (input.status !== "invoked" && input.runId !== undefined) {
      throw new TypeError("Only an invoked channel event may carry a run id");
    }
    const completedAt = safeTimestamp(input.completedAt, "channel event completedAt");
    if (completedAt < current.receivedAt) {
      throw new TypeError("channel event completedAt must not precede receivedAt");
    }
    this.events.set(
      key,
      Object.freeze({
        ...current,
        status: input.status,
        completedAt,
        ...(input.runId !== undefined && {
          runId: requiredText(input.runId, "channel event run id", 512),
        }),
      }),
    );
  }

  event(channelId: string, eventId: string): InMemoryChannelEventRecord | undefined {
    const row = this.events.get(eventKey(channelId, eventId));
    return row && Object.freeze({ ...row });
  }
}

export function channelCorrelationId(channelId: string, eventId: string): string {
  return digest(
    "event",
    requiredText(channelId, "channel id", 128),
    requiredText(eventId, "event id", 512),
  );
}

export function channelThreadId(params: {
  readonly channelId: string;
  readonly conversationId: string;
  readonly userId: string;
}): string {
  return `channel-${digest(
    "thread",
    requiredText(params.channelId, "channel id", 128),
    requiredText(params.conversationId, "conversation id", 512),
    requiredText(params.userId, "user id", 512),
  )}`;
}

interface NormalizedChannelEvent {
  readonly eventId: string;
  readonly conversation: { readonly id: string; readonly kind: ChannelConversationKind };
  readonly sender: { readonly id: string; readonly kind: ChannelSenderKind };
  readonly target: { readonly namespaceId: string; readonly agentId: string };
  readonly message: { readonly text: string };
  readonly occurredAt?: string;
}

function normalizeEvent(input: ChannelInboundEvent): NormalizedChannelEvent {
  if (!input || typeof input !== "object") throw new TypeError("Channel event must be an object");
  const conversation = input.conversation;
  const sender = input.sender;
  const target = input.target;
  const message = input.message;
  if (!conversation || typeof conversation !== "object") {
    throw new TypeError("Channel event conversation is required");
  }
  if (!sender || typeof sender !== "object")
    throw new TypeError("Channel event sender is required");
  if (!target || typeof target !== "object")
    throw new TypeError("Channel event target is required");
  if (!message || typeof message !== "object")
    throw new TypeError("Channel event message is required");
  if (conversation.kind !== "direct" && conversation.kind !== "shared") {
    throw new TypeError("Channel conversation kind must be direct or shared");
  }
  if (sender.kind !== "user" && sender.kind !== "guest" && sender.kind !== "bot") {
    throw new TypeError("Channel sender kind must be user, guest or bot");
  }
  const occurredAt = input.occurredAt;
  if (occurredAt !== undefined && !Number.isFinite(Date.parse(occurredAt))) {
    throw new TypeError("Channel event occurredAt must be an ISO timestamp");
  }
  return Object.freeze({
    eventId: requiredText(input.eventId, "channel event id", 512),
    conversation: Object.freeze({
      id: requiredText(conversation.id, "channel conversation id", 512),
      kind: conversation.kind,
    }),
    sender: Object.freeze({
      id: requiredText(sender.id, "channel sender id", 512),
      kind: sender.kind,
    }),
    target: Object.freeze({
      namespaceId: requiredText(target.namespaceId, "channel namespace id", 256),
      agentId: requiredText(target.agentId, "channel agent id", 256),
    }),
    message: Object.freeze({ text: requiredText(message.text, "channel message", 65_536) }),
    ...(occurredAt !== undefined && { occurredAt }),
  });
}

function normalizeIdentityKey(key: ChannelIdentityKey): ChannelIdentityKey {
  return Object.freeze({
    channelId: requiredText(key.channelId, "channel id", 128),
    externalUserId: requiredText(key.externalUserId, "external user id", 512),
  });
}

function identityKey(key: ChannelIdentityKey): string {
  return JSON.stringify([key.channelId, key.externalUserId]);
}

function eventKey(channelId: string, eventId: string): string {
  return JSON.stringify([
    requiredText(channelId, "channel id", 128),
    requiredText(eventId, "event id", 512),
  ]);
}

function digest(kind: string, ...parts: string[]): string {
  return createHash("sha256")
    .update(JSON.stringify([kind, ...parts]))
    .digest("hex");
}

function requiredText(value: unknown, label: string, maxBytes: number): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${label} must be a non-empty string`);
  }
  if (new TextEncoder().encode(value).byteLength > maxBytes) {
    throw new TypeError(`${label} exceeds ${maxBytes} UTF-8 bytes`);
  }
  return value;
}

function safeTimestamp(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new TypeError(`${label} must be a timestamp`);
  return value;
}

const COMPLETION_STATUSES: ReadonlySet<string> = new Set<ChannelEventCompletionStatus>([
  "unlinked",
  "refused_sender",
  "refused_identity",
  "permission_denied",
  "security_failed",
  "audit_failed",
  "invoke_failed",
  "invoked",
]);

function snapshotPrincipal(principal: Principal): Principal {
  const accessRoles = Object.fromEntries(
    Object.entries(principal.accessRoles).map(([namespaceId, roles]) => [
      namespaceId,
      Object.freeze([...roles]),
    ]),
  );
  return Object.freeze({
    kind: principal.kind,
    id: requiredText(principal.id, "channel actor id", 512),
    ...(principal.email !== undefined && { email: principal.email }),
    accessRoles: Object.freeze(accessRoles),
    ...(principal.teams !== undefined && { teams: Object.freeze([...principal.teams]) }),
    canActFor: principal.canActFor,
  });
}

function snapshotSecurityContext(context: SecurityContext): SecurityContext {
  return Object.freeze({
    actor: snapshotPrincipal(context.actor),
    effective: Object.freeze({
      kind: context.effective.kind,
      id: requiredText(context.effective.id, "effective principal id", 512),
    }),
  });
}

function validText(value: unknown, maxBytes: number): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    new TextEncoder().encode(value).byteLength <= maxBytes
  );
}
