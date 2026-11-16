import type { RegisterRetrieverInput, RetrieverRegistry } from "@promin/agent";
import type { PrincipalRef } from "./security/index.ts";
import type { ZoryaChannelRuntime } from "./channel-runtime.ts";
import type {
  DeclaredRouteHandler,
  ProtectedRouteAccess,
  RouteDefinition,
  Router,
} from "./router.ts";
import type { ZoryaServerBuilder } from "./server-builder.ts";

/** Trusted in-process channel package. Provider/channel semantics are layered on this lifecycle. */
export interface ZoryaChannel {
  readonly id: string;
  /** External identities require durable user linking and therefore an enterprise security module. */
  readonly mapsExternalUsers?: boolean;
  /** Core admission runtime used by this adapter; required when external users are mapped. */
  readonly runtime?: ZoryaChannelRuntime;
  start?(context: { readonly runtime?: ZoryaChannelRuntime }): void | Promise<void>;
  stop?(): void | Promise<void>;
}

export interface LlmPolicyRequest {
  readonly provider: string;
  readonly model: string;
  readonly namespaceId?: string;
  readonly resourceId?: string;
  readonly principal?: PrincipalRef;
}

export interface LlmPolicyDecision {
  readonly allowed: boolean;
  readonly reason: string;
}

/** Host-level policy hook; provider governance supplies the first production implementation. */
export interface ZoryaLlmPolicy {
  authorize(request: LlmPolicyRequest): LlmPolicyDecision | Promise<LlmPolicyDecision>;
}

export interface ExtensionRouteDefinition extends Omit<RouteDefinition, "access" | "handler"> {
  readonly access: ProtectedRouteAccess;
  readonly handler: DeclaredRouteHandler;
}

/** Deliberately exposes only protected route registration, never legacy/public helpers. */
export interface ZoryaExtensionRouter {
  route(definition: ExtensionRouteDefinition): ZoryaExtensionRouter;
}

export type ZoryaRoutesExtension = (router: ZoryaExtensionRouter) => void;
export type ZoryaServerExtension = (builder: ZoryaServerBuilder) => void | ZoryaServerBuilder;
export type ZoryaRetrieverExtension = RegisterRetrieverInput;

export function mountExtensionRoutes(
  router: Router,
  extensions: ReadonlyArray<ZoryaRoutesExtension>,
): void {
  const registrar: ZoryaExtensionRouter = {
    route(definition) {
      const source = definition as unknown as {
        method?: unknown;
        path?: unknown;
        access?: unknown;
      };
      const access = source.access as { kind?: unknown } | undefined;
      if (access?.kind !== "protected") {
        throw new TypeError("Extension routes must declare a protected action and resource");
      }
      if (typeof source.method !== "string" || typeof source.path !== "string") {
        throw new TypeError("Extension route requires a method and path");
      }
      if (router.hasRoute(source.method, source.path)) {
        throw new Error(
          `Extension route conflicts with an existing route: ${source.method.toUpperCase()} ${source.path}`,
        );
      }
      router.route(definition);
      return registrar;
    },
  };
  for (const extension of extensions) {
    const result = extension(registrar);
    if (isPromiseLike(result)) {
      throw new TypeError("Extension route registration must be synchronous");
    }
  }
}

export function registerExtensionRetrievers(params: {
  readonly registry: RetrieverRegistry | undefined;
  readonly entries: ReadonlyArray<ZoryaRetrieverExtension>;
}): void {
  if (params.entries.length === 0) return;
  if (!params.registry) {
    throw new Error("ZoryaServer: retriever() requires agents() with a retriever registry");
  }
  const configured = new Set<string>();
  for (const entry of params.entries) {
    const key = JSON.stringify([entry.namespaceId ?? null, entry.id]);
    if (configured.has(key)) {
      throw new Error(
        `ZoryaServer: duplicate retriever extension: ${entry.namespaceId ?? "global"}/${entry.id}`,
      );
    }
    configured.add(key);
    const existing = params.registry.get({
      id: entry.id,
      ...(entry.namespaceId !== undefined && { namespaceId: entry.namespaceId }),
    });
    if (existing !== null && existing.namespaceId === entry.namespaceId) {
      throw new Error(
        `ZoryaServer: retriever already registered: ${entry.namespaceId ?? "global"}/${entry.id}`,
      );
    }
  }
  for (const entry of params.entries) params.registry.register(entry);
}

export function validateChannel(channel: ZoryaChannel): string {
  const id = channel?.id;
  if (typeof id !== "string" || id.trim().length === 0) {
    throw new TypeError("Zorya channel id must be a non-empty string");
  }
  return id;
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as { then?: unknown }).then === "function"
  );
}
