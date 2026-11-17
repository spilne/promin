import { describe, expect, it } from "bun:test";
import { Router } from "../../router.ts";
import {
  AUTHORIZATION_REASONS,
  createSecurityRouteAccessGuard,
  setRequestSecurityContext,
  type AuthorizationRequest,
  type Principal,
  type PrincipalRef,
  type SecurityAuditEvent,
  type SecurityContext,
  type ZoryaSecurity,
} from "../index.ts";

export interface SecurityConformanceFixture {
  readonly security: ZoryaSecurity;
  readonly validRequest: Request;
  readonly invalidRequest: Request;
  readonly authenticated: PrincipalRef;
  readonly actor: Principal;
  readonly nonActingPrincipal: Principal;
  readonly asserted: PrincipalRef;
  readonly allowed: AuthorizationRequest;
  readonly allowedReason: (typeof AUTHORIZATION_REASONS)[number];
  readonly allowedEffective: PrincipalRef;
  readonly denied: AuthorizationRequest;
  readonly deniedReason: (typeof AUTHORIZATION_REASONS)[number];
  readonly permissionMatrix: ReadonlyArray<{
    readonly name: string;
    readonly request: AuthorizationRequest;
    readonly expected: {
      readonly allowed: boolean;
      readonly reason: (typeof AUTHORIZATION_REASONS)[number];
    };
  }>;
  readonly auditEvents: SecurityAuditEvent[];
  /** Key mode intentionally suppresses allowed decisions from its default logger. */
  readonly recordsAllowedAudit?: boolean;
  readonly revokeAllowedIdentity?: () => Promise<void>;
}

export interface SecurityConformanceFactory {
  create(params?: { readonly auditFailure?: Error }): Promise<SecurityConformanceFixture>;
}

/** One implementation-neutral contract suite, invoked for every security adapter. */
export function securityConformanceSuite(name: string, factory: SecurityConformanceFactory): void {
  describe(`${name} security conformance`, () => {
    it("keeps authentication on the operator plane", async () => {
      const fixture = await factory.create();
      const authenticated = await fixture.security.authenticate({
        request: fixture.validRequest,
        plane: "operator",
      });
      expect(authenticated).toMatchObject(fixture.authenticated);
      expect(
        await fixture.security.authenticate({ request: fixture.validRequest, plane: "worker" }),
      ).toBeUndefined();
      expect(
        await fixture.security.authenticate({ request: fixture.invalidRequest, plane: "operator" }),
      ).toBeUndefined();
    });

    it("keeps actor and asserted effective identity distinct", async () => {
      const fixture = await factory.create();
      const context = await fixture.security.resolveIdentity({
        actor: fixture.actor,
        asserted: fixture.asserted,
      });
      expect(context?.actor).toMatchObject({
        kind: fixture.actor.kind,
        id: fixture.actor.id,
      });
      expect(context?.effective).toEqual(fixture.asserted);
      expect(
        await fixture.security.resolveIdentity({
          actor: fixture.nonActingPrincipal,
          asserted: fixture.asserted,
        }),
      ).toBeUndefined();
    });

    it("returns stable allow and deny decisions bound to the effective snapshot", async () => {
      const fixture = await factory.create();
      const allowed = await fixture.security.authorize(fixture.allowed);
      expect(allowed).toEqual({ allowed: true, reason: fixture.allowedReason });
      expect(AUTHORIZATION_REASONS).toContain(allowed.reason);

      const bound = await fixture.security.authorizeEffective?.(fixture.allowed);
      expect(bound?.decision).toEqual(allowed);
      expect(bound?.effective).toMatchObject(fixture.allowedEffective);
      expect(Object.isFrozen(bound?.effective)).toBeTrue();

      const denied = await fixture.security.authorize(fixture.denied);
      expect(denied).toEqual({ allowed: false, reason: fixture.deniedReason });
      expect(AUTHORIZATION_REASONS).toContain(denied.reason);
    });

    it("runs the common agent, knowledge, and approval permission matrix", async () => {
      const fixture = await factory.create();
      for (const entry of fixture.permissionMatrix) {
        expect(await fixture.security.authorize(entry.request), entry.name).toEqual(entry.expected);
      }
    });

    it("enforces the same allow and deny decisions at the route boundary", async () => {
      const fixture = await factory.create();
      let handled = 0;
      const router = new Router({
        logger: { error() {} },
        accessGuard: createSecurityRouteAccessGuard({
          security: fixture.security,
          clock: { now: () => new Date("2026-11-16T00:00:00.000Z") },
          logger: { error() {} },
        }),
      }).route({
        method: "POST",
        path: "/api/agents/:id/invoke",
        access: {
          kind: "protected",
          permission: { resource: "agent", action: "invoke" },
          resolveTarget: ({ params }) => ({
            resource: { kind: "agent", namespaceId: "team-a", id: params.id },
          }),
        },
        handler: () => {
          handled += 1;
          return new Response(null, { status: 204 });
        },
      });

      const allowed = new Request("http://zorya.test/api/agents/assistant/invoke", {
        method: "POST",
      });
      setRequestSecurityContext({ req: allowed, context: fixture.allowed.context });
      expect((await router.handle(allowed)).status).toBe(204);

      const denied = new Request("http://zorya.test/api/agents/assistant/invoke", {
        method: "POST",
      });
      setRequestSecurityContext({ req: denied, context: fixture.denied.context });
      expect((await router.handle(denied)).status).toBe(403);
      expect(handled).toBe(1);
      expect(fixture.auditEvents.map((event) => event.decision?.allowed)).toEqual(
        fixture.recordsAllowedAudit === false ? [false] : [true, false],
      );
    });

    it("rechecks revocable identities when the adapter has a directory", async () => {
      const fixture = await factory.create();
      if (!fixture.revokeAllowedIdentity) {
        // Static API-key identities are canonical process configuration, not directory rows.
        expect(fixture.authenticated.kind).toBe("operator");
        return;
      }
      expect((await fixture.security.authorize(fixture.allowed)).allowed).toBeTrue();
      await fixture.revokeAllowedIdentity();
      expect(await fixture.security.authorize(fixture.allowed)).toEqual({
        allowed: false,
        reason: "principal_disabled",
      });
    });

    it("fails closed on a forged permission/resource mismatch", async () => {
      const fixture = await factory.create();
      const forged = {
        context: fixture.allowed.context,
        permission: { resource: "agent", action: "invoke" },
        resource: { kind: "knowledge", namespaceId: "team-a", id: "docs" },
      } as unknown as AuthorizationRequest;
      expect(await fixture.security.authorize(forged)).toEqual({
        allowed: false,
        reason: "permission_denied",
      });
    });

    it("returns a namespace-bound identity filter", async () => {
      const fixture = await factory.create();
      expect(
        await fixture.security.identityFilter({
          context: fixture.allowed.context,
          namespaceId: "team-a",
        }),
      ).toEqual({ kind: "namespace", namespaceId: "team-a" });
    });

    it("forwards structured audit events and propagates sink failure", async () => {
      const fixture = await factory.create();
      const source: SecurityAuditEvent = {
        event: "authorization",
        occurredAt: "2026-11-16T00:00:00.000Z",
        context: fixture.allowed.context,
        namespaceId: fixture.allowed.resource.namespaceId,
        resource: fixture.allowed.resource,
        permission: fixture.allowed.permission,
        decision:
          fixture.recordsAllowedAudit === false
            ? { allowed: false, reason: fixture.deniedReason }
            : { allowed: true, reason: fixture.allowedReason },
        metadata: { source: "conformance" },
      };
      await fixture.security.recordAudit(source);
      expect(fixture.auditEvents).toHaveLength(1);
      expect(fixture.auditEvents[0]).toMatchObject({
        event: "authorization",
        namespaceId: fixture.allowed.resource.namespaceId,
        decision:
          fixture.recordsAllowedAudit === false
            ? { allowed: false, reason: fixture.deniedReason }
            : { allowed: true, reason: fixture.allowedReason },
        metadata: { source: "conformance" },
      });

      const failure = new Error("audit unavailable");
      const failing = await factory.create({ auditFailure: failure });
      await expect(failing.security.recordAudit(source)).rejects.toBe(failure);
    });
  });
}

export function selfContext(actor: Principal): SecurityContext {
  return { actor, effective: { kind: actor.kind, id: actor.id } };
}
