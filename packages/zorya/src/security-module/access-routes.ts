import { z } from "zod";
import type { WallClock } from "@promin/workflow";
import { NamespaceIdSchema } from "../server/api-types.ts";
import { bodyNamespaceTarget, orgTarget, protectedAccess } from "../server/mount/route-security.ts";
import { parseBody } from "../server/request-validation.ts";
import { json, noContent, type Router } from "../server/router.ts";
import { issueServiceAccountCredential } from "./service-account-credentials.ts";
import type { SecurityStoreV2 } from "./security-store.ts";

const BindingBodySchema = z
  .object({
    namespaceId: NamespaceIdSchema,
    accessRole: z.string().min(1),
  })
  .strict();

const KnowledgeGrantBodySchema = BindingBodySchema.extend({
  baseId: z.string().min(1),
}).strict();

const SubjectSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("user"), id: z.string().min(1).max(256) }).strict(),
  z
    .object({
      kind: z.literal("service-account"),
      id: z.string().min(1).max(256),
    })
    .strict(),
  z.object({ kind: z.literal("group"), id: z.string().min(1).max(256) }).strict(),
]);
const SubjectBindingBodySchema = BindingBodySchema.extend({
  subject: SubjectSchema,
}).strict();
const KnowledgeAudienceBodySchema = z
  .object({
    namespaceId: NamespaceIdSchema,
    baseId: z.string().min(1),
    audience: z.discriminatedUnion("kind", [
      z
        .object({
          kind: z.literal("access-role"),
          id: z.string().min(1).max(256),
        })
        .strict(),
      z.object({ kind: z.literal("group"), id: z.string().min(1).max(256) }).strict(),
    ]),
  })
  .strict();
const ServiceAccountBodySchema = z
  .object({
    id: z.string().min(1).max(256),
    canActFor: z.boolean().optional().default(false),
    expiresAt: z.number().int().safe().optional(),
  })
  .strict();
const CredentialBodySchema = z.object({ expiresAt: z.number().int().safe().optional() }).strict();

/** Mount access-role and knowledge-grant administration routes. */
export function mountAccessRoutes(params: {
  readonly router: Router;
  readonly store: SecurityStoreV2;
  readonly credentialPepper: string;
  readonly clock: WallClock;
}): void {
  const user = orgTarget({ kind: "user", idParam: "id" });
  const serviceAccount = orgTarget({ kind: "service-account", idParam: "id" });
  params.router.route({
    method: "GET",
    path: "/api/access/users",
    access: protectedAccess({ resource: "user", action: "read" }),
    handler: async () => {
      const users = await params.store.listUsers();
      return json(200, {
        users: await Promise.all(
          users.map(async (user) => ({
            ...user,
            bindings: await params.store.listBindings(user.id),
          })),
        ),
      });
    },
  });
  params.router.route({
    method: "PUT",
    path: "/api/access/users/:id/bindings",
    access: protectedAccess({ resource: "user", action: "bind", target: user }),
    handler: async (req, route) => {
      const body = await parseBody({ req, schema: BindingBodySchema });
      await params.store.putSubjectBinding({
        subject: { kind: "user", id: route.id! },
        ...body,
      });
      return noContent();
    },
  });
  params.router.route({
    method: "DELETE",
    path: "/api/access/users/:id/bindings",
    access: protectedAccess({ resource: "user", action: "bind", target: user }),
    handler: async (req, route) => {
      const body = await parseBody({ req, schema: BindingBodySchema });
      await params.store.removeSubjectBinding({
        subject: { kind: "user", id: route.id! },
        ...body,
      });
      return noContent();
    },
  });
  params.router.route({
    method: "POST",
    path: "/api/access/users/:id/disable",
    access: protectedAccess({
      resource: "user",
      action: "disable",
      target: user,
    }),
    handler: async (_req, route) => {
      await params.store.setUserStatus({ id: route.id!, status: "disabled" });
      return noContent();
    },
  });
  for (const method of ["PUT", "DELETE"] as const) {
    params.router.route({
      method,
      path: "/api/access/bindings",
      access: protectedAccess({
        resource: "user",
        action: "bind",
        target: bodyNamespaceTarget({ kind: "user" }),
      }),
      handler: async (req) => {
        const body = await parseBody({ req, schema: SubjectBindingBodySchema });
        if (method === "PUT") await params.store.putSubjectBinding(body);
        else await params.store.removeSubjectBinding(body);
        return noContent();
      },
    });
  }
  params.router.route({
    method: "PUT",
    path: "/api/access/knowledge-grants",
    access: protectedAccess({
      resource: "knowledge",
      action: "grant",
      target: bodyNamespaceTarget({ kind: "knowledge" }),
    }),
    handler: async (req) => {
      const body = await parseBody({ req, schema: KnowledgeGrantBodySchema });
      await params.store.putKnowledgeGrant(body);
      return noContent();
    },
  });
  for (const method of ["PUT", "DELETE"] as const) {
    params.router.route({
      method,
      path: "/api/access/knowledge-audiences",
      access: protectedAccess({
        resource: "knowledge",
        action: "grant",
        target: bodyNamespaceTarget({ kind: "knowledge" }),
      }),
      handler: async (req) => {
        const body = await parseBody({
          req,
          schema: KnowledgeAudienceBodySchema,
        });
        if (method === "PUT") await params.store.putKnowledgeAudienceGrant(body);
        else await params.store.removeKnowledgeAudienceGrant(body);
        return noContent();
      },
    });
  }
  params.router.route({
    method: "DELETE",
    path: "/api/access/knowledge-grants",
    access: protectedAccess({
      resource: "knowledge",
      action: "grant",
      target: bodyNamespaceTarget({ kind: "knowledge" }),
    }),
    handler: async (req) => {
      const body = await parseBody({ req, schema: KnowledgeGrantBodySchema });
      await params.store.removeKnowledgeGrant(body);
      return noContent();
    },
  });
  params.router.route({
    method: "GET",
    path: "/api/access/service-accounts",
    access: protectedAccess({ resource: "service-account", action: "read" }),
    handler: async () => {
      const accounts = await params.store.listServiceAccounts();
      return json(200, {
        serviceAccounts: await Promise.all(
          accounts.map(async (account) => ({
            ...account,
            bindings: await params.store.listSubjectBindings({
              kind: "service-account",
              id: account.id,
            }),
            credentials: (await params.store.listServiceAccountCredentials(account.id)).map(
              ({ id, serviceAccountId, createdAt, expiresAt, revokedAt }) => ({
                id,
                serviceAccountId,
                createdAt,
                ...(expiresAt !== undefined && { expiresAt }),
                ...(revokedAt !== undefined && { revokedAt }),
              }),
            ),
          })),
        ),
      });
    },
  });
  params.router.route({
    method: "POST",
    path: "/api/access/service-accounts",
    access: protectedAccess({ resource: "service-account", action: "create" }),
    handler: async (req) => {
      const body = await parseBody({ req, schema: ServiceAccountBodySchema });
      if (body.expiresAt !== undefined && body.expiresAt <= params.clock.currentTimeMs()) {
        return json(400, { error: "invalid_credential_expiry" });
      }
      if (await params.store.getServiceAccount(body.id)) {
        return json(409, { error: "service_account_exists" });
      }
      const serviceAccount = {
        id: body.id,
        status: "active" as const,
        canActFor: body.canActFor,
      };
      await params.store.putServiceAccount(serviceAccount);
      const credential = await issueServiceAccountCredential({
        store: params.store,
        serviceAccountId: body.id,
        pepper: params.credentialPepper,
        clock: params.clock,
        ...(body.expiresAt !== undefined && { expiresAt: body.expiresAt }),
      });
      return json(201, { serviceAccount, credential });
    },
  });
  params.router.route({
    method: "POST",
    path: "/api/access/service-accounts/:id/credentials",
    access: protectedAccess({
      resource: "service-account",
      action: "rotate",
      target: serviceAccount,
    }),
    handler: async (req, route) => {
      const body = await parseBody({ req, schema: CredentialBodySchema });
      if (body.expiresAt !== undefined && body.expiresAt <= params.clock.currentTimeMs()) {
        return json(400, { error: "invalid_credential_expiry" });
      }
      const account = await params.store.getServiceAccount(route.id!);
      if (!account || account.status !== "active") {
        return json(404, { error: "service_account_not_found" });
      }
      const credential = await issueServiceAccountCredential({
        store: params.store,
        serviceAccountId: account.id,
        pepper: params.credentialPepper,
        clock: params.clock,
        ...(body.expiresAt !== undefined && { expiresAt: body.expiresAt }),
      });
      return json(201, { credential });
    },
  });
  params.router.route({
    method: "DELETE",
    path: "/api/access/service-accounts/:id/credentials/:credentialId",
    access: protectedAccess({
      resource: "service-account",
      action: "rotate",
      target: serviceAccount,
    }),
    handler: async (_req, route) => {
      const credential = await params.store.getServiceAccountCredential(route.credentialId!);
      if (!credential || credential.serviceAccountId !== route.id) {
        return json(404, { error: "credential_not_found" });
      }
      await params.store.revokeServiceAccountCredential({
        id: credential.id,
        revokedAt: params.clock.currentTimeMs(),
      });
      return noContent();
    },
  });
  params.router.route({
    method: "DELETE",
    path: "/api/access/service-accounts/:id",
    access: protectedAccess({
      resource: "service-account",
      action: "delete",
      target: serviceAccount,
    }),
    handler: async (_req, route) => {
      const account = await params.store.getServiceAccount(route.id!);
      if (!account) return json(404, { error: "service_account_not_found" });
      await params.store.putServiceAccount({ ...account, status: "disabled" });
      return noContent();
    },
  });
}
