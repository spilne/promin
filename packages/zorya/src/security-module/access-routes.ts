import { z } from "zod";
import { NamespaceIdSchema } from "../server/api-types.ts";
import { bodyNamespaceTarget, orgTarget, protectedAccess } from "../server/mount/route-security.ts";
import { parseBody } from "../server/request-validation.ts";
import { json, noContent, type Router } from "../server/router.ts";
import type { SecurityStore } from "./security-store.ts";

const BindingBodySchema = z
  .object({
    namespaceId: NamespaceIdSchema,
    accessRole: z.string().min(1),
  })
  .strict();

const KnowledgeGrantBodySchema = BindingBodySchema.extend({ baseId: z.string().min(1) }).strict();

/** Mount access-role and knowledge-grant administration routes. */
export function mountAccessRoutes(params: {
  readonly router: Router;
  readonly store: SecurityStore;
}): void {
  const user = orgTarget({ kind: "user", idParam: "id" });
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
      await params.store.putBinding({ userId: route.id!, ...body });
      return noContent();
    },
  });
  params.router.route({
    method: "DELETE",
    path: "/api/access/users/:id/bindings",
    access: protectedAccess({ resource: "user", action: "bind", target: user }),
    handler: async (req, route) => {
      const body = await parseBody({ req, schema: BindingBodySchema });
      await params.store.removeBinding({ userId: route.id!, ...body });
      return noContent();
    },
  });
  params.router.route({
    method: "POST",
    path: "/api/access/users/:id/disable",
    access: protectedAccess({ resource: "user", action: "disable", target: user }),
    handler: async (_req, route) => {
      await params.store.setUserStatus({ id: route.id!, status: "disabled" });
      return noContent();
    },
  });
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
}
