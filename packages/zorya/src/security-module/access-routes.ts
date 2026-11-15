import { z } from "zod";
import { NamespaceIdSchema } from "../server/api-types.ts";
import { parseBody } from "../server/request-validation.ts";
import { json, jsonError, noContent, type Handler, type Router } from "../server/router.ts";
import { ORG_WIDE_NAMESPACE, requestPrincipal } from "../server/security/index.ts";
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
  const admin =
    (handler: Handler): Handler =>
    async (req, route) => {
      const principal = requestPrincipal(req);
      const orgRoles =
        principal && Object.hasOwn(principal.accessRoles, ORG_WIDE_NAMESPACE)
          ? principal.accessRoles[ORG_WIDE_NAMESPACE]
          : undefined;
      if (!orgRoles?.includes("admin")) {
        return jsonError(403, "forbidden");
      }
      return handler(req, route);
    };

  params.router
    .get(
      "/api/access/users",
      admin(async () => {
        const users = await params.store.listUsers();
        return json(200, {
          users: await Promise.all(
            users.map(async (user) => ({
              ...user,
              bindings: await params.store.listBindings(user.id),
            })),
          ),
        });
      }),
    )
    .put(
      "/api/access/users/:id/bindings",
      admin(async (req, route) => {
        const body = await parseBody({ req, schema: BindingBodySchema });
        await params.store.putBinding({ userId: route.id!, ...body });
        return noContent();
      }),
    )
    .delete(
      "/api/access/users/:id/bindings",
      admin(async (req, route) => {
        const body = await parseBody({ req, schema: BindingBodySchema });
        await params.store.removeBinding({ userId: route.id!, ...body });
        return noContent();
      }),
    )
    .post(
      "/api/access/users/:id/disable",
      admin(async (_req, route) => {
        await params.store.setUserStatus({ id: route.id!, status: "disabled" });
        return noContent();
      }),
    )
    .put(
      "/api/access/knowledge-grants",
      admin(async (req) => {
        const body = await parseBody({ req, schema: KnowledgeGrantBodySchema });
        await params.store.putKnowledgeGrant(body);
        return noContent();
      }),
    )
    .delete(
      "/api/access/knowledge-grants",
      admin(async (req) => {
        const body = await parseBody({ req, schema: KnowledgeGrantBodySchema });
        await params.store.removeKnowledgeGrant(body);
        return noContent();
      }),
    );
}
