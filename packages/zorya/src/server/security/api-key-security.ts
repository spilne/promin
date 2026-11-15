import type { Auth } from "../auth.ts";
import { OPERATOR_PRINCIPAL, type ZoryaSecurity } from "./security.ts";

/** Adapt the built-in bearer API-key checker to the common security contract. */
export function createApiKeySecurity(params: { auth: Auth }): ZoryaSecurity {
  return {
    authenticate: async (req) => (params.auth.check(req) ? OPERATOR_PRINCIPAL : undefined),
    principalById: async () => undefined,
    canActIn: async () => true,
    canUseKnowledge: async () => true,
  };
}
