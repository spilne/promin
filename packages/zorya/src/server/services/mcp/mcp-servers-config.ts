// ---------------------------------------------------------------------------
// File / env form of the MCP allowlist: a JSON array of server configs,
// validated strictly so a typo fails the boot instead of silently dropping
// a field (a missing `requireApproval: false` is safe; a misspelt one is
// not what the operator meant).
// ---------------------------------------------------------------------------

import { MCP_RECIPE_SERVER_NAME, type McpServerConfig } from "@promin/agent";
import { z } from "zod";

const Name = z.string().regex(MCP_RECIPE_SERVER_NAME);
const NonEmpty = z.string().min(1);
const PositiveInt = z.number().int().positive();

const Base = {
  name: Name,
  description: z.string().optional(),
  credentialRef: NonEmpty.optional(),
  timeoutMs: PositiveInt.optional(),
  maxConcurrentCalls: PositiveInt.optional(),
  requireApproval: z.boolean().optional(),
  idempotent: z.boolean().optional(),
  tools: z.array(NonEmpty).optional(),
};

/** One allowlisted MCP server as written in the operator's config file. */
export const McpServerConfigSchema = z.discriminatedUnion("transport", [
  z
    .object({
      ...Base,
      transport: z.literal("stdio"),
      command: NonEmpty,
      args: z.array(z.string()).optional(),
      env: z.record(z.string(), z.string()).optional(),
      cwd: NonEmpty.optional(),
      credentialEnv: NonEmpty.optional(),
    })
    .strict(),
  z
    .object({
      ...Base,
      transport: z.literal("http"),
      url: z.url({ protocol: /^https?$/ }),
      headers: z.record(z.string(), z.string()).optional(),
      allowPrivateNetwork: z.boolean().optional(),
    })
    .strict(),
]);

/** The operator's MCP allowlist file: an array of server configs. */
export const McpServersConfigSchema = z.array(McpServerConfigSchema);

/** Parse an allowlist (e.g. a JSON file's contents). Throws with the offending paths. */
export function parseMcpServersConfig(raw: unknown): McpServerConfig[] {
  const parsed = McpServersConfigSchema.safeParse(raw);
  if (!parsed.success) {
    const issues = parsed.error.issues
      .slice(0, 5)
      .map((i) => `${i.path.join(".") || "(root)"}: ${i.message}`)
      .join("; ");
    throw new Error(`Invalid MCP server config: ${issues}`);
  }
  return parsed.data;
}
