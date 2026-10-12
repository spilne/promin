// ---------------------------------------------------------------------------
// Shared helpers for the agent-surface routes (agents, drafts, remote
// deployments, webhooks, memory, instances):
//
//   - `internalError` — a 500 that names a code and a request id but never
//     the underlying error message (the detail is logged server-side).
//   - Secret redaction — remote-backend bearer tokens never leave the
//     server in a response body; a redacted token sent back on an edit
//     keeps the stored one.
//   - Wire schemas — the Zod shapes create / update / draft bodies are
//     validated against before they reach a registry.
//   - `mergeAgentMetadata` — PATCH semantics for recipe metadata.
//   - `ScopeAuthorizer` — the host hook that binds a request's
//     `(namespaceId, resourceId | ownerId)` to its authenticated caller.
// ---------------------------------------------------------------------------

import {
  MCP_RECIPE_SERVER_NAME,
  type AgentBackend,
  type AgentMetadata,
  type RegisteredAgent,
  type RegisteredDeployment,
} from "@promin/agent";
import { z } from "zod";
import { json } from "../router.ts";

/** Placeholder returned in place of a stored secret value. */
export const REDACTED_SECRET = "[redacted]";

/**
 * 500 for an unexpected failure. The body carries `code` and a fresh
 * request id; the error itself is logged under that id and never returned.
 */
export function internalError(params: { code: string; err: unknown }): Response {
  const requestId = crypto.randomUUID();
  try {
    console.error(`[zorya] ${params.code} requestId=${requestId}`, params.err);
  } catch {
    // a broken console must not break the response
  }
  return json(500, { error: params.code, requestId });
}

// ---------------------------------------------------------------------------
// Redaction
// ---------------------------------------------------------------------------

/** The backend with any bearer token replaced by `REDACTED_SECRET`. */
export function redactBackend(backend: AgentBackend): AgentBackend {
  if (backend.type === "remote" && backend.auth) {
    return { ...backend, auth: { kind: backend.auth.kind, token: REDACTED_SECRET } };
  }
  return backend;
}

/** A recipe safe to return to a client: secrets in its backend redacted. */
export function redactRecipe(recipe: RegisteredAgent): RegisteredAgent {
  const backend = redactBackend(recipe.backend);
  return backend === recipe.backend ? recipe : { ...recipe, backend };
}

/** A deployment safe to return to a client: its bearer token redacted. */
export function redactDeployment(deployment: RegisteredDeployment): RegisteredDeployment {
  if (!deployment.auth) return deployment;
  return { ...deployment, auth: { kind: deployment.auth.kind, token: REDACTED_SECRET } };
}

/**
 * Put the stored token back into a backend a client round-tripped with
 * the redacted placeholder. A placeholder with nothing stored to restore
 * is an error: it would otherwise persist the placeholder as the token.
 */
export function restoreRedactedSecrets(params: {
  next: AgentBackend;
  existing?: AgentBackend;
}): AgentBackend | { error: string } {
  const { next, existing } = params;
  if (next.type !== "remote" || next.auth?.token !== REDACTED_SECRET) return next;
  if (existing?.type === "remote" && existing.auth) {
    return { ...next, auth: existing.auth };
  }
  return { error: "redacted_secret" };
}

// ---------------------------------------------------------------------------
// Wire schemas
// ---------------------------------------------------------------------------

const NonEmptyString = z.string().min(1);

const SystemPromptSchema = z.union([
  z.string(),
  z.object({ base: z.string(), layers: z.array(z.string()).optional() }),
  z.null(),
]);

const RoleDefinitionSchema = z
  .object({
    systemPrompt: SystemPromptSchema,
    tools: z.array(z.string()),
    skills: z.array(z.object({ id: NonEmptyString, version: z.string().optional() })).optional(),
    capabilities: z.array(z.string()).optional(),
  })
  .passthrough();

const RoleBindingSchema = z.union([
  z.object({ ref: z.object({ id: NonEmptyString, version: z.string().optional() }) }).strict(),
  z.object({ inline: RoleDefinitionSchema }).strict(),
]);

const PositiveInt = z.number().int().positive();

/** One `backend.knowledge[]` retriever binding. */
const KnowledgeBindingSchema = z
  .object({
    id: NonEmptyString,
    // An LLM tool name: providers accept letters, digits, `_` and `-`.
    name: z
      .string()
      .regex(/^[A-Za-z0-9_-]{1,64}$/, "must be 1-64 letters, digits, '_' or '-'")
      .optional(),
    description: z.string().optional(),
    topK: PositiveInt.optional(),
    includeSources: z.boolean().optional(),
    includeScores: z.boolean().optional(),
    maxChunkCharacters: PositiveInt.optional(),
    filter: z
      .object({
        tags: z.array(z.string()).optional(),
        metadata: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).optional(),
      })
      .strict()
      .optional(),
    mode: z.enum(["tool", "context", "tool-and-context"]).optional(),
  })
  .strict();

const KnowledgeSchema = z
  .array(KnowledgeBindingSchema)
  .refine((bindings) => new Set(bindings.map((b) => b.id)).size === bindings.length, {
    message: "retriever ids must be unique",
  });

/** `http(s)://` URL with a host and no embedded credentials. */
export const HttpUrlSchema = z.string().refine(
  (raw) => {
    try {
      const u = new URL(raw);
      return (
        (u.protocol === "http:" || u.protocol === "https:") &&
        u.hostname.length > 0 &&
        u.username === "" &&
        u.password === ""
      );
    } catch {
      return false;
    }
  },
  { message: "must be an http(s) URL without credentials" },
);

const McpToolNamesSchema = z.array(NonEmptyString).optional();

/**
 * A recipe's MCP entry: an operator-configured server by name, or an http
 * endpoint. Strict, so a recipe can't carry a command, env, cwd, headers
 * or inline token.
 */
const McpServerRecipeSchema = z.union([
  z.object({ server: NonEmptyString, tools: McpToolNamesSchema }).strict(),
  z
    .object({
      name: z.string().regex(MCP_RECIPE_SERVER_NAME, "must be 1-48 of [A-Za-z0-9_-]"),
      transport: z.literal("http"),
      url: HttpUrlSchema,
      tools: McpToolNamesSchema,
    })
    .strict(),
]);

const LocalBackendSchema = z
  .object({
    type: z.literal("local"),
    model: z.object({
      provider: NonEmptyString,
      id: NonEmptyString,
      credentialRef: z.string().optional(),
    }),
    role: RoleBindingSchema,
    mcpServers: z.array(McpServerRecipeSchema).optional(),
    knowledge: KnowledgeSchema.optional(),
    maxStepsPerTurn: PositiveInt.optional(),
    autoCompact: z.union([z.literal(false), z.object({}).passthrough()]).optional(),
    autoDistill: z.union([z.literal(false), z.object({}).passthrough()]).optional(),
    contextBudget: z
      .object({
        maxMessageTokens: z.number().positive(),
        maxEpisodeTokens: z.number().nonnegative().optional(),
      })
      .optional(),
    network: z.object({}).passthrough().optional(),
    requiredEnv: z.array(z.string()).optional(),
    extra: z.record(z.string(), z.unknown()).optional(),
  })
  .passthrough();

const RemoteBackendSchema = z
  .object({
    type: z.literal("remote"),
    endpoint: HttpUrlSchema,
    remoteAgentId: NonEmptyString,
    auth: z.object({ kind: z.literal("bearer"), token: NonEmptyString }).optional(),
    timeoutMs: PositiveInt.optional(),
  })
  .strict();

/** Every recipe backend a client may submit. */
export const AgentBackendSchema = z.discriminatedUnion("type", [
  LocalBackendSchema,
  RemoteBackendSchema,
]);

/** Recipe metadata as a client may submit it: every field optional. */
export const AgentMetadataPatchSchema = z
  .object({
    description: z.string().nullable().optional(),
    capabilities: z.array(z.string()).optional(),
    tags: z.array(z.string()).optional(),
    enabled: z.boolean().optional(),
  })
  .strict();

export type AgentMetadataPatch = z.infer<typeof AgentMetadataPatchSchema>;

/** Checks a backend's MCP entries against the host's allowlist (`ZoryaMcpServers`). */
export interface McpRecipeChecker {
  recipeProblems(backend: AgentBackend): string | undefined;
}

/**
 * Validate a submitted backend. Its MCP entries must be allowed by `mcp`;
 * without one, a backend that attaches MCP servers is refused.
 */
export function parseBackend(params: {
  readonly raw: unknown;
  readonly mcp?: McpRecipeChecker | undefined;
}): { backend: AgentBackend } | { error: string; message: string } {
  const parsed = AgentBackendSchema.safeParse(params.raw);
  if (!parsed.success) return { error: "invalid_backend", message: describeIssues(parsed.error) };
  const backend = parsed.data as AgentBackend;
  if (backend.type === "local" && (backend.mcpServers?.length ?? 0) > 0) {
    const problem = params.mcp
      ? params.mcp.recipeProblems(backend)
      : "This server has no MCP configuration, so a recipe cannot attach MCP servers.";
    if (problem !== undefined) return { error: "invalid_mcp_servers", message: problem };
  }
  return { backend };
}

/** Validate submitted metadata. */
export function parseMetadataPatch(
  raw: unknown,
): { metadata: AgentMetadataPatch } | { error: string; message: string } {
  const parsed = AgentMetadataPatchSchema.safeParse(raw);
  if (!parsed.success) return { error: "invalid_metadata", message: describeIssues(parsed.error) };
  return { metadata: parsed.data };
}

function describeIssues(err: z.ZodError): string {
  return err.issues
    .slice(0, 5)
    .map((i) => `${i.path.length > 0 ? i.path.join(".") : "(root)"}: ${i.message}`)
    .join("; ");
}

/**
 * Field-level merge for a metadata PATCH: only fields present in the
 * patch change. `enabled` in particular survives an edit that doesn't
 * mention it, so a description tweak can't undo the kill switch.
 */
export function mergeAgentMetadata(params: {
  existing: AgentMetadata;
  patch: AgentMetadataPatch;
}): AgentMetadata {
  const { existing, patch } = params;
  return {
    ...existing,
    ...(patch.description !== undefined && { description: patch.description }),
    ...(patch.capabilities !== undefined && { capabilities: patch.capabilities }),
    ...(patch.tags !== undefined && { tags: patch.tags }),
    ...(patch.enabled !== undefined && { enabled: patch.enabled }),
  };
}

// ---------------------------------------------------------------------------
// Caller scope authorization
// ---------------------------------------------------------------------------

/** A tenant scope a request asks to act in: a namespace, maybe narrowed to a resource. */
export interface TenantScope {
  readonly kind?: "tenant";
  readonly namespaceId: string;
  /** Raw resource key, when the caller named one. */
  readonly resourceId?: string;
  /** Instance owner, when the caller resolves the resource via an instance. */
  readonly ownerId?: string;
  /** Agent the request targets, when it targets one. */
  readonly agentId?: string;
}

/**
 * Operator-level access: state that is not any tenant's but applies to all
 * of them, such as global secrets (the fallback every namespace resolves
 * against). A tenant caller must not be allowed this.
 */
export interface OperatorScope {
  readonly kind: "operator";
  /** What the operator action touches. */
  readonly resource: "secrets";
}

/** The scope a request asks to act in. */
export type RequestedScope = TenantScope | OperatorScope;

/**
 * Host hook binding a request's scope to its authenticated caller. Return
 * `true` to allow, `false` to answer 403 `scope_forbidden`. Tenant scopes
 * name a namespace (and maybe a resource); an `operator` scope asks for
 * operator-level access (global secrets) and should be granted only to
 * operators, never to tenant callers. Without a hook every caller that
 * passed the server's auth may act in any scope it names, operator scope
 * included; thread ownership (a thread is reachable only through the
 * resource and agent that created it) is enforced either way.
 */
export type ScopeAuthorizer = (params: {
  readonly req: Request;
  readonly scope: RequestedScope;
}) => boolean | Promise<boolean>;
