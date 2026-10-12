// ---------------------------------------------------------------------------
// The registry kinds: how agents, roles, skills, DAGs and prompt fragments
// map onto the generic `VersionedRegistry`. Each kind normalises its input
// (metadata defaults), validates it, and names the facets `list` filters on.
// Every backend takes one of these: `new SqliteVersionedRegistry({ db,
// kind: AGENT_KIND })`; the `*Registry` classes bind the kind for you.
// ---------------------------------------------------------------------------

import { validateDag } from "../dag/validate.ts";
import type { DagFilter, DagRecipe } from "../dag/types.ts";
import {
  FRAGMENT_VERSION_SEPARATOR,
  type FragmentFilter,
  type FragmentRecipe,
} from "../fragments/types.ts";
import {
  AGENT_DRAFT_ID_PREFIX,
  type AgentFilter,
  type AgentRecipe,
  type AgentRecipeInput,
} from "../registry/types.ts";
import type { RoleFilter, RoleRecipe, RoleRecipeInput } from "../role/types.ts";
import type { SkillFilter, SkillRecipe, SkillRecipeInput } from "../skills/types.ts";
import { RegistryInputError, type FacetQuery, type RegistryKind } from "./types.ts";

function facetsOf(params: {
  readonly capabilities?: ReadonlyArray<string>;
  readonly tags?: ReadonlyArray<string>;
}): string[] {
  return [
    ...(params.capabilities ?? []).map((c) => `cap:${c}`),
    ...(params.tags ?? []).map((t) => `tag:${t}`),
  ];
}

function queryOf(params: { readonly capability?: string; readonly tag?: string }): FacetQuery {
  return {
    include: [
      ...(params.capability !== undefined ? [`cap:${params.capability}`] : []),
      ...(params.tag !== undefined ? [`tag:${params.tag}`] : []),
    ],
    exclude: [],
  };
}

function stringList(raw: ReadonlyArray<string> | undefined): string[] {
  return raw ? [...raw] : [];
}

export const AGENT_KIND: RegistryKind<AgentRecipe, AgentFilter, AgentRecipeInput> = {
  name: "agent",
  prepare: ({ id, input }) => {
    const backend = input.backend as { type?: unknown } | undefined;
    if (typeof backend !== "object" || backend === null || typeof backend.type !== "string") {
      throw new RegistryInputError(`agent "${id}": backend must be an object with a string type`);
    }
    return {
      backend: input.backend,
      metadata: {
        description: input.metadata?.description ?? null,
        capabilities: stringList(input.metadata?.capabilities),
        tags: stringList(input.metadata?.tags),
      },
    };
  },
  facets: ({ id, body }) => [
    ...facetsOf(body.metadata),
    `backend:${body.backend.type}`,
    ...(id.startsWith(AGENT_DRAFT_ID_PREFIX) ? ["draft"] : []),
  ],
  query: (filter) => {
    const base = queryOf(filter);
    return {
      include: [
        ...base.include,
        ...(filter.backendType !== undefined ? [`backend:${filter.backendType}`] : []),
        ...(filter.drafts === "only" ? ["draft"] : []),
      ],
      exclude: filter.drafts === "exclude" ? ["draft"] : [],
    };
  },
};

export const ROLE_KIND: RegistryKind<RoleRecipe, RoleFilter, RoleRecipeInput> = {
  name: "role",
  prepare: ({ id, input }) => {
    const def = input.definition as { tools?: unknown } | undefined;
    if (typeof def !== "object" || def === null || !Array.isArray(def.tools)) {
      throw new RegistryInputError(`role "${id}": definition must carry a tools array`);
    }
    return {
      definition: input.definition,
      metadata: {
        description: input.metadata?.description ?? null,
        tags: stringList(input.metadata?.tags),
        ...(input.metadata?.suggestedSecrets !== undefined && {
          suggestedSecrets: [...input.metadata.suggestedSecrets],
        }),
      },
    };
  },
  facets: ({ body }) =>
    facetsOf({ capabilities: body.definition.capabilities, tags: body.metadata.tags }),
  query: queryOf,
};

export const SKILL_KIND: RegistryKind<SkillRecipe, SkillFilter, SkillRecipeInput> = {
  name: "skill",
  prepare: ({ id, input }) => {
    for (const field of ["description", "body"] as const) {
      if (typeof input[field] !== "string" || input[field].length === 0) {
        throw new RegistryInputError(`skill "${id}": ${field} must be a non-empty string`);
      }
    }
    return {
      description: input.description,
      // Fall back to description when an author (e.g. a SKILL.md) folds the
      // trigger into the description rather than stating it separately.
      whenToUse: input.whenToUse ?? input.description,
      body: input.body,
      // Only the known fields: a skill file cannot smuggle registry state
      // (a self-declared `trust`, say) in through its metadata.
      metadata: {
        capabilities: stringList(input.metadata?.capabilities),
        tags: stringList(input.metadata?.tags),
      },
    };
  },
  facets: ({ body }) => facetsOf(body.metadata),
  query: queryOf,
};

export const DAG_KIND: RegistryKind<DagRecipe, DagFilter> = {
  name: "dag",
  // Bad graphs never enter the store: `validateDag` throws DagValidationError.
  prepare: ({ input }) => {
    const recipe: DagRecipe = {
      nodes: input.nodes,
      edges: input.edges,
      entry: input.entry,
      terminals: input.terminals,
      ...(input.metadata !== undefined && { metadata: input.metadata }),
    };
    validateDag(recipe);
    return recipe;
  },
  facets: ({ body }) => facetsOf({ tags: body.metadata?.tags }),
  query: queryOf,
};

export const FRAGMENT_KIND: RegistryKind<FragmentRecipe, FragmentFilter> = {
  name: "fragment",
  prepare: ({ id, input }) => {
    if (id.includes(FRAGMENT_VERSION_SEPARATOR)) {
      throw new RegistryInputError(
        `fragment "${id}": ids cannot contain "${FRAGMENT_VERSION_SEPARATOR}" (it pins a version in role layers)`,
      );
    }
    if (typeof input.content !== "string" || input.content.length === 0) {
      throw new RegistryInputError(`fragment "${id}": content must be a non-empty string`);
    }
    return { content: input.content };
  },
  facets: () => [],
  query: () => ({ include: [], exclude: [] }),
};
