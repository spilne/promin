// ---------------------------------------------------------------------------
// Postgres registries per kind — `PostgresVersionedRegistry` with the kind
// bound. They share the registry tables; rows are keyed by kind.
// ---------------------------------------------------------------------------

import {
  AGENT_KIND,
  DAG_KIND,
  FRAGMENT_KIND,
  ROLE_KIND,
  SKILL_KIND,
  type AgentFilter,
  type AgentRecipe,
  type AgentRecipeInput,
  type DagFilter,
  type DagRecipe,
  type FragmentFilter,
  type FragmentRecipe,
  type RoleFilter,
  type RoleRecipe,
  type RoleRecipeInput,
  type SkillFilter,
  type SkillRecipe,
  type SkillRecipeInput,
} from "@promin/agent";
import type { WallClock } from "@promin/workflow";
import type { DrizzleDb } from "@spilne/perfect-postgres";
import { PostgresVersionedRegistry } from "./versioned-registry.ts";

export interface PostgresRegistryConfig {
  readonly db: DrizzleDb;
  /** Time source for `created_at`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

export class PostgresAgentRegistry extends PostgresVersionedRegistry<
  AgentRecipe,
  AgentFilter,
  AgentRecipeInput
> {
  constructor(config: PostgresRegistryConfig) {
    super({ ...config, kind: AGENT_KIND });
  }
}

export class PostgresRoleRegistry extends PostgresVersionedRegistry<
  RoleRecipe,
  RoleFilter,
  RoleRecipeInput
> {
  constructor(config: PostgresRegistryConfig) {
    super({ ...config, kind: ROLE_KIND });
  }
}

export class PostgresSkillRegistry extends PostgresVersionedRegistry<
  SkillRecipe,
  SkillFilter,
  SkillRecipeInput
> {
  constructor(config: PostgresRegistryConfig) {
    super({ ...config, kind: SKILL_KIND });
  }
}

export class PostgresDagRegistry extends PostgresVersionedRegistry<DagRecipe, DagFilter> {
  constructor(config: PostgresRegistryConfig) {
    super({ ...config, kind: DAG_KIND });
  }
}

export class PostgresFragmentRegistry extends PostgresVersionedRegistry<
  FragmentRecipe,
  FragmentFilter
> {
  constructor(config: PostgresRegistryConfig) {
    super({ ...config, kind: FRAGMENT_KIND });
  }
}
