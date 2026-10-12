// ---------------------------------------------------------------------------
// SQLite registries per kind — `SqliteVersionedRegistry` with the kind
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
import type { SqliteDatabase } from "../sqlite-database.ts";
import { SqliteVersionedRegistry } from "./versioned-registry.ts";

export interface SqliteRegistryConfig {
  readonly db: SqliteDatabase;
  /** Time source for `created_at`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

export class SqliteAgentRegistry extends SqliteVersionedRegistry<
  AgentRecipe,
  AgentFilter,
  AgentRecipeInput
> {
  constructor(config: SqliteRegistryConfig) {
    super({ ...config, kind: AGENT_KIND });
  }
}

export class SqliteRoleRegistry extends SqliteVersionedRegistry<
  RoleRecipe,
  RoleFilter,
  RoleRecipeInput
> {
  constructor(config: SqliteRegistryConfig) {
    super({ ...config, kind: ROLE_KIND });
  }
}

export class SqliteSkillRegistry extends SqliteVersionedRegistry<
  SkillRecipe,
  SkillFilter,
  SkillRecipeInput
> {
  constructor(config: SqliteRegistryConfig) {
    super({ ...config, kind: SKILL_KIND });
  }
}

export class SqliteDagRegistry extends SqliteVersionedRegistry<DagRecipe, DagFilter> {
  constructor(config: SqliteRegistryConfig) {
    super({ ...config, kind: DAG_KIND });
  }
}

export class SqliteFragmentRegistry extends SqliteVersionedRegistry<
  FragmentRecipe,
  FragmentFilter
> {
  constructor(config: SqliteRegistryConfig) {
    super({ ...config, kind: FRAGMENT_KIND });
  }
}
