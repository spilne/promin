// ---------------------------------------------------------------------------
// In-memory registries per kind — `InMemoryVersionedRegistry` with the kind
// bound, for hosts, demos and tests that keep recipes in process.
// ---------------------------------------------------------------------------

import type { WallClock } from "@promin/workflow";
import type { DagFilter, DagRecipe } from "../dag/types.ts";
import type { FragmentFilter, FragmentRecipe } from "../fragments/types.ts";
import type { AgentFilter, AgentRecipe, AgentRecipeInput } from "../registry/types.ts";
import type { RoleFilter, RoleRecipe, RoleRecipeInput } from "../role/types.ts";
import type { SkillFilter, SkillRecipe, SkillRecipeInput } from "../skills/types.ts";
import { InMemoryVersionedRegistry } from "./in-memory-versioned-registry.ts";
import { AGENT_KIND, DAG_KIND, FRAGMENT_KIND, ROLE_KIND, SKILL_KIND } from "./kinds.ts";

export interface InMemoryRegistryConfig {
  /** Time source for `createdAt`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
}

export class InMemoryAgentRegistry extends InMemoryVersionedRegistry<
  AgentRecipe,
  AgentFilter,
  AgentRecipeInput
> {
  constructor(config: InMemoryRegistryConfig = {}) {
    super({ ...config, kind: AGENT_KIND });
  }
}

export class InMemoryRoleRegistry extends InMemoryVersionedRegistry<
  RoleRecipe,
  RoleFilter,
  RoleRecipeInput
> {
  constructor(config: InMemoryRegistryConfig = {}) {
    super({ ...config, kind: ROLE_KIND });
  }
}

export class InMemorySkillRegistry extends InMemoryVersionedRegistry<
  SkillRecipe,
  SkillFilter,
  SkillRecipeInput
> {
  constructor(config: InMemoryRegistryConfig = {}) {
    super({ ...config, kind: SKILL_KIND });
  }
}

export class InMemoryDagRegistry extends InMemoryVersionedRegistry<DagRecipe, DagFilter> {
  constructor(config: InMemoryRegistryConfig = {}) {
    super({ ...config, kind: DAG_KIND });
  }
}

export class InMemoryFragmentRegistry extends InMemoryVersionedRegistry<
  FragmentRecipe,
  FragmentFilter
> {
  constructor(config: InMemoryRegistryConfig = {}) {
    super({ ...config, kind: FRAGMENT_KIND });
  }
}
