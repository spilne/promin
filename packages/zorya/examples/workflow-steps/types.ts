import type { ActivityDefinition, WallClock } from "@promin/workflow";

/** What the demo activities may use from the host. Each is optional. */
export interface ActivityLibraryDeps {
  /** Time source of `system.now`. Default: `SystemWallClock`. */
  readonly clock?: WallClock;
  /** Enables `postgres.query`; without it that activity is not offered. */
  readonly postgres?: PostgresQueryClient;
}

/** The slice of a Postgres client `postgres.query` needs. */
export interface PostgresQueryClient {
  queryJson(sql: string): Promise<unknown>;
}

/**
 * One file's activities, given the host's dependencies.
 *
 * An arrow returning an array literal whose `configSchema`s differ in shape
 * needs an explicit `ReadonlyArray<ActivityDefinition>` return type: an
 * inferred return type widens each object literal with the other elements'
 * properties as `?: undefined`, which `JsonSchema.properties` rejects.
 */
export type ActivityLibrary = (deps: ActivityLibraryDeps) => ReadonlyArray<ActivityDefinition>;
