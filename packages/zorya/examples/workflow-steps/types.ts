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

/** One file's activities, given the host's dependencies. */
export type ActivityLibrary = (deps: ActivityLibraryDeps) => ReadonlyArray<ActivityDefinition>;
