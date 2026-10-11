// ---------------------------------------------------------------------------
// The demo's activity library for the workflow builder: one file per
// category, each a function of the host's dependencies, composed into the
// list `MapActivityCatalog` takes.
//
//   const activities = new MapActivityCatalog(demoActivities({ clock }));
//   const builder = new ZoryaWorkflowBuilder({ workflows, activities, store });
//
// Branch, parallel, map, sleep, signal and approval nodes are built into the
// builder; activities with a boolean output serve as branch conditions.
// ---------------------------------------------------------------------------

import type { ActivityDefinition } from "@promin/workflow";
import { jsonActivities } from "./json.ts";
import { mergeActivities } from "./merge.ts";
import { postgresActivities } from "./postgres.ts";
import { predicateActivities } from "./predicates.ts";
import { sourceActivities } from "./source.ts";
import { systemActivities } from "./system.ts";
import { textActivities } from "./text.ts";
import type { ActivityLibraryDeps } from "./types.ts";

export type { ActivityLibrary, ActivityLibraryDeps, PostgresQueryClient } from "./types.ts";

const LIBRARIES = [
  sourceActivities,
  jsonActivities,
  textActivities,
  mergeActivities,
  predicateActivities,
  systemActivities,
  postgresActivities,
];

/** Every demo activity the host's dependencies allow. */
export function demoActivities(deps: ActivityLibraryDeps = {}): ActivityDefinition[] {
  return LIBRARIES.flatMap((library) => library(deps));
}
