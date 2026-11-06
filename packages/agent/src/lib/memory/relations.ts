// ---------------------------------------------------------------------------
// Entity graph helpers. The graph is a set of relations (subject, predicate,
// object) per resource; an entity is just a normalized name that appears in
// one, so there is no separate entity table to keep consistent.
// ---------------------------------------------------------------------------

import type { Relation } from "./types.ts";

/** Longest entity name the agent's `memory` tool writes, in characters. */
export const MAX_ENTITY_CHARS = 80;
/** Longest relation predicate the agent's `memory` tool writes, in characters. */
export const MAX_PREDICATE_CHARS = 40;
/** Most relations the `memory` tool lets one resource hold. */
export const MAX_RELATIONS_PER_RESOURCE = 500;

/** Entity names compare case-insensitively with whitespace collapsed. */
export function normalizeEntityName(name: string): string {
  return name.trim().replace(/\s+/g, " ").toLowerCase();
}

/** Identity of a relation: the same triple is the same relation. */
export function relationKey(params: {
  readonly subject: string;
  readonly predicate: string;
  readonly object: string;
}): string {
  return JSON.stringify([
    normalizeEntityName(params.subject),
    normalizeEntityName(params.predicate),
    normalizeEntityName(params.object),
  ]);
}

/**
 * Relations within `hops` steps of the entities `query` names, nearest
 * first, at most `limit`. An entity is named when its normalized name occurs
 * in the normalized query as whole words. Relations connect in either
 * direction. Returns [] when the query names no entity.
 */
export function relationNeighborhood(params: {
  readonly relations: ReadonlyArray<Relation>;
  readonly query: string;
  readonly hops: number;
  readonly limit: number;
}): Relation[] {
  const { relations, hops, limit } = params;
  const query = ` ${normalizeEntityName(params.query).replace(/[^\p{L}\p{N}]+/gu, " ")} `;
  const names = new Set<string>();
  for (const r of relations) {
    names.add(normalizeEntityName(r.subject));
    names.add(normalizeEntityName(r.object));
  }
  let frontier = new Set(
    [...names].filter(
      (n) => n.length > 0 && query.includes(` ${n.replace(/[^\p{L}\p{N}]+/gu, " ")} `),
    ),
  );
  const seenEntities = new Set(frontier);
  const out: Relation[] = [];
  const taken = new Set<string>();
  for (let hop = 0; hop < hops && frontier.size > 0; hop++) {
    const next = new Set<string>();
    for (const r of relations) {
      const s = normalizeEntityName(r.subject);
      const o = normalizeEntityName(r.object);
      if (taken.has(r.id) || !(frontier.has(s) || frontier.has(o))) continue;
      taken.add(r.id);
      out.push(r);
      for (const n of [s, o]) {
        if (!seenEntities.has(n)) {
          seenEntities.add(n);
          next.add(n);
        }
      }
    }
    frontier = next;
  }
  return out.slice(0, limit);
}
