import type {
  RetrieverCaller,
  RetrieverFilter,
  RetrieverSubject,
  RetrieverSubjectKind,
} from "./types.ts";

export interface SnapshotRetrieverAccess {
  readonly caller?: RetrieverCaller;
  readonly filter?: RetrieverFilter;
}

const SUBJECT_KINDS = new Set<RetrieverSubjectKind>([
  "operator",
  "worker",
  "user",
  "service-account",
]);

/** Detach and validate host-provided security material, reading every getter once. */
export function snapshotRetrieverAccess(value: unknown): SnapshotRetrieverAccess {
  const record = objectRecord(value, "retriever access");
  const callerValue = Reflect.get(record, "caller") as unknown;
  const filterValue = Reflect.get(record, "filter") as unknown;
  const caller = callerValue === undefined ? undefined : snapshotRetrieverCaller(callerValue);
  const filter = filterValue === undefined ? undefined : snapshotRetrieverFilter(filterValue);
  return Object.freeze({
    ...(caller !== undefined && { caller }),
    ...(filter !== undefined && { filter }),
  });
}

/** Detach a caller so later host/proxy mutation cannot widen an ACL request. */
export function snapshotRetrieverCaller(value: unknown): RetrieverCaller {
  const record = objectRecord(value, "retriever caller");
  const subjectValue = Reflect.get(record, "subject") as unknown;
  const userIdValue = Reflect.get(record, "userId") as unknown;
  const emailValue = Reflect.get(record, "email") as unknown;
  const accessRolesValue = Reflect.get(record, "accessRoles") as unknown;
  const teamsValue = Reflect.get(record, "teams") as unknown;
  const userId = nonEmptyString(userIdValue, "retriever caller.userId");
  const subject =
    subjectValue === undefined ? undefined : snapshotRetrieverSubject(subjectValue, userId);
  if (emailValue !== undefined && typeof emailValue !== "string") {
    throw new TypeError("retriever caller.email must be a string");
  }
  const accessRoles = stringArray(accessRolesValue, "retriever caller.accessRoles");
  const teams = stringArray(teamsValue, "retriever caller.teams");
  return Object.freeze({
    ...(subject !== undefined && { subject }),
    userId,
    ...(emailValue !== undefined && { email: emailValue }),
    accessRoles,
    teams,
  });
}

/** Detach a retriever filter, including proxy-backed arrays and metadata. */
export function snapshotRetrieverFilter(value: unknown): RetrieverFilter {
  const record = objectRecord(value, "retriever filter");
  const tagsValue = Reflect.get(record, "tags") as unknown;
  const metadataValue = Reflect.get(record, "metadata") as unknown;
  const tags =
    tagsValue === undefined ? undefined : stringArray(tagsValue, "retriever filter.tags");
  const metadata = metadataValue === undefined ? undefined : snapshotFilterMetadata(metadataValue);
  return Object.freeze({
    ...(tags !== undefined && { tags }),
    ...(metadata !== undefined && { metadata }),
  });
}

/** Merge already-snapshotted filters from weakest to strongest precedence. */
export function mergeSnapshotFilters(
  ...filters: ReadonlyArray<RetrieverFilter | undefined>
): RetrieverFilter | undefined {
  const present = filters.filter((filter): filter is RetrieverFilter => filter !== undefined);
  if (present.length === 0) return undefined;
  const tags = Object.freeze([...new Set(present.flatMap((filter) => filter.tags ?? []))]);
  const metadata: Record<string, string | number | boolean> = {};
  for (const filter of present) {
    for (const [key, value] of Object.entries(filter.metadata ?? {})) {
      Object.defineProperty(metadata, key, {
        value,
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }
  }
  Object.freeze(metadata);
  return Object.freeze({
    ...(tags.length > 0 && { tags }),
    ...(Object.keys(metadata).length > 0 && { metadata }),
  });
}

function snapshotRetrieverSubject(value: unknown, userId: string): RetrieverSubject {
  const record = objectRecord(value, "retriever caller.subject");
  const kindValue = Reflect.get(record, "kind") as unknown;
  const idValue = Reflect.get(record, "id") as unknown;
  if (typeof kindValue !== "string" || !SUBJECT_KINDS.has(kindValue as RetrieverSubjectKind)) {
    throw new TypeError("retriever caller.subject.kind is invalid");
  }
  const id = nonEmptyString(idValue, "retriever caller.subject.id");
  if (id !== userId) throw new TypeError("retriever caller subject and userId must match");
  return Object.freeze({ kind: kindValue as RetrieverSubjectKind, id });
}

function snapshotFilterMetadata(
  value: unknown,
): Readonly<Record<string, string | number | boolean>> {
  const record = objectRecord(value, "retriever filter.metadata");
  const metadata: Record<string, string | number | boolean> = {};
  for (const key of Object.keys(record)) {
    const item = Reflect.get(record, key) as unknown;
    if (typeof item !== "string" && typeof item !== "number" && typeof item !== "boolean") {
      throw new TypeError(`retriever filter.metadata.${key} has an invalid value`);
    }
    Object.defineProperty(metadata, key, {
      value: item,
      enumerable: true,
      configurable: false,
      writable: false,
    });
  }
  return Object.freeze(metadata);
}

function stringArray(value: unknown, label: string): ReadonlyArray<string> {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  const length = value.length;
  const result: string[] = [];
  for (let index = 0; index < length; index++) {
    const item = Reflect.get(value, String(index)) as unknown;
    if (typeof item !== "string") throw new TypeError(`${label} must contain only strings`);
    result.push(item);
  }
  return Object.freeze(result);
}

function nonEmptyString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${label} must be a non-empty string`);
  }
  return value;
}

function objectRecord(value: unknown, label: string): object {
  if ((typeof value !== "object" && typeof value !== "function") || value === null) {
    throw new TypeError(`${label} must be an object`);
  }
  return value;
}
