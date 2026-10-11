// ---------------------------------------------------------------------------
// Workflow version order — numeric- and semver-aware, never lexical.
//
// Version strings are free-form ("3", "10", "1.2.0", "v2", "2.0.0-rc.1").
// Sorting them as strings puts "10" before "9"; this order compares
// numeric parts as numbers, so the newest version is the one a person
// would pick.
// ---------------------------------------------------------------------------

/**
 * Compare two workflow versions: negative when `a` is older, positive when
 * newer, 0 when equal. A leading `v` is ignored. The release part is split
 * on `.`; numeric parts compare as numbers, others as text, and a missing
 * part counts as 0 (`"1.2"` equals `"1.2.0"`). A pre-release (`-rc.1`)
 * sorts before its release, and pre-releases compare part by part the
 * same way.
 */
export function compareWorkflowVersions(a: string, b: string): number {
  const pa = parseVersion(a);
  const pb = parseVersion(b);
  const release = compareParts({ a: pa.release, b: pb.release, padWithZero: true });
  if (release !== 0) return release;
  if (pa.pre === undefined || pb.pre === undefined) {
    if (pa.pre === pb.pre) return 0;
    return pa.pre === undefined ? 1 : -1;
  }
  return compareParts({ a: pa.pre, b: pb.pre, padWithZero: false });
}

/** The newest of `versions` by `compareWorkflowVersions`, or undefined when empty. */
export function latestWorkflowVersion(versions: Iterable<string>): string | undefined {
  let latest: string | undefined;
  for (const v of versions) {
    if (latest === undefined || compareWorkflowVersions(v, latest) > 0) latest = v;
  }
  return latest;
}

function parseVersion(v: string): { release: string[]; pre?: string[] } {
  const trimmed = v.trim().replace(/^v(?=\d)/i, "");
  const dash = trimmed.indexOf("-");
  const release = (dash === -1 ? trimmed : trimmed.slice(0, dash)).split(".");
  if (dash === -1) return { release };
  return { release, pre: trimmed.slice(dash + 1).split(".") };
}

function compareParts(params: { a: string[]; b: string[]; padWithZero: boolean }): number {
  const { a, b, padWithZero } = params;
  const n = Math.max(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a[i];
    const y = b[i];
    if (x === undefined || y === undefined) {
      if (!padWithZero) return x === undefined ? -1 : 1;
    }
    const c = comparePart(x ?? "0", y ?? "0");
    if (c !== 0) return c;
  }
  return 0;
}

function comparePart(x: string, y: string): number {
  const nx = /^\d+$/.test(x);
  const ny = /^\d+$/.test(y);
  if (nx && ny) {
    const bx = BigInt(x);
    const by = BigInt(y);
    return bx < by ? -1 : bx > by ? 1 : 0;
  }
  // Numeric parts sort before textual ones, as in semver pre-releases.
  if (nx !== ny) return nx ? -1 : 1;
  return x < y ? -1 : x > y ? 1 : 0;
}
