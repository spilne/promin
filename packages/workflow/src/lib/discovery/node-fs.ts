// ---------------------------------------------------------------------------
// Lazy Node built-ins for the filesystem scanners.
//
// The scanners are exported from the package root, which also loads in
// browsers and bundlers. A top-level `import "node:fs/promises"` would make
// every root import fail there at module load, so the built-ins are loaded
// on the first scan instead — only code that actually scans a folder needs
// a Node-compatible runtime.
// ---------------------------------------------------------------------------

import type { Dirent } from "node:fs";

export interface NodeFsModules {
  readonly readdir: (dir: string, options: { withFileTypes: true }) => Promise<Dirent[]>;
  readonly join: (...parts: string[]) => string;
  readonly pathToFileURL: (path: string) => URL;
}

let modules: Promise<NodeFsModules> | undefined;

/** Load (once) the Node built-ins the scanners use. */
export function loadNodeFs(): Promise<NodeFsModules> {
  modules ??= Promise.all([import("node:fs/promises"), import("node:path"), import("node:url")])
    .then(([fs, path, url]) => ({
      readdir: (dir: string, options: { withFileTypes: true }) => fs.readdir(dir, options),
      join: path.join,
      pathToFileURL: url.pathToFileURL,
    }))
    .catch((err: unknown) => {
      modules = undefined;
      throw err;
    });
  return modules;
}
