// ---------------------------------------------------------------------------
// The package's entry points.
//
// `./browser` is bundled into dashboards, so nothing it reaches may load a
// Node or Bun built-in. Its static import graph is walked here (type-only
// imports are erased and so ignored), and it is bundled for the browser the
// way the zorya UI build bundles it.
// ---------------------------------------------------------------------------

import { describe, it, expect } from "bun:test";
import { builtinModules } from "node:module";
import { dirname, join, relative, resolve } from "node:path";
import ts from "typescript";

const SRC = resolve(import.meta.dir, "..");
const PACKAGE = resolve(SRC, "..");

/** Published subpath → source file. Mirrors `exports` in package.json. */
const ENTRIES: Record<string, string> = {
  ".": "index.ts",
  "./testing": "testing.ts",
  "./browser": "browser.ts",
  "./terminal": "terminal.ts",
};

const NODE_BUILTINS = new Set(builtinModules);
const isRuntimeModule = (spec: string): boolean =>
  spec.startsWith("node:") || spec.startsWith("bun:") || spec === "bun" || NODE_BUILTINS.has(spec);

const allTypeOnly = (elements: ts.NodeArray<ts.ImportSpecifier | ts.ExportSpecifier>): boolean =>
  elements.length > 0 && elements.every((e) => e.isTypeOnly);

/** Module specifiers a file loads when it is evaluated (type-only imports are erased). */
function runtimeImports(file: string): string[] {
  const sf = ts.createSourceFile(file, ts.sys.readFile(file)!, ts.ScriptTarget.Latest, true);
  const found: string[] = [];
  for (const stmt of sf.statements) {
    if (ts.isImportDeclaration(stmt)) {
      const clause = stmt.importClause;
      const typeOnly =
        clause !== undefined &&
        (clause.isTypeOnly ||
          (clause.name === undefined &&
            clause.namedBindings !== undefined &&
            ts.isNamedImports(clause.namedBindings) &&
            allTypeOnly(clause.namedBindings.elements)));
      if (!typeOnly) found.push((stmt.moduleSpecifier as ts.StringLiteral).text);
    } else if (ts.isExportDeclaration(stmt) && stmt.moduleSpecifier) {
      const typeOnly =
        stmt.isTypeOnly ||
        (stmt.exportClause !== undefined &&
          ts.isNamedExports(stmt.exportClause) &&
          allTypeOnly(stmt.exportClause.elements));
      if (!typeOnly) found.push((stmt.moduleSpecifier as ts.StringLiteral).text);
    }
  }
  const visit = (node: ts.Node): void => {
    if (
      ts.isCallExpression(node) &&
      node.expression.kind === ts.SyntaxKind.ImportKeyword &&
      node.arguments[0] &&
      ts.isStringLiteral(node.arguments[0])
    ) {
      found.push(node.arguments[0].text);
    }
    node.forEachChild(visit);
  };
  visit(sf);
  return found;
}

/** Every non-relative module the entry reaches, as `file -> module`. */
function externalImports(entry: string): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const queue = [resolve(SRC, entry)];
  while (queue.length > 0) {
    const file = queue.pop()!;
    if (seen.has(file)) continue;
    seen.add(file);
    for (const spec of runtimeImports(file)) {
      if (spec.startsWith(".")) queue.push(resolve(dirname(file), spec));
      else out.push(`${relative(SRC, file)} -> ${spec}`);
    }
  }
  return out;
}

describe("entry points", () => {
  it("./browser loads no Node or Bun built-in", () => {
    const runtime = externalImports(ENTRIES["./browser"]!).filter((line) =>
      isRuntimeModule(line.split(" -> ")[1]!),
    );
    expect(runtime).toEqual([]);
  });

  it("./browser bundles for the browser", async () => {
    const result = await Bun.build({
      entrypoints: [resolve(SRC, ENTRIES["./browser"]!)],
      target: "browser",
      conditions: ["@promin/source"],
    });
    expect(result.logs.map(String)).toEqual([]);
    expect(result.success).toBe(true);
  });

  it("package.json exports every entry, pointing at its source", async () => {
    const pkg = (await Bun.file(join(PACKAGE, "package.json")).json()) as {
      exports: Record<string, string | Record<string, string>>;
    };
    const subpaths = Object.keys(pkg.exports).filter((k) => k !== "./package.json");
    expect(subpaths.sort()).toEqual(Object.keys(ENTRIES).sort());
    for (const [subpath, file] of Object.entries(ENTRIES)) {
      const target = pkg.exports[subpath] as Record<string, string>;
      expect(target["@promin/source"]).toBe(`./src/${file}`);
      expect(target.types).toBe(`./dist/${file.replace(/\.ts$/, ".d.ts")}`);
      expect(target.import).toBe(`./dist/${file.replace(/\.ts$/, ".js")}`);
    }
  });
});
