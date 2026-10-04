// ---------------------------------------------------------------------------
// The package's entry points: what each one exports, and what each one
// loads.
//
// - The export lists are snapshotted, so a change to the public surface
//   shows up as a snapshot diff (`bun test -u` accepts it).
// - The root entry and every other runtime entry must load without Node
//   built-ins or `bun:test`: their static import graphs are walked here, and
//   the root entry is transpiled and imported under Node with a resolve hook
//   that rejects any built-in our own modules ask for.
// ---------------------------------------------------------------------------

import { describe, it, expect, beforeAll, afterAll } from "bun:test";
import { builtinModules } from "node:module";
import { mkdtemp, mkdir, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import ts from "typescript";

const SRC = resolve(import.meta.dir, "..");
const PACKAGE = resolve(SRC, "..");

/** Published subpath → source file. Mirrors `exports` in package.json. */
const ENTRIES: Record<string, string> = {
  ".": "index.ts",
  "./distributed": "distributed.ts",
  "./scheduler": "scheduler.ts",
  "./discovery": "discovery.ts",
  "./sql-models": "sql-models.ts",
  "./storage-kit": "storage-kit.ts",
  "./testing": "testing.ts",
  "./dev": "dev.ts",
};

/** Entries that run in production code and so must not load runtime-specific modules. */
const PORTABLE = [
  ".",
  "./distributed",
  "./scheduler",
  "./discovery",
  "./sql-models",
  "./storage-kit",
];

const NODE_BUILTINS = new Set(builtinModules);
const isRuntimeModule = (spec: string): boolean =>
  spec.startsWith("node:") || spec.startsWith("bun:") || spec === "bun" || NODE_BUILTINS.has(spec);

/** Module specifiers a file loads when it is evaluated (type-only imports are erased). */
function runtimeImports(file: string): { static: string[]; dynamic: string[] } {
  const sf = ts.createSourceFile(file, ts.sys.readFile(file)!, ts.ScriptTarget.Latest, true);
  const found = { static: [] as string[], dynamic: [] as string[] };
  for (const stmt of sf.statements) {
    if (ts.isImportDeclaration(stmt)) {
      const clause = stmt.importClause;
      const typeOnly =
        clause !== undefined &&
        (clause.isTypeOnly ||
          (clause.name === undefined &&
            clause.namedBindings !== undefined &&
            ts.isNamedImports(clause.namedBindings) &&
            clause.namedBindings.elements.length > 0 &&
            clause.namedBindings.elements.every((e) => e.isTypeOnly)));
      if (!typeOnly) found.static.push((stmt.moduleSpecifier as ts.StringLiteral).text);
    } else if (ts.isExportDeclaration(stmt) && stmt.moduleSpecifier) {
      const typeOnly =
        stmt.isTypeOnly ||
        (stmt.exportClause !== undefined &&
          ts.isNamedExports(stmt.exportClause) &&
          stmt.exportClause.elements.length > 0 &&
          stmt.exportClause.elements.every((e) => e.isTypeOnly));
      if (!typeOnly) found.static.push((stmt.moduleSpecifier as ts.StringLiteral).text);
    }
  }
  const visit = (node: ts.Node): void => {
    if (
      ts.isCallExpression(node) &&
      node.expression.kind === ts.SyntaxKind.ImportKeyword &&
      node.arguments[0] &&
      ts.isStringLiteral(node.arguments[0])
    ) {
      found.dynamic.push(node.arguments[0].text);
    }
    node.forEachChild(visit);
  };
  visit(sf);
  return found;
}

/** Every source file an entry reaches through relative imports. */
function reachableFiles(entry: string, opts: { includeDynamic: boolean }): Set<string> {
  const seen = new Set<string>();
  const queue = [resolve(SRC, entry)];
  while (queue.length > 0) {
    const file = queue.pop()!;
    if (seen.has(file)) continue;
    seen.add(file);
    const { static: s, dynamic: d } = runtimeImports(file);
    for (const spec of opts.includeDynamic ? [...s, ...d] : s) {
      if (spec.startsWith(".")) queue.push(resolve(dirname(file), spec));
    }
  }
  return seen;
}

/** Runtime-specific modules the entry loads eagerly, as `file -> module`. */
function eagerRuntimeModules(entry: string): string[] {
  const out: string[] = [];
  for (const file of reachableFiles(entry, { includeDynamic: false })) {
    for (const spec of runtimeImports(file).static) {
      if (isRuntimeModule(spec)) out.push(`${relative(SRC, file)} -> ${spec}`);
    }
  }
  return out;
}

describe("entry points — what they load", () => {
  for (const name of PORTABLE) {
    it(`${name} loads no Node built-in and no bun:test eagerly`, () => {
      expect(eagerRuntimeModules(ENTRIES[name]!)).toEqual([]);
    });
  }

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

// ---------------------------------------------------------------------------
// Node smoke test: transpile what the portable entries reach and import the
// result under Node with a resolve hook that fails on any runtime-specific
// module requested by our own code.
// ---------------------------------------------------------------------------

const NODE = Bun.which("node");

describe.skipIf(NODE === null)("entry points — load under Node", () => {
  let outDir: string;

  beforeAll(async () => {
    // Node reports parent URLs by real path (/var -> /private/var on macOS).
    outDir = await realpath(await mkdtemp(join(tmpdir(), "promin-workflow-node-")));
    const files = new Set<string>();
    for (const name of PORTABLE) {
      for (const f of reachableFiles(ENTRIES[name]!, { includeDynamic: true })) files.add(f);
    }
    for (const file of files) {
      const js = ts.transpileModule(ts.sys.readFile(file)!, {
        fileName: file,
        compilerOptions: {
          module: ts.ModuleKind.ESNext,
          target: ts.ScriptTarget.ES2022,
          rewriteRelativeImportExtensions: true,
          verbatimModuleSyntax: false,
        },
      }).outputText;
      const target = join(outDir, relative(SRC, file)).replace(/\.ts$/, ".js");
      await mkdir(dirname(target), { recursive: true });
      await writeFile(target, js);
    }
    // Bare imports (@spilne/perfect-core, croner, rrule) resolve from the
    // workspace's node_modules.
    await symlink(resolve(PACKAGE, "../../node_modules"), join(outDir, "node_modules"), "dir");
    await writeFile(
      join(outDir, "hooks.mjs"),
      [
        'import { builtinModules } from "node:module";',
        "const builtins = new Set(builtinModules);",
        `const own = ${JSON.stringify(`file://${outDir}/`)};`,
        "export async function resolve(specifier, context, next) {",
        '  const fromOwn = context.parentURL?.startsWith(own) && !context.parentURL.includes("/node_modules/");',
        '  const runtime = specifier.startsWith("node:") || specifier.startsWith("bun:") || builtins.has(specifier);',
        "  if (fromOwn && runtime) throw new Error(`${context.parentURL} imports ${specifier}`);",
        "  return next(specifier, context);",
        "}",
      ].join("\n"),
    );
    await writeFile(
      join(outDir, "register.mjs"),
      'import { register } from "node:module";\nregister("./hooks.mjs", import.meta.url);\n',
    );
  });

  afterAll(async () => {
    if (outDir) await rm(outDir, { recursive: true, force: true });
  });

  for (const name of PORTABLE) {
    it(`node can import ${name} without built-ins`, async () => {
      const entry = join(outDir, ENTRIES[name]!.replace(/\.ts$/, ".js"));
      const script =
        `const m = await import(${JSON.stringify(`file://${entry}`)});` +
        "console.log(Object.keys(m).length);";
      const proc = Bun.spawn(
        [NODE!, "--import", join(outDir, "register.mjs"), "--input-type=module", "-e", script],
        { cwd: outDir, stdout: "pipe", stderr: "pipe" },
      );
      const [stdout, stderr, code] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ]);
      expect(stderr).toBe("");
      expect(code).toBe(0);
      expect(Number(stdout.trim())).toBeGreaterThan(0);
    }, 30_000);
  }
});

// ---------------------------------------------------------------------------
// Exports snapshot — every name each entry exports, values and types.
// ---------------------------------------------------------------------------

describe("entry points — exported names", () => {
  it("match the snapshot", () => {
    const program = ts.createProgram(
      Object.values(ENTRIES).map((f) => resolve(SRC, f)),
      {
        allowImportingTsExtensions: true,
        noEmit: true,
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ESNext,
        moduleResolution: ts.ModuleResolutionKind.Bundler,
        customConditions: ["@promin/source"],
        skipLibCheck: true,
        types: [],
      },
    );
    const checker = program.getTypeChecker();
    const surface: Record<string, string[]> = {};
    for (const [subpath, file] of Object.entries(ENTRIES)) {
      const sf = program.getSourceFile(resolve(SRC, file))!;
      const symbol = checker.getSymbolAtLocation(sf)!;
      surface[subpath] = checker
        .getExportsOfModule(symbol)
        .map((exp) => {
          const target = exp.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(exp) : exp;
          const isValue = (target.flags & ts.SymbolFlags.Value) !== 0;
          const typeOnly =
            isValue &&
            (exp.declarations ?? []).some((d) => ts.isTypeOnlyImportOrExportDeclaration(d));
          return isValue && !typeOnly ? exp.name : `type ${exp.name}`;
        })
        .sort((a, b) => a.replace(/^type /, "").localeCompare(b.replace(/^type /, "")));
    }
    expect(surface).toMatchSnapshot();
  }, 60_000);
});
