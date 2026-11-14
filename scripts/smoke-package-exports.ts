import { access, mkdir, readdir, symlink, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { packPackages } from "./pack-packages.ts";
import { ROOT, run } from "./workspace.ts";

const packed = await packPackages();
try {
  const modules = join(packed.directory, "node_modules");
  await mkdir(join(modules, "@promin"), { recursive: true });
  for (const name of await readdir(join(ROOT, "node_modules"))) {
    if (name === "@promin" || name === ".bin") continue;
    await symlink(join(ROOT, "node_modules", name), join(modules, name), "dir");
  }
  const imports: string[] = [];
  const nodeImports: string[] = [];
  for (const entry of packed.packages) {
    const { manifest, unpacked } = entry;
    await symlink(unpacked, join(modules, manifest.name), "dir");
    for (const target of [manifest.main, manifest.types]) {
      if (!target) throw new Error(`${manifest.name}: main/types missing`);
      await access(join(unpacked, target));
    }
    for (const [subpath, value] of Object.entries(manifest.exports ?? {})) {
      if (subpath === "./package.json") continue;
      const targets = typeof value === "string" ? { default: value } : value;
      for (const [condition, target] of Object.entries(targets)) {
        // The source condition is a development convenience. Consumers must
        // resolve only shipped JS/declarations, without @promin/source.
        if (condition === "@promin/source") continue;
        await access(join(unpacked, target));
      }
      const specifier = manifest.name + (subpath === "." ? "" : subpath.slice(1));
      imports.push(`await import(${JSON.stringify(specifier)});`);
      if (!["@promin/zorya", "@promin/sqlite"].includes(manifest.name) && subpath !== "./testing") {
        nodeImports.push(`await import(${JSON.stringify(specifier)});`);
      }
    }
  }
  const postgres = packed.packages.find((p) => p.manifest.name === "@promin/postgres");
  if (postgres) {
    const journal = await Bun.file(join(postgres.unpacked, "drizzle/meta/_journal.json")).json();
    for (const entry of journal.entries)
      await access(join(postgres.unpacked, "drizzle", `${entry.tag}.sql`));
  }
  const zorya = packed.packages.find((p) => p.manifest.name === "@promin/zorya");
  if (zorya)
    for (const file of ["index.html", "app.js", "app.css"]) {
      await access(join(zorya.unpacked, "dist/public", file));
    }
  const runtimeCheck = [
    'const { workflow, createWorkflowRunner, InMemoryWorkflowStorage } = await import("@promin/workflow");',
    'const { succeed } = await import("@spilne/perfect-core");',
    'const flow = workflow({ name: "packed-smoke" }).step("double", ({ input }) => succeed(input * 2)).build();',
    "const runner = createWorkflowRunner({ storage: new InMemoryWorkflowStorage() });",
    'const result = await runner.run({ workflow: flow, workflowId: "packed-1", input: 21 });',
    "if (result !== 42) throw new Error(`Packed workflow returned ${result}`);",
  ];
  await writeFile(join(packed.directory, "smoke.mjs"), [...imports, ...runtimeCheck].join("\n"));
  await writeFile(
    join(packed.directory, "node-smoke.mjs"),
    [...nodeImports, ...runtimeCheck].join("\n"),
  );
  // NODE_OPTIONS must not inject the repo's source condition into a consumer.
  if ((process.env.NODE_OPTIONS ?? "").includes("@promin/source"))
    throw new Error("Remove @promin/source from NODE_OPTIONS for artifact smoke tests");
  await run(["bun", "smoke.mjs"], packed.directory);
  await run(["node", "node-smoke.mjs"], packed.directory);
  await writeFile(
    join(packed.directory, "consumer.ts"),
    [
      'import { workflow } from "@promin/workflow";',
      'import { succeed } from "@spilne/perfect-core";',
      'export const flow = workflow<number>({ name: "consumer" }).step("double", ({ input }) => succeed(input * 2)).build();',
    ].join("\n"),
  );
  await writeFile(
    join(packed.directory, "package.json"),
    JSON.stringify({ private: true, type: "module" }),
  );
  await writeFile(
    join(packed.directory, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        module: "nodenext",
        moduleResolution: "nodenext",
        target: "ES2023",
        strict: true,
        skipLibCheck: false,
        declaration: true,
        types: ["node"],
        outDir: "dist",
      },
      files: ["consumer.ts"],
    }),
  );
  await run(
    ["bun", resolve(ROOT, "node_modules/typescript/bin/tsc"), "-p", "tsconfig.json"],
    packed.directory,
  );
  console.log(
    `Verified ${packed.packages.length} packed packages, ${imports.length} Bun exports and ${nodeImports.length} Node exports`,
  );
} finally {
  await packed.close();
}
