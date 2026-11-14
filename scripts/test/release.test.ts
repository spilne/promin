import { expect, test } from "bun:test";
import { nextVersion } from "../release.ts";
import { assertPublishedManifest, dependencyOrder, type WorkspacePackage } from "../workspace.ts";
import { isPublished } from "../publish-packages.ts";

test("release versions advance past existing tags even when disk versions lag", () => {
  expect(nextVersion("0.1.0", ["v0.1.0", "v0.1.1"], "patch")).toBe("0.1.2");
  expect(nextVersion("0.1.0", ["v0.1.1"], "minor")).toBe("0.2.0");
  expect(nextVersion("0.1.0", [], "0.1.0")).toBe("0.1.0");
  for (const invalid of ["0.1.1", "0.1.0", "0.2.0-beta.1", "v0.2.0", "01.2.3"]) {
    expect(() => nextVersion("0.1.0", ["v0.1.1"], invalid)).toThrow();
  }
});

test("packed dependencies must resolve protocols in every dependency group", () => {
  for (const group of [
    "dependencies",
    "devDependencies",
    "peerDependencies",
    "optionalDependencies",
  ]) {
    for (const range of ["workspace:*", "catalog:", "catalog:test", "file:../other"]) {
      expect(() =>
        assertPublishedManifest({ name: "pkg", version: "1.0.0", [group]: { other: range } }),
      ).toThrow();
    }
  }
  expect(() =>
    assertPublishedManifest({
      name: "pkg",
      version: "1.0.0",
      peerDependencies: { other: "^1.0.0" },
    }),
  ).not.toThrow();
});

test("publishing respects peer and optional dependencies and rejects cycles", () => {
  const packages: WorkspacePackage[] = [
    {
      directory: "app",
      manifest: { name: "app", version: "1.0.0", peerDependencies: { db: "*" } },
    },
    {
      directory: "db",
      manifest: { name: "db", version: "1.0.0", optionalDependencies: { core: "*" } },
    },
    { directory: "core", manifest: { name: "core", version: "1.0.0" } },
  ];
  expect(dependencyOrder(packages).map((p) => p.manifest.name)).toEqual(["core", "db", "app"]);
  packages[2]!.manifest.dependencies = { app: "*" };
  expect(() => dependencyOrder(packages)).toThrow("cycle");
});

test("registry retries skip only published versions, never authentication or server failures", async () => {
  let status = 200;
  const server = Bun.serve({ port: 0, fetch: () => new Response(null, { status }) });
  try {
    expect(await isPublished("@promin/workflow", "0.1.0", server.url.href)).toBe(true);
    status = 404;
    expect(await isPublished("@promin/workflow", "0.1.0", server.url.href)).toBe(false);
    for (status of [401, 403, 500])
      await expect(isPublished("pkg", "1.0.0", server.url.href)).rejects.toThrow();
  } finally {
    server.stop(true);
  }
});
