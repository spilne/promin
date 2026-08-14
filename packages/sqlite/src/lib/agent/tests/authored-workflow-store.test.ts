import { describe, expect, it } from "bun:test";
import { Database } from "bun:sqlite";
import { SqliteAuthoredWorkflowStore } from "../authored-workflow-store.ts";

describe("SqliteAuthoredWorkflowStore", () => {
  it("round-trips authored workflows and survives store recreation", async () => {
    const db = new Database(":memory:");
    const first = SqliteAuthoredWorkflowStore.make({ db });
    await first.save({
      name: "authored",
      version: "v1",
      schema: {
        version: 1,
        name: "authored",
        steps: [{ type: "step", name: "a", dependsOn: [], activityRef: "transform.identity" }],
      },
      status: "draft",
      contentHash: "abc",
      createdAt: 1,
      updatedAt: 2,
    });

    const second = SqliteAuthoredWorkflowStore.make({ db });
    expect(await second.get("authored", "v1")).toMatchObject({
      name: "authored",
      version: "v1",
      status: "draft",
    });
    expect((await second.list())[0]?.schema.steps[0]?.name).toBe("a");

    await second.delete("authored", "v1");
    expect(await second.list()).toEqual([]);
  });

  it("returns latest by updatedAt when version is omitted", async () => {
    const store = SqliteAuthoredWorkflowStore.make({ db: new Database(":memory:") });
    for (const version of ["v1", "v2"]) {
      await store.save({
        name: "authored",
        version,
        schema: {
          version: 1,
          name: "authored",
          steps: [{ type: "step", name: version, dependsOn: [], activityRef: "x" }],
        },
        status: "draft",
        contentHash: version,
        createdAt: 1,
        updatedAt: version === "v1" ? 2 : 3,
      });
    }

    expect((await store.get("authored"))?.version).toBe("v2");
  });
});
