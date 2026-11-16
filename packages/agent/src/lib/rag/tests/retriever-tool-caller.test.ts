import { describe, expect, it } from "bun:test";
import { createRetrieverTool } from "../retriever-tool.ts";
import type { RetrieveRequest, Retriever, RetrieverCaller } from "../types.ts";

const CALLER: RetrieverCaller = {
  userId: "u1",
  accessRoles: ["member"],
  teams: [],
};

describe("createRetrieverTool caller", () => {
  it("passes the configured caller to retrieve", async () => {
    const requests: RetrieveRequest[] = [];
    const retriever: Retriever = {
      retrieve: async (request) => {
        requests.push(request);
        return [];
      },
    };
    const search = createRetrieverTool({ retriever, caller: CALLER });

    await search.execute({ query: "benefits" });

    expect(requests).toHaveLength(1);
    expect(requests[0]?.caller).toEqual(CALLER);
  });

  it("does not let model input set or replace the caller", async () => {
    const requests: RetrieveRequest[] = [];
    const retriever: Retriever = {
      retrieve: async (request) => {
        requests.push(request);
        return [];
      },
    };
    const search = createRetrieverTool({ retriever, caller: CALLER });
    const parsed = search.parameters.parse({
      query: "benefits",
      caller: { userId: "attacker", accessRoles: ["admin"], teams: ["security"] },
    });

    expect("caller" in parsed).toBe(false);
    await search.execute(parsed);
    expect(requests[0]?.caller).toEqual(CALLER);
  });

  it("reauthorizes at each tool search and observes revocation after construction", async () => {
    const requests: RetrieveRequest[] = [];
    const retriever: Retriever = {
      retrieve: async (request) => {
        requests.push(request);
        return [];
      },
    };
    let allowed = true;
    let roles: ReadonlyArray<string> = ["member"];
    const securityContexts: unknown[] = [];
    const search = createRetrieverTool({
      retriever,
      resolveAccess: async (securityContext) => {
        securityContexts.push(securityContext);
        return allowed ? { caller: { userId: "u1", accessRoles: roles, teams: [] } } : undefined;
      },
    });

    await search.execute(
      { query: "first" },
      { scope: { securityContext: { actor: { kind: "user", id: "alice" } } } },
    );
    roles = ["finance"];
    await search.execute({ query: "second" });
    allowed = false;
    expect(await search.execute({ query: "revoked" })).toEqual({ results: [] });

    expect(requests.map((request) => request.caller?.accessRoles)).toEqual([
      ["member"],
      ["finance"],
    ]);
    expect(requests.map((request) => request.query)).toEqual(["first", "second"]);
    expect(securityContexts[0]).toEqual({ actor: { kind: "user", id: "alice" } });
  });
});
