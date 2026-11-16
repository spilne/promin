import { describe, expect, it } from "bun:test";
import { AuthPlanes } from "../mount-auth.ts";

describe("AuthPlanes.storageCaller", () => {
  const request = (token?: string) =>
    new Request("http://zorya.test/rpc/storage", {
      method: "POST",
      ...(token !== undefined && { headers: { authorization: `Bearer ${token}` } }),
    });

  it("keeps anonymous insecure workers in runner scope under module security", () => {
    const planes = new AuthPlanes({
      apiKeys: undefined,
      workerApiKeys: undefined,
      operatorSecurity: true,
    });
    expect(planes.storageCaller(request())).toBe("worker");
  });

  it("preserves key-mode operator and worker separation", () => {
    const planes = new AuthPlanes({ apiKeys: ["operator"], workerApiKeys: ["worker"] });
    expect(planes.storageCaller(request("operator"))).toBe("operator");
    expect(planes.storageCaller(request("worker"))).toBe("worker");
    expect(planes.storageCaller(request())).toBeUndefined();
  });
});
