import { expect, test } from "bun:test";
import { double } from "../../examples/docs-workflow.ts";

test("the embedded in-memory workflow executes its documented result", async () => {
  expect(await double(21)).toBe(42);
  expect(await double(0)).toBe(0);
});
