// Single-op arithmetic. Avoids `eval` / Function() so the demo doesn't
// hand the model a remote-code-execution surface.
import { tool } from "@promin/agent";
import { z } from "zod";

export default tool({
  name: "calculate",
  description:
    "Compute a single arithmetic operation on two numbers. Use multiple calls for compound expressions.",
  parameters: z.object({
    a: z.number(),
    b: z.number(),
    op: z.enum(["add", "subtract", "multiply", "divide"]),
  }),
  execute: async ({ a, b, op }) => {
    switch (op) {
      case "add":
        return { result: a + b };
      case "subtract":
        return { result: a - b };
      case "multiply":
        return { result: a * b };
      case "divide":
        if (b === 0) throw new Error("division by zero");
        return { result: a / b };
    }
  },
});
