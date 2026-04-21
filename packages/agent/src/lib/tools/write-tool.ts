import { writeFile, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { z } from "zod";
import { tool } from "../tool.ts";
import type { AgentTool } from "../tool.ts";

export interface WriteToolConfig {
  /** Directory where new tool files are written. Must match the FileToolRegistry dir. */
  dir: string;
  /**
   * Whether the agent must get human approval before writing.
   * Default: true — agent-written code runs with host process privileges.
   */
  requireApproval?: boolean;
  /**
   * Import path for the `tool` and `z` helpers shown in the description and example.
   * Default: "@promin/agent" (correct for installed packages).
   * Override with a relative path when running from source, e.g. "../../lib/index.ts".
   */
  toolImportPath?: string;
}

/**
 * Returns a built-in tool that lets the agent write new tool files to a directory
 * watched by a FileToolRegistry. Newly written tools become available on the
 * next think step without any restart.
 *
 * Pair with createFileToolRegistry({ dir }) pointing at the same directory.
 */
export function createWriteToolTool(
  config: WriteToolConfig,
): AgentTool<{ name: string; description: string; code: string; category?: string }, string> {
  const importPath = config.toolImportPath ?? "@promin/agent";

  const exampleCode = [
    `import { tool } from "${importPath}";`,
    'import { z } from "zod";',
    "",
    "export default tool({",
    '  name: "fetch-url",',
    '  description: "Fetch the text content of a URL",',
    "  parameters: z.object({ url: z.string().url() }),",
    "  execute: async ({ url }) => {",
    "    const resp = await fetch(url);",
    "    return resp.text();",
    "  },",
    "});",
  ].join("\n");

  return tool({
    name: "writeTool",
    description:
      "Write a new TypeScript tool file to the tools directory. " +
      "The tool becomes available immediately on the next think step. " +
      `The file must export a default AgentTool created with the tool() helper from "${importPath}". ` +
      "Required file structure:\n\n" +
      `import { tool } from "${importPath}";\n` +
      'import { z } from "zod";\n\n' +
      "export default tool({\n" +
      '  name: "<tool-name>",\n' +
      '  description: "...",\n' +
      "  parameters: z.object({ ... }),\n" +
      "  execute: async (input) => { ... },\n" +
      "});",
    usage:
      "Use when you need a capability that no existing tool provides. " +
      "Prefer small, focused tools with a single responsibility. " +
      "Group related tools under a category (e.g. category='search' for web/google/bing tools). " +
      "After writing, call the new tool by name in the same or next turn.",
    examples: [
      {
        input: {
          name: "fetch-url",
          description: "Fetch the text content of a URL",
          code: exampleCode,
        },
        output: 'Tool "fetch-url" written to tools/fetch-url.ts and registered.',
      },
    ],
    parameters: z.object({
      name: z
        .string()
        .regex(/^[a-z][a-z0-9-]*$/, "kebab-case, e.g. fetch-url")
        .describe("Tool name in kebab-case. Becomes the filename (name.ts) and the registry key."),
      description: z.string().describe("One-line description of what the tool does."),
      code: z
        .string()
        .describe(
          "Complete TypeScript content of the tool file. " +
            `Must export a default AgentTool using tool() from "${importPath}".`,
        ),
      category: z
        .string()
        .regex(/^[a-z][a-z0-9-]*$/, "kebab-case, e.g. search")
        .optional()
        .describe(
          "Optional category (subfolder) for organizing tools, e.g. 'search', 'data', 'web'. " +
            "The tool name stays flat — category is filesystem organization only.",
        ),
    }),
    requireApproval: config.requireApproval ?? true,
    execute: async ({ name, code, category }) => {
      const dir = category ? join(config.dir, category) : config.dir;
      await mkdir(dir, { recursive: true });
      const filePath = join(dir, `${name}.ts`);
      await writeFile(filePath, code, "utf8");
      const location = category ? `tools/${category}/${name}.ts` : `tools/${name}.ts`;
      return `Tool "${name}" written to ${location} and registered.`;
    },
  });
}
