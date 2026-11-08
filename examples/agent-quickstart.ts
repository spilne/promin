import { InMemoryMemoryStore, LocalAgent, openai, tool } from "@promin/agent";
import { createWorkflowRunner, InMemoryWorkflowStorage } from "@promin/workflow";
import { z } from "zod";

const apiKey = process.env.OPENAI_API_KEY;
if (!apiKey) throw new Error("Set OPENAI_API_KEY before running bun run demo:agent");

const currentTime = tool({
  name: "currentTime",
  description: "Get the current UTC time.",
  parameters: z.object({}),
  execute: async () => new Date().toISOString(),
});

const agent = new LocalAgent({
  agent: {
    name: "quickstart-agent",
    llm: openai(process.env.OPENAI_MODEL ?? "gpt-4o", { apiKey }),
    systemPrompt: "Answer clearly. Use the currentTime tool when asked for the time.",
    tools: { currentTime },
  },
  runner: createWorkflowRunner({ storage: new InMemoryWorkflowStorage() }),
  memory: new InMemoryMemoryStore(),
  namespaceId: "quickstart",
});

const thread = await agent.thread("first-conversation", { resourceId: "demo-user" });
const first = await thread.send({ task: "What time is it in UTC?" });
console.log(await first.text);
const second = await thread.send({ task: "What did I ask you in my first message?" });
console.log(await second.text);
