import type { LLMChatParams, LLMProvider } from "@promin/agent";

/**
 * Deterministic test LLM which searches every knowledge tool it is offered,
 * then renders the retrieved chunks with their source titles.
 */
export function searchEverythingLlm(): LLMProvider {
  let callSequence = 0;
  return {
    chat: async (params) => {
      const results = retrievedSources(params);
      if (results.length > 0) {
        return {
          content: results.map(({ text, title }) => `${text} [source: ${title}]`).join("\n"),
          finishReason: "stop",
        };
      }

      const tools = (params.tools ?? []).filter((tool) => tool.name.startsWith("search_"));
      if (tools.length === 0 || params.messages.some((message) => message.role === "tool")) {
        return { content: "No knowledge available.", finishReason: "stop" };
      }

      const query = lastUserText(params);
      const turn = ++callSequence;
      return {
        content: null,
        finishReason: "tool_calls",
        toolCalls: tools.map((tool, index) => ({
          id: `search-${turn}-${index}`,
          name: tool.name,
          input: { query },
        })),
      };
    },
  };
}

function lastUserText(params: LLMChatParams): string {
  for (let index = params.messages.length - 1; index >= 0; index -= 1) {
    const message = params.messages[index]!;
    if (message.role === "user") return message.content;
  }
  return "";
}

function retrievedSources(params: LLMChatParams): Array<{ text: string; title: string }> {
  const out: Array<{ text: string; title: string }> = [];
  for (const message of params.messages) {
    if (message.role !== "tool" || message.isError) continue;
    const sources = sourceMetadata(message.metadata);
    let sourceIndex = 0;
    const lines = message.content.split("\n");
    for (let index = 0; index < lines.length; index += 1) {
      const header = /^\[\d+\] source=(.+)$/.exec(lines[index]!);
      if (!header) continue;
      const text: string[] = [];
      while (++index < lines.length) {
        const line = lines[index]!;
        if (/^\[\d+\] source=/.test(line) || line.startsWith("</reference-data>")) {
          index -= 1;
          break;
        }
        if (line.length === 0 && text.length > 0) break;
        if (line.length > 0) text.push(line);
      }
      const metadataTitle = sources[sourceIndex]?.title;
      const title = metadataTitle ?? sourceTitle(header[1]!);
      sourceIndex += 1;
      if (text.length > 0) out.push({ text: text.join("\n"), title });
    }
  }
  return out;
}

function sourceMetadata(
  metadata: Readonly<Record<string, unknown>> | undefined,
): ReadonlyArray<{ readonly title?: string }> {
  const sources = metadata?.sources;
  if (!Array.isArray(sources)) return [];
  return sources.filter(
    (source): source is { readonly title?: string } =>
      typeof source === "object" && source !== null,
  );
}

function sourceTitle(header: string): string {
  return header.replace(/ score=[\d.]+$/, "").replace(/ \([^)]*\)$/, "");
}
