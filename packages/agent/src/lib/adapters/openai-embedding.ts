import type { EmbeddingProvider } from "../memory-index.ts";

export interface OpenAIEmbeddingOptions {
  apiKey?: string;
  baseUrl?: string;
}

interface OpenAIEmbeddingResponse {
  data: Array<{ embedding: number[] }>;
}

export function openaiEmbedding(
  model: string,
  options: OpenAIEmbeddingOptions = {},
): EmbeddingProvider {
  const apiKey = options.apiKey ?? process.env["OPENAI_API_KEY"];
  const baseUrl = options.baseUrl ?? "https://api.openai.com";

  return {
    async embed(text: string): Promise<number[]> {
      const resp = await fetch(`${baseUrl}/v1/embeddings`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${apiKey ?? ""}`,
        },
        body: JSON.stringify({ model, input: text }),
      });

      if (!resp.ok) {
        const body = await resp.text();
        throw new Error(`OpenAI Embeddings API error ${resp.status}: ${body}`);
      }

      const data = (await resp.json()) as OpenAIEmbeddingResponse;
      const embedding = data.data[0]?.embedding;
      if (!embedding) throw new Error("OpenAI Embeddings API returned no embedding");
      return embedding;
    },
  };
}
