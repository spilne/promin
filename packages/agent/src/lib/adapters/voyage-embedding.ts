import type { EmbeddingProvider } from "../memory-store.ts";

export interface VoyageEmbeddingOptions {
  apiKey?: string;
  baseUrl?: string;
}

interface VoyageEmbeddingResponse {
  data: Array<{ embedding: number[] }>;
}

/**
 * Voyage AI embedding provider (https://www.voyageai.com).
 * Recommended models: voyage-3, voyage-3-lite, voyage-code-3.
 */
export function voyageEmbedding(
  model: string,
  options: VoyageEmbeddingOptions = {},
): EmbeddingProvider {
  const apiKey = options.apiKey ?? process.env["VOYAGE_API_KEY"];
  const baseUrl = options.baseUrl ?? "https://api.voyageai.com";

  return {
    async embed(text: string): Promise<number[]> {
      const resp = await fetch(`${baseUrl}/v1/embeddings`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${apiKey ?? ""}`,
        },
        body: JSON.stringify({ model, input: [text] }),
      });

      if (!resp.ok) {
        const body = await resp.text();
        throw new Error(`Voyage AI Embeddings API error ${resp.status}: ${body}`);
      }

      const data = (await resp.json()) as VoyageEmbeddingResponse;
      const embedding = data.data[0]?.embedding;
      if (!embedding) throw new Error("Voyage AI Embeddings API returned no embedding");
      return embedding;
    },
  };
}
