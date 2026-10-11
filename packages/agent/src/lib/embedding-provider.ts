/** Turns text into a dense vector for semantic search (retrievers, knowledge bases). */
export interface EmbeddingProvider {
  embed(text: string): Promise<number[]>;
}
