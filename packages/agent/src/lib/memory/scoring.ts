// ---------------------------------------------------------------------------
// Memory scoring — pure functions shared by `recall` and episode injection.
//
//   decayedSalience   — salience that ages with an exponential half-life, so
//                       a stale episode stops outranking a recent one. The
//                       stored value is never rewritten: decay is a read-time
//                       view, so it is reversible and needs no schema change.
//   rankCandidates    — hybrid ranking: BM25 lexical relevance (plus optional
//                       cosine similarity from embeddings), blended with an
//                       importance prior and a recency term. A candidate
//                       must be relevant to the query to be returned; the
//                       importance and recency terms only order the relevant.
// ---------------------------------------------------------------------------

/** Salience never decays below this share of its stored value. */
export const MIN_SALIENCE_RETENTION = 0.2;

/**
 * Age, in half-lives, past which decay stops counting. Salience is floored at
 * `MIN_SALIENCE_RETENTION` long before, so this changes no result; it keeps
 * Postgres `power(0.5, x)` from raising an underflow error for a very old episode.
 */
export const MAX_DECAY_HALF_LIVES = 64;

/** Cosine similarity below which an embedding match alone is not relevant. */
export const MIN_SEMANTIC_SIMILARITY = 0.3;

const BM25_K1 = 1.2;
const BM25_B = 0.75;

/** Blend of the final score. Relevance gates the candidate; the rest order it. */
const RELEVANCE_WEIGHT = 0.65;
const IMPORTANCE_WEIGHT = 0.15;
const RECENCY_WEIGHT = 0.2;

/** Candidates embedded per `rankCandidates` call unless `maxSemanticCandidates` says otherwise. */
const DEFAULT_MAX_SEMANTIC_CANDIDATES = 64;

/** Embedding calls in flight at once, so a large candidate set does not flood the provider. */
const EMBED_CONCURRENCY = 8;

/** Texts `memoizeEmbedder` remembers before dropping the oldest. */
const DEFAULT_EMBEDDING_CACHE_SIZE = 512;

/** Weight of the semantic score within relevance when an embedder is present. */
const SEMANTIC_SHARE = 0.5;

/**
 * Salience after `halfLifeMs` of ageing: exponential decay from
 * `occurredAt` to `now`, floored at `MIN_SALIENCE_RETENTION` of the stored
 * value. Without a positive `halfLifeMs` the stored salience is returned.
 * An `occurredAt` in the future counts as age zero.
 */
export function decayedSalience(params: {
  readonly salience: number;
  readonly occurredAt: number;
  readonly now: number;
  readonly halfLifeMs?: number;
}): number {
  const { salience, occurredAt, now, halfLifeMs } = params;
  if (halfLifeMs === undefined || !(halfLifeMs > 0)) return salience;
  const decayed = salience * halfLifeFactor({ ageMs: now - occurredAt, halfLifeMs });
  return Math.max(decayed, salience * MIN_SALIENCE_RETENTION);
}

/** `0.5 ** (age / halfLife)`, with a non-positive age counting as 1. */
export function halfLifeFactor(params: {
  readonly ageMs: number;
  readonly halfLifeMs: number;
}): number {
  if (params.ageMs <= 0) return 1;
  return 0.5 ** Math.min(params.ageMs / params.halfLifeMs, MAX_DECAY_HALF_LIVES);
}

/** Lowercased words of 3+ characters, with a trailing plural `s` removed. */
export function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .split(/[^\p{L}\p{N}]+/u)
    .filter((w) => w.length > 2)
    .map((w) => (w.length > 3 && w.endsWith("s") && !w.endsWith("ss") ? w.slice(0, -1) : w));
}

/**
 * BM25 of `query` against each document, scaled to 0..1 by the best
 * score (all zeros when nothing matches). Document frequencies come from
 * the given documents, so scores are only comparable within one call.
 */
export function bm25Scores(params: {
  readonly query: string;
  readonly documents: ReadonlyArray<string>;
}): number[] {
  const queryTerms = [...new Set(tokenize(params.query))];
  const docs = params.documents.map((d) => tokenize(d));
  if (queryTerms.length === 0 || docs.length === 0) return docs.map(() => 0);

  const avgLen = docs.reduce((sum, d) => sum + d.length, 0) / docs.length || 1;
  const docFreq = new Map<string, number>();
  for (const term of queryTerms) {
    docFreq.set(term, docs.filter((d) => d.includes(term)).length);
  }

  const raw = docs.map((tokens) => {
    let score = 0;
    for (const term of queryTerms) {
      const tf = tokens.filter((t) => t === term).length;
      if (tf === 0) continue;
      const df = docFreq.get(term) ?? 0;
      const idf = Math.log(1 + (docs.length - df + 0.5) / (df + 0.5));
      score +=
        (idf * (tf * (BM25_K1 + 1))) /
        (tf + BM25_K1 * (1 - BM25_B + BM25_B * (tokens.length / avgLen)));
    }
    return score;
  });
  const max = Math.max(...raw);
  return max > 0 ? raw.map((s) => s / max) : raw;
}

/** Cosine similarity of two vectors; 0 for mismatched lengths or a zero vector. */
export function cosineSimilarity(a: ReadonlyArray<number>, b: ReadonlyArray<number>): number {
  if (a.length === 0 || a.length !== b.length) return 0;
  let dot = 0;
  let na = 0;
  let nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i]! * b[i]!;
    na += a[i]! * a[i]!;
    nb += b[i]! * b[i]!;
  }
  return na === 0 || nb === 0 ? 0 : dot / Math.sqrt(na * nb);
}

/** One thing `rankCandidates` can return. */
export interface RankCandidate {
  readonly text: string;
  /** 0..1 prior: episode salience, or a provenance weight for a fact. */
  readonly importance: number;
  /** When it happened or was written, epoch ms; drives the recency term. */
  readonly at: number;
}

/**
 * Candidate indices with their scores, best first, dropping candidates the
 * query does not match. Lexical relevance is BM25; when `embedder` is given,
 * relevance is half BM25 and half cosine similarity, and a candidate also
 * counts as matching on similarity alone (`MIN_SEMANTIC_SIMILARITY`). If
 * the embedder throws, ranking falls back to lexical only.
 *
 * At most `maxSemanticCandidates` candidates are embedded: the ones with
 * the best importance-and-recency prior among those BM25 did not already rank.
 */
export async function rankCandidates(params: {
  readonly query: string;
  readonly candidates: ReadonlyArray<RankCandidate>;
  readonly now: number;
  readonly recencyHalfLifeMs: number;
  readonly embedder?: { embed(text: string): Promise<number[]> };
  readonly maxSemanticCandidates?: number;
  /** Called when the embedder throws; ranking then falls back to lexical only. */
  readonly onEmbedderError?: (error: unknown) => void;
}): Promise<Array<{ readonly index: number; readonly score: number }>> {
  const { query, candidates, now, recencyHalfLifeMs, embedder } = params;
  if (candidates.length === 0) return [];

  const lexical = bm25Scores({ query, documents: candidates.map((c) => c.text) });
  const semantic = embedder
    ? await semanticScores({
        query,
        candidates,
        lexical,
        embedder,
        now,
        recencyHalfLifeMs,
        max: params.maxSemanticCandidates ?? DEFAULT_MAX_SEMANTIC_CANDIDATES,
        onError: params.onEmbedderError,
      })
    : null;

  const ranked: Array<{ index: number; score: number }> = [];
  candidates.forEach((c, index) => {
    const lex = lexical[index]!;
    const sem = semantic?.[index] ?? 0;
    const matches = lex > 0 || sem >= MIN_SEMANTIC_SIMILARITY;
    if (!matches) return;
    const relevance = semantic ? (1 - SEMANTIC_SHARE) * lex + SEMANTIC_SHARE * sem : lex;
    const recency = halfLifeFactor({ ageMs: now - c.at, halfLifeMs: recencyHalfLifeMs });
    ranked.push({
      index,
      score:
        RELEVANCE_WEIGHT * relevance +
        IMPORTANCE_WEIGHT * clamp01(c.importance) +
        RECENCY_WEIGHT * recency,
    });
  });
  return ranked.sort((a, b) => b.score - a.score || a.index - b.index);
}

/** Per-candidate cosine similarity (0 where not embedded), or null on failure. */
async function semanticScores(params: {
  readonly query: string;
  readonly candidates: ReadonlyArray<RankCandidate>;
  readonly lexical: ReadonlyArray<number>;
  readonly embedder: { embed(text: string): Promise<number[]> };
  readonly now: number;
  readonly recencyHalfLifeMs: number;
  readonly max: number;
  readonly onError: ((error: unknown) => void) | undefined;
}): Promise<number[] | null> {
  const { candidates, lexical, embedder, max } = params;
  const prior = (i: number) =>
    clamp01(candidates[i]!.importance) +
    halfLifeFactor({ ageMs: params.now - candidates[i]!.at, halfLifeMs: params.recencyHalfLifeMs });
  const lexicalHits = candidates.map((_, i) => i).filter((i) => lexical[i]! > 0);
  const others = candidates
    .map((_, i) => i)
    .filter((i) => !(lexical[i]! > 0))
    .sort((a, b) => prior(b) - prior(a));
  const toEmbed = [...lexicalHits, ...others].slice(0, max);

  try {
    const queryVector = await embedder.embed(params.query);
    const scores = candidates.map(() => 0);
    const vectors = await mapWithConcurrency({
      items: toEmbed,
      limit: EMBED_CONCURRENCY,
      run: (i) => embedder.embed(candidates[i]!.text),
    });
    toEmbed.forEach((candidateIndex, n) => {
      scores[candidateIndex] = Math.max(0, cosineSimilarity(queryVector, vectors[n]!));
    });
    return scores;
  } catch (error) {
    params.onError?.(error);
    return null;
  }
}

/** `run` over `items` with at most `limit` in flight; results keep the order of `items`. */
async function mapWithConcurrency<T, R>(params: {
  readonly items: ReadonlyArray<T>;
  readonly limit: number;
  readonly run: (item: T) => Promise<R>;
}): Promise<R[]> {
  const results: R[] = new Array(params.items.length);
  let next = 0;
  const worker = async (): Promise<void> => {
    while (next < params.items.length) {
      const index = next++;
      results[index] = await params.run(params.items[index]!);
    }
  };
  await Promise.all(Array.from({ length: Math.min(params.limit, params.items.length) }, worker));
  return results;
}

/**
 * `embedder` that remembers the vectors of its most recent `maxEntries`
 * texts, so repeated recalls over the same memories do not re-embed them.
 * Failures are not cached.
 */
export function memoizeEmbedder(params: {
  readonly embedder: { embed(text: string): Promise<number[]> };
  readonly maxEntries?: number;
}): { embed(text: string): Promise<number[]> } {
  const maxEntries = params.maxEntries ?? DEFAULT_EMBEDDING_CACHE_SIZE;
  const cache = new Map<string, Promise<number[]>>();
  return {
    embed(text) {
      const hit = cache.get(text);
      if (hit) {
        cache.delete(text);
        cache.set(text, hit);
        return hit;
      }
      const pending = params.embedder.embed(text);
      cache.set(text, pending);
      pending.catch(() => cache.delete(text));
      if (cache.size > maxEntries) cache.delete(cache.keys().next().value!);
      return pending;
    },
  };
}

/** `n` limited to 0..1; NaN counts as 0. */
export function clamp01(n: number): number {
  if (Number.isNaN(n)) return 0;
  return Math.min(1, Math.max(0, n));
}
