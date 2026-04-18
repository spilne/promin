# GraphFrame

Graph abstraction built on top of `DataFrame`. Vertices and edges are regular DataFrames, so every DataFrame operation (filter, groupBy, join, expression builder, streaming I/O) works directly. Graph-specific queries and algorithms are exposed as methods.

## Main idea

A `GraphFrame` pairs two DataFrames:

- **vertices** — must include an `id` column. Any additional columns are passed through unchanged.
- **edges** — must include `src` and `dst` columns (both refer to vertex ids). Any additional columns (weight, type, timestamp, etc.) flow through and are usable by algorithms via options.

The algorithms are all in-house — zero external dependencies — and operate on the materialized adjacency. For "which creators are similar to this one?" style queries, see the [similarity playbook](#finding-similar-youtube--instagram-channels) below.

## Quick start

```typescript
import { DataFrame, GraphFrame } from "@promin/data";

const graph = GraphFrame.from({
  vertices: DataFrame.fromArray([
    { id: "a", name: "Alice" },
    { id: "b", name: "Bob" },
    { id: "c", name: "Carol" },
  ]),
  edges: DataFrame.fromArray([
    { src: "a", dst: "b", weight: 1.0 },
    { src: "b", dst: "c", weight: 0.5 },
    { src: "a", dst: "c", weight: 0.8 },
  ]),
});

const degrees = await (await graph.degrees()).collect();
// [{ id: "a", inDegree: 0, outDegree: 2, degree: 2 }, ...]
```

## Structural queries

```typescript
// Direct and multi-hop neighbours
const friends = await (await graph.neighbors("a")).collect();
const twoHop = await (await graph.neighbors("a", { depth: 2 })).collect();
const justIncoming = await (await graph.neighbors("c", { direction: "in" })).collect();

// Subgraphs via DataFrame predicates
const heavyEdges = graph.filterEdges(col("weight").gt(0.5));
const activeUsers = graph.filterVertices(col("active").eq(true));
```

## Algorithms

### PageRank

Classic uniform-teleport PageRank:

```typescript
const ranked = await (await graph.pageRank({ maxIter: 20, dampingFactor: 0.85 })).collect();
// each row has an added `pagerank` column; ranks sum to 1.
```

Personalized PageRank — bias the random walker toward a seed vertex (or a set of them):

```typescript
// "Who is important relative to Alice?"
const personalToAlice = await (
  await graph.pageRank({ personalizedFrom: "a", maxIter: 100 })
).collect();

// Multi-seed — equal weight across seeds
const personalToTwo = await (await graph.pageRank({ personalizedFrom: ["a", "b"] })).collect();
```

### Connected components

```typescript
const components = await (await graph.connectedComponents()).collect();
// each row has an added `component` (integer, stable, 0-indexed) column.
```

Edges are treated as undirected for component membership.

### Shortest paths

Dijkstra from a single source. Rejects negative weights:

```typescript
// Unweighted — distance equals hop count
const paths = await (await graph.shortestPaths({ from: "a" })).collect();

// Weighted — sum of `weight` column along the path
const weighted = await (await graph.shortestPaths({ from: "a", weightColumn: "weight" })).collect();

// Output columns: id, distance (null if unreachable), predecessor (null if source or unreachable)
```

Reconstruct a path by walking `predecessor` back to the source.

### Triangle count

```typescript
const triangles = await (await graph.triangleCount()).collect();
// each row has an added `triangles` column (number of triangles the vertex is part of).
```

Edges are treated as undirected; self-loops and parallel edges are ignored.

### Community detection (Louvain)

```typescript
const communities = await (await graph.communityDetection()).collect();

// Weighted
const weightedCommunities = await (
  await graph.communityDetection({ weightColumn: "weight" })
).collect();

// Resolution γ — higher yields more, smaller communities
const finer = await (await graph.communityDetection({ resolution: 3 })).collect();
```

On Zachary's karate club, this implementation reaches modularity ≈ 0.44, matching reference Louvain.

### Jaccard neighbour similarity

```typescript
// All-pairs similarity, highest first
const allPairs = await (await graph.jaccardSimilarity()).collect();

// Only pairs involving a specific vertex ("most similar to X")
const similarToA = await (await graph.jaccardSimilarity({ vertex: "a", top: 10 })).collect();

// Only return pairs above a threshold
const strong = await (await graph.jaccardSimilarity({ threshold: 0.3 })).collect();
```

Each row carries `a`, `b`, `similarity`, `intersection`, `union`.

## Finding similar YouTube / Instagram channels

Two common patterns use the primitives above end-to-end.

### Pattern 1 — co-subscription graph

```typescript
import { DataFrame, GraphFrame, col } from "@promin/data";

// follows is a DataFrame of { user_id, channel_id } rows.
// Build the channel-to-channel co-subscription graph:
const coSubs = await follows
  .rename({ channel_id: "ch_a" })
  .join(follows.rename({ channel_id: "ch_b" }), { on: "user_id" })
  .filter(col("ch_a").neq(col("ch_b")))
  .groupBy("ch_a", "ch_b")
  .agg({ shared: "count" })
  .collect();

const channelGraph = GraphFrame.from({
  vertices: channels, // DataFrame with `id` = channel id plus any metadata
  edges: DataFrame.fromArray(coSubs.map((r) => ({ src: r.ch_a, dst: r.ch_b, shared: r.shared }))),
});

// "Top 10 channels most similar to X by shared audience"
const topSimilar = await (
  await channelGraph.jaccardSimilarity({ vertex: "target-channel-id", top: 10 })
).collect();

// Or — personalized PageRank seeded at the target channel
const ppr = await (
  await channelGraph.pageRank({ personalizedFrom: "target-channel-id" })
).collect();
const pprTopK = ppr
  .filter((r) => r.id !== "target-channel-id")
  .sort((a, b) => b.pagerank - a.pagerank)
  .slice(0, 10);
```

### Pattern 2 — cluster the audience, look up the cluster

```typescript
const clusters = await (
  await channelGraph.communityDetection({ weightColumn: "shared" })
).collect();

// All channels in the same community as the target
const targetComm = clusters.find((r) => r.id === "target-channel-id")!.community;
const peers = clusters.filter((r) => r.community === targetComm && r.id !== "target-channel-id");
```

## Cross-platform graphs

Merge graphs from multiple platforms and link the same creator's accounts across them.

```typescript
const yt = GraphFrame.from({
  vertices: DataFrame.fromArray([
    { id: "yt:acme", platform: "youtube", followers: 100_000 },
    { id: "yt:beta", platform: "youtube", followers: 50_000 },
  ]),
  edges: DataFrame.fromArray([{ src: "yt:acme", dst: "yt:beta" }]),
});

const ig = GraphFrame.from({
  vertices: DataFrame.fromArray([
    { id: "ig:acme", platform: "instagram", followers: 80_000 },
    { id: "ig:gamma", platform: "instagram", followers: 20_000 },
  ]),
  edges: DataFrame.fromArray([{ src: "ig:acme", dst: "ig:gamma" }]),
});

// Same creator on both platforms — bridge them together.
const sameCreator = DataFrame.fromArray([{ src: "yt:acme", dst: "ig:acme", kind: "same-creator" }]);

const crossPlatform = await yt.bridge(ig, sameCreator);

// Now traversal algorithms see it as one graph.
const components = await (await crossPlatform.connectedComponents()).collect();
```

### Other set operations

```typescript
// Creators present on both YouTube and Instagram
const both = await yt.intersection(ig);

// Creators only on YouTube
const ytOnly = await yt.difference(ig);

// Plain union without cross-graph edges
const combined = await yt.union(ig);
```

`union`, `intersection`, and `difference` deduplicate vertices by id — when properties conflict on a shared id, the left operand wins.

## Composing with DataFrame

Because `graph.vertices` and `graph.edges` are just DataFrames, you can enrich them directly without any GraphFrame-specific API:

```typescript
// Enrich vertices with a stats DataFrame via a plain join
const enriched = new GraphFrame({
  vertices: graph.vertices.join(channelStats, { on: "id" }),
  edges: graph.edges,
});

// Filter edges by recency using the expression builder
const recent = graph.filterEdges(col("created_at").gt(lit("2026-01-01")));

// Stream edges from a large CSV without loading the whole file
const fromFile = GraphFrame.from({
  vertices: DataFrame.fromFile(CsvFile("channels.csv")),
  edges: DataFrame.fromFile(CsvFile("follows.csv")),
});
```

## API reference

### Structural

- `GraphFrame.from({ vertices, edges })`
- `.filterEdges(predicate)` / `.filterVertices(predicate)`
- `.neighbors(id, { depth?, direction? })` — `depth` default 1, `direction` default `"both"` (also `"in"` / `"out"`)
- `.degrees()` — returns DataFrame of `{ id, inDegree, outDegree, degree }`
- `.vertexCount()` / `.edgeCount()`

### Algorithms

- `.pageRank({ maxIter?, dampingFactor?, tolerance?, personalizedFrom? })`
- `.connectedComponents()`
- `.shortestPaths({ from, weightColumn? })`
- `.triangleCount()`
- `.communityDetection({ weightColumn?, resolution?, tolerance?, maxLocalPasses?, maxLevels? })`
- `.jaccardSimilarity({ vertex?, threshold?, top? })`

### Composition

- `.union(other)` / `.intersection(other)` / `.difference(other)`
- `.bridge(other, bridgeEdges)` — union + append cross-graph linking edges
