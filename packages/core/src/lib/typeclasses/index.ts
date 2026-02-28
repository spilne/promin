// Codec
export {
  type Codec,
  JsonCodec,
  codecFromSchema,
  codecTuple,
  codecRecord,
  codecArray,
} from "./codec.ts";

// Eq
export { type Eq, JsonEq, eqFromCodec } from "./eq.ts";

// Show
export { type Show, JsonShow } from "./show.ts";

// Monoid
export { type Monoid, arrayMonoid, sumMonoid, stringMonoid } from "./monoid.ts";

// Ord
export { type Ord, numberOrd, stringOrd, ordBy } from "./ord.ts";

// Streaming typeclasses
export {
  type Streamable,
  isStreamable,
  type Sinkable,
  isSinkable,
  type KeyedSinkable,
  isKeyedSinkable,
  type Partitionable,
  isPartitionable,
  type Replayable,
  isReplayable,
  type Offset,
  type Acknowledgeable,
  isAcknowledgeable,
  type Envelope,
  type Checkpointable,
  isCheckpointable,
} from "./streamable.ts";

// DataFrame typeclasses
export {
  type Frameable,
  isFrameable,
  type FrameSchema,
  type PushdownFilterable,
  isPushdownFilterable,
  type Predicate,
  type ColumnSelectable,
  isColumnSelectable,
  type SourceSortable,
  isSourceSortable,
} from "./frameable.ts";

// State typeclasses
export { type StateBackend } from "./state-backend.ts";
