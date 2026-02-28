import { describe, it, expect } from "bun:test";
import {
  isStreamable,
  isSinkable,
  isKeyedSinkable,
  isPartitionable,
  isReplayable,
  isAcknowledgeable,
  isCheckpointable,
  isFrameable,
  isPushdownFilterable,
  isColumnSelectable,
  isSourceSortable,
} from "./index.ts";

// ---------------------------------------------------------------------------
// Streaming typeclass type guards
// ---------------------------------------------------------------------------

describe("Streaming typeclass guards", () => {
  const mockStreamable = {
    subscribe: () => null as any,
    codec: { encode: (v: any) => v, decode: (v: any) => v },
  };

  const mockSinkable = {
    publish: async () => {},
    codec: { encode: (v: any) => v, decode: (v: any) => v },
  };

  describe("isStreamable", () => {
    it("returns true for streamable objects", () => {
      expect(isStreamable(mockStreamable)).toBe(true);
    });

    it("returns false for non-streamable", () => {
      expect(isStreamable({})).toBe(false);
      expect(isStreamable(null)).toBe(false);
      expect(isStreamable({ subscribe: "not a fn" })).toBe(false);
    });
  });

  describe("isSinkable", () => {
    it("returns true for sinkable objects", () => {
      expect(isSinkable(mockSinkable)).toBe(true);
    });

    it("returns false for non-sinkable", () => {
      expect(isSinkable({})).toBe(false);
      expect(isSinkable({ publish: "not a fn" })).toBe(false);
    });
  });

  describe("isKeyedSinkable", () => {
    it("returns true for keyed sinkable (same shape as sinkable)", () => {
      expect(isKeyedSinkable(mockSinkable)).toBe(true);
    });
  });

  describe("isPartitionable", () => {
    it("returns true when partitions field exists", () => {
      expect(isPartitionable({ ...mockStreamable, partitions: 3 })).toBe(true);
    });

    it("returns false without partitions", () => {
      expect(isPartitionable(mockStreamable)).toBe(false);
    });

    it("returns false when partitions is not a number", () => {
      expect(isPartitionable({ ...mockStreamable, partitions: "3" })).toBe(false);
    });
  });

  describe("isReplayable", () => {
    it("returns true when subscribeFrom exists", () => {
      expect(isReplayable({ ...mockStreamable, subscribeFrom: () => null })).toBe(true);
    });

    it("returns false without subscribeFrom", () => {
      expect(isReplayable(mockStreamable)).toBe(false);
    });
  });

  describe("isAcknowledgeable", () => {
    it("returns true when subscribeAck exists", () => {
      expect(isAcknowledgeable({ ...mockStreamable, subscribeAck: () => null })).toBe(true);
    });

    it("returns false without subscribeAck", () => {
      expect(isAcknowledgeable(mockStreamable)).toBe(false);
    });
  });

  describe("isCheckpointable", () => {
    it("returns true when commitOffset and getCommittedOffset exist", () => {
      expect(
        isCheckpointable({
          ...mockStreamable,
          commitOffset: async () => {},
          getCommittedOffset: async () => null,
        }),
      ).toBe(true);
    });

    it("returns false without checkpoint methods", () => {
      expect(isCheckpointable(mockStreamable)).toBe(false);
    });

    it("returns false with only commitOffset", () => {
      expect(isCheckpointable({ ...mockStreamable, commitOffset: async () => {} })).toBe(false);
    });
  });
});

// ---------------------------------------------------------------------------
// DataFrame typeclass guards
// ---------------------------------------------------------------------------

describe("DataFrame typeclass guards", () => {
  const mockFrameable = {
    load: async () => [],
    schema: { columns: [{ name: "id", type: "number" as const }] },
    codec: { encode: (v: any) => v, decode: (v: any) => v },
  };

  describe("isFrameable", () => {
    it("returns true for frameable objects", () => {
      expect(isFrameable(mockFrameable)).toBe(true);
    });

    it("returns false for non-frameable", () => {
      expect(isFrameable({})).toBe(false);
      expect(isFrameable(null)).toBe(false);
    });
  });

  describe("isPushdownFilterable", () => {
    it("returns true when loadFiltered exists", () => {
      expect(isPushdownFilterable({ ...mockFrameable, loadFiltered: async () => [] })).toBe(true);
    });

    it("returns false without loadFiltered", () => {
      expect(isPushdownFilterable(mockFrameable)).toBe(false);
    });
  });

  describe("isColumnSelectable", () => {
    it("returns true when loadColumns exists", () => {
      expect(isColumnSelectable({ ...mockFrameable, loadColumns: async () => [] })).toBe(true);
    });

    it("returns false without loadColumns", () => {
      expect(isColumnSelectable(mockFrameable)).toBe(false);
    });
  });

  describe("isSourceSortable", () => {
    it("returns true when loadSorted exists", () => {
      expect(isSourceSortable({ ...mockFrameable, loadSorted: async () => [] })).toBe(true);
    });

    it("returns false without loadSorted", () => {
      expect(isSourceSortable(mockFrameable)).toBe(false);
    });
  });
});
