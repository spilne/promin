// ---------------------------------------------------------------------------
// Pagination — simple prev/next controls plus a "page X of Y · N items"
// label. Caller owns `page` state; we just render and emit changes.
// ---------------------------------------------------------------------------

interface PaginationProps {
  page: number;
  pageSize: number;
  total: number;
  onChange: (page: number) => void;
  /** Optional label for the noun, used in the count line. Default "items". */
  itemsLabel?: string;
}

export function Pagination({
  page,
  pageSize,
  total,
  onChange,
  itemsLabel = "items",
}: PaginationProps) {
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(Math.max(1, page), pageCount);
  const startIdx = total === 0 ? 0 : (safePage - 1) * pageSize + 1;
  const endIdx = Math.min(total, safePage * pageSize);

  return (
    <div class="flex items-center justify-between gap-2 text-sm">
      <div class="text-base-content/60">
        {total === 0
          ? `No ${itemsLabel}`
          : `Showing ${startIdx}–${endIdx} of ${total} ${itemsLabel}`}
      </div>
      <div class="flex items-center gap-1">
        <button
          class="btn btn-sm btn-ghost"
          disabled={safePage <= 1}
          onClick={() => onChange(safePage - 1)}
        >
          ← Prev
        </button>
        <span class="text-base-content/60 px-2 font-mono text-xs">
          {safePage} / {pageCount}
        </span>
        <button
          class="btn btn-sm btn-ghost"
          disabled={safePage >= pageCount}
          onClick={() => onChange(safePage + 1)}
        >
          Next →
        </button>
      </div>
    </div>
  );
}
