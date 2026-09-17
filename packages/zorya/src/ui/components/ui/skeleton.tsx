import type { JSX } from "preact";

interface SkeletonProps extends JSX.HTMLAttributes<HTMLDivElement> {
  /** Tailwind width class, e.g. "w-40". Defaults to "w-full". */
  w?: string;
  /** Tailwind height class, e.g. "h-4". Defaults to "h-4". */
  h?: string;
  /** Additional Tailwind classes. */
  class?: string;
}

export function Skeleton({ w = "w-full", h = "h-4", class: klass = "", ...rest }: SkeletonProps) {
  return (
    <div
      class={`skeleton ${w} ${h} rounded bg-base-300 ${klass}`}
      {...(rest as JSX.HTMLAttributes<HTMLDivElement>)}
    />
  );
}

interface SkeletonTableProps {
  rows?: number;
  cols?: number;
}

/** N-row table of skeletons — use inside the existing <table> tbody. */
export function SkeletonRows({ rows = 8, cols = 6 }: SkeletonTableProps) {
  return (
    <>
      {Array.from({ length: rows }).map(() => (
        <tr>
          {Array.from({ length: cols }).map((_, i) => (
            <td>
              <Skeleton w={i === 0 ? "w-40" : i === 1 ? "w-32" : "w-20"} h="h-4" />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

interface SkeletonCardProps {
  rows?: number;
}

export function SkeletonCard({ rows = 4 }: SkeletonCardProps) {
  return (
    <div class="card bg-base-100 shadow">
      <div class="card-body p-4 space-y-2">
        <Skeleton w="w-32" h="h-5" />
        {Array.from({ length: rows }).map(() => (
          <Skeleton w="w-full" h="h-3" />
        ))}
      </div>
    </div>
  );
}
