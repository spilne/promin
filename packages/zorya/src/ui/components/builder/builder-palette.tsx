// ---------------------------------------------------------------------------
// BuilderPalette — what can be added to a workflow: the built-in control
// node kinds (branch, parallel, map, sleep, signal, approval) and the host's
// activities, grouped by category. Drag an item onto the canvas, or click it
// to add it next to the existing nodes.
// ---------------------------------------------------------------------------

import type { ComponentChildren } from "preact";
import { useMemo, useState } from "preact/hooks";
import type { ActivityDescriptor } from "@promin/workflow";
import { CONTROL_KINDS } from "../../lib/workflow-builder-graph.ts";
import { PALETTE_DRAG_MIME, type PaletteDrop } from "./builder-canvas.tsx";

export function BuilderPalette(props: {
  activities: ReadonlyArray<ActivityDescriptor>;
  onAdd: (item: PaletteDrop) => void;
}) {
  const [query, setQuery] = useState("");
  const needle = query.trim().toLowerCase();
  const groups = useMemo(() => {
    const matches = (fields: ReadonlyArray<string | undefined>) =>
      !needle || fields.some((f) => f?.toLowerCase().includes(needle));
    const byCategory = new Map<string, ActivityDescriptor[]>();
    for (const activity of props.activities) {
      const fields = [activity.id, activity.title, activity.description, activity.category];
      if (!matches([...fields, ...(activity.tags ?? [])])) continue;
      const category = activity.category ?? "Other";
      byCategory.set(category, [...(byCategory.get(category) ?? []), activity]);
    }
    return {
      control: CONTROL_KINDS.filter((k) => matches([k.kind, k.title, k.description])),
      activities: [...byCategory.entries()].sort(([a], [b]) => a.localeCompare(b)),
    };
  }, [props.activities, needle]);

  return (
    <div class="card bg-base-100 shadow overflow-hidden">
      <div class="border-b border-base-300 px-3 py-2 space-y-2">
        <div class="text-xs uppercase tracking-wider text-base-content/50">Palette</div>
        <input
          class="input input-bordered input-xs w-full"
          placeholder="Search activities…"
          value={query}
          onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
        />
      </div>
      <div class="max-h-[70vh] overflow-auto p-2 space-y-3">
        {groups.control.length > 0 && (
          <PaletteGroup title="Control">
            {groups.control.map((k) => (
              <PaletteItem
                key={k.kind}
                item={{ kind: k.kind }}
                title={k.title}
                subtitle={k.kind}
                description={k.description}
                onAdd={props.onAdd}
              />
            ))}
          </PaletteGroup>
        )}
        {groups.activities.map(([category, activities]) => (
          <PaletteGroup key={category} title={category}>
            {activities.map((a) => (
              <PaletteItem
                key={a.id}
                item={{ kind: "step", activityId: a.id }}
                title={a.title}
                subtitle={a.id}
                description={a.description}
                badges={a.capabilities}
                onAdd={props.onAdd}
              />
            ))}
          </PaletteGroup>
        ))}
        {groups.control.length === 0 && groups.activities.length === 0 && (
          <div class="p-2 text-xs text-base-content/50">
            {props.activities.length === 0 && !needle
              ? "The server registers no activities."
              : "Nothing matches."}
          </div>
        )}
      </div>
    </div>
  );
}

function PaletteGroup(props: { title: string; children: ComponentChildren }) {
  return (
    <div>
      <div class="px-1 pb-1 text-[10px] uppercase tracking-wider text-base-content/45">
        {props.title}
      </div>
      <div class="space-y-1">{props.children}</div>
    </div>
  );
}

function PaletteItem(props: {
  item: PaletteDrop;
  title: string;
  subtitle: string;
  description?: string;
  badges?: ReadonlyArray<string>;
  onAdd: (item: PaletteDrop) => void;
}) {
  return (
    <button
      type="button"
      draggable
      class="w-full rounded border border-base-content/10 bg-base-100 px-2 py-1.5 text-left hover:border-primary/40 hover:bg-base-200 cursor-grab"
      title={props.description ?? props.title}
      onDragStart={(e) => {
        e.dataTransfer?.setData(PALETTE_DRAG_MIME, JSON.stringify(props.item));
        if (e.dataTransfer) e.dataTransfer.effectAllowed = "copy";
      }}
      onClick={() => props.onAdd(props.item)}
    >
      <div class="text-xs font-medium leading-tight">{props.title}</div>
      <div class="font-mono text-[10px] text-base-content/45 truncate">{props.subtitle}</div>
      {props.badges && props.badges.length > 0 && (
        <div class="mt-1 flex flex-wrap gap-1">
          {props.badges.map((b) => (
            <span key={b} class="badge badge-xs badge-ghost">
              {b}
            </span>
          ))}
        </div>
      )}
    </button>
  );
}
