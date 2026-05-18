// ---------------------------------------------------------------------------
// DraftTestModal — chat a draft recipe without committing it.
//
// The edit-drawer registers a draft recipe (POST /api/agents/_draft) from
// the in-progress form state and hands its id here; this modal hosts a
// throwaway chat thread against it. Closing the modal deletes the draft
// (the drawer owns that on `onClose`); a browser closed mid-test is
// caught by the server's TTL sweep.
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";
import { ChatPane, type Tenant } from "./agent-detail.tsx";

interface Props {
  /** The `__draft__`-prefixed recipe id to chat against. */
  draftId: string;
  /** The recipe being drafted from — shown in the header. */
  sourceId: string;
  tenant: Tenant;
  /** Closes the modal; the drawer deletes the draft here. */
  onClose: () => void;
}

export function DraftTestModal({ draftId, sourceId, tenant, onClose }: Props) {
  // One throwaway thread per modal open — the first message creates it.
  const [threadId] = useState(() => `draft-${Date.now().toString(36)}`);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <>
      <div class="fixed inset-0 bg-black/40 z-30 anim-backdrop-in" onClick={onClose} aria-hidden />
      <div
        class="fixed inset-x-4 top-8 bottom-8 mx-auto max-w-3xl bg-base-100 rounded-lg shadow-2xl
               z-40 flex flex-col anim-drawer-in"
        role="dialog"
        aria-label="Test draft recipe"
      >
        <header class="flex items-start justify-between gap-2 p-4 border-b border-base-300">
          <div class="min-w-0">
            <div class="text-xs text-base-content/50 uppercase tracking-wider">
              Test draft — uncommitted
            </div>
            <div class="font-mono text-sm truncate">{sourceId}</div>
            <div class="text-[10px] text-base-content/40 mt-0.5">
              Chats a throwaway draft recipe. Nothing here is saved — close to discard, or Save in
              the drawer to keep the edit.
            </div>
          </div>
          <button class="btn btn-sm btn-ghost" onClick={onClose} title="Discard draft (Esc)">
            ✕
          </button>
        </header>

        <ChatPane
          agentId={draftId}
          threadId={threadId}
          tenant={tenant}
          onTurnComplete={() => {}}
          onInspect={() => {}}
          onArchived={onClose}
        />
      </div>
    </>
  );
}
