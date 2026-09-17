// Re-export the searchKnowledge tool from the KB module. Definition lives
// alongside the actual KB content (kb/org-knowledge-base.ts) since the
// tool closes over the in-memory document map; this file just gives the
// folder-scan registry a default-export to pick up.
import { searchKnowledgeTool } from "../kb/org-knowledge-base.ts";

export default searchKnowledgeTool;
