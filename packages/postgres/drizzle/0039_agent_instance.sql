-- Agent instance — a long-lived per-(agent, namespace, owner) instance of
-- a recipe. The id (`namespace::recipe::owner`) doubles as the resourceId
-- for the memory cascade, so each instance gets its own working memory +
-- facts. This table is the thin index; the memory store does the rest.

CREATE TABLE IF NOT EXISTS agent_instance (
  id                  TEXT  NOT NULL PRIMARY KEY,
  registered_agent_id TEXT  NOT NULL,
  namespace_id        TEXT  NOT NULL,
  owner_id            TEXT  NOT NULL,
  display_name        TEXT,
  metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at          BIGINT NOT NULL
);

-- list({ namespaceId, ownerId }) — "what agents has this owner instantiated".
CREATE INDEX IF NOT EXISTS agent_instance_ns_owner_idx
  ON agent_instance (namespace_id, owner_id);
-- list({ registeredAgentId }) — all instances of one recipe.
CREATE INDEX IF NOT EXISTS agent_instance_agent_idx
  ON agent_instance (registered_agent_id);
