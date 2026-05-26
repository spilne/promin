// ---------------------------------------------------------------------------
// Parse a markdown skill (the `SKILL.md` ecosystem convention) into a
// `RegisterSkillInput`. The skill's instructions are the markdown body; the
// catalog fields come from YAML-ish frontmatter at the top of the file.
//
// Deliberately a minimal, dependency-free frontmatter reader: SKILL.md
// frontmatter is flat `key: value` (plus the occasional inline `[a, b]`
// list). We don't pull a full YAML parser — nested structures aren't part
// of the convention, and keeping the surface small avoids a new dep.
//
// INSTRUCTION-ONLY: only the body is consumed. Bundled scripts / resource
// files that ship alongside a SKILL.md are NOT executed or read here — that
// boundary is enforced by the scanner (which warns and skips them).
// ---------------------------------------------------------------------------

import type { RegisterSkillInput, SkillMetadata } from "./types.ts";

export interface ParseMarkdownSkillParams {
  /** Full file contents, including frontmatter. */
  readonly text: string;
  /**
   * Id to use when the frontmatter omits `name` — typically the skill's
   * directory name (for `SKILL.md`) or the file basename (for `foo.md`).
   */
  readonly fallbackId?: string;
}

/**
 * Parse a markdown skill. Returns `null` (not a skill) when the file has no
 * frontmatter — that's how prose like `README.md` is skipped. Returns a
 * `{ skill }` on success or `{ warning }` when frontmatter is present but a
 * required field (a resolvable id + a description) is missing.
 */
export function parseMarkdownSkill(
  params: ParseMarkdownSkillParams,
): { skill: RegisterSkillInput } | { warning: string } | null {
  const fm = extractFrontmatter(params.text);
  if (!fm) return null; // no frontmatter → not a skill (e.g. README)

  const { fields, body } = fm;
  const name = fields.name ?? params.fallbackId;
  const id = name ? slugify(name) : undefined;
  const description = fields.description;

  if (!id) {
    return { warning: "markdown skill has frontmatter but no resolvable name/id — skipped" };
  }
  if (!description) {
    return { warning: `markdown skill "${id}" has no \`description\` in frontmatter — skipped` };
  }
  if (body.length === 0) {
    return { warning: `markdown skill "${id}" has an empty body — skipped` };
  }

  // whenToUse: explicit key wins; otherwise left undefined so the registry
  // falls back to description (the ecosystem folds the trigger into it).
  const whenToUse = fields["when-to-use"] ?? fields.whenToUse;

  const tags = parseList(fields.tags);
  const capabilities = parseList(fields.capabilities);
  const metadata: Partial<SkillMetadata> = {
    ...(tags.length > 0 && { tags }),
    ...(capabilities.length > 0 && { capabilities }),
  };

  const skill: RegisterSkillInput = {
    id,
    description,
    body,
    ...(whenToUse !== undefined && { whenToUse }),
    ...(fields.version !== undefined && { version: fields.version }),
    ...(Object.keys(metadata).length > 0 && { metadata }),
  };
  return { skill };
}

/**
 * Split a `---`-delimited frontmatter block from the body. Returns `null`
 * when the text doesn't start with a frontmatter fence.
 */
function extractFrontmatter(text: string): { fields: Record<string, string>; body: string } | null {
  // Tolerate a leading BOM / whitespace, then require an opening `---` line.
  const normalized = text.replace(/^﻿/, "");
  const match = /^---[ \t]*\r?\n([\s\S]*?)\r?\n---[ \t]*\r?\n?/.exec(normalized);
  if (!match) return null;
  const fields = parseFrontmatterFields(match[1]!);
  const body = normalized.slice(match[0].length).trim();
  return { fields, body };
}

/** Parse flat `key: value` frontmatter lines. Unquotes scalar string values. */
function parseFrontmatterFields(block: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const raw of block.split(/\r?\n/)) {
    const line = raw.trim();
    if (line.length === 0 || line.startsWith("#")) continue;
    const colon = line.indexOf(":");
    if (colon === -1) continue;
    const key = line.slice(0, colon).trim();
    let value = line.slice(colon + 1).trim();
    // Strip matching surrounding quotes on scalar values.
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (key.length > 0) out[key] = value;
  }
  return out;
}

/** Parse an inline `[a, b, c]` list (or a bare comma list) into trimmed items. */
function parseList(value: string | undefined): string[] {
  if (!value) return [];
  const inner = value.startsWith("[") && value.endsWith("]") ? value.slice(1, -1) : value;
  return inner
    .split(",")
    .map((s) => s.trim().replace(/^["']|["']$/g, ""))
    .filter((s) => s.length > 0);
}

/** Lowercase, collapse non-alphanumeric runs to single hyphens, trim hyphens. */
export function slugify(name: string): string {
  return name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}
