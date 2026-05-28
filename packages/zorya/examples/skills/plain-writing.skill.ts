import type { RegisterSkillInput } from "@promin/agent";

// Instruction-only skill: a writing rubric the agent loads when drafting
// user-facing prose. Pull into context via `loadSkill`.
export const PLAIN_WRITING: RegisterSkillInput = {
  id: "plain-writing",
  description: "A rubric for clear, plain-language prose — concise, concrete, jargon-free.",
  whenToUse:
    "The user asks you to write or rewrite prose for people to read — docs, an email, release notes, an explanation.",
  body: [
    "# Plain writing",
    "",
    "Write so a busy reader gets the point on the first pass.",
    "",
    "- **Lead with the point.** First sentence carries the conclusion; supporting detail",
    "  follows. Don't bury it under preamble.",
    "- **Short sentences.** One idea each. If a sentence has two `and`s and a `which`, split it.",
    '- **Concrete over abstract.** Name the thing. Prefer "the deploy failed" to "an issue',
    '  occurred in the deployment process".',
    '- **Cut hedges and filler.** Delete "basically", "in order to", "it should be noted',
    "  that\". Cut adverbs that don't change meaning.",
    '- **Active voice, named actor.** "The scanner skips test files", not "test files are',
    '  skipped".',
    '- **No purple prose.** No "delve", "leverage", "seamless", "robust" unless literally true.',
    "",
    "Before finishing, reread once and delete every word the sentence doesn't need.",
  ].join("\n"),
  metadata: {
    tags: ["writing"],
    trust: "trusted",
  },
};
