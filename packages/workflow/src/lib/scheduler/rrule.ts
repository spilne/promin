// ---------------------------------------------------------------------------
// `RRule` from the `rrule` package, loadable everywhere.
//
// rrule publishes its ES module build only through the non-standard
// `module` field. Bundlers and Bun pick that up and see a named `RRule`
// export; Node loads the UMD `main` build instead, where the named export
// isn't visible to ESM and `import { RRule } from "rrule"` fails at link
// time. A namespace import works in both: the class is either on the
// namespace or on its CommonJS `default`.
// ---------------------------------------------------------------------------

import * as rruleModule from "rrule";

type RRuleModule = typeof rruleModule;

const loaded = rruleModule as RRuleModule & { readonly default?: RRuleModule };

export const RRule: RRuleModule["RRule"] = loaded.RRule ?? loaded.default!.RRule;
