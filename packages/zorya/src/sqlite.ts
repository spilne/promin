// ---------------------------------------------------------------------------
// @promin/zorya/sqlite — a single-process Zorya on one Bun SQLite database.
// Kept off the root entry so `@promin/sqlite` (and `bun:sqlite`) load only
// for hosts that use it.
// ---------------------------------------------------------------------------

export {
  createSqliteZoryaStack,
  type SqliteZoryaStack,
  type SqliteZoryaStackConfig,
} from "./server/sqlite-stack.ts";
