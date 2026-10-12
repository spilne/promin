// A child process for the multi-process registry test: open the database
// file, register `count` distinct fragment bodies under one id, print a
// JSON tally of successes and errors.
//   bun --conditions=@promin/source registry-writer.ts <file> <tag> <count>

import { Database } from "bun:sqlite";
import { SqliteFragmentRegistry } from "../../registries.ts";

const [file, tag, countArg] = process.argv.slice(2);
const db = new Database(file!);
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA busy_timeout = 10000");
const registry = new SqliteFragmentRegistry({ db });
let ok = 0;
const errors: string[] = [];
for (let i = 0; i < Number(countArg); i++) {
  try {
    await registry.register({ id: "shared", content: `${tag}-${i}` });
    ok += 1;
  } catch (err) {
    errors.push(err instanceof Error ? err.message : String(err));
  }
}
console.log(JSON.stringify({ ok, errors }));
