/**
 * SQL interface — run raw SQL against DataFrames via DuckDB.
 * Full SQL with JOINs, CTEs, window functions, aggregations.
 */

import { DataFrame } from "@promin/data";
import { DuckDBExecutor } from "@promin/duckdb";

const executor = new DuckDBExecutor();

// --- Sample data ---

const users = DataFrame.fromArray([
  { id: 1, name: "Alice", department: "Engineering", salary: 120000 },
  { id: 2, name: "Bob", department: "Marketing", salary: 90000 },
  { id: 3, name: "Carol", department: "Engineering", salary: 140000 },
  { id: 4, name: "Diana", department: "Marketing", salary: 95000 },
  { id: 5, name: "Eve", department: "Engineering", salary: 110000 },
]);

const projects = DataFrame.fromArray([
  { userId: 1, project: "Alpha", hours: 120 },
  { userId: 1, project: "Beta", hours: 80 },
  { userId: 3, project: "Alpha", hours: 200 },
  { userId: 5, project: "Beta", hours: 150 },
  { userId: 2, project: "Gamma", hours: 100 },
]);

// --- 1. Simple JOIN ---

const report = await DataFrame.sql(
  `SELECT u.name, u.department, p.project, p.hours
   FROM users u
   JOIN projects p ON u.id = p.userId
   ORDER BY p.hours DESC`,
  { users, projects },
  executor,
);
console.log("Project report:", await report.collect());

// --- 2. CTE + aggregation ---

const deptStats = await DataFrame.sql(
  `WITH dept_summary AS (
    SELECT department,
           COUNT(*) as headcount,
           AVG(salary) as avg_salary,
           MAX(salary) as max_salary
    FROM users
    GROUP BY department
  )
  SELECT * FROM dept_summary
  ORDER BY avg_salary DESC`,
  { users },
  executor,
);
console.log("Department stats:", await deptStats.collect());

// --- 3. Window function ---

const ranked = await DataFrame.sql(
  `SELECT name, department, salary,
          RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank
   FROM users`,
  { users },
  executor,
);
console.log("Salary rankings:", await ranked.collect());

// --- 4. Instance .sql() method ---

const topEarners = await users
  .withExecutor(executor)
  .sql("SELECT name, salary FROM self WHERE salary > 100000 ORDER BY salary DESC");
console.log("Top earners:", await topEarners.collect());
