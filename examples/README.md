# Examples

Ordered from fundamentals to advanced. Each file is a real-world scenario — not a feature demo.

Resilient async actions (retry, timeout, circuit breaker, race, cache), stream composition and concurrency primitives come from [perfect](https://github.com/spilne/perfect) (`@spilne/perfect-core`) — see its examples for those patterns.

## 02-workflow — Durable execution

| File                    | Scenario                                            |
| ----------------------- | --------------------------------------------------- |
| 01-order-processing     | Validate → charge → ship with crash recovery        |
| 02-parallel-etl         | Extract → (enrich + validate in parallel) → load    |
| 03-bank-transfer-saga   | Debit → credit with automatic rollback on failure   |
| 04-approval-with-signal | Human-in-the-loop: submit, wait for manager, resume |
| 05-batch-processing     | Fan-out image processing with bounded concurrency   |

## 03-data — DataFrame and data quality

| File               | Scenario                                  |
| ------------------ | ----------------------------------------- |
| 01-analytics-query | Filter, group, aggregate sales data       |
| 02-data-quality    | Profile dataset, check for anomalies      |
| 03-sql-models      | dbt-style SQL pipeline with quality tests |

## 05-distributed — Multi-machine workers

| File                   | Scenario                                                       |
| ---------------------- | -------------------------------------------------------------- |
| 01-video-pipeline      | Download → transcribe → summarize (same code for dev and prod) |
| 02-multi-queue-workers | Coordinator + GPU/AI/default workers with routing              |

## 06-scheduler — Recurring tasks

| File                   | Scenario                                             |
| ---------------------- | ---------------------------------------------------- |
| 01-scheduled-workflows | Daily reports with cron, biweekly standup with rrule |

## 07-api — HTTP integration

| File            | Scenario                                                       |
| --------------- | -------------------------------------------------------------- |
| 01-workflow-api | REST endpoints: start KYC, poll status, receive webhook signal |
