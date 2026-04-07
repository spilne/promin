# Examples

Ordered from fundamentals to advanced. Each file is a real-world scenario — not a feature demo.

## 01-pipeline — Async actions with resilience

| File                | Scenario                                                     |
| ------------------- | ------------------------------------------------------------ |
| 01-fetch-with-retry | Fetch user from unreliable API — retry, timeout, safe result |
| 02-parallel-fetches | Dashboard from 3 APIs — parallel with per-branch resilience  |
| 03-circuit-breaker  | Payment API — stop calling a dead service                    |
| 04-race-providers   | 3 AI providers race — fastest wins, fallback if all fail     |
| 05-stream-etl       | Webhook queue → enrich → batch → bulk insert                 |
| 06-bulk-migration   | 100K users from legacy API — rate-limited, progress tracking |

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

## 04-streaming — Kafka and stream topology

| File                     | Scenario                                       |
| ------------------------ | ---------------------------------------------- |
| 01-kafka-consume-produce | Enrich payment events, produce to output topic |
| 02-click-analytics       | Real-time click counts per user per minute     |
| 03-distributed-shuffle   | Multi-instance stream processing with shuffle  |

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

## 08-patterns — Advanced composition

| File                  | Scenario                                                                        |
| --------------------- | ------------------------------------------------------------------------------- |
| 01-resilience         | Fan-out with per-branch fallbacks, competitive AI race, hedged multi-region     |
| 02-stream-composition | broadcastThrough fan-out, channel worker pools, pausable streams                |
| 03-data-pipelines     | Rate-limited migration with progress, parallel validation, supervised consumers |
