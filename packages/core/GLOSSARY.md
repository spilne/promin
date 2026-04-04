# Glossary — Promin concepts mapped to other systems

How Promin concepts map to Temporal, Celery, Airflow, Kafka Streams, Flink, Spark, and Pandas/Polars.

---

## Workflow & Orchestration

| Promin                 | Temporal                       | Celery                    | Airflow               | Description                                      |
| ---------------------- | ------------------------------ | ------------------------- | --------------------- | ------------------------------------------------ |
| **Workflow**           | Workflow                       | Chain/Chord               | DAG                   | A sequence of steps with dependencies            |
| **Step**               | Activity                       | Task                      | Task/Operator         | A unit of work — a function that does something  |
| **WorkflowBuilder**    | Workflow function              | canvas (chain/chord)      | DAG definition        | Declarative API to define the workflow           |
| **StepRegistry**       | Activity registry              | Task registry (@app.task) | Operator classes      | Maps step names to handler functions             |
| **WorkflowStorage**    | Workflow history (event store) | Result backend            | XCom + metadata DB    | Persists workflow state and step results         |
| **StepQueue**          | Task Queue                     | Broker (Redis/RabbitMQ)   | Executor queue        | Dispatches steps to workers                      |
| **Worker**             | Worker                         | Worker (celery worker)    | Executor              | Process that polls queues and executes steps     |
| **Coordinator**        | Temporal Server                | Beat + Flower             | Scheduler             | Orchestrates workflow execution, manages DAG     |
| **Sleep**              | Timer (workflow.sleep)         | countdown/eta             | TimeSensor            | Durable pause — survives process restart         |
| **Signal**             | Signal                         | —                         | TriggerDagRunOperator | External message delivered to a running workflow |
| **waitForSignal**      | condition() + setHandler       | —                         | ExternalTaskSensor    | Suspend workflow until external event            |
| **deliverSignal**      | client.signal()                | —                         | trigger_dag_run       | Send signal to a waiting workflow                |
| **branch**             | if/else in workflow code       | —                         | BranchPythonOperator  | Conditional execution path                       |
| **mapOver**            | —                              | group()                   | Dynamic task mapping  | Fan-out: run same step over a list               |
| **Retry**              | RetryPolicy                    | autoretry                 | retries param         | Retry failed steps with backoff                  |
| **WorkflowDefinition** | Workflow type                  | Task signature            | DAG object            | The compiled, runnable workflow                  |
| **runSafe**            | client.workflow.start()        | .apply_async()            | dag.run()             | Start a workflow execution                       |
| **loadWorkflow**       | client.workflow.query()        | AsyncResult               | get_task_instance()   | Query workflow state                             |

### Key architectural differences

```
Temporal:  Dedicated server (Go binary) + gRPC + workers
Celery:    Broker (Redis/RabbitMQ) + result backend + workers
Airflow:   Scheduler + metadata DB + executor + workers
─────────────────────────────────────────────────────────────
Promin:    Postgres only. Coordinator + workers are Bun processes.
           SKIP LOCKED = task queue. No broker, no gRPC, no scheduler binary.
```

---

## Stream Processing

| Promin                       | Kafka Streams                | Flink            | Spark Streaming | Description                              |
| ---------------------------- | ---------------------------- | ---------------- | --------------- | ---------------------------------------- |
| **StreamPipeline**           | KStream                      | DataStream       | DStream         | A stream of elements with operators      |
| **StreamTopology**           | Topology                     | JobGraph         | —               | Stateful stream processing DAG           |
| **TopologyRunner**           | KafkaStreams.start()         | env.execute()    | ssc.start()     | Compiles and runs a topology             |
| **keyBy**                    | selectKey/groupByKey         | keyBy            | —               | Partition stream by key                  |
| **tumbling/sliding/session** | TimeWindows/SessionWindows   | window()         | window()        | Time-based windowing                     |
| **aggregate**                | aggregate()                  | reduce/aggregate | reduceByWindow  | Reduce items within windows              |
| **process (stateful)**       | Processor API                | ProcessFunction  | mapWithState    | Per-key stateful processing              |
| **dedupe**                   | — (manual with state store)  | —                | —               | Deduplicate by key                       |
| **join**                     | join/leftJoin                | connect/join     | join            | Join two streams by key                  |
| **checkpoint**               | Kafka offsets + state stores | Checkpointing    | WAL             | Persist state for crash recovery         |
| **ack/nack**                 | commit offset                | —                | —               | Acknowledge processed messages           |
| **Sinkable**                 | to()                         | addSink()        | foreachRDD      | Write results to external system         |
| **mapChunks**                | —                            | —                | —               | Process entire chunks for throughput     |
| **RawStream**                | —                            | —                | —               | Zero-overhead stream (no Effect runtime) |

### Key architectural differences

```
Kafka Streams:  Embedded library, Kafka is the infrastructure
Flink:          Cluster (JobManager + TaskManagers), custom network stack
Spark:          Cluster (Driver + Executors), batch-oriented micro-batching
──────────────────────────────────────────────────────────────────────────
Promin:         Single process, Kafka consumer groups for partition assignment.
                StreamTopology runs in-process. No cluster, no JVM.
```

---

## DataFrame & Analytics

| Promin                    | Pandas                | Polars                | Spark SQL             | DuckDB                 | Description                             |
| ------------------------- | --------------------- | --------------------- | --------------------- | ---------------------- | --------------------------------------- |
| **DataFrame**             | DataFrame             | DataFrame/LazyFrame   | DataFrame             | —                      | Tabular data with operations            |
| **ArrayExecutor**         | numpy arrays          | Rust engine           | Catalyst + Tungsten   | —                      | Execution engine for operations         |
| **DuckDBExecutor**        | —                     | —                     | —                     | DuckDB                 | SQL-based execution engine              |
| **AutoExecutor**          | —                     | —                     | Catalyst (auto)       | —                      | Picks best engine per query             |
| **LogicalPlan**           | —                     | LazyFrame plan        | Logical Plan          | —                      | Operations recorded before execution    |
| **collect()**             | (eager)               | .collect()            | .collect()            | —                      | Materialize results                     |
| **groupBy.agg**           | groupby().agg()       | group_by().agg()      | groupBy().agg()       | GROUP BY               | Group and aggregate                     |
| **filter**                | df[mask]              | .filter()             | .filter()             | WHERE                  | Row filtering                           |
| **withColumn**            | df["col"] = ...       | .with_columns()       | .withColumn()         | SELECT \*, expr AS col | Add computed column                     |
| **join**                  | merge()               | join()                | join()                | JOIN                   | Combine two DataFrames                  |
| **sort**                  | sort_values()         | sort()                | orderBy()             | ORDER BY               | Sort rows                               |
| **select/drop**           | df[cols]              | select()              | select()              | SELECT / EXCLUDE       | Column selection                        |
| **pivot/unpivot**         | pivot()/melt()        | pivot()/unpivot()     | pivot()               | PIVOT/UNPIVOT          | Reshape                                 |
| **window functions**      | — (manual)            | over()                | Window                | OVER()                 | row_number, rank, lag, lead             |
| **col()**                 | —                     | pl.col()              | F.col()               | "column"               | Column reference expression             |
| **when().otherwise()**    | np.where()            | when().otherwise()    | when().otherwise()    | CASE WHEN              | Conditional expression                  |
| **Expr AST**              | —                     | Expr (Rust)           | Expression tree       | SQL AST                | Inspectable expression for optimization |
| **predicate pushdown**    | —                     | Yes (auto)            | Yes (Catalyst)        | Yes (optimizer)        | Push filter into data source            |
| **CsvFile/ParquetFile**   | read_csv/read_parquet | read_csv/read_parquet | read.csv/read.parquet | read_csv_auto          | File source adapters                    |
| **Frameable**             | —                     | —                     | DataFrameReader       | —                      | Typeclass for any data source           |
| **hint + registerLoader** | —                     | —                     | —                     | —                      | Executor-native file loading            |

### Key architectural differences

```
Pandas:   Python + C extensions, eager execution, single-threaded
Polars:   Python API → Rust SIMD engine, lazy evaluation, multi-threaded
Spark:    Python/Scala API → JVM cluster, distributed, Catalyst optimizer
───────────────────────────────────────────────────────────────────────────
Promin:   TypeScript, lazy logical plan, pluggable executors (Array/DuckDB/Auto).
          Array for small data (<10K), DuckDB for analytics (10K+).
          Beats Pandas on most operations. Matches Polars on analytical queries.
```

---

## Performance Primitives

| Promin                     | Equivalent in other systems      | Description                                    |
| -------------------------- | -------------------------------- | ---------------------------------------------- |
| **Operator fusion**        | — (fs2 has chunking, not fusion) | Fuse map+filter+tap into single pass           |
| **mapChunks**              | Kafka Streams batch processing   | Process entire chunks (~4096 items) at once    |
| **RawStream**              | Java raw iterators               | Zero-overhead stream, no Effect runtime        |
| **fuseOpsToStream**        | —                                | Compile fused operators into single mapChunks  |
| **FusibleOp**              | —                                | Tagged union for fusible operations            |
| **rechunk(n)**             | —                                | Control chunk boundaries for throughput tuning |
| **AutoExecutor**           | Spark Catalyst (cost-based)      | Pick best execution engine per query plan      |
| **table caching (DuckDB)** | Spark cache/persist              | Load data once, query many times               |

---

## Infrastructure

| Promin             | Temporal                    | Celery                          | Airflow                 | What it replaces                         |
| ------------------ | --------------------------- | ------------------------------- | ----------------------- | ---------------------------------------- |
| **Postgres**       | Temporal Server + Cassandra | Redis/RabbitMQ + result backend | Metadata DB + scheduler | Everything — queue, state, coordination  |
| **SKIP LOCKED**    | gRPC task dispatch          | AMQP/Redis BRPOP                | Executor queue          | Exactly-once task claiming               |
| **Advisory locks** | Membership protocol         | —                               | HA scheduler            | Leader election                          |
| **Bun process**    | Worker (JVM/Node)           | Worker (Python)                 | Worker (Python)         | Runtime                                  |
| **`bun:test`**     | Test framework              | pytest                          | pytest                  | Testing                                  |
| **Effect**         | —                           | —                               | —                       | Typed errors, fibers, retry, concurrency |
