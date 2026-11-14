import type { DefaultTheme } from "vitepress";

export const sidebar = [
  {
    text: "Promin",
    link: "/docs/",
  },
  {
    text: "Getting Started",
    link: "/docs/getting-started/",
  },
  {
    text: "Workflow",
    collapsed: false,
    items: [
      {
        text: "Overview",
        link: "/docs/workflow/overview",
      },
      {
        text: "Durable Execution",
        link: "/docs/workflow/workflows",
      },
      {
        text: "Storage",
        link: "/docs/workflow/storage",
      },
      {
        text: "Distributed Workflow Execution",
        link: "/docs/workflow/distributed-workers",
      },
      {
        text: "Scheduler",
        link: "/docs/workflow/scheduler",
      },
      {
        text: "Versioning",
        link: "/docs/workflow/versioning",
      },
      {
        text: "SQL Models",
        link: "/docs/workflow/sql-models",
      },
      {
        text: "Examples",
        collapsed: true,
        items: [
          {
            text: "Order processing workflow \\u2014 validate, charge, ship",
            link: "/docs/workflow/examples/01-order-processing",
          },
          {
            text: "ETL pipeline with parallel steps",
            link: "/docs/workflow/examples/02-parallel-etl",
          },
          {
            text: "Bank transfer with saga compensation",
            link: "/docs/workflow/examples/03-bank-transfer-saga",
          },
          {
            text: "Approval workflow with human-in-the-loop",
            link: "/docs/workflow/examples/04-approval-with-signal",
          },
          {
            text: "Process a batch of images with bounded concurrency",
            link: "/docs/workflow/examples/05-batch-processing",
          },
          {
            text: "State machine \u2014 event-driven with typed per-state context",
            link: "/docs/workflow/examples/06-state-machine",
          },
          {
            text: "Video processing across specialized machines",
            link: "/docs/workflow/examples/01-video-pipeline",
          },
          {
            text: "Specialized workers for the video pipeline",
            link: "/docs/workflow/examples/02-multi-queue-workers",
          },
          {
            text: "Priority queue \u2014 ordering, routing and concurrency keys",
            link: "/docs/workflow/examples/03-priority-fairness",
          },
        ],
      },
    ],
  },
  {
    text: "Agents",
    collapsed: false,
    items: [
      {
        text: "Build your first agent",
        link: "/docs/agent/quickstart",
      },
      {
        text: "Agent API",
        link: "/docs/agent/overview",
      },
      {
        text: "Agent evaluations",
        link: "/docs/agent/evals",
      },
      {
        text: "Zorya platform",
        link: "/docs/agent/zorya",
      },
    ],
  },
  {
    text: "Data",
    collapsed: false,
    items: [
      {
        text: "DataFrame",
        link: "/docs/data/dataframe",
      },
      {
        text: "Data Quality",
        link: "/docs/data/quality",
      },
      {
        text: "Examples",
        collapsed: true,
        items: [
          {
            text: "Analyze sales data \\u2014 filter, group, aggregate, sort",
            link: "/docs/data/examples/01-analytics-query",
          },
          {
            text: "Validate data quality before loading to production",
            link: "/docs/data/examples/02-data-quality",
          },
          {
            text: "dbt-style SQL pipeline \\u2014 staging views \\u2192 fact tables with quality tests",
            link: "/docs/data/examples/03-sql-models",
          },
          {
            text: "Streaming ETL \u2014 process large files in constant memory",
            link: "/docs/data/examples/04-streaming-etl",
          },
          {
            text: "Advanced analytics \u2014 percentiles, resampling, forward fill",
            link: "/docs/data/examples/05-advanced-analytics",
          },
          {
            text: "SQL queries \u2014 run raw SQL against DataFrames",
            link: "/docs/data/examples/06-sql-queries",
          },
        ],
      },
    ],
  },
  {
    text: "Integrations",
    collapsed: false,
    items: [
      {
        text: "Postgres",
        link: "/docs/integrations/postgres",
      },
      {
        text: "Redis",
        link: "/docs/integrations/redis",
      },
      {
        text: "SQLite",
        link: "/docs/integrations/sqlite",
      },
      {
        text: "Workflow over HTTP",
        link: "/docs/integrations/workflow-remote",
      },
    ],
  },
  {
    text: "Reference",
    collapsed: false,
    items: [
      {
        text: "Glossary",
        link: "/docs/reference/glossary",
      },
    ],
  },
] satisfies DefaultTheme.SidebarItem[];
