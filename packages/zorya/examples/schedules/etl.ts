import type { DurableScheduleConfig } from "@promin/workflow";

export const etlHourly: DurableScheduleConfig = {
  id: "etl-hourly",
  name: "ETL hourly",
  cron: "0 * * * *",
  timezone: "UTC",
  enabled: true,
  metadata: {
    workflowName: "etl",
    input: { source: "events-prod", batch: 7 },
  },
};
