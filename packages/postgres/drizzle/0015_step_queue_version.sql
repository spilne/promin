-- Versioned step dispatch: tag tasks with the workflow version that enqueued
-- them so workers can filter by supported versions during rolling deploys.

ALTER TABLE wf_step_queue
  ADD COLUMN IF NOT EXISTS version TEXT;
