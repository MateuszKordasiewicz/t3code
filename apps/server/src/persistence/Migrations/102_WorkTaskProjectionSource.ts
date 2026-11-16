import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE work_task_current_state_v102 (
      task_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_projections(task_id),
      configuration_json TEXT CHECK (configuration_json IS NULL OR json_valid(configuration_json)),
      desired_mode TEXT CHECK (desired_mode IS NULL OR desired_mode IN ('not_started', 'active', 'paused', 'closed')),
      execution_mode TEXT CHECK (execution_mode IS NULL OR execution_mode IN ('not_started', 'idle', 'queued', 'running', 'waiting')),
      manager_thread_id TEXT,
      manager_generation INTEGER CHECK (manager_generation IS NULL OR (typeof(manager_generation) = 'integer' AND manager_generation BETWEEN 1 AND 9007199254740991)),
      attention_json TEXT CHECK (attention_json IS NULL OR json_valid(attention_json)),
      completion TEXT CHECK (completion IS NULL OR completion IN ('incomplete', 'ready_for_review', 'validating', 'done')),
      archived INTEGER CHECK (archived IS NULL OR (typeof(archived) = 'integer' AND archived IN (0, 1))),
      last_applied_sequence INTEGER CHECK (last_applied_sequence IS NULL OR (typeof(last_applied_sequence) = 'integer' AND last_applied_sequence BETWEEN 0 AND 9007199254740991)),
      CHECK ((manager_thread_id IS NULL AND manager_generation IS NULL) OR (manager_thread_id IS NOT NULL AND length(CAST(trim(manager_thread_id) AS BLOB)) > 0 AND manager_generation IS NOT NULL))
    )
  `;
  yield* sql`INSERT INTO work_task_current_state_v102(rowid,task_id,configuration_json,desired_mode,execution_mode,manager_thread_id,manager_generation,attention_json,completion,archived,last_applied_sequence)
    SELECT rowid,task_id,configuration_json,desired_mode,execution_mode,manager_thread_id,manager_generation,attention_json,completion,archived,last_applied_sequence FROM work_task_current_state`;
  yield* sql`DROP TABLE work_task_current_state`;
  yield* sql`ALTER TABLE work_task_current_state_v102 RENAME TO work_task_current_state`;
  yield* sql`CREATE INDEX idx_task_source_type_sequence ON orchestration_events(stream_id,event_type,sequence DESC)
    WHERE aggregate_kind='task' AND application_event_version=2`;
  yield* sql`CREATE INDEX idx_task_source_manager_sequence ON orchestration_events(
    stream_id,CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.manager.managerGeneration') END,sequence DESC)
    WHERE aggregate_kind='task' AND application_event_version=2 AND event_type='task.execution-updated'
      AND CASE WHEN json_valid(payload_json) THEN json_type(payload_json,'$.manager') END='object'`;
});
