import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE work_task_current_state (
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
      CHECK ((manager_thread_id IS NULL AND manager_generation IS NULL) OR (manager_thread_id IS NOT NULL AND length(trim(manager_thread_id)) > 0 AND manager_generation IS NOT NULL))
    )
  `;
  yield* sql`
    CREATE TABLE work_task_view_fence (
      singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
      generation INTEGER NOT NULL CHECK (typeof(generation) = 'integer' AND generation BETWEEN 0 AND 9007199254740991),
      high_water_sequence INTEGER NOT NULL CHECK (typeof(high_water_sequence) = 'integer' AND high_water_sequence BETWEEN 0 AND 9007199254740991)
    )
  `;
  yield* sql`INSERT INTO work_task_view_fence VALUES (1, 0, 0)`;
  yield* sql`CREATE INDEX work_task_projections_updated_desc_idx ON work_task_projections(updated_at DESC, task_id DESC)`;
  yield* sql`CREATE INDEX work_task_projections_project_updated_desc_idx ON work_task_projections(project_id, updated_at DESC, task_id DESC)`;
});
