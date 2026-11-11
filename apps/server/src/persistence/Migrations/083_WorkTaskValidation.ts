import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE work_task_validation_attempts (
      attempt_id TEXT PRIMARY KEY NOT NULL,
      task_id TEXT NOT NULL,
      definition_revision INTEGER NOT NULL CHECK (typeof(definition_revision) = 'integer' AND definition_revision > 0),
      manager_generation INTEGER NOT NULL CHECK (typeof(manager_generation) = 'integer' AND manager_generation > 0),
      completion_policy_revision INTEGER NOT NULL CHECK (typeof(completion_policy_revision) = 'integer' AND completion_policy_revision > 0),
      evidence_id TEXT NOT NULL,
      work_watermark TEXT NOT NULL,
      validator_json TEXT NOT NULL,
      run_id TEXT UNIQUE,
      state TEXT NOT NULL CHECK (state IN ('reserved', 'started', 'result', 'uncertain')),
      outcome_json TEXT,
      created_at TEXT NOT NULL,
      started_at TEXT,
      finished_at TEXT,
      FOREIGN KEY (task_id, definition_revision) REFERENCES work_task_accepted_definitions(task_id, definition_revision),
      FOREIGN KEY (task_id, manager_generation) REFERENCES work_task_manager_bindings(task_id, generation),
      CHECK ((state = 'reserved' AND started_at IS NULL AND finished_at IS NULL AND outcome_json IS NULL)
        OR (state = 'started' AND started_at IS NOT NULL AND finished_at IS NULL AND outcome_json IS NULL)
        OR (state = 'uncertain' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND outcome_json IS NULL)
        OR (state = 'result' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND outcome_json IS NOT NULL))
    )
  `;
  yield* sql`CREATE INDEX work_task_validation_task_idx ON work_task_validation_attempts(task_id, definition_revision, evidence_id)`;
});
