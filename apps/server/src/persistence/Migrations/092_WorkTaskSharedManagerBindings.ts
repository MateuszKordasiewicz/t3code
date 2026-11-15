import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Reinsert after recreation so deferred references resolve against the original table name.
  yield* sql`PRAGMA defer_foreign_keys = ON`;
  yield* sql`CREATE TEMP TABLE work_task_manager_binding_copy AS SELECT * FROM work_task_manager_bindings`;
  yield* sql`DROP TABLE work_task_manager_bindings`;
  yield* sql`
    CREATE TABLE work_task_manager_bindings (
      task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      generation INTEGER NOT NULL CHECK (typeof(generation) = 'integer' AND generation > 0),
      thread_id TEXT NOT NULL,
      bound_at TEXT NOT NULL,
      retired_at TEXT,
      PRIMARY KEY (task_id, generation),
      UNIQUE (task_id, thread_id)
    ) WITHOUT ROWID
  `;
  yield* sql`CREATE UNIQUE INDEX work_task_manager_current_idx ON work_task_manager_bindings(task_id) WHERE retired_at IS NULL`;
  yield* sql`CREATE UNIQUE INDEX work_task_manager_thread_binding_idx ON work_task_manager_bindings(task_id, generation, thread_id)`;
  yield* sql`CREATE INDEX work_task_manager_active_thread_idx ON work_task_manager_bindings(thread_id, task_id, generation) WHERE retired_at IS NULL`;
  yield* sql`CREATE INDEX work_task_manager_thread_history_idx ON work_task_manager_bindings(thread_id, task_id, generation)`;
  yield* sql`
    CREATE TRIGGER work_task_manager_binding_insert BEFORE INSERT ON work_task_manager_bindings
    WHEN EXISTS (
      SELECT 1 FROM work_task_manager_bindings m WHERE m.task_id = NEW.task_id
        AND (m.generation = NEW.generation OR m.thread_id = NEW.thread_id
          OR (m.retired_at IS NULL AND NEW.retired_at IS NULL))
    )
    BEGIN SELECT RAISE(ABORT, 'manager binding identity in use'); END
  `;
  yield* sql`
    CREATE TRIGGER work_task_manager_binding_update BEFORE UPDATE ON work_task_manager_bindings
    WHEN NEW.task_id IS NOT OLD.task_id OR NEW.generation IS NOT OLD.generation
      OR NEW.thread_id IS NOT OLD.thread_id
      OR EXISTS (
        SELECT 1 FROM work_task_manager_bindings m
        WHERE m.task_id = NEW.task_id AND m.generation IS NOT OLD.generation
          AND m.retired_at IS NULL AND NEW.retired_at IS NULL
      )
    BEGIN SELECT RAISE(ABORT, 'manager binding identity in use'); END
  `;
  yield* sql`
    CREATE TRIGGER work_task_manager_binding_delete BEFORE DELETE ON work_task_manager_bindings
    BEGIN SELECT RAISE(ABORT, 'manager binding history is immutable'); END
  `;
  yield* sql`INSERT INTO work_task_manager_bindings (task_id,generation,thread_id,bound_at,retired_at)
    SELECT task_id,generation,thread_id,bound_at,retired_at FROM work_task_manager_binding_copy`;
  yield* sql`DROP TABLE work_task_manager_binding_copy`;
});
