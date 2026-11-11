import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE work_task_manager_bindings (
      task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      generation INTEGER NOT NULL CHECK (typeof(generation) = 'integer' AND generation > 0),
      thread_id TEXT NOT NULL UNIQUE,
      bound_at TEXT NOT NULL,
      retired_at TEXT,
      PRIMARY KEY (task_id, generation)
    )
  `;
  yield* sql`CREATE UNIQUE INDEX work_task_manager_current_idx ON work_task_manager_bindings(task_id) WHERE retired_at IS NULL`;
  yield* sql`
    CREATE TABLE work_task_allowances (
      task_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_projections(task_id),
      configured_cap INTEGER NOT NULL CHECK (typeof(configured_cap) = 'integer' AND configured_cap > 0),
      consumed INTEGER NOT NULL CHECK (typeof(consumed) = 'integer' AND consumed >= 0 AND consumed <= configured_cap)
    )
  `;
  yield* sql`
    CREATE TABLE work_task_wake_reservations (
      reservation_id TEXT PRIMARY KEY NOT NULL,
      task_id TEXT NOT NULL,
      manager_generation INTEGER NOT NULL CHECK (typeof(manager_generation) = 'integer' AND manager_generation > 0),
      command_id TEXT NOT NULL UNIQUE,
      message_id TEXT UNIQUE,
      run_id TEXT UNIQUE,
      allowance_unit INTEGER NOT NULL CHECK (typeof(allowance_unit) = 'integer' AND allowance_unit > 0),
      dirty_generation INTEGER NOT NULL CHECK (typeof(dirty_generation) = 'integer' AND dirty_generation > 0),
      acknowledged_generation INTEGER NOT NULL CHECK (typeof(acknowledged_generation) = 'integer' AND acknowledged_generation >= 0 AND acknowledged_generation <= dirty_generation),
      created_at TEXT NOT NULL,
      claimed_at TEXT,
      FOREIGN KEY (task_id, manager_generation) REFERENCES work_task_manager_bindings(task_id, generation),
      FOREIGN KEY (task_id) REFERENCES work_task_allowances(task_id),
      UNIQUE (task_id, allowance_unit),
      UNIQUE (task_id, reservation_id)
    )
  `;
  yield* sql`CREATE UNIQUE INDEX work_task_wake_pending_idx ON work_task_wake_reservations(task_id, manager_generation) WHERE claimed_at IS NULL`;
  yield* sql`
    CREATE TABLE work_task_wake_sources (
      task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      source_kind TEXT NOT NULL,
      source_id TEXT NOT NULL,
      reservation_id TEXT,
      result_ref TEXT,
      PRIMARY KEY (task_id, source_kind, source_id),
      FOREIGN KEY (task_id, reservation_id) REFERENCES work_task_wake_reservations(task_id, reservation_id)
    )
  `;
});
