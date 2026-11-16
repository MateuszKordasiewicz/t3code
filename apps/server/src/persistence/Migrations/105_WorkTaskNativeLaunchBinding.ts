import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE work_task_native_launch_intents (
    command_id TEXT PRIMARY KEY NOT NULL,
    environment_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    manager_thread_id TEXT NOT NULL,
    manager_generation INTEGER NOT NULL,
    message_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    FOREIGN KEY(environment_id,group_id,manager_generation,manager_thread_id)
      REFERENCES work_task_group_managers(environment_id,group_id,generation,manager_thread_id),
    FOREIGN KEY(task_id) REFERENCES work_task_projections(task_id)
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_native_launch_task_idx ON work_task_native_launch_intents(task_id,created_at)`;
  yield* sql`CREATE TABLE work_task_native_launch_bindings (
    command_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_native_launch_intents(command_id),
    run_id TEXT NOT NULL UNIQUE,
    run_event_id TEXT NOT NULL UNIQUE,
    run_sequence INTEGER NOT NULL UNIQUE,
    receipt_sequence INTEGER NOT NULL,
    FOREIGN KEY(run_sequence) REFERENCES orchestration_v2_events(sequence)
  ) WITHOUT ROWID`;
  yield* sql`CREATE TABLE work_task_native_terminal_observations (
    command_id TEXT NOT NULL REFERENCES work_task_native_launch_bindings(command_id),
    event_id TEXT PRIMARY KEY NOT NULL,
    sequence INTEGER NOT NULL UNIQUE REFERENCES orchestration_v2_events(sequence),
    status TEXT NOT NULL CHECK(status IN('completed','interrupted','failed','cancelled','rolled_back'))
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_native_terminal_command_idx ON work_task_native_terminal_observations(command_id,sequence DESC)`;
  for (const table of [
    "work_task_native_launch_intents",
    "work_task_native_launch_bindings",
    "work_task_native_terminal_observations",
  ]) {
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_immutable_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT,'immutable native launch evidence'); END`,
    );
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_immutable_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT,'immutable native launch evidence'); END`,
    );
  }
});
