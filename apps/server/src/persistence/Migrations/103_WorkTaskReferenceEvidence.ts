import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const paths = [
    ["manager", ["task.execution-updated"]],
    ["review", ["task.action-review-requested"]],
    [
      "binding",
      [
        "task.action-review-invalidated",
        "task.action-review-cancelled",
        "task.action-review-continuation-released",
      ],
    ],
    [
      "",
      [
        "task.manager-bound",
        "task.manager-retired",
        "task.wait-updated",
        "task.continuation-accepted",
        "task.completion-submitted",
        "task.completed",
        "task.action-review-result-recorded",
      ],
    ],
  ] as const;
  const types = paths
    .flatMap(([, names]) => names)
    .map((name) => `'${name}'`)
    .join(",");
  for (const [field, name] of [
    ["managerThreadId", "thread"],
    ["managerGeneration", "generation"],
  ] as const) {
    const expression = `CASE WHEN json_valid(payload_json) THEN CASE ${paths.map(([path, names]) => `WHEN event_type IN (${names.map((type) => `'${type}'`).join(",")}) THEN json_extract(payload_json,'$.${path ? path + "." : ""}${field}')`).join(" ")} END END`;
    yield* sql.unsafe(`CREATE INDEX idx_task_reference_${name} ON orchestration_events(stream_id,${expression},sequence)
      WHERE aggregate_kind='task' AND application_event_version=2 AND event_type IN (${types})`);
  }
  yield* sql`CREATE INDEX idx_task_allowance_debit_unit ON orchestration_events(
    stream_id,CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.allowanceUnit') END,sequence)
    WHERE aggregate_kind='task' AND application_event_version=2 AND event_type='task.allowance-debited'
      AND CASE WHEN json_valid(payload_json) THEN json_type(payload_json,'$.allowanceUnit') END='integer'`;
  yield* sql`CREATE TABLE work_task_manager_reference_checkpoints (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_projections(task_id),
    creation_event_id TEXT NOT NULL,
    creation_sequence INTEGER NOT NULL CHECK(typeof(creation_sequence)='integer' AND creation_sequence BETWEEN 0 AND 9007199254740991),
    checked_event_id TEXT NOT NULL,
    checked_sequence INTEGER NOT NULL CHECK(typeof(checked_sequence)='integer' AND checked_sequence BETWEEN creation_sequence AND 9007199254740991),
    checked_stream_version INTEGER NOT NULL CHECK(typeof(checked_stream_version)='integer' AND checked_stream_version BETWEEN 0 AND 9007199254740991),
    CHECK((checked_stream_version=0 AND checked_sequence=creation_sequence AND checked_event_id=creation_event_id)
      OR (checked_stream_version>0 AND checked_sequence>creation_sequence AND checked_event_id<>creation_event_id))
  ) WITHOUT ROWID`;
  yield* sql`CREATE TRIGGER work_task_reference_insert BEFORE INSERT ON work_task_manager_reference_checkpoints
    WHEN EXISTS(SELECT 1 FROM work_task_manager_reference_checkpoints WHERE task_id=NEW.task_id)
    BEGIN SELECT RAISE(ABORT,'reference checkpoint already exists'); END`;
  yield* sql`CREATE TRIGGER work_task_reference_update BEFORE UPDATE ON work_task_manager_reference_checkpoints
    WHEN NEW.task_id IS NOT OLD.task_id OR NEW.creation_event_id IS NOT OLD.creation_event_id OR NEW.creation_sequence IS NOT OLD.creation_sequence
      OR NOT ((NEW.checked_event_id IS OLD.checked_event_id AND NEW.checked_sequence IS OLD.checked_sequence AND NEW.checked_stream_version IS OLD.checked_stream_version)
        OR (NEW.checked_event_id IS NOT OLD.checked_event_id AND NEW.checked_sequence>OLD.checked_sequence AND NEW.checked_stream_version>OLD.checked_stream_version))
    BEGIN SELECT RAISE(ABORT,'reference checkpoint cannot regress'); END`;
  yield* sql`CREATE TRIGGER work_task_reference_delete BEFORE DELETE ON work_task_manager_reference_checkpoints
    BEGIN SELECT RAISE(ABORT,'reference checkpoint cannot be deleted'); END`;
});
