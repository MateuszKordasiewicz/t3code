import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE work_task_waits (
      task_id TEXT NOT NULL,
      wait_id TEXT NOT NULL,
      manager_generation INTEGER NOT NULL CHECK (typeof(manager_generation) = 'integer' AND manager_generation BETWEEN 1 AND 9007199254740991),
      target_json TEXT NOT NULL CHECK (CASE WHEN json_valid(target_json) THEN
        length(CAST(target_json AS BLOB)) <= 32768 AND json_type(target_json) IS 'object' AND (
          (json_extract(target_json, '$.type') = 'source' AND json_type(target_json, '$.sourceId') IS 'text') OR
          (json_extract(target_json, '$.type') = 'run' AND json_type(target_json, '$.threadId') IS 'text' AND json_type(target_json, '$.runId') IS 'text') OR
          (json_extract(target_json, '$.type') = 'result' AND json_type(target_json, '$.result') IS 'object' AND (
            (json_extract(target_json, '$.result.type') = 'run' AND json_type(target_json, '$.result.threadId') IS 'text' AND json_type(target_json, '$.result.runId') IS 'text') OR
            (json_extract(target_json, '$.result.type') = 'artifact' AND json_type(target_json, '$.result.uri') IS 'text' AND json_type(target_json, '$.result.hash') IS 'text') OR
            (json_extract(target_json, '$.result.type') = 'source_revision' AND json_type(target_json, '$.result.repository') IS 'text' AND json_type(target_json, '$.result.revision') IS 'text')
          ))
        ) IS TRUE ELSE 0 END),
      state TEXT NOT NULL CHECK (state IN ('registered', 'cleared')),
      updated_sequence INTEGER NOT NULL CHECK (typeof(updated_sequence) = 'integer' AND updated_sequence BETWEEN 0 AND 9007199254740991),
      updated_at TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
      PRIMARY KEY (task_id, wait_id),
      FOREIGN KEY (task_id, manager_generation) REFERENCES work_task_manager_bindings(task_id, generation)
    )
  `;
  // Count UTF-16 units, including NUL and supplementary characters, as in the contract.
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER work_task_waits_bounds_${operation.toLowerCase()} BEFORE ${operation} ON work_task_waits
      WHEN json_valid(NEW.target_json) AND EXISTS (
        WITH RECURSIVE strings(value) AS (
          SELECT NEW.wait_id UNION ALL SELECT value FROM json_tree(NEW.target_json) WHERE type = 'text'
        ), units(bytes, position, count) AS (
          SELECT hex(CAST(value AS BLOB)), 1, 0 FROM strings
          UNION ALL SELECT bytes, position + 2, count + CASE
            WHEN substr(bytes, position, 1) IN ('8', '9', 'A', 'B') THEN 0
            WHEN substr(bytes, position, 1) = 'F' THEN 2 ELSE 1 END
          FROM units WHERE position <= length(bytes) AND count <= 2000
        ) SELECT 1 FROM units WHERE count > 2000
      ) OR length(trim(NEW.wait_id)) = 0 OR (json_valid(NEW.target_json) AND EXISTS (
        SELECT 1 FROM json_tree(NEW.target_json) WHERE type = 'text' AND length(trim(value)) = 0
      )) BEGIN SELECT RAISE(ABORT, 'invalid wait text'); END`);
  }
  yield* sql`CREATE INDEX work_task_waits_state_idx ON work_task_waits(task_id, state, wait_id)`;
  yield* sql`CREATE INDEX work_task_thread_links_task_page_idx ON work_task_thread_links(task_id, linked_at DESC, link_event_id DESC) WHERE unlink_event_id IS NULL`;
  yield* sql`CREATE INDEX work_task_thread_links_reverse_page_idx ON work_task_thread_links(thread_id, task_id) WHERE unlink_event_id IS NULL`;
});
