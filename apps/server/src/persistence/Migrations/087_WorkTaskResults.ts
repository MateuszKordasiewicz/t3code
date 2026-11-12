import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE work_task_results (
      event_id TEXT PRIMARY KEY NOT NULL,
      task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      source_id TEXT NOT NULL,
      contributor_thread_id TEXT,
      attribution_json TEXT NOT NULL CHECK (CASE WHEN json_valid(attribution_json) THEN
        json_type(attribution_json) IS 'object' AND (
          json_extract(attribution_json, '$.type') = 'server' OR
          (json_extract(attribution_json, '$.type') = 'user' AND json_type(attribution_json, '$.sessionId') IS 'text' AND json_type(attribution_json, '$.subject') IS 'text') OR
          (json_extract(attribution_json, '$.type') IN ('agent', 'manager') AND json_type(attribution_json, '$.threadId') IS 'text' AND json_type(attribution_json, '$.providerSessionId') IS 'text' AND json_type(attribution_json, '$.runId') IS 'text' AND
            (json_extract(attribution_json, '$.type') = 'agent' OR (json_type(attribution_json, '$.generation') IS 'integer' AND json_extract(attribution_json, '$.generation') BETWEEN 1 AND 9007199254740991)))
        ) IS TRUE ELSE 0 END),
      summary TEXT NOT NULL,
      results_json TEXT NOT NULL CHECK (CASE WHEN json_valid(results_json) THEN
        json_type(results_json) IS 'array' AND json_array_length(results_json) <= 32 ELSE 0 END),
      sequence INTEGER NOT NULL CHECK (typeof(sequence) = 'integer' AND sequence BETWEEN 0 AND 9007199254740991),
      recorded_at TEXT NOT NULL
    )
  `;
  // SQLite trim needs the ECMAScript whitespace set used by the contract.
  const whitespace =
    "char(9,10,11,12,13,32,160,5760,8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,8232,8233,8239,8287,12288,65279)";
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER work_task_results_bounds_${operation.toLowerCase()} BEFORE ${operation} ON work_task_results
      WHEN CASE WHEN json_valid(NEW.results_json) AND json_valid(NEW.attribution_json) THEN
        EXISTS (SELECT 1 FROM json_each(NEW.results_json) WHERE CASE WHEN type = 'object' THEN NOT (
          (json_extract(value, '$.type') = 'run' AND json_type(value, '$.threadId') IS 'text' AND json_type(value, '$.runId') IS 'text') OR
          (json_extract(value, '$.type') = 'artifact' AND json_type(value, '$.uri') IS 'text' AND json_type(value, '$.hash') IS 'text') OR
          (json_extract(value, '$.type') = 'source_revision' AND json_type(value, '$.repository') IS 'text' AND json_type(value, '$.revision') IS 'text')
        ) IS TRUE ELSE 1 END) OR EXISTS (
          WITH RECURSIVE strings(value) AS (
            SELECT NEW.source_id UNION ALL SELECT NEW.summary UNION ALL SELECT NEW.contributor_thread_id WHERE NEW.contributor_thread_id IS NOT NULL
            UNION ALL SELECT value FROM json_each(NEW.attribution_json) WHERE type = 'text' AND ((json_extract(NEW.attribution_json, '$.type') = 'user' AND key IN ('sessionId', 'subject')) OR (json_extract(NEW.attribution_json, '$.type') IN ('agent', 'manager') AND key IN ('threadId', 'providerSessionId', 'runId')))
            UNION ALL SELECT field.value FROM json_each(NEW.results_json) AS item, json_each(item.value) AS field WHERE item.type = 'object' AND field.type = 'text' AND ((json_extract(item.value, '$.type') = 'run' AND field.key IN ('threadId', 'runId')) OR (json_extract(item.value, '$.type') = 'artifact' AND field.key IN ('uri', 'hash')) OR (json_extract(item.value, '$.type') = 'source_revision' AND field.key IN ('repository', 'revision')))
          ), units(bytes, position, count) AS (
            SELECT hex(CAST(value AS BLOB)), 1, 0 FROM strings
            UNION ALL SELECT bytes, position + 2, count + CASE
              WHEN substr(bytes, position, 1) IN ('8', '9', 'A', 'B') THEN 0
              WHEN substr(bytes, position, 1) = 'F' THEN 2 ELSE 1 END
            FROM units WHERE position <= length(bytes) AND count <= 2000
          ) SELECT 1 FROM units WHERE count > 2000
        ) OR EXISTS (
          SELECT 1 FROM (SELECT NEW.source_id AS value UNION ALL SELECT NEW.summary UNION ALL SELECT NEW.contributor_thread_id WHERE NEW.contributor_thread_id IS NOT NULL
            UNION ALL SELECT value FROM json_each(NEW.attribution_json) WHERE type = 'text' AND ((json_extract(NEW.attribution_json, '$.type') = 'user' AND key IN ('sessionId', 'subject')) OR (json_extract(NEW.attribution_json, '$.type') IN ('agent', 'manager') AND key IN ('threadId', 'providerSessionId', 'runId')))
            UNION ALL SELECT field.value FROM json_each(NEW.results_json) AS item, json_each(item.value) AS field WHERE item.type = 'object' AND field.type = 'text' AND ((json_extract(item.value, '$.type') = 'run' AND field.key IN ('threadId', 'runId')) OR (json_extract(item.value, '$.type') = 'artifact' AND field.key IN ('uri', 'hash')) OR (json_extract(item.value, '$.type') = 'source_revision' AND field.key IN ('repository', 'revision'))))
          WHERE length(CAST(trim(value, ${whitespace}) AS BLOB)) = 0
        ) OR length(CAST(trim(NEW.event_id, ${whitespace}) AS BLOB)) = 0 ELSE 0 END BEGIN SELECT RAISE(ABORT, 'invalid result'); END`);
  }
  yield* sql`CREATE INDEX work_task_results_history_idx ON work_task_results(task_id, sequence DESC, event_id DESC)`;
});
