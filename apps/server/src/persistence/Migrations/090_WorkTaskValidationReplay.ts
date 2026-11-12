import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE work_task_validation_replay (
      event_id TEXT PRIMARY KEY NOT NULL,
      attempt_id TEXT NOT NULL REFERENCES work_task_validation_identity(attempt_id),
      task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      sequence INTEGER NOT NULL CHECK (typeof(sequence) = 'integer' AND sequence BETWEEN 0 AND 9007199254740991),
      command_id TEXT NOT NULL,
      occurred_at TEXT NOT NULL,
      event_json TEXT NOT NULL CHECK (CASE WHEN json_valid(event_json) THEN json_type(event_json) IS 'object' ELSE 0 END),
      UNIQUE (task_id, sequence)
    )
  `;
  // Terminal outcome_json stores { outcome, findings, reason? } matching its result event.
  // SQLite trim needs the ECMAScript whitespace set used by the contract.
  const whitespace =
    "char(9,10,11,12,13,32,160,5760,8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,8232,8233,8239,8287,12288,65279)";
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER work_task_validation_replay_${operation.toLowerCase()} BEFORE ${operation} ON work_task_validation_replay
      WHEN CASE WHEN json_valid(NEW.event_json) THEN NOT (
        json_type(NEW.event_json) IS 'object'
        AND json_extract(NEW.event_json, '$.id') IS NEW.event_id
        AND json_extract(NEW.event_json, '$.taskId') IS NEW.task_id
        AND json_extract(NEW.event_json, '$.attemptId') IS NEW.attempt_id
        AND json_extract(NEW.event_json, '$.commandId') IS NEW.command_id
        AND json_extract(NEW.event_json, '$.occurredAt') IS NEW.occurred_at
        AND json_type(NEW.event_json, '$.attribution') IS 'object'
        AND json_extract(NEW.event_json, '$.attribution.type') IS 'server'
        AND json_type(NEW.event_json, '$.lifecycleRevision') IS 'integer'
        AND json_extract(NEW.event_json, '$.lifecycleRevision') BETWEEN 1 AND 9007199254740991
        AND json_type(NEW.event_json, '$.actionPolicyRevision') IS 'integer'
        AND json_extract(NEW.event_json, '$.actionPolicyRevision') BETWEEN 1 AND 9007199254740991
        AND json_type(NEW.event_json, '$.definitionRevision') IS 'integer'
        AND json_type(NEW.event_json, '$.completionPolicyRevision') IS 'integer'
        AND json_type(NEW.event_json, '$.managerGeneration') IS 'integer'
        AND json_type(NEW.event_json, '$.allowanceUnit') IS 'integer'
        AND length(CAST(NEW.occurred_at AS BLOB)) BETWEEN 1 AND 2000
        AND (json_type(NEW.event_json, '$.startedAt') IS NULL OR length(CAST(json_extract(NEW.event_json, '$.startedAt') AS BLOB)) BETWEEN 1 AND 2000)
        AND (json_type(NEW.event_json, '$.finishedAt') IS NULL OR length(CAST(json_extract(NEW.event_json, '$.finishedAt') AS BLOB)) BETWEEN 1 AND 2000)
        AND json_type(NEW.event_json, '$.validator') IS 'object'
        AND length(CAST(json_extract(NEW.event_json, '$.validator') AS BLOB)) <= 16000
        AND json_type(NEW.event_json, '$.validator.projectId') IS 'text'
        AND json_type(NEW.event_json, '$.validator.modelSelection') IS 'object'
        AND json_type(NEW.event_json, '$.validator.modelSelection.instanceId') IS 'text'
        AND length(CAST(trim(json_extract(NEW.event_json, '$.validator.modelSelection.instanceId'), ${whitespace}) AS BLOB)) BETWEEN 1 AND 64
        AND trim(json_extract(NEW.event_json, '$.validator.modelSelection.instanceId'), ${whitespace}) GLOB '[a-zA-Z]*' AND trim(json_extract(NEW.event_json, '$.validator.modelSelection.instanceId'), ${whitespace}) NOT GLOB '*[^a-zA-Z0-9_-]*'
        AND instr(trim(json_extract(NEW.event_json, '$.validator.modelSelection.instanceId'), ${whitespace}), char(0)) = 0
        AND json_type(NEW.event_json, '$.validator.modelSelection.model') IS 'text'
        AND json_type(NEW.event_json, '$.validator.workspace') IS 'object'
        AND (json_extract(NEW.event_json, '$.validator.workspace.type') = 'root'
          OR (json_extract(NEW.event_json, '$.validator.workspace.type') = 'existing_worktree' AND json_type(NEW.event_json, '$.validator.workspace.worktreePath') IS 'text')
          OR (json_extract(NEW.event_json, '$.validator.workspace.type') = 'worktree' AND json_type(NEW.event_json, '$.validator.workspace.baseRef') IS 'text'))
        AND (json_type(NEW.event_json, '$.validator.workspace.branch') IS NULL OR json_type(NEW.event_json, '$.validator.workspace.branch') IS 'text')
        AND (json_type(NEW.event_json, '$.validator.workspace.startFromOrigin') IS NULL OR json_type(NEW.event_json, '$.validator.workspace.startFromOrigin') IN ('true', 'false'))
        AND (json_type(NEW.event_json, '$.validator.modelSelection.options') IS NULL OR json_type(NEW.event_json, '$.validator.modelSelection.options') IS 'array')
        AND NOT EXISTS (SELECT 1 FROM json_each(NEW.event_json, '$.validator.modelSelection.options')
          WHERE type != 'object' OR json_type(value, '$.id') IS NOT 'text' OR json_type(value, '$.value') NOT IN ('text', 'true', 'false') OR json_type(value, '$.value') IS NULL)
        AND EXISTS (SELECT 1 FROM work_task_validation_attempts a JOIN work_task_validation_identity i ON i.attempt_id = a.attempt_id
          WHERE a.attempt_id = NEW.attempt_id AND a.task_id = NEW.task_id AND i.task_id = NEW.task_id
          AND json_extract(NEW.event_json, '$.definitionRevision') IS a.definition_revision
          AND json_extract(NEW.event_json, '$.completionPolicyRevision') IS a.completion_policy_revision
          AND json_extract(NEW.event_json, '$.managerGeneration') IS a.manager_generation
          AND json_extract(NEW.event_json, '$.claimId') IS i.claim_id
          AND json_extract(NEW.event_json, '$.evidenceId') IS a.evidence_id
          AND json_extract(NEW.event_json, '$.workWatermark') IS a.work_watermark
          AND json_extract(NEW.event_json, '$.allowanceUnit') IS i.allowance_unit
          AND json_extract(NEW.event_json, '$.createdAt') IS a.created_at
          AND (json_extract(NEW.event_json, '$.type') = 'task.validation-reserved' OR
            (json_extract(NEW.event_json, '$.runId') IS a.run_id AND json_extract(NEW.event_json, '$.startedAt') IS a.started_at))
          AND (json_extract(NEW.event_json, '$.type') NOT IN ('task.validation-result', 'task.validation-uncertain') OR
            json_extract(NEW.event_json, '$.finishedAt') IS a.finished_at)
          AND (json_extract(NEW.event_json, '$.type') != 'task.validation-result' OR
            CASE WHEN json_valid(a.outcome_json) THEN
              json_type(a.outcome_json) IS 'object'
              AND json_extract(a.outcome_json, '$.outcome') IS json_extract(NEW.event_json, '$.outcome')
              AND json_type(a.outcome_json, '$.findings') IS 'array'
              AND json_type(a.outcome_json, '$.reason') IS json_type(NEW.event_json, '$.reason')
              AND json_extract(a.outcome_json, '$.reason') IS json_extract(NEW.event_json, '$.reason')
              AND NOT EXISTS (SELECT fullkey, type, atom FROM json_tree(NEW.event_json, '$.findings')
                EXCEPT SELECT fullkey, type, atom FROM json_tree(a.outcome_json, '$.findings'))
              AND NOT EXISTS (SELECT fullkey, type, atom FROM json_tree(a.outcome_json, '$.findings')
                EXCEPT SELECT fullkey, type, atom FROM json_tree(NEW.event_json, '$.findings'))
            ELSE 0 END)
          AND CASE WHEN json_valid(a.validator_json) THEN
            NOT EXISTS (SELECT 1 FROM json_tree(NEW.event_json, '$.validator') AS supplied
              LEFT JOIN json_tree(a.validator_json) AS stored ON ('$' || substr(supplied.fullkey, 12)) = stored.fullkey
              WHERE stored.fullkey IS NULL OR supplied.type IS NOT stored.type OR supplied.atom IS NOT stored.atom)
            AND NOT EXISTS (SELECT 1 FROM json_tree(a.validator_json) AS stored
              LEFT JOIN json_tree(NEW.event_json, '$.validator') AS supplied ON ('$' || substr(supplied.fullkey, 12)) = stored.fullkey
              WHERE supplied.fullkey IS NULL)
          ELSE 0 END)
        AND CASE json_extract(NEW.event_json, '$.type')
          WHEN 'task.validation-reserved' THEN json_extract(NEW.event_json, '$.createdAt') IS NEW.occurred_at
            AND json_type(NEW.event_json, '$.runId') IS NULL AND json_type(NEW.event_json, '$.startedAt') IS NULL
            AND json_type(NEW.event_json, '$.finishedAt') IS NULL AND json_type(NEW.event_json, '$.outcome') IS NULL
            AND json_type(NEW.event_json, '$.findings') IS NULL AND json_type(NEW.event_json, '$.reason') IS NULL
          WHEN 'task.validation-started' THEN json_type(NEW.event_json, '$.runId') IS 'text'
            AND json_extract(NEW.event_json, '$.startedAt') IS NEW.occurred_at
            AND json_type(NEW.event_json, '$.finishedAt') IS NULL AND json_type(NEW.event_json, '$.outcome') IS NULL
            AND json_type(NEW.event_json, '$.findings') IS NULL AND json_type(NEW.event_json, '$.reason') IS NULL
          WHEN 'task.validation-uncertain' THEN json_type(NEW.event_json, '$.runId') IS 'text'
            AND json_type(NEW.event_json, '$.startedAt') IS 'text' AND json_extract(NEW.event_json, '$.finishedAt') IS NEW.occurred_at
            AND json_type(NEW.event_json, '$.reason') IS 'text' AND json_type(NEW.event_json, '$.outcome') IS NULL AND json_type(NEW.event_json, '$.findings') IS NULL
          WHEN 'task.validation-result' THEN json_type(NEW.event_json, '$.runId') IS 'text'
            AND json_type(NEW.event_json, '$.startedAt') IS 'text' AND json_extract(NEW.event_json, '$.finishedAt') IS NEW.occurred_at
            AND json_extract(NEW.event_json, '$.outcome') IN ('pass', 'fail', 'inconclusive', 'error')
            AND json_type(NEW.event_json, '$.findings') IS 'array' AND json_array_length(NEW.event_json, '$.findings') <= 64
            AND (json_type(NEW.event_json, '$.reason') IS NULL OR json_type(NEW.event_json, '$.reason') IS 'text')
            AND (json_extract(NEW.event_json, '$.outcome') != 'error' OR json_type(NEW.event_json, '$.reason') IS 'text')
            AND (json_extract(NEW.event_json, '$.outcome') != 'pass' OR (json_array_length(NEW.event_json, '$.findings') > 0 AND NOT EXISTS (SELECT 1 FROM json_each(NEW.event_json, '$.findings') WHERE json_extract(value, '$.outcome') IS NOT 'pass')))
            AND (json_extract(NEW.event_json, '$.outcome') != 'fail' OR EXISTS (SELECT 1 FROM json_each(NEW.event_json, '$.findings') WHERE json_extract(value, '$.outcome') IS 'fail'))
          ELSE 0 END
        AND NOT EXISTS (SELECT 1 FROM json_each(NEW.event_json, '$.findings') AS f WHERE CASE WHEN f.type = 'object' THEN NOT (
          json_type(f.value, '$.criterionId') IS 'text' AND json_type(f.value, '$.reason') IS 'text'
          AND json_extract(f.value, '$.outcome') IN ('pass', 'fail', 'inconclusive')
          AND json_type(f.value, '$.evidence') IS 'array' AND json_array_length(f.value, '$.evidence') <= 32
          AND NOT EXISTS (SELECT 1 FROM json_each(f.value, '$.evidence') AS r WHERE CASE WHEN r.type = 'object' THEN NOT (
            (json_extract(r.value, '$.type') = 'run' AND json_type(r.value, '$.threadId') IS 'text' AND json_type(r.value, '$.runId') IS 'text') OR
            (json_extract(r.value, '$.type') = 'artifact' AND json_type(r.value, '$.uri') IS 'text' AND json_type(r.value, '$.hash') IS 'text') OR
            (json_extract(r.value, '$.type') = 'source_revision' AND json_type(r.value, '$.repository') IS 'text' AND json_type(r.value, '$.revision') IS 'text')
          ) IS TRUE ELSE 1 END)
        ) IS TRUE ELSE 1 END)
        AND NOT EXISTS (SELECT 1 FROM json_each(NEW.event_json, '$.findings') GROUP BY trim(json_extract(value, '$.criterionId'), ${whitespace}) HAVING count(*) > 1)
        AND NOT EXISTS (
          WITH RECURSIVE strings(value) AS (
            SELECT NEW.event_id UNION ALL SELECT NEW.command_id
            UNION ALL SELECT json_extract(NEW.event_json, '$.runId') WHERE json_type(NEW.event_json, '$.runId') IS 'text'
            UNION ALL SELECT json_extract(NEW.event_json, '$.reason') WHERE json_type(NEW.event_json, '$.reason') IS 'text'
            UNION ALL SELECT field.value FROM json_each(NEW.event_json, '$.findings') AS f, json_each(f.value) AS field WHERE field.type = 'text' AND field.key IN ('criterionId', 'reason')
            UNION ALL SELECT field.value FROM json_each(NEW.event_json, '$.findings') AS f, json_each(f.value, '$.evidence') AS r, json_each(r.value) AS field WHERE field.type = 'text' AND field.key IN ('threadId', 'runId', 'uri', 'hash', 'repository', 'revision')
          ), units(bytes, position, count) AS (
            SELECT hex(CAST(trim(value, ${whitespace}) AS BLOB)), 1, 0 FROM strings
            UNION ALL SELECT bytes, position + 2, count + CASE WHEN substr(bytes, position, 1) IN ('8','9','A','B') THEN 0 WHEN substr(bytes, position, 1) = 'F' THEN 2 ELSE 1 END FROM units WHERE position <= length(bytes) AND count <= 2000
          ) SELECT 1 FROM units WHERE count > 2000
        )
        AND NOT EXISTS (SELECT 1 FROM (
          SELECT NEW.event_id AS value UNION ALL SELECT NEW.command_id
          UNION ALL SELECT json_extract(NEW.event_json, '$.runId') WHERE json_type(NEW.event_json, '$.runId') IS 'text'
          UNION ALL SELECT json_extract(NEW.event_json, '$.reason') WHERE json_type(NEW.event_json, '$.reason') IS 'text'
          UNION ALL SELECT field.value FROM json_each(NEW.event_json, '$.findings') AS f, json_each(f.value) AS field WHERE field.type = 'text' AND field.key IN ('criterionId', 'reason')
          UNION ALL SELECT field.value FROM json_each(NEW.event_json, '$.findings') AS f, json_each(f.value, '$.evidence') AS r, json_each(r.value) AS field WHERE field.type = 'text' AND field.key IN ('threadId', 'runId', 'uri', 'hash', 'repository', 'revision')
          UNION ALL SELECT json_extract(NEW.event_json, '$.validator.projectId')
          UNION ALL SELECT json_extract(NEW.event_json, '$.validator.modelSelection.instanceId')
          UNION ALL SELECT json_extract(NEW.event_json, '$.validator.modelSelection.model')
          UNION ALL SELECT json_extract(NEW.event_json, '$.validator.workspace.branch') WHERE json_type(NEW.event_json, '$.validator.workspace.branch') IS 'text'
          UNION ALL SELECT json_extract(NEW.event_json, '$.validator.workspace.worktreePath') WHERE json_extract(NEW.event_json, '$.validator.workspace.type') = 'existing_worktree'
          UNION ALL SELECT json_extract(NEW.event_json, '$.validator.workspace.baseRef') WHERE json_extract(NEW.event_json, '$.validator.workspace.type') = 'worktree'
          UNION ALL SELECT json_extract(value, '$.id') FROM json_each(NEW.event_json, '$.validator.modelSelection.options')
          UNION ALL SELECT json_extract(value, '$.value') FROM json_each(NEW.event_json, '$.validator.modelSelection.options') WHERE json_type(value, '$.value') IS 'text'
        ) WHERE length(CAST(trim(value, ${whitespace}) AS BLOB)) = 0)
      ) IS TRUE ELSE 0 END BEGIN SELECT RAISE(ABORT, 'invalid validation replay'); END`);
  }
  // node:sqlite replaces lone surrogates; bind them through JSON and page using hex/unhex.
  yield* sql`CREATE INDEX work_task_validation_replay_history_idx ON work_task_validation_replay(task_id, sequence DESC, attempt_id DESC)`;

  yield* sql`
    CREATE TRIGGER work_task_validation_replay_attempt_update BEFORE UPDATE ON work_task_validation_attempts
    WHEN EXISTS (SELECT 1 FROM work_task_validation_replay WHERE attempt_id = OLD.attempt_id)
      AND (NEW.created_at IS NOT OLD.created_at OR NEW.validator_json IS NOT OLD.validator_json
        OR (NEW.outcome_json IS NOT OLD.outcome_json AND EXISTS (SELECT 1 FROM work_task_validation_replay r WHERE r.attempt_id = OLD.attempt_id AND json_extract(r.event_json, '$.type') = 'task.validation-result'))
        OR EXISTS (SELECT 1 FROM work_task_validation_replay r WHERE r.attempt_id = OLD.attempt_id
          AND json_type(r.event_json, '$.runId') IS 'text'
          AND (NEW.run_id IS NOT OLD.run_id OR NEW.started_at IS NOT OLD.started_at))
        OR EXISTS (SELECT 1 FROM work_task_validation_replay r WHERE r.attempt_id = OLD.attempt_id
          AND json_type(r.event_json, '$.finishedAt') IS 'text' AND NEW.finished_at IS NOT OLD.finished_at))
    BEGIN SELECT RAISE(ABORT, 'validation replay binding in use'); END
  `;
  yield* sql`
    CREATE TRIGGER work_task_validation_replay_identity_update BEFORE UPDATE ON work_task_validation_identity
    WHEN (EXISTS (SELECT 1 FROM work_task_validation_replay WHERE attempt_id = OLD.attempt_id)
      AND (NEW.attempt_id IS NOT OLD.attempt_id OR NEW.task_id IS NOT OLD.task_id
        OR NEW.claim_id IS NOT OLD.claim_id OR NEW.allowance_unit IS NOT OLD.allowance_unit))
      OR EXISTS (SELECT 1 FROM work_task_validation_identity i JOIN work_task_validation_replay r ON r.attempt_id = i.attempt_id
        WHERE i.attempt_id IS NOT OLD.attempt_id AND (i.attempt_id = NEW.attempt_id OR (i.task_id = NEW.task_id AND i.allowance_unit = NEW.allowance_unit)))
    BEGIN SELECT RAISE(ABORT, 'validation replay binding in use'); END
  `;
  yield* sql`
    CREATE TRIGGER work_task_validation_replay_identity_insert BEFORE INSERT ON work_task_validation_identity
    WHEN EXISTS (SELECT 1 FROM work_task_validation_identity i JOIN work_task_validation_replay r ON r.attempt_id = i.attempt_id
      WHERE i.attempt_id = NEW.attempt_id OR (i.task_id = NEW.task_id AND i.allowance_unit = NEW.allowance_unit))
    BEGIN SELECT RAISE(ABORT, 'validation replay binding in use'); END
  `;

  // The store supplies canonical UTC seconds and a fraction with trailing zeroes removed.
  yield* sql`
    CREATE TABLE work_task_validation_instant (
      attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_validation_attempts(attempt_id),
      task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      state TEXT NOT NULL CHECK (state IN ('reserved', 'started', 'result', 'uncertain')),
      utc_seconds INTEGER NOT NULL CHECK (typeof(utc_seconds) = 'integer'),
      fraction TEXT NOT NULL CHECK (typeof(fraction) = 'text' AND fraction NOT GLOB '*[^0-9]*' AND length(CAST(fraction AS BLOB)) = length(fraction) AND (fraction = '' OR substr(fraction, -1) != '0'))
    )
  `;
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER work_task_validation_instant_${operation.toLowerCase()} BEFORE ${operation} ON work_task_validation_instant
      WHEN NOT EXISTS (SELECT 1 FROM work_task_validation_attempts a
        WHERE a.attempt_id = NEW.attempt_id AND a.task_id = NEW.task_id AND a.state = NEW.state)
      BEGIN SELECT RAISE(ABORT, 'invalid validation instant binding'); END`);
  }
  // Derived ordering is repopulated atomically by the store after parent changes.
  yield* sql`
    CREATE TRIGGER work_task_validation_instant_parent_update AFTER UPDATE ON work_task_validation_attempts
    WHEN NEW.attempt_id IS NOT OLD.attempt_id OR NEW.task_id IS NOT OLD.task_id
      OR NEW.state IS NOT OLD.state OR NEW.created_at IS NOT OLD.created_at
    BEGIN DELETE FROM work_task_validation_instant WHERE attempt_id = OLD.attempt_id; END
  `;
  yield* sql`
    CREATE TRIGGER work_task_validation_instant_parent_insert BEFORE INSERT ON work_task_validation_attempts
    WHEN EXISTS (SELECT 1 FROM work_task_validation_instant i
      JOIN work_task_validation_attempts a ON a.attempt_id = i.attempt_id
      WHERE a.attempt_id = NEW.attempt_id OR (a.run_id IS NOT NULL AND a.run_id = NEW.run_id))
    BEGIN SELECT RAISE(ABORT, 'validation instant in use'); END
  `;
  yield* sql`
    CREATE TRIGGER work_task_validation_instant_parent_collision BEFORE UPDATE ON work_task_validation_attempts
    WHEN EXISTS (SELECT 1 FROM work_task_validation_instant i
      JOIN work_task_validation_attempts a ON a.attempt_id = i.attempt_id
      WHERE a.attempt_id IS NOT OLD.attempt_id
        AND (a.attempt_id = NEW.attempt_id OR (a.run_id IS NOT NULL AND a.run_id = NEW.run_id)))
    BEGIN SELECT RAISE(ABORT, 'validation instant in use'); END
  `;
  yield* sql`CREATE INDEX work_task_validation_chronological_idx ON work_task_validation_instant(task_id, state, utc_seconds DESC, fraction DESC, attempt_id DESC)`;
});
