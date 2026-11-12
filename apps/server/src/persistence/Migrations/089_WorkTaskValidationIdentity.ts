import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE work_task_validation_identity (
      attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_validation_attempts(attempt_id),
      task_id TEXT NOT NULL REFERENCES work_task_allowances(task_id),
      claim_id TEXT NOT NULL,
      allowance_unit INTEGER NOT NULL CHECK (typeof(allowance_unit) = 'integer' AND allowance_unit BETWEEN 1 AND 9007199254740991),
      FOREIGN KEY (task_id, claim_id) REFERENCES work_task_claims(task_id, claim_id),
      UNIQUE (task_id, allowance_unit)
    )
  `;
  const whitespace =
    "char(9,10,11,12,13,32,160,5760,8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,8232,8233,8239,8287,12288,65279)";
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER work_task_validation_identity_${operation.toLowerCase()} BEFORE ${operation} ON work_task_validation_identity
      WHEN NOT EXISTS (
        SELECT 1 FROM work_task_validation_attempts a JOIN work_task_claims c
          ON c.task_id = a.task_id AND c.claim_id = NEW.claim_id
          AND c.evidence_id = a.evidence_id AND c.definition_revision = a.definition_revision
          AND c.completion_policy_revision = a.completion_policy_revision AND c.manager_generation = a.manager_generation
        JOIN work_task_accepted_definitions d ON d.task_id = a.task_id AND d.definition_revision = a.definition_revision
        JOIN work_task_manager_bindings m ON m.task_id = a.task_id AND m.generation = a.manager_generation AND m.thread_id = c.manager_thread_id
        WHERE a.attempt_id = NEW.attempt_id AND a.task_id = NEW.task_id
          AND typeof(a.definition_revision) = 'integer' AND a.definition_revision BETWEEN 1 AND 9007199254740991
          AND typeof(a.completion_policy_revision) = 'integer' AND a.completion_policy_revision BETWEEN 1 AND 9007199254740991
          AND typeof(a.manager_generation) = 'integer' AND a.manager_generation BETWEEN 1 AND 9007199254740991
          AND NOT EXISTS (
            WITH RECURSIVE strings(value) AS (
              SELECT NEW.attempt_id UNION ALL SELECT NEW.task_id UNION ALL SELECT NEW.claim_id
              UNION ALL SELECT a.evidence_id UNION ALL SELECT a.work_watermark
            ), units(bytes, position, count) AS (
              SELECT hex(CAST(trim(value, ${whitespace}) AS BLOB)), 1, 0 FROM strings
              UNION ALL SELECT bytes, position + 2, count + CASE
                WHEN substr(bytes, position, 1) IN ('8', '9', 'A', 'B') THEN 0
                WHEN substr(bytes, position, 1) = 'F' THEN 2 ELSE 1 END
              FROM units WHERE position <= length(bytes) AND count <= 2000
            ) SELECT 1 FROM units WHERE count > 2000
          ) AND NOT EXISTS (
            SELECT 1 FROM (SELECT NEW.attempt_id AS value UNION ALL SELECT NEW.task_id UNION ALL SELECT NEW.claim_id
              UNION ALL SELECT a.evidence_id UNION ALL SELECT a.work_watermark)
            WHERE typeof(value) != 'text' OR length(CAST(trim(value, ${whitespace}) AS BLOB)) = 0
          )
      ) BEGIN SELECT RAISE(ABORT, 'invalid validation identity'); END`);
  }
  yield* sql`
    CREATE TRIGGER work_task_validation_identity_attempt_update BEFORE UPDATE ON work_task_validation_attempts
    WHEN EXISTS (SELECT 1 FROM work_task_validation_identity i WHERE i.attempt_id = OLD.attempt_id)
      AND (NEW.attempt_id IS NOT OLD.attempt_id OR NEW.task_id IS NOT OLD.task_id
        OR NEW.definition_revision IS NOT OLD.definition_revision OR NEW.manager_generation IS NOT OLD.manager_generation
        OR NEW.completion_policy_revision IS NOT OLD.completion_policy_revision OR NEW.evidence_id IS NOT OLD.evidence_id
        OR NEW.work_watermark IS NOT OLD.work_watermark)
    BEGIN SELECT RAISE(ABORT, 'validation identity in use'); END
  `;
  yield* sql`
    CREATE TRIGGER work_task_validation_identity_claim_update BEFORE UPDATE ON work_task_claims
    WHEN EXISTS (SELECT 1 FROM work_task_validation_identity i WHERE i.task_id = OLD.task_id AND i.claim_id = OLD.claim_id)
      AND (NEW.task_id IS NOT OLD.task_id OR NEW.claim_id IS NOT OLD.claim_id OR NEW.evidence_id IS NOT OLD.evidence_id
        OR NEW.definition_revision IS NOT OLD.definition_revision OR NEW.manager_generation IS NOT OLD.manager_generation
        OR NEW.completion_policy_revision IS NOT OLD.completion_policy_revision)
    BEGIN SELECT RAISE(ABORT, 'validation claim in use'); END
  `;
  yield* sql`
    CREATE TRIGGER work_task_validation_identity_manager_update BEFORE UPDATE ON work_task_manager_bindings
    WHEN EXISTS (SELECT 1 FROM work_task_validation_identity i
      JOIN work_task_claims c ON c.task_id = i.task_id AND c.claim_id = i.claim_id
      WHERE c.task_id = OLD.task_id AND c.manager_generation = OLD.generation)
      AND (NEW.task_id IS NOT OLD.task_id OR NEW.generation IS NOT OLD.generation OR NEW.thread_id IS NOT OLD.thread_id)
    BEGIN SELECT RAISE(ABORT, 'validation manager in use'); END
  `;
  yield* sql`CREATE INDEX work_task_validation_state_history_idx ON work_task_validation_attempts(task_id, state, created_at, attempt_id)`;
});
