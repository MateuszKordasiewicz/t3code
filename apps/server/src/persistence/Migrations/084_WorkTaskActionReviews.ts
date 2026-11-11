import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE UNIQUE INDEX work_task_definition_policy_binding_idx ON work_task_accepted_definitions(task_id, definition_revision, action_policy_revision)`;
  yield* sql`CREATE UNIQUE INDEX work_task_manager_thread_binding_idx ON work_task_manager_bindings(task_id, generation, thread_id)`;
  yield* sql`
    CREATE TABLE work_task_action_reviews (
      review_id TEXT PRIMARY KEY NOT NULL,
      task_id TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK (typeof(revision) = 'integer' AND revision > 0),
      manager_generation INTEGER NOT NULL CHECK (typeof(manager_generation) = 'integer' AND manager_generation > 0),
      manager_thread_id TEXT NOT NULL,
      provider_session_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      command_id TEXT NOT NULL UNIQUE,
      runtime_request_id TEXT NOT NULL UNIQUE,
      rule_id TEXT NOT NULL,
      definition_revision INTEGER NOT NULL CHECK (typeof(definition_revision) = 'integer' AND definition_revision > 0),
      action_policy_revision INTEGER NOT NULL CHECK (typeof(action_policy_revision) = 'integer' AND action_policy_revision > 0),
      action TEXT NOT NULL,
      label TEXT NOT NULL,
      target TEXT NOT NULL,
      preview_json TEXT NOT NULL CHECK (json_valid(preview_json) AND length(CAST(preview_json AS BLOB)) <= 32768
        AND json_type(preview_json, '$.content') IS 'text' AND length(trim(json_extract(preview_json, '$.content'))) > 0
        AND json_type(preview_json, '$.artifacts') IS 'array' AND json_array_length(preview_json, '$.artifacts') <= 16),
      fingerprint TEXT NOT NULL CHECK (length(trim(fingerprint)) > 0),
      created_sequence INTEGER NOT NULL CHECK (typeof(created_sequence) = 'integer' AND created_sequence >= 0),
      created_at TEXT NOT NULL,
      FOREIGN KEY (task_id, definition_revision, action_policy_revision) REFERENCES work_task_accepted_definitions(task_id, definition_revision, action_policy_revision),
      FOREIGN KEY (task_id, manager_generation, manager_thread_id) REFERENCES work_task_manager_bindings(task_id, generation, thread_id),
      UNIQUE (review_id, task_id, revision, action_policy_revision, fingerprint, manager_generation)
    )
  `;
  yield* sql`
    CREATE TABLE work_task_action_review_decisions (
      review_id TEXT PRIMARY KEY NOT NULL,
      task_id TEXT NOT NULL,
      review_revision INTEGER NOT NULL,
      action_policy_revision INTEGER NOT NULL,
      fingerprint TEXT NOT NULL,
      manager_generation INTEGER NOT NULL,
      decision TEXT NOT NULL CHECK (decision IN ('approve', 'reject', 'request_changes')),
      human_subject TEXT NOT NULL CHECK (length(trim(human_subject)) > 0),
      human_session_id TEXT NOT NULL CHECK (length(trim(human_session_id)) > 0),
      feedback TEXT,
      resolved_sequence INTEGER NOT NULL CHECK (typeof(resolved_sequence) = 'integer' AND resolved_sequence >= 0),
      resolved_at TEXT NOT NULL,
      FOREIGN KEY (review_id, task_id, review_revision, action_policy_revision, fingerprint, manager_generation)
        REFERENCES work_task_action_reviews(review_id, task_id, revision, action_policy_revision, fingerprint, manager_generation),
      UNIQUE (review_id, task_id, review_revision, action_policy_revision, fingerprint, manager_generation)
    )
  `;
  yield* sql`
    CREATE TABLE work_task_action_review_releases (
      review_id TEXT PRIMARY KEY NOT NULL,
      task_id TEXT NOT NULL,
      review_revision INTEGER NOT NULL,
      action_policy_revision INTEGER NOT NULL,
      fingerprint TEXT NOT NULL,
      manager_generation INTEGER NOT NULL,
      continuation_id TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL,
      FOREIGN KEY (review_id, task_id, review_revision, action_policy_revision, fingerprint, manager_generation)
        REFERENCES work_task_action_review_decisions(review_id, task_id, review_revision, action_policy_revision, fingerprint, manager_generation)
    )
  `;
  // Count UTF-16 units from UTF-8 bytes so NUL and supplementary characters match the contract.
  yield* sql`CREATE TRIGGER work_task_action_reviews_artifacts_insert BEFORE INSERT ON work_task_action_reviews
    WHEN EXISTS (SELECT 1 FROM json_each(NEW.preview_json, '$.artifacts')
      WHERE type != 'object' OR json_type(value, '$.uri') IS NOT 'text' OR json_type(value, '$.hash') IS NOT 'text'
        OR length(trim(json_extract(value, '$.uri'))) = 0 OR length(trim(json_extract(value, '$.hash'))) = 0
        OR EXISTS (
          WITH RECURSIVE units(bytes, position, count) AS (
            SELECT hex(CAST(json_extract(value, '$.uri') AS BLOB)), 1, 0
            UNION ALL SELECT hex(CAST(json_extract(value, '$.hash') AS BLOB)), 1, 0
            UNION ALL SELECT bytes, position + 2, count + CASE
              WHEN substr(bytes, position, 1) IN ('8', '9', 'A', 'B') THEN 0
              WHEN substr(bytes, position, 1) = 'F' THEN 2 ELSE 1 END
              FROM units WHERE position <= length(bytes) AND count <= 2000
          ) SELECT 1 FROM units WHERE count > 2000
        ))
    BEGIN SELECT RAISE(ABORT, 'invalid artifact'); END`;
  yield* sql`CREATE INDEX work_task_action_reviews_task_idx ON work_task_action_reviews(task_id, created_sequence)`;
  yield* sql`CREATE TRIGGER work_task_action_reviews_no_replace BEFORE INSERT ON work_task_action_reviews
    WHEN EXISTS (SELECT 1 FROM work_task_action_reviews WHERE review_id = NEW.review_id OR command_id = NEW.command_id OR runtime_request_id = NEW.runtime_request_id)
    BEGIN SELECT RAISE(ABORT, 'duplicate review'); END`;
  yield* sql`CREATE TRIGGER work_task_action_decisions_no_replace BEFORE INSERT ON work_task_action_review_decisions
    WHEN EXISTS (SELECT 1 FROM work_task_action_review_decisions WHERE review_id = NEW.review_id)
    BEGIN SELECT RAISE(ABORT, 'duplicate decision'); END`;
  yield* sql`CREATE TRIGGER work_task_action_releases_no_replace BEFORE INSERT ON work_task_action_review_releases
    WHEN EXISTS (SELECT 1 FROM work_task_action_review_releases WHERE review_id = NEW.review_id OR continuation_id = NEW.continuation_id)
    BEGIN SELECT RAISE(ABORT, 'duplicate release'); END`;
  yield* sql`CREATE TRIGGER work_task_action_reviews_immutable_update BEFORE UPDATE ON work_task_action_reviews BEGIN SELECT RAISE(ABORT, 'immutable review'); END`;
  yield* sql`CREATE TRIGGER work_task_action_reviews_immutable_delete BEFORE DELETE ON work_task_action_reviews BEGIN SELECT RAISE(ABORT, 'immutable review'); END`;
  yield* sql`CREATE TRIGGER work_task_action_decisions_immutable_update BEFORE UPDATE ON work_task_action_review_decisions BEGIN SELECT RAISE(ABORT, 'immutable decision'); END`;
  yield* sql`CREATE TRIGGER work_task_action_decisions_immutable_delete BEFORE DELETE ON work_task_action_review_decisions BEGIN SELECT RAISE(ABORT, 'immutable decision'); END`;
  yield* sql`CREATE TRIGGER work_task_action_releases_immutable_update BEFORE UPDATE ON work_task_action_review_releases BEGIN SELECT RAISE(ABORT, 'immutable release'); END`;
  yield* sql`CREATE TRIGGER work_task_action_releases_immutable_delete BEFORE DELETE ON work_task_action_review_releases BEGIN SELECT RAISE(ABORT, 'immutable release'); END`;
});
