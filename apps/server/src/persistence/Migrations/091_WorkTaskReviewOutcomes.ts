import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const whitespace =
    "char(9,10,11,12,13,32,160,5760,8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,8232,8233,8239,8287,12288,65279)";

  yield* sql`CREATE TABLE work_task_review_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL,
    review_id TEXT NOT NULL REFERENCES work_task_action_reviews(review_id),
    sequence INTEGER NOT NULL CHECK (typeof(sequence) = 'integer' AND sequence BETWEEN 0 AND 9007199254740991),
    occurred_at TEXT NOT NULL,
    command_id TEXT,
    event_json TEXT NOT NULL CHECK (CASE WHEN json_valid(event_json) THEN json_type(event_json) IS 'object' ELSE 0 END),
    UNIQUE (task_id, sequence)
  )`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_review_events_validate BEFORE INSERT ON work_task_review_events
    WHEN CASE WHEN json_valid(NEW.event_json) THEN NOT (
      json_extract(NEW.event_json, '$.id') IS NEW.event_id
      AND json_extract(NEW.event_json, '$.taskId') IS NEW.task_id
      AND json_extract(NEW.event_json, '$.occurredAt') IS NEW.occurred_at
      AND json_extract(NEW.event_json, '$.attribution.type') IS 'server'
      AND json_type(NEW.event_json, '$.definitionRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.definitionRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.actionPolicyRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.actionPolicyRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.lifecycleRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.lifecycleRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.completionPolicyRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.completionPolicyRevision') BETWEEN 1 AND 9007199254740991
      AND NOT EXISTS (
        WITH RECURSIVE strings(value) AS (
          SELECT NEW.event_id UNION ALL SELECT NEW.task_id UNION ALL SELECT NEW.review_id
          UNION ALL SELECT NEW.command_id WHERE NEW.command_id IS NOT NULL
          UNION ALL SELECT r.manager_thread_id FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
          UNION ALL SELECT r.provider_session_id FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
          UNION ALL SELECT r.run_id FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
          UNION ALL SELECT r.runtime_request_id FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
          UNION ALL SELECT r.rule_id FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
          UNION ALL SELECT r.action FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
          UNION ALL SELECT r.label FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
          UNION ALL SELECT r.target FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
          UNION ALL SELECT r.fingerprint FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
          UNION ALL SELECT json_extract(NEW.event_json, '$.review.continuationId') WHERE json_type(NEW.event_json, '$.review.continuationId') IS 'text'
          UNION ALL SELECT json_extract(NEW.event_json, '$.human.subject') WHERE json_type(NEW.event_json, '$.human.subject') IS 'text'
          UNION ALL SELECT json_extract(NEW.event_json, '$.human.sessionId') WHERE json_type(NEW.event_json, '$.human.sessionId') IS 'text'
          UNION ALL SELECT json_extract(NEW.event_json, '$.feedback') WHERE json_type(NEW.event_json, '$.feedback') IS 'text'
        ), units(bytes, position, count) AS (
          SELECT hex(CAST(trim(value, ${whitespace}) AS BLOB)), 1, 0 FROM strings
          UNION ALL SELECT bytes, position + 2, count + CASE WHEN substr(bytes, position, 1) IN ('8','9','A','B') THEN 0 WHEN substr(bytes, position, 1) = 'F' THEN 2 ELSE 1 END FROM units WHERE position <= length(bytes) AND count <= 2000
        ) SELECT 1 FROM units WHERE (count = 0 AND position = 1 AND length(bytes) = 0) OR count > 2000
      )
      AND length(CAST(NEW.occurred_at AS BLOB)) BETWEEN 1 AND 2000
      AND EXISTS (SELECT 1 FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id AND r.task_id = NEW.task_id
        AND r.revision BETWEEN 1 AND 9007199254740991
        AND r.definition_revision BETWEEN 1 AND 9007199254740991
        AND r.action_policy_revision BETWEEN 1 AND 9007199254740991
        AND r.manager_generation BETWEEN 1 AND 9007199254740991
        AND instr(r.action, '*') = 0 AND instr(r.target, '*') = 0
        AND NOT EXISTS (
          WITH RECURSIVE units(bytes, position, count) AS (
            SELECT hex(CAST(trim(r.action, ${whitespace}) AS BLOB)), 1, 0
            UNION ALL SELECT bytes, position + 2, count + CASE WHEN substr(bytes, position, 1) IN ('8','9','A','B') THEN 0 WHEN substr(bytes, position, 1) = 'F' THEN 2 ELSE 1 END FROM units WHERE position <= length(bytes) AND count <= 120
          ) SELECT 1 FROM units WHERE count > 120)
        AND CASE json_extract(NEW.event_json, '$.type')
          WHEN 'task.action-review-requested' THEN NEW.command_id IS r.command_id
            AND json_type(NEW.event_json, '$.review.reviewId') IS 'text'
            AND json_type(NEW.event_json, '$.review.taskId') IS 'text'
            AND json_type(NEW.event_json, '$.review.runtimeRequestId') IS 'text'
            AND json_type(NEW.event_json, '$.review.managerThreadId') IS 'text'
            AND json_type(NEW.event_json, '$.review.providerSessionId') IS 'text'
            AND json_type(NEW.event_json, '$.review.runId') IS 'text'
            AND json_type(NEW.event_json, '$.review.ruleId') IS 'text'
            AND json_type(NEW.event_json, '$.review.action') IS 'text'
            AND json_type(NEW.event_json, '$.review.label') IS 'text'
            AND json_type(NEW.event_json, '$.review.target') IS 'text'
            AND json_type(NEW.event_json, '$.review.fingerprint') IS 'text'
            AND json_type(NEW.event_json, '$.review.createdAt') IS 'text'
            AND json_type(NEW.event_json, '$.review.revision') IS 'integer'
            AND json_type(NEW.event_json, '$.review.managerGeneration') IS 'integer'
            AND json_type(NEW.event_json, '$.review.definitionRevision') IS 'integer'
            AND json_type(NEW.event_json, '$.review.actionPolicyRevision') IS 'integer'
            AND json_type(NEW.event_json, '$.review.createdSequence') IS 'integer'
            AND json_extract(NEW.event_json, '$.review.reviewId') IS r.review_id
            AND json_extract(NEW.event_json, '$.review.taskId') IS r.task_id
            AND json_extract(NEW.event_json, '$.review.runtimeRequestId') IS r.runtime_request_id
            AND (json_type(NEW.event_json, '$.review.continuationId') IS NULL OR json_type(NEW.event_json, '$.review.continuationId') IS 'text')
            AND json_extract(NEW.event_json, '$.review.revision') IS r.revision
            AND json_extract(NEW.event_json, '$.review.managerGeneration') IS r.manager_generation
            AND json_extract(NEW.event_json, '$.review.managerThreadId') IS r.manager_thread_id
            AND json_extract(NEW.event_json, '$.review.providerSessionId') IS r.provider_session_id
            AND json_extract(NEW.event_json, '$.review.runId') IS r.run_id
            AND json_extract(NEW.event_json, '$.review.ruleId') IS r.rule_id
            AND json_extract(NEW.event_json, '$.review.definitionRevision') IS r.definition_revision
            AND json_extract(NEW.event_json, '$.review.actionPolicyRevision') IS r.action_policy_revision
            AND json_extract(NEW.event_json, '$.definitionRevision') IS r.definition_revision
            AND json_extract(NEW.event_json, '$.actionPolicyRevision') IS r.action_policy_revision
            AND json_extract(NEW.event_json, '$.review.action') IS r.action
            AND json_extract(NEW.event_json, '$.review.label') IS r.label
            AND json_extract(NEW.event_json, '$.review.target') IS r.target
            AND json_extract(NEW.event_json, '$.review.fingerprint') IS r.fingerprint
            AND json_extract(NEW.event_json, '$.review.state.type') IS 'pending'
            AND NOT EXISTS (SELECT fullkey, type, atom FROM json_tree(NEW.event_json, '$.review.preview')
              EXCEPT SELECT '$.review.preview' || substr(fullkey, 2), type, atom FROM json_tree(r.preview_json))
            AND NOT EXISTS (SELECT '$.review.preview' || substr(fullkey, 2), type, atom FROM json_tree(r.preview_json)
              EXCEPT SELECT fullkey, type, atom FROM json_tree(NEW.event_json, '$.review.preview'))
            AND json_extract(NEW.event_json, '$.review.createdSequence') IS r.created_sequence
            AND json_extract(NEW.event_json, '$.review.createdAt') IS r.created_at
            AND NEW.sequence IS r.created_sequence
          WHEN 'task.action-review-result-recorded' THEN
            json_type(NEW.event_json, '$.reviewId') IS 'text' AND
            json_type(NEW.event_json, '$.managerThreadId') IS 'text' AND
            json_type(NEW.event_json, '$.runId') IS 'text' AND
            json_type(NEW.event_json, '$.payloadIdentity.action') IS 'text' AND
            json_type(NEW.event_json, '$.payloadIdentity.target') IS 'text' AND
            json_type(NEW.event_json, '$.payloadIdentity.fingerprint') IS 'text' AND
            json_type(NEW.event_json, '$.decision') IS 'text' AND
            json_type(NEW.event_json, '$.human.subject') IS 'text' AND
            json_type(NEW.event_json, '$.human.sessionId') IS 'text' AND
            json_type(NEW.event_json, '$.reviewRevision') IS 'integer' AND
            json_type(NEW.event_json, '$.managerGeneration') IS 'integer' AND
            EXISTS (SELECT 1 FROM work_task_action_review_decisions d WHERE d.review_id = r.review_id
              AND json_extract(NEW.event_json, '$.reviewId') IS d.review_id
              AND json_extract(NEW.event_json, '$.reviewRevision') IS d.review_revision
              AND json_extract(NEW.event_json, '$.managerGeneration') IS r.manager_generation
              AND json_extract(NEW.event_json, '$.managerThreadId') IS r.manager_thread_id
              AND json_extract(NEW.event_json, '$.runId') IS r.run_id
              AND json_extract(NEW.event_json, '$.payloadIdentity.action') IS r.action
              AND json_extract(NEW.event_json, '$.payloadIdentity.target') IS r.target
              AND json_extract(NEW.event_json, '$.payloadIdentity.fingerprint') IS r.fingerprint
              AND json_extract(NEW.event_json, '$.decision') IS d.decision
              AND json_extract(NEW.event_json, '$.human.subject') IS d.human_subject
              AND json_extract(NEW.event_json, '$.human.sessionId') IS d.human_session_id
              AND (json_type(NEW.event_json, '$.feedback') IS NULL OR json_type(NEW.event_json, '$.feedback') IS 'text')
              AND json_extract(NEW.event_json, '$.feedback') IS d.feedback
              AND NEW.sequence IS d.resolved_sequence)
          ELSE 0 END)
    ) IS TRUE ELSE 1 END BEGIN SELECT RAISE(ABORT, 'invalid review event'); END`);
  yield* sql`CREATE TRIGGER work_task_review_events_no_replace BEFORE INSERT ON work_task_review_events
    WHEN EXISTS (SELECT 1 FROM work_task_review_events WHERE event_id = NEW.event_id OR (task_id = NEW.task_id AND sequence = NEW.sequence)
      OR (review_id = NEW.review_id AND json_extract(event_json, '$.type') = json_extract(NEW.event_json, '$.type')))
    BEGIN SELECT RAISE(ABORT, 'duplicate review event'); END`;
  yield* sql`CREATE TRIGGER work_task_review_events_no_update BEFORE UPDATE ON work_task_review_events BEGIN SELECT RAISE(ABORT, 'immutable review event'); END`;
  yield* sql`CREATE TRIGGER work_task_review_events_no_delete BEFORE DELETE ON work_task_review_events BEGIN SELECT RAISE(ABORT, 'immutable review event'); END`;
  yield* sql`CREATE UNIQUE INDEX work_task_review_events_kind_idx ON work_task_review_events(review_id, json_extract(event_json, '$.type'))`;
  yield* sql`CREATE INDEX work_task_review_events_history_idx ON work_task_review_events(task_id, sequence DESC, review_id DESC)`;

  yield* sql`CREATE TABLE work_task_review_outcomes (
    event_id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL,
    review_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (typeof(sequence) = 'integer' AND sequence BETWEEN 0 AND 9007199254740991),
    occurred_at TEXT NOT NULL,
    command_id TEXT,
    event_json TEXT NOT NULL CHECK (CASE WHEN json_valid(event_json) THEN json_type(event_json) IS 'object' ELSE 0 END),
    FOREIGN KEY (review_id) REFERENCES work_task_action_reviews(review_id),
    UNIQUE (task_id, sequence)
  )`;
  // Keep the complete event for exact replay, including revisions that can differ on invalidation.
  yield* sql.unsafe(`CREATE TRIGGER work_task_review_outcomes_validate BEFORE INSERT ON work_task_review_outcomes
    WHEN CASE WHEN json_valid(NEW.event_json) THEN NOT (
      json_type(NEW.event_json) IS 'object'
      AND json_type(NEW.event_json, '$.id') IS 'text'
      AND json_type(NEW.event_json, '$.taskId') IS 'text'
      AND json_type(NEW.event_json, '$.occurredAt') IS 'text'
      AND json_extract(NEW.event_json, '$.id') IS NEW.event_id
      AND json_extract(NEW.event_json, '$.taskId') IS NEW.task_id
      AND json_extract(NEW.event_json, '$.occurredAt') IS NEW.occurred_at
      AND json_extract(NEW.event_json, '$.attribution.type') IS 'server'
      AND json_type(NEW.event_json, '$.binding') IS 'object'
      AND json_type(NEW.event_json, '$.binding.taskId') IS 'text'
      AND json_type(NEW.event_json, '$.binding.reviewId') IS 'text'
      AND json_type(NEW.event_json, '$.binding.ruleId') IS 'text'
      AND json_type(NEW.event_json, '$.binding.runtimeRequestId') IS 'text'
      AND json_type(NEW.event_json, '$.binding.providerSessionId') IS 'text'
      AND json_type(NEW.event_json, '$.binding.managerThreadId') IS 'text'
      AND json_type(NEW.event_json, '$.binding.runId') IS 'text'
      AND json_type(NEW.event_json, '$.binding.payloadIdentity.action') IS 'text'
      AND json_type(NEW.event_json, '$.binding.payloadIdentity.target') IS 'text'
      AND json_type(NEW.event_json, '$.binding.payloadIdentity.fingerprint') IS 'text'
      AND EXISTS (SELECT 1 FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id
        AND r.task_id IS NEW.task_id
        AND json_extract(NEW.event_json, '$.binding.reviewId') IS r.review_id
        AND json_extract(NEW.event_json, '$.binding.taskId') IS r.task_id
        AND json_extract(NEW.event_json, '$.binding.reviewRevision') IS r.revision
        AND json_extract(NEW.event_json, '$.binding.definitionRevision') IS r.definition_revision
        AND json_extract(NEW.event_json, '$.binding.actionPolicyRevision') IS r.action_policy_revision
        AND json_extract(NEW.event_json, '$.binding.managerGeneration') IS r.manager_generation
        AND json_extract(NEW.event_json, '$.binding.managerThreadId') IS r.manager_thread_id
        AND json_extract(NEW.event_json, '$.binding.providerSessionId') IS r.provider_session_id
        AND json_extract(NEW.event_json, '$.binding.runtimeRequestId') IS r.runtime_request_id
        AND json_extract(NEW.event_json, '$.binding.runId') IS r.run_id
        AND json_extract(NEW.event_json, '$.binding.ruleId') IS r.rule_id
        AND json_extract(NEW.event_json, '$.binding.payloadIdentity.action') IS r.action
        AND json_extract(NEW.event_json, '$.binding.payloadIdentity.target') IS r.target
        AND json_extract(NEW.event_json, '$.binding.payloadIdentity.fingerprint') IS r.fingerprint)
      AND json_type(NEW.event_json, '$.definitionRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.definitionRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.actionPolicyRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.actionPolicyRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.lifecycleRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.lifecycleRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.completionPolicyRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.completionPolicyRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.reason') IS 'text'
      AND instr(json_extract(NEW.event_json, '$.binding.payloadIdentity.action'), '*') = 0
      AND instr(json_extract(NEW.event_json, '$.binding.payloadIdentity.target'), '*') = 0
      AND NOT EXISTS (
        WITH RECURSIVE units(bytes, position, count) AS (
          SELECT hex(CAST(trim(json_extract(NEW.event_json, '$.binding.payloadIdentity.action'), ${whitespace}) AS BLOB)), 1, 0
          UNION ALL SELECT bytes, position + 2, count + CASE WHEN substr(bytes, position, 1) IN ('8','9','A','B') THEN 0 WHEN substr(bytes, position, 1) = 'F' THEN 2 ELSE 1 END FROM units WHERE position <= length(bytes) AND count <= 120
        ) SELECT 1 FROM units WHERE count > 120
      )
      AND json_type(NEW.event_json, '$.binding.reviewRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.binding.reviewRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.binding.definitionRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.binding.definitionRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.binding.actionPolicyRevision') IS 'integer'
      AND json_extract(NEW.event_json, '$.binding.actionPolicyRevision') BETWEEN 1 AND 9007199254740991
      AND json_type(NEW.event_json, '$.binding.managerGeneration') IS 'integer'
      AND json_extract(NEW.event_json, '$.binding.managerGeneration') BETWEEN 1 AND 9007199254740991
      AND CASE json_extract(NEW.event_json, '$.type')
        WHEN 'task.action-review-invalidated' THEN json_extract(NEW.event_json, '$.cause') IN ('payload_changed', 'policy_changed', 'generation_changed', 'run_changed', 'authorization_changed')
        WHEN 'task.action-review-cancelled' THEN json_extract(NEW.event_json, '$.source.type') IS 'task.archive'
          AND json_type(NEW.event_json, '$.source.commandId') IS 'text'
          AND json_extract(NEW.event_json, '$.source.commandId') IS NEW.command_id
          AND json_type(NEW.event_json, '$.source.human.subject') IS 'text'
          AND json_type(NEW.event_json, '$.source.human.sessionId') IS 'text'
          AND json_extract(NEW.event_json, '$.definitionRevision') IS json_extract(NEW.event_json, '$.binding.definitionRevision')
          AND json_extract(NEW.event_json, '$.actionPolicyRevision') IS json_extract(NEW.event_json, '$.binding.actionPolicyRevision')
        WHEN 'task.action-review-continuation-released' THEN
          json_extract(NEW.event_json, '$.definitionRevision') IS json_extract(NEW.event_json, '$.binding.definitionRevision')
          AND json_extract(NEW.event_json, '$.actionPolicyRevision') IS json_extract(NEW.event_json, '$.binding.actionPolicyRevision')
          AND json_type(NEW.event_json, '$.decisionEventId') IS 'text'
          AND json_type(NEW.event_json, '$.continuationId') IS 'text'
          AND json_type(NEW.event_json, '$.decision') IS 'text'
          AND json_type(NEW.event_json, '$.purpose') IS 'text'
          AND json_extract(NEW.event_json, '$.decisionEventId') IS NOT NEW.event_id
          AND NOT EXISTS (SELECT 1 FROM work_task_review_events e
            WHERE e.event_id = json_extract(NEW.event_json, '$.decisionEventId')
              AND (e.review_id IS NOT NEW.review_id OR json_extract(e.event_json, '$.type') IS NOT 'task.action-review-result-recorded'))
          AND NOT EXISTS (SELECT 1 FROM work_task_review_outcomes o
            WHERE o.event_id = json_extract(NEW.event_json, '$.decisionEventId'))
          AND NOT EXISTS (SELECT 1 FROM work_task_review_events e WHERE e.review_id = NEW.review_id
            AND json_extract(e.event_json, '$.type') = 'task.action-review-result-recorded'
            AND e.event_id IS NOT json_extract(NEW.event_json, '$.decisionEventId'))
          AND EXISTS (SELECT 1 FROM work_task_action_review_releases l JOIN work_task_action_review_decisions d ON d.review_id = l.review_id
            WHERE l.review_id = NEW.review_id AND l.task_id = NEW.task_id
            AND l.continuation_id IS json_extract(NEW.event_json, '$.continuationId')
            AND d.decision IS json_extract(NEW.event_json, '$.decision')
            AND ((d.decision = 'approve' AND json_extract(NEW.event_json, '$.purpose') = 'approved_action')
              OR (d.decision IN ('reject', 'request_changes') AND json_extract(NEW.event_json, '$.purpose') = 'feedback')))
        ELSE 0 END
      AND NOT EXISTS (
        WITH RECURSIVE strings(value) AS (
          SELECT NEW.event_id UNION ALL SELECT NEW.task_id UNION ALL SELECT NEW.review_id
          UNION ALL SELECT NEW.command_id WHERE NEW.command_id IS NOT NULL
          UNION ALL SELECT json_extract(NEW.event_json, '$.reason')
          UNION ALL SELECT json_extract(NEW.event_json, '$.binding.ruleId')
          UNION ALL SELECT json_extract(NEW.event_json, '$.binding.runtimeRequestId')
          UNION ALL SELECT json_extract(NEW.event_json, '$.binding.providerSessionId')
          UNION ALL SELECT json_extract(NEW.event_json, '$.binding.managerThreadId')
          UNION ALL SELECT json_extract(NEW.event_json, '$.binding.runId')
          UNION ALL SELECT json_extract(NEW.event_json, '$.binding.payloadIdentity.action')
          UNION ALL SELECT json_extract(NEW.event_json, '$.binding.payloadIdentity.target')
          UNION ALL SELECT json_extract(NEW.event_json, '$.binding.payloadIdentity.fingerprint')
          UNION ALL SELECT json_extract(NEW.event_json, '$.source.human.subject') WHERE json_extract(NEW.event_json, '$.type') = 'task.action-review-cancelled'
          UNION ALL SELECT json_extract(NEW.event_json, '$.source.human.sessionId') WHERE json_extract(NEW.event_json, '$.type') = 'task.action-review-cancelled'
          UNION ALL SELECT json_extract(NEW.event_json, '$.continuationId') WHERE json_extract(NEW.event_json, '$.type') = 'task.action-review-continuation-released'
          UNION ALL SELECT json_extract(NEW.event_json, '$.decisionEventId') WHERE json_extract(NEW.event_json, '$.type') = 'task.action-review-continuation-released'
        ), units(bytes, position, count) AS (
          SELECT hex(CAST(trim(value, ${whitespace}) AS BLOB)), 1, 0 FROM strings
          UNION ALL SELECT bytes, position + 2, count + CASE WHEN substr(bytes, position, 1) IN ('8','9','A','B') THEN 0 WHEN substr(bytes, position, 1) = 'F' THEN 2 ELSE 1 END FROM units WHERE position <= length(bytes) AND count <= 2000
        ) SELECT 1 FROM units WHERE count = 0 AND position = 1 AND length(bytes) = 0 OR count > 2000
      )
      AND length(CAST(NEW.occurred_at AS BLOB)) BETWEEN 1 AND 2000
    ) IS TRUE ELSE 1 END BEGIN SELECT RAISE(ABORT, 'invalid review outcome'); END`);
  yield* sql`CREATE TRIGGER work_task_review_outcomes_no_replace BEFORE INSERT ON work_task_review_outcomes
    WHEN EXISTS (SELECT 1 FROM work_task_review_outcomes WHERE event_id = NEW.event_id OR (task_id = NEW.task_id AND sequence = NEW.sequence))
    BEGIN SELECT RAISE(ABORT, 'duplicate review outcome'); END`;
  yield* sql`CREATE TRIGGER work_task_review_outcomes_event_collision BEFORE INSERT ON work_task_review_outcomes
    WHEN EXISTS (SELECT 1 FROM work_task_review_events WHERE event_id = NEW.event_id OR (task_id = NEW.task_id AND sequence = NEW.sequence))
    BEGIN SELECT RAISE(ABORT, 'duplicate review event'); END`;
  yield* sql`CREATE TRIGGER work_task_review_events_decision_binding BEFORE INSERT ON work_task_review_events
    WHEN json_extract(NEW.event_json, '$.type') = 'task.action-review-result-recorded'
      AND EXISTS (SELECT 1 FROM work_task_review_outcomes o WHERE o.review_id = NEW.review_id
        AND json_extract(o.event_json, '$.type') = 'task.action-review-continuation-released'
        AND json_extract(o.event_json, '$.decisionEventId') IS NOT NEW.event_id)
    BEGIN SELECT RAISE(ABORT, 'review decision event conflict'); END`;
  yield* sql`CREATE TRIGGER work_task_review_events_outcome_collision BEFORE INSERT ON work_task_review_events
    WHEN EXISTS (SELECT 1 FROM work_task_review_outcomes WHERE event_id = NEW.event_id OR (task_id = NEW.task_id AND sequence = NEW.sequence))
    BEGIN SELECT RAISE(ABORT, 'duplicate review event'); END`;
  yield* sql`CREATE UNIQUE INDEX work_task_review_release_event_review_idx ON work_task_review_outcomes(review_id)
    WHERE json_extract(event_json, '$.type') = 'task.action-review-continuation-released'`;
  yield* sql`CREATE UNIQUE INDEX work_task_review_release_event_decision_idx ON work_task_review_outcomes(json_extract(event_json, '$.decisionEventId'))
    WHERE json_extract(event_json, '$.type') = 'task.action-review-continuation-released'`;
  yield* sql`CREATE TRIGGER work_task_review_release_event_no_replace BEFORE INSERT ON work_task_review_outcomes
    WHEN json_extract(NEW.event_json, '$.type') = 'task.action-review-continuation-released'
      AND EXISTS (SELECT 1 FROM work_task_review_outcomes o
        WHERE json_extract(o.event_json, '$.type') = 'task.action-review-continuation-released'
          AND (o.review_id = NEW.review_id OR json_extract(o.event_json, '$.decisionEventId') = json_extract(NEW.event_json, '$.decisionEventId')))
    BEGIN SELECT RAISE(ABORT, 'duplicate review release event'); END`;
  yield* sql`CREATE TRIGGER work_task_review_outcomes_referenced_identity BEFORE INSERT ON work_task_review_outcomes
    WHEN EXISTS (SELECT 1 FROM work_task_review_outcomes o
      WHERE json_extract(o.event_json, '$.type') = 'task.action-review-continuation-released'
        AND json_extract(o.event_json, '$.decisionEventId') = NEW.event_id)
    BEGIN SELECT RAISE(ABORT, 'review decision event conflict'); END`;
  yield* sql`CREATE TRIGGER work_task_review_events_referenced_identity BEFORE INSERT ON work_task_review_events
    WHEN EXISTS (SELECT 1 FROM work_task_review_outcomes o
      WHERE json_extract(o.event_json, '$.type') = 'task.action-review-continuation-released'
        AND json_extract(o.event_json, '$.decisionEventId') = NEW.event_id
        AND (o.review_id IS NOT NEW.review_id OR json_extract(NEW.event_json, '$.type') IS NOT 'task.action-review-result-recorded'))
    BEGIN SELECT RAISE(ABORT, 'review decision event conflict'); END`;
  yield* sql`CREATE TRIGGER work_task_review_outcomes_no_update BEFORE UPDATE ON work_task_review_outcomes BEGIN SELECT RAISE(ABORT, 'immutable review outcome'); END`;
  yield* sql`CREATE TRIGGER work_task_review_outcomes_no_delete BEFORE DELETE ON work_task_review_outcomes BEGIN SELECT RAISE(ABORT, 'immutable review outcome'); END`;
  yield* sql`CREATE INDEX work_task_review_outcomes_history_idx ON work_task_review_outcomes(task_id, sequence DESC, review_id DESC)`;

  yield* sql`CREATE TABLE work_task_review_status (
    review_id TEXT PRIMARY KEY NOT NULL REFERENCES work_task_action_reviews(review_id),
    task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
    state TEXT NOT NULL CHECK (state IN ('pending', 'approved', 'rejected', 'changes_requested', 'invalidated', 'cancelled')),
    sequence INTEGER NOT NULL CHECK (typeof(sequence) = 'integer' AND sequence BETWEEN 0 AND 9007199254740991)
  )`;
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER work_task_review_status_${operation.toLowerCase()} BEFORE ${operation} ON work_task_review_status
      WHEN NOT EXISTS (SELECT 1 FROM work_task_action_reviews r WHERE r.review_id = NEW.review_id AND r.task_id = NEW.task_id
        AND ((NEW.state = 'pending' AND NEW.sequence = r.created_sequence)
          OR (NEW.state IN ('approved', 'rejected', 'changes_requested') AND EXISTS (
            SELECT 1 FROM work_task_action_review_decisions d WHERE d.review_id = r.review_id
              AND d.resolved_sequence = NEW.sequence AND NEW.state = CASE d.decision
                WHEN 'approve' THEN 'approved' WHEN 'reject' THEN 'rejected' ELSE 'changes_requested' END))
          OR (NEW.state IN ('invalidated', 'cancelled') AND EXISTS (
            SELECT 1 FROM work_task_review_outcomes o WHERE o.review_id = r.review_id AND o.sequence = NEW.sequence
              AND NEW.state = CASE json_extract(o.event_json, '$.type')
                WHEN 'task.action-review-invalidated' THEN 'invalidated'
                WHEN 'task.action-review-cancelled' THEN 'cancelled' END))))
      BEGIN SELECT RAISE(ABORT, 'invalid review status binding'); END`);
  }
  yield* sql`CREATE TRIGGER work_task_review_status_no_replace BEFORE INSERT ON work_task_review_status
    WHEN EXISTS (SELECT 1 FROM work_task_review_status WHERE review_id = NEW.review_id)
    BEGIN SELECT RAISE(ABORT, 'duplicate review status'); END`;
  yield* sql`CREATE TRIGGER work_task_review_status_no_collision BEFORE UPDATE ON work_task_review_status
    WHEN NEW.review_id IS NOT OLD.review_id OR NEW.task_id IS NOT OLD.task_id
      OR EXISTS (SELECT 1 FROM work_task_review_status WHERE review_id = NEW.review_id AND review_id IS NOT OLD.review_id)
    BEGIN SELECT RAISE(ABORT, 'duplicate review status'); END`;
  yield* sql`CREATE INDEX work_task_review_status_page_idx ON work_task_review_status(task_id, state, sequence DESC, review_id DESC)`;
  yield* sql`CREATE INDEX work_task_reviews_history_idx ON work_task_action_reviews(task_id, created_sequence DESC, review_id DESC)`;
});
