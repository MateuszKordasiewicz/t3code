import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { canonicalTextSql } from "../workTaskSqlV1.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const get = (path: string, json = "NEW.event_json") => `json_extract(${json},'$.${path}')`;
  const kind = (path: string, type: string) => `json_type(NEW.event_json,'$.${path}') IS '${type}'`;
  const integer = (path: string) =>
    `(${kind(path, "integer")} AND ${get(path)} BETWEEN 0 AND 9007199254740991)`;
  const shape = (path: string, count: number) =>
    `(${kind(path, "object")} AND (SELECT count(*) FROM json_each(NEW.event_json,'$.${path}'))=${count})`;
  const absent = (paths: readonly string[]) =>
    paths.map((path) => `json_type(NEW.event_json,'$.${path}') IS NULL`).join(" AND ");
  yield* sql`CREATE TABLE work_task_run_ownership_release_bridges (
    observation_event_id TEXT NOT NULL PRIMARY KEY,work_claim_id TEXT NOT NULL,work_release_event_id TEXT NOT NULL,
    run_id TEXT NOT NULL,terminal_event_id TEXT NOT NULL,terminal_sequence INTEGER NOT NULL,
    FOREIGN KEY(observation_event_id) REFERENCES work_task_run_ownership_observations(event_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(work_release_event_id,work_claim_id,run_id,terminal_event_id,terminal_sequence)
      REFERENCES work_task_claim_releases(event_id,work_claim_id,run_id,terminal_event_id,terminal_sequence) DEFERRABLE INITIALLY DEFERRED
  ) WITHOUT ROWID`;
  yield* sql`ALTER TABLE work_task_run_ownership_observations ADD COLUMN after_release_companion_id TEXT REFERENCES work_task_run_ownership_release_bridges(observation_event_id) DEFERRABLE INITIALLY DEFERRED`;
  yield* sql`CREATE INDEX work_task_ownership_bridge_release_idx ON work_task_run_ownership_release_bridges(work_release_event_id)`;
  yield* sql`CREATE INDEX work_task_ownership_bridge_run_idx ON work_task_run_ownership_release_bridges(run_id)`;
  for (const operation of ["UPDATE", "DELETE"])
    yield* sql.unsafe(
      `CREATE TRIGGER work_task_ownership_bridge_${operation.toLowerCase()} BEFORE ${operation} ON work_task_run_ownership_release_bridges BEGIN SELECT RAISE(ABORT,'immutable ownership release bridge'); END`,
    );
  const tuple = {
    observation_event_id: "id",
    work_claim_id: "terminationEvidence.workClaimId",
    work_release_event_id: "terminationEvidence.workReleaseEvent.eventId",
    run_id: "ownership.run.runId",
    terminal_event_id: "terminationEvidence.terminalRunEvent.eventId",
    terminal_sequence: "terminationEvidence.terminalRunEvent.sequence",
  };
  const matches = (bridge: string, json: string) =>
    Object.entries(tuple)
      .map(([column, path]) => `${bridge}.${column} IS ${get(path, json)}`)
      .join(" AND ");
  const closed = (
    claim: string,
    release: string,
    run: string,
  ) => `EXISTS(SELECT 1 FROM work_task_executable_claims c JOIN work_task_claim_releases r
    ON r.event_id=c.release_event_id AND r.work_claim_id=c.work_claim_id AND r.run_id=c.run_id AND r.effect_id=c.effect_id
    WHERE c.work_claim_id=${claim} AND c.run_id=${run} AND r.event_id=${release}
      AND c.work_claim_id=(SELECT work_claim_id FROM work_task_executable_claims WHERE run_id=${run} ORDER BY accepted_sequence DESC LIMIT 1)
      AND NOT EXISTS(SELECT 1 FROM work_task_executable_claims u LEFT JOIN work_task_claim_releases ur
        ON ur.event_id=u.release_event_id AND ur.work_claim_id=u.work_claim_id AND ur.run_id=u.run_id AND ur.effect_id=u.effect_id
        WHERE u.run_id=${run} AND ur.event_id IS NULL))`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_ownership_bridge_valid BEFORE INSERT ON work_task_run_ownership_release_bridges BEGIN
    SELECT CASE WHEN (${Object.keys(tuple)
      .filter((k) => k !== "terminal_sequence")
      .map((k) => canonicalTextSql(`NEW.${k}`))
      .join(" AND ")}
      AND typeof(NEW.terminal_sequence)='integer' AND NEW.terminal_sequence BETWEEN 0 AND 9007199254740991
      AND ${closed("NEW.work_claim_id", "NEW.work_release_event_id", "NEW.run_id")}
      AND EXISTS(SELECT 1 FROM work_task_claim_releases r WHERE r.event_id=NEW.work_release_event_id AND r.terminal_event_id=NEW.terminal_event_id AND r.terminal_sequence=NEW.terminal_sequence)
    ) IS NOT TRUE THEN RAISE(ABORT,'invalid ownership release bridge') END;
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_run_ownership_release_bridges WHERE observation_event_id=NEW.observation_event_id)
      OR EXISTS(SELECT 1 FROM work_task_run_ownership_observations o WHERE o.event_id=NEW.observation_event_id AND
        (o.kind<>'task.run-ownership-terminated' OR json_extract(o.event_json,'$.terminationEvidence.kind') IS NOT 'after_executable_claim'
          OR o.after_release_companion_id IS NOT NEW.observation_event_id OR NOT(${matches("NEW", "o.event_json")})))
      THEN RAISE(ABORT,'ownership bridge role or identity conflict') END;
  END`);
  yield* sql`DROP TRIGGER work_task_owner_before_physical`;
  yield* sql`CREATE TRIGGER work_task_owner_before_physical BEFORE INSERT ON work_task_run_ownership_observations
    WHEN NEW.kind='task.run-ownership-terminated' AND json_extract(NEW.event_json,'$.terminationEvidence.kind')='before_executable_claim'
      AND EXISTS(SELECT 1 FROM work_task_executable_claims WHERE run_id=NEW.run_id)
    BEGIN SELECT RAISE(ABORT,'physical history requires after-claim closure'); END`;
  yield* sql`DROP TRIGGER work_task_run_observation_mode`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_run_observation_mode BEFORE INSERT ON work_task_run_ownership_observations BEGIN
    SELECT CASE WHEN (CASE NEW.kind WHEN 'task.run-ownership-uncertain' THEN
      ${kind("sourceEventId", "text")} AND ${integer("sourceSequence")} AND NEW.source_sequence<NEW.sequence AND ${kind("reason", "text")}
      AND ${absent(["observedSequence", "terminationEvidence"])}
    WHEN 'task.run-ownership-terminated' THEN
      ${absent(["sourceEventId", "sourceSequence", "reason"])} AND ${integer("observedSequence")} AND NEW.observed_sequence<NEW.sequence
      AND ${shape("terminationEvidence.terminalRunEvent", 3)} AND ${get("terminationEvidence.terminalRunEvent.type")} IS 'run.updated'
      AND ${kind("terminationEvidence.terminalRunEvent.eventId", "text")} AND ${integer("terminationEvidence.terminalRunEvent.sequence")}
      AND NEW.terminal_sequence>=(SELECT native_run_sequence FROM work_task_accepted_runs WHERE run_id=NEW.run_id)
      AND NEW.terminal_sequence<=NEW.observed_sequence
      AND CASE ${get("terminationEvidence.kind")} WHEN 'before_executable_claim' THEN
        ${shape("terminationEvidence", 4)} AND NEW.terminal_status IN ('cancelled','failed','interrupted') AND ${kind("terminationEvidence.settlementCommandId", "text")}
      WHEN 'after_executable_claim' THEN ${shape("terminationEvidence", 5)} AND ${kind("terminationEvidence.workClaimId", "text")}
        AND ${shape("terminationEvidence.workReleaseEvent", 3)} AND ${get("terminationEvidence.workReleaseEvent.type")} IS 'task-group.work-released'
        AND ${kind("terminationEvidence.workReleaseEvent.eventId", "text")} AND ${integer("terminationEvidence.workReleaseEvent.sequence")}
        AND ${absent(["terminationEvidence.settlementCommandId"])}
        AND ${closed(get("terminationEvidence.workClaimId"), get("terminationEvidence.workReleaseEvent.eventId"), "NEW.run_id")}
        AND EXISTS(SELECT 1 FROM work_task_claim_releases r WHERE r.event_id=${get("terminationEvidence.workReleaseEvent.eventId")}
          AND r.sequence=${get("terminationEvidence.workReleaseEvent.sequence")} AND r.terminal_event_id=NEW.terminal_event_id AND r.terminal_sequence=NEW.terminal_sequence
          AND json_extract(r.native_json,'$.run.status') IS NEW.terminal_status
          AND NEW.terminal_sequence<r.sequence AND r.sequence<=NEW.observed_sequence)
      ELSE 0 END
    ELSE 0 END) IS NOT TRUE THEN RAISE(ABORT,'unsupported ownership observation phase') END;
    SELECT CASE WHEN (CASE WHEN NEW.kind='task.run-ownership-terminated' AND ${get("terminationEvidence.kind")}='after_executable_claim' THEN
      NEW.after_release_companion_id IS NEW.event_id AND NOT EXISTS(SELECT 1 FROM work_task_run_ownership_release_bridges b WHERE b.observation_event_id=NEW.event_id AND NOT(${matches("b", "NEW.event_json")}))
      ELSE NEW.after_release_companion_id IS NULL AND NOT EXISTS(SELECT 1 FROM work_task_run_ownership_release_bridges WHERE observation_event_id=NEW.event_id) END) IS NOT TRUE
      THEN RAISE(ABORT,'ownership release companion mismatch') END;
  END`);
  yield* sql`DROP TRIGGER work_task_run_observation_reference_valid`;
  yield* sql`DROP TRIGGER work_task_run_observation_references`;
  const descriptors = (json: string, sequence: string) => {
    const task = get("ownership.run.task.taskId", json);
    return `SELECT 'recorded' role,0 position,${get("id", json)} event_id,${sequence} sequence,${get("type", json)} known_kind,${task} known_task_id
      UNION ALL SELECT 'ownership',0,${get("ownership.recorded.eventId", json)},${get("ownership.recorded.sequence", json)},'task.run-ownership-accepted',${task}
      UNION ALL SELECT 'native_run',0,${get("ownership.nativeRunEvent.eventId", json)},${get("ownership.nativeRunEvent.sequence", json)},'run.created',${task}
      UNION ALL SELECT 'origin',0,CASE ${get("ownership.origin.type", json)} WHEN 'initial_start' THEN ${get("ownership.origin.startEventId", json)} ELSE ${get("ownership.origin.debitEvent.eventId", json)} END,
        CASE ${get("ownership.origin.type", json)} WHEN 'initial_start' THEN ${get("ownership.origin.startSequence", json)} ELSE ${get("ownership.origin.debitEvent.sequence", json)} END,
        CASE ${get("ownership.origin.type", json)} WHEN 'automatic_followup' THEN 'task.allowance-debited' END,${task}
      UNION ALL SELECT 'source',0,${get("sourceEventId", json)},${get("sourceSequence", json)},NULL,NULL WHERE ${get("type", json)}='task.run-ownership-uncertain'
      UNION ALL SELECT 'terminal_run',0,${get("terminationEvidence.terminalRunEvent.eventId", json)},${get("terminationEvidence.terminalRunEvent.sequence", json)},'run.updated',${task} WHERE ${get("type", json)}='task.run-ownership-terminated'
      UNION ALL SELECT 'work_release',0,${get("terminationEvidence.workReleaseEvent.eventId", json)},${get("terminationEvidence.workReleaseEvent.sequence", json)},'task-group.work-released',${task} WHERE ${get("terminationEvidence.kind", json)}='after_executable_claim'`;
  };
  const conflicts = (
    table: string,
  ) => `EXISTS(SELECT 1 FROM ${table} r WHERE r.event_id=NEW.event_id AND
      (r.sequence<>NEW.sequence OR (r.known_kind IS NOT NULL AND NEW.known_kind IS NOT NULL AND r.known_kind<>NEW.known_kind)
      OR (r.known_task_id IS NOT NULL AND NEW.known_task_id IS NOT NULL AND r.known_task_id<>NEW.known_task_id)))
    OR EXISTS(SELECT 1 FROM ${table} r WHERE r.sequence=NEW.sequence AND r.event_id<>NEW.event_id)`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_run_observation_reference_valid BEFORE INSERT ON work_task_run_observation_references BEGIN
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM (${descriptors("(SELECT event_json FROM work_task_run_ownership_observations WHERE event_id=NEW.owner_event_id)", "(SELECT sequence FROM work_task_run_ownership_observations WHERE event_id=NEW.owner_event_id)")}) d
      WHERE d.role=NEW.reference_role AND d.position=NEW.position AND d.event_id IS NEW.event_id AND d.sequence IS NEW.sequence AND d.known_kind IS NEW.known_kind AND d.known_task_id IS NEW.known_task_id)
      OR EXISTS(SELECT 1 FROM work_task_run_observation_references WHERE owner_event_id=NEW.owner_event_id AND reference_role=NEW.reference_role AND position=NEW.position)
      THEN RAISE(ABORT,'invalid ownership observation reference') END;
    SELECT CASE WHEN ${conflicts("work_task_run_observation_references")} OR ${conflicts("work_task_accepted_run_references")}
      THEN RAISE(ABORT,'ownership observation reference conflict') END;
  END`);
  yield* sql.unsafe(`CREATE TRIGGER work_task_run_observation_references AFTER INSERT ON work_task_run_ownership_observations BEGIN
    INSERT INTO work_task_run_observation_references(owner_event_id,reference_role,position,event_id,sequence,known_kind,known_task_id)
      SELECT NEW.event_id,role,position,event_id,sequence,known_kind,known_task_id FROM (${descriptors("NEW.event_json", "NEW.sequence")});
  END`);
});
