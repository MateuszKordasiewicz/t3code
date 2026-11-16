import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import {
  canonicalEvidenceJsonSql,
  structuralJsonEqualSql,
  validTimestampSql,
} from "../workTaskSqlV1.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const get = (path: string, json = "NEW.event_json") => `json_extract(${json},'$.${path}')`;
  const kind = (path: string, type: string) => `json_type(NEW.event_json,'$.${path}') IS '${type}'`;
  const integer = (path: string) =>
    `(${kind(path, "integer")} AND ${get(path)} BETWEEN 0 AND 9007199254740991)`;
  const shape = (path: string, count: number) =>
    `(${kind(path, "object")} AND (SELECT count(*) FROM json_each(NEW.event_json,'$.${path}'))=${count})`;
  const columns: Record<string, readonly [string, string, boolean?]> = {
    event_id: ["id", "TEXT"],
    run_id: ["ownership.run.runId", "TEXT"],
    environment_id: ["environmentId", "TEXT"],
    task_id: ["ownership.run.task.taskId", "TEXT"],
    group_id: ["ownership.run.group.groupId", "TEXT"],
    manager_thread_id: ["ownership.run.manager.threadId", "TEXT"],
    kind: ["type", "TEXT"],
    command_id: ["commandId", "TEXT"],
    occurred_at: ["occurredAt", "TEXT"],
    source_event_id: ["sourceEventId", "TEXT", true],
    source_sequence: ["sourceSequence", "INTEGER", true],
    observed_sequence: ["observedSequence", "INTEGER", true],
    terminal_event_id: ["terminationEvidence.terminalRunEvent.eventId", "TEXT", true],
    terminal_sequence: ["terminationEvidence.terminalRunEvent.sequence", "INTEGER", true],
    terminal_status: ["terminationEvidence.terminalStatus", "TEXT", true],
    settlement_command_id: ["terminationEvidence.settlementCommandId", "TEXT", true],
  };
  yield* sql.unsafe(`CREATE TABLE work_task_run_ownership_observations (
    ${Object.entries(columns)
      .map(([name, [, type, nullable]]) => `${name} ${type}${nullable ? "" : " NOT NULL"}`)
      .join(",")},
    sequence INTEGER NOT NULL UNIQUE,event_json TEXT NOT NULL CHECK(json_valid(event_json)),PRIMARY KEY(event_id),
    FOREIGN KEY(run_id) REFERENCES work_task_accepted_runs(run_id)
  ) WITHOUT ROWID`);
  yield* sql`CREATE INDEX work_task_run_observation_latest_idx ON work_task_run_ownership_observations(run_id,sequence DESC,event_id)`;
  yield* sql`CREATE INDEX work_task_run_observation_task_idx ON work_task_run_ownership_observations(task_id,sequence DESC,event_id)`;
  yield* sql`CREATE UNIQUE INDEX work_task_run_observation_terminal_idx ON work_task_run_ownership_observations(run_id) WHERE kind='task.run-ownership-terminated'`;
  yield* sql`CREATE UNIQUE INDEX work_task_run_observation_terminal_event_idx ON work_task_run_ownership_observations(terminal_event_id) WHERE terminal_event_id IS NOT NULL`;
  yield* sql`CREATE TABLE work_task_run_observation_references (
    owner_event_id TEXT NOT NULL REFERENCES work_task_run_ownership_observations(event_id),reference_role TEXT NOT NULL,position INTEGER NOT NULL,
    event_id TEXT NOT NULL,sequence INTEGER NOT NULL,known_kind TEXT,known_task_id TEXT,
    PRIMARY KEY(owner_event_id,reference_role,position)
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_run_observation_ref_id_idx ON work_task_run_observation_references(event_id,sequence)`;
  yield* sql`CREATE INDEX work_task_run_observation_ref_sequence_idx ON work_task_run_observation_references(sequence,event_id)`;
  for (const table of [
    "work_task_run_ownership_observations",
    "work_task_run_observation_references",
  ])
    for (const operation of ["UPDATE", "DELETE"])
      yield* sql.unsafe(
        `CREATE TRIGGER ${table}_${operation.toLowerCase()} BEFORE ${operation} ON ${table} BEGIN SELECT RAISE(ABORT,'immutable ownership observation'); END`,
      );
  const expectedReference = `json_object('run',json(${get("ownership.run", "a.event_json")}),
    'recorded',json(${get("ownership.recorded", "a.event_json")}),
    'nativeRunEvent',json(${get("ownership.nativeAcceptance.runEvent", "a.event_json")}),
    'origin',json(CASE a.origin_type WHEN 'initial_start' THEN ${get("ownership.origin", "a.event_json")}
      ELSE json_remove(${get("ownership.origin", "a.event_json")},'$.pending','$.observedDirtyGeneration','$.acknowledgedGeneration') END))`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_run_observation_valid BEFORE INSERT ON work_task_run_ownership_observations BEGIN
    SELECT CASE WHEN (${canonicalEvidenceJsonSql("NEW.event_json")} AND json_type(NEW.event_json) IS 'object'
      AND (SELECT count(*) FROM json_each(NEW.event_json))=CASE NEW.kind WHEN 'task.run-ownership-uncertain' THEN 10 WHEN 'task.run-ownership-terminated' THEN 9 ELSE -1 END
      AND ${shape("attribution", 1)} AND ${get("attribution.type")} IS 'server' AND ${shape("ownership", 4)}
      AND ${validTimestampSql("NEW.occurred_at")} AND typeof(NEW.sequence)='integer' AND NEW.sequence BETWEEN 0 AND 9007199254740991
      AND ${Object.entries(columns)
        .map(
          ([name, [path, type, nullable]]) =>
            `(NEW.${name} IS ${get(path)} AND ${nullable ? `(NEW.${name} IS NULL OR ` : ""}${type === "INTEGER" ? integer(path) : kind(path, "text")}${nullable ? ")" : ""})`,
        )
        .join(" AND ")}
      AND EXISTS(SELECT 1 FROM work_task_accepted_runs a WHERE a.run_id=NEW.run_id AND a.environment_id=NEW.environment_id
        AND NEW.sequence>a.recorded_sequence AND ${structuralJsonEqualSql(get("ownership"), expectedReference)})
    ) IS NOT TRUE THEN RAISE(ABORT,'invalid ownership observation') END;
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_run_ownership_observations WHERE event_id=NEW.event_id OR sequence=NEW.sequence OR terminal_event_id=NEW.terminal_event_id)
      OR EXISTS(SELECT 1 FROM work_task_run_ownership_observations WHERE run_id=NEW.run_id AND kind='task.run-ownership-terminated')
      OR NEW.sequence<=(SELECT sequence FROM work_task_run_ownership_observations WHERE run_id=NEW.run_id ORDER BY sequence DESC LIMIT 1)
      THEN RAISE(ABORT,'ownership observation identity or phase conflict') END;
  END`);
  const absent = (paths: readonly string[]) =>
    paths.map((path) => `json_type(NEW.event_json,'$.${path}') IS NULL`).join(" AND ");
  yield* sql.unsafe(`CREATE TRIGGER work_task_run_observation_mode BEFORE INSERT ON work_task_run_ownership_observations BEGIN
    SELECT CASE WHEN (CASE NEW.kind WHEN 'task.run-ownership-uncertain' THEN
      ${kind("sourceEventId", "text")} AND ${integer("sourceSequence")} AND NEW.source_sequence<NEW.sequence AND ${kind("reason", "text")}
      AND ${absent(["observedSequence", "terminationEvidence"])}
    WHEN 'task.run-ownership-terminated' THEN
      ${absent(["sourceEventId", "sourceSequence", "reason"])} AND ${integer("observedSequence")} AND NEW.observed_sequence<NEW.sequence
      AND ${shape("terminationEvidence", 4)} AND ${get("terminationEvidence.kind")} IS 'before_executable_claim'
      AND ${shape("terminationEvidence.terminalRunEvent", 3)} AND ${get("terminationEvidence.terminalRunEvent.type")} IS 'run.updated'
      AND ${kind("terminationEvidence.terminalRunEvent.eventId", "text")} AND ${integer("terminationEvidence.terminalRunEvent.sequence")}
      AND NEW.terminal_sequence>=(SELECT native_run_sequence FROM work_task_accepted_runs WHERE run_id=NEW.run_id)
      AND NEW.terminal_sequence<=NEW.observed_sequence AND NEW.terminal_status IN ('cancelled','failed','interrupted')
      AND ${kind("terminationEvidence.settlementCommandId", "text")}
    ELSE 0 END) IS NOT TRUE THEN RAISE(ABORT,'unsupported ownership observation phase') END;
  END`);
  const descriptors = (json: string, sequence: string) => {
    const task = get("ownership.run.task.taskId", json);
    return `SELECT 'recorded' role,0 position,${get("id", json)} event_id,${sequence} sequence,${get("type", json)} known_kind,${task} known_task_id
      UNION ALL SELECT 'ownership',0,${get("ownership.recorded.eventId", json)},${get("ownership.recorded.sequence", json)},'task.run-ownership-accepted',${task}
      UNION ALL SELECT 'native_run',0,${get("ownership.nativeRunEvent.eventId", json)},${get("ownership.nativeRunEvent.sequence", json)},'run.created',${task}
      UNION ALL SELECT 'origin',0,CASE ${get("ownership.origin.type", json)} WHEN 'initial_start' THEN ${get("ownership.origin.startEventId", json)} ELSE ${get("ownership.origin.debitEvent.eventId", json)} END,
        CASE ${get("ownership.origin.type", json)} WHEN 'initial_start' THEN ${get("ownership.origin.startSequence", json)} ELSE ${get("ownership.origin.debitEvent.sequence", json)} END,
        CASE ${get("ownership.origin.type", json)} WHEN 'automatic_followup' THEN 'task.allowance-debited' END,${task}
      UNION ALL SELECT 'source',0,${get("sourceEventId", json)},${get("sourceSequence", json)},NULL,NULL WHERE ${get("type", json)}='task.run-ownership-uncertain'
      UNION ALL SELECT 'terminal_run',0,${get("terminationEvidence.terminalRunEvent.eventId", json)},${get("terminationEvidence.terminalRunEvent.sequence", json)},'run.updated',${task} WHERE ${get("type", json)}='task.run-ownership-terminated'`;
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
  yield* sql.unsafe(`CREATE TRIGGER work_task_accepted_reference_observation BEFORE INSERT ON work_task_accepted_run_references BEGIN
    SELECT CASE WHEN ${conflicts("work_task_run_observation_references")} THEN RAISE(ABORT,'accepted observation reference conflict') END;
  END`);
  yield* sql.unsafe(`CREATE TRIGGER work_task_run_observation_references AFTER INSERT ON work_task_run_ownership_observations BEGIN
    INSERT INTO work_task_run_observation_references(owner_event_id,reference_role,position,event_id,sequence,known_kind,known_task_id)
      SELECT NEW.event_id,role,position,event_id,sequence,known_kind,known_task_id FROM (${descriptors("NEW.event_json", "NEW.sequence")});
  END`);
});
