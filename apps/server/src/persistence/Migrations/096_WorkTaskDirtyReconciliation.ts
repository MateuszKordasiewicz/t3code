import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import {
  canonicalEvidenceJsonSql,
  structuralJsonEqualSql,
  validTimestampSql,
} from "../workTaskSqlV1.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const get = (path: string, json = "NEW.event_json") => `json_extract(${json},'${path}')`;
  const kind = (path: string, type: string, json = "NEW.event_json") =>
    `json_type(${json},'${path}') IS '${type}'`;
  const object = (path: string, count: number, json = "NEW.event_json") =>
    `(${kind(path, "object", json)} AND (SELECT count(*) FROM json_each(${json},'${path}'))=${count})`;
  const integer = (path: string, min = 0, json = "NEW.event_json") =>
    `(${kind(path, "integer", json)} AND ${get(path, json)} BETWEEN ${min} AND 9007199254740991)`;
  const ref = (path: string, key: string, json = "NEW.event_json") =>
    `(${object(path, 2, json)} AND ${kind(path + ".environmentId", "text", json)} AND ${kind(path + "." + key, "text", json)})`;
  const column = (name: string, path: string, type = "TEXT") =>
    `${name} ${type} GENERATED ALWAYS AS (json_extract(event_json,'${path}')) STORED`;
  const common = `event_id TEXT PRIMARY KEY NOT NULL, sequence INTEGER NOT NULL UNIQUE CHECK(typeof(sequence)='integer' AND sequence BETWEEN 0 AND 9007199254740991), event_json TEXT NOT NULL CHECK(json_valid(event_json)),
    ${column("environment_id", "$.environmentId")}, ${column("kind", "$.type")}`;
  yield* sql.unsafe(`CREATE TABLE work_task_dirty_events (
    ${common}, ${column("task_id", "$.task.taskId")}, ${column("source_id", "$.sourceEventId")}, ${column("source_sequence", "$.sourceSequence", "INTEGER")},
    ${column("dirty_generation", "$.dirtyGeneration", "INTEGER")}, ${column("acknowledged_generation", "$.acknowledgedGeneration", "INTEGER")},
    ${column("membership_generation", "$.membership.membershipGeneration", "INTEGER")}, ${column("group_id", "$.membership.group.groupId")},
    source_task_id TEXT GENERATED ALWAYS AS(CASE WHEN kind='task.dirty-source-recorded' THEN task_id END) STORED,
    FOREIGN KEY(task_id) REFERENCES work_task_projections(task_id),
    FOREIGN KEY(task_id,membership_generation) REFERENCES work_task_group_memberships(task_id,membership_generation),
    UNIQUE(environment_id,source_task_id,source_id,source_sequence)
  ) WITHOUT ROWID`);
  yield* sql`CREATE UNIQUE INDEX work_task_dirty_source_idx ON work_task_dirty_events(task_id,source_id) WHERE kind='task.dirty-source-recorded'`;
  yield* sql`CREATE UNIQUE INDEX work_task_dirty_generation_idx ON work_task_dirty_events(task_id,dirty_generation) WHERE kind='task.dirty-source-recorded'`;
  yield* sql`CREATE INDEX work_task_dirty_latest_idx ON work_task_dirty_events(task_id,sequence DESC)`;
  yield* sql.unsafe(`CREATE TABLE work_task_reconciliation_events (
    ${common}, ${column("scan_kind", "$.scan.kind")},
    target_id TEXT GENERATED ALWAYS AS(CASE WHEN scan_kind='graph_dependents' THEN json_extract(event_json,'$.scan.blocker.taskId') ELSE json_extract(event_json,'$.scan.group.groupId') END) STORED,
    ${column("source_id", "$.scan.sourceEventId")}, ${column("source_sequence", "$.scan.sourceSequence", "INTEGER")},
    ${column("captured_revision", "$.scan.capturedRevision", "INTEGER")}, ${column("observed_revision", "$.observedRevision", "INTEGER")}, ${column("completed", "$.completed", "INTEGER")},
    job_key TEXT GENERATED ALWAYS AS(json_array(scan_kind,environment_id,target_id,source_id)) STORED,
    blocker_id TEXT GENERATED ALWAYS AS(CASE WHEN scan_kind='graph_dependents' THEN target_id END) STORED,
    group_id TEXT GENERATED ALWAYS AS(CASE WHEN scan_kind='group_members' THEN target_id END) STORED,
    FOREIGN KEY(blocker_id) REFERENCES work_task_projections(task_id), FOREIGN KEY(environment_id,group_id) REFERENCES work_task_manager_groups(environment_id,group_id)
  ) WITHOUT ROWID`);
  yield* sql`CREATE INDEX work_task_reconciliation_latest_idx ON work_task_reconciliation_events(job_key,sequence DESC)`;
  yield* sql`CREATE TABLE work_task_reconciliation_visits (
    event_id TEXT NOT NULL REFERENCES work_task_reconciliation_events(event_id), environment_id TEXT NOT NULL,
    task_id TEXT NOT NULL, source_id TEXT NOT NULL, source_sequence INTEGER NOT NULL, membership_generation INTEGER,
    PRIMARY KEY(event_id,task_id),
    FOREIGN KEY(environment_id,task_id,source_id,source_sequence) REFERENCES work_task_dirty_events(environment_id,source_task_id,source_id,source_sequence) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(task_id,membership_generation) REFERENCES work_task_group_memberships(task_id,membership_generation)
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_reconciliation_dirty_idx ON work_task_reconciliation_visits(environment_id,task_id,source_id,source_sequence)`;
  yield* sql`CREATE INDEX work_task_reconciliation_membership_idx ON work_task_reconciliation_visits(task_id,membership_generation)`;
  const tables = ["work_task_dirty_events", "work_task_reconciliation_events"];
  const base = `(${canonicalEvidenceJsonSql("NEW.event_json")} AND ${get("$.id")} IS NEW.event_id
    AND ${kind("$.id", "text")} AND ${kind("$.environmentId", "text")} AND ${kind("$.commandId", "text")} AND ${validTimestampSql(get("$.occurredAt"))}
    AND ${object("$.attribution", 1)} AND ${get("$.attribution.type")} IS 'server'
    AND NEW.source_id IS NOT NEW.event_id AND NEW.source_sequence<NEW.sequence)`;
  for (const table of tables) {
    yield* sql.unsafe(`CREATE INDEX ${table}_source_id_idx ON ${table}(source_id)`);
    yield* sql.unsafe(`CREATE INDEX ${table}_source_sequence_idx ON ${table}(source_sequence)`);
    for (const operation of ["UPDATE", "DELETE"])
      yield* sql.unsafe(
        `CREATE TRIGGER ${table}_${operation.toLowerCase()} BEFORE ${operation} ON ${table} BEGIN SELECT RAISE(ABORT,'immutable dirty reconciliation history'); END`,
      );
    yield* sql.unsafe(`CREATE TRIGGER ${table}_identity BEFORE INSERT ON ${table} BEGIN
      SELECT CASE WHEN (${base}) IS NOT TRUE THEN RAISE(ABORT,'invalid coordination context') END;
      ${tables
        .map(
          (
            other,
          ) => `SELECT CASE WHEN EXISTS(SELECT 1 FROM ${other} WHERE event_id=NEW.event_id OR sequence=NEW.sequence)
        OR EXISTS(SELECT 1 FROM ${other} WHERE event_id=NEW.source_id AND sequence<>NEW.source_sequence)
        OR EXISTS(SELECT 1 FROM ${other} WHERE sequence=NEW.source_sequence AND event_id<>NEW.source_id)
        OR EXISTS(SELECT 1 FROM ${other} WHERE source_id=NEW.source_id AND source_sequence<>NEW.source_sequence)
        OR EXISTS(SELECT 1 FROM ${other} WHERE source_sequence=NEW.source_sequence AND source_id<>NEW.source_id)
        OR EXISTS(SELECT 1 FROM ${other} WHERE source_id=NEW.event_id AND source_sequence<>NEW.sequence)
        OR EXISTS(SELECT 1 FROM ${other} WHERE source_sequence=NEW.sequence AND source_id<>NEW.event_id)
        THEN RAISE(ABORT,'coordination identity conflict') END;`,
        )
        .join("\n")}
    END`);
  }
  const latestDirty = `(SELECT event_json FROM work_task_dirty_events WHERE task_id=NEW.task_id ORDER BY sequence DESC LIMIT 1)`;
  const membership = `(${kind("$.membership", "null")} OR (${object("$.membership", 2)} AND ${ref("$.membership.group", "groupId")}
    AND ${integer("$.membership.membershipGeneration", 1)} AND ${get("$.membership.group.environmentId")} IS NEW.environment_id
    AND EXISTS(SELECT 1 FROM work_task_group_memberships m WHERE m.task_id=NEW.task_id AND m.membership_generation=NEW.membership_generation AND m.environment_id=NEW.environment_id AND m.group_id=NEW.group_id)))`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_dirty_valid BEFORE INSERT ON work_task_dirty_events BEGIN
    SELECT CASE WHEN (${ref("$.task", "taskId")} AND ${get("$.task.environmentId")} IS NEW.environment_id AND ${membership}
      AND ${kind("$.sourceEventId", "text")} AND ${integer("$.sourceSequence")} AND ${integer("$.dirtyGeneration")} AND ${integer("$.acknowledgedGeneration")}
      AND CASE NEW.kind WHEN 'task.dirty-source-recorded' THEN ${object("$", 13)} AND ${integer("$.previousDirtyGeneration")}
        AND NEW.dirty_generation=${get("$.previousDirtyGeneration")}+1 AND NEW.acknowledged_generation<=${get("$.previousDirtyGeneration")}
        AND ${get("$.previousDirtyGeneration")}=coalesce(json_extract(${latestDirty},'$.dirtyGeneration'),0)
        AND NEW.acknowledged_generation=coalesce(json_extract(${latestDirty},'$.acknowledgedGeneration'),0)
      WHEN 'task.dirty-acknowledged' THEN ${object("$", 14)} AND ${integer("$.previousAcknowledgedGeneration")} AND ${integer("$.observedDirtyGeneration")}
        AND ${get("$.previousAcknowledgedGeneration")}<=NEW.acknowledged_generation AND NEW.acknowledged_generation<=${get("$.observedDirtyGeneration")} AND ${get("$.observedDirtyGeneration")}<=NEW.dirty_generation
        AND ${get("$.previousAcknowledgedGeneration")}=coalesce(json_extract(${latestDirty},'$.acknowledgedGeneration'),0)
        AND NEW.dirty_generation=coalesce(json_extract(${latestDirty},'$.dirtyGeneration'),0)
      ELSE 0 END) IS NOT TRUE THEN RAISE(ABORT,'dirty state conflict') END;
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_dirty_events WHERE task_id=NEW.task_id AND sequence=(SELECT max(sequence) FROM work_task_dirty_events WHERE task_id=NEW.task_id)
      AND (environment_id<>NEW.environment_id OR sequence>=NEW.sequence)) THEN RAISE(ABORT,'dirty task context conflict') END;
    SELECT CASE WHEN NEW.kind='task.dirty-source-recorded' AND EXISTS(SELECT 1 FROM work_task_dirty_events WHERE task_id=NEW.task_id AND source_id=NEW.source_id AND kind='task.dirty-source-recorded') THEN RAISE(ABORT,'duplicate dirty source') END;
  END`);
  const task = (json: string) =>
    `CASE NEW.scan_kind WHEN 'graph_dependents' THEN json_extract(${json},'$.taskId') ELSE json_extract(${json},'$.task.taskId') END`;
  const position = (
    path: string,
  ) => `(${kind(path, "null")} OR CASE NEW.scan_kind WHEN 'graph_dependents' THEN ${object(path, 2)} AND ${kind(path + ".dependentTaskId", "text")} AND ${kind(path + ".edgeId", "text")} AND ${get(path + ".dependentTaskId")}<>NEW.target_id
    ELSE ${object(path, 1)} AND ${kind(path + ".taskId", "text")} END)`;
  const previousTask = `CASE NEW.scan_kind WHEN 'graph_dependents' THEN ${get("$.previousPosition.dependentTaskId")} ELSE ${get("$.previousPosition.taskId")} END`;
  const positionTask = `CASE NEW.scan_kind WHEN 'graph_dependents' THEN ${get("$.position.dependentTaskId")} ELSE ${get("$.position.taskId")} END`;
  const latestScan = `(SELECT event_json FROM work_task_reconciliation_events WHERE job_key=NEW.job_key ORDER BY sequence DESC LIMIT 1)`;
  const same = (a: string, b: string) =>
    structuralJsonEqualSql(`coalesce(${a},'null')`, `coalesce(${b},'null')`);
  yield* sql.unsafe(`CREATE TRIGGER work_task_reconciliation_valid BEFORE INSERT ON work_task_reconciliation_events BEGIN
    SELECT CASE WHEN (${object("$", 12)} AND NEW.kind IS 'task.reconciliation-progressed' AND ${object("$.scan", 5)}
      AND ${kind("$.scan.sourceEventId", "text")} AND ${integer("$.scan.sourceSequence")} AND ${integer("$.scan.capturedRevision")}
      AND ${integer("$.observedRevision")} AND NEW.observed_revision>=NEW.captured_revision AND json_type(NEW.event_json,'$.completed') IN ('true','false')
      AND ${kind("$.visited", "array")} AND json_array_length(NEW.event_json,'$.visited')<=100
      AND CASE NEW.scan_kind WHEN 'graph_dependents' THEN ${ref("$.scan.blocker", "taskId")} AND ${get("$.scan.blocker.environmentId")} IS NEW.environment_id
        WHEN 'group_members' THEN ${ref("$.scan.group", "groupId")} AND ${get("$.scan.group.environmentId")} IS NEW.environment_id ELSE 0 END
      AND ${position("$.previousPosition")} AND ${position("$.position")}
      AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.visited') v WHERE NOT (CASE NEW.scan_kind WHEN 'graph_dependents' THEN ${ref("$", "taskId", "v.value")} AND json_extract(v.value,'$.environmentId') IS NEW.environment_id AND json_extract(v.value,'$.taskId')<>NEW.target_id
        ELSE ${object("$", 2, "v.value")} AND ${ref("$.task", "taskId", "v.value")} AND json_extract(v.value,'$.task.environmentId') IS NEW.environment_id AND ${integer("$.membershipGeneration", 1, "v.value")} END))
      AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.visited') v JOIN json_each(NEW.event_json,'$.visited') w ON w.key=v.key+1 WHERE ${task("v.value")}>=${task("w.value")} COLLATE BINARY)
      AND CASE WHEN json_array_length(NEW.event_json,'$.visited')=0 THEN NEW.completed=1 AND ${same(get("$.previousPosition"), get("$.position"))}
      ELSE ${get("$.position")} IS NOT NULL AND ${positionTask} IS ${task("json_extract(NEW.event_json,'$.visited[#-1]')")}
        AND (${get("$.previousPosition")} IS NULL OR CASE NEW.scan_kind WHEN 'graph_dependents' THEN
          (${positionTask},${get("$.position.edgeId")})>(${previousTask},${get("$.previousPosition.edgeId")}) AND ${task("json_extract(NEW.event_json,'$.visited[0]')")}>=(${previousTask})
          ELSE ${task("json_extract(NEW.event_json,'$.visited[0]')")}>(${previousTask}) END) END)
      IS NOT TRUE THEN RAISE(ABORT,'invalid reconciliation traversal') END;
    SELECT CASE WHEN NOT (${same(get("$.previousPosition"), `json_extract(${latestScan},'$.position')`)})
      OR EXISTS(SELECT 1 FROM work_task_reconciliation_events WHERE job_key=NEW.job_key AND sequence=(SELECT max(sequence) FROM work_task_reconciliation_events WHERE job_key=NEW.job_key)
        AND (completed=1 OR sequence>=NEW.sequence OR observed_revision>NEW.observed_revision OR captured_revision<>NEW.captured_revision OR source_sequence<>NEW.source_sequence))
      THEN RAISE(ABORT,'reconciliation cursor conflict') END;
  END`);
  for (const operation of ["UPDATE", "DELETE"])
    yield* sql.unsafe(
      `CREATE TRIGGER work_task_reconciliation_visits_${operation.toLowerCase()} BEFORE ${operation} ON work_task_reconciliation_visits BEGIN SELECT RAISE(ABORT,'immutable reconciliation obligation'); END`,
    );
  yield* sql`CREATE TRIGGER work_task_reconciliation_visits_insert BEFORE INSERT ON work_task_reconciliation_visits BEGIN
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_reconciliation_visits WHERE event_id=NEW.event_id AND task_id=NEW.task_id)
      OR NOT EXISTS(SELECT 1 FROM work_task_reconciliation_events p,json_each(p.event_json,'$.visited') v WHERE p.event_id=NEW.event_id
        AND p.environment_id=NEW.environment_id AND p.source_id=NEW.source_id AND p.source_sequence=NEW.source_sequence
        AND CASE p.scan_kind WHEN 'graph_dependents' THEN json_extract(v.value,'$.taskId')=NEW.task_id AND NEW.membership_generation IS NULL
          ELSE json_extract(v.value,'$.task.taskId')=NEW.task_id AND json_extract(v.value,'$.membershipGeneration') IS NEW.membership_generation
            AND EXISTS(SELECT 1 FROM work_task_group_memberships m WHERE m.task_id=NEW.task_id AND m.membership_generation=NEW.membership_generation AND m.environment_id=p.environment_id AND m.group_id=p.group_id) END)
      THEN RAISE(ABORT,'invalid reconciliation obligation') END;
  END`;
  yield* sql`CREATE TRIGGER work_task_reconciliation_visits_create AFTER INSERT ON work_task_reconciliation_events BEGIN
    INSERT INTO work_task_reconciliation_visits(event_id,environment_id,task_id,source_id,source_sequence,membership_generation)
      SELECT NEW.event_id,NEW.environment_id,CASE NEW.scan_kind WHEN 'graph_dependents' THEN json_extract(value,'$.taskId') ELSE json_extract(value,'$.task.taskId') END,
        NEW.source_id,NEW.source_sequence,CASE NEW.scan_kind WHEN 'group_members' THEN json_extract(value,'$.membershipGeneration') END
      FROM json_each(NEW.event_json,'$.visited');
  END`;
});
