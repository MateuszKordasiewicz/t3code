import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE work_task_manager_groups (
      environment_id TEXT NOT NULL,
      group_id TEXT NOT NULL,
      control_revision INTEGER NOT NULL CHECK(typeof(control_revision)='integer' AND control_revision BETWEEN 0 AND 9007199254740991),
      membership_revision INTEGER NOT NULL CHECK(typeof(membership_revision)='integer' AND membership_revision BETWEEN 0 AND 9007199254740991),
      highest_manager_generation INTEGER NOT NULL CHECK(typeof(highest_manager_generation)='integer' AND highest_manager_generation BETWEEN 0 AND 9007199254740991),
      current_manager_generation INTEGER,
      current_manager_thread_id TEXT,
      dispatch_hold TEXT NOT NULL CHECK(dispatch_hold IN ('none','paused','stopped')),
      created_event_id TEXT NOT NULL,
      created_sequence INTEGER NOT NULL CHECK(typeof(created_sequence)='integer' AND created_sequence BETWEEN 0 AND 9007199254740991),
      created_event_json TEXT NOT NULL CHECK(json_valid(created_event_json)),
      control_event_id TEXT NOT NULL,
      control_sequence INTEGER NOT NULL CHECK(typeof(control_sequence)='integer' AND control_sequence BETWEEN 0 AND 9007199254740991),
      control_event_json TEXT NOT NULL CHECK(json_valid(control_event_json)),
      PRIMARY KEY(environment_id,group_id),
      UNIQUE(environment_id,current_manager_thread_id),
      UNIQUE(environment_id,group_id,current_manager_generation,current_manager_thread_id),
      CHECK((current_manager_generation IS NULL AND current_manager_thread_id IS NULL) OR
        (current_manager_generation IS NOT NULL AND current_manager_thread_id IS NOT NULL AND typeof(current_manager_generation)='integer' AND current_manager_generation=highest_manager_generation AND current_manager_generation>0)),
      FOREIGN KEY(environment_id,group_id,current_manager_generation,current_manager_thread_id)
        REFERENCES work_task_group_managers(environment_id,group_id,current_generation,current_manager_thread_id) DEFERRABLE INITIALLY DEFERRED
    ) WITHOUT ROWID
  `;
  yield* sql`
    CREATE TABLE work_task_group_managers (
      environment_id TEXT NOT NULL,
      group_id TEXT NOT NULL,
      generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation BETWEEN 1 AND 9007199254740991),
      manager_thread_id TEXT NOT NULL,
      bound_event_id TEXT NOT NULL,
      bound_sequence INTEGER NOT NULL CHECK(typeof(bound_sequence)='integer' AND bound_sequence BETWEEN 0 AND 9007199254740991),
      bound_event_json TEXT NOT NULL CHECK(json_valid(bound_event_json)),
      retired_event_id TEXT,
      retired_sequence INTEGER,
      retired_event_json TEXT,
      current_generation INTEGER,
      current_manager_thread_id TEXT,
      PRIMARY KEY(environment_id,group_id,generation),
      UNIQUE(environment_id,group_id,generation,manager_thread_id),
      UNIQUE(environment_id,group_id,current_generation,current_manager_thread_id),
      FOREIGN KEY(environment_id,group_id) REFERENCES work_task_manager_groups(environment_id,group_id),
      FOREIGN KEY(environment_id,group_id,current_generation,current_manager_thread_id)
        REFERENCES work_task_manager_groups(environment_id,group_id,current_manager_generation,current_manager_thread_id) DEFERRABLE INITIALLY DEFERRED,
      CHECK((retired_event_id IS NULL AND retired_sequence IS NULL AND retired_event_json IS NULL AND current_generation IS NOT NULL AND current_manager_thread_id IS NOT NULL AND current_generation=generation AND current_manager_thread_id=manager_thread_id)
        OR (retired_event_id IS NOT NULL AND retired_sequence IS NOT NULL AND retired_event_json IS NOT NULL AND current_generation IS NULL AND current_manager_thread_id IS NULL
          AND typeof(retired_sequence)='integer' AND retired_sequence BETWEEN 0 AND 9007199254740991 AND retired_sequence>bound_sequence AND json_valid(retired_event_json)))
    ) WITHOUT ROWID
  `;
  yield* sql`CREATE INDEX work_task_group_manager_thread_history_idx ON work_task_group_managers(environment_id,manager_thread_id,group_id,generation)`;
  yield* sql`CREATE INDEX work_task_group_manager_current_idx ON work_task_group_managers(environment_id,group_id,current_generation) WHERE current_generation IS NOT NULL`;
  yield* sql`CREATE UNIQUE INDEX work_task_group_created_event_idx ON work_task_manager_groups(created_event_id)`;
  yield* sql`CREATE UNIQUE INDEX work_task_group_control_event_idx ON work_task_manager_groups(control_event_id)`;
  yield* sql`CREATE UNIQUE INDEX work_task_group_created_sequence_idx ON work_task_manager_groups(created_sequence)`;
  yield* sql`CREATE UNIQUE INDEX work_task_group_control_sequence_idx ON work_task_manager_groups(control_sequence)`;
  yield* sql`CREATE INDEX work_task_group_current_thread_idx ON work_task_manager_groups(current_manager_thread_id,environment_id,group_id) WHERE current_manager_thread_id IS NOT NULL`;
  yield* sql`CREATE INDEX work_task_group_bound_sequence_idx ON work_task_group_managers(bound_sequence)`;
  yield* sql`CREATE INDEX work_task_group_retired_sequence_idx ON work_task_group_managers(retired_sequence) WHERE retired_sequence IS NOT NULL`;
  yield* sql`CREATE INDEX work_task_group_bound_event_idx ON work_task_group_managers(bound_event_id,bound_sequence)`;
  yield* sql`CREATE INDEX work_task_group_retired_event_idx ON work_task_group_managers(retired_event_id,retired_sequence) WHERE retired_event_id IS NOT NULL`;
  const whitespace =
    "char(9,10,11,12,13,32,160,5760,8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,8232,8233,8239,8287,12288,65279)";
  const timestamp = (value: string) => {
    const zone = `(CASE WHEN substr(${value},-1)='Z' THEN 1 ELSE 6 END)`;
    const fraction = `substr(${value},20,length(${value})-19-${zone})`;
    return `(typeof(${value})='text' AND length(${value}) BETWEEN 20 AND 2000 AND length(${value})=length(CAST(${value} AS BLOB))
      AND substr(${value},1,4) NOT GLOB '*[^0-9]*' AND substr(${value},5,1)='-' AND substr(${value},8,1)='-'
      AND substr(${value},11,1)='T' AND substr(${value},14,1)=':' AND substr(${value},17,1)=':'
      AND substr(${value},6,2) NOT GLOB '*[^0-9]*' AND substr(${value},9,2) NOT GLOB '*[^0-9]*'
      AND substr(${value},12,2) NOT GLOB '*[^0-9]*' AND substr(${value},15,2) NOT GLOB '*[^0-9]*' AND substr(${value},18,2) NOT GLOB '*[^0-9]*'
      AND CAST(substr(${value},6,2) AS INTEGER) BETWEEN 1 AND 12
      AND CAST(substr(${value},9,2) AS INTEGER) BETWEEN 1 AND CASE CAST(substr(${value},6,2) AS INTEGER)
        WHEN 2 THEN CASE WHEN CAST(substr(${value},1,4) AS INTEGER)%4=0 AND (CAST(substr(${value},1,4) AS INTEGER)%100<>0 OR CAST(substr(${value},1,4) AS INTEGER)%400=0) THEN 29 ELSE 28 END
        WHEN 4 THEN 30 WHEN 6 THEN 30 WHEN 9 THEN 30 WHEN 11 THEN 30 ELSE 31 END
      AND CAST(substr(${value},12,2) AS INTEGER) BETWEEN 0 AND 23 AND CAST(substr(${value},15,2) AS INTEGER) BETWEEN 0 AND 59 AND CAST(substr(${value},18,2) AS INTEGER) BETWEEN 0 AND 59
      AND (substr(${value},-1)='Z' OR (length(${value})>=25 AND substr(${value},-6,1) IN ('+','-') AND substr(${value},-3,1)=':'
        AND substr(${value},-5,2) NOT GLOB '*[^0-9]*' AND substr(${value},-2,2) NOT GLOB '*[^0-9]*'
        AND CAST(substr(${value},-5,2) AS INTEGER) BETWEEN 0 AND 23 AND CAST(substr(${value},-2,2) AS INTEGER) BETWEEN 0 AND 59))
      AND (${fraction}='' OR (length(${fraction})>1 AND substr(${fraction},1,1)='.' AND substr(${fraction},2) NOT GLOB '*[^0-9]*'))) IS TRUE`;
  };

  const counter = (json: string, path: string) =>
    `(json_type(${json},'${path}') IS 'integer' AND json_extract(${json},'${path}') BETWEEN 0 AND 9007199254740991)`;
  const control = (
    json: string,
    path: string,
  ) => `(json_type(${json},'${path}') IS 'object' AND (SELECT count(*) FROM json_each(${json},'${path}'))=6
    AND json_type(${json},'${path}.group') IS 'object' AND (SELECT count(*) FROM json_each(${json},'${path}.group'))=2
    AND json_type(${json},'${path}.group.environmentId') IS 'text' AND json_type(${json},'${path}.group.groupId') IS 'text'
    AND ${counter(json, `${path}.controlRevision`)} AND ${counter(json, `${path}.membershipRevision`)} AND ${counter(json, `${path}.highestManagerGeneration`)}
    AND json_extract(${json},'${path}.dispatchHold') IN ('none','paused','stopped')
    AND (json_type(${json},'${path}.currentManager') IS 'null' OR (json_type(${json},'${path}.currentManager') IS 'object'
      AND (SELECT count(*) FROM json_each(${json},'${path}.currentManager'))=2 AND json_type(${json},'${path}.currentManager.threadId') IS 'text'
      AND ${counter(json, `${path}.currentManager.managerGeneration`)} AND json_extract(${json},'${path}.currentManager.managerGeneration')>0
      AND json_extract(${json},'${path}.currentManager.managerGeneration') IS json_extract(${json},'${path}.highestManagerGeneration'))))`;
  const columnControl = (
    json: string,
    path: string,
    row: string,
  ) => `(json_extract(${json},'${path}.group.environmentId') IS ${row}.environment_id AND json_extract(${json},'${path}.group.groupId') IS ${row}.group_id
    AND json_extract(${json},'${path}.controlRevision') IS ${row}.control_revision AND json_extract(${json},'${path}.membershipRevision') IS ${row}.membership_revision
    AND json_extract(${json},'${path}.highestManagerGeneration') IS ${row}.highest_manager_generation AND json_extract(${json},'${path}.dispatchHold') IS ${row}.dispatch_hold
    AND json_extract(${json},'${path}.currentManager.managerGeneration') IS ${row}.current_manager_generation AND json_extract(${json},'${path}.currentManager.threadId') IS ${row}.current_manager_thread_id)`;
  const eventShape = `(json_type(NEW.control_event_json) IS 'object'
    AND json_type(NEW.control_event_json,'$.id') IS 'text' AND json_extract(NEW.control_event_json,'$.id') IS NEW.control_event_id
    AND json_type(NEW.control_event_json,'$.environmentId') IS 'text' AND json_extract(NEW.control_event_json,'$.environmentId') IS NEW.environment_id
    AND json_type(NEW.control_event_json,'$.commandId') IS 'text' AND ${timestamp("json_extract(NEW.control_event_json,'$.occurredAt')")}
    AND json_type(NEW.control_event_json,'$.performedBy') IS 'object' AND (SELECT count(*) FROM json_each(NEW.control_event_json,'$.performedBy'))=2
    AND json_type(NEW.control_event_json,'$.performedBy.subject') IS 'text' AND json_type(NEW.control_event_json,'$.performedBy.sessionId') IS 'text'
    AND ${control("NEW.control_event_json", "$.control")} AND ${columnControl("NEW.control_event_json", "$.control", "NEW")}
    AND CASE json_extract(NEW.control_event_json,'$.type') WHEN 'task-group.created' THEN (SELECT count(*) FROM json_each(NEW.control_event_json))=7
      WHEN 'task-group.manager-changed' THEN ${control("NEW.control_event_json", "$.previousControl")} AND (SELECT count(*) FROM json_each(NEW.control_event_json))=8+(json_type(NEW.control_event_json,'$.reason') IS NOT NULL)
      WHEN 'task-group.hold-changed' THEN ${control("NEW.control_event_json", "$.previousControl")} AND (SELECT count(*) FROM json_each(NEW.control_event_json))=8+(json_type(NEW.control_event_json,'$.reason') IS NOT NULL)
      ELSE 0 END
    AND (json_type(NEW.control_event_json,'$.reason') IS NULL OR json_type(NEW.control_event_json,'$.reason') IS 'text')) IS TRUE`;
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER work_task_group_canonical_${operation.toLowerCase()} BEFORE ${operation} ON work_task_manager_groups
      WHEN CASE WHEN json_valid(NEW.control_event_json) THEN NOT (${eventShape}) OR EXISTS (
        SELECT 1 FROM json_tree(NEW.control_event_json) WHERE key IS NOT NULL GROUP BY parent,key HAVING count(*)>1
      ) OR EXISTS (
        WITH RECURSIVE strings(value) AS (
          SELECT NEW.environment_id UNION ALL SELECT NEW.group_id UNION ALL SELECT NEW.control_event_id UNION ALL SELECT NEW.current_manager_thread_id WHERE NEW.current_manager_thread_id IS NOT NULL
          UNION ALL SELECT value FROM json_tree(NEW.control_event_json) WHERE type='text'
        ), units(bytes,position,count) AS (
          SELECT hex(CAST(value AS BLOB)),1,0 FROM strings UNION ALL SELECT bytes,position+2,count+CASE WHEN substr(bytes,position,1) IN ('8','9','A','B') THEN 0 WHEN substr(bytes,position,1)='F' THEN 2 ELSE 1 END FROM units WHERE position<=length(bytes) AND count<=2000
        ) SELECT 1 FROM units WHERE count>2000
      ) OR EXISTS (SELECT 1 FROM json_tree(NEW.control_event_json) WHERE type='text' AND (length(CAST(value AS BLOB))=0 OR hex(value) IS NOT hex(trim(value,${whitespace}))))
      ELSE 1 END BEGIN SELECT RAISE(ABORT,'invalid canonical group control'); END`);
    yield* sql.unsafe(`CREATE TRIGGER work_task_group_membership_closed_${operation.toLowerCase()} BEFORE ${operation} ON work_task_manager_groups
      WHEN NEW.membership_revision IS NOT 0 OR json_extract(NEW.control_event_json,'$.type') NOT IN ('task-group.created','task-group.manager-changed','task-group.hold-changed')
      BEGIN SELECT RAISE(ABORT,'group membership storage unavailable'); END`);
    yield* sql.unsafe(`CREATE TRIGGER work_task_group_legacy_role_${operation.toLowerCase()} BEFORE ${operation} ON work_task_manager_groups
      WHEN NEW.current_manager_thread_id IS NOT NULL AND EXISTS(SELECT 1 FROM work_task_manager_bindings WHERE thread_id=NEW.current_manager_thread_id AND retired_at IS NULL)
      BEGIN SELECT RAISE(ABORT,'legacy manager role still current'); END`);
    yield* sql.unsafe(`CREATE TRIGGER work_task_legacy_group_role_${operation.toLowerCase()} BEFORE ${operation} ON work_task_manager_bindings
      WHEN NEW.retired_at IS NULL AND EXISTS(SELECT 1 FROM work_task_manager_groups WHERE current_manager_thread_id=NEW.thread_id)
      BEGIN SELECT RAISE(ABORT,'group manager role still current'); END`);
  }
  const highWater = `(SELECT coalesce(max(generation),0) FROM work_task_group_managers WHERE environment_id=NEW.environment_id AND group_id=NEW.group_id)`;
  const knownEvent = (
    id: string,
    sequence: string,
  ) => `EXISTS(SELECT 1 FROM work_task_manager_groups WHERE created_event_id=${id} OR control_event_id=${id} OR created_sequence=${sequence} OR control_sequence=${sequence})
    OR EXISTS(SELECT 1 FROM work_task_group_managers WHERE bound_event_id=${id} OR bound_sequence=${sequence} OR retired_event_id=${id} OR retired_sequence=${sequence})`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_group_insert BEFORE INSERT ON work_task_manager_groups
    WHEN NEW.control_revision IS NOT 0 OR NEW.highest_manager_generation IS NOT 0 OR NEW.current_manager_generation IS NOT NULL OR NEW.current_manager_thread_id IS NOT NULL OR NEW.dispatch_hold IS NOT 'none'
      OR NEW.created_event_id IS NOT NEW.control_event_id OR NEW.created_sequence IS NOT NEW.control_sequence OR NEW.created_event_json IS NOT NEW.control_event_json
      OR json_extract(NEW.control_event_json,'$.type') IS NOT 'task-group.created'
      OR EXISTS(SELECT 1 FROM work_task_manager_groups WHERE environment_id=NEW.environment_id AND group_id=NEW.group_id)
      OR ${knownEvent("NEW.control_event_id", "NEW.control_sequence")}
    BEGIN SELECT RAISE(ABORT,'group creation identity conflict'); END`);
  yield* sql.unsafe(`CREATE TRIGGER work_task_group_update BEFORE UPDATE ON work_task_manager_groups
    WHEN NEW.environment_id IS NOT OLD.environment_id OR NEW.group_id IS NOT OLD.group_id OR NEW.created_event_id IS NOT OLD.created_event_id OR NEW.created_sequence IS NOT OLD.created_sequence OR NEW.created_event_json IS NOT OLD.created_event_json
      OR NEW.control_revision IS NOT OLD.control_revision+1 OR NEW.control_sequence<=OLD.control_sequence
      OR NOT ${columnControl("NEW.control_event_json", "$.previousControl", "OLD")}
      OR OLD.highest_manager_generation IS NOT ${highWater}
      OR (OLD.current_manager_generation IS NOT NULL AND NOT EXISTS(SELECT 1 FROM work_task_group_managers WHERE environment_id=OLD.environment_id AND group_id=OLD.group_id AND current_generation=OLD.current_manager_generation AND current_manager_thread_id=OLD.current_manager_thread_id))
      OR EXISTS(SELECT 1 FROM work_task_group_managers WHERE environment_id=OLD.environment_id AND group_id=OLD.group_id AND current_generation IS NOT NULL AND (current_generation IS NOT OLD.current_manager_generation OR current_manager_thread_id IS NOT OLD.current_manager_thread_id))
      OR CASE json_extract(NEW.control_event_json,'$.type')
        WHEN 'task-group.manager-changed' THEN NEW.dispatch_hold IS NOT OLD.dispatch_hold OR (CASE WHEN NEW.current_manager_generation IS NULL THEN OLD.current_manager_generation IS NULL OR NEW.highest_manager_generation IS NOT OLD.highest_manager_generation
          ELSE NEW.highest_manager_generation IS NOT OLD.highest_manager_generation+1 OR NEW.current_manager_thread_id IS OLD.current_manager_thread_id END)
        WHEN 'task-group.hold-changed' THEN NEW.highest_manager_generation IS NOT OLD.highest_manager_generation OR NEW.current_manager_generation IS NOT OLD.current_manager_generation OR NEW.current_manager_thread_id IS NOT OLD.current_manager_thread_id OR NEW.dispatch_hold IS OLD.dispatch_hold
        ELSE 1 END
      OR EXISTS(SELECT 1 FROM work_task_manager_groups WHERE environment_id=NEW.environment_id AND group_id<>OLD.group_id AND current_manager_thread_id=NEW.current_manager_thread_id)
      OR ${knownEvent("NEW.control_event_id", "NEW.control_sequence")}
    BEGIN SELECT RAISE(ABORT,'group control or pending manager conflict'); END`);
  yield* sql`CREATE TRIGGER work_task_group_delete BEFORE DELETE ON work_task_manager_groups BEGIN SELECT RAISE(ABORT,'group identity is immutable'); END`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_group_manager_insert BEFORE INSERT ON work_task_group_managers
    WHEN NEW.generation IS NOT ${highWater}+1 OR NEW.retired_event_id IS NOT NULL OR NEW.retired_sequence IS NOT NULL OR NEW.retired_event_json IS NOT NULL
      OR EXISTS(SELECT 1 FROM work_task_group_managers WHERE environment_id=NEW.environment_id AND group_id=NEW.group_id AND generation=NEW.generation)
      OR NOT EXISTS(SELECT 1 FROM work_task_manager_groups g WHERE g.environment_id=NEW.environment_id AND g.group_id=NEW.group_id AND g.current_manager_generation=NEW.generation AND g.current_manager_thread_id=NEW.manager_thread_id
        AND g.control_event_id IS NEW.bound_event_id AND g.control_sequence IS NEW.bound_sequence AND g.control_event_json IS NEW.bound_event_json AND json_extract(g.control_event_json,'$.type')='task-group.manager-changed')
    BEGIN SELECT RAISE(ABORT,'group manager binding conflict'); END`);
  yield* sql.unsafe(`CREATE TRIGGER work_task_group_manager_update BEFORE UPDATE ON work_task_group_managers
    WHEN OLD.retired_event_id IS NOT NULL OR NEW.retired_event_id IS NULL OR NEW.retired_sequence IS NULL OR NEW.retired_event_json IS NULL
      OR NEW.environment_id IS NOT OLD.environment_id OR NEW.group_id IS NOT OLD.group_id OR NEW.generation IS NOT OLD.generation OR NEW.manager_thread_id IS NOT OLD.manager_thread_id
      OR NEW.bound_event_id IS NOT OLD.bound_event_id OR NEW.bound_sequence IS NOT OLD.bound_sequence OR NEW.bound_event_json IS NOT OLD.bound_event_json
      OR NOT EXISTS(SELECT 1 FROM work_task_manager_groups g WHERE g.environment_id=NEW.environment_id AND g.group_id=NEW.group_id
        AND g.control_event_id IS NEW.retired_event_id AND g.control_sequence IS NEW.retired_sequence AND g.control_event_json IS NEW.retired_event_json AND json_extract(g.control_event_json,'$.type')='task-group.manager-changed'
        AND json_extract(g.control_event_json,'$.previousControl.currentManager.managerGeneration') IS OLD.generation AND json_extract(g.control_event_json,'$.previousControl.currentManager.threadId') IS OLD.manager_thread_id)
    BEGIN SELECT RAISE(ABORT,'group manager retirement conflict'); END`);
  yield* sql`CREATE TRIGGER work_task_group_manager_delete BEFORE DELETE ON work_task_group_managers BEGIN SELECT RAISE(ABORT,'group manager history is immutable'); END`;
});
