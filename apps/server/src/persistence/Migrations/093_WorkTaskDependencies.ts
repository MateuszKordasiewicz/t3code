import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE work_task_graph_edges (
      environment_id TEXT NOT NULL,
      edge_id TEXT NOT NULL,
      dependent_task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      blocker_task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
      created_graph_revision INTEGER NOT NULL CHECK(typeof(created_graph_revision)='integer' AND created_graph_revision BETWEEN 1 AND 9007199254740991),
      adopted_event_id TEXT NOT NULL,
      adopted_sequence INTEGER NOT NULL CHECK(typeof(adopted_sequence)='integer' AND adopted_sequence BETWEEN 0 AND 9007199254740991),
      edge_json TEXT NOT NULL CHECK(typeof(edge_json)='text' AND json_valid(edge_json)),
      removed_graph_revision INTEGER,
      removed_event_id TEXT,
      removed_sequence INTEGER,
      removal_provenance_json TEXT,
      PRIMARY KEY(environment_id,edge_id),
      CHECK(dependent_task_id <> blocker_task_id),
      CHECK((removed_graph_revision IS NULL AND removed_event_id IS NULL AND removed_sequence IS NULL AND removal_provenance_json IS NULL)
        OR (removed_graph_revision IS NOT NULL AND removed_event_id IS NOT NULL AND removed_sequence IS NOT NULL AND removal_provenance_json IS NOT NULL
          AND typeof(removed_graph_revision)='integer' AND removed_graph_revision BETWEEN 1 AND 9007199254740991 AND removed_graph_revision > created_graph_revision
          AND typeof(removed_sequence)='integer' AND removed_sequence BETWEEN 0 AND 9007199254740991 AND removed_sequence > adopted_sequence
          AND typeof(removal_provenance_json)='text' AND json_valid(removal_provenance_json)))
    ) WITHOUT ROWID
  `;
  yield* sql`CREATE UNIQUE INDEX work_task_graph_edges_created_revision_idx ON work_task_graph_edges(environment_id,created_graph_revision)`;
  yield* sql`CREATE UNIQUE INDEX work_task_graph_edges_removed_revision_idx ON work_task_graph_edges(environment_id,removed_graph_revision) WHERE removed_graph_revision IS NOT NULL`;
  yield* sql`CREATE UNIQUE INDEX work_task_graph_edges_active_idx ON work_task_graph_edges(environment_id,dependent_task_id,blocker_task_id) WHERE removed_graph_revision IS NULL`;
  yield* sql`CREATE INDEX work_task_graph_edges_forward_idx ON work_task_graph_edges(environment_id,dependent_task_id,blocker_task_id,edge_id) WHERE removed_graph_revision IS NULL`;
  yield* sql`CREATE INDEX work_task_graph_edges_reverse_idx ON work_task_graph_edges(environment_id,blocker_task_id,dependent_task_id,edge_id) WHERE removed_graph_revision IS NULL`;
  yield* sql`CREATE UNIQUE INDEX work_task_graph_edges_adopted_event_idx ON work_task_graph_edges(adopted_event_id)`;
  yield* sql`CREATE UNIQUE INDEX work_task_graph_edges_adopted_sequence_idx ON work_task_graph_edges(adopted_sequence)`;
  yield* sql`CREATE UNIQUE INDEX work_task_graph_edges_removed_event_idx ON work_task_graph_edges(removed_event_id) WHERE removed_event_id IS NOT NULL`;
  yield* sql`CREATE UNIQUE INDEX work_task_graph_edges_removed_sequence_idx ON work_task_graph_edges(removed_sequence) WHERE removed_sequence IS NOT NULL`;

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
  const provenance = (json: string, path: string) => {
    const value = (key: string) => `json_extract(${json},'${path}.${key}')`;
    return `(json_type(${json},'${path}') IS 'object' AND (SELECT count(*) FROM json_each(${json},'${path}'))=4
      AND json_type(${json},'${path}.eventId') IS 'text' AND json_type(${json},'${path}.commandId') IS 'text'
      AND json_type(${json},'${path}.occurredAt') IS 'text' AND ${timestamp(value("occurredAt"))}
      AND json_type(${json},'${path}.adoptedBy') IS 'object' AND (SELECT count(*) FROM json_each(${json},'${path}.adoptedBy'))=2
      AND json_type(${json},'${path}.adoptedBy.subject') IS 'text' AND json_type(${json},'${path}.adoptedBy.sessionId') IS 'text')`;
  };
  const jsonShape = `(json_type(NEW.edge_json) IS 'object' AND (SELECT count(*) FROM json_each(NEW.edge_json))=5
    AND json_type(NEW.edge_json,'$.edgeId') IS 'text' AND json_extract(NEW.edge_json,'$.edgeId') IS NEW.edge_id
    AND json_type(NEW.edge_json,'$.createdGraphRevision') IS 'integer' AND json_extract(NEW.edge_json,'$.createdGraphRevision') IS NEW.created_graph_revision
    AND json_type(NEW.edge_json,'$.dependent') IS 'object' AND (SELECT count(*) FROM json_each(NEW.edge_json,'$.dependent'))=2
    AND json_type(NEW.edge_json,'$.blocker') IS 'object' AND (SELECT count(*) FROM json_each(NEW.edge_json,'$.blocker'))=2
    AND json_type(NEW.edge_json,'$.dependent.environmentId') IS 'text' AND json_type(NEW.edge_json,'$.dependent.taskId') IS 'text'
    AND json_type(NEW.edge_json,'$.blocker.environmentId') IS 'text' AND json_type(NEW.edge_json,'$.blocker.taskId') IS 'text'
    AND json_extract(NEW.edge_json,'$.dependent.environmentId') IS NEW.environment_id AND json_extract(NEW.edge_json,'$.blocker.environmentId') IS NEW.environment_id
    AND json_extract(NEW.edge_json,'$.dependent.taskId') IS NEW.dependent_task_id AND json_extract(NEW.edge_json,'$.blocker.taskId') IS NEW.blocker_task_id
    AND ${provenance("NEW.edge_json", "$.provenance")} AND json_extract(NEW.edge_json,'$.provenance.eventId') IS NEW.adopted_event_id
    AND (NEW.removal_provenance_json IS NULL OR (${provenance("NEW.removal_provenance_json", "$")}
      AND json_extract(NEW.removal_provenance_json,'$.eventId') IS NEW.removed_event_id))) IS TRUE`;
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER work_task_graph_edges_bounds_${operation.toLowerCase()} BEFORE ${operation} ON work_task_graph_edges
      WHEN CASE WHEN json_valid(NEW.edge_json) AND (NEW.removal_provenance_json IS NULL OR json_valid(NEW.removal_provenance_json)) THEN NOT (${jsonShape}) OR EXISTS (
        WITH RECURSIVE strings(value) AS (
          SELECT NEW.environment_id UNION ALL SELECT NEW.edge_id UNION ALL SELECT NEW.dependent_task_id UNION ALL SELECT NEW.blocker_task_id UNION ALL SELECT NEW.adopted_event_id
          UNION ALL SELECT NEW.removed_event_id WHERE NEW.removed_event_id IS NOT NULL
          UNION ALL SELECT value FROM json_tree(NEW.edge_json) WHERE type='text'
          UNION ALL SELECT value FROM json_tree(coalesce(NEW.removal_provenance_json,'null')) WHERE type='text'
        ), units(bytes,position,count) AS (
          SELECT hex(CAST(value AS BLOB)),1,0 FROM strings
          UNION ALL SELECT bytes,position+2,count+CASE WHEN substr(bytes,position,1) IN ('8','9','A','B') THEN 0 WHEN substr(bytes,position,1)='F' THEN 2 ELSE 1 END
          FROM units WHERE position<=length(bytes) AND count<=2000
        ) SELECT 1 FROM units WHERE count>2000
      ) OR EXISTS (
        SELECT 1 FROM (
          SELECT NEW.environment_id AS value UNION ALL SELECT NEW.edge_id UNION ALL SELECT NEW.dependent_task_id UNION ALL SELECT NEW.blocker_task_id UNION ALL SELECT NEW.adopted_event_id
          UNION ALL SELECT NEW.removed_event_id WHERE NEW.removed_event_id IS NOT NULL
          UNION ALL SELECT value FROM json_tree(NEW.edge_json) WHERE type='text'
          UNION ALL SELECT value FROM json_tree(coalesce(NEW.removal_provenance_json,'null')) WHERE type='text'
        ) WHERE typeof(value) IS NOT 'text' OR length(CAST(value AS BLOB))=0 OR hex(value) IS NOT hex(trim(value,${whitespace}))
      ) ELSE 1 END BEGIN SELECT RAISE(ABORT,'invalid canonical graph record'); END`);
  }
  const revision = `max(coalesce((SELECT max(created_graph_revision) FROM work_task_graph_edges WHERE environment_id=NEW.environment_id),0),coalesce((SELECT max(removed_graph_revision) FROM work_task_graph_edges WHERE environment_id=NEW.environment_id AND removed_graph_revision IS NOT NULL),0))`;
  const sequence = `max(coalesce((SELECT adopted_sequence FROM work_task_graph_edges WHERE environment_id=NEW.environment_id ORDER BY created_graph_revision DESC LIMIT 1),-1),coalesce((SELECT removed_sequence FROM work_task_graph_edges WHERE environment_id=NEW.environment_id AND removed_graph_revision IS NOT NULL ORDER BY removed_graph_revision DESC LIMIT 1),-1))`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_graph_edges_insert BEFORE INSERT ON work_task_graph_edges
    WHEN NEW.removed_graph_revision IS NOT NULL OR NEW.removed_event_id IS NOT NULL OR NEW.removed_sequence IS NOT NULL OR NEW.removal_provenance_json IS NOT NULL
      OR NEW.created_graph_revision IS NOT (${revision}+1) OR NEW.adopted_sequence<=${sequence}
      OR EXISTS(SELECT 1 FROM work_task_graph_edges WHERE (environment_id=NEW.environment_id AND edge_id=NEW.edge_id)
        OR adopted_event_id=NEW.adopted_event_id OR removed_event_id=NEW.adopted_event_id OR adopted_sequence=NEW.adopted_sequence OR removed_sequence=NEW.adopted_sequence)
    BEGIN SELECT RAISE(ABORT,'graph adoption identity or revision conflict'); END`);
  yield* sql.unsafe(`CREATE TRIGGER work_task_graph_edges_update BEFORE UPDATE ON work_task_graph_edges
    WHEN OLD.removed_graph_revision IS NOT NULL OR NEW.removed_graph_revision IS NULL OR NEW.removed_event_id IS NULL OR NEW.removed_sequence IS NULL OR NEW.removal_provenance_json IS NULL
      OR NEW.environment_id IS NOT OLD.environment_id OR NEW.edge_id IS NOT OLD.edge_id OR NEW.dependent_task_id IS NOT OLD.dependent_task_id OR NEW.blocker_task_id IS NOT OLD.blocker_task_id
      OR NEW.created_graph_revision IS NOT OLD.created_graph_revision OR NEW.adopted_event_id IS NOT OLD.adopted_event_id OR NEW.adopted_sequence IS NOT OLD.adopted_sequence OR NEW.edge_json IS NOT OLD.edge_json
      OR NEW.removed_graph_revision IS NOT (${revision}+1) OR NEW.removed_sequence<=${sequence}
      OR EXISTS(SELECT 1 FROM work_task_graph_edges WHERE adopted_event_id=NEW.removed_event_id OR removed_event_id=NEW.removed_event_id OR adopted_sequence=NEW.removed_sequence OR removed_sequence=NEW.removed_sequence)
    BEGIN SELECT RAISE(ABORT,'immutable graph adoption or removal conflict'); END`);
  yield* sql`CREATE TRIGGER work_task_graph_edges_delete BEFORE DELETE ON work_task_graph_edges BEGIN SELECT RAISE(ABORT,'immutable graph history'); END`;
});
