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
  const type = (path: string, kind: string) => `json_type(NEW.event_json,'$.${path}') IS '${kind}'`;
  const object = (path: string, count: number) =>
    `(json_type(NEW.event_json,'${path ? "$." + path : "$"}') IS 'object' AND (SELECT count(*) FROM json_each(NEW.event_json,'${path ? "$." + path : "$"}'))=${count})`;
  const integer = (path: string, min = 0) =>
    `(${type(path, "integer")} AND ${get(path)} BETWEEN ${min} AND 9007199254740991)`;
  const nullable = (path: string, min = 0) => `(${type(path, "null")} OR ${integer(path, min)})`;
  const same = (a: string, b: string) => structuralJsonEqualSql(a, b);
  const columns: Record<string, readonly [string, string]> = {
    environment_id: ["environmentId", "TEXT"],
    task_id: ["task.taskId", "TEXT"],
    group_id: ["group.groupId", "TEXT"],
    membership_generation: ["membershipGeneration", "INTEGER"],
    kind: ["kind", "TEXT"],
    source_event_id: ["sourceEventId", "TEXT"],
    source_sequence: ["sourceSequence", "INTEGER"],
    previous_oldest_ready_sequence: ["previousOldestReadySequence", "INTEGER"],
    oldest_ready_sequence: ["oldestReadySequence", "INTEGER"],
    accepted_definition_revision: ["basis.taskRevisions.acceptedDefinitionRevision", "INTEGER"],
    manager_generation: ["basis.managerGeneration", "INTEGER"],
    claim_id: ["claim.binding.workClaimId", "TEXT"],
    claim_accepted_sequence: ["claim.acceptedSequence", "INTEGER"],
    release_event_id: ["releaseEventId", "TEXT"],
    release_sequence: ["releaseSequence", "INTEGER"],
  };
  yield* sql.unsafe(`CREATE TABLE work_task_ready_events (
    event_id TEXT NOT NULL PRIMARY KEY,sequence INTEGER NOT NULL UNIQUE CHECK(typeof(sequence)='integer' AND sequence BETWEEN 0 AND 9007199254740991),
    event_json TEXT NOT NULL CHECK(json_valid(event_json)),
    ${Object.entries(columns)
      .map(([k, [p, t]]) => `${k} ${t} GENERATED ALWAYS AS(${get(p, "event_json")}) STORED`)
      .join(",")},
    FOREIGN KEY(task_id,membership_generation) REFERENCES work_task_group_memberships(task_id,membership_generation),
    FOREIGN KEY(environment_id,group_id,manager_generation) REFERENCES work_task_group_managers(environment_id,group_id,generation),
    FOREIGN KEY(task_id,accepted_definition_revision) REFERENCES work_task_accepted_definitions(task_id,definition_revision),
    FOREIGN KEY(claim_id) REFERENCES work_task_executable_claims(work_claim_id),
    FOREIGN KEY(release_event_id) REFERENCES work_task_claim_releases(event_id)
  ) WITHOUT ROWID`);
  for (const [name, keys, where, unique] of [
    ["latest", "task_id,membership_generation,sequence DESC,event_id", "", false],
    ["accepted", "task_id,membership_generation,sequence DESC", "WHERE kind='accepted'", false],
    [
      "served",
      "task_id,membership_generation,sequence DESC",
      "WHERE kind IN ('accepted','settled')",
      false,
    ],
    ["claim", "claim_id", "WHERE kind='accepted'", true],
    ["release", "release_event_id", "WHERE kind='settled'", true],
    ["task", "task_id,sequence", "", false],
    ["definition", "task_id,accepted_definition_revision", "", false],
  ] as const)
    yield* sql.unsafe(
      `CREATE ${unique ? "UNIQUE " : ""}INDEX work_task_ready_${name}_idx ON work_task_ready_events(${keys}) ${where}`,
    );
  yield* sql`CREATE TABLE work_task_ready_state (
    task_id TEXT NOT NULL,membership_generation INTEGER NOT NULL,environment_id TEXT NOT NULL,group_id TEXT NOT NULL,
    latest_event_id TEXT NOT NULL UNIQUE REFERENCES work_task_ready_events(event_id),latest_sequence INTEGER NOT NULL,
    basis_json TEXT NOT NULL,oldest_ready_sequence INTEGER,epoch_start_event_id TEXT REFERENCES work_task_ready_events(event_id),
    pending_claim_id TEXT REFERENCES work_task_executable_claims(work_claim_id),last_served_claim_id TEXT REFERENCES work_task_executable_claims(work_claim_id),last_served_sequence INTEGER,
    PRIMARY KEY(task_id,membership_generation),FOREIGN KEY(task_id,membership_generation) REFERENCES work_task_group_memberships(task_id,membership_generation)
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_ready_selection_idx ON work_task_ready_state(environment_id,group_id,oldest_ready_sequence,task_id,membership_generation) WHERE oldest_ready_sequence IS NOT NULL`;
  yield* sql`CREATE TABLE work_task_ready_references (
    owner_event_id TEXT NOT NULL REFERENCES work_task_ready_events(event_id),reference_role TEXT NOT NULL,position INTEGER NOT NULL,
    event_id TEXT NOT NULL,sequence INTEGER NOT NULL,known_kind TEXT,known_task_id TEXT,
    PRIMARY KEY(owner_event_id,reference_role,position)
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_ready_ref_id_idx ON work_task_ready_references(event_id,sequence)`;
  yield* sql`CREATE INDEX work_task_ready_ref_sequence_idx ON work_task_ready_references(sequence,event_id)`;
  for (const table of ["work_task_ready_events", "work_task_ready_references"])
    for (const op of ["UPDATE", "DELETE"])
      yield* sql.unsafe(
        `CREATE TRIGGER ${table}_${op.toLowerCase()} BEFORE ${op} ON ${table} BEGIN SELECT RAISE(ABORT,'immutable ready evidence'); END`,
      );
  const key = (alias: string, subject = "NEW") =>
    `${alias}.task_id=${subject}.task_id AND ${alias}.membership_generation=${subject}.membership_generation`;
  const latest = `(SELECT event_json FROM work_task_ready_events e WHERE ${key("e")} ORDER BY sequence DESC LIMIT 1)`;
  const pending = (subject: string) => `(SELECT a.claim_id FROM work_task_ready_events a
    WHERE a.event_id=(SELECT event_id FROM work_task_ready_events e WHERE ${key("e", subject)} AND kind='accepted' ORDER BY sequence DESC LIMIT 1)
    AND NOT EXISTS(SELECT 1 FROM work_task_executable_claims c JOIN work_task_ready_events s ON s.release_event_id=c.release_event_id AND s.kind='settled'
      WHERE c.work_claim_id=a.claim_id AND s.task_id=a.task_id AND s.membership_generation=a.membership_generation AND s.claim_id=a.claim_id AND s.sequence>a.sequence))`;
  const latestClaim = (subject: string) =>
    `(SELECT work_claim_id FROM work_task_executable_claims c WHERE c.task_id=${subject}.task_id AND c.accepted_sequence<${subject}.sequence ORDER BY accepted_sequence DESC LIMIT 1)`;
  const occupied = (
    subject: string,
  ) => `EXISTS(SELECT 1 FROM work_task_executable_claims c WHERE c.task_id=${subject}.task_id AND c.accepted_sequence<${subject}.sequence
    AND NOT EXISTS(SELECT 1 FROM work_task_claim_releases r WHERE r.event_id=c.release_event_id AND r.work_claim_id=c.work_claim_id AND r.sequence<${subject}.sequence))`;
  const reasons =
    "'task_pause','task_stop','done','archive','prerequisite_changed','definition_changed','policy_changed','membership_changed'";
  yield* sql.unsafe(`CREATE TRIGGER work_task_ready_valid BEFORE INSERT ON work_task_ready_events BEGIN
    SELECT CASE WHEN (${canonicalEvidenceJsonSql("NEW.event_json")} AND NEW.event_id IS ${get("id")} AND ${type("id", "text")}
      AND ${get("type")} IS 'task.ready-sequence-updated' AND ${object("attribution", 1)} AND ${get("attribution.type")} IS 'server'
      AND ${["environmentId", "commandId", "occurredAt", "sourceEventId"].map((p) => type(p, "text")).join(" AND ")}
      AND ${validTimestampSql(get("occurredAt"))} AND ${object("task", 2)} AND ${object("group", 2)}
      AND ${type("task.taskId", "text")} AND ${type("group.groupId", "text")} AND ${get("task.environmentId")} IS NEW.environment_id AND ${get("group.environmentId")} IS NEW.environment_id
      AND ${integer("membershipGeneration", 1)} AND ${integer("sourceSequence")} AND NEW.source_sequence<NEW.sequence
      AND ${nullable("previousOldestReadySequence")} AND ${nullable("oldestReadySequence")}
      AND (NEW.previous_oldest_ready_sequence IS NULL OR NEW.previous_oldest_ready_sequence<NEW.sequence)
      AND (NEW.oldest_ready_sequence IS NULL OR NEW.oldest_ready_sequence<=NEW.sequence)
      AND ${object("basis", 5)} AND ${object("basis.taskRevisions", 5)}
      AND ${["definitionRevision", "lifecycleRevision", "actionPolicyRevision", "completionPolicyRevision"].map((p) => integer(`basis.taskRevisions.${p}`, 1)).join(" AND ")}
      AND ${nullable("basis.taskRevisions.acceptedDefinitionRevision", 1)} AND (NEW.accepted_definition_revision IS NULL OR NEW.accepted_definition_revision<=${get("basis.taskRevisions.definitionRevision")})
      AND ${integer("basis.graphRevision")} AND ${integer("basis.controlRevision")} AND ${integer("basis.membershipRevision", 1)} AND ${nullable("basis.managerGeneration", 1)}
      AND CASE NEW.kind
        WHEN 'qualified' THEN ${object("", 15)} AND json_type(NEW.event_json,'$.claim') IS NULL AND NEW.oldest_ready_sequence IS NOT NULL AND NEW.accepted_definition_revision IS NOT NULL AND NEW.manager_generation IS NOT NULL
          AND NEW.oldest_ready_sequence=coalesce(NEW.previous_oldest_ready_sequence,NEW.sequence)
        WHEN 'accepted' THEN ${object("", 16)} AND NEW.previous_oldest_ready_sequence IS NOT NULL AND NEW.oldest_ready_sequence IS NULL AND NEW.previous_oldest_ready_sequence<=NEW.claim_accepted_sequence
        WHEN 'settled' THEN ${object("", 18)} AND NEW.previous_oldest_ready_sequence IS NULL AND NEW.oldest_ready_sequence=NEW.sequence AND NEW.accepted_definition_revision IS NOT NULL AND NEW.manager_generation IS NOT NULL
          AND ${type("releaseEventId", "text")} AND ${integer("releaseSequence")} AND NEW.claim_accepted_sequence<NEW.release_sequence AND NEW.release_sequence<NEW.sequence
        WHEN 'disqualified' THEN ${object("", 16)} AND json_type(NEW.event_json,'$.claim') IS NULL AND NEW.oldest_ready_sequence IS NULL AND ${get("reason")} IN (${reasons}) ELSE 0 END
      AND EXISTS(SELECT 1 FROM work_task_group_memberships m WHERE ${key("m")} AND m.environment_id=NEW.environment_id AND m.group_id=NEW.group_id)
      ) IS NOT TRUE THEN RAISE(ABORT,'invalid ready event') END;
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_ready_events WHERE event_id=NEW.event_id OR sequence=NEW.sequence)
      OR NEW.previous_oldest_ready_sequence IS NOT json_extract(${latest},'$.oldestReadySequence')
      OR EXISTS(SELECT 1 FROM work_task_ready_events e WHERE ${key("e")} AND e.sequence>=NEW.sequence)
      THEN RAISE(ABORT,'ready compare and swap conflict') END;
    SELECT CASE WHEN NEW.kind IN ('qualified','accepted') AND ${pending("NEW")} IS NOT NULL
      OR NEW.kind IN ('qualified','settled') AND ${occupied("NEW")}
      OR NEW.kind='settled' AND ${pending("NEW")} IS NOT NULL AND ${pending("NEW")} IS NOT NEW.claim_id
      THEN RAISE(ABORT,'ready pending physical custody') END;
    SELECT CASE WHEN NEW.kind IN ('accepted','settled') AND NOT EXISTS(
      SELECT 1 FROM work_task_executable_claims c WHERE c.work_claim_id=NEW.claim_id AND c.task_id=NEW.task_id AND c.group_id=NEW.group_id
        AND c.environment_id=NEW.environment_id AND c.membership_generation=NEW.membership_generation AND c.accepted_sequence<NEW.sequence
        AND c.work_claim_id=${latestClaim("NEW")} AND ${object("claim", 4)} AND ${same(get("claim.binding"), "json_extract(c.event_json,'$.claim.binding')")}
        AND ${get("claim.acceptedEventId")} IS c.accepted_event_id AND NEW.claim_accepted_sequence IS c.accepted_sequence AND ${get("claim.acceptedAt")} IS json_extract(c.event_json,'$.occurredAt')
        AND CASE NEW.kind WHEN 'accepted' THEN NEW.source_event_id=c.accepted_event_id AND NEW.source_sequence=c.accepted_sequence
          AND ${same(get("basis.taskRevisions"), "json_extract(c.event_json,'$.claim.binding.taskRevisions')")}
          AND ${["graphRevision", "controlRevision", "membershipRevision"].map((p) => `${get(`basis.${p}`)} IS json_extract(c.event_json,'$.claim.binding.${p}')`).join(" AND ")}
          AND NEW.manager_generation=c.manager_generation
        ELSE EXISTS(SELECT 1 FROM work_task_claim_releases r WHERE r.event_id=c.release_event_id AND r.event_id=NEW.release_event_id
          AND r.work_claim_id=c.work_claim_id AND r.sequence=NEW.release_sequence AND NEW.source_event_id=r.event_id AND NEW.source_sequence=r.sequence) END)
      THEN RAISE(ABORT,'ready physical reference conflict') END;
  END`);
  yield* sql`CREATE TRIGGER work_task_ready_physical_reverse BEFORE INSERT ON work_task_executable_claims BEGIN
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_ready_events e WHERE e.task_id=NEW.task_id AND e.sequence>NEW.accepted_sequence
      AND ((e.kind IN ('accepted','settled') AND e.claim_accepted_sequence<NEW.accepted_sequence)
        OR (e.kind IN ('qualified','settled') AND NOT EXISTS(SELECT 1 FROM work_task_claim_releases r WHERE r.event_id=NEW.release_event_id AND r.sequence<e.sequence))))
      THEN RAISE(ABORT,'physical history contradicts ready evidence') END;
  END`;
  const served = (subject: string, column: string) =>
    `(SELECT ${column} FROM work_task_ready_events e WHERE ${key("e", subject)} AND kind IN ('accepted','settled') ORDER BY sequence DESC LIMIT 1)`;
  const projection = (subject: string) => ({
    task_id: `${subject}.task_id`,
    membership_generation: `${subject}.membership_generation`,
    environment_id: `${subject}.environment_id`,
    group_id: `${subject}.group_id`,
    latest_event_id: `${subject}.event_id`,
    latest_sequence: `${subject}.sequence`,
    basis_json: `json_extract(${subject}.event_json,'$.basis')`,
    oldest_ready_sequence: `${subject}.oldest_ready_sequence`,
    epoch_start_event_id: `(SELECT event_id FROM work_task_ready_events e WHERE e.sequence=${subject}.oldest_ready_sequence AND ${key("e", subject)} AND e.kind IN ('qualified','settled') AND e.previous_oldest_ready_sequence IS NULL)`,
    pending_claim_id: pending(subject),
    last_served_claim_id: served(subject, "claim_id"),
    last_served_sequence: served(subject, "claim_accepted_sequence"),
  });
  for (const op of ["INSERT", "UPDATE"])
    yield* sql.unsafe(`CREATE TRIGGER work_task_ready_state_${op.toLowerCase()} BEFORE ${op} ON work_task_ready_state BEGIN
    SELECT CASE WHEN ${op === "INSERT" ? `EXISTS(SELECT 1 FROM work_task_ready_state s WHERE ${key("s")}) OR ` : "NEW.task_id IS NOT OLD.task_id OR NEW.membership_generation IS NOT OLD.membership_generation OR "}
      NOT EXISTS(SELECT 1 FROM work_task_ready_events actual WHERE actual.event_id=(SELECT event_id FROM work_task_ready_events r WHERE ${key("r")} ORDER BY sequence DESC LIMIT 1)
        AND ${Object.entries(projection("actual"))
          .map(([k, v]) => (k === "basis_json" ? same(`NEW.${k}`, v) : `NEW.${k} IS ${v}`))
          .join(" AND ")})
      THEN RAISE(ABORT,'invalid derived ready state') END;
  END`);
  yield* sql`CREATE TRIGGER work_task_ready_state_delete BEFORE DELETE ON work_task_ready_state BEGIN SELECT RAISE(ABORT,'retained ready state'); END`;
  const derived = projection("NEW");
  yield* sql.unsafe(`CREATE TRIGGER work_task_ready_project AFTER INSERT ON work_task_ready_events BEGIN
    INSERT INTO work_task_ready_state(${Object.keys(derived).join(",")}) SELECT ${Object.values(derived).join(",")} WHERE NOT EXISTS(SELECT 1 FROM work_task_ready_state s WHERE ${key("s")});
    UPDATE work_task_ready_state SET ${Object.entries(derived)
      .map(([k, v]) => `${k}=${v}`)
      .join(",")} WHERE task_id=NEW.task_id AND membership_generation=NEW.membership_generation;
  END`);
  const descriptors = (
    json: string,
    sequence: string,
  ) => `SELECT 'recorded' role,0 position,${get("id", json)} event_id,${sequence} sequence,'task.ready-sequence-updated' known_kind,${get("task.taskId", json)} known_task_id
    UNION ALL SELECT 'source',0,${get("sourceEventId", json)},${get("sourceSequence", json)},NULL,NULL
    UNION ALL SELECT 'claim',0,c.accepted_event_id,c.accepted_sequence,'task-group.work-accepted',c.task_id FROM work_task_executable_claims c WHERE c.work_claim_id=${get("claim.binding.workClaimId", json)}
    UNION ALL SELECT 'release',0,r.event_id,r.sequence,'task-group.work-released',${get("task.taskId", json)} FROM work_task_claim_releases r WHERE r.event_id=${get("releaseEventId", json)}
    UNION ALL SELECT 'owner',0,a.recorded_event_id,a.recorded_sequence,'task.run-ownership-accepted',a.task_id FROM work_task_accepted_runs a WHERE a.run_id=${get("claim.binding.runId", json)}`;
  const conflicts = (
    table: string,
    id = "event_id",
    seq = "sequence",
    kind = "r.known_kind",
    task = "r.known_task_id",
  ) => `EXISTS(SELECT 1 FROM ${table} r WHERE r.${id}=NEW.event_id AND
    (r.${seq}<>NEW.sequence OR ((${kind}) IS NOT NULL AND NEW.known_kind IS NOT NULL AND (${kind})<>NEW.known_kind)
      OR ((${task}) IS NOT NULL AND NEW.known_task_id IS NOT NULL AND (${task})<>NEW.known_task_id)))
    OR EXISTS(SELECT 1 FROM ${table} r WHERE r.${seq}=NEW.sequence AND r.${id}<>NEW.event_id)`;
  const historical = [
    [
      "work_task_graph_edges",
      "adopted_event_id",
      "adopted_sequence",
      "'task.graph-edge-adopted'",
      "r.dependent_task_id",
    ],
    [
      "work_task_graph_edges",
      "removed_event_id",
      "removed_sequence",
      "'task.graph-edge-removed'",
      "r.dependent_task_id",
    ],
    [
      "work_task_manager_groups",
      "created_event_id",
      "created_sequence",
      "'task-group.created'",
      "NULL",
    ],
    [
      "work_task_manager_groups",
      "control_event_id",
      "control_sequence",
      "json_extract(r.control_event_json,'$.type')",
      "NULL",
    ],
    [
      "work_task_group_managers",
      "bound_event_id",
      "bound_sequence",
      "'task-group.manager-changed'",
      "NULL",
    ],
    [
      "work_task_group_managers",
      "retired_event_id",
      "retired_sequence",
      "'task-group.manager-changed'",
      "NULL",
    ],
    [
      "work_task_membership_companions",
      "event_id",
      "sequence",
      "'task-group.membership-changed'",
      "r.task_id",
    ],
    [
      "work_task_membership_companions",
      "root_event_id",
      "root_sequence",
      "'task.membership-changed'",
      "r.task_id",
    ],
  ] as const;
  const sourceFacts = [
    ...historical,
    ["work_task_dirty_events", "event_id", "sequence", "r.kind", "r.task_id"],
    ["work_task_dirty_events", "source_id", "source_sequence", "NULL", "NULL"],
    ["work_task_reconciliation_events", "event_id", "sequence", "r.kind", "NULL"],
    ["work_task_reconciliation_events", "source_id", "source_sequence", "NULL", "NULL"],
  ] as const;
  const referenceTables = [
    "work_task_ready_references",
    "work_task_accepted_run_references",
    "work_task_run_observation_references",
    "work_task_claim_references",
  ];
  yield* sql.unsafe(`CREATE TRIGGER work_task_ready_reference_valid BEFORE INSERT ON work_task_ready_references BEGIN
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM (${descriptors("(SELECT event_json FROM work_task_ready_events WHERE event_id=NEW.owner_event_id)", "(SELECT sequence FROM work_task_ready_events WHERE event_id=NEW.owner_event_id)")}) d
      WHERE d.role=NEW.reference_role AND d.position=NEW.position AND d.event_id IS NEW.event_id AND d.sequence IS NEW.sequence AND d.known_kind IS NEW.known_kind AND d.known_task_id IS NEW.known_task_id)
      OR EXISTS(SELECT 1 FROM work_task_ready_references WHERE owner_event_id=NEW.owner_event_id AND reference_role=NEW.reference_role AND position=NEW.position)
      THEN RAISE(ABORT,'invalid ready reference') END;
    SELECT CASE WHEN ${referenceTables.map((t) => conflicts(t)).join(" OR ")} OR ${sourceFacts.map(([t, id, seq, k, task]) => conflicts(t, id, seq, k, task)).join(" OR ")}
      THEN RAISE(ABORT,'ready reference identity conflict') END;
  END`);
  for (const table of referenceTables.slice(1))
    yield* sql.unsafe(`CREATE TRIGGER ${table}_ready BEFORE INSERT ON ${table} BEGIN
    SELECT CASE WHEN ${conflicts("work_task_ready_references")} THEN RAISE(ABORT,'ready reference conflict') END; END`);
  for (const [table, id, seq, kind, task] of sourceFacts)
    for (const op of ["INSERT", "UPDATE"])
      yield* sql.unsafe(`CREATE TRIGGER ${table}_${id}_ready_${op.toLowerCase()} BEFORE ${op} ON ${table} BEGIN
      SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_ready_references r WHERE r.event_id=NEW.${id} AND (r.sequence<>NEW.${seq}
        OR (r.known_kind IS NOT NULL AND (${kind.replaceAll("r.", "NEW.")}) IS NOT NULL AND r.known_kind<>(${kind.replaceAll("r.", "NEW.")}))
        OR (r.known_task_id IS NOT NULL AND (${task.replaceAll("r.", "NEW.")}) IS NOT NULL AND r.known_task_id<>(${task.replaceAll("r.", "NEW.")}))))
        OR EXISTS(SELECT 1 FROM work_task_ready_references r WHERE r.sequence=NEW.${seq} AND r.event_id<>NEW.${id})
        THEN RAISE(ABORT,'historical ready identity conflict') END;
    END`);
  yield* sql.unsafe(`CREATE TRIGGER work_task_ready_refs AFTER INSERT ON work_task_ready_events BEGIN
    INSERT INTO work_task_ready_references(owner_event_id,reference_role,position,event_id,sequence,known_kind,known_task_id)
      SELECT NEW.event_id,role,position,event_id,sequence,known_kind,known_task_id FROM (${descriptors("NEW.event_json", "NEW.sequence")});
  END`);
  const definitionKeys = [
    "task_id",
    "definition_revision",
    "action_policy_revision",
    "definition_json",
    "attribution_json",
    "accepted_event_id",
    "accepted_at",
  ];
  for (const op of ["INSERT", "UPDATE"])
    yield* sql.unsafe(`CREATE TRIGGER work_task_ready_definition_${op.toLowerCase()} BEFORE ${op} ON work_task_accepted_definitions BEGIN
    SELECT CASE WHEN ${op === "UPDATE" ? `(EXISTS(SELECT 1 FROM work_task_ready_events WHERE task_id=OLD.task_id AND accepted_definition_revision=OLD.definition_revision) AND (${definitionKeys.map((k) => `NEW.${k} IS NOT OLD.${k}`).join(" OR ")})) OR ` : ""}
      EXISTS(SELECT 1 FROM work_task_accepted_definitions d JOIN work_task_ready_events e ON e.task_id=d.task_id AND e.accepted_definition_revision=d.definition_revision
        WHERE ((d.task_id=NEW.task_id AND d.definition_revision=NEW.definition_revision) OR d.accepted_event_id=NEW.accepted_event_id)${op === "UPDATE" ? " AND NOT(d.task_id=OLD.task_id AND d.definition_revision=OLD.definition_revision)" : ""})
      THEN RAISE(ABORT,'ready accepted definition conflict') END;
  END`);
});
