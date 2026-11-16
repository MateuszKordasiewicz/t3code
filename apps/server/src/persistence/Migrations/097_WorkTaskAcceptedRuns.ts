import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { canonicalEvidenceJsonSql, validTimestampSql } from "../workTaskSqlV1.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const get = (path: string, json = "NEW.event_json") => `json_extract(${json},'$.${path}')`;
  const kind = (path: string, type: string, json = "NEW.event_json") =>
    `json_type(${json},'$.${path}') IS '${type}'`;
  const object = (path: string, count: number, json = "NEW.event_json") =>
    `(${kind(path, "object", json)} AND (SELECT count(*) FROM json_each(${json},'$.${path}'))=${count})`;
  const integer = (path: string, min = 0, json = "NEW.event_json") =>
    `(${kind(path, "integer", json)} AND ${get(path, json)} BETWEEN ${min} AND 9007199254740991)`;
  const ref = (path: string, key: string, json = "NEW.event_json") =>
    `(${object(path, 2, json)} AND ${kind(path + ".environmentId", "text", json)} AND ${kind(path + "." + key, "text", json)})`;
  const columns: Record<string, readonly [string, string, boolean?]> = {
    run_id: ["ownership.run.runId", "TEXT"],
    message_id: ["ownership.run.messageId", "TEXT"],
    environment_id: ["environmentId", "TEXT"],
    task_id: ["ownership.run.task.taskId", "TEXT"],
    group_id: ["ownership.run.group.groupId", "TEXT"],
    manager_generation: ["ownership.run.manager.managerGeneration", "INTEGER"],
    manager_thread_id: ["ownership.run.manager.threadId", "TEXT"],
    membership_generation: ["ownership.run.membershipGeneration", "INTEGER"],
    project_id: ["ownership.run.projectId", "TEXT"],
    provider_instance_id: ["ownership.run.providerInstanceId", "TEXT"],
    definition_revision: ["ownership.run.taskRevisions.definitionRevision", "INTEGER"],
    accepted_definition_revision: [
      "ownership.run.taskRevisions.acceptedDefinitionRevision",
      "INTEGER",
    ],
    lifecycle_revision: ["ownership.run.taskRevisions.lifecycleRevision", "INTEGER"],
    completion_policy_revision: ["ownership.run.taskRevisions.completionPolicyRevision", "INTEGER"],
    action_policy_revision: ["ownership.run.taskRevisions.actionPolicyRevision", "INTEGER"],
    graph_revision: ["ownership.run.graphRevision", "INTEGER"],
    control_revision: ["ownership.run.controlRevision", "INTEGER"],
    membership_revision: ["ownership.run.membershipRevision", "INTEGER"],
    recorded_event_id: ["id", "TEXT"],
    recorded_sequence: ["ownership.recorded.sequence", "INTEGER"],
    recorded_command_id: ["commandId", "TEXT"],
    recorded_at: ["occurredAt", "TEXT"],
    native_run_event_id: ["ownership.nativeAcceptance.runEvent.eventId", "TEXT"],
    native_run_sequence: ["ownership.nativeAcceptance.runEvent.sequence", "INTEGER"],
    native_message_event_id: ["ownership.nativeAcceptance.messageEvent.eventId", "TEXT"],
    native_message_sequence: ["ownership.nativeAcceptance.messageEvent.sequence", "INTEGER"],
    native_receipt_command_id: ["ownership.nativeAcceptance.receipt.commandId", "TEXT"],
    native_receipt_result_sequence: [
      "ownership.nativeAcceptance.receipt.resultSequence",
      "INTEGER",
    ],
    origin_type: ["ownership.origin.type", "TEXT"],
    reservation_id: ["ownership.origin.reservationId", "TEXT", true],
    reservation_command_id: ["ownership.origin.reservationCommandId", "TEXT", true],
    allowance_unit: ["ownership.origin.allowanceUnit", "INTEGER", true],
    debit_event_id: ["ownership.origin.debitEvent.eventId", "TEXT", true],
    debit_sequence: ["ownership.origin.debitEvent.sequence", "INTEGER", true],
  };
  const ownerTuple =
    "run_id,debit_event_id,task_id,reservation_command_id,allowance_unit,debit_sequence,definition_revision,lifecycle_revision,completion_policy_revision,action_policy_revision";
  const debitTuple =
    "run_id,event_id,task_id,command_id,allowance_unit,sequence,definition_revision,lifecycle_revision,completion_policy_revision,action_policy_revision";
  yield* sql.unsafe(`CREATE TABLE work_task_accepted_runs (
    ${Object.entries(columns)
      .map(([name, [, type, nullable]]) => `${name} ${type}${nullable ? "" : " NOT NULL"}`)
      .join(",")},
    event_json TEXT NOT NULL CHECK(json_valid(event_json)), PRIMARY KEY(run_id),
    UNIQUE(message_id),UNIQUE(recorded_event_id),UNIQUE(recorded_sequence),UNIQUE(native_run_event_id),UNIQUE(native_message_event_id),
    UNIQUE(reservation_id),UNIQUE(reservation_command_id),UNIQUE(debit_event_id),UNIQUE(task_id,allowance_unit),UNIQUE(${ownerTuple}),
    FOREIGN KEY(task_id) REFERENCES work_task_projections(task_id),
    FOREIGN KEY(task_id,accepted_definition_revision) REFERENCES work_task_accepted_definitions(task_id,definition_revision),
    FOREIGN KEY(environment_id,group_id,manager_generation,manager_thread_id) REFERENCES work_task_group_managers(environment_id,group_id,generation,manager_thread_id),
    FOREIGN KEY(task_id,membership_generation) REFERENCES work_task_group_memberships(task_id,membership_generation),
    FOREIGN KEY(${ownerTuple}) REFERENCES work_task_accepted_run_debits(${debitTuple}) DEFERRABLE INITIALLY DEFERRED
  ) WITHOUT ROWID`);
  yield* sql.unsafe(`CREATE TABLE work_task_accepted_run_debits (
    event_id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL UNIQUE, sequence INTEGER NOT NULL UNIQUE,
    task_id TEXT NOT NULL,command_id TEXT NOT NULL,allowance_unit INTEGER NOT NULL,
    definition_revision INTEGER NOT NULL,lifecycle_revision INTEGER NOT NULL,completion_policy_revision INTEGER NOT NULL,action_policy_revision INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,event_json TEXT NOT NULL CHECK(json_valid(event_json)),
    UNIQUE(task_id,allowance_unit),UNIQUE(${debitTuple}),
    FOREIGN KEY(${debitTuple}) REFERENCES work_task_accepted_runs(${ownerTuple}) DEFERRABLE INITIALLY DEFERRED
  ) WITHOUT ROWID`);
  yield* sql`CREATE INDEX work_task_accepted_debit_command_idx ON work_task_accepted_run_debits(command_id)`;
  yield* sql`CREATE INDEX work_task_accepted_task_idx ON work_task_accepted_runs(task_id,recorded_sequence DESC,run_id)`;
  yield* sql`CREATE INDEX work_task_accepted_definition_idx ON work_task_accepted_runs(task_id,accepted_definition_revision)`;
  yield* sql`CREATE INDEX work_task_accepted_group_idx ON work_task_accepted_runs(environment_id,group_id,recorded_sequence,run_id)`;
  yield* sql`CREATE INDEX work_task_accepted_thread_idx ON work_task_accepted_runs(manager_thread_id,run_id)`;
  yield* sql`CREATE TABLE work_task_accepted_run_references (
    owner_event_id TEXT NOT NULL,accepted_event_id TEXT REFERENCES work_task_accepted_runs(recorded_event_id),debit_event_id TEXT REFERENCES work_task_accepted_run_debits(event_id),
    reference_role TEXT NOT NULL,position INTEGER NOT NULL,event_id TEXT NOT NULL,sequence INTEGER NOT NULL,known_kind TEXT,known_task_id TEXT,
    PRIMARY KEY(owner_event_id,reference_role,position),
    CHECK((accepted_event_id IS NOT NULL AND debit_event_id IS NULL AND owner_event_id=accepted_event_id)
      OR (accepted_event_id IS NULL AND debit_event_id IS NOT NULL AND owner_event_id=debit_event_id))
  ) WITHOUT ROWID`;
  for (const [suffix, keys] of Object.entries({
    id: "event_id,sequence",
    sequence: "sequence,event_id",
    owner: "accepted_event_id",
    debit: "debit_event_id",
  }))
    yield* sql.unsafe(
      `CREATE INDEX work_task_accepted_ref_${suffix}_idx ON work_task_accepted_run_references(${keys})`,
    );
  for (const table of [
    "work_task_accepted_runs",
    "work_task_accepted_run_debits",
    "work_task_accepted_run_references",
  ])
    for (const operation of ["UPDATE", "DELETE"])
      yield* sql.unsafe(
        `CREATE TRIGGER ${table}_${operation.toLowerCase()} BEFORE ${operation} ON ${table} BEGIN SELECT RAISE(ABORT,'immutable accepted ownership'); END`,
      );
  const shape = (path: string, count: number) => object(path, count);
  const root = `(json_type(NEW.event_json) IS 'object' AND (SELECT count(*) FROM json_each(NEW.event_json))=7)`;
  const revisions = [
    "definitionRevision",
    "acceptedDefinitionRevision",
    "lifecycleRevision",
    "completionPolicyRevision",
    "actionPolicyRevision",
  ];
  const run = `(${shape("ownership.run", 13)} AND ${get("ownership.run.version")} IS 2 AND ${kind("ownership.run.version", "integer")}
    AND ${ref("ownership.run.task", "taskId")} AND ${ref("ownership.run.group", "groupId")} AND ${shape("ownership.run.manager", 2)}
    AND ${get("ownership.run.task.environmentId")} IS NEW.environment_id AND ${get("ownership.run.group.environmentId")} IS NEW.environment_id
    AND ${shape("ownership.run.taskRevisions", 5)} AND ${revisions.map((key) => integer("ownership.run.taskRevisions." + key, 1)).join(" AND ")}
    AND NEW.accepted_definition_revision<=NEW.definition_revision
    AND ${["membershipGeneration", "membershipRevision"].map((key) => integer("ownership.run." + key, 1)).join(" AND ")}
    AND ${integer("ownership.run.manager.managerGeneration", 1)} AND ${integer("ownership.run.controlRevision")} AND ${integer("ownership.run.graphRevision")}
    AND length(CAST(NEW.provider_instance_id AS BLOB)) BETWEEN 1 AND 64 AND instr(NEW.provider_instance_id,char(0))=0
    AND substr(NEW.provider_instance_id,1,1) GLOB '[A-Za-z]' AND NEW.provider_instance_id NOT GLOB '*[^A-Za-z0-9_-]*')`;
  const context = `(${shape("ownership.recorded", 4)} AND ${get("ownership.recorded.eventId")} IS NEW.recorded_event_id
    AND ${get("ownership.recorded.commandId")} IS NEW.recorded_command_id AND ${get("ownership.recorded.occurredAt")} IS NEW.recorded_at AND ${integer("ownership.recorded.sequence")})`;
  const native = `(${shape("ownership.nativeAcceptance", 4)} AND ${shape("ownership.nativeAcceptance.receipt", 7)}
    AND ${get("ownership.nativeAcceptance.receipt.status")} IS 'accepted' AND ${kind("ownership.nativeAcceptance.receipt.error", "null")}
    AND ${get("ownership.nativeAcceptance.receipt.threadId")} IS NEW.manager_thread_id AND ${kind("ownership.nativeAcceptance.receipt.commandType", "text")}
    AND ${validTimestampSql(get("ownership.nativeAcceptance.receipt.acceptedAt"))} AND ${integer("ownership.nativeAcceptance.receipt.resultSequence")}
    AND ${["runEvent", "messageEvent"].map((name) => `${shape("ownership.nativeAcceptance." + name, 3)} AND ${integer("ownership.nativeAcceptance." + name + ".sequence")}`).join(" AND ")}
    AND ${get("ownership.nativeAcceptance.runEvent.type")} IS 'run.created' AND ${get("ownership.nativeAcceptance.messageEvent.type")} IS 'message.updated'
    AND ${get("ownership.nativeAcceptance.runStatus")} IN ('queued','preparing','starting')
    AND NEW.native_receipt_result_sequence>=NEW.native_run_sequence AND NEW.native_receipt_result_sequence>=NEW.native_message_sequence)`;
  const pending = `(${shape("ownership.origin.pending", 2)} AND ${kind("ownership.origin.pending.sources", "array")} AND json_array_length(NEW.event_json,'$.ownership.origin.pending.sources')<=32
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.ownership.origin.pending.sources') p WHERE NOT (${object("source", 3, "json_object('source',json(p.value))")}))
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.ownership.origin.pending.sources') p WHERE
      json_type(p.value,'$.kind') IS NOT 'text' OR json_type(p.value,'$.sourceId') IS NOT 'text' OR (json_type(p.value,'$.resultRef') IS NOT 'text' AND json_type(p.value,'$.resultRef') IS NOT 'null'))
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.ownership.origin.pending.sources') GROUP BY hex(json_extract(value,'$.kind')),hex(json_extract(value,'$.sourceId')) HAVING count(*)>1)
    AND (${kind("ownership.origin.pending.overflowThroughGeneration", "null")} OR (${integer("ownership.origin.pending.overflowThroughGeneration", 1)} AND ${get("ownership.origin.pending.overflowThroughGeneration")}<=${get("ownership.origin.observedDirtyGeneration")})))`;
  const origin = `(CASE NEW.origin_type WHEN 'initial_start' THEN ${shape("ownership.origin", 4)} AND ${kind("ownership.origin.startCommandId", "text")}
    AND ${kind("ownership.origin.startEventId", "text")} AND ${integer("ownership.origin.startSequence")}
    AND ${Object.entries(columns)
      .filter(([, v]) => v[2])
      .map(
        ([name, [path]]) => `NEW.${name} IS NULL AND json_type(NEW.event_json,'$.${path}') IS NULL`,
      )
      .join(" AND ")}
    WHEN 'automatic_followup' THEN ${shape("ownership.origin", 8)} AND ${shape("ownership.origin.debitEvent", 10)} AND ${pending}
    AND ${integer("ownership.origin.observedDirtyGeneration", 1)} AND ${integer("ownership.origin.acknowledgedGeneration")}
    AND ${get("ownership.origin.acknowledgedGeneration")}<=${get("ownership.origin.observedDirtyGeneration")}
    AND ${get("ownership.origin.debitEvent.type")} IS 'task.allowance-debited' AND ${get("ownership.origin.debitEvent.taskId")} IS NEW.task_id
    AND ${get("ownership.origin.debitEvent.commandId")} IS NEW.reservation_command_id AND ${get("ownership.origin.debitEvent.allowanceUnit")} IS NEW.allowance_unit AND ${integer("ownership.origin.debitEvent.allowanceUnit", 1)}
    AND ${["definitionRevision", "lifecycleRevision", "completionPolicyRevision", "actionPolicyRevision"].map((key) => `${get("ownership.origin.debitEvent." + key)} IS ${get("ownership.run.taskRevisions." + key)} AND ${integer("ownership.origin.debitEvent." + key, 1)}`).join(" AND ")}
    AND ${Object.entries(columns)
      .filter(([, v]) => v[2])
      .map(
        ([name, [path, type]]) =>
          `NEW.${name} IS NOT NULL AND ${type === "INTEGER" ? integer(path, name === "debit_sequence" ? 0 : 1) : kind(path, "text")}`,
      )
      .join(" AND ")}
    ELSE 0 END)`;
  const prerequisites = `(${kind("ownership.prerequisites", "array")} AND json_array_length(NEW.event_json,'$.ownership.prerequisites')<=64
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.ownership.prerequisites') p WHERE
      json_type(p.value) IS NOT 'object' OR (SELECT count(*) FROM json_each(p.value))<>4 OR json_type(p.value,'$.edgeId') IS NOT 'text'
      OR json_type(p.value,'$.blocker') IS NOT 'object' OR (SELECT count(*) FROM json_each(p.value,'$.blocker'))<>2
      OR json_type(p.value,'$.blocker.environmentId') IS NOT 'text' OR json_extract(p.value,'$.blocker.environmentId') IS NOT NEW.environment_id
      OR json_type(p.value,'$.blocker.taskId') IS NOT 'text' OR json_extract(p.value,'$.blocker.taskId') IS NEW.task_id
      OR json_type(p.value,'$.completionEventId') IS NOT 'text' OR json_type(p.value,'$.completionSequence') IS NOT 'integer'
      OR json_extract(p.value,'$.completionSequence') NOT BETWEEN 0 AND 9007199254740991)
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.ownership.prerequisites') GROUP BY hex(json_extract(value,'$.edgeId')) HAVING count(*)>1)
    AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.ownership.prerequisites') GROUP BY hex(json_extract(value,'$.blocker.taskId')) HAVING count(*)>1))`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_accepted_valid BEFORE INSERT ON work_task_accepted_runs BEGIN
    SELECT CASE WHEN (${canonicalEvidenceJsonSql("NEW.event_json")} AND ${root} AND ${get("type")} IS 'task.run-ownership-accepted'
      AND ${shape("attribution", 1)} AND ${get("attribution.type")} IS 'server' AND ${shape("ownership", 8)}
      AND ${validTimestampSql("NEW.recorded_at")} AND ${run} AND ${context} AND ${native} AND ${origin} AND ${prerequisites}
      AND ${kind("ownership.sourceEventId", "text")} AND ${integer("ownership.sourceSequence")} AND ${integer("ownership.observedWorkSequence")}
      AND ${get("ownership.observedWorkSequence")}<=NEW.recorded_sequence
      AND ${Object.entries(columns)
        .map(
          ([name, [path, type, nullable]]) =>
            `(NEW.${name} IS ${get(path)}${nullable ? "" : ` AND ${type === "INTEGER" ? integer(path) : kind(path, "text")}`})`,
        )
        .join(" AND ")}
      AND EXISTS(SELECT 1 FROM work_task_group_memberships WHERE task_id=NEW.task_id AND membership_generation=NEW.membership_generation AND environment_id=NEW.environment_id AND group_id=NEW.group_id)
    ) IS NOT TRUE THEN RAISE(ABORT,'invalid accepted ownership') END;
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_accepted_runs WHERE run_id=NEW.run_id OR message_id=NEW.message_id OR recorded_event_id=NEW.recorded_event_id OR recorded_sequence=NEW.recorded_sequence
      OR native_run_event_id=NEW.native_run_event_id OR native_message_event_id=NEW.native_message_event_id OR reservation_id=NEW.reservation_id OR reservation_command_id=NEW.reservation_command_id OR debit_event_id=NEW.debit_event_id OR (task_id=NEW.task_id AND allowance_unit=NEW.allowance_unit))
      THEN RAISE(ABORT,'accepted ownership identity conflict') END;
  END`);
  const debitFields: Record<string, string> = {
    event_id: "id",
    task_id: "taskId",
    command_id: "commandId",
    allowance_unit: "allowanceUnit",
    definition_revision: "definitionRevision",
    lifecycle_revision: "lifecycleRevision",
    completion_policy_revision: "completionPolicyRevision",
    action_policy_revision: "actionPolicyRevision",
    occurred_at: "occurredAt",
  };
  const configured = (path: string) =>
    `(${shape(path, 3)} AND ${get(path + ".type")} IS 'configured' AND ${integer(path + ".cap", 1)} AND ${integer(path + ".consumed")} AND ${get(path + ".consumed")}<=${get(path + ".cap")})`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_accepted_debit_valid BEFORE INSERT ON work_task_accepted_run_debits BEGIN
    SELECT CASE WHEN (${canonicalEvidenceJsonSql("NEW.event_json")} AND json_type(NEW.event_json) IS 'object' AND (SELECT count(*) FROM json_each(NEW.event_json))=13
      AND ${get("type")} IS 'task.allowance-debited' AND ${shape("attribution", 1)} AND ${get("attribution.type")} IS 'server'
      AND ${configured("previous")} AND ${configured("resulting")} AND ${get("previous.cap")}=${get("resulting.cap")}
      AND ${get("resulting.consumed")}=${get("previous.consumed")}+1 AND NEW.allowance_unit=${get("resulting.consumed")}
      AND ${validTimestampSql("NEW.occurred_at")} AND typeof(NEW.sequence)='integer' AND NEW.sequence BETWEEN 0 AND 9007199254740991
      AND ${Object.entries(debitFields)
        .map(
          ([name, path]) =>
            `(NEW.${name} IS ${get(path)} AND ${["event_id", "task_id", "command_id", "occurred_at"].includes(name) ? kind(path, "text") : integer(path, 1)})`,
        )
        .join(" AND ")}
    ) IS NOT TRUE THEN RAISE(ABORT,'invalid accepted debit evidence') END;
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_accepted_run_debits WHERE event_id=NEW.event_id OR run_id=NEW.run_id OR sequence=NEW.sequence OR (task_id=NEW.task_id AND allowance_unit=NEW.allowance_unit)) THEN RAISE(ABORT,'accepted debit identity conflict') END;
  END`);
  const descriptors = (json: string, sequence: string, accepted: boolean) => {
    const task = get(accepted ? "ownership.run.task.taskId" : "taskId", json);
    const own = `SELECT 'recorded' role,0 position,${get("id", json)} event_id,${sequence} sequence,'${accepted ? "task.run-ownership-accepted" : "task.allowance-debited"}' known_kind,${task} known_task_id`;
    if (!accepted) return own;
    return `${own}
      UNION ALL SELECT 'native_run',0,${get("ownership.nativeAcceptance.runEvent.eventId", json)},${get("ownership.nativeAcceptance.runEvent.sequence", json)},'run.created',${task}
      UNION ALL SELECT 'native_message',0,${get("ownership.nativeAcceptance.messageEvent.eventId", json)},${get("ownership.nativeAcceptance.messageEvent.sequence", json)},'message.updated',${task}
      UNION ALL SELECT 'source',0,${get("ownership.sourceEventId", json)},${get("ownership.sourceSequence", json)},NULL,NULL
      UNION ALL SELECT 'origin',0,CASE ${get("ownership.origin.type", json)} WHEN 'initial_start' THEN ${get("ownership.origin.startEventId", json)} ELSE ${get("ownership.origin.debitEvent.eventId", json)} END,
        CASE ${get("ownership.origin.type", json)} WHEN 'initial_start' THEN ${get("ownership.origin.startSequence", json)} ELSE ${get("ownership.origin.debitEvent.sequence", json)} END,
        CASE ${get("ownership.origin.type", json)} WHEN 'automatic_followup' THEN 'task.allowance-debited' END,${task}
      UNION ALL SELECT 'prerequisite',p.key,json_extract(p.value,'$.completionEventId'),json_extract(p.value,'$.completionSequence'),'task.completed',json_extract(p.value,'$.blocker.taskId') FROM json_each(${json},'$.ownership.prerequisites') p`;
  };
  yield* sql.unsafe(`CREATE TRIGGER work_task_accepted_reference_valid BEFORE INSERT ON work_task_accepted_run_references BEGIN
    SELECT CASE WHEN NOT EXISTS(
      SELECT 1 FROM (${descriptors("(SELECT event_json FROM work_task_accepted_runs WHERE recorded_event_id=NEW.accepted_event_id)", "(SELECT recorded_sequence FROM work_task_accepted_runs WHERE recorded_event_id=NEW.accepted_event_id)", true)}) d
      WHERE NEW.accepted_event_id IS NOT NULL AND d.role=NEW.reference_role AND d.position=NEW.position AND d.event_id IS NEW.event_id AND d.sequence IS NEW.sequence AND d.known_kind IS NEW.known_kind AND d.known_task_id IS NEW.known_task_id
    ) AND NOT EXISTS(SELECT 1 FROM work_task_accepted_run_debits a WHERE a.event_id=NEW.debit_event_id AND NEW.reference_role='recorded' AND NEW.position=0 AND a.event_id IS NEW.event_id AND a.sequence IS NEW.sequence AND NEW.known_kind IS 'task.allowance-debited' AND a.task_id IS NEW.known_task_id)
      THEN RAISE(ABORT,'invalid accepted reference') END;
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_accepted_run_references WHERE owner_event_id=NEW.owner_event_id AND reference_role=NEW.reference_role AND position=NEW.position)
      OR EXISTS(SELECT 1 FROM work_task_accepted_run_references r WHERE r.event_id=NEW.event_id AND (r.sequence<>NEW.sequence OR (r.known_kind IS NOT NULL AND NEW.known_kind IS NOT NULL AND r.known_kind<>NEW.known_kind) OR (r.known_task_id IS NOT NULL AND NEW.known_task_id IS NOT NULL AND r.known_task_id<>NEW.known_task_id)))
      OR EXISTS(SELECT 1 FROM work_task_accepted_run_references r WHERE r.sequence=NEW.sequence AND r.event_id<>NEW.event_id)
      THEN RAISE(ABORT,'accepted reference identity conflict') END;
  END`);
  for (const [table, accepted] of [
    ["work_task_accepted_runs", true],
    ["work_task_accepted_run_debits", false],
  ] as const) {
    const id = accepted ? "NEW.recorded_event_id" : "NEW.event_id";
    const seq = accepted ? "NEW.recorded_sequence" : "NEW.sequence";
    yield* sql.unsafe(`CREATE TRIGGER ${table}_references AFTER INSERT ON ${table} BEGIN
      ${accepted ? `SELECT CASE WHEN EXISTS(SELECT 1 FROM (${descriptors("NEW.event_json", seq, true)}) WHERE role<>'recorded' AND (event_id=${id} OR sequence>=${seq})) THEN RAISE(ABORT,'accepted reference order') END;` : ""}
      INSERT INTO work_task_accepted_run_references(owner_event_id,accepted_event_id,debit_event_id,reference_role,position,event_id,sequence,known_kind,known_task_id)
        SELECT ${id},${accepted ? id : "NULL"},${accepted ? "NULL" : id},role,position,event_id,sequence,known_kind,known_task_id FROM (${descriptors("NEW.event_json", seq, accepted)});
    END`);
    const command = accepted ? "NEW.reservation_command_id" : "NEW.command_id";
    yield* sql.unsafe(`CREATE TRIGGER ${table}_legacy BEFORE INSERT ON ${table} BEGIN
      SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_wake_reservations WHERE run_id=NEW.run_id OR command_id=${command} OR (task_id=NEW.task_id AND allowance_unit=NEW.allowance_unit)${accepted ? " OR message_id=NEW.message_id OR reservation_id=NEW.reservation_id" : ""})
        OR EXISTS(SELECT 1 FROM work_task_validation_identity WHERE task_id=NEW.task_id AND allowance_unit=NEW.allowance_unit) THEN RAISE(ABORT,'accepted legacy identity conflict') END;
    END`);
  }
  for (const [table, wake] of [
    ["work_task_wake_reservations", true],
    ["work_task_validation_identity", false],
  ] as const)
    for (const op of ["INSERT", "UPDATE"])
      yield* sql.unsafe(`CREATE TRIGGER ${table}_accepted_${op.toLowerCase()} BEFORE ${op} ON ${table} BEGIN
        SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_accepted_runs WHERE (task_id=NEW.task_id AND allowance_unit=NEW.allowance_unit)${wake ? " OR run_id=NEW.run_id OR message_id=NEW.message_id OR reservation_id=NEW.reservation_id OR reservation_command_id=NEW.command_id" : ""})
          OR EXISTS(SELECT 1 FROM work_task_accepted_run_debits WHERE (task_id=NEW.task_id AND allowance_unit=NEW.allowance_unit)${wake ? " OR run_id=NEW.run_id OR command_id=NEW.command_id" : ""}) THEN RAISE(ABORT,'legacy accepted identity conflict') END;
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
    yield* sql.unsafe(`CREATE TRIGGER work_task_accepted_definition_${op.toLowerCase()} BEFORE ${op} ON work_task_accepted_definitions BEGIN
      SELECT CASE WHEN ${op === "UPDATE" ? `(EXISTS(SELECT 1 FROM work_task_accepted_runs WHERE task_id=OLD.task_id AND accepted_definition_revision=OLD.definition_revision) AND (${definitionKeys.map((key) => `NEW.${key} IS NOT OLD.${key}`).join(" OR ")})) OR ` : ""}
        EXISTS(SELECT 1 FROM work_task_accepted_definitions d JOIN work_task_accepted_runs a ON a.task_id=d.task_id AND a.accepted_definition_revision=d.definition_revision
          WHERE ((d.task_id=NEW.task_id AND d.definition_revision=NEW.definition_revision) OR d.accepted_event_id=NEW.accepted_event_id)${op === "UPDATE" ? " AND NOT(d.task_id=OLD.task_id AND d.definition_revision=OLD.definition_revision)" : ""})
        THEN RAISE(ABORT,'accepted definition identity conflict') END;
    END`);
});
