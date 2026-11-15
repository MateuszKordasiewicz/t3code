import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import {
  canonicalEvidenceJsonSql,
  canonicalTextSql,
  structuralJsonEqualSql,
  validTimestampSql,
  timestampAtOrBeforeSql,
} from "../workTaskSqlV1.ts";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE work_task_group_memberships (
    task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
    membership_generation INTEGER NOT NULL CHECK(typeof(membership_generation)='integer' AND membership_generation BETWEEN 1 AND 9007199254740991),
    environment_id TEXT NOT NULL, group_id TEXT NOT NULL,
    accepted_definition_revision INTEGER NOT NULL,
    joined_membership_revision INTEGER NOT NULL,
    joined_event_id TEXT NOT NULL, joined_record_json TEXT NOT NULL CHECK(json_valid(joined_record_json)), joined_companion_id TEXT NOT NULL UNIQUE,
    retired_membership_revision INTEGER, retired_event_id TEXT, retired_record_json TEXT, retired_companion_id TEXT UNIQUE,
    PRIMARY KEY(task_id,membership_generation),
    UNIQUE(task_id,membership_generation,joined_companion_id), UNIQUE(task_id,membership_generation,retired_companion_id),
    FOREIGN KEY(task_id,accepted_definition_revision) REFERENCES work_task_accepted_definitions(task_id,definition_revision),
    FOREIGN KEY(environment_id,group_id) REFERENCES work_task_manager_groups(environment_id,group_id),
    FOREIGN KEY(joined_companion_id) REFERENCES work_task_membership_companions(event_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(retired_companion_id) REFERENCES work_task_membership_companions(event_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK((retired_membership_revision IS NULL AND retired_event_id IS NULL AND retired_record_json IS NULL AND retired_companion_id IS NULL)
      OR (retired_membership_revision IS NOT NULL AND retired_event_id IS NOT NULL AND retired_record_json IS NOT NULL AND retired_companion_id IS NOT NULL AND json_valid(retired_record_json)))
  ) WITHOUT ROWID`;
  yield* sql`CREATE UNIQUE INDEX work_task_membership_current_idx ON work_task_group_memberships(task_id) WHERE retired_event_id IS NULL`;
  yield* sql`CREATE INDEX work_task_group_members_current_idx ON work_task_group_memberships(environment_id,group_id,task_id) WHERE retired_event_id IS NULL`;
  yield* sql`CREATE INDEX work_task_membership_history_idx ON work_task_group_memberships(task_id,membership_generation DESC)`;
  yield* sql`CREATE TABLE work_task_membership_companions (
    event_id TEXT PRIMARY KEY NOT NULL, sequence INTEGER NOT NULL UNIQUE CHECK(typeof(sequence)='integer' AND sequence BETWEEN 0 AND 9007199254740991),
    environment_id TEXT NOT NULL, group_id TEXT NOT NULL, task_id TEXT NOT NULL REFERENCES work_task_projections(task_id),
    membership_generation INTEGER NOT NULL CHECK(typeof(membership_generation)='integer' AND membership_generation BETWEEN 1 AND 9007199254740991),
    role TEXT NOT NULL CHECK(role IN ('join','retire')), root_event_id TEXT NOT NULL,
    root_sequence INTEGER NOT NULL CHECK(typeof(root_sequence)='integer' AND root_sequence BETWEEN 0 AND 9007199254740991),
    root_event_json TEXT NOT NULL CHECK(json_valid(root_event_json)), event_json TEXT NOT NULL CHECK(json_valid(event_json)),
    joined_generation INTEGER, retired_generation INTEGER, peer_event_id TEXT,
    FOREIGN KEY(environment_id,group_id) REFERENCES work_task_manager_groups(environment_id,group_id),
    FOREIGN KEY(task_id,joined_generation,event_id) REFERENCES work_task_group_memberships(task_id,membership_generation,joined_companion_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(task_id,retired_generation,event_id) REFERENCES work_task_group_memberships(task_id,membership_generation,retired_companion_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(peer_event_id) REFERENCES work_task_membership_companions(event_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK((role='join' AND joined_generation IS NOT NULL AND joined_generation=membership_generation AND retired_generation IS NULL)
      OR (role='retire' AND retired_generation IS NOT NULL AND retired_generation=membership_generation AND joined_generation IS NULL)),
    CHECK(root_sequence<>sequence AND root_event_id<>event_id AND (peer_event_id IS NULL OR peer_event_id<>event_id))
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_membership_peer_idx ON work_task_membership_companions(peer_event_id)`;
  yield* sql`CREATE INDEX work_task_membership_root_idx ON work_task_membership_companions(root_event_id,root_sequence)`;
  yield* sql`CREATE INDEX work_task_membership_root_sequence_idx ON work_task_membership_companions(root_sequence)`;
  yield* sql`CREATE INDEX work_task_membership_group_history_idx ON work_task_membership_companions(environment_id,group_id,sequence DESC)`;
  yield* sql`ALTER TABLE work_task_manager_groups ADD COLUMN latest_membership_event_id TEXT REFERENCES work_task_membership_companions(event_id) DEFERRABLE INITIALLY DEFERRED`;
  const get = (json: string, path: string) => `json_extract(${json},'${path}')`;
  const kind = (json: string, path: string, type: string) =>
    `json_type(${json},'${path}') IS '${type}'`;
  const object = (json: string, path: string, count: number) =>
    `(${kind(json, path, "object")} AND (SELECT count(*) FROM json_each(${json},'${path}'))=${count})`;
  const integer = (json: string, path: string, min = 0) =>
    `(${kind(json, path, "integer")} AND ${get(json, path)} BETWEEN ${min} AND 9007199254740991)`;
  const text = (json: string, path: string) => kind(json, path, "text");
  const same = (left: string, right: string) =>
    structuralJsonEqualSql(`coalesce(${left},'null')`, `coalesce(${right},'null')`);
  const ref = (json: string, path: string, key: string) =>
    `(${object(json, path, 2)} AND ${text(json, path + ".environmentId")} AND ${text(json, path + "." + key)})`;
  const principal = (json: string, path: string) =>
    `(${object(json, path, 2)} AND ${text(json, path + ".subject")} AND ${text(json, path + ".sessionId")})`;
  const provenance = (json: string, path: string) =>
    `(${object(json, path, 4)} AND ${text(json, path + ".eventId")} AND ${text(json, path + ".commandId")} AND ${principal(json, path + ".performedBy")} AND ${validTimestampSql(get(json, path + ".occurredAt"))})`;
  const control = (
    json: string,
    path: string,
  ) => `(${object(json, path, 6)} AND ${ref(json, path + ".group", "groupId")}
    AND ${integer(json, path + ".controlRevision")} AND ${integer(json, path + ".membershipRevision")} AND ${integer(json, path + ".highestManagerGeneration")}
    AND ${get(json, path + ".dispatchHold")} IN ('none','paused','stopped')
    AND (${kind(json, path + ".currentManager", "null")} OR (${object(json, path + ".currentManager", 2)} AND ${text(json, path + ".currentManager.threadId")}
      AND ${integer(json, path + ".currentManager.managerGeneration", 1)} AND ${get(json, path + ".currentManager.managerGeneration")} IS ${get(json, path + ".highestManagerGeneration")})))`;
  const columnControl = (
    json: string,
    path: string,
    row: string,
  ) => `(${get(json, path + ".group.environmentId")} IS ${row}.environment_id AND ${get(json, path + ".group.groupId")} IS ${row}.group_id
    AND ${get(json, path + ".controlRevision")} IS ${row}.control_revision AND ${get(json, path + ".membershipRevision")} IS ${row}.membership_revision
    AND ${get(json, path + ".highestManagerGeneration")} IS ${row}.highest_manager_generation AND ${get(json, path + ".dispatchHold")} IS ${row}.dispatch_hold
    AND ${get(json, path + ".currentManager.managerGeneration")} IS ${row}.current_manager_generation AND ${get(json, path + ".currentManager.threadId")} IS ${row}.current_manager_thread_id)`;
  const member = (
    json: string,
    path: string,
    retired = false,
  ) => `(${object(json, path, retired ? 9 : 7)} AND ${ref(json, path + ".task", "taskId")} AND ${ref(json, path + ".group", "groupId")}
    AND ${get(json, path + ".task.environmentId")} IS ${get(json, path + ".group.environmentId")} AND ${integer(json, path + ".membershipGeneration", 1)}
    AND ${integer(json, path + ".acceptedDefinitionRevisionAtJoin", 1)} AND ${integer(json, path + ".joinedMembershipRevision", 1)} AND ${provenance(json, path + ".joined")}
    AND ${get(json, path + ".state")} IS '${retired ? "retired" : "current"}' ${
      retired
        ? `AND ${integer(json, path + ".retiredMembershipRevision", 1)} AND ${get(json, path + ".retiredMembershipRevision")}>${get(json, path + ".joinedMembershipRevision")}
      AND ${provenance(json, path + ".retired")} AND ${timestampAtOrBeforeSql(get(json, path + ".joined.occurredAt"), get(json, path + ".retired.occurredAt"))}`
        : ""
    })`;
  const snapshot = (
    json: string,
    path: string,
  ) => `(${object(json, path, 4)} AND ${ref(json, path + ".task", "taskId")} AND ${integer(json, path + ".highestMembershipGeneration")}
    AND ${object(json, path + ".taskRevisions", 5)} AND ${["definitionRevision", "lifecycleRevision", "actionPolicyRevision", "completionPolicyRevision"].map((key) => integer(json, path + ".taskRevisions." + key, 1)).join(" AND ")}
    AND (${kind(json, path + ".taskRevisions.acceptedDefinitionRevision", "null")} OR (${integer(json, path + ".taskRevisions.acceptedDefinitionRevision", 1)} AND ${get(json, path + ".taskRevisions.acceptedDefinitionRevision")}<=${get(json, path + ".taskRevisions.definitionRevision")}))
    AND (${get(json, path + ".highestMembershipGeneration")}=0 OR ${get(json, path + ".taskRevisions.acceptedDefinitionRevision")} IS NOT NULL)
    AND (${kind(json, path + ".currentMembership", "null")} OR (${member(json, path + ".currentMembership")} AND ${same(get(json, path + ".task"), get(json, path + ".currentMembership.task"))}
      AND ${get(json, path + ".highestMembershipGeneration")} IS ${get(json, path + ".currentMembership.membershipGeneration")} AND ${get(json, path + ".currentMembership.acceptedDefinitionRevisionAtJoin")}<=${get(json, path + ".taskRevisions.acceptedDefinitionRevision")})))`;
  const baseEvent = (
    json: string,
  ) => `(${kind(json, "$", "object")} AND ${canonicalEvidenceJsonSql(json)} AND ${text(json, "$.id")} AND ${text(json, "$.environmentId")}
    AND ${text(json, "$.commandId")} AND ${validTimestampSql(get(json, "$.occurredAt"))} AND ${principal(json, "$.performedBy")})`;
  const membershipChange = (
    json: string,
    previous: string,
    current: string,
  ) => `(${control(json, previous)} AND ${control(json, current)}
    AND ${same(get(json, previous + ".group"), get(json, current + ".group"))} AND ${get(json, current + ".membershipRevision")} IS ${get(json, previous + ".membershipRevision")}+1
    AND ${["controlRevision", "highestManagerGeneration", "dispatchHold"].map((key) => `${get(json, current + "." + key)} IS ${get(json, previous + "." + key)}`).join(" AND ")}
    AND ${same(get(json, current + ".currentManager"), get(json, previous + ".currentManager"))})`;
  const root = "NEW.root_event_json",
    event = "NEW.event_json";
  const previous = "$.previousSnapshot.currentMembership",
    destination = "$.snapshot.currentMembership";
  const sourceExists = `${get(root, previous)} IS NOT NULL`,
    destinationExists = `${get(root, destination)} IS NOT NULL`;
  const rootProvenance = (
    path: string,
  ) => `(${get(root, path + ".eventId")} IS NEW.root_event_id AND ${get(root, path + ".commandId")} IS ${get(root, "$.commandId")}
    AND ${get(root, path + ".occurredAt")} IS ${get(root, "$.occurredAt")} AND ${same(get(root, path + ".performedBy"), get(root, "$.performedBy"))})`;
  const rootShape = `(${baseEvent(root)} AND ${get(root, "$.type")} IS 'task.membership-changed'
    AND (SELECT count(*) FROM json_each(${root}))=10+(${kind(root, "$.reason", "text")}) AND (json_type(${root},'$.reason') IS NULL OR ${text(root, "$.reason")})
    AND ${snapshot(root, "$.previousSnapshot")} AND ${snapshot(root, "$.snapshot")}
    AND ${same(get(root, "$.previousSnapshot.task"), get(root, "$.snapshot.task"))} AND ${same(get(root, "$.previousSnapshot.taskRevisions"), get(root, "$.snapshot.taskRevisions"))}
    AND ${get(root, "$.snapshot.task.environmentId")} IS NEW.environment_id AND ${get(root, "$.snapshot.task.taskId")} IS NEW.task_id
    AND (${sourceExists} OR ${destinationExists}) AND NOT (${sourceExists} AND ${destinationExists} AND ${get(root, previous + ".group.groupId")} IS ${get(root, destination + ".group.groupId")})
    AND ${get(root, "$.snapshot.highestMembershipGeneration")} IS ${get(root, "$.previousSnapshot.highestMembershipGeneration")}+(${destinationExists})
    AND ${kind(root, "$.groupTransitions", "array")} AND json_array_length(${root},'$.groupTransitions')=(${sourceExists})+(${destinationExists})
    AND NOT EXISTS(SELECT 1 FROM json_each(${root},'$.groupTransitions') t WHERE NOT (${object("t.value", "$", 3)} AND ${text("t.value", "$.companionEventId")}
      AND ${membershipChange("t.value", "$.previousControl", "$.control")} AND ${get("t.value", "$.control.group.environmentId")} IS NEW.environment_id
      AND ${get("t.value", "$.companionEventId")}<>NEW.root_event_id AND ${get("t.value", "$.control.group.groupId")} IN (${get(root, previous + ".group.groupId")},${get(root, destination + ".group.groupId")})) IS TRUE)
    AND NOT EXISTS(SELECT 1 FROM json_each(${root},'$.groupTransitions') GROUP BY json_extract(value,'$.companionEventId') HAVING count(*)>1)
    AND NOT EXISTS(SELECT 1 FROM json_each(${root},'$.groupTransitions') GROUP BY json_extract(value,'$.control.group.groupId') HAVING count(*)>1)
    AND CASE WHEN ${sourceExists} THEN ${member(root, "$.retiredMembership", true)} AND ${rootProvenance("$.retiredMembership.retired")}
      AND ${same(`json_remove(json_set(${get(root, "$.retiredMembership")},'$.state','current'),'$.retiredMembershipRevision','$.retired')`, get(root, previous))}
      AND NEW.root_event_id IS NOT ${get(root, previous + ".joined.eventId")}
      AND NOT EXISTS(SELECT 1 FROM json_each(${root},'$.groupTransitions') WHERE json_extract(value,'$.companionEventId') IS ${get(root, previous + ".joined.eventId")})
      AND EXISTS(SELECT 1 FROM json_each(${root},'$.groupTransitions') WHERE json_extract(value,'$.control.group.groupId') IS ${get(root, previous + ".group.groupId")} AND json_extract(value,'$.control.membershipRevision') IS ${get(root, "$.retiredMembership.retiredMembershipRevision")})
      ELSE ${kind(root, "$.retiredMembership", "null")} END
    AND CASE WHEN ${destinationExists} THEN ${rootProvenance(destination + ".joined")} AND ${get(root, destination + ".acceptedDefinitionRevisionAtJoin")} IS ${get(root, "$.snapshot.taskRevisions.acceptedDefinitionRevision")}
      AND EXISTS(SELECT 1 FROM json_each(${root},'$.groupTransitions') WHERE json_extract(value,'$.control.group.groupId') IS ${get(root, destination + ".group.groupId")} AND json_extract(value,'$.control.membershipRevision') IS ${get(root, destination + ".joinedMembershipRevision")}) ELSE 1 END) IS TRUE`;
  const companionShape = `(${baseEvent(event)} AND (SELECT count(*) FROM json_each(${event}))=10 AND ${get(event, "$.type")} IS 'task-group.membership-changed'
    AND ${get(event, "$.id")} IS NEW.event_id AND ${get(event, "$.environmentId")} IS NEW.environment_id AND ${get(event, "$.rootEventId")} IS NEW.root_event_id
    AND ${get(root, "$.id")} IS NEW.root_event_id AND ${get(root, "$.environmentId")} IS NEW.environment_id
    AND ${same(get(event, "$.task"), get(root, "$.snapshot.task"))}
    AND ${["commandId", "occurredAt"].map((key) => `${get(event, "$." + key)} IS ${get(root, "$." + key)}`).join(" AND ")} AND ${same(get(event, "$.performedBy"), get(root, "$.performedBy"))}
    AND ${get(event, "$.control.group.groupId")} IS NEW.group_id
    AND EXISTS(SELECT 1 FROM json_each(${root},'$.groupTransitions') t WHERE json_extract(t.value,'$.companionEventId') IS NEW.event_id
      AND ${same(get("t.value", "$.previousControl"), get(event, "$.previousControl"))} AND ${same(get("t.value", "$.control"), get(event, "$.control"))})
    AND NEW.role IS CASE WHEN ${get(root, destination + ".group.groupId")} IS NEW.group_id THEN 'join' ELSE 'retire' END
    AND NEW.membership_generation IS CASE NEW.role WHEN 'join' THEN ${get(root, destination + ".membershipGeneration")} ELSE ${get(root, previous + ".membershipGeneration")} END
    AND NEW.peer_event_id IS (SELECT json_extract(value,'$.companionEventId') FROM json_each(${root},'$.groupTransitions') WHERE json_extract(value,'$.companionEventId')<>NEW.event_id)) IS TRUE`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_membership_companion_insert BEFORE INSERT ON work_task_membership_companions
    WHEN NOT (${rootShape}) OR NOT (${companionShape})
      OR EXISTS(SELECT 1 FROM work_task_membership_companions WHERE event_id=NEW.event_id OR sequence=NEW.sequence)
      OR NOT EXISTS(SELECT 1 FROM work_task_manager_groups g WHERE g.environment_id=NEW.environment_id AND g.group_id=NEW.group_id AND g.latest_membership_event_id=NEW.event_id
        AND g.control_sequence=NEW.sequence AND ${same("g.control_event_json", event)})
      OR NOT EXISTS(SELECT 1 FROM work_task_projections p WHERE p.task_id=NEW.task_id
        AND p.definition_revision IS ${get(root, "$.snapshot.taskRevisions.definitionRevision")} AND p.lifecycle_revision IS ${get(root, "$.snapshot.taskRevisions.lifecycleRevision")}
        AND p.action_policy_revision IS ${get(root, "$.snapshot.taskRevisions.actionPolicyRevision")} AND p.completion_policy_revision IS ${get(root, "$.snapshot.taskRevisions.completionPolicyRevision")})
      OR ${get(root, "$.snapshot.taskRevisions.acceptedDefinitionRevision")} IS NOT (SELECT max(definition_revision) FROM work_task_accepted_definitions WHERE task_id=NEW.task_id)
      OR ${get(root, "$.previousSnapshot.highestMembershipGeneration")} IS NOT (SELECT coalesce(max(membership_generation),0) FROM work_task_group_memberships WHERE task_id=NEW.task_id)
      OR CASE WHEN ${sourceExists} THEN NOT EXISTS(SELECT 1 FROM work_task_group_memberships m WHERE m.task_id=NEW.task_id AND m.retired_event_id IS NULL AND ${same("m.joined_record_json", get(root, previous))})
        ELSE EXISTS(SELECT 1 FROM work_task_group_memberships WHERE task_id=NEW.task_id AND retired_event_id IS NULL) END
      OR EXISTS(SELECT 1 FROM work_task_membership_companions p WHERE (p.event_id=NEW.peer_event_id OR p.peer_event_id=NEW.event_id)
        AND (p.event_id IS NOT NEW.peer_event_id OR p.peer_event_id IS NOT NEW.event_id OR p.role=NEW.role OR p.group_id=NEW.group_id OR p.task_id<>NEW.task_id OR p.environment_id<>NEW.environment_id
          OR p.root_event_id<>NEW.root_event_id OR p.root_sequence<>NEW.root_sequence OR NOT ${same("p.root_event_json", root)}))
    BEGIN SELECT RAISE(ABORT,'membership companion or task snapshot conflict'); END`);
  for (const operation of ["UPDATE", "DELETE"]) {
    yield* sql.unsafe(
      `CREATE TRIGGER work_task_membership_companion_${operation.toLowerCase()} BEFORE ${operation} ON work_task_membership_companions BEGIN SELECT RAISE(ABORT,'immutable membership companion'); END`,
    );
  }
  const memberColumns = (
    json: string,
  ) => `(${get(json, "$.task.taskId")} IS NEW.task_id AND ${get(json, "$.task.environmentId")} IS NEW.environment_id AND ${get(json, "$.group.groupId")} IS NEW.group_id
    AND ${get(json, "$.membershipGeneration")} IS NEW.membership_generation AND ${get(json, "$.acceptedDefinitionRevisionAtJoin")} IS NEW.accepted_definition_revision
    AND ${get(json, "$.joinedMembershipRevision")} IS NEW.joined_membership_revision AND ${get(json, "$.joined.eventId")} IS NEW.joined_event_id)`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_membership_insert BEFORE INSERT ON work_task_group_memberships
    WHEN NOT (${canonicalEvidenceJsonSql("NEW.joined_record_json")} AND ${member("NEW.joined_record_json", "$")} AND ${memberColumns("NEW.joined_record_json")}) IS TRUE
      OR NEW.accepted_definition_revision IS NOT (SELECT max(definition_revision) FROM work_task_accepted_definitions WHERE task_id=NEW.task_id)
      OR NEW.retired_event_id IS NOT NULL OR NEW.membership_generation IS NOT (SELECT coalesce(max(membership_generation),0)+1 FROM work_task_group_memberships WHERE task_id=NEW.task_id)
      OR EXISTS(SELECT 1 FROM work_task_group_memberships WHERE task_id=NEW.task_id AND retired_event_id IS NULL)
      OR NOT EXISTS(SELECT 1 FROM work_task_membership_companions c WHERE c.event_id=NEW.joined_companion_id AND c.role='join' AND c.task_id=NEW.task_id AND c.membership_generation=NEW.membership_generation
        AND c.environment_id=NEW.environment_id AND c.group_id=NEW.group_id AND c.root_event_id=NEW.joined_event_id AND ${same("NEW.joined_record_json", get("c.root_event_json", "$.snapshot.currentMembership"))})
    BEGIN SELECT RAISE(ABORT,'membership join conflict'); END`);
  yield* sql.unsafe(`CREATE TRIGGER work_task_membership_update BEFORE UPDATE ON work_task_group_memberships
    WHEN OLD.retired_event_id IS NOT NULL OR NEW.retired_event_id IS NULL
      OR ${["task_id", "membership_generation", "environment_id", "group_id", "accepted_definition_revision", "joined_membership_revision", "joined_event_id", "joined_record_json", "joined_companion_id"].map((key) => `NEW.${key} IS NOT OLD.${key}`).join(" OR ")}
      OR NOT (${canonicalEvidenceJsonSql("NEW.retired_record_json")} AND ${member("NEW.retired_record_json", "$", true)} AND ${memberColumns("NEW.retired_record_json")}
        AND ${get("NEW.retired_record_json", "$.retiredMembershipRevision")} IS NEW.retired_membership_revision AND ${get("NEW.retired_record_json", "$.retired.eventId")} IS NEW.retired_event_id) IS TRUE
      OR NOT EXISTS(SELECT 1 FROM work_task_membership_companions c WHERE c.event_id=NEW.retired_companion_id AND c.role='retire' AND c.task_id=NEW.task_id AND c.membership_generation=NEW.membership_generation
        AND c.environment_id=NEW.environment_id AND c.group_id=NEW.group_id AND c.root_event_id=NEW.retired_event_id AND ${same("NEW.retired_record_json", get("c.root_event_json", "$.retiredMembership"))})
    BEGIN SELECT RAISE(ABORT,'membership retirement conflict'); END`);
  yield* sql`CREATE TRIGGER work_task_membership_delete BEFORE DELETE ON work_task_group_memberships BEGIN SELECT RAISE(ABORT,'immutable membership history'); END`;

  const eventShape = `(${baseEvent("NEW.control_event_json")} AND ${get("NEW.control_event_json", "$.id")} IS NEW.control_event_id
    AND ${get("NEW.control_event_json", "$.environmentId")} IS NEW.environment_id AND ${control("NEW.control_event_json", "$.control")} AND ${columnControl("NEW.control_event_json", "$.control", "NEW")}
    AND CASE ${get("NEW.control_event_json", "$.type")}
      WHEN 'task-group.created' THEN (SELECT count(*) FROM json_each(NEW.control_event_json))=7
      WHEN 'task-group.membership-changed' THEN (SELECT count(*) FROM json_each(NEW.control_event_json))=10 AND ${ref("NEW.control_event_json", "$.task", "taskId")}
        AND ${get("NEW.control_event_json", "$.task.environmentId")} IS NEW.environment_id AND ${text("NEW.control_event_json", "$.rootEventId")}
        AND ${get("NEW.control_event_json", "$.rootEventId")} IS NOT NEW.control_event_id AND ${membershipChange("NEW.control_event_json", "$.previousControl", "$.control")}
      WHEN 'task-group.manager-changed' THEN ${control("NEW.control_event_json", "$.previousControl")} AND (SELECT count(*) FROM json_each(NEW.control_event_json))=8+(${kind("NEW.control_event_json", "$.reason", "text")})
      WHEN 'task-group.hold-changed' THEN ${control("NEW.control_event_json", "$.previousControl")} AND (SELECT count(*) FROM json_each(NEW.control_event_json))=8+(${kind("NEW.control_event_json", "$.reason", "text")})
      ELSE 0 END AND (json_type(NEW.control_event_json,'$.reason') IS NULL OR ${text("NEW.control_event_json", "$.reason")})) IS TRUE`;
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`DROP TRIGGER work_task_group_membership_closed_${operation.toLowerCase()}`);
    yield* sql.unsafe(`DROP TRIGGER work_task_group_canonical_${operation.toLowerCase()}`);
    yield* sql.unsafe(`CREATE TRIGGER work_task_group_canonical_${operation.toLowerCase()} BEFORE ${operation} ON work_task_manager_groups
      WHEN NOT (${eventShape}) OR NOT ${canonicalTextSql("NEW.environment_id")} OR NOT ${canonicalTextSql("NEW.group_id")}
      BEGIN SELECT RAISE(ABORT,'invalid canonical group control'); END`);
  }
  yield* sql`CREATE TRIGGER work_task_group_membership_initial BEFORE INSERT ON work_task_manager_groups
    WHEN NEW.membership_revision<>0 OR NEW.latest_membership_event_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'group membership starts empty'); END`;
  const highWater = `(SELECT coalesce(max(generation),0) FROM work_task_group_managers WHERE environment_id=NEW.environment_id AND group_id=NEW.group_id)`;
  const knownEvent = (
    id: string,
    sequence: string,
  ) => `EXISTS(SELECT 1 FROM work_task_manager_groups WHERE created_event_id=${id} OR control_event_id=${id} OR created_sequence=${sequence} OR control_sequence=${sequence})
    OR EXISTS(SELECT 1 FROM work_task_group_managers WHERE bound_event_id=${id} OR bound_sequence=${sequence} OR retired_event_id=${id} OR retired_sequence=${sequence})`;
  yield* sql`DROP TRIGGER work_task_group_update`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_group_update BEFORE UPDATE ON work_task_manager_groups
    WHEN NEW.environment_id IS NOT OLD.environment_id OR NEW.group_id IS NOT OLD.group_id OR NEW.created_event_id IS NOT OLD.created_event_id OR NEW.created_sequence IS NOT OLD.created_sequence OR NEW.created_event_json IS NOT OLD.created_event_json
      OR NEW.control_sequence<=OLD.control_sequence OR NOT ${columnControl("NEW.control_event_json", "$.previousControl", "OLD")}
      OR OLD.highest_manager_generation IS NOT ${highWater}
      OR (OLD.current_manager_generation IS NOT NULL AND NOT EXISTS(SELECT 1 FROM work_task_group_managers WHERE environment_id=OLD.environment_id AND group_id=OLD.group_id AND current_generation=OLD.current_manager_generation AND current_manager_thread_id=OLD.current_manager_thread_id))
      OR EXISTS(SELECT 1 FROM work_task_group_managers WHERE environment_id=OLD.environment_id AND group_id=OLD.group_id AND current_generation IS NOT NULL AND (current_generation IS NOT OLD.current_manager_generation OR current_manager_thread_id IS NOT OLD.current_manager_thread_id))
      OR (OLD.latest_membership_event_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM work_task_membership_companions c WHERE c.event_id=OLD.latest_membership_event_id AND c.environment_id=OLD.environment_id AND c.group_id=OLD.group_id))
      OR CASE ${get("NEW.control_event_json", "$.type")}
        WHEN 'task-group.membership-changed' THEN NEW.control_revision IS NOT OLD.control_revision OR NEW.membership_revision IS NOT OLD.membership_revision+1
          OR NEW.highest_manager_generation IS NOT OLD.highest_manager_generation OR NEW.current_manager_generation IS NOT OLD.current_manager_generation OR NEW.current_manager_thread_id IS NOT OLD.current_manager_thread_id
          OR NEW.dispatch_hold IS NOT OLD.dispatch_hold OR NEW.latest_membership_event_id IS NOT NEW.control_event_id
        ELSE NEW.control_revision IS NOT OLD.control_revision+1 OR NEW.membership_revision IS NOT OLD.membership_revision OR NEW.latest_membership_event_id IS NOT OLD.latest_membership_event_id
          OR CASE ${get("NEW.control_event_json", "$.type")}
            WHEN 'task-group.manager-changed' THEN NEW.dispatch_hold IS NOT OLD.dispatch_hold OR (CASE WHEN NEW.current_manager_generation IS NULL THEN OLD.current_manager_generation IS NULL OR NEW.highest_manager_generation IS NOT OLD.highest_manager_generation
              ELSE NEW.highest_manager_generation IS NOT OLD.highest_manager_generation+1 OR NEW.current_manager_thread_id IS OLD.current_manager_thread_id END)
            WHEN 'task-group.hold-changed' THEN NEW.highest_manager_generation IS NOT OLD.highest_manager_generation OR NEW.current_manager_generation IS NOT OLD.current_manager_generation OR NEW.current_manager_thread_id IS NOT OLD.current_manager_thread_id OR NEW.dispatch_hold IS OLD.dispatch_hold
            ELSE 1 END END
      OR EXISTS(SELECT 1 FROM work_task_manager_groups WHERE environment_id=NEW.environment_id AND group_id<>OLD.group_id AND current_manager_thread_id=NEW.current_manager_thread_id)
      OR ${knownEvent("NEW.control_event_id", "NEW.control_sequence")}
    BEGIN SELECT RAISE(ABORT,'group control or pending membership conflict'); END`);
  const roleCollision = (
    id: string,
    sequence: string,
    json: string,
  ) => `EXISTS(SELECT 1 FROM work_task_membership_companions c WHERE c.root_event_id=${id} OR c.root_sequence=${sequence}
    OR ((c.event_id=${id} OR c.sequence=${sequence}) AND (c.event_id IS NOT ${id} OR c.sequence IS NOT ${sequence} OR NOT ${same("c.event_json", json)})))`;
  for (const operation of ["INSERT", "UPDATE"]) {
    yield* sql.unsafe(`CREATE TRIGGER work_task_group_membership_identity_${operation.toLowerCase()} BEFORE ${operation} ON work_task_manager_groups
      WHEN ${roleCollision("NEW.control_event_id", "NEW.control_sequence", "NEW.control_event_json")} BEGIN SELECT RAISE(ABORT,'membership event identity conflict'); END`);
    yield* sql.unsafe(`CREATE TRIGGER work_task_manager_membership_identity_${operation.toLowerCase()} BEFORE ${operation} ON work_task_group_managers
      WHEN ${roleCollision("NEW.bound_event_id", "NEW.bound_sequence", "NEW.bound_event_json")} OR (NEW.retired_event_id IS NOT NULL AND ${roleCollision("NEW.retired_event_id", "NEW.retired_sequence", "NEW.retired_event_json")})
      BEGIN SELECT RAISE(ABORT,'membership manager event identity conflict'); END`);
  }
  yield* sql.unsafe(`CREATE TRIGGER work_task_membership_event_identity BEFORE INSERT ON work_task_membership_companions
    WHEN ${knownEvent("NEW.root_event_id", "NEW.root_sequence")}
      OR EXISTS(SELECT 1 FROM work_task_manager_groups g WHERE (g.created_event_id=NEW.event_id OR g.created_sequence=NEW.sequence)
        OR ((g.control_event_id=NEW.event_id OR g.control_sequence=NEW.sequence) AND (g.control_event_id IS NOT NEW.event_id OR g.control_sequence IS NOT NEW.sequence OR NOT ${same("g.control_event_json", "NEW.event_json")})))
      OR EXISTS(SELECT 1 FROM work_task_group_managers WHERE bound_event_id=NEW.event_id OR bound_sequence=NEW.sequence OR retired_event_id=NEW.event_id OR retired_sequence=NEW.sequence)
      OR EXISTS(SELECT 1 FROM work_task_membership_companions c WHERE c.event_id=NEW.root_event_id OR c.sequence=NEW.root_sequence OR c.root_event_id=NEW.event_id OR c.root_sequence=NEW.sequence
        OR ((c.root_event_id=NEW.root_event_id OR c.root_sequence=NEW.root_sequence) AND (c.event_id IS NOT NEW.peer_event_id OR c.peer_event_id IS NOT NEW.event_id
          OR c.root_event_id IS NOT NEW.root_event_id OR c.root_sequence IS NOT NEW.root_sequence OR NOT ${same("c.root_event_json", "NEW.root_event_json")})))
    BEGIN SELECT RAISE(ABORT,'membership root or companion identity conflict'); END`);
  for (const operation of ["INSERT", "UPDATE"]) {
    const sourceGuard =
      operation === "UPDATE"
        ? `EXISTS(SELECT 1 FROM work_task_group_memberships WHERE task_id=OLD.task_id AND accepted_definition_revision=OLD.definition_revision)
      AND (${["task_id", "definition_revision", "action_policy_revision", "definition_json", "attribution_json", "accepted_event_id", "accepted_at"].map((key) => `NEW.${key} IS NOT OLD.${key}`).join(" OR ")}) OR `
        : "";
    const destinationGuard =
      operation === "UPDATE"
        ? "AND NOT (a.task_id=OLD.task_id AND a.definition_revision=OLD.definition_revision)"
        : "";
    yield* sql.unsafe(`CREATE TRIGGER work_task_membership_accepted_${operation.toLowerCase()} BEFORE ${operation} ON work_task_accepted_definitions
      WHEN ${sourceGuard} EXISTS(SELECT 1 FROM work_task_accepted_definitions a JOIN work_task_group_memberships m ON m.task_id=a.task_id AND m.accepted_definition_revision=a.definition_revision
        WHERE ((a.task_id=NEW.task_id AND a.definition_revision=NEW.definition_revision) OR a.accepted_event_id=NEW.accepted_event_id) ${destinationGuard})
      BEGIN SELECT RAISE(ABORT,'referenced membership definition is immutable'); END`);
    yield* sql.unsafe(`CREATE TRIGGER work_task_membership_task_${operation.toLowerCase()} BEFORE ${operation} ON work_task_projections
      WHEN EXISTS(SELECT 1 FROM work_task_group_memberships WHERE task_id=NEW.task_id) ${operation === "UPDATE" ? "AND NEW.task_id IS NOT OLD.task_id" : ""}
      BEGIN SELECT RAISE(ABORT,'referenced membership task replacement'); END`);
  }
});
