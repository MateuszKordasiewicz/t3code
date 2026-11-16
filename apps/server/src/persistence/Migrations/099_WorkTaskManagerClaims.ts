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
  const type = (path: string, kind: string, json = "NEW.event_json") =>
    `json_type(${json},'$.${path}') IS '${kind}'`;
  const object = (path: string, count: number, json = "NEW.event_json") =>
    `(${type(path, "object", json)} AND (SELECT count(*) FROM json_each(${json},'$.${path}'))=${count})`;
  const integer = (path: string, min = 0, json = "NEW.event_json") =>
    `(${type(path, "integer", json)} AND ${get(path, json)} BETWEEN ${min} AND 9007199254740991)`;
  const same = (a: string, b: string) =>
    structuralJsonEqualSql(`json_quote(${a})`, `json_quote(${b})`);
  const columns: Record<string, readonly [string, string]> = {
    work_claim_id: ["claim.binding.workClaimId", "TEXT"],
    run_id: ["claim.binding.runId", "TEXT"],
    environment_id: ["environmentId", "TEXT"],
    task_id: ["claim.binding.task.taskId", "TEXT"],
    group_id: ["claim.binding.group.groupId", "TEXT"],
    manager_generation: ["claim.binding.manager.managerGeneration", "INTEGER"],
    manager_thread_id: ["claim.binding.manager.threadId", "TEXT"],
    membership_generation: ["claim.binding.membershipGeneration", "INTEGER"],
    accepted_event_id: ["id", "TEXT"],
    accepted_sequence: ["claim.acceptedSequence", "INTEGER"],
    effect_id: ["claim.binding.effectId", "TEXT"],
    accepted_definition_revision: [
      "claim.binding.taskRevisions.acceptedDefinitionRevision",
      "INTEGER",
    ],
  };
  const closure = "work_claim_id,release_event_id,run_id,effect_id";
  const reverseClosure = "work_claim_id,event_id,run_id,effect_id";
  yield* sql.unsafe(`CREATE TABLE work_task_executable_claims (
    ${Object.entries(columns)
      .map(([name, [, kind]]) => `${name} ${kind} NOT NULL`)
      .join(",")},
    event_json TEXT NOT NULL CHECK(json_valid(event_json)),release_event_id TEXT,
    PRIMARY KEY(work_claim_id),UNIQUE(accepted_event_id),UNIQUE(accepted_sequence),UNIQUE(effect_id),UNIQUE(${closure}),
    FOREIGN KEY(run_id) REFERENCES work_task_accepted_runs(run_id),
    FOREIGN KEY(task_id,accepted_definition_revision) REFERENCES work_task_accepted_definitions(task_id,definition_revision),
    FOREIGN KEY(environment_id,group_id,manager_generation,manager_thread_id) REFERENCES work_task_group_managers(environment_id,group_id,generation,manager_thread_id),
    FOREIGN KEY(task_id,membership_generation) REFERENCES work_task_group_memberships(task_id,membership_generation),
    FOREIGN KEY(${closure}) REFERENCES work_task_claim_releases(${reverseClosure}) DEFERRABLE INITIALLY DEFERRED
  ) WITHOUT ROWID`);
  const resources = {
    group: "environment_id,group_id",
    task: "task_id",
    thread: "manager_thread_id",
  };
  for (const [name, keys] of Object.entries(resources)) {
    yield* sql.unsafe(
      `CREATE UNIQUE INDEX work_task_claim_${name}_active_idx ON work_task_executable_claims(${keys}) WHERE release_event_id IS NULL`,
    );
    yield* sql.unsafe(
      `CREATE INDEX work_task_claim_${name}_history_idx ON work_task_executable_claims(${keys},accepted_sequence DESC,work_claim_id)`,
    );
  }
  yield* sql`CREATE INDEX work_task_claim_run_idx ON work_task_executable_claims(run_id,accepted_sequence DESC)`;
  yield* sql`CREATE INDEX work_task_claim_definition_idx ON work_task_executable_claims(task_id,accepted_definition_revision)`;
  yield* sql`CREATE TABLE work_task_claim_uncertainties (
    event_id TEXT NOT NULL PRIMARY KEY,sequence INTEGER NOT NULL UNIQUE,work_claim_id TEXT NOT NULL REFERENCES work_task_executable_claims(work_claim_id),
    event_json TEXT NOT NULL CHECK(json_valid(event_json))
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_claim_uncertain_latest_idx ON work_task_claim_uncertainties(work_claim_id,sequence DESC)`;
  yield* sql.unsafe(`CREATE TABLE work_task_claim_releases (
    event_id TEXT NOT NULL PRIMARY KEY,sequence INTEGER NOT NULL UNIQUE,work_claim_id TEXT NOT NULL UNIQUE,run_id TEXT NOT NULL,effect_id TEXT NOT NULL,
    terminal_event_id TEXT NOT NULL,terminal_sequence INTEGER NOT NULL,event_json TEXT NOT NULL CHECK(json_valid(event_json)),native_json TEXT NOT NULL CHECK(json_valid(native_json)),
    UNIQUE(${reverseClosure}),UNIQUE(event_id,work_claim_id,run_id,terminal_event_id,terminal_sequence),
    FOREIGN KEY(${reverseClosure}) REFERENCES work_task_executable_claims(${closure}) DEFERRABLE INITIALLY DEFERRED
  ) WITHOUT ROWID`);
  yield* sql`CREATE INDEX work_task_claim_release_terminal_idx ON work_task_claim_releases(terminal_event_id)`;
  yield* sql`CREATE TABLE work_task_claim_references (
    owner_event_id TEXT NOT NULL,claim_event_id TEXT REFERENCES work_task_executable_claims(accepted_event_id),uncertain_event_id TEXT REFERENCES work_task_claim_uncertainties(event_id),release_event_id TEXT REFERENCES work_task_claim_releases(event_id),
    reference_role TEXT NOT NULL,position INTEGER NOT NULL,event_id TEXT NOT NULL,sequence INTEGER NOT NULL,known_kind TEXT,known_task_id TEXT,
    PRIMARY KEY(owner_event_id,reference_role,position),
    CHECK((claim_event_id IS NOT NULL AND owner_event_id=claim_event_id AND uncertain_event_id IS NULL AND release_event_id IS NULL)
      OR (uncertain_event_id IS NOT NULL AND owner_event_id=uncertain_event_id AND claim_event_id IS NULL AND release_event_id IS NULL)
      OR (release_event_id IS NOT NULL AND owner_event_id=release_event_id AND claim_event_id IS NULL AND uncertain_event_id IS NULL))
  ) WITHOUT ROWID`;
  yield* sql`CREATE INDEX work_task_claim_ref_id_idx ON work_task_claim_references(event_id,sequence)`;
  yield* sql`CREATE INDEX work_task_claim_ref_sequence_idx ON work_task_claim_references(sequence,event_id)`;
  for (const table of [
    "work_task_claim_uncertainties",
    "work_task_claim_releases",
    "work_task_claim_references",
  ])
    for (const operation of ["UPDATE", "DELETE"])
      yield* sql.unsafe(
        `CREATE TRIGGER ${table}_${operation.toLowerCase()} BEFORE ${operation} ON ${table} BEGIN SELECT RAISE(ABORT,'immutable physical evidence'); END`,
      );
  yield* sql`CREATE TRIGGER work_task_claim_delete BEFORE DELETE ON work_task_executable_claims BEGIN SELECT RAISE(ABORT,'immutable physical claim'); END`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_claim_close BEFORE UPDATE ON work_task_executable_claims BEGIN
    SELECT CASE WHEN OLD.release_event_id IS NOT NULL OR NEW.release_event_id IS NULL OR NEW.event_json IS NOT OLD.event_json
      OR ${Object.keys(columns)
        .map((name) => `NEW.${name} IS NOT OLD.${name}`)
        .join(" OR ")}
      THEN RAISE(ABORT,'immutable physical claim or closure') END;
  END`);
  const common = (
    count: number,
  ) => `${canonicalEvidenceJsonSql("NEW.event_json")} AND json_type(NEW.event_json) IS 'object'
    AND (SELECT count(*) FROM json_each(NEW.event_json))=${count} AND ${object("attribution", 1)} AND ${get("attribution.type")} IS 'server'
    AND ${["id", "environmentId", "commandId", "occurredAt"].map((p) => type(p, "text")).join(" AND ")} AND ${validTimestampSql(get("occurredAt"))}`;
  const stable = [
    "version",
    "task",
    "group",
    "manager",
    "membershipGeneration",
    "projectId",
    "providerInstanceId",
    "runId",
    "messageId",
  ];
  const counters = ["membershipRevision", "controlRevision", "graphRevision"];
  const revisions = [
    "definitionRevision",
    "acceptedDefinitionRevision",
    "lifecycleRevision",
    "completionPolicyRevision",
    "actionPolicyRevision",
  ];
  const member = get("membership.currentMembership");
  yield* sql.unsafe(`CREATE TRIGGER work_task_claim_valid BEFORE INSERT ON work_task_executable_claims BEGIN
    SELECT CASE WHEN (${common(10)} AND ${get("type")} IS 'task-group.work-accepted' AND ${type("previousActiveClaimId", "null")}
      AND ${object("claim", 9)} AND ${object("claim.binding", 16)} AND ${object("claim.binding.taskRevisions", 5)}
      AND ${Object.entries(columns)
        .map(
          ([name, [path, kind]]) =>
            `(NEW.${name} IS ${get(path)} AND ${kind === "INTEGER" ? integer(path) : type(path, "text")})`,
        )
        .join(" AND ")}
      AND ${type("claim.binding.commandId", "text")} AND ${get("claim.binding.commandId")} IS ${get("commandId")}
      AND ${type("claim.acceptedEventId", "text")} AND ${type("claim.acceptedAt", "text")} AND ${get("claim.acceptedEventId")} IS NEW.accepted_event_id AND ${get("claim.acceptedAt")} IS ${get("occurredAt")}
      AND ${counters.map((p) => integer(`claim.binding.${p}`, p === "membershipRevision" ? 1 : 0)).join(" AND ")}
      AND ${revisions.map((p) => integer(`claim.binding.taskRevisions.${p}`, 1)).join(" AND ")}
      AND ${get("claim.binding.taskRevisions.acceptedDefinitionRevision")}<=${get("claim.binding.taskRevisions.definitionRevision")}
      AND ${type("claim.sourceEventId", "text")} AND ${integer("claim.sourceSequence")} AND ${get("claim.sourceSequence")}<=NEW.accepted_sequence
      AND ${integer("claim.observedWorkSequence")} AND ${get("claim.observedWorkSequence")}<=NEW.accepted_sequence
      AND EXISTS(SELECT 1 FROM work_task_accepted_runs a WHERE a.run_id=NEW.run_id AND a.environment_id=NEW.environment_id AND a.recorded_sequence<NEW.accepted_sequence
        AND ${stable.map((p) => same(get(`claim.binding.${p}`), get(`ownership.run.${p}`, "a.event_json"))).join(" AND ")}
        AND CASE a.origin_type WHEN 'initial_start' THEN ${same(get("claim.launchOrigin"), get("ownership.origin", "a.event_json"))}
          ELSE ${object("claim.launchOrigin", 6)} AND ${get("claim.launchOrigin.type")} IS 'automatic_followup'
            AND ${integer("claim.launchOrigin.allowanceUnit", 1)} AND ${integer("claim.launchOrigin.observedDirtyGeneration", 1)} AND ${integer("claim.launchOrigin.acknowledgedGeneration")}
            AND ${["reservationId", "reservationCommandId", "allowanceUnit", "observedDirtyGeneration", "acknowledgedGeneration"].map((p) => same(get(`claim.launchOrigin.${p}`), get(`ownership.origin.${p}`, "a.event_json"))).join(" AND ")} END)
      AND ${object("control", 6)} AND ${same(get("control.group"), get("claim.binding.group"))} AND ${same(get("control.currentManager"), get("claim.binding.manager"))}
      AND ${get("control.dispatchHold")} IS 'none' AND ${integer("control.highestManagerGeneration", 1)} AND ${get("control.highestManagerGeneration")} IS NEW.manager_generation
      AND ${["controlRevision", "membershipRevision"].map((p) => `(${integer(`control.${p}`)} AND ${get(`control.${p}`)} IS ${get(`claim.binding.${p}`)})`).join(" AND ")}
      AND ${object("membership", 4)} AND ${same(get("membership.task"), get("claim.binding.task"))} AND ${same(get("membership.taskRevisions"), get("claim.binding.taskRevisions"))}
      AND ${integer("membership.highestMembershipGeneration", 1)} AND ${get("membership.highestMembershipGeneration")} IS NEW.membership_generation
      AND EXISTS(SELECT 1 FROM work_task_group_memberships m WHERE m.task_id=NEW.task_id AND m.membership_generation=NEW.membership_generation
        AND m.environment_id=NEW.environment_id AND m.group_id=NEW.group_id AND ${same(member, "json(m.joined_record_json)")}
        AND m.joined_membership_revision<=${get("claim.binding.membershipRevision")} AND m.accepted_definition_revision<=NEW.accepted_definition_revision)
      AND ${type("claim.prerequisites", "array")} AND json_array_length(NEW.event_json,'$.claim.prerequisites')<=64
      AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.claim.prerequisites') p WHERE (${canonicalEvidenceJsonSql("p.value")}
        AND (json_type(p.value) IS 'object' AND (SELECT count(*) FROM json_each(p.value))=4) AND ${object("blocker", 2, "p.value")}
        AND ${["edgeId", "blocker.environmentId", "blocker.taskId", "completionEventId"].map((p) => type(p, "text", "p.value")).join(" AND ")}
        AND ${integer("completionSequence", 0, "p.value")} AND ${get("completionSequence", "p.value")}<=NEW.accepted_sequence
        AND ${get("blocker.environmentId", "p.value")} IS NEW.environment_id AND ${get("blocker.taskId", "p.value")}<>NEW.task_id
        AND EXISTS(SELECT 1 FROM work_task_graph_edges e WHERE e.environment_id=NEW.environment_id AND e.edge_id=${get("edgeId", "p.value")}
          AND e.dependent_task_id=NEW.task_id AND e.blocker_task_id=${get("blocker.taskId", "p.value")} AND e.adopted_sequence<=NEW.accepted_sequence
          AND e.created_graph_revision<=${get("claim.binding.graphRevision")} AND (e.removed_graph_revision IS NULL OR e.removed_graph_revision>${get("claim.binding.graphRevision")}))) IS NOT TRUE)
      AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.claim.prerequisites') GROUP BY json_extract(value,'$.edgeId') HAVING count(*)>1)
      AND NOT EXISTS(SELECT 1 FROM json_each(NEW.event_json,'$.claim.prerequisites') GROUP BY json_extract(value,'$.blocker.taskId') HAVING count(*)>1)
    ) IS NOT TRUE THEN RAISE(ABORT,'invalid physical claim') END;
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_executable_claims WHERE work_claim_id=NEW.work_claim_id OR accepted_event_id=NEW.accepted_event_id OR accepted_sequence=NEW.accepted_sequence OR effect_id=NEW.effect_id)
      OR EXISTS(SELECT 1 FROM work_task_run_ownership_observations WHERE run_id=NEW.run_id AND kind='task.run-ownership-terminated')
      THEN RAISE(ABORT,'physical claim identity or terminal ownership conflict') END;
  END`);
  for (const [name, keys] of Object.entries(resources)) {
    const matches = keys
      .split(",")
      .map((key) => `c.${key}=NEW.${key}`)
      .join(" AND ");
    yield* sql.unsafe(`CREATE TRIGGER work_task_claim_${name}_order BEFORE INSERT ON work_task_executable_claims BEGIN
      SELECT CASE WHEN EXISTS(SELECT 1 FROM (SELECT c.release_event_id,c.accepted_sequence FROM work_task_executable_claims c WHERE ${matches} ORDER BY c.accepted_sequence DESC LIMIT 1) prior
        LEFT JOIN work_task_claim_releases r ON r.event_id=prior.release_event_id
        WHERE r.event_id IS NULL OR prior.accepted_sequence>=NEW.accepted_sequence OR r.sequence>=NEW.accepted_sequence)
        THEN RAISE(ABORT,'physical resource remains occupied or history overlaps') END;
    END`);
  }
  yield* sql`CREATE TRIGGER work_task_owner_before_physical BEFORE INSERT ON work_task_run_ownership_observations
    WHEN NEW.kind='task.run-ownership-terminated' AND EXISTS(SELECT 1 FROM work_task_executable_claims WHERE run_id=NEW.run_id)
    BEGIN SELECT RAISE(ABORT,'physical history requires after-claim closure'); END`;
  const retained = `json_object('binding',json(json_extract(c.event_json,'$.claim.binding')),'acceptedEventId',c.accepted_event_id,'acceptedSequence',c.accepted_sequence,'acceptedAt',json_extract(c.event_json,'$.claim.acceptedAt'))`;
  for (const [table, eventKind, count] of [
    ["work_task_claim_uncertainties", "task-group.work-uncertain", 10],
    ["work_task_claim_releases", "task-group.work-released", 9],
  ] as const) {
    yield* sql.unsafe(`CREATE TRIGGER ${table}_valid BEFORE INSERT ON ${table} BEGIN
      SELECT CASE WHEN (${common(count)} AND ${get("type")} IS '${eventKind}' AND NEW.event_id IS ${get("id")}
        AND typeof(NEW.sequence)='integer' AND NEW.sequence BETWEEN 0 AND 9007199254740991 AND ${object("claim", 4)}
        AND NEW.work_claim_id IS ${get("claim.binding.workClaimId")}
        AND EXISTS(SELECT 1 FROM work_task_executable_claims c WHERE c.work_claim_id=NEW.work_claim_id
          AND c.environment_id IS ${get("environmentId")} AND c.accepted_sequence<NEW.sequence AND ${same(get("claim"), retained)}
          ${table === "work_task_claim_uncertainties" ? "AND c.release_event_id IS NULL" : "AND c.run_id=NEW.run_id AND c.effect_id=NEW.effect_id AND (c.release_event_id IS NULL OR c.release_event_id=NEW.event_id)"})
        AND NEW.sequence>coalesce((SELECT sequence FROM work_task_claim_uncertainties WHERE work_claim_id=NEW.work_claim_id ORDER BY sequence DESC LIMIT 1),-1)
        ${table === "work_task_claim_uncertainties" ? `AND ${type("sourceEventId", "text")} AND ${integer("sourceSequence")} AND ${get("sourceSequence")}<NEW.sequence AND ${type("reason", "text")}` : ""}
      ) IS NOT TRUE THEN RAISE(ABORT,'invalid retained physical evidence') END;
      SELECT CASE WHEN EXISTS(SELECT 1 FROM ${table} WHERE event_id=NEW.event_id OR sequence=NEW.sequence)
        ${table === "work_task_claim_releases" ? "OR EXISTS(SELECT 1 FROM work_task_claim_releases WHERE work_claim_id=NEW.work_claim_id)" : "OR EXISTS(SELECT 1 FROM work_task_claim_releases WHERE work_claim_id=NEW.work_claim_id)"}
        THEN RAISE(ABORT,'physical evidence identity or phase conflict') END;
    END`);
  }
  const evidence = "releaseEvidence";
  const witness = (
    path: string,
    count: number,
    eventPath: string,
    kind: string,
  ) => `${object(path, count, "NEW.native_json")}
    AND ${get(`${path}.type`, "NEW.native_json")} IS '${kind}' AND ${get(`${path}.eventId`, "NEW.native_json")} IS ${get(`${eventPath}.eventId`)}
    AND ${integer(`${path}.sequence`, 0, "NEW.native_json")} AND ${get(`${path}.sequence`, "NEW.native_json")} IS ${get(`${eventPath}.sequence`)}
    AND ${get(`${path}.runId`, "NEW.native_json")} IS NEW.run_id`;
  yield* sql.unsafe(`CREATE TRIGGER work_task_release_evidence BEFORE INSERT ON work_task_claim_releases BEGIN
    SELECT CASE WHEN (${canonicalEvidenceJsonSql("NEW.native_json")} AND json_type(NEW.native_json) IS 'object'
      AND ${integer("observedSequence")} AND ${get("observedSequence")}<NEW.sequence
      AND ${object(`${evidence}.terminalRunEvent`, 3)} AND ${get(`${evidence}.terminalRunEvent.type`)} IS 'run.updated'
      AND ${type(`${evidence}.terminalRunEvent.eventId`, "text")} AND ${integer(`${evidence}.terminalRunEvent.sequence`)}
      AND NEW.terminal_event_id IS ${get(`${evidence}.terminalRunEvent.eventId`)} AND NEW.terminal_sequence IS ${get(`${evidence}.terminalRunEvent.sequence`)}
      AND NEW.terminal_sequence<=${get("observedSequence")} AND NEW.terminal_sequence>(SELECT accepted_sequence FROM work_task_executable_claims WHERE work_claim_id=NEW.work_claim_id)
      AND ${witness("run", 7, `${evidence}.terminalRunEvent`, "run.updated")}
      AND ${get("run.threadId", "NEW.native_json")} IS (SELECT manager_thread_id FROM work_task_executable_claims WHERE work_claim_id=NEW.work_claim_id)
      AND ${validTimestampSql(get("run.occurredAt", "NEW.native_json"))}
      AND CASE ${get(`${evidence}.type`)} WHEN 'not_started' THEN ${object(evidence, 3)} AND (SELECT count(*) FROM json_each(NEW.native_json))=1
        AND ${type(`${evidence}.settlementCommandId`, "text")} AND ${get("run.status", "NEW.native_json")} IN ('cancelled','failed','interrupted')
      WHEN 'run_settled' THEN ${object(evidence, 3)} OR ${object(evidence, 4)} OR ${object(evidence, 5)} ELSE 0 END
    ) IS NOT TRUE THEN RAISE(ABORT,'invalid physical release witness') END;
    SELECT CASE WHEN ${get(`${evidence}.type`)}='run_settled' AND (
      ${get("run.status", "NEW.native_json")} IN ('completed','cancelled','failed','interrupted','rolled_back')
      AND ${object(`${evidence}.attempt`, 4)} AND ${type(`${evidence}.attempt.attemptId`, "text")} AND ${type(`${evidence}.attempt.providerThreadId`, "text")}
      AND (${type(`${evidence}.attempt.providerTurnId`, "null")} OR ${type(`${evidence}.attempt.providerTurnId`, "text")})
      AND ${object(`${evidence}.attempt.terminalAttemptEvent`, 3)} AND ${get(`${evidence}.attempt.terminalAttemptEvent.type`)} IS 'run-attempt.updated'
      AND ${type(`${evidence}.attempt.terminalAttemptEvent.eventId`, "text")} AND ${integer(`${evidence}.attempt.terminalAttemptEvent.sequence`)}
      AND ${get(`${evidence}.attempt.terminalAttemptEvent.sequence`)} BETWEEN (SELECT accepted_sequence FROM work_task_executable_claims WHERE work_claim_id=NEW.work_claim_id) AND ${get("observedSequence")}
      AND ${witness("attempt", 9, `${evidence}.attempt.terminalAttemptEvent`, "run-attempt.updated")}
      AND ${["attemptId", "providerThreadId", "providerTurnId"].map((p) => same(get(`attempt.${p}`, "NEW.native_json"), get(`${evidence}.attempt.${p}`))).join(" AND ")}
      AND ${validTimestampSql(get("attempt.occurredAt", "NEW.native_json"))} AND ${get("attempt.status", "NEW.native_json")} IN ('completed','interrupted','failed','cancelled','superseded')
      AND (json_type(NEW.event_json,'$.releaseEvidence.settlementCommandId') IS NULL OR ${type(`${evidence}.settlementCommandId`, "text")})
      AND (SELECT count(*) FROM json_each(NEW.event_json,'$.releaseEvidence'))=3+CASE WHEN json_type(NEW.event_json,'$.releaseEvidence.settlementCommandId') IS NULL THEN 0 ELSE 1 END+CASE WHEN json_type(NEW.event_json,'$.releaseEvidence.interruptResult') IS NULL THEN 0 ELSE 1 END
      AND CASE WHEN json_type(NEW.event_json,'$.releaseEvidence.interruptResult') IS NULL THEN (SELECT count(*) FROM json_each(NEW.native_json))=2
        ELSE (SELECT count(*) FROM json_each(NEW.native_json))=3 AND ${object(`${evidence}.interruptResult`, 4)} AND ${get(`${evidence}.interruptResult.type`)} IS 'turn-item.updated'
          AND ${type(`${evidence}.interruptResult.eventId`, "text")} AND ${integer(`${evidence}.interruptResult.sequence`)} AND ${type(`${evidence}.interruptResult.turnItemId`, "text")}
          AND ${get(`${evidence}.interruptResult.sequence`)} BETWEEN (SELECT accepted_sequence FROM work_task_executable_claims WHERE work_claim_id=NEW.work_claim_id) AND ${get("observedSequence")} AND ${witness("interrupt", 8, `${evidence}.interruptResult`, "turn-item.updated")}
          AND ${get("interrupt.turnItemId", "NEW.native_json")} IS ${get(`${evidence}.interruptResult.turnItemId`)} AND ${get("interrupt.itemType", "NEW.native_json")} IS 'run_interrupt_result'
          AND ${get("interrupt.threadId", "NEW.native_json")} IS (SELECT manager_thread_id FROM work_task_executable_claims WHERE work_claim_id=NEW.work_claim_id)
          AND ${validTimestampSql(get("interrupt.occurredAt", "NEW.native_json"))} END
    ) IS NOT TRUE THEN RAISE(ABORT,'invalid settled attempt witness') END;
  END`);
  const descriptors = (json: string, sequence: string, accepted: boolean) => {
    const task = get("claim.binding.task.taskId", json);
    const base = `SELECT 'recorded' role,0 position,${get("id", json)} event_id,${sequence} sequence,${get("type", json)} known_kind,${task} known_task_id`;
    if (!accepted)
      return `${base}
      UNION ALL SELECT 'claim',0,${get("claim.acceptedEventId", json)},${get("claim.acceptedSequence", json)},'task-group.work-accepted',${task}
      UNION ALL SELECT 'source',0,${get("sourceEventId", json)},${get("sourceSequence", json)},NULL,NULL WHERE ${get("type", json)}='task-group.work-uncertain'
      UNION ALL SELECT 'terminal_run',0,${get("releaseEvidence.terminalRunEvent.eventId", json)},${get("releaseEvidence.terminalRunEvent.sequence", json)},'run.updated',${task} WHERE ${get("type", json)}='task-group.work-released'
      UNION ALL SELECT 'terminal_attempt',0,${get("releaseEvidence.attempt.terminalAttemptEvent.eventId", json)},${get("releaseEvidence.attempt.terminalAttemptEvent.sequence", json)},'run-attempt.updated',${task} WHERE ${get("releaseEvidence.type", json)}='run_settled'
      UNION ALL SELECT 'interrupt',0,${get("releaseEvidence.interruptResult.eventId", json)},${get("releaseEvidence.interruptResult.sequence", json)},'turn-item.updated',${task} WHERE json_type(${json},'$.releaseEvidence.interruptResult')='object'`;
    return `${base}
      UNION ALL SELECT 'source',0,${get("claim.sourceEventId", json)},${get("claim.sourceSequence", json)},NULL,NULL
      UNION ALL SELECT 'ownership',0,a.recorded_event_id,a.recorded_sequence,'task.run-ownership-accepted',a.task_id FROM work_task_accepted_runs a WHERE a.run_id=${get("claim.binding.runId", json)}
      UNION ALL SELECT 'origin',0,${get("claim.launchOrigin.startEventId", json)},${get("claim.launchOrigin.startSequence", json)},NULL,${task} WHERE ${get("claim.launchOrigin.type", json)}='initial_start'
      UNION ALL SELECT 'debit',0,a.debit_event_id,a.debit_sequence,'task.allowance-debited',a.task_id FROM work_task_accepted_runs a WHERE a.run_id=${get("claim.binding.runId", json)} AND a.origin_type='automatic_followup'
      UNION ALL SELECT 'prerequisite',p.key,json_extract(p.value,'$.completionEventId'),json_extract(p.value,'$.completionSequence'),'task.completed',json_extract(p.value,'$.blocker.taskId') FROM json_each(${json},'$.claim.prerequisites') p`;
  };
  const conflicts = (
    table: string,
    id = "event_id",
    sequence = "sequence",
    kind = "known_kind",
    task = "known_task_id",
  ) => `EXISTS(SELECT 1 FROM ${table} r WHERE r.${id}=NEW.event_id AND
    (r.${sequence}<>NEW.sequence OR ((${kind}) IS NOT NULL AND NEW.known_kind IS NOT NULL AND (${kind})<>NEW.known_kind)
      OR ((${task}) IS NOT NULL AND NEW.known_task_id IS NOT NULL AND (${task})<>NEW.known_task_id)))
    OR EXISTS(SELECT 1 FROM ${table} r WHERE r.${sequence}=NEW.sequence AND r.${id}<>NEW.event_id)`;
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
  yield* sql.unsafe(`CREATE TRIGGER work_task_claim_reference_valid BEFORE INSERT ON work_task_claim_references BEGIN
    SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM (${descriptors("(SELECT event_json FROM work_task_executable_claims WHERE accepted_event_id=NEW.claim_event_id)", "(SELECT accepted_sequence FROM work_task_executable_claims WHERE accepted_event_id=NEW.claim_event_id)", true)}) d
      WHERE NEW.claim_event_id IS NOT NULL AND d.role=NEW.reference_role AND d.position=NEW.position AND d.event_id IS NEW.event_id AND d.sequence IS NEW.sequence AND d.known_kind IS NEW.known_kind AND d.known_task_id IS NEW.known_task_id)
      AND NOT EXISTS(SELECT 1 FROM (${descriptors("coalesce((SELECT event_json FROM work_task_claim_uncertainties WHERE event_id=NEW.uncertain_event_id),(SELECT event_json FROM work_task_claim_releases WHERE event_id=NEW.release_event_id))", "coalesce((SELECT sequence FROM work_task_claim_uncertainties WHERE event_id=NEW.uncertain_event_id),(SELECT sequence FROM work_task_claim_releases WHERE event_id=NEW.release_event_id))", false)}) d
      WHERE NEW.claim_event_id IS NULL AND d.role=NEW.reference_role AND d.position=NEW.position AND d.event_id IS NEW.event_id AND d.sequence IS NEW.sequence AND d.known_kind IS NEW.known_kind AND d.known_task_id IS NEW.known_task_id)
      THEN RAISE(ABORT,'invalid physical reference') END;
    SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_claim_references WHERE owner_event_id=NEW.owner_event_id AND reference_role=NEW.reference_role AND position=NEW.position)
      OR ${["work_task_claim_references", "work_task_accepted_run_references", "work_task_run_observation_references"].map((t) => conflicts(t, "event_id", "sequence", "r.known_kind", "r.known_task_id")).join(" OR ")}
      OR ${historical.map(([t, id, seq, k, task]) => conflicts(t, id, seq, k, task)).join(" OR ")}
      THEN RAISE(ABORT,'physical reference identity conflict') END;
  END`);
  for (const table of ["work_task_accepted_run_references", "work_task_run_observation_references"])
    yield* sql.unsafe(
      `CREATE TRIGGER ${table}_physical BEFORE INSERT ON ${table} BEGIN SELECT CASE WHEN ${conflicts("work_task_claim_references", "event_id", "sequence", "r.known_kind", "r.known_task_id")} THEN RAISE(ABORT,'physical reference conflict') END; END`,
    );
  for (const [table, id, sequence, kind, task] of historical)
    for (const operation of ["INSERT", "UPDATE"])
      yield* sql.unsafe(`CREATE TRIGGER ${table}_${id}_physical_${operation.toLowerCase()} BEFORE ${operation} ON ${table} BEGIN
        SELECT CASE WHEN EXISTS(SELECT 1 FROM work_task_claim_references r WHERE r.event_id=NEW.${id} AND (r.sequence<>NEW.${sequence}
          OR (r.known_kind IS NOT NULL AND r.known_kind<>(${kind.replaceAll("r.", "NEW.")})) OR (r.known_task_id IS NOT NULL AND (${task.replaceAll("r.", "NEW.")}) IS NOT NULL AND r.known_task_id<>(${task.replaceAll("r.", "NEW.")}))))
          OR EXISTS(SELECT 1 FROM work_task_claim_references r WHERE r.sequence=NEW.${sequence} AND r.event_id<>NEW.${id})
          THEN RAISE(ABORT,'historical physical reference conflict') END;
      END`);
  for (const [table, selector, accepted] of [
    ["work_task_executable_claims", "claim_event_id", true],
    ["work_task_claim_uncertainties", "uncertain_event_id", false],
    ["work_task_claim_releases", "release_event_id", false],
  ] as const) {
    const id = accepted ? "NEW.accepted_event_id" : "NEW.event_id";
    const seq = accepted ? "NEW.accepted_sequence" : "NEW.sequence";
    yield* sql.unsafe(`CREATE TRIGGER ${table}_references AFTER INSERT ON ${table} BEGIN
      SELECT CASE WHEN EXISTS(SELECT 1 FROM (${descriptors("NEW.event_json", seq, accepted)}) WHERE role<>'recorded' AND (event_id=${id} OR sequence>=${seq})) THEN RAISE(ABORT,'physical reference causal order') END;
      INSERT INTO work_task_claim_references(owner_event_id,${selector},reference_role,position,event_id,sequence,known_kind,known_task_id)
      SELECT ${id},${id},role,position,event_id,sequence,known_kind,known_task_id FROM (${descriptors("NEW.event_json", seq, accepted)});
    END`);
  }
  const definitionKeys = [
    "task_id",
    "definition_revision",
    "action_policy_revision",
    "definition_json",
    "attribution_json",
    "accepted_event_id",
    "accepted_at",
  ];
  for (const operation of ["INSERT", "UPDATE"])
    yield* sql.unsafe(`CREATE TRIGGER work_task_physical_definition_${operation.toLowerCase()} BEFORE ${operation} ON work_task_accepted_definitions BEGIN
      SELECT CASE WHEN ${operation === "UPDATE" ? `(EXISTS(SELECT 1 FROM work_task_executable_claims WHERE task_id=OLD.task_id AND accepted_definition_revision=OLD.definition_revision) AND (${definitionKeys.map((k) => `NEW.${k} IS NOT OLD.${k}`).join(" OR ")})) OR ` : ""}
        EXISTS(SELECT 1 FROM work_task_accepted_definitions d JOIN work_task_executable_claims c ON c.task_id=d.task_id AND c.accepted_definition_revision=d.definition_revision
          WHERE ((d.task_id=NEW.task_id AND d.definition_revision=NEW.definition_revision) OR d.accepted_event_id=NEW.accepted_event_id)${operation === "UPDATE" ? " AND NOT(d.task_id=OLD.task_id AND d.definition_revision=OLD.definition_revision)" : ""})
        THEN RAISE(ABORT,'physical accepted definition conflict') END;
    END`);
});
