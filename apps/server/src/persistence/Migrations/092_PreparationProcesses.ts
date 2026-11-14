import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

const maximum = 9007199254740991;
const text = (value: string) => `typeof(${value})='text' AND length(${value})>0`;
const digest = (value: string) =>
  `${text(value)} AND length(${value})=64 AND ${value} NOT GLOB '*[^0-9a-f]*'`;
const integer = (value: string, minimum = 0) =>
  `typeof(${value})='integer' AND ${value} BETWEEN ${minimum} AND ${maximum}`;
const member = (column: string, path: string) => `json_extract(${column},'$.${path}')`;
const type = (column: string, path: string, expected: string) =>
  `json_type(${column},'$.${path}')='${expected}'`;
const jsonText = (column: string, path: string) =>
  `${type(column, path, "text")} AND length(${member(column, path)})>0`;
const jsonNullableText = (column: string, path: string) =>
  `(${type(column, path, "null")} OR (${jsonText(column, path)}))`;
const jsonInteger = (column: string, path: string, minimum = 0) =>
  `${type(column, path, "integer")} AND ${member(column, path)} BETWEEN ${minimum} AND ${maximum}`;
const jsonDigest = (column: string, path: string) =>
  `${type(column, path, "text")} AND ${digest(member(column, path))}`;
const bindingFields = ["unit", "invocationId", "controlGroup", "pid", "pidStartTicks"];
const currentFields = [
  "driver",
  "providerInstanceId",
  "providerThreadId",
  "providerSessionId",
  "nativeThreadId",
];
const claimFields = [
  "operationId",
  "expectedFingerprint",
  "expectedGeneration",
  "bootId",
  "workerId",
];

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const context = "creation_context_json";
  const binding = "binding_json";
  const settlement = "settlement_json";
  const identity = [
    "format_version",
    "resource_id",
    "job_id",
    "job_request_identity_hash",
    "environment_id",
    "project_id",
    "thread_id",
    "run_id",
    "server_boot_id",
    "host_boot_id",
    "scope_namespace",
    "proposed_unit",
    "creation_input_hash",
    "creation_context_json",
    "creation_context_hash",
    "created_at",
  ];
  const contextShadows = {
    "jobSelection.jobId": "job_id",
    "jobSelection.requestIdentityHash": "job_request_identity_hash",
    "jobSelection.serverBootId": "server_boot_id",
    environmentId: "environment_id",
    projectId: "project_id",
    threadId: "thread_id",
    runId: "run_id",
    "host.hostBootId": "host_boot_id",
    "host.scopeNamespace": "scope_namespace",
    proposedUnit: "proposed_unit",
    creationInputHash: "creation_input_hash",
  };
  const parentOpen = `EXISTS(SELECT 1 FROM orchestration_v2_preparation_jobs p
    WHERE p.job_id=NEW.job_id AND p.state='active' AND p.code_closed_at IS NULL)`;
  yield* sql.unsafe(`CREATE TABLE orchestration_v2_preparation_process_resources (
 format_version INTEGER NOT NULL CHECK(typeof(format_version)='integer' AND format_version=1),
 resource_id TEXT PRIMARY KEY NOT NULL CHECK(${text("resource_id")}),
 job_id TEXT NOT NULL REFERENCES orchestration_v2_preparation_jobs(job_id) CHECK(${text("job_id")}),
 job_request_identity_hash TEXT NOT NULL CHECK(${digest("job_request_identity_hash")}),
 environment_id TEXT NOT NULL CHECK(${text("environment_id")}),
 project_id TEXT NOT NULL CHECK(${text("project_id")}),
 thread_id TEXT NOT NULL CHECK(${text("thread_id")}),
 run_id TEXT CHECK(run_id IS NULL OR (${text("run_id")})),
 server_boot_id TEXT NOT NULL CHECK(${text("server_boot_id")}),
 host_boot_id TEXT NOT NULL CHECK(${text("host_boot_id")}),
 scope_namespace TEXT NOT NULL CHECK(${text("scope_namespace")}),
 proposed_unit TEXT NOT NULL UNIQUE CHECK(${text("proposed_unit")}),
 creation_input_hash TEXT NOT NULL CHECK(${digest("creation_input_hash")}),
 creation_context_json TEXT NOT NULL CHECK(typeof(creation_context_json)='text' AND json_valid(creation_context_json) AND json_type(creation_context_json)='object'),
 creation_context_hash TEXT NOT NULL CHECK(${digest("creation_context_hash")}),
 created_at TEXT NOT NULL CHECK(${text("created_at")}),
 record_version INTEGER NOT NULL DEFAULT 0 CHECK(${integer("record_version")}),
 state TEXT NOT NULL DEFAULT 'active' CHECK(typeof(state)='text' AND state IN ('active','quiescing','unknown','settled')),
 creation_started_at TEXT CHECK(creation_started_at IS NULL OR (${text("creation_started_at")})),
 child_pid INTEGER CHECK(child_pid IS NULL OR (${integer("child_pid", 1)})),
 binding_json TEXT CHECK(binding_json IS NULL OR typeof(binding_json)='text'),
 binding_hash TEXT CHECK(binding_hash IS NULL OR (${digest("binding_hash")})),
 bound_at TEXT CHECK(bound_at IS NULL OR (${text("bound_at")})),
 release_started_at TEXT CHECK(release_started_at IS NULL OR (${text("release_started_at")})),
 settlement_json TEXT CHECK(settlement_json IS NULL OR typeof(settlement_json)='text'),
 settlement_hash TEXT CHECK(settlement_hash IS NULL OR (${digest("settlement_hash")})),
 settled_at TEXT CHECK(settled_at IS NULL OR (${text("settled_at")})),
 CHECK(COALESCE(
   ${type(context, "v", "integer")} AND ${member(context, "v")}=1 AND
   ${type(context, "profile", "text")} AND ${member(context, "profile")}='linux-node-no-stdin-v1' AND
   ${type(context, "jobSelection", "object")} AND ${jsonText(context, "jobSelection.entryKey")} AND
   ${jsonInteger(context, "jobSelection.expectedVersion")} AND ${type(context, "host", "object")} AND
   ${Object.entries(contextShadows)
     .map(
       ([path, column]) =>
         `(${path === "runId" ? jsonNullableText(context, path) : jsonText(context, path)}) AND ${member(context, path)} IS ${column}`,
     )
     .join(" AND ")} AND
   (${type(context, "executionClaim", "null")} OR (
     ${type(context, "executionClaim", "object")} AND
     ${jsonText(context, "executionClaim.operationId")} AND ${jsonDigest(context, "executionClaim.expectedFingerprint")} AND
     ${jsonInteger(context, "executionClaim.expectedGeneration")} AND ${jsonText(context, "executionClaim.bootId")} AND
     ${member(context, "executionClaim.bootId")}=server_boot_id AND ${jsonText(context, "executionClaim.workerId")})) AND
   (${type(context, "current", "null")} OR (${type(context, "current", "object")} AND
     ${currentFields.map((field) => (field === "providerSessionId" || field === "nativeThreadId" ? jsonNullableText(context, `current.${field}`) : jsonText(context, `current.${field}`))).join(" AND ")})),0)),
 CHECK(child_pid IS NULL OR creation_started_at IS NOT NULL),
 CHECK((binding_json IS NULL AND binding_hash IS NULL AND bound_at IS NULL) OR
   (binding_json IS NOT NULL AND binding_hash IS NOT NULL AND bound_at IS NOT NULL AND
    child_pid IS NOT NULL AND creation_started_at IS NOT NULL AND json_valid(binding_json) AND json_type(binding_json)='object' AND COALESCE(
      ${jsonText(binding, "unit")} AND ${member(binding, "unit")}=proposed_unit AND
      ${jsonText(binding, "invocationId")} AND length(${member(binding, "invocationId")})=32 AND
      ${member(binding, "invocationId")} NOT GLOB '*[^0-9a-f]*' AND ${member(binding, "invocationId")}<>'00000000000000000000000000000000' AND
      ${jsonText(binding, "controlGroup")} AND substr(${member(binding, "controlGroup")},1,1)='/' AND
      ${jsonInteger(binding, "pid", 1)} AND ${member(binding, "pid")}=child_pid AND
      ${jsonText(binding, "pidStartTicks")} AND ${member(binding, "pidStartTicks")} NOT GLOB '*[^0-9]*' AND substr(${member(binding, "pidStartTicks")},1,1) BETWEEN '1' AND '9',0))),
 CHECK(release_started_at IS NULL OR binding_json IS NOT NULL),
 CHECK((state='settled')=(settlement_json IS NOT NULL)),
 CHECK((settlement_json IS NULL AND settlement_hash IS NULL AND settled_at IS NULL) OR
   (settlement_json IS NOT NULL AND settlement_hash IS NOT NULL AND settled_at IS NOT NULL AND binding_json IS NOT NULL AND
    json_valid(settlement_json) AND json_type(settlement_json)='object' AND COALESCE(
      ${type(settlement, "v", "integer")} AND ${member(settlement, "v")}=1 AND
      ${jsonText(settlement, "resourceId")} AND ${member(settlement, "resourceId")}=resource_id AND
      ${jsonDigest(settlement, "contextHash")} AND ${member(settlement, "contextHash")}=creation_context_hash AND
      ${jsonInteger(settlement, "priorVersion")} AND ${type(settlement, "childExit", "object")} AND
      ${jsonInteger(settlement, "childExit.pid", 1)} AND ${member(settlement, "childExit.pid")}=child_pid AND
      ${jsonInteger(settlement, "childExit.exitCode")} AND ${type(settlement, "scope", "object")} AND
      ${type(settlement, "scope.binding", "object")} AND ${type(settlement, "scope.observation", "text")} AND
      ${member(settlement, "scope.observation")} IN ('empty','removed') AND
      ${bindingFields.map((field) => `${type(settlement, `scope.binding.${field}`, field === "pid" ? "integer" : "text")} AND ${member(settlement, `scope.binding.${field}`)} IS ${member(binding, field)}`).join(" AND ")},0)))
) WITHOUT ROWID`);
  yield* sql.unsafe(`CREATE TRIGGER preparation_process_resources_insert_guard BEFORE INSERT ON orchestration_v2_preparation_process_resources
 WHEN NEW.record_version<>0 OR NEW.state<>'active' OR NEW.creation_started_at IS NOT NULL OR NEW.child_pid IS NOT NULL OR NEW.binding_json IS NOT NULL OR NEW.binding_hash IS NOT NULL OR NEW.bound_at IS NOT NULL OR NEW.release_started_at IS NOT NULL OR NEW.settlement_json IS NOT NULL OR NEW.settlement_hash IS NOT NULL OR NEW.settled_at IS NOT NULL
 OR EXISTS(SELECT 1 FROM orchestration_v2_preparation_process_resources WHERE resource_id=NEW.resource_id OR proposed_unit=NEW.proposed_unit)
 OR NOT EXISTS(SELECT 1 FROM orchestration_v2_preparation_jobs p WHERE p.job_id=NEW.job_id AND p.state='active' AND p.code_closed_at IS NULL AND p.process_exposed=1 AND
   p.request_identity_hash=NEW.job_request_identity_hash AND p.environment_id=NEW.environment_id AND p.project_id=NEW.project_id AND p.thread_id=NEW.thread_id AND p.run_id IS NEW.run_id AND p.server_boot_id=NEW.server_boot_id AND
   ${member("NEW.creation_context_json", "jobSelection.entryKey")}=p.entry_key AND ${member("NEW.creation_context_json", "jobSelection.expectedVersion")}=p.record_version AND
   json_valid(p.request_identity_json) AND json_type(p.request_identity_json,'$.v')='integer' AND
   ((json_extract(p.request_identity_json,'$.v')=2 AND ${type("NEW.creation_context_json", "executionClaim", "null")} AND ${type("NEW.creation_context_json", "current", "null")}) OR
    (json_extract(p.request_identity_json,'$.v')=3 AND ${type("NEW.creation_context_json", "executionClaim", "object")} AND ${type("NEW.creation_context_json", "current", "object")} AND
      ${claimFields.map((field) => `${member("NEW.creation_context_json", `executionClaim.${field}`)} IS ${member("p.request_identity_json", `nativeOwner.executionClaim.${field}`)}`).join(" AND ")} AND
      ${currentFields.map((field) => `json_type(p.request_identity_json,'$.request.current.${field}')=json_type(NEW.creation_context_json,'$.current.${field}') AND ${member("NEW.creation_context_json", `current.${field}`)} IS ${member("p.request_identity_json", `request.current.${field}`)}`).join(" AND ")}))) BEGIN SELECT RAISE(ABORT,'preparation process requires fresh parent custody'); END`);
  const writeOnce = [
    "creation_started_at",
    "child_pid",
    "binding_json",
    "binding_hash",
    "bound_at",
    "release_started_at",
  ];
  yield* sql.unsafe(`CREATE TRIGGER preparation_process_resources_update_guard BEFORE UPDATE ON orchestration_v2_preparation_process_resources
 WHEN OLD.state='settled' OR ${identity.map((column) => `NEW.${column} IS NOT OLD.${column}`).join(" OR ")} OR
 NEW.record_version<>OLD.record_version+1 OR
 NOT ((OLD.state='active' AND NEW.state IN ('active','quiescing','unknown','settled')) OR
      (OLD.state='quiescing' AND NEW.state IN ('quiescing','unknown','settled')) OR
      (OLD.state='unknown' AND NEW.state IN ('unknown','settled'))) OR
 ${writeOnce.map((column) => `(OLD.${column} IS NOT NULL AND NEW.${column} IS NOT OLD.${column})`).join(" OR ")} OR
 (OLD.creation_started_at IS NULL AND NEW.creation_started_at IS NOT NULL AND NOT
   ((OLD.state='active' AND NEW.state='active' AND ${parentOpen}) OR
    (OLD.binding_json IS NULL AND NEW.binding_json IS NOT NULL))) OR
 (OLD.release_started_at IS NULL AND NEW.release_started_at IS NOT NULL AND NOT
   (OLD.state='active' AND NEW.state='active' AND OLD.binding_json IS NOT NULL AND ${parentOpen})) OR
 (NEW.state='settled' AND (OLD.binding_json IS NULL OR ${member("NEW.settlement_json", "priorVersion")} IS NOT OLD.record_version))
 BEGIN SELECT RAISE(ABORT,'preparation process identity and evidence are monotonic'); END`);
  yield* sql`CREATE TRIGGER preparation_process_resources_delete_guard BEFORE DELETE ON orchestration_v2_preparation_process_resources
 BEGIN SELECT RAISE(ABORT,'preparation process history is permanent'); END`;
  yield* sql`CREATE INDEX preparation_process_resources_job_history ON orchestration_v2_preparation_process_resources(job_id,resource_id)`;
});
