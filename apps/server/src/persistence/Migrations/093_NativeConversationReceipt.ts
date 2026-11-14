import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

const identity = [
  "format_version",
  "transport_id",
  "environment_id",
  "project_id",
  "thread_id",
  "run_id",
  "run_ordinal",
  "attempt_id",
  "root_node_id",
  "intent_id",
  "driver",
  "provider_instance_id",
  "provider_session_id",
  "provider_thread_id",
  "method",
  "selection_json",
  "selection_hash",
  "input_json",
  "input_hash",
  "request_id_json",
  "created_at",
];
const outcome = ["local_outcome", "local_reason", "local_observed_at"];
const response = ["response_json", "response_hash", "response_observed_at"];
const digest = (column: string) => `length(${column})=64 AND ${column} NOT GLOB '*[^0-9a-f]*'`;
const empty = (columns: readonly string[], prefix = "") =>
  columns.map((column) => `${prefix}${column} IS NULL`).join(" AND ");
const present = (columns: readonly string[]) =>
  columns.map((column) => `${column} IS NOT NULL`).join(" AND ");

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(`CREATE TABLE orchestration_v2_native_conversations (
    format_version INTEGER NOT NULL CHECK(format_version=1),
    transport_id TEXT PRIMARY KEY NOT NULL REFERENCES orchestration_v2_submission_transports(transport_id),
    environment_id TEXT NOT NULL CHECK(length(environment_id)>0),
    project_id TEXT NOT NULL CHECK(length(project_id)>0),
    thread_id TEXT NOT NULL CHECK(length(thread_id)>0),
    run_id TEXT NOT NULL CHECK(length(run_id)>0),
    run_ordinal INTEGER NOT NULL CHECK(typeof(run_ordinal)='integer' AND run_ordinal BETWEEN 1 AND 9007199254740991),
    attempt_id TEXT NOT NULL CHECK(length(attempt_id)>0),
    root_node_id TEXT NOT NULL CHECK(length(root_node_id)>0),
    intent_id TEXT NOT NULL CHECK(length(intent_id)>0),
    driver TEXT NOT NULL CHECK(driver='codex'),
    provider_instance_id TEXT NOT NULL CHECK(length(provider_instance_id)>0),
    provider_session_id TEXT NOT NULL CHECK(length(provider_session_id)>0),
    provider_thread_id TEXT NOT NULL CHECK(length(provider_thread_id)>0),
    method TEXT NOT NULL CHECK(method='thread/start'),
    selection_json TEXT NOT NULL CHECK(json_valid(selection_json) AND json_type(selection_json)='object' AND COALESCE(
      json_extract(selection_json,'$.v')=1 AND json_extract(selection_json,'$.kind')='fresh-conversation' AND
      json_extract(selection_json,'$.transportId')=transport_id AND json_extract(selection_json,'$.environmentId')=environment_id AND
      json_extract(selection_json,'$.projectId')=project_id AND json_extract(selection_json,'$.threadId')=thread_id AND
      json_extract(selection_json,'$.runId')=run_id AND json_extract(selection_json,'$.runOrdinal')=run_ordinal AND
      json_extract(selection_json,'$.attemptId')=attempt_id AND json_extract(selection_json,'$.rootNodeId')=root_node_id AND
      json_extract(selection_json,'$.intentId')=intent_id AND json_extract(selection_json,'$.driver')=driver AND
      json_extract(selection_json,'$.providerInstanceId')=provider_instance_id AND json_extract(selection_json,'$.providerSessionId')=provider_session_id AND
      json_extract(selection_json,'$.providerThreadId')=provider_thread_id AND json_type(selection_json,'$.invocation')='integer' AND
      json_extract(selection_json,'$.invocation') BETWEEN 1 AND 9007199254740991 AND json_type(selection_json,'$.expectedClaim')='object' AND
      json_type(selection_json,'$.expectedClaim.operationId')='text' AND length(json_extract(selection_json,'$.expectedClaim.operationId'))>0 AND
      json_type(selection_json,'$.expectedClaim.expectedGeneration')='integer' AND json_extract(selection_json,'$.expectedClaim.expectedGeneration') BETWEEN 0 AND 9007199254740991 AND
      json_type(selection_json,'$.expectedClaim.expectedFingerprint')='text' AND ${digest("json_extract(selection_json,'$.expectedClaim.expectedFingerprint')")} AND
      json_type(selection_json,'$.expectedClaim.bootId')='text' AND length(json_extract(selection_json,'$.expectedClaim.bootId'))>0 AND
      json_type(selection_json,'$.expectedClaim.workerId')='text' AND length(json_extract(selection_json,'$.expectedClaim.workerId'))>0,0)),
    selection_hash TEXT NOT NULL CHECK(${digest("selection_hash")}),
    input_json TEXT NOT NULL CHECK(json_valid(input_json) AND json_type(input_json)='object'),
    input_hash TEXT NOT NULL CHECK(${digest("input_hash")}),
    request_id_json TEXT NOT NULL CHECK(json_valid(request_id_json) AND json_type(request_id_json)='integer' AND json_extract(request_id_json,'$') BETWEEN 1 AND 9007199254740991),
    created_at TEXT NOT NULL CHECK(length(created_at)>0),
    local_outcome TEXT, local_reason TEXT, local_observed_at TEXT,
    response_json TEXT, response_hash TEXT, response_observed_at TEXT,
    CHECK((${empty(outcome)}) OR (${present(outcome)} AND length(local_observed_at)>0 AND
      local_outcome IN ('not_submitted','unknown','submitted') AND local_reason IN ('completed','invalid_frame','queue_limit','output_blocked','reentrant','frame_limit','invalid_timeout','continuation','wrapper','sink','timeout','interrupted','outcome_hook') AND
      ((local_outcome='submitted' AND local_reason='completed') OR (local_outcome<>'submitted' AND local_reason<>'completed')))),
    CHECK((${empty(response)}) OR (${present(response)} AND local_outcome IS NOT NULL AND local_outcome='submitted' AND length(response_observed_at)>0 AND
      ${digest("response_hash")} AND length(CAST(response_json AS BLOB))<=16384 AND json_valid(response_json) AND json_type(response_json)='object' AND COALESCE(
      json_extract(response_json,'$.v')=1 AND json_type(response_json,'$.nativeThreadId')='text' AND length(json_extract(response_json,'$.nativeThreadId'))>0 AND
      json_type(response_json,'$.returnedProviderThreadId')='text' AND length(json_extract(response_json,'$.returnedProviderThreadId'))>0 AND
      json_extract(response_json,'$.driver')=driver AND json_extract(response_json,'$.providerInstanceId')=provider_instance_id AND
      json_extract(response_json,'$.providerSessionId')=provider_session_id AND json_extract(response_json,'$.threadId')=thread_id,0)))
  ) WITHOUT ROWID`);
  yield* sql.unsafe(`CREATE TRIGGER native_conversations_insert_guard BEFORE INSERT ON orchestration_v2_native_conversations
    WHEN NOT (${empty([...outcome, ...response], "NEW.")}) OR EXISTS(SELECT 1 FROM orchestration_v2_native_conversations WHERE transport_id=NEW.transport_id)
    OR NOT EXISTS(SELECT 1 FROM orchestration_v2_submission_transports t WHERE t.transport_id=NEW.transport_id AND
      t.environment_id=NEW.environment_id AND t.project_id=NEW.project_id AND t.opening_thread_id=NEW.thread_id AND
      t.driver=NEW.driver AND t.provider_instance_id=NEW.provider_instance_id AND t.provider_session_id=NEW.provider_session_id AND
      t.binding_json IS NOT NULL AND t.settlement_json IS NULL AND
      json_type(t.creation_context_json,'$.expectedClaim')='object' AND
      json_extract(t.creation_context_json,'$.expectedClaim')=json_extract(NEW.selection_json,'$.expectedClaim'))
    BEGIN SELECT RAISE(ABORT,'conversation requires fresh original bound transport'); END`);
  yield* sql.unsafe(`CREATE TRIGGER native_conversations_update_guard BEFORE UPDATE ON orchestration_v2_native_conversations
    WHEN ${identity.map((column) => `NEW.${column} IS NOT OLD.${column}`).join(" OR ")}
      OR ${[...outcome, ...response].map((column) => `(OLD.${column} IS NOT NULL AND NEW.${column} IS NOT OLD.${column})`).join(" OR ")}
      OR (OLD.response_json IS NULL AND NEW.response_json IS NOT NULL AND OLD.local_outcome IS NOT 'submitted')
    BEGIN SELECT RAISE(ABORT,'conversation identity or observed evidence is immutable'); END`);
  yield* sql`CREATE TRIGGER native_conversations_delete_guard BEFORE DELETE ON orchestration_v2_native_conversations
    BEGIN SELECT RAISE(ABORT,'conversation tombstone is permanent'); END`;
  yield* sql`CREATE INDEX native_conversations_thread_unresolved ON orchestration_v2_native_conversations(environment_id,thread_id)
    WHERE local_outcome IS NULL OR local_outcome='unknown' OR (local_outcome='submitted' AND response_json IS NULL)`;
  yield* sql`CREATE INDEX native_conversations_run ON orchestration_v2_native_conversations(environment_id,run_id)`;
});
