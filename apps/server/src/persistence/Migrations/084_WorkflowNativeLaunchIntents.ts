import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE workflow_native_launch_intents (
 intent_id TEXT PRIMARY KEY NOT NULL CHECK(length(intent_id)>0),
 workflow_run_id TEXT REFERENCES workflow_runs(id), planning_session_id TEXT REFERENCES workflow_planning_sessions(id),
 operation_id TEXT NOT NULL UNIQUE CHECK(length(operation_id)>0),
 attempt_id TEXT, pass_id TEXT UNIQUE, item_id TEXT, charge_attempt INTEGER NOT NULL CHECK(charge_attempt IN (0,1)), repair INTEGER NOT NULL CHECK(repair IN (0,1)),
 role TEXT NOT NULL CHECK(length(role)>0), environment_id TEXT NOT NULL CHECK(length(environment_id)>0), project_id TEXT NOT NULL CHECK(length(project_id)>0),
 launch_command_id TEXT NOT NULL UNIQUE CHECK(length(launch_command_id)>0), thread_id TEXT NOT NULL CHECK(length(thread_id)>0), message_id TEXT NOT NULL CHECK(length(message_id)>0),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64 AND fingerprint NOT GLOB '*[^0-9a-f]*'), draft_json TEXT NOT NULL CHECK(json_valid(draft_json) AND json_type(draft_json)='object'),
 generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>=0 AND generation<=9007199254740991),
 state TEXT NOT NULL CHECK(state IN ('reserved','dispatching','unknown','finalized','cancelled')),
 held INTEGER NOT NULL DEFAULT 0 CHECK(held IN (0,1)), boot_id TEXT, worker_id TEXT, native_run_id TEXT UNIQUE,
 finalized_operation_id TEXT UNIQUE REFERENCES workflow_operations(id),
 CHECK((workflow_run_id IS NOT NULL AND planning_session_id IS NULL AND pass_id IS NULL AND ((attempt_id IS NULL AND item_id IS NULL AND charge_attempt=0) OR (attempt_id IS NOT NULL AND length(attempt_id)>0 AND item_id IS NOT NULL AND length(item_id)>0)) AND (charge_attempt=1 OR repair=0)) OR (workflow_run_id IS NULL AND planning_session_id IS NOT NULL AND pass_id IS NOT NULL AND length(pass_id)>0 AND attempt_id IS NULL AND item_id IS NULL AND charge_attempt=0)),
 CHECK((boot_id IS NULL AND worker_id IS NULL) OR (boot_id IS NOT NULL AND length(boot_id)>0 AND worker_id IS NOT NULL AND length(worker_id)>0)),
 CHECK(state NOT IN ('dispatching','unknown') OR boot_id IS NOT NULL),
 CHECK(held=0 OR state='reserved'),
 CHECK(native_run_id IS NULL OR length(native_run_id)>0),
 CHECK((state='finalized' AND native_run_id IS NOT NULL AND finalized_operation_id IS NOT NULL AND finalized_operation_id=operation_id) OR (state<>'finalized' AND finalized_operation_id IS NULL)),
 UNIQUE(thread_id,message_id)
)`;
  yield* sql`CREATE UNIQUE INDEX workflow_native_intents_charged_attempt ON workflow_native_launch_intents(workflow_run_id,attempt_id) WHERE charge_attempt=1`;
  yield* sql`CREATE INDEX workflow_native_launch_intents_thread ON workflow_native_launch_intents(thread_id)`;
  yield* sql`CREATE INDEX workflow_native_launch_intents_run ON workflow_native_launch_intents(workflow_run_id,state)`;
  yield* sql`CREATE INDEX workflow_native_launch_intents_session ON workflow_native_launch_intents(planning_session_id,state)`;
  yield* sql`CREATE TABLE workflow_native_intent_receipts (
 command_id TEXT PRIMARY KEY NOT NULL CHECK(length(command_id)>0), receipt_id TEXT NOT NULL UNIQUE CHECK(length(receipt_id)>0),
 intent_id TEXT NOT NULL UNIQUE REFERENCES workflow_native_launch_intents(intent_id),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64 AND fingerprint NOT GLOB '*[^0-9a-f]*'),
 result_json TEXT NOT NULL CHECK(json_valid(result_json) AND json_type(result_json)='object')
)`;
});
