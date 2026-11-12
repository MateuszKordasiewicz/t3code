import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE workflow_planning_observations (pass_id TEXT PRIMARY KEY REFERENCES workflow_planning_passes(id), operation_id TEXT NOT NULL UNIQUE REFERENCES workflow_operations(id), native_thread_id TEXT NOT NULL, native_run_id TEXT NOT NULL, terminal_message_id TEXT NOT NULL, manifest_hash TEXT NOT NULL CHECK(length(manifest_hash)=64 AND manifest_hash NOT GLOB '*[^0-9a-f]*'), observation_json TEXT NOT NULL CHECK(json_valid(observation_json)), UNIQUE(native_thread_id,native_run_id,terminal_message_id));`;
  yield* sql`CREATE TABLE workflow_planning_dispositions (review_pass_id TEXT NOT NULL REFERENCES workflow_planning_passes(id), finding_id TEXT NOT NULL CHECK(length(finding_id)>0), repair_pass_id TEXT NOT NULL REFERENCES workflow_planning_passes(id), disposition_json TEXT NOT NULL CHECK(json_valid(disposition_json)), PRIMARY KEY(review_pass_id,finding_id));`;
  yield* sql`CREATE TABLE workflow_planning_settlements (operation_id TEXT PRIMARY KEY REFERENCES workflow_operations(id), fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64 AND fingerprint NOT GLOB '*[^0-9a-f]*'), result_json TEXT NOT NULL CHECK(json_valid(result_json)));`;
});
