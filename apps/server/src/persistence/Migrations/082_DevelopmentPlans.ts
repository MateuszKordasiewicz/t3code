import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE development_plans (plan_id TEXT PRIMARY KEY REFERENCES workflow_plan_identities(plan_id), environment_id TEXT NOT NULL, project_id TEXT NOT NULL, title TEXT NOT NULL, current_revision_id TEXT, version INTEGER NOT NULL CHECK(version >= 0), created_at TEXT NOT NULL, FOREIGN KEY(plan_id,current_revision_id) REFERENCES development_plan_revisions(plan_id,revision_id));`;
  yield* sql`CREATE TABLE development_plan_source_packets (plan_id TEXT NOT NULL REFERENCES development_plans(plan_id), source_packet_hash TEXT NOT NULL CHECK(length(source_packet_hash)=64 AND source_packet_hash NOT GLOB '*[^0-9a-f]*'), packet_json TEXT NOT NULL CHECK(json_valid(packet_json)), PRIMARY KEY(plan_id,source_packet_hash));`;
  yield* sql`CREATE TABLE development_plan_revisions (plan_id TEXT NOT NULL, revision_id TEXT NOT NULL, revision_hash TEXT NOT NULL, source_packet_hash TEXT NOT NULL, revision_json TEXT NOT NULL CHECK(json_valid(revision_json)), created_at TEXT NOT NULL, PRIMARY KEY(plan_id,revision_id), FOREIGN KEY(plan_id,revision_id,revision_hash,source_packet_hash) REFERENCES workflow_revision_identities(plan_id,revision_id,revision_hash,source_packet_hash), FOREIGN KEY(plan_id,source_packet_hash) REFERENCES development_plan_source_packets(plan_id,source_packet_hash));`;
  yield* sql`CREATE TABLE development_plan_threads (plan_id TEXT NOT NULL REFERENCES development_plans(plan_id), thread_id TEXT NOT NULL, PRIMARY KEY(plan_id,thread_id));`;
  yield* sql`CREATE TABLE development_plan_commands (command_id TEXT PRIMARY KEY, plan_id TEXT NOT NULL REFERENCES development_plans(plan_id), fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64 AND fingerprint NOT GLOB '*[^0-9a-f]*'), result_json TEXT NOT NULL CHECK(json_valid(result_json)));`;
});
