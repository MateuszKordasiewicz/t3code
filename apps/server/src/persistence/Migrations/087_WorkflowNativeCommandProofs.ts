import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE workflow_native_command_proofs (command_id TEXT PRIMARY KEY NOT NULL CHECK(length(command_id)>0) REFERENCES orchestration_command_receipts(command_id), intent_id TEXT NOT NULL REFERENCES workflow_native_launch_intents(intent_id), phase TEXT NOT NULL CHECK(phase IN ('claim','message')), proof_json TEXT NOT NULL CHECK(json_valid(proof_json) AND json_type(proof_json)='object'), proof_hash TEXT NOT NULL CHECK(length(proof_hash)=64 AND proof_hash NOT GLOB '*[^0-9a-f]*'), UNIQUE(intent_id,phase))`;
  yield* sql`CREATE TRIGGER workflow_native_command_proofs_insert_guard BEFORE INSERT ON workflow_native_command_proofs WHEN EXISTS (SELECT 1 FROM workflow_native_command_proofs WHERE command_id=NEW.command_id OR (intent_id=NEW.intent_id AND phase=NEW.phase)) BEGIN SELECT RAISE(ABORT, 'workflow native command proof is immutable'); END`;
  yield* sql`CREATE TRIGGER workflow_native_command_proofs_update_guard BEFORE UPDATE ON workflow_native_command_proofs BEGIN SELECT RAISE(ABORT, 'workflow native command proof is immutable'); END`;
  yield* sql`CREATE TRIGGER workflow_native_command_proofs_delete_guard BEFORE DELETE ON workflow_native_command_proofs BEGIN SELECT RAISE(ABORT, 'workflow native command proof is immutable'); END`;
});
