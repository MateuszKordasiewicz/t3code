import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE TABLE workflow_native_reservation_material (intent_id TEXT PRIMARY KEY NOT NULL REFERENCES workflow_native_launch_intents(intent_id), material_json TEXT NOT NULL CHECK(json_valid(material_json) AND json_type(material_json)='object'), material_hash TEXT NOT NULL CHECK(length(material_hash)=64 AND material_hash NOT GLOB '*[^0-9a-f]*'))`;
  yield* sql`CREATE TRIGGER workflow_native_reservation_material_insert_guard BEFORE INSERT ON workflow_native_reservation_material WHEN EXISTS (SELECT 1 FROM workflow_native_reservation_material WHERE intent_id=NEW.intent_id) BEGIN SELECT RAISE(ABORT, 'workflow native reservation material is immutable'); END`;
  yield* sql`CREATE TRIGGER workflow_native_reservation_material_update_guard BEFORE UPDATE ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'workflow native reservation material is immutable'); END`;
  yield* sql`CREATE TRIGGER workflow_native_reservation_material_delete_guard BEFORE DELETE ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'workflow native reservation material is immutable'); END`;
});
