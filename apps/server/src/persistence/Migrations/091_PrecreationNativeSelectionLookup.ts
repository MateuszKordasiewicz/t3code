import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE INDEX preparation_jobs_native_history ON orchestration_v2_preparation_jobs (
 environment_id,
 CASE WHEN json_valid(request_identity_json) THEN json_extract(request_identity_json,'$.nativeSelection.driver') END,
 CASE WHEN json_valid(request_identity_json) THEN json_extract(request_identity_json,'$.nativeSelection.providerInstanceId') END,
 CASE WHEN json_valid(request_identity_json) THEN json_extract(request_identity_json,'$.nativeSelection.nativeId') END,
 job_id
)`;
  yield* sql`CREATE INDEX workflow_native_material_native_history ON workflow_native_reservation_material (
 CASE WHEN json_valid(material_json) THEN json_extract(material_json,'$.launch.importedNativeThread.ref.driver') END,
 CASE WHEN json_valid(material_json) THEN json_extract(material_json,'$.launch.modelSelection.instanceId') END,
 CASE WHEN json_valid(material_json) THEN json_extract(material_json,'$.launch.importedNativeThread.ref.nativeId') END,
 intent_id
)`;
});
