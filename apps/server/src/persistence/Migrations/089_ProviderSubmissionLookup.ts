import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE INDEX submission_transports_thread_history ON orchestration_v2_submission_transports(environment_id,opening_thread_id)`;
  yield* sql`CREATE INDEX submission_transports_session_history ON orchestration_v2_submission_transports(environment_id,provider_session_id)`;
  yield* sql`CREATE INDEX submission_attempts_thread_history ON orchestration_v2_submission_attempts(environment_id,thread_id)`;
  yield* sql`CREATE INDEX submission_attempts_provider_thread_history ON orchestration_v2_submission_attempts(environment_id,provider_thread_id)`;
  yield* sql`CREATE INDEX submission_attempts_native_history ON orchestration_v2_submission_attempts(environment_id,driver,provider_instance_id,native_thread_id)`;
  yield* sql`CREATE INDEX orchestration_v2_provider_threads_native_lookup ON orchestration_v2_projection_provider_threads(CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.nativeThreadRef.nativeId') END,provider_thread_id)`;
});
