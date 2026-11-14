import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE INDEX native_conversations_native_history ON orchestration_v2_native_conversations(environment_id,driver,provider_instance_id,json_extract(response_json,'$.nativeThreadId'),transport_id)`;
  yield* sql`CREATE INDEX native_conversations_provider_history ON orchestration_v2_native_conversations(environment_id,provider_thread_id,transport_id)`;
});
