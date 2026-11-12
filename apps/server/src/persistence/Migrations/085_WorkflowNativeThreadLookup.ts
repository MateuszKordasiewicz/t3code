import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`CREATE INDEX workflow_operations_native_thread ON workflow_operations(CASE WHEN json_valid(record_json) THEN json_extract(record_json, '$.parameters.nativeThreadId') END)`;
});
