import * as Effect from "effect/Effect";
import type * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import type { ServerConfig } from "../config.ts";
import {
  makeNativeCommandProtocol,
  NativeCommandProofError,
} from "./WorkflowNativeCommandProtocol.ts";
import { snapshotNativeInput } from "./WorkflowNativeCommandSnapshot.ts";
import * as Store from "./WorkflowRunStore.ts";
export {
  encodeNativeCommandProof,
  NativeCommandProofError,
} from "./WorkflowNativeCommandProtocol.ts";

export function makeNativeCommandProofVerifier(
  sql: SqlClient.SqlClient,
  config: ServerConfig["Service"],
  path: Path.Path,
) {
  const protocol = makeNativeCommandProtocol(sql, config, path);
  return Effect.gen(function* () {
    const store = yield* Store.make().pipe(Effect.provideService(SqlClient.SqlClient, sql));
    const kernel = yield* protocol;
    const reservation = Effect.fn("WorkflowNativeCommandProof.readReservation")(function* (
      snapshot: ReturnType<typeof snapshotNativeInput>,
    ) {
      return yield* store
        .readNativeReservationInTransaction({ intentId: snapshot.hint.intentId })
        .pipe(
          Effect.mapError(
            (error) =>
              new NativeCommandProofError({
                reason: error.reason === "stale" ? "conflict" : error.reason,
              }),
          ),
        );
    });
    const prepare = Effect.fn("WorkflowNativeCommandProof.prepareInTransaction")(function* (
      snapshot: ReturnType<typeof snapshotNativeInput>,
    ) {
      return yield* kernel.prepareInTransaction(
        yield* reservation(snapshot),
        snapshot.command,
        snapshot.hint,
      );
    });
    const read = Effect.fn("WorkflowNativeCommandProof.readStoredInTransaction")(function* (
      snapshot: ReturnType<typeof snapshotNativeInput>,
    ) {
      return yield* kernel.readStoredInTransaction(
        yield* reservation(snapshot),
        snapshot.command,
        snapshot.hint,
      );
    });
    return {
      prepareInTransaction(command: unknown, hint: unknown) {
        try {
          return prepare(snapshotNativeInput(command, hint));
        } catch {
          return Effect.fail(new NativeCommandProofError({ reason: "invalid" }));
        }
      },
      readStoredInTransaction(command: unknown, hint: unknown) {
        try {
          return read(snapshotNativeInput(command, hint));
        } catch {
          return Effect.fail(new NativeCommandProofError({ reason: "invalid" }));
        }
      },
    };
  });
}
