import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import { ServerConfig } from "../config.ts";
import { makeNativeCommandProtocol } from "./WorkflowNativeCommandProtocol.ts";
import * as Store from "./WorkflowRunStore.ts";

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.WorkflowRunStore;
  const protocol = yield* makeNativeCommandProtocol(sql, yield* ServerConfig, yield* Path.Path);
  const deriveInTransaction = Effect.fn("WorkflowNativeLaunchDerivation.deriveInTransaction")(
    function* (input: { readonly intentId: string }) {
      const reservation = yield* store.readNativeReservationInTransaction({ ...input });
      const derived = yield* protocol.deriveInTransaction(reservation).pipe(
        Effect.mapError(
          (error) =>
            new Store.WorkflowRunStoreError({
              reason: error.reason === "persistence" ? "persistence" : "integrity",
            }),
        ),
      );
      return { ...derived, reservation };
    },
  );
  return { deriveInTransaction };
});

export class WorkflowNativeLaunchDerivation extends Context.Service<
  WorkflowNativeLaunchDerivation,
  Effect.Success<typeof make>
>()("t3/workflowRuns/WorkflowNativeLaunchDerivation") {}
export const layer = Layer.effect(WorkflowNativeLaunchDerivation, make);
