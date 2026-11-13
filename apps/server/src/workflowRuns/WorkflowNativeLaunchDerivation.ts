import { CommandId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";

import { ServerConfig } from "../config.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import {
  buildClaimCommand,
  buildInitialMessageCommand,
} from "../orchestration-v2/ThreadLaunchCommands.ts";
import type {
  ThreadLaunchInput,
  ThreadLaunchInitialMessage,
} from "../orchestration-v2/ThreadLaunchService.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
import * as Store from "./WorkflowRunStore.ts";

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.WorkflowRunStore;
  const config = yield* ServerConfig;
  const path = yield* Path.Path;
  const projects = yield* ProjectStore.make.pipe(Effect.provideService(SqlClient.SqlClient, sql));
  const integrity = () => new Store.WorkflowRunStoreError({ reason: "integrity" });

  const deriveInTransaction = Effect.fn("WorkflowNativeLaunchDerivation.deriveInTransaction")(
    function* (input: { readonly intentId: string }) {
      const selection = { ...input };
      if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
        return yield* integrity();
      const reservation = yield* store.readNativeReservationInTransaction(selection);
      const launch = reservation.material.launch;
      const project = yield* projects.get(launch.projectId).pipe(
        Effect.mapError(
          (error) =>
            new Store.WorkflowRunStoreError({
              reason: SqlError.isSqlError(error.cause) ? "persistence" : "integrity",
            }),
        ),
      );
      if (Option.isNone(project)) return yield* integrity();
      const workspaceRoot = project.value.workspaceRoot;
      if (!workspaceRoot.trim() || !path.isAbsolute(workspaceRoot)) return yield* integrity();
      const scratchRoot = path.resolve(config.baseDir, "scratch");
      if (path.resolve(workspaceRoot) === scratchRoot) return yield* integrity();
      if (
        launch.initialMessage.attachments.length ||
        launch.initialMessage.context?.records.some(
          (record) => record.kind === "image" || record.kind === "file",
        )
      )
        return yield* integrity();
      const strategy = launch.workspaceStrategy;
      const normalization = {
        version: 1 as const,
        project: { projectId: launch.projectId, workspaceRoot },
        scratchRoot,
        workspace: { requested: strategy, effective: strategy, reason: "unchanged" as const },
        reuse: {
          requested: launch.reuseExistingThread === true,
          claimKind:
            launch.reuseExistingThread === true
              ? ("metadata-update" as const)
              : ("create" as const),
        },
        attachments: { kind: "none" as const },
      };
      // The retained material's canonical fixed-point check excludes explicit undefined values.
      const builderInput = launch as ThreadLaunchInput;
      const message = launch.initialMessage as ThreadLaunchInitialMessage;
      const claimCommand = buildClaimCommand(builderInput, launch.threadId, strategy);
      const messageCommand = buildInitialMessageCommand(
        builderInput,
        message,
        CommandId.make(`${launch.commandId}:initial-message`),
        launch.threadId,
        launch.initialMessage.messageId,
        strategy,
      );
      return {
        reservation,
        normalization,
        normalizationHash: WorkflowCanonicalHash.tagged(
          "t3.workflow-native-normalization.v1",
          normalization,
        ),
        claimCommand,
        messageCommand,
        expectedClaimCommandHash: WorkflowCanonicalHash.tagged(
          "t3.workflow-native-command.v1",
          claimCommand,
        ),
        expectedMessageCommandHash: WorkflowCanonicalHash.tagged(
          "t3.workflow-native-command.v1",
          messageCommand,
        ),
      };
    },
  );
  return { deriveInTransaction };
});

export class WorkflowNativeLaunchDerivation extends Context.Service<
  WorkflowNativeLaunchDerivation,
  Effect.Success<typeof make>
>()("t3/workflowRuns/WorkflowNativeLaunchDerivation") {}
export const layer = Layer.effect(WorkflowNativeLaunchDerivation, make);
