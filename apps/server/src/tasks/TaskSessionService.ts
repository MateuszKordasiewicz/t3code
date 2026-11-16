import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  WorkTaskError,
  WorkTaskCreateDraftInput,
  WorkTaskAcceptDraftInput,
  WorkTaskDetailInput,
  WorkTaskCreateDraftResult,
  WorkTaskInspectDraftResult,
  WorkTaskCreateInitialGroupInput,
  WorkTaskCreateInitialGroupResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { AuthenticatedSession } from "../auth/EnvironmentAuth.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import { TaskService } from "./TaskService.ts";
import { reserveNativeLaunch, type NativeLaunchIntent } from "./TaskNativeLaunchBinding.ts";

const isTaskError = Schema.is(WorkTaskError);
const decodeCommitted = Schema.decodeUnknownEffect(WorkTaskCreateDraftResult);
const decodeInspection = Schema.decodeUnknownEffect(WorkTaskInspectDraftResult);
const decodeGroup = Schema.decodeUnknownEffect(WorkTaskCreateInitialGroupResult);
export const makeTaskSessionService = Effect.fn("TaskSessionService.make")(function* (
  session: AuthenticatedSession,
) {
  const tasks = yield* TaskService;
  const environment = yield* ServerEnvironment;
  const authorize = <A, E, R>(mutation: boolean, operation: Effect.Effect<A, E, R>) => {
    const scope = mutation ? AuthOrchestrationOperateScope : AuthOrchestrationReadScope;
    return !session.scopes.includes(scope) ||
      (mutation && session.method !== "browser-session-cookie")
      ? Effect.fail(
          new WorkTaskError({
            code: "unsupported",
            message: "Task mutations require an authenticated browser user with operate access",
          }),
        )
      : operation;
  };
  const caller = { type: "user" as const, sessionId: session.sessionId, subject: session.subject };
  const inspect = Effect.fn("TaskSessionService.inspect")(function* (input: WorkTaskDetailInput) {
    const current = yield* tasks.inspect(input.taskId);
    if (!current) return yield* new WorkTaskError({ code: "not_found", message: "Task not found" });
    if (
      !current.sourceVerified ||
      !current.draft ||
      current.projection.current?.lastSequence == null
    )
      return yield* new WorkTaskError({
        code: "invalid_state",
        message: "Task source is not verified",
      });
    return yield* decodeInspection({
      ...current.projection,
      sequence: current.projection.current.lastSequence,
      definition: current.draft,
      accepted: current.accepted ?? null,
    });
  });
  const create = Effect.fn("TaskSessionService.create")(function* (
    input: WorkTaskCreateDraftInput,
  ) {
    yield* tasks.create({ ...input, definition: { ...input.definition, revision: 1 } }, caller);
    const current = yield* inspect({ taskId: input.taskId });
    const history = yield* tasks.readNativeHistory({
      taskId: input.taskId,
      throughSequence: current.sequence,
      limit: 1,
    });
    const creation = history.records[0]?.recorded;
    if (!creation || creation.event.type !== "task.created")
      return yield* new WorkTaskError({
        code: "invalid_state",
        message: "Task creation source is unavailable",
      });
    return yield* decodeCommitted({ ...creation.event, sequence: creation.context.sequence });
  });
  const accept = Effect.fn("TaskSessionService.accept")(function* (
    input: WorkTaskAcceptDraftInput,
  ) {
    const sequence = yield* tasks.acceptDraft(input, caller);
    return yield* decodeCommitted({
      taskId: input.taskId,
      definitionRevision: input.expectedDefinitionRevision,
      lifecycleRevision: input.expectedLifecycleRevision,
      actionPolicyRevision: input.expectedActionPolicyRevision,
      sequence,
    });
  });
  const createInitialGroup = Effect.fn("TaskSessionService.createInitialGroup")(function* (
    input: WorkTaskCreateInitialGroupInput,
  ) {
    const environmentId = yield* environment.getEnvironmentId;
    return yield* decodeGroup(yield* tasks.createInitialGroup({ ...input, environmentId }, caller));
  });
  const reserveLaunch = Effect.fn("TaskSessionService.reserveLaunch")(function* (
    input: Omit<NativeLaunchIntent, "environmentId">,
  ) {
    const environmentId = yield* environment.getEnvironmentId;
    return yield* reserveNativeLaunch({ ...input, environmentId });
  });
  const mapError = (error: unknown) =>
    isTaskError(error)
      ? error
      : new WorkTaskError({ code: "unavailable", message: "Native Task operation failed" });
  return {
    create: (input: WorkTaskCreateDraftInput) =>
      authorize(true, create(input)).pipe(Effect.mapError(mapError)),
    inspect: (input: WorkTaskDetailInput) =>
      authorize(false, inspect(input)).pipe(Effect.mapError(mapError)),
    accept: (input: WorkTaskAcceptDraftInput) =>
      authorize(true, accept(input)).pipe(Effect.mapError(mapError)),
    createInitialGroup: (input: Parameters<typeof createInitialGroup>[0]) =>
      authorize(true, createInitialGroup(input)).pipe(Effect.mapError(mapError)),
    reserveLaunch: (input: Parameters<typeof reserveLaunch>[0]) =>
      authorize(true, reserveLaunch(input)).pipe(Effect.mapError(mapError)),
  };
});
