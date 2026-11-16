import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as Option from "effect/Option";
import * as NodeUtil from "node:util";
import type { ProjectId } from "@t3tools/contracts";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import {
  WorkTaskDraftDefinition,
  WorkTaskAcceptedDefinition,
  WorkTaskEvent,
  WorkTaskPersistedEvent,
  WorkTaskCallerContext,
  WorkTaskError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { TaskStore } from "./TaskStore.ts";
import { persistInitialGroup } from "./TaskGroupStore.ts";

const decodeAcceptedDefinition = Schema.decodeEffect(WorkTaskAcceptedDefinition);
const decodeEvent = Schema.decodeEffect(WorkTaskEvent);
const encodeEvent = Schema.encodeEffect(Schema.fromJsonString(WorkTaskEvent));
const decodeEventJson = Schema.decodeEffect(Schema.fromJsonString(WorkTaskEvent));
const decodeRecorded = Schema.decodeUnknownEffect(WorkTaskPersistedEvent);

export const makeTaskService = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* TaskStore;
  const projects = yield* ProjectStore.make;
  const append = Effect.fn("TaskService.append")(function* (
    event: WorkTaskEvent,
    commandId: string,
    version: number,
  ) {
    const payload = yield* encodeEvent(event);
    const rows = yield* sql<{ sequence: number }>`INSERT INTO orchestration_events
      (event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES (${event.id},'task',${event.taskId},${version},${event.type},${event.occurredAt},${commandId},'system',${payload},'{}',2)
      RETURNING sequence`;
    const recorded = yield* decodeRecorded({
      event,
      context: {
        sequence: rows[0]!.sequence,
        eventId: event.id,
        occurredAt: event.occurredAt,
        commandId,
      },
    });
    yield* store.applyFoundation(recorded);
    return recorded;
  });
  const create = Effect.fn("TaskService.create")(function* (
    input: {
      readonly definition: WorkTaskDraftDefinition;
      readonly taskId?: string;
      readonly commandId?: string;
      readonly projectId?: ProjectId | undefined;
    },
    caller: WorkTaskCallerContext = { type: "server" },
  ) {
    const event = yield* decodeEvent({
      id: yield* Effect.sync(() => SecureRandom.uuidV4Unsafe()),
      taskId: input.taskId ?? (yield* Effect.sync(() => SecureRandom.uuidV4Unsafe())),
      occurredAt: DateTime.formatIso(yield* DateTime.now),
      attribution: caller,
      ...(input.projectId === undefined ? {} : { projectId: input.projectId }),
      definitionRevision: input.definition.revision,
      lifecycleRevision: 1,
      completionPolicyRevision: 1,
      actionPolicyRevision: 1,
      type: "task.created",
      definition: input.definition,
    });
    const commandId = input.commandId ?? event.id;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const prior = yield* sql<{
          payload_json: string;
          aggregate_kind: string;
        }>`SELECT payload_json,aggregate_kind FROM orchestration_events WHERE command_id=${commandId}`;
        if (prior.length > 0) {
          if (prior.length !== 1 || prior[0]!.aggregate_kind !== "task")
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Create command already used",
            });
          const previous = yield* decodeEventJson(prior[0]!.payload_json);
          if (
            previous.type !== "task.created" ||
            previous.taskId !== event.taskId ||
            previous.projectId !== input.projectId ||
            !NodeUtil.isDeepStrictEqual(previous.definition, input.definition) ||
            !NodeUtil.isDeepStrictEqual(previous.attribution, caller)
          )
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Create command already used",
            });
          yield* store.readNativeEvent({ taskId: previous.taskId, eventId: previous.id });
          return;
        }
        if (input.projectId !== undefined && Option.isNone(yield* projects.get(input.projectId)))
          return yield* new WorkTaskError({
            code: "not_found",
            message: "Project not found in this environment",
          });
        if (yield* store.readFoundation(event.taskId))
          return yield* new WorkTaskError({
            code: "revision_conflict",
            message: "Task identity already exists",
          });
        yield* append(event, commandId, 0);
      }),
    );
    return event.taskId;
  });
  // Caller context must come from an authenticated transport, never from a request payload.
  const acceptDraft = Effect.fn("TaskService.acceptDraft")(function* (
    input: {
      readonly taskId: string;
      readonly commandId: string;
      readonly expectedDefinitionRevision: number;
      readonly expectedLifecycleRevision: number;
      readonly expectedActionPolicyRevision: number;
    },
    caller: WorkTaskCallerContext,
  ) {
    if (caller.type !== "user")
      return yield* new WorkTaskError({
        code: "unsupported",
        message: "Only an authenticated human can accept a task definition",
      });
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const current = yield* store.readFoundation(input.taskId);
        if (!current)
          return yield* new WorkTaskError({ code: "not_found", message: "Task not found" });
        if (!current.sourceVerified || !current.draft)
          return yield* new WorkTaskError({
            code: "invalid_state",
            message: "Task source is not verified",
          });
        const projection = current.projection;
        const prior = yield* sql<{
          payload_json: string;
          sequence: number;
          aggregate_kind: string;
        }>`SELECT payload_json,sequence,aggregate_kind FROM orchestration_events
        WHERE command_id=${input.commandId}`;
        if (prior.length > 0) {
          if (prior.length !== 1 || prior[0]!.aggregate_kind !== "task")
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Acceptance command already used",
            });
          const event = yield* decodeEventJson(prior[0]!.payload_json);
          if (
            event.type !== "task.definition-accepted" ||
            event.taskId !== input.taskId ||
            event.definitionRevision !== input.expectedDefinitionRevision ||
            event.lifecycleRevision !== input.expectedLifecycleRevision ||
            event.actionPolicyRevision !== input.expectedActionPolicyRevision ||
            event.attribution.sessionId !== caller.sessionId ||
            event.attribution.subject !== caller.subject
          )
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Acceptance command was already used for another decision",
            });
          return prior[0]!.sequence;
        }
        if (
          projection.definitionRevision !== input.expectedDefinitionRevision ||
          projection.lifecycleRevision !== input.expectedLifecycleRevision ||
          projection.actionPolicyRevision !== input.expectedActionPolicyRevision ||
          current.accepted?.definitionRevision === projection.definitionRevision
        )
          return yield* new WorkTaskError({
            code: "revision_conflict",
            message: "Task definition changed or was already accepted",
          });
        const definition = yield* decodeAcceptedDefinition(current.draft).pipe(
          Effect.mapError(
            () =>
              new WorkTaskError({
                code: "invalid_definition",
                message:
                  "Complete the task title, objective and acceptance criteria before accepting",
              }),
          ),
        );
        const event = yield* decodeEvent({
          id: yield* Effect.sync(() => SecureRandom.uuidV4Unsafe()),
          taskId: input.taskId,
          occurredAt: DateTime.formatIso(yield* DateTime.now),
          attribution: caller,
          definitionRevision: projection.definitionRevision,
          lifecycleRevision: projection.lifecycleRevision,
          completionPolicyRevision: projection.completionPolicyRevision,
          actionPolicyRevision: projection.actionPolicyRevision,
          type: "task.definition-accepted",
          definition,
        });
        const versions = yield* sql<{
          version: number;
        }>`SELECT MAX(stream_version) AS version FROM orchestration_events
        WHERE aggregate_kind='task' AND stream_id=${input.taskId}`;
        return (yield* append(event, input.commandId, versions[0]!.version + 1)).context.sequence;
      }),
    );
  });
  const createInitialGroup = Effect.fn("TaskService.createInitialGroup")(function* (
    input: {
      readonly taskId: string;
      readonly environmentId: string;
      readonly groupId: string;
      readonly managerThreadId: string;
      readonly commandId: string;
    },
    caller: WorkTaskCallerContext,
  ) {
    if (caller.type !== "user")
      return yield* new WorkTaskError({
        code: "unsupported",
        message: "Only an authenticated human can create a task group",
      });
    return yield* persistInitialGroup({
      ...input,
      subject: caller.subject,
      sessionId: caller.sessionId,
    }).pipe(Effect.provideService(TaskStore, store));
  });
  return {
    create,
    acceptDraft,
    createInitialGroup,
    inspect: store.readFoundation,
    inspectResults: store.readResults,
    readNativeHistory: store.readNativeHistory,
  };
});

export class TaskService extends Context.Service<
  TaskService,
  Effect.Success<typeof makeTaskService>
>()("t3/tasks/TaskService") {}
export const layer = Layer.effect(TaskService, makeTaskService);
