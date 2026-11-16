import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import {
  WorkTaskDraftDefinition,
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

const decodeEvent = Schema.decodeEffect(WorkTaskEvent);
const encodeEvent = Schema.encodeEffect(Schema.fromJsonString(WorkTaskEvent));
const decodeEventJson = Schema.decodeEffect(Schema.fromJsonString(WorkTaskEvent));
const decodeRecorded = Schema.decodeUnknownEffect(WorkTaskPersistedEvent);

export const makeTaskService = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* TaskStore;
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
  const create = Effect.fn("TaskService.create")(function* (input: {
    readonly definition: WorkTaskDraftDefinition;
  }) {
    const event = yield* decodeEvent({
      id: yield* Effect.sync(() => SecureRandom.uuidV4Unsafe()),
      taskId: yield* Effect.sync(() => SecureRandom.uuidV4Unsafe()),
      occurredAt: DateTime.formatIso(yield* DateTime.now),
      attribution: { type: "server" },
      definitionRevision: input.definition.revision,
      lifecycleRevision: 1,
      completionPolicyRevision: 1,
      actionPolicyRevision: 1,
      type: "task.created",
      definition: input.definition,
    });
    yield* sql.withTransaction(append(event, event.id, 0));
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
        }>`SELECT payload_json,sequence FROM orchestration_events
        WHERE command_id=${input.commandId} AND aggregate_kind='task'`;
        if (prior.length > 0) {
          const event = yield* decodeEventJson(prior[0]!.payload_json);
          if (
            prior.length !== 1 ||
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
          definition: current.draft,
        });
        const versions = yield* sql<{
          version: number;
        }>`SELECT MAX(stream_version) AS version FROM orchestration_events
        WHERE aggregate_kind='task' AND stream_id=${input.taskId}`;
        return (yield* append(event, input.commandId, versions[0]!.version + 1)).context.sequence;
      }),
    );
  });
  return { create, acceptDraft, inspect: store.readFoundation, inspectResults: store.readResults };
});

export class TaskService extends Context.Service<
  TaskService,
  Effect.Success<typeof makeTaskService>
>()("t3/tasks/TaskService") {}
export const layer = Layer.effect(TaskService, makeTaskService);
