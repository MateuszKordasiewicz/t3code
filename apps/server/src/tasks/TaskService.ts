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
  WorkTaskPauseInput,
  WorkTaskStopInput,
  WorkTaskConfigureNativeAllowanceInput,
  WorkTaskStartPreparedInput,
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
    if (
      event.type === "task.allowance-configured" ||
      event.type === "task.allowance-topped-up" ||
      event.type === "task.allowance-debited"
    )
      yield* store.applyManagerAllowance(recorded);
    else yield* store.applyFoundation(recorded);
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
  const configureNativeAllowance = Effect.fn("TaskService.configureNativeAllowance")(function* (
    input: WorkTaskConfigureNativeAllowanceInput,
    caller: WorkTaskCallerContext,
  ) {
    if (caller.type !== "user")
      return yield* new WorkTaskError({
        code: "unsupported",
        message: "Only an authenticated human can configure task allowance",
      });
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const prior = yield* sql<{
          payload_json: string;
          sequence: number;
          aggregate_kind: string;
        }>`
            SELECT payload_json,sequence,aggregate_kind FROM orchestration_events WHERE command_id=${input.commandId}`;
        if (prior.length) {
          if (prior.length !== 1 || prior[0]!.aggregate_kind !== "task")
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Task command already used",
            });
          const event = yield* decodeEventJson(prior[0]!.payload_json);
          if (
            event.type !== "task.allowance-configured" ||
            event.taskId !== input.taskId ||
            event.resulting.cap !== input.cap ||
            event.lifecycleRevision !== input.expectedLifecycleRevision ||
            !NodeUtil.isDeepStrictEqual(event.attribution, caller)
          )
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Task command already used",
            });
          return {
            taskId: event.taskId,
            definitionRevision: event.definitionRevision,
            lifecycleRevision: event.lifecycleRevision,
            completionPolicyRevision: event.completionPolicyRevision,
            actionPolicyRevision: event.actionPolicyRevision,
            sequence: prior[0]!.sequence,
          };
        }
        const task = yield* store.readFoundation(input.taskId);
        if (!task?.sourceVerified || !task.accepted || !task.projection.current)
          return yield* new WorkTaskError({
            code: "invalid_state",
            message: "Task has no accepted definition",
          });
        const projection = task.projection;
        const state = projection.current;
        if (
          projection.lifecycleRevision !== input.expectedLifecycleRevision ||
          (state?.desiredMode !== null && state?.desiredMode !== "not_started") ||
          (yield* sql`SELECT 1 FROM work_task_allowances WHERE task_id=${input.taskId}`).length
        )
          return yield* new WorkTaskError({
            code: "revision_conflict",
            message: "Task allowance or lifecycle changed",
          });
        const event = yield* decodeEvent({
          id: yield* Effect.sync(() => SecureRandom.uuidV4Unsafe()),
          taskId: input.taskId,
          commandId: input.commandId,
          occurredAt: DateTime.formatIso(yield* DateTime.now),
          attribution: caller,
          definitionRevision: projection.definitionRevision,
          lifecycleRevision: projection.lifecycleRevision,
          completionPolicyRevision: projection.completionPolicyRevision,
          actionPolicyRevision: projection.actionPolicyRevision,
          type: "task.allowance-configured",
          previous: { type: "unconfigured", consumed: 0 },
          resulting: { type: "configured", cap: input.cap, consumed: 0 },
        });
        const versions = yield* sql<{
          version: number;
        }>`SELECT MAX(stream_version) AS version FROM orchestration_events WHERE aggregate_kind='task' AND stream_id=${input.taskId}`;
        const recorded = yield* append(event, input.commandId, versions[0]!.version + 1);
        return {
          taskId: event.taskId,
          definitionRevision: event.definitionRevision,
          lifecycleRevision: event.lifecycleRevision,
          completionPolicyRevision: event.completionPolicyRevision,
          actionPolicyRevision: event.actionPolicyRevision,
          sequence: recorded.context.sequence,
        };
      }),
    );
  });
  const startPrepared = Effect.fn("TaskService.startPrepared")(function* (
    input: WorkTaskStartPreparedInput,
    caller: WorkTaskCallerContext,
    environmentId: string,
  ) {
    if (caller.type !== "user")
      return yield* new WorkTaskError({
        code: "unsupported",
        message: "Only an authenticated human can start a task",
      });
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const prior = yield* sql<{
          payload_json: string;
          sequence: number;
          aggregate_kind: string;
        }>`SELECT payload_json,sequence,aggregate_kind FROM orchestration_events WHERE command_id=${input.commandId}`;
        if (prior.length) {
          if (prior.length !== 1 || prior[0]!.aggregate_kind !== "task")
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Task command already used",
            });
          const event = yield* decodeEventJson(prior[0]!.payload_json);
          if (
            event.type !== "task.execution-updated" ||
            event.reason !== "task_start" ||
            event.taskId !== input.taskId ||
            event.lifecycleRevision !== input.expectedLifecycleRevision + 1 ||
            event.groupId !== input.groupId ||
            event.manager?.managerThreadId !== input.managerThreadId ||
            !NodeUtil.isDeepStrictEqual(event.attribution, caller)
          )
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Task command already used",
            });
          return {
            taskId: event.taskId,
            definitionRevision: event.definitionRevision,
            lifecycleRevision: event.lifecycleRevision,
            completionPolicyRevision: event.completionPolicyRevision,
            actionPolicyRevision: event.actionPolicyRevision,
            sequence: prior[0]!.sequence,
          };
        }
        const task = yield* store.readFoundation(input.taskId);
        if (
          !task?.sourceVerified ||
          !task.accepted ||
          !task.projection.current ||
          task.projection.projectId === null
        )
          return yield* new WorkTaskError({
            code: "invalid_state",
            message: "Task has no accepted project definition",
          });
        const projection = task.projection;
        const state = projection.current;
        if (
          projection.lifecycleRevision !== input.expectedLifecycleRevision ||
          task.accepted.definitionRevision !== projection.definitionRevision ||
          (state?.desiredMode !== null && state?.desiredMode !== "not_started") ||
          state?.managerThreadId !== null ||
          (state?.executionMode !== null && state?.executionMode !== "not_started")
        )
          return yield* new WorkTaskError({
            code: "revision_conflict",
            message: "Task execution changed",
          });
        const eligible = yield* sql<{
          generation: number;
        }>`SELECT g.current_manager_generation generation
            FROM work_task_manager_groups g
            JOIN work_task_group_memberships m ON m.environment_id=g.environment_id AND m.group_id=g.group_id
            JOIN orchestration_v2_projection_threads p ON p.thread_id=g.current_manager_thread_id
            JOIN work_task_allowances a ON a.task_id=m.task_id
            WHERE g.environment_id=${environmentId} AND g.group_id=${input.groupId}
              AND g.current_manager_thread_id=${input.managerThreadId} AND g.dispatch_hold='none'
              AND m.task_id=${input.taskId} AND m.retired_event_id IS NULL
              AND m.accepted_definition_revision=${projection.definitionRevision}
              AND p.project_id=${projection.projectId} AND p.deleted_at IS NULL
              AND a.configured_cap>0 AND a.consumed=0`;
        if (eligible.length !== 1 || eligible[0]!.generation === null)
          return yield* new WorkTaskError({
            code: "invalid_state",
            message: "Prepared manager or finite allowance is unavailable",
          });
        const event = yield* decodeEvent({
          id: yield* Effect.sync(() => SecureRandom.uuidV4Unsafe()),
          taskId: input.taskId,
          occurredAt: DateTime.formatIso(yield* DateTime.now),
          attribution: caller,
          definitionRevision: projection.definitionRevision,
          lifecycleRevision: projection.lifecycleRevision + 1,
          completionPolicyRevision: projection.completionPolicyRevision,
          actionPolicyRevision: projection.actionPolicyRevision,
          type: "task.execution-updated",
          desiredMode: "active",
          previousManagerGeneration: null,
          manager: {
            managerThreadId: input.managerThreadId,
            managerGeneration: eligible[0]!.generation,
          },
          reason: "task_start",
          groupId: input.groupId,
        });
        const versions = yield* sql<{
          version: number;
        }>`SELECT MAX(stream_version) AS version FROM orchestration_events WHERE aggregate_kind='task' AND stream_id=${input.taskId}`;
        const recorded = yield* append(event, input.commandId, versions[0]!.version + 1);
        return {
          taskId: event.taskId,
          definitionRevision: event.definitionRevision,
          lifecycleRevision: event.lifecycleRevision,
          completionPolicyRevision: event.completionPolicyRevision,
          actionPolicyRevision: event.actionPolicyRevision,
          sequence: recorded.context.sequence,
        };
      }),
    );
  });
  const setExecutionHold = Effect.fn("TaskService.setExecutionHold")(function* (
    input: WorkTaskPauseInput | WorkTaskStopInput,
    desiredMode: "paused" | "closed",
    caller: WorkTaskCallerContext,
  ) {
    if (caller.type !== "user")
      return yield* new WorkTaskError({
        code: "unsupported",
        message: "Only an authenticated human can change task execution",
      });
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const prior = yield* sql<{
          payload_json: string;
          sequence: number;
          aggregate_kind: string;
        }>`
          SELECT payload_json,sequence,aggregate_kind FROM orchestration_events
          WHERE command_id=${input.commandId}`;
        if (prior.length) {
          if (prior.length !== 1 || prior[0]!.aggregate_kind !== "task")
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Task command already used",
            });
          const event = yield* decodeEventJson(prior[0]!.payload_json);
          if (
            event.type !== "task.execution-updated" ||
            event.taskId !== input.taskId ||
            event.desiredMode !== desiredMode ||
            event.lifecycleRevision !== input.expectedLifecycleRevision + 1 ||
            event.previousManagerGeneration !== input.expectedManagerGeneration ||
            !NodeUtil.isDeepStrictEqual(event.attribution, caller)
          )
            return yield* new WorkTaskError({
              code: "revision_conflict",
              message: "Task command already used",
            });
          return {
            taskId: event.taskId,
            definitionRevision: event.definitionRevision,
            lifecycleRevision: event.lifecycleRevision,
            completionPolicyRevision: event.completionPolicyRevision,
            actionPolicyRevision: event.actionPolicyRevision,
            sequence: prior[0]!.sequence,
          };
        }
        const current = yield* store.readFoundation(input.taskId);
        if (!current)
          return yield* new WorkTaskError({ code: "not_found", message: "Task not found" });
        if (!current.sourceVerified)
          return yield* new WorkTaskError({
            code: "invalid_state",
            message: "Task source is not verified",
          });
        const projection = current.projection;
        const state = projection.current;
        if (!state)
          return yield* new WorkTaskError({
            code: "invalid_state",
            message: "Task has no execution state",
          });
        if (
          projection.lifecycleRevision !== input.expectedLifecycleRevision ||
          state.managerGeneration !== input.expectedManagerGeneration ||
          state.desiredMode !== "active"
        )
          return yield* new WorkTaskError({
            code: "revision_conflict",
            message: "Task execution changed",
          });
        const event = yield* decodeEvent({
          id: yield* Effect.sync(() => SecureRandom.uuidV4Unsafe()),
          taskId: input.taskId,
          occurredAt: DateTime.formatIso(yield* DateTime.now),
          attribution: caller,
          definitionRevision: projection.definitionRevision,
          lifecycleRevision: projection.lifecycleRevision + 1,
          completionPolicyRevision: projection.completionPolicyRevision,
          actionPolicyRevision: projection.actionPolicyRevision,
          type: "task.execution-updated",
          desiredMode,
          previousManagerGeneration: input.expectedManagerGeneration,
          manager:
            state.managerThreadId === null || state.managerGeneration === null
              ? null
              : {
                  managerThreadId: state.managerThreadId,
                  managerGeneration: state.managerGeneration,
                },
          reason: desiredMode === "paused" ? "task_pause" : "task_stop",
        });
        const versions = yield* sql<{ version: number }>`SELECT MAX(stream_version) AS version
          FROM orchestration_events WHERE aggregate_kind='task' AND stream_id=${input.taskId}`;
        const recorded = yield* append(event, input.commandId, versions[0]!.version + 1);
        return {
          taskId: input.taskId,
          definitionRevision: event.definitionRevision,
          lifecycleRevision: event.lifecycleRevision,
          completionPolicyRevision: event.completionPolicyRevision,
          actionPolicyRevision: event.actionPolicyRevision,
          sequence: recorded.context.sequence,
        };
      }),
    );
  });
  return {
    create,
    acceptDraft,
    createInitialGroup,
    configureNativeAllowance,
    startPrepared,
    pause: (input: WorkTaskPauseInput, caller: WorkTaskCallerContext) =>
      setExecutionHold(input, "paused", caller),
    stop: (input: WorkTaskStopInput, caller: WorkTaskCallerContext) =>
      setExecutionHold(input, "closed", caller),
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
