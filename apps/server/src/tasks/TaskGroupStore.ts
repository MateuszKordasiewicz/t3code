import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  WorkTaskGroupControlEvent,
  WorkTaskError,
  WorkTaskMembershipEventBatch,
  WorkTaskPersistedGroupControlEvent,
  WorkTaskPersistedMembershipEvent,
} from "@t3tools/contracts";
import { TaskStore } from "./TaskStore.ts";
import {
  emptyTaskGroupControl,
  reduceTaskGroupControl,
  reduceTaskMembership,
} from "./TaskGroup.ts";

const decodeControl = Schema.decodeUnknownEffect(WorkTaskGroupControlEvent);
const decodeBatch = Schema.decodeUnknownEffect(WorkTaskMembershipEventBatch);
const decodeRecordedControl = Schema.decodeUnknownEffect(WorkTaskPersistedGroupControlEvent);
const decodeRecordedRoot = Schema.decodeUnknownEffect(WorkTaskPersistedMembershipEvent);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const uuid = () => SecureRandom.uuidV4Unsafe();

export const persistInitialGroup = Effect.fn("TaskGroupStore.persistInitialGroup")(
  function* (input: {
    readonly taskId: string;
    readonly environmentId: string;
    readonly groupId: string;
    readonly managerThreadId: string;
    readonly subject: string;
    readonly sessionId: string;
    readonly commandId: string;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const store = yield* TaskStore;
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const task = yield* store.readFoundation(input.taskId);
        if (!task?.sourceVerified || !task.accepted || task.projection.projectId === null)
          return yield* new WorkTaskError({
            code: "invalid_state",
            message: "Task needs verified accepted definition and project",
          });
        if (task.accepted.definitionRevision !== task.projection.definitionRevision)
          return yield* new WorkTaskError({
            code: "revision_conflict",
            message: "Accepted definition is stale",
          });
        const existing =
          yield* sql`SELECT 1 FROM work_task_group_memberships WHERE task_id=${input.taskId} LIMIT 1`;
        if (existing.length)
          return yield* new WorkTaskError({
            code: "revision_conflict",
            message: "Task already has membership",
          });
        const now = DateTime.formatIso(yield* DateTime.now);
        const principal = { subject: input.subject, sessionId: input.sessionId };
        const group = { environmentId: input.environmentId, groupId: input.groupId };
        const ref = { environmentId: input.environmentId, taskId: input.taskId };
        const fields = (id: string) => ({
          id,
          environmentId: input.environmentId,
          commandId: input.commandId,
          occurredAt: now,
          performedBy: principal,
        });
        const empty = {
          group,
          controlRevision: 0,
          membershipRevision: 0,
          highestManagerGeneration: 0,
          currentManager: null,
          dispatchHold: "none",
        };
        const created = yield* decodeControl({
          ...fields(uuid()),
          type: "task-group.created",
          control: empty,
        });
        const managerControl = {
          ...empty,
          controlRevision: 1,
          highestManagerGeneration: 1,
          currentManager: { threadId: input.managerThreadId, managerGeneration: 1 },
        };
        const bound = yield* decodeControl({
          ...fields(uuid()),
          type: "task-group.manager-changed",
          previousControl: empty,
          control: managerControl,
        });
        const memberControl = { ...managerControl, membershipRevision: 1 };
        const rootFields = fields(uuid());
        const companionId = uuid();
        const provenance = {
          eventId: rootFields.id,
          commandId: input.commandId,
          occurredAt: now,
          performedBy: principal,
        };
        const revisions = {
          definitionRevision: task.projection.definitionRevision,
          acceptedDefinitionRevision: task.accepted.definitionRevision,
          lifecycleRevision: task.projection.lifecycleRevision,
          actionPolicyRevision: task.projection.actionPolicyRevision,
          completionPolicyRevision: task.projection.completionPolicyRevision,
        };
        const previousSnapshot = {
          task: ref,
          highestMembershipGeneration: 0,
          currentMembership: null,
          taskRevisions: revisions,
        };
        const joined = {
          task: ref,
          group,
          membershipGeneration: 1,
          acceptedDefinitionRevisionAtJoin: task.accepted.definitionRevision,
          joinedMembershipRevision: 1,
          joined: provenance,
          state: "current",
        };
        const batch = yield* decodeBatch({
          event: {
            ...rootFields,
            type: "task.membership-changed",
            previousSnapshot,
            snapshot: {
              ...previousSnapshot,
              highestMembershipGeneration: 1,
              currentMembership: joined,
            },
            retiredMembership: null,
            groupTransitions: [
              {
                companionEventId: companionId,
                previousControl: managerControl,
                control: memberControl,
              },
            ],
          },
          companions: [
            {
              ...rootFields,
              id: companionId,
              type: "task-group.membership-changed",
              rootEventId: rootFields.id,
              task: ref,
              previousControl: managerControl,
              control: memberControl,
            },
          ],
        });
        const append = Effect.fn("TaskGroupStore.append")(function* (
          event: typeof created | typeof batch.event,
          kind: string,
          stream: string,
          version: number,
        ) {
          const encoded = yield* encodeJson(event);
          const rows = yield* sql<{ sequence: number }>`INSERT INTO orchestration_events
        (event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
        VALUES (${event.id},${kind},${stream},${version},${event.type},${event.occurredAt},${event.commandId},'client',${encoded},'{}',2)
        RETURNING sequence`;
          return {
            event,
            context: {
              eventId: event.id,
              commandId: event.commandId,
              occurredAt: event.occurredAt,
              sequence: rows[0]!.sequence,
            },
          };
        });
        const first = yield* decodeRecordedControl(
          yield* append(created, "task-group", input.groupId, 0),
        );
        const second = yield* decodeRecordedControl(
          yield* append(bound, "task-group", input.groupId, 1),
        );
        const root = yield* decodeRecordedRoot(
          // Membership has its own stream so TaskStore's definition history remains decodable.
          yield* append(batch.event, "task-membership", input.taskId, 0),
        );
        const companion = yield* decodeRecordedControl(
          yield* append(batch.companions[0]!, "task-group", input.groupId, 2),
        );
        let state = reduceTaskGroupControl(emptyTaskGroupControl(first.event.environmentId), first);
        state = reduceTaskGroupControl(state, second);
        if (companion.event.type !== "task-group.membership-changed")
          return yield* new WorkTaskError({
            code: "invalid_state",
            message: "Expected membership companion",
          });
        reduceTaskMembership(state, {
          root,
          companions: [{ event: companion.event, context: companion.context }],
        });
        const createdJson = yield* encodeJson(created),
          boundJson = yield* encodeJson(bound);
        const companionJson = yield* encodeJson(companion.event),
          rootJson = yield* encodeJson(root.event);
        const joinedJson = yield* encodeJson(joined);
        yield* sql`INSERT INTO work_task_manager_groups (environment_id,group_id,control_revision,membership_revision,highest_manager_generation,current_manager_generation,current_manager_thread_id,dispatch_hold,created_event_id,created_sequence,created_event_json,control_event_id,control_sequence,control_event_json)
      VALUES (${input.environmentId},${input.groupId},0,0,0,NULL,NULL,'none',${created.id},${first.context.sequence},${createdJson},${created.id},${first.context.sequence},${createdJson})`;
        yield* sql`UPDATE work_task_manager_groups SET control_revision=1,highest_manager_generation=1,current_manager_generation=1,current_manager_thread_id=${input.managerThreadId},control_event_id=${bound.id},control_sequence=${second.context.sequence},control_event_json=${boundJson} WHERE environment_id=${input.environmentId} AND group_id=${input.groupId}`;
        yield* sql`INSERT INTO work_task_group_managers (environment_id,group_id,generation,manager_thread_id,bound_event_id,bound_sequence,bound_event_json,current_generation,current_manager_thread_id)
      VALUES (${input.environmentId},${input.groupId},1,${input.managerThreadId},${bound.id},${second.context.sequence},${boundJson},1,${input.managerThreadId})`;
        yield* sql`UPDATE work_task_manager_groups SET membership_revision=1,control_event_id=${companion.event.id},control_sequence=${companion.context.sequence},control_event_json=${companionJson},latest_membership_event_id=${companion.event.id} WHERE environment_id=${input.environmentId} AND group_id=${input.groupId}`;
        yield* sql`INSERT INTO work_task_membership_companions (event_id,sequence,environment_id,group_id,task_id,membership_generation,role,root_event_id,root_sequence,root_event_json,event_json,joined_generation,retired_generation,peer_event_id)
      VALUES (${companion.event.id},${companion.context.sequence},${input.environmentId},${input.groupId},${input.taskId},1,'join',${root.event.id},${root.context.sequence},${rootJson},${companionJson},1,NULL,NULL)`;
        yield* sql`INSERT INTO work_task_group_memberships (task_id,membership_generation,environment_id,group_id,accepted_definition_revision,joined_membership_revision,joined_event_id,joined_record_json,joined_companion_id)
      VALUES (${input.taskId},1,${input.environmentId},${input.groupId},${task.accepted.definitionRevision},1,${root.event.id},${joinedJson},${companion.event.id})`;
        return { events: [first.event, second.event, root.event, companion.event] };
      }),
    );
  },
);
