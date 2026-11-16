import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  WorkTaskError,
  WorkTaskAcceptedRunOwnershipEvent,
  WorkTaskPersistedAcceptedRunOwnershipEvent,
  WorkTaskPersistedCoordinationEvent,
  type OrchestrationV2StoredEvent,
} from "@t3tools/contracts";
import { readNativeLaunchReservation } from "./TaskNativeLaunchBinding.ts";

const decodeOwner = Schema.decodeUnknownEffect(WorkTaskPersistedAcceptedRunOwnershipEvent);
const decodeOwnerEvent = Schema.decodeUnknownEffect(WorkTaskAcceptedRunOwnershipEvent);
const decodeWork = Schema.decodeUnknownEffect(WorkTaskPersistedCoordinationEvent);
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const invalid = (message: string) => new WorkTaskError({ code: "invalid_state", message });
const ownerColumns = {
  run_id: "ownership.run.runId",
  message_id: "ownership.run.messageId",
  environment_id: "environmentId",
  task_id: "ownership.run.task.taskId",
  group_id: "ownership.run.group.groupId",
  manager_generation: "ownership.run.manager.managerGeneration",
  manager_thread_id: "ownership.run.manager.threadId",
  membership_generation: "ownership.run.membershipGeneration",
  project_id: "ownership.run.projectId",
  provider_instance_id: "ownership.run.providerInstanceId",
  definition_revision: "ownership.run.taskRevisions.definitionRevision",
  accepted_definition_revision: "ownership.run.taskRevisions.acceptedDefinitionRevision",
  lifecycle_revision: "ownership.run.taskRevisions.lifecycleRevision",
  completion_policy_revision: "ownership.run.taskRevisions.completionPolicyRevision",
  action_policy_revision: "ownership.run.taskRevisions.actionPolicyRevision",
  graph_revision: "ownership.run.graphRevision",
  control_revision: "ownership.run.controlRevision",
  membership_revision: "ownership.run.membershipRevision",
  recorded_event_id: "id",
  recorded_sequence: "ownership.recorded.sequence",
  recorded_command_id: "commandId",
  recorded_at: "occurredAt",
  native_run_event_id: "ownership.nativeAcceptance.runEvent.eventId",
  native_run_sequence: "ownership.nativeAcceptance.runEvent.sequence",
  native_message_event_id: "ownership.nativeAcceptance.messageEvent.eventId",
  native_message_sequence: "ownership.nativeAcceptance.messageEvent.sequence",
  native_receipt_command_id: "ownership.nativeAcceptance.receipt.commandId",
  native_receipt_result_sequence: "ownership.nativeAcceptance.receipt.resultSequence",
  origin_type: "ownership.origin.type",
  reservation_id: "ownership.origin.reservationId",
  reservation_command_id: "ownership.origin.reservationCommandId",
  allowance_unit: "ownership.origin.allowanceUnit",
  debit_event_id: "ownership.origin.debitEvent.eventId",
  debit_sequence: "ownership.origin.debitEvent.sequence",
} as const;
const workColumns = {
  work_claim_id: "claim.binding.workClaimId",
  run_id: "claim.binding.runId",
  environment_id: "environmentId",
  task_id: "claim.binding.task.taskId",
  group_id: "claim.binding.group.groupId",
  manager_generation: "claim.binding.manager.managerGeneration",
  manager_thread_id: "claim.binding.manager.threadId",
  membership_generation: "claim.binding.membershipGeneration",
  accepted_event_id: "id",
  accepted_sequence: "claim.acceptedSequence",
  effect_id: "claim.binding.effectId",
  accepted_definition_revision: "claim.binding.taskRevisions.acceptedDefinitionRevision",
} as const;

export const acceptNativeInitialRun = Effect.fn("TaskNativeAcceptedClaim.acceptInitialRun")(
  function* (input: {
    readonly commandId: string;
    readonly threadId: string;
    readonly acceptedAt: string;
    readonly receiptSequence: number;
    readonly storedEvents: ReadonlyArray<OrchestrationV2StoredEvent>;
    readonly startEffectId: string;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const reservation = yield* readNativeLaunchReservation(input.commandId);
    if (!reservation) return yield* invalid("Native Task reservation is missing");
    const runs = input.storedEvents.filter((item) => item.event.type === "run.created");
    const messages = input.storedEvents.filter(
      (item) =>
        item.event.type === "message.updated" && item.event.payload.id === reservation.messageId,
    );
    const run = runs[0],
      message = messages[0];
    if (
      runs.length !== 1 ||
      messages.length !== 1 ||
      !run ||
      !message ||
      run.event.type !== "run.created" ||
      message.event.type !== "message.updated" ||
      run.event.payload.userMessageId !== reservation.messageId ||
      run.event.payload.threadId !== input.threadId ||
      run.event.payload.modelSelection.instanceId !== run.event.payload.providerInstanceId ||
      run.event.payload.status !== "starting" ||
      message.event.payload.threadId !== input.threadId ||
      message.event.payload.runId !== run.event.payload.id ||
      input.threadId !== reservation.managerThreadId ||
      run.sequence > input.receiptSequence ||
      message.sequence > input.receiptSequence
    )
      return yield* invalid("Native Task run or message does not match its reservation");
    const runId = run.event.payload.id;
    const existing = yield* sql<{
      event_json: string;
    }>`SELECT event_json FROM work_task_accepted_runs WHERE native_receipt_command_id=${input.commandId}`;
    if (existing.length) {
      if (existing.length !== 1)
        return yield* invalid("Native Task receipt has multiple accepted owners");
      const saved = yield* decodeOwnerEvent(yield* decodeJson(existing[0]!.event_json));
      const claims = yield* sql<{
        effect_id: string;
        event_json: string;
      }>`SELECT effect_id,event_json FROM work_task_executable_claims WHERE run_id=${runId}`;
      const claim = claims[0]
        ? ((yield* decodeJson(claims[0].event_json)) as {
            claim?: { sourceEventId?: string; binding?: { runId?: string; effectId?: string } };
          })
        : undefined;
      if (
        saved.type !== "task.run-ownership-accepted" ||
        saved.ownership.run.runId !== runId ||
        saved.ownership.run.task.taskId !== reservation.taskId ||
        saved.ownership.nativeAcceptance.receipt.commandId !== input.commandId ||
        saved.ownership.nativeAcceptance.receipt.resultSequence !== input.receiptSequence ||
        saved.ownership.nativeAcceptance.runEvent.eventId !== run.event.id ||
        saved.ownership.nativeAcceptance.runEvent.sequence !== run.sequence ||
        saved.ownership.nativeAcceptance.messageEvent.eventId !== message.event.id ||
        saved.ownership.nativeAcceptance.messageEvent.sequence !== message.sequence ||
        claims.length !== 1 ||
        claims[0]!.effect_id !== input.startEffectId ||
        claim?.claim?.sourceEventId !== saved.id ||
        claim?.claim?.binding?.runId !== runId ||
        claim?.claim?.binding?.effectId !== input.startEffectId
      )
        return yield* invalid("Native Task claim recovery differs from the accepted run");
      return;
    }
    const facts = yield* sql<{
      project_id: string;
      definition_revision: number;
      lifecycle_revision: number;
      completion_policy_revision: number;
      action_policy_revision: number;
      accepted_definition_revision: number;
      membership_generation: number;
      highest_membership_generation: number;
      joined_record_json: string;
      control_event_json: string;
      control_revision: number;
      membership_revision: number;
      highest_manager_generation: number;
      graph_revision: number;
      start_event_id: string;
      start_sequence: number;
      start_command_id: string;
    }>`SELECT t.project_id,t.definition_revision,t.lifecycle_revision,t.completion_policy_revision,
      t.action_policy_revision,d.definition_revision accepted_definition_revision,
      m.membership_generation,m.membership_generation highest_membership_generation,m.joined_record_json,
      g.control_event_json,g.control_revision,g.membership_revision,g.highest_manager_generation,
      max(coalesce((SELECT max(created_graph_revision) FROM work_task_graph_edges
        WHERE environment_id=c.environment_id),0),
        coalesce((SELECT max(removed_graph_revision) FROM work_task_graph_edges
        WHERE environment_id=c.environment_id),0)) graph_revision,
      e.event_id start_event_id,e.sequence start_sequence,e.command_id start_command_id
      FROM work_task_native_launch_claims c
      JOIN work_task_projections t ON t.task_id=c.task_id
      JOIN work_task_accepted_definitions d ON d.task_id=t.task_id AND d.definition_revision=t.definition_revision
      JOIN work_task_group_memberships m ON m.task_id=c.task_id AND m.membership_generation=c.membership_generation
      JOIN work_task_manager_groups g ON g.environment_id=c.environment_id AND g.group_id=c.group_id
      JOIN work_task_current_state s ON s.task_id=c.task_id
      JOIN orchestration_v2_projection_threads p ON p.thread_id=c.manager_thread_id
      JOIN orchestration_events e ON e.aggregate_kind='task' AND e.stream_id=c.task_id
        AND e.application_event_version=2 AND e.event_type='task.execution-updated'
        AND e.sequence=(SELECT max(sequence) FROM orchestration_events
          WHERE aggregate_kind='task' AND stream_id=c.task_id AND event_type='task.execution-updated')
      WHERE c.command_id=${input.commandId} AND c.task_id=${reservation.taskId}
        AND t.project_id=p.project_id AND p.default_provider=${run.event.payload.providerInstanceId}
        AND t.definition_revision=c.definition_revision AND t.lifecycle_revision=c.lifecycle_revision
        AND s.desired_mode='active' AND s.execution_mode='idle'
        AND s.manager_thread_id=c.manager_thread_id AND s.manager_generation=c.manager_generation
        AND g.dispatch_hold='none' AND g.current_manager_thread_id=c.manager_thread_id
        AND g.current_manager_generation=c.manager_generation AND m.retired_event_id IS NULL
        AND json_extract(e.payload_json,'$.reason')='task_start'
        AND json_extract(e.payload_json,'$.desiredMode')='active'
        AND json_extract(e.payload_json,'$.groupId')=c.group_id
        AND json_extract(e.payload_json,'$.manager.managerThreadId')=c.manager_thread_id
        AND json_extract(e.payload_json,'$.manager.managerGeneration')=c.manager_generation
        AND NOT EXISTS(SELECT 1 FROM work_task_graph_edges x WHERE x.environment_id=c.environment_id
          AND x.dependent_task_id=c.task_id AND x.removed_graph_revision IS NULL)`;
    if (facts.length !== 1)
      return yield* invalid("Native Task start or readiness evidence is unavailable");
    const fact = facts[0]!;
    const controlEvent = (yield* decodeJson(fact.control_event_json)) as { control: unknown };
    const control = controlEvent.control as { controlRevision: number; membershipRevision: number };
    const member = yield* decodeJson(fact.joined_record_json);
    if (
      control.controlRevision !== fact.control_revision ||
      control.membershipRevision !== fact.membership_revision
    )
      return yield* invalid("Native Task group control source differs from projection");
    const revisions = {
      definitionRevision: fact.definition_revision,
      acceptedDefinitionRevision: fact.accepted_definition_revision,
      lifecycleRevision: fact.lifecycle_revision,
      completionPolicyRevision: fact.completion_policy_revision,
      actionPolicyRevision: fact.action_policy_revision,
    };
    const task = { environmentId: reservation.environmentId, taskId: reservation.taskId };
    const group = { environmentId: reservation.environmentId, groupId: reservation.groupId };
    const manager = {
      threadId: reservation.managerThreadId,
      managerGeneration: reservation.managerGeneration,
    };
    const origin = {
      type: "initial_start" as const,
      startCommandId: fact.start_command_id,
      startEventId: fact.start_event_id,
      startSequence: fact.start_sequence,
    };
    const identity = {
      version: 2 as const,
      task,
      group,
      manager,
      membershipGeneration: fact.membership_generation,
      membershipRevision: fact.membership_revision,
      controlRevision: fact.control_revision,
      graphRevision: fact.graph_revision,
      projectId: fact.project_id,
      providerInstanceId: run.event.payload.providerInstanceId,
      taskRevisions: revisions,
      runId,
      messageId: reservation.messageId,
    };
    const nextSequence = yield* sql<{ sequence: number }>`SELECT max(
      coalesce((SELECT seq FROM sqlite_sequence WHERE name='orchestration_events'),0),
      coalesce((SELECT max(sequence) FROM orchestration_events),0)) + 1 sequence`;
    const ownerSequence = nextSequence[0]!.sequence;
    const occurredAt = DateTime.formatIso(yield* DateTime.now);
    const ownerId = yield* Effect.sync(() => SecureRandom.uuidV4Unsafe());
    const ownerCommand = yield* Effect.sync(() => SecureRandom.uuidV4Unsafe());
    const owner = yield* decodeOwner({
      context: { eventId: ownerId, sequence: ownerSequence, commandId: ownerCommand, occurredAt },
      event: {
        id: ownerId,
        type: "task.run-ownership-accepted",
        environmentId: reservation.environmentId,
        commandId: ownerCommand,
        occurredAt,
        attribution: { type: "server" },
        ownership: {
          recorded: {
            eventId: ownerId,
            sequence: ownerSequence,
            commandId: ownerCommand,
            occurredAt,
          },
          run: identity,
          nativeAcceptance: {
            receipt: {
              commandId: input.commandId,
              threadId: input.threadId,
              commandType: "message.dispatch",
              acceptedAt: input.acceptedAt,
              resultSequence: input.receiptSequence,
              status: "accepted",
              error: null,
            },
            runEvent: { type: "run.created", eventId: run.event.id, sequence: run.sequence },
            messageEvent: {
              type: "message.updated",
              eventId: message.event.id,
              sequence: message.sequence,
            },
            runStatus: run.event.payload.status,
          },
          sourceEventId: fact.start_event_id,
          sourceSequence: fact.start_sequence,
          observedWorkSequence: fact.start_sequence,
          prerequisites: [],
          origin,
        },
      },
    });
    const ownerJson = yield* encodeJson(owner.event);
    const ownerVersion = yield* sql<{
      version: number;
    }>`SELECT coalesce(max(stream_version),-1)+1 version
      FROM orchestration_events WHERE aggregate_kind='task-group' AND stream_id=${reservation.groupId}`;
    yield* sql`INSERT INTO orchestration_events
      (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES (${ownerSequence},${ownerId},'task-group',${reservation.groupId},${ownerVersion[0]!.version},
        'task.run-ownership-accepted',${occurredAt},${ownerCommand},'system',${ownerJson},'{}',2)`;
    yield* sql.unsafe(
      `INSERT INTO work_task_accepted_runs (${Object.keys(ownerColumns).join(",")},event_json)
      SELECT ${Object.values(ownerColumns)
        .map((path) => `json_extract(value,'$.${path}')`)
        .join(",")},value
      FROM (SELECT ? value)`,
      [ownerJson],
    );
    const claimSequence = ownerSequence + 1;
    const claimId = yield* Effect.sync(() => SecureRandom.uuidV4Unsafe());
    const claimCommand = yield* Effect.sync(() => SecureRandom.uuidV4Unsafe());
    const claimAt = DateTime.formatIso(yield* DateTime.now);
    const work = yield* decodeWork({
      context: {
        eventId: claimId,
        sequence: claimSequence,
        commandId: claimCommand,
        occurredAt: claimAt,
      },
      event: {
        id: claimId,
        type: "task-group.work-accepted",
        environmentId: reservation.environmentId,
        commandId: claimCommand,
        occurredAt: claimAt,
        attribution: { type: "server" },
        previousActiveClaimId: null,
        claim: {
          binding: {
            ...identity,
            workClaimId: claimId,
            commandId: claimCommand,
            effectId: input.startEffectId,
          },
          acceptedEventId: claimId,
          acceptedSequence: claimSequence,
          acceptedAt: claimAt,
          sourceEventId: ownerId,
          sourceSequence: ownerSequence,
          observedWorkSequence: ownerSequence,
          prerequisites: [],
          launchOrigin: origin,
        },
        control,
        membership: {
          task,
          highestMembershipGeneration: fact.highest_membership_generation,
          currentMembership: member,
          taskRevisions: revisions,
        },
      },
    });
    const workJson = yield* encodeJson(work.event);
    yield* sql`INSERT INTO orchestration_events
      (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES (${claimSequence},${claimId},'task-group',${reservation.groupId},${ownerVersion[0]!.version + 1},
        'task-group.work-accepted',${claimAt},${claimCommand},'system',${workJson},'{}',2)`;
    yield* sql.unsafe(
      `INSERT INTO work_task_executable_claims (${Object.keys(workColumns).join(",")},event_json)
      SELECT ${Object.values(workColumns)
        .map((path) => `json_extract(value,'$.${path}')`)
        .join(",")},value
      FROM (SELECT ? value)`,
      [workJson],
    );
    const consumed = yield* sql<{
      consumed: number;
    }>`SELECT consumed FROM work_task_allowances WHERE task_id=${reservation.taskId}`;
    if (consumed.length !== 1 || consumed[0]!.consumed !== 0)
      return yield* invalid("Initial Start cannot consume automatic allowance");
    yield* sql`UPDATE work_task_current_state SET execution_mode='running' WHERE task_id=${reservation.taskId}
      AND desired_mode='active' AND execution_mode='idle'`;
  },
);
