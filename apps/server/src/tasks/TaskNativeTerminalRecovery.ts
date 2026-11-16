import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  WorkTaskError,
  WorkTaskPersistedCoordinationEvent,
  WorkTaskPersistedAcceptedRunOwnershipEvent,
} from "@t3tools/contracts";
import { reconcileTaskWorkReleaseEvidence } from "./TaskGroup.ts";

const decodeWork = Schema.decodeUnknownEffect(WorkTaskPersistedCoordinationEvent);
const decodeOwner = Schema.decodeUnknownEffect(WorkTaskPersistedAcceptedRunOwnershipEvent);
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const invalid = (message: string) => new WorkTaskError({ code: "invalid_state", message });

type NativeRow = {
  event_id: string;
  sequence: number;
  occurred_at: string;
  command_id: string;
  payload_json: string;
};

export const recoverNativeTaskTerminal = Effect.fn("TaskNativeTerminalRecovery.recover")(function* (
  commandId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const claims = yield* sql<{
        event_json: string;
        work_claim_id: string;
        run_id: string;
        effect_id: string;
        accepted_sequence: number;
        release_event_id: string | null;
        manager_thread_id: string;
        group_id: string;
      }>`SELECT c.event_json,c.work_claim_id,c.run_id,c.effect_id,c.accepted_sequence,
          c.release_event_id,c.manager_thread_id,c.group_id
          FROM work_task_executable_claims c
          JOIN work_task_native_launch_intents i ON i.command_id=${commandId}
          JOIN work_task_accepted_runs a ON a.run_id=c.run_id
          WHERE a.native_receipt_command_id=${commandId} AND c.task_id=i.task_id
            AND c.group_id=i.group_id AND c.manager_thread_id=i.manager_thread_id`;
      if (claims.length !== 1) return yield* invalid("Accepted native Task claim is unavailable");
      const claim = claims[0]!;
      const prior = yield* sql<{ event_id: string; work_release_event_id: string | null }>`
        SELECT o.event_id,b.work_release_event_id FROM work_task_run_ownership_observations o
        LEFT JOIN work_task_run_ownership_release_bridges b ON b.observation_event_id=o.event_id
        WHERE o.run_id=${claim.run_id} AND o.kind='task.run-ownership-terminated'`;
      if (claim.release_event_id !== null || prior.length) {
        if (
          claim.release_event_id === null ||
          prior.length !== 1 ||
          prior[0]!.work_release_event_id !== claim.release_event_id
        )
          return yield* invalid("Native Task terminal recovery is incomplete");
        return { state: "released" as const, releaseEventId: claim.release_event_id };
      }
      const terminalRows =
        yield* sql<NativeRow>`SELECT event_id,sequence,occurred_at,command_id,payload_json
          FROM orchestration_events WHERE aggregate_kind='thread' AND stream_id=${claim.manager_thread_id}
            AND application_event_version=2 AND event_type='run.updated'
            AND sequence>${claim.accepted_sequence}
            AND json_extract(payload_json,'$.id')=${claim.run_id}
            AND json_extract(payload_json,'$.status') IN ('completed','cancelled','failed','interrupted','rolled_back')
          ORDER BY sequence`;
      if (!terminalRows.length) return { state: "running" as const };
      if (terminalRows.length !== 1)
        return yield* invalid("Native Task has conflicting terminal run evidence");
      const terminal = terminalRows[0]!;
      const run = (yield* decodeJson(terminal.payload_json)) as {
        id: string;
        threadId: string;
        userMessageId: string;
        status: string;
      };
      const intent = yield* sql<{
        message_id: string;
      }>`SELECT message_id FROM work_task_native_launch_intents
          WHERE command_id=${commandId}`;
      if (run.threadId !== claim.manager_thread_id || run.userMessageId !== intent[0]?.message_id)
        return yield* invalid("Native terminal run differs from accepted custody");
      const attempts =
        yield* sql<NativeRow>`SELECT event_id,sequence,occurred_at,command_id,payload_json
          FROM orchestration_events WHERE aggregate_kind='thread' AND stream_id=${claim.manager_thread_id}
            AND application_event_version=2 AND event_type='run-attempt.updated'
            AND sequence>${claim.accepted_sequence} AND sequence<=${terminal.sequence}
            AND json_extract(payload_json,'$.runId')=${claim.run_id}
            AND json_extract(payload_json,'$.status') IN ('completed','cancelled','failed','interrupted','superseded')
          ORDER BY sequence DESC LIMIT 1`;
      if (!attempts.length && !["cancelled", "failed", "interrupted"].includes(run.status))
        return yield* invalid("Settled native Task run has no terminal attempt evidence");
      const original = (yield* decodeJson(claim.event_json)) as {
        claim: {
          binding: unknown;
          acceptedEventId: string;
          acceptedSequence: number;
          acceptedAt: string;
        };
      };
      const ownerRows = yield* sql<{
        event_json: string;
      }>`SELECT event_json FROM work_task_accepted_runs
          WHERE run_id=${claim.run_id}`;
      const owner = (yield* decodeJson(ownerRows[0]!.event_json)) as {
        ownership: {
          run: unknown;
          recorded: unknown;
          nativeAcceptance: { runEvent: unknown };
          origin: unknown;
        };
      };
      const attempt = attempts[0];
      const attemptPayload = attempt
        ? ((yield* decodeJson(attempt.payload_json)) as {
            id: string;
            runId: string;
            providerThreadId: string;
            providerTurnId: string | null;
            status: string;
          })
        : null;
      const nativeRun = {
        type: "run.updated" as const,
        eventId: terminal.event_id,
        sequence: terminal.sequence,
        runId: claim.run_id,
        threadId: claim.manager_thread_id,
        status: run.status,
        occurredAt: terminal.occurred_at,
      };
      const nativeAttempt =
        attempt && attemptPayload
          ? {
              type: "run-attempt.updated" as const,
              eventId: attempt.event_id,
              sequence: attempt.sequence,
              runId: claim.run_id,
              attemptId: attemptPayload.id,
              providerThreadId: attemptPayload.providerThreadId,
              providerTurnId: attemptPayload.providerTurnId,
              status: attemptPayload.status,
              occurredAt: attempt.occurred_at,
            }
          : null;
      const releaseId = yield* Effect.sync(() => SecureRandom.uuidV4Unsafe());
      const releaseCommand = yield* Effect.sync(() => SecureRandom.uuidV4Unsafe());
      const releaseAt = DateTime.formatIso(yield* DateTime.now);
      const next = yield* sql<{ sequence: number }>`SELECT max(
          coalesce((SELECT seq FROM sqlite_sequence WHERE name='orchestration_events'),0),
          coalesce((SELECT max(sequence) FROM orchestration_events),0)) + 1 sequence`;
      const releaseSequence = next[0]!.sequence;
      const terminalRef = {
        type: "run.updated" as const,
        eventId: terminal.event_id,
        sequence: terminal.sequence,
      };
      const evidence = nativeAttempt
        ? {
            type: "run_settled" as const,
            terminalRunEvent: terminalRef,
            attempt: {
              attemptId: nativeAttempt.attemptId,
              providerThreadId: nativeAttempt.providerThreadId,
              providerTurnId: nativeAttempt.providerTurnId,
              terminalAttemptEvent: {
                type: "run-attempt.updated" as const,
                eventId: nativeAttempt.eventId,
                sequence: nativeAttempt.sequence,
              },
            },
          }
        : {
            type: "not_started" as const,
            terminalRunEvent: terminalRef,
            settlementCommandId: terminal.command_id,
          };
      const release = yield* decodeWork({
        context: {
          eventId: releaseId,
          sequence: releaseSequence,
          commandId: releaseCommand,
          occurredAt: releaseAt,
        },
        event: {
          id: releaseId,
          type: "task-group.work-released",
          environmentId: (original.claim.binding as { task: { environmentId: string } }).task
            .environmentId,
          commandId: releaseCommand,
          occurredAt: releaseAt,
          attribution: { type: "server" },
          claim: {
            binding: original.claim.binding,
            acceptedEventId: original.claim.acceptedEventId,
            acceptedSequence: original.claim.acceptedSequence,
            acceptedAt: original.claim.acceptedAt,
          },
          observedSequence: Math.max(terminal.sequence, nativeAttempt?.sequence ?? 0),
          releaseEvidence: evidence,
        },
      });
      if (release.event.type !== "task-group.work-released")
        return yield* invalid("Native Task release has an invalid event type");
      const releaseJson = yield* encodeJson(release.event);
      const nativeEvidence = {
        run: nativeRun,
        ...(nativeAttempt ? { attempt: nativeAttempt } : {}),
      };
      reconcileTaskWorkReleaseEvidence({ ...release, event: release.event }, nativeEvidence);
      const nativeJson = yield* encodeJson(nativeEvidence);
      const version = yield* sql<{
        version: number;
      }>`SELECT coalesce(max(stream_version),-1)+1 version
          FROM orchestration_events WHERE aggregate_kind='task-group' AND stream_id=${claim.group_id}`;
      yield* sql`INSERT INTO orchestration_events
          (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
          VALUES (${releaseSequence},${releaseId},'task-group',${claim.group_id},${version[0]!.version},
            'task-group.work-released',${releaseAt},${releaseCommand},'system',${releaseJson},'{}',2)`;
      yield* sql`INSERT INTO work_task_claim_releases
          (event_id,sequence,work_claim_id,run_id,effect_id,terminal_event_id,terminal_sequence,event_json,native_json)
          VALUES (${releaseId},${releaseSequence},${claim.work_claim_id},${claim.run_id},${claim.effect_id},
            ${terminal.event_id},${terminal.sequence},${releaseJson},${nativeJson})`;
      const closed = yield* sql<{ work_claim_id: string }>`UPDATE work_task_executable_claims
        SET release_event_id=${releaseId}
        WHERE work_claim_id=${claim.work_claim_id} AND release_event_id IS NULL
        RETURNING work_claim_id`;
      if (closed.length !== 1)
        return yield* invalid("Native Task claim changed during terminal recovery");
      const terminatedId = yield* Effect.sync(() => SecureRandom.uuidV4Unsafe());
      const terminatedCommand = yield* Effect.sync(() => SecureRandom.uuidV4Unsafe());
      const terminatedAt = DateTime.formatIso(yield* DateTime.now);
      const terminatedSequence = releaseSequence + 1;
      const terminated = yield* decodeOwner({
        context: {
          eventId: terminatedId,
          sequence: terminatedSequence,
          commandId: terminatedCommand,
          occurredAt: terminatedAt,
        },
        event: {
          id: terminatedId,
          type: "task.run-ownership-terminated",
          environmentId: release.event.environmentId,
          commandId: terminatedCommand,
          occurredAt: terminatedAt,
          attribution: { type: "server" },
          ownership: {
            run: owner.ownership.run,
            recorded: owner.ownership.recorded,
            nativeRunEvent: owner.ownership.nativeAcceptance.runEvent,
            origin: owner.ownership.origin,
          },
          observedSequence: releaseSequence,
          terminationEvidence: {
            kind: "after_executable_claim",
            terminalRunEvent: terminalRef,
            terminalStatus: run.status,
            workClaimId: claim.work_claim_id,
            workReleaseEvent: {
              type: "task-group.work-released",
              eventId: releaseId,
              sequence: releaseSequence,
            },
          },
        },
      });
      const terminatedJson = yield* encodeJson(terminated.event);
      yield* sql`INSERT INTO orchestration_events
          (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
          VALUES (${terminatedSequence},${terminatedId},'task-group',${claim.group_id},${version[0]!.version + 1},
            'task.run-ownership-terminated',${terminatedAt},${terminatedCommand},'system',${terminatedJson},'{}',2)`;
      yield* sql`INSERT INTO work_task_run_ownership_release_bridges
          (observation_event_id,work_claim_id,work_release_event_id,run_id,terminal_event_id,terminal_sequence)
          VALUES (${terminatedId},${claim.work_claim_id},${releaseId},${claim.run_id},${terminal.event_id},${terminal.sequence})`;
      yield* sql`INSERT INTO work_task_run_ownership_observations
          (event_id,run_id,environment_id,task_id,group_id,manager_thread_id,kind,command_id,occurred_at,
            source_event_id,source_sequence,observed_sequence,terminal_event_id,terminal_sequence,
            terminal_status,settlement_command_id,sequence,event_json,after_release_companion_id)
          VALUES (${terminatedId},${claim.run_id},${release.event.environmentId},
            ${(original.claim.binding as { task: { taskId: string } }).task.taskId},${claim.group_id},
            ${claim.manager_thread_id},'task.run-ownership-terminated',${terminatedCommand},${terminatedAt},
            NULL,NULL,${releaseSequence},${terminal.event_id},${terminal.sequence},${run.status},NULL,
            ${terminatedSequence},${terminatedJson},${terminatedId})`;
      yield* sql`UPDATE work_task_current_state SET execution_mode='idle'
          WHERE task_id=${(original.claim.binding as { task: { taskId: string } }).task.taskId}
            AND execution_mode='running'`;
      return { state: "released" as const, releaseEventId: releaseId };
    }),
  );
});
