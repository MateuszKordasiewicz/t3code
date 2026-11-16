import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import { WorkTaskError } from "@t3tools/contracts";

export interface NativeLaunchIntent {
  readonly environmentId: string;
  readonly taskId: string;
  readonly groupId: string;
  readonly managerThreadId: string;
  readonly commandId: string;
  readonly messageId: string;
}

type IntentRow = {
  environment_id: string;
  task_id: string;
  group_id: string;
  manager_thread_id: string;
  manager_generation: number;
  command_id: string;
  message_id: string;
  created_at: string;
};
type BindingRow = {
  run_id: string;
  run_event_id: string;
  run_sequence: number;
  receipt_sequence: number;
};
type TerminalRow = { event_id: string; sequence: number; status: string };

const error = (code: WorkTaskError["code"], message: string) =>
  new WorkTaskError({ code, message });

export const reserveNativeLaunch = Effect.fn("TaskNativeLaunchBinding.reserve")(function* (
  input: NativeLaunchIntent,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const prior =
        yield* sql<IntentRow>`SELECT * FROM work_task_native_launch_intents WHERE command_id=${input.commandId}`;
      if (prior.length) {
        const row = prior[0]!;
        if (
          row.environment_id !== input.environmentId ||
          row.task_id !== input.taskId ||
          row.group_id !== input.groupId ||
          row.manager_thread_id !== input.managerThreadId ||
          row.message_id !== input.messageId
        )
          return yield* error("revision_conflict", "Launch command already reserved differently");
        const claim = yield* sql`SELECT 1 FROM work_task_native_launch_claims
          WHERE command_id=${input.commandId} AND environment_id=${input.environmentId}
            AND task_id=${input.taskId} AND group_id=${input.groupId}
            AND manager_thread_id=${input.managerThreadId}
            AND manager_generation=${row.manager_generation}`;
        if (claim.length !== 1)
          return yield* error("invalid_state", "Native launch has no durable work claim");
        return row;
      }
      const receipt =
        yield* sql`SELECT 1 FROM orchestration_v2_command_receipts WHERE command_id=${input.commandId}`;
      if (receipt.length)
        return yield* error("revision_conflict", "Native command predates the Task reservation");
      const eligible = yield* sql<{
        generation: number;
        membership_generation: number;
        definition_revision: number;
        lifecycle_revision: number;
        allowance_unit: number;
      }>`SELECT g.current_manager_generation generation,
          m.membership_generation, t.definition_revision, t.lifecycle_revision,
          a.consumed+1 allowance_unit
        FROM work_task_manager_groups g
        JOIN work_task_group_memberships m ON m.environment_id=g.environment_id AND m.group_id=g.group_id
        JOIN work_task_projections t ON t.task_id=m.task_id
        JOIN work_task_current_state s ON s.task_id=t.task_id
        JOIN work_task_allowances a ON a.task_id=t.task_id
        JOIN orchestration_v2_projection_threads p ON p.thread_id=g.current_manager_thread_id AND p.project_id=t.project_id
        WHERE g.environment_id=${input.environmentId} AND g.group_id=${input.groupId}
          AND g.current_manager_thread_id=${input.managerThreadId} AND g.dispatch_hold='none'
          AND m.task_id=${input.taskId} AND m.retired_event_id IS NULL
          AND p.deleted_at IS NULL AND s.desired_mode='active' AND s.execution_mode='idle'
          AND a.consumed<a.configured_cap
          AND NOT EXISTS(SELECT 1 FROM work_task_native_launch_intents i
            WHERE i.environment_id=g.environment_id AND (i.group_id=g.group_id OR i.task_id=t.task_id OR i.manager_thread_id=g.current_manager_thread_id))
          AND NOT EXISTS(SELECT 1 FROM work_task_native_launch_claims c
            WHERE c.environment_id=g.environment_id AND (c.group_id=g.group_id OR c.task_id=t.task_id OR c.manager_thread_id=g.current_manager_thread_id))
          AND NOT EXISTS(SELECT 1 FROM work_task_executable_claims c
            WHERE c.release_event_id IS NULL AND (c.task_id=t.task_id OR (c.environment_id=g.environment_id AND c.group_id=g.group_id)
              OR c.manager_thread_id=g.current_manager_thread_id))
          AND EXISTS(SELECT 1 FROM work_task_accepted_definitions d WHERE d.task_id=t.task_id AND d.definition_revision=t.definition_revision)`;
      if (eligible.length !== 1 || eligible[0]!.generation === null)
        return yield* error(
          "invalid_state",
          "Task, manager, or accepted definition is unavailable",
        );
      const {
        generation,
        membership_generation,
        definition_revision,
        lifecycle_revision,
        allowance_unit,
      } = eligible[0]!;
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      yield* sql`INSERT INTO work_task_native_launch_intents
        (command_id,environment_id,task_id,group_id,manager_thread_id,manager_generation,message_id,created_at)
        VALUES (${input.commandId},${input.environmentId},${input.taskId},${input.groupId},${input.managerThreadId},${generation},${input.messageId},${createdAt})`;
      yield* sql`INSERT INTO work_task_native_launch_claims
        (command_id,environment_id,task_id,group_id,manager_thread_id,manager_generation,
          membership_generation,definition_revision,lifecycle_revision,allowance_unit,claimed_at)
        VALUES (${input.commandId},${input.environmentId},${input.taskId},${input.groupId},
          ${input.managerThreadId},${generation},${membership_generation},${definition_revision},
          ${lifecycle_revision},${allowance_unit},${createdAt})`;
      return {
        command_id: input.commandId,
        environment_id: input.environmentId,
        task_id: input.taskId,
        group_id: input.groupId,
        manager_thread_id: input.managerThreadId,
        manager_generation: generation,
        message_id: input.messageId,
        created_at: createdAt,
      };
    }),
  );
});

export const reconcileNativeLaunch = Effect.fn("TaskNativeLaunchBinding.reconcile")(function* (
  commandId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const intents =
        yield* sql<IntentRow>`SELECT * FROM work_task_native_launch_intents WHERE command_id=${commandId}`;
      const intent = intents[0];
      if (!intent) return yield* error("not_found", "Native launch reservation not found");
      const receipts = yield* sql<{
        thread_id: string;
        command_type: string;
        status: string;
        result_sequence: number;
      }>`SELECT thread_id,command_type,status,result_sequence FROM orchestration_v2_command_receipts WHERE command_id=${commandId}`;
      const receipt = receipts[0];
      if (!receipt) return { state: "reserved" as const, commandId };
      if (
        receipt.thread_id !== intent.manager_thread_id ||
        receipt.command_type !== "message.dispatch"
      )
        return yield* error(
          "revision_conflict",
          "Native receipt has a different command or thread",
        );
      if (receipt.status === "rejected") return { state: "rejected" as const, commandId };
      if (receipt.status !== "accepted")
        return yield* error("invalid_state", "Native receipt status is unknown");
      const runs = yield* sql<{
        event_id: string;
        sequence: number;
        run_id: string | null;
        payload_run_id: string | null;
        message_id: string | null;
      }>`SELECT event_id,sequence,run_id,json_extract(payload_json,'$.payload.id') payload_run_id,
          json_extract(payload_json,'$.payload.userMessageId') message_id
        FROM orchestration_v2_events WHERE command_id=${commandId} AND thread_id=${intent.manager_thread_id}
          AND event_type='run.created' ORDER BY sequence`;
      if (!runs.length) return { state: "uncertain" as const, commandId };
      if (
        runs.length !== 1 ||
        runs[0]!.run_id === null ||
        runs[0]!.payload_run_id !== runs[0]!.run_id ||
        runs[0]!.message_id !== intent.message_id ||
        runs[0]!.sequence > receipt.result_sequence
      )
        return yield* error("invalid_state", "Native run does not match the reserved message");
      const run = runs[0]!;
      const previous =
        yield* sql<BindingRow>`SELECT * FROM work_task_native_launch_bindings WHERE command_id=${commandId}`;
      if (previous.length) {
        if (
          previous[0]!.run_id !== run.run_id ||
          previous[0]!.run_event_id !== run.event_id ||
          previous[0]!.run_sequence !== run.sequence ||
          previous[0]!.receipt_sequence !== receipt.result_sequence
        )
          return yield* error("revision_conflict", "Native launch binding changed");
      } else {
        yield* sql`INSERT INTO work_task_native_launch_bindings
          (command_id,run_id,run_event_id,run_sequence,receipt_sequence)
          VALUES (${commandId},${run.run_id},${run.event_id},${run.sequence},${receipt.result_sequence})`;
      }
      const terminals =
        yield* sql<TerminalRow>`SELECT event_id,sequence,json_extract(payload_json,'$.payload.status') status
        FROM orchestration_v2_events WHERE thread_id=${intent.manager_thread_id} AND run_id=${run.run_id}
          AND event_type='run.updated' AND sequence>=${run.sequence}
          AND json_extract(payload_json,'$.payload.id')=${run.run_id}
          AND json_extract(payload_json,'$.payload.userMessageId')=${intent.message_id}
          AND json_extract(payload_json,'$.payload.status') IN ('completed','interrupted','failed','cancelled','rolled_back')
        ORDER BY sequence`;
      for (const terminal of terminals) {
        const observed = yield* sql<{
          command_id: string;
          sequence: number;
          status: string;
        }>`SELECT command_id,sequence,status FROM work_task_native_terminal_observations WHERE event_id=${terminal.event_id}`;
        if (observed.length) {
          if (
            observed[0]!.command_id !== commandId ||
            observed[0]!.sequence !== terminal.sequence ||
            observed[0]!.status !== terminal.status
          )
            return yield* error("revision_conflict", "Native terminal evidence changed");
        } else {
          yield* sql`INSERT INTO work_task_native_terminal_observations
            (command_id,event_id,sequence,status)
            VALUES (${commandId},${terminal.event_id},${terminal.sequence},${terminal.status})`;
        }
      }
      const latest = terminals.at(-1);
      return latest
        ? {
            state: "terminal" as const,
            commandId,
            runId: run.run_id,
            status: latest.status,
            sequence: latest.sequence,
          }
        : { state: "bound" as const, commandId, runId: run.run_id, sequence: run.sequence };
    }),
  );
});
