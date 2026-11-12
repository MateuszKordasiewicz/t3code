import {
  decodePlanningSessionV1,
  decodeWorkflowOperationV1,
  decodeWorkflowRunV1,
  type WorkflowOperationV1,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
import { WorkflowRunPolicy } from "./WorkflowRunPolicy.ts";

type PolicyInput = Parameters<typeof WorkflowRunPolicy.decide>[0];
type Command = PolicyInput["command"];
type Facts = PolicyInput["facts"];
const encode = (value: unknown) => JSON.stringify(value);
const parse = (value: string): unknown => JSON.parse(value);

export class WorkflowRunStoreError extends Schema.TaggedError<WorkflowRunStoreError>()(
  "WorkflowRunStoreError",
  { reason: Schema.Literals(["integrity", "stale", "persistence"]) },
) {}
const isStoreError = Schema.is(WorkflowRunStoreError);
const integrity = () => new WorkflowRunStoreError({ reason: "integrity" });
const stale = () => new WorkflowRunStoreError({ reason: "stale" });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const inTransaction = <A, E, R>(work: Effect.Effect<A, E, R>) =>
    sql
      .withTransaction(work)
      .pipe(
        Effect.mapError((error) =>
          isStoreError(error) ? error : new WorkflowRunStoreError({ reason: "persistence" }),
        ),
      );
  const createRun = Effect.fn("WorkflowRunStore.createRun")(function* (record: unknown) {
    const run = yield* decodeWorkflowRunV1(record).pipe(Effect.mapError(integrity));
    if (
      run.status !== "active" ||
      run.recordVersion !== 0 ||
      run.controlGeneration !== 0 ||
      run.amendmentSequence !== 0 ||
      run.priorSpecHash !== undefined ||
      run.allowance.attemptsConsumed !== 0 ||
      run.allowance.repairRoundsConsumed !== 0
    )
      return yield* integrity();
    const work = Effect.gen(function* () {
      const plan = run.spec.plan;
      yield* sql`INSERT OR IGNORE INTO workflow_plan_identities (plan_id) VALUES (${plan.planId})`;
      yield* sql`INSERT OR IGNORE INTO workflow_revision_identities (plan_id, revision_id, revision_hash, source_packet_hash) VALUES (${plan.planId}, ${plan.revisionId}, ${plan.revisionHash}, ${plan.sourcePacketHash})`;
      const revisions = yield* sql<{
        revision_hash: string;
        source_packet_hash: string;
      }>`SELECT revision_hash, source_packet_hash FROM workflow_revision_identities WHERE plan_id = ${plan.planId} AND revision_id = ${plan.revisionId}`;
      if (
        revisions[0]?.revision_hash !== plan.revisionHash ||
        revisions[0]?.source_packet_hash !== plan.sourcePacketHash
      )
        return yield* integrity();
      const inserted = yield* sql<{
        id: string;
      }>`INSERT INTO workflow_runs (id, plan_id, revision_id, revision_hash, source_packet_hash, spec_hash, status, record_version, control_generation, record_json) VALUES (${run.id}, ${plan.planId}, ${plan.revisionId}, ${plan.revisionHash}, ${plan.sourcePacketHash}, ${run.spec.ref.sha256}, ${run.status}, ${run.recordVersion}, ${run.controlGeneration}, ${encode(run)}) ON CONFLICT DO NOTHING RETURNING id`;
      if (inserted.length !== 1) return yield* integrity();
      return run;
    });
    return yield* inTransaction(work);
  });
  const createPlanningSession = Effect.fn("WorkflowRunStore.createPlanningSession")(function* (
    record: unknown,
  ) {
    const session = yield* decodePlanningSessionV1(record).pipe(Effect.mapError(integrity));
    if (
      session.status !== "active" ||
      session.recordVersion !== 0 ||
      session.controlGeneration !== 0 ||
      session.consumed.passes !== 0 ||
      session.consumed.repairRounds !== 0
    )
      return yield* integrity();
    const work = Effect.gen(function* () {
      yield* sql`INSERT OR IGNORE INTO workflow_plan_identities (plan_id) VALUES (${session.planId})`;
      const inserted = yield* sql<{
        id: string;
      }>`INSERT INTO workflow_planning_sessions (id, plan_id, predecessor_id, status, record_version, control_generation, record_json) VALUES (${session.id}, ${session.planId}, ${session.predecessorSessionId ?? null}, ${session.status}, ${session.recordVersion}, ${session.controlGeneration}, ${encode(session)}) ON CONFLICT DO NOTHING RETURNING id`;
      if (inserted.length !== 1) return yield* integrity();
      return session;
    });
    return yield* inTransaction(work);
  });
  const commitTransition = Effect.fn("WorkflowRunStore.commitTransition")(function* (input: {
    readonly runId: string;
    readonly command: Command;
    readonly externalFacts: Facts;
    readonly operation?: WorkflowOperationV1;
    readonly receiptId: string;
  }) {
    const work = Effect.gen(function* () {
      if (!input.runId || !input.command.commandId || !input.receiptId) return yield* integrity();
      const fingerprint = yield* Effect.try({
        try: () => WorkflowCanonicalHash.command(input.runId, input.command),
        catch: integrity,
      });
      const receipts = yield* sql<{
        workflow_run_id: string | null;
        fingerprint: string;
        receipt_id: string;
        result_json: string;
      }>`SELECT workflow_run_id, fingerprint, receipt_id, result_json FROM workflow_command_receipts WHERE command_id = ${input.command.commandId}`;
      const prior = receipts[0];
      if (prior) {
        if (prior.workflow_run_id !== input.runId || prior.fingerprint !== fingerprint)
          return yield* integrity();
        return { kind: "replay" as const, receiptId: prior.receipt_id };
      }
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM workflow_runs WHERE id = ${input.runId}`;
      if (!rows[0]) return yield* integrity();
      const raw = yield* Effect.try({ try: () => parse(rows[0]!.record_json), catch: integrity });
      const run = yield* decodeWorkflowRunV1(raw).pipe(Effect.mapError(integrity));
      if (run.id !== input.runId) return yield* integrity();
      const command = { ...input.command, fingerprint } as Command;
      const operations = yield* sql<{
        id: WorkflowOperationV1["id"];
        fingerprint: string;
        status: WorkflowOperationV1["status"];
        held: number;
        workflow_run_id: string | null;
        record_json: string;
      }>`SELECT id, fingerprint, status, held, workflow_run_id, record_json FROM workflow_operations WHERE workflow_run_id = ${input.runId}`;
      const attempts = yield* sql<{
        id: string;
        item_id: string;
      }>`SELECT id, item_id FROM workflow_attempts WHERE workflow_run_id = ${input.runId}`;
      const acceptances = yield* sql<{
        acceptance_key: string;
      }>`SELECT acceptance_key FROM workflow_acceptances WHERE workflow_run_id = ${input.runId} AND invalidated = 0 ORDER BY rowid`;
      // Admission remains closed until operation replay and durable attempt facts are verified.
      if (command.kind === "admitAttempt") return yield* integrity();
      if (command.kind === "advance" || command.kind === "complete" || command.kind === "resume")
        return yield* integrity();
      const {
        commandReceipt: _commandReceipt,
        operationReceipt: _operationReceipt,
        ...externalFacts
      } = input.externalFacts;
      const facts: Facts = {
        ...externalFacts,
        knownAttemptIds: attempts.map((a) => a.id),
        knownOperationIds: operations.map((o) => o.id),
        acceptedItemIds: acceptances
          .filter((a) => a.acceptance_key.startsWith("item:"))
          .map((a) => a.acceptance_key.slice(5)),
        finalAccepted: acceptances.some((a) => a.acceptance_key === "final"),
        heldOperations: operations
          .filter((o) => o.held === 1)
          .map((o) => ({
            id: o.id,
            fingerprint: o.fingerprint,
            status: o.status,
            provablyUnstarted: o.status === "pending",
          })),
      };
      const decision = WorkflowRunPolicy.decide({ run, command, facts });
      if (decision.kind !== "accepted") return decision;
      if (decision.actions.some((a) => a.kind === "invalidate_evidence" && a.scope === "selected"))
        return yield* integrity();
      // Evidence lineage is not yet available to verify acceptance against stored results.
      if (decision.actions.some((a) => a.kind === "accept_item" || a.kind === "accept_final"))
        return yield* integrity();
      const reserve = decision.actions.find((a) => a.kind === "reserve_attempt");
      if (!!reserve !== !!input.operation) return yield* integrity();
      let operation: WorkflowOperationV1 | undefined;
      if (reserve && input.operation) {
        operation = yield* decodeWorkflowOperationV1(input.operation).pipe(
          Effect.mapError(integrity),
        );
        const validHash = yield* Effect.try({
          try: () => WorkflowCanonicalHash.verifyOperation(operation!),
          catch: integrity,
        });
        if (
          !validHash ||
          operation.id !== reserve.operationId ||
          operation.fingerprint !== reserve.fingerprint ||
          operation.owner.kind !== "workflow_run" ||
          operation.owner.workflowRunId !== run.id ||
          operation.status !== "pending" ||
          operation.controlGeneration !== run.controlGeneration ||
          operation.parameters.kind === "planning_pass" ||
          operation.parameters.context.specHash !== run.spec.ref.sha256 ||
          operation.parameters.context.itemId !== reserve.itemId ||
          operation.parameters.context.attemptId !== reserve.attemptId ||
          !run.access.origins.some(
            (o) =>
              o.environmentId === operation!.destination.environmentId &&
              o.projectId === operation!.destination.projectId,
          )
        )
          return yield* integrity();
      }
      const updated = yield* sql<{
        id: string;
      }>`UPDATE workflow_runs SET status = ${decision.run.status}, record_version = ${decision.run.recordVersion}, control_generation = ${decision.run.controlGeneration}, spec_hash = ${decision.run.spec.ref.sha256}, plan_id = ${decision.run.spec.plan.planId}, revision_id = ${decision.run.spec.plan.revisionId}, revision_hash = ${decision.run.spec.plan.revisionHash}, source_packet_hash = ${decision.run.spec.plan.sourcePacketHash}, record_json = ${encode(decision.run)} WHERE id = ${run.id} AND record_version = ${run.recordVersion} AND control_generation = ${run.controlGeneration} AND spec_hash = ${run.spec.ref.sha256} RETURNING id`;
      if (updated.length !== 1) return yield* stale();
      for (const action of decision.actions) {
        switch (action.kind) {
          case "reserve_attempt": {
            if (!operation) return yield* integrity();
            yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, next_due_at, record_json) VALUES (${operation.id}, ${run.id}, ${operation.kind}, ${operation.fingerprint}, ${operation.status}, ${operation.controlGeneration}, ${operation.nextDueAt ?? null}, ${encode(operation)})`;
            yield* sql`INSERT INTO workflow_attempts VALUES (${action.attemptId}, ${run.id}, ${operation.id}, ${action.itemId}, ${action.repair ? 1 : 0})`;
            break;
          }
          case "hold_unstarted":
            yield* sql`UPDATE workflow_operations SET held = 1 WHERE workflow_run_id = ${run.id} AND status = 'pending'`;
            break;
          case "cancel_owned":
            yield* sql`UPDATE workflow_operations SET cancel_requested = 1 WHERE workflow_run_id = ${run.id} AND status IN ('claimed', 'unknown')`;
            break;
          case "invalidate_evidence":
            yield* sql`UPDATE workflow_role_results SET invalidated = 1 WHERE workflow_run_id = ${run.id}`;
            yield* sql`UPDATE workflow_acceptances SET invalidated = 1 WHERE workflow_run_id = ${run.id}`;
            break;
          case "rearm": {
            const held = operations.find(
              (o) =>
                o.id === action.operationId &&
                o.fingerprint === action.fingerprint &&
                o.status === "pending" &&
                o.held === 1,
            );
            if (!held) return yield* integrity();
            const decoded = yield* Effect.try({
              try: () => parse(held.record_json),
              catch: integrity,
            }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity));
            const changed = { ...decoded, controlGeneration: decision.run.controlGeneration };
            const rearmed = yield* sql<{
              id: string;
            }>`UPDATE workflow_operations SET held = 0, generation = ${changed.controlGeneration}, record_json = ${encode(changed)} WHERE id = ${action.operationId} AND workflow_run_id = ${run.id} AND fingerprint = ${action.fingerprint} AND status = 'pending' AND held = 1 RETURNING id`;
            if (rearmed.length !== 1) return yield* integrity();
            break;
          }
          case "accept_item":
          case "accept_final":
            return yield* integrity();
        }
      }
      const result = { kind: "accepted" as const, receiptId: input.receiptId };
      yield* sql`INSERT INTO workflow_command_receipts (command_id, workflow_run_id, fingerprint, receipt_id, operation_id, result_json) VALUES (${command.commandId}, ${run.id}, ${fingerprint}, ${input.receiptId}, ${operation?.id ?? null}, ${encode(result)})`;
      return result;
    });
    return yield* inTransaction(work);
  });
  return { createRun, createPlanningSession, commitTransition };
});

export class WorkflowRunStore extends Context.Service<
  WorkflowRunStore,
  Effect.Success<typeof make>
>()("t3/workflowRuns/WorkflowRunStore") {}
export const layer = Layer.effect(WorkflowRunStore, make);
