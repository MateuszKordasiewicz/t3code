import {
  decodePlanningSessionV1,
  decodeWorkflowOperationV1,
  decodeWorkflowRoleResultV1,
  decodeWorkflowRunV1,
  type WorkflowOperationV1,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
import { WorkflowRunPolicy } from "./WorkflowRunPolicy.ts";

type PolicyInput = Parameters<typeof WorkflowRunPolicy.decide>[0];
type Command = PolicyInput["command"];
type Facts = PolicyInput["facts"];
type Binding = NonNullable<Facts["currentBinding"]>;
const sameBinding = (a: Binding, b: Binding) =>
  a.specHash === b.specHash &&
  a.policyHash === b.policyHash &&
  a.candidateHash === b.candidateHash &&
  a.configurationHash === b.configurationHash &&
  a.itemId === b.itemId &&
  a.attemptId === b.attemptId;
const contextBinding = (operation: WorkflowOperationV1): Binding | undefined => {
  const params = operation.parameters;
  if (params.kind === "planning_pass") return undefined;
  const c = params.context;
  if (
    !c.candidateHash ||
    !c.configurationHash ||
    (c.itemId === undefined) !== (c.attemptId === undefined)
  )
    return undefined;
  return {
    specHash: c.specHash,
    policyHash: c.policyHash,
    candidateHash: c.candidateHash,
    configurationHash: c.configurationHash,
    ...(c.itemId !== undefined && c.attemptId !== undefined
      ? { itemId: c.itemId, attemptId: c.attemptId }
      : {}),
  };
};
const encode = (value: unknown) => JSON.stringify(value);
const parse = (value: string): unknown => JSON.parse(value);

export class WorkflowRunStoreError extends Schema.TaggedError<WorkflowRunStoreError>()(
  "WorkflowRunStoreError",
  { reason: Schema.Literals(["integrity", "stale", "persistence"]) },
) {}
const isStoreError = Schema.is(WorkflowRunStoreError);
const integrity = () => new WorkflowRunStoreError({ reason: "integrity" });
const stale = () => new WorkflowRunStoreError({ reason: "stale" });

const make = Effect.fn("WorkflowRunStore.make")(function* (
  beforePublish: Effect.Effect<void> = Effect.void,
) {
  const changes = yield* Effect.acquireRelease(PubSub.sliding<void>(1), PubSub.shutdown);
  const notify = beforePublish.pipe(
    Effect.andThen(PubSub.publish(changes, undefined)),
    Effect.asVoid,
  );
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
      const revisions = yield* sql<{
        revision_id: string;
      }>`SELECT r.revision_id FROM development_plan_revisions r JOIN workflow_revision_identities i ON i.plan_id = r.plan_id AND i.revision_id = r.revision_id AND i.revision_hash = r.revision_hash AND i.source_packet_hash = r.source_packet_hash WHERE r.plan_id = ${plan.planId} AND r.revision_id = ${plan.revisionId} AND r.revision_hash = ${plan.revisionHash} AND r.source_packet_hash = ${plan.sourcePacketHash}`;
      if (revisions.length !== 1) return yield* integrity();
      const inserted = yield* sql<{
        id: string;
      }>`INSERT INTO workflow_runs (id, plan_id, revision_id, revision_hash, source_packet_hash, spec_hash, status, record_version, control_generation, record_json) VALUES (${run.id}, ${plan.planId}, ${plan.revisionId}, ${plan.revisionHash}, ${plan.sourcePacketHash}, ${run.spec.ref.sha256}, ${run.status}, ${run.recordVersion}, ${run.controlGeneration}, ${encode(run)}) ON CONFLICT DO NOTHING RETURNING id`;
      if (inserted.length !== 1) return yield* integrity();
      return run;
    });
    const result = yield* inTransaction(work);
    yield* notify;
    return result;
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
      const revisions = yield* sql<{
        revision_id: string;
      }>`SELECT r.revision_id FROM development_plans p JOIN development_plan_revisions r ON r.plan_id = p.plan_id AND r.revision_id = p.current_revision_id WHERE p.plan_id = ${session.planId}`;
      if (revisions.length !== 1) return yield* integrity();
      const inserted = yield* sql<{
        id: string;
      }>`INSERT INTO workflow_planning_sessions (id, plan_id, predecessor_id, status, record_version, control_generation, record_json) VALUES (${session.id}, ${session.planId}, ${session.predecessorSessionId ?? null}, ${session.status}, ${session.recordVersion}, ${session.controlGeneration}, ${encode(session)}) ON CONFLICT DO NOTHING RETURNING id`;
      if (inserted.length !== 1) return yield* integrity();
      return session;
    });
    const result = yield* inTransaction(work);
    yield* notify;
    return result;
  });
  const recordRoleResult = Effect.fn("WorkflowRunStore.recordRoleResult")(function* (input: {
    readonly runId: string;
    readonly operationId: string;
    readonly resultId: string;
    readonly record: unknown;
  }) {
    const result = yield* decodeWorkflowRoleResultV1(input.record).pipe(Effect.mapError(integrity));
    if (
      result.id !== input.resultId ||
      result.operationId !== input.operationId ||
      result.workflowRunId !== input.runId
    )
      return yield* integrity();
    const recorded = yield* inTransaction(
      Effect.gen(function* () {
        const rows = yield* sql<{
          record_json: string;
          status: string;
        }>`SELECT record_json, status FROM workflow_operations WHERE id = ${input.operationId} AND workflow_run_id = ${input.runId}`;
        if (!rows[0] || rows[0].status !== "settled") return yield* integrity();
        const operation = yield* Effect.try({
          try: () => parse(rows[0]!.record_json),
          catch: integrity,
        }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity));
        if (
          operation.id !== input.operationId ||
          operation.owner.kind !== "workflow_run" ||
          operation.owner.workflowRunId !== input.runId ||
          operation.status !== "settled"
        )
          return yield* integrity();
        if (result.result.kind !== "rollup") {
          const binding = contextBinding(operation);
          if (
            (operation.kind !== "check" && operation.kind !== "stage_launch") ||
            !binding ||
            !sameBinding(binding, result.binding)
          )
            return yield* integrity();
        }
        const inserted = yield* sql<{
          id: string;
        }>`INSERT INTO workflow_role_results (id, workflow_run_id, operation_id, record_json) VALUES (${input.resultId}, ${input.runId}, ${input.operationId}, ${encode(result)}) ON CONFLICT DO NOTHING RETURNING id`;
        if (inserted.length !== 1) return yield* integrity();
        return result;
      }),
    );
    yield* notify;
    return recorded;
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
        generation: number;
        kind: string;
        stage: string | null;
        has_receipt: number;
        workflow_run_id: string | null;
        record_json: string;
      }>`SELECT o.id, o.fingerprint, o.status, o.held, o.generation, o.kind, json_extract(o.record_json, '$.parameters.stage') AS stage, EXISTS(SELECT 1 FROM workflow_command_receipts r WHERE r.operation_id = o.id) AS has_receipt, o.workflow_run_id, o.record_json FROM workflow_operations o WHERE o.workflow_run_id = ${input.runId} ORDER BY o.rowid`;
      const attempts = yield* sql<{
        id: string;
        item_id: string;
        status: WorkflowOperationV1["status"];
        outcome: string | null;
        operation_id: string;
      }>`SELECT a.id, a.item_id, a.operation_id, o.status, json_extract(o.record_json, '$.outcome.kind') AS outcome FROM workflow_attempts a JOIN workflow_operations o ON o.id = a.operation_id AND o.workflow_run_id = a.workflow_run_id WHERE a.workflow_run_id = ${input.runId} ORDER BY a.rowid`;
      const acceptances = yield* sql<{
        acceptance_key: string;
        binding_json: string;
      }>`SELECT acceptance_key, binding_json FROM workflow_acceptances WHERE workflow_run_id = ${input.runId} AND invalidated = 0 ORDER BY rowid`;

      const {
        commandReceipt: _commandReceipt,
        operationReceipt: _operationReceipt,
        currentAttemptItemId: _currentAttemptItemId,
        currentBinding: _currentBinding,
        acceptance: _acceptance,
        finalAccepted: _finalAccepted,
        publicationSettled: _publicationSettled,
        publishedHead: _publishedHead,
        requiredCiPassed: _requiredCiPassed,
        ciHead: _ciHead,
        ...externalFacts
      } = input.externalFacts;
      const acceptedItemIds = acceptances
        .filter((a) => a.acceptance_key.startsWith("item:"))
        .map((a) => a.acceptance_key.slice(5));
      const latest = attempts.at(-1);
      if (
        latest?.status === "settled" &&
        latest.outcome !== "succeeded" &&
        latest.outcome !== "failed"
      )
        return yield* integrity();
      const currentAttemptState: NonNullable<Facts["currentAttemptState"]> = !latest
        ? "none"
        : latest.status === "unknown"
          ? "unknown"
          : latest.status === "pending" || latest.status === "claimed"
            ? "active"
            : latest.status === "settled" && latest.outcome === "succeeded"
              ? "settled_succeeded"
              : "settled_failed";
      const operationReceipts =
        command.kind === "admitAttempt"
          ? yield* sql<{
              operation_id: WorkflowOperationV1["id"];
              fingerprint: string;
              receipt_id: string;
            }>`SELECT o.id AS operation_id, o.fingerprint, r.receipt_id FROM workflow_command_receipts r JOIN workflow_operations o ON o.id = r.operation_id WHERE r.workflow_run_id = ${input.runId} AND r.operation_id = ${command.operationId} LIMIT 1`
          : [];
      const operationReceipt = operationReceipts[0];
      // The run stores a spec reference, not its ordered item payload.
      const orderedItemIds = externalFacts.orderedItemIds;
      const itemIndex =
        command.kind === "admitAttempt" ? orderedItemIds?.indexOf(command.itemId) : undefined;
      let currentBinding: Binding | undefined;
      let acceptance: Facts["acceptance"];
      if (command.kind === "advance") {
        const acceptedItem = acceptances.filter((a) => a.acceptance_key.startsWith("item:")).at(-1);
        const priorBinding = acceptedItem
          ? yield* Effect.try({
              try: () => parse(acceptedItem.binding_json) as Binding,
              catch: integrity,
            })
          : undefined;
        const relevantAttempt = command.finalCombined
          ? attempts.find(
              (a) =>
                a.id === priorBinding?.attemptId &&
                a.item_id === acceptedItem?.acceptance_key.slice(5),
            )
          : latest;
        const implementationRow = operations.find((o) => o.id === relevantAttempt?.operation_id);
        const implementation = implementationRow
          ? yield* Effect.try({
              try: () => parse(implementationRow.record_json),
              catch: integrity,
            }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity))
          : undefined;
        if (command.finalCombined && acceptedItem) {
          if (
            priorBinding &&
            priorBinding.candidateHash &&
            priorBinding.configurationHash &&
            priorBinding.policyHash
          )
            currentBinding = {
              specHash: run.spec.ref.sha256,
              candidateHash: priorBinding.candidateHash,
              configurationHash: priorBinding.configurationHash,
              policyHash: priorBinding.policyHash,
            };
        } else if (!command.finalCombined && implementation)
          currentBinding = contextBinding(implementation);
        const resultRows = yield* sql<{
          id: string;
          operation_id: string;
          record_json: string;
        }>`SELECT id, operation_id, record_json FROM workflow_role_results WHERE workflow_run_id = ${input.runId} AND invalidated = 0 ORDER BY rowid`;
        const results = yield* Effect.forEach(resultRows, (row) =>
          Effect.try({ try: () => parse(row.record_json), catch: integrity }).pipe(
            Effect.flatMap(decodeWorkflowRoleResultV1),
            Effect.mapError(integrity),
          ),
        );
        const rollup = results.findLast(
          (r) => r.result.kind === "rollup" && sameBinding(r.binding, command.binding),
        );
        if (
          rollup?.result.kind === "rollup" &&
          implementation?.parameters.kind === "stage_launch" &&
          implementation.parameters.stage === "implementation"
        ) {
          const provider = implementation.parameters.provider;
          let independent = false;
          for (const result of results) {
            if (
              (result.result.kind !== "code_review" && result.result.kind !== "semantic_review") ||
              !sameBinding(result.binding, command.binding)
            )
              continue;
            const row = operations.find((o) => o.id === result.operationId);
            if (!row) continue;
            const op = yield* Effect.try({
              try: () => parse(row.record_json),
              catch: integrity,
            }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity));
            if (
              op.parameters.kind === "stage_launch" &&
              op.parameters.stage === result.result.kind &&
              (op.parameters.provider.providerInstanceId !== provider.providerInstanceId ||
                op.parameters.provider.model !== provider.model)
            )
              independent = true;
          }
          acceptance = {
            binding: rollup.binding,
            verdict: rollup.result.verdict,
            finalCombined: rollup.binding.itemId === undefined,
            evidenceAvailable: rollup.result.resultRefs.every((ref) =>
              results.some((r) => String(r.id) === String(ref.id)),
            ),
            independent,
          };
        }
      }
      const succeeded = operations
        .filter((o) => o.status === "settled")
        .map((o) => ({ ...o, raw: parse(o.record_json) as WorkflowOperationV1 }));
      const publication = succeeded.findLast(
        (o) =>
          o.kind === "publication" &&
          o.raw.outcome?.kind === "succeeded" &&
          o.raw.parameters.kind === "publication",
      );
      const ci = succeeded.findLast(
        (o) =>
          o.kind === "ci_observation" &&
          o.raw.outcome?.kind === "succeeded" &&
          o.raw.parameters.kind === "ci_observation",
      );
      const publishedHead =
        publication?.raw.parameters.kind === "publication"
          ? publication.raw.parameters.candidateCommit
          : undefined;
      const ciHead =
        ci?.raw.parameters.kind === "ci_observation" ? ci.raw.parameters.headCommit : undefined;
      const facts: Facts = {
        ...externalFacts,
        custody: operations.some((o) => o.status === "unknown")
          ? "unknown"
          : operations.some((o) => o.status === "claimed")
            ? "active"
            : "none",
        activeWriterCount: operations.filter(
          (o) =>
            o.status === "claimed" && o.kind === "stage_launch" && o.stage === "implementation",
        ).length,
        knownAttemptIds: attempts.map((a) => a.id),
        knownOperationIds: operations.map((o) => o.id),
        currentAttemptState,
        ...(latest ? { currentAttemptItemId: latest.item_id } : {}),
        ...(operationReceipt
          ? {
              operationReceipt: {
                operationId: operationReceipt.operation_id,
                fingerprint: operationReceipt.fingerprint,
                receiptId: operationReceipt.receipt_id,
              },
            }
          : {}),
        acceptedItemIds,
        ...(currentBinding ? { currentBinding } : {}),
        ...(acceptance ? { acceptance } : {}),
        publicationSettled: !!publishedHead,
        requiredCiPassed: !!ciHead,
        ...(publishedHead ? { publishedHead } : {}),
        ...(ciHead ? { ciHead } : {}),
        predecessorAccepted:
          itemIndex !== undefined &&
          itemIndex >= 0 &&
          acceptedItemIds.length === itemIndex &&
          acceptedItemIds.every((id, i) => id === orderedItemIds?.[i]),
        finalAccepted: acceptances.some((a) => a.acceptance_key === "final"),
        heldOperations: operations
          .filter((o) => o.held === 1)
          .map((o) => ({
            id: o.id,
            fingerprint: o.fingerprint,
            status: o.status,
            provablyUnstarted: o.status === "pending" && o.has_receipt === 0,
          })),
      };
      const decision = WorkflowRunPolicy.decide({ run, command, facts });
      if (decision.kind !== "accepted") return decision;
      if (decision.actions.some((a) => a.kind === "invalidate_evidence" && a.scope === "selected"))
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
          case "accept_final": {
            const key = action.kind === "accept_final" ? "final" : `item:${action.binding.itemId}`;
            yield* sql`INSERT INTO workflow_acceptances (workflow_run_id, acceptance_key, binding_json) VALUES (${run.id}, ${key}, ${encode(action.binding)}) ON CONFLICT(workflow_run_id, acceptance_key) DO UPDATE SET binding_json = excluded.binding_json, invalidated = 0 WHERE workflow_acceptances.invalidated = 1`;
            break;
          }
        }
      }
      const result = { kind: "accepted" as const, receiptId: input.receiptId };
      yield* sql`INSERT INTO workflow_command_receipts (command_id, workflow_run_id, fingerprint, receipt_id, operation_id, result_json) VALUES (${command.commandId}, ${run.id}, ${fingerprint}, ${input.receiptId}, ${operation?.id ?? null}, ${encode(result)})`;
      return result;
    });
    const result = yield* inTransaction(work);
    if (result.kind === "accepted") yield* notify;
    return result;
  });
  const subscribeChanges = PubSub.subscribe(changes);
  const listPending = Effect.fn("WorkflowRunStore.listPending")(
    function* () {
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM workflow_operations WHERE status = 'pending' AND held = 0 ORDER BY id`;
      return yield* Effect.forEach(rows, (row) =>
        Effect.try({ try: () => parse(row.record_json), catch: integrity }).pipe(
          Effect.flatMap(decodeWorkflowOperationV1),
          Effect.mapError(integrity),
        ),
      );
    },
    Effect.mapError((error) =>
      isStoreError(error) ? error : new WorkflowRunStoreError({ reason: "persistence" }),
    ),
  );
  const claim = Effect.fn("WorkflowRunStore.claim")(function* (input: {
    readonly operationId: string;
    readonly expectedOwnerVersion: number;
    readonly expectedGeneration: number;
    readonly bootId: string;
    readonly workerId: string;
    readonly now: string;
    readonly trustedEligibility?: boolean;
  }) {
    if (input.trustedEligibility !== true) return { kind: "denied" as const };
    const result = yield* inTransaction(
      Effect.gen(function* () {
        const rows = yield* sql<{
          record_json: string;
          status: string;
          held: number;
          generation: number;
          workflow_run_id: string | null;
          planning_session_id: string | null;
        }>`SELECT record_json, status, held, generation, workflow_run_id, planning_session_id FROM workflow_operations WHERE id = ${input.operationId}`;
        const row = rows[0];
        if (!row) return { kind: "denied" as const };
        const operation = yield* Effect.try({
          try: () => parse(row.record_json),
          catch: integrity,
        }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity));
        if (
          operation.id !== input.operationId ||
          operation.status !== row.status ||
          operation.controlGeneration !== row.generation ||
          (operation.owner.kind === "workflow_run"
            ? operation.owner.workflowRunId !== row.workflow_run_id ||
              row.planning_session_id !== null
            : operation.owner.planningSessionId !== row.planning_session_id ||
              row.workflow_run_id !== null)
        )
          return yield* integrity();
        const owners =
          operation.owner.kind === "workflow_run"
            ? yield* sql<{
                record_version: number;
                control_generation: number;
                status: string;
              }>`SELECT record_version, control_generation, status FROM workflow_runs WHERE id = ${operation.owner.workflowRunId}`
            : yield* sql<{
                record_version: number;
                control_generation: number;
                status: string;
              }>`SELECT record_version, control_generation, status FROM workflow_planning_sessions WHERE id = ${operation.owner.planningSessionId}`;
        const owner = owners[0];
        if (!owner) return yield* integrity();
        if (
          owner.record_version !== input.expectedOwnerVersion ||
          owner.control_generation !== input.expectedGeneration ||
          row.generation !== input.expectedGeneration ||
          row.status === "claimed"
        )
          return { kind: "stale" as const };
        if (row.status !== "pending" || row.held !== 0 || owner.status !== "active")
          return { kind: "denied" as const };
        const claimed = yield* decodeWorkflowOperationV1({
          ...operation,
          status: "claimed",
          claim: {
            bootId: input.bootId,
            workerId: input.workerId,
            generation: input.expectedGeneration,
            claimedAt: input.now,
          },
        }).pipe(Effect.mapError(integrity));
        const updated = yield* sql<{
          id: string;
        }>`UPDATE workflow_operations SET status = 'claimed', generation = ${input.expectedGeneration}, record_json = ${encode(claimed)} WHERE id = ${input.operationId} AND status = 'pending' AND held = 0 AND generation = ${input.expectedGeneration} RETURNING id`;
        if (updated.length !== 1) return { kind: "stale" as const };
        return { kind: "claimed" as const, operation: claimed };
      }),
    );
    if (result.kind === "claimed") yield* notify;
    return result;
  });
  const readRecovery = Effect.fn("WorkflowRunStore.readRecovery")(function* (operationId: string) {
    return yield* inTransaction(
      Effect.gen(function* () {
        const rows = yield* sql<{
          record_json: string;
          fingerprint: string;
          status: string;
          generation: number;
          workflow_run_id: string | null;
          planning_session_id: string | null;
        }>`SELECT record_json, fingerprint, status, generation, workflow_run_id, planning_session_id FROM workflow_operations WHERE id = ${operationId}`;
        const row = rows[0];
        if (!row) return undefined;
        const operation = yield* Effect.try({
          try: () => parse(row.record_json),
          catch: integrity,
        }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity));
        if (
          operation.id !== operationId ||
          operation.fingerprint !== row.fingerprint ||
          operation.status !== row.status ||
          operation.controlGeneration !== row.generation ||
          (operation.owner.kind === "workflow_run"
            ? operation.owner.workflowRunId !== row.workflow_run_id ||
              row.planning_session_id !== null
            : operation.owner.planningSessionId !== row.planning_session_id ||
              row.workflow_run_id !== null)
        )
          return yield* integrity();
        const owners =
          operation.owner.kind === "workflow_run"
            ? yield* sql<{
                record_json: string;
                record_version: number;
                control_generation: number;
                status: string;
              }>`SELECT record_json, record_version, control_generation, status FROM workflow_runs WHERE id = ${operation.owner.workflowRunId}`
            : yield* sql<{
                record_json: string;
                record_version: number;
                control_generation: number;
                status: string;
              }>`SELECT record_json, record_version, control_generation, status FROM workflow_planning_sessions WHERE id = ${operation.owner.planningSessionId}`;
        const rowOwner = owners[0];
        if (!rowOwner) return yield* integrity();
        const rawOwner = yield* Effect.try({
          try: () => parse(rowOwner.record_json),
          catch: integrity,
        });
        const owner =
          operation.owner.kind === "workflow_run"
            ? yield* decodeWorkflowRunV1(rawOwner).pipe(Effect.mapError(integrity))
            : yield* decodePlanningSessionV1(rawOwner).pipe(Effect.mapError(integrity));
        if (
          owner.id !==
            (operation.owner.kind === "workflow_run"
              ? operation.owner.workflowRunId
              : operation.owner.planningSessionId) ||
          owner.recordVersion !== rowOwner.record_version ||
          owner.controlGeneration !== rowOwner.control_generation ||
          owner.status !== rowOwner.status
        )
          return yield* integrity();
        const pass =
          operation.owner.kind === "planning_session"
            ? (yield* sql<{
                native_thread_id: string | null;
                native_run_id: string | null;
              }>`SELECT native_thread_id, native_run_id FROM workflow_planning_passes WHERE operation_id = ${operation.id}`)[0]
            : undefined;
        const params = operation.parameters;
        const native =
          params.kind === "stage_launch" || params.kind === "supervision"
            ? { threadId: params.nativeThreadId, runId: params.nativeRunId }
            : pass?.native_thread_id && pass.native_run_id
              ? { threadId: pass.native_thread_id, runId: pass.native_run_id }
              : undefined;
        return { operation, owner, native };
      }),
    );
  });
  const compareAndApplyRecovery = Effect.fn("WorkflowRunStore.compareAndApplyRecovery")(
    function* (input: {
      readonly operationId: string;
      readonly expectedOwnerVersion: number;
      readonly expectedGeneration: number;
      readonly expectedFingerprint: string;
      readonly observed: {
        readonly operation: WorkflowOperationV1;
        readonly ownerGeneration: number;
        readonly native: { readonly threadId: string; readonly runId: string } | undefined;
        readonly kind: "active" | "unknown" | "settled";
        readonly outcome?: NonNullable<WorkflowOperationV1["outcome"]>;
      };
    }) {
      const result = yield* inTransaction(
        Effect.gen(function* () {
          const snapshot = yield* readRecovery(input.operationId);
          if (!snapshot) return { kind: "stale" as const };
          const { operation, owner } = snapshot;
          if (
            owner.recordVersion !== input.expectedOwnerVersion ||
            owner.controlGeneration !== input.observed.ownerGeneration ||
            encode(snapshot.native) !== encode(input.observed.native) ||
            operation.controlGeneration !== input.expectedGeneration ||
            operation.fingerprint !== input.expectedFingerprint ||
            encode(operation.owner) !== encode(input.observed.operation.owner) ||
            encode(operation.claim) !== encode(input.observed.operation.claim) ||
            !operation.claim ||
            operation.claim.generation !== input.expectedGeneration ||
            encode(operation) !== encode(input.observed.operation)
          )
            return { kind: "stale" as const };
          if (operation.status !== "claimed" && operation.status !== "unknown")
            return { kind: "replay" as const };
          if (input.observed.kind === "active" && operation.status === "claimed")
            return { kind: "replay" as const };
          const outcome =
            input.observed.kind === "unknown"
              ? { kind: "unknown" as const, summary: "Native outcome unknown", evidence: [] }
              : input.observed.outcome;
          if (
            input.observed.kind === "settled" &&
            (!outcome ||
              (outcome.kind !== "succeeded" &&
                outcome.kind !== "failed" &&
                outcome.kind !== "cancelled"))
          )
            return yield* integrity();
          const { outcome: _outcome, ...base } = operation;
          const status =
            input.observed.kind === "active"
              ? "claimed"
              : input.observed.kind === "unknown"
                ? "unknown"
                : outcome?.kind === "cancelled"
                  ? "cancelled"
                  : "settled";
          const changed = yield* decodeWorkflowOperationV1({
            ...base,
            status,
            ...(outcome && status !== "claimed" ? { outcome } : {}),
          }).pipe(Effect.mapError(integrity));
          const ownerChange =
            input.observed.kind === "unknown" &&
            (owner.status === "active" || owner.status === "pausing" || owner.status === "paused");
          if (encode(changed) === encode(operation) && !ownerChange)
            return { kind: "replay" as const };
          const updated = yield* sql<{
            id: string;
          }>`UPDATE workflow_operations SET status = ${status}, record_json = ${encode(changed)} WHERE id = ${operation.id} AND record_json = ${encode(operation)} RETURNING id`;
          if (updated.length !== 1) return yield* stale();
          if (ownerChange) {
            const changedOwner = {
              ...owner,
              status: "needs_attention" as const,
              recordVersion: owner.recordVersion + 1,
            };
            if (operation.owner.kind === "workflow_run") {
              yield* decodeWorkflowRunV1(changedOwner).pipe(Effect.mapError(integrity));
              yield* sql`UPDATE workflow_runs SET status = 'needs_attention', record_version = ${changedOwner.recordVersion}, record_json = ${encode(changedOwner)} WHERE id = ${owner.id} AND record_version = ${owner.recordVersion}`;
            } else {
              yield* decodePlanningSessionV1(changedOwner).pipe(Effect.mapError(integrity));
              yield* sql`UPDATE workflow_planning_sessions SET status = 'needs_attention', record_version = ${changedOwner.recordVersion}, record_json = ${encode(changedOwner)} WHERE id = ${owner.id} AND record_version = ${owner.recordVersion}`;
            }
          }
          return { kind: "updated" as const };
        }),
      );
      if (result.kind === "updated") yield* notify;
      return result;
    },
  );
  return {
    createRun,
    createPlanningSession,
    recordRoleResult,
    commitTransition,
    subscribeChanges,
    listPending,
    claim,
    readRecovery,
    compareAndApplyRecovery,
  };
});

export class WorkflowRunStore extends Context.Service<
  WorkflowRunStore,
  Effect.Success<ReturnType<typeof make>>
>()("t3/workflowRuns/WorkflowRunStore") {}
export const layer = Layer.effect(WorkflowRunStore, make());
export const layerWithPublishFault = (fault: Effect.Effect<void>) =>
  Layer.effect(WorkflowRunStore, make(fault));
