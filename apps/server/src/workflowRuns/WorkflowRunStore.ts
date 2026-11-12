import {
  PlanningSessionV1,
  PlanningPassV1,
  decodeDevelopmentPlanRevisionV1,
  decodePlanningSessionV1,
  decodePlanningPassV1,
  isWorkflowTimestamp,
  isWorkflowTimestampAfter,
  decodeWorkflowOperationV1,
  decodeWorkflowRoleResultV1,
  decodeWorkflowRunV1,
  WorkflowOperationV1,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { PlanningObservation } from "../developmentPlans/PlanningPassObservation.ts";
import { DevelopmentPlanHash } from "../developmentPlans/DevelopmentPlanHash.ts";
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

export class PlanningControlError extends Schema.TaggedError<PlanningControlError>()(
  "PlanningControlError",
  {
    reason: Schema.Literals([
      "not-found",
      "conflict",
      "invalid",
      "integrity",
      "storage",
      "unauthorized",
      "limit",
      "deadline",
      "custody",
      "ineligible",
      "observation-unknown",
    ]),
    currentVersion: Schema.optionalKey(Schema.Number),
  },
) {
  override get message() {
    return "Planning session operation failed.";
  }
}
type S = PlanningSessionV1;
type P = PlanningPassV1;
type Report = NonNullable<P["report"]>;
type Disposition = Extract<Report, { kind: "repair" }>["dispositions"][number];
const decodeSettlement = Schema.decodeUnknownEffect(
  Schema.Struct({ session: PlanningSessionV1, pass: PlanningPassV1 }),
);
const decodeDispositionResult = Schema.decodeUnknownEffect(
  Schema.Struct({
    session: PlanningSessionV1,
    disposition: PlanningPassV1.fields.report.schema.members[1].fields.dispositions.value,
  }),
);
const decodeAdmission = Schema.decodeUnknownEffect(
  Schema.Struct({
    session: PlanningSessionV1,
    pass: PlanningPassV1,
    operation: WorkflowOperationV1,
  }),
);
type PlanningContext = {
  readonly actor: S["origin"];
  readonly destination: {
    readonly environmentId: S["environmentId"];
    readonly projectId: S["projectId"];
  };
};
type PlanningCommand = { readonly commandId: string; readonly receiptId: string };
const planningError = (reason: PlanningControlError["reason"]) =>
  new PlanningControlError({ reason });
const isPlanningError = Schema.is(PlanningControlError);
const isPlanningContext = Schema.is(
  Schema.Struct({
    actor: PlanningSessionV1.fields.origin,
    destination: Schema.Struct({
      environmentId: PlanningSessionV1.fields.environmentId,
      projectId: PlanningSessionV1.fields.projectId,
    }),
  }),
);
const isPlanningCommand = Schema.is(
  Schema.Struct({ commandId: Schema.String, receiptId: Schema.String }),
);

// A competing writer on another connection surfaces as a lock timeout; that is a retryable conflict.
const isWriteContention = (error: unknown): boolean =>
  (error as { reason?: { _tag?: unknown } } | undefined)?.reason?._tag === "LockTimeoutError";

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
  const planningTransaction = <A, E, R>(work: Effect.Effect<A, E, R>) =>
    sql
      .withTransaction(work)
      .pipe(
        Effect.mapError((error) =>
          isPlanningError(error)
            ? error
            : planningError(isWriteContention(error) ? "conflict" : "storage"),
        ),
      );
  const readSession = Effect.fn("WorkflowRunStore.readPlanningSession")(function* (id: S["id"]) {
    const rows = yield* sql<{
      id: string;
      plan_id: string;
      predecessor_id: string | null;
      status: string;
      record_version: number;
      control_generation: number;
      record_json: string;
    }>`SELECT * FROM workflow_planning_sessions WHERE id = ${id}`;
    const row = rows[0];
    if (!row) return yield* planningError("not-found");
    const raw = yield* Effect.try({
      try: () => parse(row.record_json),
      catch: () => planningError("integrity"),
    });
    const value = yield* decodePlanningSessionV1(raw).pipe(
      Effect.mapError(() => planningError("integrity")),
    );
    if (
      value.id !== row.id ||
      value.planId !== row.plan_id ||
      (value.predecessorSessionId ?? null) !== row.predecessor_id ||
      value.status !== row.status ||
      value.recordVersion !== row.record_version ||
      value.controlGeneration !== row.control_generation
    )
      return yield* planningError("integrity");
    return value;
  });
  const fingerprint = (context: PlanningContext, method: string, input: object) =>
    Effect.try({
      try: () => DevelopmentPlanHash.command({ ...context, method, input }),
      catch: () => planningError("invalid"),
    });
  const checkReceipt = Effect.fn("WorkflowRunStore.planningReceipt")(function* (
    command: PlanningCommand,
    owner: S["id"],
    hash: string,
  ) {
    const rows = yield* sql<{
      planning_session_id: string | null;
      fingerprint: string;
      receipt_id: string;
      result_json: string;
    }>`SELECT planning_session_id, fingerprint, receipt_id, result_json FROM workflow_command_receipts WHERE command_id = ${command.commandId}`;
    const row = rows[0];
    if (!row) return undefined;
    if (row.planning_session_id !== owner || row.fingerprint !== hash)
      return yield* planningError("conflict");
    const raw = yield* Effect.try({
      try: () => parse(row.result_json),
      catch: () => planningError("integrity"),
    });
    const value = yield* decodePlanningSessionV1(raw).pipe(
      Effect.mapError(() => planningError("integrity")),
    );
    return { kind: "replay" as const, receiptId: row.receipt_id, value };
  });
  const savePlanningReceipt = Effect.fn("WorkflowRunStore.savePlanningReceipt")(function* (
    command: PlanningCommand,
    hash: string,
    value: S,
  ) {
    const rows = yield* sql<{
      receipt_id: string;
    }>`INSERT INTO workflow_command_receipts (command_id, planning_session_id, fingerprint, receipt_id, result_json) VALUES (${command.commandId}, ${value.id}, ${hash}, ${command.receiptId}, ${encode(value)}) ON CONFLICT DO NOTHING RETURNING receipt_id`;
    if (rows.length !== 1) return yield* planningError("conflict");
    return { kind: "applied" as const, receiptId: command.receiptId, value };
  });
  const validCommand = (command: PlanningCommand) =>
    isPlanningCommand(command) &&
    typeof command.commandId === "string" &&
    command.commandId.length > 0 &&
    typeof command.receiptId === "string" &&
    command.receiptId.length > 0;
  const sameContext = (value: S, context: PlanningContext) =>
    value.environmentId === context.destination.environmentId &&
    value.projectId === context.destination.projectId &&
    value.origin.kind === context.actor.kind &&
    value.origin.id === context.actor.id;
  const beginPlanning = Effect.fn("WorkflowRunStore.beginPlanning")(function* (input: {
    readonly session: S;
    readonly now: string;
    readonly context: PlanningContext;
    readonly command: PlanningCommand;
  }) {
    const value = yield* decodePlanningSessionV1(input.session).pipe(
      Effect.mapError(() => planningError("invalid")),
    );
    if (
      !validCommand(input.command) ||
      !isPlanningContext(input.context) ||
      typeof input.now !== "string" ||
      !isWorkflowTimestamp(input.now) ||
      !sameContext(value, input.context) ||
      value.status !== "active" ||
      value.recordVersion !== 0 ||
      value.controlGeneration !== 0 ||
      value.consumed.passes !== 0 ||
      value.consumed.repairRounds !== 0
    )
      return yield* planningError("invalid");
    const { createdAt: _createdAt, ...stable } = value;
    const hash = yield* fingerprint(input.context, "beginPlanning", { session: stable });
    const result = yield* planningTransaction(
      Effect.gen(function* () {
        const prior = yield* checkReceipt(input.command, value.id, hash);
        if (prior) return prior;
        if (!isWorkflowTimestampAfter(value.limits.deadline, input.now))
          return yield* planningError("deadline");
        const plans = yield* sql<{
          environment_id: string;
          project_id: string;
          current_revision_id: string | null;
        }>`UPDATE development_plans SET version = version WHERE plan_id = ${value.planId} RETURNING environment_id, project_id, current_revision_id`;
        const plan = plans[0];
        if (!plan) return yield* planningError("not-found");
        if (plan.environment_id !== value.environmentId || plan.project_id !== value.projectId)
          return yield* planningError("unauthorized");
        if (!plan.current_revision_id) return yield* planningError("integrity");
        const revisions =
          yield* sql`SELECT revision_id FROM development_plan_revisions WHERE plan_id = ${value.planId} AND revision_id = ${plan.current_revision_id}`;
        if (revisions.length !== 1) return yield* planningError("integrity");
        const sessions = yield* sql<{
          id: string;
          status: string;
        }>`SELECT id, status FROM workflow_planning_sessions WHERE plan_id = ${value.planId} ORDER BY rowid DESC`;
        if (sessions.some((s) => s.status !== "stopped" && s.status !== "completed"))
          return yield* planningError("custody");
        const latest = sessions[0];
        if ((latest?.id ?? null) !== (value.predecessorSessionId ?? null))
          return yield* planningError("custody");
        if (latest) {
          yield* readSession(latest.id as S["id"]);
          const custody =
            yield* sql`SELECT id FROM workflow_operations WHERE planning_session_id = ${latest.id} AND status IN ('claimed','unknown')`;
          if (custody.length) return yield* planningError("custody");
        }
        const inserted = yield* sql<{
          id: string;
        }>`INSERT INTO workflow_planning_sessions (id, plan_id, predecessor_id, status, record_version, control_generation, record_json) VALUES (${value.id}, ${value.planId}, ${value.predecessorSessionId ?? null}, ${value.status}, ${value.recordVersion}, ${value.controlGeneration}, ${encode(value)}) ON CONFLICT DO NOTHING RETURNING id`;
        if (inserted.length !== 1) return yield* planningError("conflict");
        return yield* savePlanningReceipt(input.command, hash, value);
      }),
    );
    if (result.kind === "applied") yield* notify;
    return result;
  });
  const admitPlanningPass = Effect.fn("WorkflowRunStore.admitPlanningPass")(function* (input: {
    readonly sessionId: S["id"];
    readonly expectedVersion: number;
    readonly pass: P;
    readonly operation: WorkflowOperationV1;
    readonly now: string;
    readonly context: PlanningContext;
    readonly command: PlanningCommand;
  }) {
    if (
      !input.sessionId ||
      !Number.isSafeInteger(input.expectedVersion) ||
      input.expectedVersion < 0 ||
      !validCommand(input.command) ||
      !isPlanningContext(input.context) ||
      !isWorkflowTimestamp(input.now)
    )
      return yield* planningError("invalid");
    const pass = yield* decodePlanningPassV1(input.pass).pipe(
      Effect.mapError(() => planningError("invalid")),
    );
    const operation = yield* decodeWorkflowOperationV1(input.operation).pipe(
      Effect.mapError(() => planningError("invalid")),
    );
    const params = operation.parameters;
    const hashes = yield* Effect.try({
      try: () => ({
        operation: WorkflowCanonicalHash.operation(operation),
        manifest: WorkflowCanonicalHash.tagged("t3.planning-input-manifest.v1", pass.inputManifest),
        binding: WorkflowCanonicalHash.tagged("binding", pass.binding),
        provider: WorkflowCanonicalHash.tagged("provider", pass.provider),
      }),
      catch: () => planningError("invalid"),
    });
    if (
      pass.status !== "pending" ||
      pass.report ||
      !pass.native ||
      pass.native.terminalMessageId ||
      operation.status !== "pending" ||
      operation.owner.kind !== "planning_session" ||
      params.kind !== "planning_pass" ||
      operation.kind !== "planning_pass" ||
      operation.owner.planningSessionId !== input.sessionId ||
      pass.planningSessionId !== input.sessionId ||
      pass.operationId !== operation.id ||
      params.passId !== pass.id ||
      params.role !== pass.role ||
      hashes.operation !== operation.fingerprint ||
      hashes.manifest !== params.inputManifestHash ||
      hashes.binding !== WorkflowCanonicalHash.tagged("binding", params.binding) ||
      hashes.provider !== WorkflowCanonicalHash.tagged("provider", params.provider) ||
      operation.origin.kind !== input.context.actor.kind ||
      operation.origin.id !== input.context.actor.id ||
      operation.destination.environmentId !== input.context.destination.environmentId ||
      operation.destination.projectId !== input.context.destination.projectId
    )
      return yield* planningError("integrity");
    // The operation fingerprint is canonical and excludes the control generation, which a
    // pause/resume advances; fingerprinting the whole operation would break replay after one.
    const hash = yield* fingerprint(input.context, "admitPlanningPass", {
      sessionId: input.sessionId,
      expectedVersion: input.expectedVersion,
      pass,
      operationFingerprint: operation.fingerprint,
    });
    const result = yield* planningTransaction(
      Effect.gen(function* () {
        const receipts = yield* sql<{
          planning_session_id: string | null;
          fingerprint: string;
          receipt_id: string;
          result_json: string;
        }>`SELECT planning_session_id, fingerprint, receipt_id, result_json FROM workflow_command_receipts WHERE command_id = ${input.command.commandId}`;
        const prior = receipts[0];
        if (prior) {
          if (prior.planning_session_id !== input.sessionId || prior.fingerprint !== hash)
            return yield* planningError("conflict");
          const raw = yield* Effect.try({
            try: () => parse(prior.result_json),
            catch: () => planningError("integrity"),
          });
          const value = yield* decodeAdmission(raw).pipe(
            Effect.mapError(() => planningError("integrity")),
          );
          return { kind: "replay" as const, receiptId: prior.receipt_id, value };
        }
        const current = yield* readSession(input.sessionId);
        if (!sameContext(current, input.context)) return yield* planningError("unauthorized");
        if (current.recordVersion !== input.expectedVersion)
          return yield* new PlanningControlError({
            reason: "conflict",
            currentVersion: current.recordVersion,
          });
        if (current.status !== "active") return yield* planningError("ineligible");
        if (!isWorkflowTimestampAfter(current.limits.deadline, input.now))
          return yield* planningError("deadline");
        if (
          pass.binding.planId !== current.planId ||
          operation.controlGeneration !== current.controlGeneration ||
          WorkflowCanonicalHash.tagged("limits", params.limits) !==
            WorkflowCanonicalHash.tagged("limits", current.limits)
        )
          return yield* planningError("integrity");
        if (
          !current.allowedRoles.includes(pass.role) ||
          !current.allowedProviders.some(
            (p) =>
              p.providerInstanceId === pass.provider.providerInstanceId &&
              p.model === pass.provider.model,
          )
        )
          return yield* planningError("ineligible");
        if (
          current.consumed.passes >= current.limits.maxPasses ||
          (pass.role === "repair" &&
            current.consumed.repairRounds >= current.limits.maxRepairRounds)
        )
          return yield* planningError("limit");
        if (pass.role === "author" || pass.role === "repair") {
          const writers =
            yield* sql`SELECT p.id FROM workflow_planning_passes p JOIN workflow_operations o ON o.id = p.operation_id WHERE p.plan_id = ${current.planId} AND o.status IN ('pending','claimed','unknown') AND json_extract(p.record_json, '$.role') IN ('author','repair')`;
          if (writers.length) return yield* planningError("custody");
        }
        const b = pass.binding;
        const rows = yield* sql<{
          revision_json: string;
          packet_json: string;
          created_at: string;
        }>`SELECT r.revision_json, p.packet_json, r.created_at FROM development_plan_revisions r JOIN development_plan_source_packets p ON p.plan_id = r.plan_id AND p.source_packet_hash = r.source_packet_hash JOIN workflow_revision_identities i ON i.plan_id = r.plan_id AND i.revision_id = r.revision_id AND i.revision_hash = r.revision_hash AND i.source_packet_hash = r.source_packet_hash WHERE r.plan_id = ${b.planId} AND r.revision_id = ${b.revisionId} AND r.revision_hash = ${b.revisionHash} AND r.source_packet_hash = ${b.sourcePacketHash}`;
        const row = rows[0];
        if (!row) return yield* planningError("integrity");
        const content = yield* Effect.try({
          try: () => ({ revision: parse(row.revision_json), sourcePacket: parse(row.packet_json) }),
          catch: () => planningError("integrity"),
        });
        const envelope = yield* decodeDevelopmentPlanRevisionV1({
          planId: b.planId,
          revisionId: b.revisionId,
          revisionHash: b.revisionHash,
          sourcePacketHash: b.sourcePacketHash,
          ...content,
          createdAt: row.created_at,
        }).pipe(
          Effect.provide(Digest.layerWebCrypto),
          Effect.mapError(() => planningError("integrity")),
        );
        const actual = yield* Effect.try({
          try: () => ({
            revision: DevelopmentPlanHash.revision(envelope.revision),
            source: DevelopmentPlanHash.sourcePacket(envelope.sourcePacket),
          }),
          catch: () => planningError("integrity"),
        });
        if (actual.revision !== b.revisionHash || actual.source !== b.sourcePacketHash)
          return yield* planningError("integrity");
        const updated = {
          ...current,
          recordVersion: current.recordVersion + 1,
          consumed: {
            passes: current.consumed.passes + 1,
            repairRounds: current.consumed.repairRounds + (pass.role === "repair" ? 1 : 0),
          },
        };
        const changed =
          yield* sql`UPDATE workflow_planning_sessions SET record_version = ${updated.recordVersion}, record_json = ${encode(updated)} WHERE id = ${current.id} AND record_version = ${current.recordVersion} RETURNING id`;
        if (changed.length !== 1) return yield* planningError("conflict");
        const inserted =
          yield* sql`INSERT INTO workflow_operations (id, planning_session_id, kind, fingerprint, status, held, cancel_requested, generation, record_json) VALUES (${operation.id}, ${current.id}, 'planning_pass', ${operation.fingerprint}, 'pending', 0, 0, ${operation.controlGeneration}, ${encode(operation)}) ON CONFLICT DO NOTHING RETURNING id`;
        if (inserted.length !== 1) return yield* planningError("conflict");
        const native = pass.native!;
        const insertedPass =
          yield* sql`INSERT INTO workflow_planning_passes VALUES (${pass.id}, ${current.id}, ${b.planId}, ${b.revisionId}, ${b.revisionHash}, ${b.sourcePacketHash}, ${operation.id}, ${native.threadId}, ${native.runId}, ${encode(pass)}) ON CONFLICT DO NOTHING RETURNING id`;
        if (insertedPass.length !== 1) return yield* planningError("conflict");
        const value = { session: updated, pass, operation };
        const saved =
          yield* sql`INSERT INTO workflow_command_receipts (command_id, planning_session_id, fingerprint, receipt_id, operation_id, result_json) VALUES (${input.command.commandId}, ${current.id}, ${hash}, ${input.command.receiptId}, ${operation.id}, ${encode(value)}) ON CONFLICT DO NOTHING RETURNING receipt_id`;
        if (saved.length !== 1) return yield* planningError("conflict");
        return { kind: "applied" as const, receiptId: input.command.receiptId, value };
      }),
    );
    if (result.kind === "applied") yield* notify;
    return result;
  });
  const readPass = Effect.fn("WorkflowRunStore.readPlanningPass")(function* (id: P["id"]) {
    const rows = yield* sql<{
      id: string;
      planning_session_id: string;
      plan_id: string;
      revision_id: string;
      revision_hash: string;
      source_packet_hash: string;
      operation_id: string;
      native_thread_id: string | null;
      native_run_id: string | null;
      record_json: string;
    }>`SELECT * FROM workflow_planning_passes WHERE id = ${id}`;
    const row = rows[0];
    if (!row) return yield* planningError("integrity");
    const raw = yield* Effect.try({
      try: () => parse(row.record_json),
      catch: () => planningError("integrity"),
    });
    const pass = yield* decodePlanningPassV1(raw).pipe(
      Effect.mapError(() => planningError("integrity")),
    );
    if (
      pass.id !== row.id ||
      pass.planningSessionId !== row.planning_session_id ||
      pass.binding.planId !== row.plan_id ||
      pass.binding.revisionId !== row.revision_id ||
      pass.binding.revisionHash !== row.revision_hash ||
      pass.binding.sourcePacketHash !== row.source_packet_hash ||
      pass.operationId !== row.operation_id ||
      (pass.native?.threadId ?? null) !== row.native_thread_id ||
      (pass.native?.runId ?? null) !== row.native_run_id
    )
      return yield* planningError("integrity");
    return pass;
  });
  const sameCanonical = (a: unknown, b: unknown) =>
    WorkflowCanonicalHash.tagged("compare", a) === WorkflowCanonicalHash.tagged("compare", b);
  const validateDisposition = Effect.fn("WorkflowRunStore.validatePlanningDisposition")(function* (
    disposition: Disposition,
    repair: P,
  ) {
    const review = yield* readPass(disposition.reportId);
    if (
      review.status !== "settled" ||
      review.role !== "review" ||
      review.report?.kind !== "review" ||
      review.binding.planId !== repair.binding.planId ||
      !sameCanonical(review.binding, repair.binding)
    )
      return yield* planningError("integrity");
    const finding = review.report.findings.find((item) => item.id === disposition.findingId);
    if (
      !finding ||
      new Set(disposition.affectedObjectIds).size !== disposition.affectedObjectIds.length ||
      disposition.affectedObjectIds.some((id) => !finding.affectedObjectIds.includes(id))
    )
      return yield* planningError("integrity");
  });
  const insertDisposition = Effect.fn("WorkflowRunStore.insertPlanningDisposition")(function* (
    disposition: Disposition,
    repair: P,
  ) {
    yield* validateDisposition(disposition, repair);
    const rows = yield* sql<{
      repair_pass_id: string;
      disposition_json: string;
    }>`SELECT repair_pass_id, disposition_json FROM workflow_planning_dispositions WHERE review_pass_id = ${disposition.reportId} AND finding_id = ${disposition.findingId}`;
    const prior = rows[0];
    if (prior) {
      const value = yield* Effect.try({
        try: () => parse(prior.disposition_json),
        catch: () => planningError("integrity"),
      });
      if (prior.repair_pass_id !== repair.id || !sameCanonical(value, disposition))
        return yield* planningError("conflict");
      return;
    }
    const inserted =
      yield* sql`INSERT INTO workflow_planning_dispositions (review_pass_id, finding_id, repair_pass_id, disposition_json) VALUES (${disposition.reportId}, ${disposition.findingId}, ${repair.id}, ${encode(disposition)}) ON CONFLICT DO NOTHING RETURNING finding_id`;
    if (inserted.length !== 1) return yield* planningError("conflict");
  });
  const settlePlanningPass = Effect.fn("WorkflowRunStore.settlePlanningPass")(function* (input: {
    readonly sessionId: S["id"];
    readonly passId: P["id"];
    readonly operationId: P["operationId"];
    readonly expectedVersion: number;
    readonly observation: Extract<PlanningObservation, { kind: "terminal" }>;
    readonly report: Report;
  }) {
    const hash = yield* Effect.try({
      try: () =>
        WorkflowCanonicalHash.tagged("t3.planning-settlement.v1", {
          sessionId: input.sessionId,
          passId: input.passId,
          operationId: input.operationId,
          observation: input.observation,
          report: input.report,
        }),
      catch: () => planningError("integrity"),
    });
    const result = yield* planningTransaction(
      Effect.gen(function* () {
        const receipts = yield* sql<{
          fingerprint: string;
          result_json: string;
        }>`SELECT fingerprint, result_json FROM workflow_planning_settlements WHERE operation_id = ${input.operationId}`;
        const receipt = receipts[0];
        if (receipt) {
          if (receipt.fingerprint !== hash) return yield* planningError("integrity");
          const raw = yield* Effect.try({
            try: () => parse(receipt.result_json),
            catch: () => planningError("integrity"),
          });
          const value = yield* decodeSettlement(raw).pipe(
            Effect.mapError(() => planningError("integrity")),
          );
          return { kind: "replay" as const, receiptId: input.operationId, value };
        }
        const session = yield* readSession(input.sessionId);
        if (session.recordVersion !== input.expectedVersion)
          return yield* new PlanningControlError({
            reason: "conflict",
            currentVersion: session.recordVersion,
          });
        const pass = yield* readPass(input.passId);
        const rows = yield* sql<{
          record_json: string;
          fingerprint: string;
          status: string;
          generation: number;
          planning_session_id: string | null;
          kind: string;
        }>`SELECT record_json, fingerprint, status, generation, planning_session_id, kind FROM workflow_operations WHERE id = ${input.operationId}`;
        const row = rows[0];
        if (!row) return yield* planningError("integrity");
        const raw = yield* Effect.try({
          try: () => parse(row.record_json),
          catch: () => planningError("integrity"),
        });
        const operation = yield* decodeWorkflowOperationV1(raw).pipe(
          Effect.mapError(() => planningError("integrity")),
        );
        const params = operation.parameters;
        const observation = input.observation;
        if (observation.kind !== "terminal" || observation.outcome !== "succeeded")
          return yield* planningError("ineligible");
        if (
          operation.id !== input.operationId ||
          operation.owner.kind !== "planning_session" ||
          operation.owner.planningSessionId !== session.id ||
          operation.origin.kind !== session.origin.kind ||
          operation.origin.id !== session.origin.id ||
          operation.destination.environmentId !== session.environmentId ||
          operation.destination.projectId !== session.projectId ||
          row.planning_session_id !== session.id ||
          row.kind !== "planning_pass" ||
          params.kind !== "planning_pass" ||
          operation.fingerprint !== row.fingerprint ||
          operation.fingerprint !== WorkflowCanonicalHash.operation(operation) ||
          operation.status !== row.status ||
          operation.controlGeneration !== row.generation ||
          (operation.status !== "claimed" && operation.status !== "unknown") ||
          pass.status !== operation.status ||
          pass.operationId !== operation.id ||
          pass.planningSessionId !== session.id ||
          pass.binding.planId !== session.planId ||
          params.passId !== pass.id ||
          params.role !== pass.role ||
          !sameCanonical(params.binding, pass.binding) ||
          !sameCanonical(params.provider, pass.provider) ||
          !sameCanonical(params.limits, session.limits) ||
          params.inputManifestHash !==
            WorkflowCanonicalHash.tagged("t3.planning-input-manifest.v1", pass.inputManifest) ||
          observation.inputManifestHash !== params.inputManifestHash ||
          !sameCanonical(observation.actualInputManifest, pass.inputManifest) ||
          observation.operationId !== operation.id ||
          observation.passId !== pass.id ||
          !pass.native ||
          pass.native.terminalMessageId ||
          observation.native.threadId !== pass.native.threadId ||
          observation.native.runId !== pass.native.runId ||
          !observation.native.terminalMessageId
        )
          return yield* planningError("integrity");
        const settled = yield* decodePlanningPassV1({
          schemaVersion: pass.schemaVersion,
          id: pass.id,
          planningSessionId: pass.planningSessionId,
          role: pass.role,
          provider: pass.provider,
          binding: pass.binding,
          operationId: pass.operationId,
          inputManifest: pass.inputManifest,
          status: "settled",
          native: {
            threadId: pass.native.threadId,
            runId: pass.native.runId,
            terminalMessageId: observation.native.terminalMessageId,
          },
          report: input.report,
          access: pass.access,
        }).pipe(Effect.mapError(() => planningError("integrity")));
        if (settled.report?.kind === "repair")
          for (const disposition of settled.report.dispositions)
            yield* insertDisposition(disposition, settled);
        const nextOperation = yield* decodeWorkflowOperationV1({
          schemaVersion: operation.schemaVersion,
          id: operation.id,
          owner: operation.owner,
          kind: operation.kind,
          parameters: operation.parameters,
          fingerprint: operation.fingerprint,
          origin: operation.origin,
          destination: operation.destination,
          controlGeneration: operation.controlGeneration,
          status: "settled",
          access: operation.access,
          ...(operation.claim ? { claim: operation.claim } : {}),
          outcome: { kind: "succeeded", summary: "Planning pass completed", evidence: [] },
          ...(operation.nextDueAt ? { nextDueAt: operation.nextDueAt } : {}),
        }).pipe(Effect.mapError(() => planningError("integrity")));
        const remaining =
          yield* sql`SELECT id FROM workflow_operations WHERE planning_session_id = ${session.id} AND id != ${operation.id} AND status IN ('claimed','unknown')`;
        const status =
          remaining.length === 0 && session.status === "pausing"
            ? "paused"
            : remaining.length === 0 && session.status === "stopping"
              ? "stopped"
              : session.status;
        const updatedSession = { ...session, status, recordVersion: session.recordVersion + 1 };
        yield* decodePlanningSessionV1(updatedSession).pipe(
          Effect.mapError(() => planningError("integrity")),
        );
        const changedSession =
          yield* sql`UPDATE workflow_planning_sessions SET status = ${status}, record_version = ${updatedSession.recordVersion}, record_json = ${encode(updatedSession)} WHERE id = ${session.id} AND record_version = ${session.recordVersion} RETURNING id`;
        const changedPass =
          yield* sql`UPDATE workflow_planning_passes SET record_json = ${encode(settled)} WHERE id = ${pass.id} AND record_json = ${encode(pass)} RETURNING id`;
        const changedOperation =
          yield* sql`UPDATE workflow_operations SET status = 'settled', record_json = ${encode(nextOperation)} WHERE id = ${operation.id} AND record_json = ${encode(operation)} RETURNING id`;
        if (
          changedSession.length !== 1 ||
          changedPass.length !== 1 ||
          changedOperation.length !== 1
        )
          return yield* planningError("conflict");
        const observed = {
          ...observation,
          eligibleForReadiness: session.status === "active" || session.status === "pausing",
        };
        const inserted =
          yield* sql`INSERT INTO workflow_planning_observations (pass_id, operation_id, native_thread_id, native_run_id, terminal_message_id, manifest_hash, observation_json) VALUES (${pass.id}, ${operation.id}, ${observation.native.threadId}, ${observation.native.runId}, ${observation.native.terminalMessageId}, ${observation.inputManifestHash}, ${encode(observed)}) ON CONFLICT DO NOTHING RETURNING pass_id`;
        if (inserted.length !== 1) return yield* planningError("integrity");
        const value = { session: updatedSession, pass: settled };
        const saved =
          yield* sql`INSERT INTO workflow_planning_settlements (operation_id, fingerprint, result_json) VALUES (${operation.id}, ${hash}, ${encode(value)}) ON CONFLICT DO NOTHING RETURNING operation_id`;
        if (saved.length !== 1) return yield* planningError("integrity");
        return { kind: "applied" as const, receiptId: operation.id, value };
      }),
    );
    if (result.kind === "applied") yield* notify;
    return result;
  });
  const recordPlanningDisposition = Effect.fn("WorkflowRunStore.recordPlanningDisposition")(
    function* (input: {
      readonly sessionId: S["id"];
      readonly reviewPassId: P["id"];
      readonly findingId: string;
      readonly repairPassId: P["id"];
      readonly disposition: Disposition;
      readonly expectedVersion: number;
      readonly context: PlanningContext;
      readonly command: PlanningCommand;
    }) {
      if (
        !input.sessionId ||
        !input.reviewPassId ||
        !input.repairPassId ||
        !input.findingId ||
        !validCommand(input.command) ||
        !isPlanningContext(input.context) ||
        !Number.isSafeInteger(input.expectedVersion) ||
        input.expectedVersion < 0
      )
        return yield* planningError("invalid");
      const hash = yield* fingerprint(input.context, "recordPlanningDisposition", {
        sessionId: input.sessionId,
        reviewPassId: input.reviewPassId,
        findingId: input.findingId,
        repairPassId: input.repairPassId,
        disposition: input.disposition,
        expectedVersion: input.expectedVersion,
      });
      const result = yield* planningTransaction(
        Effect.gen(function* () {
          const receipts = yield* sql<{
            planning_session_id: string | null;
            fingerprint: string;
            receipt_id: string;
            result_json: string;
          }>`SELECT planning_session_id, fingerprint, receipt_id, result_json FROM workflow_command_receipts WHERE command_id = ${input.command.commandId}`;
          const receipt = receipts[0];
          if (receipt) {
            if (receipt.planning_session_id !== input.sessionId || receipt.fingerprint !== hash)
              return yield* planningError("conflict");
            const raw = yield* Effect.try({
              try: () => parse(receipt.result_json),
              catch: () => planningError("integrity"),
            });
            const value = yield* decodeDispositionResult(raw).pipe(
              Effect.mapError(() => planningError("integrity")),
            );
            return { kind: "replay" as const, receiptId: receipt.receipt_id, value };
          }
          const session = yield* readSession(input.sessionId);
          if (!sameContext(session, input.context)) return yield* planningError("unauthorized");
          if (session.recordVersion !== input.expectedVersion)
            return yield* new PlanningControlError({
              reason: "conflict",
              currentVersion: session.recordVersion,
            });
          const repair = yield* readPass(input.repairPassId);
          if (
            repair.status !== "settled" ||
            repair.report?.kind !== "repair" ||
            repair.binding.planId !== session.planId ||
            input.disposition.reportId !== input.reviewPassId ||
            input.disposition.findingId !== input.findingId ||
            !repair.report.dispositions.some((item) => sameCanonical(item, input.disposition))
          )
            return yield* planningError("integrity");
          yield* insertDisposition(input.disposition, repair);
          const updated = { ...session, recordVersion: session.recordVersion + 1 };
          const changed =
            yield* sql`UPDATE workflow_planning_sessions SET record_version = ${updated.recordVersion}, record_json = ${encode(updated)} WHERE id = ${session.id} AND record_version = ${session.recordVersion} RETURNING id`;
          if (changed.length !== 1) return yield* planningError("conflict");
          const value = { session: updated, disposition: input.disposition };
          const saved =
            yield* sql`INSERT INTO workflow_command_receipts (command_id, planning_session_id, fingerprint, receipt_id, result_json) VALUES (${input.command.commandId}, ${session.id}, ${hash}, ${input.command.receiptId}, ${encode(value)}) ON CONFLICT DO NOTHING RETURNING receipt_id`;
          if (saved.length !== 1) return yield* planningError("conflict");
          return { kind: "applied" as const, receiptId: input.command.receiptId, value };
        }),
      );
      if (result.kind === "applied") yield* notify;
      return result;
    },
  );
  const getPlanningPass = Effect.fn("WorkflowRunStore.getPlanningPass")(function* (input: {
    readonly passId: P["id"];
  }) {
    if (typeof input.passId !== "string" || !input.passId) return yield* planningError("invalid");
    const exists =
      yield* sql`SELECT id FROM workflow_planning_passes WHERE id = ${input.passId}`.pipe(
        Effect.mapError(() => planningError("storage")),
      );
    if (!exists.length) return yield* planningError("not-found");
    return yield* readPass(input.passId).pipe(
      Effect.mapError((error) => (isPlanningError(error) ? error : planningError("storage"))),
    );
  });
  const getPlanning = Effect.fn("WorkflowRunStore.getPlanning")(function* (input: {
    readonly sessionId: S["id"];
  }) {
    if (typeof input.sessionId !== "string" || !input.sessionId)
      return yield* planningError("invalid");
    return yield* readSession(input.sessionId).pipe(
      Effect.mapError((error) => (isPlanningError(error) ? error : planningError("storage"))),
    );
  });
  const controlPlanning = Effect.fn("WorkflowRunStore.controlPlanning")(function* (input: {
    readonly sessionId: S["id"];
    readonly action: "pause" | "resume" | "stop" | "expire";
    readonly expectedVersion: number;
    readonly now: string;
    readonly context: PlanningContext;
    readonly command: PlanningCommand;
  }) {
    if (
      !input.sessionId ||
      !validCommand(input.command) ||
      !isPlanningContext(input.context) ||
      typeof input.now !== "string" ||
      !isWorkflowTimestamp(input.now) ||
      !Number.isSafeInteger(input.expectedVersion) ||
      input.expectedVersion < 0 ||
      !["pause", "resume", "stop", "expire"].includes(input.action)
    )
      return yield* planningError("invalid");
    const hash = yield* fingerprint(input.context, "controlPlanning", {
      sessionId: input.sessionId,
      action: input.action,
      expectedVersion: input.expectedVersion,
    });
    const result = yield* planningTransaction(
      Effect.gen(function* () {
        const prior = yield* checkReceipt(input.command, input.sessionId, hash);
        if (prior) return prior;
        const value = yield* readSession(input.sessionId);
        if (!sameContext(value, input.context)) return yield* planningError("unauthorized");
        if (value.recordVersion !== input.expectedVersion)
          return yield* new PlanningControlError({
            reason: "conflict",
            currentVersion: value.recordVersion,
          });
        const rows = yield* sql<{
          id: string;
          status: string;
          held: number;
          generation: number;
          fingerprint: string;
          record_json: string;
        }>`SELECT id, status, held, generation, fingerprint, record_json FROM workflow_operations WHERE planning_session_id = ${value.id}`;
        const operations = yield* Effect.forEach(rows, (row) =>
          Effect.gen(function* () {
            const raw = yield* Effect.try({
              try: () => parse(row.record_json),
              catch: () => planningError("integrity"),
            });
            const operation = yield* decodeWorkflowOperationV1(raw).pipe(
              Effect.mapError(() => planningError("integrity")),
            );
            if (
              operation.id !== row.id ||
              operation.status !== row.status ||
              operation.controlGeneration !== row.generation ||
              operation.fingerprint !== row.fingerprint ||
              operation.owner.kind !== "planning_session" ||
              operation.owner.planningSessionId !== value.id
            )
              return yield* planningError("integrity");
            return { row, operation };
          }),
        );
        const claimed = operations.some(({ operation }) => operation.status === "claimed");
        const unknown = operations.some(({ operation }) => operation.status === "unknown");
        let status: S["status"];
        if (input.action === "pause") {
          if (value.status !== "active") return yield* planningError("ineligible");
          status = unknown ? "needs_attention" : claimed ? "pausing" : "paused";
        } else if (input.action === "resume") {
          if (value.status !== "paused") return yield* planningError("ineligible");
          if (claimed || unknown) return yield* planningError("custody");
          if (!isWorkflowTimestampAfter(value.limits.deadline, input.now))
            return yield* planningError("deadline");
          if (value.consumed.passes >= value.limits.maxPasses) return yield* planningError("limit");
          status = "active";
        } else {
          if (!["active", "paused", "pausing", "needs_attention"].includes(value.status))
            return yield* planningError("ineligible");
          status = claimed || unknown ? "stopping" : "stopped";
        }
        const updated = {
          ...value,
          status,
          recordVersion: value.recordVersion + 1,
          controlGeneration: value.controlGeneration + 1,
        };
        for (const { row, operation } of operations) {
          if (operation.status === "pending" && input.action === "pause") {
            yield* sql`UPDATE workflow_operations SET held = 1 WHERE id = ${operation.id}`;
          } else if (
            operation.status === "pending" &&
            input.action === "resume" &&
            row.held === 1
          ) {
            const next = { ...operation, controlGeneration: updated.controlGeneration };
            yield* sql`UPDATE workflow_operations SET held = 0, generation = ${updated.controlGeneration}, record_json = ${encode(next)} WHERE id = ${operation.id}`;
          } else if (
            operation.status === "pending" &&
            (input.action === "stop" || input.action === "expire")
          ) {
            const next = {
              ...operation,
              status: "cancelled" as const,
              outcome: { kind: "cancelled" as const, summary: "Cancelled", evidence: [] },
            };
            yield* sql`UPDATE workflow_operations SET status = 'cancelled', held = 0, cancel_requested = 1, record_json = ${encode(next)} WHERE id = ${operation.id}`;
            const passes = yield* sql<{
              record_json: string;
            }>`SELECT record_json FROM workflow_planning_passes WHERE operation_id = ${operation.id}`;
            if (passes.length !== 1) return yield* planningError("integrity");
            const raw = yield* Effect.try({
              try: () => parse(passes[0]!.record_json),
              catch: () => planningError("integrity"),
            });
            const pass = yield* decodePlanningPassV1(raw).pipe(
              Effect.mapError(() => planningError("integrity")),
            );
            if (
              pass.operationId !== operation.id ||
              pass.planningSessionId !== value.id ||
              pass.status !== "pending"
            )
              return yield* planningError("integrity");
            yield* sql`UPDATE workflow_planning_passes SET record_json = ${encode({ ...pass, status: "cancelled" })} WHERE operation_id = ${operation.id}`;
          } else if (
            (operation.status === "claimed" || operation.status === "unknown") &&
            (input.action === "stop" || input.action === "expire")
          ) {
            yield* sql`UPDATE workflow_operations SET cancel_requested = 1 WHERE id = ${operation.id}`;
          }
        }
        const changed = yield* sql<{
          id: string;
        }>`UPDATE workflow_planning_sessions SET status = ${status}, record_version = ${updated.recordVersion}, control_generation = ${updated.controlGeneration}, record_json = ${encode(updated)} WHERE id = ${value.id} AND record_version = ${input.expectedVersion} RETURNING id`;
        if (changed.length !== 1) return yield* planningError("conflict");
        return yield* savePlanningReceipt(input.command, hash, updated);
      }),
    );
    if (result.kind === "applied") yield* notify;
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
        let planningPass: P | undefined;
        if (operation.owner.kind === "planning_session") {
          const session = yield* readSession(operation.owner.planningSessionId).pipe(
            Effect.mapError(integrity),
          );
          if (
            !isWorkflowTimestamp(input.now) ||
            !isWorkflowTimestampAfter(session.limits.deadline, input.now)
          )
            return { kind: "denied" as const };
          const passes = yield* sql<{
            id: string;
            planning_session_id: string;
            plan_id: string;
            revision_id: string;
            revision_hash: string;
            source_packet_hash: string;
            operation_id: string;
            native_thread_id: string | null;
            native_run_id: string | null;
            record_json: string;
          }>`SELECT * FROM workflow_planning_passes WHERE operation_id = ${operation.id}`;
          const p = passes[0];
          if (!p || operation.parameters.kind !== "planning_pass") return yield* integrity();
          const raw = yield* Effect.try({ try: () => parse(p.record_json), catch: integrity });
          const pass = yield* decodePlanningPassV1(raw).pipe(Effect.mapError(integrity));
          if (
            pass.id !== p.id ||
            pass.planningSessionId !== p.planning_session_id ||
            pass.planningSessionId !== session.id ||
            pass.operationId !== operation.id ||
            p.operation_id !== operation.id ||
            pass.binding.planId !== p.plan_id ||
            pass.binding.revisionId !== p.revision_id ||
            pass.binding.revisionHash !== p.revision_hash ||
            pass.binding.sourcePacketHash !== p.source_packet_hash ||
            !pass.native ||
            pass.native.terminalMessageId ||
            pass.native.threadId !== p.native_thread_id ||
            pass.native.runId !== p.native_run_id ||
            pass.status !== "pending" ||
            operation.parameters.passId !== pass.id ||
            WorkflowCanonicalHash.tagged("binding", operation.parameters.binding) !==
              WorkflowCanonicalHash.tagged("binding", pass.binding)
          )
            return yield* integrity();
          planningPass = pass;
        }
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
        if (planningPass) {
          const changed =
            yield* sql`UPDATE workflow_planning_passes SET record_json = ${encode({ ...planningPass, status: "claimed" })} WHERE id = ${planningPass.id} AND operation_id = ${operation.id} RETURNING id`;
          if (changed.length !== 1) return yield* integrity();
        }
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
          if (operation.kind === "planning_pass" && input.observed.kind === "settled")
            return yield* integrity();
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
          if (operation.owner.kind === "planning_session") {
            const passes = yield* sql<{
              record_json: string;
            }>`SELECT record_json FROM workflow_planning_passes WHERE operation_id = ${operation.id}`;
            const passRow = passes[0];
            if (!passRow) return yield* integrity();
            const raw = yield* Effect.try({
              try: () => parse(passRow.record_json),
              catch: integrity,
            });
            const pass = yield* decodePlanningPassV1(raw).pipe(Effect.mapError(integrity));
            if (
              pass.operationId !== operation.id ||
              pass.planningSessionId !== operation.owner.planningSessionId ||
              pass.status !== operation.status
            )
              return yield* integrity();
            const updatedPass =
              yield* sql`UPDATE workflow_planning_passes SET record_json = ${encode({ ...pass, status })} WHERE id = ${pass.id} AND record_json = ${passRow.record_json} RETURNING id`;
            if (updatedPass.length !== 1) return yield* stale();
          }
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
    beginPlanning,
    admitPlanningPass,
    settlePlanningPass,
    recordPlanningDisposition,
    getPlanning,
    getPlanningPass,
    controlPlanning,
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
