// @effect-diagnostics deterministicKeys:off -- The authorization port key is fixed by the integration contract.
import {
  decodeDevelopmentPlanDocumentV2,
  decodeDevelopmentPlanRevisionV1,
  PlanningPassV1,
  upgradeDevelopmentPlanDocumentV1,
  OrchestrationV2PlanArtifact,
  DevelopmentPlanRevisionV1,
  type PlanningSessionV1,
  type WorkflowOperationV1,
  decodePlanningSessionV1,
  decodePlanningPassV1,
  decodeWorkflowOperationV1,
  RevisionEnvelope,
} from "@t3tools/contracts";
import * as Digest from "effect/crypto/Digest";
import * as Base64 from "effect/encoding/Base64";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Store from "./DevelopmentPlanStore.ts";
import * as WorkflowStore from "../workflowRuns/WorkflowRunStore.ts";
import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";
import * as Observation from "./PlanningPassObservation.ts";
import { DevelopmentPlanCoverage } from "./DevelopmentPlanCoverage.ts";
import { DevelopmentPlanHash } from "./DevelopmentPlanHash.ts";

export class DevelopmentPlanServiceError extends Schema.TaggedError<DevelopmentPlanServiceError>()(
  "DevelopmentPlanServiceError",
  {
    reason: Schema.Literals([
      "not-found",
      "conflict",
      "invalid",
      "integrity",
      "storage",
      "unauthorized",
    ]),
    currentVersion: Schema.optionalKey(Schema.Number),
  },
) {}

type Origin = {
  readonly environmentId: string;
  readonly projectId: string;
  readonly threadId?: string;
};
type Operation = "create" | "read" | "update" | "link" | "unlink";
type RequestContext = {
  readonly actor: { readonly kind: "human" | "agent" | "system"; readonly id: string };
  readonly destination: { readonly environmentId: string; readonly projectId: string };
};
export class DevelopmentPlanAuthorization extends Context.Service<
  DevelopmentPlanAuthorization,
  {
    readonly check: (
      input: RequestContext & {
        readonly operation: Operation;
        readonly origins: readonly Origin[];
      },
    ) => Effect.Effect<void, DevelopmentPlanServiceError>;
  }
>()("t3/developmentPlans/DevelopmentPlanAuthorization") {}
export const denyAllLayer = Layer.succeed(DevelopmentPlanAuthorization, {
  check: Effect.fn("DevelopmentPlanAuthorization.check")(() =>
    Effect.fail(new DevelopmentPlanServiceError({ reason: "unauthorized" })),
  ),
});

export class DevelopmentPlanPassReader extends Context.Service<
  DevelopmentPlanPassReader,
  {
    readonly findReview: (input: {
      readonly planId: string;
      readonly revisionId: string;
      readonly revisionHash: string;
      readonly sourcePacketHash: string;
    }) => Effect.Effect<PlanningPassV1 | undefined>;
  }
>()("t3/developmentPlans/DevelopmentPlanPassReader") {}
export const emptyPassReaderLayer = Layer.succeed(DevelopmentPlanPassReader, {
  findReview: Effect.fn("DevelopmentPlanPassReader.findReview")(() => Effect.undefined),
});

export class DevelopmentPlanReviewAttestation extends Context.Service<
  DevelopmentPlanReviewAttestation,
  {
    readonly attest: (input: {
      readonly passId: string;
      readonly revisionHash: string;
      readonly sourcePacketHash: string;
    }) => Effect.Effect<boolean>;
  }
>()("t3/developmentPlans/DevelopmentPlanReviewAttestation") {}
export const denyAttestationLayer = Layer.succeed(DevelopmentPlanReviewAttestation, {
  attest: Effect.fn("DevelopmentPlanReviewAttestation.attest")(() => Effect.succeed(false)),
});

const parseEvidence = (value: string): unknown => JSON.parse(value);
const sameEvidence = (a: unknown, b: unknown) =>
  WorkflowCanonicalHash.tagged("t3.evidence-comparison.v1", a) ===
  WorkflowCanonicalHash.tagged("t3.evidence-comparison.v1", b);
const decodeObservation = Schema.decodeUnknownEffect(
  Schema.Struct({
    kind: Schema.Literal("terminal"),
    operationId: PlanningPassV1.fields.operationId,
    passId: PlanningPassV1.fields.id,
    native: Schema.Struct({
      threadId: PlanningPassV1.fields.native.schema.fields.threadId,
      runId: PlanningPassV1.fields.native.schema.fields.runId,
      terminalMessageId: PlanningPassV1.fields.native.schema.fields.terminalMessageId.schema,
    }),
    outcome: Schema.Literals(["succeeded", "failed", "cancelled"]),
    actualInputManifest: PlanningPassV1.fields.inputManifest,
    inputManifestHash: Schema.String,
    freshConversation: Schema.Boolean,
    sourcePacketDelivered: Schema.Boolean,
    forbiddenContextDelivered: Schema.Boolean,
    restrictedProfileQualified: Schema.Boolean,
    eligibleForReadiness: Schema.Boolean,
  }),
);
const decodeDisposition = Schema.decodeUnknownEffect(
  PlanningPassV1.fields.report.schema.members[1].fields.dispositions.value,
);
class EvidenceInvalid extends Schema.TaggedError<EvidenceInvalid>()("EvidenceInvalid", {}) {}
const evidenceInvalid = () => new EvidenceInvalid();
const parseStored = (value: string) =>
  Effect.try({ try: () => parseEvidence(value), catch: evidenceInvalid });

type ReviewRow = {
  id: string;
  planning_session_id: string;
  plan_id: string;
  revision_id: string;
  revision_hash: string;
  source_packet_hash: string;
  operation_id: string;
  native_thread_id: string | null;
  native_run_id: string | null;
  pass_json: string;
  operation_json: string;
  operation_fingerprint: string;
  operation_status: string;
  operation_generation: number;
  operation_session_id: string;
  operation_kind: string;
  settlement_fingerprint: string;
  settlement_json: string;
  observation_json: string;
  observed_thread_id: string;
  observed_run_id: string;
  observed_message_id: string;
  manifest_hash: string;
};
const decodeReviewRow = Effect.fn("DevelopmentPlanEvidence.decodeReviewRow")(function* (
  row: ReviewRow,
) {
  const pass = yield* decodePlanningPassV1(yield* parseStored(row.pass_json));
  const operation = yield* decodeWorkflowOperationV1(yield* parseStored(row.operation_json));
  const observation = yield* decodeObservation(yield* parseStored(row.observation_json), {
    onExcessProperty: "error",
  });
  const settlement = yield* parseStored(row.settlement_json);
  const valid = yield* Effect.try({
    catch: evidenceInvalid,
    try: () => {
      const { eligibleForReadiness: _eligibleForReadiness, ...terminal } = observation;
      const settlementHash = WorkflowCanonicalHash.tagged("t3.planning-settlement.v1", {
        sessionId: pass.planningSessionId,
        passId: pass.id,
        operationId: operation.id,
        observation: terminal,
        report: pass.report,
      });
      const saved = settlement as { pass?: unknown };
      if (settlementHash !== row.settlement_fingerprint || !sameEvidence(saved.pass, pass))
        return false;
      const params = operation.parameters;
      return (
        pass.id === row.id &&
        pass.planningSessionId === row.planning_session_id &&
        pass.binding.planId === row.plan_id &&
        pass.binding.revisionId === row.revision_id &&
        pass.binding.revisionHash === row.revision_hash &&
        pass.binding.sourcePacketHash === row.source_packet_hash &&
        pass.operationId === row.operation_id &&
        pass.native?.threadId === row.native_thread_id &&
        pass.native?.runId === row.native_run_id &&
        pass.role === "review" &&
        pass.status === "settled" &&
        pass.report?.kind === "review" &&
        pass.native?.terminalMessageId === row.observed_message_id &&
        pass.native.threadId === row.observed_thread_id &&
        pass.native.runId === row.observed_run_id &&
        observation.passId === pass.id &&
        observation.operationId === operation.id &&
        sameEvidence(observation.native, pass.native) &&
        operation.id === row.operation_id &&
        operation.status === "settled" &&
        row.operation_status === "settled" &&
        operation.controlGeneration === row.operation_generation &&
        row.operation_kind === "planning_pass" &&
        row.operation_session_id === pass.planningSessionId &&
        operation.owner.kind === "planning_session" &&
        operation.owner.planningSessionId === pass.planningSessionId &&
        params.kind === "planning_pass" &&
        params.passId === pass.id &&
        params.role === pass.role &&
        sameEvidence(params.binding, pass.binding) &&
        sameEvidence(params.provider, pass.provider) &&
        operation.fingerprint === row.operation_fingerprint &&
        operation.fingerprint === WorkflowCanonicalHash.operation(operation)
      );
    },
  });
  if (!valid) return yield* Effect.fail("invalid evidence");
  return { pass, operation, observation };
});
const selectReviews = (
  sql: SqlClient.SqlClient,
  input: { planId: string; revisionId: string; revisionHash: string; sourcePacketHash: string },
) =>
  sql<ReviewRow>`SELECT p.id, p.planning_session_id, p.plan_id, p.revision_id, p.revision_hash, p.source_packet_hash, p.operation_id, p.native_thread_id, p.native_run_id, p.record_json AS pass_json, o.record_json AS operation_json, o.fingerprint AS operation_fingerprint, o.status AS operation_status, o.generation AS operation_generation, o.planning_session_id AS operation_session_id, o.kind AS operation_kind, st.fingerprint AS settlement_fingerprint, st.result_json AS settlement_json, ob.observation_json, ob.native_thread_id AS observed_thread_id, ob.native_run_id AS observed_run_id, ob.terminal_message_id AS observed_message_id, ob.manifest_hash FROM workflow_planning_passes p JOIN workflow_operations o ON o.id = p.operation_id LEFT JOIN workflow_planning_observations ob ON ob.pass_id = p.id AND ob.operation_id = o.id LEFT JOIN workflow_planning_settlements st ON st.operation_id = o.id WHERE p.plan_id = ${input.planId} AND p.revision_id = ${input.revisionId} AND p.revision_hash = ${input.revisionHash} AND p.source_packet_hash = ${input.sourcePacketHash} AND o.status = 'settled' ORDER BY st.rowid DESC, p.id DESC`;

export const persistedPassReaderLayer = Layer.effect(
  DevelopmentPlanPassReader,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return DevelopmentPlanPassReader.of({
      findReview: Effect.fn("DevelopmentPlanPassReader.findReview")(
        function* (input) {
          const rows = yield* selectReviews(sql, input);
          // A malformed newest settled candidate is never bypassed.
          const row = rows[0];
          if (!row) return undefined;
          return (yield* decodeReviewRow(row)).pass;
        },
        Effect.orElseSucceed(() => undefined),
      ),
    });
  }),
);

export const localAttestationLayer = Layer.effect(
  DevelopmentPlanReviewAttestation,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const digest = yield* Digest.Digest;
    return DevelopmentPlanReviewAttestation.of({
      attest: Effect.fn("DevelopmentPlanReviewAttestation.attest")(
        function* (input) {
          const ids = yield* sql<{
            plan_id: string;
            revision_id: string;
            revision_hash: string;
            source_packet_hash: string;
          }>`SELECT plan_id, revision_id, revision_hash, source_packet_hash FROM workflow_planning_passes WHERE id = ${input.passId}`;
          const id = ids[0];
          if (
            !id ||
            id.revision_hash !== input.revisionHash ||
            id.source_packet_hash !== input.sourcePacketHash
          )
            return false;
          const rows = yield* selectReviews(sql, {
            planId: id.plan_id,
            revisionId: id.revision_id,
            revisionHash: input.revisionHash,
            sourcePacketHash: input.sourcePacketHash,
          });
          const row = rows[0];
          if (!row || row.id !== input.passId) return false;
          const { pass, operation, observation } = yield* decodeReviewRow(row);
          if (
            pass.report?.kind !== "review" ||
            !pass.native ||
            operation.parameters.kind !== "planning_pass"
          )
            return false;
          const data = yield* sql<{
            session_json: string;
            revision_json: string;
            packet_json: string;
            created_at: string;
            session_status: string;
            record_version: number;
            control_generation: number;
          }>`SELECT s.record_json AS session_json, s.status AS session_status, s.record_version, s.control_generation, r.revision_json, r.created_at, sp.packet_json FROM workflow_planning_sessions s JOIN development_plan_revisions r ON r.plan_id = s.plan_id AND r.revision_id = ${id.revision_id} AND r.revision_hash = ${input.revisionHash} AND r.source_packet_hash = ${input.sourcePacketHash} JOIN development_plan_source_packets sp ON sp.plan_id = r.plan_id AND sp.source_packet_hash = r.source_packet_hash WHERE s.id = ${pass.planningSessionId}`;
          const stored = data[0];
          if (!stored) return false;
          const session = yield* decodePlanningSessionV1(yield* parseStored(stored.session_json));
          const envelope = yield* decodeDevelopmentPlanRevisionV1({
            planId: id.plan_id,
            revisionId: id.revision_id,
            revisionHash: input.revisionHash,
            sourcePacketHash: input.sourcePacketHash,
            revision: yield* parseStored(stored.revision_json),
            sourcePacket: yield* parseStored(stored.packet_json),
            createdAt: stored.created_at,
          }).pipe(Effect.provideService(Digest.Digest, digest));
          const valid = yield* Effect.try({
            catch: evidenceInvalid,
            try: () => {
              const params = operation.parameters;
              if (params.kind !== "planning_pass") return false;
              const manifestHash = WorkflowCanonicalHash.tagged(
                "t3.planning-input-manifest.v1",
                observation.actualInputManifest,
              );
              return (
                session.id === pass.planningSessionId &&
                session.planId === id.plan_id &&
                session.status === stored.session_status &&
                session.recordVersion === stored.record_version &&
                session.controlGeneration === stored.control_generation &&
                sameEvidence(operation.origin, session.origin) &&
                operation.destination.environmentId === session.environmentId &&
                operation.destination.projectId === session.projectId &&
                sameEvidence(params.limits, session.limits) &&
                DevelopmentPlanHash.revision(envelope.revision) === input.revisionHash &&
                DevelopmentPlanHash.sourcePacket(envelope.sourcePacket) ===
                  input.sourcePacketHash &&
                observation.outcome === "succeeded" &&
                observation.eligibleForReadiness &&
                observation.freshConversation &&
                observation.sourcePacketDelivered &&
                !observation.forbiddenContextDelivered &&
                observation.restrictedProfileQualified &&
                manifestHash === row.manifest_hash &&
                manifestHash === observation.inputManifestHash &&
                manifestHash === params.inputManifestHash &&
                sameEvidence(observation.actualInputManifest, pass.inputManifest)
              );
            },
          });
          if (
            !valid ||
            pass.report.verdict !== "pass" ||
            pass.report.findings.some((f) => f.severity === "blocking") ||
            pass.report.assessment.inventory.some((i) => i.disposition !== "mapped")
          )
            return false;
          const others = yield* sql<{
            id: string;
            native_thread_id: string | null;
            record_json: string;
          }>`SELECT id, native_thread_id, record_json FROM workflow_planning_passes WHERE plan_id = ${id.plan_id} AND id != ${pass.id}`;
          for (const other of others) {
            const decoded = yield* decodePlanningPassV1(yield* parseStored(other.record_json));
            if (
              decoded.id !== other.id ||
              (decoded.native?.threadId ?? null) !== other.native_thread_id ||
              other.native_thread_id === pass.native.threadId
            )
              return false;
          }
          const dispositions = yield* sql<{
            finding_id: string;
            repair_pass_id: string;
            disposition_json: string;
            repair_json: string;
          }>`SELECT d.finding_id, d.repair_pass_id, d.disposition_json, p.record_json AS repair_json FROM workflow_planning_dispositions d JOIN workflow_planning_passes p ON p.id = d.repair_pass_id WHERE d.review_pass_id = ${pass.id}`;
          for (const row of dispositions) {
            const disposition = yield* decodeDisposition(yield* parseStored(row.disposition_json), {
              onExcessProperty: "error",
            });
            const repair = yield* decodePlanningPassV1(yield* parseStored(row.repair_json));
            const finding = pass.report.findings.find((f) => f.id === row.finding_id);
            if (
              !finding ||
              disposition.reportId !== pass.id ||
              disposition.findingId !== finding.id ||
              !disposition.rationale.trim() ||
              (disposition.decision === "defer" && !disposition.deferTrigger?.trim()) ||
              disposition.affectedObjectIds.some((id) => !finding.affectedObjectIds.includes(id)) ||
              repair.id !== row.repair_pass_id ||
              repair.status !== "settled" ||
              repair.report?.kind !== "repair" ||
              !sameEvidence(repair.binding, pass.binding) ||
              !repair.report.dispositions.some((d) => sameEvidence(d, disposition))
            )
              return false;
          }
          return true;
        },
        Effect.orElseSucceed(() => false),
      ),
    });
  }),
);

type StoreService = Store.DevelopmentPlanStore["Service"];
type Create = Omit<
  Parameters<StoreService["create"]>[0],
  "environmentId" | "projectId" | "createdAt"
>;
type Update = Omit<Parameters<StoreService["append"]>[0], "createdAt">;
type Link = Parameters<StoreService["linkThread"]>[0];
const originsOf = (
  revision: DevelopmentPlanRevisionV1,
  packet: RevisionEnvelope["sourcePacket"],
  destination: RequestContext["destination"],
  threadId?: string,
): Origin[] => {
  const origins: Origin[] = [
    ...revision.access.origins,
    ...packet.access.origins,
    { ...destination, ...(threadId === undefined ? {} : { threadId }) },
  ];
  return origins.filter(
    (origin, i) =>
      origins.findIndex(
        (other) =>
          other.environmentId === origin.environmentId &&
          other.projectId === origin.projectId &&
          other.threadId === origin.threadId,
      ) === i,
  );
};
const mapStore = (error: Store.DevelopmentPlanStoreError) =>
  new DevelopmentPlanServiceError({
    reason: error.reason,
    ...(error.currentVersion === undefined ? {} : { currentVersion: error.currentVersion }),
  });

const parseBytes = (bytes: Uint8Array): unknown =>
  JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
const decodeArtifact = Schema.decodeUnknownEffect(OrchestrationV2PlanArtifact);
const decodeRevision = Schema.decodeUnknownEffect(DevelopmentPlanRevisionV1);
const decodePacket = Schema.decodeUnknownEffect(RevisionEnvelope.fields.sourcePacket);

type Import = {
  readonly format: "v1" | "v2" | "markdown" | "plan-artifact";
  readonly bytes: Uint8Array;
  readonly planId: string;
  readonly revisionId: string;
  readonly title: string;
  readonly commandId: string;
  readonly documentId?: string;
};

const make = Effect.gen(function* () {
  const store = yield* Store.DevelopmentPlanStore;
  const digest = yield* Digest.Digest;
  const authorization = yield* DevelopmentPlanAuthorization;
  const passReader = yield* DevelopmentPlanPassReader;
  const attestation = yield* DevelopmentPlanReviewAttestation;
  const workflows = yield* WorkflowStore.WorkflowRunStore;
  const observer = yield* Observation.PlanningPassObservation;
  const check = (context: RequestContext, operation: Operation, origins: readonly Origin[]) =>
    authorization.check({ ...context, operation, origins });
  const precheck = (context: RequestContext, operation: Operation) =>
    check(context, operation, [context.destination]);
  const create = Effect.fn("DevelopmentPlanService.create")(function* (
    input: Create,
    context: RequestContext,
  ) {
    yield* precheck(context, "create");
    yield* check(context, "create", originsOf(input.revision, input.packet, context.destination));
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    return yield* store
      .create({ ...input, ...context.destination, createdAt }, context)
      .pipe(Effect.mapError(mapStore));
  });
  const importPlan = Effect.fn("DevelopmentPlanService.import")(function* (
    input: Import,
    context: RequestContext,
  ) {
    yield* precheck(context, "create");
    const invalid = () => new DevelopmentPlanServiceError({ reason: "invalid" });
    let revision: DevelopmentPlanRevisionV1;
    let packet: RevisionEnvelope["sourcePacket"];
    if (input.format === "v1" || input.format === "v2") {
      const document =
        input.format === "v1"
          ? yield* upgradeDevelopmentPlanDocumentV1({
              bytes: input.bytes,
              hashSourcePacket: DevelopmentPlanHash.sourcePacket,
            }).pipe(Effect.provideService(Digest.Digest, digest), Effect.mapError(invalid))
          : yield* Effect.try({ try: () => parseBytes(input.bytes), catch: invalid }).pipe(
              Effect.flatMap(decodeDevelopmentPlanDocumentV2),
              Effect.provideService(Digest.Digest, digest),
              Effect.mapError(invalid),
            );
      if (
        document.planId !== input.planId ||
        document.revisionId !== input.revisionId ||
        document.native.revision.sourcePacketHash !==
          DevelopmentPlanHash.sourcePacket(document.native.sourcePacket)
      )
        return yield* invalid();
      revision = document.native.revision;
      packet = document.native.sourcePacket;
    } else {
      if (input.documentId === undefined) return yield* invalid();
      if (input.format === "plan-artifact") {
        yield* Effect.try({ try: () => parseBytes(input.bytes), catch: invalid }).pipe(
          Effect.flatMap((value) => decodeArtifact(value, { onExcessProperty: "error" })),
          Effect.mapError(invalid),
        );
      }
      const hash = yield* digest.digest(Digest.sha256, input.bytes).pipe(Effect.mapError(invalid));
      packet = yield* decodePacket({
        schemaVersion: 1,
        sources: [
          {
            id: "source-1",
            kind: "imported_document",
            ...context.destination,
            documentId: input.documentId,
            capturedAt: DateTime.formatIso(yield* DateTime.now),
            attribution: context.actor.id,
            bytesBase64: Base64.encode(input.bytes),
            byteLength: input.bytes.length,
            sha256: Array.from(hash, (byte) => byte.toString(16).padStart(2, "0")).join(""),
          },
        ],
        statements: [],
        selectionLimitations: [],
        access: { origins: [context.destination] },
      }).pipe(Effect.mapError(invalid));
      revision = yield* decodeRevision({
        schemaVersion: 1,
        planId: input.planId,
        sourcePacketHash: DevelopmentPlanHash.sourcePacket(packet),
        brief: input.title,
        requirements: [],
        decisions: [],
        questions: [],
        assumptions: [],
        approach: [],
        workItems: [],
        access: packet.access,
        coverageReceipts: [],
        unmappedMaterial: [
          {
            sourceId: "source-1",
            description: `Imported ${input.format === "markdown" ? "markdown" : "plan artifact"}; requirements not extracted`,
          },
        ],
      }).pipe(Effect.mapError(invalid));
    }
    return yield* create(
      {
        planId: input.planId,
        revisionId: input.revisionId,
        title: input.title,
        commandId: input.commandId,
        revision,
        packet,
        fingerprintInput: {
          import: input.format,
          bytesBase64: Base64.encode(input.bytes),
          planId: input.planId,
          revisionId: input.revisionId,
          title: input.title,
          commandId: input.commandId,
          ...(input.documentId === undefined ? {} : { documentId: input.documentId }),
        },
      },
      context,
    );
  });
  const exportPlan = Effect.fn("DevelopmentPlanService.export")(function* (
    input: Parameters<StoreService["get"]>[0],
    context: RequestContext,
  ) {
    const result = yield* get(input, context);
    const { plan, revision, sourcePacket, revisionId } = result;
    return yield* decodeDevelopmentPlanDocumentV2({
      schemaVersion: 2,
      planId: plan.planId,
      revisionId,
      environmentId: plan.environmentId,
      projectId: plan.projectId,
      title: plan.title,
      linkedThreadIds: plan.linkedThreadIds,
      brief: revision.brief,
      sources: sourcePacket.sources,
      sourceStatements: sourcePacket.statements,
      requirements: revision.requirements.map(({ id, text, sourceStatementIds, criteria }) => ({
        id,
        text,
        sourceStatementIds,
        criteria: criteria.map(({ id, text }) => ({ id, text })),
      })),
      native: { revision, sourcePacket },
    }).pipe(
      Effect.provideService(Digest.Digest, digest),
      Effect.mapError(() => new DevelopmentPlanServiceError({ reason: "integrity" })),
    );
  });
  const get = Effect.fn("DevelopmentPlanService.get")(function* (
    input: Parameters<StoreService["get"]>[0],
    context: RequestContext,
  ) {
    yield* precheck(context, "read");
    const result = yield* store.get(input).pipe(Effect.mapError(mapStore));
    yield* check(
      context,
      "read",
      originsOf(result.revision, result.sourcePacket, context.destination),
    );
    return result;
  });
  const validateCoverage = Effect.fn("DevelopmentPlanService.validateCoverage")(function* (
    input: Parameters<StoreService["get"]>[0],
    context: RequestContext,
  ) {
    yield* precheck(context, "read");
    const result = yield* store.get(input).pipe(Effect.mapError(mapStore));
    yield* check(
      context,
      "read",
      originsOf(result.revision, result.sourcePacket, context.destination),
    );
    const review = yield* passReader.findReview({
      planId: input.planId,
      revisionId: result.revisionId,
      revisionHash: DevelopmentPlanHash.revision(result.revision),
      sourcePacketHash: DevelopmentPlanHash.sourcePacket(result.sourcePacket),
    });
    const coverage = DevelopmentPlanCoverage.validate({
      revision: result.revision,
      sourcePacket: result.sourcePacket,
      ...(review === undefined ? {} : { review }),
    });
    const reviewReady =
      review && coverage.structuralBlockers.length === 0 && coverage.reviewBlockers.length === 0
        ? yield* attestation.attest({
            passId: review.id,
            revisionHash: coverage.revisionHash,
            sourcePacketHash: coverage.sourcePacketHash,
          })
        : false;
    return { revisionId: result.revisionId, coverage, reviewReady };
  });
  const getObject = Effect.fn("DevelopmentPlanService.getObject")(function* (
    input: Parameters<StoreService["getObject"]>[0],
    context: RequestContext,
  ) {
    yield* precheck(context, "read");
    const result = yield* store
      .get({ planId: input.planId, revisionId: input.revisionId })
      .pipe(Effect.mapError(mapStore));
    yield* check(
      context,
      "read",
      originsOf(result.revision, result.sourcePacket, context.destination),
    );
    return yield* store.getObject(input).pipe(Effect.mapError(mapStore));
  });
  const list = Effect.fn("DevelopmentPlanService.list")(function* (
    input: Parameters<StoreService["list"]>[0],
    context: RequestContext,
  ) {
    yield* precheck(context, "read");
    if (input.projectId !== context.destination.projectId)
      return yield* new DevelopmentPlanServiceError({ reason: "unauthorized" });
    const page = yield* store.list(input).pipe(Effect.mapError(mapStore));
    const plans: typeof page.plans = [];
    for (const plan of page.plans) {
      const result = yield* store.get({ planId: plan.planId }).pipe(Effect.mapError(mapStore));
      const allowed = yield* Effect.result(
        check(
          context,
          "read",
          originsOf(result.revision, result.sourcePacket, context.destination),
        ),
      );
      if (allowed._tag === "Success") plans.push(plan);
      else if (allowed.failure.reason !== "unauthorized") return yield* allowed.failure;
    }
    return { ...page, plans };
  });
  const updateDraft = Effect.fn("DevelopmentPlanService.updateDraft")(function* (
    input: Update,
    context: RequestContext,
  ) {
    yield* precheck(context, "update");
    const existing = yield* store.get({ planId: input.planId }).pipe(Effect.mapError(mapStore));
    const origins = [
      ...originsOf(existing.revision, existing.sourcePacket, context.destination),
      ...originsOf(input.revision, input.packet, context.destination),
    ];
    yield* check(
      context,
      "update",
      origins.filter(
        (origin, i) =>
          origins.findIndex(
            (other) =>
              other.environmentId === origin.environmentId &&
              other.projectId === origin.projectId &&
              other.threadId === origin.threadId,
          ) === i,
      ),
    );
    const createdAt = DateTime.formatIso(yield* DateTime.now);
    return yield* store.append({ ...input, createdAt }, context).pipe(Effect.mapError(mapStore));
  });
  const linkThread = Effect.fn("DevelopmentPlanService.linkThread")(function* (
    input: Link,
    context: RequestContext,
  ) {
    yield* precheck(context, "link");
    const existing = yield* store.get({ planId: input.planId }).pipe(Effect.mapError(mapStore));
    yield* check(
      context,
      "link",
      originsOf(existing.revision, existing.sourcePacket, context.destination, input.threadId),
    );
    return yield* store.linkThread(input, context).pipe(Effect.mapError(mapStore));
  });
  const unlinkThread = Effect.fn("DevelopmentPlanService.unlinkThread")(function* (
    input: Link,
    context: RequestContext,
  ) {
    yield* precheck(context, "unlink");
    const existing = yield* store.get({ planId: input.planId }).pipe(Effect.mapError(mapStore));
    yield* check(
      context,
      "unlink",
      originsOf(existing.revision, existing.sourcePacket, context.destination, input.threadId),
    );
    return yield* store.unlinkThread(input, context).pipe(Effect.mapError(mapStore));
  });
  type PlanningContext = {
    readonly actor: PlanningSessionV1["origin"];
    readonly destination: {
      readonly environmentId: PlanningSessionV1["environmentId"];
      readonly projectId: PlanningSessionV1["projectId"];
    };
  };
  type Command = { readonly commandId: string; readonly receiptId: string };
  type ControlInput = {
    readonly sessionId: PlanningSessionV1["id"];
    readonly expectedVersion: number;
    readonly command: Command;
  };
  const invalidPlanning = () => new WorkflowStore.PlanningControlError({ reason: "invalid" });
  const authorizePlanning = Effect.fn("DevelopmentPlanService.authorizePlanning")(function* (
    session: PlanningSessionV1,
    context: PlanningContext,
    operation: "read" | "update",
    passes: readonly PlanningPassV1[] = [],
  ) {
    if (
      session.environmentId !== context.destination.environmentId ||
      session.projectId !== context.destination.projectId ||
      session.origin.kind !== context.actor.kind ||
      session.origin.id !== context.actor.id
    )
      return yield* new DevelopmentPlanServiceError({ reason: "unauthorized" });
    const plan = yield* store.get({ planId: session.planId }).pipe(Effect.mapError(mapStore));
    const origins = [
      ...originsOf(plan.revision, plan.sourcePacket, context.destination),
      ...session.access.origins,
    ];
    for (const pass of passes) {
      const bound = yield* store
        .get({ planId: pass.binding.planId, revisionId: pass.binding.revisionId })
        .pipe(Effect.mapError(mapStore));
      origins.push(
        ...originsOf(bound.revision, bound.sourcePacket, context.destination),
        ...pass.access.origins,
        ...pass.inputManifest.access.origins,
      );
    }
    yield* check(
      context,
      operation,
      origins.filter(
        (origin, i) =>
          origins.findIndex(
            (other) =>
              other.environmentId === origin.environmentId &&
              other.projectId === origin.projectId &&
              other.threadId === origin.threadId,
          ) === i,
      ),
    );
  });
  const beginPlanning = Effect.fn("DevelopmentPlanService.beginPlanning")(function* (
    input: { readonly session: PlanningSessionV1; readonly command: Command },
    context: PlanningContext,
  ) {
    yield* check(context, "update", []);
    const now = DateTime.formatIso(yield* DateTime.now);
    const session = yield* decodePlanningSessionV1({
      schemaVersion: input.session.schemaVersion,
      id: input.session.id,
      environmentId: input.session.environmentId,
      projectId: input.session.projectId,
      planId: input.session.planId,
      ...(input.session.predecessorSessionId === undefined
        ? {}
        : { predecessorSessionId: input.session.predecessorSessionId }),
      origin: input.session.origin,
      request: input.session.request,
      mode: input.session.mode,
      allowedRoles: input.session.allowedRoles,
      allowedProviders: input.session.allowedProviders,
      limits: input.session.limits,
      consumed: input.session.consumed,
      controlGeneration: input.session.controlGeneration,
      recordVersion: input.session.recordVersion,
      status: input.session.status,
      createdAt: now,
      access: input.session.access,
    }).pipe(Effect.mapError(invalidPlanning));
    yield* authorizePlanning(session, context, "update");
    return yield* workflows.beginPlanning({ session, command: input.command, context, now });
  });
  const getPlanning = Effect.fn("DevelopmentPlanService.getPlanning")(function* (
    input: { readonly sessionId: PlanningSessionV1["id"] },
    context: PlanningContext,
  ) {
    yield* check(context, "read", []);
    const session = yield* workflows.getPlanning(input);
    yield* authorizePlanning(session, context, "read");
    return session;
  });
  const control = Effect.fn("DevelopmentPlanService.controlPlanning")(function* (
    input: ControlInput,
    context: PlanningContext,
    action: "pause" | "resume" | "stop",
  ) {
    yield* check(context, "update", []);
    const session = yield* workflows.getPlanning({ sessionId: input.sessionId });
    yield* authorizePlanning(session, context, "update");
    const now = DateTime.formatIso(yield* DateTime.now);
    return yield* workflows.controlPlanning({ ...input, context, action, now });
  });
  const pausePlanning = Effect.fn("DevelopmentPlanService.pausePlanning")(
    (input: ControlInput, context: PlanningContext) => control(input, context, "pause"),
  );
  const resumePlanning = Effect.fn("DevelopmentPlanService.resumePlanning")(
    (input: ControlInput, context: PlanningContext) => control(input, context, "resume"),
  );
  const stopPlanning = Effect.fn("DevelopmentPlanService.stopPlanning")(
    (input: ControlInput, context: PlanningContext) => control(input, context, "stop"),
  );
  const beginPass = Effect.fn("DevelopmentPlanService.beginPass")(function* (
    input: ControlInput & {
      readonly pass: PlanningPassV1;
      readonly operation: WorkflowOperationV1;
    },
    context: PlanningContext,
  ) {
    yield* check(context, "update", []);
    const session = yield* workflows.getPlanning({ sessionId: input.sessionId });
    const pass = yield* decodePlanningPassV1(input.pass).pipe(Effect.mapError(invalidPlanning));
    yield* authorizePlanning(session, context, "update", [pass]);
    yield* decodeWorkflowOperationV1(input.operation).pipe(Effect.mapError(invalidPlanning));
    const operation = yield* decodeWorkflowOperationV1({
      schemaVersion: 1,
      id: pass.operationId,
      owner: { kind: "planning_session", planningSessionId: session.id },
      kind: "planning_pass",
      parameters: {
        kind: "planning_pass",
        passId: pass.id,
        binding: pass.binding,
        role: pass.role,
        provider: pass.provider,
        limits: session.limits,
        inputManifestHash: WorkflowCanonicalHash.tagged(
          "t3.planning-input-manifest.v1",
          pass.inputManifest,
        ),
      },
      fingerprint: "0".repeat(64),
      origin: session.origin,
      destination: context.destination,
      controlGeneration: session.controlGeneration,
      status: "pending",
      access: pass.access,
    }).pipe(Effect.mapError(invalidPlanning));
    const now = DateTime.formatIso(yield* DateTime.now);
    return yield* workflows.admitPlanningPass({
      sessionId: input.sessionId,
      expectedVersion: input.expectedVersion,
      pass,
      operation: { ...operation, fingerprint: WorkflowCanonicalHash.operation(operation) },
      context,
      command: input.command,
      now,
    });
  });
  const settlePass = Effect.fn("DevelopmentPlanService.settlePass")(function* (input: {
    readonly sessionId: PlanningSessionV1["id"];
    readonly passId: PlanningPassV1["id"];
    readonly operationId: PlanningPassV1["operationId"];
    readonly expectedVersion: number;
    readonly native: NonNullable<PlanningPassV1["native"]>;
    readonly report: NonNullable<PlanningPassV1["report"]>;
  }) {
    const observation = yield* observer.observe({
      sessionId: input.sessionId,
      passId: input.passId,
      operationId: input.operationId,
      native: input.native,
    });
    if (observation.kind === "unknown")
      return yield* new WorkflowStore.PlanningControlError({ reason: "observation-unknown" });
    return yield* workflows.settlePlanningPass({
      sessionId: input.sessionId,
      passId: input.passId,
      operationId: input.operationId,
      expectedVersion: input.expectedVersion,
      report: input.report,
      observation,
    });
  });
  const recordDisposition = Effect.fn("DevelopmentPlanService.recordDisposition")(function* (
    input: Omit<
      Parameters<WorkflowStore.WorkflowRunStore["Service"]["recordPlanningDisposition"]>[0],
      "context"
    >,
    context: PlanningContext,
  ) {
    yield* check(context, "update", []);
    const session = yield* workflows.getPlanning({ sessionId: input.sessionId });
    const review = yield* workflows.getPlanningPass({ passId: input.reviewPassId });
    const repair = yield* workflows.getPlanningPass({ passId: input.repairPassId });
    yield* authorizePlanning(session, context, "update", [review, repair]);
    return yield* workflows.recordPlanningDisposition({ ...input, context });
  });
  return {
    create,
    import: importPlan,
    export: exportPlan,
    get,
    getObject,
    list,
    updateDraft,
    linkThread,
    unlinkThread,
    validateCoverage,
    beginPlanning,
    getPlanning,
    pausePlanning,
    resumePlanning,
    stopPlanning,
    beginPass,
    settlePass,
    recordDisposition,
  };
});
export class DevelopmentPlanService extends Context.Service<
  DevelopmentPlanService,
  Effect.Success<typeof make>
>()("t3/developmentPlans/DevelopmentPlanService") {}
export const layer = Layer.effect(DevelopmentPlanService, make);
