// @effect-diagnostics deterministicKeys:off -- The authorization port key is fixed by the integration contract.
import {
  decodeDevelopmentPlanDocumentV2,
  upgradeDevelopmentPlanDocumentV1,
  OrchestrationV2PlanArtifact,
  DevelopmentPlanRevisionV1,
  type PlanningPassV1,
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
