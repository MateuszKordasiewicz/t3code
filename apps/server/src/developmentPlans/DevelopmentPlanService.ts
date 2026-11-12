// @effect-diagnostics deterministicKeys:off -- The authorization port key is fixed by the integration contract.
import type {
  DevelopmentPlanRevisionV1,
  PlanningPassV1,
  RevisionEnvelope,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Store from "./DevelopmentPlanStore.ts";
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

const make = Effect.gen(function* () {
  const store = yield* Store.DevelopmentPlanStore;
  const authorization = yield* DevelopmentPlanAuthorization;
  const passReader = yield* DevelopmentPlanPassReader;
  const attestation = yield* DevelopmentPlanReviewAttestation;
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
  return { create, get, getObject, list, updateDraft, linkThread, unlinkThread, validateCoverage };
});
export class DevelopmentPlanService extends Context.Service<
  DevelopmentPlanService,
  Effect.Success<typeof make>
>()("t3/developmentPlans/DevelopmentPlanService") {}
export const layer = Layer.effect(DevelopmentPlanService, make);
