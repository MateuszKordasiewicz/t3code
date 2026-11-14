import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";
import * as Derivation from "../workflowRuns/WorkflowNativeLaunchDerivation.ts";
import * as Store from "../workflowRuns/WorkflowRunStore.ts";
import * as PreparationJob from "../preparation/PreparationJob.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";

export class WorkflowNativeLaunchError extends Schema.TaggedError<WorkflowNativeLaunchError>()(
  "WorkflowNativeLaunchError",
  {
    stage: Schema.Literals([
      "construction",
      "input",
      "boundary",
      "derive",
      "intent",
      "claim-command",
      "message-command",
      "binding",
      "finalization",
      "operation",
      "preparation",
    ]),
    reason: Schema.Literals([
      "invalid",
      "stale",
      "denied",
      "integrity",
      "reconciliation_required",
      "phase_failed",
      "unavailable",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

type Selection = {
  readonly intentId: string;
  readonly expectedFingerprint: string;
  readonly expectedOwnerVersion: number;
  readonly expectedGeneration: number;
};
type Coordinator = { readonly bootId: string; readonly workerId: string };

function ownRecord<A extends object>(value: A, keys: string) {
  WorkflowCanonicalHash.assertCanonical(value);
  if (Object.keys(value).sort().join() !== keys) throw new Error("Invalid native launch selection");
  return Object.freeze(structuredClone(value));
}
function validId(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0 && value.isWellFormed();
}
const atStage = <A, E, R>(
  stage: WorkflowNativeLaunchError["stage"],
  work: Effect.Effect<A, E, R>,
) =>
  work.pipe(
    Effect.mapError(
      (cause) => new WorkflowNativeLaunchError({ stage, reason: "phase_failed", cause }),
    ),
  );

const makeOwned = Effect.fn("WorkflowNativeLaunchService.make")(function* (
  coordinator: Coordinator,
) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.WorkflowRunStore;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* EventSink.EventSinkV2;
  const derivation = yield* Derivation.make;
  const replay = sink.replayNativeCommand;
  if (!replay)
    return yield* new WorkflowNativeLaunchError({ stage: "construction", reason: "unavailable" });
  const claimReserved = Effect.fn("WorkflowNativeLaunchService.claimReservedIntent")(function* (
    input: Selection,
  ) {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* new WorkflowNativeLaunchError({ stage: "boundary", reason: "invalid" });
    const derived = yield* atStage(
      "derive",
      sql.withTransaction(derivation.deriveInTransaction({ intentId: input.intentId })),
    );
    const selected = derived.reservation;
    if (
      selected.fingerprint !== input.expectedFingerprint ||
      selected.generation !== input.expectedGeneration
    )
      return yield* new WorkflowNativeLaunchError({ stage: "input", reason: "stale" });
    if (selected.state !== "reserved")
      return yield* new WorkflowNativeLaunchError({
        stage: "intent",
        reason: "reconciliation_required",
      });
    if (selected.held || selected.material.version !== 2)
      return yield* new WorkflowNativeLaunchError({ stage: "intent", reason: "denied" });
    if (derived.messageCommand.titleSeed !== undefined)
      return yield* new WorkflowNativeLaunchError({ stage: "intent", reason: "unavailable" });
    const history = yield* atStage(
      "boundary",
      replay({ command: derived.claimCommand, hint: { intentId: input.intentId, phase: "claim" } }),
    );
    if (history.kind !== "absent")
      return yield* new WorkflowNativeLaunchError({
        stage: "boundary",
        reason: "reconciliation_required",
      });
    const intent = yield* atStage("intent", store.claimNativeIntent({ ...input, ...coordinator }));
    if (intent.kind !== "claimed")
      return yield* new WorkflowNativeLaunchError({
        stage: "intent",
        reason: intent.kind === "stale" ? "stale" : "denied",
      });
    if (
      intent.claim.intentId !== input.intentId ||
      intent.claim.fingerprint !== input.expectedFingerprint ||
      intent.claim.generation !== input.expectedGeneration ||
      intent.claim.bootId !== coordinator.bootId ||
      intent.claim.workerId !== coordinator.workerId ||
      WorkflowCanonicalHash.tagged("owner", intent.claim.owner) !==
        WorkflowCanonicalHash.tagged("owner", selected.owner)
    )
      return yield* new WorkflowNativeLaunchError({ stage: "intent", reason: "integrity" });
    yield* atStage(
      "claim-command",
      orchestrator.dispatch(derived.claimCommand, { intentId: input.intentId, phase: "claim" }),
    );
    yield* atStage(
      "message-command",
      orchestrator.dispatch(derived.messageCommand, { intentId: input.intentId, phase: "message" }),
    );
    const bindingInput = {
      intentId: input.intentId,
      expectedFingerprint: input.expectedFingerprint,
      expectedGeneration: input.expectedGeneration,
    };
    const bound = yield* atStage("binding", store.bindNativeIntent(bindingInput));
    if (bound.kind !== "bound" || bound.state !== "dispatching")
      return yield* new WorkflowNativeLaunchError({
        stage: "binding",
        reason: "reconciliation_required",
      });
    const finalized = yield* atStage(
      "finalization",
      selected.owner.kind === "workflow_run"
        ? store.finalizeWorkflowNativeIntent(bindingInput)
        : store.finalizePlanningNativeIntent(bindingInput),
    );
    if (finalized.kind !== "finalized")
      return yield* new WorkflowNativeLaunchError({
        stage: "finalization",
        reason: "reconciliation_required",
      });
    const result = yield* atStage(
      "operation",
      store.claim({
        operationId: finalized.operationId,
        expectedOwnerVersion: input.expectedOwnerVersion,
        expectedGeneration: input.expectedGeneration,
        ...coordinator,
        now: DateTime.formatIso(yield* DateTime.now),
        trustedEligibility: true,
      }),
    );
    if (result.kind !== "claimed")
      return yield* new WorkflowNativeLaunchError({
        stage: "operation",
        reason: result.kind === "stale" ? "stale" : "denied",
      });
    const operation = result.operation;
    if (
      operation.id !== finalized.operationId ||
      operation.status !== "claimed" ||
      operation.controlGeneration !== input.expectedGeneration ||
      operation.claim?.generation !== input.expectedGeneration ||
      operation.claim.bootId !== coordinator.bootId ||
      operation.claim.workerId !== coordinator.workerId ||
      WorkflowCanonicalHash.tagged("owner", operation.owner) !==
        WorkflowCanonicalHash.tagged("owner", intent.claim.owner)
    )
      return yield* new WorkflowNativeLaunchError({ stage: "operation", reason: "integrity" });
    const executionClaim = Object.freeze({
      operationId: operation.id,
      expectedFingerprint: operation.fingerprint,
      expectedGeneration: operation.controlGeneration,
      bootId: operation.claim.bootId,
      workerId: operation.claim.workerId,
    });
    const preparation = yield* atStage(
      "preparation",
      store.beginClaimedNativePreparation(executionClaim),
    );
    if (preparation.kind !== "acquired")
      return yield* new WorkflowNativeLaunchError({
        stage: "preparation",
        reason: "reconciliation_required",
      });
    const owner = yield* atStage("preparation", store.consumeNativePreparationOwner(preparation));
    return Object.freeze({
      kind: "claimed" as const,
      native: Object.freeze({ threadId: selected.threadId, runId: bound.nativeRunId }),
      executionClaim,
      preparation: Object.freeze({
        owner,
        job: preparation.job,
        selection: PreparationJob.selection(preparation.job),
      }),
    });
  });
  return {
    claimReservedIntent(input: Selection) {
      try {
        const owned = ownRecord(
          input,
          "expectedFingerprint,expectedGeneration,expectedOwnerVersion,intentId",
        );
        if (
          !validId(owned.intentId) ||
          typeof owned.expectedFingerprint !== "string" ||
          owned.expectedFingerprint.length !== 64 ||
          !/^[0-9a-f]+$/.test(owned.expectedFingerprint) ||
          ![owned.expectedOwnerVersion, owned.expectedGeneration].every(
            (value) => Number.isSafeInteger(value) && value >= 0,
          )
        )
          throw new Error();
        return claimReserved(owned);
      } catch {
        return Effect.fail(new WorkflowNativeLaunchError({ stage: "input", reason: "invalid" }));
      }
    },
  };
});

export const make = (coordinator: Coordinator) => {
  try {
    const owned = ownRecord(coordinator, "bootId,workerId");
    if (!validId(owned.bootId) || !validId(owned.workerId)) throw new Error();
    return makeOwned(owned);
  } catch {
    return Effect.fail(new WorkflowNativeLaunchError({ stage: "construction", reason: "invalid" }));
  }
};

export class WorkflowNativeLaunchService extends Context.Service<
  WorkflowNativeLaunchService,
  Effect.Success<ReturnType<typeof make>>
>()("t3/orchestration-v2/WorkflowNativeLaunchService") {}
