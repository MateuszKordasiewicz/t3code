import type { WorkflowOperationV1 } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Store from "./WorkflowRunStore.ts";

type Identity = {
  readonly owner: WorkflowOperationV1["owner"];
  readonly fingerprint: string;
  readonly native: { readonly threadId: string; readonly runId: string } | undefined;
};
type Observation =
  | { readonly kind: "unknown" | "missing" }
  | ({ readonly kind: "active" } & Identity)
  | ({
      readonly kind: "settled";
      readonly outcome: NonNullable<WorkflowOperationV1["outcome"]>;
    } & Identity);
export class WorkflowRunObservation extends Context.Service<
  WorkflowRunObservation,
  {
    readonly observe: (
      identity: Identity & { readonly operation: WorkflowOperationV1 },
    ) => Effect.Effect<Observation>;
  }
>()("t3/workflowRuns/WorkflowRunRecovery/WorkflowRunObservation") {}

const sameOwner = (a: Identity["owner"], b: Identity["owner"]) =>
  a.kind === "workflow_run"
    ? b.kind === "workflow_run" && a.workflowRunId === b.workflowRunId
    : b.kind === "planning_session" && a.planningSessionId === b.planningSessionId;
const make = Effect.gen(function* () {
  const store = yield* Store.WorkflowRunStore;
  const port = yield* WorkflowRunObservation;
  const reconcile = Effect.fn("WorkflowRunRecovery.reconcile")(function* ({
    operationId,
  }: {
    readonly operationId: string;
  }) {
    const snapshot = yield* store.readRecovery(operationId);
    if (
      !snapshot ||
      !snapshot.operation.claim ||
      (snapshot.operation.status !== "claimed" && snapshot.operation.status !== "unknown")
    )
      return { kind: "unchanged" as const };
    const { operation, owner, native } = snapshot;
    const observation = yield* port.observe({
      operation,
      owner: operation.owner,
      fingerprint: operation.fingerprint,
      native,
    });
    const matching =
      (observation.kind === "active" || observation.kind === "settled") &&
      sameOwner(observation.owner, operation.owner) &&
      observation.fingerprint === operation.fingerprint &&
      observation.native?.threadId === native?.threadId &&
      observation.native?.runId === native?.runId &&
      (operation.parameters.kind !== "planning_pass" || native !== undefined);
    const kind =
      matching && (observation.kind === "active" || observation.kind === "settled")
        ? observation.kind
        : "unknown";
    const result = yield* store.compareAndApplyRecovery({
      operationId,
      expectedOwnerVersion: owner.recordVersion,
      expectedGeneration: operation.controlGeneration,
      expectedFingerprint: operation.fingerprint,
      observed: {
        operation,
        ownerGeneration: owner.controlGeneration,
        native,
        kind,
        ...(kind === "settled" && observation.kind === "settled"
          ? { outcome: observation.outcome }
          : {}),
      },
    });
    if (result.kind === "stale") return { kind: "stale" as const };
    if (result.kind === "replay")
      return { kind: kind === "active" ? ("active" as const) : ("unchanged" as const) };
    return { kind };
  });
  return { reconcile };
});
export class WorkflowRunRecovery extends Context.Service<
  WorkflowRunRecovery,
  Effect.Success<typeof make>
>()("t3/workflowRuns/WorkflowRunRecovery") {}
export const layer = Layer.effect(WorkflowRunRecovery, make);
