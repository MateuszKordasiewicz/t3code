import {
  EnvironmentId,
  ProjectId,
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
  WorkflowRunV1,
  OrchestrationV2ProviderThreadJson,
  OrchestrationV2ProviderSessionJson,
  OrchestrationV2SubagentJson,
  OrchestrationV2RunJson,
  OrchestrationV2RunAttemptJson,
  OrchestrationV2ExecutionNodeJson,
  OrchestrationV2CheckpointScopeJson,
} from "@t3tools/contracts";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Resource identities use Node's cryptographic UUID source.
import * as NodeCrypto from "node:crypto";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as Context from "effect/Context";
import * as Digest from "effect/crypto/Digest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Path from "effect/Path";
import { stableStringify } from "@t3tools/shared/relaySigning";
import type { ServerConfig } from "../config.ts";
import * as NativeProtocol from "./WorkflowNativeCommandProtocol.ts";
import * as PreparationJob from "../preparation/PreparationJob.ts";
import * as PreparationResource from "../preparation/PreparationProcessResource.ts";
import * as PreparationOwner from "../preparation/PreparationProcessOwner.ts";
import * as PreparationGate from "../preparation/PreparationProcessGate.ts";
import * as PreparationNode from "../preparation/PreparationNodeChild.ts";
import {
  decodeSubmissionTransportData,
  decodeSubmissionAttemptResourceData,
} from "../orchestration-v2/ProviderSubmissionCodec.ts";
import type { PlanningObservation } from "../developmentPlans/PlanningPassObservation.ts";
import { DevelopmentPlanHash } from "../developmentPlans/DevelopmentPlanHash.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
import {
  decodeNativeReservationLaunch,
  decodeNativeReservationMaterial,
  decodeNativeReservationAuthority,
} from "./WorkflowNativeReservationMaterial.ts";
import { WorkflowRunPolicy } from "./WorkflowRunPolicy.ts";
import * as ExecutionAuthority from "./WorkflowExecutionAuthority.ts";
import type { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import type { ManagedPolicy } from "../policy/ManagedPolicy.ts";

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
const attemptFacts = <
  A extends {
    readonly item_id: string;
    readonly status: WorkflowOperationV1["status"];
    readonly outcome: string | null;
  },
>(
  attempts: readonly A[],
  targetItemId: string | undefined,
) => {
  const latest =
    targetItemId === undefined
      ? attempts.at(-1)
      : attempts.findLast((attempt) => attempt.item_id === targetItemId);
  const state: NonNullable<Facts["currentAttemptState"]> = attempts.some(
    (attempt) => attempt.status === "unknown",
  )
    ? "unknown"
    : attempts.some((attempt) => attempt.status === "pending" || attempt.status === "claimed")
      ? "active"
      : !latest
        ? "none"
        : latest.status === "settled" && latest.outcome === "succeeded"
          ? "settled_succeeded"
          : "settled_failed";
  return { latest, state };
};
const encode = (value: unknown) => JSON.stringify(value);
const parse = (value: string): unknown => JSON.parse(value);
const decodePreparationWorkflowOwner = Schema.decodeEffect(WorkflowOperationV1.fields.owner);
const decodeNativeFinalizationRun = Schema.decodeEffect(
  Schema.fromJsonString(OrchestrationV2RunJson),
);
const decodeNativeFinalizationAttempt = Schema.decodeEffect(
  Schema.fromJsonString(OrchestrationV2RunAttemptJson),
);
const decodeNativeFinalizationNode = Schema.decodeEffect(
  Schema.fromJsonString(OrchestrationV2ExecutionNodeJson),
);
const decodeNativeCheckpointScope = Schema.decodeEffect(
  Schema.fromJsonString(OrchestrationV2CheckpointScopeJson),
);
const decodeRecoveryProviderThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderThreadJson),
);

const decodeSubmissionSession = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2ProviderSessionJson),
);
const decodeSubmissionSubagent = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2SubagentJson),
);

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

type Material = Effect.Success<ReturnType<typeof decodeNativeReservationMaterial>>["material"];
type CurrentAuthority = Effect.Success<
  ReturnType<typeof ExecutionAuthority.makeCapturedWorkflowExecutionAuthority>
>;
type NativeCoordinator = { readonly bootId: string; readonly workerId: string };
type PreparationActor = {
  job: PreparationJob.Job;
  readonly resources: Map<string, PreparationResource.Resource>;
};
type NativeBindingRuntime = {
  readonly scope: Scope.Scope;
  readonly agentScope: typeof AgentScope.Service;
  readonly nodeChild: typeof PreparationNode.PreparationNodeChild.Service;
  readonly path: Path.Path;
  readonly hostEnvironment: NodeJS.ProcessEnv;
  readonly hostCwd: string;
  readonly environmentId: string;
  readonly protocol: Effect.Success<ReturnType<typeof NativeProtocol.makeNativeCommandProtocol>>;
  readonly coordinator: NativeCoordinator;
};
// Public constructors own the construction span; this helper is shared by their two capability sets.
const makeStore = Effect.fnUntraced(function* (
  beforePublish: Effect.Effect<void> = Effect.void,
  currentAuthority?: CurrentAuthority,
  nativeBinding?: NativeBindingRuntime,
) {
  const changes = yield* Effect.acquireRelease(PubSub.sliding<void>(1), PubSub.shutdown);
  const notify = beforePublish.pipe(
    Effect.andThen(PubSub.publish(changes, undefined)),
    Effect.asVoid,
  );
  const sql = yield* SqlClient.SqlClient;
  const preparationActors = new WeakSet<PreparationActor>();
  const preparationAcquisitions = new WeakMap<
    object,
    { readonly job: PreparationJob.Job; consumed: boolean }
  >();
  const validateActorInventory = Effect.fn("WorkflowRunStore.validateActorInventory")(function* (
    actor: PreparationActor,
  ) {
    if (!preparationActors.has(actor)) return yield* integrity();
    const selected = yield* readPreparationJob(actor.job.input.jobId);
    if (!preparationMatches(selected, PreparationJob.selection(actor.job))) return yield* stale();
    let cursor = "";
    let count = 0;
    for (;;) {
      const rows =
        yield* sql`SELECT * FROM orchestration_v2_preparation_process_resources WHERE job_id=${actor.job.input.jobId} AND resource_id>${cursor} ORDER BY resource_id LIMIT 128`;
      for (const row of rows) {
        const resource = yield* Effect.try({
          try: () => PreparationResource.decodeRow(row),
          catch: integrity,
        });
        const original = actor.resources.get(resource.resourceId);
        if (!original || !sameCustodyValue(original, resource)) return yield* stale();
        cursor = resource.resourceId;
        count++;
      }
      if (rows.length < 128) break;
    }
    if (count !== actor.resources.size) return yield* integrity();
  });
  const checkWorkflowAuthority = Effect.fn("WorkflowRunStore.checkNativeAuthority")(function* (
    material: Material,
    operation: WorkflowOperationV1,
    owner: Pick<WorkflowRunV1 | S, "origin" | "access">,
    requireNativeThread = false,
  ) {
    if (material.version !== 2) return true;
    if (
      !currentAuthority ||
      owner.origin.kind !== "human" ||
      material.authority.subject !== owner.origin.id ||
      material.authority.environmentId !== operation.destination.environmentId
    )
      return false;
    const destination = {
      environmentId: EnvironmentId.make(operation.destination.environmentId),
      projectId: ProjectId.make(operation.destination.projectId),
      ...(requireNativeThread || material.launch.reuseExistingThread === true
        ? { threadId: material.launch.threadId }
        : {}),
    };
    return yield* currentAuthority
      .checkNativeAgentInTransaction({
        reference: material.authority,
        destination,
        origins: [...owner.access.origins, ...operation.access.origins],
        runtimeMode: material.launch.runtimeMode,
      })
      .pipe(
        Effect.as(true),
        Effect.catch((error) =>
          error.reason === "persistence"
            ? Effect.fail(new WorkflowRunStoreError({ reason: "persistence" }))
            : Effect.succeed(false),
        ),
      );
  });

  // Shares the reservation transaction's span and writer connection.
  const runningNativeEffect = Effect.fnUntraced(function* (threadId: string) {
    const rows =
      yield* sql`SELECT 1 FROM orchestration_v2_effect_outbox WHERE thread_id = ${threadId} AND status = 'running' AND effect_type IN ('provider-turn.start', 'provider-turn.restart', 'provider-runtime.continue') LIMIT 1`;
    return rows.length > 0;
  });
  const readPreparationJob = Effect.fn("WorkflowRunStore.readPreparationJob")(function* (
    jobId: string,
  ) {
    const rows = yield* sql`SELECT * FROM orchestration_v2_preparation_jobs WHERE job_id=${jobId}`;
    if (rows.length !== 1) return yield* integrity();
    return yield* Effect.try({ try: () => PreparationJob.decodeRow(rows[0]), catch: integrity });
  });
  const preparationMatches = (job: PreparationJob.Job, selected: PreparationJob.Selection) =>
    stableStringify(PreparationJob.selection(job)) === stableStringify(selected);
  const readPreparationParent = Effect.fn("WorkflowRunStore.readPreparationParent")(function* (
    input: PreparationJob.Input,
  ) {
    if (input.parentSelection === null) return null;
    const parent = yield* readPreparationJob(input.parentSelection.jobId);
    if (
      Object.hasOwn(input, "nativeSelection") &&
      (parent.identityVersion !== 2 ||
        stableStringify(input.nativeSelection) !== stableStringify(parent.input.nativeSelection))
    )
      return yield* integrity();
    const selected = input.parentSelection;
    if (
      parent.input.kind !== "launch" ||
      parent.input.environmentId !== input.environmentId ||
      parent.input.projectId !== input.projectId ||
      parent.input.threadId !== input.threadId ||
      parent.input.serverBootId !== input.serverBootId ||
      parent.input.entryKey !== selected.entryKey ||
      parent.requestIdentityHash !== selected.requestIdentityHash ||
      parent.input.serverBootId !== selected.serverBootId ||
      parent.recordVersion < selected.expectedVersion
    )
      return yield* integrity();
    return parent;
  });
  const readPreparationCustody = Effect.fn("WorkflowRunStore.readPreparationCustody")(function* (
    environmentId: string,
    threadId: string,
    actor?: PreparationActor,
  ) {
    let cursor = "";
    let held = false;
    for (;;) {
      const rows =
        yield* sql`SELECT * FROM orchestration_v2_preparation_jobs WHERE environment_id=${environmentId} AND thread_id=${threadId} AND job_id>${cursor} ORDER BY job_id LIMIT 128`;
      for (const row of rows) {
        const job = yield* Effect.try({
          try: () => PreparationJob.decodeRow(row),
          catch: integrity,
        });
        yield* readPreparationParent(job.input);
        if (actor && job.input.jobId === actor.job.input.jobId) {
          yield* validateActorInventory(actor);
          continue;
        }
        if (
          job.identityVersion !== 2 ||
          job.state !== "settled" ||
          job.processExposed ||
          job.settlement?.kind !== "code_only"
        )
          held = true;
      }
      if (rows.length < 128) break;
      cursor = rows.at(-1)!.job_id as string;
    }
    return held;
  });
  const nativeFromLaunch = (launch: Material["launch"]): PreparationJob.NativeSelection | null =>
    launch.importedNativeThread === undefined
      ? null
      : {
          driver: launch.importedNativeThread.ref.driver,
          providerInstanceId: launch.modelSelection.instanceId,
          nativeId: launch.importedNativeThread.ref.nativeId,
        };
  const readNativePreparationCustody = Effect.fn("WorkflowRunStore.readNativePreparationCustody")(
    function* (
      environmentId: string,
      native: PreparationJob.NativeSelection,
      actor?: PreparationActor,
    ) {
      let cursor = "";
      let held = false;
      for (;;) {
        const rows =
          yield* sql`SELECT * FROM orchestration_v2_preparation_jobs WHERE environment_id=${environmentId} AND CASE WHEN json_valid(request_identity_json) THEN json_extract(request_identity_json,'$.nativeSelection.driver') END=${native.driver} AND CASE WHEN json_valid(request_identity_json) THEN json_extract(request_identity_json,'$.nativeSelection.providerInstanceId') END=${native.providerInstanceId} AND CASE WHEN json_valid(request_identity_json) THEN json_extract(request_identity_json,'$.nativeSelection.nativeId') END=${native.nativeId} AND job_id>${cursor} ORDER BY job_id LIMIT 128`;
        for (const row of rows) {
          const job = yield* Effect.try({
            try: () => PreparationJob.decodeRow(row),
            catch: integrity,
          });
          if (
            (job.identityVersion !== 2 && job.identityVersion !== 3) ||
            job.input.environmentId !== environmentId ||
            !sameCustodyValue(job.input.nativeSelection, native)
          )
            return yield* integrity();
          yield* readPreparationParent(job.input);
          if (actor && job.input.jobId === actor.job.input.jobId) {
            yield* validateActorInventory(actor);
            continue;
          }
          if (
            job.identityVersion !== 2 ||
            job.state !== "settled" ||
            job.processExposed ||
            job.settlement?.kind !== "code_only"
          )
            held = true;
        }
        if (rows.length < 128) break;
        cursor = rows.at(-1)!.job_id as string;
      }
      return held;
    },
  );
  // Public readers own the span; both purposes share the captured projection traversal.
  const walkResourceCustodyInTransaction = Effect.fnUntraced(
    function* (
      input:
        | {
            purpose: "physical";
            environmentId: string;
            threadId: string;
            expectedOwner: WorkflowOperationV1["owner"];
            nativeSelection?: PreparationJob.NativeSelection | null;
          }
        | {
            purpose: "workflow";
            environmentId?: string;
            nativeSelection?: PreparationJob.NativeSelection | null;
            threadId: string;
            providerSessionIds: readonly string[];
            providerThreadIds?: readonly string[];
          },
      actor?: PreparationActor,
    ) {
      const validatedThreads = new Map<
        string,
        Effect.Success<ReturnType<typeof readThreadCustodyInTransaction>>
      >();
      const environmentId = input.environmentId ?? "";
      const threadId = input.threadId;
      if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
        return yield* integrity();
      const validId = (value: unknown): value is string =>
        typeof value === "string" && value.trim().length > 0 && value.isWellFormed();
      if (!validId(threadId)) return yield* integrity();
      if (input.purpose === "physical") {
        if (!validId(environmentId)) return yield* integrity();
      } else if (
        !Array.isArray(input.providerSessionIds) ||
        !input.providerSessionIds.every(validId) ||
        (input.providerThreadIds !== undefined &&
          (!Array.isArray(input.providerThreadIds) || !input.providerThreadIds.every(validId)))
      )
        return yield* integrity();
      type Row = Readonly<Record<string, unknown>>;
      type Kind =
        | "thread"
        | "session"
        | "provider"
        | "provider-reference"
        | "node"
        | "parent"
        | "subagent";
      type Native = { driver: string; instance: string; id: string };
      type Work = { kind: Kind; id: string } | { kind: "native"; native: Native };
      const work: Work[] = [];
      const queued = new Set<string>();
      const parents = new Map<string, Row>();
      const attempts = new Set<string>();
      const sessionIdentities = new Map<string, { driver: string; instance: string }>();
      const queue = (kind: Kind, id: unknown) => {
        if (!validId(id)) return false;
        const key = `${kind}\0${id}`;
        if (!queued.has(key)) {
          queued.add(key);
          work.push({ kind, id });
        }
        return true;
      };
      const queueNullable = (kind: Kind, id: unknown) => id === null || queue(kind, id);
      const queueSession = (id: unknown, identity?: { driver: string; instance: string }) => {
        if (!validId(id)) return false;
        if (identity) {
          if (!validId(identity.driver) || !validId(identity.instance)) return false;
          const previous = sessionIdentities.get(id);
          if (
            previous &&
            (previous.driver !== identity.driver || previous.instance !== identity.instance)
          )
            return false;
          sessionIdentities.set(id, identity);
        }
        return queue("session", id);
      };
      const queueNative = (native: Native) => {
        if (!validId(native.driver) || !validId(native.instance) || !validId(native.id))
          return false;
        const key = `native\0${JSON.stringify([native.driver, native.instance, native.id])}`;
        if (!queued.has(key)) {
          queued.add(key);
          work.push({ kind: "native", native });
        }
        return true;
      };
      const matches = (row: Row, shadows: Row) =>
        Object.entries(shadows).every(([key, value]) => row[key] === value);
      const sameTime = (raw: unknown, value: DateTime.DateTime | null) => {
        if (raw === null) return value === null;
        if (typeof raw !== "string" || !isWorkflowTimestamp(raw) || value === null) return false;
        const parsed = DateTime.make(raw);
        return (
          Option.isSome(parsed) &&
          DateTime.toEpochMillis(parsed.value) === DateTime.toEpochMillis(value)
        );
      };
      // Per-row helpers share the outer reader's span and captured writer connection.
      const parseAttempt = Effect.fnUntraced(function* (raw: Row, parent: Row) {
        const row = yield* Effect.try({
          try: () => decodeSubmissionAttemptResourceData(raw),
          catch: integrity,
        });
        if (
          !matches(row, {
            transport_id: parent.transport_id,
            environment_id: parent.environment_id,
            driver: parent.driver,
            provider_instance_id: parent.provider_instance_id,
            provider_session_id: parent.provider_session_id,
          })
        )
          return yield* integrity();
        attempts.add(row.attempt_id as string);
        return row;
      });
      // Repeated history matches share one validation pass inside this reader.
      const inspectParent = Effect.fnUntraced(function* (raw: Row) {
        const row = yield* Effect.try({
          try: () => decodeSubmissionTransportData(raw),
          catch: integrity,
        });
        if (row.environment_id !== environmentId) return yield* integrity();
        const id = row.transport_id as string;
        if (parents.has(id)) return parents.get(id)!.settlement_json === null;
        parents.set(id, row);
        if (row.settlement_json === null) return true;
        let cursor = "";
        for (;;) {
          const children =
            yield* sql`SELECT * FROM orchestration_v2_submission_attempts WHERE transport_id=${id} AND attempt_id>${cursor} ORDER BY attempt_id LIMIT 128`;
          for (const child of children) yield* parseAttempt(child, row);
          if (children.length < 128) break;
          const last = children.at(-1)!.attempt_id;
          if (!validId(last) || last === cursor) return yield* integrity();
          cursor = last;
        }
        return false;
      });
      // All historical children must agree with their immutable physical parent.
      const inspectAttempt = Effect.fnUntraced(function* (raw: Row) {
        if (validId(raw.attempt_id) && attempts.has(raw.attempt_id)) return false;
        const row = yield* Effect.try({
          try: () => decodeSubmissionAttemptResourceData(raw),
          catch: integrity,
        });
        const id = row.transport_id as string;
        let parent = parents.get(id);
        let held = false;
        if (!parent) {
          const found =
            yield* sql`SELECT * FROM orchestration_v2_submission_transports WHERE transport_id=${id}`;
          if (found.length !== 1 || !found[0]) return yield* integrity();
          held = yield* inspectParent(found[0]);
          parent = parents.get(id)!;
        } else held = parent.settlement_json === null;
        yield* parseAttempt(raw, parent);
        return held;
      });
      // Page boundaries bound result arrays without dropping matched released history.
      const transportHistory = Effect.fnUntraced(function* (
        field: "opening_thread_id" | "provider_session_id",
        id: string,
      ) {
        let cursor = "";
        for (;;) {
          const rows =
            yield* sql`SELECT * FROM orchestration_v2_submission_transports WHERE environment_id=${environmentId} AND ${sql(field)}=${id} AND transport_id>${cursor} ORDER BY transport_id LIMIT 128`;
          for (const row of rows) if (yield* inspectParent(row)) return true;
          if (rows.length < 128) return false;
          const last = rows.at(-1)!.transport_id;
          if (!validId(last) || last === cursor) return yield* integrity();
          cursor = last;
        }
      });
      // The exact native prefix uses089; session history is deliberately parent-first.
      const attemptHistory = Effect.fnUntraced(function* (
        target: { field: "thread_id" | "provider_thread_id"; id: string } | Native,
      ) {
        let cursor = "";
        for (;;) {
          const rows =
            "field" in target
              ? yield* sql`SELECT * FROM orchestration_v2_submission_attempts WHERE environment_id=${environmentId} AND ${sql(target.field)}=${target.id} AND attempt_id>${cursor} ORDER BY attempt_id LIMIT 128`
              : yield* sql`SELECT * FROM orchestration_v2_submission_attempts WHERE environment_id=${environmentId} AND driver=${target.driver} AND provider_instance_id=${target.instance} AND native_thread_id=${target.id} AND attempt_id>${cursor} ORDER BY attempt_id LIMIT 128`;
          for (const row of rows) if (yield* inspectAttempt(row)) return true;
          if (rows.length < 128) return false;
          const last = rows.at(-1)!.attempt_id;
          if (!validId(last) || last === cursor) return yield* integrity();
          cursor = last;
        }
      });
      // Projection decoding validates selected rows, not every unrelated legacy record.
      const providerData = Effect.fnUntraced(function* (row: Row) {
        const value = yield* decodeRecoveryProviderThread(row.payload_json, {
          onExcessProperty: "error",
        }).pipe(Effect.mapError(integrity));
        if (
          !matches(row, {
            provider_thread_id: value.id,
            thread_id: value.appThreadId,
            owner_node_id: value.ownerNodeId,
            provider_session_id: value.providerSessionId,
            driver: value.driver,
            provider_instance_id: value.providerInstanceId,
            status: value.status,
            first_run_ordinal: value.firstRunOrdinal,
            last_run_ordinal: value.lastRunOrdinal,
          }) ||
          !sameTime(row.updated_at, value.updatedAt)
        )
          return yield* integrity();
        if (
          !validId(value.id) ||
          !validId(value.providerInstanceId) ||
          (value.nativeThreadRef !== null &&
            (value.nativeThreadRef.driver !== value.driver ||
              (value.nativeThreadRef.nativeId !== null &&
                !validId(value.nativeThreadRef.nativeId))))
        )
          return yield* integrity();
        return value;
      });
      queue("thread", threadId);
      if (
        input.nativeSelection &&
        !queueNative({
          driver: input.nativeSelection.driver,
          instance: input.nativeSelection.providerInstanceId,
          id: input.nativeSelection.nativeId,
        })
      )
        return yield* integrity();
      if (environmentId !== "" && !validId(environmentId)) return yield* integrity();
      if (input.purpose === "workflow") {
        for (const id of input.providerSessionIds) queueSession(id);
        for (const id of input.providerThreadIds ?? []) queue("provider", id);
      }
      for (let index = 0; index < work.length; index++) {
        const selected = work[index]!;
        if (selected.kind === "thread") {
          if (input.purpose === "physical") {
            if (yield* readPreparationCustody(environmentId, selected.id, actor)) return true;
            if (yield* transportHistory("opening_thread_id", selected.id)) return true;
            if (yield* attemptHistory({ field: "thread_id", id: selected.id })) return true;
          }
          const sessions =
            yield* sql`SELECT provider_session_id FROM orchestration_v2_projection_provider_sessions WHERE thread_id=${selected.id} UNION ALL SELECT provider_session_id FROM orchestration_v2_projection_provider_session_bindings WHERE thread_id=${selected.id}`;
          for (const row of sessions)
            if (!queueSession(row.provider_session_id)) return yield* integrity();
          const references =
            yield* sql`SELECT provider_thread_id FROM orchestration_v2_projection_provider_threads WHERE thread_id=${selected.id} UNION ALL SELECT p.provider_thread_id FROM orchestration_v2_projection_provider_threads p JOIN orchestration_v2_projection_nodes n ON n.node_id=p.owner_node_id WHERE n.thread_id=${selected.id}`;
          for (const row of references)
            if (!queue("provider", row.provider_thread_id)) return yield* integrity();
          const children =
            yield* sql`SELECT subagent_id FROM orchestration_v2_projection_subagents WHERE thread_id=${selected.id} OR child_thread_id=${selected.id} UNION ALL SELECT s.subagent_id FROM orchestration_v2_projection_subagents s JOIN orchestration_v2_projection_nodes n ON n.node_id=s.parent_node_id WHERE n.thread_id=${selected.id}`;
          for (const row of children)
            if (!queue("subagent", row.subagent_id)) return yield* integrity();
        } else if (selected.kind === "session") {
          if (input.purpose === "physical") {
            if (yield* transportHistory("provider_session_id", selected.id)) return true;
          }
          const rows =
            yield* sql`SELECT * FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${selected.id}`;
          const peers =
            yield* sql`SELECT thread_id FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id=${selected.id}`;
          const references =
            yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads WHERE provider_session_id=${selected.id}`;
          if (!rows.length) {
            if (peers.length || !references.length) return yield* integrity();
            for (const row of references) {
              const value = yield* providerData(row);
              if (
                input.purpose === "workflow" &&
                value.appThreadId === null &&
                value.ownerNodeId === null
              ) {
                const attribution =
                  yield* sql`SELECT subagent_id FROM orchestration_v2_projection_subagents WHERE provider_thread_id=${value.id} LIMIT 1`;
                if (!attribution.length) return yield* integrity();
              }
              if (
                value.status !== "not_loaded" ||
                value.nativeThreadRef !== null ||
                value.nativeConversationHeadRef !== null ||
                (value.pendingBackgroundTasks?.length ?? 0) !== 0 ||
                !queueSession(selected.id, {
                  driver: value.driver,
                  instance: value.providerInstanceId,
                })
              )
                return yield* integrity();
            }
          } else {
            if (rows.length !== 1 || !rows[0]) return yield* integrity();
            const row = rows[0];
            // The projection writer keeps model only in payload, not the legacy SQL column.
            const value = yield* decodeSubmissionSession(row.payload_json, {
              onExcessProperty: "error",
            }).pipe(Effect.mapError(integrity));
            if (
              !matches(row, {
                provider_session_id: value.id,
                driver: value.driver,
                provider_instance_id: value.providerInstanceId,
                status: value.status,
              }) ||
              !sameTime(row.updated_at, value.updatedAt) ||
              !queueSession(value.id, {
                driver: value.driver,
                instance: value.providerInstanceId,
              }) ||
              !queueNullable("thread", row.thread_id)
            )
              return yield* integrity();
          }
          for (const row of peers) if (!queue("thread", row.thread_id)) return yield* integrity();
          for (const row of references)
            if (!queue("provider", row.provider_thread_id)) return yield* integrity();
        } else if (selected.kind === "provider" || selected.kind === "provider-reference") {
          if (input.purpose === "physical") {
            if (yield* attemptHistory({ field: "provider_thread_id", id: selected.id }))
              return true;
          }
          const rows =
            yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id=${selected.id}`;
          // Historical nodes can retain an ID without a current provider-thread projection.
          if (rows.length === 0 && selected.kind === "provider-reference") continue;
          if (rows.length !== 1 || !rows[0]) return yield* integrity();
          const value = yield* providerData(rows[0]);
          if (
            !queueNullable("thread", value.appThreadId) ||
            !queueNullable("node", value.ownerNodeId)
          )
            return yield* integrity();
          if (
            value.providerSessionId !== null &&
            !queueSession(value.providerSessionId, {
              driver: value.driver,
              instance: value.providerInstanceId,
            })
          )
            return yield* integrity();
          if (
            value.nativeThreadRef !== null &&
            value.nativeThreadRef.nativeId !== null &&
            !queueNative({
              driver: value.driver,
              instance: value.providerInstanceId,
              id: value.nativeThreadRef.nativeId,
            })
          )
            return yield* integrity();
          const children =
            yield* sql`SELECT subagent_id FROM orchestration_v2_projection_subagents WHERE provider_thread_id=${selected.id}`;
          if (
            input.purpose === "workflow" &&
            input.providerThreadIds?.includes(value.id) &&
            value.appThreadId === null &&
            value.ownerNodeId === null &&
            children.length === 0
          )
            return yield* integrity();
          for (const row of children)
            if (!queue("subagent", row.subagent_id)) return yield* integrity();
        } else if (selected.kind === "node") {
          const rows =
            yield* sql`SELECT * FROM orchestration_v2_projection_nodes WHERE node_id=${selected.id}`;
          if (rows.length !== 1 || !rows[0]) return yield* integrity();
          const row = rows[0];
          if (typeof row.payload_json !== "string") return yield* integrity();
          const value = yield* decodeNativeFinalizationNode(row.payload_json, {
            onExcessProperty: "error",
          }).pipe(Effect.mapError(integrity));
          if (
            !matches(row, {
              node_id: value.id,
              thread_id: value.threadId,
              run_id: value.runId,
              parent_node_id: value.parentNodeId,
              root_node_id: value.rootNodeId,
              kind: value.kind,
              status: value.status,
              provider_thread_id: value.providerThreadId,
              provider_turn_id: value.providerTurnId,
            }) ||
            !sameTime(row.started_at, value.startedAt) ||
            !sameTime(row.completed_at, value.completedAt) ||
            !queue("thread", value.threadId) ||
            !queueNullable("provider-reference", value.providerThreadId) ||
            !queueNullable("parent", value.parentNodeId) ||
            !queue("parent", value.rootNodeId)
          )
            return yield* integrity();
          const references =
            yield* sql`SELECT provider_thread_id FROM orchestration_v2_projection_provider_threads WHERE owner_node_id=${selected.id}`;
          for (const row of references)
            if (!queue("provider", row.provider_thread_id)) return yield* integrity();
          const children =
            yield* sql`SELECT subagent_id FROM orchestration_v2_projection_subagents WHERE parent_node_id=${selected.id}`;
          for (const row of children)
            if (!queue("subagent", row.subagent_id)) return yield* integrity();
        } else if (selected.kind === "parent") {
          // Hierarchy IDs can name a node or subagent, and older parent records may be absent.
          const nodes =
            yield* sql`SELECT node_id FROM orchestration_v2_projection_nodes WHERE node_id=${selected.id}`;
          const subagents =
            yield* sql`SELECT subagent_id FROM orchestration_v2_projection_subagents WHERE subagent_id=${selected.id}`;
          if (nodes.length > 1 || subagents.length > 1) return yield* integrity();
          for (const row of nodes) if (!queue("node", row.node_id)) return yield* integrity();
          for (const row of subagents)
            if (!queue("subagent", row.subagent_id)) return yield* integrity();
        } else if (selected.kind === "subagent") {
          const rows =
            yield* sql`SELECT * FROM orchestration_v2_projection_subagents WHERE subagent_id=${selected.id}`;
          if (rows.length !== 1 || !rows[0]) return yield* integrity();
          const row = rows[0];
          const value = yield* decodeSubmissionSubagent(row.payload_json, {
            onExcessProperty: "error",
          }).pipe(Effect.mapError(integrity));
          if (
            !matches(row, {
              subagent_id: value.id,
              thread_id: value.threadId,
              run_id: value.runId,
              parent_node_id: value.parentNodeId,
              provider_thread_id: value.providerThreadId,
              child_thread_id: value.childThreadId,
              driver: value.driver,
              provider_instance_id: value.providerInstanceId,
              origin: value.origin,
              status: value.status,
            }) ||
            !sameTime(row.started_at, value.startedAt) ||
            !sameTime(row.completed_at, value.completedAt) ||
            !sameTime(row.updated_at, value.updatedAt) ||
            !queue("thread", value.threadId) ||
            !queue("parent", value.parentNodeId) ||
            !queueNullable("provider-reference", value.providerThreadId)
          )
            return yield* integrity();
          if (value.remoteChild === undefined) {
            if (!queueNullable("thread", value.childThreadId)) return yield* integrity();
          } else if (value.childThreadId !== null) return yield* integrity();
        } else if (selected.kind === "native") {
          const native = {
            driver: selected.native.driver,
            providerInstanceId: selected.native.instance,
            nativeId: selected.native.id,
          };
          if (input.purpose === "physical") {
            if (yield* readNativePreparationCustody(environmentId, native, actor)) return true;
            if (yield* attemptHistory(selected.native)) return true;
          }
          if (environmentId !== "") {
            let materialCursor = "";
            for (;;) {
              const materials =
                yield* sql`SELECT m.intent_id FROM workflow_native_reservation_material m JOIN workflow_native_launch_intents i ON i.intent_id=m.intent_id WHERE CASE WHEN json_valid(m.material_json) THEN json_extract(m.material_json,'$.launch.importedNativeThread.ref.driver') END=${native.driver} AND CASE WHEN json_valid(m.material_json) THEN json_extract(m.material_json,'$.launch.modelSelection.instanceId') END=${native.providerInstanceId} AND CASE WHEN json_valid(m.material_json) THEN json_extract(m.material_json,'$.launch.importedNativeThread.ref.nativeId') END=${native.nativeId} AND i.environment_id=${environmentId} AND m.intent_id>${materialCursor} ORDER BY m.intent_id LIMIT 128`;
              for (const row of materials) {
                if (!validId(row.intent_id)) return yield* integrity();
                const retained = yield* readNativeReservationInTransaction(
                  {
                    intentId: row.intent_id,
                  },
                  validatedThreads,
                );
                if (
                  retained.destination.environmentId !== environmentId ||
                  !sameCustodyValue(nativeFromLaunch(retained.material.launch), native) ||
                  !queue("thread", retained.threadId)
                )
                  return yield* integrity();
              }
              if (materials.length < 128) break;
              const last = materials.at(-1)!.intent_id;
              if (!validId(last) || last === materialCursor) return yield* integrity();
              materialCursor = last;
            }
          }
          let cursor = "";
          for (;;) {
            const candidates =
              yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads WHERE CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.nativeThreadRef.nativeId') END=${selected.native.id} AND provider_thread_id>${cursor} ORDER BY provider_thread_id LIMIT 128`;
            for (const row of candidates) {
              const value = yield* providerData(row);
              if (
                value.driver === selected.native.driver &&
                value.providerInstanceId === selected.native.instance &&
                value.nativeThreadRef?.nativeId === selected.native.id &&
                !queue("provider", value.id)
              )
                return yield* integrity();
            }
            if (candidates.length < 128) break;
            const last = candidates.at(-1)!.provider_thread_id;
            if (!validId(last) || last === cursor) return yield* integrity();
            cursor = last;
          }
        }
      }
      let owned = false;
      for (const selected of work) {
        if (selected.kind !== "thread") continue;
        const custody =
          validatedThreads.get(selected.id) ?? (yield* readThreadCustodyInTransaction(selected.id));
        if (
          custody.kind === "owned" &&
          (input.purpose === "workflow" || !sameCustodyValue(custody.owner, input.expectedOwner))
        )
          owned = true;
      }
      return owned;
    },
    Effect.mapError((error) =>
      isStoreError(error) ? error : new WorkflowRunStoreError({ reason: "persistence" }),
    ),
  );

  const readSubmissionCustodyInTransaction = Effect.fn(
    "WorkflowRunStore.readSubmissionCustodyInTransaction",
  )(
    (
      environmentId: string,
      threadId: string,
      expectedOwner: WorkflowOperationV1["owner"],
      nativeSelection: PreparationJob.NativeSelection | null = null,
    ) =>
      walkResourceCustodyInTransaction({
        purpose: "physical",
        environmentId,
        threadId,
        expectedOwner,
        nativeSelection,
      }),
  );

  const inTransaction = <A, E, R>(work: Effect.Effect<A, E, R>) =>
    sql
      .withTransaction(work)
      .pipe(
        Effect.mapError((error) =>
          isStoreError(error) ? error : new WorkflowRunStoreError({ reason: "persistence" }),
        ),
      );
  const beginOrdinaryPreparation = Effect.fn("WorkflowRunStore.beginOrdinaryPreparation")(
    function* (input: PreparationJob.Input) {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* integrity();
      const result = yield* inTransaction(
        Effect.gen(function* () {
          const encoded = PreparationJob.identity(input);
          if (encoded.version === 3) return yield* integrity();
          const retained =
            yield* sql`SELECT * FROM orchestration_v2_preparation_jobs WHERE job_id=${input.jobId} OR entry_key=${input.entryKey}`;
          const jobs = yield* Effect.forEach(retained, (row) =>
            Effect.try({ try: () => PreparationJob.decodeRow(row), catch: integrity }),
          );
          if (jobs.length) {
            if (jobs.length !== 1 || jobs[0]!.requestIdentityHash !== encoded.hash)
              return yield* integrity();
            return Object.freeze({ kind: "existing" as const, job: jobs[0]! });
          }
          if (encoded.version !== 2) return yield* integrity();
          if (
            yield* walkResourceCustodyInTransaction({
              purpose: "workflow",
              environmentId: input.environmentId,
              nativeSelection: input.nativeSelection ?? null,
              threadId: input.threadId,
              providerSessionIds: [],
            })
          )
            return Object.freeze({ kind: "denied" as const, reason: "custody" as const });
          const parent = yield* readPreparationParent(input);
          if (
            parent !== null &&
            (parent.identityVersion !== 2 ||
              !preparationMatches(parent, input.parentSelection!) ||
              parent.state !== "active" ||
              parent.codeClosedAt !== null)
          )
            return yield* stale();
          const createdAt = DateTime.formatIso(yield* DateTime.now);
          const rows = yield* sql`INSERT INTO orchestration_v2_preparation_jobs ${sql.insert({
            format_version: 1,
            job_id: input.jobId,
            entry_key: input.entryKey,
            kind: input.kind,
            environment_id: input.environmentId,
            project_id: input.projectId,
            thread_id: input.threadId,
            run_id: input.runId,
            originating_launch_job_id: input.originatingLaunchJobId,
            server_boot_id: input.serverBootId,
            request_identity_json: encoded.text,
            request_identity_hash: encoded.hash,
            created_at: createdAt,
          })} RETURNING *`;
          const job = yield* Effect.try({
            try: () => PreparationJob.decodeRow(rows[0]),
            catch: integrity,
          });
          return Object.freeze({ kind: "acquired" as const, job });
        }),
      );
      if (result.kind === "acquired") yield* notify;
      return result;
    },
  );
  const mutatePreparation = Effect.fn("WorkflowRunStore.mutatePreparation")(function* (
    selected: PreparationJob.Selection,
    action: "close" | "quiescing" | "unknown" | "settle" | "expose",
    nativeClaim?: PreparationJob.NativeClaim,
  ) {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* integrity();
    const result = yield* inTransaction(
      Effect.gen(function* () {
        const rows =
          yield* sql`SELECT * FROM orchestration_v2_preparation_jobs WHERE job_id=${selected.jobId}`;
        if (rows.length !== 1) return yield* stale();
        const job = yield* Effect.try({
          try: () => PreparationJob.decodeRow(rows[0]),
          catch: integrity,
        });
        if (nativeClaim === undefined) {
          if (job.identityVersion !== 2) return yield* integrity();
        } else {
          const owner = job.input.nativeOwner;
          if (
            job.identityVersion !== 3 ||
            !owner ||
            action === "settle" ||
            !nativeBinding ||
            job.input.environmentId !== nativeBinding.environmentId ||
            nativeClaim.bootId !== nativeBinding.coordinator.bootId ||
            nativeClaim.workerId !== nativeBinding.coordinator.workerId ||
            !sameCustodyValue(owner.executionClaim, nativeClaim)
          )
            return yield* integrity();
        }
        if (
          !preparationMatches(job, selected) ||
          job.state === "settled" ||
          job.recordVersion === Number.MAX_SAFE_INTEGER
        )
          return yield* stale();
        if (nativeClaim !== undefined) {
          const retained = yield* validateNativePreparationHistory(job, nativeClaim);
          if (action === "expose") {
            const current = yield* verifyClaimedNativeOperationInTransaction(nativeClaim);
            if (current.kind !== "current" || current.stage !== "preparing") return yield* stale();
            const observation = {
              driver: current.driver,
              providerInstanceId: current.providerInstanceId,
              providerThreadId: current.providerThreadId,
              providerSessionId: current.providerSessionId,
              nativeThreadId: current.nativeThreadId,
            };
            if (
              current.intentId !== retained.intentId ||
              current.threadId !== job.input.threadId ||
              current.runId !== job.input.runId ||
              !sameCustodyValue(observation, job.input.request.current)
            )
              return yield* stale();
          }
        }
        yield* readPreparationParent(job.input);
        const now = DateTime.formatIso(yield* DateTime.now);
        let patch: Record<string, string | number>;
        if (action === "expose") {
          if (job.state !== "active" || job.codeClosedAt !== null || job.processExposed)
            return yield* stale();
          patch = { process_exposed: 1 };
        } else if (action === "close") {
          if (job.codeClosedAt !== null) return yield* stale();
          patch = { code_closed_at: now };
        } else if (action === "settle") {
          if (job.codeClosedAt === null || job.processExposed) return yield* integrity();
          const settlement = {
            v: 1,
            jobId: selected.jobId,
            entryKey: selected.entryKey,
            priorVersion: job.recordVersion,
            kind: "code_only",
          };
          patch = {
            state: "settled",
            settlement_json: stableStringify(settlement),
            settlement_hash: PreparationJob.settlementHash(
              job.requestIdentityHash,
              job.codeClosedAt,
              settlement,
            ),
            settled_at: now,
          };
        } else {
          if (job.state === action || job.state === "unknown") return yield* stale();
          patch = { state: action };
        }
        const changed =
          yield* sql`UPDATE orchestration_v2_preparation_jobs SET ${sql.update({ ...patch, record_version: job.recordVersion + 1 })} WHERE job_id=${selected.jobId} AND entry_key=${selected.entryKey} AND request_identity_hash=${selected.requestIdentityHash} AND server_boot_id=${selected.serverBootId} AND record_version=${selected.expectedVersion} RETURNING *`;
        if (changed.length !== 1) return yield* stale();
        return yield* Effect.try({
          try: () => PreparationJob.decodeRow(changed[0]),
          catch: integrity,
        });
      }),
    );
    yield* notify;
    return result;
  });
  const ownPreparationMutation = (
    selected: PreparationJob.Selection,
    action: "close" | "quiescing" | "unknown" | "settle" | "expose",
  ) => {
    try {
      return mutatePreparation(PreparationJob.ownSelection(selected), action);
    } catch {
      return Effect.fail(integrity());
    }
  };
  const ownNativePreparationMutation = (
    input: PreparationJob.NativeControl,
    action: "close" | "quiescing" | "unknown" | "expose",
  ) => {
    try {
      const control = PreparationJob.ownNativeControl(input);
      return mutatePreparation(control.selection, action, control.executionClaim);
    } catch {
      return Effect.fail(integrity());
    }
  };
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
  const planningTransaction = <A, E, R>(
    work: Effect.Effect<A, E, R>,
    contentionReason: "conflict" | "storage" = "conflict",
  ) =>
    sql
      .withTransaction(work)
      .pipe(
        Effect.mapError((error) =>
          isPlanningError(error)
            ? error
            : planningError(isWriteContention(error) ? contentionReason : "storage"),
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
  type PlanningNativeBase = {
    readonly intentId: string;
    readonly sessionId: S["id"];
    readonly operationId: WorkflowOperationV1["id"];
    readonly passId: P["id"];
    readonly role: P["role"];
    readonly environmentId: string;
    readonly projectId: string;
    readonly launchCommandId: string;
    readonly threadId: string;
    readonly messageId: string;
    readonly generation: number;
    readonly draft: {
      readonly version: 1;
      readonly kind: "planning_pass";
      readonly operation: Omit<
        WorkflowOperationV1,
        "fingerprint" | "controlGeneration" | "status" | "claim" | "outcome" | "nextDueAt"
      >;
      readonly pass: Omit<P, "native">;
      readonly admission: {
        readonly sessionId: S["id"];
        readonly expectedVersion: number;
        readonly context: PlanningContext;
        readonly command: PlanningCommand;
      };
    };
    readonly now: string;
  };
  type PlanningNativeInput = PlanningNativeBase &
    (
      | { readonly materialVersion?: never; readonly launch?: never; readonly authority?: never }
      | { readonly materialVersion: 1; readonly launch: unknown; readonly authority?: never }
      | { readonly materialVersion: 2; readonly launch: unknown; readonly authority: unknown }
    );
  const validatePlanningNativeIntent = Effect.fn("WorkflowRunStore.validatePlanningNativeIntent")(
    function* (input: Omit<PlanningNativeBase, "now">) {
      const { draft } = input;
      const valid = yield* Effect.try({
        try: () => {
          WorkflowCanonicalHash.assertCanonical(draft);
          return (
            [
              input.intentId,
              input.sessionId,
              input.operationId,
              input.passId,
              input.environmentId,
              input.projectId,
              input.launchCommandId,
              input.threadId,
              input.messageId,
            ].every((x) => typeof x === "string" && x.length > 0) &&
            Object.keys(draft).sort().join() === "admission,kind,operation,pass,version" &&
            draft.version === 1 &&
            draft.kind === "planning_pass" &&
            Object.keys(draft.admission).sort().join() ===
              "command,context,expectedVersion,sessionId" &&
            Object.keys(draft.admission.command).sort().join() === "commandId,receiptId" &&
            Object.keys(draft.admission.context).sort().join() === "actor,destination" &&
            Object.keys(draft.admission.context.destination).sort().join() ===
              "environmentId,projectId" &&
            validCommand(draft.admission.command) &&
            isPlanningContext(draft.admission.context) &&
            Number.isSafeInteger(draft.admission.expectedVersion) &&
            draft.admission.expectedVersion >= 0 &&
            draft.admission.sessionId === input.sessionId &&
            ![input.launchCommandId, `${input.launchCommandId}:initial-message`].includes(
              draft.admission.command.commandId,
            )
          );
        },
        catch: () => planningError("invalid"),
      });
      if (!valid) return yield* planningError("invalid");
      const pass = yield* decodePlanningPassV1({
        ...draft.pass,
        native: { threadId: input.threadId, runId: "reservation-validation-only" },
      }).pipe(Effect.mapError(() => planningError("invalid")));
      const operation = yield* decodeWorkflowOperationV1({
        ...draft.operation,
        fingerprint: "0".repeat(64),
        controlGeneration: 0,
        status: "pending",
      }).pipe(Effect.mapError(() => planningError("invalid")));
      const { native: _native, ...passBase } = pass;
      const {
        fingerprint: _fingerprint,
        controlGeneration: _generation,
        status: _status,
        claim: _claim,
        outcome: _outcome,
        nextDueAt: _nextDueAt,
        ...operationBase
      } = operation;
      const same = (a: unknown, b: unknown) =>
        WorkflowCanonicalHash.tagged("compare", a) === WorkflowCanonicalHash.tagged("compare", b);
      const params = operation.parameters;
      const context = draft.admission.context;
      const consistent = yield* Effect.try({
        try: () =>
          same(passBase, draft.pass) &&
          same(operationBase, draft.operation) &&
          pass.status === "pending" &&
          !pass.report &&
          operation.kind === "planning_pass" &&
          params.kind === "planning_pass" &&
          operation.owner.kind === "planning_session" &&
          operation.owner.planningSessionId === input.sessionId &&
          pass.planningSessionId === input.sessionId &&
          pass.id === input.passId &&
          pass.operationId === input.operationId &&
          operation.id === input.operationId &&
          pass.role === input.role &&
          params.passId === pass.id &&
          params.role === pass.role &&
          same(pass.binding, params.binding) &&
          same(pass.provider, params.provider) &&
          WorkflowCanonicalHash.tagged("t3.planning-input-manifest.v1", pass.inputManifest) ===
            params.inputManifestHash &&
          same(operation.origin, context.actor) &&
          same(operation.destination, context.destination) &&
          input.environmentId === context.destination.environmentId &&
          input.projectId === context.destination.projectId,
        catch: () => planningError("invalid"),
      });
      if (!consistent || params.kind !== "planning_pass") return yield* planningError("integrity");
      const fingerprint = yield* Effect.try({
        try: () =>
          WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v1", {
            intentId: input.intentId,
            workflowRunId: null,
            planningSessionId: input.sessionId,
            operationId: input.operationId,
            attemptId: null,
            passId: input.passId,
            itemId: null,
            chargeAttempt: 0,
            repair: input.role === "repair",
            role: input.role,
            environmentId: input.environmentId,
            projectId: input.projectId,
            launchCommandId: input.launchCommandId,
            threadId: input.threadId,
            messageId: input.messageId,
            draft,
          }),
        catch: () => planningError("invalid"),
      });
      const command = draft.admission.command;
      return { pass, operation, params, context, fingerprint, command, same };
    },
  );
  const planningV3Fingerprint = (
    input: Omit<PlanningNativeBase, "now" | "draft">,
    draft: unknown,
    receiptId: string,
    authority = false,
  ) =>
    WorkflowCanonicalHash.tagged(
      authority ? "t3.workflow-native-launch-intent.v4" : "t3.workflow-native-launch-intent.v3",
      {
        intentId: input.intentId,
        workflowRunId: null,
        planningSessionId: input.sessionId,
        operationId: input.operationId,
        attemptId: null,
        passId: input.passId,
        itemId: null,
        chargeAttempt: 0,
        repair: input.role === "repair",
        role: input.role,
        environmentId: input.environmentId,
        projectId: input.projectId,
        launchCommandId: input.launchCommandId,
        threadId: input.threadId,
        messageId: input.messageId,
        draft,
        receiptId,
      },
    );
  const verifyStoredPlanningIntent = Effect.fn("WorkflowRunStore.verifyStoredPlanningIntent")(
    function* (
      input: Omit<PlanningNativeBase, "now" | "draft">,
      storedDraft: unknown,
      receipt: {
        command_id: string;
        receipt_id: string;
        intent_id: string;
        fingerprint: string;
        result_json: string;
      },
    ) {
      const info = yield* Effect.try({
        try: () => {
          WorkflowCanonicalHash.assertCanonical(storedDraft);
          const draft = storedDraft as PlanningNativeBase["draft"] & {
            material?: { version: number; launchHash: string; authorityHash?: string };
          };
          const authority = (draft.version as number) === 3;
          if ((draft.version as number) !== 2 && !authority)
            return { draft, marked: false as const };
          if (
            Object.keys(draft).sort().join() !== "admission,kind,material,operation,pass,version" ||
            !draft.material ||
            Object.keys(draft.material).sort().join() !==
              (authority ? "authorityHash,launchHash,version" : "launchHash,version") ||
            draft.material.version !== (authority ? 2 : 1) ||
            (authority &&
              (typeof draft.material.authorityHash !== "string" ||
                !/^[0-9a-f]{64}$/.test(draft.material.authorityHash))) ||
            !/^[0-9a-f]{64}$/.test(draft.material.launchHash)
          )
            throw new Error();
          const { material, ...base } = draft;
          return {
            draft: { ...base, version: 1 as const },
            marked: true as const,
            authority,
            marker: material,
          };
        },
        catch: () => planningError("integrity"),
      });
      const validated = yield* validatePlanningNativeIntent({ ...input, draft: info.draft }).pipe(
        Effect.mapError(() => planningError("integrity")),
      );
      if (
        receipt.intent_id !== input.intentId ||
        receipt.command_id !== validated.command.commandId ||
        receipt.receipt_id !== validated.command.receiptId ||
        !receipt.receipt_id
      )
        return yield* planningError("integrity");
      let result: object;
      let retainedMaterial: Material | undefined;
      if (info.marked) {
        const fingerprint = yield* Effect.try({
          try: () => planningV3Fingerprint(input, storedDraft, receipt.receipt_id, info.authority),
          catch: () => planningError("integrity"),
        });
        if (fingerprint !== receipt.fingerprint) return yield* planningError("integrity");
        const rows = yield* sql<{
          material_json: string;
          material_hash: string;
        }>`SELECT material_json, material_hash FROM workflow_native_reservation_material WHERE intent_id = ${input.intentId}`;
        if (rows.length !== 1 || !rows[0]) return yield* planningError("integrity");
        const raw = yield* Effect.try({
          try: () => parse(rows[0]!.material_json),
          catch: () => planningError("integrity"),
        });
        const { material, launchHash, materialHash, authorityHash } =
          yield* decodeNativeReservationMaterial(raw).pipe(
            Effect.mapError(() => planningError("integrity")),
          );
        const snapshot = material.planningSession;
        const launch = material.launch;
        if (
          material.version !== (info.authority ? 2 : 1) ||
          (material.version === 2 &&
            (authorityHash !== info.marker.authorityHash ||
              material.authority.environmentId !== input.environmentId ||
              snapshot?.origin.kind !== "human" ||
              material.authority.subject !== snapshot.origin.id)) ||
          !snapshot ||
          material.intentId !== input.intentId ||
          material.receiptId !== receipt.receipt_id ||
          material.intentFingerprint !== fingerprint ||
          rows[0].material_hash !== materialHash ||
          info.marker.launchHash !== launchHash ||
          launch.commandId !== input.launchCommandId ||
          launch.threadId !== input.threadId ||
          launch.initialMessage.messageId !== input.messageId ||
          launch.projectId !== input.projectId ||
          launch.modelSelection.instanceId !== validated.pass.provider.providerInstanceId ||
          launch.modelSelection.model !== validated.pass.provider.model ||
          snapshot.id !== input.sessionId ||
          snapshot.environmentId !== input.environmentId ||
          snapshot.projectId !== input.projectId ||
          !validated.same(snapshot.origin, validated.context.actor) ||
          snapshot.planId !== validated.pass.binding.planId ||
          !validated.same(snapshot.limits, validated.params.limits) ||
          !snapshot.allowedRoles.includes(validated.pass.role) ||
          !snapshot.allowedProviders.some(
            (p) =>
              p.providerInstanceId === validated.pass.provider.providerInstanceId &&
              p.model === validated.pass.provider.model,
          ) ||
          snapshot.status !== "active" ||
          snapshot.recordVersion !== info.draft.admission.expectedVersion + 1 ||
          !Number.isSafeInteger(snapshot.controlGeneration) ||
          !Number.isSafeInteger(input.generation) ||
          snapshot.controlGeneration > input.generation ||
          snapshot.consumed.passes < 1 ||
          (input.role === "repair" && snapshot.consumed.repairRounds < 1)
        )
          return yield* planningError("integrity");
        retainedMaterial = material;
        result = {
          intentId: input.intentId,
          fingerprint,
          materialVersion: material.version,
          materialHash,
        };
      } else {
        if (validated.fingerprint !== receipt.fingerprint) return yield* planningError("integrity");
        result = { intentId: input.intentId, fingerprint: receipt.fingerprint };
      }
      const matches = yield* Effect.try({
        try: () => validated.same(parse(receipt.result_json), result),
        catch: () => planningError("integrity"),
      });
      if (!matches) return yield* planningError("integrity");
      return { ...validated, draft: info.draft, marked: info.marked, material: retainedMaterial };
    },
  );
  const validatePlanningRevision = Effect.fn("WorkflowRunStore.validatePlanningRevision")(
    function* (pass: Pick<P, "binding">) {
      const b = pass.binding;
      const rows = yield* sql<{
        revision_json: string;
        packet_json: string;
        created_at: string;
      }>`SELECT r.revision_json, p.packet_json, r.created_at FROM development_plan_revisions r JOIN development_plan_source_packets p ON p.plan_id = r.plan_id AND p.source_packet_hash = r.source_packet_hash JOIN workflow_revision_identities i ON i.plan_id = r.plan_id AND i.revision_id = r.revision_id AND i.revision_hash = r.revision_hash AND i.source_packet_hash = r.source_packet_hash WHERE r.plan_id = ${b.planId} AND r.revision_id = ${b.revisionId} AND r.revision_hash = ${b.revisionHash} AND r.source_packet_hash = ${b.sourcePacketHash}`;
      const row = rows[0];
      if (!row) return yield* planningError("integrity");
      const content = yield* Effect.try({
        try: () => ({
          revision: parse(row.revision_json),
          sourcePacket: parse(row.packet_json),
        }),
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
    },
  );
  const reservePlanningNativeIntent = Effect.fn("WorkflowRunStore.reservePlanningNativeIntent")(
    function* (request: PlanningNativeInput) {
      const input = yield* Effect.try({
        try: () => {
          const copy = { ...request };
          WorkflowCanonicalHash.assertCanonical(copy.draft);
          const snapshot = { ...copy, draft: structuredClone(copy.draft) };
          if (!Object.hasOwn(copy, "launch")) return snapshot;
          WorkflowCanonicalHash.assertCanonical(copy.launch);
          return {
            ...snapshot,
            launch: structuredClone(copy.launch),
            ...(Object.hasOwn(copy, "authority")
              ? { authority: structuredClone(copy.authority) }
              : {}),
          };
        },
        catch: () => planningError("invalid"),
      });
      const {
        pass,
        operation,
        params,
        context,
        fingerprint: legacyFingerprint,
        command,
        same,
      } = yield* validatePlanningNativeIntent(input);
      const { draft } = input;
      const hasVersion = Object.hasOwn(input, "materialVersion");
      const hasLaunch = Object.hasOwn(input, "launch");
      const hasAuthority = Object.hasOwn(input, "authority");
      if (
        hasVersion !== hasLaunch ||
        (hasVersion && input.materialVersion !== 1 && input.materialVersion !== 2) ||
        hasAuthority !== (input.materialVersion === 2)
      )
        return yield* planningError("invalid");
      const retained = yield* Effect.gen(function* () {
        if (!hasVersion) return null;
        const { launch, launchHash } = yield* decodeNativeReservationLaunch(input.launch).pipe(
          Effect.mapError(() => planningError("integrity")),
        );
        if (
          launch.commandId !== input.launchCommandId ||
          launch.threadId !== input.threadId ||
          launch.initialMessage.messageId !== input.messageId ||
          launch.projectId !== input.projectId ||
          launch.modelSelection.instanceId !== pass.provider.providerInstanceId ||
          launch.modelSelection.model !== pass.provider.model
        )
          return yield* planningError("integrity");
        const authorized =
          input.materialVersion === 2
            ? yield* decodeNativeReservationAuthority(input.authority).pipe(
                Effect.mapError(() => planningError("integrity")),
              )
            : null;
        const persistedDraft = {
          ...draft,
          version: authorized ? 3 : 2,
          material: authorized
            ? { version: 2, launchHash, authorityHash: authorized.authorityHash }
            : { version: 1, launchHash },
        };
        const fingerprint = yield* Effect.try({
          try: () =>
            planningV3Fingerprint(input, persistedDraft, command.receiptId, authorized !== null),
          catch: () => planningError("integrity"),
        });
        return {
          launch,
          authorized,
          persistedDraft,
          fingerprint,
          persistedDraftJson: encode(persistedDraft),
        };
      });
      const fingerprint = retained?.fingerprint ?? legacyFingerprint;
      const result = yield* planningTransaction(
        Effect.gen(function* () {
          const receipts = yield* sql<{
            command_id: string;
            receipt_id: string;
            intent_id: string;
            fingerprint: string;
            result_json: string;
            draft_json: string;
            intent_fingerprint: string;
          }>`SELECT r.*, i.draft_json, i.fingerprint AS intent_fingerprint FROM workflow_native_intent_receipts r JOIN workflow_native_launch_intents i ON i.intent_id = r.intent_id WHERE r.command_id = ${command.commandId} OR r.receipt_id = ${command.receiptId} OR r.intent_id = ${input.intentId}`;
          const orphan = yield* sql<{
            workflow_run_id: string | null;
            planning_session_id: string | null;
            operation_id: string;
            pass_id: string | null;
            attempt_id: string | null;
            item_id: string | null;
            charge_attempt: number;
            repair: number;
            role: string;
            environment_id: string;
            project_id: string;
            launch_command_id: string;
            thread_id: string;
            message_id: string;
            generation: number;
          }>`SELECT * FROM workflow_native_launch_intents WHERE intent_id = ${input.intentId}`;
          if (receipts.length || orphan.length) {
            const r = receipts[0];
            if (
              receipts.length !== 1 ||
              orphan.length !== 1 ||
              !r ||
              r.command_id !== command.commandId ||
              r.receipt_id !== command.receiptId ||
              r.intent_id !== input.intentId ||
              r.fingerprint !== fingerprint ||
              r.intent_fingerprint !== fingerprint
            )
              return yield* planningError("conflict");
            const i = orphan[0]!;
            if (
              i.workflow_run_id !== null ||
              i.planning_session_id !== input.sessionId ||
              i.operation_id !== input.operationId ||
              i.pass_id !== input.passId ||
              i.attempt_id !== null ||
              i.item_id !== null ||
              i.charge_attempt !== 0 ||
              i.repair !== (input.role === "repair" ? 1 : 0) ||
              i.role !== input.role ||
              i.environment_id !== input.environmentId ||
              i.project_id !== input.projectId ||
              i.launch_command_id !== input.launchCommandId ||
              i.thread_id !== input.threadId ||
              i.message_id !== input.messageId
            )
              return yield* planningError("integrity");
            const storedDraft = yield* Effect.try({
              try: () => parse(r.draft_json),
              catch: () => planningError("integrity"),
            });
            const matches = yield* Effect.try({
              try: () => same(storedDraft, retained?.persistedDraft ?? draft),
              catch: () => planningError("integrity"),
            });
            if (!matches) return yield* planningError("integrity");
            const verified = yield* verifyStoredPlanningIntent(
              { ...input, generation: i.generation },
              storedDraft,
              r,
            );
            if (verified.marked !== (retained !== null)) return yield* planningError("integrity");
            return {
              kind: "replay" as const,
              receiptId: r.receipt_id,
              intentId: input.intentId,
              fingerprint,
            };
          }
          if (!retained) return yield* planningError("integrity");
          if (yield* runningNativeEffect(input.threadId)) return yield* planningError("custody");
          const submitted = yield* readSubmissionCustodyInTransaction(
            input.environmentId,
            input.threadId,
            { kind: "planning_session", planningSessionId: input.sessionId },
            nativeFromLaunch(retained.launch),
          ).pipe(
            Effect.mapError((error) =>
              planningError(error.reason === "integrity" ? "integrity" : "storage"),
            ),
          );
          if (submitted) return yield* planningError("custody");
          const custody = yield* readThreadCustodyInTransaction(input.threadId).pipe(
            Effect.mapError((error) =>
              planningError(error.reason === "integrity" ? "integrity" : "storage"),
            ),
          );
          if (
            custody.kind === "owned" &&
            (custody.owner.kind !== "planning_session" ||
              custody.owner.planningSessionId !== input.sessionId)
          )
            return yield* planningError("custody");
          const collisions =
            yield* sql`SELECT 1 FROM workflow_command_receipts WHERE command_id IN (${command.commandId}, ${input.launchCommandId}, ${input.launchCommandId + ":initial-message"}) OR receipt_id = ${command.receiptId} OR operation_id = ${input.operationId} UNION ALL SELECT 1 FROM workflow_operations WHERE id = ${input.operationId} UNION ALL SELECT 1 FROM workflow_planning_passes WHERE id = ${input.passId} OR operation_id = ${input.operationId} UNION ALL SELECT 1 FROM workflow_native_launch_intents WHERE operation_id = ${input.operationId} OR pass_id = ${input.passId} OR launch_command_id IN (${command.commandId}, ${input.launchCommandId}, ${input.launchCommandId + ":initial-message"}) OR launch_command_id || ':initial-message' IN (${command.commandId}, ${input.launchCommandId}) OR (thread_id = ${input.threadId} AND message_id = ${input.messageId}) UNION ALL SELECT 1 FROM workflow_native_intent_receipts WHERE command_id IN (${command.commandId}, ${input.launchCommandId}, ${input.launchCommandId + ":initial-message"}) OR receipt_id = ${command.receiptId}`;
          if (collisions.length) return yield* planningError("conflict");
          if (
            !Number.isSafeInteger(input.generation) ||
            input.generation < 0 ||
            !isWorkflowTimestamp(input.now)
          )
            return yield* planningError("invalid");
          const current = yield* readSession(input.sessionId);
          if (!sameContext(current, context)) return yield* planningError("unauthorized");
          if (current.recordVersion !== draft.admission.expectedVersion)
            return yield* new PlanningControlError({
              reason: "conflict",
              currentVersion: current.recordVersion,
            });
          if (current.status !== "active") return yield* planningError("ineligible");
          const currentNow = retained.authorized
            ? DateTime.formatIso(yield* DateTime.now)
            : input.now;
          if (!isWorkflowTimestampAfter(current.limits.deadline, currentNow))
            return yield* planningError("deadline");
          if (
            input.generation !== current.controlGeneration ||
            pass.binding.planId !== current.planId ||
            !same(params.limits, current.limits)
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
              yield* sql`SELECT 1 FROM workflow_planning_passes p JOIN workflow_operations o ON o.id = p.operation_id WHERE p.plan_id = ${current.planId} AND o.status IN ('pending','claimed','unknown') AND json_extract(p.record_json, '$.role') IN ('author','repair') UNION ALL SELECT 1 FROM workflow_native_launch_intents i JOIN workflow_planning_sessions s ON s.id = i.planning_session_id WHERE s.plan_id = ${current.planId} AND i.state IN ('reserved','dispatching','unknown') AND i.role IN ('author','repair')`;
            if (writers.length) return yield* planningError("custody");
          }
          yield* validatePlanningRevision(pass);
          const updated = {
            ...current,
            recordVersion: current.recordVersion + 1,
            consumed: {
              passes: current.consumed.passes + 1,
              repairRounds: current.consumed.repairRounds + (pass.role === "repair" ? 1 : 0),
            },
          };
          const decodedUpdated = yield* decodePlanningSessionV1(updated).pipe(
            Effect.mapError(() => planningError("integrity")),
          );
          if (
            decodedUpdated.id !== current.id ||
            decodedUpdated.controlGeneration !== input.generation ||
            !sameContext(decodedUpdated, context) ||
            decodedUpdated.recordVersion !== current.recordVersion + 1 ||
            decodedUpdated.consumed.passes !== current.consumed.passes + 1 ||
            decodedUpdated.consumed.repairRounds !==
              current.consumed.repairRounds + (pass.role === "repair" ? 1 : 0)
          )
            return yield* planningError("integrity");
          const decodedMaterial = yield* decodeNativeReservationMaterial({
            version: retained.authorized ? 2 : 1,
            ...(retained.authorized ? { authority: retained.authorized.authority } : {}),
            intentId: input.intentId,
            receiptId: command.receiptId,
            intentFingerprint: fingerprint,
            launch: retained.launch,
            planningSession: decodedUpdated,
          }).pipe(Effect.mapError(() => planningError("integrity")));
          if (
            !(yield* checkWorkflowAuthority(decodedMaterial.material, operation, current).pipe(
              Effect.mapError(() => planningError("storage")),
            ))
          )
            return yield* planningError("unauthorized");
          const ownerJson = encode(decodedUpdated);
          const materialJson = encode(decodedMaterial.material);
          const receiptJson = encode({
            intentId: input.intentId,
            fingerprint,
            materialVersion: decodedMaterial.material.version,
            materialHash: decodedMaterial.materialHash,
          });
          const changed =
            yield* sql`UPDATE workflow_planning_sessions SET record_version = ${decodedUpdated.recordVersion}, record_json = ${ownerJson} WHERE id = ${current.id} AND record_version = ${current.recordVersion} AND control_generation = ${current.controlGeneration} RETURNING id`;
          if (changed.length !== 1) return yield* planningError("conflict");
          const inserted =
            yield* sql`INSERT INTO workflow_native_launch_intents (intent_id, planning_session_id, operation_id, pass_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, generation, state, held) VALUES (${input.intentId}, ${input.sessionId}, ${input.operationId}, ${input.passId}, 0, ${input.role === "repair" ? 1 : 0}, ${input.role}, ${input.environmentId}, ${input.projectId}, ${input.launchCommandId}, ${input.threadId}, ${input.messageId}, ${fingerprint}, ${retained.persistedDraftJson}, ${input.generation}, 'reserved', 0) RETURNING intent_id`;
          if (inserted.length !== 1) return yield* planningError("conflict");
          const material =
            yield* sql`INSERT INTO workflow_native_reservation_material (intent_id, material_json, material_hash) VALUES (${input.intentId}, ${materialJson}, ${decodedMaterial.materialHash}) RETURNING intent_id`;
          if (material.length !== 1) return yield* planningError("conflict");
          const saved =
            yield* sql`INSERT INTO workflow_native_intent_receipts (command_id, receipt_id, intent_id, fingerprint, result_json) VALUES (${command.commandId}, ${command.receiptId}, ${input.intentId}, ${fingerprint}, ${receiptJson}) RETURNING receipt_id`;
          if (saved.length !== 1) return yield* planningError("conflict");
          return {
            kind: "reserved" as const,
            receiptId: command.receiptId,
            intentId: input.intentId,
            fingerprint,
          };
        }),
        "storage",
      );
      if (result.kind === "reserved") yield* notify;
      return result;
    },
  );
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
        const outstanding =
          yield* sql`SELECT 1 FROM workflow_native_launch_intents i LEFT JOIN workflow_native_intent_receipts r ON r.intent_id = i.intent_id WHERE (i.operation_id = ${operation.id} OR i.pass_id = ${pass.id} OR r.command_id = ${input.command.commandId} OR r.receipt_id = ${input.command.receiptId} OR i.launch_command_id IN (${input.command.commandId}, ${input.command.commandId + ":initial-message"}))`;
        if (outstanding.length) return yield* planningError("conflict");
        if (yield* runningNativeEffect(pass.native!.threadId))
          return yield* planningError("custody");
        const submitted = yield* readSubmissionCustodyInTransaction(
          operation.destination.environmentId,
          pass.native!.threadId,
          operation.owner,
        ).pipe(
          Effect.mapError((error) =>
            planningError(error.reason === "integrity" ? "integrity" : "storage"),
          ),
        );
        if (submitted) return yield* planningError("custody");
        const custody = yield* readThreadCustodyInTransaction(pass.native!.threadId).pipe(
          Effect.mapError((error) =>
            planningError(error.reason === "integrity" ? "integrity" : "storage"),
          ),
        );
        if (
          custody.kind === "owned" &&
          (custody.owner.kind !== "planning_session" ||
            custody.owner.planningSessionId !== input.sessionId)
        )
          return yield* planningError("custody");
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
            yield* sql`SELECT p.id FROM workflow_planning_passes p JOIN workflow_operations o ON o.id = p.operation_id WHERE p.plan_id = ${current.planId} AND o.status IN ('pending','claimed','unknown') AND json_extract(p.record_json, '$.role') IN ('author','repair') UNION ALL SELECT i.intent_id FROM workflow_native_launch_intents i JOIN workflow_planning_sessions s ON s.id = i.planning_session_id WHERE s.plan_id = ${current.planId} AND i.state IN ('reserved','dispatching','unknown') AND i.role IN ('author','repair')`;
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
        yield* decodePlanningSessionV1(updated).pipe(
          Effect.mapError(() => planningError("integrity")),
        );
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
          yield* sql`SELECT id FROM workflow_operations WHERE planning_session_id = ${session.id} AND id != ${operation.id} AND status IN ('claimed','unknown') UNION ALL SELECT intent_id AS id FROM workflow_native_launch_intents WHERE planning_session_id = ${session.id} AND state IN ('dispatching','unknown')`;
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
        const nativeRows = yield* sql<{
          intent_id: string;
          workflow_run_id: string | null;
          planning_session_id: string | null;
          operation_id: WorkflowOperationV1["id"];
          attempt_id: string | null;
          pass_id: P["id"];
          item_id: string | null;
          charge_attempt: number;
          repair: number;
          role: P["role"];
          environment_id: string;
          project_id: string;
          launch_command_id: string;
          thread_id: string;
          message_id: string;
          fingerprint: string;
          draft_json: string;
          generation: number;
          state: string;
          held: number;
          boot_id: string | null;
          worker_id: string | null;
          native_run_id: string | null;
          finalized_operation_id: string | null;
        }>`SELECT * FROM workflow_native_launch_intents WHERE planning_session_id = ${value.id} AND state IN ('reserved','dispatching','unknown')`;
        const intents = yield* Effect.forEach(nativeRows, (row) =>
          Effect.gen(function* () {
            if (
              row.workflow_run_id !== null ||
              row.planning_session_id !== value.id ||
              row.attempt_id !== null ||
              row.item_id !== null ||
              row.charge_attempt !== 0 ||
              row.repair !== (row.role === "repair" ? 1 : 0) ||
              !Number.isSafeInteger(row.generation) ||
              row.generation < 0 ||
              row.generation > value.controlGeneration ||
              (row.state === "reserved" && row.generation !== value.controlGeneration - row.held) ||
              (row.held !== 0 && row.held !== 1) ||
              (row.state !== "reserved" && (row.held !== 0 || !row.boot_id || !row.worker_id)) ||
              (row.state === "reserved" &&
                (row.boot_id !== null ||
                  row.worker_id !== null ||
                  row.native_run_id !== null ||
                  row.finalized_operation_id !== null))
            )
              return yield* planningError("integrity");
            const draft = yield* Effect.try({
              try: () => parse(row.draft_json) as PlanningNativeInput["draft"],
              catch: () => planningError("integrity"),
            });
            const command = yield* Effect.try({
              try: () => draft.admission.command,
              catch: () => planningError("integrity"),
            });
            if (!validCommand(command)) return yield* planningError("integrity");
            const receipts = yield* sql<{
              command_id: string;
              receipt_id: string;
              intent_id: string;
              fingerprint: string;
              result_json: string;
            }>`SELECT * FROM workflow_native_intent_receipts WHERE intent_id = ${row.intent_id} OR command_id = ${command.commandId} OR receipt_id = ${command.receiptId}`;
            const receipt = receipts[0];
            if (receipts.length !== 1 || !receipt || receipt.fingerprint !== row.fingerprint)
              return yield* planningError("integrity");
            const validated = yield* verifyStoredPlanningIntent(
              {
                intentId: row.intent_id,
                sessionId: value.id,
                operationId: row.operation_id,
                passId: row.pass_id,
                role: row.role,
                environmentId: row.environment_id,
                projectId: row.project_id,
                launchCommandId: row.launch_command_id,
                threadId: row.thread_id,
                messageId: row.message_id,
                generation: row.generation,
              },
              draft,
              receipt,
            );
            if (!sameContext(value, validated.context)) return yield* planningError("integrity");
            return { row, ...validated };
          }),
        );
        const claimed =
          operations.some(({ operation }) => operation.status === "claimed") ||
          intents.some(({ row }) => row.state === "dispatching");
        const unknown =
          operations.some(({ operation }) => operation.status === "unknown") ||
          intents.some(({ row }) => row.state === "unknown");
        let status: S["status"];
        if (input.action === "pause") {
          if (value.status !== "active") return yield* planningError("ineligible");
          status = unknown ? "needs_attention" : claimed ? "pausing" : "paused";
        } else if (input.action === "resume") {
          if (value.status !== "paused") return yield* planningError("ineligible");
          if (claimed || unknown) return yield* planningError("custody");
          const nativeRearms = yield* readNativeHeldRearmsInTransaction(
            { kind: "planning_session", planningSessionId: value.id },
            value.recordVersion,
          ).pipe(
            Effect.mapError((error) =>
              planningError(
                isStoreError(error) && error.reason === "integrity" ? "integrity" : "storage",
              ),
            ),
          );
          if (nativeRearms.kind !== "current")
            return yield* planningError(
              nativeRearms.kind === "stale"
                ? "conflict"
                : nativeRearms.reason === "authority"
                  ? "unauthorized"
                  : nativeRearms.reason === "deadline"
                    ? "deadline"
                    : "custody",
            );
          const verifiedHeld = new Set(nativeRearms.operations.map((operation) => operation.id));
          const held = intents.filter(({ row }) => row.state === "reserved" && row.held === 1);
          const currentNow =
            held.some(({ material }) => material?.version === 2) || verifiedHeld.size > 0
              ? DateTime.formatIso(yield* DateTime.now)
              : input.now;
          if (!isWorkflowTimestampAfter(value.limits.deadline, currentNow))
            return yield* planningError("deadline");
          if (
            intents.some(({ row }) => row.state === "reserved" && row.held !== 1) ||
            value.consumed.passes < held.length ||
            value.consumed.repairRounds < held.filter(({ pass }) => pass.role === "repair").length
          )
            return yield* planningError("integrity");
          if (
            value.consumed.passes > value.limits.maxPasses ||
            (value.consumed.passes === value.limits.maxPasses &&
              held.length + verifiedHeld.size === 0)
          )
            return yield* planningError("limit");
          for (const intent of held) {
            const { row, pass, params, same } = intent;
            if (
              intent.material?.version === 2 &&
              !(yield* checkWorkflowAuthority(intent.material, intent.operation, value).pipe(
                Effect.mapError(() => planningError("storage")),
              ))
            )
              return yield* planningError("unauthorized");
            if (pass.binding.planId !== value.planId || !same(params.limits, value.limits))
              return yield* planningError("integrity");
            if (
              !value.allowedRoles.includes(pass.role) ||
              !value.allowedProviders.some(
                (provider) =>
                  provider.providerInstanceId === pass.provider.providerInstanceId &&
                  provider.model === pass.provider.model,
              )
            )
              return yield* planningError("ineligible");
            if (pass.role === "author" || pass.role === "repair") {
              const writers =
                yield* sql`SELECT 1 FROM workflow_planning_passes p JOIN workflow_operations o ON o.id = p.operation_id WHERE p.plan_id = ${value.planId} AND o.status IN ('pending','claimed','unknown') AND json_extract(p.record_json, '$.role') IN ('author','repair') UNION ALL SELECT 1 FROM workflow_native_launch_intents i JOIN workflow_planning_sessions s ON s.id = i.planning_session_id WHERE s.plan_id = ${value.planId} AND i.intent_id <> ${row.intent_id} AND i.state IN ('reserved','dispatching','unknown') AND i.role IN ('author','repair')`;
              if (writers.length) return yield* planningError("custody");
            }
            yield* validatePlanningRevision(pass);
          }
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
        yield* decodePlanningSessionV1(updated).pipe(
          Effect.mapError(() => planningError("integrity")),
        );
        for (const { row } of intents) {
          if (row.state !== "reserved") continue;
          if (input.action === "pause" && row.held === 0) {
            const changed =
              yield* sql`UPDATE workflow_native_launch_intents SET held = 1 WHERE intent_id = ${row.intent_id} AND planning_session_id = ${value.id} AND state = 'reserved' AND held = 0 AND generation = ${row.generation} AND fingerprint = ${row.fingerprint} AND boot_id IS NULL AND worker_id IS NULL AND native_run_id IS NULL AND finalized_operation_id IS NULL RETURNING intent_id`;
            if (changed.length !== 1) return yield* planningError("integrity");
          } else if (input.action === "resume" && row.held === 1) {
            const changed =
              yield* sql`UPDATE workflow_native_launch_intents SET held = 0, generation = ${updated.controlGeneration} WHERE intent_id = ${row.intent_id} AND planning_session_id = ${value.id} AND state = 'reserved' AND held = 1 AND generation = ${row.generation} AND fingerprint = ${row.fingerprint} AND boot_id IS NULL AND worker_id IS NULL AND native_run_id IS NULL AND finalized_operation_id IS NULL RETURNING intent_id`;
            if (changed.length !== 1) return yield* planningError("integrity");
          } else if (input.action === "stop" || input.action === "expire") {
            const changed =
              yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled', held = 0 WHERE intent_id = ${row.intent_id} AND planning_session_id = ${value.id} AND state = 'reserved' AND held = ${row.held} AND generation = ${row.generation} AND fingerprint = ${row.fingerprint} AND boot_id IS NULL AND worker_id IS NULL AND native_run_id IS NULL AND finalized_operation_id IS NULL RETURNING intent_id`;
            if (changed.length !== 1) return yield* planningError("integrity");
          }
        }
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
        }>`UPDATE workflow_planning_sessions SET status = ${status}, record_version = ${updated.recordVersion}, control_generation = ${updated.controlGeneration}, record_json = ${encode(updated)} WHERE id = ${value.id} AND record_version = ${input.expectedVersion} AND control_generation = ${value.controlGeneration} RETURNING id`;
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
  type WorkflowNativeBase = {
    readonly intentId: string;
    readonly runId: string;
    readonly operationId: WorkflowOperationV1["id"];
    readonly attemptId: string;
    readonly itemId: string;
    readonly repair: boolean;
    readonly environmentId: string;
    readonly projectId: string;
    readonly launchCommandId: string;
    readonly threadId: string;
    readonly messageId: string;
    readonly receiptId: string;
    readonly generation: number;
    readonly draft: {
      readonly version: 1;
      readonly kind: "stage_launch";
      readonly operation: Omit<
        WorkflowOperationV1,
        | "fingerprint"
        | "controlGeneration"
        | "status"
        | "claim"
        | "outcome"
        | "nextDueAt"
        | "parameters"
      > & {
        readonly parameters: Omit<
          Extract<WorkflowOperationV1["parameters"], { kind: "stage_launch" }>,
          "nativeRunId"
        >;
      };
      readonly admission: {
        readonly command: Omit<
          Extract<Command, { kind: "admitAttempt" }>,
          "fingerprint" | "operationFingerprint"
        >;
      };
    };
    readonly externalFacts: Facts;
  };
  type WorkflowNativeInput = WorkflowNativeBase &
    (
      | { readonly materialVersion?: never; readonly launch?: never; readonly authority?: never }
      | { readonly materialVersion: 1; readonly launch: unknown; readonly authority?: never }
      | { readonly materialVersion: 2; readonly launch: unknown; readonly authority: unknown }
    );
  const validateWorkflowNativeIntent = Effect.fn("WorkflowRunStore.validateWorkflowNativeIntent")(
    function* (input: Omit<WorkflowNativeBase, "externalFacts">) {
      const { draft } = input;
      const valid = yield* Effect.try({
        try: () => {
          WorkflowCanonicalHash.assertCanonical(draft);
          const command = draft.admission.command;
          return (
            [
              input.intentId,
              input.runId,
              input.operationId,
              input.attemptId,
              input.itemId,
              input.environmentId,
              input.projectId,
              input.launchCommandId,
              input.threadId,
              input.messageId,
              input.receiptId,
              command.commandId,
            ].every((id) => typeof id === "string" && id.length > 0) &&
            Number.isSafeInteger(input.generation) &&
            input.generation >= 0 &&
            typeof input.repair === "boolean" &&
            Object.keys(draft).sort().join() === "admission,kind,operation,version" &&
            draft.version === 1 &&
            draft.kind === "stage_launch" &&
            Object.keys(draft.admission).join() === "command" &&
            Object.keys(command).sort().join() ===
              "attemptId,commandId,expectedGeneration,expectedSpecHash,expectedVersion,itemId,kind,operationId,repair" &&
            Number.isSafeInteger(command.expectedVersion) &&
            command.expectedVersion >= 0 &&
            Number.isSafeInteger(command.expectedGeneration) &&
            command.expectedGeneration >= 0 &&
            command.kind === "admitAttempt" &&
            command.operationId === input.operationId &&
            command.attemptId === input.attemptId &&
            command.itemId === input.itemId &&
            command.repair === input.repair &&
            command.commandId !== input.launchCommandId &&
            command.commandId !== `${input.launchCommandId}:initial-message`
          );
        },
        catch: integrity,
      });
      if (!valid) return yield* integrity();
      const supplemented = {
        ...draft.operation,
        parameters: { ...draft.operation.parameters, nativeRunId: "reservation-validation-only" },
        fingerprint: "0".repeat(64),
        controlGeneration: input.generation,
        status: "pending",
      };
      const operation = yield* decodeWorkflowOperationV1(supplemented).pipe(
        Effect.mapError(integrity),
      );
      if (operation.kind !== "stage_launch" || operation.parameters.kind !== "stage_launch")
        return yield* integrity();
      const {
        fingerprint: _fingerprint,
        controlGeneration: _controlGeneration,
        status: _status,
        claim: _claim,
        outcome: _outcome,
        nextDueAt: _nextDueAt,
        ...operationBase
      } = operation;
      const { nativeRunId: _nativeRunId, ...parameters } = operation.parameters;
      const canonical = yield* Effect.try({
        try: () =>
          WorkflowCanonicalHash.tagged("compare", { ...operationBase, parameters }) ===
          WorkflowCanonicalHash.tagged("compare", draft.operation),
        catch: integrity,
      });
      if (
        !canonical ||
        operation.parameters.stage !== "implementation" ||
        operation.id !== input.operationId ||
        operation.owner.kind !== "workflow_run" ||
        operation.owner.workflowRunId !== input.runId ||
        operation.destination.environmentId !== input.environmentId ||
        operation.destination.projectId !== input.projectId ||
        operation.parameters.nativeThreadId !== input.threadId ||
        operation.parameters.context.itemId !== input.itemId ||
        operation.parameters.context.attemptId !== input.attemptId ||
        operation.parameters.context.specHash !== draft.admission.command.expectedSpecHash
      )
        return yield* integrity();
      const fingerprints = yield* Effect.try({
        try: () => {
          const envelope = {
            intentId: input.intentId,
            workflowRunId: input.runId,
            planningSessionId: null,
            operationId: input.operationId,
            attemptId: input.attemptId,
            passId: null,
            itemId: input.itemId,
            chargeAttempt: 1,
            repair: input.repair,
            role: "implementation",
            environmentId: input.environmentId,
            projectId: input.projectId,
            launchCommandId: input.launchCommandId,
            threadId: input.threadId,
            messageId: input.messageId,
            draft,
          };
          return {
            fingerprint: WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v2", {
              ...envelope,
              receiptId: input.receiptId,
            }),
            legacyFingerprint: WorkflowCanonicalHash.tagged(
              "t3.workflow-native-launch-intent.v1",
              envelope,
            ),
          };
        },
        catch: integrity,
      });
      return { operation, ...fingerprints };
    },
  );
  const verifyStoredWorkflowIntent = Effect.fn("WorkflowRunStore.verifyStoredWorkflowIntent")(
    function* (
      input: Omit<WorkflowNativeBase, "externalFacts" | "draft">,
      storedDraft: unknown,
      receipt: {
        command_id: string;
        receipt_id: string;
        intent_id: string;
        fingerprint: string;
        result_json: string;
      },
    ) {
      const draftInfo = yield* Effect.try({
        try: () => {
          WorkflowCanonicalHash.assertCanonical(storedDraft);
          const draft = storedDraft as WorkflowNativeInput["draft"] & {
            material?: { version: number; launchHash: string; authorityHash?: string };
          };
          if ((draft.version as number) !== 2 && (draft.version as number) !== 3)
            return { draft, marked: false as const };
          const authority = (draft.version as number) === 3;
          if (
            Object.keys(draft).sort().join() !== "admission,kind,material,operation,version" ||
            !draft.material ||
            Object.keys(draft.material).sort().join() !==
              (authority ? "authorityHash,launchHash,version" : "launchHash,version") ||
            draft.material.version !== (authority ? 2 : 1) ||
            (authority &&
              (typeof draft.material.authorityHash !== "string" ||
                !/^[0-9a-f]{64}$/.test(draft.material.authorityHash))) ||
            typeof draft.material.launchHash !== "string" ||
            !/^[0-9a-f]{64}$/.test(draft.material.launchHash)
          )
            throw integrity();
          const { material, ...base } = draft;
          return {
            draft: { ...base, version: 1 as const },
            marked: true as const,
            authority,
            marker: material,
          };
        },
        catch: integrity,
      });
      const validated = yield* validateWorkflowNativeIntent({ ...input, draft: draftInfo.draft });
      if (
        receipt.intent_id !== input.intentId ||
        receipt.receipt_id !== input.receiptId ||
        receipt.command_id !== draftInfo.draft.admission.command.commandId ||
        !receipt.receipt_id
      )
        return yield* integrity();
      let format: "legacyV1" | "receiptV2" | "materialV3" | "authorityV4";
      let retainedMaterial: Material | undefined;
      let result: object;
      if (draftInfo.marked) {
        const fingerprint = yield* Effect.try({
          try: () =>
            WorkflowCanonicalHash.tagged(
              draftInfo.authority
                ? "t3.workflow-native-launch-intent.v4"
                : "t3.workflow-native-launch-intent.v3",
              {
                intentId: input.intentId,
                workflowRunId: input.runId,
                planningSessionId: null,
                operationId: input.operationId,
                attemptId: input.attemptId,
                passId: null,
                itemId: input.itemId,
                chargeAttempt: 1,
                repair: input.repair,
                role: "implementation",
                environmentId: input.environmentId,
                projectId: input.projectId,
                launchCommandId: input.launchCommandId,
                threadId: input.threadId,
                messageId: input.messageId,
                draft: storedDraft,
                receiptId: input.receiptId,
              },
            ),
          catch: integrity,
        });
        if (fingerprint !== receipt.fingerprint) return yield* integrity();
        const rows = yield* sql<{
          material_json: string;
          material_hash: string;
        }>`SELECT material_json, material_hash FROM workflow_native_reservation_material WHERE intent_id = ${input.intentId}`;
        if (rows.length !== 1 || !rows[0]) return yield* integrity();
        const raw = yield* Effect.try({
          try: () => parse(rows[0]!.material_json),
          catch: integrity,
        });
        const { material, launchHash, materialHash, authorityHash } =
          yield* decodeNativeReservationMaterial(raw).pipe(Effect.mapError(integrity));
        const launch = material.launch;
        const params = validated.operation.parameters;
        if (
          material.version !== (draftInfo.authority ? 2 : 1) ||
          (material.version === 2 &&
            (authorityHash !== draftInfo.marker.authorityHash ||
              material.authority.environmentId !== input.environmentId ||
              validated.operation.origin.kind !== "human" ||
              material.authority.subject !== validated.operation.origin.id)) ||
          material.planningSession !== null ||
          material.intentId !== input.intentId ||
          material.receiptId !== input.receiptId ||
          material.intentFingerprint !== fingerprint ||
          rows[0].material_hash !== materialHash ||
          draftInfo.marker.launchHash !== launchHash ||
          launch.commandId !== input.launchCommandId ||
          launch.threadId !== input.threadId ||
          launch.initialMessage.messageId !== input.messageId ||
          launch.projectId !== input.projectId ||
          params.kind !== "stage_launch" ||
          launch.modelSelection.instanceId !== params.provider.providerInstanceId ||
          launch.modelSelection.model !== params.provider.model
        )
          return yield* integrity();
        format = draftInfo.authority ? "authorityV4" : "materialV3";
        retainedMaterial = material;
        result = {
          intentId: input.intentId,
          fingerprint,
          materialVersion: material.version,
          materialHash,
        };
      } else {
        if (receipt.fingerprint === validated.fingerprint) format = "receiptV2";
        else if (receipt.fingerprint === validated.legacyFingerprint) format = "legacyV1";
        else return yield* integrity();
        result = { intentId: input.intentId, fingerprint: receipt.fingerprint };
      }
      const matches = yield* Effect.try({
        try: () => sameCustodyValue(parse(receipt.result_json), result),
        catch: integrity,
      });
      if (!matches) return yield* integrity();
      return {
        format,
        operation: validated.operation,
        draft: draftInfo.draft,
        material: retainedMaterial,
      };
    },
  );
  type CustodyOwner = WorkflowOperationV1["owner"];
  type IntentRow = {
    intent_id: string;
    workflow_run_id: string | null;
    planning_session_id: S["id"] | null;
    operation_id: WorkflowOperationV1["id"];
    attempt_id: string | null;
    pass_id: P["id"] | null;
    item_id: string | null;
    charge_attempt: number | null;
    repair: number | null;
    role: string;
    environment_id: string;
    project_id: string;
    launch_command_id: string;
    thread_id: string;
    message_id: string;
    fingerprint: string;
    draft_json: string;
    generation: number | null;
    state: string;
    held: number | null;
    boot_id: string | null;
    worker_id: string | null;
    native_run_id: string | null;
    finalized_operation_id: string | null;
  };
  type OperationRow = {
    id: string;
    workflow_run_id: string | null;
    planning_session_id: string | null;
    kind: string;
    fingerprint: string;
    status: string;
    generation: number | null;
    held: number | null;
    cancel_requested: number | null;
    next_due_at: string | null;
    record_json: string;
  };
  type PassRow = {
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
  };
  const sameCustodyValue = (a: unknown, b: unknown) =>
    WorkflowCanonicalHash.tagged("compare", a) === WorkflowCanonicalHash.tagged("compare", b);
  const readThreadCustodyInTransaction = Effect.fn(
    "WorkflowRunStore.readThreadCustodyInTransaction",
  )(
    function* (threadId: string) {
      if (typeof threadId !== "string" || threadId.length === 0) return yield* integrity();
      let owner: CustodyOwner | undefined;
      const addOwner = Effect.fn("WorkflowRunStore.validateCustodyOwner")(function* (
        candidate: CustodyOwner,
        operation: WorkflowOperationV1,
      ) {
        const id =
          candidate.kind === "workflow_run" ? candidate.workflowRunId : candidate.planningSessionId;
        if (owner && !sameCustodyValue(owner, candidate)) return yield* integrity();
        const rows =
          candidate.kind === "workflow_run"
            ? yield* sql<{
                id: string;
                record_json: string;
                status: string;
                record_version: number;
                control_generation: number;
                spec_hash: string;
                plan_id: string;
                revision_id: string;
                revision_hash: string;
                source_packet_hash: string;
              }>`SELECT id, record_json, status, CASE WHEN typeof(record_version) = 'integer' AND record_version BETWEEN 0 AND 9007199254740991 THEN record_version ELSE NULL END AS record_version, CASE WHEN typeof(control_generation) = 'integer' AND control_generation BETWEEN 0 AND 9007199254740991 THEN control_generation ELSE NULL END AS control_generation, spec_hash, plan_id, revision_id, revision_hash, source_packet_hash FROM workflow_runs WHERE id = ${id}`
            : yield* sql<{
                id: string;
                record_json: string;
                status: string;
                record_version: number;
                control_generation: number;
                plan_id: string;
                predecessor_id: string | null;
              }>`SELECT id, record_json, status, CASE WHEN typeof(record_version) = 'integer' AND record_version BETWEEN 0 AND 9007199254740991 THEN record_version ELSE NULL END AS record_version, CASE WHEN typeof(control_generation) = 'integer' AND control_generation BETWEEN 0 AND 9007199254740991 THEN control_generation ELSE NULL END AS control_generation, plan_id, predecessor_id FROM workflow_planning_sessions WHERE id = ${id}`;
        const row = rows[0];
        if (rows.length !== 1 || !row) return yield* integrity();
        const raw = yield* Effect.try({ try: () => parse(row.record_json), catch: integrity });
        if (candidate.kind === "workflow_run") {
          const value = yield* decodeWorkflowRunV1(raw).pipe(Effect.mapError(integrity));
          if (
            value.id !== id ||
            value.status !== row.status ||
            value.recordVersion !== row.record_version ||
            value.controlGeneration !== row.control_generation ||
            !("spec_hash" in row) ||
            value.spec.ref.sha256 !== row.spec_hash ||
            !("revision_id" in row) ||
            value.spec.plan.planId !== row.plan_id ||
            value.spec.plan.revisionId !== row.revision_id ||
            value.spec.plan.revisionHash !== row.revision_hash ||
            value.spec.plan.sourcePacketHash !== row.source_packet_hash
          )
            return yield* integrity();
        } else {
          const value = yield* decodePlanningSessionV1(raw).pipe(Effect.mapError(integrity));
          if (
            value.id !== id ||
            value.status !== row.status ||
            value.recordVersion !== row.record_version ||
            value.controlGeneration !== row.control_generation ||
            !("predecessor_id" in row) ||
            value.planId !== row.plan_id ||
            (value.predecessorSessionId ?? null) !== row.predecessor_id ||
            !sameContext(value, { actor: operation.origin, destination: operation.destination })
          )
            return yield* integrity();
        }
        owner = candidate;
      });
      const readOperation = Effect.fn("WorkflowRunStore.readCustodyOperation")(function* (
        row: OperationRow,
      ) {
        const raw = yield* Effect.try({ try: () => parse(row.record_json), catch: integrity });
        const operation = yield* decodeWorkflowOperationV1(raw).pipe(Effect.mapError(integrity));
        const canonical = yield* Effect.try({
          try: () => WorkflowCanonicalHash.operation(operation),
          catch: integrity,
        });
        if (
          operation.id !== row.id ||
          operation.kind !== row.kind ||
          operation.fingerprint !== row.fingerprint ||
          operation.fingerprint !== canonical ||
          operation.status !== row.status ||
          operation.controlGeneration !== row.generation ||
          row.generation === null ||
          !Number.isSafeInteger(row.generation) ||
          row.generation < 0 ||
          row.held === null ||
          row.cancel_requested === null ||
          ![0, 1].includes(row.held) ||
          ![0, 1].includes(row.cancel_requested) ||
          (row.held === 1 && row.status !== "pending") ||
          (operation.nextDueAt ?? null) !== row.next_due_at ||
          (operation.owner.kind === "workflow_run"
            ? operation.owner.workflowRunId !== row.workflow_run_id ||
              row.planning_session_id !== null
            : operation.owner.planningSessionId !== row.planning_session_id ||
              row.workflow_run_id !== null)
        )
          return yield* integrity();
        yield* addOwner(operation.owner, operation);
        return operation;
      });
      const readPass = Effect.fn("WorkflowRunStore.readCustodyPass")(function* (row: PassRow) {
        const raw = yield* Effect.try({ try: () => parse(row.record_json), catch: integrity });
        const pass = yield* decodePlanningPassV1(raw).pipe(Effect.mapError(integrity));
        const operations =
          yield* sql<OperationRow>`SELECT id, workflow_run_id, planning_session_id, kind, fingerprint, status, CASE WHEN typeof(generation) = 'integer' AND generation BETWEEN 0 AND 9007199254740991 THEN generation ELSE NULL END AS generation, CASE WHEN typeof(held) = 'integer' AND held IN (0, 1) THEN held ELSE NULL END AS held, CASE WHEN typeof(cancel_requested) = 'integer' AND cancel_requested IN (0, 1) THEN cancel_requested ELSE NULL END AS cancel_requested, next_due_at, record_json FROM workflow_operations WHERE id = ${row.operation_id}`;
        if (operations.length !== 1 || !operations[0]) return yield* integrity();
        const operation = yield* readOperation(operations[0]);
        const sessions = yield* sql<{
          record_json: string;
        }>`SELECT record_json FROM workflow_planning_sessions WHERE id = ${row.planning_session_id}`;
        if (sessions.length !== 1 || !sessions[0]) return yield* integrity();
        const sessionRaw = yield* Effect.try({
          try: () => parse(sessions[0]!.record_json),
          catch: integrity,
        });
        const session = yield* decodePlanningSessionV1(sessionRaw).pipe(Effect.mapError(integrity));
        const params = operation.parameters;
        const consistent = yield* Effect.try({
          try: () =>
            pass.id === row.id &&
            pass.planningSessionId === row.planning_session_id &&
            pass.operationId === row.operation_id &&
            pass.binding.planId === row.plan_id &&
            pass.binding.planId === session.planId &&
            pass.binding.revisionId === row.revision_id &&
            pass.binding.revisionHash === row.revision_hash &&
            pass.binding.sourcePacketHash === row.source_packet_hash &&
            pass.native?.threadId === row.native_thread_id &&
            pass.native?.runId === row.native_run_id &&
            row.native_thread_id === threadId &&
            operation.owner.kind === "planning_session" &&
            operation.owner.planningSessionId === row.planning_session_id &&
            operation.kind === "planning_pass" &&
            params.kind === "planning_pass" &&
            params.passId === pass.id &&
            params.role === pass.role &&
            sameCustodyValue(params.binding, pass.binding) &&
            sameCustodyValue(params.provider, pass.provider) &&
            sameCustodyValue(params.limits, session.limits) &&
            params.inputManifestHash ===
              WorkflowCanonicalHash.tagged("t3.planning-input-manifest.v1", pass.inputManifest) &&
            operation.status === pass.status,
          catch: integrity,
        });
        if (!consistent) return yield* integrity();
        return { pass, operation };
      });
      const intents =
        yield* sql<IntentRow>`SELECT intent_id, workflow_run_id, planning_session_id, operation_id, attempt_id, pass_id, item_id, CASE WHEN typeof(charge_attempt) = 'integer' AND charge_attempt IN (0, 1) THEN charge_attempt ELSE NULL END AS charge_attempt, CASE WHEN typeof(repair) = 'integer' AND repair IN (0, 1) THEN repair ELSE NULL END AS repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, CASE WHEN typeof(generation) = 'integer' AND generation BETWEEN 0 AND 9007199254740991 THEN generation ELSE NULL END AS generation, state, CASE WHEN typeof(held) = 'integer' AND held IN (0, 1) THEN held ELSE NULL END AS held, boot_id, worker_id, native_run_id, finalized_operation_id FROM workflow_native_launch_intents WHERE thread_id = ${threadId}`;
      for (const row of intents) {
        if (
          row.generation === null ||
          !Number.isSafeInteger(row.generation) ||
          row.generation < 0 ||
          row.held === null ||
          row.repair === null ||
          row.charge_attempt === null ||
          ![0, 1].includes(row.held) ||
          ![0, 1].includes(row.repair) ||
          !["reserved", "dispatching", "unknown", "finalized", "cancelled"].includes(row.state) ||
          (row.held === 1 && row.state !== "reserved") ||
          (row.boot_id === null) !== (row.worker_id === null) ||
          (row.boot_id !== null && (!row.boot_id || !row.worker_id)) ||
          (row.native_run_id !== null && !row.native_run_id) ||
          ((row.state === "reserved" || row.state === "cancelled") &&
            (row.boot_id !== null ||
              row.worker_id !== null ||
              row.native_run_id !== null ||
              row.finalized_operation_id !== null)) ||
          ((row.state === "dispatching" || row.state === "unknown") &&
            (row.boot_id === null || row.finalized_operation_id !== null)) ||
          (row.state === "finalized" &&
            (!row.native_run_id || row.finalized_operation_id !== row.operation_id))
        )
          return yield* integrity();
        const receipts = yield* sql<{
          command_id: string;
          receipt_id: string;
          intent_id: string;
          fingerprint: string;
          result_json: string;
        }>`SELECT * FROM workflow_native_intent_receipts WHERE intent_id = ${row.intent_id}`;
        const receipt = receipts[0];
        if (
          receipts.length !== 1 ||
          !receipt ||
          !receipt.command_id ||
          !receipt.receipt_id ||
          receipt.intent_id !== row.intent_id ||
          receipt.fingerprint !== row.fingerprint
        )
          return yield* integrity();
        const draft = yield* Effect.try({ try: () => parse(row.draft_json), catch: integrity });
        let draftOperation: WorkflowOperationV1;
        let draftPass: P | undefined;
        if (
          row.workflow_run_id !== null &&
          row.planning_session_id === null &&
          row.attempt_id !== null &&
          row.item_id !== null &&
          row.pass_id === null &&
          row.charge_attempt === 1 &&
          row.role === "implementation"
        ) {
          const validated = yield* verifyStoredWorkflowIntent(
            {
              intentId: row.intent_id,
              runId: row.workflow_run_id,
              operationId: row.operation_id,
              attemptId: row.attempt_id,
              itemId: row.item_id,
              repair: row.repair === 1,
              environmentId: row.environment_id,
              projectId: row.project_id,
              launchCommandId: row.launch_command_id,
              threadId: row.thread_id,
              messageId: row.message_id,
              receiptId: receipt.receipt_id,
              generation: row.generation,
            },
            draft,
            receipt,
          );
          draftOperation = validated.operation;
        } else if (
          row.workflow_run_id === null &&
          row.planning_session_id !== null &&
          row.attempt_id === null &&
          row.item_id === null &&
          row.pass_id !== null &&
          row.charge_attempt === 0
        ) {
          if (row.repair !== (row.role === "repair" ? 1 : 0)) return yield* integrity();
          const validated = yield* verifyStoredPlanningIntent(
            {
              intentId: row.intent_id,
              sessionId: row.planning_session_id,
              operationId: row.operation_id,
              passId: row.pass_id,
              role: row.role as P["role"],
              environmentId: row.environment_id,
              projectId: row.project_id,
              launchCommandId: row.launch_command_id,
              threadId: row.thread_id,
              messageId: row.message_id,
              generation: row.generation,
            },
            draft,
            receipt,
          ).pipe(Effect.mapError((error) => (isPlanningError(error) ? integrity() : error)));
          draftOperation = validated.operation;
          draftPass = validated.pass;
        } else return yield* integrity();
        yield* addOwner(draftOperation.owner, draftOperation);
        if (row.state === "finalized") {
          const operations =
            yield* sql<OperationRow>`SELECT id, workflow_run_id, planning_session_id, kind, fingerprint, status, CASE WHEN typeof(generation) = 'integer' AND generation BETWEEN 0 AND 9007199254740991 THEN generation ELSE NULL END AS generation, CASE WHEN typeof(held) = 'integer' AND held IN (0, 1) THEN held ELSE NULL END AS held, CASE WHEN typeof(cancel_requested) = 'integer' AND cancel_requested IN (0, 1) THEN cancel_requested ELSE NULL END AS cancel_requested, next_due_at, record_json FROM workflow_operations WHERE id = ${row.operation_id}`;
          if (operations.length !== 1 || !operations[0]) return yield* integrity();
          const operation = yield* readOperation(operations[0]);
          const immutableOperation = (value: WorkflowOperationV1) => {
            const {
              fingerprint: _fingerprint,
              controlGeneration: _generation,
              status: _status,
              claim: _claim,
              outcome: _outcome,
              nextDueAt: _nextDueAt,
              ...base
            } = value;
            if (base.parameters.kind === "stage_launch") {
              const { nativeRunId: _nativeRunId, ...parameters } = base.parameters;
              return { ...base, parameters };
            }
            return base;
          };
          const matches = yield* Effect.try({
            try: () =>
              sameCustodyValue(immutableOperation(operation), immutableOperation(draftOperation)),
            catch: integrity,
          });
          if (!matches) return yield* integrity();
          if (
            operation.parameters.kind === "stage_launch" &&
            (operation.parameters.nativeThreadId !== threadId ||
              operation.parameters.nativeRunId !== row.native_run_id)
          )
            return yield* integrity();
          if (draftPass) {
            const passes =
              yield* sql<PassRow>`SELECT * FROM workflow_planning_passes WHERE id = ${row.pass_id}`;
            if (passes.length !== 1 || !passes[0]) return yield* integrity();
            const linked = yield* readPass(passes[0]);
            const immutablePass = (value: P) => {
              const { native: _native, status: _status, report: _report, ...base } = value;
              return base;
            };
            const passMatches = yield* Effect.try({
              try: () => sameCustodyValue(immutablePass(linked.pass), immutablePass(draftPass)),
              catch: integrity,
            });
            if (!passMatches || linked.pass.native?.runId !== row.native_run_id)
              return yield* integrity();
          }
        }
      }
      const operations =
        yield* sql<OperationRow>`SELECT id, workflow_run_id, planning_session_id, kind, fingerprint, status, CASE WHEN typeof(generation) = 'integer' AND generation BETWEEN 0 AND 9007199254740991 THEN generation ELSE NULL END AS generation, CASE WHEN typeof(held) = 'integer' AND held IN (0, 1) THEN held ELSE NULL END AS held, CASE WHEN typeof(cancel_requested) = 'integer' AND cancel_requested IN (0, 1) THEN cancel_requested ELSE NULL END AS cancel_requested, next_due_at, record_json FROM workflow_operations WHERE CASE WHEN json_valid(record_json) THEN json_extract(record_json, '$.parameters.nativeThreadId') END = ${threadId}`;
      for (const row of operations) {
        const operation = yield* readOperation(row);
        if (
          (operation.parameters.kind !== "stage_launch" &&
            operation.parameters.kind !== "supervision") ||
          operation.parameters.nativeThreadId !== threadId ||
          operation.owner.kind !== "workflow_run"
        )
          return yield* integrity();
      }
      const passes =
        yield* sql<PassRow>`SELECT * FROM workflow_planning_passes WHERE native_thread_id = ${threadId}`;
      for (const row of passes) yield* readPass(row);
      return owner ? { kind: "owned" as const, owner } : { kind: "ordinary" as const };
    },
    Effect.mapError((error) =>
      isStoreError(error) ? error : new WorkflowRunStoreError({ reason: "persistence" }),
    ),
  );
  const readThreadCustody = Effect.fn("WorkflowRunStore.readThreadCustody")(function* (input: {
    readonly threadId: string;
  }) {
    return yield* inTransaction(readThreadCustodyInTransaction(input.threadId));
  });
  const readNativeReservationInTransaction = Effect.fn(
    "WorkflowRunStore.readNativeReservationInTransaction",
  )(
    function* (
      input: { readonly intentId: string },
      validatedThreads?: Map<
        string,
        Effect.Success<ReturnType<typeof readThreadCustodyInTransaction>>
      >,
    ) {
      if (typeof input !== "object" || input === null || Object.keys(input).join() !== "intentId")
        return yield* integrity();
      const intentId = input.intentId;
      if (typeof intentId !== "string" || intentId.length === 0) return yield* integrity();
      if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
        return yield* integrity();
      const rows =
        yield* sql<IntentRow>`SELECT * FROM workflow_native_launch_intents WHERE intent_id = ${intentId}`;
      const row = rows[0];
      if (rows.length !== 1 || !row) return yield* integrity();
      const custody =
        validatedThreads?.get(row.thread_id) ??
        (yield* readThreadCustodyInTransaction(row.thread_id));
      validatedThreads?.set(row.thread_id, custody);
      const owner =
        row.workflow_run_id !== null && row.planning_session_id === null
          ? { kind: "workflow_run" as const, workflowRunId: row.workflow_run_id }
          : row.workflow_run_id === null && row.planning_session_id !== null
            ? { kind: "planning_session" as const, planningSessionId: row.planning_session_id }
            : undefined;
      if (!owner || custody.kind !== "owned" || !sameCustodyValue(custody.owner, owner))
        return yield* integrity();
      const draft = yield* Effect.try({ try: () => parse(row.draft_json), catch: integrity });
      if (
        typeof draft !== "object" ||
        draft === null ||
        !("version" in draft) ||
        (draft.version !== 2 && draft.version !== 3)
      )
        return yield* integrity();
      const receipts = yield* sql<{
        command_id: string;
        receipt_id: string;
        fingerprint: string;
      }>`SELECT command_id, receipt_id, fingerprint FROM workflow_native_intent_receipts WHERE intent_id = ${intentId}`;
      const materials = yield* sql<{
        material_json: string;
        material_hash: string;
      }>`SELECT material_json, material_hash FROM workflow_native_reservation_material WHERE intent_id = ${intentId}`;
      if (receipts.length !== 1 || !receipts[0] || materials.length !== 1 || !materials[0])
        return yield* integrity();
      const raw = yield* Effect.try({
        try: () => parse(materials[0]!.material_json),
        catch: integrity,
      });
      const decoded = yield* decodeNativeReservationMaterial(raw).pipe(Effect.mapError(integrity));
      if (
        decoded.materialHash !== materials[0].material_hash ||
        decoded.material.intentId !== row.intent_id ||
        decoded.material.intentFingerprint !== row.fingerprint ||
        receipts[0].fingerprint !== row.fingerprint ||
        decoded.material.intentFingerprint !== receipts[0].fingerprint ||
        decoded.material.receiptId !== receipts[0].receipt_id
      )
        return yield* integrity();
      return {
        ...decoded,
        intentId: row.intent_id,
        receiptId: receipts[0].receipt_id,
        reservationCommandId: receipts[0].command_id,
        fingerprint: row.fingerprint,
        owner,
        operationId: row.operation_id,
        attemptId: row.attempt_id,
        passId: row.pass_id,
        itemId: row.item_id,
        chargeAttempt: row.charge_attempt === 1,
        repair: row.repair === 1,
        role: row.role,
        destination: { environmentId: row.environment_id, projectId: row.project_id },
        launchCommandId: row.launch_command_id,
        threadId: row.thread_id,
        messageId: row.message_id,
        generation: row.generation!,
        state: row.state,
        held: row.held === 1,
        bootId: row.boot_id,
        workerId: row.worker_id,
        nativeRunId: row.native_run_id,
        finalizedOperationId: row.finalized_operation_id,
      };
    },
    Effect.mapError((error) =>
      isStoreError(error) ? error : new WorkflowRunStoreError({ reason: "persistence" }),
    ),
  );
  const readRecoveryCustodyInTransaction = Effect.fn(
    "WorkflowRunStore.readRecoveryCustodyInTransaction",
  )(function* (input: {
    readonly threadId: string;
    readonly providerSessionIds: readonly string[];
    readonly providerThreadIds?: readonly string[];
  }) {
    const owned = yield* walkResourceCustodyInTransaction({
      purpose: "workflow",
      threadId: input.threadId,
      providerSessionIds: input.providerSessionIds,
      ...(input.providerThreadIds === undefined
        ? {}
        : { providerThreadIds: input.providerThreadIds }),
    });
    return owned ? { kind: "owned" as const } : { kind: "ordinary" as const };
  });

  const reserveWorkflowNativeIntent = Effect.fn("WorkflowRunStore.reserveWorkflowNativeIntent")(
    function* (request: WorkflowNativeInput) {
      const input = yield* Effect.try({
        try: () => {
          const copy = { ...request };
          WorkflowCanonicalHash.assertCanonical(copy.draft);
          const snapshot = { ...copy, draft: structuredClone(copy.draft) };
          if (!Object.hasOwn(copy, "launch")) return snapshot;
          WorkflowCanonicalHash.assertCanonical(copy.launch);
          return {
            ...snapshot,
            launch: structuredClone(copy.launch),
            ...(copy.materialVersion === 2 ? { authority: structuredClone(copy.authority) } : {}),
          };
        },
        catch: integrity,
      });
      const work = Effect.gen(function* () {
        const { draft } = input;
        const {
          operation,
          fingerprint: v2Fingerprint,
          legacyFingerprint,
        } = yield* validateWorkflowNativeIntent(input);
        const hasVersion = Object.hasOwn(input, "materialVersion");
        const hasLaunch = Object.hasOwn(input, "launch");
        if (
          hasVersion !== hasLaunch ||
          (hasVersion && input.materialVersion !== 1 && input.materialVersion !== 2) ||
          Object.hasOwn(input, "authority") !== (input.materialVersion === 2)
        )
          return yield* integrity();
        const retained = yield* Effect.gen(function* () {
          if (!hasVersion) return null;
          const { launch, launchHash } = yield* decodeNativeReservationLaunch(input.launch).pipe(
            Effect.mapError(integrity),
          );
          const params = operation.parameters;
          if (
            launch.commandId !== input.launchCommandId ||
            launch.threadId !== input.threadId ||
            launch.initialMessage.messageId !== input.messageId ||
            launch.projectId !== input.projectId ||
            params.kind !== "stage_launch" ||
            launch.modelSelection.instanceId !== params.provider.providerInstanceId ||
            launch.modelSelection.model !== params.provider.model
          )
            return yield* integrity();
          const grant =
            input.materialVersion === 2
              ? yield* decodeNativeReservationAuthority(input.authority).pipe(
                  Effect.mapError(integrity),
                )
              : undefined;
          const persistedDraft = {
            ...draft,
            version: grant ? 3 : 2,
            material: grant
              ? { version: 2, launchHash, authorityHash: grant.authorityHash }
              : { version: 1, launchHash },
          };
          const fingerprint = yield* Effect.try({
            try: () =>
              WorkflowCanonicalHash.tagged(
                grant
                  ? "t3.workflow-native-launch-intent.v4"
                  : "t3.workflow-native-launch-intent.v3",
                {
                  intentId: input.intentId,
                  workflowRunId: input.runId,
                  planningSessionId: null,
                  operationId: input.operationId,
                  attemptId: input.attemptId,
                  passId: null,
                  itemId: input.itemId,
                  chargeAttempt: 1,
                  repair: input.repair,
                  role: "implementation",
                  environmentId: input.environmentId,
                  projectId: input.projectId,
                  launchCommandId: input.launchCommandId,
                  threadId: input.threadId,
                  messageId: input.messageId,
                  draft: persistedDraft,
                  receiptId: input.receiptId,
                },
              ),
            catch: integrity,
          });
          const decoded = yield* decodeNativeReservationMaterial({
            version: grant ? 2 : 1,
            ...(grant ? { authority: grant.authority } : {}),
            intentId: input.intentId,
            receiptId: input.receiptId,
            intentFingerprint: fingerprint,
            launch,
            planningSession: null,
          }).pipe(Effect.mapError(integrity));
          return {
            persistedDraft,
            fingerprint,
            ...decoded,
            persistedDraftJson: encode(persistedDraft),
            materialJson: encode(decoded.material),
          };
        });
        const receipts = yield* sql<{
          command_id: string;
          receipt_id: string;
          intent_id: string;
          fingerprint: string;
          result_json: string;
        }>`SELECT * FROM workflow_native_intent_receipts WHERE command_id = ${draft.admission.command.commandId} OR receipt_id = ${input.receiptId} OR intent_id = ${input.intentId}`;
        const intents = yield* sql<{
          intent_id: string;
          workflow_run_id: string | null;
          planning_session_id: string | null;
          operation_id: string;
          attempt_id: string | null;
          pass_id: string | null;
          item_id: string | null;
          charge_attempt: number;
          repair: number;
          role: string;
          environment_id: string;
          project_id: string;
          launch_command_id: string;
          thread_id: string;
          message_id: string;
          fingerprint: string;
          draft_json: string;
        }>`SELECT * FROM workflow_native_launch_intents WHERE intent_id = ${input.intentId}`;
        if (receipts.length || intents.length) {
          const receipt = receipts[0];
          const intent = intents[0];
          // V1 is replay-only; current control authority requires the receipt-anchored V2 hash.
          const replayFingerprint = receipt?.fingerprint;
          if (
            receipts.length !== 1 ||
            intents.length !== 1 ||
            !receipt ||
            !intent ||
            receipt.command_id !== draft.admission.command.commandId ||
            receipt.receipt_id !== input.receiptId ||
            receipt.intent_id !== input.intentId ||
            (retained
              ? replayFingerprint !== retained.fingerprint
              : replayFingerprint !== v2Fingerprint && replayFingerprint !== legacyFingerprint) ||
            intent.workflow_run_id !== input.runId ||
            intent.planning_session_id !== null ||
            intent.operation_id !== input.operationId ||
            intent.attempt_id !== input.attemptId ||
            intent.pass_id !== null ||
            intent.item_id !== input.itemId ||
            intent.charge_attempt !== 1 ||
            intent.repair !== (input.repair ? 1 : 0) ||
            intent.role !== "implementation" ||
            intent.environment_id !== input.environmentId ||
            intent.project_id !== input.projectId ||
            intent.launch_command_id !== input.launchCommandId ||
            intent.thread_id !== input.threadId ||
            intent.message_id !== input.messageId ||
            intent.fingerprint !== replayFingerprint ||
            (yield* Effect.try({
              try: () =>
                sameCustodyValue(parse(intent.draft_json), retained?.persistedDraft ?? draft),
              catch: integrity,
            })) === false
          )
            return yield* integrity();
          const verified = yield* verifyStoredWorkflowIntent(
            input,
            yield* Effect.try({
              try: () => parse(intent.draft_json),
              catch: integrity,
            }),
            receipt,
          );
          if (["materialV3", "authorityV4"].includes(verified.format) !== (retained !== null))
            return yield* integrity();
          return {
            kind: "replay" as const,
            receiptId: receipt.receipt_id,
            intentId: input.intentId,
            fingerprint: replayFingerprint,
          };
        }
        if (!retained) return yield* integrity();
        const { fingerprint } = retained;
        if (yield* runningNativeEffect(input.threadId))
          return { kind: "rejected" as const, reason: "custody" as const };
        if (
          yield* readSubmissionCustodyInTransaction(
            input.environmentId,
            input.threadId,
            operation.owner,
            nativeFromLaunch(retained.material.launch),
          )
        )
          return { kind: "rejected" as const, reason: "custody" as const };
        const custody = yield* readThreadCustodyInTransaction(input.threadId);
        if (
          custody.kind === "owned" &&
          (custody.owner.kind !== "workflow_run" || custody.owner.workflowRunId !== input.runId)
        )
          return { kind: "rejected" as const, reason: "custody" as const };
        const collisions =
          yield* sql`SELECT 1 FROM workflow_command_receipts WHERE command_id IN (${draft.admission.command.commandId}, ${input.launchCommandId}, ${input.launchCommandId + ":initial-message"}) OR receipt_id = ${input.receiptId} OR operation_id = ${input.operationId} UNION ALL SELECT 1 FROM workflow_operations WHERE id = ${input.operationId} UNION ALL SELECT 1 FROM workflow_attempts WHERE id = ${input.attemptId} UNION ALL SELECT 1 FROM workflow_native_launch_intents WHERE operation_id = ${input.operationId} OR (charge_attempt = 1 AND attempt_id = ${input.attemptId}) OR launch_command_id IN (${input.launchCommandId}, ${draft.admission.command.commandId}, ${input.launchCommandId + ":initial-message"}) OR launch_command_id || ':initial-message' IN (${input.launchCommandId}, ${draft.admission.command.commandId}) OR (thread_id = ${input.threadId} AND message_id = ${input.messageId}) UNION ALL SELECT 1 FROM workflow_native_intent_receipts WHERE command_id IN (${input.launchCommandId}, ${input.launchCommandId + ":initial-message"})`;
        if (collisions.length) return yield* integrity();
        const rows = yield* sql<{
          record_json: string;
          record_version: number;
          control_generation: number;
          spec_hash: string;
          status: string;
        }>`SELECT record_json, record_version, control_generation, spec_hash, status FROM workflow_runs WHERE id = ${input.runId}`;
        const row = rows[0];
        if (!row) return yield* integrity();
        const run = yield* Effect.try({ try: () => parse(row.record_json), catch: integrity }).pipe(
          Effect.flatMap(decodeWorkflowRunV1),
          Effect.mapError(integrity),
        );
        if (
          run.id !== input.runId ||
          run.recordVersion !== row.record_version ||
          run.controlGeneration !== row.control_generation ||
          run.spec.ref.sha256 !== row.spec_hash ||
          run.status !== row.status ||
          WorkflowCanonicalHash.tagged("compare", run.origin) !==
            WorkflowCanonicalHash.tagged("compare", operation.origin) ||
          !run.access.origins.some(
            (origin) =>
              origin.environmentId === input.environmentId && origin.projectId === input.projectId,
          )
        )
          return yield* integrity();
        const currentNow =
          retained.material.version === 2
            ? DateTime.formatIso(yield* DateTime.now)
            : input.externalFacts.now;
        if (retained.material.version === 2) {
          if (!(yield* checkWorkflowAuthority(retained.material, operation, run)))
            return { kind: "rejected" as const, reason: "unauthorized" as const };
        }
        if (input.generation !== run.controlGeneration)
          return { kind: "rejected" as const, reason: "stale" as const };
        const operations = yield* sql<{
          id: WorkflowOperationV1["id"];
          status: WorkflowOperationV1["status"];
          kind: string;
          stage: string | null;
        }>`SELECT id, status, kind, json_extract(record_json, '$.parameters.stage') AS stage FROM workflow_operations WHERE workflow_run_id = ${input.runId} ORDER BY rowid`;
        const attempts = yield* sql<{
          id: string;
          item_id: string;
          status: WorkflowOperationV1["status"];
          outcome: string | null;
        }>`SELECT a.id, a.item_id, o.status, json_extract(o.record_json, '$.outcome.kind') AS outcome FROM workflow_attempts a JOIN workflow_operations o ON o.id = a.operation_id AND o.workflow_run_id = a.workflow_run_id WHERE a.workflow_run_id = ${input.runId} ORDER BY a.rowid`;
        const activeIntents = yield* sql<{
          operation_id: WorkflowOperationV1["id"];
          attempt_id: string | null;
          state: string;
          role: string;
        }>`SELECT operation_id, attempt_id, state, role FROM workflow_native_launch_intents WHERE workflow_run_id = ${input.runId} AND state IN ('reserved','dispatching','unknown')`;
        const acceptances = yield* sql<{
          acceptance_key: string;
        }>`SELECT acceptance_key FROM workflow_acceptances WHERE workflow_run_id = ${input.runId} AND invalidated = 0 ORDER BY rowid`;
        const acceptedItemIds = acceptances
          .filter((a) => a.acceptance_key.startsWith("item:"))
          .map((a) => a.acceptance_key.slice(5));
        const { latest, state: currentAttemptState } = attemptFacts(attempts, input.itemId);
        if (
          latest?.status === "settled" &&
          latest.outcome !== "succeeded" &&
          latest.outcome !== "failed"
        )
          return yield* integrity();
        if (!isWorkflowTimestamp(input.externalFacts.now)) return yield* integrity();
        const ordered = input.externalFacts.orderedItemIds;
        if (
          ordered !== undefined &&
          (!Array.isArray(ordered) || ordered.some((id) => typeof id !== "string" || !id))
        )
          return yield* integrity();
        const index = ordered?.indexOf(input.itemId);
        const facts: Facts = {
          now: currentNow,
          authority: input.externalFacts.authority === true,
          capabilitiesCurrent: input.externalFacts.capabilitiesCurrent === true,
          compatibilityCurrent: input.externalFacts.compatibilityCurrent === true,
          fingerprintCurrent: input.externalFacts.fingerprintCurrent === true,
          originGrantCurrent: input.externalFacts.originGrantCurrent === true,
          heldOperations: [],
          ...(ordered ? { orderedItemIds: ordered } : {}),
          custody:
            activeIntents.some((i) => i.state === "unknown") ||
            operations.some((o) => o.status === "unknown")
              ? "unknown"
              : activeIntents.length || operations.some((o) => o.status === "claimed")
                ? "active"
                : "none",
          activeWriterCount:
            activeIntents.filter((i) => i.role === "implementation").length +
            operations.filter(
              (o) =>
                o.status === "claimed" && o.kind === "stage_launch" && o.stage === "implementation",
            ).length,
          knownAttemptIds: [
            ...attempts.map((a) => a.id),
            ...activeIntents.flatMap((i) => (i.attempt_id ? [i.attempt_id] : [])),
          ],
          knownOperationIds: [
            ...operations.map((o) => o.id),
            ...activeIntents.map((i) => i.operation_id),
          ],
          currentAttemptState: activeIntents.some((i) => i.state === "unknown")
            ? "unknown"
            : activeIntents.length
              ? "active"
              : currentAttemptState,
          ...(latest ? { currentAttemptItemId: latest.item_id } : {}),
          acceptedItemIds,
          predecessorAccepted:
            index !== undefined &&
            index >= 0 &&
            acceptedItemIds.length === index &&
            acceptedItemIds.every((id, i) => id === ordered?.[i]),
        };
        const baseCommand = { ...draft.admission.command, operationFingerprint: fingerprint };
        const command = {
          ...baseCommand,
          fingerprint: WorkflowCanonicalHash.command(run.id, baseCommand),
        };
        const decision = WorkflowRunPolicy.decide({ run, command, facts });
        if (decision.kind !== "accepted")
          return decision.kind === "replay" ? yield* integrity() : decision;
        yield* decodeWorkflowRunV1(decision.run).pipe(Effect.mapError(integrity));
        const action = decision.actions[0];
        if (
          decision.actions.length !== 1 ||
          action?.kind !== "reserve_attempt" ||
          action.operationId !== input.operationId ||
          action.fingerprint !== fingerprint ||
          action.attemptId !== input.attemptId ||
          action.itemId !== input.itemId ||
          action.repair !== input.repair
        )
          return yield* integrity();
        const updated =
          yield* sql`UPDATE workflow_runs SET status = ${decision.run.status}, record_version = ${decision.run.recordVersion}, control_generation = ${decision.run.controlGeneration}, spec_hash = ${decision.run.spec.ref.sha256}, record_json = ${encode(decision.run)} WHERE id = ${run.id} AND record_version = ${run.recordVersion} AND control_generation = ${run.controlGeneration} AND spec_hash = ${run.spec.ref.sha256} RETURNING id`;
        if (updated.length !== 1) return yield* stale();
        const inserted =
          yield* sql`INSERT INTO workflow_native_launch_intents (intent_id, workflow_run_id, operation_id, attempt_id, item_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, generation, state, held) VALUES (${input.intentId}, ${input.runId}, ${input.operationId}, ${input.attemptId}, ${input.itemId}, 1, ${input.repair ? 1 : 0}, 'implementation', ${input.environmentId}, ${input.projectId}, ${input.launchCommandId}, ${input.threadId}, ${input.messageId}, ${fingerprint}, ${retained.persistedDraftJson}, ${input.generation}, 'reserved', 0) RETURNING intent_id`;
        if (inserted.length !== 1) return yield* integrity();
        const material =
          yield* sql`INSERT INTO workflow_native_reservation_material (intent_id, material_json, material_hash) VALUES (${input.intentId}, ${retained.materialJson}, ${retained.materialHash}) RETURNING intent_id`;
        if (material.length !== 1) return yield* integrity();
        const receipt =
          yield* sql`INSERT INTO workflow_native_intent_receipts (command_id, receipt_id, intent_id, fingerprint, result_json) VALUES (${command.commandId}, ${input.receiptId}, ${input.intentId}, ${fingerprint}, ${encode({ intentId: input.intentId, fingerprint, materialVersion: retained.material.version, materialHash: retained.materialHash })}) RETURNING receipt_id`;
        if (receipt.length !== 1) return yield* integrity();
        return {
          kind: "reserved" as const,
          receiptId: input.receiptId,
          intentId: input.intentId,
          fingerprint,
        };
      });
      const result = yield* inTransaction(work);
      if (result.kind === "reserved") yield* notify;
      return result;
    },
  );
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
        planning_session_id: string | null;
        operation_id: string | null;
        fingerprint: string;
        receipt_id: string;
        result_json: string;
      }>`SELECT workflow_run_id, planning_session_id, operation_id, fingerprint, receipt_id, result_json FROM workflow_command_receipts WHERE command_id = ${input.command.commandId}`;
      // This helper shares the enclosing transaction span.
      const validateAdmissionReplay = Effect.fnUntraced(function* (
        receipt: {
          workflow_run_id: string | null;
          planning_session_id: string | null;
          operation_id: string | null;
          receipt_id: string;
          result_json: string;
        },
        command: Extract<Command, { kind: "admitAttempt" }>,
      ) {
        if (
          receipt.workflow_run_id !== input.runId ||
          receipt.planning_session_id !== null ||
          receipt.operation_id !== command.operationId
        )
          return false;
        const result = yield* Effect.try({
          try: () => parse(receipt.result_json),
          catch: integrity,
        });
        const validResult = yield* Effect.try({
          try: () => sameCustodyValue(result, { kind: "accepted", receiptId: receipt.receipt_id }),
          catch: integrity,
        });
        if (!validResult) return yield* integrity();
        const rows = yield* sql<{
          id: string;
          workflow_run_id: string | null;
          planning_session_id: string | null;
          fingerprint: string;
          record_json: string;
        }>`SELECT id, workflow_run_id, planning_session_id, fingerprint, record_json FROM workflow_operations WHERE id = ${command.operationId}`;
        const row = rows[0];
        if (rows.length !== 1 || !row) return yield* integrity();
        const raw = yield* Effect.try({ try: () => parse(row.record_json), catch: integrity });
        const stored = yield* decodeWorkflowOperationV1(raw).pipe(Effect.mapError(integrity));
        const valid = yield* Effect.try({
          try: () => WorkflowCanonicalHash.verifyOperation(stored),
          catch: integrity,
        });
        if (
          !valid ||
          stored.id !== row.id ||
          stored.fingerprint !== row.fingerprint ||
          row.workflow_run_id !== input.runId ||
          row.planning_session_id !== null ||
          stored.owner.kind !== "workflow_run" ||
          stored.owner.workflowRunId !== input.runId
        )
          return yield* integrity();
        const attempts = yield* sql<{
          id: string;
          workflow_run_id: string;
          operation_id: string;
          item_id: string;
          repair: number;
        }>`SELECT id, workflow_run_id, operation_id, item_id, repair FROM workflow_attempts WHERE operation_id = ${command.operationId}`;
        const attempt = attempts[0];
        if (
          attempts.length !== 1 ||
          !attempt ||
          attempt.workflow_run_id !== input.runId ||
          attempt.operation_id !== stored.id ||
          stored.parameters.kind === "planning_pass" ||
          stored.parameters.context.attemptId !== attempt.id ||
          stored.parameters.context.itemId !== attempt.item_id ||
          (attempt.repair !== 0 && attempt.repair !== 1)
        )
          return yield* integrity();
        if (
          attempt.id !== command.attemptId ||
          attempt.item_id !== command.itemId ||
          attempt.repair !== (command.repair ? 1 : 0) ||
          command.operationFingerprint !== stored.fingerprint
        )
          return false;
        if (input.operation !== undefined) {
          const decoded = yield* Effect.result(decodeWorkflowOperationV1(input.operation));
          if (decoded._tag === "Failure") return false;
          const supplied = decoded.success;
          const validSupplied = yield* Effect.try({
            try: () => WorkflowCanonicalHash.verifyOperation(supplied),
            catch: integrity,
          });
          if (!validSupplied) return false;
          const immutable = (operation: WorkflowOperationV1) => {
            const {
              fingerprint: _fingerprint,
              controlGeneration: _generation,
              status: _status,
              claim: _claim,
              outcome: _outcome,
              nextDueAt: _nextDueAt,
              ...value
            } = operation;
            return value;
          };
          if (!sameCustodyValue(immutable(stored), immutable(supplied))) return false;
        }
        return true;
      });
      const prior = receipts[0];
      if (prior) {
        if (prior.workflow_run_id !== input.runId || prior.fingerprint !== fingerprint)
          return yield* integrity();
        if (
          input.command.kind === "admitAttempt" &&
          !(yield* validateAdmissionReplay(prior, input.command))
        )
          return yield* integrity();
        return { kind: "replay" as const, receiptId: prior.receipt_id };
      }
      if (input.command.kind === "admitAttempt") {
        const operationReceipts = yield* sql<{
          workflow_run_id: string | null;
          planning_session_id: string | null;
          operation_id: string | null;
          receipt_id: string;
          result_json: string;
        }>`SELECT workflow_run_id, planning_session_id, operation_id, receipt_id, result_json FROM workflow_command_receipts WHERE workflow_run_id = ${input.runId} AND operation_id = ${input.command.operationId}`;
        const operationReceipt = operationReceipts[0];
        if (operationReceipt) {
          if (operationReceipts.length !== 1) return yield* integrity();
          return (yield* validateAdmissionReplay(operationReceipt, input.command))
            ? { kind: "replay" as const, receiptId: operationReceipt.receipt_id }
            : { kind: "rejected" as const, reason: "integrity" as const };
        }
      }
      if (input.command.kind === "admitAttempt") {
        const admissionCommand = input.command;
        const conflicts = yield* sql<{
          workflow_run_id: string | null;
          operation_id: string;
          attempt_id: string | null;
          command_id: string | null;
        }>`SELECT i.workflow_run_id, i.operation_id, i.attempt_id, r.command_id FROM workflow_native_launch_intents i LEFT JOIN workflow_native_intent_receipts r ON r.intent_id = i.intent_id WHERE ((i.workflow_run_id = ${input.runId} AND i.state IN ('reserved','dispatching','unknown')) OR i.operation_id = ${admissionCommand.operationId} OR i.attempt_id = ${admissionCommand.attemptId} OR r.command_id = ${admissionCommand.commandId} OR r.receipt_id = ${input.receiptId} OR i.launch_command_id IN (${admissionCommand.commandId}, ${admissionCommand.commandId + ":initial-message"}))`;
        if (
          conflicts.some(
            (i) =>
              i.workflow_run_id !== input.runId ||
              (i.command_id === admissionCommand.commandId &&
                (i.operation_id !== admissionCommand.operationId ||
                  i.attempt_id !== admissionCommand.attemptId)) ||
              (i.operation_id === admissionCommand.operationId &&
                i.attempt_id !== admissionCommand.attemptId) ||
              (i.attempt_id === admissionCommand.attemptId &&
                i.operation_id !== admissionCommand.operationId),
          )
        )
          return yield* integrity();
        if (conflicts.length) return { kind: "rejected" as const, reason: "custody" as const };
      }
      const rows = yield* sql<{
        record_json: string;
        record_version: number;
        control_generation: number;
        spec_hash: string;
        status: string;
        plan_id: string;
        revision_id: string;
        revision_hash: string;
        source_packet_hash: string;
      }>`SELECT record_json, record_version, control_generation, spec_hash, status, plan_id, revision_id, revision_hash, source_packet_hash FROM workflow_runs WHERE id = ${input.runId}`;
      if (!rows[0]) return yield* integrity();
      const raw = yield* Effect.try({ try: () => parse(rows[0]!.record_json), catch: integrity });
      const run = yield* decodeWorkflowRunV1(raw).pipe(Effect.mapError(integrity));
      if (
        run.id !== input.runId ||
        run.recordVersion !== rows[0].record_version ||
        run.controlGeneration !== rows[0].control_generation ||
        run.spec.ref.sha256 !== rows[0].spec_hash ||
        run.status !== rows[0].status ||
        run.spec.plan.planId !== rows[0].plan_id ||
        run.spec.plan.revisionId !== rows[0].revision_id ||
        run.spec.plan.revisionHash !== rows[0].revision_hash ||
        run.spec.plan.sourcePacketHash !== rows[0].source_packet_hash
      )
        return yield* integrity();
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
      const { latest, state: currentAttemptState } = attemptFacts(
        attempts,
        command.kind === "admitAttempt" ? command.itemId : undefined,
      );
      if (
        latest?.status === "settled" &&
        latest.outcome !== "succeeded" &&
        latest.outcome !== "failed"
      )
        return yield* integrity();
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
      const nativeRows = yield* sql<{
        intent_id: string;
        workflow_run_id: string | null;
        planning_session_id: string | null;
        operation_id: WorkflowOperationV1["id"];
        attempt_id: string | null;
        pass_id: string | null;
        item_id: string | null;
        charge_attempt: number;
        repair: number;
        role: string;
        environment_id: string;
        project_id: string;
        launch_command_id: string;
        thread_id: string;
        message_id: string;
        fingerprint: string;
        draft_json: string;
        generation: number;
        state: string;
        held: number;
        boot_id: string | null;
        worker_id: string | null;
        native_run_id: string | null;
        finalized_operation_id: string | null;
      }>`SELECT * FROM workflow_native_launch_intents WHERE workflow_run_id = ${run.id} AND state IN ('reserved','dispatching','unknown')`;
      const intents = yield* Effect.forEach(nativeRows, (row) =>
        Effect.gen(function* () {
          if (
            row.workflow_run_id !== run.id ||
            row.planning_session_id !== null ||
            row.pass_id !== null ||
            !row.attempt_id ||
            !row.item_id ||
            row.charge_attempt !== 1 ||
            row.role !== "implementation" ||
            (row.repair !== 0 && row.repair !== 1) ||
            (row.held !== 0 && row.held !== 1) ||
            !Number.isSafeInteger(row.generation) ||
            row.generation < 0 ||
            row.generation > run.controlGeneration ||
            (row.state === "reserved" &&
              row.held === 0 &&
              row.generation !== run.controlGeneration) ||
            (row.state === "reserved" &&
              (row.boot_id !== null ||
                row.worker_id !== null ||
                row.native_run_id !== null ||
                row.finalized_operation_id !== null)) ||
            (row.state !== "reserved" && (row.held !== 0 || !row.boot_id || !row.worker_id))
          )
            return yield* integrity();
          const draft = yield* Effect.try({
            try: () => parse(row.draft_json) as WorkflowNativeInput["draft"],
            catch: integrity,
          });
          const commandId = yield* Effect.try({
            try: () => draft.admission.command.commandId,
            catch: integrity,
          });
          const receipts = yield* sql<{
            command_id: string;
            receipt_id: string;
            intent_id: string;
            fingerprint: string;
            result_json: string;
          }>`SELECT * FROM workflow_native_intent_receipts WHERE intent_id = ${row.intent_id} OR command_id = ${commandId}`;
          const receipt = receipts[0];
          if (
            receipts.length !== 1 ||
            !receipt ||
            receipt.intent_id !== row.intent_id ||
            receipt.command_id !== draft.admission.command.commandId ||
            receipt.fingerprint !== row.fingerprint
          )
            return yield* integrity();
          const validated = yield* verifyStoredWorkflowIntent(
            {
              intentId: row.intent_id,
              runId: run.id,
              operationId: row.operation_id,
              attemptId: row.attempt_id,
              itemId: row.item_id,
              repair: row.repair === 1,
              environmentId: row.environment_id,
              projectId: row.project_id,
              launchCommandId: row.launch_command_id,
              threadId: row.thread_id,
              messageId: row.message_id,
              receiptId: receipt.receipt_id,
              generation: row.generation,
            },
            draft,
            receipt,
          );
          if (
            validated.format === "legacyV1" ||
            draft.admission.command.expectedGeneration > row.generation ||
            draft.admission.command.expectedVersion >= run.recordVersion
          )
            return yield* integrity();
          return {
            row,
            draft,
            operation: validated.operation,
            receipt,
            material: validated.material,
          };
        }),
      );
      const heldNative = intents.filter(({ row }) => row.state === "reserved" && row.held === 1);
      if (
        run.allowance.attemptsConsumed < intents.length ||
        run.allowance.repairRoundsConsumed < intents.filter(({ row }) => row.repair === 1).length
      )
        return yield* integrity();
      const verifiedNativeHeld = new Map<string, string>();
      if (command.kind === "resume") {
        const nativeRearms = yield* readNativeHeldRearmsInTransaction(
          { kind: "workflow_run", workflowRunId: run.id },
          run.recordVersion,
        );
        if (nativeRearms.kind !== "current")
          return {
            kind: "rejected" as const,
            reason:
              nativeRearms.kind === "stale"
                ? ("freshness" as const)
                : nativeRearms.reason === "authority"
                  ? ("unauthorized" as const)
                  : nativeRearms.reason === "deadline"
                    ? ("deadline" as const)
                    : ("custody" as const),
          };
        for (const operation of nativeRearms.operations)
          verifiedNativeHeld.set(operation.id, operation.fingerprint);
      }
      const currentNativeNow =
        command.kind === "resume" &&
        (heldNative.some((intent) => intent.material?.version === 2) || verifiedNativeHeld.size > 0)
          ? DateTime.formatIso(yield* DateTime.now)
          : undefined;
      if (command.kind === "resume") {
        if (
          intents.some(({ row }) => row.state === "reserved" && row.held !== 1) ||
          heldNative.length > 1
        )
          return yield* integrity();
        if (
          heldNative.length &&
          operations.some(
            (op) =>
              op.kind === "stage_launch" &&
              op.stage === "implementation" &&
              ["pending", "claimed", "unknown"].includes(op.status),
          )
        )
          return { kind: "rejected" as const, reason: "custody" as const };
        for (const intent of heldNative) {
          if (
            intent.material?.version === 2 &&
            !(yield* checkWorkflowAuthority(intent.material, intent.operation, run))
          )
            return { kind: "rejected" as const, reason: "unauthorized" as const };
          const conflicts =
            yield* sql`SELECT 1 FROM workflow_native_launch_intents WHERE thread_id = ${intent.row.thread_id} AND intent_id <> ${intent.row.intent_id} AND state <> 'cancelled' AND (workflow_run_id IS NULL OR workflow_run_id <> ${run.id}) UNION ALL SELECT 1 FROM workflow_operations WHERE json_extract(record_json, '$.parameters.nativeThreadId') = ${intent.row.thread_id} AND (workflow_run_id IS NULL OR workflow_run_id <> ${run.id} OR planning_session_id IS NOT NULL) UNION ALL SELECT 1 FROM workflow_planning_passes WHERE native_thread_id = ${intent.row.thread_id}`;
          if (conflicts.length) return { kind: "rejected" as const, reason: "custody" as const };
          const item = intent.row.item_id!;
          const prior = attemptFacts(attempts, item);
          if (
            !orderedItemIds ||
            new Set(orderedItemIds).size !== orderedItemIds.length ||
            orderedItemIds[acceptedItemIds.length] !== item ||
            !acceptedItemIds.every((id, index) => id === orderedItemIds[index]) ||
            (intent.row.repair === 1
              ? prior.latest?.item_id !== item ||
                !["settled_failed", "settled_rejected"].includes(prior.state)
              : prior.state !== "none")
          )
            return { kind: "rejected" as const, reason: "ordering" as const };
          if (
            intent.draft.admission.command.expectedSpecHash !== run.spec.ref.sha256 ||
            WorkflowCanonicalHash.tagged("compare", intent.operation.origin) !==
              WorkflowCanonicalHash.tagged("compare", run.origin) ||
            !run.access.origins.some(
              (origin) =>
                origin.environmentId === intent.row.environment_id &&
                origin.projectId === intent.row.project_id,
            )
          )
            return { kind: "rejected" as const, reason: "freshness" as const };
        }
      }
      const nativeCustody = intents.map(({ row }) => row);
      const reservedBlocks =
        !["pause", "stop", "deadline", "resume", "custodySettled", "observeUnknown"].includes(
          command.kind,
        ) &&
        nativeCustody.some(
          (intent) =>
            intent.state === "reserved" &&
            (command.kind !== "approveAmendment" || intent.held === 0),
        );
      const facts: Facts = {
        ...externalFacts,
        ...(currentNativeNow === undefined ? {} : { now: currentNativeNow }),
        custody:
          operations.some((o) => o.status === "unknown") ||
          nativeCustody.some((intent) => intent.state === "unknown")
            ? "unknown"
            : operations.some((o) => o.status === "claimed") ||
                reservedBlocks ||
                nativeCustody.some((intent) => intent.state === "dispatching")
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
        verifiedNativeHeldOperations: [...verifiedNativeHeld].map(([operationId, fingerprint]) => ({
          operationId,
          fingerprint,
        })),
        heldNativeIntents: heldNative.map(({ row }) => ({
          kind: "native_intent" as const,
          intentId: row.intent_id,
          fingerprint: row.fingerprint,
        })),
        heldOperations: operations
          .filter((o) => o.held === 1)
          .map((o) => ({
            id: o.id,
            fingerprint: o.fingerprint,
            status: o.status,
            provablyUnstarted:
              o.status === "pending" &&
              (o.has_receipt === 0 || verifiedNativeHeld.get(o.id) === o.fingerprint),
          })),
      };
      const decision = WorkflowRunPolicy.decide({ run, command, facts });
      if (decision.kind !== "accepted") return decision;
      yield* decodeWorkflowRunV1(decision.run).pipe(Effect.mapError(integrity));
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
      if (
        operation &&
        (operation.parameters.kind === "stage_launch" ||
          operation.parameters.kind === "supervision")
      ) {
        const threadId = operation.parameters.nativeThreadId;
        if (yield* runningNativeEffect(threadId))
          return { kind: "rejected" as const, reason: "custody" as const };
        if (
          yield* readSubmissionCustodyInTransaction(
            operation.destination.environmentId,
            threadId,
            operation.owner,
          )
        )
          return { kind: "rejected" as const, reason: "custody" as const };
        const custody = yield* readThreadCustodyInTransaction(threadId);
        if (
          custody.kind === "owned" &&
          (custody.owner.kind !== "workflow_run" || custody.owner.workflowRunId !== run.id)
        )
          return { kind: "rejected" as const, reason: "custody" as const };
      }
      const updated = yield* sql<{
        id: string;
      }>`UPDATE workflow_runs SET status = ${decision.run.status}, record_version = ${decision.run.recordVersion}, control_generation = ${decision.run.controlGeneration}, spec_hash = ${decision.run.spec.ref.sha256}, plan_id = ${decision.run.spec.plan.planId}, revision_id = ${decision.run.spec.plan.revisionId}, revision_hash = ${decision.run.spec.plan.revisionHash}, source_packet_hash = ${decision.run.spec.plan.sourcePacketHash}, record_json = ${encode(decision.run)} WHERE id = ${run.id} AND record_version = ${run.recordVersion} AND control_generation = ${run.controlGeneration} AND spec_hash = ${run.spec.ref.sha256} RETURNING id`;
      if (updated.length !== 1) return yield* stale();
      for (const { row, receipt } of intents) {
        if (row.state !== "reserved") continue;
        const hold =
          decision.actions.some((action) => action.kind === "hold_unstarted") && row.held === 0;
        const cancel = decision.actions.some((action) => action.kind === "cancel_owned");
        const rearm = decision.actions.some(
          (action) =>
            action.kind === "rearm_native_intent" &&
            action.intentId === row.intent_id &&
            action.fingerprint === row.fingerprint,
        );
        if (!hold && !cancel && !rearm) continue;
        const changed =
          yield* sql`UPDATE workflow_native_launch_intents SET held = ${cancel || rearm ? 0 : 1}, state = ${cancel ? "cancelled" : "reserved"}, generation = ${rearm ? decision.run.controlGeneration : row.generation} WHERE intent_id = ${row.intent_id} AND workflow_run_id = ${run.id} AND planning_session_id IS NULL AND operation_id = ${row.operation_id} AND attempt_id = ${row.attempt_id} AND item_id = ${row.item_id} AND pass_id IS NULL AND charge_attempt = 1 AND repair = ${row.repair} AND role = ${row.role} AND environment_id = ${row.environment_id} AND project_id = ${row.project_id} AND launch_command_id = ${row.launch_command_id} AND thread_id = ${row.thread_id} AND message_id = ${row.message_id} AND fingerprint = ${row.fingerprint} AND draft_json = ${row.draft_json} AND state = 'reserved' AND held = ${row.held} AND generation = ${row.generation} AND boot_id IS NULL AND worker_id IS NULL AND native_run_id IS NULL AND finalized_operation_id IS NULL AND EXISTS (SELECT 1 FROM workflow_native_intent_receipts r WHERE r.intent_id = ${row.intent_id} AND r.command_id = ${receipt.command_id} AND r.receipt_id = ${receipt.receipt_id} AND r.fingerprint = ${receipt.fingerprint} AND r.result_json = ${receipt.result_json}) RETURNING intent_id`;
        if (changed.length !== 1) return yield* integrity();
        const receiptStillMatches =
          yield* sql`SELECT 1 FROM workflow_native_intent_receipts WHERE intent_id = ${row.intent_id} AND command_id = ${receipt.command_id} AND receipt_id = ${receipt.receipt_id} AND fingerprint = ${receipt.fingerprint} AND result_json = ${receipt.result_json}`;
        if (receiptStillMatches.length !== 1) return yield* integrity();
      }
      for (const action of decision.actions) {
        switch (action.kind) {
          case "rearm_native_intent":
            break;
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
  type NativeIntentClaimInput = {
    readonly intentId: string;
    readonly expectedFingerprint: string;
    readonly expectedOwnerVersion: number;
    readonly expectedGeneration: number;
    readonly bootId: string;
    readonly workerId: string;
  };
  const nativeClaimValidationError = (error: unknown) =>
    isPlanningError(error) && error.reason !== "storage"
      ? integrity()
      : new WorkflowRunStoreError({ reason: "persistence" });
  const nativeIntentDenied = (
    reason: "authority" | "state" | "deadline" | "custody" | "evidence" | "ineligible",
  ) => ({ kind: "denied" as const, reason });
  const nativeExecutionJournalBrand = Symbol("verified-native-execution-journal");
  type NativeExecutionJournal = Effect.Success<
    ReturnType<typeof readNativeExecutionJournalInTransaction>
  >;
  const verifyNativeIntentEligibility = Effect.fn("WorkflowRunStore.verifyNativeIntentEligibility")(
    function* (
      reservation: Effect.Success<ReturnType<typeof readNativeReservationInTransaction>>,
      row: IntentRow,
      expectedOwnerVersion?: number,
      requireNativeThread = false,
      execution?: NativeExecutionJournal,
      phase: "pending" | "resume" | "claimed" = "pending",
    ) {
      const resuming = phase === "resume";
      if (phase !== "pending" && !execution) return yield* integrity();
      if (
        execution &&
        (execution[nativeExecutionJournalBrand] !== true ||
          execution.operation.controlGeneration < reservation.generation ||
          execution.reservation !== reservation ||
          execution.row !== row ||
          reservation.state !== "finalized" ||
          reservation.finalizedOperationId !== execution.operation.id ||
          row.operation_id !== execution.operation.id ||
          !sameCustodyValue(reservation.owner, execution.operation.owner))
      )
        return yield* integrity();
      if (
        execution &&
        (execution.held !== (resuming ? 1 : 0) ||
          execution.cancelRequested !== 0 ||
          execution.operation.status !== (phase === "claimed" ? "claimed" : "pending"))
      )
        return nativeIntentDenied("state");
      const currentGeneration = execution
        ? execution.operation.controlGeneration + (resuming ? 1 : 0)
        : reservation.generation;
      const ownOperationId = execution?.operation.id ?? "";
      if (reservation.material.version !== 2 || !currentAuthority)
        return nativeIntentDenied("authority");
      const draft = yield* Effect.try({ try: () => parse(row.draft_json), catch: integrity });
      const receipts = yield* sql<{
        command_id: string;
        receipt_id: string;
        intent_id: string;
        fingerprint: string;
        result_json: string;
      }>`SELECT * FROM workflow_native_intent_receipts WHERE intent_id = ${row.intent_id}`;
      const receipt = receipts[0];
      if (!receipt || receipts.length !== 1) return yield* integrity();
      let operation: WorkflowOperationV1;
      let owner: WorkflowRunV1 | S;
      const now = DateTime.formatIso(yield* DateTime.now);
      if (reservation.owner.kind === "workflow_run") {
        const verified = yield* verifyStoredWorkflowIntent(
          {
            intentId: row.intent_id,
            runId: reservation.owner.workflowRunId,
            operationId: row.operation_id,
            attemptId: reservation.attemptId!,
            itemId: reservation.itemId!,
            repair: reservation.repair,
            environmentId: row.environment_id,
            projectId: row.project_id,
            launchCommandId: row.launch_command_id,
            threadId: row.thread_id,
            messageId: row.message_id,
            receiptId: receipt.receipt_id,
            generation: reservation.generation,
          },
          draft,
          receipt,
        );
        operation = verified.operation;
        const owners = yield* sql<{
          record_json: string;
        }>`SELECT record_json FROM workflow_runs WHERE id = ${reservation.owner.workflowRunId}`;
        const value = yield* Effect.try({
          try: () => parse(owners[0]!.record_json),
          catch: integrity,
        }).pipe(Effect.flatMap(decodeWorkflowRunV1), Effect.mapError(integrity));
        owner = value;
        if (
          (expectedOwnerVersion !== undefined && value.recordVersion !== expectedOwnerVersion) ||
          value.controlGeneration !== currentGeneration
        )
          return { kind: "stale" as const };
        if (value.status !== (resuming ? "paused" : "active")) return nativeIntentDenied("state");
        if (!isWorkflowTimestampAfter(value.allowance.deadline, now))
          return nativeIntentDenied("deadline");
        if (
          operation.parameters.kind !== "stage_launch" ||
          operation.parameters.stage !== "implementation" ||
          operation.parameters.context.specHash !== value.spec.ref.sha256 ||
          verified.draft.admission.command.expectedSpecHash !== value.spec.ref.sha256 ||
          !sameCustodyValue(value.origin, operation.origin)
        )
          return yield* integrity();
        const charged = yield* sql<{
          total: number;
          repairs: number;
        }>`SELECT COUNT(*) AS total, COALESCE(SUM(repair),0) AS repairs FROM workflow_native_launch_intents WHERE workflow_run_id = ${value.id} AND charge_attempt = 1`;
        if (
          !charged[0] ||
          value.allowance.attemptsConsumed < charged[0].total ||
          value.allowance.attemptsConsumed > value.allowance.maxAttempts ||
          value.allowance.repairRoundsConsumed < charged[0].repairs ||
          value.allowance.repairRoundsConsumed > value.allowance.maxRepairRounds
        )
          return yield* integrity();
        const competing =
          yield* sql`SELECT 1 FROM workflow_native_launch_intents WHERE workflow_run_id = ${value.id} AND intent_id <> ${row.intent_id} AND state IN ('reserved','dispatching','unknown') UNION ALL SELECT 1 FROM workflow_operations WHERE workflow_run_id = ${value.id} AND id <> ${ownOperationId} AND (status IN ('claimed','unknown') OR (status = 'pending' AND kind = 'stage_launch' AND json_extract(record_json,'$.parameters.stage') = 'implementation'))`;
        if (competing.length) return nativeIntentDenied("custody");
        yield* validatePlanningRevision({ binding: value.spec.plan }).pipe(
          Effect.mapError(nativeClaimValidationError),
        );
      } else {
        const verified = yield* verifyStoredPlanningIntent(
          {
            intentId: row.intent_id,
            sessionId: reservation.owner.planningSessionId,
            operationId: row.operation_id,
            passId: reservation.passId!,
            role: row.role as P["role"],
            environmentId: row.environment_id,
            projectId: row.project_id,
            launchCommandId: row.launch_command_id,
            threadId: row.thread_id,
            messageId: row.message_id,
            generation: reservation.generation,
          },
          draft,
          receipt,
        ).pipe(Effect.mapError(nativeClaimValidationError));
        operation = verified.operation;
        const value = yield* readSession(reservation.owner.planningSessionId).pipe(
          Effect.mapError(nativeClaimValidationError),
        );
        owner = value;
        if (
          (expectedOwnerVersion !== undefined && value.recordVersion !== expectedOwnerVersion) ||
          value.controlGeneration !== currentGeneration
        )
          return { kind: "stale" as const };
        if (value.status !== (resuming ? "paused" : "active")) return nativeIntentDenied("state");
        if (!isWorkflowTimestampAfter(value.limits.deadline, now))
          return nativeIntentDenied("deadline");
        if (
          !sameContext(value, verified.context) ||
          verified.pass.binding.planId !== value.planId ||
          !sameCustodyValue(verified.params.limits, value.limits)
        )
          return yield* integrity();
        if (
          !value.allowedRoles.includes(verified.pass.role) ||
          !value.allowedProviders.some(
            (provider) =>
              provider.providerInstanceId === verified.pass.provider.providerInstanceId &&
              provider.model === verified.pass.provider.model,
          )
        )
          return nativeIntentDenied("ineligible");
        const charged = yield* sql<{
          total: number;
          repairs: number;
        }>`SELECT COUNT(*) AS total, COALESCE(SUM(repair),0) AS repairs FROM workflow_native_launch_intents WHERE planning_session_id = ${value.id}`;
        if (
          !charged[0] ||
          value.consumed.passes < charged[0].total ||
          value.consumed.passes > value.limits.maxPasses ||
          value.consumed.repairRounds < charged[0].repairs ||
          value.consumed.repairRounds > value.limits.maxRepairRounds
        )
          return yield* integrity();
        if (verified.pass.role === "author" || verified.pass.role === "repair") {
          const competing =
            yield* sql`SELECT 1 FROM workflow_planning_passes p JOIN workflow_operations o ON o.id = p.operation_id WHERE p.plan_id = ${value.planId} AND o.id <> ${ownOperationId} AND o.status IN ('pending','claimed','unknown') AND json_extract(p.record_json,'$.role') IN ('author','repair') UNION ALL SELECT 1 FROM workflow_native_launch_intents i JOIN workflow_planning_sessions s ON s.id = i.planning_session_id WHERE s.plan_id = ${value.planId} AND i.intent_id <> ${row.intent_id} AND i.state IN ('reserved','dispatching','unknown') AND i.role IN ('author','repair')`;
          if (competing.length) return nativeIntentDenied("custody");
        }
        yield* validatePlanningRevision(verified.pass).pipe(
          Effect.mapError(nativeClaimValidationError),
        );
      }
      if (yield* runningNativeEffect(row.thread_id)) return nativeIntentDenied("custody");
      if (
        !(yield* checkWorkflowAuthority(
          reservation.material,
          operation,
          owner,
          requireNativeThread,
        ))
      )
        return nativeIntentDenied("authority");
      return { kind: "current" as const };
    },
  );
  const claimNativeIntent = Effect.fn("WorkflowRunStore.claimNativeIntent")(function* (
    input: NativeIntentClaimInput,
  ) {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* integrity();
    if (
      nativeBinding &&
      (input.bootId !== nativeBinding.coordinator.bootId ||
        input.workerId !== nativeBinding.coordinator.workerId)
    )
      return { kind: "stale" as const };
    const result = yield* inTransaction(
      Effect.gen(function* () {
        const reservation = yield* readNativeReservationInTransaction({ intentId: input.intentId });
        if (
          reservation.fingerprint !== input.expectedFingerprint ||
          reservation.generation !== input.expectedGeneration
        )
          return { kind: "stale" as const };
        if (reservation.state !== "reserved" || reservation.held)
          return nativeIntentDenied("state");
        if (reservation.material.version !== 2 || !currentAuthority)
          return nativeIntentDenied("authority");
        const rows =
          yield* sql<IntentRow>`SELECT * FROM workflow_native_launch_intents WHERE intent_id = ${input.intentId}`;
        const row = rows[0];
        if (
          !row ||
          row.boot_id !== null ||
          row.worker_id !== null ||
          row.native_run_id !== null ||
          row.finalized_operation_id !== null
        )
          return yield* integrity();
        const eligibility = yield* verifyNativeIntentEligibility(
          reservation,
          row,
          input.expectedOwnerVersion,
        );
        if (eligibility.kind !== "current") return eligibility;
        const messageCommandId = `${row.launch_command_id}:initial-message`;
        const evidence =
          yield* sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id IN (${row.launch_command_id},${messageCommandId}) UNION ALL SELECT 1 FROM orchestration_events WHERE command_id IN (${row.launch_command_id},${messageCommandId}) UNION ALL SELECT 1 FROM workflow_native_command_proofs WHERE intent_id = ${row.intent_id} OR command_id IN (${row.launch_command_id},${messageCommandId})`;
        if (evidence.length) return nativeIntentDenied("evidence");
        const changed =
          yield* sql`UPDATE workflow_native_launch_intents SET state = 'dispatching', boot_id = ${input.bootId}, worker_id = ${input.workerId} WHERE intent_id = ${row.intent_id} AND fingerprint = ${input.expectedFingerprint} AND generation = ${input.expectedGeneration} AND state = 'reserved' AND held = 0 AND boot_id IS NULL AND worker_id IS NULL AND native_run_id IS NULL AND finalized_operation_id IS NULL RETURNING intent_id`;
        if (changed.length !== 1) return { kind: "stale" as const };
        const claimed = yield* readNativeReservationInTransaction({ intentId: row.intent_id });
        if (
          claimed.state !== "dispatching" ||
          claimed.bootId !== input.bootId ||
          claimed.workerId !== input.workerId
        )
          return yield* integrity();
        return {
          kind: "claimed" as const,
          claim: Object.freeze({
            intentId: row.intent_id,
            fingerprint: row.fingerprint,
            owner: Object.freeze({ ...reservation.owner }),
            generation: input.expectedGeneration,
            bootId: input.bootId,
            workerId: input.workerId,
          }),
        };
      }),
    );
    if (result.kind === "claimed") yield* notify;
    return result;
  });
  type ClaimedNativeIntentSelection = {
    readonly intentId: string;
    readonly bootId: string;
    readonly workerId: string;
  };
  const verifyClaimedNativeIntentInTransaction = Effect.fn(
    "WorkflowRunStore.verifyClaimedNativeIntentInTransaction",
  )(function* (input: ClaimedNativeIntentSelection) {
    if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
      return yield* integrity();
    const reservation = yield* readNativeReservationInTransaction({ intentId: input.intentId });
    if (
      reservation.state !== "dispatching" ||
      reservation.held ||
      reservation.nativeRunId !== null ||
      reservation.finalizedOperationId !== null
    )
      return nativeIntentDenied("state");
    if (reservation.bootId !== input.bootId || reservation.workerId !== input.workerId)
      return { kind: "stale" as const };
    const rows =
      yield* sql<IntentRow>`SELECT * FROM workflow_native_launch_intents WHERE intent_id = ${input.intentId}`;
    if (rows.length !== 1 || !rows[0]) return yield* integrity();
    return yield* verifyNativeIntentEligibility(reservation, rows[0]);
  });
  type NativeBindingInput = {
    readonly intentId: string;
    readonly expectedFingerprint: string;
    readonly expectedGeneration: number;
  };
  const nativeProtocolError = (error: NativeProtocol.NativeCommandProofError) =>
    new WorkflowRunStoreError({
      reason:
        error.reason === "persistence"
          ? "persistence"
          : error.reason === "conflict"
            ? "stale"
            : "integrity",
    });
  const bindNativeIntent = Effect.fn("WorkflowRunStore.bindNativeIntent")(function* (
    input: NativeBindingInput,
  ) {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* integrity();
    if (!nativeBinding) return { kind: "denied" as const, reason: "unavailable" as const };
    const outcome = yield* inTransaction(
      Effect.gen(function* () {
        const reservation = yield* readNativeReservationInTransaction({ intentId: input.intentId });
        if (
          reservation.fingerprint !== input.expectedFingerprint ||
          reservation.generation !== input.expectedGeneration
        )
          return { kind: "stale" as const, notify: false };
        if (reservation.material.version !== 2)
          return { kind: "denied" as const, reason: "authority" as const, notify: false };
        if (
          (reservation.state !== "dispatching" && reservation.state !== "unknown") ||
          reservation.held ||
          reservation.finalizedOperationId !== null
        )
          return { kind: "denied" as const, reason: "state" as const, notify: false };
        if (
          reservation.bootId !== nativeBinding.coordinator.bootId ||
          reservation.workerId !== nativeBinding.coordinator.workerId
        )
          return { kind: "stale" as const, notify: false };
        const rows =
          yield* sql<IntentRow>`SELECT * FROM workflow_native_launch_intents WHERE intent_id = ${input.intentId}`;
        const row = rows[0];
        if (rows.length !== 1 || !row) return yield* integrity();
        const retainUnknown = Effect.gen(function* () {
          if (reservation.nativeRunId !== null) return yield* integrity();
          if (reservation.state === "unknown")
            return {
              kind: "reconciliation_required" as const,
              state: "unknown" as const,
              notify: false,
            };
          const changed =
            yield* sql`UPDATE workflow_native_launch_intents SET state = 'unknown' WHERE intent_id = ${row.intent_id} AND fingerprint = ${input.expectedFingerprint} AND generation = ${input.expectedGeneration} AND state = 'dispatching' AND held = 0 AND boot_id = ${nativeBinding.coordinator.bootId} AND worker_id = ${nativeBinding.coordinator.workerId} AND native_run_id IS NULL AND finalized_operation_id IS NULL RETURNING intent_id`;
          if (changed.length !== 1) return yield* stale();
          const retained = yield* readNativeReservationInTransaction({ intentId: row.intent_id });
          if (
            retained.state !== "unknown" ||
            retained.nativeRunId !== null ||
            retained.bootId !== reservation.bootId ||
            retained.workerId !== reservation.workerId ||
            retained.generation !== reservation.generation ||
            retained.fingerprint !== reservation.fingerprint
          )
            return yield* integrity();
          return {
            kind: "reconciliation_required" as const,
            state: "unknown" as const,
            notify: true,
          };
        });
        const derived = yield* nativeBinding.protocol
          .deriveInTransaction(reservation)
          .pipe(Effect.mapError(nativeProtocolError));
        const claim = yield* nativeBinding.protocol
          .readStoredInTransaction(reservation, derived.claimCommand, {
            intentId: row.intent_id,
            phase: "claim",
          })
          .pipe(Effect.mapError(nativeProtocolError));
        if (claim.kind === "absent") return yield* retainUnknown;
        const evidence = yield* nativeBinding.protocol
          .readStoredInTransaction(reservation, derived.messageCommand, {
            intentId: row.intent_id,
            phase: "message",
          })
          .pipe(Effect.mapError(nativeProtocolError));
        if (evidence.kind === "absent") return yield* retainUnknown;
        if (
          !("actualT3RunId" in evidence) ||
          typeof evidence.actualT3RunId !== "string" ||
          !evidence.actualT3RunId
        )
          return yield* integrity();
        const nativeRunId = evidence.actualT3RunId;
        if (reservation.nativeRunId !== null) {
          if (reservation.nativeRunId !== nativeRunId) return yield* integrity();
          return { kind: "replay" as const, nativeRunId, state: reservation.state, notify: false };
        }
        const eligible =
          reservation.state === "dispatching"
            ? yield* verifyNativeIntentEligibility(reservation, row, undefined, true)
            : undefined;
        const state =
          reservation.state === "unknown" || eligible?.kind !== "current"
            ? ("unknown" as const)
            : ("dispatching" as const);
        const changed =
          yield* sql`UPDATE workflow_native_launch_intents SET native_run_id = ${nativeRunId}, state = ${state} WHERE intent_id = ${row.intent_id} AND fingerprint = ${input.expectedFingerprint} AND generation = ${input.expectedGeneration} AND state = ${reservation.state} AND held = 0 AND boot_id = ${nativeBinding.coordinator.bootId} AND worker_id = ${nativeBinding.coordinator.workerId} AND native_run_id IS NULL AND finalized_operation_id IS NULL RETURNING intent_id`;
        if (changed.length !== 1) return yield* stale();
        const bound = yield* readNativeReservationInTransaction({ intentId: row.intent_id });
        if (
          bound.nativeRunId !== nativeRunId ||
          bound.state !== state ||
          bound.bootId !== reservation.bootId ||
          bound.workerId !== reservation.workerId ||
          bound.generation !== reservation.generation ||
          bound.fingerprint !== reservation.fingerprint
        )
          return yield* integrity();
        return { kind: "bound" as const, nativeRunId, state, notify: true };
      }),
    );
    if (outcome.notify) yield* notify;
    const { notify: _notify, ...result } = outcome;
    return result;
  });
  const loadNativeRunStartFootprint = Effect.fn("WorkflowRunStore.loadNativeRunStartFootprint")(
    function* (
      nativeRunId: string,
      threadId: string,
      events: readonly { readonly event_type: string; readonly payload_json: string }[],
    ) {
      const originals = events.filter((event) => event.event_type === "run.created");
      if (originals.length !== 1 || !originals[0]) return yield* integrity();
      const original = yield* decodeNativeFinalizationRun(originals[0].payload_json).pipe(
        Effect.mapError(integrity),
      );
      const rows = yield* sql<{
        run_id: string;
        thread_id: string;
        ordinal: number;
        requested_at: string;
        provider_instance_id: string;
        provider_thread_id: string | null;
        status: string;
        completed_at: string | null;
        payload_json: string;
      }>`SELECT * FROM orchestration_v2_projection_runs WHERE run_id = ${nativeRunId}`;
      const row = rows[0];
      if (rows.length !== 1 || !row) return yield* integrity();
      const run = yield* decodeNativeFinalizationRun(row.payload_json).pipe(
        Effect.mapError(integrity),
      );
      if (
        original.id !== nativeRunId ||
        original.threadId !== threadId ||
        original.status !== "preparing" ||
        original.startedAt !== null ||
        original.completedAt !== null ||
        original.workStartedAt !== undefined ||
        !original.activeAttemptId ||
        !original.rootNodeId ||
        !original.providerThreadId ||
        row.run_id !== run.id ||
        row.thread_id !== run.threadId ||
        run.id !== nativeRunId ||
        run.threadId !== threadId ||
        row.status !== run.status ||
        row.provider_instance_id !== run.providerInstanceId ||
        row.provider_thread_id !== run.providerThreadId ||
        run.activeAttemptId !== original.activeAttemptId ||
        run.rootNodeId !== original.rootNodeId ||
        run.providerThreadId !== original.providerThreadId ||
        run.providerInstanceId !== original.providerInstanceId
      )
        return yield* integrity();
      const attempts = yield* sql<{
        attempt_id: string;
        thread_id: string;
        run_id: string;
        attempt_ordinal: number;
        root_node_id: string;
        provider_instance_id: string;
        provider_thread_id: string | null;
        provider_turn_id: string | null;
        status: string;
        payload_json: string;
      }>`SELECT * FROM orchestration_v2_projection_run_attempts WHERE attempt_id = ${run.activeAttemptId}`;
      const nodes = yield* sql<{
        node_id: string;
        thread_id: string;
        run_id: string;
        parent_node_id: string | null;
        root_node_id: string;
        kind: string;
        status: string;
        provider_thread_id: string | null;
        provider_turn_id: string | null;
        started_at: string | null;
        completed_at: string | null;
        payload_json: string;
      }>`SELECT * FROM orchestration_v2_projection_nodes WHERE node_id = ${run.rootNodeId}`;
      const a = attempts[0];
      const n = nodes[0];
      if (attempts.length !== 1 || !a || nodes.length !== 1 || !n) return yield* integrity();
      const attempt = yield* decodeNativeFinalizationAttempt(a.payload_json).pipe(
        Effect.mapError(integrity),
      );
      const node = yield* decodeNativeFinalizationNode(n.payload_json).pipe(
        Effect.mapError(integrity),
      );
      if (
        a.attempt_id !== attempt.id ||
        attempt.id !== run.activeAttemptId ||
        a.thread_id !== threadId ||
        a.run_id !== nativeRunId ||
        attempt.runId !== nativeRunId ||
        a.attempt_ordinal !== attempt.attemptOrdinal ||
        attempt.attemptOrdinal !== 1 ||
        attempt.reason !== "initial" ||
        a.root_node_id !== attempt.rootNodeId ||
        attempt.rootNodeId !== run.rootNodeId ||
        a.provider_instance_id !== attempt.providerInstanceId ||
        attempt.providerInstanceId !== run.providerInstanceId ||
        a.provider_thread_id !== attempt.providerThreadId ||
        attempt.providerThreadId !== run.providerThreadId ||
        a.provider_turn_id !== attempt.providerTurnId ||
        a.status !== attempt.status ||
        n.node_id !== node.id ||
        node.id !== run.rootNodeId ||
        n.thread_id !== node.threadId ||
        node.threadId !== threadId ||
        n.run_id !== node.runId ||
        node.runId !== nativeRunId ||
        n.root_node_id !== node.rootNodeId ||
        node.rootNodeId !== node.id ||
        n.parent_node_id !== node.parentNodeId ||
        node.parentNodeId !== null ||
        n.kind !== node.kind ||
        node.kind !== "root_turn" ||
        n.status !== node.status ||
        n.provider_thread_id !== node.providerThreadId ||
        node.providerThreadId !== run.providerThreadId ||
        n.provider_turn_id !== node.providerTurnId ||
        !node.countsForRun
      )
        return yield* integrity();
      return { original, run, attempt, node, runRow: row, attemptRow: a, nodeRow: n };
    },
  );
  type NativeRunStartFootprint = Effect.Success<ReturnType<typeof loadNativeRunStartFootprint>>;
  const isPreparingNativeFootprint = ({ run, attempt, node, nodeRow }: NativeRunStartFootprint) => {
    return (
      run.status === "preparing" &&
      run.startedAt === null &&
      run.completedAt === null &&
      run.workStartedAt === undefined &&
      attempt.status === "pending" &&
      attempt.providerTurnId === null &&
      attempt.startedAt === null &&
      attempt.completedAt === null &&
      node.status === "pending" &&
      node.providerTurnId === null &&
      node.startedAt === null &&
      node.completedAt === null &&
      nodeRow.started_at === null &&
      nodeRow.completed_at === null &&
      node.runtimeRequestId === null &&
      node.checkpointScopeId === null &&
      node.nativeItemRef === null
    );
  };
  const verifyPreparingNativeRun = Effect.fn("WorkflowRunStore.verifyPreparingNativeRun")(
    function* (
      nativeRunId: string,
      threadId: string,
      events: readonly { readonly event_type: string; readonly payload_json: string }[],
    ) {
      const footprint = yield* loadNativeRunStartFootprint(nativeRunId, threadId, events);
      if (footprint.runRow.completed_at !== null) return yield* integrity();
      return isPreparingNativeFootprint(footprint);
    },
  );
  const readCurrentNativeFootprintInTransaction = Effect.fn(
    "WorkflowRunStore.readCurrentNativeFootprintInTransaction",
  )(function* (
    nativeRunId: string,
    threadId: string,
    events: readonly { readonly event_type: string; readonly payload_json: string }[],
  ) {
    const footprint = yield* loadNativeRunStartFootprint(nativeRunId, threadId, events);
    const { original, run, attempt, node, runRow, nodeRow } = footprint;
    if (
      runRow.ordinal !== run.ordinal ||
      runRow.requested_at !== DateTime.formatIso(run.requestedAt) ||
      run.ordinal !== original.ordinal ||
      run.modelSelection.instanceId !== run.providerInstanceId ||
      !(yield* Effect.try({
        try: () => sameCustodyValue(run.modelSelection, original.modelSelection),
        catch: integrity,
      }))
    )
      return yield* integrity();
    if (
      run.completedAt !== null ||
      runRow.completed_at !== null ||
      attempt.completedAt !== null ||
      node.completedAt !== null ||
      nodeRow.completed_at !== null ||
      attempt.providerTurnId !== null ||
      node.providerTurnId !== null ||
      node.runtimeRequestId !== null ||
      node.nativeItemRef !== null ||
      run.workStartedAt !== undefined
    )
      return nativeIntentDenied("state");
    const stage =
      run.status === "preparing" && attempt.status === "pending" && node.status === "pending"
        ? ("preparing" as const)
        : run.status === "starting" && attempt.status === "pending" && node.status === "pending"
          ? ("starting" as const)
          : run.status === "running" && attempt.status === "running" && node.status === "running"
            ? ("running" as const)
            : undefined;
    if (!stage) return nativeIntentDenied("state");
    if (
      stage === "preparing"
        ? !isPreparingNativeFootprint(footprint)
        : stage === "starting"
          ? run.startedAt !== null ||
            attempt.startedAt !== null ||
            node.startedAt !== null ||
            nodeRow.started_at !== null ||
            node.checkpointScopeId === null
          : run.startedAt === null ||
            attempt.startedAt === null ||
            node.startedAt === null ||
            nodeRow.started_at !== DateTime.formatIso(node.startedAt) ||
            node.checkpointScopeId === null
    )
      return nativeIntentDenied("state");
    const providers = yield* sql<{
      provider_thread_id: string;
      thread_id: string | null;
      owner_node_id: string | null;
      driver: string;
      provider_instance_id: string;
      provider_session_id: string | null;
      status: string;
      first_run_ordinal: number | null;
      last_run_ordinal: number | null;
      updated_at: string;
      payload_json: string;
    }>`SELECT * FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id=${run.providerThreadId}`;
    const providerRow = providers[0];
    if (providers.length !== 1 || !providerRow) return yield* integrity();
    const provider = yield* decodeRecoveryProviderThread(providerRow.payload_json).pipe(
      Effect.mapError(integrity),
    );
    if (
      providerRow.provider_thread_id !== provider.id ||
      provider.id !== run.providerThreadId ||
      providerRow.thread_id !== provider.appThreadId ||
      provider.appThreadId !== run.threadId ||
      providerRow.owner_node_id !== provider.ownerNodeId ||
      providerRow.driver !== provider.driver ||
      providerRow.provider_instance_id !== provider.providerInstanceId ||
      provider.providerInstanceId !== run.providerInstanceId ||
      providerRow.provider_session_id !== provider.providerSessionId ||
      providerRow.status !== provider.status ||
      providerRow.first_run_ordinal !== provider.firstRunOrdinal ||
      providerRow.last_run_ordinal !== provider.lastRunOrdinal ||
      providerRow.updated_at !== DateTime.formatIso(provider.updatedAt) ||
      (provider.nativeThreadRef !== null && provider.nativeThreadRef.driver !== provider.driver)
    )
      return yield* integrity();
    const originalProviders = yield* Effect.forEach(
      events.filter((event) => event.event_type === "provider-thread.updated"),
      (event) => decodeRecoveryProviderThread(event.payload_json).pipe(Effect.mapError(integrity)),
    );
    const selectedProviders = originalProviders.filter(
      (value) => value.id === run.providerThreadId,
    );
    const originalProvider = selectedProviders[0];
    if (
      selectedProviders.length !== 1 ||
      !originalProvider ||
      originalProvider.appThreadId !== run.threadId ||
      originalProvider.providerInstanceId !== run.providerInstanceId ||
      provider.driver !== originalProvider.driver ||
      provider.ownerNodeId !== originalProvider.ownerNodeId
    )
      return yield* integrity();
    if (
      provider.status === "error" ||
      provider.status === "closed" ||
      provider.status === "archived" ||
      (stage === "running" && provider.status !== "active")
    )
      return nativeIntentDenied("state");
    if (node.checkpointScopeId !== null) {
      const scopes = yield* sql<{
        scope_id: string;
        thread_id: string;
        run_id: string | null;
        node_id: string;
        parent_scope_id: string | null;
        created_at: string;
        provider_thread_id: string | null;
        kind: string;
        ordinal_within_parent: number;
        advances_app_run_count: number;
        payload_json: string;
      }>`SELECT * FROM orchestration_v2_projection_checkpoint_scopes WHERE scope_id=${node.checkpointScopeId}`;
      const row = scopes[0];
      if (scopes.length !== 1 || !row) return yield* integrity();
      const scope = yield* decodeNativeCheckpointScope(row.payload_json).pipe(
        Effect.mapError(integrity),
      );
      if (
        row.scope_id !== scope.id ||
        scope.id !== node.checkpointScopeId ||
        row.thread_id !== scope.threadId ||
        scope.threadId !== run.threadId ||
        row.run_id !== scope.runId ||
        scope.runId !== run.id ||
        row.node_id !== scope.nodeId ||
        scope.nodeId !== node.id ||
        row.provider_thread_id !== scope.providerThreadId ||
        scope.providerThreadId !== provider.id ||
        row.kind !== scope.kind ||
        scope.kind !== "root_run" ||
        row.parent_scope_id !== scope.parentScopeId ||
        scope.parentScopeId !== null ||
        row.created_at !== DateTime.formatIso(scope.createdAt) ||
        row.ordinal_within_parent !== scope.ordinalWithinParent ||
        scope.ordinalWithinParent !== 0 ||
        row.advances_app_run_count !== 1 ||
        !scope.advancesAppRunCount
      )
        return yield* integrity();
    }
    return {
      kind: "current" as const,
      stage,
      threadId: run.threadId,
      runId: run.id,
      attemptId: attempt.id,
      rootNodeId: node.id,
      providerInstanceId: run.providerInstanceId,
      providerThreadId: provider.id,
      driver: provider.driver,
      providerSessionId: provider.providerSessionId,
      nativeThreadId: provider.nativeThreadRef?.nativeId ?? null,
      checkpointScopeId: node.checkpointScopeId,
    };
  });
  type NativeReservation = Effect.Success<ReturnType<typeof readNativeReservationInTransaction>>;
  const prepareWorkflowNativeFinalization = Effect.fn(
    "WorkflowRunStore.prepareWorkflowNativeFinalization",
  )(function* (reservation: NativeReservation, row: IntentRow) {
    if (
      !nativeBinding ||
      reservation.owner.kind !== "workflow_run" ||
      !reservation.nativeRunId ||
      reservation.material.version !== 2
    )
      return yield* integrity();
    const workflowRunId = reservation.owner.workflowRunId;
    const receipts = yield* sql<{
      command_id: string;
      intent_id: string;
      receipt_id: string;
      fingerprint: string;
      result_json: string;
    }>`SELECT * FROM workflow_native_intent_receipts WHERE intent_id = ${row.intent_id}`;
    if (receipts.length !== 1 || !receipts[0]) return yield* integrity();
    const draft = yield* Effect.try({ try: () => parse(row.draft_json), catch: integrity });
    const verified = yield* verifyStoredWorkflowIntent(
      {
        intentId: row.intent_id,
        runId: workflowRunId,
        operationId: row.operation_id,
        attemptId: reservation.attemptId!,
        itemId: reservation.itemId!,
        repair: reservation.repair,
        environmentId: row.environment_id,
        projectId: row.project_id,
        launchCommandId: row.launch_command_id,
        threadId: row.thread_id,
        messageId: row.message_id,
        receiptId: reservation.receiptId,
        generation: reservation.generation,
      },
      draft,
      receipts[0],
    );
    if (verified.operation.parameters.kind !== "stage_launch") return yield* integrity();
    const base = {
      ...verified.operation,
      parameters: {
        ...verified.operation.parameters,
        nativeRunId: reservation.nativeRunId,
      },
    };
    const decoded = yield* decodeWorkflowOperationV1(base).pipe(Effect.mapError(integrity));
    const operation = { ...decoded, fingerprint: WorkflowCanonicalHash.operation(decoded) };
    const command = {
      ...verified.draft.admission.command,
      operationFingerprint: operation.fingerprint,
    };
    const hash = WorkflowCanonicalHash.command(workflowRunId, command);
    const derived = yield* nativeBinding.protocol
      .deriveInTransaction(reservation)
      .pipe(Effect.mapError(nativeProtocolError));
    const evidence = yield* nativeBinding.protocol
      .readStoredInTransaction(reservation, derived.messageCommand, {
        intentId: row.intent_id,
        phase: "message",
      })
      .pipe(Effect.mapError(nativeProtocolError));
    if (
      evidence.kind !== "verified" ||
      !("actualT3RunId" in evidence) ||
      evidence.actualT3RunId !== reservation.nativeRunId
    )
      return yield* integrity();
    const readArtifacts = Effect.gen(function* () {
      const operations = yield* sql<{
        record_json: string;
        held: number;
        cancel_requested: number;
      }>`SELECT record_json, held, cancel_requested FROM workflow_operations WHERE id = ${operation.id}`;
      const attempts = yield* sql<{
        id: string;
        workflow_run_id: string;
        operation_id: string;
        item_id: string;
        repair: number;
      }>`SELECT * FROM workflow_attempts WHERE id = ${reservation.attemptId} OR operation_id = ${operation.id}`;
      const admissions = yield* sql<{
        command_id: string;
        workflow_run_id: string | null;
        planning_session_id: string | null;
        fingerprint: string;
        receipt_id: string;
        operation_id: string | null;
        result_json: string;
      }>`SELECT * FROM workflow_command_receipts WHERE command_id = ${command.commandId} OR receipt_id = ${reservation.receiptId} OR operation_id = ${operation.id}`;
      return { operations, attempts, admissions };
    });
    const artifacts = yield* readArtifacts;
    const accepted = { kind: "accepted" as const, receiptId: reservation.receiptId };
    const validateArtifacts = Effect.fn("WorkflowRunStore.verifyWorkflowFinalization")(function* (
      artifacts: Effect.Success<typeof readArtifacts>,
      fresh = false,
    ) {
      const { operations, attempts, admissions } = artifacts;
      if (
        operations.length !== 1 ||
        !operations[0] ||
        attempts.length !== 1 ||
        !attempts[0] ||
        admissions.length !== 1 ||
        !admissions[0]
      )
        return yield* integrity();
      const stored = yield* Effect.try({
        try: () => parse(operations[0]!.record_json),
        catch: integrity,
      }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity));
      const immutable = (value: WorkflowOperationV1) => {
        const {
          status: _status,
          controlGeneration: _generation,
          claim: _claim,
          outcome: _outcome,
          nextDueAt: _due,
          ...rest
        } = value;
        return rest;
      };
      const a = attempts[0];
      const r = admissions[0];
      const matches = yield* Effect.try({
        try: () =>
          (!fresh ||
            (operations[0]!.held === 0 &&
              operations[0]!.cancel_requested === 0 &&
              sameCustodyValue(stored, operation))) &&
          sameCustodyValue(immutable(stored), immutable(operation)) &&
          sameCustodyValue(parse(r.result_json), accepted),
        catch: integrity,
      });
      if (
        !matches ||
        a.id !== reservation.attemptId ||
        a.workflow_run_id !== workflowRunId ||
        a.operation_id !== operation.id ||
        a.item_id !== reservation.itemId ||
        a.repair !== (reservation.repair ? 1 : 0) ||
        r.command_id !== command.commandId ||
        r.workflow_run_id !== workflowRunId ||
        r.planning_session_id !== null ||
        r.fingerprint !== hash ||
        r.receipt_id !== reservation.receiptId ||
        r.operation_id !== operation.id
      )
        return yield* integrity();
      return {
        operation: stored,
        held: operations[0].held,
        cancelRequested: operations[0].cancel_requested,
      };
    });

    return {
      operation,
      command,
      hash,
      accepted,
      evidence,
      readArtifacts,
      validateArtifacts,
      artifacts,
    };
  });
  const preparePlanningNativeFinalization = Effect.fn(
    "WorkflowRunStore.preparePlanningNativeFinalization",
  )(function* (reservation: NativeReservation, row: IntentRow) {
    if (
      !nativeBinding ||
      reservation.owner.kind !== "planning_session" ||
      !reservation.nativeRunId ||
      reservation.material.version !== 2
    )
      return yield* integrity();
    const sessionId = reservation.owner.planningSessionId;
    const receipts = yield* sql<{
      command_id: string;
      intent_id: string;
      receipt_id: string;
      fingerprint: string;
      result_json: string;
    }>`SELECT * FROM workflow_native_intent_receipts WHERE intent_id=${row.intent_id}`;
    if (receipts.length !== 1 || !receipts[0]) return yield* integrity();
    const draft = yield* Effect.try({ try: () => parse(row.draft_json), catch: integrity });
    const verified = yield* verifyStoredPlanningIntent(
      {
        intentId: row.intent_id,
        sessionId,
        operationId: row.operation_id,
        passId: reservation.passId!,
        role: row.role as P["role"],
        environmentId: row.environment_id,
        projectId: row.project_id,
        launchCommandId: row.launch_command_id,
        threadId: row.thread_id,
        messageId: row.message_id,
        generation: reservation.generation,
      },
      draft,
      receipts[0],
    ).pipe(Effect.mapError(nativeClaimValidationError));
    const operationBase = {
      ...verified.operation,
      controlGeneration: reservation.generation,
    };
    const operation = yield* decodeWorkflowOperationV1({
      ...operationBase,
      fingerprint: WorkflowCanonicalHash.operation(operationBase),
    }).pipe(Effect.mapError(integrity));
    const pass = yield* decodePlanningPassV1({
      ...verified.pass,
      native: { threadId: reservation.threadId, runId: reservation.nativeRunId },
    }).pipe(Effect.mapError(integrity));
    if (!reservation.material.planningSession || !pass.native) return yield* integrity();
    const value = { session: reservation.material.planningSession, pass, operation };
    const hash = yield* fingerprint(verified.context, "admitPlanningPass", {
      sessionId,
      expectedVersion: verified.draft.admission.expectedVersion,
      pass,
      operationFingerprint: operation.fingerprint,
    }).pipe(Effect.mapError(nativeClaimValidationError));
    const derived = yield* nativeBinding.protocol
      .deriveInTransaction(reservation)
      .pipe(Effect.mapError(nativeProtocolError));
    const evidence = yield* nativeBinding.protocol
      .readStoredInTransaction(reservation, derived.messageCommand, {
        intentId: row.intent_id,
        phase: "message",
      })
      .pipe(Effect.mapError(nativeProtocolError));
    if (
      evidence.kind !== "verified" ||
      !("actualT3RunId" in evidence) ||
      evidence.actualT3RunId !== reservation.nativeRunId
    )
      return yield* integrity();
    const readArtifacts = Effect.gen(function* () {
      const operations = yield* sql<{
        record_json: string;
        held: number;
        cancel_requested: number;
      }>`SELECT record_json, held, cancel_requested FROM workflow_operations WHERE id=${operation.id}`;
      const passes = yield* sql<{
        id: P["id"];
      }>`SELECT id FROM workflow_planning_passes WHERE id=${pass.id} OR operation_id=${operation.id}`;
      const admissions = yield* sql<{
        command_id: string;
        workflow_run_id: string | null;
        planning_session_id: string | null;
        operation_id: string | null;
        fingerprint: string;
        receipt_id: string;
        result_json: string;
      }>`SELECT * FROM workflow_command_receipts WHERE command_id=${verified.command.commandId} OR receipt_id=${verified.command.receiptId} OR operation_id=${operation.id}`;
      return { operations, passes, admissions };
    });
    const artifacts = yield* readArtifacts;
    const validateArtifacts = Effect.fn("WorkflowRunStore.verifyPlanningFinalization")(function* (
      artifacts: Effect.Success<typeof readArtifacts>,
      fresh = false,
    ) {
      const { operations, passes, admissions } = artifacts;
      if (
        operations.length !== 1 ||
        !operations[0] ||
        passes.length !== 1 ||
        !passes[0] ||
        admissions.length !== 1 ||
        !admissions[0]
      )
        return yield* integrity();
      const storedOperation = yield* Effect.try({
        try: () => parse(operations[0]!.record_json),
        catch: integrity,
      }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity));
      const storedPass = yield* readPass(passes[0].id).pipe(
        Effect.mapError(nativeClaimValidationError),
      );
      const immutableOperation = (value: WorkflowOperationV1) => {
        const {
          status: _status,
          controlGeneration: _generation,
          claim: _claim,
          outcome: _outcome,
          nextDueAt: _due,
          ...rest
        } = value;
        return rest;
      };
      const immutablePass = (value: P) => {
        const { status: _status, report: _report, native: _native, ...rest } = value;
        return rest;
      };
      const receipt = admissions[0];
      const matches = yield* Effect.try({
        try: () =>
          (!fresh ||
            (operations[0]!.held === 0 &&
              operations[0]!.cancel_requested === 0 &&
              sameCustodyValue(storedOperation, operation) &&
              sameCustodyValue(storedPass, pass))) &&
          sameCustodyValue(immutableOperation(storedOperation), immutableOperation(operation)) &&
          sameCustodyValue(immutablePass(storedPass), immutablePass(pass)) &&
          sameCustodyValue(parse(receipt.result_json), value),
        catch: integrity,
      });
      if (
        !matches ||
        storedPass.native?.threadId !== pass.native!.threadId ||
        storedPass.native.runId !== pass.native!.runId ||
        receipt.command_id !== verified.command.commandId ||
        receipt.workflow_run_id !== null ||
        receipt.planning_session_id !== sessionId ||
        receipt.operation_id !== operation.id ||
        receipt.fingerprint !== hash ||
        receipt.receipt_id !== verified.command.receiptId
      )
        return yield* integrity();
      return {
        operation: storedOperation,
        pass: storedPass,
        held: operations[0].held,
        cancelRequested: operations[0].cancel_requested,
      };
    });

    return {
      operation,
      pass,
      hash,
      value,
      evidence,
      verified,
      readArtifacts,
      validateArtifacts,
      artifacts,
    };
  });
  const finalizeWorkflowNativeIntent = Effect.fn("WorkflowRunStore.finalizeWorkflowNativeIntent")(
    function* (input: NativeBindingInput) {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* integrity();
      if (!nativeBinding) return { kind: "denied" as const, reason: "unavailable" as const };
      const outcome = yield* inTransaction(
        Effect.gen(function* () {
          const reservation = yield* readNativeReservationInTransaction({
            intentId: input.intentId,
          });
          if (
            reservation.fingerprint !== input.expectedFingerprint ||
            reservation.generation !== input.expectedGeneration
          )
            return { kind: "stale" as const, notify: false };
          if (reservation.material.version !== 2 || reservation.owner.kind !== "workflow_run")
            return { kind: "denied" as const, reason: "authority" as const, notify: false };
          const workflowRunId = reservation.owner.workflowRunId;
          if (
            reservation.bootId !== nativeBinding.coordinator.bootId ||
            reservation.workerId !== nativeBinding.coordinator.workerId
          )
            return { kind: "stale" as const, notify: false };
          if (
            !reservation.nativeRunId ||
            (reservation.state !== "dispatching" && reservation.state !== "finalized") ||
            reservation.held
          )
            return { kind: "denied" as const, reason: "state" as const, notify: false };
          const rows =
            yield* sql<IntentRow>`SELECT * FROM workflow_native_launch_intents WHERE intent_id = ${input.intentId}`;
          const row = rows[0];
          if (rows.length !== 1 || !row) return yield* integrity();
          const {
            operation,
            command,
            hash,
            accepted,
            evidence,
            readArtifacts,
            validateArtifacts,
            artifacts,
          } = yield* prepareWorkflowNativeFinalization(reservation, row);
          const { operations, attempts, admissions } = artifacts;
          if (reservation.state === "finalized") {
            if (reservation.finalizedOperationId !== operation.id) return yield* integrity();
            yield* validateArtifacts({ operations, attempts, admissions });
            return {
              kind: "replay" as const,
              operationId: operation.id,
              receiptId: reservation.receiptId,
              notify: false,
            };
          }
          if (
            reservation.finalizedOperationId !== null ||
            operations.length ||
            attempts.length ||
            admissions.length
          )
            return yield* integrity();
          const eligible = yield* verifyNativeIntentEligibility(reservation, row, undefined, true);
          if (eligible.kind !== "current") return { ...eligible, notify: false };
          if (
            !(yield* verifyPreparingNativeRun(
              reservation.nativeRunId,
              reservation.threadId,
              evidence.events,
            ))
          )
            return { kind: "denied" as const, reason: "state" as const, notify: false };
          yield* sql`INSERT INTO workflow_operations (id,workflow_run_id,kind,fingerprint,status,generation,record_json) VALUES (${operation.id},${workflowRunId},${operation.kind},${operation.fingerprint},'pending',${operation.controlGeneration},${encode(operation)})`;
          yield* sql`INSERT INTO workflow_attempts VALUES (${reservation.attemptId},${workflowRunId},${operation.id},${reservation.itemId},${reservation.repair ? 1 : 0})`;
          yield* sql`INSERT INTO workflow_command_receipts (command_id,workflow_run_id,fingerprint,receipt_id,operation_id,result_json) VALUES (${command.commandId},${workflowRunId},${hash},${reservation.receiptId},${operation.id},${encode(accepted)})`;
          const changed =
            yield* sql`UPDATE workflow_native_launch_intents SET state = 'finalized',finalized_operation_id = ${operation.id} WHERE intent_id = ${row.intent_id} AND state = 'dispatching' AND held = 0 AND fingerprint = ${input.expectedFingerprint} AND generation = ${input.expectedGeneration} AND boot_id = ${nativeBinding.coordinator.bootId} AND worker_id = ${nativeBinding.coordinator.workerId} AND native_run_id = ${reservation.nativeRunId} AND finalized_operation_id IS NULL RETURNING intent_id`;
          if (changed.length !== 1) return yield* stale();
          const retained = yield* readNativeReservationInTransaction({ intentId: row.intent_id });
          if (
            retained.state !== "finalized" ||
            retained.finalizedOperationId !== operation.id ||
            retained.nativeRunId !== reservation.nativeRunId
          )
            return yield* integrity();
          yield* validateArtifacts(yield* readArtifacts, true);
          return {
            kind: "finalized" as const,
            operationId: operation.id,
            receiptId: reservation.receiptId,
            notify: true,
          };
        }),
      );
      if (outcome.notify) yield* notify;
      const { notify: _notify, ...result } = outcome;
      return result;
    },
  );
  const finalizePlanningNativeIntent = Effect.fn("WorkflowRunStore.finalizePlanningNativeIntent")(
    function* (input: NativeBindingInput) {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* integrity();
      if (!nativeBinding) return { kind: "denied" as const, reason: "unavailable" as const };
      const outcome = yield* inTransaction(
        Effect.gen(function* () {
          const reservation = yield* readNativeReservationInTransaction({
            intentId: input.intentId,
          });
          if (
            reservation.fingerprint !== input.expectedFingerprint ||
            reservation.generation !== input.expectedGeneration
          )
            return { kind: "stale" as const, notify: false };
          if (reservation.material.version !== 2 || reservation.owner.kind !== "planning_session")
            return { kind: "denied" as const, reason: "authority" as const, notify: false };
          const sessionId = reservation.owner.planningSessionId;
          if (
            reservation.bootId !== nativeBinding.coordinator.bootId ||
            reservation.workerId !== nativeBinding.coordinator.workerId
          )
            return { kind: "stale" as const, notify: false };
          if (
            !reservation.nativeRunId ||
            (reservation.state !== "dispatching" && reservation.state !== "finalized") ||
            reservation.held
          )
            return { kind: "denied" as const, reason: "state" as const, notify: false };
          const rows =
            yield* sql<IntentRow>`SELECT * FROM workflow_native_launch_intents WHERE intent_id=${input.intentId}`;
          const row = rows[0];
          if (rows.length !== 1 || !row) return yield* integrity();
          const {
            operation,
            pass,
            hash,
            value,
            evidence,
            verified,
            readArtifacts,
            validateArtifacts,
            artifacts,
          } = yield* preparePlanningNativeFinalization(reservation, row);
          if (!pass.native) return yield* integrity();
          if (reservation.state === "finalized") {
            if (reservation.finalizedOperationId !== operation.id) return yield* integrity();
            yield* validateArtifacts(artifacts);
            return {
              kind: "replay" as const,
              operationId: operation.id,
              passId: pass.id,
              receiptId: verified.command.receiptId,
              notify: false,
            };
          }
          if (
            reservation.finalizedOperationId !== null ||
            artifacts.operations.length ||
            artifacts.passes.length ||
            artifacts.admissions.length
          )
            return yield* integrity();
          const eligible = yield* verifyNativeIntentEligibility(reservation, row, undefined, true);
          if (eligible.kind !== "current") return { ...eligible, notify: false };
          if (
            !(yield* verifyPreparingNativeRun(
              reservation.nativeRunId,
              reservation.threadId,
              evidence.events,
            ))
          )
            return { kind: "denied" as const, reason: "state" as const, notify: false };
          yield* sql`INSERT INTO workflow_operations (id,planning_session_id,kind,fingerprint,status,generation,record_json) VALUES (${operation.id},${sessionId},'planning_pass',${operation.fingerprint},'pending',${operation.controlGeneration},${encode(operation)})`;
          const binding = pass.binding;
          yield* sql`INSERT INTO workflow_planning_passes VALUES (${pass.id},${sessionId},${binding.planId},${binding.revisionId},${binding.revisionHash},${binding.sourcePacketHash},${operation.id},${pass.native.threadId},${pass.native.runId},${encode(pass)})`;
          yield* sql`INSERT INTO workflow_command_receipts (command_id,planning_session_id,fingerprint,receipt_id,operation_id,result_json) VALUES (${verified.command.commandId},${sessionId},${hash},${verified.command.receiptId},${operation.id},${encode(value)})`;
          const changed =
            yield* sql`UPDATE workflow_native_launch_intents SET state='finalized',finalized_operation_id=${operation.id} WHERE intent_id=${row.intent_id} AND state='dispatching' AND held=0 AND fingerprint=${input.expectedFingerprint} AND generation=${input.expectedGeneration} AND boot_id=${nativeBinding.coordinator.bootId} AND worker_id=${nativeBinding.coordinator.workerId} AND native_run_id=${reservation.nativeRunId} AND finalized_operation_id IS NULL RETURNING intent_id`;
          if (changed.length !== 1) return yield* stale();
          const retained = yield* readNativeReservationInTransaction({ intentId: row.intent_id });
          if (
            retained.state !== "finalized" ||
            retained.finalizedOperationId !== operation.id ||
            retained.nativeRunId !== reservation.nativeRunId
          )
            return yield* integrity();
          yield* validateArtifacts(yield* readArtifacts, true);
          return {
            kind: "finalized" as const,
            operationId: operation.id,
            passId: pass.id,
            receiptId: verified.command.receiptId,
            notify: true,
          };
        }),
      );
      if (outcome.notify) yield* notify;
      const { notify: _notify, ...result } = outcome;
      return result;
    },
  );
  const readNativeExecutionJournalInTransaction = Effect.fn(
    "WorkflowRunStore.readNativeExecutionJournalInTransaction",
  )(function* (reservation: NativeReservation, row: IntentRow) {
    if (
      !nativeBinding ||
      reservation.material.version !== 2 ||
      reservation.state !== "finalized" ||
      reservation.finalizedOperationId !== row.operation_id ||
      reservation.operationId !== row.operation_id ||
      !reservation.nativeRunId ||
      reservation.held
    )
      return yield* integrity();
    if (reservation.owner.kind === "workflow_run") {
      const journal = yield* prepareWorkflowNativeFinalization(reservation, row);
      const current = yield* journal.validateArtifacts(journal.artifacts);
      return {
        reservation,
        row,
        operation: current.operation,
        pass: undefined,
        held: current.held,
        cancelRequested: current.cancelRequested,
        evidence: journal.evidence,
        [nativeExecutionJournalBrand]: true as const,
      };
    }
    const journal = yield* preparePlanningNativeFinalization(reservation, row);
    const current = yield* journal.validateArtifacts(journal.artifacts);
    return {
      reservation,
      row,
      operation: current.operation,
      pass: current.pass,
      held: current.held,
      cancelRequested: current.cancelRequested,
      evidence: journal.evidence,
      [nativeExecutionJournalBrand]: true as const,
    };
  });
  const readNativeExecutionLinksInTransaction = Effect.fn(
    "WorkflowRunStore.readNativeExecutionLinksInTransaction",
  )(function* (operation: WorkflowOperationV1) {
    const nativeRunId =
      operation.parameters.kind === "stage_launch" || operation.parameters.kind === "supervision"
        ? operation.parameters.nativeRunId
        : ((yield* sql<{
            native_run_id: string | null;
          }>`SELECT native_run_id FROM workflow_planning_passes WHERE operation_id=${operation.id}`)[0]
            ?.native_run_id ?? null);
    return yield* sql<IntentRow>`SELECT * FROM workflow_native_launch_intents WHERE operation_id=${operation.id} OR finalized_operation_id=${operation.id} OR native_run_id=${nativeRunId}`;
  });
  const readNativeHeldRearmsInTransaction = Effect.fn(
    "WorkflowRunStore.readNativeHeldRearmsInTransaction",
  )(function* (owner: CustodyOwner, expectedOwnerVersion: number) {
    const heldRows =
      owner.kind === "workflow_run"
        ? yield* sql<{
            id: string;
            record_json: string;
          }>`SELECT id,record_json FROM workflow_operations WHERE workflow_run_id=${owner.workflowRunId} AND status='pending' AND held=1`
        : yield* sql<{
            id: string;
            record_json: string;
          }>`SELECT id,record_json FROM workflow_operations WHERE planning_session_id=${owner.planningSessionId} AND status='pending' AND held=1`;
    const operations: WorkflowOperationV1[] = [];
    for (const held of heldRows) {
      const operation = yield* Effect.try({
        try: () => parse(held.record_json),
        catch: integrity,
      }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity));
      if (operation.id !== held.id || !sameCustodyValue(owner, operation.owner))
        return yield* integrity();
      const links = yield* readNativeExecutionLinksInTransaction(operation);
      if (!links.length) continue;
      if (!nativeBinding || !currentAuthority) return nativeIntentDenied("authority");
      const row = links[0];
      if (
        links.length !== 1 ||
        !row ||
        row.operation_id !== operation.id ||
        row.finalized_operation_id !== operation.id
      )
        return yield* integrity();
      const reservation = yield* readNativeReservationInTransaction({ intentId: row.intent_id });
      const journal = yield* readNativeExecutionJournalInTransaction(reservation, row);
      if (!sameCustodyValue(operation, journal.operation)) return yield* integrity();
      if (
        !nativeBinding ||
        reservation.bootId !== nativeBinding.coordinator.bootId ||
        reservation.workerId !== nativeBinding.coordinator.workerId
      )
        return nativeIntentDenied("authority");
      const current = yield* verifyNativeIntentEligibility(
        reservation,
        row,
        expectedOwnerVersion,
        true,
        journal,
        "resume",
      );
      if (current.kind !== "current") return current;
      if (
        !(yield* verifyPreparingNativeRun(
          reservation.nativeRunId!,
          reservation.threadId,
          journal.evidence.events,
        ))
      )
        return nativeIntentDenied("state");
      operations.push(journal.operation);
    }
    return { kind: "current" as const, operations };
  });
  type ClaimedNativeOperationSelection = {
    readonly operationId: string;
    readonly expectedFingerprint: string;
    readonly expectedGeneration: number;
    readonly bootId: string;
    readonly workerId: string;
  };
  const verifyClaimedNativeOperationInTransaction = Effect.fn(
    "WorkflowRunStore.verifyClaimedNativeOperationInTransaction",
  )(
    function* (input: ClaimedNativeOperationSelection) {
      if (Option.isNone(yield* Effect.serviceOption(sql.transactionService)))
        return yield* integrity();
      if (!nativeBinding || !currentAuthority) return nativeIntentDenied("authority");
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM workflow_operations WHERE id=${input.operationId}`;
      if (rows.length === 0) return nativeIntentDenied("state");
      if (rows.length !== 1 || !rows[0]) return yield* integrity();
      const operation = yield* Effect.try({
        try: () => parse(rows[0]!.record_json),
        catch: integrity,
      }).pipe(Effect.flatMap(decodeWorkflowOperationV1), Effect.mapError(integrity));
      if (operation.id !== input.operationId) return yield* integrity();
      const links = yield* readNativeExecutionLinksInTransaction(operation);
      if (!links.length) return nativeIntentDenied("authority");
      const row = links[0];
      if (
        links.length !== 1 ||
        !row ||
        row.operation_id !== operation.id ||
        row.finalized_operation_id !== operation.id
      )
        return yield* integrity();
      const reservation = yield* readNativeReservationInTransaction({ intentId: row.intent_id });
      if (reservation.material.version !== 2) return nativeIntentDenied("authority");
      const journal = yield* readNativeExecutionJournalInTransaction(reservation, row);
      if (
        journal.operation.fingerprint !== input.expectedFingerprint ||
        journal.operation.controlGeneration !== input.expectedGeneration
      )
        return { kind: "stale" as const };
      const claim = journal.operation.claim;
      if (journal.operation.status !== "claimed" || !claim) return nativeIntentDenied("state");
      if (
        claim.generation !== input.expectedGeneration ||
        claim.bootId !== input.bootId ||
        claim.workerId !== input.workerId ||
        input.bootId !== nativeBinding.coordinator.bootId ||
        input.workerId !== nativeBinding.coordinator.workerId ||
        reservation.bootId !== nativeBinding.coordinator.bootId ||
        reservation.workerId !== nativeBinding.coordinator.workerId
      )
        return { kind: "stale" as const };
      const current = yield* verifyNativeIntentEligibility(
        reservation,
        row,
        undefined,
        true,
        journal,
        "claimed",
      );
      if (current.kind !== "current") return current;
      const footprint = yield* readCurrentNativeFootprintInTransaction(
        reservation.nativeRunId!,
        reservation.threadId,
        journal.evidence.events,
      );
      if (footprint.kind !== "current") return footprint;
      // Running projections can precede the SDK request; this read cannot prove whether input was sent.
      return Object.freeze({
        ...footprint,
        operationId: operation.id,
        intentId: reservation.intentId,
        operationFingerprint: journal.operation.fingerprint,
        generation: journal.operation.controlGeneration,
      });
    },
    Effect.mapError((error) =>
      isStoreError(error) ? error : new WorkflowRunStoreError({ reason: "persistence" }),
    ),
  );
  const nativePreparationEntry = (claim: PreparationJob.NativeClaim) =>
    `native-preparation:${WorkflowCanonicalHash.tagged("t3.native-preparation-entry.v1", claim)}`;
  const nativePreparationInput = (
    claim: PreparationJob.NativeClaim,
    retained: NativeReservation,
    current: {
      driver: string;
      providerInstanceId: string;
      providerThreadId: string;
      providerSessionId: string | null;
      nativeThreadId: string | null;
    },
  ) => {
    const imported = nativeFromLaunch(retained.material.launch);
    if (
      imported &&
      (imported.driver !== current.driver ||
        imported.providerInstanceId !== current.providerInstanceId ||
        (current.nativeThreadId !== null && imported.nativeId !== current.nativeThreadId))
    )
      throw integrity();
    return PreparationJob.ownInput({
      jobId: nativePreparationEntry(claim),
      entryKey: nativePreparationEntry(claim),
      kind: "launch",
      environmentId: retained.destination.environmentId,
      projectId: retained.destination.projectId,
      threadId: retained.threadId,
      runId: retained.nativeRunId,
      originatingLaunchJobId: null,
      parentSelection: null,
      serverBootId: claim.bootId,
      nativeSelection:
        current.nativeThreadId === null
          ? imported
          : {
              driver: current.driver,
              providerInstanceId: current.providerInstanceId,
              nativeId: current.nativeThreadId,
            },
      nativeOwner: { intentId: retained.intentId, owner: retained.owner, executionClaim: claim },
      request: {
        phase: "native-preparation",
        materialHash: retained.materialHash,
        launchHash: retained.launchHash,
        launch: retained.material.launch,
        current,
      },
    });
  };
  const validateNativePreparationHistory = Effect.fn(
    "WorkflowRunStore.validateNativePreparationHistory",
  )(function* (job: PreparationJob.Job, claim: PreparationJob.NativeClaim) {
    const owner = job.input.nativeOwner;
    if (job.identityVersion !== 3 || !owner || !sameCustodyValue(owner.executionClaim, claim))
      return yield* integrity();
    const retained = yield* readNativeReservationInTransaction({
      intentId: owner.intentId,
    });
    const observation = job.input.request.current;
    if (typeof observation !== "object" || observation === null || Array.isArray(observation))
      return yield* integrity();
    const current = observation as Record<string, unknown>;
    const validId = (value: unknown): value is string =>
      typeof value === "string" && value.trim().length > 0 && value.isWellFormed();
    if (
      Object.keys(current).sort().join() !==
        "driver,nativeThreadId,providerInstanceId,providerSessionId,providerThreadId" ||
      !validId(current.driver) ||
      !validId(current.providerInstanceId) ||
      !validId(current.providerThreadId) ||
      (current.providerSessionId !== null && !validId(current.providerSessionId)) ||
      (current.nativeThreadId !== null && !validId(current.nativeThreadId))
    )
      return yield* integrity();
    const observationSnapshot = {
      driver: current.driver,
      providerInstanceId: current.providerInstanceId,
      providerThreadId: current.providerThreadId,
      providerSessionId: current.providerSessionId,
      nativeThreadId: current.nativeThreadId,
    };
    const expected = yield* Effect.try({
      try: () => nativePreparationInput(claim, retained, observationSnapshot),
      catch: integrity,
    });
    if (
      !sameCustodyValue(expected, job.input) ||
      retained.operationId !== claim.operationId ||
      retained.material.version !== 2
    )
      return yield* integrity();
    return retained;
  });
  const beginClaimedNativePreparation = Effect.fn("WorkflowRunStore.beginClaimedNativePreparation")(
    function* (claim: PreparationJob.NativeClaim) {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* integrity();
      const result = yield* inTransaction(
        Effect.gen(function* () {
          const entry = nativePreparationEntry(claim);
          const rows =
            yield* sql`SELECT * FROM orchestration_v2_preparation_jobs WHERE job_id=${entry} OR entry_key=${entry}`;
          if (rows.length) {
            if (rows.length !== 1) return yield* integrity();
            const job = yield* Effect.try({
              try: () => PreparationJob.decodeRow(rows[0]),
              catch: integrity,
            });
            yield* validateNativePreparationHistory(job, claim);
            return Object.freeze({ kind: "existing" as const, job });
          }
          const current = yield* verifyClaimedNativeOperationInTransaction(claim);
          if (current.kind !== "current") return current;
          if (current.stage !== "preparing") return nativeIntentDenied("state");
          const retained = yield* readNativeReservationInTransaction({
            intentId: current.intentId,
          });
          if (
            retained.nativeRunId !== current.runId ||
            retained.threadId !== current.threadId ||
            retained.operationId !== claim.operationId
          )
            return yield* integrity();
          const observation = {
            driver: current.driver,
            providerInstanceId: current.providerInstanceId,
            providerThreadId: current.providerThreadId,
            providerSessionId: current.providerSessionId,
            nativeThreadId: current.nativeThreadId,
          };
          const input = yield* Effect.try({
            try: () => nativePreparationInput(claim, retained, observation),
            catch: integrity,
          });
          const encoded = PreparationJob.identity(input);
          const createdAt = DateTime.formatIso(yield* DateTime.now);
          const inserted =
            yield* sql`INSERT INTO orchestration_v2_preparation_jobs ${sql.insert({ format_version: 1, job_id: input.jobId, entry_key: input.entryKey, kind: input.kind, environment_id: input.environmentId, project_id: input.projectId, thread_id: input.threadId, run_id: input.runId, originating_launch_job_id: null, server_boot_id: input.serverBootId, request_identity_json: encoded.text, request_identity_hash: encoded.hash, created_at: createdAt })} RETURNING *`;
          if (inserted.length !== 1) return yield* integrity();
          const job = yield* Effect.try({
            try: () => PreparationJob.decodeRow(inserted[0]),
            catch: integrity,
          });
          return Object.freeze({ kind: "acquired" as const, job });
        }),
      );
      if (result.kind !== "acquired") return result;
      yield* notify;
      return yield* Effect.sync(() => {
        preparationAcquisitions.set(result, { job: result.job, consumed: false });
        return result;
      });
    },
  );
  const consumeNativePreparationOwner = Effect.fn("WorkflowRunStore.consumeNativePreparationOwner")(
    function* (acquired: object) {
      const runtime = nativeBinding;
      const acquisition = preparationAcquisitions.get(acquired);
      if (!runtime || !acquisition || acquisition.consumed || runtime.scope.state._tag === "Closed")
        return yield* integrity();
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* integrity();
      acquisition.consumed = true;
      const claim = acquisition.job.input.nativeOwner?.executionClaim;
      if (!claim) return yield* integrity();
      const actor: PreparationActor = { job: acquisition.job, resources: new Map() };
      preparationActors.add(actor);
      const flags = { intakeSealed: false, stopping: false, blocked: false };
      const mutex = yield* Semaphore.make(1);
      type Gate = Effect.Success<ReturnType<typeof PreparationGate.preparePreparationProcessGate>>;
      type Pending = Effect.Success<ReturnType<Gate["spawnHeld"]>>;
      type Held = Effect.Success<ReturnType<Pending["awaitHeld"]>>;
      type CommandRecord = {
        readonly id: string;
        readonly state: PreparationOwner.CommandState;
        readonly gate: Gate;
        resource?: PreparationResource.Resource;
        pending?: Pending;
        held?: Held;
        receipt?: PreparationGate.PreparationGateLocalReceipt;
        createAttempted: boolean;
        releaseAttempted: boolean;
      };
      const commands = new Map<object, CommandRecord>();
      const outside = Effect.fn("WorkflowRunStore.preparationOutsideWriter")(function* (
        stage: PreparationOwner.PreparationProcessOwnerError["stage"],
      ) {
        if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
          return yield* PreparationOwner.error(stage, "invalid");
      });
      const checkJob = Effect.fn("WorkflowRunStore.checkPreparationActor")(function* () {
        const job = yield* readPreparationJob(actor.job.input.jobId);
        if (
          !preparationMatches(job, PreparationJob.selection(actor.job)) ||
          job.identityVersion !== 3 ||
          job.input.environmentId !== runtime.environmentId ||
          claim.bootId !== runtime.coordinator.bootId ||
          claim.workerId !== runtime.coordinator.workerId
        )
          return yield* stale();
        yield* validateNativePreparationHistory(job, claim);
        return job;
      });
      const checkNewWork = Effect.fn("WorkflowRunStore.checkPreparationWork")(function* (
        state?: PreparationOwner.CommandState,
      ) {
        const job = yield* checkJob();
        if (
          flags.stopping ||
          flags.blocked ||
          runtime.scope.state._tag === "Closed" ||
          job.state !== "active" ||
          job.codeClosedAt !== null ||
          (state &&
            (state.sealed ||
              [state.callerScope, state.commandScope, state.ownerScope, state.storeScope].some(
                (scope) => scope.state._tag === "Closed",
              )))
        )
          return yield* stale();
        const current = yield* verifyClaimedNativeOperationInTransaction(claim);
        if (current.kind !== "current" || current.stage !== "preparing") return yield* stale();
        const observation = {
          driver: current.driver,
          providerInstanceId: current.providerInstanceId,
          providerThreadId: current.providerThreadId,
          providerSessionId: current.providerSessionId,
          nativeThreadId: current.nativeThreadId,
        };
        if (
          !sameCustodyValue(observation, job.input.request.current) ||
          current.threadId !== job.input.threadId ||
          current.runId !== job.input.runId
        )
          return yield* stale();
        const owner = job.input.nativeOwner;
        if (
          !owner ||
          (yield* walkResourceCustodyInTransaction(
            {
              purpose: "physical",
              environmentId: runtime.environmentId,
              threadId: job.input.threadId,
              expectedOwner: yield* decodePreparationWorkflowOwner(owner.owner).pipe(
                Effect.mapError(integrity),
              ),
              nativeSelection: job.input.nativeSelection ?? null,
            },
            actor,
          ))
        )
          return yield* stale();
        return job;
      });
      // Selection changes follow observed commit returns while the same-owner mutex is still held.
      const writer = <A, E>(
        stage: PreparationOwner.PreparationProcessOwnerError["stage"],
        body: Effect.Effect<A, E>,
        accept: (value: A) => void,
      ) =>
        outside(stage).pipe(
          Effect.andThen(
            mutex
              .withPermit(
                inTransaction(body).pipe(
                  Effect.interruptible,
                  Effect.tap((value) => Effect.sync(() => accept(value))),
                ),
              )
              .pipe(Effect.interruptible),
          ),
          (work) => PreparationOwner.sanitize(stage, work),
          Effect.onError(() =>
            Effect.sync(() => {
              flags.blocked = true;
              flags.intakeSealed = true;
            }),
          ),
        );
      const acceptResource = (record: CommandRecord, value: PreparationResource.Resource) => {
        record.resource = value;
        actor.resources.set(record.id, value);
      };
      const checkResource = Effect.fn("WorkflowRunStore.checkPreparationResource")(function* (
        record: CommandRecord,
      ) {
        yield* checkJob();
        const expected = record.resource;
        if (!expected) return yield* integrity();
        const rows =
          yield* sql`SELECT * FROM orchestration_v2_preparation_process_resources WHERE resource_id=${record.id}`;
        if (rows.length !== 1) return yield* stale();
        const resource = yield* Effect.try({
          try: () => PreparationResource.decodeRow(rows[0]),
          catch: integrity,
        });
        if (
          !sameCustodyValue(resource, expected) ||
          resource.recordVersion === Number.MAX_SAFE_INTEGER
        )
          return yield* stale();
        return resource;
      });
      const updateResource = Effect.fn("WorkflowRunStore.updatePreparationResource")(function* (
        record: CommandRecord,
        patch: Readonly<Record<string, string | number | null>>,
      ) {
        const resource = yield* checkResource(record);
        const rows =
          yield* sql`UPDATE orchestration_v2_preparation_process_resources SET ${sql.update({ ...patch, record_version: resource.recordVersion + 1 })} WHERE resource_id=${record.id} AND creation_context_hash=${resource.contextHash} AND record_version=${resource.recordVersion} RETURNING *`;
        if (rows.length !== 1) return yield* stale();
        return yield* Effect.try({
          try: () => PreparationResource.decodeRow(rows[0]),
          catch: integrity,
        });
      });
      const recordBinding = Effect.fn("WorkflowRunStore.recordPreparationBinding")(function* (
        record: CommandRecord,
        evidence: unknown,
      ) {
        const resource = record.resource;
        const pending = record.pending;
        if (!resource || !pending) return yield* integrity();
        const binding = yield* Effect.try({
          try: () => PreparationResource.ownBinding(evidence, resource.context),
          catch: integrity,
        });
        if (binding.pid !== pending.pid) return yield* integrity();
        if (resource.binding !== null) {
          if (!sameCustodyValue(binding, resource.binding)) return yield* integrity();
          return;
        }
        const encoded = PreparationResource.encodeBinding(record.id, resource.context, binding);
        yield* writer(
          "binding",
          Effect.gen(function* () {
            const now = DateTime.formatIso(yield* DateTime.now);
            return yield* updateResource(record, {
              creation_started_at: resource.creationStartedAt ?? now,
              child_pid: pending.pid,
              binding_json: encoded.text,
              binding_hash: encoded.hash,
              bound_at: now,
            });
          }),
          (value) => acceptResource(record, value),
        );
      });
      const cleanup = Effect.fn("WorkflowRunStore.cleanupPreparationCommand")(function* (
        record: CommandRecord,
      ) {
        yield* outside("cleanup");
        const pending = record.pending;
        if (!pending) return;
        // Physical stop precedes terminal waiting and all optional persistence work.
        const observed = yield* Effect.result(pending.observeRetirement());
        let scope = observed._tag === "Success" ? observed.success : undefined;
        const running = yield* Effect.result(pending.isRunning);
        if (!scope || running._tag === "Failure" || running.success) {
          yield* pending.stopOwnedChild().pipe(Effect.ignore);
          const stopped = yield* Effect.result(pending.stopAndObserve());
          scope = stopped._tag === "Success" ? stopped.success : undefined;
        }
        const terminal = yield* pending.awaitTerminal.pipe(
          Effect.timeoutOption("2 seconds"),
          Effect.interruptible,
          Effect.result,
        );
        if (
          !scope ||
          terminal._tag !== "Success" ||
          Option.isNone(terminal.success) ||
          !record.resource
        )
          return;
        const binding = record.held?.binding ?? scope.binding;
        if (!sameCustodyValue(binding, scope.binding)) return;
        yield* recordBinding(record, binding).pipe(Effect.ignore);
        const resource = record.resource;
        if (!resource?.binding || resource.state === "settled") return;
        const result = terminal.success.value;
        const encoded = yield* Effect.try({
          try: () =>
            PreparationResource.encodeSettlement(
              record.id,
              resource.context,
              resource.binding!,
              resource.recordVersion,
              {
                v: 1,
                resourceId: record.id,
                contextHash: resource.contextHash,
                priorVersion: resource.recordVersion,
                childExit: { pid: pending.pid, exitCode: result.code, signal: result.signal },
                scope,
              },
            ),
          catch: integrity,
        });
        yield* writer(
          "cleanup",
          updateResource(record, {
            state: "settled",
            settlement_json: encoded.text,
            settlement_hash: encoded.hash,
            settled_at: DateTime.formatIso(yield* DateTime.now),
          }),
          (value) => acceptResource(record, value),
        ).pipe(Effect.ignore);
      }, Effect.interruptible);
      const readCommand = (ref: object) => {
        const record = commands.get(ref);
        return record
          ? Effect.succeed(record)
          : Effect.fail(PreparationOwner.error("input", "invalid"));
      };
      const port: PreparationOwner.CommandPort = {
        checkOutsideWriter: outside,
        prepareCommand: Effect.fn("WorkflowRunStore.prepareOwnedCommand")(function* (input, state) {
          yield* writer(
            "admission",
            Effect.gen(function* () {
              const job = yield* checkNewWork(state);
              if (job.processExposed) return job;
              const rows =
                yield* sql`UPDATE orchestration_v2_preparation_jobs SET process_exposed=1,record_version=${job.recordVersion + 1} WHERE job_id=${job.input.jobId} AND record_version=${job.recordVersion} RETURNING *`;
              if (rows.length !== 1) return yield* stale();
              return yield* Effect.try({
                try: () => PreparationJob.decodeRow(rows[0]),
                catch: integrity,
              });
            }),
            (job) => {
              actor.job = job;
            },
          );
          const gate = yield* PreparationOwner.sanitize(
            "admission",
            PreparationGate.preparePreparationProcessGate({
              ...input,
              threadId: actor.job.input.threadId,
            }).pipe(
              Effect.provideService(AgentScope, runtime.agentScope),
              Effect.provideService(PreparationNode.PreparationNodeChild, runtime.nodeChild),
              Effect.provideService(Path.Path, runtime.path),
              Effect.provideService(HostProcess.Environment, runtime.hostEnvironment),
              Effect.provideService(HostProcess.WorkingDirectory, runtime.hostCwd),
              Effect.provideService(Scope.Scope, state.commandScope),
            ),
          );
          const record: CommandRecord = {
            id: NodeCrypto.randomUUID(),
            state,
            gate,
            createAttempted: false,
            releaseAttempted: false,
          };
          const ref = Object.freeze({});
          commands.set(ref, record);
          yield* Scope.addFinalizer(state.commandScope, cleanup(record).pipe(Effect.ignore));
          yield* writer(
            "admission",
            Effect.gen(function* () {
              const job = yield* checkNewWork(state);
              const context = PreparationResource.ownContext({
                v: 1,
                profile: "linux-node-no-stdin-v1",
                jobSelection: PreparationJob.selection(job),
                environmentId: job.input.environmentId,
                projectId: job.input.projectId,
                threadId: job.input.threadId,
                runId: job.input.runId,
                executionClaim: claim,
                current: job.input.request.current,
                host: gate.hostIdentity,
                proposedUnit: gate.proposedUnit,
                creationInputHash: gate.creationInputHash,
              });
              const encoded = PreparationResource.encodeContext(context);
              const rows =
                yield* sql`INSERT INTO orchestration_v2_preparation_process_resources ${sql.insert({
                  format_version: 1,
                  resource_id: record.id,
                  job_id: job.input.jobId,
                  job_request_identity_hash: job.requestIdentityHash,
                  environment_id: job.input.environmentId,
                  project_id: job.input.projectId,
                  thread_id: job.input.threadId,
                  run_id: job.input.runId,
                  server_boot_id: job.input.serverBootId,
                  host_boot_id: gate.hostIdentity.hostBootId,
                  scope_namespace: gate.hostIdentity.scopeNamespace,
                  proposed_unit: gate.proposedUnit,
                  creation_input_hash: gate.creationInputHash,
                  creation_context_json: encoded.text,
                  creation_context_hash: encoded.hash,
                  created_at: DateTime.formatIso(yield* DateTime.now),
                })} RETURNING *`;
              if (rows.length !== 1) return yield* integrity();
              return yield* Effect.try({
                try: () => PreparationResource.decodeRow(rows[0]),
                catch: integrity,
              });
            }),
            (resource) => acceptResource(record, resource),
          );
          return ref;
        }),
        createCommand: Effect.fn("WorkflowRunStore.createOwnedCommand")(function* (ref) {
          const record = yield* readCommand(ref);
          yield* writer(
            "creation",
            Effect.gen(function* () {
              yield* checkNewWork(record.state);
              yield* checkResource(record);
              if (record.createAttempted) return yield* integrity();
              yield* Effect.uninterruptible(
                Effect.gen(function* () {
                  if (
                    flags.stopping ||
                    flags.blocked ||
                    record.state.sealed ||
                    [
                      record.state.callerScope,
                      record.state.commandScope,
                      record.state.ownerScope,
                      record.state.storeScope,
                    ].some((scope) => scope.state._tag === "Closed")
                  )
                    return yield* stale();
                  record.createAttempted = true;
                  record.pending = yield* PreparationOwner.sanitize(
                    "creation",
                    record.gate
                      .spawnHeld()
                      .pipe(Effect.provideService(Scope.Scope, record.state.commandScope)),
                  );
                }),
              );
              if (!record.pending) return yield* integrity();
              return yield* updateResource(record, {
                creation_started_at: DateTime.formatIso(yield* DateTime.now),
                child_pid: record.pending.pid,
              });
            }),
            (value) => acceptResource(record, value),
          );
          if (!record.pending) return yield* PreparationOwner.error("creation", "unknown");
          return {
            resourceId: record.id,
            stderr: record.pending.stderr.pipe(
              Stream.mapError(() => PreparationOwner.error("output", "unknown")),
            ),
          };
        }),
        startCommand: Effect.fn("WorkflowRunStore.startOwnedCommand")(function* (ref) {
          const record = yield* readCommand(ref);
          if (!record.pending) return yield* PreparationOwner.error("ready", "unknown");
          record.held = yield* PreparationOwner.sanitize(
            "ready",
            record.pending
              .awaitHeld()
              .pipe(Effect.provideService(Scope.Scope, record.state.commandScope)),
          );
          yield* PreparationOwner.sanitize("binding", recordBinding(record, record.held.binding));
          const held = record.held;
          yield* writer(
            "release",
            Effect.gen(function* () {
              yield* checkNewWork(record.state);
              const resource = yield* checkResource(record);
              if (
                record.releaseAttempted ||
                !resource.binding ||
                !sameCustodyValue(resource.binding, held.binding)
              )
                return yield* integrity();
              record.releaseAttempted = true;
              const receipt = yield* PreparationOwner.sanitize("release", held.submitLocal());
              record.receipt = receipt;
              return yield* updateResource(record, {
                release_started_at: DateTime.formatIso(yield* DateTime.now),
              });
            }),
            (value) => acceptResource(record, value),
          );
          const receipt = record.receipt;
          if (!receipt) return yield* PreparationOwner.error("release", "unknown");
          return (yield* PreparationOwner.sanitize("output", held.awaitExec(receipt))).pipe(
            Stream.mapError(() => PreparationOwner.error("output", "unknown")),
          );
        }),
        collectCommand: Effect.fn("WorkflowRunStore.collectOwnedCommand")(function* (ref) {
          const record = yield* readCommand(ref);
          if (!record.pending) return yield* PreparationOwner.error("output", "unknown");
          return yield* PreparationOwner.sanitize("output", record.pending.awaitExit);
        }),
        assessInventory: Effect.fn("WorkflowRunStore.assessPreparationInventory")(function* () {
          yield* outside("inventory");
          const checked = yield* Effect.result(
            mutex
              .withPermit(inTransaction(validateActorInventory(actor)))
              .pipe(Effect.interruptible),
          );
          const complete = checked._tag === "Success" && commands.size === actor.resources.size;
          const resources = [...actor.resources.values()].map((resource) => ({
            resourceId: resource.resourceId,
            state:
              complete && resource.state === "settled" ? ("settled" as const) : ("held" as const),
          }));
          return {
            kind: "sealed" as const,
            jobId: actor.job.input.jobId,
            inventory:
              complete &&
              !flags.blocked &&
              resources.every((resource) => resource.state === "settled")
                ? ("all-retired" as const)
                : ("held" as const),
            resources,
          };
        }),
      };
      yield* writer("admission", checkNewWork(), () => {});
      return yield* PreparationOwner.make(
        {
          jobId: actor.job.input.jobId,
          threadId: actor.job.input.threadId,
          runId: actor.job.input.runId!,
        },
        runtime.scope,
        port,
        flags,
      );
    },
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
    const inheritedTransaction = Option.isSome(yield* Effect.serviceOption(sql.transactionService));
    const result = yield* inTransaction(
      Effect.gen(function* () {
        const rows = yield* sql<{
          record_json: string;
          status: string;
          held: number;
          cancel_requested: number;
          generation: number;
          workflow_run_id: string | null;
          planning_session_id: string | null;
        }>`SELECT record_json, status, held, cancel_requested, generation, workflow_run_id, planning_session_id FROM workflow_operations WHERE id = ${input.operationId}`;
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
        const nativeLinks = yield* readNativeExecutionLinksInTransaction(operation);
        const nativeLinked = nativeLinks.length > 0;
        if (nativeLinked && inheritedTransaction) return yield* integrity();
        const now = nativeLinked ? DateTime.formatIso(yield* DateTime.now) : input.now;
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
          if (!isWorkflowTimestamp(now) || !isWorkflowTimestampAfter(session.limits.deadline, now))
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
        let nativeJournal: NativeExecutionJournal | undefined;
        if (nativeLinked) {
          if (row.cancel_requested !== 0) return { kind: "denied" as const };
          if (!nativeBinding || !currentAuthority) return { kind: "denied" as const };
          const link = nativeLinks[0];
          if (
            nativeLinks.length !== 1 ||
            !link ||
            link.operation_id !== operation.id ||
            link.finalized_operation_id !== operation.id ||
            link.state !== "finalized"
          )
            return yield* integrity();
          const reservation = yield* readNativeReservationInTransaction({
            intentId: link.intent_id,
          });
          if (reservation.material.version !== 2) return { kind: "denied" as const };
          if (
            reservation.bootId !== nativeBinding.coordinator.bootId ||
            reservation.workerId !== nativeBinding.coordinator.workerId ||
            input.bootId !== nativeBinding.coordinator.bootId ||
            input.workerId !== nativeBinding.coordinator.workerId
          )
            return { kind: "denied" as const };
          nativeJournal = yield* readNativeExecutionJournalInTransaction(reservation, link);
          if (!sameCustodyValue(nativeJournal.operation, operation)) return yield* integrity();
          const eligibility = yield* verifyNativeIntentEligibility(
            reservation,
            link,
            input.expectedOwnerVersion,
            true,
            nativeJournal,
          );
          if (eligibility.kind !== "current")
            return eligibility.kind === "stale"
              ? { kind: "stale" as const }
              : { kind: "denied" as const };
          if (
            !(yield* verifyPreparingNativeRun(
              reservation.nativeRunId!,
              reservation.threadId,
              nativeJournal.evidence.events,
            ))
          )
            return { kind: "denied" as const };
        }
        const claimed = yield* decodeWorkflowOperationV1({
          ...operation,
          status: "claimed",
          claim: {
            bootId: input.bootId,
            workerId: input.workerId,
            generation: input.expectedGeneration,
            claimedAt: now,
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
        if (nativeJournal) {
          const reservation = yield* readNativeReservationInTransaction({
            intentId: nativeJournal.reservation.intentId,
          });
          const links =
            yield* sql<IntentRow>`SELECT * FROM workflow_native_launch_intents WHERE intent_id=${reservation.intentId}`;
          if (links.length !== 1 || !links[0]) return yield* integrity();
          const current = yield* readNativeExecutionJournalInTransaction(reservation, links[0]);
          if (
            current.held !== 0 ||
            current.cancelRequested !== 0 ||
            !sameCustodyValue(current.operation, claimed) ||
            (planningPass &&
              !sameCustodyValue(current.pass, { ...planningPass, status: "claimed" }))
          )
            return yield* integrity();
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
    // V2 authority and admission inputs must be owned before returning the writer Effect.
    reserveWorkflowNativeIntent: (input: WorkflowNativeInput) => {
      if (input.materialVersion !== 2) return reserveWorkflowNativeIntent(input);
      try {
        WorkflowCanonicalHash.assertCanonical(input);
        return reserveWorkflowNativeIntent(structuredClone(input));
      } catch {
        return Effect.fail(integrity());
      }
    },
    reservePlanningNativeIntent: (input: PlanningNativeInput) => {
      if (input.materialVersion !== 2) return reservePlanningNativeIntent(input);
      try {
        WorkflowCanonicalHash.assertCanonical(input);
        return reservePlanningNativeIntent(structuredClone(input));
      } catch {
        return Effect.fail(planningError("invalid"));
      }
    },
    bindNativeIntent: (input: NativeBindingInput) => {
      try {
        WorkflowCanonicalHash.assertCanonical(input);
        if (
          Object.keys(input).sort().join() !== "expectedFingerprint,expectedGeneration,intentId" ||
          typeof input.intentId !== "string" ||
          !input.intentId.trim() ||
          !/^[0-9a-f]{64}$/.test(input.expectedFingerprint) ||
          !Number.isSafeInteger(input.expectedGeneration) ||
          input.expectedGeneration < 0
        )
          throw integrity();
        return bindNativeIntent(structuredClone(input));
      } catch {
        return Effect.fail(integrity());
      }
    },
    finalizeWorkflowNativeIntent: (input: NativeBindingInput) => {
      try {
        WorkflowCanonicalHash.assertCanonical(input);
        if (
          Object.keys(input).sort().join() !== "expectedFingerprint,expectedGeneration,intentId" ||
          typeof input.intentId !== "string" ||
          !input.intentId.trim() ||
          !/^[0-9a-f]{64}$/.test(input.expectedFingerprint) ||
          !Number.isSafeInteger(input.expectedGeneration) ||
          input.expectedGeneration < 0
        )
          throw integrity();
        return finalizeWorkflowNativeIntent(structuredClone(input));
      } catch {
        return Effect.fail(integrity());
      }
    },
    finalizePlanningNativeIntent: (input: NativeBindingInput) => {
      try {
        WorkflowCanonicalHash.assertCanonical(input);
        if (
          Object.keys(input).sort().join() !== "expectedFingerprint,expectedGeneration,intentId" ||
          typeof input.intentId !== "string" ||
          !input.intentId.trim() ||
          !/^[0-9a-f]{64}$/.test(input.expectedFingerprint) ||
          !Number.isSafeInteger(input.expectedGeneration) ||
          input.expectedGeneration < 0
        )
          throw integrity();
        return finalizePlanningNativeIntent(structuredClone(input));
      } catch {
        return Effect.fail(integrity());
      }
    },
    verifyClaimedNativeOperationInTransaction: (input: ClaimedNativeOperationSelection) => {
      try {
        WorkflowCanonicalHash.assertCanonical(input);
        if (
          Object.keys(input).sort().join() !==
            "bootId,expectedFingerprint,expectedGeneration,operationId,workerId" ||
          ![input.operationId, input.bootId, input.workerId].every(
            (value) => typeof value === "string" && value.trim().length > 0,
          ) ||
          !/^[0-9a-f]{64}$/.test(input.expectedFingerprint) ||
          !Number.isSafeInteger(input.expectedGeneration) ||
          input.expectedGeneration < 0
        )
          throw integrity();
        return verifyClaimedNativeOperationInTransaction({ ...input });
      } catch {
        return Effect.fail(integrity());
      }
    },
    verifyClaimedNativeIntentInTransaction: (input: ClaimedNativeIntentSelection) => {
      try {
        WorkflowCanonicalHash.assertCanonical(input);
        if (
          Object.keys(input).sort().join() !== "bootId,intentId,workerId" ||
          ![input.intentId, input.bootId, input.workerId].every(
            (id) => typeof id === "string" && id.trim().length > 0,
          )
        )
          throw integrity();
        return verifyClaimedNativeIntentInTransaction(structuredClone(input));
      } catch {
        return Effect.fail(integrity());
      }
    },
    claimNativeIntent: (input: NativeIntentClaimInput) => {
      try {
        WorkflowCanonicalHash.assertCanonical(input);
        if (
          Object.keys(input).sort().join() !==
            "bootId,expectedFingerprint,expectedGeneration,expectedOwnerVersion,intentId,workerId" ||
          ![input.intentId, input.bootId, input.workerId].every(
            (id) => typeof id === "string" && id.trim().length > 0,
          ) ||
          !/^[0-9a-f]{64}$/.test(input.expectedFingerprint) ||
          ![input.expectedOwnerVersion, input.expectedGeneration].every(
            (value) => Number.isSafeInteger(value) && value >= 0,
          )
        )
          throw integrity();
        return claimNativeIntent(structuredClone(input));
      } catch {
        return Effect.fail(integrity());
      }
    },
    consumeNativePreparationOwner,
    beginClaimedNativePreparation: (claim: PreparationJob.NativeClaim) => {
      try {
        return beginClaimedNativePreparation(PreparationJob.ownNativeClaim(claim));
      } catch {
        return Effect.fail(integrity());
      }
    },
    beginOrdinaryPreparation: (input: PreparationJob.Input) => {
      try {
        return beginOrdinaryPreparation(PreparationJob.ownInput(input));
      } catch {
        return Effect.fail(integrity());
      }
    },
    markNativePreparationProcessExposed: (input: PreparationJob.NativeControl) =>
      ownNativePreparationMutation(input, "expose"),
    closeNativePreparationCode: (input: PreparationJob.NativeControl) =>
      ownNativePreparationMutation(input, "close"),
    quiesceNativePreparationJob: (
      input: PreparationJob.NativeControl,
      disposition: "quiescing" | "unknown" = "quiescing",
    ) =>
      disposition === "quiescing" || disposition === "unknown"
        ? ownNativePreparationMutation(input, disposition)
        : Effect.fail(integrity()),
    markPreparationProcessExposed: (selected: PreparationJob.Selection) =>
      ownPreparationMutation(selected, "expose"),
    closePreparationCode: (selected: PreparationJob.Selection) =>
      ownPreparationMutation(selected, "close"),
    quiescePreparationJob: (
      selected: PreparationJob.Selection,
      disposition: "quiescing" | "unknown" = "quiescing",
    ) =>
      disposition === "quiescing" || disposition === "unknown"
        ? ownPreparationMutation(selected, disposition)
        : Effect.fail(integrity()),
    settleCodeOnlyPreparation: (selected: PreparationJob.Selection) =>
      ownPreparationMutation(selected, "settle"),
    readThreadCustody,
    readNativeReservationInTransaction: (
      input: Parameters<typeof readNativeReservationInTransaction>[0],
    ) => readNativeReservationInTransaction(input),
    readRecoveryCustodyInTransaction: (
      input: Parameters<typeof readRecoveryCustodyInTransaction>[0],
    ) => {
      try {
        const optional = Object.getOwnPropertyDescriptor(input, "providerThreadIds");
        let data = input;
        if (optional?.enumerable && "value" in optional && optional.value === undefined) {
          const { providerThreadIds: _absent, ...descriptors } =
            Object.getOwnPropertyDescriptors(input);
          data = Object.create(Object.getPrototypeOf(input), descriptors) as typeof input;
        }
        WorkflowCanonicalHash.assertCanonical(data);
        const owned = structuredClone(data);
        Object.freeze(owned.providerSessionIds);
        if (owned.providerThreadIds !== undefined) Object.freeze(owned.providerThreadIds);
        return readRecoveryCustodyInTransaction(Object.freeze(owned));
      } catch {
        return Effect.fail(integrity());
      }
    },
    subscribeChanges,
    listPending,
    claim: (input: Parameters<typeof claim>[0]) => claim({ ...input }),
    readRecovery,
    compareAndApplyRecovery,
  };
});

export const make = Effect.fn("WorkflowRunStore.make")(function* (
  beforePublish: Effect.Effect<void> = Effect.void,
) {
  return yield* makeStore(beforePublish);
});

export const makeCapturedAuthorizedWorkflowRunStore = Effect.fn(
  "WorkflowRunStore.makeCapturedAuthorized",
)(function* (
  sql: SqlClient.SqlClient,
  identity: ServerEnvironmentIdentity["Service"],
  policy: ManagedPolicy["Service"],
) {
  const authority = yield* ExecutionAuthority.makeCapturedWorkflowExecutionAuthority(
    sql,
    identity,
    policy,
  );
  return yield* makeStore(Effect.void, authority).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
  );
});

const makeNativeStore = Effect.fn("WorkflowRunStore.makeCapturedNative")(function* (
  sql: SqlClient.SqlClient,
  config: ServerConfig["Service"],
  path: Path.Path,
  identity: ServerEnvironmentIdentity["Service"],
  policy: ManagedPolicy["Service"],
  coordinator: NativeCoordinator,
) {
  const environmentId = yield* identity.getEnvironmentId;
  const authority = yield* ExecutionAuthority.makeCapturedWorkflowExecutionAuthority(
    sql,
    { getEnvironmentId: Effect.succeed(environmentId) },
    policy,
  );
  const protocol = yield* NativeProtocol.makeNativeCommandProtocol(sql, config, path);
  const scope = yield* Effect.scope;
  const agentScope = yield* AgentScope;
  const nodeChild = yield* PreparationNode.PreparationNodeChild;
  const hostEnvironment = yield* HostProcess.Environment;
  const hostCwd = yield* HostProcess.WorkingDirectory;
  return yield* makeStore(Effect.void, authority, {
    protocol,
    coordinator,
    environmentId,
    scope,
    agentScope: Object.freeze({ ...agentScope }),
    nodeChild: Object.freeze({ prepare: nodeChild.prepare }),
    path,
    hostEnvironment: Object.freeze({ ...hostEnvironment }),
    hostCwd,
  }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
});

export function makeCapturedNativeWorkflowRunStore(
  sql: SqlClient.SqlClient,
  config: ServerConfig["Service"],
  path: Path.Path,
  identity: ServerEnvironmentIdentity["Service"],
  policy: ManagedPolicy["Service"],
  coordinator: NativeCoordinator,
) {
  const owned = Result.try({
    try: () => {
      WorkflowCanonicalHash.assertCanonical(coordinator);
      const ownedCoordinator = Object.freeze({ ...coordinator });
      if (
        Object.keys(ownedCoordinator).sort().join() !== "bootId,workerId" ||
        ![ownedCoordinator.bootId, ownedCoordinator.workerId].every(
          (id) => typeof id === "string" && id.trim().length > 0,
        )
      )
        throw integrity();
      return { config: { ...config, baseDir: config.baseDir }, coordinator: ownedCoordinator };
    },
    catch: integrity,
  });
  return Effect.fromResult(owned).pipe(
    Effect.flatMap((input) =>
      makeNativeStore(sql, input.config, path, identity, policy, input.coordinator),
    ),
  );
}

export class WorkflowRunStore extends Context.Service<
  WorkflowRunStore,
  Effect.Success<ReturnType<typeof make>>
>()("t3/workflowRuns/WorkflowRunStore") {}
export const layer = Layer.effect(WorkflowRunStore, make());
export const layerWithPublishFault = (fault: Effect.Effect<void>) =>
  Layer.effect(WorkflowRunStore, make(fault));
