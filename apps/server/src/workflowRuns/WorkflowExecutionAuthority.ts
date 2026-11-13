import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentId,
  isRuntimeModeAbove,
  OrchestrationV2AppThreadJson,
  ProjectId,
  RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { AuthSessionExecutionConstraints, type VerifiedSession } from "../auth/SessionStore.ts";
import type { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import type { ManagedPolicy } from "../policy/ManagedPolicy.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";

export const LocalRpcExecutionAuthority = Schema.Struct({
  ...AuthSessionExecutionConstraints.fields,
  kind: Schema.Literal("local-rpc-session"),
  provenance: Schema.Literals(["verified-session", "rpc-ticket"]),
});
export type LocalRpcExecutionAuthority = typeof LocalRpcExecutionAuthority.Type;

const Resource = Schema.Struct({
  environmentId: EnvironmentId,
  projectId: ProjectId,
  threadId: Schema.optionalKey(ThreadId),
});
const Check = Schema.Struct({
  reference: LocalRpcExecutionAuthority,
  destination: Resource,
  origins: Schema.Array(Resource),
  runtimeMode: RuntimeMode,
});
const decodeReference = Schema.decodeUnknownSync(LocalRpcExecutionAuthority);
const encodeReference = Schema.encodeSync(LocalRpcExecutionAuthority);
const decodeCheck = Schema.decodeUnknownSync(Check);
const encodeCheck = Schema.encodeSync(Check);
const decodeThread = Schema.decodeUnknownEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);

export class WorkflowExecutionAuthorityError extends Schema.TaggedError<WorkflowExecutionAuthorityError>()(
  "WorkflowExecutionAuthorityError",
  { reason: Schema.Literals(["unauthorized", "transaction", "persistence"]) },
) {}
const isAuthorityError = Schema.is(WorkflowExecutionAuthorityError);
const denied = () => new WorkflowExecutionAuthorityError({ reason: "unauthorized" });

export function captureLocalRpcExecutionAuthority(
  session: Pick<
    VerifiedSession,
    | "sessionId"
    | "subject"
    | "method"
    | "scopes"
    | "expiresAt"
    | "runtimeModeCeiling"
    | "peerLink"
    | "proofKeyThumbprint"
  >,
  environmentId: EnvironmentId,
  provenance: LocalRpcExecutionAuthority["provenance"],
): LocalRpcExecutionAuthority | undefined {
  try {
    if (session.expiresAt === undefined || session.subject === "mcp-client") return undefined;
    const reference = decodeReference(
      {
        version: 1,
        kind: "local-rpc-session",
        provenance,
        environmentId,
        sessionId: session.sessionId,
        subject: session.subject,
        method: session.method,
        observedScopes: [...session.scopes],
        expiresAt: DateTime.formatIso(session.expiresAt),
        runtimeModeCeiling: session.runtimeModeCeiling ?? null,
        peerLink: session.peerLink === true,
        proofKeyThumbprint: session.proofKeyThumbprint ?? null,
      },
      { onExcessProperty: "error" },
    );
    Object.freeze(reference.observedScopes);
    return Object.freeze(reference);
  } catch {
    return undefined;
  }
}

export const makeCapturedWorkflowExecutionAuthority = Effect.fn(
  "WorkflowExecutionAuthority.makeCaptured",
)(function* (
  sql: SqlClient.SqlClient,
  identity: ServerEnvironmentIdentity["Service"],
  policy: ManagedPolicy["Service"],
) {
  const servingId = yield* identity.getEnvironmentId;
  const managedCeiling = policy.summary?.limits?.maxRuntimeMode;
  const sessions = yield* AuthSessions.make.pipe(Effect.provideService(SqlClient.SqlClient, sql));
  const projects = yield* ProjectStore.make.pipe(Effect.provideService(SqlClient.SqlClient, sql));
  const check = Effect.fn("WorkflowExecutionAuthority.checkNativeAgentInTransaction")(
    function* (input: typeof Check.Type) {
      if (Option.isNone(yield* Effect.serviceOption(sql.transactionService))) {
        return yield* new WorkflowExecutionAuthorityError({ reason: "transaction" });
      }
      const reference = input.reference;
      const expires = DateTime.make(reference.expiresAt);
      const now = yield* DateTime.now;
      const required = [AuthOrchestrationReadScope, AuthOrchestrationOperateScope];
      if (
        reference.environmentId !== servingId ||
        input.destination.environmentId !== servingId ||
        reference.subject === "mcp-client" ||
        reference.peerLink ||
        Option.isNone(expires) ||
        expires.value.epochMilliseconds <= now.epochMilliseconds ||
        !required.every((scope) => reference.observedScopes.includes(scope)) ||
        (reference.runtimeModeCeiling !== null &&
          isRuntimeModeAbove(input.runtimeMode, reference.runtimeModeCeiling)) ||
        (managedCeiling !== undefined && isRuntimeModeAbove(input.runtimeMode, managedCeiling))
      )
        return yield* denied();
      const found = yield* sessions.getById({ sessionId: reference.sessionId });
      if (Option.isNone(found)) return yield* denied();
      const session = found.value;
      if (
        session.sessionId !== reference.sessionId ||
        session.subject !== reference.subject ||
        session.method !== reference.method ||
        session.revokedAt !== null ||
        session.expiresAt.epochMilliseconds <= now.epochMilliseconds ||
        session.issuedAt.epochMilliseconds > now.epochMilliseconds ||
        !required.every((scope) => session.scopes.includes(scope))
      )
        return yield* denied();
      for (const resource of [input.destination, ...input.origins]) {
        if (
          resource.environmentId !== servingId ||
          resource.projectId !== input.destination.projectId
        )
          return yield* denied();
        const project = yield* projects.get(resource.projectId);
        if (
          Option.isNone(project) ||
          project.value.deletedAt !== null ||
          project.value.projectId !== resource.projectId
        )
          return yield* denied();
        if (resource.threadId !== undefined) {
          const rows = yield* sql<{
            thread_id: string;
            project_id: string;
            deleted_at: string | null;
            payload_json: string;
          }>`SELECT thread_id, project_id, deleted_at, payload_json FROM orchestration_v2_projection_threads WHERE thread_id = ${resource.threadId}`;
          if (rows.length !== 1 || !rows[0]) return yield* denied();
          const row = rows[0];
          const thread = yield* decodeThread(row.payload_json);
          if (
            row.thread_id !== resource.threadId ||
            row.project_id !== resource.projectId ||
            row.deleted_at !== null ||
            thread.id !== resource.threadId ||
            thread.projectId !== resource.projectId ||
            thread.deletedAt !== null
          )
            return yield* denied();
        }
      }
    },
    Effect.mapError((error) =>
      isAuthorityError(error)
        ? error
        : new WorkflowExecutionAuthorityError({ reason: "persistence" }),
    ),
  );
  return {
    checkNativeAgentInTransaction(input: typeof Check.Type) {
      try {
        WorkflowCanonicalHash.assertCanonical(input);
        const serialized = stableStringify(input);
        const owned = decodeCheck(JSON.parse(serialized), { onExcessProperty: "error" });
        if (
          stableStringify(encodeCheck(owned)) !== serialized ||
          stableStringify(encodeReference(owned.reference)) !== stableStringify(owned.reference)
        )
          return Effect.fail(denied());
        return check(owned);
      } catch {
        return Effect.fail(denied());
      }
    },
  };
});
