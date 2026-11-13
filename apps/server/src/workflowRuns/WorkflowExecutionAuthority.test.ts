import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentId,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as ServerConfig from "../config.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as Authority from "./WorkflowExecutionAuthority.ts";

const environmentId = EnvironmentId.make("authority-environment");
const projectId = ProjectId.make("authority-project");
const threadId = ThreadId.make("authority-thread");
const scopes = [AuthOrchestrationReadScope, AuthOrchestrationOperateScope];
const identity = { getEnvironmentId: Effect.succeed(environmentId) };
const layerTest = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) => {
  const platform = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
  const base = Layer.mergeAll(
    database,
    ServerConfig.layerTest(process.cwd(), { prefix: "t3-workflow-authority-" }),
    Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, identity),
    ManagedPolicy.layerTest(),
  ).pipe(Layer.provideMerge(platform));
  const secret = ServerSecretStore.layer.pipe(Layer.provideMerge(base));
  return SessionStore.layer.pipe(Layer.provideMerge(secret));
};
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const now = DateTime.formatIso(yield* DateTime.now);
  yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES (${projectId},'Authority project','/authority','[]',${now},${now})`;
  const thread = {
    id: threadId,
    projectId,
    title: "Authority thread",
    providerInstanceId: "provider",
    modelSelection: { instanceId: "provider", model: "model" },
    createdBy: "user",
    creationSource: "web",
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  yield* sql`INSERT INTO orchestration_v2_projection_threads (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES (${threadId},${projectId},'Authority thread','provider','full-access','default',${now},${now},${JSON.stringify(thread)})`;
});
const reference = Effect.fn("test.localAuthority")(function* (
  input: Parameters<SessionStore.SessionStore["Service"]["issue"]>[0] = {},
) {
  const sessions = yield* SessionStore.SessionStore;
  const issued = yield* sessions.issue({
    subject: "authority-user",
    method: "bearer-access-token",
    scopes,
    ...input,
  });
  const verified = yield* sessions.verify(issued.token);
  const captured = Authority.captureLocalRpcExecutionAuthority(
    verified,
    environmentId,
    "verified-session",
  );
  if (captured === undefined) throw new Error("expected local reference");
  return captured;
});
const reader = Effect.gen(function* () {
  return yield* Authority.makeCapturedWorkflowExecutionAuthority(
    yield* SqlClient.SqlClient,
    identity,
    yield* ManagedPolicy.ManagedPolicy,
  );
});
const inputFor = (reference: Authority.LocalRpcExecutionAuthority) => ({
  reference,
  destination: { environmentId, projectId },
  origins: [{ environmentId, projectId, threadId }],
  runtimeMode: "full-access" as const,
});

it.effect("checks real local authority only inside its captured transaction and owns inputs", () =>
  Effect.gen(function* () {
    yield* seed;
    const ref = yield* reference();
    const auth = yield* reader;
    const sql = yield* SqlClient.SqlClient;
    const input = inputFor(ref);
    assert.strictEqual(
      (yield* Effect.flip(auth.checkNativeAgentInTransaction(input))).reason,
      "transaction",
    );
    yield* sql.withTransaction(auth.checkNativeAgentInTransaction(input));
    const mutable = {
      ...input,
      reference: { ...ref, observedScopes: [...ref.observedScopes] },
      destination: { ...input.destination },
      origins: input.origins.map((origin) => ({ ...origin })),
    };
    const queued = auth.checkNativeAgentInTransaction(mutable);
    mutable.reference.observedScopes.length = 0;
    Object.assign(mutable.destination, { projectId: "changed-project" });
    Object.assign(mutable.origins[0]!, { threadId: "changed-thread" });
    yield* sql.withTransaction(queued);
    assert.isTrue(Object.isFrozen(ref));
    assert.isTrue(Object.isFrozen(ref.observedScopes));
    assert.notProperty(ref, "token");
  }).pipe(Effect.provide(layerTest(SqlitePersistence.layerMemory))),
);

it.effect(
  "rejects stale identity, scopes, expiries, constraints and local resource mismatches",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const ref = yield* reference();
      const auth = yield* reader;
      const sql = yield* SqlClient.SqlClient;
      const check = (input = inputFor(ref)) =>
        sql.withTransaction(auth.checkNativeAgentInTransaction(input));
      for (const override of [
        { sessionId: AuthSessionId.make("missing") },
        { subject: "different-user" },
        { method: "browser-session-cookie" as const },
        { observedScopes: [AuthOrchestrationReadScope] },
        { environmentId: EnvironmentId.make("foreign-environment") },
        { expiresAt: DateTime.formatIso(yield* DateTime.now) },
        { expiresAt: "malformed" },
        { runtimeModeCeiling: "approval-required" as const },
        { peerLink: true },
        { subject: "mcp-client" },
      ])
        assert.strictEqual(
          (yield* Effect.flip(check({ ...inputFor(ref), reference: { ...ref, ...override } })))
            .reason,
          "unauthorized",
        );
      for (const changed of [
        { destination: { environmentId: EnvironmentId.make("foreign"), projectId } },
        { origins: [{ environmentId, projectId: ProjectId.make("foreign-project"), threadId }] },
        { origins: [{ environmentId, projectId, threadId: ThreadId.make("missing-thread") }] },
      ])
        assert.strictEqual(
          (yield* Effect.flip(check({ ...inputFor(ref), ...changed }))).reason,
          "unauthorized",
        );
      const [original] =
        yield* sql`SELECT * FROM auth_sessions WHERE session_id = ${ref.sessionId}`;
      if (!original) throw new Error("session missing");
      for (const update of [
        { scopes: JSON.stringify([AuthOrchestrationReadScope]) },
        { subject: "changed" },
        { method: "browser-session-cookie" },
        { expires_at: DateTime.formatIso(yield* DateTime.now) },
      ]) {
        yield* sql`UPDATE auth_sessions SET ${sql.update(update)} WHERE session_id = ${ref.sessionId}`;
        assert.strictEqual((yield* Effect.flip(check())).reason, "unauthorized");
        yield* sql`UPDATE auth_sessions SET subject = ${original.subject}, method = ${original.method}, scopes = ${original.scopes}, expires_at = ${original.expires_at} WHERE session_id = ${ref.sessionId}`;
      }
      yield* sql`UPDATE auth_sessions SET scopes = 'malformed' WHERE session_id = ${ref.sessionId}`;
      assert.strictEqual((yield* Effect.flip(check())).reason, "persistence");
      yield* sql`UPDATE auth_sessions SET scopes = ${original.scopes} WHERE session_id = ${ref.sessionId}`;
      yield* sql`UPDATE projection_projects SET deleted_at = ${DateTime.formatIso(yield* DateTime.now)} WHERE project_id = ${projectId}`;
      assert.strictEqual((yield* Effect.flip(check())).reason, "unauthorized");
      yield* sql`UPDATE projection_projects SET deleted_at = NULL WHERE project_id = ${projectId}`;
      yield* sql`UPDATE orchestration_v2_projection_threads SET project_id = 'foreign-project' WHERE thread_id = ${threadId}`;
      assert.strictEqual((yield* Effect.flip(check())).reason, "unauthorized");
      yield* sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectId}, payload_json = '{}' WHERE thread_id = ${threadId}`;
      assert.strictEqual((yield* Effect.flip(check())).reason, "persistence");
    }).pipe(Effect.provide(layerTest(SqlitePersistence.layerMemory))),
);

it.effect("honors actual revocation, revoke-all, replacement and original credential expiry", () =>
  Effect.gen(function* () {
    yield* seed;
    const sql = yield* SqlClient.SqlClient;
    const sessions = yield* SessionStore.SessionStore;
    const auth = yield* reader;
    const check = (ref: Authority.LocalRpcExecutionAuthority) =>
      sql.withTransaction(auth.checkNativeAgentInTransaction(inputFor(ref)));
    const old = yield* reference();
    yield* sessions.revoke(old.sessionId);
    assert.strictEqual((yield* Effect.flip(check(old))).reason, "unauthorized");
    const replaced = yield* reference({ subject: "replacement" });
    const replacement = yield* reference({
      subject: "replacement",
      replaceActiveForSubjectAndMethod: true,
      replaceSessionId: replaced.sessionId,
    });
    assert.notStrictEqual(replaced.sessionId, replacement.sessionId);
    assert.strictEqual((yield* Effect.flip(check(replaced))).reason, "unauthorized");
    yield* check(replacement);
    const keep = yield* reference({ subject: "keep" });
    yield* sessions.revokeAllExcept(keep.sessionId);
    assert.strictEqual((yield* Effect.flip(check(replacement))).reason, "unauthorized");
    yield* check(keep);
    const short = yield* reference({ subject: "short", ttl: Duration.seconds(1) });
    yield* sql`UPDATE auth_sessions SET expires_at = '2099-01-01T00:00:00.000Z' WHERE session_id = ${short.sessionId}`;
    yield* TestClock.adjust(Duration.seconds(1));
    assert.strictEqual((yield* Effect.flip(check(short))).reason, "unauthorized");
    const narrow = yield* reference({ subject: "narrow", scopes: [AuthOrchestrationReadScope] });
    yield* sql`UPDATE auth_sessions SET scopes = ${JSON.stringify(scopes)} WHERE session_id = ${narrow.sessionId}`;
    assert.strictEqual((yield* Effect.flip(check(narrow))).reason, "unauthorized");
  }).pipe(Effect.provide(layerTest(SqlitePersistence.layerMemory))),
);

it.effect("rechecks the startup managed ceiling inside the authority snapshot", () =>
  Effect.gen(function* () {
    yield* seed;
    const ref = yield* reference();
    const policy = yield* ManagedPolicy.fromDocument("/test/policy.json", {
      schemaVersion: 1,
      organization: "Test",
      limits: { maxRuntimeMode: "approval-required" },
    });
    const auth = yield* Authority.makeCapturedWorkflowExecutionAuthority(
      yield* SqlClient.SqlClient,
      identity,
      policy,
    );
    const sql = yield* SqlClient.SqlClient;
    assert.strictEqual(
      (yield* Effect.flip(sql.withTransaction(auth.checkNativeAgentInTransaction(inputFor(ref)))))
        .reason,
      "unauthorized",
    );
    yield* sql.withTransaction(
      auth.checkNativeAgentInTransaction({ ...inputFor(ref), runtimeMode: "approval-required" }),
    );
  }).pipe(Effect.provide(layerTest(SqlitePersistence.layerMemory))),
);

it.effect(
  "uses the captured writer and observes both commit orders of another client's revocation",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "workflow-authority-order-" });
      const filename = path.join(directory, "state.sqlite");
      const context = yield* Layer.build(
        Layer.fresh(layerTest(SqlitePersistence.layerFromPath(filename))),
      );
      yield* Effect.gen(function* () {
        yield* seed;
        const sqlA = yield* SqlClient.SqlClient;
        const sqlB = Context.get(
          yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
          SqlClient.SqlClient,
        );
        const auth = yield* reader;
        const ref = yield* reference();
        const remoteRepository = yield* AuthSessions.make.pipe(
          Effect.provideService(SqlClient.SqlClient, sqlB),
        );
        const check = auth.checkNativeAgentInTransaction(inputFor(ref));
        assert.strictEqual((yield* Effect.flip(sqlB.withTransaction(check))).reason, "transaction");
        yield* sqlA.withTransaction(check.pipe(Effect.provideService(SqlClient.SqlClient, sqlB)));
        yield* sqlB.withTransaction(
          remoteRepository.revoke({ sessionId: ref.sessionId, revokedAt: yield* DateTime.now }),
        );
        assert.strictEqual(
          (yield* Effect.flip(
            sqlA.withTransaction(
              Effect.gen(function* () {
                yield* sqlA`UPDATE projection_projects SET title = 'must rollback' WHERE project_id = ${projectId}`;
                yield* check;
              }),
            ),
          )).reason,
          "unauthorized",
        );
        assert.deepStrictEqual(
          yield* sqlA`SELECT title FROM projection_projects WHERE project_id = ${projectId}`,
          [{ title: "Authority project" }],
        );
      }).pipe(Effect.provide(context));
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
