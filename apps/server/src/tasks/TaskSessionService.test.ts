import { assert, it } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
  WorkTaskDraftDefinition,
  WorkTaskId,
  CommandId,
  ProjectId,
  WorkTaskCreateDraftInput,
  WorkTaskAcceptDraftInput,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as RpcTest from "effect/rpc/RpcTest";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import { runMigrations } from "../persistence/Migrations.ts";
import * as TaskService from "./TaskService.ts";
import * as TaskStore from "./TaskStore.ts";
import { makeTaskSessionService } from "./TaskSessionService.ts";
const decodeCreate = Schema.decodeUnknownEffect(WorkTaskCreateDraftInput);
const decodeAccept = Schema.decodeUnknownEffect(WorkTaskAcceptDraftInput);
const tested = [
  WS_METHODS.workTasksCreateDraft,
  WS_METHODS.workTasksInspectDraft,
  WS_METHODS.workTasksAcceptDraft,
] as const;
const group = WsRpcGroup.omit(
  ...[...WsRpcGroup.requests.keys()].filter(
    (
      tag,
    ): tag is Exclude<keyof typeof RpcAuthorization.RPC_REQUIRED_SCOPES, (typeof tested)[number]> =>
      !(tested as ReadonlyArray<string>).includes(tag),
  ),
);
const decodeDefinition = Schema.decodeEffect(WorkTaskDraftDefinition);
const services = TaskService.layer.pipe(
  Layer.provideMerge(
    TaskStore.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" }))),
  ),
);
it.effect("rejects token callers before any task mutation", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const session = {
      sessionId: AuthSessionId.make("agent-session"),
      subject: "agent",
      method: "bearer-access-token" as const,
      scopes: [AuthOrchestrationOperateScope, AuthOrchestrationReadScope],
    };
    const tasks = yield* makeTaskSessionService(session);
    const definition = yield* decodeDefinition({
      revision: 1,
      title: "Explain entry point",
      objective: "Read the server entry point",
      criteria: [{ id: "entry", condition: "Entry named", verification: "Inspect response" }],
      actionPolicy: { version: 1, rules: [] },
    });
    assert.isTrue(
      yield* tasks
        .create({
          taskId: WorkTaskId.make("task"),
          commandId: CommandId.make("create"),
          definition,
        })
        .pipe(Effect.isFailure),
    );
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ count: number }>`SELECT COUNT(*) AS count FROM orchestration_events`;
    assert.strictEqual(rows[0]!.count, 0);
  }).pipe(Effect.provide(services)),
);

it.effect("serves native drafts and accepts only the authenticated user's displayed revision", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const session = {
      sessionId: AuthSessionId.make("human-session"),
      subject: "maintainer",
      method: "browser-session-cookie" as const,
      scopes: [AuthOrchestrationOperateScope, AuthOrchestrationReadScope],
    };
    const handlers = yield* makeTaskSessionService(session);
    const client = yield* RpcTest.makeClient(group).pipe(
      Effect.provide(
        Layer.mergeAll(
          group.toLayerHandler(WS_METHODS.workTasksCreateDraft, handlers.create),
          group.toLayerHandler(WS_METHODS.workTasksInspectDraft, handlers.inspect),
          group.toLayerHandler(WS_METHODS.workTasksAcceptDraft, handlers.accept),
          RpcAuthorization.layer(session.scopes),
        ),
      ),
    );
    const create = yield* decodeCreate({
      taskId: "task",
      commandId: "create-command",
      definition: {
        title: "Explain entry point",
        objective: "Read the server entry point",
        criteria: [{ id: "entry", condition: "Entry named", verification: "Inspect response" }],
        actionPolicy: { version: 1, rules: [] },
      },
      attribution: { type: "user", sessionId: "forged", subject: "forged" },
    });
    const created = yield* client[WS_METHODS.workTasksCreateDraft](create);
    assert.deepStrictEqual(yield* client[WS_METHODS.workTasksCreateDraft](create), created);
    const inspected = yield* client[WS_METHODS.workTasksInspectDraft]({ taskId: create.taskId });
    assert.strictEqual(inspected.definition.objective, create.definition.objective);
    assert.isNull(inspected.accepted);
    const duplicateId = yield* client[WS_METHODS.workTasksCreateDraft]({
      ...create,
      commandId: CommandId.make("new-create-command"),
    }).pipe(Effect.flip);
    assert.strictEqual(
      duplicateId._tag === "WorkTaskError" && duplicateId.code,
      "revision_conflict",
    );
    const accept = yield* decodeAccept({
      taskId: create.taskId,
      commandId: "accept-command",
      expectedDefinitionRevision: 1,
      expectedLifecycleRevision: 1,
      expectedActionPolicyRevision: 1,
      caller: { type: "user", sessionId: "forged", subject: "forged" },
    });
    const stale = yield* client[WS_METHODS.workTasksAcceptDraft]({
      ...accept,
      expectedDefinitionRevision: 2,
    }).pipe(Effect.flip);
    assert.strictEqual(stale._tag === "WorkTaskError" && stale.code, "revision_conflict");
    const accepted = yield* client[WS_METHODS.workTasksAcceptDraft](accept);
    assert.deepStrictEqual(yield* client[WS_METHODS.workTasksAcceptDraft](accept), accepted);
    assert.deepStrictEqual(yield* client[WS_METHODS.workTasksCreateDraft](create), created);
    const after = yield* client[WS_METHODS.workTasksInspectDraft]({ taskId: create.taskId });
    assert.deepStrictEqual(after.accepted?.attribution, {
      type: "user",
      sessionId: session.sessionId,
      subject: session.subject,
    });
    assert.strictEqual(after.accepted?.definition.objective, create.definition.objective);
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_events ORDER BY sequence`;
    assert.strictEqual(rows.length, 2);
    assert.isFalse(rows.some((row) => row.payload_json.includes("forged")));
    const conflict = yield* client[WS_METHODS.workTasksCreateDraft]({
      ...create,
      definition: { ...create.definition, objective: "Changed" },
    }).pipe(Effect.flip);
    assert.strictEqual(conflict._tag === "WorkTaskError" && conflict.code, "revision_conflict");
    for (const method of ["bearer-access-token", "dpop-access-token"] as const) {
      const agent = yield* makeTaskSessionService({ ...session, method });
      assert.isTrue(
        yield* agent
          .accept({ ...accept, commandId: CommandId.make("forged-accept") })
          .pipe(Effect.isFailure),
      );
    }
    const missingProject = yield* client[WS_METHODS.workTasksCreateDraft]({
      ...create,
      taskId: WorkTaskId.make("missing-project-task"),
      commandId: CommandId.make("missing-project-create"),
      projectId: ProjectId.make("missing-project"),
    }).pipe(Effect.flip);
    assert.strictEqual(missingProject._tag === "WorkTaskError" && missingProject.code, "not_found");
    const incomplete = {
      ...create,
      taskId: WorkTaskId.make("incomplete-task"),
      commandId: CommandId.make("incomplete-create"),
      definition: { ...create.definition, title: "", objective: "", criteria: [] },
    };
    yield* client[WS_METHODS.workTasksCreateDraft](incomplete);
    const invalid = yield* client[WS_METHODS.workTasksAcceptDraft]({
      ...accept,
      taskId: incomplete.taskId,
      commandId: CommandId.make("incomplete-accept"),
    }).pipe(Effect.flip);
    assert.strictEqual(invalid._tag === "WorkTaskError" && invalid.code, "invalid_definition");
    assert.isNull(
      (yield* client[WS_METHODS.workTasksInspectDraft]({ taskId: incomplete.taskId })).accepted,
    );
    const reader = yield* makeTaskSessionService({
      ...session,
      scopes: [AuthOrchestrationReadScope],
    });
    assert.isTrue(yield* reader.create(create).pipe(Effect.isFailure));
    assert.isTrue(yield* reader.accept(accept).pipe(Effect.isFailure));
    assert.strictEqual((yield* reader.inspect({ taskId: create.taskId })).taskId, create.taskId);
    const denied = yield* makeTaskSessionService({ ...session, scopes: [] });
    assert.isTrue(yield* denied.inspect({ taskId: create.taskId }).pipe(Effect.isFailure));
  }).pipe(Effect.scoped, Effect.provide(services)),
);
