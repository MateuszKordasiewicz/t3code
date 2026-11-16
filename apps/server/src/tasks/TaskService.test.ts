import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { WorkTaskDraftDefinition, WorkTaskCallerContext } from "@t3tools/contracts";
import { runMigrations } from "../persistence/Migrations.ts";
import * as TaskStore from "./TaskStore.ts";
import { makeTaskService } from "./TaskService.ts";
const decodeCaller = Schema.decodeEffect(WorkTaskCallerContext);
const decodeDefinition = Schema.decodeEffect(WorkTaskDraftDefinition);
const db = NodeSqliteClient.layer({ filename: ":memory:" });
const services = TaskStore.layer.pipe(Layer.provideMerge(db));
it.effect("creates distinct native drafts with durable source and no execution authority", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    const service = yield* makeTaskService;
    const definition = yield* decodeDefinition({
      revision: 1,
      title: "Explain this workspace",
      objective: "Describe the entry point",
      criteria: [
        { id: "entry", condition: "Entry point identified", verification: "Inspect response" },
      ],
      actionPolicy: { version: 1, rules: [] },
    });
    const taskId = yield* service.create({ definition });
    const otherId = yield* service.create({ definition });
    assert.notStrictEqual(taskId, otherId);
    const first = yield* service.inspect(taskId);
    assert.isDefined(first);
    assert.isTrue(first!.sourceVerified);
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      sequence: number;
    }>`SELECT sequence FROM orchestration_events WHERE stream_id=${taskId}`;
    assert.strictEqual(rows.length, 1);
    assert.deepStrictEqual(yield* service.inspect(taskId), first);
    assert.deepStrictEqual((yield* service.inspectResults({ taskId }))?.records, []);
    assert.isUndefined(yield* service.inspect("missing"));
    const human = yield* decodeCaller({
      type: "user",
      sessionId: "session",
      subject: "maintainer",
    });
    const acceptance = {
      taskId,
      commandId: "accept-1",
      expectedDefinitionRevision: 1,
      expectedLifecycleRevision: 1,
      expectedActionPolicyRevision: 1,
    };
    for (const caller of [
      { type: "server" } as const,
      { type: "agent", threadId: "agent", providerSessionId: "provider", runId: "run" } as const,
      {
        type: "manager",
        threadId: "manager",
        providerSessionId: "provider",
        runId: "run",
        generation: 1,
      } as const,
    ]) {
      const denied = yield* service
        .acceptDraft(acceptance, yield* decodeCaller(caller))
        .pipe(Effect.flip);
      assert.strictEqual(denied._tag === "WorkTaskError" && denied.code, "unsupported");
    }
    const stale = yield* service
      .acceptDraft({ ...acceptance, expectedDefinitionRevision: 2 }, human)
      .pipe(Effect.flip);
    assert.strictEqual(stale._tag === "WorkTaskError" && stale.code, "revision_conflict");
    for (const changed of [{ expectedLifecycleRevision: 2 }, { expectedActionPolicyRevision: 2 }]) {
      const error = yield* service
        .acceptDraft({ ...acceptance, ...changed }, human)
        .pipe(Effect.flip);
      assert.strictEqual(error._tag === "WorkTaskError" && error.code, "revision_conflict");
    }
    const sequence = yield* service.acceptDraft(acceptance, human);
    assert.strictEqual(yield* service.acceptDraft(acceptance, human), sequence);
    assert.strictEqual((yield* service.inspect(taskId))?.accepted?.attribution.type, "user");
    const conflict = yield* service
      .acceptDraft({ ...acceptance, taskId: otherId }, human)
      .pipe(Effect.flip);
    assert.strictEqual(conflict._tag === "WorkTaskError" && conflict.code, "revision_conflict");
    const duplicate = yield* service
      .acceptDraft({ ...acceptance, commandId: "accept-2" }, human)
      .pipe(Effect.flip);
    assert.strictEqual(duplicate._tag === "WorkTaskError" && duplicate.code, "revision_conflict");
    yield* sql`CREATE TRIGGER reject_acceptance BEFORE INSERT ON work_task_accepted_definitions
      BEGIN SELECT RAISE(ABORT, 'acceptance unavailable'); END`;
    assert.isTrue(
      yield* service
        .acceptDraft({ ...acceptance, taskId: otherId, commandId: "accept-other" }, human)
        .pipe(Effect.isFailure),
    );
    assert.isUndefined((yield* service.inspect(otherId))?.accepted);
    yield* sql`CREATE TRIGGER reject_task_projection BEFORE INSERT ON work_task_projections
      BEGIN SELECT RAISE(ABORT, 'projection unavailable'); END`;
    assert.isTrue(yield* service.create({ definition }).pipe(Effect.isFailure));
    const count = yield* sql<{ count: number }>`SELECT COUNT(*) AS count FROM orchestration_events`;
    assert.strictEqual(count[0]!.count, 3);
  }).pipe(Effect.provide(services)),
);
