// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodePathService from "@effect/platform-node/NodePath";
import { assert, it } from "@effect/vitest";
import { WorkflowOperationV1, WorkflowRunV1 } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ServerConfig } from "../config.ts";
import { runMigrations } from "../persistence/Migrations.ts";
import * as Derivation from "./WorkflowNativeLaunchDerivation.ts";
import * as Store from "./WorkflowRunStore.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
import { WorkflowRunPolicy } from "./WorkflowRunPolicy.ts";

const hash = "a".repeat(64);
const run = Schema.decodeSync(WorkflowRunV1)({
  schemaVersion: 1,
  id: "run",
  spec: {
    ref: { kind: "configuration", id: "spec", sha256: hash },
    plan: {
      planId: "plan",
      revisionId: "revision",
      revisionSchemaVersion: 1,
      revisionHash: hash,
      sourcePacketHash: hash,
    },
  },
  mode: "ordinary",
  origin: { kind: "human", id: "human" },
  status: "active",
  controlGeneration: 0,
  recordVersion: 0,
  allowance: {
    maxAttempts: 2,
    maxRepairRounds: 1,
    maxActiveWriters: 1,
    attemptsConsumed: 0,
    repairRoundsConsumed: 0,
    deadline: "2026-12-01T00:00:00Z",
  },
  amendmentSequence: 0,
  createdAt: "2026-11-01T00:00:00Z",
  access: { origins: [] },
});
type Input = Parameters<typeof WorkflowRunPolicy.decide>[0];
const facts: Input["facts"] = {
  now: "2026-11-12T00:00:00Z",
  authority: true,
  custody: "none",
  activeWriterCount: 0,
  heldOperations: [],
  capabilitiesCurrent: true,
  compatibilityCurrent: true,
  fingerprintCurrent: true,
  originGrantCurrent: true,
};
const command: Input["command"] = {
  kind: "pause",
  commandId: "command",
  fingerprint: hash,
  expectedVersion: 0,
  expectedGeneration: 0,
  expectedSpecHash: hash,
};
const json = (value: unknown) => JSON.stringify(value);
const seedPlan = (level: "identity" | "plan" | "complete" = "complete") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
    if (level === "identity") {
      yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${hash}, ${hash})`;
      return;
    }
    yield* sql`INSERT INTO development_plans VALUES ('plan', 'environment', 'project', 'Plan', NULL, 0, '2026-11-01T00:00:00Z')`;
    if (level === "plan") return;
    yield* sql`INSERT INTO development_plan_source_packets VALUES ('plan', ${hash}, '{}')`;
    yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${hash}, ${hash})`;
    yield* sql`INSERT INTO development_plan_revisions VALUES ('plan', 'revision', ${hash}, ${hash}, '{}', '2026-11-01T00:00:00Z')`;
    yield* sql`UPDATE development_plans SET current_revision_id = 'revision' WHERE plan_id = 'plan'`;
  });
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* runMigrations();
  yield* seedPlan();
  yield* sql`INSERT INTO workflow_runs VALUES ('run', 'plan', 'revision', ${hash}, ${hash}, ${hash}, 'active', 0, 0, ${json(run)})`;
});

const decodeOperation = Schema.decodeSync(WorkflowOperationV1);
const destination = { environmentId: "environment", projectId: "project" };
const operationFor = (id: string, attemptId: string) => {
  const value = decodeOperation({
    schemaVersion: 1,
    id,
    owner: { kind: "workflow_run", workflowRunId: run.id },
    kind: "stage_launch",
    parameters: {
      kind: "stage_launch",
      stage: "implementation",
      context: {
        specHash: hash,
        policyHash: hash,
        itemId: "item",
        attemptId,
        candidateHash: hash,
        configurationHash: hash,
      },
      provider: { providerInstanceId: "provider", model: "model" },
      nativeThreadId: "thread",
      nativeRunId: id,
    },
    fingerprint: hash,
    origin: run.origin,
    destination,
    controlGeneration: 0,
    status: "pending",
    access: { origins: [] },
  });
  return { ...value, fingerprint: WorkflowCanonicalHash.operation(value) };
};
const admission = (
  operation: WorkflowOperationV1,
  attemptId: string,
  repair = false,
): Extract<Input["command"], { kind: "admitAttempt" }> => ({
  ...command,
  kind: "admitAttempt",
  commandId: attemptId,
  operationId: operation.id,
  operationFingerprint: operation.fingerprint,
  attemptId,
  itemId: "item",
  repair,
});
const admissionFacts = { ...facts, orderedItemIds: ["item"] };
const setupAdmission = Effect.gen(function* () {
  yield* setup;
  const sql = yield* SqlClient.SqlClient;
  const updated = { ...run, access: { origins: [{ ...destination, threadId: "thread" }] } };
  yield* sql`UPDATE workflow_runs SET record_json = ${json(updated)} WHERE id = ${run.id}`;
});

const legacyReservation = (operation: WorkflowOperationV1, attemptId: string) => {
  if (operation.parameters.kind !== "stage_launch") throw new Error("expected stage launch");
  const {
    fingerprint: _fingerprint,
    controlGeneration: _generation,
    status: _status,
    ...rest
  } = operation;
  const { nativeRunId: _nativeRunId, ...parameters } = operation.parameters;
  const {
    fingerprint: _commandHash,
    operationFingerprint: _operationHash,
    ...command
  } = admission(operation, attemptId);
  return {
    intentId: `intent-${attemptId}`,
    runId: run.id,
    operationId: operation.id,
    attemptId,
    itemId: "item",
    repair: false,
    environmentId: destination.environmentId,
    projectId: destination.projectId,
    launchCommandId: `launch-${attemptId}`,
    threadId: parameters.nativeThreadId,
    messageId: `message-${attemptId}`,
    receiptId: `receipt-${attemptId}`,
    generation: 0,
    draft: {
      version: 1 as const,
      kind: "stage_launch" as const,
      operation: { ...rest, parameters },
      admission: { command },
    },
    externalFacts: admissionFacts,
  };
};

const reservation = (operation: WorkflowOperationV1, attemptId: string) => {
  const input = legacyReservation(operation, attemptId);
  return {
    ...input,
    materialVersion: 1 as const,
    launch: {
      commandId: input.launchCommandId,
      threadId: input.threadId,
      projectId: input.projectId,
      title: "Work",
      modelSelection: { instanceId: "provider", model: "model" },
      runtimeMode: "full-access",
      interactionMode: "default",
      workspaceStrategy: { type: "root", branch: "main" },
      initialMessage: { messageId: input.messageId, text: "Do the work", attachments: [] },
      createdBy: "agent",
      creationSource: "mcp",
    },
  };
};

const testLayer = (filename = ":memory:") => {
  const dependencies = Layer.mergeAll(
    NodeSqliteClient.layer({ filename }),
    NodePathService.layerPosix,
    Layer.succeed(ServerConfig, { baseDir: "/config" } as ServerConfig["Service"]),
  );
  const withStore = Store.layer.pipe(Layer.provideMerge(dependencies));
  return Derivation.layer.pipe(Layer.provideMerge(withStore));
};
const seedProject = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at) VALUES ('project', 'Project', '/old', '[]', '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z')`;
});
const basicInput = () => reservation(operationFor("operation", "attempt"), "attempt");

it.effect(
  "derives frozen commands and detached opaque context inside the captured transaction",
  () =>
    Effect.gen(function* () {
      yield* setupAdmission;
      yield* seedProject;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
      const input = basicInput();
      const launch = {
        ...input.launch,
        generateTitle: true,
        initialMessage: {
          ...input.launch.initialMessage,
          context: {
            version: 1,
            records: [
              {
                version: 1,
                contextId: "opaque",
                kind: "future-kind",
                label: "Future",
                payload: { nested: ["original"] },
              },
            ],
          },
        },
      };
      assert.strictEqual(
        (yield* store.reserveWorkflowNativeIntent({ ...input, launch })).kind,
        "reserved",
      );
      assert.strictEqual(
        (yield* Effect.flip(derive.deriveInTransaction({ intentId: input.intentId }))).reason,
        "integrity",
      );
      const read = () =>
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId }));
      const first = yield* read();
      assert.deepStrictEqual<unknown>(first.normalization, {
        version: 1,
        project: { projectId: "project", workspaceRoot: "/old" },
        scratchRoot: "/config/scratch",
        workspace: {
          requested: launch.workspaceStrategy,
          effective: launch.workspaceStrategy,
          reason: "unchanged",
        },
        reuse: { requested: false, claimKind: "create" },
        attachments: { kind: "none" },
      });
      assert.strictEqual(first.claimCommand.commandId, launch.commandId);
      assert.strictEqual(first.messageCommand.commandId, `${launch.commandId}:initial-message`);
      assert.strictEqual(first.messageCommand.messageId, launch.initialMessage.messageId);
      assert.deepStrictEqual<unknown>(first.messageCommand.context, launch.initialMessage.context);
      assert.strictEqual(first.messageCommand.titleSeed, launch.title);
      assert.strictEqual(
        first.normalizationHash,
        WorkflowCanonicalHash.tagged("t3.workflow-native-normalization.v1", first.normalization),
      );
      assert.strictEqual(
        first.expectedClaimCommandHash,
        WorkflowCanonicalHash.tagged("t3.workflow-native-command.v1", first.claimCommand),
      );
      assert.strictEqual(
        first.expectedMessageCommandHash,
        WorkflowCanonicalHash.tagged("t3.workflow-native-command.v1", first.messageCommand),
      );
      const records = first.reservation.material.launch.initialMessage.context!.records;
      (records[0] as { payload: { nested: string[] } }).payload.nested[0] = "mutated";
      assert.deepStrictEqual<unknown>(
        (yield* read()).messageCommand.context,
        launch.initialMessage.context,
      );
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each([
  ["missing", null],
  ["deleted", "/deleted"],
  ["empty", ""],
  ["relative", "relative"],
  ["scratch", "/config/other/../scratch"],
] as const)("refuses %s project", ([scenario, root]) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
    const input = basicInput();
    yield* store.reserveWorkflowNativeIntent(input);
    if (root !== null) {
      yield* seedProject;
      yield* sql`UPDATE projection_projects SET workspace_root = ${root}, deleted_at = ${scenario === "deleted" ? "2026-11-02T00:00:00Z" : null}`;
    }
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })),
      )).reason,
      "integrity",
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["attachment", "image", "file"] as const)("refuses %s inputs", (scenario) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    yield* seedProject;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
    const input = basicInput();
    const attachment = {
      type: "file",
      id: "thread-file",
      name: "a.txt",
      mimeType: "text/plain",
      sizeBytes: 1,
    };
    const record = {
      version: 1,
      contextId: "ref",
      kind: scenario,
      label: "ref",
      attachmentId: "thread-file",
      name: "a.txt",
      mimeType: scenario === "image" ? "image/png" : "text/plain",
      sizeBytes: 1,
    };
    const launch = {
      ...input.launch,
      initialMessage: {
        ...input.launch.initialMessage,
        attachments: scenario === "attachment" ? [attachment] : [],
        ...(scenario === "attachment" ? {} : { context: { version: 1, records: [record] } }),
      },
    };
    assert.strictEqual(
      (yield* store.reserveWorkflowNativeIntent({ ...input, launch })).kind,
      "reserved",
    );
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })),
      )).reason,
      "integrity",
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "uses writer-local project state while an independent reader sees committed state, then rolls back",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-derivation-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        yield* Effect.gen(function* () {
          yield* setupAdmission;
          yield* seedProject;
          const sql = yield* SqlClient.SqlClient;
          const store = yield* Store.WorkflowRunStore;
          const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
          const input = basicInput();
          yield* store.reserveWorkflowNativeIntent(input);
          const reader = yield* Effect.acquireRelease(
            Effect.sync(() => new NodeSqlite.DatabaseSync(filename, { readOnly: true })),
            (db) => Effect.sync(() => db.close()),
          );
          const externalRoot = () =>
            (
              reader
                .prepare(
                  "SELECT workspace_root FROM projection_projects WHERE project_id = 'project'",
                )
                .get() as { workspace_root: string }
            ).workspace_root;
          assert.strictEqual(externalRoot(), "/old");
          const before = yield* sql.withTransaction(
            derive.deriveInTransaction({ intentId: input.intentId }),
          );
          const result = yield* Effect.result(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* sql`UPDATE projection_projects SET workspace_root = '/writer' WHERE project_id = 'project'`;
                const writerResult = yield* derive.deriveInTransaction({
                  intentId: input.intentId,
                });
                assert.strictEqual(writerResult.normalization.project.workspaceRoot, "/writer");
                assert.notStrictEqual(writerResult.normalizationHash, before.normalizationHash);
                assert.strictEqual(externalRoot(), "/old");
                return yield* Effect.fail("rollback");
              }),
            ),
          );
          assert.strictEqual(result._tag, "Failure");
          assert.strictEqual(externalRoot(), "/old");
          assert.strictEqual(
            (yield* sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })))
              .normalization.project.workspaceRoot,
            "/old",
          );
        }).pipe(Effect.provide(testLayer(filename)));
      }),
    ),
);

it.effect.each([
  { type: "root", branch: "feature" },
  { type: "worktree", baseRef: "origin/main", branch: "feature", startFromOrigin: true },
  { type: "existing_worktree", worktreePath: "/somewhere", branch: "feature" },
] as const)("preserves requested $type strategy and reuse", (strategy) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    yield* seedProject;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
    const input = basicInput();
    const launch = { ...input.launch, workspaceStrategy: strategy, reuseExistingThread: true };
    yield* store.reserveWorkflowNativeIntent({ ...input, launch });
    const result = yield* sql.withTransaction(
      derive.deriveInTransaction({ intentId: input.intentId }),
    );
    assert.deepStrictEqual(result.normalization.workspace, {
      requested: strategy,
      effective: strategy,
      reason: "unchanged",
    });
    assert.deepStrictEqual(result.normalization.reuse, {
      requested: true,
      claimKind: "metadata-update",
    });
    assert.strictEqual(result.claimCommand.type, "thread.metadata.update");
    assert.deepStrictEqual(result.messageCommand.dispatchMode, {
      type: "defer_start",
      workspaceStrategy: strategy,
    });
  }).pipe(Effect.provide(testLayer())),
);

it.effect("classifies malformed project data and SQL failures separately", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    yield* seedProject;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
    const input = basicInput();
    yield* store.reserveWorkflowNativeIntent(input);
    yield* sql`UPDATE projection_projects SET scripts_json = 'not-json'`;
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })),
      )).reason,
      "integrity",
    );
    yield* sql`DROP TABLE projection_projects`;
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })),
      )).reason,
      "persistence",
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "rejects a foreign client transaction and independently checks the store's captured client",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const derivation = yield* Derivation.WorkflowNativeLaunchDerivation;
      yield* Effect.gen(function* () {
        const foreignSql = yield* SqlClient.SqlClient;
        const foreignStore = yield* Store.WorkflowRunStore;
        assert.notStrictEqual(foreignStore, store);
        assert.strictEqual(
          (yield* Effect.flip(
            foreignSql.withTransaction(derivation.deriveInTransaction({ intentId: "missing" })),
          )).reason,
          "integrity",
        );
        assert.strictEqual(
          (yield* Effect.flip(
            foreignSql.withTransaction(
              store.readNativeReservationInTransaction({ intentId: "missing" }),
            ),
          )).reason,
          "integrity",
        );
        const mismatched = yield* Derivation.WorkflowNativeLaunchDerivation.pipe(
          Effect.provide(Layer.fresh(Derivation.layer)),
          Effect.provideService(Store.WorkflowRunStore, foreignStore),
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        assert.strictEqual(
          (yield* Effect.flip(
            sql.withTransaction(mismatched.deriveInTransaction({ intentId: "missing" })),
          )).reason,
          "integrity",
        );
      }).pipe(
        Effect.provide(
          Layer.fresh(Store.layer).pipe(
            Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
          ),
        ),
      );
    }).pipe(Effect.provide(testLayer())),
);
