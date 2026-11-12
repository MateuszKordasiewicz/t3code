// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import { WorkflowOperationV1, WorkflowRunV1 } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/081_WorkflowRuns.ts";
import planMigration from "../persistence/Migrations/082_DevelopmentPlans.ts";
import intentMigration from "../persistence/Migrations/084_WorkflowNativeLaunchIntents.ts";
import * as Store from "./WorkflowRunStore.ts";
import * as Recovery from "./WorkflowRunRecovery.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
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

const json = (value: unknown) => JSON.stringify(value);
const makeLayer = (filename: string, port: Recovery.WorkflowRunObservation["Service"]) => {
  const store = Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
  return Recovery.layer.pipe(
    Layer.provideMerge(Layer.merge(store, Layer.succeed(Recovery.WorkflowRunObservation, port))),
  );
};
const withDatabase = <A, E, R>(f: (filename: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "workflow-recovery-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      return yield* f(NodePath.join(directory, "test.sqlite"));
    }),
  );
const seedPlan = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
  yield* sql`INSERT INTO development_plans VALUES ('plan', 'environment', 'project', 'Plan', NULL, 0, '2026-11-01T00:00:00Z')`;
  yield* sql`INSERT INTO development_plan_source_packets VALUES ('plan', ${hash}, '{}')`;
  yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${hash}, ${hash})`;
  yield* sql`INSERT INTO development_plan_revisions VALUES ('plan', 'revision', ${hash}, ${hash}, '{}', '2026-11-01T00:00:00Z')`;
  yield* sql`UPDATE development_plans SET current_revision_id = 'revision' WHERE plan_id = 'plan'`;
});
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* migration;
  yield* planMigration;
  yield* intentMigration;
  yield* seedPlan;
  const store = yield* Store.WorkflowRunStore;
  yield* store.createRun(run);
  const op = operationFor("op", "attempt");
  const claimed = {
    ...op,
    status: "claimed",
    claim: {
      bootId: "old-boot",
      workerId: "worker",
      generation: 0,
      claimedAt: "2026-11-01T00:00:00Z",
    },
  };
  yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, record_json) VALUES (${op.id}, ${run.id}, ${op.kind}, ${op.fingerprint}, 'claimed', 0, ${json(claimed)})`;
});
const unknownPort: Recovery.WorkflowRunObservation["Service"] = {
  observe: () => Effect.succeed({ kind: "unknown" }),
};
const matchingPort = (kind: "active" | "settled"): Recovery.WorkflowRunObservation["Service"] => ({
  observe: (input) =>
    Effect.succeed(
      kind === "active"
        ? { kind, owner: input.owner, fingerprint: input.fingerprint, native: input.native }
        : {
            kind,
            owner: input.owner,
            fingerprint: input.fingerprint,
            native: input.native,
            outcome: { kind: "succeeded", summary: "Done", evidence: [] },
          },
    ),
});

it.effect.each(["unknown", "missing", "foreign", "old"] as const)(
  "retains %s custody across restart",
  (scenario) =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const port: Recovery.WorkflowRunObservation["Service"] =
          scenario === "missing"
            ? { observe: () => Effect.succeed({ kind: "missing" }) }
            : scenario === "foreign"
              ? {
                  observe: (input) =>
                    Effect.succeed({
                      kind: "settled",
                      owner: input.owner,
                      fingerprint: input.fingerprint,
                      native: { threadId: "foreign", runId: "op" },
                      outcome: { kind: "succeeded", summary: "Done", evidence: [] },
                    }),
                }
              : scenario === "old"
                ? matchingPort("active")
                : unknownPort;
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seed;
            const recovery = yield* Recovery.WorkflowRunRecovery;
            assert.strictEqual(
              (yield* recovery.reconcile({ operationId: "op" })).kind,
              scenario === "old" ? "active" : "unknown",
            );
          }).pipe(Effect.provide(makeLayer(filename, port))),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const recovery = yield* Recovery.WorkflowRunRecovery;
            const store = yield* Store.WorkflowRunStore;
            const snap = yield* store.readRecovery("op");
            assert.strictEqual(snap?.operation.status, scenario === "old" ? "claimed" : "unknown");
            assert.strictEqual(snap?.operation.claim?.bootId, "old-boot");
            assert.isDefined(snap?.owner);
            if (snap && "allowance" in snap.owner)
              assert.deepStrictEqual(snap.owner.allowance, run.allowance);
            assert.deepStrictEqual(yield* store.listPending(), []);
            assert.strictEqual(
              (yield* recovery.reconcile({ operationId: "op" })).kind,
              scenario === "old" ? "active" : "unchanged",
            );
          }).pipe(Effect.provide(makeLayer(filename, port))),
        );
      }),
    ),
);

it.effect("settles once after a lost acknowledgement and leaves active claims intact", () =>
  withDatabase((filename) =>
    Effect.gen(function* () {
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* seed;
          const recovery = yield* Recovery.WorkflowRunRecovery;
          assert.strictEqual((yield* recovery.reconcile({ operationId: "op" })).kind, "active");
        }).pipe(Effect.provide(makeLayer(filename, matchingPort("active")))),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const recovery = yield* Recovery.WorkflowRunRecovery;
          const store = yield* Store.WorkflowRunStore;
          assert.strictEqual((yield* recovery.reconcile({ operationId: "op" })).kind, "settled");
          assert.strictEqual((yield* recovery.reconcile({ operationId: "op" })).kind, "unchanged");
          assert.strictEqual(
            (yield* store.readRecovery("op"))?.operation.outcome?.kind,
            "succeeded",
          );
        }).pipe(Effect.provide(makeLayer(filename, matchingPort("settled")))),
      );
    }),
  ),
);

it.effect("rejects a stale observation after a pause", () =>
  withDatabase((filename) =>
    Effect.gen(function* () {
      let pause: Effect.Effect<void> = Effect.void;
      const port: Recovery.WorkflowRunObservation["Service"] = {
        observe: (input) =>
          pause.pipe(
            Effect.as({
              kind: "settled" as const,
              owner: input.owner,
              fingerprint: input.fingerprint,
              native: input.native,
              outcome: { kind: "succeeded" as const, summary: "Done", evidence: [] },
            }),
          ),
      };
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* seed;
          const recovery = yield* Recovery.WorkflowRunRecovery;
          const store = yield* Store.WorkflowRunStore;
          pause = store
            .commitTransition({
              runId: "run",
              command: {
                kind: "pause",
                commandId: "pause",
                fingerprint: hash,
                expectedVersion: 0,
                expectedGeneration: 0,
                expectedSpecHash: hash,
              },
              externalFacts: {
                now: "2026-11-12T00:00:00Z",
                authority: true,
                custody: "none",
                activeWriterCount: 0,
                heldOperations: [],
                capabilitiesCurrent: true,
                compatibilityCurrent: true,
                fingerprintCurrent: true,
                originGrantCurrent: true,
              },
              receiptId: "pause",
            })
            .pipe(
              Effect.tap((result) =>
                Effect.sync(() => assert.strictEqual(result.kind, "accepted")),
              ),
              Effect.asVoid,
              Effect.orDie,
            );
          assert.strictEqual((yield* recovery.reconcile({ operationId: "op" })).kind, "stale");
          assert.strictEqual((yield* store.readRecovery("op"))?.operation.status, "claimed");
        }).pipe(Effect.provide(makeLayer(filename, port))),
      );
    }),
  ),
);

it.effect("settles while stopped without reopening", () =>
  withDatabase((filename) =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* seed;
        const sql = yield* SqlClient.SqlClient;
        yield* sql`UPDATE workflow_runs SET status = 'stopped', record_version = 1, control_generation = 1, record_json = json_set(record_json, '$.status', 'stopped', '$.recordVersion', 1, '$.controlGeneration', 1) WHERE id = 'run'`;
        const recovery = yield* Recovery.WorkflowRunRecovery;
        const store = yield* Store.WorkflowRunStore;
        assert.strictEqual((yield* recovery.reconcile({ operationId: "op" })).kind, "settled");
        const snap = yield* store.readRecovery("op");
        assert.strictEqual(snap?.owner.status, "stopped");
        assert.strictEqual(snap?.operation.status, "settled");
      }).pipe(Effect.provide(makeLayer(filename, matchingPort("settled")))),
    ),
  ),
);

it.effect("settles a matching result after unknown custody without reopening the run", () =>
  withDatabase((filename) =>
    Effect.gen(function* () {
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* seed;
          const recovery = yield* Recovery.WorkflowRunRecovery;
          assert.strictEqual((yield* recovery.reconcile({ operationId: "op" })).kind, "unknown");
        }).pipe(Effect.provide(makeLayer(filename, unknownPort))),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const recovery = yield* Recovery.WorkflowRunRecovery;
          const store = yield* Store.WorkflowRunStore;
          const sql = yield* SqlClient.SqlClient;
          assert.strictEqual((yield* recovery.reconcile({ operationId: "op" })).kind, "settled");
          const snapshot = yield* store.readRecovery("op");
          assert.strictEqual(snapshot?.operation.status, "settled");
          assert.strictEqual(snapshot?.operation.outcome?.kind, "succeeded");
          assert.deepStrictEqual(yield* sql`SELECT status FROM workflow_runs`, [
            { status: "needs_attention" },
          ]);
        }).pipe(Effect.provide(makeLayer(filename, matchingPort("settled")))),
      );
    }),
  ),
);
