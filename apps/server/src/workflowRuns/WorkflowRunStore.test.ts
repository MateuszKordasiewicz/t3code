import { assert, it } from "@effect/vitest";
import { WorkflowRunV1 } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/081_WorkflowRuns.ts";
import * as Store from "./WorkflowRunStore.ts";
import { WorkflowRunPolicy } from "./WorkflowRunPolicy.ts";

const hash = "a".repeat(64);
const nextHash = "b".repeat(64);
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
const layer = Store.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* migration;
  yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
  yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${hash}, ${hash})`;
  yield* sql`INSERT INTO workflow_runs VALUES ('run', 'plan', 'revision', ${hash}, ${hash}, ${hash}, 'active', 0, 0, ${json(run)})`;
});

it.effect("replays identical commands and rejects changed identities and stale versions", () =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = { runId: run.id, command, externalFacts: facts, receiptId: "receipt" };
    assert.deepStrictEqual(yield* store.commitTransition(input), {
      kind: "accepted",
      receiptId: "receipt",
    });
    assert.deepStrictEqual(yield* store.commitTransition(input), {
      kind: "replay",
      receiptId: "receipt",
    });
    for (const changed of [
      { ...input, command: { ...command, kind: "stop" as const } },
      { ...input, runId: "foreign" },
    ]) {
      assert.isTrue(Exit.isFailure(yield* Effect.exit(store.commitTransition(changed))));
    }
    assert.deepStrictEqual(
      yield* store.commitTransition({ ...input, command: { ...command, commandId: "stale" } }),
      { kind: "rejected", reason: "stale" },
    );
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 1);
    assert.deepStrictEqual(
      yield* sql`SELECT record_version, control_generation, status FROM workflow_runs`,
      [{ record_version: 1, control_generation: 1, status: "paused" }],
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("rolls back a failure after the run update", () =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* sql`CREATE TRIGGER reject_receipt BEFORE INSERT ON workflow_command_receipts BEGIN SELECT RAISE(ABORT, 'failure'); END`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          store.commitTransition({
            runId: run.id,
            command,
            externalFacts: facts,
            receiptId: "receipt",
          }),
        ),
      ),
    );
    assert.deepStrictEqual(
      yield* sql`SELECT record_version, control_generation, status FROM workflow_runs`,
      [{ record_version: 0, control_generation: 0, status: "active" }],
    );
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("serializes decisions against the same version", () =>
  Effect.gen(function* () {
    yield* setup;
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const results = yield* Effect.forEach(
      ["one", "two"],
      (id) =>
        store.commitTransition({
          runId: run.id,
          command: { ...command, commandId: id },
          externalFacts: facts,
          receiptId: id,
        }),
      { concurrency: "unbounded" },
    );
    assert.strictEqual(results.filter((r) => r.kind === "accepted").length, 1);
    assert.strictEqual(
      results.filter((r) => r.kind === "rejected" && r.reason === "stale").length,
      1,
    );
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 1);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["all", "unknown"],
  ["selected", ["evidence"]],
] as const)("amendment invalidation %s", ([scope, affected]) =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* sql`INSERT INTO workflow_operations VALUES ('op', 'run', NULL, 'check', ${hash}, 'pending', 0, 0, 0, NULL, '{}')`;
    yield* sql`INSERT INTO workflow_role_results VALUES ('result', 'run', 'op', '{}', 0)`;
    yield* sql`INSERT INTO workflow_acceptances VALUES ('run', 'item:item', '{}', 0)`;
    const amendment: Input["command"] = {
      ...command,
      kind: "approveAmendment",
      proposal: {
        proposalHash: "proposal",
        previousSpecHash: hash,
        newSpecRef: { ...run.spec.ref, sha256: nextHash },
        newPlanBinding: run.spec.plan,
        limits: {
          maxAttempts: 2,
          maxRepairRounds: 1,
          maxActiveWriters: 1,
          deadline: run.allowance.deadline,
        },
        affectedEvidence: affected,
      },
      actor: { kind: "human", id: "reviewer" },
      approvedProposalHash: "proposal",
    };
    const approval: Input["facts"] = {
      ...facts,
      implementerId: "implementer",
      amendmentApproval: {
        actor: { kind: "human", id: "reviewer" },
        approvedProposalHash: "proposal",
        runId: run.id,
        recordVersion: 0,
        previousSpecHash: hash,
        newSpecHash: nextHash,
      },
    };
    const result = yield* Effect.exit(
      store.commitTransition({
        runId: run.id,
        command: amendment,
        externalFacts: approval,
        receiptId: "receipt",
      }),
    );
    assert.strictEqual(Exit.isSuccess(result), scope === "all");
    const expected = scope === "all" ? 1 : 0;
    assert.deepStrictEqual(yield* sql`SELECT invalidated FROM workflow_role_results`, [
      { invalidated: expected },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT invalidated FROM workflow_acceptances`, [
      { invalidated: expected },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT record_version FROM workflow_runs`, [
      { record_version: expected },
    ]);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, expected);
  }).pipe(Effect.provide(layer)),
);
