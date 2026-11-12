// @effect-diagnostics nodeBuiltinImport:off -- Temporary SQLite files require Node filesystem paths.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import { upgradeDevelopmentPlanDocumentV1 } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/081_WorkflowRuns.ts";
import planMigration from "../persistence/Migrations/082_DevelopmentPlans.ts";
import { DevelopmentPlanHash } from "./DevelopmentPlanHash.ts";
import * as Store from "./DevelopmentPlanStore.ts";

const json = (value: unknown) => JSON.stringify(value);
const context = {
  actor: { kind: "human" as const, id: "actor" },
  destination: { environmentId: "env", projectId: "project" },
};
const timestamp = "2026-11-10T00:00:00.000Z";
const fixture = Effect.gen(function* () {
  const example = {
    schemaVersion: 1,
    planId: "plan",
    revisionId: "revision",
    environmentId: "env",
    projectId: "project",
    title: "Example",
    brief: "Outcome",
    linkedThreadIds: [],
    sources: [
      {
        id: "source",
        kind: "thread_message",
        environmentId: "env",
        projectId: "project",
        threadId: "thread",
        messageId: "message",
        capturedAt: timestamp,
        attribution: "user",
        bytesBase64: "WA==",
        byteLength: 1,
        sha256: "4b68ab3847feda7d6c62c1fbcbeebfa35eab7351ed5e78f4ddadea5df64b8015",
      },
    ],
    sourceStatements: [
      { id: "statement", sourceId: "source", startByte: 0, endByte: 1, text: "X" },
    ],
    requirements: [
      {
        id: "requirement",
        text: "Outcome",
        sourceStatementIds: ["statement"],
        criteria: [{ id: "criterion", text: "Observable" }],
      },
    ],
  };
  const { native } = yield* upgradeDevelopmentPlanDocumentV1({
    bytes: new TextEncoder().encode(json(example)),
    hashSourcePacket: DevelopmentPlanHash.sourcePacket,
  });
  return {
    planId: "plan",
    environmentId: "env",
    projectId: "project",
    title: "Example",
    revisionId: "revision",
    revision: native.revision,
    packet: native.sourcePacket,
    createdAt: timestamp,
    commandId: "create",
  };
});
const layer = (filename = ":memory:") =>
  Store.layer.pipe(
    Layer.provideMerge(Layer.merge(NodeSqliteClient.layer({ filename }), Digest.layerWebCrypto)),
  );
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* migration;
  yield* planMigration;
});
const withDatabase = <A, E, R>(f: (filename: string) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const dir = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "plan-store-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      return yield* f(NodePath.join(dir, "test.sqlite"));
    }),
  );

describe("DevelopmentPlanStore", () => {
  it.effect("creates, appends, links, unlinks and replays original receipts", () =>
    Effect.gen(function* () {
      yield* setup;
      const store = yield* Store.DevelopmentPlanStore;
      const input = yield* fixture;
      const created = yield* store.create(input, context);
      expect(created.plan.version).toBe(0);
      const append = {
        ...input,
        revisionId: "revision-2",
        commandId: "append",
        expectedVersion: 0,
      };
      expect((yield* store.append(append, context)).plan.version).toBe(1);
      const link = { planId: "plan", expectedVersion: 1, threadId: "thread", commandId: "link" };
      expect((yield* store.linkThread(link, context)).plan.linkedThreadIds).toEqual(["thread"]);
      expect(
        (yield* store.unlinkThread({ ...link, expectedVersion: 2, commandId: "unlink" }, context))
          .plan.linkedThreadIds,
      ).toEqual([]);
      expect(yield* store.create(input, context)).toEqual({ kind: "replay", plan: created.plan });
      expect(
        (yield* Effect.flip(store.linkThread({ ...link, threadId: "other" }, context))).reason,
      ).toBe("integrity");
      expect(
        (yield* store.getObject({
          planId: "plan",
          revisionId: "revision",
          family: "requirements",
          id: "requirement",
        })).id,
      ).toBe("requirement");
      expect(
        (yield* Effect.flip(
          store.getObject({
            planId: "plan",
            revisionId: "revision",
            family: "requirements",
            id: "missing",
          }),
        )).reason,
      ).toBe("not-found");
      expect(
        (yield* Effect.flip(
          store.append({ ...append, expectedVersion: 3, commandId: "duplicate" }, context),
        )).reason,
      ).toBe("integrity");
      expect(
        (yield* Effect.flip(
          store.append(
            {
              ...append,
              expectedVersion: 3,
              commandId: "bad",
              revision: { ...input.revision, sourcePacketHash: "0".repeat(64) },
            },
            context,
          ),
        )).reason,
      ).toBe("invalid");
    }).pipe(Effect.provide(layer())),
  );
  it.effect.each(["create", "append", "link", "unlink"] as const)(
    "rolls back %s and its receipt on storage failure",
    (operation) =>
      Effect.gen(function* () {
        yield* setup;
        const sql = yield* SqlClient.SqlClient;
        const store = yield* Store.DevelopmentPlanStore;
        const input = yield* fixture;
        if (operation !== "create") yield* store.create(input, context);
        if (operation === "unlink")
          yield* store.linkThread(
            { planId: "plan", expectedVersion: 0, threadId: "thread", commandId: "initial-link" },
            context,
          );
        yield* sql`CREATE TRIGGER fail_receipt BEFORE INSERT ON development_plan_commands BEGIN SELECT RAISE(ABORT, 'injected'); END`;
        const run =
          operation === "create"
            ? store.create(input, context)
            : operation === "append"
              ? store.append(
                  { ...input, revisionId: "revision-2", commandId: operation, expectedVersion: 0 },
                  context,
                )
              : operation === "link"
                ? store.linkThread(
                    {
                      planId: "plan",
                      expectedVersion: 0,
                      threadId: "thread",
                      commandId: operation,
                    },
                    context,
                  )
                : store.unlinkThread(
                    {
                      planId: "plan",
                      expectedVersion: 1,
                      threadId: "thread",
                      commandId: operation,
                    },
                    context,
                  );
        expect((yield* Effect.flip(run)).reason).toBe("storage");
        const receipts = yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM development_plan_commands WHERE command_id = ${operation}`;
        expect(receipts[0]?.count).toBe(0);
        if (operation === "create")
          expect((yield* Effect.flip(store.get({ planId: "plan" }))).reason).toBe("not-found");
        else {
          const plan = (yield* store.get({ planId: "plan" })).plan;
          expect(plan.version).toBe(operation === "unlink" ? 1 : 0);
          expect(plan.linkedThreadIds).toEqual(operation === "unlink" ? ["thread"] : []);
        }
      }).pipe(Effect.provide(layer())),
  );
  it.effect.each(["revision", "packet", "source bytes"] as const)(
    "fails closed on corrupted %s",
    (target) =>
      Effect.gen(function* () {
        yield* setup;
        const sql = yield* SqlClient.SqlClient;
        const store = yield* Store.DevelopmentPlanStore;
        yield* store.create(yield* fixture, context);
        if (target === "revision")
          yield* sql`UPDATE development_plan_revisions SET revision_json = '{}'`;
        else if (target === "packet")
          yield* sql`UPDATE development_plan_source_packets SET packet_json = '{}'`;
        else
          yield* sql`UPDATE development_plan_source_packets SET packet_json = json_set(packet_json, '$.sources[0].bytesBase64', 'WQ==')`;
        expect((yield* Effect.flip(store.get({ planId: "plan" }))).reason).toBe("integrity");
      }).pipe(Effect.provide(layer())),
  );
  it.effect("lists with keyset paging and clamped limits", () =>
    Effect.gen(function* () {
      yield* setup;
      const store = yield* Store.DevelopmentPlanStore;
      yield* store.create(yield* fixture, context);
      const second = yield* fixture;
      yield* store.create(
        {
          ...second,
          planId: "z",
          revisionId: "z-revision",
          commandId: "z-create",
          revision: { ...second.revision, planId: "z" as typeof second.revision.planId },
        },
        context,
      );
      const page = yield* store.list({ projectId: "project", limit: 0 });
      expect(page.plans.map((p) => p.planId)).toEqual(["plan"]);
      expect(page.nextCursor).toBe("plan");
      expect(
        (yield* store.list({
          projectId: "project",
          cursor: "plan",
          limit: 1000,
        })).plans.map((p) => p.planId),
      ).toEqual(["z"]);
    }).pipe(Effect.provide(layer())),
  );
  it.effect("serializes competing file database edits and survives reopen", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const input = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* setup;
            const store = yield* Store.DevelopmentPlanStore;
            const value = yield* fixture;
            yield* store.create(value, context);
            return value;
          }).pipe(Effect.provide(layer(filename))),
        );
        const edit = (id: string) =>
          Effect.scoped(
            Effect.gen(function* () {
              const sql = yield* SqlClient.SqlClient;
              yield* sql`PRAGMA foreign_keys = ON`;
              const store = yield* Store.DevelopmentPlanStore;
              return yield* Effect.result(
                store.append(
                  { ...input, revisionId: id, commandId: id, expectedVersion: 0 },
                  context,
                ),
              );
            }).pipe(Effect.provide(layer(filename))),
          );
        const results = yield* Effect.all([edit("a"), edit("b")], { concurrency: "unbounded" });
        expect(results.filter((r) => r._tag === "Success")).toHaveLength(1);
        const failures = results.filter((r) => r._tag === "Failure");
        expect(failures).toHaveLength(1);
        expect(failures[0]?.failure.reason).toBe("conflict");
        expect(failures[0]?.failure.currentVersion).toBe(1);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.DevelopmentPlanStore;
            const snapshot = yield* store.get({ planId: "plan" });
            expect(snapshot.sourcePacket).toEqual(input.packet);
            expect(
              (yield* store.append(
                { ...input, revisionId: "retry", commandId: "retry", expectedVersion: 1 },
                context,
              )).plan.version,
            ).toBe(2);
          }).pipe(Effect.provide(layer(filename))),
        );
      }),
    ),
  );
});
