// @effect-diagnostics nodeBuiltinImport:off -- Temporary SQLite files require Node filesystem paths.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import {
  decodePlanningPassV1,
  upgradeDevelopmentPlanDocumentV1,
  type PlanningPassV1,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/081_WorkflowRuns.ts";
import planMigration from "../persistence/Migrations/082_DevelopmentPlans.ts";
import { DevelopmentPlanHash } from "./DevelopmentPlanHash.ts";
import * as Store from "./DevelopmentPlanStore.ts";
import * as Service from "./DevelopmentPlanService.ts";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";

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
const key = (origin: {
  readonly environmentId: string;
  readonly projectId: string;
  readonly threadId?: string;
}) => `${origin.environmentId}|${origin.projectId}|${origin.threadId ?? ""}`;
const layer = (
  filename: string,
  grants: Ref.Ref<Set<string>>,
  ports = Layer.merge(Service.emptyPassReaderLayer, Service.denyAttestationLayer),
) => {
  const auth = Layer.succeed(Service.DevelopmentPlanAuthorization, {
    check: ({ origins }) =>
      Ref.get(grants).pipe(
        Effect.flatMap((set) =>
          origins.every((origin) => set.has(key(origin)))
            ? Effect.void
            : Effect.fail(new Service.DevelopmentPlanServiceError({ reason: "unauthorized" })),
        ),
      ),
  });
  return Service.layer.pipe(
    Layer.provideMerge(Layer.mergeAll(Store.layer, auth, ports)),
    Layer.provideMerge(Layer.merge(NodeSqliteClient.layer({ filename }), Digest.layerWebCrypto)),
  );
};
const deniedLayer = Service.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      Store.layer,
      Service.denyAllLayer,
      Service.emptyPassReaderLayer,
      Service.denyAttestationLayer,
    ),
  ),
  Layer.provideMerge(
    Layer.merge(NodeSqliteClient.layer({ filename: ":memory:" }), Digest.layerWebCrypto),
  ),
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
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "plan-service-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      return yield* f(NodePath.join(dir, "test.sqlite"));
    }),
  );
const allGrants = () => new Set(["env|project|", "env|project|thread"]);
const object = {
  planId: "plan",
  revisionId: "revision",
  family: "requirements" as const,
  id: "requirement",
};
const link = { planId: "plan", expectedVersion: 1, threadId: "thread", commandId: "link" };

describe("DevelopmentPlanService", () => {
  it.effect("authorizes all operations and preserves receipts and conflicts", () =>
    Effect.gen(function* () {
      const grants = yield* Ref.make(allGrants());
      yield* Effect.gen(function* () {
        yield* setup;
        const service = yield* Service.DevelopmentPlanService;
        const input = yield* fixture;
        const created = yield* service.create(input, context);
        yield* TestClock.adjust("1 second");
        expect(yield* service.create(input, context)).toEqual({
          kind: "replay",
          plan: created.plan,
        });
        expect((yield* service.get({ planId: "plan" }, context)).createdAt).toBe(
          created.plan.createdAt,
        );
        expect((yield* service.getObject(object, context)).id).toBe("requirement");
        expect(
          (yield* service.list({ projectId: "project", limit: 10 }, context)).plans,
        ).toHaveLength(1);
        const update = {
          ...input,
          revisionId: "revision-2",
          commandId: "update",
          expectedVersion: 0,
        };
        const updated = yield* service.updateDraft(update, context);
        yield* TestClock.adjust("1 second");
        expect(yield* service.updateDraft(update, context)).toEqual({
          kind: "replay",
          plan: updated.plan,
        });
        const conflict = yield* Effect.flip(
          service.updateDraft({ ...update, revisionId: "revision-3", commandId: "other" }, context),
        );
        expect([conflict.reason, conflict.currentVersion]).toEqual(["conflict", 1]);
        expect(
          (yield* Effect.flip(
            service.updateDraft({ ...update, revisionId: "revision-3" }, context),
          )).reason,
        ).toBe("integrity");
        expect((yield* service.linkThread(link, context)).plan.linkedThreadIds).toEqual(["thread"]);
        expect(
          (yield* service.unlinkThread(
            { ...link, expectedVersion: 2, commandId: "unlink" },
            context,
          )).plan.linkedThreadIds,
        ).toEqual([]);
        yield* Ref.set(grants, new Set(["env|project|"]));
        expect(
          (yield* Effect.flip(service.updateDraft({ ...update, commandId: "denied" }, context)))
            .reason,
        ).toBe("unauthorized");
      }).pipe(Effect.provide(layer(":memory:", grants)));
    }),
  );
  it.effect(
    "denies every operation before store access and exposes not-found only to an authorized destination",
    () =>
      Effect.gen(function* () {
        yield* Effect.gen(function* () {
          yield* setup;
          const service = yield* Service.DevelopmentPlanService;
          const store = yield* Store.DevelopmentPlanStore;
          const input = yield* fixture;
          const calls = [
            service.create(input, context),
            service.get({ planId: "missing" }, context),
            service.validateCoverage({ planId: "missing" }, context),
            service.getObject(object, context),
            service.list({ projectId: "project", limit: 10 }, context),
            service.updateDraft({ ...input, expectedVersion: 0 }, context),
            service.linkThread(link, context),
            service.unlinkThread(link, context),
          ];
          for (const call of calls)
            expect(
              (yield* Effect.flip(
                Effect.asVoid(call as Effect.Effect<unknown, Service.DevelopmentPlanServiceError>),
              )).reason,
            ).toBe("unauthorized");
          expect((yield* store.list({ projectId: "project", limit: 10 })).plans).toEqual([]);
        }).pipe(Effect.provide(deniedLayer));
        const grants = yield* Ref.make(allGrants());
        yield* Effect.gen(function* () {
          yield* setup;
          const service = yield* Service.DevelopmentPlanService;
          expect((yield* Effect.flip(service.get({ planId: "missing" }, context))).reason).toBe(
            "not-found",
          );
          expect(
            (yield* Effect.flip(service.list({ projectId: "other", limit: 10 }, context))).reason,
          ).toBe("unauthorized");
        }).pipe(Effect.provide(layer(":memory:", grants)));
      }),
  );
  it.effect("validates exact revision coverage and requires attestation", () =>
    Effect.gen(function* () {
      const grants = yield* Ref.make(allGrants());
      const reviews = yield* Ref.make(new Map<string, PlanningPassV1>());
      const attestations = yield* Ref.make<string[]>([]);
      const reviewKey = (input: {
        planId: string;
        revisionId: string;
        revisionHash: string;
        sourcePacketHash: string;
      }) => [input.planId, input.revisionId, input.revisionHash, input.sourcePacketHash].join("|");
      const reader = Layer.succeed(Service.DevelopmentPlanPassReader, {
        findReview: (input) =>
          Ref.get(reviews).pipe(Effect.map((map) => map.get(reviewKey(input)))),
      });
      const attester = Layer.succeed(Service.DevelopmentPlanReviewAttestation, {
        attest: (input) =>
          Ref.update(attestations, (calls) => [
            ...calls,
            [input.passId, input.revisionHash, input.sourcePacketHash].join("|"),
          ]).pipe(Effect.as(true)),
      });
      const ports = Layer.merge(reader, attester);
      yield* Effect.gen(function* () {
        yield* setup;
        const service = yield* Service.DevelopmentPlanService;
        const input = yield* fixture;
        const revision = {
          ...input.revision,
          workItems: [
            {
              id: "work",
              explanation: "Explanation",
              sourceStatementIds: ["statement"],
              access: input.revision.access,
              title: "Work",
              state: "active" as const,
              requirementIds: [input.revision.requirements[0]!.id],
              criterionIds: ["criterion"],
              dependencyIds: [],
              fileChanges: [],
              verification: [
                { id: "verification", description: "Check", criterionIds: ["criterion"] },
              ],
            },
          ],
        };
        yield* service.create({ ...input, revision }, context);
        const sourcePacketHash = DevelopmentPlanHash.sourcePacket(input.packet);
        const revisionHash = DevelopmentPlanHash.revision(revision);
        const reference = { kind: "source", id: "source", sha256: input.packet.sources[0]!.sha256 };
        const makeReview = (revisionId: string, hash: string, disposition: "missing" | "mapped") =>
          decodePlanningPassV1({
            schemaVersion: 1,
            id: "pass",
            planningSessionId: "session",
            role: "review",
            provider: { providerInstanceId: "codex", model: "model" },
            binding: {
              planId: "plan",
              revisionId,
              revisionSchemaVersion: 1,
              revisionHash: hash,
              sourcePacketHash,
            },
            operationId: "op",
            inputManifest: {
              sources: [reference],
              references: [],
              additionalContext: [],
              access: input.packet.access,
            },
            status: "settled",
            native: { threadId: "thread", runId: "run", terminalMessageId: "message" },
            report: {
              kind: "review",
              verdict: "pass",
              assessment: {
                revisionHash: hash,
                sourcePacketHash,
                inventory: [
                  {
                    id: "inventory",
                    text: "Outcome",
                    sourceRefs: [reference],
                    requirementIds: ["requirement"],
                    disposition,
                  },
                ],
              },
              findings: [],
            },
            access: input.packet.access,
          });
        const lookup = { planId: "plan", revisionId: "revision", revisionHash, sourcePacketHash };
        expect(
          (yield* service.validateCoverage({ planId: "plan" }, context)).coverage.reviewBlockers,
        ).toEqual([{ code: "review-missing" }]);
        const missing = yield* makeReview("revision", revisionHash, "missing");
        yield* Ref.update(reviews, (map) => new Map(map).set(reviewKey(lookup), missing));
        const omitted = yield* service.validateCoverage(
          { planId: "plan", revisionId: "revision" },
          context,
        );
        expect(omitted.coverage.structuralBlockers).toEqual([]);
        expect(omitted.coverage.reviewBlockers).toEqual([
          { code: "anchored-omission", objectId: "inventory" },
        ]);
        expect(omitted.reviewReady).toBe(false);
        const stale = yield* makeReview("revision", "0".repeat(64), "mapped");
        yield* Ref.update(reviews, (map) => new Map(map).set(reviewKey(lookup), stale));
        expect(
          (yield* service.validateCoverage({ planId: "plan" }, context)).coverage.reviewBlockers,
        ).toEqual([{ code: "review-stale" }]);
        const corrected = { ...revision, brief: "Corrected" };
        yield* service.updateDraft(
          {
            ...input,
            revision: corrected,
            revisionId: "revision-2",
            expectedVersion: 0,
            commandId: "update",
          },
          context,
        );
        const correctedHash = DevelopmentPlanHash.revision(corrected);
        const fresh = yield* makeReview("revision-2", correctedHash, "mapped");
        yield* Ref.update(reviews, (map) =>
          new Map(map).set(
            reviewKey({ ...lookup, revisionId: "revision-2", revisionHash: correctedHash }),
            fresh,
          ),
        );
        const ready = yield* service.validateCoverage({ planId: "plan" }, context);
        expect(ready).toEqual({
          revisionId: "revision-2",
          coverage: {
            revisionHash: correctedHash,
            sourcePacketHash,
            structuralBlockers: [],
            reviewBlockers: [],
          },
          reviewReady: true,
        });
        expect(yield* Ref.get(attestations)).toEqual([
          ["pass", correctedHash, sourcePacketHash].join("|"),
        ]);
        yield* Ref.set(grants, new Set(["env|project|"]));
        expect(
          (yield* Effect.flip(service.validateCoverage({ planId: "plan" }, context))).reason,
        ).toBe("unauthorized");
        yield* Ref.set(grants, allGrants());
        const denied = yield* Effect.gen(function* () {
          const serviceWithDeny = yield* Service.DevelopmentPlanService;
          return yield* serviceWithDeny.validateCoverage({ planId: "plan" }, context);
        }).pipe(
          Effect.provide(
            Layer.fresh(Service.layer).pipe(
              Layer.provide(Layer.merge(reader, Service.denyAttestationLayer)),
            ),
          ),
        );
        expect(denied.reviewReady).toBe(false);
      }).pipe(Effect.provide(layer(":memory:", grants, ports)));
    }),
  );
  it.effect("filters revoked origins after reopening persisted plans and retains cursors", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const grants = yield* Ref.make(allGrants());
        const input = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* setup;
            const service = yield* Service.DevelopmentPlanService;
            const value = yield* fixture;
            yield* service.create(value, context);
            yield* service.create(
              {
                ...value,
                planId: "z",
                revisionId: "z-revision",
                commandId: "z-create",
                revision: { ...value.revision, planId: "z" as typeof value.revision.planId },
              },
              context,
            );
            return value;
          }).pipe(Effect.provide(layer(filename, grants))),
        );
        yield* Ref.set(grants, new Set(["env|project|"]));
        yield* Effect.scoped(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql`PRAGMA foreign_keys = ON`;
            const service = yield* Service.DevelopmentPlanService;
            expect((yield* Effect.flip(service.get({ planId: "plan" }, context))).reason).toBe(
              "unauthorized",
            );
            expect((yield* Effect.flip(service.getObject(object, context))).reason).toBe(
              "unauthorized",
            );
            expect(yield* service.list({ projectId: "project", limit: 1 }, context)).toEqual({
              plans: [],
              nextCursor: "plan",
            });
            expect(
              (yield* Effect.flip(
                service.updateDraft(
                  { ...input, revisionId: "new", commandId: "new", expectedVersion: 0 },
                  context,
                ),
              )).reason,
            ).toBe("unauthorized");
          }).pipe(Effect.provide(layer(filename, grants))),
        );
      }),
    ),
  );
});
