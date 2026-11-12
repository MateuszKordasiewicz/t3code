// @effect-diagnostics nodeBuiltinImport:off -- Temporary SQLite files require Node filesystem paths.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "@effect/vitest";
import {
  decodePlanningPassV1,
  upgradeDevelopmentPlanDocumentV1,
  PlanningPassV1,
  PlanningSessionV1,
  WorkflowOperationV1,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as WorkflowStore from "../workflowRuns/WorkflowRunStore.ts";
import * as Observation from "./PlanningPassObservation.ts";
import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";
import evidenceMigration from "../persistence/Migrations/083_PlanningEvidence.ts";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/081_WorkflowRuns.ts";
import planMigration from "../persistence/Migrations/082_DevelopmentPlans.ts";
import intentMigration from "../persistence/Migrations/084_WorkflowNativeLaunchIntents.ts";
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
  ports: Layer.Layer<
    Service.DevelopmentPlanPassReader | Service.DevelopmentPlanReviewAttestation,
    never,
    SqlClient.SqlClient
  > = Layer.merge(Service.emptyPassReaderLayer, Service.denyAttestationLayer),
) => {
  const auth = Layer.succeed(Service.DevelopmentPlanAuthorization, {
    check: ({ origins, destination }) =>
      Ref.get(grants).pipe(
        Effect.flatMap((set) =>
          set.has(key(destination)) && origins.every((origin) => set.has(key(origin)))
            ? Effect.void
            : Effect.fail(new Service.DevelopmentPlanServiceError({ reason: "unauthorized" })),
        ),
      ),
  });
  return Service.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Store.layer,
        WorkflowStore.layer,
        Observation.unknownObservationLayer,
        auth,
        ports,
      ),
    ),
    Layer.provideMerge(Layer.merge(NodeSqliteClient.layer({ filename }), Digest.layerWebCrypto)),
  );
};
const deniedLayer = Service.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      Store.layer,
      WorkflowStore.layer,
      Observation.unknownObservationLayer,
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
  yield* intentMigration;
  yield* evidenceMigration;
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

const decodeSession = Schema.decodeSync(PlanningSessionV1);
const decodePass = Schema.decodeSync(PlanningPassV1);
const decodeOperation = Schema.decodeSync(WorkflowOperationV1);
const planningSession = decodeSession({
  schemaVersion: 1,
  id: "session",
  environmentId: "env",
  projectId: "project",
  planId: "plan",
  origin: context.actor,
  request: "Plan",
  mode: "ordinary",
  allowedRoles: ["author", "review", "repair"],
  allowedProviders: [{ providerInstanceId: "provider", model: "model" }],
  limits: { maxPasses: 4, maxRepairRounds: 2, deadline: "2026-12-01T00:00:00Z" },
  consumed: { passes: 0, repairRounds: 0 },
  controlGeneration: 0,
  recordVersion: 0,
  status: "active",
  createdAt: timestamp,
  access: { origins: [] },
});
const planningContext = {
  actor: planningSession.origin,
  destination: {
    environmentId: planningSession.environmentId,
    projectId: planningSession.projectId,
  },
};
const planningCommand = (id: string) => ({ commandId: id, receiptId: `receipt-${id}` });
const makePass = (
  value: Effect.Success<typeof fixture>,
  id: string,
  role: "author" | "review" | "repair" = "author",
) => {
  const binding = {
    planId: planningSession.planId,
    revisionId: value.revision.planId === planningSession.planId ? "revision" : "other",
    revisionSchemaVersion: 1 as const,
    revisionHash: DevelopmentPlanHash.revision(value.revision),
    sourcePacketHash: DevelopmentPlanHash.sourcePacket(value.packet),
  };
  const pass = decodePass({
    schemaVersion: 1,
    id: `pass-${id}`,
    planningSessionId: planningSession.id,
    role,
    provider: { providerInstanceId: "provider", model: "model" },
    binding,
    operationId: `operation-${id}`,
    inputManifest: {
      sources: [{ kind: "source", id: "source", sha256: binding.sourcePacketHash }],
      references: [],
      additionalContext: [],
      access: { origins: [] },
    },
    status: "pending",
    native: { threadId: `native-${id}`, runId: `run-${id}` },
    access: { origins: [] },
  });
  const operation = decodeOperation({
    schemaVersion: 1,
    id: pass.operationId,
    owner: { kind: "planning_session", planningSessionId: planningSession.id },
    kind: "planning_pass",
    parameters: {
      kind: "planning_pass",
      passId: pass.id,
      binding: pass.binding,
      role,
      provider: pass.provider,
      limits: planningSession.limits,
      inputManifestHash: WorkflowCanonicalHash.tagged(
        "t3.planning-input-manifest.v1",
        pass.inputManifest,
      ),
    },
    fingerprint: "0".repeat(64),
    origin: planningSession.origin,
    destination: planningContext.destination,
    controlGeneration: 0,
    status: "pending",
    access: pass.access,
  });
  return { pass, operation };
};
const decodeThread = Schema.decodeSync(PlanningPassV1.fields.native.schema.fields.threadId);
const decodeFinding = Schema.decodeSync(
  PlanningPassV1.fields.report.schema.members[1].fields.dispositions.value.fields.findingId,
);
const decodeObject = Schema.decodeSync(
  PlanningPassV1.fields.report.schema.members[1].fields.dispositions.value.fields.affectedObjectIds
    .value,
);
const reviewOrigin = decodeThread("review-origin");
const findingId = decodeFinding("finding");
const objectId = decodeObject("object");
const terminalId = Schema.decodeSync(
  PlanningPassV1.fields.native.schema.fields.terminalMessageId.schema,
)("terminal");
describe("DevelopmentPlanService", () => {
  it.effect("hides plans owned by another destination across every operation", () =>
    Effect.gen(function* () {
      for (const owner of [
        { environmentId: "other-env", projectId: "project" },
        { environmentId: "env", projectId: "other-project" },
      ]) {
        const grants = yield* Ref.make(allGrants());
        yield* Effect.gen(function* () {
          yield* setup;
          const service = yield* Service.DevelopmentPlanService;
          const sql = yield* SqlClient.SqlClient;
          const value = yield* fixture;
          yield* service.create(value, context);
          yield* service.beginPlanning(
            { session: planningSession, command: planningCommand("begin") },
            planningContext,
          );
          const pass = makePass(value, "existing");
          yield* service.beginPass(
            {
              sessionId: planningSession.id,
              expectedVersion: 0,
              ...pass,
              command: planningCommand("pass"),
            },
            planningContext,
          );
          yield* sql`INSERT INTO workflow_plan_identities (plan_id) VALUES ('missing')`;
          yield* sql`INSERT INTO workflow_planning_sessions (id, plan_id, predecessor_id, status, record_version, control_generation, record_json) SELECT 'missing-session', 'missing', predecessor_id, status, record_version, control_generation, json_set(record_json, '$.id', 'missing-session', '$.planId', 'missing') FROM workflow_planning_sessions WHERE id = 'session'`;
          yield* sql`UPDATE development_plans SET environment_id = ${owner.environmentId}, project_id = ${owner.projectId} WHERE plan_id = 'plan'`;
          const snapshot = Effect.gen(function* () {
            return {
              plans: yield* sql`SELECT * FROM development_plans`,
              revisions: yield* sql`SELECT * FROM development_plan_revisions`,
              packets: yield* sql`SELECT * FROM development_plan_source_packets`,
              links: yield* sql`SELECT * FROM development_plan_threads`,
              commands: yield* sql`SELECT * FROM development_plan_commands`,
              sessions: yield* sql`SELECT * FROM workflow_planning_sessions`,
              passes: yield* sql`SELECT * FROM workflow_planning_passes`,
              operations: yield* sql`SELECT * FROM workflow_operations`,
              receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
              dispositions: yield* sql`SELECT * FROM workflow_planning_dispositions`,
            };
          });
          const before = yield* snapshot;
          const command = planningCommand("denied-owner");

          const disposition = {
            reportId: pass.pass.id,
            findingId,
            decision: "accept" as const,
            rationale: "reason",
            affectedObjectIds: [],
          };
          const actions = (
            planId: string,
            session: typeof planningSession,
          ): ReadonlyArray<
            readonly [string, Effect.Effect<unknown, { readonly reason: string }>]
          > => {
            const control = { sessionId: session.id, expectedVersion: 1, command };
            return [
              ["get", service.get({ planId }, context)],
              ["getObject", service.getObject({ ...object, planId }, context)],
              ["export", service.export({ planId }, context)],
              ["validateCoverage", service.validateCoverage({ planId }, context)],
              [
                "updateDraft",
                service.updateDraft(
                  { ...value, planId, expectedVersion: 0, commandId: "update-owner" },
                  context,
                ),
              ],
              ["linkThread", service.linkThread({ ...link, planId, expectedVersion: 0 }, context)],
              [
                "unlinkThread",
                service.unlinkThread({ ...link, planId, expectedVersion: 0 }, context),
              ],
              ["beginPlanning", service.beginPlanning({ session, command }, planningContext)],
              ["getPlanning", service.getPlanning({ sessionId: session.id }, planningContext)],
              ["pausePlanning", service.pausePlanning(control, planningContext)],
              ["resumePlanning", service.resumePlanning(control, planningContext)],
              ["stopPlanning", service.stopPlanning(control, planningContext)],
              ["beginPass", service.beginPass({ ...control, ...pass }, planningContext)],
              [
                "recordDisposition",
                service.recordDisposition(
                  {
                    ...control,
                    reviewPassId: pass.pass.id,
                    repairPassId: pass.pass.id,
                    findingId: "finding",
                    disposition,
                  },
                  planningContext,
                ),
              ],
            ];
          };
          const existing = actions("plan", planningSession);
          const missing = actions(
            "missing",
            decodeSession({ ...planningSession, id: "missing-session", planId: "missing" }),
          );
          for (let i = 0; i < existing.length; i++) {
            const [name, action] = existing[i]!;
            const error = yield* Effect.flip(Effect.asVoid(action));
            expect(error.reason, name).toBe("not-found");
            const missingError = yield* Effect.flip(Effect.asVoid(missing[i]![1]));
            expect(error, name).toEqual(missingError);
            expect(yield* snapshot, name).toEqual(before);
          }
        }).pipe(Effect.provide(layer(":memory:", grants)));
      }
    }),
  );
  it.effect("filters list owners without changing the source cursor", () =>
    Effect.gen(function* () {
      const grants = yield* Ref.make(allGrants());
      yield* Effect.gen(function* () {
        yield* setup;
        const service = yield* Service.DevelopmentPlanService;
        const store = yield* Store.DevelopmentPlanStore;
        const sql = yield* SqlClient.SqlClient;
        const value = yield* fixture;
        yield* service.create(value, context);
        const input = { projectId: "project", limit: 1 };
        expect((yield* service.list(input, context)).plans).toHaveLength(1);
        yield* sql`INSERT INTO workflow_plan_identities (plan_id) VALUES ('z-plan')`;
        yield* sql`INSERT INTO development_plans (plan_id, environment_id, project_id, title, current_revision_id, version, created_at) VALUES ('z-plan', 'env', 'project', 'Next', NULL, 0, ${timestamp})`;
        yield* sql`UPDATE development_plans SET environment_id = 'other-env' WHERE plan_id = 'plan'`;
        const page = yield* store.list(input);
        expect(page.nextCursor).toBe("plan");
        expect(yield* service.list(input, context)).toEqual({ ...page, plans: [] });
      }).pipe(Effect.provide(layer(":memory:", grants)));
    }),
  );
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
  it.effect("imports V1 bytes and round-trips V2 native data into a fresh database", () =>
    Effect.gen(function* () {
      const grants = yield* Ref.make(allGrants());
      const exported = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* setup;
          const service = yield* Service.DevelopmentPlanService;
          const value = yield* fixture;
          yield* service.create(value, context);
          const original = yield* service.export({ planId: "plan" }, context);
          const { native: _native, ...projection } = original;
          const v1 = { ...projection, schemaVersion: 1 };
          const bytes = new TextEncoder().encode(JSON.stringify(v1, null, 2));
          const mismatch = yield* Effect.flip(
            service.import(
              {
                format: "v2",
                bytes: new TextEncoder().encode(json(original)),
                planId: "other",
                revisionId: "revision",
                title: "Example",
                commandId: "mismatch",
              },
              context,
            ),
          );
          expect(mismatch.reason).toBe("invalid");
          return { original, v1, bytes };
        }).pipe(Effect.provide(layer(":memory:", grants))),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* setup;
          const service = yield* Service.DevelopmentPlanService;
          yield* service.import(
            {
              format: "v1",
              bytes: exported.bytes,
              planId: "plan",
              revisionId: "revision",
              title: "Example",
              commandId: "v1",
            },
            context,
          );
          const v2 = yield* service.export({ planId: "plan" }, context);
          const { native, ...projection } = v2;
          expect({ ...projection, schemaVersion: 1 }).toEqual(exported.v1);
          expect(native.revision.legacyDocument?.bytesBase64).toBe(
            Buffer.from(exported.bytes).toString("base64"),
          );
        }).pipe(Effect.provide(layer(":memory:", grants))),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* setup;
          const service = yield* Service.DevelopmentPlanService;
          yield* service.import(
            {
              format: "v2",
              bytes: new TextEncoder().encode(json(exported.original)),
              planId: "plan",
              revisionId: "revision",
              title: "Example",
              commandId: "v2",
            },
            context,
          );
          const actual = yield* service.get({ planId: "plan" }, context);
          expect({ revision: actual.revision, sourcePacket: actual.sourcePacket }).toEqual(
            exported.original.native,
          );
        }).pipe(Effect.provide(layer(":memory:", grants))),
      );
    }),
  );
  it.effect(
    "imports exact markdown and validated plan artifacts without extracting requirements",
    () =>
      Effect.gen(function* () {
        const grants = yield* Ref.make(allGrants());
        yield* Effect.gen(function* () {
          yield* setup;
          const service = yield* Service.DevelopmentPlanService;
          const bytes = new TextEncoder().encode("# Hello\r\nWorld\n");
          const input = {
            format: "markdown" as const,
            bytes,
            planId: "plan",
            revisionId: "revision",
            title: "Title",
            commandId: "create",
            documentId: "doc",
          };
          const markdownImport = yield* service.import(input, context);
          yield* TestClock.adjust("1 hour");
          expect(yield* service.import(input, context)).toEqual({
            kind: "replay",
            plan: markdownImport.plan,
          });
          const result = yield* service.get({ planId: "plan" }, context);
          expect(result.sourcePacket.sources[0]).toMatchObject({
            documentId: "doc",
            bytesBase64: Buffer.from(bytes).toString("base64"),
            byteLength: bytes.length,
            sha256: "19a70ac56edc83e42014282da204802c43f103cf775616b50a5cdb8f7dbb1a4f",
          });
          expect(result.revision.requirements).toEqual([]);
          expect(result.revision.unmappedMaterial).toEqual([
            { sourceId: "source-1", description: "Imported markdown; requirements not extracted" },
          ]);
          for (const raw of ["{", "{}", "\xff"]) {
            expect(
              (yield* Effect.flip(
                service.import(
                  { ...input, format: "plan-artifact", bytes: new TextEncoder().encode(raw) },
                  context,
                ),
              )).reason,
            ).toBe("invalid");
          }
          expect(
            (yield* Effect.flip(
              service.import(
                {
                  format: "markdown",
                  bytes,
                  planId: "missing",
                  revisionId: "revision",
                  title: "Title",
                  commandId: "missing",
                },
                context,
              ),
            )).reason,
          ).toBe("invalid");
          const artifact = {
            id: "artifact",
            threadId: "thread",
            runId: null,
            nodeId: "node",
            status: "draft",
            kind: "proposed_plan",
            markdown: "Plan",
          };
          const artifactBytes = new TextEncoder().encode(JSON.stringify(artifact, null, 2));
          yield* service.import(
            {
              ...input,
              format: "plan-artifact",
              bytes: artifactBytes,
              planId: "artifact-plan",
              revisionId: "artifact-revision",
              commandId: "artifact",
            },
            context,
          );
          const imported = yield* service.get({ planId: "artifact-plan" }, context);
          expect(imported.sourcePacket.sources[0]?.bytesBase64).toBe(
            Buffer.from(artifactBytes).toString("base64"),
          );
          expect(imported.revision.requirements).toEqual([]);
        }).pipe(Effect.provide(layer(":memory:", grants)));
      }),
  );
  it.effect("checks destination before import and export and denies export after revocation", () =>
    Effect.gen(function* () {
      const input = {
        format: "markdown" as const,
        bytes: new Uint8Array(),
        planId: "plan",
        revisionId: "revision",
        title: "Title",
        commandId: "create",
        documentId: "doc",
      };
      yield* Effect.gen(function* () {
        const service = yield* Service.DevelopmentPlanService;
        expect((yield* Effect.flip(service.import(input, context))).reason).toBe("unauthorized");
        expect((yield* Effect.flip(service.export({ planId: "missing" }, context))).reason).toBe(
          "unauthorized",
        );
      }).pipe(Effect.provide(deniedLayer));
      const grants = yield* Ref.make(allGrants());
      yield* Effect.gen(function* () {
        yield* setup;
        const service = yield* Service.DevelopmentPlanService;
        const value = yield* fixture;
        yield* service.create(value, context);
        yield* Ref.set(grants, new Set(["env|project|"]));
        expect((yield* Effect.flip(service.export({ planId: "plan" }, context))).reason).toBe(
          "unauthorized",
        );
      }).pipe(Effect.provide(layer(":memory:", grants)));
    }),
  );
  it.effect("authorizes planning controls and replays across pause and resume", () =>
    withDatabase((filename) =>
      Effect.gen(function* () {
        const grants = yield* Ref.make(allGrants());
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* setup;
            const service = yield* Service.DevelopmentPlanService;
            const value = yield* fixture;
            yield* service.create(value, context);
            const begin = { session: planningSession, command: planningCommand("begin") };
            const started = yield* service.beginPlanning(begin, planningContext);
            expect((yield* service.beginPlanning(begin, planningContext)).kind).toBe("replay");
            expect(
              yield* service.getPlanning({ sessionId: planningSession.id }, planningContext),
            ).toEqual(started.value);
            const pass = makePass(value, "one");
            const admission = {
              sessionId: planningSession.id,
              expectedVersion: 0,
              ...pass,
              command: planningCommand("pass"),
            };
            yield* service.beginPass(admission, planningContext);
            const pause = {
              sessionId: planningSession.id,
              expectedVersion: 1,
              command: planningCommand("pause"),
            };
            const paused = yield* service.pausePlanning(pause, planningContext);
            expect((yield* service.pausePlanning(pause, planningContext)).kind).toBe("replay");
            const resume = {
              sessionId: planningSession.id,
              expectedVersion: paused.value.recordVersion,
              command: planningCommand("resume"),
            };
            const resumed = yield* service.resumePlanning(resume, planningContext);
            expect((yield* service.resumePlanning(resume, planningContext)).kind).toBe("replay");
            expect((yield* service.beginPass(admission, planningContext)).kind).toBe("replay");
            const stop = {
              sessionId: planningSession.id,
              expectedVersion: resumed.value.recordVersion,
              command: planningCommand("stop"),
            };
            yield* service.stopPlanning(stop, planningContext);
            expect((yield* service.stopPlanning(stop, planningContext)).kind).toBe("replay");
            yield* Ref.set(grants, new Set(["env|project|"]));
            expect((yield* Effect.flip(service.beginPass(admission, planningContext))).reason).toBe(
              "unauthorized",
            );
          }).pipe(Effect.provide(layer(filename, grants))),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            expect(yield* sql`SELECT id FROM workflow_planning_sessions`).toHaveLength(1);
            expect(yield* sql`SELECT id FROM workflow_planning_passes`).toHaveLength(1);
          }).pipe(Effect.provide(NodeSqliteClient.layer({ filename }))),
        );
      }),
    ),
  );
  it.effect("denies all public planning methods before lookup and writes", () =>
    Effect.gen(function* () {
      const grants = yield* Ref.make(allGrants());
      yield* Effect.gen(function* () {
        yield* setup;
        const service = yield* Service.DevelopmentPlanService;
        const value = yield* fixture;
        yield* service.create(value, context);
        yield* Ref.set(grants, new Set());
        const pass = makePass(value, "denied");
        const command = planningCommand("denied");
        const input = { sessionId: planningSession.id, expectedVersion: 0, command };
        const disposition = {
          reportId: pass.pass.id,
          findingId,
          decision: "accept" as const,
          rationale: "reason",
          affectedObjectIds: [],
        };
        const actions: ReadonlyArray<Effect.Effect<unknown, { readonly reason: string }>> = [
          service.beginPlanning({ session: planningSession, command }, planningContext),
          service.getPlanning({ sessionId: planningSession.id }, planningContext),
          service.pausePlanning(input, planningContext),
          service.resumePlanning(input, planningContext),
          service.stopPlanning(input, planningContext),
          service.beginPass({ ...input, ...pass }, planningContext),
          service.recordDisposition(
            {
              ...input,
              reviewPassId: pass.pass.id,
              repairPassId: pass.pass.id,
              findingId: "finding",
              disposition,
            },
            planningContext,
          ),
        ];
        for (const action of actions)
          expect((yield* Effect.flip(Effect.asVoid(action))).reason).toBe("unauthorized");
        const sql = yield* SqlClient.SqlClient;
        expect(yield* sql`SELECT * FROM workflow_planning_sessions`).toHaveLength(0);
        expect(yield* sql`SELECT * FROM workflow_command_receipts`).toHaveLength(0);
      }).pipe(Effect.provide(layer(":memory:", grants)));
    }),
  );
  it.effect("settles only an observed terminal pass", () =>
    Effect.gen(function* () {
      const grants = yield* Ref.make(allGrants());
      yield* Effect.gen(function* () {
        yield* setup;
        const service = yield* Service.DevelopmentPlanService;
        const workflow = yield* WorkflowStore.WorkflowRunStore;
        const value = yield* fixture;
        yield* service.create(value, context);
        yield* service.beginPlanning(
          { session: planningSession, command: planningCommand("begin") },
          planningContext,
        );
        const admission = makePass(value, "one");
        yield* service.beginPass(
          {
            sessionId: planningSession.id,
            expectedVersion: 0,
            ...admission,
            command: planningCommand("pass"),
          },
          planningContext,
        );
        yield* workflow.claim({
          operationId: admission.pass.operationId,
          expectedOwnerVersion: 1,
          expectedGeneration: 0,
          bootId: "boot",
          workerId: "worker",
          now: timestamp,
          trustedEligibility: true,
        });
        const native = admission.pass.native;
        if (!native) throw new Error("missing native");
        const input = {
          sessionId: planningSession.id,
          passId: admission.pass.id,
          operationId: admission.pass.operationId,
          expectedVersion: 1,
          native,
          report: { kind: "author" as const, summary: "Done", outputs: [] },
        };
        expect((yield* Effect.flip(service.settlePass(input))).reason).toBe("observation-unknown");
        const sql = yield* SqlClient.SqlClient;
        expect(yield* sql`SELECT * FROM workflow_planning_observations`).toHaveLength(0);
        const terminal = Layer.succeed(Observation.PlanningPassObservation, {
          observe: () =>
            Effect.succeed({
              kind: "terminal" as const,
              operationId: input.operationId,
              passId: input.passId,
              native: { ...native, terminalMessageId: terminalId },
              outcome: "succeeded" as const,
              actualInputManifest: admission.pass.inputManifest,
              inputManifestHash: WorkflowCanonicalHash.tagged(
                "t3.planning-input-manifest.v1",
                admission.pass.inputManifest,
              ),
              freshConversation: true,
              sourcePacketDelivered: true,
              forbiddenContextDelivered: false,
              restrictedProfileQualified: false,
            }),
        });
        const settled = yield* Effect.gen(function* () {
          const current = yield* Service.DevelopmentPlanService;
          return yield* current.settlePass(input);
        }).pipe(Effect.provide(Layer.fresh(Service.layer).pipe(Layer.provide(terminal))));
        expect(settled.value.pass.status).toBe("settled");
        expect(yield* sql`SELECT * FROM workflow_planning_observations`).toHaveLength(1);
      }).pipe(Effect.provide(layer(":memory:", grants)));
    }),
  );
  it.effect("authorizes both pass origins before recording a disposition and replay", () =>
    Effect.gen(function* () {
      const grants = yield* Ref.make(new Set([...allGrants(), "env|project|review-origin"]));
      yield* Effect.gen(function* () {
        yield* setup;
        const service = yield* Service.DevelopmentPlanService;
        const workflow = yield* WorkflowStore.WorkflowRunStore;
        const value = yield* fixture;
        yield* service.create(value, context);
        yield* service.beginPlanning(
          { session: planningSession, command: planningCommand("begin") },
          planningContext,
        );
        const reviewBase = makePass(value, "review", "review");
        const review = {
          ...reviewBase,
          pass: {
            ...reviewBase.pass,
            access: {
              origins: [
                {
                  environmentId: planningSession.environmentId,
                  projectId: planningSession.projectId,
                  threadId: reviewOrigin,
                },
              ],
            },
          },
        };
        const admitAndSettle = Effect.fn("test.admitAndSettle")(function* (
          entry: ReturnType<typeof makePass>,
          version: number,
          report: NonNullable<PlanningPassV1["report"]>,
        ) {
          yield* service.beginPass(
            {
              sessionId: planningSession.id,
              expectedVersion: version,
              ...entry,
              command: planningCommand(entry.pass.id),
            },
            planningContext,
          );
          yield* workflow.claim({
            operationId: entry.pass.operationId,
            expectedOwnerVersion: version + 1,
            expectedGeneration: 0,
            bootId: "boot",
            workerId: "worker",
            now: timestamp,
            trustedEligibility: true,
          });
          if (!entry.pass.native) throw new Error("missing native");
          return yield* workflow.settlePlanningPass({
            sessionId: planningSession.id,
            passId: entry.pass.id,
            operationId: entry.pass.operationId,
            expectedVersion: version + 1,
            report,
            observation: {
              kind: "terminal",
              operationId: entry.pass.operationId,
              passId: entry.pass.id,
              native: { ...entry.pass.native, terminalMessageId: terminalId },
              outcome: "succeeded",
              actualInputManifest: entry.pass.inputManifest,
              inputManifestHash: WorkflowCanonicalHash.tagged(
                "t3.planning-input-manifest.v1",
                entry.pass.inputManifest,
              ),
              freshConversation: true,
              sourcePacketDelivered: true,
              forbiddenContextDelivered: false,
              restrictedProfileQualified: false,
            },
          });
        });
        yield* admitAndSettle(review, 0, {
          kind: "review",
          verdict: "changes_requested",
          assessment: {
            revisionHash: review.pass.binding.revisionHash,
            sourcePacketHash: review.pass.binding.sourcePacketHash,
            inventory: [],
          },
          findings: [
            {
              id: findingId,
              severity: "blocking",
              description: "Problem",
              affectedObjectIds: [objectId],
              evidence: [],
              proposedRemedy: "Repair",
            },
          ],
        });
        const repair = makePass(value, "repair", "repair");
        const disposition = {
          reportId: review.pass.id,
          findingId,
          decision: "accept" as const,
          rationale: "Fixed",
          affectedObjectIds: [objectId],
        };
        yield* admitAndSettle(repair, 2, {
          kind: "repair",
          summary: "Fixed",
          dispositions: [disposition],
        });
        const input = {
          sessionId: planningSession.id,
          reviewPassId: review.pass.id,
          repairPassId: repair.pass.id,
          findingId: "finding",
          disposition,
          expectedVersion: 4,
          command: planningCommand("disposition"),
        };
        yield* Ref.set(grants, allGrants());
        expect((yield* Effect.flip(service.recordDisposition(input, planningContext))).reason).toBe(
          "unauthorized",
        );
        const sql = yield* SqlClient.SqlClient;
        expect(
          yield* sql`SELECT * FROM workflow_command_receipts WHERE command_id = 'disposition'`,
        ).toHaveLength(0);
        yield* Ref.set(grants, new Set([...allGrants(), "env|project|review-origin"]));
        expect((yield* service.recordDisposition(input, planningContext)).kind).toBe("applied");
        expect((yield* service.recordDisposition(input, planningContext)).kind).toBe("replay");
        yield* Ref.set(grants, allGrants());
        expect((yield* Effect.flip(service.recordDisposition(input, planningContext))).reason).toBe(
          "unauthorized",
        );
      }).pipe(Effect.provide(layer(":memory:", grants)));
    }),
  );
  it.effect("attests persisted reviews and fails closed on missing or altered evidence", () =>
    Effect.gen(function* () {
      const cases = [
        "positive",
        "stale hash",
        "failed verdict",
        "not fresh",
        "sources not delivered",
        "forbidden context",
        "unqualified profile",
        "manifest mismatch",
        "blocking finding",
        "unresolved disposition",
        "non-review",
        "ambiguous reviews",
        "malformed",
        "no review",
      ] as const;
      for (const scenario of cases) {
        const grants = yield* Ref.make(allGrants());
        yield* Effect.gen(function* () {
          yield* setup;
          const service = yield* Service.DevelopmentPlanService;
          const workflow = yield* WorkflowStore.WorkflowRunStore;
          const reader = yield* Service.DevelopmentPlanPassReader;
          const sql = yield* SqlClient.SqlClient;
          const base = yield* fixture;
          const revision = {
            ...base.revision,
            workItems: [
              {
                id: "work",
                explanation: "Explanation",
                sourceStatementIds: ["statement"],
                access: base.revision.access,
                title: "Work",
                state: "active" as const,
                requirementIds: [base.revision.requirements[0]!.id],
                criterionIds: ["criterion"],
                dependencyIds: [],
                fileChanges: [],
                verification: [
                  { id: "verification", description: "Check", criterionIds: ["criterion"] },
                ],
              },
            ],
          };
          const value = { ...base, revision };
          yield* service.create(value, context);
          const lookup = {
            planId: value.planId,
            revisionId: value.revisionId,
            revisionHash: DevelopmentPlanHash.revision(revision),
            sourcePacketHash: DevelopmentPlanHash.sourcePacket(value.packet),
          };
          if (scenario === "no review") {
            expect(yield* reader.findReview(lookup), scenario).toBeUndefined();
            expect(
              (yield* service.validateCoverage({ planId: "plan" }, context)).reviewReady,
              scenario,
            ).toBe(false);
            return;
          }
          yield* service.beginPlanning(
            { session: planningSession, command: planningCommand("begin") },
            planningContext,
          );
          const entry = makePass(value, "review", scenario === "non-review" ? "author" : "review");
          yield* service.beginPass(
            {
              sessionId: planningSession.id,
              expectedVersion: 0,
              ...entry,
              command: planningCommand("pass"),
            },
            planningContext,
          );
          yield* workflow.claim({
            operationId: entry.pass.operationId,
            expectedOwnerVersion: 1,
            expectedGeneration: 0,
            bootId: "boot",
            workerId: "worker",
            now: timestamp,
            trustedEligibility: true,
          });
          const native = entry.pass.native;
          if (!native) throw new Error("missing native");
          const reviewReport: NonNullable<PlanningPassV1["report"]> =
            scenario === "non-review"
              ? { kind: "author", summary: "Done", outputs: [] }
              : {
                  kind: "review",
                  verdict: scenario === "failed verdict" ? "changes_requested" : "pass",
                  assessment: {
                    revisionHash: lookup.revisionHash,
                    sourcePacketHash: lookup.sourcePacketHash,
                    inventory: [],
                  },
                  findings:
                    scenario === "blocking finding" || scenario === "unresolved disposition"
                      ? [
                          {
                            id: findingId,
                            severity: scenario === "blocking finding" ? "blocking" : "nonblocking",
                            description: "Problem",
                            affectedObjectIds: [objectId],
                            evidence: [],
                            proposedRemedy: "Fix",
                          },
                        ]
                      : [],
                };
          const terminal = Layer.succeed(Observation.PlanningPassObservation, {
            observe: () =>
              Effect.succeed({
                kind: "terminal" as const,
                operationId: entry.pass.operationId,
                passId: entry.pass.id,
                native: { ...native, terminalMessageId: terminalId },
                outcome: "succeeded" as const,
                actualInputManifest: entry.pass.inputManifest,
                inputManifestHash: WorkflowCanonicalHash.tagged(
                  "t3.planning-input-manifest.v1",
                  entry.pass.inputManifest,
                ),
                freshConversation: scenario !== "not fresh",
                sourcePacketDelivered: scenario !== "sources not delivered",
                forbiddenContextDelivered: scenario === "forbidden context",
                restrictedProfileQualified: scenario !== "unqualified profile",
              }),
          });
          yield* Effect.gen(function* () {
            const current = yield* Service.DevelopmentPlanService;
            yield* current.settlePass({
              sessionId: planningSession.id,
              passId: entry.pass.id,
              operationId: entry.pass.operationId,
              expectedVersion: 1,
              native,
              report: reviewReport,
            });
          }).pipe(Effect.provide(Layer.fresh(Service.layer).pipe(Layer.provide(terminal))));
          if (scenario === "stale hash")
            yield* service.updateDraft(
              {
                planId: value.planId,
                expectedVersion: 0,
                revisionId: "revision-2",
                revision: { ...value.revision, brief: "Changed after review" },
                packet: value.packet,
                commandId: "stale-update",
              },
              context,
            );
          if (scenario === "manifest mismatch")
            yield* sql`UPDATE workflow_planning_observations SET manifest_hash = ${"0".repeat(64)}`;
          if (scenario === "malformed")
            yield* sql`UPDATE workflow_planning_passes SET record_json = '{}'`;
          if (scenario === "unresolved disposition") {
            const disposition = {
              reportId: entry.pass.id,
              findingId,
              decision: "defer",
              rationale: "Later",
              affectedObjectIds: [objectId],
            };
            yield* sql`INSERT INTO workflow_planning_dispositions (review_pass_id, finding_id, repair_pass_id, disposition_json) VALUES (${entry.pass.id}, ${findingId}, ${entry.pass.id}, ${json(disposition)})`;
          }
          if (scenario === "ambiguous reviews") {
            const other = makePass(value, "other", "review");
            yield* service.beginPass(
              {
                sessionId: planningSession.id,
                expectedVersion: 2,
                ...other,
                command: planningCommand("other"),
              },
              planningContext,
            );
            yield* sql`UPDATE workflow_planning_passes SET native_thread_id = ${native.threadId}, record_json = json_set(record_json, '$.native.threadId', ${native.threadId}) WHERE id = ${other.pass.id}`;
          }
          if (scenario === "malformed" || scenario === "non-review")
            expect(yield* reader.findReview(lookup), scenario).toBeUndefined();
          expect(
            (yield* service.validateCoverage({ planId: "plan" }, context)).reviewReady,
            scenario,
          ).toBe(scenario === "positive");
        }).pipe(
          Effect.provide(
            layer(
              ":memory:",
              grants,
              Layer.merge(Service.persistedPassReaderLayer, Service.localAttestationLayer).pipe(
                Layer.provide(Digest.layerWebCrypto),
              ),
            ),
          ),
        );
      }
    }),
  );
});
