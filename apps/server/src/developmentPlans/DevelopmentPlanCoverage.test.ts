import { describe, expect, it } from "@effect/vitest";
import { decodePlanningPassV1, upgradeDevelopmentPlanDocumentV1 } from "@t3tools/contracts";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import { DevelopmentPlanCoverage } from "./DevelopmentPlanCoverage.ts";
import { DevelopmentPlanHash } from "./DevelopmentPlanHash.ts";

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
    bytes: new TextEncoder().encode(JSON.stringify(example)),
    hashSourcePacket: DevelopmentPlanHash.sourcePacket,
  });
  const requirement = native.revision.requirements[0]!;
  const workItem = {
    id: "work",
    explanation: "Explanation",
    sourceStatementIds: ["statement"],
    access: native.revision.access,
    title: "Work",
    state: "active" as const,
    requirementIds: [requirement.id],
    criterionIds: ["criterion"],
    dependencyIds: [] as string[],
    fileChanges: [],
    verification: [{ id: "verification", description: "Check", criterionIds: ["criterion"] }],
  };
  const revision = { ...native.revision, workItems: [workItem] };
  const revisionHash = DevelopmentPlanHash.revision(revision);
  const sourcePacketHash = DevelopmentPlanHash.sourcePacket(native.sourcePacket);
  const access = native.sourcePacket.access;
  const reference = { kind: "source", id: "source", sha256: example.sources[0]!.sha256 };
  const manifest = { sources: [reference], references: [], additionalContext: [], access };
  const review = yield* decodePlanningPassV1({
    schemaVersion: 1,
    id: "pass",
    planningSessionId: "session",
    role: "review",
    provider: { providerInstanceId: "codex", model: "model" },
    binding: {
      planId: "plan",
      revisionId: "revision",
      revisionSchemaVersion: 1,
      revisionHash,
      sourcePacketHash,
    },
    operationId: "op",
    inputManifest: manifest,
    status: "settled",
    native: { threadId: "thread", runId: "run", terminalMessageId: "message" },
    report: {
      kind: "review",
      verdict: "pass",
      assessment: {
        revisionHash,
        sourcePacketHash,
        inventory: [
          {
            id: "inventory",
            text: "Outcome",
            sourceRefs: [reference],
            requirementIds: [requirement.id],
            disposition: "mapped",
          },
        ],
      },
      findings: [],
    },
    access,
  });
  if (!review.report || review.report.kind !== "review")
    return yield* Effect.die("invalid fixture");
  return {
    revision,
    sourcePacket: native.sourcePacket,
    review: { ...review, report: review.report },
    workItem,
    requirement,
    revisionHash,
    sourcePacketHash,
  };
});

describe("DevelopmentPlanCoverage", () => {
  it.effect("reports structural coverage and activity rules", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const criterion = f.requirement.criteria[0]!;
      const question = {
        id: "question",
        explanation: "Explanation",
        sourceStatementIds: ["statement"],
        access: f.revision.access,
        text: "Question",
        state: "open" as const,
        executionBlocking: true,
      };
      const cases = [
        { name: "complete", revision: f.revision, expected: [] },
        {
          name: "no active criterion",
          revision: {
            ...f.revision,
            requirements: [{ ...f.requirement, criteria: [{ ...criterion, state: "withdrawn" }] }],
          },
          expected: [{ code: "requirement-without-criterion", objectId: "requirement" }],
        },
        {
          name: "no mapping",
          revision: { ...f.revision, workItems: [{ ...f.workItem, criterionIds: [] }] },
          expected: [{ code: "criterion-without-work-item", objectId: "criterion" }],
        },
        {
          name: "no verification",
          revision: { ...f.revision, workItems: [{ ...f.workItem, verification: [] }] },
          expected: [{ code: "criterion-without-verification", objectId: "criterion" }],
        },
        {
          name: "blocking question",
          revision: { ...f.revision, questions: [question] },
          expected: [{ code: "open-blocking-question", objectId: "question" }],
        },
        {
          name: "withdrawn requirement",
          revision: {
            ...f.revision,
            requirements: [{ ...f.requirement, state: "withdrawn" }],
            workItems: [],
          },
          expected: [],
        },
        {
          name: "draft work",
          revision: { ...f.revision, workItems: [{ ...f.workItem, state: "draft" as const }] },
          expected: [
            { code: "criterion-without-verification", objectId: "criterion" },
            { code: "criterion-without-work-item", objectId: "criterion" },
          ],
        },
      ];
      for (const c of cases) {
        const result = DevelopmentPlanCoverage.validate({ ...f, revision: c.revision });
        expect(result.structuralBlockers, c.name).toEqual(c.expected);
      }
      expect(DevelopmentPlanCoverage.validate(f)).toEqual({
        revisionHash: f.revisionHash,
        sourcePacketHash: f.sourcePacketHash,
        structuralBlockers: [],
        reviewBlockers: [],
      });
    }).pipe(Effect.provide(Digest.layerWebCrypto)),
  );

  it.effect("reports review blockers independently and deterministically", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const report = f.review.report;
      const { report: _report, ...reviewWithoutReport } = f.review;
      const inventory = report.assessment.inventory[0]!;
      const finding = {
        id: "finding" as typeof inventory.id as never,
        severity: "blocking" as const,
        description: "Problem",
        affectedObjectIds: [],
        evidence: [],
        proposedRemedy: "Fix",
      };
      const cases = [
        { name: "missing", review: undefined, expected: [{ code: "review-missing" }] },
        {
          name: "wrong role",
          review: { ...f.review, role: "author" as const },
          expected: [{ code: "review-not-review" }],
        },
        {
          name: "wrong report",
          review: {
            ...f.review,
            report: { kind: "author" as const, summary: "Summary", outputs: [] },
          },
          expected: [{ code: "review-not-review" }],
        },
        {
          name: "unsettled",
          review: { ...f.review, status: "claimed" as const },
          expected: [{ code: "review-unsettled" }],
        },
        {
          name: "no report",
          review: reviewWithoutReport,
          expected: [{ code: "review-unsettled" }],
        },
        ...(["revisionHash", "sourcePacketHash"] as const).flatMap((key) => [
          {
            name: `binding ${key}`,
            review: { ...f.review, binding: { ...f.review.binding, [key]: "0".repeat(64) } },
            expected: [{ code: "review-stale" }],
          },
          {
            name: `assessment ${key}`,
            review: {
              ...f.review,
              report: { ...report, assessment: { ...report.assessment, [key]: "0".repeat(64) } },
            },
            expected: [{ code: "review-stale" }],
          },
        ]),
        {
          name: "nonpassing",
          review: { ...f.review, report: { ...report, verdict: "inconclusive" as const } },
          expected: [{ code: "review-not-passing" }],
        },
        ...(["missing", "uncertain", "proposed_exclusion"] as const).map((disposition) => ({
          name: disposition,
          review: {
            ...f.review,
            report: {
              ...report,
              assessment: { ...report.assessment, inventory: [{ ...inventory, disposition }] },
            },
          },
          expected: [{ code: "anchored-omission", objectId: "inventory" }],
        })),
        {
          name: "blocking finding",
          review: { ...f.review, report: { ...report, findings: [finding] } },
          expected: [{ code: "blocking-finding", objectId: "finding" }],
        },
      ];
      for (const c of cases) {
        const result = DevelopmentPlanCoverage.validate({
          revision: f.revision,
          sourcePacket: f.sourcePacket,
          ...(c.review ? { review: c.review } : {}),
        });
        expect(result.structuralBlockers, c.name).toEqual([]);
        expect(result.reviewBlockers, c.name).toEqual(c.expected);
      }
      const result = DevelopmentPlanCoverage.validate({
        ...f,
        review: {
          ...f.review,
          report: {
            ...report,
            verdict: "changes_requested",
            assessment: {
              ...report.assessment,
              inventory: [{ ...inventory, disposition: "missing" }],
            },
            findings: [finding],
          },
        },
      });
      expect(result.reviewBlockers).toEqual([
        { code: "anchored-omission", objectId: "inventory" },
        { code: "blocking-finding", objectId: "finding" },
        { code: "review-not-passing" },
      ]);
    }).pipe(Effect.provide(Digest.layerWebCrypto)),
  );

  it.effect("prevalidates references and cycles", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const envelope = {
        planId: f.revision.planId,
        revisionId: "revision",
        revisionHash: f.revisionHash,
        sourcePacketHash: f.sourcePacketHash,
        revision: f.revision,
        sourcePacket: f.sourcePacket,
        createdAt: timestamp,
      };
      const cases = [
        { input: envelope, expected: { ok: true, envelope } },
        {
          input: {
            ...envelope,
            revision: { ...f.revision, workItems: [{ ...f.workItem, criterionIds: ["missing"] }] },
          },
          expected: { ok: false, blockers: [{ code: "invalid-reference" }] },
        },
        {
          input: {
            ...envelope,
            revision: { ...f.revision, workItems: [{ ...f.workItem, dependencyIds: ["work"] }] },
          },
          expected: { ok: false, blockers: [{ code: "dependency-cycle" }] },
        },
        { input: {}, expected: { ok: false, blockers: [{ code: "invalid-record" }] } },
      ];
      for (const c of cases)
        expect(yield* DevelopmentPlanCoverage.prevalidate(c.input)).toEqual(c.expected);
    }).pipe(Effect.provide(Digest.layerWebCrypto)),
  );
});
